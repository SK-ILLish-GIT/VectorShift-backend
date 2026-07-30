// Package idempotency provides HTTP middleware that makes mutating endpoints
// safe to retry.
//
// A request carrying an Idempotency-Key header executes its handler at most
// once; subsequent requests with the same key replay the stored response
// instead of executing again. State is held in memory on a single node.
package idempotency

import (
	"bytes"
	"crypto/sha256"
	"encoding/binary"
	"io"
	"net/http"
	"time"
)

const (
	// HeaderKey is the request header carrying the caller's idempotency key.
	HeaderKey = "Idempotency-Key"
	// HeaderReplay marks a response served from the store rather than the handler.
	HeaderReplay = "Idempotent-Replay"
)

// Defaults applied when an Options field is left zero.
const (
	DefaultTTL          = 24 * time.Hour
	DefaultMaxEntries   = 10_000
	DefaultMaxBodyBytes = 1 << 20 // 1 MiB
)

// Options configures a Middleware. The zero value is usable and applies the
// defaults above.
type Options struct {
	// TTL is how long a key stays protected after it is first seen.
	TTL time.Duration
	// MaxEntries caps the number of keys held on this node.
	MaxEntries int
	// MaxBodyBytes caps both the request body read for fingerprinting and the
	// response body retained for replay.
	MaxBodyBytes int64
	// RequireKey rejects requests that arrive without an idempotency key.
	// Off by default so the middleware can be adopted one endpoint at a time.
	RequireKey bool
}

func (o Options) withDefaults() Options {
	if o.TTL == 0 {
		o.TTL = DefaultTTL
	}
	if o.MaxEntries == 0 {
		o.MaxEntries = DefaultMaxEntries
	}
	if o.MaxBodyBytes == 0 {
		o.MaxBodyBytes = DefaultMaxBodyBytes
	}
	return o
}

// Middleware wraps handlers so that retries with the same idempotency key do
// not execute them twice. It is safe for concurrent use.
type Middleware struct {
	opts  Options
	store *store
}

// New builds a Middleware. Call Close when it is no longer needed to stop the
// background expiry sweeper.
func New(opts Options) *Middleware {
	opts = opts.withDefaults()
	return &Middleware{opts: opts, store: newStore(opts.TTL, opts.MaxEntries)}
}

// Close stops the background sweeper. It is safe to call more than once.
// In-flight requests are unaffected; stored entries simply stop expiring.
func (m *Middleware) Close() error {
	m.store.close()
	return nil
}

// Wrap returns next guarded by this middleware.
func (m *Middleware) Wrap(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get(HeaderKey)
		if key == "" {
			if m.opts.RequireKey {
				http.Error(w, HeaderKey+" header is required", http.StatusBadRequest)
				return
			}
			next.ServeHTTP(w, r)
			return
		}

		// The body has to be read to be fingerprinted, so it is buffered and the
		// handler is handed a fresh reader over the same bytes. A body too large
		// to fingerprint is refused rather than passed through, so that the
		// largest requests do not end up with the weakest guarantee.
		body, err := io.ReadAll(io.LimitReader(r.Body, m.opts.MaxBodyBytes+1))
		if err != nil {
			http.Error(w, "could not read request body", http.StatusBadRequest)
			return
		}
		if int64(len(body)) > m.opts.MaxBodyBytes {
			http.Error(w, "request body exceeds idempotency limit", http.StatusRequestEntityTooLarge)
			return
		}
		r.Body = io.NopCloser(bytes.NewReader(body))

		fp := fingerprint(r.Method, r.URL.RequestURI(), body)

		switch result, stored := m.store.claim(key, fp); result {
		case claimInFlight:
			http.Error(w, "a request with this "+HeaderKey+" is already in progress", http.StatusConflict)
			return
		case claimMismatch:
			http.Error(w, HeaderKey+" was already used for a different request", http.StatusUnprocessableEntity)
			return
		case claimAtCapacity:
			http.Error(w, "idempotency store is at capacity", http.StatusServiceUnavailable)
			return
		case claimReplay:
			writeStored(w, stored)
			return
		case claimAcquired:
			// This caller owns the key; fall through and execute.
		}

		rec := &recorder{ResponseWriter: w, status: http.StatusOK, limit: m.opts.MaxBodyBytes}
		completed := false
		defer func() {
			// Reached only when the handler panicked. Release the key so a retry
			// can succeed, then let the panic continue unchanged: recovering it
			// here would hide the crash from whatever the application already
			// uses to report them.
			if !completed {
				m.store.abandon(key)
			}
		}()

		next.ServeHTTP(rec, r)
		completed = true

		// A 5xx is presumptively transient. Retaining it would poison the key for
		// its whole TTL, leaving the caller unable to ever succeed. An oversized
		// response cannot be replayed faithfully, so it is not retained either.
		if rec.status >= http.StatusInternalServerError || rec.overflowed {
			m.store.abandon(key)
			return
		}
		m.store.complete(key, rec.captured())
	})
}

// fingerprint identifies a request by method, target and body. Fields are
// length-prefixed so that no combination of contents can produce the same
// digest as a different combination.
func fingerprint(method, target string, body []byte) [sha256.Size]byte {
	h := sha256.New()
	var n [8]byte
	for _, field := range [][]byte{[]byte(method), []byte(target), body} {
		binary.BigEndian.PutUint64(n[:], uint64(len(field)))
		h.Write(n[:])
		h.Write(field)
	}
	var out [sha256.Size]byte
	copy(out[:], h.Sum(nil))
	return out
}

func writeStored(w http.ResponseWriter, resp *storedResponse) {
	for k, vs := range resp.header {
		for _, v := range vs {
			w.Header().Add(k, v)
		}
	}
	w.Header().Set(HeaderReplay, "true")
	w.WriteHeader(resp.status)
	w.Write(resp.body)
}

// recorder captures a handler's response while passing it through to the
// client unchanged.
type recorder struct {
	http.ResponseWriter
	status      int
	wroteHeader bool
	header      http.Header
	body        bytes.Buffer
	limit       int64
	overflowed  bool
}

func (rec *recorder) WriteHeader(code int) {
	if rec.wroteHeader {
		return
	}
	rec.status = code
	// Snapshot before the server adds its own headers on the way out, so only
	// what the handler set is replayed.
	rec.header = rec.ResponseWriter.Header().Clone()
	rec.wroteHeader = true
	rec.ResponseWriter.WriteHeader(code)
}

func (rec *recorder) Write(p []byte) (int, error) {
	if !rec.wroteHeader {
		rec.WriteHeader(http.StatusOK)
	}
	if !rec.overflowed {
		if int64(rec.body.Len()+len(p)) > rec.limit {
			rec.overflowed = true
			rec.body.Reset()
		} else {
			rec.body.Write(p)
		}
	}
	return rec.ResponseWriter.Write(p)
}

// Flush lets streaming handlers keep working through the recorder.
func (rec *recorder) Flush() {
	if f, ok := rec.ResponseWriter.(http.Flusher); ok {
		f.Flush()
	}
}

func (rec *recorder) captured() *storedResponse {
	header := rec.header
	if header == nil { // the handler returned without writing anything
		header = rec.ResponseWriter.Header().Clone()
	}
	return &storedResponse{
		status: rec.status,
		header: header,
		body:   bytes.Clone(rec.body.Bytes()),
	}
}
