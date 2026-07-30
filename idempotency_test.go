package idempotency

import (
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// fakeClock is race-safe because the store reads it from its sweeper goroutine.
type fakeClock struct {
	mu sync.Mutex
	t  time.Time
}

func (c *fakeClock) now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.t
}

func (c *fakeClock) advance(d time.Duration) {
	c.mu.Lock()
	c.t = c.t.Add(d)
	c.mu.Unlock()
}

// setClock swaps the store's clock under its own lock, which is where every
// other read of it happens.
func setClock(m *Middleware, c *fakeClock) {
	m.store.mu.Lock()
	m.store.now = c.now
	m.store.mu.Unlock()
}

func newMiddleware(t *testing.T, opts Options) *Middleware {
	t.Helper()
	m := New(opts)
	t.Cleanup(func() { m.Close() })
	return m
}

func do(h http.Handler, method, target, key, body string) *httptest.ResponseRecorder {
	r := httptest.NewRequest(method, target, strings.NewReader(body))
	if key != "" {
		r.Header.Set(HeaderKey, key)
	}
	w := httptest.NewRecorder()
	h.ServeHTTP(w, r)
	return w
}

// countingHandler reports how many times it actually executed.
func countingHandler(calls *int32, status int, body string) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(calls, 1)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(status)
		fmt.Fprint(w, body)
	})
}

func TestRetryReplaysStoredResponse(t *testing.T) {
	var calls int32
	h := newMiddleware(t, Options{}).Wrap(countingHandler(&calls, http.StatusCreated, `{"id":1}`))

	first := do(h, http.MethodPost, "/charges", "k1", `{"amount":100}`)
	second := do(h, http.MethodPost, "/charges", "k1", `{"amount":100}`)

	if calls != 1 {
		t.Fatalf("handler executed %d times, want 1", calls)
	}
	if first.Code != http.StatusCreated || second.Code != http.StatusCreated {
		t.Fatalf("statuses = %d, %d; want 201, 201", first.Code, second.Code)
	}
	if first.Body.String() != second.Body.String() {
		t.Fatalf("bodies differ: %q vs %q", first.Body, second.Body)
	}
	if got := second.Header().Get("Content-Type"); got != "application/json" {
		t.Errorf("replayed Content-Type = %q, want application/json", got)
	}
	if first.Header().Get(HeaderReplay) != "" {
		t.Error("first response should not be marked as a replay")
	}
	if second.Header().Get(HeaderReplay) != "true" {
		t.Error("replayed response missing " + HeaderReplay)
	}
}

func TestRequestWithoutKeyPassesThrough(t *testing.T) {
	var calls int32
	h := newMiddleware(t, Options{}).Wrap(countingHandler(&calls, http.StatusOK, "ok"))

	do(h, http.MethodPost, "/charges", "", "body")
	do(h, http.MethodPost, "/charges", "", "body")

	if calls != 2 {
		t.Fatalf("handler executed %d times, want 2: unkeyed requests are not deduplicated", calls)
	}
}

func TestRequireKeyRejectsUnkeyedRequests(t *testing.T) {
	var calls int32
	h := newMiddleware(t, Options{RequireKey: true}).Wrap(countingHandler(&calls, http.StatusOK, "ok"))

	w := do(h, http.MethodPost, "/charges", "", "body")

	if w.Code != http.StatusBadRequest {
		t.Fatalf("status = %d, want 400", w.Code)
	}
	if calls != 0 {
		t.Fatalf("handler executed %d times, want 0", calls)
	}
}

func TestSameKeyDifferentBodyIsRejected(t *testing.T) {
	var calls int32
	h := newMiddleware(t, Options{}).Wrap(countingHandler(&calls, http.StatusOK, "ok"))

	do(h, http.MethodPost, "/charges", "k1", `{"amount":100}`)
	w := do(h, http.MethodPost, "/charges", "k1", `{"amount":999}`)

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", w.Code)
	}
	if calls != 1 {
		t.Fatalf("handler executed %d times, want 1", calls)
	}
}

func TestSameKeyDifferentRouteIsRejected(t *testing.T) {
	var calls int32
	h := newMiddleware(t, Options{}).Wrap(countingHandler(&calls, http.StatusOK, "ok"))

	do(h, http.MethodPost, "/charges", "k1", "same body")
	w := do(h, http.MethodPost, "/refunds", "k1", "same body")

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", w.Code)
	}
	if calls != 1 {
		t.Fatalf("handler executed %d times, want 1", calls)
	}
}

func TestSameKeyDifferentMethodIsRejected(t *testing.T) {
	var calls int32
	h := newMiddleware(t, Options{}).Wrap(countingHandler(&calls, http.StatusOK, "ok"))

	do(h, http.MethodPost, "/charges", "k1", "same body")
	w := do(h, http.MethodPut, "/charges", "k1", "same body")

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", w.Code)
	}
	if calls != 1 {
		t.Fatalf("handler executed %d times, want 1", calls)
	}
}

// The fingerprint covers the whole request target, not just the path, so a key
// cannot be carried across two requests that differ only in their query.
func TestSameKeyDifferentQueryIsRejected(t *testing.T) {
	var calls int32
	h := newMiddleware(t, Options{}).Wrap(countingHandler(&calls, http.StatusOK, "ok"))

	do(h, http.MethodPost, "/charges", "k1", "same body")
	w := do(h, http.MethodPost, "/charges?dry_run=1", "k1", "same body")

	if w.Code != http.StatusUnprocessableEntity {
		t.Fatalf("status = %d, want 422", w.Code)
	}
	if calls != 1 {
		t.Fatalf("handler executed %d times, want 1", calls)
	}
}

func TestClientErrorIsReplayed(t *testing.T) {
	var calls int32
	h := newMiddleware(t, Options{}).Wrap(countingHandler(&calls, http.StatusBadRequest, "invalid"))

	do(h, http.MethodPost, "/charges", "k1", "body")
	w := do(h, http.MethodPost, "/charges", "k1", "body")

	if calls != 1 {
		t.Fatalf("handler executed %d times, want 1: a 4xx is deterministic and replays", calls)
	}
	if w.Code != http.StatusBadRequest || w.Header().Get(HeaderReplay) != "true" {
		t.Fatalf("status = %d, replay = %q; want 400, true", w.Code, w.Header().Get(HeaderReplay))
	}
}

func TestServerErrorAllowsReExecution(t *testing.T) {
	var calls int32
	h := newMiddleware(t, Options{}).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			http.Error(w, "upstream unavailable", http.StatusInternalServerError)
			return
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "recovered")
	}))

	if w := do(h, http.MethodPost, "/charges", "k1", "body"); w.Code != http.StatusInternalServerError {
		t.Fatalf("first status = %d, want 500", w.Code)
	}
	w := do(h, http.MethodPost, "/charges", "k1", "body")

	if calls != 2 {
		t.Fatalf("handler executed %d times, want 2: a 5xx must not poison the key", calls)
	}
	if w.Code != http.StatusOK || w.Body.String() != "recovered" {
		t.Fatalf("retry got %d %q; want 200 \"recovered\"", w.Code, w.Body)
	}
}

func TestPanicReleasesTheKeyAndPropagates(t *testing.T) {
	var calls int32
	h := newMiddleware(t, Options{}).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if atomic.AddInt32(&calls, 1) == 1 {
			panic("boom")
		}
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, "fine")
	}))

	func() {
		defer func() {
			if recover() == nil {
				t.Error("panic should reach the server's own recovery, not be swallowed here")
			}
		}()
		do(h, http.MethodPost, "/charges", "k1", "body")
	}()

	w := do(h, http.MethodPost, "/charges", "k1", "body")

	if calls != 2 {
		t.Fatalf("handler executed %d times, want 2", calls)
	}
	if w.Code != http.StatusOK {
		t.Fatalf("retry status = %d, want 200", w.Code)
	}
}

func TestRetryDuringExecutionGetsConflict(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var calls int32
	h := newMiddleware(t, Options{}).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		close(started)
		<-release
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, "done")
	}))

	done := make(chan *httptest.ResponseRecorder, 1)
	go func() { done <- do(h, http.MethodPost, "/charges", "k1", "body") }()
	<-started

	retry := do(h, http.MethodPost, "/charges", "k1", "body")
	if retry.Code != http.StatusConflict {
		t.Fatalf("in-flight retry status = %d, want 409", retry.Code)
	}

	close(release)
	if first := <-done; first.Code != http.StatusCreated {
		t.Fatalf("first status = %d, want 201", first.Code)
	}
	if calls != 1 {
		t.Fatalf("handler executed %d times, want 1", calls)
	}

	// Once the first execution finishes, the same retry replays instead.
	after := do(h, http.MethodPost, "/charges", "k1", "body")
	if after.Code != http.StatusCreated || after.Header().Get(HeaderReplay) != "true" {
		t.Fatalf("post-completion retry got %d replay=%q; want 201 true", after.Code, after.Header().Get(HeaderReplay))
	}
}

func TestExpiredKeyReExecutes(t *testing.T) {
	var calls int32
	m := newMiddleware(t, Options{TTL: time.Minute})
	clock := &fakeClock{t: time.Now()}
	setClock(m, clock)
	h := m.Wrap(countingHandler(&calls, http.StatusOK, "ok"))

	do(h, http.MethodPost, "/charges", "k1", "body")
	clock.advance(2 * time.Minute)
	do(h, http.MethodPost, "/charges", "k1", "body")

	if calls != 2 {
		t.Fatalf("handler executed %d times, want 2 after expiry", calls)
	}
}

// The sweeper is the half of expiry that no request triggers: an untouched key
// must still be reclaimed, or a quiet store never gives its memory back.
func TestSweeperReclaimsUntouchedExpiredEntries(t *testing.T) {
	var calls int32
	m := newMiddleware(t, Options{TTL: 2 * time.Second})
	clock := &fakeClock{t: time.Now()}
	setClock(m, clock)
	h := m.Wrap(countingHandler(&calls, http.StatusOK, "ok"))

	do(h, http.MethodPost, "/charges", "k1", "body")
	if got := m.store.len(); got != 1 {
		t.Fatalf("store holds %d entries, want 1", got)
	}

	clock.advance(time.Hour)

	deadline := time.Now().Add(5 * time.Second)
	for m.store.len() != 0 {
		if time.Now().After(deadline) {
			t.Fatal("sweeper never reclaimed the expired entry")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

// A response too large to retain cannot be replayed faithfully, so it must not
// be replayed at all.
func TestOversizedResponseIsNotStored(t *testing.T) {
	var calls int32
	big := strings.Repeat("y", 64)
	h := newMiddleware(t, Options{MaxBodyBytes: 16}).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		w.WriteHeader(http.StatusOK)
		fmt.Fprint(w, big)
	}))

	first := do(h, http.MethodPost, "/charges", "k1", "small")
	if first.Body.String() != big {
		t.Fatal("the client must still receive the whole response, capped or not")
	}

	second := do(h, http.MethodPost, "/charges", "k1", "small")
	if calls != 2 {
		t.Fatalf("handler executed %d times, want 2: an unstorable response must not be replayed", calls)
	}
	if second.Header().Get(HeaderReplay) == "true" {
		t.Error("nothing was stored, so nothing should be marked as a replay")
	}
}

func TestEvictionTakesTheOldestCompletedEntry(t *testing.T) {
	var calls int32
	m := newMiddleware(t, Options{MaxEntries: 2})
	h := m.Wrap(countingHandler(&calls, http.StatusOK, "ok"))

	do(h, http.MethodPost, "/charges", "k1", "body")
	do(h, http.MethodPost, "/charges", "k2", "body")
	do(h, http.MethodPost, "/charges", "k3", "body") // evicts k1

	if got := m.store.len(); got != 2 {
		t.Fatalf("store holds %d entries, want 2", got)
	}
	if calls != 3 {
		t.Fatalf("handler executed %d times, want 3", calls)
	}

	// k2 survived and still replays; k1 was evicted and re-executes.
	if w := do(h, http.MethodPost, "/charges", "k2", "body"); w.Header().Get(HeaderReplay) != "true" {
		t.Error("k2 should still replay")
	}
	if w := do(h, http.MethodPost, "/charges", "k1", "body"); w.Header().Get(HeaderReplay) == "true" {
		t.Error("k1 was evicted and should have re-executed")
	}
}

func TestCapacityWithNothingEvictableIsRefused(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	m := newMiddleware(t, Options{MaxEntries: 1})
	h := m.Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		close(started)
		<-release
		w.WriteHeader(http.StatusOK)
	}))

	go do(h, http.MethodPost, "/charges", "k1", "body")
	<-started

	w := do(h, http.MethodPost, "/charges", "k2", "body")
	if w.Code != http.StatusServiceUnavailable {
		t.Fatalf("status = %d, want 503: an in-flight entry must not be evicted", w.Code)
	}
	close(release)
}

func TestOversizedBodyIsRefused(t *testing.T) {
	var calls int32
	h := newMiddleware(t, Options{MaxBodyBytes: 16}).Wrap(countingHandler(&calls, http.StatusOK, "ok"))

	w := do(h, http.MethodPost, "/charges", "k1", strings.Repeat("x", 17))

	if w.Code != http.StatusRequestEntityTooLarge {
		t.Fatalf("status = %d, want 413", w.Code)
	}
	if calls != 0 {
		t.Fatalf("handler executed %d times, want 0", calls)
	}
}

func TestHandlerStillReceivesTheBody(t *testing.T) {
	got := make(chan string, 1)
	h := newMiddleware(t, Options{}).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		b := make([]byte, 32)
		n, _ := r.Body.Read(b)
		got <- string(b[:n])
	}))

	do(h, http.MethodPost, "/charges", "k1", "payload")

	if v := <-got; v != "payload" {
		t.Fatalf("handler read %q, want %q: the body must be replaced after fingerprinting", v, "payload")
	}
}

func TestConcurrentRetriesExecuteOnce(t *testing.T) {
	const goroutines = 64
	var calls int32
	h := newMiddleware(t, Options{}).Wrap(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		atomic.AddInt32(&calls, 1)
		time.Sleep(time.Millisecond)
		w.WriteHeader(http.StatusCreated)
		fmt.Fprint(w, "charged")
	}))

	var wg sync.WaitGroup
	codes := make([]int, goroutines)
	start := make(chan struct{})
	for i := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			<-start
			codes[i] = do(h, http.MethodPost, "/charges", "same-key", "body").Code
		}()
	}
	close(start)
	wg.Wait()

	if calls != 1 {
		t.Fatalf("handler executed %d times, want exactly 1", calls)
	}
	for i, c := range codes {
		if c != http.StatusCreated && c != http.StatusConflict {
			t.Fatalf("goroutine %d got %d; want 201 or 409", i, c)
		}
	}
}

func TestConcurrentDistinctKeysAllExecute(t *testing.T) {
	const goroutines = 64
	var calls int32
	h := newMiddleware(t, Options{}).Wrap(countingHandler(&calls, http.StatusOK, "ok"))

	var wg sync.WaitGroup
	for i := range goroutines {
		wg.Add(1)
		go func() {
			defer wg.Done()
			do(h, http.MethodPost, "/charges", fmt.Sprintf("key-%d", i), "body")
		}()
	}
	wg.Wait()

	if calls != goroutines {
		t.Fatalf("handler executed %d times, want %d", calls, goroutines)
	}
}
