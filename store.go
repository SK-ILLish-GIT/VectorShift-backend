package idempotency

import (
	"crypto/sha256"
	"net/http"
	"sync"
	"time"
)

type entryState int

const (
	stateInFlight entryState = iota
	stateDone
)

// storedResponse is a handler's response, captured for replay.
type storedResponse struct {
	status int
	header http.Header
	body   []byte
}

type entry struct {
	fingerprint [sha256.Size]byte
	state       entryState
	createdAt   time.Time
	response    *storedResponse
}

// claimResult is the store's verdict on a request.
type claimResult int

const (
	claimAcquired   claimResult = iota // the caller owns the key and must run the handler
	claimInFlight                      // another execution of this key is running
	claimReplay                        // a stored response is available
	claimMismatch                      // the key is known but describes a different request
	claimAtCapacity                    // the store is full of in-flight entries
)

type store struct {
	mu         sync.Mutex
	entries    map[string]*entry
	ttl        time.Duration
	maxEntries int

	// now is a seam for tests; production always uses time.Now.
	now func() time.Time

	stop     chan struct{}
	stopOnce sync.Once
}

func newStore(ttl time.Duration, maxEntries int) *store {
	s := &store{
		entries:    make(map[string]*entry),
		ttl:        ttl,
		maxEntries: maxEntries,
		now:        time.Now,
		stop:       make(chan struct{}),
	}
	go s.sweepLoop(sweepInterval(ttl))
	return s
}

func sweepInterval(ttl time.Duration) time.Duration {
	return min(max(ttl/4, time.Second), time.Minute)
}

func (s *store) sweepLoop(every time.Duration) {
	t := time.NewTicker(every)
	defer t.Stop()
	for {
		select {
		case <-s.stop:
			return
		case <-t.C:
			s.sweep()
		}
	}
}

func (s *store) close() {
	s.stopOnce.Do(func() { close(s.stop) })
}

// claim decides what happens to a request carrying key, and reserves the key
// when the caller is the one who must execute the handler.
func (s *store) claim(key string, fp [sha256.Size]byte) (claimResult, *storedResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if e, ok := s.entries[key]; ok {
		if s.expiredLocked(e) {
			delete(s.entries, key)
		} else {
			// Fingerprint before state: a caller reusing a key for a different
			// request has made a mistake worth reporting, whether or not the
			// original execution has finished.
			if e.fingerprint != fp {
				return claimMismatch, nil
			}
			if e.state == stateInFlight {
				return claimInFlight, nil
			}
			return claimReplay, e.response
		}
	}

	if s.maxEntries > 0 && len(s.entries) >= s.maxEntries && !s.evictOldestCompletedLocked() {
		return claimAtCapacity, nil
	}

	s.entries[key] = &entry{fingerprint: fp, state: stateInFlight, createdAt: s.now()}
	return claimAcquired, nil
}

// complete stores a response against a key that this caller claimed.
func (s *store) complete(key string, resp *storedResponse) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[key]; ok && e.state == stateInFlight {
		e.state = stateDone
		e.response = resp
	}
}

// abandon releases a claimed key without storing a response, so a retry
// re-executes the handler.
func (s *store) abandon(key string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	if e, ok := s.entries[key]; ok && e.state == stateInFlight {
		delete(s.entries, key)
	}
}

// expiredLocked reports whether an entry has aged out. Only completed entries
// expire: an in-flight entry older than the TTL means a handler is still
// running, and dropping it would let a duplicate execute.
func (s *store) expiredLocked(e *entry) bool {
	if s.ttl <= 0 || e.state != stateDone {
		return false
	}
	return s.now().Sub(e.createdAt) >= s.ttl
}

// evictOldestCompletedLocked frees one slot. In-flight entries are never
// evicted, because dropping one silently re-enables double execution for a key
// the caller still believes is protected. The scan is linear, but only runs
// when the store is already at its ceiling.
func (s *store) evictOldestCompletedLocked() bool {
	var oldestKey string
	var oldestAt time.Time
	found := false
	for k, e := range s.entries {
		if e.state != stateDone {
			continue
		}
		if !found || e.createdAt.Before(oldestAt) {
			oldestKey, oldestAt, found = k, e.createdAt, true
		}
	}
	if found {
		delete(s.entries, oldestKey)
	}
	return found
}

func (s *store) sweep() {
	s.mu.Lock()
	defer s.mu.Unlock()
	for k, e := range s.entries {
		if s.expiredLocked(e) {
			delete(s.entries, k)
		}
	}
}

func (s *store) len() int {
	s.mu.Lock()
	defer s.mu.Unlock()
	return len(s.entries)
}
