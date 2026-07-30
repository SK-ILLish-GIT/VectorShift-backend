# Design Notes

An idempotency-key middleware for HTTP POST handlers. In-memory, single node, stdlib only.

The middleware wraps an `http.Handler`. It reads the `Idempotency-Key` request header and decides
one of four things: run the handler and remember the result, replay a result it already has, reject
the request, or pass through untouched.

## Semantics

| Situation | Behaviour |
| --- | --- |
| No `Idempotency-Key` | Pass through. Handler runs, nothing is stored. |
| Request body exceeds `MaxBodyBytes` | `413 Request Entity Too Large`. Handler does not run. |
| Unknown key | Handler runs. Status, headers and body are captured and stored. |
| Unknown key, store full of in-flight entries | `503 Service Unavailable`. Handler does not run. |
| Key in flight | `409 Conflict`. Handler does not run. |
| Key complete, fingerprint matches | Stored response replayed with `Idempotent-Replay: true`. |
| Key complete, fingerprint differs | `422 Unprocessable Entity`. Handler does not run. |
| Key completed with 5xx, or panicked | Entry is not retained. Handler runs again. |
| Response exceeds `MaxBodyBytes` | Client still receives it in full, but it is not retained. Handler runs again. |
| Key expired or evicted | Treated as unknown. |

Every decision below is a choice between defensible options, so each records what was rejected.

## 1. A retry arrives while the first execution is still running

**`409 Conflict`, immediately. The handler is not invoked.**

The second caller's only correct next action is to back off and retry, and 409 tells them that in one
round trip. The alternative — holding the connection open — couples the retry's latency and lifetime
to a handler this library does not control and cannot bound.

*Rejected: block until the first execution completes, then replay its response.* This is better for
the caller: one request, one coherent answer, no client-side retry loop. It costs a completion
channel per entry, a wait timeout, a policy for callers parked behind a hung handler, and a second
class of failure to test. Against a fixed budget, the version that is obviously correct beats the
version that is nicer and subtly wrong.

*Rejected: `202 Accepted`.* Implies an asynchronous job with somewhere to poll for the result. No
such endpoint exists here, so 202 would be a promise the library cannot keep.

## 2. Same key, different body or route

**Fingerprint the request and reject a mismatch with `422`.** The fingerprint is
`SHA-256(method || request-target || body)`, stored with the entry and compared on every subsequent
request for that key. The target is the full request URI, query string included, so `/charges` and
`/charges?dry_run=1` cannot share a key. Each field is length-prefixed before it is hashed, so no
combination of contents can produce the same digest as a different combination.

An idempotency key is a claim by the caller that this is the same request as before. Unverified, a
client bug that reuses a key across two genuinely different requests receives a confident answer to
the wrong question — and receives it silently, with a 200. Verification costs one hash and turns an
invisible correctness failure into a loud one.

This forces a related decision: the body must be read to be hashed, so it is buffered up to
`MaxBodyBytes` (default 1 MiB) and the handler is given a fresh reader over those bytes. A request
over the cap is rejected with `413` rather than passed through unfingerprinted, because passing it
through would mean the largest requests get the weakest guarantee.

**Known limitation, stated deliberately:** the hash is over raw bytes, so requests that are
semantically identical but not byte-identical count as different. `?a=1&b=2` and `?b=2&a=1` differ,
as do two JSON bodies with the same fields in a different order, and either earns a `422` from a
caller who did nothing wrong. Normalising would put the middleware in the business of deciding what
"equivalent" means for content types it does not own — canonical JSON, sorted query parameters, a
rule per encoding — and being wrong there would replay an answer to a question the caller never
asked. A spurious `422` is visible and recoverable; a spurious replay is neither.

*Rejected: trust the key and replay the stored response regardless of body.* Fewer moving parts, no
buffering, no size cap. Also the worst available failure mode, because it is silent.

*Rejected: fingerprint the body alone.* The same payload posted to `/refunds` and `/charges` would
collide, and the brief calls out route as well as body.

## 3. The first execution failed

**Replay 4xx. Re-execute on 5xx and on panic.**

A 4xx is a deterministic function of the request. Retrying it cannot produce a different outcome, so
replaying the stored 4xx is both correct and cheaper than re-running the handler. A 5xx is
presumptively transient — a timeout, a dropped connection, a dependency blip — and caching it would
poison the key for its entire TTL, leaving the caller unable to ever succeed with the key they were
told to retry with.

A panicking handler releases its key and the panic then propagates unchanged. Cleaning up its own
state is this library's business; deciding what a crash means is not. Recovering here would silently
disable whatever panic reporting the service already has, and `net/http` already recovers per
connection.

*Rejected: recover the panic and return 500.* Kinder to the caller, who gets a response instead of a
dropped connection — but it makes a narrow middleware opinionated about a concern outside it, and a
swallowed panic is a crash nobody hears about.

**Known limitation, stated deliberately:** a handler that commits a side effect and *then* fails with
a 5xx will be re-executed, and the side effect happens twice. No middleware positioned outside the
handler can detect that boundary; only the handler's own transaction can. This is the one place where
"never execute twice" and "always return a coherent response" genuinely conflict, and this library
resolves it in favour of the caller being able to make progress.

*Rejected: store every response, including 5xx.* Upholds "execute at most once" absolutely, at the
cost of permanently poisoned keys after any transient fault. Trades a rare double-execution for a
common dead end.

*Rejected: store nothing when the handler errors at all.* Simpler to describe, but throws away the
useful half — a retry of a request that failed validation re-enters the handler for no reason, and
the caller can observe two different 4xx bodies for one key.

## 4. Bounded memory

**TTL plus a hard entry ceiling. Lazy expiry on access, a background sweeper, and at capacity, evict
the oldest completed entry — never an in-flight one.**

`Options{TTL, MaxEntries}`, both with defaults. Expired entries are dropped when their key is next
touched; a sweeper goroutine reclaims the rest, and `Close()` stops it.

Eviction is a correctness decision, not just a memory one: dropping an entry silently downgrades the
guarantee for a key the caller still believes is protected. So eviction is confined to entries whose
handler has already returned, where the cost is a re-execution the caller could have caused anyway by
retrying late. If every entry is in flight, the store refuses new keys with `503` — failing loudly is
better than quietly weakening the property the library exists to provide.

*Rejected: TTL with no ceiling.* Unbounded under a burst inside the TTL window, which is precisely
the retry storm this library invites. "Bounded on one node" was a requirement, not an aspiration.

*Rejected: strict LRU over all entries.* Would evict in-flight entries under load and break the core
guarantee at exactly the moment it matters most.

*Rejected: one `time.AfterFunc` per key.* Correct, and one goroutine-backed timer per key. A single
sweeper is the boring version.

## 5. Requests without a key

**Pass through untouched. `Options{RequireKey: true}` opts into `400` instead.**

A library that rejects unkeyed traffic by default cannot be introduced to a running service
incrementally — it would have to be adopted for every endpoint at once, or not at all. Permissive by
default makes it adoptable; strict-on-request serves the service that has finished migrating.

*Rejected: always `400`.* Safer in isolation, unadoptable in practice.

*Rejected: synthesise a key from the request fingerprint.* Turns two legitimately identical requests
— the same idempotent action taken twice on purpose — into a false duplicate, inventing a guarantee
the caller never asked for.

## Concurrency

One mutex guards the key map; each entry carries its own state. **The map lock is never held across
handler execution** — doing so would serialise every request through the middleware, and would pass
`-race` while doing it. A request takes the lock, claims or reads the entry, releases it, runs the
handler, then takes the lock again to record the result.

## Out of scope

Persistence and multi-node are excluded by the brief. What changes when they are not: the map lock
becomes a distributed compare-and-swap with a lease, so claiming a key becomes a conditional write
rather than a local mutex. The in-flight case gets materially harder, because a lease can expire
while the handler is still running, forcing a choice between a double execution and a permanently
stuck key. Eviction and TTL stop being this library's concern and become the store's.
