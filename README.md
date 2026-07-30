# idempotency

HTTP middleware that makes mutating endpoints safe to retry. A request carrying an
`Idempotency-Key` header executes its handler at most once; retries with the same key replay the
stored response instead of executing again.

In-memory, single node, standard library only.

```go
mw := idempotency.New(idempotency.Options{TTL: time.Hour, MaxEntries: 10_000})
defer mw.Close()

mux.Handle("POST /charges", mw.Wrap(chargeHandler))
```

| Situation | Behaviour |
| --- | --- |
| No `Idempotency-Key` | Pass through (or `400` with `RequireKey`) |
| Unknown key | Handler runs, response stored |
| Key in flight | `409 Conflict` |
| Key complete, same request | Stored response replayed, `Idempotent-Replay: true` |
| Key complete, different request | `422 Unprocessable Entity` |
| Handler returned 5xx or panicked | Not stored; a retry runs the handler again |
| Request body over `MaxBodyBytes` | `413 Request Entity Too Large` |
| Response over `MaxBodyBytes` | Sent to the client in full, but not stored |
| Store full of in-flight keys | `503 Service Unavailable` |

`DESIGN.md` covers why each of those is what it is, and what was rejected to get there.

## Running it

```
go test -race ./...
go run ./example
```
