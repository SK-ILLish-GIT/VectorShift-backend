// Command example runs a small payments service whose POST endpoint is guarded
// by the idempotency middleware, so it can be poked at with curl.
package main

import (
	"encoding/json"
	"log"
	"net/http"
	"os"
	"sync/atomic"
	"time"

	"idempotency"
)

// executions counts how many times the handler actually ran, which is the only
// number that matters when demonstrating this library.
var executions atomic.Int64

func charge(w http.ResponseWriter, r *http.Request) {
	var req struct {
		Amount int `json:"amount"`
	}
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, `{"error":"invalid json"}`, http.StatusBadRequest)
		return
	}
	if req.Amount <= 0 {
		http.Error(w, `{"error":"amount must be positive"}`, http.StatusBadRequest)
		return
	}

	// Stand-in for real work, wide enough to fire a concurrent retry by hand.
	time.Sleep(2 * time.Second)
	n := executions.Add(1)

	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusCreated)
	json.NewEncoder(w).Encode(map[string]any{
		"charge_id":  n,
		"amount":     req.Amount,
		"executions": n,
	})
}

func main() {
	mw := idempotency.New(idempotency.Options{
		TTL:        2 * time.Minute,
		MaxEntries: 1000,
	})
	defer mw.Close()

	mux := http.NewServeMux()
	mux.Handle("POST /charges", mw.Wrap(http.HandlerFunc(charge)))
	mux.HandleFunc("GET /executions", func(w http.ResponseWriter, r *http.Request) {
		json.NewEncoder(w).Encode(map[string]int64{"executions": executions.Load()})
	})

	port := os.Getenv("PORT")
	if port == "" {
		port = "8080"
	}
	log.Printf(usage, "localhost:"+port)

	srv := &http.Server{Addr: ":" + port, Handler: mux, ReadHeaderTimeout: 5 * time.Second}
	log.Fatal(srv.ListenAndServe())
}

const usage = `
listening on %[1]s — the handler sleeps 2s and counts its own executions

  # runs the handler, returns 201
  curl -si %[1]s/charges -H 'Idempotency-Key: k1' -d '{"amount":100}'

  # same key, same body: replayed, note Idempotent-Replay: true and the
  # unchanged charge_id
  curl -si %[1]s/charges -H 'Idempotency-Key: k1' -d '{"amount":100}'

  # same key, different body: 422
  curl -si %[1]s/charges -H 'Idempotency-Key: k1' -d '{"amount":999}'

  # retry while the first is still running: 409
  curl -s %[1]s/charges -H 'Idempotency-Key: k2' -d '{"amount":50}' &
  sleep 0.2
  curl -si %[1]s/charges -H 'Idempotency-Key: k2' -d '{"amount":50}'

  # no key at all: passes straight through, every time
  curl -si %[1]s/charges -d '{"amount":100}'

  # how many times the handler really ran
  curl -s %[1]s/executions
`
