// Command collector is a stand-in for the cluster collector and the metrics
// backend behind it: it receives OTLP and answers questions over HTTP about
// what it stored.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net"
	"net/http"
	"sort"

	"google.golang.org/grpc"

	"github.com/agent-substrate/ate-identity-prototype/internal/identity"
	"github.com/agent-substrate/ate-identity-prototype/internal/upstream"
)

func main() {
	otlpAddr := flag.String("otlp", "127.0.0.1:14317", "OTLP gRPC listen address")
	httpAddr := flag.String("http", "127.0.0.1:14318", "HTTP query address")
	flag.Parse()

	col := upstream.NewCollector()
	lis, err := net.Listen("tcp", *otlpAddr)
	if err != nil {
		log.Fatalf("listen: %v", err)
	}
	gs := grpc.NewServer()
	col.Register(gs)
	go func() { log.Fatal(gs.Serve(lis)) }()
	log.Printf("collector: OTLP on %s, queries on %s", *otlpAddr, *httpAddr)

	mux := http.NewServeMux()
	mux.HandleFunc("/dump", func(w http.ResponseWriter, r *http.Request) {
		totals := col.Totals()
		keys := make([]string, 0, len(totals))
		for k := range totals {
			keys = append(keys, k)
		}
		sort.Strings(keys)
		out := struct {
			SeriesCount int                `json:"series_count"`
			Series      map[string]float64 `json:"series"`
			SeriesOrder []string           `json:"series_order"`
			Spans       int                `json:"spans"`
			ActorUIDs   map[string]int     `json:"trace_actor_uids"`
			WorkerPods  map[string]int     `json:"trace_worker_pods"`
			LogActors   map[string]int     `json:"log_actor_uids"`
		}{
			SeriesCount: len(totals),
			Series:      totals,
			SeriesOrder: keys,
			Spans:       col.SpanCount(),
			ActorUIDs:   col.TraceResourceAttr(identity.KeyActorUID),
			WorkerPods:  col.TraceResourceAttr(identity.KeyWorkerPod),
			LogActors:   col.LogResourceAttr(identity.KeyActorUID),
		}
		w.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(w)
		enc.SetIndent("", "  ")
		_ = enc.Encode(out)
	})
	mux.HandleFunc("/reset", func(w http.ResponseWriter, r *http.Request) {
		col.Reset()
		_, _ = w.Write([]byte("ok\n"))
	})
	log.Fatal(http.ListenAndServe(*httpAddr, mux))
}
