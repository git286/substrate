// Command relay is the ateom-side receiver, run standalone.
//
// In Substrate this is not a process: it is a listener ateom starts inside the
// worker pod's netns on 169.254.17.1:4317, and Activate/Deactivate are called
// from the RunWorkload / RestoreWorkload / CheckpointWorkload handlers, which
// already carry the actor's identity. Here those calls are exposed over a
// small admin HTTP API so the demo can drive them.
package main

import (
	"encoding/json"
	"flag"
	"log"
	"net/http"
	"os"
	"os/signal"
	"strings"
	"syscall"
	"time"

	"github.com/agent-substrate/ate-identity-prototype/internal/harness"
	"github.com/agent-substrate/ate-identity-prototype/internal/identity"
	"github.com/agent-substrate/ate-identity-prototype/internal/relay"
)

func main() {
	listen := flag.String("listen", "127.0.0.1:4317", "actor-facing OTLP address (169.254.17.1:4317 in a real worker pod)")
	upstreamAddr := flag.String("upstream", "127.0.0.1:14317", "cluster collector OTLP address")
	admin := flag.String("admin", "127.0.0.1:14319", "admin HTTP address (stands in for ateom's activation RPCs)")
	pod := flag.String("pod", "worker-1", "worker pod name")
	node := flag.String("node", "node-a", "worker node name")
	interval := flag.Duration("interval", 5*time.Second, "aggregation interval")
	gauges := flag.String("gauge-policy", "queue_depth=sum", "comma-separated metric=sum|avg declarations; undeclared gauges are dropped")
	maxSeries := flag.Int("max-series-per-metric", 200, "cardinality cap per metric per interval")
	maxTotal := flag.Int("max-series-total", 2000, "accumulator budget across all metrics per interval")
	flag.Parse()

	policy := map[string]relay.GaugeAggregation{}
	for _, part := range strings.Split(*gauges, ",") {
		part = strings.TrimSpace(part)
		if part == "" {
			continue
		}
		name, agg, ok := strings.Cut(part, "=")
		if !ok {
			log.Fatalf("bad -gauge-policy entry %q", part)
		}
		policy[name] = relay.GaugeAggregation(agg)
	}

	w, err := harness.StartWorker(harness.WorkerConfig{
		Addr:         *listen,
		UpstreamAddr: *upstreamAddr,
		Worker:       identity.Worker{PodName: *pod, NodeName: *node},
		Interval:     *interval,
		GaugePolicy:  policy,
		MaxSeries:    *maxSeries,
		MaxTotal:     *maxTotal,
	})
	if err != nil {
		log.Fatalf("worker: %v", err)
	}
	log.Printf("ateom relay: listening for actor OTLP on %s, forwarding to %s (pod=%s node=%s, interval=%s)",
		w.Addr, *upstreamAddr, *pod, *node, *interval)

	// A worker pod that goes away without flushing loses up to one aggregation
	// interval of actor telemetry, permanently. Handle the signal.
	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		s := <-sigs
		log.Printf("received %s: flushing aggregation buffer before exit", s)
		w.Stop()
		os.Exit(0)
	}()

	mux := http.NewServeMux()
	mux.HandleFunc("/activate", func(rw http.ResponseWriter, r *http.Request) {
		q := r.URL.Query()
		a := identity.Actor{
			UID:               q.Get("uid"),
			Name:              q.Get("name"),
			Atespace:          def(q.Get("atespace"), "prod"),
			TemplateNamespace: def(q.Get("template_namespace"), "team-a"),
			TemplateName:      def(q.Get("template_name"), "chat-agent"),
			ContainerName:     def(q.Get("container"), "app"),
		}
		if a.UID == "" || a.Name == "" {
			http.Error(rw, "uid and name are required", http.StatusBadRequest)
			return
		}
		w.Relay.Activate(a)
		log.Printf("activated actor %s/%s (uid=%s)", a.Atespace, a.Name, a.UID)
		_, _ = rw.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/deactivate", func(rw http.ResponseWriter, r *http.Request) {
		w.Relay.Deactivate()
		_, _ = rw.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/flush", func(rw http.ResponseWriter, r *http.Request) {
		w.Relay.Flush(r.Context())
		_, _ = rw.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/stats", func(rw http.ResponseWriter, r *http.Request) {
		rw.Header().Set("Content-Type", "application/json")
		enc := json.NewEncoder(rw)
		enc.SetIndent("", "  ")
		_ = enc.Encode(w.Relay.Stats())
	})
	log.Fatal(http.ListenAndServe(*admin, mux))
}

func def(v, fallback string) string {
	if v == "" {
		return fallback
	}
	return v
}
