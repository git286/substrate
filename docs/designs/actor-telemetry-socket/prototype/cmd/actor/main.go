// Command actor is a stock-OTel-SDK workload. Its entire telemetry
// configuration is constants — the same three environment variables every
// actor of the template gets, frozen into the golden snapshot at the right
// values.
package main

import (
	"context"
	"flag"
	"log"
	"os"
	"strings"
	"time"

	"github.com/agent-substrate/ate-identity-prototype/internal/actorsdk"
)

func main() {
	endpoint := flag.String("endpoint", envOr("OTEL_EXPORTER_OTLP_ENDPOINT", "127.0.0.1:4317"),
		"OTLP endpoint (169.254.17.1:4317 in a real actor)")
	service := flag.String("service", "demo-actor", "service.name")
	work := flag.Int("work", 10, "units of work to perform")
	every := flag.Duration("every", 0, "if set, keep doing -work units this often until killed")
	interval := flag.Duration("interval", 2*time.Second, "export interval")
	queue := flag.Int64("queue-depth", 3, "observable gauge value")
	forge := flag.String("forge-identity", "", "emit this actor_uid/service.instance.id in the payload (should be overridden)")
	cumulative := flag.Bool("cumulative", false, "export cumulative sums instead of delta (should be refused)")
	flag.Parse()

	ctx := context.Background()
	a, err := actorsdk.Start(ctx, actorsdk.Config{
		Endpoint:       strings.TrimPrefix(strings.TrimPrefix(*endpoint, "http://"), "https://"),
		ServiceName:    *service,
		Interval:       *interval,
		Cumulative:     *cumulative,
		ForgedIdentity: *forge,
	})
	if err != nil {
		log.Fatalf("actor: %v", err)
	}
	defer func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.Shutdown(sctx)
	}()

	a.SetQueueDepth(*queue)
	round := func() {
		a.DoWork(ctx, *work)
		a.SetCPU(ctx, 42)
		a.Log(ctx, "did some work")
		fctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := a.ForceFlush(fctx); err != nil {
			// Partial success arrives here too: the relay telling the actor
			// which of its datapoints it refused, and why.
			log.Printf("export: %v", err)
		}
	}

	round()
	log.Printf("actor: emitted %d work items to %s", *work, *endpoint)
	if *every > 0 {
		t := time.NewTicker(*every)
		defer t.Stop()
		for range t.C {
			round()
			log.Printf("actor: emitted %d more work items", *work)
		}
	}
}

func envOr(key, fallback string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return fallback
}
