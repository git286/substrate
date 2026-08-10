// Package e2e drives the whole path — stock OTel SDK actor -> ateom relay ->
// cluster collector — and checks the properties the design claims. Every test
// here corresponds to an item in §12 of actor-telemetry-socket-design.md.
package e2e

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/agent-substrate/ate-identity-prototype/internal/actorsdk"
	"github.com/agent-substrate/ate-identity-prototype/internal/harness"
	"github.com/agent-substrate/ate-identity-prototype/internal/identity"
	"github.com/agent-substrate/ate-identity-prototype/internal/relay"
	"github.com/agent-substrate/ate-identity-prototype/internal/upstream"
)

const (
	templateNS   = "team-a"
	templateName = "chat-agent"
	atespace     = "prod"
	container    = "app"
)

func actorOf(uid, name string) identity.Actor {
	return identity.Actor{
		UID:               uid,
		Name:              name,
		Atespace:          atespace,
		TemplateNamespace: templateNS,
		TemplateName:      templateName,
		ContainerName:     container,
	}
}

func startCollector(t *testing.T) (*upstream.Collector, string) {
	t.Helper()
	c, addr, stop, err := upstream.Serve()
	if err != nil {
		t.Fatalf("collector: %v", err)
	}
	t.Cleanup(stop)
	return c, addr
}

type workerOpt func(*harness.WorkerConfig)

func withAddr(a string) workerOpt   { return func(c *harness.WorkerConfig) { c.Addr = a } }
func withMaxSeries(n int) workerOpt { return func(c *harness.WorkerConfig) { c.MaxSeries = n } }
func withGaugePolicy(p map[string]relay.GaugeAggregation) workerOpt {
	return func(c *harness.WorkerConfig) { c.GaugePolicy = p }
}

func startWorker(t *testing.T, collectorAddr, pod, node string, opts ...workerOpt) *harness.Worker {
	t.Helper()
	cfg := harness.WorkerConfig{
		UpstreamAddr: collectorAddr,
		Worker:       identity.Worker{PodName: pod, NodeName: node},
		// Long interval: tests drive flushes explicitly so results are
		// deterministic rather than timing-dependent.
		Interval: time.Hour,
	}
	for _, o := range opts {
		o(&cfg)
	}
	w, err := harness.StartWorker(cfg)
	if err != nil {
		t.Fatalf("worker: %v", err)
	}
	t.Cleanup(w.Stop)
	return w
}

func startActor(t *testing.T, cfg actorsdk.Config) *actorsdk.Actor {
	t.Helper()
	ctx := context.Background()
	if cfg.Interval == 0 {
		cfg.Interval = time.Hour // explicit flushes only
	}
	a, err := actorsdk.Start(ctx, cfg)
	if err != nil {
		t.Fatalf("actor: %v", err)
	}
	t.Cleanup(func() {
		sctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = a.Shutdown(sctx)
	})
	return a
}

// settle pushes the actor's buffers to the relay, then the relay's aggregation
// interval upstream.
func settle(t *testing.T, a *actorsdk.Actor, w *harness.Worker) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()
	if err := a.ForceFlush(ctx); err != nil && !isPartialSuccess(err) {
		t.Fatalf("actor flush: %v", err)
	}
	w.Relay.Flush(ctx)
}

// isPartialSuccess reports whether an export error is the relay telling the
// actor it refused some datapoints. The Go SDK surfaces OTLP partial success
// as an export error, which is exactly the loudness open question 4 asks for:
// the actor cannot not notice.
func isPartialSuccess(err error) bool {
	return err != nil && strings.Contains(err.Error(), "partial success")
}

// -------------------------------------------------------------- §12 test 1

// Two concurrent actors of one template must be distinguishable. The design
// splits the answer by signal: traces keep per-actor identity, metrics do not
// and collapse to one series on purpose.
func TestTwoConcurrentActorsOneTemplate(t *testing.T) {
	col, colAddr := startCollector(t)

	w1 := startWorker(t, colAddr, "worker-1", "node-a")
	w2 := startWorker(t, colAddr, "worker-2", "node-b")
	w1.Relay.Activate(actorOf("uid-aaa", "actor-a"))
	w2.Relay.Activate(actorOf("uid-bbb", "actor-b"))

	a1 := startActor(t, actorsdk.Config{Endpoint: w1.Addr})
	a2 := startActor(t, actorsdk.Config{Endpoint: w2.Addr})

	ctx := context.Background()
	a1.DoWork(ctx, 10)
	a2.DoWork(ctx, 3)
	settle(t, a1, w1)
	settle(t, a2, w2)

	// Traces: identity kept, one instance id per actor. This is the check PR
	// #750's README step 3 is missing.
	ids := col.TraceResourceAttr(identity.KeyServiceInstanceID)
	if len(ids) != 2 {
		t.Fatalf("distinct service.instance.id on traces = %d, want 2 (%v)", len(ids), ids)
	}
	if ids["uid-aaa"] != 10 || ids["uid-bbb"] != 3 {
		t.Errorf("spans per actor = %v, want uid-aaa:10 uid-bbb:3", ids)
	}

	// Metrics: identity stripped, both actors land in one bounded series with
	// the correct total.
	series := col.SeriesFor("work_items")
	if len(series) != 1 {
		t.Fatalf("work_items series = %d, want 1: %v", len(series), series)
	}
	if got := col.Total("work_items", ""); got != 13 {
		t.Errorf("work_items total = %v, want 13", got)
	}
	if strings.Contains(series[0], "actor_uid") || strings.Contains(series[0], "instance.id") {
		t.Errorf("metric series leaks actor identity: %s", series[0])
	}
	// And no host facts on the metric series either — those change on
	// migration and would fragment it.
	if strings.Contains(series[0], "worker_pod") || strings.Contains(series[0], "worker_node") {
		t.Errorf("metric series carries host facts: %s", series[0])
	}
}

// -------------------------------------------------------------- §12 test 2

// The test that fails today: drive a known number of increments, migrate to a
// different worker, and assert the backend total is exact. The actor's
// exporter config never changes — only what answers at the constant address.
func TestCounterCorrectnessAcrossMigration(t *testing.T) {
	col, colAddr := startCollector(t)

	addr, err := harness.FreeAddr()
	if err != nil {
		t.Fatal(err)
	}

	w1 := startWorker(t, colAddr, "worker-1", "node-a", withAddr(addr))
	w1.Relay.Activate(actorOf("uid-aaa", "actor-a"))

	a := startActor(t, actorsdk.Config{Endpoint: addr})
	ctx := context.Background()

	a.DoWork(ctx, 10)
	settle(t, a, w1)

	// Suspend: flush first (that is #503's flush, out of scope here but the
	// reason PreSuspend matters), then the worker goes away.
	w1.Stop()

	// Resume on a different worker. Same address, different ateom, different
	// activation.
	w2 := startWorker(t, colAddr, "worker-2", "node-b", withAddr(addr))
	w2.Relay.Activate(actorOf("uid-aaa", "actor-a"))
	if err := harness.WaitDialable(addr, 5*time.Second); err != nil {
		t.Fatal(err)
	}

	a.DoWork(ctx, 5)
	a.DoWork(ctx, 5)
	settle(t, a, w2)

	if got := col.Total("work_items", ""); got != 20 {
		t.Errorf("total across migration = %v, want 20", got)
	}
	// One series, not one per worker: the migration is invisible to storage.
	if s := col.SeriesFor("work_items"); len(s) != 1 {
		t.Errorf("work_items series after migration = %d, want 1: %v", len(s), s)
	}
	// Traces still attribute both halves to the same actor.
	ids := col.TraceResourceAttr(identity.KeyServiceInstanceID)
	if len(ids) != 1 || ids["uid-aaa"] != 20 {
		t.Errorf("trace attribution across migration = %v, want uid-aaa:20", ids)
	}
	// ...and record where each half ran, as a host fact.
	pods := col.TraceResourceAttr(identity.KeyWorkerPod)
	if pods["worker-1"] != 10 || pods["worker-2"] != 10 {
		t.Errorf("worker attribution on traces = %v, want worker-1:10 worker-2:10", pods)
	}
}

// -------------------------------------------------------------- §12 test 3

// An actor that does no work must report zero, not the golden's totals.
//
// Both halves of §7.5's claim are checked: delta cancels the golden baseline
// down to a bounded one-time residue g, and a PreSnapshot flush removes even
// that.
func TestGoldenBaseline(t *testing.T) {
	for _, tc := range []struct {
		name         string
		preSnapFlush bool
		wantRestored float64
	}{
		{"with PreSnapshot flush", true, 0},
		{"without PreSnapshot flush", false, 7}, // bounded, one-time
	} {
		t.Run(tc.name, func(t *testing.T) {
			col, colAddr := startCollector(t)
			addr, err := harness.FreeAddr()
			if err != nil {
				t.Fatal(err)
			}

			// Golden build: the template's init does some work.
			gw := startWorker(t, colAddr, "golden-builder", "node-g", withAddr(addr))
			gw.Relay.Activate(actorOf("uid-golden", "golden"))
			a := startActor(t, actorsdk.Config{Endpoint: addr})
			ctx := context.Background()
			a.DoWork(ctx, 7)

			if tc.preSnapFlush {
				settle(t, a, gw)
			}
			// Snapshot is taken here; the SDK's accumulated deltas and its
			// last-collection bookmark are frozen with the process.
			gw.Stop()

			// Restore onto a real worker. The actor does nothing at all.
			col.Reset()
			w := startWorker(t, colAddr, "worker-1", "node-a", withAddr(addr))
			w.Relay.Activate(actorOf("uid-aaa", "actor-a"))
			if err := harness.WaitDialable(addr, 5*time.Second); err != nil {
				t.Fatal(err)
			}
			settle(t, a, w)

			if got := col.Total("work_items", ""); got != tc.wantRestored {
				t.Errorf("restored idle actor reported %v, want %v", got, tc.wantRestored)
			}

			// Whatever the residue, it is one-time: a second interval with no
			// work reports nothing.
			col.Reset()
			settle(t, a, w)
			if got := col.Total("work_items", ""); got != 0 {
				t.Errorf("second idle interval reported %v, want 0", got)
			}
		})
	}
}

// -------------------------------------------------------------- §12 test 4

// Forwarded series count must not vary with actor count.
func TestCardinalityBound(t *testing.T) {
	for _, n := range []int{1, 10, 100} {
		t.Run(fmt.Sprintf("N=%d", n), func(t *testing.T) {
			col, colAddr := startCollector(t)
			ctx := context.Background()

			for i := 0; i < n; i++ {
				w := startWorker(t, colAddr, fmt.Sprintf("worker-%d", i), "node-a")
				w.Relay.Activate(actorOf(fmt.Sprintf("uid-%04d", i), fmt.Sprintf("actor-%04d", i)))
				a := startActor(t, actorsdk.Config{Endpoint: w.Addr, MetricsOnly: true})
				a.AddWithAttrs(ctx, 1, attribute.String("kind", "task"))
				settle(t, a, w)
			}

			if got := len(col.SeriesFor("work_items")); got != 1 {
				t.Errorf("N=%d produced %d work_items series, want 1: %v", n, got, col.SeriesFor("work_items"))
			}
			if got := col.Total("work_items", ""); got != float64(n) {
				t.Errorf("N=%d total = %v, want %d", n, got, n)
			}
		})
	}
}

// -------------------------------------------------------------- §12 test 5

// An actor emitting its own identity must be overridden, not merged.
func TestAttributionCannotBeForged(t *testing.T) {
	col, colAddr := startCollector(t)
	w := startWorker(t, colAddr, "worker-1", "node-a")
	w.Relay.Activate(actorOf("uid-real", "actor-real"))

	a := startActor(t, actorsdk.Config{Endpoint: w.Addr, ForgedIdentity: "uid-victim"})
	ctx := context.Background()
	a.DoWork(ctx, 2)
	settle(t, a, w)

	ids := col.TraceResourceAttr(identity.KeyServiceInstanceID)
	if len(ids) != 1 || ids["uid-real"] != 2 {
		t.Errorf("service.instance.id on traces = %v, want only uid-real:2", ids)
	}
	uids := col.TraceResourceAttr(identity.KeyActorUID)
	if _, forged := uids["uid-victim"]; forged {
		t.Errorf("forged actor_uid survived: %v", uids)
	}
	spaces := col.TraceResourceAttr(identity.KeyAtespace)
	if _, forged := spaces["victim-atespace"]; forged {
		t.Errorf("forged atespace survived: %v", spaces)
	}
	for _, s := range col.AllSeries() {
		if strings.Contains(s, "uid-victim") || strings.Contains(s, "victim-atespace") {
			t.Errorf("forged identity reached a metric series: %s", s)
		}
	}
	if n := w.Relay.Stats().AttributesOverridden; n == 0 {
		t.Errorf("relay did not count any overridden attributes")
	}
}

// ------------------------------------------------- open question 4: cumulative

// An actor that cannot be reconfigured to delta must fail loudly and
// attributably rather than silently produce a wrong number.
func TestCumulativeIsRejectedLoudly(t *testing.T) {
	col, colAddr := startCollector(t)
	w := startWorker(t, colAddr, "worker-1", "node-a")
	w.Relay.Activate(actorOf("uid-aaa", "actor-a"))

	a := startActor(t, actorsdk.Config{Endpoint: w.Addr, Cumulative: true, MetricsOnly: true})
	ctx := context.Background()
	a.AddWithAttrs(ctx, 4, attribute.String("kind", "task"))
	settle(t, a, w)

	if got := col.Total("work_items", ""); got != 0 {
		t.Errorf("cumulative datapoints were forwarded (total %v), want none", got)
	}
	st := w.Relay.Stats()
	if st.DatapointsRejected["cumulative_sum"] == 0 {
		t.Errorf("no cumulative_sum rejections recorded: %v", st.DatapointsRejected)
	}
	// The refusal is itself telemetry — Substrate's own, so it is unaffected
	// by the problem being fixed (open question 3).
	if got := col.Total(relay.MetricRejected, "reason=cumulative_sum"); got == 0 {
		t.Errorf("rejection not reported as relay self-telemetry: %v", col.AllSeries())
	}
}

// -------------------------------------------------------- §7.6 gauge policy

func TestGaugeRequiresDeclaredAggregation(t *testing.T) {
	col, colAddr := startCollector(t)
	// queue_depth sums across actors; cpu_utilization has no declared rule and
	// must be dropped rather than silently summed.
	w := startWorker(t, colAddr, "worker-1", "node-a",
		withGaugePolicy(map[string]relay.GaugeAggregation{"queue_depth": relay.GaugeSum}))
	w.Relay.Activate(actorOf("uid-aaa", "actor-a"))

	a := startActor(t, actorsdk.Config{Endpoint: w.Addr, MetricsOnly: true})
	ctx := context.Background()
	a.SetQueueDepth(5)
	a.SetCPU(ctx, 93)
	settle(t, a, w)

	if got := col.Total("queue_depth", ""); got != 5 {
		t.Errorf("declared gauge queue_depth = %v, want 5", got)
	}
	if got := col.SeriesFor("cpu_utilization"); len(got) != 0 {
		t.Errorf("undeclared gauge was forwarded: %v", got)
	}
	if n := w.Relay.Stats().DatapointsRejected["gauge_no_aggregation_declared"]; n == 0 {
		t.Errorf("undeclared gauge drop was not counted")
	}
}

// ------------------------------------------------------- unbounded actor labels

// The design's series-count formula assumes the actor contributes nothing
// unbounded. It can: datapoint attributes are the actor's to choose. The relay
// caps them and folds the rest into one overflow series, so the total is still
// right and the cardinality is still bounded.
func TestActorSuppliedLabelsAreCapped(t *testing.T) {
	col, colAddr := startCollector(t)
	w := startWorker(t, colAddr, "worker-1", "node-a", withMaxSeries(4))
	w.Relay.Activate(actorOf("uid-aaa", "actor-a"))

	a := startActor(t, actorsdk.Config{Endpoint: w.Addr, MetricsOnly: true})
	ctx := context.Background()
	for i := 0; i < 50; i++ {
		a.AddWithAttrs(ctx, 1, attribute.String("user_id", fmt.Sprintf("user-%d", i)))
	}
	settle(t, a, w)

	series := col.SeriesFor("work_items")
	if len(series) > 4 {
		t.Errorf("cardinality cap breached: %d series: %v", len(series), series)
	}
	if got := col.Total("work_items", ""); got != 50 {
		t.Errorf("total after overflow folding = %v, want 50", got)
	}
	var sawOverflow bool
	for _, s := range series {
		if strings.Contains(s, relay.OverflowKey) {
			sawOverflow = true
		}
	}
	if !sawOverflow {
		t.Errorf("no overflow series present: %v", series)
	}
}

// ------------------------------------------------------------ no activation

// Telemetry arriving with no actor activated is refused, and counted. A worker
// pod's relay is only ever meaningful during an activation.
func TestExportWithoutActivationIsRefused(t *testing.T) {
	col, colAddr := startCollector(t)
	w := startWorker(t, colAddr, "worker-1", "node-a")
	// deliberately not activated

	a := startActor(t, actorsdk.Config{Endpoint: w.Addr, MetricsOnly: true})
	ctx := context.Background()
	a.AddWithAttrs(ctx, 1, attribute.String("kind", "task"))
	fctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	if err := a.ForceFlush(fctx); err == nil {
		t.Errorf("export without activation succeeded, want failure")
	}
	w.Relay.Flush(context.Background())

	if got := col.Total("work_items", ""); got != 0 {
		t.Errorf("forwarded %v datapoints with no activation, want 0", got)
	}
	if n := w.Relay.Stats().ExportsWithoutActor; n == 0 {
		t.Errorf("no exports-without-activation counted")
	}
	if got := col.Total(relay.MetricNoActor, ""); got == 0 {
		t.Errorf("exports-without-activation not reported upstream: %v", col.AllSeries())
	}
}

// --------------------------------------------------------------- three signals

// Metrics, traces and logs must join on the same label set — that is why §7.3
// reuses the log labels verbatim.
func TestSignalsJoin(t *testing.T) {
	col, colAddr := startCollector(t)
	w := startWorker(t, colAddr, "worker-1", "node-a")
	w.Relay.Activate(actorOf("uid-aaa", "actor-a"))

	a := startActor(t, actorsdk.Config{Endpoint: w.Addr})
	ctx := context.Background()
	a.DoWork(ctx, 1)
	a.Log(ctx, "hello from the actor")
	settle(t, a, w)

	if got := col.LogResourceAttr(identity.KeyActorUID); got["uid-aaa"] != 1 {
		t.Errorf("log records attributed to %v, want uid-aaa:1", got)
	}
	if got := col.TraceResourceAttr(identity.KeyTemplateName); got[templateName] != 1 {
		t.Errorf("trace template attribution = %v", got)
	}
	found := false
	for _, s := range col.SeriesFor("work_items") {
		if strings.Contains(s, "ate.dev/actor_template_name="+templateName) &&
			strings.Contains(s, "ate.dev/actor_atespace="+atespace) {
			found = true
		}
	}
	if !found {
		t.Errorf("metric series does not carry the joinable label set: %v", col.SeriesFor("work_items"))
	}
}

// ------------------------------------------------- found during implementation

// The relay is a second buffer, and delta does not self-heal. Whatever sits in
// the aggregation window when an activation ends is lost unless the end of the
// activation flushes it — so Deactivate (CheckpointWorkload, workload exit)
// must flush before it clears the identity, and the process must flush on
// SIGTERM. Found by the demo: killing a worker mid-interval silently ate an
// actor's first 10 work items.
func TestDeactivationFlushesPendingWindow(t *testing.T) {
	col, colAddr := startCollector(t)
	// Interval is an hour: nothing reaches the collector on a timer.
	w := startWorker(t, colAddr, "worker-1", "node-a")
	w.Relay.Activate(actorOf("uid-aaa", "actor-a"))

	a := startActor(t, actorsdk.Config{Endpoint: w.Addr})
	ctx := context.Background()
	a.DoWork(ctx, 7)
	if err := a.ForceFlush(ctx); err != nil && !isPartialSuccess(err) {
		t.Fatalf("actor flush: %v", err)
	}
	if got := col.Total("work_items", ""); got != 0 {
		t.Fatalf("precondition: relay should still be holding the window, got %v", got)
	}

	w.Relay.Deactivate() // no explicit Flush

	if got := col.Total("work_items", ""); got != 7 {
		t.Errorf("work_items after deactivation = %v, want 7 (window lost)", got)
	}
}

// Same window, lost the other way: the worker pod goes away. Close must flush
// what Deactivate would have.
func TestShutdownFlushesPendingWindow(t *testing.T) {
	col, colAddr := startCollector(t)
	w := startWorker(t, colAddr, "worker-1", "node-a")
	w.Relay.Activate(actorOf("uid-aaa", "actor-a"))

	a := startActor(t, actorsdk.Config{Endpoint: w.Addr})
	ctx := context.Background()
	a.DoWork(ctx, 4)
	if err := a.ForceFlush(ctx); err != nil && !isPartialSuccess(err) {
		t.Fatalf("actor flush: %v", err)
	}

	w.Stop() // relay.Close

	if got := col.Total("work_items", ""); got != 4 {
		t.Errorf("work_items after shutdown = %v, want 4 (window lost)", got)
	}
}
