package e2e

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/attribute"

	"github.com/agent-substrate/ate-identity-prototype/internal/actorsdk"
	"github.com/agent-substrate/ate-identity-prototype/internal/harness"
)

// The worked example from §7.5. Actor A counts 10 -> 15 -> 20, actor B counts
// 3 -> 8; the true total is 28.
//
// The two tests below are the same workload run two ways: straight at the
// collector the way an actor does today, and through the relay. The first is
// the bug, reproduced; the second is the fix, measured.

// Today: identical Resources, cumulative sums, one series with two writers.
// Every dip reads as a counter restart, phantom increments are added, and the
// real number is unrecoverable.
func TestStatusQuoInflatesCounters(t *testing.T) {
	col, colAddr := startCollector(t)

	// Both actors are byte-identical clones of one golden snapshot: same
	// Resource, no per-actor identity, exporting straight upstream.
	a := startActor(t, actorsdk.Config{Endpoint: colAddr, Cumulative: true, MetricsOnly: true})
	b := startActor(t, actorsdk.Config{Endpoint: colAddr, Cumulative: true, MetricsOnly: true})

	ctx := context.Background()
	kind := attribute.String("kind", "task")
	flush := func(x *actorsdk.Actor) {
		fctx, cancel := context.WithTimeout(ctx, 10*time.Second)
		defer cancel()
		if err := x.ForceFlush(fctx); err != nil && !isPartialSuccess(err) {
			t.Fatalf("flush: %v", err)
		}
	}

	a.AddWithAttrs(ctx, 10, kind) // A: 10
	flush(a)
	b.AddWithAttrs(ctx, 3, kind) // B: 3
	flush(b)
	a.AddWithAttrs(ctx, 5, kind) // A: 15
	flush(a)
	b.AddWithAttrs(ctx, 5, kind) // B: 8
	flush(b)
	a.AddWithAttrs(ctx, 5, kind) // A: 20
	flush(a)

	if got := len(col.PromSeries("work_items")); got != 1 {
		t.Fatalf("two actors produced %d series, expected them to collapse into 1", got)
	}
	got := col.PromTotal("work_items")
	if got == 28 {
		t.Fatalf("status quo produced the correct total 28 — the bug did not reproduce")
	}
	// 10, then +3 (restart), +12, +8 (restart), +12 = 45 against a true 28.
	if got != 45 {
		t.Errorf("inflated total = %v, want 45 (10+3+12+8+12)", got)
	}
	t.Logf("status quo: one series, two writers, backend reads %v where the truth is 28", got)
}

// With the relay: same two actors, same increments, delta temporality, one
// stored series, and actor_uid never leaves the pod.
func TestRelayGivesExactlyTwentyEight(t *testing.T) {
	col, colAddr := startCollector(t)

	w1 := startWorker(t, colAddr, "worker-1", "node-a")
	w2 := startWorker(t, colAddr, "worker-2", "node-b")
	w1.Relay.Activate(actorOf("uid-aaa", "actor-a"))
	w2.Relay.Activate(actorOf("uid-bbb", "actor-b"))

	a := startActor(t, actorsdk.Config{Endpoint: w1.Addr, MetricsOnly: true})
	b := startActor(t, actorsdk.Config{Endpoint: w2.Addr, MetricsOnly: true})

	ctx := context.Background()
	kind := attribute.String("kind", "task")

	// Three relay intervals: A contributes 10, 5, 5 and B contributes 3, 5.
	// Forwarded per interval: 13, 10, 5.
	steps := []struct {
		a, b int64
		want float64
	}{
		{10, 3, 13},
		{5, 5, 10},
		{5, 0, 5},
	}
	var running float64
	for i, s := range steps {
		if s.a > 0 {
			a.AddWithAttrs(ctx, s.a, kind)
		}
		if s.b > 0 {
			b.AddWithAttrs(ctx, s.b, kind)
		}
		flushBoth(t, a, w1, b, w2)
		running += s.want
		if got := col.Total("work_items", ""); got != running {
			t.Fatalf("interval %d: cumulative-at-backend = %v, want %v", i+1, got, running)
		}
	}

	if got := col.Total("work_items", ""); got != 28 {
		t.Errorf("total = %v, want 28", got)
	}
	if got := col.SeriesFor("work_items"); len(got) != 1 {
		t.Errorf("stored series = %d, want 1: %v", len(got), got)
	}
}

func flushBoth(t *testing.T, a *actorsdk.Actor, wa *harness.Worker, b *actorsdk.Actor, wb *harness.Worker) {
	t.Helper()
	settle(t, a, wa)
	settle(t, b, wb)
}
