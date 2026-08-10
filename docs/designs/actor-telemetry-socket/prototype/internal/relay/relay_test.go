package relay

import (
	"context"
	"fmt"
	"strings"
	"testing"
	"time"

	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/agent-substrate/ate-identity-prototype/internal/identity"
)

type capture struct {
	metrics []*metricspb.ResourceMetrics
	traces  []*tracepb.ResourceSpans
	logs    []*logspb.ResourceLogs
}

func (c *capture) ExportMetrics(_ context.Context, rms []*metricspb.ResourceMetrics) error {
	c.metrics = append(c.metrics, rms...)
	return nil
}
func (c *capture) ExportTraces(_ context.Context, rss []*tracepb.ResourceSpans) error {
	c.traces = append(c.traces, rss...)
	return nil
}
func (c *capture) ExportLogs(_ context.Context, rls []*logspb.ResourceLogs) error {
	c.logs = append(c.logs, rls...)
	return nil
}

func testActor(uid, name, tmpl string) identity.Actor {
	return identity.Actor{
		UID: uid, Name: name, Atespace: "prod",
		TemplateNamespace: "team-a", TemplateName: tmpl, ContainerName: "app",
	}
}

func newTestRelay(t *testing.T, policy map[string]GaugeAggregation) (*Relay, *capture) {
	t.Helper()
	cap := &capture{}
	r := New(Config{
		Worker:      identity.Worker{PodName: "worker-1", NodeName: "node-a"},
		Upstream:    cap,
		Interval:    time.Hour,
		GaugePolicy: policy,
	})
	return r, cap
}

func deltaSum(name string, value int64, exemplars []*metricspb.Exemplar, attrs ...*commonpb.KeyValue) *metricspb.Metric {
	return &metricspb.Metric{
		Name: name,
		Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
			IsMonotonic:            true,
			DataPoints: []*metricspb.NumberDataPoint{{
				Attributes:        attrs,
				StartTimeUnixNano: 1,
				TimeUnixNano:      2,
				Value:             &metricspb.NumberDataPoint_AsInt{AsInt: value},
				Exemplars:         exemplars,
			}},
		}},
	}
}

func request(resAttrs []*commonpb.KeyValue, metrics ...*metricspb.Metric) *colmetricspb.ExportMetricsServiceRequest {
	return requestScope("test", resAttrs, metrics...)
}

func requestScope(scope string, resAttrs []*commonpb.KeyValue, metrics ...*metricspb.Metric) *colmetricspb.ExportMetricsServiceRequest {
	return &colmetricspb.ExportMetricsServiceRequest{
		ResourceMetrics: []*metricspb.ResourceMetrics{{
			Resource: &resourcepb.Resource{Attributes: resAttrs},
			ScopeMetrics: []*metricspb.ScopeMetrics{{
				Scope:   &commonpb.InstrumentationScope{Name: scope},
				Metrics: metrics,
			}},
		}},
	}
}

func export(t *testing.T, r *Relay, req *colmetricspb.ExportMetricsServiceRequest) *colmetricspb.ExportMetricsServiceResponse {
	t.Helper()
	svc := &metricsService{r: r}
	resp, err := svc.Export(context.Background(), req)
	if err != nil {
		t.Fatalf("export: %v", err)
	}
	return resp
}

// isSelfTelemetry reports whether a forwarded resource is the relay describing
// itself rather than an actor.
func isSelfTelemetry(rm *metricspb.ResourceMetrics) bool {
	for _, kv := range rm.GetResource().GetAttributes() {
		if kv.GetKey() == "service.name" && kv.GetValue().GetStringValue() == "ateom" {
			return true
		}
	}
	return false
}

func findMetric(rms []*metricspb.ResourceMetrics, name string) *metricspb.Metric {
	for _, rm := range rms {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				if m.GetName() == name {
					return m
				}
			}
		}
	}
	return nil
}

// A worker hosts many actors over time. Two activations of the same template
// must land on one series; a different template must not.
//
// Note the merge is a property of the series key, not of a single export:
// Deactivate flushes the pending window (otherwise the activation's last
// datapoints die with it), so sequential activations arrive as separate delta
// exports that the backend adds on the same key.
func TestSequentialActivationsMergeByTemplate(t *testing.T) {
	r, cap := newTestRelay(t, nil)

	r.Activate(testActor("uid-1", "actor-1", "chat-agent"))
	export(t, r, request(nil, deltaSum("work_items", 4, nil)))
	r.Deactivate()

	r.Activate(testActor("uid-2", "actor-2", "chat-agent"))
	export(t, r, request(nil, deltaSum("work_items", 6, nil)))
	r.Deactivate()

	r.Activate(testActor("uid-3", "actor-3", "other-template"))
	export(t, r, request(nil, deltaSum("work_items", 1, nil)))

	r.Flush(context.Background())

	series := map[string]int64{}
	for _, rm := range cap.metrics {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				if m.GetName() != "work_items" {
					continue
				}
				for _, dp := range m.GetSum().GetDataPoints() {
					key := canonical(rm.GetResource().GetAttributes()) + "|" + canonical(dp.GetAttributes())
					series[key] += dp.GetValue().(*metricspb.NumberDataPoint_AsInt).AsInt
				}
			}
		}
	}
	if len(series) != 2 {
		t.Errorf("work_items series = %d, want 2 (one per template): %v", len(series), series)
	}
	var total int64
	for _, v := range series {
		total += v
	}
	if total != 11 {
		t.Errorf("work_items total = %d, want 11", total)
	}
	// The two chat-agent activations share a key; 4+6 landed together.
	found := false
	for k, v := range series {
		if strings.Contains(k, "chat-agent") && v == 10 {
			found = true
		}
	}
	if !found {
		t.Errorf("chat-agent activations did not share a series: %v", series)
	}
}

// Exemplars are what preserve per-actor debugging without per-actor series, so
// the trace ID must survive aggregation.
func TestExemplarsSurviveAggregation(t *testing.T) {
	r, cap := newTestRelay(t, nil)
	r.Activate(testActor("uid-1", "actor-1", "chat-agent"))

	traceID := []byte{1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16}
	ex := []*metricspb.Exemplar{{
		TraceId:      traceID,
		SpanId:       []byte{1, 2, 3, 4, 5, 6, 7, 8},
		TimeUnixNano: 2,
		Value:        &metricspb.Exemplar_AsInt{AsInt: 1},
	}}
	export(t, r, request(nil, deltaSum("work_items", 1, ex)))
	r.Flush(context.Background())

	m := findMetric(cap.metrics, "work_items")
	if m == nil {
		t.Fatal("work_items not forwarded")
	}
	dps := m.GetSum().GetDataPoints()
	if len(dps) != 1 || len(dps[0].GetExemplars()) != 1 {
		t.Fatalf("exemplars lost: %v", dps)
	}
	if string(dps[0].GetExemplars()[0].GetTraceId()) != string(traceID) {
		t.Errorf("exemplar trace id changed")
	}
}

// Delta histograms from two actors merge bucket-wise.
func TestHistogramsMerge(t *testing.T) {
	r, cap := newTestRelay(t, nil)

	hist := func(count uint64, sum float64, buckets []uint64) *metricspb.Metric {
		s := sum
		return &metricspb.Metric{
			Name: "work_duration_ms",
			Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
				DataPoints: []*metricspb.HistogramDataPoint{{
					StartTimeUnixNano: 1,
					TimeUnixNano:      2,
					Count:             count,
					Sum:               &s,
					ExplicitBounds:    []float64{1, 10},
					BucketCounts:      buckets,
				}},
			}},
		}
	}

	r.Activate(testActor("uid-1", "actor-1", "chat-agent"))
	export(t, r, request(nil, hist(3, 6, []uint64{1, 2, 0})))
	r.Activate(testActor("uid-2", "actor-2", "chat-agent"))
	export(t, r, request(nil, hist(2, 30, []uint64{0, 1, 1})))
	r.Flush(context.Background())

	m := findMetric(cap.metrics, "work_duration_ms")
	if m == nil {
		t.Fatal("histogram not forwarded")
	}
	dps := m.GetHistogram().GetDataPoints()
	if len(dps) != 1 {
		t.Fatalf("histogram datapoints = %d, want 1", len(dps))
	}
	if dps[0].GetCount() != 5 || dps[0].GetSum() != 36 {
		t.Errorf("merged histogram = count %d sum %v, want 5 / 36", dps[0].GetCount(), dps[0].GetSum())
	}
	want := []uint64{1, 3, 1}
	for i, b := range dps[0].GetBucketCounts() {
		if b != want[i] {
			t.Errorf("bucket %d = %d, want %d", i, b, want[i])
		}
	}
}

// A gauge declared as an average is averaged, not summed. Silently summing a
// utilization gauge is the class of plausible-wrong number this design is
// about.
func TestGaugeAverageAggregation(t *testing.T) {
	r, cap := newTestRelay(t, map[string]GaugeAggregation{"cpu_utilization": GaugeAvg})

	gauge := func(v int64) *metricspb.Metric {
		return &metricspb.Metric{
			Name: "cpu_utilization",
			Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
				DataPoints: []*metricspb.NumberDataPoint{{
					TimeUnixNano: 2,
					Value:        &metricspb.NumberDataPoint_AsInt{AsInt: v},
				}},
			}},
		}
	}

	r.Activate(testActor("uid-1", "actor-1", "chat-agent"))
	export(t, r, request(nil, gauge(90)))
	r.Activate(testActor("uid-2", "actor-2", "chat-agent"))
	export(t, r, request(nil, gauge(10)))
	r.Flush(context.Background())

	m := findMetric(cap.metrics, "cpu_utilization")
	if m == nil {
		t.Fatal("gauge not forwarded")
	}
	dps := m.GetGauge().GetDataPoints()
	if len(dps) != 1 {
		t.Fatalf("gauge datapoints = %d, want 1", len(dps))
	}
	got := dps[0].GetValue().(*metricspb.NumberDataPoint_AsDouble).AsDouble
	if got != 50 {
		t.Errorf("averaged gauge = %v, want 50", got)
	}
}

// Datapoint-level attributes are another forgery surface, not just the
// Resource.
func TestDatapointLevelForgeryStripped(t *testing.T) {
	r, cap := newTestRelay(t, nil)
	r.Activate(testActor("uid-real", "actor-real", "chat-agent"))

	export(t, r, request(
		[]*commonpb.KeyValue{str(identity.KeyActorUID, "uid-victim")},
		deltaSum("work_items", 1, nil, str(identity.KeyActorUID, "uid-victim"), str("kind", "task")),
	))
	r.Flush(context.Background())

	m := findMetric(cap.metrics, "work_items")
	for _, dp := range m.GetSum().GetDataPoints() {
		for _, kv := range dp.GetAttributes() {
			if kv.GetKey() == identity.KeyActorUID {
				t.Errorf("forged datapoint attribute survived: %v", kv)
			}
		}
	}
	for _, rm := range cap.metrics {
		if isSelfTelemetry(rm) {
			continue // ateom's own metrics legitimately carry host facts
		}
		for _, kv := range rm.GetResource().GetAttributes() {
			if kv.GetKey() == identity.KeyActorUID {
				t.Errorf("forged resource attribute survived: %v", kv)
			}
			if kv.GetKey() == identity.KeyWorkerPod || kv.GetKey() == identity.KeyWorkerNode {
				t.Errorf("host fact on an actor metric resource: %v", kv)
			}
		}
	}
	if r.Stats().AttributesOverridden < 2 {
		t.Errorf("overridden attributes counted = %d, want >= 2", r.Stats().AttributesOverridden)
	}
}

// Unknown actor-supplied resource attributes must not reach metrics: the
// resource is part of the series key and the actor's set is unbounded.
func TestUnknownResourceAttributesDropped(t *testing.T) {
	r, cap := newTestRelay(t, nil)
	r.Activate(testActor("uid-1", "actor-1", "chat-agent"))

	export(t, r, request([]*commonpb.KeyValue{
		str("service.name", "demo"),
		str("session.id", "abc-123-unbounded"),
	}, deltaSum("work_items", 1, nil)))
	r.Flush(context.Background())

	for _, rm := range cap.metrics {
		for _, kv := range rm.GetResource().GetAttributes() {
			if kv.GetKey() == "session.id" {
				t.Errorf("unbounded actor resource attribute reached metrics: %v", kv)
			}
		}
	}
	if findMetric(cap.metrics, "work_items") == nil {
		t.Error("work_items should still be forwarded")
	}
}

// countActorSeries returns the exported datapoints and their total, ignoring
// the relay's own telemetry.
func countActorSeries(rms []*metricspb.ResourceMetrics) (series int, total int64) {
	for _, rm := range rms {
		if isSelfTelemetry(rm) {
			continue
		}
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				sum, ok := m.GetData().(*metricspb.Metric_Sum)
				if !ok {
					continue
				}
				for _, dp := range sum.Sum.GetDataPoints() {
					series++
					total += dp.GetValue().(*metricspb.NumberDataPoint_AsInt).AsInt
				}
			}
		}
	}
	return series, total
}

// MaxSeriesPerMetric only bounds attribute sets inside one (resource, metric
// name) pair. The metric name and the scope name are upstream of that cap and
// are the actor's to choose, so without a global budget an actor emitting N
// distinct metric names holds N accumulators in ateom and exports N series: a
// measured 5000-against-a-cap-of-200 before this budget existed. Since ateom is
// the privileged sidecar that owns the sandbox, that is the actor taking down
// its own worker pod.
func TestSeriesBudgetBoundsMetricAndScopeNames(t *testing.T) {
	const (
		budget = 50
		n      = 1000
	)
	cases := []struct {
		name string
		emit func(*testing.T, *Relay, int)
	}{
		{"metric names", func(t *testing.T, r *Relay, i int) {
			export(t, r, request(nil, deltaSum(fmt.Sprintf("metric_%d", i), 1, nil)))
		}},
		{"scope names", func(t *testing.T, r *Relay, i int) {
			export(t, r, requestScope(fmt.Sprintf("scope_%d", i), nil, deltaSum("work_items", 1, nil)))
		}},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			cap := &capture{}
			r := New(Config{
				Worker:             identity.Worker{PodName: "worker-1", NodeName: "node-a"},
				Upstream:           cap,
				Interval:           time.Hour,
				MaxSeriesPerMetric: 200,
				MaxSeriesTotal:     budget,
			})
			r.Activate(testActor("uid-1", "actor-1", "chat-agent"))
			for i := 0; i < n; i++ {
				tc.emit(t, r, i)
			}
			r.Flush(context.Background())

			series, total := countActorSeries(cap.metrics)
			if series > budget {
				t.Errorf("exported %d series, want at most the budget of %d", series, budget)
			}
			// Refusals are loud and attributable, not silent: every datapoint
			// is either in the output or counted under a reason.
			st := r.Stats()
			refused := st.DatapointsRejected[reasonSeriesBudget]
			if refused == 0 {
				t.Error("no datapoints counted as refused, so the budget never engaged")
			}
			if st.DatapointsAccepted+refused != n {
				t.Errorf("accepted %d + refused %d != %d emitted", st.DatapointsAccepted, refused, n)
			}
			// Series that were opened before the budget ran out must still be
			// correct; the budget refuses new ones, it does not corrupt old ones.
			if total != st.DatapointsAccepted {
				t.Errorf("exported total %d, want %d accepted datapoints", total, st.DatapointsAccepted)
			}

			// The budget is per interval, not for the life of the relay: drain
			// resets it, so the next interval starts with a clean map.
			r.Activate(testActor("uid-1", "actor-1", "chat-agent"))
			cap.metrics = nil
			export(t, r, request(nil, deltaSum("after_flush", 1, nil)))
			r.Flush(context.Background())
			if findMetric(cap.metrics, "after_flush") == nil {
				t.Error("after a flush the budget should be reset, but a new series was still refused")
			}
		})
	}
}

// Resource *values* are actor-controlled even when the keys are not, so an
// allow-list does not bound them: 1000 values of service.name were 1000 series.
// The metric resource is now built entirely from the activation RPC.
func TestActorResourceValuesCannotMultiplySeries(t *testing.T) {
	cap := &capture{}
	r := New(Config{
		Worker:   identity.Worker{PodName: "worker-1", NodeName: "node-a"},
		Upstream: cap,
		Interval: time.Hour,
	})
	r.Activate(testActor("uid-1", "actor-1", "chat-agent"))

	const n = 1000
	for i := 0; i < n; i++ {
		export(t, r, request([]*commonpb.KeyValue{
			str("service.name", fmt.Sprintf("svc-%d", i)),
			str("service.version", fmt.Sprintf("v%d", i)),
			str("telemetry.sdk.version", fmt.Sprintf("1.0.%d", i)),
		}, deltaSum("work_items", 1, nil)))
	}
	r.Flush(context.Background())

	series, total := countActorSeries(cap.metrics)
	if series != 1 {
		t.Errorf("exported %d series, want 1: actor resource values must not be part of the metric series key", series)
	}
	if total != n {
		t.Errorf("exported total %d, want %d", total, n)
	}
	if refused := r.Stats().DatapointsRejected[reasonSeriesBudget]; refused != 0 {
		t.Errorf("%d datapoints refused, want 0: collapsing the resource should keep this well inside the budget", refused)
	}

	// service.name is pinned to the template, not dropped: dashboards still
	// have it, and it is a fact the platform controls.
	for _, rm := range cap.metrics {
		if isSelfTelemetry(rm) {
			continue
		}
		for _, kv := range rm.GetResource().GetAttributes() {
			if kv.GetKey() == "service.name" && kv.GetValue().GetStringValue() != "chat-agent" {
				t.Errorf("service.name = %q, want the template name", kv.GetValue().GetStringValue())
			}
		}
	}
}

// Partial success carries the reason back to the actor.
func TestPartialSuccessReportsReasons(t *testing.T) {
	r, _ := newTestRelay(t, nil)
	r.Activate(testActor("uid-1", "actor-1", "chat-agent"))

	cumulative := &metricspb.Metric{
		Name: "legacy_total",
		Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
			AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
			IsMonotonic:            true,
			DataPoints: []*metricspb.NumberDataPoint{{
				TimeUnixNano: 2,
				Value:        &metricspb.NumberDataPoint_AsInt{AsInt: 7},
			}},
		}},
	}
	resp := export(t, r, request(nil, cumulative, deltaSum("work_items", 1, nil)))
	ps := resp.GetPartialSuccess()
	if ps == nil || ps.GetRejectedDataPoints() != 1 {
		t.Fatalf("partial success = %v, want 1 rejected datapoint", ps)
	}
	if got := ps.GetErrorMessage(); got == "" {
		t.Error("partial success carried no error message")
	}
}
