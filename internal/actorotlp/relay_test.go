// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package actorotlp

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"

	"github.com/agent-substrate/substrate/internal/resources"
)

// sink is the fake atelet relay: it records what the actor relay forwards.
type sink struct {
	coltracepb.UnimplementedTraceServiceServer
	mu      sync.Mutex
	traces  []*coltracepb.ExportTraceServiceRequest
	metrics []*colmetricspb.ExportMetricsServiceRequest
	logs    []*collogspb.ExportLogsServiceRequest
}

func (s *sink) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.traces = append(s.traces, proto.Clone(req).(*coltracepb.ExportTraceServiceRequest))
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

type metricsSink struct {
	colmetricspb.UnimplementedMetricsServiceServer
	parent *sink
}

func (m *metricsSink) Export(_ context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	m.parent.mu.Lock()
	defer m.parent.mu.Unlock()
	m.parent.metrics = append(m.parent.metrics, proto.Clone(req).(*colmetricspb.ExportMetricsServiceRequest))
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

type logsSink struct {
	collogspb.UnimplementedLogsServiceServer
	parent *sink
}

func (l *logsSink) Export(_ context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	l.parent.mu.Lock()
	defer l.parent.mu.Unlock()
	l.parent.logs = append(l.parent.logs, proto.Clone(req).(*collogspb.ExportLogsServiceRequest))
	return &collogspb.ExportLogsServiceResponse{}, nil
}

func startSink(t *testing.T) (*sink, *grpc.ClientConn) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	s := &sink{}
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, s)
	colmetricspb.RegisterMetricsServiceServer(srv, &metricsSink{parent: s})
	collogspb.RegisterLogsServiceServer(srv, &logsSink{parent: s})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return s, conn
}

var (
	worker = Worker{PodUID: "pod-uid-1", PodName: "worker-abc", PodNamespace: "ate-demo", NodeName: "node-1"}
	actorA = resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: "team-a", Name: "crawler-1"},
		UID:              "11111111-1111-1111-1111-111111111111",
		TemplateAtespace: "team-a",
		TemplateName:     "crawler",
	}
	actorB = resources.ActorAttribution{
		Ref:              resources.ActorRef{Atespace: "team-b", Name: "crawler-2"},
		UID:              "22222222-2222-2222-2222-222222222222",
		TemplateAtespace: "team-b",
		TemplateName:     "crawler",
	}
	activatedAt = time.Date(2026, 9, 30, 12, 0, 0, 0, time.UTC)
)

// newRelay builds a relay whose clock starts at activatedAt and advances only
// when the test says so.
func newRelay(t *testing.T, upstream *grpc.ClientConn) (*Relay, *time.Time) {
	t.Helper()
	r, err := New(upstream, worker, 4)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	now := activatedAt
	r.now = func() time.Time { return now }
	return r, &now
}

// endpoint serves the actor's two listeners on loopback and returns their
// addresses. The HTTP listener's port is what tells the relay which is which.
type endpoint struct {
	grpcConn *grpc.ClientConn
	grpcAddr string
	httpBase string
}

func serveActor(t *testing.T, r *Relay, actorUID string) endpoint {
	t.Helper()
	serve, err := r.Bind(actorUID)
	if err != nil {
		t.Fatalf("Bind: %v", err)
	}
	grpcLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	httpLis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan error, 2)
	go func() { done <- serve(ctx, grpcLis) }()
	go func() { done <- serve(ctx, httpLis) }()
	t.Cleanup(func() {
		grpcLis.Close()
		httpLis.Close()
		cancel()
		for range 2 {
			if err := <-done; err != nil {
				t.Errorf("serve returned %v after the listener closed, want nil", err)
			}
		}
	})
	conn, err := grpc.NewClient(grpcLis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return endpoint{grpcConn: conn, grpcAddr: grpcLis.Addr().String(), httpBase: "http://" + httpLis.Addr().String()}
}

func str(key, value string) *commonpb.KeyValue { return stringAttr(key, value) }

func attrMap(attrs []*commonpb.KeyValue) map[string]string {
	out := map[string]string{}
	for _, kv := range attrs {
		switch v := kv.GetValue().GetValue().(type) {
		case *commonpb.AnyValue_StringValue:
			out[kv.GetKey()] = v.StringValue
		case *commonpb.AnyValue_IntValue:
			out[kv.GetKey()] = strconv.FormatInt(v.IntValue, 10)
		default:
			out[kv.GetKey()] = kv.GetValue().String()
		}
	}
	return out
}

// forgedResource is what a hostile or merely misconfigured actor sends: an
// identity of its own choosing under every namespace the platform owns, plus
// an attribute of its own that must survive.
func forgedResource() *resourcepb.Resource {
	return &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
		str("service.name", "forged-service"),
		str("service.instance.id", "forged-instance"),
		str("service.version", "1.2.3"),
		str("ate.actor.uid", "forged-uid"),
		str("ate.atespace", "victim-tenant"),
		str("k8s.pod.name", "victim-pod"),
		str("telemetry.sdk.language", "go"),
		str("deployment.environment.name", "prod"),
	}}
}

func TestTracesAreScrubbedAndStamped(t *testing.T) {
	s, upstream := startSink(t)
	r, _ := newRelay(t, upstream)
	if err := r.Register(actorA); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ep := serveActor(t, r, actorA.UID)

	_, err := coltracepb.NewTraceServiceClient(ep.grpcConn).Export(context.Background(), &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: forgedResource(),
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope: &commonpb.InstrumentationScope{Name: "app", Attributes: []*commonpb.KeyValue{str("ate.forged", "x"), str("kept", "y")}},
				Spans: []*tracepb.Span{{
					Name:       "tick",
					Attributes: []*commonpb.KeyValue{str("ate.actor.uid", "forged"), str("http.route", "/tick")},
					Events:     []*tracepb.Span_Event{{Name: "ev", Attributes: []*commonpb.KeyValue{str("ate.template.name", "forged"), str("k", "v")}}},
					Links:      []*tracepb.Span_Link{{Attributes: []*commonpb.KeyValue{str("ate.atespace", "forged")}}},
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatalf("Export: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.traces) != 1 {
		t.Fatalf("upstream got %d trace exports, want 1", len(s.traces))
	}
	rs := s.traces[0].GetResourceSpans()[0]
	got := attrMap(rs.GetResource().GetAttributes())
	want := map[string]string{
		"service.name":           "crawler",
		"service.namespace":      "team-a",
		"service.instance.id":    "pod-uid-1/0",
		"ate.telemetry.plane":    "actor",
		"ate.template.atespace":  "team-a",
		"ate.template.name":      "crawler",
		"ate.actor.slot":         "0",
		"k8s.pod.uid":            "pod-uid-1",
		"k8s.pod.name":           "worker-abc",
		"k8s.namespace.name":     "ate-demo",
		"k8s.node.name":          "node-1",
		"ate.atespace":           "team-a",
		"ate.actor.name":         "crawler-1",
		"ate.actor.uid":          actorA.UID,
		"telemetry.sdk.language": "go",
		// Not a platform namespace, so the actor's word stands.
		"deployment.environment.name": "prod",
	}
	for k, v := range want {
		if got[k] != v {
			t.Errorf("resource %s = %q, want %q", k, got[k], v)
		}
	}
	if v, ok := got["service.version"]; ok {
		t.Errorf("resource kept the actor's service.version=%q; every service.* claim is dropped", v)
	}
	if len(got) != len(want) {
		t.Errorf("resource has %d attributes, want %d: %v", len(got), len(want), got)
	}

	ss := rs.GetScopeSpans()[0]
	if m := attrMap(ss.GetScope().GetAttributes()); m["ate.forged"] != "" || m["kept"] != "y" {
		t.Errorf("scope attributes = %v, want ate.* dropped and the rest kept", m)
	}
	span := ss.GetSpans()[0]
	if m := attrMap(span.GetAttributes()); m["ate.actor.uid"] != "" || m["http.route"] != "/tick" {
		t.Errorf("span attributes = %v, want ate.* dropped and the rest kept", m)
	}
	if m := attrMap(span.GetEvents()[0].GetAttributes()); m["ate.template.name"] != "" || m["k"] != "v" {
		t.Errorf("event attributes = %v, want ate.* dropped and the rest kept", m)
	}
	if m := attrMap(span.GetLinks()[0].GetAttributes()); len(m) != 0 {
		t.Errorf("link attributes = %v, want ate.* dropped", m)
	}
}

func TestMetricsCarryNoActorIdentityAndGetRelayTimestamps(t *testing.T) {
	s, upstream := startSink(t)
	r, now := newRelay(t, upstream)
	if err := r.Register(actorA); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ep := serveActor(t, r, actorA.UID)

	golden := uint64(activatedAt.Add(-48 * time.Hour).UnixNano())
	actorClock := uint64(activatedAt.Add(-time.Minute).UnixNano()) // a lagging guest clock
	*now = activatedAt.Add(10 * time.Second)

	export := func() {
		t.Helper()
		_, err := colmetricspb.NewMetricsServiceClient(ep.grpcConn).Export(context.Background(), &colmetricspb.ExportMetricsServiceRequest{
			ResourceMetrics: []*metricspb.ResourceMetrics{{
				Resource: forgedResource(),
				ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{
					{Name: "ticks", Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
						DataPoints: []*metricspb.NumberDataPoint{{
							StartTimeUnixNano: golden, TimeUnixNano: actorClock,
							Attributes: []*commonpb.KeyValue{str("ate.actor.uid", "forged"), str("kind", "a")},
							Exemplars:  []*metricspb.Exemplar{{FilteredAttributes: []*commonpb.KeyValue{str("ate.actor.name", "forged"), str("trace", "t")}}},
							Value:      &metricspb.NumberDataPoint_AsInt{AsInt: 3},
						}},
					}}},
					{Name: "temp", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
						DataPoints: []*metricspb.NumberDataPoint{{TimeUnixNano: actorClock, Value: &metricspb.NumberDataPoint_AsDouble{AsDouble: 1.5}}},
					}}},
					{Name: "lat", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
						DataPoints: []*metricspb.HistogramDataPoint{{StartTimeUnixNano: golden, TimeUnixNano: actorClock, Count: 1}},
					}}},
				}}},
			}},
		})
		if err != nil {
			t.Fatalf("Export: %v", err)
		}
	}
	export()
	export() // Same relay clock: the second export must still be strictly later.

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.metrics) != 2 {
		t.Fatalf("upstream got %d metric exports, want 2", len(s.metrics))
	}
	rm := s.metrics[0].GetResourceMetrics()[0]
	got := attrMap(rm.GetResource().GetAttributes())
	for _, k := range []string{"ate.actor.uid", "ate.actor.name", "ate.atespace"} {
		if v, ok := got[k]; ok {
			t.Errorf("metric resource carries %s=%q; actor identity is never a metric label", k, v)
		}
	}
	for k, v := range map[string]string{"service.name": "crawler", "service.instance.id": "pod-uid-1/0", "ate.actor.slot": "0", "ate.telemetry.plane": "actor", "k8s.pod.uid": "pod-uid-1"} {
		if got[k] != v {
			t.Errorf("metric resource %s = %q, want %q", k, got[k], v)
		}
	}

	metrics := rm.GetScopeMetrics()[0].GetMetrics()
	wantNow := uint64((*now).UnixNano())
	sum := metrics[0].GetSum().GetDataPoints()[0]
	if sum.GetStartTimeUnixNano() != uint64(activatedAt.UnixNano()) {
		t.Errorf("sum start = %d, want the activation time %d (the golden build's start must not survive)", sum.GetStartTimeUnixNano(), activatedAt.UnixNano())
	}
	if sum.GetTimeUnixNano() != wantNow {
		t.Errorf("sum time = %d, want the relay's clock %d, not the actor's %d", sum.GetTimeUnixNano(), wantNow, actorClock)
	}
	if m := attrMap(sum.GetAttributes()); m["ate.actor.uid"] != "" || m["kind"] != "a" {
		t.Errorf("data point attributes = %v, want ate.* dropped and the rest kept", m)
	}
	if m := attrMap(sum.GetExemplars()[0].GetFilteredAttributes()); m["ate.actor.name"] != "" || m["trace"] != "t" {
		t.Errorf("exemplar attributes = %v, want ate.* dropped and the rest kept", m)
	}
	gauge := metrics[1].GetGauge().GetDataPoints()[0]
	if gauge.GetStartTimeUnixNano() != 0 || gauge.GetTimeUnixNano() != wantNow {
		t.Errorf("gauge (start, time) = (%d, %d), want (0, %d)", gauge.GetStartTimeUnixNano(), gauge.GetTimeUnixNano(), wantNow)
	}
	hist := metrics[2].GetHistogram().GetDataPoints()[0]
	if hist.GetStartTimeUnixNano() != uint64(activatedAt.UnixNano()) || hist.GetTimeUnixNano() != wantNow {
		t.Errorf("histogram (start, time) = (%d, %d), want (%d, %d)", hist.GetStartTimeUnixNano(), hist.GetTimeUnixNano(), activatedAt.UnixNano(), wantNow)
	}

	second := s.metrics[1].GetResourceMetrics()[0].GetScopeMetrics()[0].GetMetrics()[0].GetSum().GetDataPoints()[0]
	if second.GetTimeUnixNano() <= sum.GetTimeUnixNano() {
		t.Errorf("second export time %d is not after the first %d; points must stay ordered per stream", second.GetTimeUnixNano(), sum.GetTimeUnixNano())
	}
}

func TestStartAfterReceiveTimeIsClamped(t *testing.T) {
	_, upstream := startSink(t)
	r, now := newRelay(t, upstream)
	if err := r.Register(actorA); err != nil {
		t.Fatalf("Register: %v", err)
	}
	*now = activatedAt.Add(time.Second)
	ex := &exporter{relay: r, act: r.actors[actorA.UID]}
	ahead := uint64(activatedAt.Add(time.Hour).UnixNano())
	start, end := ex.stampTimes(ahead, ex.nextStamp())
	if start != end {
		t.Errorf("(start, end) = (%d, %d) for a start ahead of the relay clock, want a zero-length interval", start, end)
	}
}

func TestLogsOverHTTP(t *testing.T) {
	s, upstream := startSink(t)
	r, _ := newRelay(t, upstream)
	if err := r.Register(actorB); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ep := serveActor(t, r, actorB.UID)

	body, err := proto.Marshal(&collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: forgedResource(),
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			Body:       &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "hello"}},
			Attributes: []*commonpb.KeyValue{str("ate.actor.uid", "forged"), str("level", "info")},
		}}}},
	}}})
	if err != nil {
		t.Fatal(err)
	}
	resp, err := http.Post(ep.httpBase+"/v1/logs", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("POST /v1/logs: %v", err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		b, _ := io.ReadAll(resp.Body)
		t.Fatalf("POST /v1/logs = %d %q, want 200", resp.StatusCode, b)
	}
	if ct := resp.Header.Get("Content-Type"); ct != "application/x-protobuf" {
		t.Errorf("response Content-Type = %q, want application/x-protobuf", ct)
	}
	raw, _ := io.ReadAll(resp.Body)
	if err := proto.Unmarshal(raw, &collogspb.ExportLogsServiceResponse{}); err != nil {
		t.Errorf("response body is not an ExportLogsServiceResponse: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.logs) != 1 {
		t.Fatalf("upstream got %d log exports, want 1", len(s.logs))
	}
	rl := s.logs[0].GetResourceLogs()[0]
	got := attrMap(rl.GetResource().GetAttributes())
	if got["service.name"] != "crawler" || got["ate.actor.uid"] != actorB.UID || got["ate.atespace"] != "team-b" || got["service.instance.id"] != "pod-uid-1/0" {
		t.Errorf("log resource = %v, want the template as service and actor B's identity", got)
	}
	// The record carries the identity too, under the stdout pipeline's keys,
	// with the actor's forged value replaced rather than merely dropped.
	m := attrMap(rl.GetScopeLogs()[0].GetLogRecords()[0].GetAttributes())
	if m["level"] != "info" {
		t.Errorf("log record attributes = %v, want the actor's own kept", m)
	}
	for k, v := range map[string]string{
		"ate.actor.uid": actorB.UID, "ate.actor.name": "crawler-2", "ate.atespace": "team-b",
		"ate.template.atespace": "team-b", "ate.template.name": "crawler",
	} {
		if m[k] != v {
			t.Errorf("log record %s = %q, want %q", k, m[k], v)
		}
	}
}

func TestHTTPRefusesWhatItCannotParse(t *testing.T) {
	_, upstream := startSink(t)
	r, _ := newRelay(t, upstream)
	if err := r.Register(actorA); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ep := serveActor(t, r, actorA.UID)

	for _, tc := range []struct {
		name        string
		method      string
		contentType string
		body        string
		want        int
	}{
		{name: "GET", method: http.MethodGet, contentType: "application/x-protobuf", want: http.StatusMethodNotAllowed},
		{name: "JSON body", method: http.MethodPost, contentType: "application/json", body: "{}", want: http.StatusUnsupportedMediaType},
		{name: "garbage protobuf", method: http.MethodPost, contentType: "application/x-protobuf", body: "\xff\xff\xff", want: http.StatusBadRequest},
	} {
		t.Run(tc.name, func(t *testing.T) {
			req, _ := http.NewRequest(tc.method, ep.httpBase+"/v1/traces", bytes.NewBufferString(tc.body))
			req.Header.Set("Content-Type", tc.contentType)
			resp, err := http.DefaultClient.Do(req)
			if err != nil {
				t.Fatal(err)
			}
			resp.Body.Close()
			if resp.StatusCode != tc.want {
				t.Errorf("status = %d, want %d", resp.StatusCode, tc.want)
			}
		})
	}
}

func TestNoUpstreamIsUnavailable(t *testing.T) {
	r, _ := newRelay(t, nil)
	if err := r.Register(actorA); err != nil {
		t.Fatalf("Register: %v", err)
	}
	ep := serveActor(t, r, actorA.UID)

	_, err := coltracepb.NewTraceServiceClient(ep.grpcConn).Export(context.Background(), &coltracepb.ExportTraceServiceRequest{})
	if got := status.Code(err); got != codes.Unavailable {
		t.Errorf("gRPC export without an upstream = %v (%v), want Unavailable", got, err)
	}

	body, _ := proto.Marshal(&colmetricspb.ExportMetricsServiceRequest{})
	resp, err := http.Post(ep.httpBase+"/v1/metrics", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusServiceUnavailable {
		t.Errorf("HTTP export without an upstream = %d, want 503", resp.StatusCode)
	}
	if resp.Header.Get("Retry-After") == "" {
		t.Error("503 carries no Retry-After; the SDKs back off on it")
	}
}

func TestBudgetExhaustedIsResourceExhausted(t *testing.T) {
	s, upstream := startSink(t)
	r, _ := newRelay(t, upstream)
	if err := r.Register(actorA); err != nil {
		t.Fatalf("Register: %v", err)
	}
	// A budget of two, refilling too slowly to matter within the test.
	r.actors[actorA.UID].limiter = rate.NewLimiter(rate.Every(time.Hour), 2)
	ep := serveActor(t, r, actorA.UID)

	client := collogspb.NewLogsServiceClient(ep.grpcConn)
	for i := range 2 {
		if _, err := client.Export(context.Background(), &collogspb.ExportLogsServiceRequest{}); err != nil {
			t.Fatalf("export %d within budget: %v", i, err)
		}
	}
	_, err := client.Export(context.Background(), &collogspb.ExportLogsServiceRequest{})
	if got := status.Code(err); got != codes.ResourceExhausted {
		t.Errorf("export over budget = %v (%v), want ResourceExhausted", got, err)
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.logs) != 2 {
		t.Errorf("upstream got %d exports, want the 2 within budget only", len(s.logs))
	}
}

func TestSlotsAreReusedOnlyAfterRelease(t *testing.T) {
	r, _ := newRelay(t, nil)
	slotOf := func(uid string) int {
		t.Helper()
		slot, ok := r.Slot(uid)
		if !ok {
			t.Fatalf("actor %s is not registered", uid)
		}
		return slot
	}
	if err := r.Register(actorA); err != nil {
		t.Fatal(err)
	}
	if err := r.Register(actorB); err != nil {
		t.Fatal(err)
	}
	if a, b := slotOf(actorA.UID), slotOf(actorB.UID); a == b {
		t.Fatalf("two live actors share slot %d", a)
	}
	// A re-register is a fresh activation of a still-hosted actor: same seat.
	before := slotOf(actorA.UID)
	if err := r.Register(actorA); err != nil {
		t.Fatal(err)
	}
	if after := slotOf(actorA.UID); after != before {
		t.Errorf("re-registering moved actor A from slot %d to %d", before, after)
	}

	third := actorA
	third.UID = "33333333-3333-3333-3333-333333333333"
	r.Release(actorA.UID)
	if err := r.Register(third); err != nil {
		t.Fatal(err)
	}
	if got := slotOf(third.UID); got != before {
		t.Errorf("the next actor took slot %d, want the released slot %d", got, before)
	}
	if _, ok := r.Slot(actorA.UID); ok {
		t.Error("released actor still reports a slot")
	}

	// Capacity is the slot count: filling every seat refuses the next.
	for i := 0; ; i++ {
		extra := actorA
		extra.UID = fmt.Sprintf("44444444-4444-4444-4444-%012d", i)
		if err := r.Register(extra); err != nil {
			if i != 2 {
				t.Errorf("registration %d failed: %v; want the worker full only after 4 slots", i, err)
			}
			break
		}
		if i > 3 {
			t.Fatal("registered more actors than the worker has slots")
		}
	}
}

func TestBindUnknownActorFails(t *testing.T) {
	r, _ := newRelay(t, nil)
	if _, err := r.Bind("nobody"); err == nil {
		t.Error("Bind of an unregistered actor succeeded; a listener must never serve without an identity")
	}
	if err := r.Register(resources.ActorAttribution{}); err == nil {
		t.Error("Register without a UID succeeded")
	}
}

func TestClosingTheListenerStopsAcceptedConnections(t *testing.T) {
	_, upstream := startSink(t)
	r, _ := newRelay(t, upstream)
	if err := r.Register(actorA); err != nil {
		t.Fatal(err)
	}
	serve, err := r.Bind(actorA.UID)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	done := make(chan error, 1)
	go func() { done <- serve(context.Background(), lis) }()

	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	client := coltracepb.NewTraceServiceClient(conn)
	if _, err := client.Export(context.Background(), &coltracepb.ExportTraceServiceRequest{}); err != nil {
		t.Fatalf("export before close: %v", err)
	}

	lis.Close()
	select {
	case err := <-done:
		if err != nil {
			t.Errorf("serve returned %v, want nil after the listener closed", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("serve did not return after the listener closed")
	}
	// The connection accepted before the close is gone too, so nothing bound
	// to this activation can carry data after teardown.
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if _, err := client.Export(ctx, &coltracepb.ExportTraceServiceRequest{}); err == nil {
		t.Error("export on a connection accepted before teardown succeeded; the server must close accepted connections")
	}
}

// TestEveryListenerSpeaksBothProtocols is the property the environment
// relies on: it points every actor at one port, so an SDK that only speaks
// HTTP/protobuf, or that ignores OTEL_EXPORTER_OTLP_PROTOCOL, must be served
// there too. The test crosses the protocols over: HTTP to the listener a gRPC
// client would use, and gRPC to the one an HTTP client would use.
func TestEveryListenerSpeaksBothProtocols(t *testing.T) {
	s, upstream := startSink(t)
	r, _ := newRelay(t, upstream)
	if err := r.Register(actorA); err != nil {
		t.Fatal(err)
	}
	ep := serveActor(t, r, actorA.UID)

	// HTTP/protobuf to the gRPC connection's address.
	body, _ := proto.Marshal(&coltracepb.ExportTraceServiceRequest{ResourceSpans: []*tracepb.ResourceSpans{{Resource: forgedResource()}}})
	resp, err := http.Post("http://"+ep.grpcAddr+"/v1/traces", "application/x-protobuf", bytes.NewReader(body))
	if err != nil {
		t.Fatalf("HTTP POST to the gRPC listener: %v", err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		t.Errorf("HTTP POST to the gRPC listener = %d, want 200", resp.StatusCode)
	}

	// gRPC to the HTTP listener's address.
	conn, err := grpc.NewClient(strings.TrimPrefix(ep.httpBase, "http://"), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Close()
	if _, err := collogspb.NewLogsServiceClient(conn).Export(context.Background(), &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{Resource: forgedResource()}}}); err != nil {
		t.Errorf("gRPC export to the HTTP listener: %v", err)
	}

	s.mu.Lock()
	defer s.mu.Unlock()
	if len(s.traces) != 1 || len(s.logs) != 1 {
		t.Errorf("upstream got %d traces and %d logs, want 1 and 1", len(s.traces), len(s.logs))
	}
	if got := attrMap(s.traces[0].GetResourceSpans()[0].GetResource().GetAttributes())["service.name"]; got != "crawler" {
		t.Errorf("HTTP export on the gRPC listener was attributed to %q, want crawler", got)
	}
}
