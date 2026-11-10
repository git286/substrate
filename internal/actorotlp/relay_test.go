// Copyright 2026 The Agent Substrate Authors
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
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"sync"
	"testing"
	"time"

	"github.com/google/go-cmp/cmp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/internal/resources"
)

// sink stands in for atelet's relay socket and records what is forwarded.
type sink struct {
	mu      sync.Mutex
	traces  []*coltracepb.ExportTraceServiceRequest
	metrics []*colmetricspb.ExportMetricsServiceRequest
	logs    []*collogspb.ExportLogsServiceRequest
}

type traceSink struct {
	coltracepb.UnimplementedTraceServiceServer
	*sink
}

func (s traceSink) Export(_ context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.traces = append(s.traces, req)
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

type metricsSink struct {
	colmetricspb.UnimplementedMetricsServiceServer
	*sink
}

func (s metricsSink) Export(_ context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.metrics = append(s.metrics, req)
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

type logsSink struct {
	collogspb.UnimplementedLogsServiceServer
	*sink
}

func (s logsSink) Export(_ context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.logs = append(s.logs, req)
	return &collogspb.ExportLogsServiceResponse{}, nil
}

func startSink(t *testing.T) (*sink, *grpc.ClientConn) {
	t.Helper()
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	s := &sink{}
	srv := grpc.NewServer()
	coltracepb.RegisterTraceServiceServer(srv, traceSink{sink: s})
	colmetricspb.RegisterMetricsServiceServer(srv, metricsSink{sink: s})
	collogspb.RegisterLogsServiceServer(srv, logsSink{sink: s})
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return s, conn
}

var (
	testWorker = Worker{PodUID: "pod-uid-1", PodName: "worker-abc", PodNamespace: "pool-ns", NodeName: "node-1"}
	actorA     = resources.ActorAttribution{
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
	activatedAt = time.Date(2026, 11, 10, 12, 0, 0, 0, time.UTC)
)

type fixture struct {
	relay  *Relay
	sink   *sink
	reader *sdkmetric.ManualReader
	// clock is what the relay reads. It moves only when a test moves it.
	clock time.Time
}

// newFixture builds a relay with four slots whose clock starts at activatedAt.
// A nil upstream is a worker that started without atelet's socket.
func newFixture(t *testing.T, withUpstream bool) *fixture {
	t.Helper()
	f := &fixture{reader: sdkmetric.NewManualReader(), clock: activatedAt}
	var upstream *grpc.ClientConn
	if withUpstream {
		f.sink, upstream = startSink(t)
	}
	meter := sdkmetric.NewMeterProvider(sdkmetric.WithReader(f.reader)).Meter("test")
	r, err := New(upstream, testWorker, 4, meter)
	if err != nil {
		t.Fatal(err)
	}
	r.now = func() time.Time { return f.clock }
	f.relay = r
	return f
}

// endpoint is one actor's listener, as its exporter reaches it.
type endpoint struct {
	addr     string
	listener net.Listener
	conn     *grpc.ClientConn
	served   chan struct{}
}

// host registers the actor and serves its listener, as the workers do.
func (f *fixture) host(t *testing.T, a resources.ActorAttribution) endpoint {
	t.Helper()
	if err := f.relay.Register(a); err != nil {
		t.Fatal(err)
	}
	serve, err := f.relay.Bind(a.UID)
	if err != nil {
		t.Fatal(err)
	}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	served := make(chan struct{})
	go func() {
		defer close(served)
		if err := serve(context.Background(), lis); err != nil {
			t.Errorf("serve: %v", err)
		}
	}()
	t.Cleanup(func() {
		_ = lis.Close()
		<-served
	})
	conn, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return endpoint{addr: lis.Addr().String(), listener: lis, conn: conn, served: served}
}

func attrs(kv ...string) []*commonpb.KeyValue {
	out := make([]*commonpb.KeyValue, 0, len(kv)/2)
	for i := 0; i < len(kv); i += 2 {
		out = append(out, stringAttr(kv[i], kv[i+1]))
	}
	return out
}

func attrMap(kvs []*commonpb.KeyValue) map[string]string {
	m := map[string]string{}
	for _, kv := range kvs {
		switch v := kv.GetValue().GetValue().(type) {
		case *commonpb.AnyValue_StringValue:
			m[kv.GetKey()] = v.StringValue
		case *commonpb.AnyValue_IntValue:
			m[kv.GetKey()] = fmt.Sprint(v.IntValue)
		}
	}
	return m
}

// forgedResource claims another actor's identity and a route of its own.
func forgedResource() *resourcepb.Resource {
	return &resourcepb.Resource{Attributes: attrs(
		"service.name", "ateworker-gvisor",
		"service.namespace", "ate-system",
		"service.instance.id", "someone-else",
		"ate.actor.uid", "victim",
		"ate.telemetry.plane", "platform",
		"k8s.pod.name", "ate-api-server-0",
		"gcp.project_id", "other-project",
		"host.name", "golden-builder",
		"telemetry.sdk.language", "go",
		"process.runtime.name", "go",
	)}
}

// forged are attributes an actor may not set below the resource.
func forged() []*commonpb.KeyValue {
	return attrs("ate.actor.uid", "victim", "ate.atespace", "other", "route", "/x")
}

// forgedPoint adds the names Prometheus gives resource attributes.
func forgedPoint() []*commonpb.KeyValue {
	return append(forged(), attrs("service.name", "evil", "k8s.pod.uid", "evil", "job", "evil", "instance", "evil")...)
}

func boundedIdentity(a resources.ActorAttribution, slot string) map[string]string {
	return map[string]string{
		"service.name":           a.TemplateName,
		"service.namespace":      a.TemplateAtespace,
		"service.instance.id":    testWorker.PodUID + "/" + slot,
		"ate.telemetry.plane":    "actor",
		"ate.template.atespace":  a.TemplateAtespace,
		"ate.template.name":      a.TemplateName,
		"ate.worker.slot":        slot,
		"k8s.pod.uid":            testWorker.PodUID,
		"k8s.pod.name":           testWorker.PodName,
		"k8s.namespace.name":     testWorker.PodNamespace,
		"k8s.node.name":          testWorker.NodeName,
		"telemetry.sdk.language": "go",
		"process.runtime.name":   "go",
	}
}

func fullIdentity(a resources.ActorAttribution, slot string) map[string]string {
	m := boundedIdentity(a, slot)
	m["ate.actor.uid"] = a.UID
	m["ate.actor.name"] = a.Ref.Name
	m["ate.atespace"] = a.Ref.Atespace
	return m
}

func TestTracesAreScrubbedAndStamped(t *testing.T) {
	f := newFixture(t, true)
	ep := f.host(t, actorA)

	_, err := coltracepb.NewTraceServiceClient(ep.conn).Export(t.Context(), &coltracepb.ExportTraceServiceRequest{
		ResourceSpans: []*tracepb.ResourceSpans{{
			Resource: forgedResource(),
			ScopeSpans: []*tracepb.ScopeSpans{{
				Scope: &commonpb.InstrumentationScope{Name: "app", Attributes: forged()},
				Spans: []*tracepb.Span{{
					Name:              "work",
					StartTimeUnixNano: 5,
					EndTimeUnixNano:   7,
					Attributes:        forged(),
					Events:            []*tracepb.Span_Event{{Attributes: forged()}},
					Links:             []*tracepb.Span_Link{{Attributes: forged()}},
				}},
			}},
		}},
	})
	if err != nil {
		t.Fatal(err)
	}

	rs := f.sink.traces[0].GetResourceSpans()[0]
	if diff := cmp.Diff(fullIdentity(actorA, "0"), attrMap(rs.GetResource().GetAttributes())); diff != "" {
		t.Errorf("resource attributes (-want +got):\n%s", diff)
	}
	ss := rs.GetScopeSpans()[0]
	span := ss.GetSpans()[0]
	kept := map[string]string{"route": "/x"}
	for level, got := range map[string][]*commonpb.KeyValue{
		"scope": ss.GetScope().GetAttributes(),
		"span":  span.GetAttributes(),
		"event": span.GetEvents()[0].GetAttributes(),
		"link":  span.GetLinks()[0].GetAttributes(),
	} {
		if diff := cmp.Diff(kept, attrMap(got)); diff != "" {
			t.Errorf("%s attributes (-want +got):\n%s", level, diff)
		}
	}
	if span.GetStartTimeUnixNano() != 5 || span.GetEndTimeUnixNano() != 7 {
		t.Errorf("span times = %d..%d, want them as sent", span.GetStartTimeUnixNano(), span.GetEndTimeUnixNano())
	}
}

func TestMetricsGetWriterIdentityAndRelayTimestamps(t *testing.T) {
	f := newFixture(t, true)
	ep := f.host(t, actorA)
	// The golden build's SDK started an hour before this activation.
	golden := uint64(activatedAt.Add(-time.Hour).UnixNano())
	f.clock = activatedAt.Add(5 * time.Second)

	export := func() *metricspb.ResourceMetrics {
		t.Helper()
		_, err := colmetricspb.NewMetricsServiceClient(ep.conn).Export(t.Context(), &colmetricspb.ExportMetricsServiceRequest{
			ResourceMetrics: []*metricspb.ResourceMetrics{{
				Resource: forgedResource(),
				ScopeMetrics: []*metricspb.ScopeMetrics{{Metrics: []*metricspb.Metric{
					{Name: "requests", Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
						AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
						DataPoints: []*metricspb.NumberDataPoint{{
							Attributes:        forgedPoint(),
							StartTimeUnixNano: golden,
							TimeUnixNano:      golden + 1,
							Exemplars:         []*metricspb.Exemplar{{FilteredAttributes: forgedPoint()}},
						}},
					}}},
					{Name: "latency", Data: &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
						DataPoints: []*metricspb.HistogramDataPoint{{Attributes: forgedPoint(), StartTimeUnixNano: golden, TimeUnixNano: golden + 1}},
					}}},
					{Name: "queue", Data: &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{
						DataPoints: []*metricspb.NumberDataPoint{{TimeUnixNano: golden + 1}},
					}}},
				}}},
			}},
		})
		if err != nil {
			t.Fatal(err)
		}
		return f.sink.metrics[len(f.sink.metrics)-1].GetResourceMetrics()[0]
	}

	rm := export()
	if diff := cmp.Diff(boundedIdentity(actorA, "0"), attrMap(rm.GetResource().GetAttributes())); diff != "" {
		t.Errorf("resource attributes (-want +got):\n%s", diff)
	}
	metrics := rm.GetScopeMetrics()[0].GetMetrics()
	sum := metrics[0].GetSum().GetDataPoints()[0]
	hist := metrics[1].GetHistogram().GetDataPoints()[0]
	gauge := metrics[2].GetGauge().GetDataPoints()[0]
	kept := map[string]string{"route": "/x"}
	for level, got := range map[string][]*commonpb.KeyValue{
		"sum point":       sum.GetAttributes(),
		"exemplar":        sum.GetExemplars()[0].GetFilteredAttributes(),
		"histogram point": hist.GetAttributes(),
	} {
		if diff := cmp.Diff(kept, attrMap(got)); diff != "" {
			t.Errorf("%s attributes (-want +got):\n%s", level, diff)
		}
	}

	activation, received := uint64(activatedAt.UnixNano()), uint64(f.clock.UnixNano())
	for name, got := range map[string][2]uint64{
		"sum":       {sum.GetStartTimeUnixNano(), sum.GetTimeUnixNano()},
		"histogram": {hist.GetStartTimeUnixNano(), hist.GetTimeUnixNano()},
	} {
		if got != [2]uint64{activation, received} {
			t.Errorf("%s start..time = %v, want the activation and the receive time %v", name, got, [2]uint64{activation, received})
		}
	}
	if gauge.GetStartTimeUnixNano() != 0 || gauge.GetTimeUnixNano() != received {
		t.Errorf("gauge start..time = %d..%d, want 0..%d", gauge.GetStartTimeUnixNano(), gauge.GetTimeUnixNano(), received)
	}

	// The clock has not moved, and the next export still gets a later time.
	next := export().GetScopeMetrics()[0].GetMetrics()[0].GetSum().GetDataPoints()[0]
	if next.GetTimeUnixNano() <= received {
		t.Errorf("second export time = %d, want it after the first %d", next.GetTimeUnixNano(), received)
	}
}

func TestStampTimes(t *testing.T) {
	act := uint64(activatedAt.UnixNano())
	e := &exporter{act: &activation{activatedAt: activatedAt}}
	for _, tc := range []struct {
		name      string
		start     uint64
		now       uint64
		wantStart uint64
	}{
		{name: "start within the activation is kept", start: act + 10, now: act + 20, wantStart: act + 10},
		{name: "start before the activation is raised to it", start: act - 10, now: act + 20, wantStart: act},
		{name: "start after the receive time is clamped to it", start: act + 30, now: act + 20, wantStart: act + 20},
		{name: "zero start stays zero", start: 0, now: act + 20, wantStart: 0},
	} {
		t.Run(tc.name, func(t *testing.T) {
			start, end := e.stampTimes(tc.start, tc.now)
			if start != tc.wantStart || end != tc.now {
				t.Errorf("stampTimes(%d, %d) = %d, %d, want %d, %d", tc.start, tc.now, start, end, tc.wantStart, tc.now)
			}
		})
	}
}

func logsRequest() *collogspb.ExportLogsServiceRequest {
	return &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		Resource: forgedResource(),
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{
			{TimeUnixNano: 11, ObservedTimeUnixNano: 12, Attributes: forged()},
			{TimeUnixNano: 13, ObservedTimeUnixNano: 14},
		}}},
	}}}
}

// post sends an OTLP/HTTP export and returns the status.
func post(t *testing.T, addr, path, contentType string, body []byte, gzipped bool) int {
	t.Helper()
	if gzipped {
		var buf bytes.Buffer
		zw := gzip.NewWriter(&buf)
		_, _ = zw.Write(body)
		_ = zw.Close()
		body = buf.Bytes()
	}
	req, err := http.NewRequestWithContext(t.Context(), http.MethodPost, "http://"+addr+path, bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Content-Type", contentType)
	if gzipped {
		req.Header.Set("Content-Encoding", "gzip")
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	_, _ = io.Copy(io.Discard, resp.Body)
	return resp.StatusCode
}

func TestLogsOverHTTP(t *testing.T) {
	f := newFixture(t, true)
	ep := f.host(t, actorA)
	f.clock = activatedAt.Add(3 * time.Second)
	body, err := proto.Marshal(logsRequest())
	if err != nil {
		t.Fatal(err)
	}

	for _, gzipped := range []bool{false, true} {
		if got := post(t, ep.addr, "/v1/logs", "application/x-protobuf", body, gzipped); got != http.StatusOK {
			t.Fatalf("POST /v1/logs gzip=%v = %d, want 200", gzipped, got)
		}
	}
	if len(f.sink.logs) != 2 {
		t.Fatalf("forwarded %d exports, want 2", len(f.sink.logs))
	}
	rl := f.sink.logs[1].GetResourceLogs()[0]
	if diff := cmp.Diff(fullIdentity(actorA, "0"), attrMap(rl.GetResource().GetAttributes())); diff != "" {
		t.Errorf("resource attributes (-want +got):\n%s", diff)
	}
	records := rl.GetScopeLogs()[0].GetLogRecords()
	if diff := cmp.Diff(map[string]string{"route": "/x"}, attrMap(records[0].GetAttributes())); diff != "" {
		t.Errorf("record attributes (-want +got):\n%s", diff)
	}
	for i, wantTime := range []uint64{11, 13} {
		if records[i].GetTimeUnixNano() != wantTime {
			t.Errorf("record %d time = %d, want %d as sent", i, records[i].GetTimeUnixNano(), wantTime)
		}
		if got, want := records[i].GetObservedTimeUnixNano(), uint64(f.clock.UnixNano()); got != want {
			t.Errorf("record %d observed time = %d, want the receive time %d", i, got, want)
		}
	}
}

func TestHTTPRefusals(t *testing.T) {
	f := newFixture(t, true)
	ep := f.host(t, actorA)
	valid, err := proto.Marshal(logsRequest())
	if err != nil {
		t.Fatal(err)
	}
	// Compresses to a few kilobytes and inflates past the cap.
	bomb := make([]byte, maxRecvMsgSize+1)

	for _, tc := range []struct {
		name        string
		contentType string
		body        []byte
		gzipped     bool
		want        int
	}{
		{name: "JSON body", contentType: "application/json", body: []byte(`{}`), want: http.StatusUnsupportedMediaType},
		{name: "not protobuf", contentType: "application/x-protobuf", body: []byte{0xff}, want: http.StatusBadRequest},
		{name: "over the size cap", contentType: "application/x-protobuf", body: bomb, want: http.StatusRequestEntityTooLarge},
		{name: "over the size cap once inflated", contentType: "application/x-protobuf", body: bomb, gzipped: true, want: http.StatusRequestEntityTooLarge},
		{name: "valid", contentType: "application/x-protobuf", body: valid, want: http.StatusOK},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := post(t, ep.addr, "/v1/logs", tc.contentType, tc.body, tc.gzipped); got != tc.want {
				t.Errorf("status = %d, want %d", got, tc.want)
			}
		})
	}
}

// counted returns the relay's two counters as "name signal error.type" to value.
func (f *fixture) counted(t *testing.T) map[string]int64 {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := f.reader.Collect(t.Context(), &rm); err != nil {
		t.Fatal(err)
	}
	got := map[string]int64{}
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			for _, dp := range m.Data.(metricdata.Sum[int64]).DataPoints {
				signal, _ := dp.Attributes.Value("ate.telemetry.signal")
				errType, _ := dp.Attributes.Value("error.type")
				template, _ := dp.Attributes.Value("ate.template.name")
				if template.AsString() != actorA.TemplateName {
					t.Errorf("%s ate.template.name = %q, want %q", m.Name, template.AsString(), actorA.TemplateName)
				}
				if dp.Attributes.HasValue("ate.actor.uid") {
					t.Errorf("%s carries ate.actor.uid", m.Name)
				}
				got[fmt.Sprintf("%s %s %s", m.Name, signal.AsString(), errType.AsString())] = dp.Value
			}
		}
	}
	return got
}

func TestRefusalsAreCounted(t *testing.T) {
	for _, tc := range []struct {
		name         string
		withUpstream bool
		// resources is how many resources each export carries. Zero sends one.
		resources  int
		code       codes.Code
		httpStatus int
	}{
		{name: "no socket to atelet", withUpstream: false, code: codes.Unavailable, httpStatus: http.StatusServiceUnavailable},
		{name: "more resources than the bound", withUpstream: true, resources: maxResources + 1, code: codes.InvalidArgument, httpStatus: http.StatusBadRequest},
		{name: "over the request budget", withUpstream: true, code: codes.ResourceExhausted, httpStatus: http.StatusTooManyRequests},
	} {
		t.Run(tc.name, func(t *testing.T) {
			f := newFixture(t, tc.withUpstream)
			ep := f.host(t, actorA)
			if tc.withUpstream && tc.resources == 0 {
				f.relay.actors[actorA.UID].limiter.SetLimit(0)
				f.relay.actors[actorA.UID].limiter.SetBurst(0)
			}
			req := logsRequest()
			for len(req.ResourceLogs) < tc.resources {
				req.ResourceLogs = append(req.ResourceLogs, &logspb.ResourceLogs{})
			}

			_, err := collogspb.NewLogsServiceClient(ep.conn).Export(t.Context(), req)
			if status.Code(err) != tc.code {
				t.Errorf("gRPC export error = %v, want %v", err, tc.code)
			}
			body, _ := proto.Marshal(req)
			if got := post(t, ep.addr, "/v1/logs", "application/x-protobuf", body, false); got != tc.httpStatus {
				t.Errorf("HTTP export status = %d, want %d", got, tc.httpStatus)
			}

			want := map[string]int64{
				"ate.actor.telemetry.requests logs " + tc.code.String(): 2,
				"ate.actor.telemetry.items logs " + tc.code.String():    4,
			}
			if diff := cmp.Diff(want, f.counted(t)); diff != "" {
				t.Errorf("counters (-want +got):\n%s", diff)
			}
		})
	}
}

func TestBudgetIsPerActor(t *testing.T) {
	f := newFixture(t, true)
	noisy, quiet := f.host(t, actorA), f.host(t, actorB)
	f.relay.actors[actorA.UID].limiter = rate.NewLimiter(0, 1)

	client := collogspb.NewLogsServiceClient(noisy.conn)
	if _, err := client.Export(t.Context(), logsRequest()); err != nil {
		t.Fatalf("first export within the burst: %v", err)
	}
	if _, err := client.Export(t.Context(), logsRequest()); status.Code(err) != codes.ResourceExhausted {
		t.Errorf("export past the burst = %v, want ResourceExhausted", err)
	}
	// Compressed, as OTEL_EXPORTER_OTLP_COMPRESSION=gzip makes an SDK send.
	if _, err := collogspb.NewLogsServiceClient(quiet.conn).Export(t.Context(), logsRequest(), grpc.UseCompressor("gzip")); err != nil {
		t.Errorf("the other actor's export: %v", err)
	}
}

func TestSlots(t *testing.T) {
	f := newFixture(t, true)
	slot := func(uid string) int { return f.relay.actors[uid].slot }
	register := func(uid string) error {
		return f.relay.Register(resources.ActorAttribution{UID: uid})
	}

	for i, uid := range []string{"a", "b", "c", "d"} {
		if err := register(uid); err != nil {
			t.Fatal(err)
		}
		if slot(uid) != i {
			t.Errorf("actor %s slot = %d, want %d", uid, slot(uid), i)
		}
	}
	if err := register("e"); err == nil {
		t.Error("Register on a full worker succeeded, want an error")
	}

	// A re-host keeps the slot and starts a new activation.
	f.clock = activatedAt.Add(time.Minute)
	if err := register("b"); err != nil {
		t.Fatal(err)
	}
	if slot("b") != 1 || !f.relay.actors["b"].activatedAt.Equal(f.clock) {
		t.Errorf("re-registered actor b = slot %d activated %v, want slot 1 activated %v", slot("b"), f.relay.actors["b"].activatedAt, f.clock)
	}

	// The lowest free slot goes to the next actor.
	f.relay.Release("c")
	f.relay.Release("a")
	f.relay.Release("a")
	if err := register("e"); err != nil {
		t.Fatal(err)
	}
	if slot("e") != 0 {
		t.Errorf("actor e slot = %d, want the freed slot 0", slot("e"))
	}
	if _, err := f.relay.Bind("c"); err == nil {
		t.Error("Bind of a released actor succeeded, want an error")
	}
}

func TestListenerIsTheIdentity(t *testing.T) {
	f := newFixture(t, true)
	a, b := f.host(t, actorA), f.host(t, actorB)

	// Both send the same payload, each claiming to be the other's victim.
	for _, ep := range []endpoint{a, b} {
		if _, err := collogspb.NewLogsServiceClient(ep.conn).Export(t.Context(), logsRequest()); err != nil {
			t.Fatal(err)
		}
	}
	for i, want := range []map[string]string{fullIdentity(actorA, "0"), fullIdentity(actorB, "1")} {
		got := attrMap(f.sink.logs[i].GetResourceLogs()[0].GetResource().GetAttributes())
		if diff := cmp.Diff(want, got); diff != "" {
			t.Errorf("export %d resource attributes (-want +got):\n%s", i, diff)
		}
	}
}

// TestClosingTheListenerClosesItsConnections holds a gRPC and an HTTP/1.1
// connection open across the actor's teardown. Neither may reach the actor
// that takes the slot next.
func TestClosingTheListenerClosesItsConnections(t *testing.T) {
	f := newFixture(t, true)
	a := f.host(t, actorA)
	client := collogspb.NewLogsServiceClient(a.conn)
	if _, err := client.Export(t.Context(), logsRequest()); err != nil {
		t.Fatal(err)
	}
	raw, err := net.Dial("tcp", a.addr)
	if err != nil {
		t.Fatal(err)
	}
	defer raw.Close()

	// Teardown: the session closes the listener, then the worker frees the slot.
	_ = a.listener.Close()
	<-a.served
	f.relay.Release(actorA.UID)
	b := f.host(t, actorB)
	if got := f.relay.actors[actorB.UID].slot; got != 0 {
		t.Fatalf("actor B slot = %d, want actor A's slot 0", got)
	}

	if _, err := client.Export(t.Context(), logsRequest()); status.Code(err) != codes.Unavailable {
		t.Errorf("export on the old gRPC connection = %v, want Unavailable", err)
	}
	_ = raw.SetReadDeadline(time.Now().Add(10 * time.Second))
	// EOF or a reset, depending on the platform.
	var netErr net.Error
	if _, err := raw.Read(make([]byte, 1)); err == nil || (errors.As(err, &netErr) && netErr.Timeout()) {
		t.Errorf("read on the old idle connection = %v, want it closed", err)
	}
	if _, err := collogspb.NewLogsServiceClient(b.conn).Export(t.Context(), logsRequest()); err != nil {
		t.Fatal(err)
	}
	if len(f.sink.logs) != 2 {
		t.Fatalf("forwarded %d exports, want 2", len(f.sink.logs))
	}
	got := attrMap(f.sink.logs[1].GetResourceLogs()[0].GetResource().GetAttributes())
	if got["ate.actor.uid"] != actorB.UID {
		t.Errorf("ate.actor.uid after reuse = %q, want %q", got["ate.actor.uid"], actorB.UID)
	}
}
