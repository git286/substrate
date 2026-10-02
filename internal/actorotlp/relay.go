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

// Package actorotlp is the actor half of the OTLP relay: an OTLP endpoint that
// ateom serves to each actor from inside the actor's own sandbox network, and
// that attributes what arrives to that actor before forwarding it to atelet.
//
// The problem it solves is that an actor cannot be trusted to identify its own
// telemetry, and after a golden snapshot it cannot even do so by accident:
// whatever identity its SDK computed at build time is frozen into every actor
// restored from the snapshot, so all of a template's actors report the same
// instance and the same counter baseline. Attribution therefore belongs to the
// channel, not the payload, which is the rule the log pipeline already applies
// (see actorlog) and which Prometheus applies when the scraper, not the target,
// assigns the instance label.
//
// The endpoint is a constant address, the sandbox's gateway, on the standard
// OTLP ports. Constants are safe to freeze into a golden snapshot; what answers
// at the address is per activation. Each actor has its own network namespace,
// so a listener opened inside it can be reached by that actor alone: the
// listener is the identity, and no lookup by address or port is needed.
//
// Per export the relay strips every attribute the actor claimed under the
// platform's namespaces, stamps the identity ateom knows from the activation,
// rewrites metric timestamps the snapshot froze, and forwards over the unix
// socket atelet serves. Metrics get the bounded writer identity only, worker
// pod plus slot; traces and logs get the full actor identity as well. There is
// no fallback to the pod network: the point of the socket is that the worker
// pod needs none.
package actorotlp

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	"golang.org/x/net/http2"
	"golang.org/x/net/http2/h2c"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/resources"
)

const (
	// GRPCPort and HTTPPort are the standard OTLP ports. Both are served, and
	// each speaks both protocols: the environment points every actor at
	// GRPCPort, and an SDK that only speaks HTTP/protobuf, or that honors the
	// endpoint variable but not the protocol one, posts HTTP there. A port that
	// answered only one protocol would be a silent black hole for the other.
	GRPCPort uint16 = 4317
	HTTPPort uint16 = 4318

	// maxRecvMsgSize bounds one export. The OTel SDKs' batch processors emit
	// far smaller messages; the bound is against a sandbox that does not.
	maxRecvMsgSize = 4 << 20

	// upstreamTimeout bounds the forward to atelet, so a stalled node-local
	// relay cannot pin an actor's exporter goroutine indefinitely.
	upstreamTimeout = 15 * time.Second

	// Per-actor request budget. Every actor on the worker shares one socket to
	// atelet and one connection from atelet to the collector, so one noisy actor
	// must be turned away before it delays its neighbors or Substrate's own
	// telemetry. The SDK defaults export each signal every few seconds; the
	// budget leaves an order of magnitude of headroom over that.
	requestsPerSecond = 20
	requestBurst      = 60

	scopeName = "github.com/agent-substrate/substrate/internal/actorotlp"
)

// Worker is the identity of the worker pod, stamped as the host facts of every
// relayed signal.
type Worker struct {
	PodUID       string
	PodName      string
	PodNamespace string
	NodeName     string
}

// WorkerFromEnv reads the pod name, namespace, and node from the downward API
// variables atecontroller sets on worker pods. The UID arrives as a flag, so
// the caller supplies it.
func WorkerFromEnv(podUID string) Worker {
	return Worker{
		PodUID:       podUID,
		PodName:      os.Getenv("POD_NAME"),
		PodNamespace: os.Getenv("POD_NAMESPACE"),
		NodeName:     os.Getenv("NODE_NAME"),
	}
}

// Relay serves the actor OTLP endpoint for every actor a worker hosts and
// forwards over upstream, atelet's relay socket. A nil upstream serves the
// endpoint but refuses every export as Unavailable, so an actor's SDK reports
// the failure rather than exporting into nothing.
type Relay struct {
	upstream *grpc.ClientConn
	worker   Worker
	// now is the relay's clock, replaced in tests.
	now                func() time.Time
	grpcPort, httpPort uint16

	requests metric.Int64Counter

	mu     sync.Mutex
	actors map[string]*activation
	// slots[i] reports whether slot i is held. Bounded by the worker's actor
	// capacity, so (pod, slot) is a bounded metric label.
	slots []bool
}

// activation is one hosting of an actor, from Register to Release. It is the
// identity every connection bound while it lasts carries; a later activation
// of the same actor is a new one.
type activation struct {
	attribution resources.ActorAttribution
	slot        int
	activatedAt time.Time
	limiter     *rate.Limiter

	mu sync.Mutex
	// lastStamp is the newest timestamp this activation put on a metric
	// point, so points stay strictly ordered per stream however the clock and
	// the arrival order behave.
	lastStamp time.Time
}

// New builds a relay for a worker that hosts at most maxActors at once.
func New(upstream *grpc.ClientConn, worker Worker, maxActors int) (*Relay, error) {
	if maxActors <= 0 {
		return nil, fmt.Errorf("actorotlp: maxActors must be positive, got %d", maxActors)
	}
	requests, err := otel.Meter(scopeName).Int64Counter("ate.actor.telemetry.requests",
		metric.WithDescription("OTLP export requests actors sent to the relay, by signal and outcome."),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("actorotlp: while creating the requests counter: %w", err)
	}
	return &Relay{
		upstream: upstream,
		worker:   worker,
		now:      time.Now,
		grpcPort: GRPCPort,
		httpPort: HTTPPort,
		requests: requests,
		actors:   map[string]*activation{},
		slots:    make([]bool, maxActors),
	}, nil
}

// Register begins an activation: it takes a slot for the actor and records
// the activation time, which becomes the floor of every metric start time the
// actor reports. It must run before the actor's listeners serve, so no
// connection is accepted without an identity to bind to. Registering an actor
// that is already registered starts a fresh activation in the same slot, which
// is what a stale re-host is.
func (r *Relay) Register(a resources.ActorAttribution) error {
	if a.UID == "" {
		return errors.New("actorotlp: actor UID is required")
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	slot := -1
	if old, ok := r.actors[a.UID]; ok {
		slot = old.slot
	} else {
		for i, held := range r.slots {
			if !held {
				slot = i
				break
			}
		}
		if slot < 0 {
			return fmt.Errorf("actorotlp: no free slot among %d for actor %s", len(r.slots), a.UID)
		}
		r.slots[slot] = true
	}
	r.actors[a.UID] = &activation{
		attribution: a,
		slot:        slot,
		activatedAt: r.now(),
		limiter:     rate.NewLimiter(rate.Limit(requestsPerSecond), requestBurst),
	}
	return nil
}

// Release ends the actor's activation and frees its slot. The caller closes
// the actor's listeners first (the sandbox session does, on teardown), so no
// connection can still be bound to the released activation. Safe to repeat.
func (r *Relay) Release(actorUID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if act, ok := r.actors[actorUID]; ok {
		r.slots[act.slot] = false
		delete(r.actors, actorUID)
	}
}

// Slot reports the slot an actor holds, for callers that stamp the same
// identity elsewhere. ok is false while the actor is not registered.
func (r *Relay) Slot(actorUID string) (slot int, ok bool) {
	r.mu.Lock()
	defer r.mu.Unlock()
	act, ok := r.actors[actorUID]
	if !ok {
		return 0, false
	}
	return act.slot, true
}

// Ports implements ateomnet.SandboxService: the listeners each actor gets in
// its gateway namespace.
func (r *Relay) Ports() []uint16 { return []uint16{r.grpcPort, r.httpPort} }

// Bind implements ateomnet.SandboxService. It captures the actor's activation
// before its listeners start serving, so a connection accepted on them can
// never pick up a later activation's identity, and returns the function that
// serves one listener. Every listener speaks both OTLP protocols.
func (r *Relay) Bind(actorUID string) (func(context.Context, net.Listener) error, error) {
	r.mu.Lock()
	act, ok := r.actors[actorUID]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("actorotlp: actor %s is not registered", actorUID)
	}
	ex := &exporter{relay: r, act: act}
	return ex.serve, nil
}

// exporter is one activation's view of the relay: what the servers bound to
// its listeners call.
type exporter struct {
	relay *Relay
	act   *activation
}

// serve answers both OTLP protocols on one listener until l is closed or ctx
// ends. The two are told apart by what the client sends, which the protocols
// fix: a gRPC client opens with the HTTP/2 preface and marks every request
// application/grpc; an OTLP/HTTP client speaks HTTP/1.1, or HTTP/2 with
// application/x-protobuf. h2c does the preface detection, and the handler
// routes on the content type. gRPC requests go through the gRPC server's own
// ServeHTTP, which keeps its method dispatch and message size limit.
//
// When the listener closes, the server is closed too, so that connections
// already accepted are closed with it: closing a listener alone leaves them
// open, and an actor being torn down must not keep a channel into the next
// occupant of its slot.
func (e *exporter) serve(ctx context.Context, l net.Listener) error {
	grpcSrv := grpc.NewServer(grpc.MaxRecvMsgSize(maxRecvMsgSize))
	coltracepb.RegisterTraceServiceServer(grpcSrv, &traceService{ex: e})
	colmetricspb.RegisterMetricsServiceServer(grpcSrv, &metricsService{ex: e})
	collogspb.RegisterLogsServiceServer(grpcSrv, &logsService{ex: e})

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", e.httpHandler(func() proto.Message { return &coltracepb.ExportTraceServiceRequest{} },
		func(ctx context.Context, m proto.Message) (proto.Message, error) {
			return e.traces(ctx, m.(*coltracepb.ExportTraceServiceRequest))
		}))
	mux.HandleFunc("/v1/metrics", e.httpHandler(func() proto.Message { return &colmetricspb.ExportMetricsServiceRequest{} },
		func(ctx context.Context, m proto.Message) (proto.Message, error) {
			return e.metrics(ctx, m.(*colmetricspb.ExportMetricsServiceRequest))
		}))
	mux.HandleFunc("/v1/logs", e.httpHandler(func() proto.Message { return &collogspb.ExportLogsServiceRequest{} },
		func(ctx context.Context, m proto.Message) (proto.Message, error) {
			return e.logs(ctx, m.(*collogspb.ExportLogsServiceRequest))
		}))

	route := http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.ProtoMajor == 2 && strings.HasPrefix(req.Header.Get("Content-Type"), "application/grpc") {
			grpcSrv.ServeHTTP(w, req)
			return
		}
		mux.ServeHTTP(w, req)
	})
	srv := &http.Server{
		Handler:           h2c.NewHandler(route, &http2.Server{}),
		ReadHeaderTimeout: 10 * time.Second,
	}

	done := make(chan error, 1)
	go func() { done <- srv.Serve(l) }()
	select {
	case err := <-done:
		_ = srv.Close()
		grpcSrv.Stop()
		if errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
		_ = srv.Close()
		grpcSrv.Stop()
		<-done
		return nil
	}
}

// httpHandler adapts one signal's export to OTLP/HTTP: a POST with a protobuf
// body, optionally gzip-encoded, answered with a protobuf body and the HTTP
// status the gRPC code maps to. JSON bodies are refused rather than decoded:
// the spec permits both, but every SDK speaks protobuf, and a second decoder is
// a second surface to keep correct.
func (e *exporter) httpHandler(newReq func() proto.Message, export func(context.Context, proto.Message) (proto.Message, error)) http.HandlerFunc {
	return func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost {
			w.Header().Set("Allow", http.MethodPost)
			http.Error(w, "OTLP/HTTP exports are POSTs", http.StatusMethodNotAllowed)
			return
		}
		if mt, _, err := mime.ParseMediaType(req.Header.Get("Content-Type")); err != nil || mt != "application/x-protobuf" {
			http.Error(w, "the relay accepts application/x-protobuf bodies only", http.StatusUnsupportedMediaType)
			return
		}
		var body io.Reader = http.MaxBytesReader(w, req.Body, maxRecvMsgSize)
		if req.Header.Get("Content-Encoding") == "gzip" {
			gz, err := gzip.NewReader(body)
			if err != nil {
				http.Error(w, "invalid gzip body", http.StatusBadRequest)
				return
			}
			defer gz.Close()
			body = gz
		}
		raw, err := io.ReadAll(body)
		if err != nil {
			http.Error(w, "reading the export body: "+err.Error(), http.StatusRequestEntityTooLarge)
			return
		}
		msg := newReq()
		if err := proto.Unmarshal(raw, msg); err != nil {
			http.Error(w, "the export body is not the expected protobuf: "+err.Error(), http.StatusBadRequest)
			return
		}

		resp, err := export(req.Context(), msg)
		w.Header().Set("Content-Type", "application/x-protobuf")
		if err != nil {
			st := status.Convert(err)
			code := httpStatus(st.Code())
			if code == http.StatusTooManyRequests || code == http.StatusServiceUnavailable {
				w.Header().Set("Retry-After", "1")
			}
			out, _ := proto.Marshal(st.Proto())
			w.WriteHeader(code)
			_, _ = w.Write(out)
			return
		}
		out, err := proto.Marshal(resp)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		_, _ = w.Write(out)
	}
}

// httpStatus maps the gRPC codes the relay produces onto the statuses the OTLP
// HTTP spec names for them.
func httpStatus(code codes.Code) int {
	switch code {
	case codes.OK:
		return http.StatusOK
	case codes.ResourceExhausted:
		return http.StatusTooManyRequests
	case codes.Unavailable, codes.DeadlineExceeded:
		return http.StatusServiceUnavailable
	case codes.InvalidArgument:
		return http.StatusBadRequest
	case codes.PermissionDenied, codes.Unauthenticated:
		return http.StatusForbidden
	default:
		return http.StatusInternalServerError
	}
}

// admit applies the per-actor budget and the upstream check that every signal
// shares. The errors are gRPC statuses the SDKs know how to handle:
// ResourceExhausted and Unavailable are both retryable with backoff.
func (e *exporter) admit() error {
	if !e.act.limiter.Allow() {
		return status.Error(codes.ResourceExhausted, "the actor telemetry relay's per-actor budget is exhausted; export less often")
	}
	if e.relay.upstream == nil {
		return status.Error(codes.Unavailable, "the actor telemetry relay has no path to atelet; the worker started without the relay socket, so actor telemetry is being dropped")
	}
	return nil
}

// record counts one export by signal and outcome. error.type is the gRPC code
// on failure and absent on success, per the registry's reporting rule.
func (e *exporter) record(ctx context.Context, signal string, err error) {
	attrs := []attribute.KeyValue{
		ateattr.TelemetrySignalKey.String(signal),
		ateattr.TemplateAtespaceKey.String(e.act.attribution.TemplateAtespace),
		ateattr.TemplateNameKey.String(e.act.attribution.TemplateName),
	}
	if err != nil {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(status.Code(err).String()))
	}
	e.relay.requests.Add(ctx, 1, metric.WithAttributes(attrs...))
}

func (e *exporter) traces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (resp *coltracepb.ExportTraceServiceResponse, err error) {
	defer func() { e.record(ctx, ateattr.TelemetrySignalTraces, err) }()
	if err := e.admit(); err != nil {
		return nil, err
	}
	e.rewriteTraces(req)
	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()
	return coltracepb.NewTraceServiceClient(e.relay.upstream).Export(ctx, req)
}

func (e *exporter) metrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (resp *colmetricspb.ExportMetricsServiceResponse, err error) {
	defer func() { e.record(ctx, ateattr.TelemetrySignalMetrics, err) }()
	if err := e.admit(); err != nil {
		return nil, err
	}
	e.rewriteMetrics(req)
	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()
	return colmetricspb.NewMetricsServiceClient(e.relay.upstream).Export(ctx, req)
}

func (e *exporter) logs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (resp *collogspb.ExportLogsServiceResponse, err error) {
	defer func() { e.record(ctx, ateattr.TelemetrySignalLogs, err) }()
	if err := e.admit(); err != nil {
		return nil, err
	}
	e.rewriteLogs(req)
	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()
	return collogspb.NewLogsServiceClient(e.relay.upstream).Export(ctx, req)
}

// The OTLP services all declare a method named Export with different request
// types, so one type cannot implement more than one of them.

type traceService struct {
	coltracepb.UnimplementedTraceServiceServer
	ex *exporter
}

func (s *traceService) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	return s.ex.traces(ctx, req)
}

type metricsService struct {
	colmetricspb.UnimplementedMetricsServiceServer
	ex *exporter
}

func (s *metricsService) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	return s.ex.metrics(ctx, req)
}

type logsService struct {
	collogspb.UnimplementedLogsServiceServer
	ex *exporter
}

func (s *logsService) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	return s.ex.logs(ctx, req)
}

// instanceID is the writer identity of an activation: the worker pod and the
// slot, which together name a single writer at any moment and stay bounded by
// worker count times capacity.
func (r *Relay) instanceID(act *activation) string {
	return r.worker.PodUID + "/" + strconv.Itoa(act.slot)
}

// LogRelayStart is what ateom logs once at startup, so an operator reading
// the worker's log knows whether actor telemetry has a path out.
func (r *Relay) LogRelayStart(ctx context.Context) {
	if r.upstream == nil {
		slog.WarnContext(ctx, "Actor telemetry relay serving without a path to atelet: actor exports will be refused as Unavailable until the worker restarts with the relay socket")
		return
	}
	slog.InfoContext(ctx, "Actor telemetry relay ready", slog.String("pod", r.worker.PodName), slog.Int("slots", len(r.slots)))
}
