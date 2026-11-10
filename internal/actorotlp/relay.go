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

// Package actorotlp serves an OTLP endpoint to each actor from inside the
// actor's own sandbox network and forwards what arrives to atelet's relay
// socket.
//
// Each actor has its own network namespace, so a listener opened inside it is
// reachable by that actor alone, and the listener identifies the sender. Per
// export the relay drops the identity the actor claimed, stamps the identity
// the worker knows from the activation, and rewrites the timestamps a golden
// snapshot froze. Metrics get the worker pod and slot as their writer. Traces
// and logs also get the actor's identity.
//
// The relay forwards over atelet's socket only. Without the socket it answers
// every export Unavailable.
package actorotlp

import (
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"io"
	"mime"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	"golang.org/x/time/rate"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	// Registers the gzip compressor: OTEL_EXPORTER_OTLP_COMPRESSION=gzip is common.
	_ "google.golang.org/grpc/encoding/gzip"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/proto"

	"go.ate.dev/substrate/internal/ateattr"
	"go.ate.dev/substrate/internal/resources"
)

const (
	// maxRecvMsgSize bounds one export.
	maxRecvMsgSize = 4 << 20

	// maxResources bounds the resources in one export. Each gets the stamped
	// identity, so a request of many empty resources would grow a
	// hundredfold. An SDK sends one.
	maxResources = 64

	// upstreamTimeout bounds the forward to atelet.
	upstreamTimeout = 15 * time.Second

	// Every actor on the worker shares one socket to atelet, so each actor has
	// a request budget. The SDKs export each signal every few seconds.
	requestsPerSecond = 20
	requestBurst      = 60
)

// Worker identifies the worker pod.
type Worker struct {
	PodUID       string
	PodName      string
	PodNamespace string
	NodeName     string
}

// WorkerFromEnv reads the pod name, namespace, and node from the downward API
// variables atecontroller sets on worker pods.
func WorkerFromEnv(podUID string) Worker {
	return Worker{
		PodUID:       podUID,
		PodName:      os.Getenv("POD_NAME"),
		PodNamespace: os.Getenv("POD_NAMESPACE"),
		NodeName:     os.Getenv("NODE_NAME"),
	}
}

// Relay serves the OTLP endpoint of every actor a worker hosts.
type Relay struct {
	// upstream is atelet's relay socket. Nil refuses every export.
	upstream *grpc.ClientConn
	worker   Worker
	now      func() time.Time
	requests metric.Int64Counter
	items    metric.Int64Counter

	mu     sync.Mutex
	actors map[string]*activation
	// slots[i] reports whether slot i is held.
	slots []bool
}

// activation is one hosting of an actor, from Register to Release.
type activation struct {
	attribution resources.ActorAttribution
	slot        int
	activatedAt time.Time
	limiter     *rate.Limiter

	mu sync.Mutex
	// lastStamp is the newest timestamp put on this activation's metric points.
	lastStamp time.Time
}

// New builds a relay for a worker that hosts at most maxActors at once.
func New(upstream *grpc.ClientConn, worker Worker, maxActors int, meter metric.Meter) (*Relay, error) {
	requests, err := meter.Int64Counter("ate.actor.telemetry.requests",
		metric.WithDescription("OTLP export requests actors sent to the worker's relay."),
		metric.WithUnit("{request}"))
	if err != nil {
		return nil, fmt.Errorf("while creating ate.actor.telemetry.requests: %w", err)
	}
	items, err := meter.Int64Counter("ate.actor.telemetry.items",
		metric.WithDescription("Spans, data points, and log records in the export requests actors sent to the worker's relay."),
		metric.WithUnit("{item}"))
	if err != nil {
		return nil, fmt.Errorf("while creating ate.actor.telemetry.items: %w", err)
	}
	return &Relay{
		upstream: upstream,
		worker:   worker,
		now:      time.Now,
		requests: requests,
		items:    items,
		actors:   map[string]*activation{},
		slots:    make([]bool, maxActors),
	}, nil
}

// Register starts an activation and gives the actor the lowest free slot. Call
// it before the actor's listener serves. An actor that is already registered
// keeps its slot.
func (r *Relay) Register(a resources.ActorAttribution) error {
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
			return fmt.Errorf("no free telemetry slot among %d for actor %s", len(r.slots), a.UID)
		}
		r.slots[slot] = true
	}
	r.actors[a.UID] = &activation{
		attribution: a,
		slot:        slot,
		activatedAt: r.now(),
		limiter:     rate.NewLimiter(requestsPerSecond, requestBurst),
	}
	return nil
}

// Release ends the actor's activation and frees its slot. Close the actor's
// listener first. Repeated calls are safe.
func (r *Relay) Release(actorUID string) {
	r.mu.Lock()
	defer r.mu.Unlock()
	if act, ok := r.actors[actorUID]; ok {
		r.slots[act.slot] = false
		delete(r.actors, actorUID)
	}
}

// Ports are the ports the relay serves in each actor's gateway namespace.
func (r *Relay) Ports() []uint16 { return []uint16{Port} }

// Bind returns the function that serves the actor's listener. It captures the
// activation, so a connection accepted on the listener keeps this identity.
func (r *Relay) Bind(actorUID string) (func(context.Context, net.Listener) error, error) {
	r.mu.Lock()
	act, ok := r.actors[actorUID]
	r.mu.Unlock()
	if !ok {
		return nil, fmt.Errorf("actor %s is not registered with the telemetry relay", actorUID)
	}
	return (&exporter{relay: r, act: act}).serve, nil
}

// exporter serves one activation.
type exporter struct {
	relay *Relay
	act   *activation
}

// serve answers OTLP/gRPC and OTLP/HTTP on l until l closes or ctx ends. A gRPC
// client sends the HTTP/2 preface and an application/grpc content type.
// Everything else is served as OTLP/HTTP. Returning closes the connections
// already accepted: closing the listener alone would leave them open into the
// next actor in the slot.
func (e *exporter) serve(ctx context.Context, l net.Listener) error {
	grpcSrv := grpc.NewServer(grpc.MaxRecvMsgSize(maxRecvMsgSize))
	coltracepb.RegisterTraceServiceServer(grpcSrv, &traceService{ex: e})
	colmetricspb.RegisterMetricsServiceServer(grpcSrv, &metricsService{ex: e})
	collogspb.RegisterLogsServiceServer(grpcSrv, &logsService{ex: e})

	mux := http.NewServeMux()
	mux.HandleFunc("/v1/traces", httpHandler(e.traces))
	mux.HandleFunc("/v1/metrics", httpHandler(e.metrics))
	mux.HandleFunc("/v1/logs", httpHandler(e.logs))

	protocols := new(http.Protocols)
	protocols.SetHTTP1(true)
	protocols.SetUnencryptedHTTP2(true)
	srv := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
			if req.ProtoMajor == 2 && strings.HasPrefix(req.Header.Get("Content-Type"), "application/grpc") {
				grpcSrv.ServeHTTP(w, req)
				return
			}
			mux.ServeHTTP(w, req)
		}),
		Protocols:         protocols,
		ReadHeaderTimeout: 10 * time.Second,
	}

	done := make(chan error, 1)
	go func() { done <- srv.Serve(l) }()
	var err error
	select {
	case err = <-done:
	case <-ctx.Done():
	}
	_ = srv.Close()
	grpcSrv.Stop()
	if errors.Is(err, net.ErrClosed) || errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}

// httpHandler serves one signal over OTLP/HTTP with protobuf bodies. JSON
// bodies are refused.
func httpHandler[Req any, PReq interface {
	*Req
	proto.Message
}, Resp proto.Message](export func(context.Context, PReq) (Resp, error)) http.HandlerFunc {
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
		// One byte over the cap tells an oversized body from one at the cap.
		raw, err := io.ReadAll(io.LimitReader(body, maxRecvMsgSize+1))
		if err != nil || len(raw) > maxRecvMsgSize {
			http.Error(w, "the export body is unreadable or over the size cap", http.StatusRequestEntityTooLarge)
			return
		}
		msg := PReq(new(Req))
		if err := proto.Unmarshal(raw, msg); err != nil {
			http.Error(w, "the export body is not the expected protobuf", http.StatusBadRequest)
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

// httpStatus maps a gRPC code to the status the OTLP/HTTP spec gives it.
func httpStatus(code codes.Code) int {
	switch code {
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

// admit applies the actor's request budget and the resource bound. The SDKs
// retry ResourceExhausted and Unavailable with backoff.
func (e *exporter) admit(resources int) error {
	if resources > maxResources {
		return status.Errorf(codes.InvalidArgument, "the export has %d resources, more than the %d the relay accepts", resources, maxResources)
	}
	if !e.act.limiter.Allow() {
		return status.Error(codes.ResourceExhausted, "this actor is over its telemetry request budget")
	}
	if e.relay.upstream == nil {
		return status.Error(codes.Unavailable, "the worker started without atelet's OTLP relay socket, so actor telemetry has no path out")
	}
	return nil
}

// record counts one export and the items it carried. error.type is the gRPC
// code, absent on success. The template is the label: actor identity is not a
// metric attribute.
func (e *exporter) record(ctx context.Context, signal string, items int64, err error) {
	attrs := []attribute.KeyValue{
		ateattr.TelemetrySignalKey.String(signal),
		ateattr.TemplateAtespaceKey.String(e.act.attribution.TemplateAtespace),
		ateattr.TemplateNameKey.String(e.act.attribution.TemplateName),
	}
	if err != nil {
		attrs = append(attrs, ateattr.ErrorTypeKey.String(status.Code(err).String()))
	}
	set := metric.WithAttributes(attrs...)
	e.relay.requests.Add(ctx, 1, set)
	e.relay.items.Add(ctx, items, set)
}

func (e *exporter) traces(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (_ *coltracepb.ExportTraceServiceResponse, err error) {
	defer func(spans int64) { e.record(ctx, ateattr.TelemetrySignalTraces, spans, err) }(countSpans(req))
	if err := e.admit(len(req.GetResourceSpans())); err != nil {
		return nil, err
	}
	e.rewriteTraces(req)
	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()
	return coltracepb.NewTraceServiceClient(e.relay.upstream).Export(ctx, req)
}

func (e *exporter) metrics(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (_ *colmetricspb.ExportMetricsServiceResponse, err error) {
	defer func(points int64) { e.record(ctx, ateattr.TelemetrySignalMetrics, points, err) }(countPoints(req))
	// A refused export takes no timestamp.
	if err := e.admit(len(req.GetResourceMetrics())); err != nil {
		return nil, err
	}
	e.rewriteMetrics(req)
	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()
	return colmetricspb.NewMetricsServiceClient(e.relay.upstream).Export(ctx, req)
}

func (e *exporter) logs(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (_ *collogspb.ExportLogsServiceResponse, err error) {
	defer func(records int64) { e.record(ctx, ateattr.TelemetrySignalLogs, records, err) }(countLogRecords(req))
	if err := e.admit(len(req.GetResourceLogs())); err != nil {
		return nil, err
	}
	e.rewriteLogs(req)
	ctx, cancel := context.WithTimeout(ctx, upstreamTimeout)
	defer cancel()
	return collogspb.NewLogsServiceClient(e.relay.upstream).Export(ctx, req)
}

// The OTLP services each declare an Export method with a different request
// type, so each needs its own type.

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

// instanceID is the service.instance.id of an activation: the worker pod and
// the slot.
func (r *Relay) instanceID(act *activation) string {
	return r.worker.PodUID + "/" + strconv.Itoa(act.slot)
}
