// Package relay implements the per-actor telemetry relay that ateom runs
// inside each worker pod: it listens on the constant worker-side veth gateway
// address, stamps trusted identity onto everything that arrives, separates
// telemetry per actor, aggregates metrics to bounded cardinality, and forwards
// upstream.
//
// Attribution belongs to the channel, not the payload.
package relay

import (
	"context"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"

	"github.com/agent-substrate/ate-identity-prototype/internal/identity"
)

// Forwarder sends stamped telemetry upstream (cluster collector -> backend).
type Forwarder interface {
	ExportMetrics(context.Context, []*metricspb.ResourceMetrics) error
	ExportTraces(context.Context, []*tracepb.ResourceSpans) error
	ExportLogs(context.Context, []*logspb.ResourceLogs) error
}

// Config configures a relay.
type Config struct {
	Worker   identity.Worker
	Upstream Forwarder
	Interval time.Duration
	Logger   *slog.Logger

	// GaugePolicy declares, per metric name, how gauge datapoints from
	// different actors combine. Undeclared gauges are dropped, never guessed.
	GaugePolicy map[string]GaugeAggregation

	// MaxSeriesPerMetric caps distinct actor-supplied attribute sets per
	// metric per interval; the rest fold into one overflow series.
	MaxSeriesPerMetric int

	// MaxSeriesTotal caps accumulators held across all metrics for one
	// interval. MaxSeriesPerMetric bounds only what happens inside a fixed
	// (resource, metric name) pair, and the metric name and scope name are
	// the actor's to choose. Past this budget new series are refused and
	// counted; open ones keep accumulating.
	MaxSeriesTotal int

	// MaxExemplars caps exemplars carried per output series.
	MaxExemplars int
}

// Stats is the relay's own telemetry — Substrate telemetry, and so unaffected
// by the problem this design fixes. Silently failing exports would be worse
// than the status quo, so every refusal is counted and attributable.
type Stats struct {
	MetricRequests       int64
	TraceRequests        int64
	LogRequests          int64
	DatapointsAccepted   int64
	DatapointsRejected   map[string]int64
	SpansForwarded       int64
	LogRecordsForwarded  int64
	AttributesOverridden int64
	ExportsWithoutActor  int64
	ForwardFailures      int64
}

// Relay is one worker pod's receiver.
type Relay struct {
	cfg Config
	reg identity.Registry
	agg *aggregator
	log *slog.Logger

	mu    sync.Mutex
	stats Stats

	// lastReported and lastSelfFlush track what the relay has already emitted
	// about itself, so self-telemetry is delta like everything else. Guarded
	// by flushMu (lastSelfFlush) and mu (lastReported).
	flushMu       sync.Mutex
	lastReported  Stats
	lastSelfFlush uint64

	stop     chan struct{}
	done     chan struct{}
	stopOnce sync.Once
}

// New builds a relay. It is inert until Start is called.
func New(cfg Config) *Relay {
	if cfg.Interval <= 0 {
		cfg.Interval = 10 * time.Second
	}
	if cfg.MaxSeriesPerMetric <= 1 {
		cfg.MaxSeriesPerMetric = 200
	}
	if cfg.MaxSeriesTotal <= 1 {
		cfg.MaxSeriesTotal = 2000
	}
	if cfg.MaxExemplars <= 0 {
		cfg.MaxExemplars = 5
	}
	if cfg.Logger == nil {
		cfg.Logger = slog.Default()
	}
	return &Relay{
		cfg:          cfg,
		agg:          newAggregator(cfg.MaxSeriesPerMetric, cfg.MaxSeriesTotal, cfg.MaxExemplars, cfg.GaugePolicy),
		log:          cfg.Logger,
		stats:        Stats{DatapointsRejected: map[string]int64{}},
		lastReported: Stats{DatapointsRejected: map[string]int64{}},
		stop:         make(chan struct{}),
		done:         make(chan struct{}),
	}
}

// Activate records the actor now running in this worker pod. In Substrate this
// is called from ateom's RunWorkload / RestoreWorkload handlers, which already
// carry the full identity.
func (r *Relay) Activate(a identity.Actor) { r.reg.Activate(a) }

// Deactivate clears it (checkpoint, or workload exit).
//
// It flushes first, and must: the relay buffers up to one aggregation interval
// in memory, and delta does not self-heal across a lost export. Whatever the
// actor emitted in the current window would otherwise die with the activation.
// In Substrate this is the CheckpointWorkload path — the same place #450's
// PreSuspend hook belongs.
func (r *Relay) Deactivate() {
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	r.Flush(ctx)
	r.reg.Deactivate()
}

// Register attaches the OTLP receiver services to a gRPC server.
func (r *Relay) Register(gs *grpc.Server) {
	colmetricspb.RegisterMetricsServiceServer(gs, &metricsService{r: r})
	coltracepb.RegisterTraceServiceServer(gs, &traceService{r: r})
	collogspb.RegisterLogsServiceServer(gs, &logsService{r: r})
}

// Start begins the aggregation interval loop.
func (r *Relay) Start() {
	go func() {
		defer close(r.done)
		t := time.NewTicker(r.cfg.Interval)
		defer t.Stop()
		for {
			select {
			case <-r.stop:
				r.Flush(context.Background())
				return
			case <-t.C:
				r.Flush(context.Background())
			}
		}
	}()
}

// Close stops the loop after one final flush. Idempotent.
func (r *Relay) Close() {
	r.stopOnce.Do(func() { close(r.stop) })
	<-r.done
}

// Flush drains the current aggregation interval and forwards it.
func (r *Relay) Flush(ctx context.Context) {
	r.flushMu.Lock()
	defer r.flushMu.Unlock()
	rms := r.agg.drain()
	rms = append(rms, r.selfMetrics()...)
	if len(rms) == 0 {
		return
	}
	if err := r.cfg.Upstream.ExportMetrics(ctx, rms); err != nil {
		// Delta is lossier than cumulative: a failed export is gone, it does
		// not self-heal on the next one. Count it loudly.
		r.bump(func(s *Stats) { s.ForwardFailures++ })
		r.log.Error("forwarding actor metrics upstream failed", "err", err)
	}
}

// Stats returns a snapshot.
func (r *Relay) Stats() Stats {
	r.mu.Lock()
	defer r.mu.Unlock()
	out := r.stats
	out.DatapointsRejected = map[string]int64{}
	for k, v := range r.stats.DatapointsRejected {
		out.DatapointsRejected[k] = v
	}
	return out
}

func (r *Relay) bump(fn func(*Stats)) {
	r.mu.Lock()
	defer r.mu.Unlock()
	fn(&r.stats)
}

func (r *Relay) reject(reason string, n int64) {
	r.bump(func(s *Stats) { s.DatapointsRejected[reason] += n })
}

// ---------------------------------------------------------------- metrics

type metricsService struct {
	colmetricspb.UnimplementedMetricsServiceServer
	r *Relay
}

func (s *metricsService) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	r := s.r
	r.bump(func(st *Stats) { st.MetricRequests++ })

	actor, ok := r.reg.Current()
	if !ok {
		r.bump(func(st *Stats) { st.ExportsWithoutActor++ })
		return nil, status.Error(codes.FailedPrecondition, "ateom relay: no actor is activated on this worker")
	}

	bounded := boundedIdentityAttrs(actor)
	var rejected int64
	var reasons []string
	note := func(reason, metric string, n int64) {
		rejected += n
		r.reject(reason, n)
		reasons = append(reasons, fmt.Sprintf("%s (%s x%d)", metric, reason, n))
	}

	for _, rm := range req.GetResourceMetrics() {
		resAttrs, overridden := r.metricResource(rm.GetResource(), bounded)
		if overridden > 0 {
			r.bump(func(st *Stats) { st.AttributesOverridden += int64(overridden) })
		}
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				// Identity can be forged per datapoint, not just per
				// resource. Strip there too, and count it.
				if n := sanitizeDatapointAttrs(m); n > 0 {
					r.bump(func(st *Stats) { st.AttributesOverridden += int64(n) })
				}
				switch data := m.GetData().(type) {
				case *metricspb.Metric_Sum:
					if data.Sum.GetAggregationTemporality() != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA {
						// Cumulative cannot be merged across workers without
						// per-actor history, which is the original bug one
						// layer up. Refuse loudly and attributably rather
						// than silently produce a wrong number.
						n := int64(len(data.Sum.GetDataPoints()))
						note("cumulative_sum", m.GetName(), n)
						r.log.Warn("rejected cumulative sum from actor",
							"metric", m.GetName(), "actor_uid", actor.UID, "actor_name", actor.Name,
							"atespace", actor.Atespace, "datapoints", n)
						continue
					}
					for _, dp := range data.Sum.GetDataPoints() {
						if reason := r.agg.addSum(resAttrs, sm.GetScope(), m, data.Sum.GetIsMonotonic(), dp); reason != "" {
							note(reason, m.GetName(), 1)
							continue
						}
						r.bump(func(st *Stats) { st.DatapointsAccepted++ })
					}
				case *metricspb.Metric_Gauge:
					for _, dp := range data.Gauge.GetDataPoints() {
						if reason := r.agg.addGauge(resAttrs, sm.GetScope(), m, dp); reason != "" {
							note(reason, m.GetName(), 1)
							continue
						}
						r.bump(func(st *Stats) { st.DatapointsAccepted++ })
					}
				case *metricspb.Metric_Histogram:
					if data.Histogram.GetAggregationTemporality() != metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA {
						n := int64(len(data.Histogram.GetDataPoints()))
						note("cumulative_histogram", m.GetName(), n)
						continue
					}
					for _, dp := range data.Histogram.GetDataPoints() {
						if reason := r.agg.addHistogram(resAttrs, sm.GetScope(), m, dp); reason != "" {
							note(reason, m.GetName(), 1)
							continue
						}
						r.bump(func(st *Stats) { st.DatapointsAccepted++ })
					}
				default:
					note("unsupported_metric_type", m.GetName(), 1)
				}
			}
		}
	}

	resp := &colmetricspb.ExportMetricsServiceResponse{}
	if rejected > 0 {
		// OTLP partial success: loud (stock SDKs log it) without discarding
		// the whole batch because one instrument is wrong.
		resp.PartialSuccess = &colmetricspb.ExportMetricsPartialSuccess{
			RejectedDataPoints: rejected,
			ErrorMessage: fmt.Sprintf("ateom relay rejected datapoints for actor %s/%s: %s",
				actor.Atespace, actor.Name, strings.Join(reasons, "; ")),
		}
	}
	return resp, nil
}

// sanitizeDatapointAttrs removes platform-owned keys from every datapoint of a
// metric, returning how many it removed.
func sanitizeDatapointAttrs(m *metricspb.Metric) int {
	removed := 0
	strip := func(in []*commonpb.KeyValue) []*commonpb.KeyValue {
		out, n := stripReserved(in)
		removed += n
		return out
	}
	switch data := m.GetData().(type) {
	case *metricspb.Metric_Sum:
		for _, dp := range data.Sum.GetDataPoints() {
			dp.Attributes = strip(dp.GetAttributes())
		}
	case *metricspb.Metric_Gauge:
		for _, dp := range data.Gauge.GetDataPoints() {
			dp.Attributes = strip(dp.GetAttributes())
		}
	case *metricspb.Metric_Histogram:
		for _, dp := range data.Histogram.GetDataPoints() {
			dp.Attributes = strip(dp.GetAttributes())
		}
	}
	return removed
}

// metricResource builds the forwarded metric resource. Nothing the actor sent
// survives it: every attribute is derived from the activation RPC.
//
// An earlier version allow-listed a few of the actor's own resource attributes
// (service.name, telemetry.sdk.*) on the theory that they are per-template
// constants. They are not: the allow-list filters keys, and the *values* are
// still the actor's to choose, so one actor varying service.name multiplied
// every series it emitted. The resource is part of the series key, and the
// series key must be built entirely from facts the platform controls — same
// reasoning that keeps host facts out (see identity.KeyWorkerPod).
//
// service.name is therefore pinned to the template, which is what the metric
// actually describes now that per-actor identity is gone. Nothing is lost: the
// actor's own resource still reaches traces and logs verbatim, and the join key
// across all three signals is the ate.dev/* set, not service.name.
func (r *Relay) metricResource(res *resourcepb.Resource, bounded []*commonpb.KeyValue) ([]*commonpb.KeyValue, int) {
	overridden := 0
	for _, kv := range res.GetAttributes() {
		if reserved(kv.GetKey()) {
			overridden++
		}
	}
	return bounded, overridden
}

// ---------------------------------------------------------------- traces

type traceService struct {
	coltracepb.UnimplementedTraceServiceServer
	r *Relay
}

func (s *traceService) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	r := s.r
	r.bump(func(st *Stats) { st.TraceRequests++ })

	actor, ok := r.reg.Current()
	if !ok {
		r.bump(func(st *Stats) { st.ExportsWithoutActor++ })
		return nil, status.Error(codes.FailedPrecondition, "ateom relay: no actor is activated on this worker")
	}

	// A span is an event, not a series: full actor identity is kept, and so is
	// the host fact of where it ran.
	trusted := fullIdentityAttrs(actor, r.cfg.Worker)
	var spans int64
	for _, rs := range req.GetResourceSpans() {
		stamped, overridden := stampResource(rs.GetResource(), trusted)
		rs.Resource = stamped
		if overridden > 0 {
			r.bump(func(st *Stats) { st.AttributesOverridden += int64(overridden) })
		}
		for _, ss := range rs.GetScopeSpans() {
			spans += int64(len(ss.GetSpans()))
			for _, sp := range ss.GetSpans() {
				if attrs, removed := stripReserved(sp.GetAttributes()); removed > 0 {
					sp.Attributes = attrs
					r.bump(func(st *Stats) { st.AttributesOverridden += int64(removed) })
				}
			}
		}
	}
	if err := r.cfg.Upstream.ExportTraces(ctx, req.GetResourceSpans()); err != nil {
		r.bump(func(st *Stats) { st.ForwardFailures++ })
		return nil, status.Errorf(codes.Unavailable, "ateom relay: forwarding traces failed: %v", err)
	}
	r.bump(func(st *Stats) { st.SpansForwarded += spans })
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

// ---------------------------------------------------------------- logs

type logsService struct {
	collogspb.UnimplementedLogsServiceServer
	r *Relay
}

func (s *logsService) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	r := s.r
	r.bump(func(st *Stats) { st.LogRequests++ })

	actor, ok := r.reg.Current()
	if !ok {
		r.bump(func(st *Stats) { st.ExportsWithoutActor++ })
		return nil, status.Error(codes.FailedPrecondition, "ateom relay: no actor is activated on this worker")
	}
	trusted := fullIdentityAttrs(actor, r.cfg.Worker)
	var records int64
	for _, rl := range req.GetResourceLogs() {
		stamped, overridden := stampResource(rl.GetResource(), trusted)
		rl.Resource = stamped
		if overridden > 0 {
			r.bump(func(st *Stats) { st.AttributesOverridden += int64(overridden) })
		}
		for _, sl := range rl.GetScopeLogs() {
			records += int64(len(sl.GetLogRecords()))
		}
	}
	if err := r.cfg.Upstream.ExportLogs(ctx, req.GetResourceLogs()); err != nil {
		r.bump(func(st *Stats) { st.ForwardFailures++ })
		return nil, status.Errorf(codes.Unavailable, "ateom relay: forwarding logs failed: %v", err)
	}
	r.bump(func(st *Stats) { st.LogRecordsForwarded += records })
	return &collogspb.ExportLogsServiceResponse{}, nil
}
