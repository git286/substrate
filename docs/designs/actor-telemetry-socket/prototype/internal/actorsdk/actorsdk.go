// Package actorsdk is a stock-OTel-SDK workload: the actor side of the
// contract. It configures nothing per-actor — the endpoint is a constant, so
// freezing it in the golden snapshot freezes the right answer — and it emits
// no identity attributes of its own.
//
// It can also be told to misbehave (forge identity, use cumulative
// temporality) so the relay's guarantees can be tested rather than asserted.
package actorsdk

import (
	"context"
	"fmt"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
)

// Config configures the workload.
type Config struct {
	// Endpoint is host:port. In a real actor this is
	// 169.254.17.1:4317 — the worker-side veth gateway, a constant.
	Endpoint string

	ServiceName string

	// Interval is the periodic reader's export interval.
	Interval time.Duration

	// Cumulative makes the actor ignore the documented guidance and export
	// cumulative sums, so the relay's refusal can be tested.
	Cumulative bool

	// MetricsOnly skips the trace and log pipelines, for tests that only care
	// about metric cardinality.
	MetricsOnly bool

	// ForgedIdentity, if set, is stamped into the actor's own Resource under
	// the platform-owned keys — an actor trying to attribute its telemetry to
	// somebody else.
	ForgedIdentity string
}

// Actor is one running workload.
type Actor struct {
	cfg Config

	mp     *sdkmetric.MeterProvider
	tp     *sdktrace.TracerProvider
	lp     *sdklog.LoggerProvider
	tracer trace.Tracer
	logger otellog.Logger

	work     metric.Int64Counter
	dur      metric.Float64Histogram
	syncGa   metric.Int64Gauge
	queue    int64
	queueObs metric.Int64ObservableGauge
}

// Start builds the providers. This models everything an actor computes in
// main() — and therefore everything the golden snapshot freezes.
func Start(ctx context.Context, cfg Config) (*Actor, error) {
	if cfg.Interval <= 0 {
		cfg.Interval = time.Second
	}
	if cfg.ServiceName == "" {
		cfg.ServiceName = "demo-actor"
	}

	attrs := []attribute.KeyValue{
		attribute.String("service.name", cfg.ServiceName),
	}
	if cfg.ForgedIdentity != "" {
		// Exactly the shape of the mistake — or the attack — that §7.3 is
		// about: identity asserted by the payload.
		attrs = append(attrs,
			attribute.String("service.instance.id", cfg.ForgedIdentity),
			attribute.String("ate.dev/actor_uid", cfg.ForgedIdentity),
			attribute.String("ate.dev/actor_name", cfg.ForgedIdentity),
			attribute.String("ate.dev/actor_atespace", "victim-atespace"),
		)
	}
	res := resource.NewWithAttributes("", attrs...)

	temporality := deltaSelector
	if cfg.Cumulative {
		temporality = func(sdkmetric.InstrumentKind) metricdata.Temporality {
			return metricdata.CumulativeTemporality
		}
	}

	mexp, err := otlpmetricgrpc.New(ctx,
		otlpmetricgrpc.WithEndpoint(cfg.Endpoint),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithTemporalitySelector(temporality),
	)
	if err != nil {
		return nil, fmt.Errorf("metric exporter: %w", err)
	}
	a := &Actor{cfg: cfg}
	a.mp = sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(mexp, sdkmetric.WithInterval(cfg.Interval))),
	)

	if !cfg.MetricsOnly {
		texp, terr := otlptracegrpc.New(ctx,
			otlptracegrpc.WithEndpoint(cfg.Endpoint),
			otlptracegrpc.WithInsecure(),
		)
		if terr != nil {
			return nil, fmt.Errorf("trace exporter: %w", terr)
		}
		lexp, lerr := otlploggrpc.New(ctx,
			otlploggrpc.WithEndpoint(cfg.Endpoint),
			otlploggrpc.WithInsecure(),
		)
		if lerr != nil {
			return nil, fmt.Errorf("log exporter: %w", lerr)
		}
		a.tp = sdktrace.NewTracerProvider(
			sdktrace.WithResource(res),
			sdktrace.WithBatcher(texp, sdktrace.WithBatchTimeout(cfg.Interval)),
		)
		a.lp = sdklog.NewLoggerProvider(
			sdklog.WithResource(res),
			sdklog.WithProcessor(sdklog.NewBatchProcessor(lexp, sdklog.WithExportInterval(cfg.Interval))),
		)
	} else {
		a.tp = sdktrace.NewTracerProvider(sdktrace.WithResource(res),
			sdktrace.WithSampler(sdktrace.AlwaysSample()))
		a.lp = sdklog.NewLoggerProvider(sdklog.WithResource(res))
	}

	meter := a.mp.Meter("demo/actor")
	if a.work, err = meter.Int64Counter("work_items", metric.WithDescription("units of work completed")); err != nil {
		return nil, err
	}
	if a.dur, err = meter.Float64Histogram("work_duration_ms"); err != nil {
		return nil, err
	}
	// A sync gauge: inherits and repeats the golden's value until the actor
	// writes it again (lastvalue.go's TODO #3006).
	if a.syncGa, err = meter.Int64Gauge("cpu_utilization"); err != nil {
		return nil, err
	}
	// An observable gauge: clears on collect and re-runs against live state,
	// which is why §7.6 prefers it.
	if a.queueObs, err = meter.Int64ObservableGauge("queue_depth",
		metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
			o.Observe(a.queue)
			return nil
		})); err != nil {
		return nil, err
	}

	a.tracer = a.tp.Tracer("demo/actor")
	a.logger = a.lp.Logger("demo/actor")
	return a, nil
}

// DoWork performs n units of traced work, incrementing the counter inside a
// sampled span so the datapoint carries an exemplar.
func (a *Actor) DoWork(ctx context.Context, n int) {
	for i := 0; i < n; i++ {
		ctx, span := a.tracer.Start(ctx, "work")
		a.work.Add(ctx, 1, metric.WithAttributes(attribute.String("kind", "task")))
		a.dur.Record(ctx, 1.5)
		span.End()
	}
}

// AddWithAttrs increments the counter with caller-chosen attributes, for the
// cardinality tests.
func (a *Actor) AddWithAttrs(ctx context.Context, v int64, kv ...attribute.KeyValue) {
	a.work.Add(ctx, v, metric.WithAttributes(kv...))
}

// SetQueueDepth updates the observable gauge's backing state.
func (a *Actor) SetQueueDepth(v int64) { a.queue = v }

// SetCPU writes the sync gauge.
func (a *Actor) SetCPU(ctx context.Context, v int64) { a.syncGa.Record(ctx, v) }

// Log emits one log record.
func (a *Actor) Log(ctx context.Context, msg string) {
	var r otellog.Record
	r.SetTimestamp(time.Now())
	r.SetBody(attribute.StringValue(msg))
	a.logger.Emit(ctx, r)
}

// ForceFlush pushes everything buffered. This is what a PreSnapshot hook would
// call so the golden's accumulated deltas do not become a restored actor's
// opening balance.
func (a *Actor) ForceFlush(ctx context.Context) error {
	// Traces and logs first: a counter datapoint's exemplar points at a span,
	// so the span should already be on its way.
	if err := a.tp.ForceFlush(ctx); err != nil {
		return err
	}
	if err := a.lp.ForceFlush(ctx); err != nil {
		return err
	}
	return a.mp.ForceFlush(ctx)
}

// Shutdown stops the providers.
func (a *Actor) Shutdown(ctx context.Context) error {
	_ = a.mp.Shutdown(ctx)
	_ = a.tp.Shutdown(ctx)
	return a.lp.Shutdown(ctx)
}

// deltaSelector mirrors the SDK's DeltaTemporalitySelector, including the part
// that matters: gauges — and UpDownCounters, sync and async — are not covered
// by delta and stay cumulative.
func deltaSelector(k sdkmetric.InstrumentKind) metricdata.Temporality {
	switch k {
	case sdkmetric.InstrumentKindCounter,
		sdkmetric.InstrumentKindHistogram,
		sdkmetric.InstrumentKindObservableCounter:
		return metricdata.DeltaTemporality
	default:
		return metricdata.CumulativeTemporality
	}
}
