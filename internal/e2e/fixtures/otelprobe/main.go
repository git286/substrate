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

// Command otelprobe is an actor that exports OpenTelemetry traces, metrics,
// and logs the way an ordinary workload would: through the OTel Go SDK,
// configured by nothing but the OTEL_* environment the platform gives every
// actor container. It reports whether each export was acknowledged, so an
// e2e suite can assert the whole path, actor SDK to ateom's relay to atelet to
// the collector, without reaching the collector itself.
//
// It deliberately claims a forged identity in its resource. The relay must
// replace every bit of it; a suite with collector access asserts that too.
package main

import (
	"context"
	"encoding/json"
	"errors"
	"log"
	"net/http"
	"os"
	"sync"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploghttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetrichttp"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	otellog "go.opentelemetry.io/otel/log"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
)

// ForgedService is the service.name the probe claims. The relay replaces it
// with the template's name; a collector that ever shows it has been reached
// around the relay.
const ForgedService = "forged-service"

// tally counts one signal's export outcomes.
type tally struct {
	Exports   atomic.Int64
	Failures  atomic.Int64
	mu        sync.Mutex
	lastError string
}

func (t *tally) observe(err error) {
	if err == nil {
		t.Exports.Add(1)
		return
	}
	t.Failures.Add(1)
	t.mu.Lock()
	t.lastError = err.Error()
	t.mu.Unlock()
}

// Status is what /otel reports: one tally set per protocol. GRPC is the
// exporter family the platform environment asks for. HTTP is what an SDK that
// only speaks HTTP/protobuf, or that ignores OTEL_EXPORTER_OTLP_PROTOCOL,
// would do with the same OTEL_EXPORTER_OTLP_ENDPOINT: post to it directly.
// Both must be served at the one address the environment names.
type Status struct {
	Endpoint string         `json:"endpoint"`
	GRPC     ProtocolStatus `json:"grpc"`
	HTTP     ProtocolStatus `json:"http"`
}

// ProtocolStatus is the per-signal tally of one exporter family.
type ProtocolStatus struct {
	Traces  SignalStatus `json:"traces"`
	Metrics SignalStatus `json:"metrics"`
	Logs    SignalStatus `json:"logs"`
}

// SignalStatus is the export tally of one signal.
type SignalStatus struct {
	Exports   int64  `json:"exports"`
	Failures  int64  `json:"failures"`
	LastError string `json:"last_error,omitempty"`
}

func (t *tally) status() SignalStatus {
	t.mu.Lock()
	defer t.mu.Unlock()
	return SignalStatus{Exports: t.Exports.Load(), Failures: t.Failures.Load(), LastError: t.lastError}
}

// Wrappers that count what the SDK exporters return. Success means the far
// end acknowledged the batch: ateom forwarded it and atelet's collector
// accepted it, since the relay returns the upstream's answer.

type countingSpanExporter struct {
	sdktrace.SpanExporter
	t *tally
}

func (c *countingSpanExporter) ExportSpans(ctx context.Context, spans []sdktrace.ReadOnlySpan) error {
	err := c.SpanExporter.ExportSpans(ctx, spans)
	c.t.observe(err)
	return err
}

type countingMetricExporter struct {
	sdkmetric.Exporter
	t *tally
}

func (c *countingMetricExporter) Export(ctx context.Context, rm *metricdata.ResourceMetrics) error {
	err := c.Exporter.Export(ctx, rm)
	c.t.observe(err)
	return err
}

type countingLogExporter struct {
	sdklog.Exporter
	t *tally
}

func (c *countingLogExporter) Export(ctx context.Context, records []sdklog.Record) error {
	err := c.Exporter.Export(ctx, records)
	c.t.observe(err)
	return err
}

// tallies is one exporter family's counters.
type tallies struct{ traces, metrics, logs tally }

func (t *tallies) status() ProtocolStatus {
	return ProtocolStatus{Traces: t.traces.status(), Metrics: t.metrics.status(), Logs: t.logs.status()}
}

// start builds one exporter family's providers around the given exporters,
// and runs the tick loop that drives them. name distinguishes the metric so
// the two families are two series at the backend.
func start(ctx context.Context, name string, res *resource.Resource, t *tallies,
	spanExp sdktrace.SpanExporter, metricExp sdkmetric.Exporter, logExp sdklog.Exporter) (shutdown func()) {
	tp := sdktrace.NewTracerProvider(
		sdktrace.WithResource(res),
		sdktrace.WithBatcher(&countingSpanExporter{SpanExporter: spanExp, t: &t.traces}, sdktrace.WithBatchTimeout(time.Second)),
	)
	mp := sdkmetric.NewMeterProvider(
		sdkmetric.WithResource(res),
		sdkmetric.WithReader(sdkmetric.NewPeriodicReader(&countingMetricExporter{Exporter: metricExp, t: &t.metrics})),
	)
	lp := sdklog.NewLoggerProvider(
		sdklog.WithResource(res),
		sdklog.WithProcessor(sdklog.NewBatchProcessor(&countingLogExporter{Exporter: logExp, t: &t.logs}, sdklog.WithExportInterval(time.Second))),
	)

	tracer := tp.Tracer("otelprobe")
	ticks, err := mp.Meter("otelprobe").Int64Counter("otelprobe.ticks." + name)
	if err != nil {
		log.Fatalf("Int64Counter: %v", err)
	}
	logger := lp.Logger("otelprobe")

	go func() {
		for tick := 0; ; tick++ {
			_, span := tracer.Start(ctx, "tick")
			span.SetAttributes(attribute.Int("tick", tick), attribute.String("protocol", name), attribute.String("ate.actor.uid", "forged-on-span"))
			ticks.Add(ctx, 1)
			var rec otellog.Record
			rec.SetTimestamp(time.Now())
			rec.SetSeverity(otellog.SeverityInfo)
			rec.SetBody(attribute.StringValue("tick"))
			rec.AddAttributes(attribute.Int("tick", tick), attribute.String("protocol", name), attribute.String("ate.actor.uid", "forged-on-record"))
			logger.Emit(ctx, rec)
			span.End()
			time.Sleep(time.Second)
		}
	}()
	return func() {
		_ = tp.Shutdown(ctx)
		_ = mp.Shutdown(ctx)
		_ = lp.Shutdown(ctx)
	}
}

func main() {
	ctx := context.Background()
	var grpcTallies, httpTallies tallies

	// The forged identity: every key here is one the platform owns.
	res := resource.NewSchemaless(
		attribute.String("service.name", ForgedService),
		attribute.String("service.instance.id", "forged-instance"),
		attribute.String("ate.actor.uid", "forged-uid"),
		attribute.String("ate.atespace", "forged-atespace"),
		attribute.String("k8s.pod.name", "forged-pod"),
		attribute.String("probe.kept", "yes"),
	)

	// Endpoints, temporality, and export interval all come from the
	// environment. Nothing is configured in code, because a real workload
	// would not be. The gRPC family is what the environment asks for; the HTTP
	// family takes the same endpoint and posts to it, as an SDK that does not
	// speak gRPC would.
	spanGRPC, err := otlptracegrpc.New(ctx)
	if err != nil {
		log.Fatalf("otlptracegrpc.New: %v", err)
	}
	metricGRPC, err := otlpmetricgrpc.New(ctx)
	if err != nil {
		log.Fatalf("otlpmetricgrpc.New: %v", err)
	}
	logGRPC, err := otlploggrpc.New(ctx)
	if err != nil {
		log.Fatalf("otlploggrpc.New: %v", err)
	}
	defer start(ctx, "grpc", res, &grpcTallies, spanGRPC, metricGRPC, logGRPC)()

	spanHTTP, err := otlptracehttp.New(ctx)
	if err != nil {
		log.Fatalf("otlptracehttp.New: %v", err)
	}
	metricHTTP, err := otlpmetrichttp.New(ctx)
	if err != nil {
		log.Fatalf("otlpmetrichttp.New: %v", err)
	}
	logHTTP, err := otlploghttp.New(ctx)
	if err != nil {
		log.Fatalf("otlploghttp.New: %v", err)
	}
	defer start(ctx, "http", res, &httpTallies, spanHTTP, metricHTTP, logHTTP)()

	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	mux.HandleFunc("/otel", func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(Status{
			Endpoint: os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
			GRPC:     grpcTallies.status(),
			HTTP:     httpTallies.status(),
		})
	})
	srv := &http.Server{Addr: ":80", Handler: mux, ReadHeaderTimeout: 10 * time.Second}
	if err := srv.ListenAndServe(); err != nil && !errors.Is(err, http.ErrServerClosed) {
		log.Fatalf("ListenAndServe: %v", err)
	}
}
