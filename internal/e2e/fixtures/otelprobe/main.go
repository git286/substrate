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

// Command otelprobe is an actor that exports traces, metrics, and logs with
// the OpenTelemetry SDK, configured only by the environment the platform
// gives it. It claims an identity that is not its own, on the resource and on
// every record, so a test can check what the worker's relay does with it.
//
// The SDK starts before the wakeup probe passes, so a golden snapshot holds
// its start time and its counters.
package main

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"strconv"
	"sync/atomic"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlplog/otlploggrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	otellog "go.opentelemetry.io/otel/log"
	"go.opentelemetry.io/otel/metric"
	sdklog "go.opentelemetry.io/otel/sdk/log"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"go.opentelemetry.io/otel/trace"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/protobuf/proto"
)

// forged is the identity the probe claims at every level.
var forged = []attribute.KeyValue{
	attribute.String("ate.actor.uid", "forged-actor-uid"),
	attribute.String("ate.atespace", "forged-atespace"),
}

func main() {
	ctx := context.Background()
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))
	// Export failures are the probe's only view of a refusal.
	otel.SetErrorHandler(otel.ErrorHandlerFunc(func(err error) {
		slog.Error("otel export error", "err", err.Error())
	}))

	bootID := make([]byte, 8)
	_, _ = rand.Read(bootID)
	boot := hex.EncodeToString(bootID)

	res := resource.NewSchemaless(append(forged,
		attribute.String("service.name", "forged-service"),
		attribute.String("service.namespace", "ate-system"),
		attribute.String("service.instance.id", "forged-instance"),
		attribute.String("k8s.pod.name", "forged-pod"),
		attribute.String("gcp.project_id", "forged-project"),
		attribute.String("host.name", "forged-host"),
		attribute.String("telemetry.sdk.language", "go"),
		attribute.String("otelprobe.boot", boot),
	)...)

	traceExp, err := otlptracegrpc.New(ctx)
	must(err)
	tp := sdktrace.NewTracerProvider(sdktrace.WithResource(res), sdktrace.WithBatcher(traceExp, sdktrace.WithBatchTimeout(2*time.Second)))
	metricExp, err := otlpmetricgrpc.New(ctx)
	must(err)
	mp := sdkmetric.NewMeterProvider(sdkmetric.WithResource(res), sdkmetric.WithReader(sdkmetric.NewPeriodicReader(metricExp)))
	logExp, err := otlploggrpc.New(ctx)
	must(err)
	lp := sdklog.NewLoggerProvider(sdklog.WithResource(res), sdklog.WithProcessor(sdklog.NewBatchProcessor(logExp, sdklog.WithExportInterval(2*time.Second))))

	tracer := tp.Tracer("otelprobe")
	meter := mp.Meter("otelprobe")
	logger := lp.Logger("otelprobe")
	ticks, err := meter.Int64Counter("otelprobe.ticks")
	must(err)
	work, err := meter.Int64Counter("otelprobe.work")
	must(err)
	duration, err := meter.Float64Histogram("otelprobe.work.duration", metric.WithUnit("s"))
	must(err)
	var total atomic.Int64
	_, err = meter.Int64ObservableGauge("otelprobe.total", metric.WithInt64Callback(func(_ context.Context, o metric.Int64Observer) error {
		o.Observe(total.Load())
		return nil
	}))
	must(err)

	// pointAttrs carries the names Prometheus gives resource attributes.
	pointAttrs := metric.WithAttributes(append(forged,
		attribute.String("service.name", "forged-service"),
		attribute.String("k8s.pod.name", "forged-pod"),
		attribute.String("job", "forged-job"),
		attribute.String("instance", "forged-instance"),
		attribute.String("route", "/work"),
	)...)

	unit := func(ctx context.Context) string {
		start := time.Now()
		ctx, span := tracer.Start(ctx, "work")
		span.SetAttributes(forged...)
		span.AddEvent("step", trace.WithAttributes(forged...))
		work.Add(ctx, 1, pointAttrs)
		n := total.Add(1)
		var rec otellog.Record
		rec.SetTimestamp(time.Now())
		rec.SetSeverity(otellog.SeverityInfo)
		rec.SetBody(attribute.StringValue("work " + strconv.FormatInt(n, 10)))
		rec.AddAttributes(attribute.String("ate.actor.uid", "forged-actor-uid"), attribute.String("otelprobe.boot", boot))
		logger.Emit(ctx, rec)
		duration.Record(ctx, time.Since(start).Seconds(), pointAttrs)
		span.End()
		return span.SpanContext().TraceID().String()
	}

	// Ticks run without any request, so the golden snapshot holds a count.
	go func() {
		for i := 0; ; i++ {
			ticks.Add(ctx, 1, pointAttrs)
			if i%5 == 0 {
				slog.Info("tick", "boot", boot, "tick", i, "http_logs_status", postLogsOverHTTP(ctx, boot))
			}
			time.Sleep(2 * time.Second)
		}
	}()

	http.HandleFunc("/readyz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusOK) })
	http.HandleFunc("/work", func(w http.ResponseWriter, r *http.Request) {
		n, _ := strconv.Atoi(r.URL.Query().Get("n"))
		traceIDs := make([]string, 0, max(n, 1))
		for range max(n, 1) {
			traceIDs = append(traceIDs, unit(r.Context()))
		}
		// Flushed so a test can read the backend straight after the response.
		flushCtx, cancel := context.WithTimeout(r.Context(), 10*time.Second)
		defer cancel()
		flushErr := fmt.Sprint(tp.ForceFlush(flushCtx), mp.ForceFlush(flushCtx), lp.ForceFlush(flushCtx))
		_ = json.NewEncoder(w).Encode(map[string]any{
			"boot": boot, "total": total.Load(), "trace_ids": traceIDs, "flush": flushErr,
			"env": map[string]string{
				"OTEL_EXPORTER_OTLP_ENDPOINT":                       os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT"),
				"OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE": os.Getenv("OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE"),
				"OTEL_METRIC_EXPORT_INTERVAL":                       os.Getenv("OTEL_METRIC_EXPORT_INTERVAL"),
			},
		})
	})
	must(http.ListenAndServe(":80", nil))
}

// postLogsOverHTTP sends one log record as OTLP/HTTP to the same endpoint the
// gRPC exporters use, and returns the status.
func postLogsOverHTTP(ctx context.Context, boot string) string {
	now := uint64(time.Now().UnixNano())
	body, err := proto.Marshal(&collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{
		ScopeLogs: []*logspb.ScopeLogs{{LogRecords: []*logspb.LogRecord{{
			TimeUnixNano: now,
			Body:         &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: "http tick " + boot}},
		}}}},
	}}})
	if err != nil {
		return err.Error()
	}
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, os.Getenv("OTEL_EXPORTER_OTLP_ENDPOINT")+"/v1/logs", bytes.NewReader(body))
	if err != nil {
		return err.Error()
	}
	req.Header.Set("Content-Type", "application/x-protobuf")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return err.Error()
	}
	defer resp.Body.Close()
	return resp.Status
}

func must(err error) {
	if err != nil {
		slog.Error("otelprobe failed", "err", err.Error())
		os.Exit(1)
	}
}
