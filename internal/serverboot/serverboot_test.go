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

package serverboot

import (
	"bytes"
	"context"
	"io"
	"log/slog"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
	"go.opentelemetry.io/otel/sdk/resource"
	semconv "go.opentelemetry.io/otel/semconv/v1.21.0"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

func resourceAttrs(res *resource.Resource) map[string]string {
	m := make(map[string]string)
	for _, kv := range res.Attributes() {
		m[string(kv.Key)] = kv.Value.String()
	}
	return m
}

func TestNewResourceDefaults(t *testing.T) {
	res, err := newResource(context.Background(), "ateapi")
	if err != nil {
		t.Fatalf("newResource: %v", err)
	}
	attrs := resourceAttrs(res)
	if got := attrs[string(semconv.ServiceNameKey)]; got != "ateapi" {
		t.Errorf("service.name = %q, want ateapi", got)
	}
	if attrs[string(semconv.ServiceInstanceIDKey)] == "" {
		t.Error("service.instance.id must be set")
	}
}

func TestNewResourceEnvWins(t *testing.T) {
	t.Setenv("OTEL_SERVICE_NAME", "from-env")
	t.Setenv("OTEL_RESOURCE_ATTRIBUTES", "service.instance.id=fixed-id")
	res, err := newResource(context.Background(), "ateapi")
	if err != nil {
		t.Fatalf("newResource: %v", err)
	}
	attrs := resourceAttrs(res)
	if got := attrs[string(semconv.ServiceNameKey)]; got != "from-env" {
		t.Errorf("service.name = %q, want from-env (OTEL_SERVICE_NAME must win)", got)
	}
	if got := attrs[string(semconv.ServiceInstanceIDKey)]; got != "fixed-id" {
		t.Errorf("service.instance.id = %q, want fixed-id (OTEL_RESOURCE_ATTRIBUTES must win)", got)
	}
}

// lazyConn is a ClientConn that never dials: grpc.NewClient connects on first
// RPC, and relayAttrs only cares whether the pointer is nil.
func lazyConn(t *testing.T) *grpc.ClientConn {
	t.Helper()
	conn, err := grpc.NewClient("passthrough:///unused", grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatalf("grpc.NewClient: %v", err)
	}
	t.Cleanup(func() { conn.Close() })
	return conn
}

// The three cases relayAttrs exists to separate: a component that got the
// relay, one that wanted it and fell back, and one that was never offered one.
// The last must carry no attribute at all rather than a misleading "direct".
func TestRelayAttrs(t *testing.T) {
	for _, tc := range []struct {
		name         string
		relayCapable bool
		conn         bool
		want         string // "" means the attribute must be absent
	}{
		{name: "relay capable with conn", relayCapable: true, conn: true, want: "relay"},
		{name: "relay capable fell back", relayCapable: true, conn: false, want: "direct"},
		{name: "not relay capable", relayCapable: false, conn: false},
		// atecontroller stays unlabeled even if some future caller hands it a
		// connection for another reason: capability, not the conn, is the gate.
		{name: "not relay capable with conn", relayCapable: false, conn: true},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var conn *grpc.ClientConn
			if tc.conn {
				conn = lazyConn(t)
			}
			res, err := newResource(context.Background(), "ateom-gvisor", relayAttrs(tc.relayCapable, conn)...)
			if err != nil {
				t.Fatalf("newResource: %v", err)
			}
			got, ok := resourceAttrs(res)[string(ateattr.OTLPRelayKey)]
			if tc.want == "" {
				if ok {
					t.Errorf("%s = %q, want absent", string(ateattr.OTLPRelayKey), got)
				}
				return
			}
			if got != tc.want {
				t.Errorf("%s = %q, want %q", string(ateattr.OTLPRelayKey), got, tc.want)
			}
		})
	}
}

// collectedResource reads back the resource a meter provider actually stamps on
// its exports, by attaching a ManualReader alongside the OTLP one. The provider
// does not expose its resource any other way, and asserting on newResource's
// return would only re-test relayAttrs.
func collectedResource(t *testing.T, relayCapable bool, conn *grpc.ClientConn) map[string]string {
	t.Helper()
	reader := sdkmetric.NewManualReader()
	mp, err := newMeterProvider(context.Background(), "ateom-gvisor", relayCapable, conn, nil, reader)
	if err != nil {
		t.Fatalf("newMeterProvider: %v", err)
	}
	t.Cleanup(func() {
		// Shutdown flushes the OTLP reader too, and no collector is listening
		// here: a live context spends the exporter's full retry budget (~10s)
		// per provider. A cancelled one skips the flush, which is all this test
		// wants from Shutdown anyway.
		ctx, cancel := context.WithCancel(context.Background())
		cancel()
		mp.Shutdown(ctx)
	})
	// A meter with no instruments still collects, carrying the resource.
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	return resourceAttrs(rm.Resource)
}

// The metric path's half of the decision, asserted on what the provider exports
// rather than on what relayAttrs returns: a wiring mistake in newMeterProvider
// (passing the wrong flag, dropping the attrs) fails here and not in
// TestRelayAttrs.
func TestMeterProviderRelayAttribute(t *testing.T) {
	if got, ok := collectedResource(t, true, lazyConn(t))[string(ateattr.OTLPRelayKey)]; !ok || got != "relay" {
		t.Errorf("%s = %q (present %t), want relay", string(ateattr.OTLPRelayKey), got, ok)
	}
	if got, ok := collectedResource(t, true, nil)[string(ateattr.OTLPRelayKey)]; !ok || got != "direct" {
		t.Errorf("%s = %q (present %t), want direct", string(ateattr.OTLPRelayKey), got, ok)
	}
	// atecontroller and ateapi: no relay was ever offered, so no claim is made
	// about which path they took.
	if got, ok := collectedResource(t, false, nil)[string(ateattr.OTLPRelayKey)]; ok {
		t.Errorf("%s = %q, want absent for a component with no relay", string(ateattr.OTLPRelayKey), got)
	}
}

func TestReadyzDrainsWhileHealthzStaysUp(t *testing.T) {
	readiness := &Readiness{}
	mux := metricsMux(MetricsServerOptions{
		Readiness:     readiness,
		EnableHealthz: true,
	})

	if got := getCode(t, mux, "/readyz"); got != http.StatusOK {
		t.Errorf("/readyz before drain = %d, want %d", got, http.StatusOK)
	}
	if got := getCode(t, mux, "/healthz"); got != http.StatusOK {
		t.Errorf("/healthz before drain = %d, want %d", got, http.StatusOK)
	}

	readiness.MarkNotReady()

	if got := getCode(t, mux, "/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("/readyz during drain = %d, want %d", got, http.StatusServiceUnavailable)
	}
	if got := getCode(t, mux, "/healthz"); got != http.StatusOK {
		t.Errorf("/healthz during drain = %d, want %d (liveness must not fail while draining)", got, http.StatusOK)
	}
}

func TestReadyzStaticWithZeroValueReadiness(t *testing.T) {
	mux := metricsMux(MetricsServerOptions{Readiness: &Readiness{}})
	if got := getCode(t, mux, "/readyz"); got != http.StatusOK {
		t.Errorf("/readyz with zero-value Readiness = %d, want %d", got, http.StatusOK)
	}
}

func TestReadinessMux(t *testing.T) {
	readiness := &Readiness{}
	mux := readinessMux(readiness)

	if got := getCode(t, mux, "/readyz"); got != http.StatusOK {
		t.Errorf("/readyz before drain = %d, want %d", got, http.StatusOK)
	}
	if got := getCode(t, mux, "/metrics"); got != http.StatusNotFound {
		t.Errorf("/metrics = %d, want %d", got, http.StatusNotFound)
	}

	readiness.MarkNotReady()
	if got := getCode(t, mux, "/readyz"); got != http.StatusServiceUnavailable {
		t.Errorf("/readyz during drain = %d, want %d", got, http.StatusServiceUnavailable)
	}
}

func TestReadyzAbsentWithoutReadiness(t *testing.T) {
	mux := metricsMux(MetricsServerOptions{})
	if got := getCode(t, mux, "/readyz"); got != http.StatusNotFound {
		t.Errorf("/readyz with nil Readiness = %d, want %d", got, http.StatusNotFound)
	}
}

func TestHealthzAbsentUnlessEnabled(t *testing.T) {
	mux := metricsMux(MetricsServerOptions{Readiness: &Readiness{}})
	if got := getCode(t, mux, "/healthz"); got != http.StatusNotFound {
		t.Errorf("/healthz without EnableHealthz = %d, want %d", got, http.StatusNotFound)
	}
}

func TestInitMetricsPushOnlyHasNoPrometheusSurface(t *testing.T) {
	mp, err := InitMetricsPushOnly(context.Background(), "test-pushonly")
	if err != nil {
		t.Fatalf("InitMetricsPushOnly: %v", err)
	}
	// Bound shutdown: the periodic reader would otherwise block flushing to the
	// unreachable default OTLP endpoint until the export timeout.
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = mp.Shutdown(ctx)
	})

	ctr, err := mp.Meter("test").Int64Counter("ate.test.pushonly.count")
	if err != nil {
		t.Fatalf("create counter: %v", err)
	}
	ctr.Add(context.Background(), 1)

	// A push-only provider registers no Prometheus reader, so what it records must
	// not surface on the default registry StartMetricsServer's /metrics serves.
	rec := httptest.NewRecorder()
	promhttp.Handler().ServeHTTP(rec, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	if strings.Contains(rec.Body.String(), "ate_test_pushonly") {
		t.Error("push-only MeterProvider must not expose a Prometheus pull surface")
	}
}

func TestInitMetricsPushOnlyRequiresServiceName(t *testing.T) {
	if _, err := InitMetricsPushOnly(context.Background(), ""); err == nil {
		t.Error("InitMetricsPushOnly(\"\") must return an error")
	}
}

func TestInitMetricsRequiresServiceName(t *testing.T) {
	if _, err := InitMetrics(context.Background(), ""); err == nil {
		t.Error("InitMetrics(\"\") must return an error")
	}
}

func getCode(t *testing.T, mux *http.ServeMux, path string) int {
	t.Helper()
	rec := httptest.NewRecorder()
	mux.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, path, nil))
	return rec.Code
}

func TestSetLogLevel(t *testing.T) {
	t.Cleanup(func() { logLevel.Set(slog.LevelInfo) })

	// The untouched default must be exactly info: every existing deployment
	// relies on this for "no behavior change without the flag".
	if got := logLevel.Level(); got != slog.LevelInfo {
		t.Fatalf("default log level = %v, want %v", got, slog.LevelInfo)
	}

	var buf bytes.Buffer
	InitLoggerWithWriter(&buf)
	t.Cleanup(InitLogger)

	slog.Info("visible at default level")
	if !strings.Contains(buf.String(), "visible at default level") {
		t.Errorf("info line not emitted at default level: %s", buf.String())
	}
	buf.Reset()
	slog.Debug("hidden at default level")
	if buf.Len() != 0 {
		t.Errorf("debug line emitted at default level: %s", buf.String())
	}

	if err := SetLogLevel("debug"); err != nil {
		t.Fatalf("SetLogLevel(debug): %v", err)
	}
	slog.Debug("visible at debug")
	if !strings.Contains(buf.String(), "visible at debug") {
		t.Errorf("debug line not emitted after SetLogLevel(debug): %s", buf.String())
	}

	// Case-insensitive, and dynamic: raising the level silences info.
	if err := SetLogLevel("WARN"); err != nil {
		t.Fatalf("SetLogLevel(WARN): %v", err)
	}
	buf.Reset()
	slog.Info("hidden at warn")
	if buf.Len() != 0 {
		t.Errorf("info line emitted at warn level: %s", buf.String())
	}

	if err := SetLogLevel("verbose"); err == nil {
		t.Error("SetLogLevel accepted an invalid level")
	}

	// Empty means unset: no error, level unchanged.
	if err := SetLogLevel(""); err != nil {
		t.Errorf("SetLogLevel(\"\") = %v, want nil", err)
	}
	if got := logLevel.Level(); got != slog.LevelWarn {
		t.Errorf("SetLogLevel(\"\") changed the level to %v", got)
	}
}

// firstByteServer accepts one TCP connection and reports its first byte: 0x16
// opens a TLS handshake, 'P' opens the plaintext HTTP/2 preface.
func firstByteServer(t *testing.T) (addr string, first <-chan byte) {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	t.Cleanup(func() { _ = ln.Close() })
	ch := make(chan byte, 1)
	go func() {
		conn, err := ln.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		_ = conn.SetReadDeadline(time.Now().Add(5 * time.Second))
		buf := make([]byte, 1)
		if _, err := io.ReadFull(conn, buf); err == nil {
			ch <- buf[0]
		}
	}()
	return ln.Addr().String(), ch
}

func clearOTLPEnv(t *testing.T) {
	t.Helper()
	for _, n := range []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT", "OTEL_EXPORTER_OTLP_TRACES_ENDPOINT", "OTEL_EXPORTER_OTLP_METRICS_ENDPOINT",
		"OTEL_EXPORTER_OTLP_INSECURE", "OTEL_EXPORTER_OTLP_TRACES_INSECURE", "OTEL_EXPORTER_OTLP_METRICS_INSECURE",
		"OTEL_EXPORTER_OTLP_CERTIFICATE", "OTEL_EXPORTER_OTLP_CLIENT_CERTIFICATE", "OTEL_EXPORTER_OTLP_CLIENT_KEY",
	} {
		t.Setenv(n, "")
	}
}

// TestTraceExporterFollowsEndpointScheme drives a real exporter at a listener
// that only looks at the first byte: an https:// endpoint must open a TLS
// handshake and an http:// endpoint must not. The exporter used to pin
// plaintext regardless of the scheme.
func TestTraceExporterFollowsEndpointScheme(t *testing.T) {
	for _, tc := range []struct {
		scheme string
		want   byte
	}{
		{scheme: "https", want: 0x16},
		{scheme: "http", want: 'P'},
	} {
		t.Run(tc.scheme, func(t *testing.T) {
			clearOTLPEnv(t)
			addr, first := firstByteServer(t)
			t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", tc.scheme+"://"+addr)

			tp, err := InitTracing(context.Background(), TracingOptions{
				ServiceName: "test-scheme",
				Sampling:    ParentRatioSampling(1),
			})
			if err != nil {
				t.Fatalf("InitTracing: %v", err)
			}
			t.Cleanup(func() {
				ctx, cancel := context.WithTimeout(context.Background(), time.Second)
				defer cancel()
				_ = tp.Shutdown(ctx)
			})
			_, span := tp.Tracer("test").Start(context.Background(), "probe")
			span.End()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			_ = tp.ForceFlush(ctx) // fails either way: the listener never answers

			select {
			case got := <-first:
				if got != tc.want {
					t.Errorf("first byte on the wire = %#x, want %#x for an %s:// endpoint", got, tc.want, tc.scheme)
				}
			case <-time.After(5 * time.Second):
				t.Fatal("the exporter never connected")
			}
		})
	}
}

// TestMetricExporterFollowsEndpointScheme is the metrics counterpart.
func TestMetricExporterFollowsEndpointScheme(t *testing.T) {
	clearOTLPEnv(t)
	addr, first := firstByteServer(t)
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://"+addr)

	mp, err := InitMetricsPushOnly(context.Background(), "test-scheme")
	if err != nil {
		t.Fatalf("InitMetricsPushOnly: %v", err)
	}
	t.Cleanup(func() {
		ctx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		_ = mp.Shutdown(ctx)
	})
	ctr, err := mp.Meter("test").Int64Counter("ate.test.scheme.count")
	if err != nil {
		t.Fatalf("create counter: %v", err)
	}
	ctr.Add(context.Background(), 1)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = mp.ForceFlush(ctx)

	select {
	case got := <-first:
		if got != 0x16 {
			t.Errorf("first byte on the wire = %#x, want a TLS handshake (0x16) for an https:// endpoint", got)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("the exporter never connected")
	}
}

func TestExporterTransportOptions(t *testing.T) {
	marker := func() string { return "insecure" }
	clearOTLPEnv(t)
	if got := exporterTransportOptions(marker); len(got) != 1 {
		t.Errorf("exporterTransportOptions() with no OTLP variable = %v, want the plaintext option", got)
	}
	t.Setenv("OTEL_EXPORTER_OTLP_ENDPOINT", "https://collector:4317")
	if got := exporterTransportOptions(marker); len(got) != 0 {
		t.Errorf("exporterTransportOptions() with an endpoint set = %v, want none so the SDK decides", got)
	}
}
