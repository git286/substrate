// Copyright 2026 Google LLC
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//	http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package contextlogging

import (
	"bytes"
	"context"
	"encoding/json"
	"log/slog"
	"strings"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

const (
	testTraceID = "4bf92f3577b34da6a3ce929d0e0e4736"
	testSpanID  = "00f067aa0ba902b7"
)

func spanContext(t *testing.T, flags trace.TraceFlags) trace.SpanContext {
	t.Helper()
	traceID, err := trace.TraceIDFromHex(testTraceID)
	if err != nil {
		t.Fatalf("TraceIDFromHex(%q): %v", testTraceID, err)
	}
	spanID, err := trace.SpanIDFromHex(testSpanID)
	if err != nil {
		t.Fatalf("SpanIDFromHex(%q): %v", testSpanID, err)
	}
	return trace.NewSpanContext(trace.SpanContextConfig{TraceID: traceID, SpanID: spanID, TraceFlags: flags})
}

func TestHandleTraceCorrelation(t *testing.T) {
	tests := []struct {
		name           string
		ctx            func(t *testing.T) context.Context
		wantTraceID    string
		wantSpanID     string
		wantTraceFlags string
	}{
		{
			name: "no span in context",
			ctx:  func(*testing.T) context.Context { return context.Background() },
		},
		{
			name: "invalid span context contributes nothing",
			ctx: func(*testing.T) context.Context {
				return trace.ContextWithSpanContext(context.Background(), trace.NewSpanContext(trace.SpanContextConfig{}))
			},
		},
		{
			name: "sampled span",
			ctx: func(t *testing.T) context.Context {
				return trace.ContextWithSpanContext(context.Background(), spanContext(t, trace.FlagsSampled))
			},
			wantTraceID:    testTraceID,
			wantSpanID:     testSpanID,
			wantTraceFlags: "01",
		},
		{
			// An unsampled record still says which request it belongs to; the flags
			// are what tells a reader why the trace is not in the backend.
			name: "unsampled span still correlates",
			ctx: func(t *testing.T) context.Context {
				return trace.ContextWithSpanContext(context.Background(), spanContext(t, 0))
			},
			wantTraceID:    testTraceID,
			wantSpanID:     testSpanID,
			wantTraceFlags: "00",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var buf bytes.Buffer
			logger := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil)))
			logger.InfoContext(tt.ctx(t), "something happened")

			var rec map[string]any
			if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
				t.Fatalf("failed to parse log record %q: %v", buf.String(), err)
			}

			for field, want := range map[string]string{
				ateattr.LogTraceIDField:    tt.wantTraceID,
				ateattr.LogSpanIDField:     tt.wantSpanID,
				ateattr.LogTraceFlagsField: tt.wantTraceFlags,
			} {
				got, present := rec[field]
				if want == "" {
					if present {
						t.Errorf("%s = %v, want absent", field, got)
					}
					continue
				}
				if got != want {
					t.Errorf("%s = %v, want %q", field, got, want)
				}
			}
		})
	}
}

// TestHandleUngroupedKeepsTraceFieldsTopLevel pins the placement the spec
// requires: a collector only lifts these onto the log record from the top level.
func TestHandleUngroupedKeepsTraceFieldsTopLevel(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil)))
	ctx := trace.ContextWithSpanContext(context.Background(), spanContext(t, trace.FlagsSampled))
	logger.With(slog.String("component", "atelet")).InfoContext(ctx, "something happened", slog.String("id", "abc"))

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("failed to parse log record %q: %v", buf.String(), err)
	}
	if rec[ateattr.LogTraceIDField] != testTraceID {
		t.Errorf("%s = %v, want %q at the top level, got record %v", ateattr.LogTraceIDField, rec[ateattr.LogTraceIDField], testTraceID, rec)
	}
}

// tokenValuer stands in for a type that implements slog.LogValuer and
// resolves to a proto; the handler must resolve it before deciding.
type tokenValuer struct {
	m *ateapipb.MintActorJWTResponse
}

func (v tokenValuer) LogValue() slog.Value { return slog.AnyValue(v.m) }

func TestHandleRedactsProtoAttrsAnywhereInTheRecord(t *testing.T) {
	const token = "eyJhbGciOiJSUzI1NiJ9.eyJzdWIiOiJhY3RvciJ9.c2lnbmF0dXJl"
	resp := &ateapipb.MintActorJWTResponse{ActorJwt: token}
	ref := &ateapipb.ObjectRef{Atespace: "ate-demo-sandbox", Name: "agent"}
	tpl := &ateapipb.ActorTemplate{Containers: []*ateapipb.Container{{
		Name: "c", Env: []*ateapipb.EnvVar{{Name: "API_KEY", Value: "sk-secret"}},
	}}}

	var buf bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil)))

	// Direct attr, attr inside a group, LogValuer resolving to a proto, and a
	// pre-bound attr via With (WithAttrs), plus clean values that must survive.
	logger.With(slog.Any("bound", tpl)).InfoContext(context.Background(), "test",
		slog.Any("resp", resp),
		slog.Group("nested", slog.String("plain", "keep"), slog.Any("inner", resp)),
		slog.Any("valuer", tokenValuer{resp}),
		slog.Any("actor", ref),
		slog.String("host", "example.com"),
		slog.Int("n", 3),
	)
	got := buf.String()

	for _, leak := range []string{token, "sk-secret"} {
		if strings.Contains(got, leak) {
			t.Fatalf("log contains %q: %s", leak, got)
		}
	}
	for _, want := range []string{
		`"resp":{"actor_jwt":"[REDACTED]"}`,
		`"nested":{"plain":"keep","inner":{"actor_jwt":"[REDACTED]"}}`,
		`"valuer":{"actor_jwt":"[REDACTED]"}`,
		`"bound":{"containers":[{"name":"c","env":[{"name":"API_KEY","value":"[REDACTED]"}]}]}`,
		`"actor":{"atespace":"ate-demo-sandbox","name":"agent"}`,
		`"host":"example.com"`,
		`"n":3`,
	} {
		if !strings.Contains(got, want) {
			t.Errorf("log missing %s\n got: %s", want, got)
		}
	}
	// Originals are untouched: the caller still holds the real values.
	if resp.GetActorJwt() != token || tpl.GetContainers()[0].GetEnv()[0].GetValue() != "sk-secret" {
		t.Fatal("handler mutated the logged message")
	}
}

func TestHandleLeavesRecordsWithoutProtosAlone(t *testing.T) {
	// Same record through the bare JSON handler and through ours must match
	// byte for byte (no span in ctx, so no trace fields are added).
	var plainBuf, oursBuf bytes.Buffer
	plain := slog.New(slog.NewJSONHandler(&plainBuf, &slog.HandlerOptions{ReplaceAttr: dropTime}))
	ours := slog.New(NewHandler(slog.NewJSONHandler(&oursBuf, &slog.HandlerOptions{ReplaceAttr: dropTime})))
	for _, l := range []*slog.Logger{plain, ours} {
		l.Info("x", slog.String("a", "b"), slog.Int("n", 1), slog.Any("nilproto", (*ateapipb.ObjectRef)(nil)), slog.Any("m", map[string]int{"k": 1}))
	}
	if plainBuf.String() != oursBuf.String() {
		t.Fatalf("records differ:\n plain: %s\n ours:  %s", plainBuf.String(), oursBuf.String())
	}
}

func dropTime(_ []string, a slog.Attr) slog.Attr {
	if a.Key == slog.TimeKey {
		return slog.Attr{}
	}
	return a
}
