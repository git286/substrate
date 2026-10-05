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
	"io"
	"log/slog"
	"testing"

	"go.opentelemetry.io/otel/trace"

	"github.com/agent-substrate/substrate/internal/ateattr"
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

func TestHandleAddsContextAttrs(t *testing.T) {
	var buf bytes.Buffer
	logger := slog.New(NewHandler(slog.NewJSONHandler(&buf, nil)))
	ctx := WithAttrs(context.Background(), slog.String("ate.actor.uid", "uid-1"), slog.String("ate.actor.name", "from-ctx"))
	ctx = WithAttrs(ctx, slog.String("phase", "boot"))
	logger.InfoContext(ctx, "something happened", slog.String("ate.actor.name", "from-call"))

	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("failed to parse log record %q: %v", buf.String(), err)
	}
	for field, want := range map[string]string{
		"ate.actor.uid":  "uid-1",
		"ate.actor.name": "from-call",
		"phase":          "boot",
	} {
		if got := rec[field]; got != want {
			t.Errorf("%s = %v, want %q", field, got, want)
		}
	}
	if n := bytes.Count(buf.Bytes(), []byte(`"ate.actor.name"`)); n != 1 {
		t.Errorf("ate.actor.name appears %d times in %s, want once", n, buf.String())
	}
}

// TestWithAttrsDoesNotLeakIntoSiblings guards the append in WithAttrs: two
// contexts derived from one parent must not share a backing array.
func TestWithAttrsDoesNotLeakIntoSiblings(t *testing.T) {
	parent := WithAttrs(context.Background(), slog.String("a", "1"), slog.String("b", "2"))
	first := WithAttrs(parent, slog.String("c", "first"))
	_ = WithAttrs(parent, slog.String("c", "second"))

	var buf bytes.Buffer
	slog.New(NewHandler(slog.NewJSONHandler(&buf, nil))).InfoContext(first, "x")
	var rec map[string]any
	if err := json.Unmarshal(buf.Bytes(), &rec); err != nil {
		t.Fatalf("failed to parse log record %q: %v", buf.String(), err)
	}
	if rec["c"] != "first" {
		t.Errorf("c = %v, want %q", rec["c"], "first")
	}
}

// BenchmarkHandle measures what WithAttrs adds to a record: every component
// logs through this handler, most with no attrs on the context.
func BenchmarkHandle(b *testing.B) {
	logger := slog.New(NewHandler(slog.NewJSONHandler(io.Discard, nil)))
	plain := context.Background()
	withAttrs := WithAttrs(plain,
		slog.String("ate.atespace", "space"), slog.String("ate.actor.name", "a1"),
		slog.String("ate.actor.uid", "uid-1"), slog.String("ate.template.atespace", "tspace"),
		slog.String("ate.template.name", "tmpl"))
	for _, bc := range []struct {
		name string
		ctx  context.Context
	}{{"no_ctx_attrs", plain}, {"five_ctx_attrs", withAttrs}} {
		b.Run(bc.name, func(b *testing.B) {
			b.ReportAllocs()
			for b.Loop() {
				logger.InfoContext(bc.ctx, "About to run runsc create", slog.String("container", "_pause"))
			}
		})
	}
}
