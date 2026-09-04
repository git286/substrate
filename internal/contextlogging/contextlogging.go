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
	"context"
	"fmt"
	"log/slog"
	"os"
	"time"

	"cloud.google.com/go/compute/metadata"
	"go.opentelemetry.io/otel/trace"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

type ContextHandler struct {
	internal slog.Handler
	// traceProject, when set, additionally records the span under the Cloud
	// Logging special keys, which is what makes log lines link to their trace
	// in the console. Empty means off GCE (or the project is unknown) and only
	// the OTel spellings are written.
	traceProject string
}

// Option configures a ContextHandler.
type Option func(*ContextHandler)

// WithGCETraceProject enables the Cloud Logging trace special keys, qualified
// with the given project ID ("projects/<project>/traces/<id>"). Empty is a
// no-op, so callers can pass DetectGCETraceProject's result unconditionally.
func WithGCETraceProject(project string) Option {
	return func(h *ContextHandler) { h.traceProject = project }
}

func NewHandler(internal slog.Handler, opts ...Option) *ContextHandler {
	h := &ContextHandler{
		internal: internal,
	}
	for _, opt := range opts {
		opt(h)
	}
	return h
}

// DetectGCETraceProject resolves the project ID to qualify Cloud Logging trace
// links with: $GOOGLE_CLOUD_PROJECT when set, the metadata server on GCE, and
// "" everywhere else. "" disables the special keys and nothing is lost — off
// GCE there is no Cloud Logging console to link into.
func DetectGCETraceProject(ctx context.Context) string {
	if p := os.Getenv("GOOGLE_CLOUD_PROJECT"); p != "" {
		return p
	}
	if !metadata.OnGCE() {
		return ""
	}
	ctx, cancel := context.WithTimeout(ctx, 2*time.Second)
	defer cancel()
	p, err := metadata.ProjectIDWithContext(ctx)
	if err != nil {
		return ""
	}
	return p
}

func (h *ContextHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.internal.Enabled(ctx, lvl)
}

// Handle records the active span on the log record under the field names the OTel
// spec fixes for non-OTLP log formats, so a collector can lift them onto the log
// record's own trace fields. Gated on the whole span context being valid: a trace
// ID without a span ID names a request but not the operation within it.
//
// On GCE the collector that reads container stdout is the Cloud Logging agent,
// which lifts only its own special keys, so when the project is known the span
// is recorded under those too — that is what puts the log line on its trace in
// the console.
func (h *ContextHandler) Handle(ctx context.Context, rec slog.Record) error {
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		rec.AddAttrs(
			slog.String(ateattr.LogTraceIDField, sc.TraceID().String()),
			slog.String(ateattr.LogSpanIDField, sc.SpanID().String()),
			slog.String(ateattr.LogTraceFlagsField, fmt.Sprintf("%02x", byte(sc.TraceFlags()))),
		)
		if h.traceProject != "" {
			rec.AddAttrs(
				slog.String(ateattr.LogGCETraceField, "projects/"+h.traceProject+"/traces/"+sc.TraceID().String()),
				slog.String(ateattr.LogGCESpanIDField, sc.SpanID().String()),
				slog.Bool(ateattr.LogGCETraceSampledField, sc.IsSampled()),
			)
		}
	}

	return h.internal.Handle(ctx, rec)
}

func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{internal: h.internal.WithAttrs(attrs), traceProject: h.traceProject}
}

func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{internal: h.internal.WithGroup(name), traceProject: h.traceProject}
}
