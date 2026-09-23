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

	"go.opentelemetry.io/otel/trace"
	"google.golang.org/protobuf/proto"

	"github.com/agent-substrate/substrate/internal/ateattr"
	"github.com/agent-substrate/substrate/internal/protoredact"
)

type ContextHandler struct {
	internal slog.Handler
}

func NewHandler(internal slog.Handler) *ContextHandler {
	return &ContextHandler{
		internal: internal,
	}
}

func (h *ContextHandler) Enabled(ctx context.Context, lvl slog.Level) bool {
	return h.internal.Enabled(ctx, lvl)
}

// Handle redacts protobuf attributes and records the active span on the log
// record.
//
// Redaction: every attribute whose value is a proto.Message, directly or inside
// a group, is replaced by a copy with its debug_redact fields masked (see
// protoredact). Doing this here rather than at each call site means any slog
// call in a process that installs this handler is covered, not only the gRPC
// interceptor. Records with no proto attribute, which is nearly all of them,
// pass through after a scan that allocates nothing.
//
// Trace correlation: the span is recorded under the field names the OTel spec
// fixes for non-OTLP log formats, so a collector can lift them onto the log
// record's own trace fields. Gated on the whole span context being valid: a
// trace ID without a span ID names a request but not the operation within it.
func (h *ContextHandler) Handle(ctx context.Context, rec slog.Record) error {
	rec = redactRecord(rec)
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		rec.AddAttrs(
			slog.String(ateattr.LogTraceIDField, sc.TraceID().String()),
			slog.String(ateattr.LogSpanIDField, sc.SpanID().String()),
			slog.String(ateattr.LogTraceFlagsField, fmt.Sprintf("%02x", byte(sc.TraceFlags()))),
		)
	}

	return h.internal.Handle(ctx, rec)
}

// WithAttrs redacts the pre-bound attributes once, when they are bound, since
// Handle never sees them again.
func (h *ContextHandler) WithAttrs(attrs []slog.Attr) slog.Handler {
	return &ContextHandler{internal: h.internal.WithAttrs(redactAttrs(attrs))}
}

func (h *ContextHandler) WithGroup(name string) slog.Handler {
	return &ContextHandler{internal: h.internal.WithGroup(name)}
}

// redactRecord returns rec with every proto.Message attribute replaced by a
// redacted copy. A record without one is returned unchanged; slog.Record
// cannot be edited in place, so a new record is built only when needed.
func redactRecord(rec slog.Record) slog.Record {
	found := false
	rec.Attrs(func(a slog.Attr) bool {
		found = needsRedaction(a)
		return !found
	})
	if !found {
		return rec
	}
	out := slog.NewRecord(rec.Time, rec.Level, rec.Message, rec.PC)
	rec.Attrs(func(a slog.Attr) bool {
		out.AddAttrs(redactAttr(a))
		return true
	})
	return out
}

// redactAttrs is redactRecord for a plain attribute slice (WithAttrs). The
// input slice is not modified.
func redactAttrs(attrs []slog.Attr) []slog.Attr {
	for _, a := range attrs {
		if needsRedaction(a) {
			out := make([]slog.Attr, len(attrs))
			for i := range attrs {
				out[i] = redactAttr(attrs[i])
			}
			return out
		}
	}
	return attrs
}

// needsRedaction reports whether a carries a proto.Message, directly or inside
// a group. LogValuer values are resolved first, as the terminal handler would.
func needsRedaction(a slog.Attr) bool {
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindAny:
		_, ok := v.Any().(proto.Message)
		return ok
	case slog.KindGroup:
		for _, g := range v.Group() {
			if needsRedaction(g) {
				return true
			}
		}
	}
	return false
}

// redactAttr returns a with any proto.Message value, direct or inside a
// group, replaced by its redacted copy. Other attributes are returned as-is.
func redactAttr(a slog.Attr) slog.Attr {
	v := a.Value.Resolve()
	switch v.Kind() {
	case slog.KindAny:
		if m, ok := v.Any().(proto.Message); ok {
			return slog.Any(a.Key, protoredact.Clone(m))
		}
	case slog.KindGroup:
		g := v.Group()
		out := make([]slog.Attr, len(g))
		for i := range g {
			out[i] = redactAttr(g[i])
		}
		return slog.Attr{Key: a.Key, Value: slog.GroupValue(out...)}
	}
	return a
}
