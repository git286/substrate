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

package actorotlp

import (
	"strings"
	"time"

	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"go.ate.dev/substrate/internal/ateattr"
)

// resourceKept are the only resource attributes an actor's own value survives
// under: SDK and runtime descriptors. Collectors and backends route on
// resource attributes such as gcp.project_id, so the rest is dropped.
var resourceKept = []string{"telemetry.sdk.", "telemetry.distro.", "process.runtime."}

// keepResourceAttr reports whether the actor's resource attribute is kept.
func keepResourceAttr(key string) bool {
	return hasAnyPrefix(key, resourceKept)
}

// keepRecordAttr reports whether an attribute below the resource is kept.
func keepRecordAttr(key string) bool {
	return !strings.HasPrefix(key, ateattr.ReservedNamespace)
}

// keepPointAttr reports whether a data point or exemplar attribute is kept.
// Prometheus copies resource attributes onto data points under these names, so
// an actor's own would read as the platform's.
func keepPointAttr(key string) bool {
	if key == "job" || key == "instance" {
		return false
	}
	return keepRecordAttr(key) && !strings.HasPrefix(key, "service.") && !strings.HasPrefix(key, "k8s.")
}

func hasAnyPrefix(key string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

// scrub filters attrs in place.
func scrub(attrs []*commonpb.KeyValue, keep func(string) bool) []*commonpb.KeyValue {
	kept := attrs[:0]
	for _, kv := range attrs {
		if keep(kv.GetKey()) {
			kept = append(kept, kv)
		}
	}
	clear(attrs[len(kept):])
	return kept
}

func stringAttr[K ~string](key K, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: string(key), Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}}
}

// resourceAttributes is the identity stamped on a resource. Actor identity is
// unbounded, so only traces and logs get it.
func (e *exporter) resourceAttributes(withActorIdentity bool) []*commonpb.KeyValue {
	a := e.act.attribution
	w := e.relay.worker
	attrs := []*commonpb.KeyValue{
		stringAttr(semconv.ServiceNameKey, a.TemplateName),
		stringAttr(semconv.ServiceNamespaceKey, a.TemplateAtespace),
		stringAttr(semconv.ServiceInstanceIDKey, e.relay.instanceID(e.act)),
		stringAttr(ateattr.TelemetryPlaneKey, ateattr.TelemetryPlaneActor),
		stringAttr(ateattr.TemplateAtespaceKey, a.TemplateAtespace),
		stringAttr(ateattr.TemplateNameKey, a.TemplateName),
		{Key: string(ateattr.WorkerSlotKey), Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: int64(e.act.slot)}}},
	}
	for _, kv := range []struct {
		key   string
		value string
	}{
		{string(semconv.K8SPodUIDKey), w.PodUID},
		{string(semconv.K8SPodNameKey), w.PodName},
		{string(semconv.K8SNamespaceNameKey), w.PodNamespace},
		{string(semconv.K8SNodeNameKey), w.NodeName},
	} {
		if kv.value != "" {
			attrs = append(attrs, stringAttr(kv.key, kv.value))
		}
	}
	if withActorIdentity {
		attrs = append(attrs,
			stringAttr(ateattr.AtespaceKey, a.Ref.Atespace),
			stringAttr(ateattr.ActorNameKey, a.Ref.Name),
			stringAttr(ateattr.ActorUIDKey, a.UID),
		)
	}
	return attrs
}

// rewriteResource replaces the identity the actor claimed. An export without
// a resource gets one.
func (e *exporter) rewriteResource(res **resourcepb.Resource, withActorIdentity bool) {
	if *res == nil {
		*res = &resourcepb.Resource{}
	}
	r := *res
	r.Attributes = append(scrub(r.Attributes, keepResourceAttr), e.resourceAttributes(withActorIdentity)...)
	// Entity references name attributes the actor chose as identifying.
	r.EntityRefs = nil
}

func scrubScope(scope *commonpb.InstrumentationScope) {
	if scope != nil {
		scope.Attributes = scrub(scope.Attributes, keepRecordAttr)
	}
}

// rewriteTraces leaves span times as sent.
func (e *exporter) rewriteTraces(req *coltracepb.ExportTraceServiceRequest) {
	for _, rs := range req.GetResourceSpans() {
		e.rewriteResource(&rs.Resource, true)
		for _, ss := range rs.GetScopeSpans() {
			scrubScope(ss.GetScope())
			for _, span := range ss.GetSpans() {
				span.Attributes = scrub(span.Attributes, keepRecordAttr)
				for _, ev := range span.GetEvents() {
					ev.Attributes = scrub(ev.Attributes, keepRecordAttr)
				}
				for _, link := range span.GetLinks() {
					link.Attributes = scrub(link.Attributes, keepRecordAttr)
				}
			}
		}
	}
}

// rewriteLogs sets each record's observed time to the relay's clock and leaves
// the event time as sent.
func (e *exporter) rewriteLogs(req *collogspb.ExportLogsServiceRequest) {
	observed := uint64(e.relay.now().UnixNano())
	for _, rl := range req.GetResourceLogs() {
		e.rewriteResource(&rl.Resource, true)
		for _, sl := range rl.GetScopeLogs() {
			scrubScope(sl.GetScope())
			for _, rec := range sl.GetLogRecords() {
				rec.Attributes = scrub(rec.Attributes, keepRecordAttr)
				rec.ObservedTimeUnixNano = observed
			}
		}
	}
}

// rewriteMetrics gives every point in the export the same timestamp; see
// stampTimes.
func (e *exporter) rewriteMetrics(req *colmetricspb.ExportMetricsServiceRequest) {
	now := e.nextStamp()
	for _, rm := range req.GetResourceMetrics() {
		e.rewriteResource(&rm.Resource, false)
		for _, sm := range rm.GetScopeMetrics() {
			scrubScope(sm.GetScope())
			for _, m := range sm.GetMetrics() {
				m.Metadata = scrub(m.Metadata, keepRecordAttr)
				switch data := m.GetData().(type) {
				case *metricspb.Metric_Gauge:
					for _, dp := range data.Gauge.GetDataPoints() {
						e.stampNumber(dp, now)
					}
				case *metricspb.Metric_Sum:
					for _, dp := range data.Sum.GetDataPoints() {
						e.stampNumber(dp, now)
					}
				case *metricspb.Metric_Histogram:
					for _, dp := range data.Histogram.GetDataPoints() {
						dp.Attributes = scrub(dp.Attributes, keepPointAttr)
						scrubExemplars(dp.GetExemplars())
						dp.StartTimeUnixNano, dp.TimeUnixNano = e.stampTimes(dp.GetStartTimeUnixNano(), now)
					}
				case *metricspb.Metric_ExponentialHistogram:
					for _, dp := range data.ExponentialHistogram.GetDataPoints() {
						dp.Attributes = scrub(dp.Attributes, keepPointAttr)
						scrubExemplars(dp.GetExemplars())
						dp.StartTimeUnixNano, dp.TimeUnixNano = e.stampTimes(dp.GetStartTimeUnixNano(), now)
					}
				case *metricspb.Metric_Summary:
					for _, dp := range data.Summary.GetDataPoints() {
						dp.Attributes = scrub(dp.Attributes, keepPointAttr)
						dp.StartTimeUnixNano, dp.TimeUnixNano = e.stampTimes(dp.GetStartTimeUnixNano(), now)
					}
				}
			}
		}
	}
}

func (e *exporter) stampNumber(dp *metricspb.NumberDataPoint, now uint64) {
	dp.Attributes = scrub(dp.Attributes, keepPointAttr)
	scrubExemplars(dp.GetExemplars())
	dp.StartTimeUnixNano, dp.TimeUnixNano = e.stampTimes(dp.GetStartTimeUnixNano(), now)
}

func scrubExemplars(exemplars []*metricspb.Exemplar) {
	for _, ex := range exemplars {
		ex.FilteredAttributes = scrub(ex.FilteredAttributes, keepPointAttr)
	}
}

// nextStamp returns the timestamp for one export: the relay's clock, forced
// past the previous export's. A collector converting delta to cumulative drops
// a point whose time is not after the last one it kept for the stream.
func (e *exporter) nextStamp() uint64 {
	e.act.mu.Lock()
	defer e.act.mu.Unlock()
	now := e.relay.now()
	if !now.After(e.act.lastStamp) {
		now = e.act.lastStamp.Add(time.Nanosecond)
	}
	e.act.lastStamp = now
	return uint64(now.UnixNano())
}

// stampTimes returns a point's start and end. The end is now. A start older
// than the activation becomes the activation: an SDK restored from a golden
// snapshot reports the golden build's start, which the same conversion drops
// as older than the stream. A zero start, which gauges carry, stays zero.
func (e *exporter) stampTimes(start, now uint64) (uint64, uint64) {
	if start == 0 {
		return 0, now
	}
	start = max(start, uint64(e.act.activatedAt.UnixNano()))
	return min(start, now), now
}

// countPoints returns the number of data points in req.
func countPoints(req *colmetricspb.ExportMetricsServiceRequest) int64 {
	var points int64
	for _, rm := range req.GetResourceMetrics() {
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				switch data := m.GetData().(type) {
				case *metricspb.Metric_Gauge:
					points += int64(len(data.Gauge.GetDataPoints()))
				case *metricspb.Metric_Sum:
					points += int64(len(data.Sum.GetDataPoints()))
				case *metricspb.Metric_Histogram:
					points += int64(len(data.Histogram.GetDataPoints()))
				case *metricspb.Metric_ExponentialHistogram:
					points += int64(len(data.ExponentialHistogram.GetDataPoints()))
				case *metricspb.Metric_Summary:
					points += int64(len(data.Summary.GetDataPoints()))
				}
			}
		}
	}
	return points
}

func countSpans(req *coltracepb.ExportTraceServiceRequest) int64 {
	var spans int64
	for _, rs := range req.GetResourceSpans() {
		for _, ss := range rs.GetScopeSpans() {
			spans += int64(len(ss.GetSpans()))
		}
	}
	return spans
}

func countLogRecords(req *collogspb.ExportLogsServiceRequest) int64 {
	var records int64
	for _, rl := range req.GetResourceLogs() {
		for _, sl := range rl.GetScopeLogs() {
			records += int64(len(sl.GetLogRecords()))
		}
	}
	return records
}
