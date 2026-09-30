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

package actorotlp

import (
	"sort"
	"strings"
	"time"

	semconv "go.opentelemetry.io/otel/semconv/v1.40.0"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/agent-substrate/substrate/internal/ateattr"
)

// Attribute namespaces the relay owns. Whatever an actor put under them is
// removed before the trusted values are set: at the resource, where identity
// lives, and at every level below it, because a forged ate.actor.uid on a data
// point would otherwise be the only actor identity that reaches storage for a
// metric. service.* and k8s.* are resource-level concepts and are scrubbed
// there only.
var (
	resourcePrefixes = []string{ateattr.ReservedNamespace, "service.", "k8s."}
	recordPrefixes   = []string{ateattr.ReservedNamespace}
)

// scrub returns attrs without the keys under any of prefixes. It filters in
// place: the request is the relay's to modify once it has arrived.
func scrub(attrs []*commonpb.KeyValue, prefixes []string) []*commonpb.KeyValue {
	kept := attrs[:0]
	for _, kv := range attrs {
		if !hasAnyPrefix(kv.GetKey(), prefixes) {
			kept = append(kept, kv)
		}
	}
	// Clear the tail so dropped values are not retained by the backing array.
	for i := len(kept); i < len(attrs); i++ {
		attrs[i] = nil
	}
	return kept
}

func hasAnyPrefix(key string, prefixes []string) bool {
	for _, p := range prefixes {
		if strings.HasPrefix(key, p) {
			return true
		}
	}
	return false
}

func stringAttr(key, value string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: value}}}
}

func intAttr(key string, value int64) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: key, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_IntValue{IntValue: value}}}
}

// resourceAttributes is what the relay stamps on a resource. The bounded set
// goes on every signal: the template as the service, the (pod, slot) writer as
// the instance, and the worker's host facts. Actor identity is unbounded and
// goes on traces and logs only, per the no-actor-identity cardinality rule.
func (e *exporter) resourceAttributes(withActorIdentity bool) []*commonpb.KeyValue {
	a := e.act.attribution
	w := e.relay.worker
	attrs := []*commonpb.KeyValue{
		stringAttr(string(semconv.ServiceNameKey), a.TemplateName),
		stringAttr(string(semconv.ServiceNamespaceKey), a.TemplateAtespace),
		stringAttr(string(semconv.ServiceInstanceIDKey), e.relay.instanceID(e.act)),
		stringAttr(string(ateattr.TelemetryPlaneKey), ateattr.TelemetryPlaneActor),
		stringAttr(string(ateattr.TemplateAtespaceKey), a.TemplateAtespace),
		stringAttr(string(ateattr.TemplateNameKey), a.TemplateName),
		intAttr(string(ateattr.ActorSlotKey), int64(e.act.slot)),
	}
	for _, kv := range []struct{ key, value string }{
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
			stringAttr(string(ateattr.AtespaceKey), a.Ref.Atespace),
			stringAttr(string(ateattr.ActorNameKey), a.Ref.Name),
			stringAttr(string(ateattr.ActorUIDKey), a.UID),
		)
	}
	return attrs
}

// rewriteResource replaces the actor's claimed identity with the platform's.
// A nil resource is created: an SDK that sent none still gets attributed.
func (e *exporter) rewriteResource(res **resourcepb.Resource, withActorIdentity bool) {
	if *res == nil {
		*res = &resourcepb.Resource{}
	}
	r := *res
	r.Attributes = append(scrub(r.Attributes, resourcePrefixes), e.resourceAttributes(withActorIdentity)...)
}

func scrubScope(scope *commonpb.InstrumentationScope) {
	if scope != nil {
		scope.Attributes = scrub(scope.Attributes, recordPrefixes)
	}
}

func (e *exporter) rewriteTraces(req *coltracepb.ExportTraceServiceRequest) {
	for _, rs := range req.GetResourceSpans() {
		e.rewriteResource(&rs.Resource, true)
		for _, ss := range rs.GetScopeSpans() {
			scrubScope(ss.GetScope())
			for _, span := range ss.GetSpans() {
				span.Attributes = scrub(span.Attributes, recordPrefixes)
				for _, ev := range span.GetEvents() {
					ev.Attributes = scrub(ev.Attributes, recordPrefixes)
				}
				for _, link := range span.GetLinks() {
					link.Attributes = scrub(link.Attributes, recordPrefixes)
				}
			}
		}
	}
}

// rewriteLogs stamps actor identity on the resource and on every record. The
// record copy uses the same keys as the labels on an actor's stdout lines
// (ateattr.ActorLogLabels), so the two log streams filter alike, and it
// survives a log backend that keeps record attributes but not the resource's:
// Cloud Logging is one.
func (e *exporter) rewriteLogs(req *collogspb.ExportLogsServiceRequest) {
	identity := e.logRecordAttributes()
	for _, rl := range req.GetResourceLogs() {
		e.rewriteResource(&rl.Resource, true)
		for _, sl := range rl.GetScopeLogs() {
			scrubScope(sl.GetScope())
			for _, rec := range sl.GetLogRecords() {
				rec.Attributes = append(scrub(rec.Attributes, recordPrefixes), identity...)
			}
		}
	}
}

// logRecordAttributes is the actor identity as record attributes, one copy
// shared by every record of an export: the request is marshaled, not retained.
func (e *exporter) logRecordAttributes() []*commonpb.KeyValue {
	labels := ateattr.ActorLogLabels(e.act.attribution, "")
	keys := make([]string, 0, len(labels))
	for k := range labels {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	attrs := make([]*commonpb.KeyValue, 0, len(keys))
	for _, k := range keys {
		attrs = append(attrs, stringAttr(k, labels[k]))
	}
	return attrs
}

// rewriteMetrics attributes the batch to the bounded writer identity and
// rewrites every data point's timestamps; see stampTimes.
func (e *exporter) rewriteMetrics(req *colmetricspb.ExportMetricsServiceRequest) {
	now := e.nextStamp()
	for _, rm := range req.GetResourceMetrics() {
		e.rewriteResource(&rm.Resource, false)
		for _, sm := range rm.GetScopeMetrics() {
			scrubScope(sm.GetScope())
			for _, m := range sm.GetMetrics() {
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
						dp.Attributes = scrub(dp.Attributes, recordPrefixes)
						scrubExemplars(dp.GetExemplars())
						dp.StartTimeUnixNano, dp.TimeUnixNano = e.stampTimes(dp.GetStartTimeUnixNano(), now)
					}
				case *metricspb.Metric_ExponentialHistogram:
					for _, dp := range data.ExponentialHistogram.GetDataPoints() {
						dp.Attributes = scrub(dp.Attributes, recordPrefixes)
						scrubExemplars(dp.GetExemplars())
						dp.StartTimeUnixNano, dp.TimeUnixNano = e.stampTimes(dp.GetStartTimeUnixNano(), now)
					}
				case *metricspb.Metric_Summary:
					for _, dp := range data.Summary.GetDataPoints() {
						dp.Attributes = scrub(dp.Attributes, recordPrefixes)
						dp.StartTimeUnixNano, dp.TimeUnixNano = e.stampTimes(dp.GetStartTimeUnixNano(), now)
					}
				}
			}
		}
	}
}

func (e *exporter) stampNumber(dp *metricspb.NumberDataPoint, now uint64) {
	dp.Attributes = scrub(dp.Attributes, recordPrefixes)
	scrubExemplars(dp.GetExemplars())
	dp.StartTimeUnixNano, dp.TimeUnixNano = e.stampTimes(dp.GetStartTimeUnixNano(), now)
}

func scrubExemplars(exemplars []*metricspb.Exemplar) {
	for _, ex := range exemplars {
		ex.FilteredAttributes = scrub(ex.FilteredAttributes, recordPrefixes)
	}
}

// nextStamp returns the timestamp for every point in one export: the relay's
// clock, forced strictly past the previous export's so points stay ordered
// per stream whatever the actor's clock does. A restored micro-VM can lag or
// jump, and the collector's delta-to-cumulative conversion drops a point
// whose time is not after the last one it kept.
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

// stampTimes rewrites one point's interval. The end is the receive time. The
// start is the actor's own, unless it predates the activation, in which case it
// is the activation: an SDK restored from a golden snapshot reports the start
// of the golden build, and the same collector conversion drops a point whose
// start is older than the stream it already holds, which under slot reuse is
// the previous occupant's. A zero start (a gauge) stays zero. A start that is
// not before the end is clamped to it, so the interval is never negative.
func (e *exporter) stampTimes(start, now uint64) (uint64, uint64) {
	if start == 0 {
		return 0, now
	}
	if floor := uint64(e.act.activatedAt.UnixNano()); start < floor {
		start = floor
	}
	if start > now {
		start = now
	}
	return start, now
}
