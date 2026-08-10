package relay

import (
	"time"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/agent-substrate/ate-identity-prototype/internal/identity"
)

// Self-telemetry metric names. These are ateom's own metrics, not the actor's:
// they describe the component, so they carry worker pod/node and are unaffected
// by the identity problem this relay exists to fix.
const (
	MetricAccepted    = "ateom_relay_datapoints_accepted"
	MetricRejected    = "ateom_relay_datapoints_rejected"
	MetricNoActor     = "ateom_relay_exports_without_activation"
	MetricOverridden  = "ateom_relay_actor_attributes_overridden"
	MetricFwdFailures = "ateom_relay_forward_failures"
)

// selfMetrics emits the relay's own counters as delta sums for this interval.
// Answers open question 3: an endpoint that silently drops actor telemetry
// would be worse than the status quo, so every drop is visible here.
func (r *Relay) selfMetrics() []*metricspb.ResourceMetrics {
	r.mu.Lock()
	cur := r.stats
	prev := r.lastReported
	// Deep-copy the rejection map for the next diff.
	snapshot := map[string]int64{}
	for k, v := range cur.DatapointsRejected {
		snapshot[k] = v
	}
	cur.DatapointsRejected = snapshot
	r.lastReported = cur
	r.mu.Unlock()

	now := uint64(time.Now().UnixNano())
	start := r.lastSelfFlush
	if start == 0 {
		start = now
	}
	r.lastSelfFlush = now

	var dps []*metricspb.Metric
	counter := func(name string, delta int64, attrs ...*commonpb.KeyValue) {
		if delta == 0 {
			return
		}
		dps = append(dps, &metricspb.Metric{
			Name: name,
			Unit: "1",
			Data: &metricspb.Metric_Sum{Sum: &metricspb.Sum{
				AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
				IsMonotonic:            true,
				DataPoints: []*metricspb.NumberDataPoint{{
					Attributes:        attrs,
					StartTimeUnixNano: start,
					TimeUnixNano:      now,
					Value:             &metricspb.NumberDataPoint_AsInt{AsInt: delta},
				}},
			}},
		})
	}

	counter(MetricAccepted, cur.DatapointsAccepted-prev.DatapointsAccepted)
	counter(MetricNoActor, cur.ExportsWithoutActor-prev.ExportsWithoutActor)
	counter(MetricOverridden, cur.AttributesOverridden-prev.AttributesOverridden)
	counter(MetricFwdFailures, cur.ForwardFailures-prev.ForwardFailures)
	for reason, v := range cur.DatapointsRejected {
		counter(MetricRejected, v-prev.DatapointsRejected[reason], str("reason", reason))
	}
	if len(dps) == 0 {
		return nil
	}

	return []*metricspb.ResourceMetrics{{
		Resource: &resourcepb.Resource{Attributes: []*commonpb.KeyValue{
			str("service.name", "ateom"),
			str(identity.KeyWorkerPod, r.cfg.Worker.PodName),
			str(identity.KeyWorkerNode, r.cfg.Worker.NodeName),
		}},
		ScopeMetrics: []*metricspb.ScopeMetrics{{
			Scope:   &commonpb.InstrumentationScope{Name: "ateom/relay"},
			Metrics: dps,
		}},
	}}
}
