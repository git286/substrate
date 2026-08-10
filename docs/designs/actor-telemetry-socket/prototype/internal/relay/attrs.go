package relay

import (
	"sort"
	"strconv"
	"strings"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"

	"github.com/agent-substrate/ate-identity-prototype/internal/identity"
)

func str(k, v string) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_StringValue{StringValue: v}}}
}

func boolean(k string, v bool) *commonpb.KeyValue {
	return &commonpb.KeyValue{Key: k, Value: &commonpb.AnyValue{Value: &commonpb.AnyValue_BoolValue{BoolValue: v}}}
}

// reserved reports whether an attribute key is one the platform owns. Actors
// may not write these; anything they emit under them is dropped before the
// platform stamps its own value. Overriding rather than merging is the point:
// a workload must not be able to attribute its telemetry to another actor or
// another atespace.
func reserved(key string) bool {
	return strings.HasPrefix(key, identity.ReservedPrefix) || key == identity.KeyServiceInstanceID
}

// stripReserved removes platform-owned keys and reports how many it removed.
func stripReserved(in []*commonpb.KeyValue) (out []*commonpb.KeyValue, removed int) {
	out = make([]*commonpb.KeyValue, 0, len(in))
	for _, kv := range in {
		if kv == nil {
			continue
		}
		if reserved(kv.GetKey()) {
			removed++
			continue
		}
		out = append(out, kv)
	}
	return out, removed
}

// fullIdentityAttrs is the complete trusted attribute set: what traces and
// logs carry.
func fullIdentityAttrs(a identity.Actor, w identity.Worker) []*commonpb.KeyValue {
	return []*commonpb.KeyValue{
		str(identity.KeyServiceInstanceID, a.UID),
		str(identity.KeyActorUID, a.UID),
		str(identity.KeyActorName, a.Name),
		str(identity.KeyAtespace, a.Atespace),
		str(identity.KeyTemplateNamespace, a.TemplateNamespace),
		str(identity.KeyTemplateName, a.TemplateName),
		str(identity.KeyContainerName, a.ContainerName),
		str(identity.KeyWorkerPod, w.PodName),
		str(identity.KeyWorkerNode, w.NodeName),
	}
}

// boundedIdentityAttrs is what metrics carry: bounded, and independent of both
// actor count and of which worker happened to host the actor. Output series
// count is templates x atespaces x containers x metrics.
//
// Note the two deliberate omissions:
//   - actor_uid / actor_name / service.instance.id: unbounded, churns on every
//     actor create.
//   - worker_pod / worker_node: bounded, but they change on migration, which
//     would fragment the very series this design keeps whole.
//
// service.name and service.namespace are pinned to the template rather than
// taken from the actor's resource. Backends and dashboards expect them, but
// their values would otherwise be actor-controlled and so unbounded — see
// Relay.metricResource. The template is what the series describes anyway.
func boundedIdentityAttrs(a identity.Actor) []*commonpb.KeyValue {
	return []*commonpb.KeyValue{
		str("service.namespace", a.TemplateNamespace),
		str("service.name", a.TemplateName),
		str(identity.KeyTemplateNamespace, a.TemplateNamespace),
		str(identity.KeyTemplateName, a.TemplateName),
		str(identity.KeyAtespace, a.Atespace),
		str(identity.KeyContainerName, a.ContainerName),
	}
}

// stampResource replaces the actor's resource attributes with the actor's own
// attributes minus anything platform-owned, plus the trusted set.
func stampResource(res *resourcepb.Resource, trusted []*commonpb.KeyValue) (*resourcepb.Resource, int) {
	var in []*commonpb.KeyValue
	var dropped uint32
	if res != nil {
		in = res.GetAttributes()
		dropped = res.GetDroppedAttributesCount()
	}
	kept, removed := stripReserved(in)
	return &resourcepb.Resource{
		Attributes:             append(kept, trusted...),
		DroppedAttributesCount: dropped,
	}, removed
}

// canonical renders an attribute set as a stable string usable as a map key.
func canonical(attrs []*commonpb.KeyValue) string {
	parts := make([]string, 0, len(attrs))
	for _, kv := range attrs {
		parts = append(parts, kv.GetKey()+"="+anyValueString(kv.GetValue()))
	}
	sort.Strings(parts)
	return strings.Join(parts, "\x1f")
}

func anyValueString(v *commonpb.AnyValue) string {
	if v == nil {
		return ""
	}
	switch t := v.Value.(type) {
	case *commonpb.AnyValue_StringValue:
		return t.StringValue
	case *commonpb.AnyValue_BoolValue:
		return strconv.FormatBool(t.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return strconv.FormatInt(t.IntValue, 10)
	case *commonpb.AnyValue_DoubleValue:
		return strconv.FormatFloat(t.DoubleValue, 'g', -1, 64)
	case *commonpb.AnyValue_ArrayValue:
		parts := make([]string, 0, len(t.ArrayValue.GetValues()))
		for _, e := range t.ArrayValue.GetValues() {
			parts = append(parts, anyValueString(e))
		}
		return "[" + strings.Join(parts, ",") + "]"
	case *commonpb.AnyValue_KvlistValue:
		return canonical(t.KvlistValue.GetValues())
	default:
		return "?"
	}
}
