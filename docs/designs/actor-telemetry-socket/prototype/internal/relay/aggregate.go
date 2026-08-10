package relay

import (
	"fmt"
	"sync"

	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	resourcepb "go.opentelemetry.io/proto/otlp/resource/v1"
)

// GaugeAggregation is the explicit per-metric combine rule for gauges.
//
// Counters add; gauges don't. queue_depth wants sum, cpu_utilization wants
// average, and OTLP carries no metadata saying which. So the relay refuses to
// guess: an undeclared gauge is dropped and counted, never silently summed.
type GaugeAggregation string

const (
	GaugeSum  GaugeAggregation = "sum"
	GaugeAvg  GaugeAggregation = "avg"
	GaugeDrop GaugeAggregation = "drop"
)

// OverflowKey is the standard OTel cardinality-limit marker. Datapoints past
// the per-metric limit are folded into a single overflow series rather than
// dropped, so the total still adds up.
const OverflowKey = "otel.metric.overflow"

// Reasons the aggregator refuses a datapoint. They travel back to the actor in
// the OTLP partial-success message and out to the backend as the `reason` label
// on ateom_relay_datapoints_rejected.
const (
	reasonGaugeUndeclared = "gauge_no_aggregation_declared"
	reasonBucketMismatch  = "histogram_bucket_mismatch"
	reasonSeriesBudget    = "series_budget_exhausted"
)

type kind int

const (
	kindSum kind = iota
	kindGauge
	kindHist
)

type aggKey struct {
	res      string
	scope    string
	scopeVer string
	metric   string
	unit     string
	desc     string
	kind     kind
	attrs    string
}

type aggSeries struct {
	resAttrs  []*commonpb.KeyValue
	attrs     []*commonpb.KeyValue
	monotonic bool
	start     uint64
	end       uint64

	// sum / gauge
	isInt    bool
	sumInt   int64
	sumFloat float64
	n        int64 // datapoints merged, for GaugeAvg

	// histogram
	histCount   uint64
	histSum     float64
	histBounds  []float64
	histBuckets []uint64
	histMin     *float64
	histMax     *float64

	exemplars []*metricspb.Exemplar
}

// aggregator merges delta datapoints from many actors into bounded-cardinality
// series for one flush interval.
//
// Note what the key is *not*: actor_uid. Identity is replaced with the bounded
// set before a datapoint gets here (see metricResource), so writers are already
// merged at ingest. That is correct and cheaper for delta sums and histograms,
// where addition is associative and a datapoint is self-contained.
//
// It is NOT correct for gauges. A gauge datapoint is an instantaneous reading,
// so the right combine is last-value-per-writer and then the declared rule
// across writers. Keyed this way, an actor exporting queue_depth=4 three times
// inside one relay interval contributes 12 under GaugeSum, and n counts
// datapoints rather than writers under GaugeAvg. The relay interval and the
// actor's export interval are independent, so this is reachable in normal
// operation.
//
// TODO: add a per-writer inner key for kindGauge only, holding the last value
// per actor_uid, and collapse it in drain. See §7.4.1 of the design.
//
// Two limits, because there are two ways to multiply series and the cheap one
// only stops the first:
//
//   - maxSeriesPerMetric bounds distinct actor-supplied datapoint attribute
//     sets *within* one (resource, metric name) pair, folding the rest into
//     otel.metric.overflow so the total still adds up.
//   - maxSeriesTotal bounds the number of pairs. Metric name and scope name
//     are actor-controlled and are upstream of the per-metric cap, so without
//     this an actor emitting N distinct metric names holds N accumulators and
//     exports N series no matter what the per-metric cap says.
type aggregator struct {
	mu       sync.Mutex
	series   map[aggKey]*aggSeries
	distinct map[string]map[string]struct{} // res+metric -> attr sets seen

	maxSeriesPerMetric int
	maxSeriesTotal     int
	maxExemplars       int
	gaugePolicy        map[string]GaugeAggregation
}

func newAggregator(maxSeries, maxTotal, maxExemplars int, policy map[string]GaugeAggregation) *aggregator {
	return &aggregator{
		series:             map[aggKey]*aggSeries{},
		distinct:           map[string]map[string]struct{}{},
		maxSeriesPerMetric: maxSeries,
		maxSeriesTotal:     maxTotal,
		maxExemplars:       maxExemplars,
		gaugePolicy:        policy,
	}
}

// rejection describes datapoints the relay refused, so the caller can report
// them back to the actor via OTLP partial success.
type rejection struct {
	reason string
	metric string
	count  int64
}

// addSum merges one delta Sum datapoint. It returns "" on success, otherwise
// the reason the datapoint was refused.
func (a *aggregator) addSum(resAttrs []*commonpb.KeyValue, scope *commonpb.InstrumentationScope, m *metricspb.Metric, monotonic bool, dp *metricspb.NumberDataPoint) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, ok := a.lookup(resAttrs, scope, m, kindSum, dp.GetAttributes(), dp.GetStartTimeUnixNano(), dp.GetTimeUnixNano())
	if !ok {
		return reasonSeriesBudget
	}
	s.monotonic = monotonic
	a.accumulateNumber(s, dp)
	return ""
}

// addGauge merges one Gauge datapoint under the declared policy. It returns ""
// on success, otherwise the reason the datapoint was refused.
func (a *aggregator) addGauge(resAttrs []*commonpb.KeyValue, scope *commonpb.InstrumentationScope, m *metricspb.Metric, dp *metricspb.NumberDataPoint) string {
	policy, ok := a.gaugePolicy[m.GetName()]
	if !ok || policy == GaugeDrop {
		return reasonGaugeUndeclared
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	s, ok := a.lookup(resAttrs, scope, m, kindGauge, dp.GetAttributes(), dp.GetStartTimeUnixNano(), dp.GetTimeUnixNano())
	if !ok {
		return reasonSeriesBudget
	}
	a.accumulateNumber(s, dp)
	return ""
}

// addHistogram merges one delta Histogram datapoint. It returns "" on success,
// otherwise the reason the datapoint was refused.
func (a *aggregator) addHistogram(resAttrs []*commonpb.KeyValue, scope *commonpb.InstrumentationScope, m *metricspb.Metric, dp *metricspb.HistogramDataPoint) string {
	a.mu.Lock()
	defer a.mu.Unlock()
	s, ok := a.lookup(resAttrs, scope, m, kindHist, dp.GetAttributes(), dp.GetStartTimeUnixNano(), dp.GetTimeUnixNano())
	if !ok {
		return reasonSeriesBudget
	}
	if s.histBounds == nil && s.histCount == 0 && len(s.histBuckets) == 0 {
		s.histBounds = append([]float64(nil), dp.GetExplicitBounds()...)
		s.histBuckets = make([]uint64, len(dp.GetBucketCounts()))
	}
	if !sameBounds(s.histBounds, dp.GetExplicitBounds()) || len(s.histBuckets) != len(dp.GetBucketCounts()) {
		return reasonBucketMismatch
	}
	s.histCount += dp.GetCount()
	s.histSum += dp.GetSum()
	for i, c := range dp.GetBucketCounts() {
		s.histBuckets[i] += c
	}
	if dp.Min != nil && (s.histMin == nil || *dp.Min < *s.histMin) {
		v := dp.GetMin()
		s.histMin = &v
	}
	if dp.Max != nil && (s.histMax == nil || *dp.Max > *s.histMax) {
		v := dp.GetMax()
		s.histMax = &v
	}
	a.addExemplars(s, dp.GetExemplars())
	return ""
}

func (a *aggregator) accumulateNumber(s *aggSeries, dp *metricspb.NumberDataPoint) {
	switch v := dp.Value.(type) {
	case *metricspb.NumberDataPoint_AsInt:
		if s.n == 0 {
			s.isInt = true
		}
		if s.isInt {
			s.sumInt += v.AsInt
		} else {
			s.sumFloat += float64(v.AsInt)
		}
	case *metricspb.NumberDataPoint_AsDouble:
		if s.isInt {
			// Mixed representation for one series: fall back to float.
			s.sumFloat = float64(s.sumInt)
			s.sumInt = 0
			s.isInt = false
		}
		s.sumFloat += v.AsDouble
	}
	s.n++
	a.addExemplars(s, dp.GetExemplars())
}

func (a *aggregator) addExemplars(s *aggSeries, ex []*metricspb.Exemplar) {
	// Exemplars carry the trace ID, so a spike on an aggregate graph links
	// through to the trace and therefore to the specific actor. That is what
	// preserves per-actor debugging without per-actor series.
	for _, e := range ex {
		if len(s.exemplars) >= a.maxExemplars {
			return
		}
		s.exemplars = append(s.exemplars, e)
	}
}

// overflowAttrs is the single datapoint attribute set every capped series
// collapses onto.
func overflowAttrs() []*commonpb.KeyValue {
	return []*commonpb.KeyValue{boolean(OverflowKey, true)}
}

// lookup returns the accumulator for one output series, or false if the relay
// refused to open one. Caller holds a.mu.
func (a *aggregator) lookup(resAttrs []*commonpb.KeyValue, scope *commonpb.InstrumentationScope, m *metricspb.Metric, k kind, dpAttrs []*commonpb.KeyValue, start, end uint64) (*aggSeries, bool) {
	resKey := canonical(resAttrs)
	attrs, _ := stripReserved(dpAttrs) // forgery is possible per-datapoint too
	attrKey := canonical(attrs)

	keyFor := func(ak string) aggKey {
		return aggKey{
			res:      resKey,
			scope:    scope.GetName(),
			scopeVer: scope.GetVersion(),
			metric:   m.GetName(),
			unit:     m.GetUnit(),
			desc:     m.GetDescription(),
			kind:     k,
			attrs:    ak,
		}
	}
	touch := func(s *aggSeries) (*aggSeries, bool) {
		if start != 0 && (s.start == 0 || start < s.start) {
			s.start = start
		}
		if end > s.end {
			s.end = end
		}
		return s, true
	}

	// Global budget, checked before anything is allocated. The per-metric cap
	// below only bounds attribute sets inside a fixed (resource, metric) pair;
	// metric name and scope name are actor-controlled and multiply the pairs
	// themselves. Once the budget is spent the relay stops opening
	// accumulators — series already open keep taking datapoints, so a
	// well-behaved metric is not starved by a noisy neighbour, and the refused
	// datapoints come back to the actor as partial success.
	//
	// Unlike the per-metric cap this drops rather than folds, and it has to:
	// the overflow series preserves a total only when the datapoints being
	// folded belong to the same metric. Summing work_items into latency_ms
	// would produce a number worse than no number.
	if a.maxSeriesTotal > 0 && len(a.series) >= a.maxSeriesTotal {
		if s, ok := a.series[keyFor(attrKey)]; ok {
			return touch(s)
		}
		if s, ok := a.series[keyFor(canonical(overflowAttrs()))]; ok {
			return touch(s)
		}
		return nil, false
	}

	// Cardinality limit: actor-supplied datapoint attributes are not bounded
	// by anything the platform controls, so cap distinct sets per metric and
	// fold the rest into one overflow series.
	metricKey := resKey + "\x1e" + m.GetName()
	seen := a.distinct[metricKey]
	if seen == nil {
		seen = map[string]struct{}{}
		a.distinct[metricKey] = seen
	}
	if _, ok := seen[attrKey]; !ok {
		// The limit counts the overflow series itself, so the cap is a hard
		// ceiling on output series per metric, matching the OTel spec.
		if len(seen) >= a.maxSeriesPerMetric-1 {
			attrs = overflowAttrs()
			attrKey = canonical(attrs)
		}
		seen[attrKey] = struct{}{}
	}

	key := keyFor(attrKey)
	s, ok := a.series[key]
	if !ok {
		s = &aggSeries{resAttrs: resAttrs, attrs: attrs, start: start, end: end}
		a.series[key] = s
	}
	return touch(s)
}

// drain builds the OTLP payload for this interval and resets state. Delta
// temporality is what makes this safe: each interval's output is
// self-contained, so contributions from different workers simply add
// downstream.
func (a *aggregator) drain() []*metricspb.ResourceMetrics {
	a.mu.Lock()
	series := a.series
	a.series = map[aggKey]*aggSeries{}
	a.distinct = map[string]map[string]struct{}{}
	a.mu.Unlock()

	type scopeBucket struct {
		scope   *commonpb.InstrumentationScope
		metrics map[string]*metricspb.Metric
		order   []string
	}
	type resBucket struct {
		attrs  []*commonpb.KeyValue
		scopes map[string]*scopeBucket
		order  []string
	}
	resources := map[string]*resBucket{}
	var resOrder []string

	for key, s := range series {
		rb := resources[key.res]
		if rb == nil {
			rb = &resBucket{attrs: s.resAttrs, scopes: map[string]*scopeBucket{}}
			resources[key.res] = rb
			resOrder = append(resOrder, key.res)
		}
		scopeID := key.scope + "\x1f" + key.scopeVer
		sb := rb.scopes[scopeID]
		if sb == nil {
			sb = &scopeBucket{
				scope:   &commonpb.InstrumentationScope{Name: key.scope, Version: key.scopeVer},
				metrics: map[string]*metricspb.Metric{},
			}
			rb.scopes[scopeID] = sb
			rb.order = append(rb.order, scopeID)
		}
		metricID := fmt.Sprintf("%s\x1f%s\x1f%s\x1f%d", key.metric, key.unit, key.desc, key.kind)
		m := sb.metrics[metricID]
		if m == nil {
			m = &metricspb.Metric{Name: key.metric, Unit: key.unit, Description: key.desc}
			switch key.kind {
			case kindSum:
				m.Data = &metricspb.Metric_Sum{Sum: &metricspb.Sum{
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
					IsMonotonic:            s.monotonic,
				}}
			case kindGauge:
				m.Data = &metricspb.Metric_Gauge{Gauge: &metricspb.Gauge{}}
			case kindHist:
				m.Data = &metricspb.Metric_Histogram{Histogram: &metricspb.Histogram{
					AggregationTemporality: metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_DELTA,
				}}
			}
			sb.metrics[metricID] = m
			sb.order = append(sb.order, metricID)
		}

		switch key.kind {
		case kindSum, kindGauge:
			dp := &metricspb.NumberDataPoint{
				Attributes:        s.attrs,
				StartTimeUnixNano: s.start,
				TimeUnixNano:      s.end,
				Exemplars:         s.exemplars,
			}
			value := s.sumFloat
			if s.isInt {
				value = float64(s.sumInt)
			}
			if key.kind == kindGauge && a.gaugePolicy[key.metric] == GaugeAvg && s.n > 0 {
				dp.Value = &metricspb.NumberDataPoint_AsDouble{AsDouble: value / float64(s.n)}
			} else if s.isInt {
				dp.Value = &metricspb.NumberDataPoint_AsInt{AsInt: s.sumInt}
			} else {
				dp.Value = &metricspb.NumberDataPoint_AsDouble{AsDouble: s.sumFloat}
			}
			switch d := m.Data.(type) {
			case *metricspb.Metric_Sum:
				d.Sum.DataPoints = append(d.Sum.DataPoints, dp)
			case *metricspb.Metric_Gauge:
				d.Gauge.DataPoints = append(d.Gauge.DataPoints, dp)
			}
		case kindHist:
			sum := s.histSum
			dp := &metricspb.HistogramDataPoint{
				Attributes:        s.attrs,
				StartTimeUnixNano: s.start,
				TimeUnixNano:      s.end,
				Count:             s.histCount,
				Sum:               &sum,
				BucketCounts:      s.histBuckets,
				ExplicitBounds:    s.histBounds,
				Min:               s.histMin,
				Max:               s.histMax,
				Exemplars:         s.exemplars,
			}
			if d, ok := m.Data.(*metricspb.Metric_Histogram); ok {
				d.Histogram.DataPoints = append(d.Histogram.DataPoints, dp)
			}
		}
	}

	out := make([]*metricspb.ResourceMetrics, 0, len(resources))
	for _, resKey := range resOrder {
		rb := resources[resKey]
		rm := &metricspb.ResourceMetrics{Resource: &resourcepb.Resource{Attributes: rb.attrs}}
		for _, scopeID := range rb.order {
			sb := rb.scopes[scopeID]
			sm := &metricspb.ScopeMetrics{Scope: sb.scope}
			for _, mid := range sb.order {
				sm.Metrics = append(sm.Metrics, sb.metrics[mid])
			}
			rm.ScopeMetrics = append(rm.ScopeMetrics, sm)
		}
		out = append(out, rm)
	}
	return out
}

func sameBounds(a, b []float64) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
