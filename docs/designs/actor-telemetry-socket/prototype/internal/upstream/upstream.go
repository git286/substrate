// Package upstream is a recording stand-in for the cluster collector and the
// metrics backend behind it. It stores what it receives and answers the
// questions the design's test plan asks: how many series, what totals, how
// many distinct service.instance.id values.
package upstream

import (
	"context"
	"fmt"
	"net"
	"sort"
	"strings"
	"sync"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	commonpb "go.opentelemetry.io/proto/otlp/common/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
)

// Collector accumulates delta datapoints the way a delta-aware backend would:
// each series is a key, and each arriving delta adds to the stored total.
type Collector struct {
	mu         sync.Mutex
	totals     map[string]float64
	counts     map[string]int64
	promTotals map[string]float64
	promLast   map[string]float64
	spans      []*tracepb.ResourceSpans
	logs       []*logspb.ResourceLogs
	requests   int64
}

func NewCollector() *Collector {
	return &Collector{
		totals:     map[string]float64{},
		counts:     map[string]int64{},
		promTotals: map[string]float64{},
		promLast:   map[string]float64{},
	}
}

func (c *Collector) Register(gs *grpc.Server) {
	colmetricspb.RegisterMetricsServiceServer(gs, metricsSrv{c: c})
	coltracepb.RegisterTraceServiceServer(gs, traceSrv{c: c})
	collogspb.RegisterLogsServiceServer(gs, logsSrv{c: c})
}

type metricsSrv struct {
	colmetricspb.UnimplementedMetricsServiceServer
	c *Collector
}

type traceSrv struct {
	coltracepb.UnimplementedTraceServiceServer
	c *Collector
}

type logsSrv struct {
	collogspb.UnimplementedLogsServiceServer
	c *Collector
}

func (s metricsSrv) Export(ctx context.Context, req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	return s.c.ExportMetricsRequest(req)
}

func (s traceSrv) Export(ctx context.Context, req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	return s.c.ExportTraceRequest(req)
}

func (s logsSrv) Export(ctx context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	return s.c.ExportLogsRequest(req)
}

func (c *Collector) ExportMetricsRequest(req *colmetricspb.ExportMetricsServiceRequest) (*colmetricspb.ExportMetricsServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.requests++
	for _, rm := range req.GetResourceMetrics() {
		res := attrString(rm.GetResource().GetAttributes())
		for _, sm := range rm.GetScopeMetrics() {
			for _, m := range sm.GetMetrics() {
				switch d := m.GetData().(type) {
				case *metricspb.Metric_Sum:
					cumulative := d.Sum.GetAggregationTemporality() == metricspb.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE
					for _, dp := range d.Sum.GetDataPoints() {
						k := seriesKey(res, m.GetName(), dp.GetAttributes())
						if cumulative {
							c.addCumulative(k, numberValue(dp))
							continue
						}
						c.add(k, numberValue(dp))
					}
				case *metricspb.Metric_Gauge:
					for _, dp := range d.Gauge.GetDataPoints() {
						// A gauge is a last-value, not a total.
						k := seriesKey(res, m.GetName(), dp.GetAttributes())
						c.totals[k] = numberValue(dp)
						c.counts[k]++
					}
				case *metricspb.Metric_Histogram:
					for _, dp := range d.Histogram.GetDataPoints() {
						c.add(seriesKey(res, m.GetName()+"_count", dp.GetAttributes()), float64(dp.GetCount()))
						c.add(seriesKey(res, m.GetName()+"_sum", dp.GetAttributes()), dp.GetSum())
					}
				}
			}
		}
	}
	return &colmetricspb.ExportMetricsServiceResponse{}, nil
}

func (c *Collector) ExportTraceRequest(req *coltracepb.ExportTraceServiceRequest) (*coltracepb.ExportTraceServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.spans = append(c.spans, req.GetResourceSpans()...)
	return &coltracepb.ExportTraceServiceResponse{}, nil
}

func (c *Collector) ExportLogsRequest(req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.logs = append(c.logs, req.GetResourceLogs()...)
	return &collogspb.ExportLogsServiceResponse{}, nil
}

func (c *Collector) add(key string, v float64) {
	c.totals[key] += v
	c.counts[key]++
}

// addCumulative is what a Prometheus-style backend does with a cumulative
// counter: increase() over the series, treating any decrease as a counter
// restart and counting the new value in full. With many writers on one series
// that is precisely how totals inflate.
func (c *Collector) addCumulative(key string, v float64) {
	last, seen := c.promLast[key]
	switch {
	case !seen || v < last:
		c.promTotals[key] += v // restart: the whole value counts as an increase
	default:
		c.promTotals[key] += v - last
	}
	c.promLast[key] = v
	c.counts[key]++
}

// PromTotal returns what a backend would graph for a cumulative counter.
func (c *Collector) PromTotal(metric string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var sum float64
	for k, v := range c.promTotals {
		if seriesMetric(k) == metric {
			sum += v
		}
	}
	return sum
}

// PromSeries returns the stored cumulative series keys.
func (c *Collector) PromSeries(metric string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for k := range c.promTotals {
		if seriesMetric(k) == metric {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// ------------------------------------------------------------------ queries

// Total returns the accumulated value of a single series, matched by metric
// name and a substring of the series key (use "" to sum every series of that
// metric).
func (c *Collector) Total(metric, match string) float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	var sum float64
	for k, v := range c.totals {
		if seriesMetric(k) == metric && strings.Contains(k, match) {
			sum += v
		}
	}
	return sum
}

// SeriesFor returns the stored series keys for one metric — this is the thing
// the cardinality bound is about.
func (c *Collector) SeriesFor(metric string) []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	var out []string
	for k := range c.totals {
		if seriesMetric(k) == metric {
			out = append(out, k)
		}
	}
	sort.Strings(out)
	return out
}

// AllSeries returns every stored series key.
func (c *Collector) AllSeries() []string {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make([]string, 0, len(c.totals))
	for k := range c.totals {
		out = append(out, k)
	}
	sort.Strings(out)
	return out
}

// TraceResourceAttr collects the distinct values of one resource attribute
// across all received spans.
func (c *Collector) TraceResourceAttr(key string) map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for _, rs := range c.spans {
		v, ok := lookup(rs.GetResource().GetAttributes(), key)
		if !ok {
			continue
		}
		n := 0
		for _, ss := range rs.GetScopeSpans() {
			n += len(ss.GetSpans())
		}
		out[v] += n
	}
	return out
}

// SpanAttrValues returns distinct values of a span-level attribute.
func (c *Collector) SpanAttrValues(key string) map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for _, rs := range c.spans {
		for _, ss := range rs.GetScopeSpans() {
			for _, sp := range ss.GetSpans() {
				if v, ok := lookup(sp.GetAttributes(), key); ok {
					out[v]++
				}
			}
		}
	}
	return out
}

// LogResourceAttr mirrors TraceResourceAttr for logs.
func (c *Collector) LogResourceAttr(key string) map[string]int {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := map[string]int{}
	for _, rl := range c.logs {
		v, ok := lookup(rl.GetResource().GetAttributes(), key)
		if !ok {
			continue
		}
		n := 0
		for _, sl := range rl.GetScopeLogs() {
			n += len(sl.GetLogRecords())
		}
		out[v] += n
	}
	return out
}

// Totals returns every stored series and its accumulated value.
func (c *Collector) Totals() map[string]float64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	out := make(map[string]float64, len(c.totals))
	for k, v := range c.totals {
		out[k] = v
	}
	return out
}

// SpanCount returns the number of spans received.
func (c *Collector) SpanCount() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	n := 0
	for _, rs := range c.spans {
		for _, ss := range rs.GetScopeSpans() {
			n += len(ss.GetSpans())
		}
	}
	return n
}

func (c *Collector) Requests() int64 {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.requests
}

// Reset clears accumulated state — used to measure "what happened after this
// point" in the golden-baseline and migration tests.
func (c *Collector) Reset() {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.totals = map[string]float64{}
	c.counts = map[string]int64{}
	c.promTotals = map[string]float64{}
	c.promLast = map[string]float64{}
	c.spans = nil
	c.logs = nil
}

// ------------------------------------------------------------------ serving

// Serve starts the collector on a loopback address and returns it with a stop
// function.
func Serve() (*Collector, string, func(), error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return nil, "", nil, err
	}
	c := NewCollector()
	gs := grpc.NewServer()
	c.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	return c, lis.Addr().String(), gs.Stop, nil
}

// ------------------------------------------------------------------ helpers

func seriesKey(res, metric string, attrs []*commonpb.KeyValue) string {
	return metric + "{" + res + "|" + attrString(attrs) + "}"
}

func seriesMetric(key string) string {
	if i := strings.IndexByte(key, '{'); i >= 0 {
		return key[:i]
	}
	return key
}

func attrString(attrs []*commonpb.KeyValue) string {
	parts := make([]string, 0, len(attrs))
	for _, kv := range attrs {
		parts = append(parts, kv.GetKey()+"="+valueString(kv.GetValue()))
	}
	sort.Strings(parts)
	return strings.Join(parts, ",")
}

func lookup(attrs []*commonpb.KeyValue, key string) (string, bool) {
	for _, kv := range attrs {
		if kv.GetKey() == key {
			return valueString(kv.GetValue()), true
		}
	}
	return "", false
}

func valueString(v *commonpb.AnyValue) string {
	switch t := v.GetValue().(type) {
	case *commonpb.AnyValue_StringValue:
		return t.StringValue
	case *commonpb.AnyValue_BoolValue:
		return fmt.Sprintf("%t", t.BoolValue)
	case *commonpb.AnyValue_IntValue:
		return fmt.Sprintf("%d", t.IntValue)
	case *commonpb.AnyValue_DoubleValue:
		return fmt.Sprintf("%g", t.DoubleValue)
	default:
		return ""
	}
}

// numberValue reads either representation of an OTLP number datapoint.
func numberValue(dp *metricspb.NumberDataPoint) float64 {
	switch v := dp.GetValue().(type) {
	case *metricspb.NumberDataPoint_AsInt:
		return float64(v.AsInt)
	case *metricspb.NumberDataPoint_AsDouble:
		return v.AsDouble
	}
	return 0
}
