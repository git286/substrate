package relay

import (
	"context"

	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	colmetricspb "go.opentelemetry.io/proto/otlp/collector/metrics/v1"
	coltracepb "go.opentelemetry.io/proto/otlp/collector/trace/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	metricspb "go.opentelemetry.io/proto/otlp/metrics/v1"
	tracepb "go.opentelemetry.io/proto/otlp/trace/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// GRPCForwarder ships stamped telemetry to the cluster collector over OTLP.
// Direct, not via atelet (open question 5): one fewer hop, and it keeps the
// actor-facing surface inside the pod that already owns the actor's blast
// radius.
type GRPCForwarder struct {
	conn    *grpc.ClientConn
	metrics colmetricspb.MetricsServiceClient
	traces  coltracepb.TraceServiceClient
	logs    collogspb.LogsServiceClient
}

func Dial(target string) (*GRPCForwarder, error) {
	conn, err := grpc.NewClient(target, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, err
	}
	return &GRPCForwarder{
		conn:    conn,
		metrics: colmetricspb.NewMetricsServiceClient(conn),
		traces:  coltracepb.NewTraceServiceClient(conn),
		logs:    collogspb.NewLogsServiceClient(conn),
	}, nil
}

func (f *GRPCForwarder) Close() error { return f.conn.Close() }

func (f *GRPCForwarder) ExportMetrics(ctx context.Context, rms []*metricspb.ResourceMetrics) error {
	_, err := f.metrics.Export(ctx, &colmetricspb.ExportMetricsServiceRequest{ResourceMetrics: rms})
	return err
}

func (f *GRPCForwarder) ExportTraces(ctx context.Context, rss []*tracepb.ResourceSpans) error {
	_, err := f.traces.Export(ctx, &coltracepb.ExportTraceServiceRequest{ResourceSpans: rss})
	return err
}

func (f *GRPCForwarder) ExportLogs(ctx context.Context, rls []*logspb.ResourceLogs) error {
	_, err := f.logs.Export(ctx, &collogspb.ExportLogsServiceRequest{ResourceLogs: rls})
	return err
}
