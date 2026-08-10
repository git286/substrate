// Package harness wires the three pieces together — worker pod (ateom relay),
// upstream collector, actor — so tests and the demo build the same topology.
package harness

import (
	"fmt"
	"log/slog"
	"net"
	"sync"
	"time"

	"google.golang.org/grpc"

	"github.com/agent-substrate/ate-identity-prototype/internal/identity"
	"github.com/agent-substrate/ate-identity-prototype/internal/relay"
)

// Worker is one worker pod: a gRPC server on the (constant, per-pod) actor-
// facing address, serving the relay's OTLP receiver.
type Worker struct {
	Relay *relay.Relay
	Addr  string

	gs        *grpc.Server
	forwarder *relay.GRPCForwarder
	stopOnce  sync.Once
}

// WorkerConfig configures a worker pod.
type WorkerConfig struct {
	// Addr is the actor-facing listen address. In Substrate this is always
	// 169.254.17.1:4317 inside the worker pod's netns; in tests it is a
	// loopback address held constant across a migration.
	Addr string

	// UpstreamAddr is the cluster collector.
	UpstreamAddr string

	Worker      identity.Worker
	Interval    time.Duration
	GaugePolicy map[string]relay.GaugeAggregation
	MaxSeries   int
	MaxTotal    int
	Logger      *slog.Logger
}

// StartWorker brings up a worker pod. Pass Addr == "" for an ephemeral port.
func StartWorker(cfg WorkerConfig) (*Worker, error) {
	addr := cfg.Addr
	if addr == "" {
		addr = "127.0.0.1:0"
	}
	lis, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, fmt.Errorf("listen %s: %w", addr, err)
	}
	fwd, err := relay.Dial(cfg.UpstreamAddr)
	if err != nil {
		_ = lis.Close()
		return nil, err
	}
	r := relay.New(relay.Config{
		Worker:             cfg.Worker,
		Upstream:           fwd,
		Interval:           cfg.Interval,
		GaugePolicy:        cfg.GaugePolicy,
		MaxSeriesPerMetric: cfg.MaxSeries,
		MaxSeriesTotal:     cfg.MaxTotal,
		Logger:             cfg.Logger,
	})
	gs := grpc.NewServer()
	r.Register(gs)
	go func() { _ = gs.Serve(lis) }()
	r.Start()
	return &Worker{Relay: r, Addr: lis.Addr().String(), gs: gs, forwarder: fwd}, nil
}

// Stop tears the worker pod down: final flush, then the listener goes away —
// which is exactly what an actor sees when it is suspended and moved.
func (w *Worker) Stop() {
	w.stopOnce.Do(func() {
		w.Relay.Close()
		w.gs.Stop()
		_ = w.forwarder.Close()
	})
}

// FreeAddr reserves a loopback address and releases it, so a caller can bind
// the same address again later (the constant-address property, modelled).
func FreeAddr() (string, error) {
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		return "", err
	}
	addr := lis.Addr().String()
	return addr, lis.Close()
}

// WaitDialable blocks until something is listening on addr.
func WaitDialable(addr string, timeout time.Duration) error {
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 200*time.Millisecond)
		if err == nil {
			_ = conn.Close()
			return nil
		}
		time.Sleep(10 * time.Millisecond)
	}
	return fmt.Errorf("nothing listening on %s after %s", addr, timeout)
}
