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

package actornet_test

import (
	"context"
	"net"
	"testing"

	"go.opentelemetry.io/otel/metric/noop"
	collogspb "go.opentelemetry.io/proto/otlp/collector/logs/v1"
	logspb "go.opentelemetry.io/proto/otlp/logs/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"

	"go.ate.dev/substrate/internal/actorotlp"
	"go.ate.dev/substrate/internal/ateworker/actornet"
	"go.ate.dev/substrate/internal/netns"
	"go.ate.dev/substrate/internal/resources"
	"go.ate.dev/substrate/internal/roottest"
)

// actorUIDSink stands in for atelet's relay socket. It records the
// ate.actor.uid of each forwarded export.
type actorUIDSink struct {
	collogspb.UnimplementedLogsServiceServer
	uids chan string
}

func (s *actorUIDSink) Export(_ context.Context, req *collogspb.ExportLogsServiceRequest) (*collogspb.ExportLogsServiceResponse, error) {
	for _, attr := range req.GetResourceLogs()[0].GetResource().GetAttributes() {
		if attr.GetKey() == "ate.actor.uid" {
			s.uids <- attr.GetValue().GetStringValue()
		}
	}
	return &collogspb.ExportLogsServiceResponse{}, nil
}

// TestActorsReachOnlyTheirOwnTelemetryListener hosts two actors the way the
// workers do. Both dial the same gateway address from their own namespace,
// with no egress listener, so a connection the egress redirect caught would
// fail.
func TestActorsReachOnlyTheirOwnTelemetryListener(t *testing.T) {
	roottest.Require(t, "creates network namespaces")

	sink := &actorUIDSink{uids: make(chan string, 4)}
	lis, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	srv := grpc.NewServer()
	collogspb.RegisterLogsServiceServer(srv, sink)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)
	upstream, err := grpc.NewClient(lis.Addr().String(), grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = upstream.Close() })
	relay, err := actorotlp.New(upstream, actorotlp.Worker{PodUID: "pod-uid"}, 2, noop.NewMeterProvider().Meter("test"))
	if err != nil {
		t.Fatal(err)
	}

	hostActor := func(uid string) (*actornet.SandboxSession, collogspb.LogsServiceClient) {
		t.Helper()
		if err := relay.Register(resources.ActorAttribution{UID: uid}); err != nil {
			t.Fatal(err)
		}
		// gVisor's shape. A microVM has one namespace and a tap the guest
		// owns, which this test has no guest to drive.
		session, err := actornet.ServeSandbox(t.Context(), actornet.SandboxNetworkConfig{
			ActorUID: uid, Veth: true, EgressPort: 15001,
		}, nil, nil, relay)
		if err != nil {
			t.Fatalf("ServeSandbox: %v", err)
		}
		t.Cleanup(func() { _ = session.Close(context.Background()) })
		// The actor's side of the network, where its exporter dials from.
		dial := netns.Dialer(session.Network.RuntimeNetNS)
		conn, err := grpc.NewClient("passthrough:///"+net.JoinHostPort(actorotlp.GatewayAddress, "4317"),
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithContextDialer(func(ctx context.Context, addr string) (net.Conn, error) {
				return dial(ctx, "tcp", addr)
			}))
		if err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = conn.Close() })
		return session, collogspb.NewLogsServiceClient(conn)
	}
	const uidA, uidB = "aaaaaaaa-1111-4111-8111-111111111111", "bbbbbbbb-2222-4222-8222-222222222222"
	sessionA, clientA := hostActor(uidA)
	_, clientB := hostActor(uidB)

	export := func(client collogspb.LogsServiceClient) error {
		_, err := client.Export(t.Context(), &collogspb.ExportLogsServiceRequest{ResourceLogs: []*logspb.ResourceLogs{{}}})
		return err
	}
	for _, tc := range []struct {
		client collogspb.LogsServiceClient
		want   string
	}{{clientA, uidA}, {clientB, uidB}} {
		if err := export(tc.client); err != nil {
			t.Fatalf("Export: %v", err)
		}
		if got := <-sink.uids; got != tc.want {
			t.Errorf("forwarded ate.actor.uid = %q, want %q", got, tc.want)
		}
	}

	// Teardown closes the connection actor A holds.
	if err := sessionA.Close(t.Context()); err != nil {
		t.Fatalf("Close: %v", err)
	}
	if err := export(clientA); status.Code(err) != codes.Unavailable {
		t.Errorf("export after teardown = %v, want Unavailable", err)
	}
	if err := export(clientB); err != nil {
		t.Errorf("the other actor's export after teardown: %v", err)
	}
}
