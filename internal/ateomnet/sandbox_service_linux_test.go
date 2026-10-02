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

package ateomnet

import (
	"context"
	"io"
	"net"
	"strconv"
	"sync"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/actorotlp"
	"github.com/agent-substrate/substrate/internal/ateomnet/netns"
	"github.com/agent-substrate/substrate/internal/roottest"
)

// echoService is a SandboxService that answers each connection with the actor
// it was bound to, which is the property the telemetry relay rests on: a
// listener inside the sandbox's namespace identifies the sender.
type echoService struct {
	ports []uint16
	mu    sync.Mutex
	bound []string
}

func (e *echoService) Ports() []uint16 { return e.ports }

func (e *echoService) Bind(actorUID string) (func(context.Context, net.Listener) error, error) {
	e.mu.Lock()
	e.bound = append(e.bound, actorUID)
	e.mu.Unlock()
	return func(ctx context.Context, l net.Listener) error {
		for {
			c, err := l.Accept()
			if err != nil {
				return nil
			}
			go func() {
				defer c.Close()
				_, _ = io.WriteString(c, actorUID)
			}()
		}
	}, nil
}

// The actor reaches a SandboxService at the gateway address on the service's
// own port, past the egress redirect, and the service sees the actor it was
// bound to before serving.
func TestSandboxServiceIsReachableFromTheActor(t *testing.T) {
	roottest.Require(t, "creates network namespaces")
	ctx := context.Background()
	const actorUID = "88888888-8888-8888-8888-888888888888"

	// Two ports, so a service that names more than one is covered; the relay
	// itself names one.
	svc := &echoService{ports: []uint16{actorotlp.Port, actorotlp.Port + 1}}
	session, err := ServeSandbox(ctx, SandboxNetworkConfig{
		ActorUID: actorUID, Veth: true, EgressPort: testEgressPort,
	}, nil, nil, svc)
	if err != nil {
		t.Fatalf("ServeSandbox: %v", err)
	}
	t.Cleanup(func() { _ = session.Close(ctx) })

	svc.mu.Lock()
	bound := append([]string(nil), svc.bound...)
	svc.mu.Unlock()
	if len(bound) != 1 || bound[0] != actorUID {
		t.Fatalf("Bind was called for %v, want exactly [%s] before serving", bound, actorUID)
	}

	for _, port := range svc.ports {
		target := net.JoinHostPort(ActorVethGateway, strconv.Itoa(int(port)))
		var got string
		if err := netns.Do(ctx, session.Network.RuntimeNetNS, func(context.Context) error {
			c, err := net.DialTimeout("tcp", target, 3*time.Second)
			if err != nil {
				return err
			}
			defer c.Close()
			b, err := io.ReadAll(c)
			got = string(b)
			return err
		}); err != nil {
			t.Fatalf("actor dial to %s: %v", target, err)
		}
		if got != actorUID {
			t.Errorf("the service on %s answered %q, want the bound actor %q", target, got, actorUID)
		}
	}

	// Closing the session closes the service's listeners with the rest.
	if err := session.Close(ctx); err != nil {
		t.Fatalf("Close: %v", err)
	}
}

// The relay tells atelet where to point actors by a constant it cannot import
// from here; the two must not drift.
func TestGatewayAddressMatchesAteomnet(t *testing.T) {
	if actorotlp.GatewayAddress != ActorVethGateway {
		t.Errorf("actorotlp.GatewayAddress = %q, ateomnet.ActorVethGateway = %q; actors would be pointed at the wrong address", actorotlp.GatewayAddress, ActorVethGateway)
	}
}
