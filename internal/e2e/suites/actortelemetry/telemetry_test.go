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

package actortelemetry

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/agent-substrate/substrate/internal/e2e"
	"github.com/agent-substrate/substrate/internal/resources"
	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// otelStatus mirrors the probe's /otel payload: one tally set per exporter
// family, gRPC and HTTP/protobuf, both pointed at the same endpoint.
type otelStatus struct {
	Endpoint string         `json:"endpoint"`
	GRPC     protocolStatus `json:"grpc"`
	HTTP     protocolStatus `json:"http"`
}

type protocolStatus struct {
	Traces  signalStatus `json:"traces"`
	Metrics signalStatus `json:"metrics"`
	Logs    signalStatus `json:"logs"`
}

func (p protocolStatus) signals() map[string]signalStatus {
	return map[string]signalStatus{"traces": p.Traces, "metrics": p.Metrics, "logs": p.Logs}
}

func (p protocolStatus) attemptedAll() bool {
	for _, s := range p.signals() {
		if s.Exports+s.Failures == 0 {
			return false
		}
	}
	return true
}

type signalStatus struct {
	Exports   int64  `json:"exports"`
	Failures  int64  `json:"failures"`
	LastError string `json:"last_error"`
}

// forgedService is what the probe claims to be; see the fixture.
const forgedService = "forged-service"

// TestActorTelemetryIsRelayed asserts the actor telemetry path end to end.
// An actor configured by nothing but the platform's default OTEL_* env
// exports traces, metrics, and logs through the OTel SDK, and every export is
// acknowledged: acknowledgement means ateom accepted it, forwarded it over
// atelet's socket, and the collector took it. Two actors of one template run
// at once, so the path is exercised as the multi-actor worker will use it.
//
// With the kind stack's collector in reach, it also asserts what arrived: the
// template's name as the service, one instance per actor, the actor plane
// stamped, and the probe's forged identity gone.
func TestActorTelemetryIsRelayed(t *testing.T) {
	env, err := e2e.CheckEnv("BUCKET_NAME", "KO_DOCKER_REPO")
	if err != nil {
		t.Fatalf("CheckEnv failed: %v", err)
	}
	ctx := context.Background()
	clients := e2e.GetClients()

	atespace, _ := e2e.DeployOTelProbe(t, env["BUCKET_NAME"], "actortelemetry")

	rc, err := e2e.NewRouterClient(ctx)
	if err != nil {
		t.Fatalf("NewRouterClient: %v", err)
	}
	defer rc.Close()

	actors := []string{"otelprobe-a", "otelprobe-b"}
	for _, name := range actors {
		createAndResumeActor(t, ctx, clients, atespace, e2e.OTelProbeName, name)
	}

	for _, name := range actors {
		t.Run(name, func(t *testing.T) {
			status := awaitExports(t, ctx, rc, atespace, name)
			t.Logf("%s gRPC exports: traces %+v, metrics %+v, logs %+v", name, status.GRPC.Traces, status.GRPC.Metrics, status.GRPC.Logs)
			t.Logf("%s HTTP exports: traces %+v, metrics %+v, logs %+v", name, status.HTTP.Traces, status.HTTP.Metrics, status.HTTP.Logs)
			if !strings.Contains(status.Endpoint, "169.254.17.1:4317") {
				t.Errorf("the actor's OTEL_EXPORTER_OTLP_ENDPOINT is %q, want the sandbox gateway; the platform default did not reach the container env", status.Endpoint)
			}
			for protocol, p := range map[string]protocolStatus{"gRPC": status.GRPC, "HTTP/protobuf": status.HTTP} {
				for signal, s := range p.signals() {
					if s.Failures != 0 {
						t.Errorf("%s %s: %d of %d exports failed, last error %q; every export must be acknowledged through the relay, whichever protocol the SDK speaks at the one endpoint the platform names", protocol, signal, s.Failures, s.Failures+s.Exports, s.LastError)
					}
				}
			}
		})
	}

	if !e2e.CollectorScrapeEnabled() {
		t.Logf("%s=false: the collector's view is not asserted on this cluster; the exporters' acknowledgements above cover the path to it", "E2E_COLLECTOR_SCRAPE")
		return
	}
	// The relay sets service.namespace to the atespace, and the Prometheus
	// exporter renders job as namespace/name. That slash is also what keeps an
	// actor's job from ever colliding with a system component's bare name.
	assertCollectorView(t, ctx, atespace+"/"+e2e.OTelProbeName, len(actors))
}

// awaitExports polls the probe until every signal of both exporter families
// has at least one export, acknowledged or not, and returns the status. The metric reader's interval
// is the slowest of the three; the platform sets it to five seconds.
func awaitExports(t *testing.T, ctx context.Context, rc *e2e.RouterClient, atespace, name string) otelStatus {
	t.Helper()
	deadline := time.Now().Add(2 * time.Minute)
	var last otelStatus
	for time.Now().Before(deadline) {
		last = probeStatus(t, ctx, rc, atespace, name)
		if last.GRPC.attemptedAll() && last.HTTP.attemptedAll() {
			return last
		}
		time.Sleep(2 * time.Second)
	}
	t.Fatalf("actor %s never attempted an export of every signal within the deadline; last status %+v", name, last)
	return last
}

func probeStatus(t *testing.T, ctx context.Context, rc *e2e.RouterClient, atespace, name string) otelStatus {
	t.Helper()
	resp, err := rc.Get(ctx, resources.ActorRef{Atespace: atespace, Name: name}, "/otel")
	if err != nil {
		t.Fatalf("GET /otel for %q: %v", name, err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		t.Fatalf("GET /otel for %q: status %d, body %q", name, resp.StatusCode, body)
	}
	var out otelStatus
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		t.Fatalf("decoding /otel for %q: %v", name, err)
	}
	return out
}

// assertCollectorView reads the kind collector's Prometheus surface. The
// resource attributes land on target_info, one series per (job, instance).
func assertCollectorView(t *testing.T, ctx context.Context, job string, wantInstances int) {
	t.Helper()
	deadline := time.Now().Add(90 * time.Second)
	var scrape string
	var instances map[string]bool
	for time.Now().Before(deadline) {
		var err error
		scrape, err = e2e.ScrapeCollectorMetrics(ctx)
		if err != nil {
			t.Fatalf("ScrapeCollectorMetrics: %v", err)
		}
		instances = targetInfoInstances(scrape, job)
		if len(instances) >= wantInstances {
			break
		}
		time.Sleep(3 * time.Second)
	}
	if len(instances) != wantInstances {
		t.Fatalf("collector holds %d instance(s) of job %q (%v), want %d: each live actor is its own writer", len(instances), job, instances, wantInstances)
	}
	if e2e.CollectorHasService(scrape, forgedService) {
		t.Errorf("collector holds telemetry from service %q; the probe's forged identity reached storage", forgedService)
	}
	if got := e2e.TargetInfoLabel(scrape, job, "ate_telemetry_plane"); got != "actor" {
		t.Errorf("target_info ate_telemetry_plane = %q, want actor", got)
	}
	for _, forbidden := range []string{"ate_actor_uid", "ate_actor_name", "ate_atespace"} {
		if got := e2e.TargetInfoLabel(scrape, job, forbidden); got != "" {
			t.Errorf("target_info carries %s=%q; actor identity is never a metric label", forbidden, got)
		}
	}
	if got := e2e.TargetInfoLabel(scrape, job, "probe_kept"); got != "yes" {
		t.Errorf("target_info probe_kept = %q, want the probe's own attribute kept", got)
	}
	// Both exporter families' counters arrive as delta and must come out of
	// the collector as cumulative series, one per actor.
	for _, metric := range []string{"otelprobe_ticks_grpc_total", "otelprobe_ticks_http_total"} {
		if got := seriesForJob(scrape, metric, job); got != wantInstances {
			t.Errorf("collector exposes %d series of %s for job %q, want %d (one per actor); delta metrics must be converted, not dropped", got, metric, job, wantInstances)
		}
	}
}

// seriesForJob counts the exposition lines of metric that carry job.
func seriesForJob(scrape, metric, job string) int {
	n := 0
	for _, line := range strings.Split(scrape, "\n") {
		if strings.HasPrefix(line, metric+"{") && strings.Contains(line, `job="`+job+`"`) {
			n++
		}
	}
	return n
}

// targetInfoInstances returns the distinct instance labels of a service's
// target_info series.
func targetInfoInstances(scrape, service string) map[string]bool {
	out := map[string]bool{}
	for _, line := range strings.Split(scrape, "\n") {
		if !strings.HasPrefix(line, "target_info{") || !strings.Contains(line, `job="`+service+`"`) {
			continue
		}
		if i := strings.Index(line, `instance="`); i >= 0 {
			rest := line[i+len(`instance="`):]
			if j := strings.Index(rest, `"`); j >= 0 {
				out[rest[:j]] = true
			}
		}
	}
	return out
}

func createAndResumeActor(t *testing.T, ctx context.Context, clients *e2e.Clients, atespace, template, id string) {
	t.Helper()
	if _, err := clients.SubstrateAPI.CreateActor(ctx, &ateapipb.CreateActorRequest{Actor: &ateapipb.Actor{
		Metadata:      &ateapipb.ResourceMetadata{Atespace: atespace, Name: id},
		ActorTemplate: &ateapipb.ObjectRef{Atespace: atespace, Name: template},
	}}); err != nil {
		t.Fatalf("CreateActor %q: %v", id, err)
	}
	t.Cleanup(func() {
		ref := &ateapipb.ObjectRef{Atespace: atespace, Name: id}
		_, _ = clients.SubstrateAPI.SuspendActor(ctx, &ateapipb.SuspendActorRequest{Actor: ref})
		_, _ = clients.SubstrateAPI.DeleteActor(ctx, &ateapipb.DeleteActorRequest{Actor: ref})
	})
	if _, err := e2e.ResumeActorAwaitCapacity(t, ctx, clients, &ateapipb.ResumeActorRequest{
		Actor: &ateapipb.ObjectRef{Atespace: atespace, Name: id},
	}); err != nil {
		t.Fatalf("ResumeActor %q: %v", id, err)
	}
}
