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

package e2e

import (
	"context"
	"os"
	"testing"

	"github.com/agent-substrate/substrate/pkg/proto/ateapipb"
)

// OTelProbeName is the name of the otelprobe fixture's WorkerPool and
// ActorTemplate, inside the atespace DeployOTelProbe returns. It is also the
// service.name the relay stamps on the probe's telemetry.
const OTelProbeName = "otelprobe"

var otelProbeManifests = SubstrateFixtureManifests{
	Pool:     "internal/e2e/fixtures/otelprobe/otelprobe.yaml.tmpl",
	Template: "internal/e2e/fixtures/otelprobe/otelprobe-template.yaml.tmpl",
}

// DeployOTelProbe builds the otelprobe fixture image and installs the fixture
// for the sandbox class under test, removing it when the test ends. See
// DeployProbe for the naming contract.
func DeployOTelProbe(t *testing.T, bucket, name string) (string, *ateapipb.ActorTemplate) {
	t.Helper()
	atespace, templates := DeploySubstrateFixture(t, context.Background(), GetClients(), otelProbeManifests, bucket, name, false)
	return atespace, templates[0]
}

// collectorScrapeEnv turns the kind stack's collector scrape off for clusters
// that have no such collector, such as GKE with managed OTLP. It is a named
// knob rather than a silent skip: CI runs against kind and leaves it unset, so
// a suite that asserts on the collector fails there if the scrape is gone.
const collectorScrapeEnv = "E2E_COLLECTOR_SCRAPE"

// CollectorScrapeEnabled reports whether the suites may assert on the kind
// stack's collector. Unset means yes.
func CollectorScrapeEnabled() bool {
	return os.Getenv(collectorScrapeEnv) != "false"
}
