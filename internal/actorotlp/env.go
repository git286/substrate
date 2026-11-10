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

package actorotlp

// GatewayAddress is the sandbox gateway, where the relay answers inside every
// actor's network. It mirrors actornet.ActorVethGateway, which is linux-only.
const GatewayAddress = "169.254.17.1"

// Port serves both OTLP/gRPC and OTLP/HTTP.
const Port uint16 = 4317

// Endpoint is the OTLP endpoint an actor's exporter is given.
const Endpoint = "http://" + GatewayAddress + ":4317"

// ActorEnv is the exporter configuration every actor container starts with.
// The values are the same for every actor, so a golden snapshot can hold them.
// No protocol is set: the endpoint answers gRPC and HTTP/protobuf. The export
// interval is short because many actors live less than the SDK's one minute
// default.
func ActorEnv() []string {
	return []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT=" + Endpoint,
		"OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta",
		"OTEL_METRIC_EXPORT_INTERVAL=5000",
	}
}
