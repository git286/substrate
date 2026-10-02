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

package actorotlp

import "strconv"

// GatewayAddress is the sandbox's default gateway, the address the relay
// answers at from inside every actor's network. It mirrors
// ateomnet.ActorVethGateway, which atelet cannot import (ateomnet is
// linux-only); TestGatewayAddressMatchesAteomnet keeps the two equal.
const GatewayAddress = "169.254.17.1"

// Endpoint is the OTLP endpoint as an actor's exporter sees it. It answers
// gRPC and HTTP/protobuf alike, so no protocol variable is set: whichever an
// SDK speaks by default, or is told to speak by its own configuration, is
// served here. Under OTLP/HTTP the SDK appends the signal path to it.
const Endpoint = "http://" + GatewayAddress + ":4317"

// metricExportIntervalMillis is short because actors are often short-lived
// and the SDK default of one minute means many never export at all.
const metricExportIntervalMillis = 5000

// ActorEnv is the exporter configuration every actor container starts with.
// These are constants, identical for every actor, so freezing them into a
// golden snapshot freezes the right answer. Delta temporality is required
// rather than preferred: under cumulative, a restored actor reports the golden
// build's baseline forever, and the relay would have to hold per-actor state to
// correct it. The template's own env takes precedence, so an author who needs
// something else can say so; changing these on an existing template requires
// a golden rebuild like any other env change.
func ActorEnv() []string {
	return []string{
		"OTEL_EXPORTER_OTLP_ENDPOINT=" + Endpoint,
		"OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta",
		"OTEL_METRIC_EXPORT_INTERVAL=" + strconv.Itoa(metricExportIntervalMillis),
	}
}
