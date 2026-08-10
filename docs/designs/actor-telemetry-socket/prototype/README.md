# Per-actor telemetry relay — working prototype

A runnable implementation of `../actor-telemetry-socket-design.md`: a stock
OpenTelemetry SDK actor exports to a constant address, and whatever answers at
that address stamps the identity. The actor's telemetry configuration is frozen
into the golden snapshot and never has to be right, because it never carries
identity in the first place.

Everything runs on loopback in one machine. Nothing here needs Kubernetes, a
sandbox, or a real collector.

## Topology

```
  actor process              ateom relay                cluster collector
  (stock OTel SDK)           (per worker pod)           (+ metrics backend)

  OTLP -> 169.254.17.1:4317  strips actor-asserted      stores series, spans,
  service.name=demo-actor    identity, stamps its own   log records; answers
  no instance id, no uid     from the activation RPC,   questions over HTTP
  delta temporality          aggregates one interval,
                             forwards delta upstream
```

- The actor's endpoint is a **constant**. On migration the actor's config does
  not change — a different relay answers at the same address.
- Metrics lose actor identity and keep a bounded key
  (`template_namespace × template_name × atespace × container_name`). The whole
  metric resource is platform-built — `service.name` is pinned to the template,
  not taken from the actor — because a resource *value* the actor chooses is a
  series key the actor chooses.
- Traces and logs keep full per-actor identity plus the worker pod/node they
  ran on. Exemplars carry trace IDs, so an aggregate spike links back to the
  individual actor that caused it.

## Layout

| Path | What it is |
| --- | --- |
| `internal/relay` | the ateom-side receiver: OTLP servers, attribution, aggregation, refusals, self-telemetry |
| `internal/identity` | attribute keys and the activation registry (what ateom knows about the current actor) |
| `internal/actorsdk` | the workload: stock OTel SDK, constants only, no identity |
| `internal/upstream` | stand-in cluster collector + metrics backend, with `increase()`-style cumulative accounting so the status quo can be reproduced |
| `internal/harness` | wires a relay to a collector for tests and the demo |
| `internal/e2e` | the §12 suite, driven end to end through real gRPC |
| `cmd/{actor,relay,collector}` | the same pieces as processes, for the demo |
| `demo.sh` | one scripted scenario: two actors, a migration, a forgery, a cumulative exporter |

In Substrate the relay is not a process. It is a listener ateom starts inside
the worker pod's netns, and `Activate`/`Deactivate` are called from the
`RunWorkload` / `RestoreWorkload` / `CheckpointWorkload` handlers, which already
carry `atespace`, `actor_name`, `actor_uid`, `actor_template_namespace` and
`actor_template_name`. `cmd/relay` exposes those calls over an admin HTTP API
only so a shell script can drive them.

## Running

```sh
go test ./... -race          # ~25s
./demo.sh                    # prints relay stats and what the backend stored
```

The demo ends with the backend dump. The three things to read in it:

- `work_items` — **one** series, total 20. No `actor_uid`, no
  `service.instance.id`, no `worker_pod`, no `worker_node`.
- `trace_actor_uids` — per-actor, and `uid-victim` is absent even though an
  actor asserted it.
- `ateom_relay_datapoints_rejected` — refusals are counted and exported, with a
  `reason`.

## Test map

Each §12 item of the design, and where it is checked:

| Design §12 | Test |
| --- | --- |
| Two concurrent actors of one template are distinguishable | `e2e.TestTwoConcurrentActorsOneTemplate` |
| Counter correctness across migration | `e2e.TestCounterCorrectnessAcrossMigration` |
| A restored actor doing nothing reports zero | `e2e.TestGoldenBaseline` (with and without a PreSnapshot flush) |
| Cardinality is bounded regardless of actor count | `e2e.TestCardinalityBound` (N = 1, 10, 100 → 1 series) |
| Attribution cannot be forged | `e2e.TestAttributionCannotBeForged`, `relay.TestDatapointLevelForgeryStripped` |
| Both runtimes | **not covered** — see below |

Additional properties the implementation forced:

| Property | Test |
| --- | --- |
| The worked example: status quo inflates 28 to 45 | `e2e.TestStatusQuoInflatesCounters` |
| The relay gives exactly 28 | `e2e.TestRelayGivesExactlyTwentyEight` |
| Cumulative sums and histograms are refused loudly | `e2e.TestCumulativeIsRejectedLoudly`, `relay.TestPartialSuccessReportsReasons` |
| Gauges need a declared aggregation, else dropped | `e2e.TestGaugeRequiresDeclaredAggregation`, `relay.TestGaugeAverageAggregation` |
| Actor-supplied labels cannot blow up cardinality | `e2e.TestActorSuppliedLabelsAreCapped`, `relay.TestUnknownResourceAttributesDropped` |
| Neither can actor-supplied metric names or scope names | `relay.TestSeriesBudgetBoundsMetricAndScopeNames` |
| Nor actor-supplied resource *values* | `relay.TestActorResourceValuesCannotMultiplySeries` |
| An export with no active actor is refused | `e2e.TestExportWithoutActivationIsRefused` |
| Metrics, traces and logs join on one label set | `e2e.TestSignalsJoin` |
| Sequential activations share a template series | `relay.TestSequentialActivationsMergeByTemplate` |
| Exemplars survive aggregation | `relay.TestExemplarsSurviveAggregation` |
| Histograms merge bucket-wise | `relay.TestHistogramsMerge` |
| The pending aggregation window is not lost at checkpoint or shutdown | `e2e.TestDeactivationFlushesPendingWindow`, `e2e.TestShutdownFlushesPendingWindow` |

## What this prototype does not show

- **Both runtimes.** Everything is loopback TCP. gVisor's netns and the
  micro-VM's virtio path are the two places the constant address could behave
  differently, and neither is exercised here. On micro-VM the identity bind
  mount is dropped entirely (`cmd/ateom-microvm/spec.go`), which is one more
  reason the design takes identity from the activation RPC rather than
  `/run/ate/actor-id`.
- **A real backend.** `internal/upstream` implements enough of a metrics store
  to model both delta accumulation and Prometheus-style `increase()` with reset
  detection. It is not Prometheus.
- **Scale.** The cardinality cap and the aggregation interval are exercised, but
  not under load.

Findings from building this are folded back into
`../actor-telemetry-socket-design.md`.
