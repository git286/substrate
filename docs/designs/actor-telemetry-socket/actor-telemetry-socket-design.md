# Design: per-actor telemetry relay

**Status:** draft / for discussion — **validated by a working prototype** (§13)
**Related:** [#761](https://github.com/agent-substrate/substrate/issues/761) (this problem), [#503](https://github.com/agent-substrate/substrate/issues/503) (telemetry continuity), [#450](https://github.com/agent-substrate/substrate/issues/450) (lifecycle hooks), [#745](https://github.com/agent-substrate/substrate/issues/745) (OTLP endpoint sprawl), [#741](https://github.com/agent-substrate/substrate/issues/741) (`WithInsecure`)

> Supersedes an earlier draft of this file that placed the receiver in atelet and
> used a Unix socket. Both were wrong — see §5 and §9.
>
> Sections marked **[prototype]** were added or corrected after building the
> thing. §13 lists what changed and why.

---

## 1. Summary

Actors cannot correctly identify their own telemetry, so all telemetry from an
ActorTemplate collapses into a single time series with many writers. Counters
come out wrong and nothing errors.

This proposes that **ateom** expose a fixed OTLP endpoint inside the worker pod,
stamp trusted `ate.dev/*` identity onto everything that arrives, and aggregate
metrics to bounded cardinality before forwarding upstream.

The principle is one Substrate already applies to logs:

> **Attribution belongs to the channel, not the payload.**

`internal/actorlog/logger.go:132-158` already does this for stdout — atelet
injects `ate.dev/actor_atespace`, `actor_name`, `actor_uid`,
`actor_template_namespace`, `actor_template_name`, `container_name` into every
log line, out of band. The actor never touches them, so they can't be frozen,
forged, or lost. Metrics and traces are the signals that haven't caught up.

It's also the same division of labour Kubernetes already uses. Under Prometheus,
identity comes from the scrape target, not the workload — `instance` and `pod`
are assigned by the scraper, and emitting them yourself is an anti-pattern.
Actors can't be scraped, so the relay is that job relocated to somewhere it can
still happen. This is not a novel mechanism; it's the conventional one adapted
to push.

## 2. Background

**The golden snapshot freezes anything computed at startup.** The snapshot is
taken after init, so an OTel `Resource` built in `main()` is byte-identical for
every actor of the template. `cmd/atelet/oci.go:37-45` documents this for env
vars specifically:

> It is delivered as a per-actor bind mount rather than environment variables
> because env lives in the checkpointed process memory and would be frozen at
> the golden snapshot's values after a restore.

The escapes are all closed: `os.Hostname()` is hardcoded to `runsc`
(`cmd/atelet/oci.go:271`), and ActorTemplate does not support downward-API
`valueFrom.fieldRef` (`docs/api-guide.md:101`).

The sharpest illustration is in Substrate's own code. `internal/serverboot/serverboot.go:58-59`:

```go
// serviceInstanceID is generated once so the tracer and meter resources share it.
var serviceInstanceID = uuid.NewString()
```

Correct for a DaemonSet. Inside an actor it would run once, in the golden, and
be captured by the snapshot — producing a random UUID shared identically by
every actor of the template. It's normal, idiomatic code that the platform
silently breaks, which is the point: this is not a contributor error class.

**Two merge regimes, both broken.** Because the Resource is identical
cluster-wide:

- *Today* (no `k8sattributes` in the pipeline — confirmed at
  `manifests/ate-install/kind/otel-collector.yaml:52-54`, which is
  `otlp → batch → prometheus`): **one series per template, cluster-wide, N
  simultaneous writers.** Gauges collide; backends that require ordered samples
  per series will reject points.
- *If `k8sattributes` were added*: split into per-worker series, each with
  serial writers over time (a Worker "hosts at most one Actor at a time; many
  Actors are multiplexed across a pool over time" — `docs/glossary.md:30-32`),
  fragmenting on every migration. And it can't work anyway: actor egress
  masquerades behind the pod IP (`cmd/ateom-microvm/net.go:19-24`) and all
  actors share one interior IP (`169.254.17.2`).

**Counters inherit the golden's values.** Every actor starts pre-loaded with the
golden's accumulated totals, so there is no clean reset for `rate()` to detect
either.

## 3. Goals

1. Every actor's telemetry is attributed to **that actor**, with no cooperation
   from actor code beyond pointing at an endpoint.
2. Attribution is **trusted** — derived by the platform, not asserted by the
   workload — so it can back per-tenant views, quota, and billing later.
3. Attribution **survives suspend/resume/migration** with nothing re-derived.
4. Storage cardinality is **bounded and independent of actor count**.
5. Works with **stock OTel SDKs** and stock exporter configuration.
6. Works on **both runtimes**, gVisor and micro-VM.

## 4. Non-goals

- Buffered-telemetry loss on `onPause: Data`. That is #503 and needs #450's
  `PreSuspend`. Complementary, not a substitute.
- Control-plane component telemetry, which works today.
- Per-actor metric series in long-term storage — explicitly rejected, §7.

## 5. Which component

Corrected from the earlier draft. The topology:

- **atelet** — a **DaemonSet**, one per *node*, shared across every worker pod on
  it (`manifests/ate-install/atelet.yaml:48`). Unprivileged: *"it only
  reads/writes the `/var/lib/ateom-gvisor` hostPath as root, so it needs no
  Linux capabilities"* (`atelet.yaml:75-76`).
- **ateom** — the sidecar **inside each worker pod** (`docs/threat-model.md:41`).
  Privileged; creates the sandbox, owns the pod netns and the veth pair, runs
  runsc.

**The receiver belongs in ateom**, for four reasons:

1. **atelet cannot reach the address.** The worker-side veth
   (`ateom0`, `169.254.17.1/30`) lives in the *worker pod's* netns
   (`cmd/ateom-gvisor/main.go:492-494`). atelet is a node daemon in a different
   namespace. For a network transport this isn't a preference, it's the only
   option.
2. **Blast radius.** The receiver parses untrusted actor input. ateom is already
   inside the actor's blast radius — it runs the sandbox and shares the pod.
   atelet is shared by every worker pod on the node, so putting actor-controlled
   input there converts a per-pod risk into a per-node one.
3. **Attribution is trivial.** One actor per worker at a time
   (`docs/glossary.md:30-32`), so "who sent this" has exactly one answer.
4. **It already has the identity — in memory, not from a file.** **[prototype]**
   The earlier text said ateom reads `/run/ate/actor-id`. It should not, for two
   reasons. That file holds only the actor *name*
   (`cmd/atelet/main.go:807` writes `actorName` and nothing else), and it does
   not exist on micro-VM at all — the identity bind mount is dropped there
   (`cmd/ateom-microvm/spec.go:92`). ateom already receives the **full** set on
   every activation RPC: `RunWorkloadRequest`, `RestoreWorkloadRequest` and
   `CheckpointWorkloadRequest` all carry `atespace`, `actor_name`, `actor_uid`,
   `actor_template_namespace` and `actor_template_name`
   (`internal/proto/ateompb/ateom.proto`). Attribution state is therefore a
   field set by the RPC handler, and its lifetime is exactly the activation's —
   which is also what makes the flush rule in §7.5 expressible.

Division of labour, with nothing reassigned:

| Component | Role |
|---|---|
| **atelet** | Knows the actor's identity and writes it down. **Unchanged.** |
| **ateom** | Listens, stamps, separates, aggregates, forwards. **New.** |

## 6. Architecture

```
┌─ worker pod ────────────────────────────────────────┐
│                                                     │
│  ┌─ actor sandbox ─────────┐      ┌─ ateom ───────┐ │
│  │  app                    │      │  identity     │ │
│  │   │ OTLP                │      │  from the     │ │
│  │   └─► 169.254.17.1:4317 ┼──────┼─►activation   │ │
│  │  (no identity in        │      │  receiver     │ │
│  │   payload)              │      │   · stamps    │ │
│  │                         │      │   · separates │ │
│  └─────────────────────────┘      │   · aggregates│ │
│                                   └───────┬───────┘ │
└───────────────────────────────────────────┼─────────┘
                                            │ OTLP, bounded labels
                                            ▼
                                   cluster collector → backend
```

The actor's exporter config is a **constant address**, so it survives the golden
snapshot intact. What answers at that address is **per-actor**, re-created on
every activation. Substrate already relies on this trick twice: the constant
interior IP (`cmd/ateom-microvm/net.go:28-30` — "Because that address is a
CONSTANT, a restored guest's frozen network config stays valid on any pod") and
the constant identity path `/run/ate/actor-id`.

## 7. Detailed design

### 7.1 Transport

**`169.254.17.1:4317`** — the existing worker-side veth gateway, already created
per activation by both runtimes.

Chosen over a Unix socket in the identity dir (the earlier draft) because:

- **It works on micro-VM.** `cmd/ateom-microvm/spec.go:91-96` records that the
  identity bind mount is dropped entirely there — "The micro-VM guest can't see
  host paths" — so a UDS is unavailable. But kata wires the guest to the same
  veth via virtio-net (`cmd/ateom-microvm/net.go:26-30`), so a network endpoint
  works on both runtimes with no new plumbing.
- **No host-fd exposure.** runsc gates host Unix sockets behind `--host-uds`,
  which `cmd/ateom-gvisor/runsc.go:84-96` does not pass. Enabling it would widen
  the sandbox surface and need a threat-model review. A netstack connection
  needs neither.
- **Checkpoint safety.** An open host fd may block `runsc checkpoint`; a
  netstack socket is ordinary severed-connection handling.

The cost is that a shared address relies on one-actor-per-worker for unambiguous
attribution. That holds today by definition (`docs/glossary.md:30-32`) and is the
thing to revisit if concurrency lands — see §11.

### 7.2 Actor-side configuration

```
OTEL_EXPORTER_OTLP_ENDPOINT=http://169.254.17.1:4317
OTEL_EXPORTER_OTLP_PROTOCOL=grpc
OTEL_EXPORTER_OTLP_METRICS_TEMPORALITY_PREFERENCE=delta
```

Env is safe here **because these are constants.** The golden-snapshot hazard
applies to values that must differ per actor; these do not, so freezing them
freezes the right answer. That distinction is not stated anywhere in
`docs/api-guide.md` today, which reads as "never use env" — stronger than the
actual constraint, and worth correcting.

Note these are captured into the golden, so changing them on an existing
template requires a golden rebuild (`docs/api-guide.md:101`: values are
inherited "until the golden snapshot is recreated").

Actors emit **no identity attributes**. Anything they do emit under `ate.dev/`
is overridden (§7.3).

### 7.3 Attribution

ateom sets, **overriding any existing value**:

```
service.instance.id              (= actor UID; the standard name for this)
ate.dev/actor_uid
ate.dev/actor_name
ate.dev/actor_atespace
ate.dev/actor_template_namespace
ate.dev/actor_template_name
ate.dev/container_name
```

Same set as the log labels, deliberately, so the three signals join.
`service.instance.id` is included because every backend already understands it —
it maps to `instance` on the Prometheus path and to `task_id` on Cloud
Monitoring's `generic_task`.

Overriding rather than merging is the point: a workload must not be able to
attribute its telemetry to another actor or another atespace.

**[prototype] Override the resource, strip the datapoints.** Identity forgery
has two surfaces, and only one of them is a resource attribute. An actor can
also put `ate.dev/actor_uid` on an individual *datapoint*, span attribute or log
attribute, where "override" has no meaning because there is nothing to override.
The rule is therefore: **any attribute under the reserved `ate.dev/` prefix, at
any level, is removed before the platform stamps its own** — and each removal is
counted (`ateom_relay_actor_attributes_overridden`), because an actor doing this
is either confused or probing.

### 7.4 Cardinality policy

The relay solves *identity*. Identity is the prerequisite for aggregating
*correctly*. Aggregation is what solves cardinality.

**You cannot correctly sum counters from many writers without first telling the
writers apart.** Substrate today merges without ever separating, which is
exactly why the numbers are wrong. ateom separates, then merges:

| Signal | Actor identity | Rationale |
|---|---|---|
| Metrics | stripped before forwarding | a series is durable: index entry, interned labels, chunks, retained for the full window after the actor dies |
| Traces | kept | a span is an *event*, not a series — 10k actors/day is 10k rows, not 200k index entries |
| Logs | kept | already the case today |

Forwarded metric labels: `actor_template_namespace`, `actor_template_name`,
`atespace`, `container_name`. Output series count is
`templates × atespaces × containers × metrics` — constant, and independent of
how many actors ever ran.

**[prototype] That formula is only true if the actor's own attributes are
bounded, and they are not.** Stripping identity bounds the labels *ateom* adds;
it does nothing about the labels the actor adds. Three leaks, all found by
building it:

- **Resource attributes.** The actor's `Resource` is forwarded, and an actor is
  free to put a request ID or a hostname in it. The first fix was an
  **allow-list**, not a deny-list — `service.name`, `service.namespace`,
  `service.version`, `telemetry.sdk.*` survive onto metric resources; everything
  else is dropped. **That was not enough: an allow-list filters keys, and the
  *values* are still the actor's** (finding #9). The metric resource is now
  built entirely from the activation RPC, with `service.name` pinned to the
  template name and `service.namespace` to the template namespace. Traces and
  logs keep the actor's full resource verbatim, where a high-cardinality
  attribute costs one row, not one series; the join key across signals is the
  `ate.dev/*` set, not `service.name`.
- **Datapoint attributes.** `counter.Add(ctx, 1, attribute.String("user", id))`
  is ordinary, correct-looking SDK usage that produces one series per user. No
  allow-list is possible here — the whole point of datapoint attributes is that
  the actor chooses them. Fix: a **per-metric cardinality cap** per interval.
  Past the cap, further attribute sets fold into the standard
  `otel.metric.overflow` marker series. Totals stay exact; only the breakdown is
  lost, and the marker makes the loss visible rather than silent.
- **Metric names and scope names.** Also the actor's to choose, and *upstream*
  of the per-metric cap, which is keyed on `(resource, metric name)` and so
  cannot see them (finding #9). Fix: a **global accumulator budget** per
  interval, on top of the per-metric cap.

The caps are a backstop against a bad actor blowing up the store, not a
substitute for guidance. They belong in M1's docs alongside the delta rule.

What the three fixes have in common: **every component of a metric series key
must be a fact the platform controls.** Anything the actor supplies is dropped,
replaced, or capped — never merely filtered.

`actor_uid` exists only in ateom's memory, for one aggregation interval, as a
hash-map key. High cardinality is free where it's a hash map and an outage where
it's a database. §7.4.1 works through what that map actually holds and why it
stays small.

Datapoints carry **exemplars** with the trace ID, so a spike on an aggregate
graph links through to the trace and therefore to the specific actor. That
preserves per-actor debugging without per-actor series.

### 7.4.1 The attribution hashmap **[prototype]**

The whole cardinality argument rests on one move: `actor_uid` is a **key in
ateom's memory** and never a **label in the database**. This is what that costs.

**Shape.** The aggregator is a map from an output series key to an accumulator,
plus a per-writer inner dimension:

```
outer key (what the backend will see)        inner key        accumulator
─────────────────────────────────────        ─────────        ───────────
resource(platform-built: template_ns,        actor_uid        delta sum │
         template, atespace, container)                       count+buckets │
+ scope + metric name/unit/kind                               last value
+ datapoint attrs (actor-supplied, capped)
```

An actor's identity enters at the inner key and leaves at drain: the accumulators
under one outer key are combined and emitted as **one** datapoint, and the inner
dimension is simply not serialized. Nothing downstream ever learns it existed.
`actor_uid` is added on ingest — the platform's value, not the actor's (§7.3) —
used to keep writers apart while they are being merged, and dropped on the way
out. That is the entire trick.

**The inner dimension is only load-bearing for gauges.** Worth being precise,
because it is tempting to key everything by actor and it is unnecessary:

- **Delta sums and histograms** don't need it. Addition is associative and delta
  datapoints are self-contained, so two actors' contributions can go straight
  into the same accumulator in any order. The prototype collapses these at
  ingest and gets identical output for less memory.
- **Gauges do need it.** A gauge datapoint is an instantaneous reading, so the
  correct combine is *last value per writer, then the declared rule across
  writers* (§7.6). Without the inner key you are summing readings over time: an
  actor exporting `queue_depth=4` three times inside one relay interval
  contributes 12 under a naive `sum`. The relay interval and the actor's export
  interval are independent, so this is the normal case, not an edge one.

So the honest statement of the rule is: **the writer dimension exists for the
combines where writer identity changes the answer.** For monotonic sums it does
not, and pretending otherwise costs memory for nothing.

Note the inner dimension does **not** multiply output series. The cardinality cap
(§7.4) is applied to the *outer* key, so `w` writers sharing a template still
produce at most `L` series for a metric, not `w × L`.

**When it is flushed.** The map is per worker pod and per interval, and `drain`
swaps in a fresh one — it does not evict, decay, or reuse. Four triggers, and
the last three exist because a lost interval of delta is a lost interval
forever (finding #2):

| Trigger | Why |
|---|---|
| the interval ticker | the normal path |
| an explicit flush | `PreSnapshot`, so the golden image contains no pending window |
| `Deactivate` | checkpoint or exit; the activation's last datapoints would otherwise die with it |
| `SIGTERM` | worker pod teardown; same window, same loss |

So **across** intervals there is no incremental leak and no eviction policy to
tune. A dead actor's entries are gone at the next tick rather than aging out.

**Within one interval it is not self-limiting, and an earlier draft of this
section was wrong to imply it was.** The size is

```
entries ≈ R × S × M × A     R = distinct metric resources
                            S = distinct instrumentation scopes
                            M = distinct metric names
                            A = distinct datapoint attribute sets, capped at L
```

The per-metric cap only bounds `A`, because it is keyed on
`(resource, metric name)` — it cannot see the axes that produce the pairs. `R`,
`S` and `M` were all actor-controlled: the resource allow-list filtered keys
while the actor still chose the values, and nothing at all looked at metric or
scope names. Measured in the prototype: **5,000 distinct metric names against a
cap of 200 held 5,000 accumulators and exported 5,000 series**, and varying
`service.name` gave the identical result. That is not just heap in ateom, it is
cardinality reaching the database — the exact failure this design exists to
prevent, arriving by a different door.

Two fixes, both in the prototype:

- **`R` is now 1 per worker.** The metric resource is built entirely from the
  activation RPC (§7.4), so no actor-chosen value is part of it.
- **A global accumulator budget per interval**, on top of the per-metric cap.
  Past the budget ateom stops opening accumulators; series already open keep
  taking datapoints, so a noisy metric cannot starve a well-behaved one, and the
  refused datapoints come back to the actor as OTLP partial success and out to
  the backend as `ateom_relay_datapoints_rejected{reason="series_budget_exhausted"}`.
  Unlike the per-metric cap this **drops rather than folds**, and it has to: an
  overflow series preserves a total only when everything folded into it is the
  same metric. Summing `work_items` into `latency_ms` would be worse than no
  number.

With both, the bound is real: `entries ≤ min(B, S × M × L)` with `B` the budget,
and `B` is a number the platform picks (2,000 in the prototype) rather than one
the actor picks. At a few hundred bytes an entry that is sub-MB, in a sidecar
already running a sandbox. The realistic case (`M ≈ 20`, `A ≈ 5`) is ~100
entries, tens of KB.

The consequence of getting this wrong is worth stating plainly, because it is
the price of putting the aggregator in ateom rather than at the collector:
**ateom is the privileged sidecar that owns the sandbox**, so an actor that OOMs
it takes down its own worker pod. Bounding the map is not a tidiness concern.

**The asymmetry that makes this work:**

| | ateom's hashmap | the TSDB |
|---|---|---|
| scales with | actors writing to **one worker** during **one interval** | actors that have **ever** run, cluster-wide |
| lifetime | one interval, then dropped | the full retention window, after the actor is dead |
| cost per entry | a few hundred bytes of heap | index entry + interned labels + chunk headers, and a slot in the active-series budget |
| failure mode | heap in the privileged sidecar; unbounded, it OOMs ateom and takes the worker pod with it | query latency and ingest limits for everyone |

10,000 actors/day × 20 metrics is 200,000 new series/day and ~6M over 30 days in
the database (§8). In the hashmap those 10,000 actors never coexist: a worker
hosts one at a time (`docs/glossary.md:30-32`), so the peak is `w`, set by the
activation rate, not by the fleet size. Fleet growth adds *maps*, not entries per
map.

**It is sharded by construction, which is the other half of the scalability
argument.** Each ateom only ever sees its own worker's actor, so the aggregation
state is partitioned across the fleet with no coordination, no shared index, and
no hot key. This is the concrete reason the relay belongs in ateom rather than at
the cluster collector (§5, and the "enrich at the collector" rejection in §9):
a collector-side aggregator would have to hold per-actor state for every actor in
the cluster in one process, which is the same cardinality problem moved one hop
and given a single point of failure.

**What does escape, bounded.** Exemplars carry trace IDs, which are unbounded by
nature. They ride along on datapoints rather than in the series key, so they cost
payload rather than series — but they are still capped per series per interval,
or a high-throughput actor would turn "a few links back to traces" into an
export the size of its trace volume.

### 7.5 Temporality

Delta is **required**, not preferred.

Under cumulative, ateom would have to hold each actor's last-seen value and
compute the merged total itself. On migration the destination ateom has no
history and the merged series drops — the original bug, one layer up. Under
delta each export is self-contained, so contributions from different workers
simply add.

Worked example. Actor A counts `10 → 15 → 20`, actor B counts `3 → 8`; true
total 28.

*Today:* backend receives `10, 3, 15, 8, 20`. Every dip reads as a counter
restart, phantom increments are added, the total inflates, and the real number
is unrecoverable.

*With this design:* A's deltas `10, 5, 5`; B's `3, 5`; forwarded per interval as
`13, 10, 5`. Total **28**, one stored series, `actor_uid` never left the pod.

Delta also **cancels the golden baseline**. The SDK's last-collection bookmark is
snapshotted alongside the counter, so a restored actor reports only its own work.
The residue is `g`, whatever the golden accrued after its last collection — a
bounded one-time offset per actor rather than a growing one, and removable by
forcing a collection immediately before the golden snapshot is taken. That is a
small, cheap addition worth folding into #450's scope (a `PreSnapshot` sibling of
`PreSuspend`).

**Delta is genuinely lossier.** Cumulative self-heals across a dropped export;
delta does not, so an `onPause: Data` kill loses that window permanently. Still
the right trade, because what cumulative preserves faithfully is a wrong number.

**[prototype] The relay is a second buffer, and it must flush on the way out.**
This is the one thing the prototype got wrong at first, and it cost real data in
the demo before anyone noticed — the actor's first 10 work items simply vanished
and every other number looked right. The relay holds up to one aggregation
interval in memory by design; because delta does not self-heal, that window is
gone if the process ends without draining it. So:

- **`CheckpointWorkload` must flush before it clears the activation.** This is
  the same hook #503 needs and the same place #450's `PreSuspend` belongs; the
  flush is cheap and the alternative is silent, plausible-looking undercounting.
- **Deactivation flushes, generally.** Ending an activation is the last moment
  the platform knows which actor those datapoints belong to.
- **SIGTERM flushes.** A worker pod being drained or evicted gets a termination
  grace period; use it.

The loss window is then bounded by what is in flight, not by the aggregation
interval. Note the consequence for §7.4's merge: two sequential activations of
the same template on one worker now arrive as two delta exports rather than one,
and merge at the backend on the shared series key. Same total, same series count.

**Reject cumulative *histograms* too.** The design said "cumulative" and meant
sums; `Histogram` carries the same `aggregation_temporality` field and the same
problem, and a stock SDK configured for delta will still send cumulative
histograms if the temporality preference is set wrong. Both are refused (§11 Q4).

### 7.6 Gauges

Delta cannot help: the OTLP `Gauge` message has no temporality field at all
(`metrics.pb.go:699-707`, versus `Sum` at `:750-763`), and
`DeltaTemporalitySelector` routes gauge kinds to cumulative anyway
(`reader.go:152-158`). **Also note it routes UpDownCounters there** — sync and
async — so those stay broken under the interim guidance too, which is easy to
miss.

Three distinct gauge problems:

1. **Simultaneous writers.** Worse than counters: no sawtooth signature, just a
   plausible number belonging to a random actor, plus ingest rejections from
   backends requiring ordered samples per series.
2. **Sync gauges inherit and repeat the golden's value forever.**
   `cumulativeLastValue.collect` never clears — it re-emits every stored
   attribute set each cycle, with a standing `TODO (#3006)` about exactly this
   (`lastvalue.go:151-190`). A restored actor reports the golden's reading until
   its own code happens to write that gauge again.
   **Observable gauges do not have this problem**: they use
   `precomputedLastValue`, which clears on collect (`lastvalue.go:127`, "*Do not
   report stale values*") and re-runs the callback against live state.
3. **No canonical combine.** Counters add; gauges don't. `queue_depth` wants sum,
   `cpu_utilization` wants average, and OTLP carries no metadata saying which.

Mitigations: prefer `ObservableGauge` over sync `Gauge` (fixes 2 outright, one
line per instrument); reshape gauges as histograms where the question allows it;
put genuinely instantaneous per-actor state in logs. The relay fixes 1 and 2. For
3, **do not silently aggregate** — forward gauges with a bounded identity
dimension at shorter retention, or require an explicit per-metric aggregation
declaration and drop the rest. Silently summing a utilization gauge is exactly
the class of plausible-wrong number this whole issue is about.

**[prototype] Declaration-or-drop is workable, and the drop must be loud.** The
prototype implements the second option: a per-metric map of `sum` or `avg`, and
an undeclared gauge is dropped, counted with
`reason=gauge_no_aggregation_declared`, and reported back to the actor in the
OTLP partial-success response. In the demo `cpu_utilization` has no rule and is
dropped on every export, which is the correct outcome and also visibly annoying
— exactly the pressure needed to make someone either declare it or reshape it as
a histogram. What would be unacceptable is dropping it quietly.

### 7.7 Forwarding and enrichment

Forwarded telemetry must **not** be re-enriched with `k8s.pod.name` /
`k8s.node.name` upstream. Those describe the current host, change on every
migration, and are what makes the failure read like ordinary pod churn. ateom
should stamp `ate.dev/worker_pod` / `ate.dev/worker_node` instead — clearly named
as host facts, not workload identity, and not part of the metric series key.

**[prototype] "Not part of the series key" is too weak — keep them off metric
resources entirely.** A resource attribute *is* part of the series key on every
backend that matters; the Prometheus path turns resource attributes into
`target_info` and job/instance labels, and OTLP-native stores key series on the
resource. Stamping `worker_pod` on a metric resource re-creates per-worker
fragmentation on the far side of the relay, which is the original bug wearing a
different label name. So: **host facts go on traces and logs, never on actor
metric resources.** They still appear on metrics in one place — ateom's *own*
self-telemetry (`service.name=ateom`), where per-worker is the correct
granularity because the worker is the subject.

### 7.8 Suspend and resume

Nothing to re-derive. The frozen exporter config points at a constant address;
on resume that address is served by the new worker's ateom, which knows the new
activation. The gRPC client sees a dead connection and reconnects to the same
address, which now leads somewhere else. That is the whole mechanism.

This is why attribution does not depend on #450, though #450 is still needed for
flushing (#503).

**[prototype] The reconnect is not instant.** Re-binding the address and having
the actor's gRPC client notice takes seconds, not milliseconds: the client sits
in backoff and the OTLP exporter retries. Correctness survives it — delta exports
are self-contained, so a retried batch lands on the new worker and still adds up
— but two things follow. The actor's export queue must be deep enough to hold a
few intervals (stock SDK defaults are), and resume should not be considered
"telemetry-healthy" the instant the workload runs. In the prototype's migration
test this reconnect is the dominant cost of the run, and the final total is still
exact.

## 8. Rejected: per-actor metric series in storage

10,000 short-lived actors/day × 20 metrics = 200,000 new series/day, ~6M over a
30-day retention, nearly all dead within minutes and all still costing index
space, query latency, and active-series budget. `actor_uid` is unbounded and
churns on every create. It is not a label.

## 9. Alternatives considered

**Inject `OTEL_RESOURCE_ATTRIBUTES` with per-actor identity.** The obvious idea
and a trap: env is frozen, so every actor receives the golden's values. This is
the failure `docs/api-guide.md:108-111` warns about and the one PR #750 hits.

**Have the actor read `/run/ate/actor-id` and build its Resource from it.** Fails
on timing, not on the file. Read in `main()`, it returns the *golden's* name and
that gets snapshotted. Read later, it can't be applied — `Resource` is immutable
by design (`sdk/resource/resource.go:32`) and baked into the provider. And the
actor has no signal that it was restored, so there is no moment at which it knows
to rebuild. Polling the file and rebuilding providers on change would half-work,
at the cost of a wrong-identity window after every resume, discarded buffers, and
every actor author reimplementing it.

**Read it fresh and attach per-measurement.** This *is* correct today and is the
current recommendation — but only for traces and logs. On metrics it costs
unbounded cardinality and remains self-asserted. It also leaves a ghost: the
golden's attribute set stays in the SDK's memory and is re-emitted forever under
cumulative (`lastvalue.go:151-190`). Delta clears that.

**`PostResume` (#450) and let actors rebuild their providers.** Needed anyway for
the flush. As the *primary* answer it can't do three things: identity stays
self-asserted (untrusted), cardinality is untouched, and every actor author has
to implement it with silent failure if they don't.

**Enrich at the cluster collector.** Impossible, not misconfigured. NAT destroys
the distinction before the collector sees a packet, and all actors share one
interior IP. It also presupposes an identity the actor cannot produce.

**Receiver in atelet.** The earlier draft. Wrong on all of §5 — wrong namespace
for the address, and it widens the blast radius from one pod to a whole node.

**Unix socket in the identity dir.** See §7.1: unavailable on micro-VM, requires
enabling `--host-uds`, and may block checkpoint.

**Per-actor collector sidecar.** Defeats the density model.

**File drop.** Actor writes serialized OTLP into a per-actor directory that ateom
tails. Sidesteps fd and checkpoint concerns entirely and rides the existing
virtiofsd share on micro-VM (`cmd/ateom-microvm/durable.go:28-36`). Costs a
custom exporter instead of a stock one, plus a durability/ordering contract.
Fallback if the network path hits an unforeseen problem.

## 10. Phasing

**M1 — interim guidance, no platform change.** Update `docs/observability.md` §6:
delta temporality; `ObservableGauge` over sync `Gauge`; metric labels bounded;
actor identity on traces and logs only, read fresh per request
(`internal/e2e/fixtures/probe/main.go:31` is the canonical pattern); note that
UpDownCounters are not covered by delta. State the cardinality rule *now*, before
identity works, so the reflex isn't to add `actor_uid` as a label. Fix PR #750's
demo to match.

**M2 — ateom receiver + attribution, gVisor.** Goals 1–3, 5. Metrics forwarded
with identity intact but not yet aggregated; demo-scale only, explicitly not
production.

**M3 — micro-VM parity.** Should be near-free on the network transport; the point
at which goal 6 is verified rather than assumed.

**M4 — aggregation and cardinality policy.** Goal 4. Also where upstream
`k8sattributes` must be suppressed for actor telemetry (§7.7) and where the gauge
aggregation decision (§7.6) has to be made. **[prototype]** Add the resource
allow-list and the per-metric cardinality cap here too — they are part of what
makes goal 4 true, not a later hardening pass.

**[prototype]** Two items move earlier than they look. The **flush on
deactivation** (§7.5) belongs in **M2**, not M4: without it M2's demo produces
undercounts that read as correct. And the **`ate.dev/` strip** at both resource
and datapoint level (§7.3) is part of M2's attribution, since "stamps identity"
without it is not actually trustworthy.

## 11. Open questions

1. **Concurrency.** A shared worker-pod address relies on one-actor-per-worker. If
   that changes, attribution needs per-actor addressing or a per-actor socket.
   Worth knowing the cost before it's forced.
2. **Is `atespace` a metric label?** Bounded but tenant-shaped, so it multiplies
   series by tenant count. Probably yes, deliberately — but as a stated cost, not
   a default.
3. **Endpoint unavailable.** Silently failing exports are arguably worse than
   today. Needs an ateom-side counter for dropped actor telemetry — itself
   Substrate telemetry, and so unaffected by this problem.
   **[prototype] Answered.** The relay emits its own delta sums under
   `service.name=ateom` with `ate.dev/worker_pod` / `worker_node`:
   `ateom_relay_datapoints_accepted`, `..._rejected{reason}`,
   `..._exports_without_activation`, `..._actor_attributes_overridden`,
   `..._forward_failures`. Bounded by worker count and reason count. The
   `exports_without_activation` counter is the one that catches the nastiest
   case — an actor exporting into a relay with no active identity, which must be
   refused rather than attributed to whoever ran last.
4. **Cumulative fallback?** For actors that can't be reconfigured, suggest
   rejecting at the receiver with a loud attributable error rather than silently
   producing wrong numbers.
   **[prototype] Answered: OTLP partial success.** `ExportMetricsServiceResponse`
   has a `partial_success` field carrying `rejected_data_points` and a free-text
   `error_message`, which is exactly the right shape: the accepted datapoints are
   still stored, the refused ones are named with a reason, and — the part that
   makes it loud — the OTel Go SDK surfaces a non-empty partial success **as an
   export error in the actor's own logs**. The actor cannot not notice, and a
   whole batch is never discarded to punish one bad instrument. A gRPC-level
   error would have thrown away the good data with the bad. The reasons
   currently emitted: `cumulative_sum`, `cumulative_histogram`,
   `gauge_no_aggregation_declared`, `histogram_bucket_mismatch`,
   `series_budget_exhausted`, `unsupported_metric_type`. An export arriving with
   no activation is the one case that is a gRPC error rather than a partial
   success — there is no actor to attribute it to, so there is nothing to keep.
5. **Does ateom forward directly, or via atelet?** Direct is simpler; via atelet
   gives one egress point per node and fewer upstream connections. Leaning
   direct.

## 12. Testing

- **Two concurrent actors, one template** — distinct `service.instance.id` count
  at the receiver must be 2. This is the check PR #750's README step 3 is
  missing; as written it cannot fail.
- **Counter correctness across migration** — drive a known number of increments,
  suspend, resume on a different worker, assert the backend total is exact. This
  is the test that fails today and is the whole point.
- **Golden baseline** — an actor that does no work must report zero, not the
  golden's totals.
- **Cardinality bound** — run N actors for N ∈ {1, 10, 100}; forwarded series
  count must not vary with N.
- **Attribution cannot be forged** — an actor emitting its own `ate.dev/actor_uid`
  must be overridden, not merged. **[prototype]** At the datapoint level too, not
  just the resource.
- **Both runtimes** — the same suite against gVisor and micro-VM. **The one item
  the prototype cannot cover**: it is loopback TCP, and the two runtimes differ
  precisely in the netns and virtio paths it stubs out.

**[prototype] Added by the implementation:**

- **The worked example, both ways** — the same workload must produce 45 under the
  status quo (phantom counter resets) and exactly 28 through the relay. Asserting
  the *bug* reproduces is what makes the fix's number mean anything.
- **Cumulative sums and histograms are refused, and the refusal reaches the
  actor** — assert on the partial-success response, not just on a counter.
- **An undeclared gauge is dropped and counted**, not silently summed.
- **Actor-supplied labels are capped** — a metric with unbounded datapoint
  attributes folds into `otel.metric.overflow` with the total preserved.
- **So are the axes above them** — N distinct metric names and N distinct scope
  names must produce at most the global budget of series, with the remainder
  counted under `series_budget_exhausted` and the accepted series still exact;
  N distinct actor `service.name` values must produce exactly one. The cap test
  above passes without either of these, which is why they are separate tests.
- **An export with no active actor is refused** — never attributed to the
  previous activation.
- **The pending aggregation window survives checkpoint and shutdown** — emit,
  deactivate (or SIGTERM) without an explicit flush, assert the backend total is
  still exact. This test exists because the demo silently lost data without it.
- **Exemplars survive aggregation** — otherwise §7.4's "you can still get back to
  the individual actor" is a claim with nothing behind it.
- **Metrics, traces and logs join** on the label set §7.3 shares with the log
  labels.

## 13. Prototype

`prototype/` is a working implementation: a stock-OTel-SDK actor, the ateom-side
relay, and a stand-in cluster collector, wired over real gRPC. `go test ./...
-race` runs the §12 suite; `./demo.sh` runs one scripted scenario — two
concurrent actors of one template, a migration onto a different worker at the
same address, an actor forging its identity, and an actor exporting cumulative
sums — and prints what the backend stored. See `prototype/README.md`.

What it confirmed, unchanged:

- Counter correctness across migration: exact totals, one series, migration
  invisible to storage.
- The golden baseline argument in §7.5, both halves — a restored idle actor
  reports a bounded one-time residue, and a `PreSnapshot` flush removes even
  that. Worth folding into #450 as §7.5 says.
- Cardinality independent of actor count: N ∈ {1, 10, 100} → one series.
- The constant-address trick: the actor's exporter configuration is never
  touched across the migration, and the numbers still come out right.

What it changed — the **[prototype]** marks above, in rough order of how much
they matter:

1. **Flush at the end of an activation** (§7.5). A missing flush loses a whole
   aggregation window, silently, and everything else still looks correct.
2. **Actor-supplied attributes are unbounded** (§7.4). The cardinality argument
   had a hole in it: resource attributes need filtering, datapoint attributes
   need a cap. See #9 — the first version of this fix was itself incomplete.
3. **Host facts off metric resources entirely** (§7.7), not merely off the series
   key — a resource attribute is part of the key.
4. **Identity comes from the activation RPC** (§5), not `/run/ate/actor-id`,
   which holds only the actor name and does not exist on micro-VM.
5. **Partial success is the refusal mechanism** (§11 Q4) — loud to the actor,
   non-destructive to the batch.
6. **Datapoint-level forgery** is a separate surface from resource-level (§7.3).
7. **Cumulative histograms** need refusing alongside cumulative sums (§7.5).
8. **Gauges need a per-writer key in the aggregator; sums do not** (§7.4.1).
   Writing up the hashmap exposed this: the prototype merges all writers at
   ingest, which is right for delta sums and wrong for gauges, where an actor
   exporting more often than the relay flushes multiplies a summed gauge.
   Documented and marked `TODO` in `internal/relay/aggregate.go`; the fix is a
   last-value-per-`actor_uid` inner key for gauge kinds only.
9. **The per-metric cap does not bound the hashmap** (§7.4, §7.4.1). Asking
   "when is the map flushed?" turned up a second hole under the first one. The
   cap is keyed on `(resource, metric name)`, so it bounds only datapoint
   attributes *inside* a pair; three axes upstream of it were actor-controlled
   and uncapped — metric name, scope name, and resource **values** (the
   allow-list filtered keys, not values). Measured: 5,000 distinct metric names
   against a cap of 200 gave 5,000 accumulators and 5,000 exported series, and
   varying `service.name` gave the same. So it was not a memory bug that stayed
   inside ateom; it reached the database. Fixed two ways: the metric resource is
   now built entirely from the activation RPC with `service.name` pinned to the
   template, and a global per-interval accumulator budget refuses (rather than
   folds) new series past its limit, counting them under
   `reason="series_budget_exhausted"`. Tests:
   `relay.TestSeriesBudgetBoundsMetricAndScopeNames`,
   `relay.TestActorResourceValuesCannotMultiplySeries`.

   The general lesson, now stated as a rule in §7.4: a cap on one component of a
   series key bounds nothing unless every other component is already bounded.
