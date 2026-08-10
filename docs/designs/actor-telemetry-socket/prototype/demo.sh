#!/usr/bin/env bash
# End-to-end demo of the per-actor telemetry relay.
#
# Topology (all on loopback, standing in for one cluster):
#
#   actor A --\                       (constant address per worker pod)
#              +--> ateom relay 1 --\
#   actor B --/                      +--> cluster collector
#              +--> ateom relay 2 --/
#
# Scenario:
#   1. two concurrent actors of one template emit metrics, traces and logs
#   2. actor A migrates: relay 1 dies, relay 2' takes over its address
#   3. an actor tries to forge its identity
#   4. an actor exports cumulative sums and is refused
#
# Then it prints what the backend stored.
set -euo pipefail

cd "$(dirname "$0")"

COLLECTOR_OTLP=127.0.0.1:14317
COLLECTOR_HTTP=127.0.0.1:14318
R1_OTLP=127.0.0.1:14401
R1_ADMIN=127.0.0.1:14501
R2_OTLP=127.0.0.1:14402
R2_ADMIN=127.0.0.1:14502

pids=()
cleanup() {
  for p in "${pids[@]:-}"; do kill "$p" 2>/dev/null || true; done
  wait 2>/dev/null || true
}
trap cleanup EXIT

log() { printf '\n\033[1m== %s\033[0m\n' "$*"; }

go build -o /tmp/ate-relay-demo/collector ./cmd/collector
go build -o /tmp/ate-relay-demo/relay ./cmd/relay
go build -o /tmp/ate-relay-demo/actor ./cmd/actor

log "starting cluster collector"
/tmp/ate-relay-demo/collector -otlp "$COLLECTOR_OTLP" -http "$COLLECTOR_HTTP" &
pids+=($!)
sleep 0.5

start_relay() { # name addr admin pod node
  /tmp/ate-relay-demo/relay -listen "$2" -admin "$3" -upstream "$COLLECTOR_OTLP" \
    -pod "$4" -node "$5" -interval 1s >"/tmp/ate-relay-demo/$1.log" 2>&1 &
  pids+=($!)
  wait_http "$3"
}

wait_http() { # addr
  for _ in $(seq 1 100); do
    if curl -sS --max-time 1 "http://$1/stats" >/dev/null 2>&1; then return 0; fi
    sleep 0.1
  done
  echo "timed out waiting for $1" >&2
  return 1
}

log "starting two worker pods (ateom relays)"
start_relay r1 "$R1_OTLP" "$R1_ADMIN" worker-1 node-a
start_relay r2 "$R2_OTLP" "$R2_ADMIN" worker-2 node-b

log "activating one actor in each worker (what ateom's RunWorkload does)"
curl -sS "http://$R1_ADMIN/activate?uid=uid-aaa&name=actor-a" >/dev/null
curl -sS "http://$R2_ADMIN/activate?uid=uid-bbb&name=actor-b" >/dev/null

log "actor A: 10 work items; actor B: 3 work items"
/tmp/ate-relay-demo/actor -endpoint "$R1_OTLP" -work 10 -queue-depth 4
/tmp/ate-relay-demo/actor -endpoint "$R2_OTLP" -work 3 -queue-depth 1

log "actor C tries to forge identity as uid-victim (relay must override)"
curl -sS "http://$R2_ADMIN/activate?uid=uid-ccc&name=actor-c" >/dev/null
/tmp/ate-relay-demo/actor -endpoint "$R2_OTLP" -work 2 -forge-identity uid-victim

log "actor D exports cumulative sums (relay must refuse, loudly)"
/tmp/ate-relay-demo/actor -endpoint "$R2_OTLP" -work 4 -cumulative || true

log "migration: worker-1 goes away (SIGTERM), worker-3 takes over the same address"
# The relay holds up to one aggregation interval in memory and delta does not
# self-heal, so the dying worker must flush on the way out. Without this the
# demo silently loses actor A's first 10 items.
kill "${pids[1]}" 2>/dev/null || true
sleep 0.5
start_relay r3 "$R1_OTLP" "$R1_ADMIN" worker-3 node-c
curl -sS "http://$R1_ADMIN/activate?uid=uid-aaa&name=actor-a" >/dev/null

log "actor A resumes on the new worker: 5 more work items, same endpoint"
/tmp/ate-relay-demo/actor -endpoint "$R1_OTLP" -work 5

sleep 1.5
curl -sS "http://$R1_ADMIN/flush" >/dev/null || true
curl -sS "http://$R2_ADMIN/flush" >/dev/null || true
sleep 0.5

log "relay stats (worker-2): refusals are counted, not silent"
curl -sS "http://$R2_ADMIN/stats"

log "what the backend stored"
curl -sS "http://$COLLECTOR_HTTP/dump"

cat <<'EOF'

Read the dump above for:
  * work_items: ONE series, total 20 (10 + 3 + 2 + 5) — the cumulative actor's
    4 were refused, and no series carries actor_uid, service.instance.id,
    worker_pod or worker_node. The first 10 only survive because worker-1
    flushed its pending aggregation window when it caught SIGTERM.
  * trace_actor_uids: per-actor, uid-aaa split 10 + 5 across the migration.
    uid-victim is absent: forged identity was overridden.
  * trace_worker_pods: worker-1 and worker-3 for actor A — where it ran, kept
    as a host fact on traces, off the metric series.
  * ateom_relay_datapoints_rejected: cumulative_sum and
    gauge_no_aggregation_declared (cpu_utilization has no declared rule).
EOF
