# gRPC overload admission benchmark — 2026-07-23

## Scope

`GrpcOverloadBenchmark` models the production failure shape through the real
local gRPC server and generated client:

- continuous explicit `MAINTENANCE` projection-style puts;
- rate-limited `FOREGROUND` puts, point reads, and cold first/last ranges;
- bounded bursts of cancellable maintenance requests;
- a fixed 5,000 ms gRPC deadline and matching first/last storage budget;
- foreground-only and maintenance-flood phases against the same preloaded
  SST-heavy database.

The runner samples one atomic write-admission snapshot, so handoffs cannot
produce false combined-concurrency violations. It records throughput,
p50/p95/p99/max latency, deadlines, cancellations, per-lane peak and ending
queue depth, per-lane rejections, active concurrency, maintenance progress, and
drain state. `results.json` is the machine-readable source of truth;
`results.md` contains the same measurements for humans.

The runner fails its explicit release gate when any of these is false:

- foreground and first/last deadline counts are zero;
- maintenance-flood foreground p99 is at most 2x foreground-only p99;
- maintenance and cancellation both make progress;
- combined active writes and active maintenance writes stay within configured
  limits;
- no foreground request is rejected and no unexpected request fails;
- both queues, pending operations, transactions, iterators, range cursors, and
  gRPC iterator leases drain;
- native leak detection observes no still-owned handle, and client, server,
  scheduler, metrics, and database shutdown is clean.

The latency thresholds live only in this opt-in runner. Ordinary CI executes
the deterministic gate-unit and scheduler correctness tests, not this disk and
timing workload.

## Exact HDD and cold-cache command

Run this only on the dedicated reference host. The target mount must be backed
by a rotational device; page-cache eviction affects every workload on the host.
The first Java process prepares and closes the database. The cache is dropped
only after that close, and the second process verifies the preparation marker,
preload dimensions, and byte-for-byte Rockserver configuration before reopening
the same database for both phases.

```bash
cd /srv/rockserver

mvn -q -DskipTests test-compile dependency:build-classpath \
  -Dmdep.outputFile=target/overload-benchmark.classpath

overload_classpath="target/test-classes:target/classes:$(<target/overload-benchmark.classpath)"
overload_root="/mnt/rockserver-hdd/overload-plan3-reference-20260723"
overload_options=(
  "--root=${overload_root}"
  "--preload-keys=1000000"
  "--preload-flush-keys=50000"
  "--value-bytes=256"
  "--warmup-seconds=15"
  "--measure-seconds=60"
  "--point-readers=8"
  "--foreground-writers=4"
  "--maintenance-writers=64"
  "--first-last-readers=2"
  "--cancellation-workers=2"
  "--foreground-write-rate=1000"
  "--maintenance-write-rate=0"
  "--first-last-rate=50"
  "--cancellation-rate=100"
  "--cancellation-delay-ms=1"
  "--cancellation-burst=64"
  "--point-request-count=8192"
  "--range-request-count=8192"
  "--range-width=1024"
  "--read-parallelism=20"
  "--write-parallelism=36"
  "--maintenance-write-parallelism=1"
  "--foreground-queue-capacity=4096"
  "--maintenance-queue-capacity=512"
  "--admission-sample-micros=250"
  "--max-latency-samples=1000000"
  "--write-buffer-size=64MiB"
  "--direct-io=false"
  "--spinning=false"
  "--seed=5931033225068892758"
)

findmnt -T "${overload_root%/*}"
lsblk -d -o NAME,ROTA,SIZE,MODEL

java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
  -cp "${overload_classpath}" \
  it.cavallium.rockserver.core.impl.benchmark.GrpcOverloadBenchmark \
  "${overload_options[@]}" --prepare-only=true

sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'

java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
  -cp "${overload_classpath}" \
  it.cavallium.rockserver.core.impl.benchmark.GrpcOverloadBenchmark \
  "${overload_options[@]}" --reuse-preloaded=true --enforce=true

jq '{passed:.acceptance.passed, phases:[.phases[] | {
  phase,
  foreground_p99_ns:.operations.foreground.p99_ns,
  deadlines:.operations.foreground.deadlines,
  maintenance_progress:.operations["maintenance-write"].successes,
  max_maintenance_active:.admission.maintenance.max_active,
  drained:.resources_after_drain.drained
}]}' "${overload_root}/results.json"
```

The generated config always retains `maximum-open-files: -1`. `write=36` is the
hard combined worker limit; `maintenance-write=1` is a sub-limit, not an extra
worker. The 5,000 ms deadline and 2x p99 gate are not command-line knobs.

## Local structural baseline

This development host exposes only a non-rotational Samsung NVMe device, and
the writable `/tmp` benchmark root is tmpfs. Therefore the following is a
transport, admission, report, drain, and leak baseline—not the required HDD
release result. The prepare/reopen workflow was exercised without privileged
global page-cache eviction using OpenJDK 25.0.3, 12 available processors,
10,000 preloaded keys, 1 second warmup, and 2 second measurement phases.

| Measurement | Foreground only | Maintenance flood |
|---|---:|---:|
| Foreground throughput/s | 5,659.227 | 7,181.246 |
| Foreground p99 | 0.966 ms | 0.645 ms |
| Foreground deadlines | 0 | 0 |
| First/last deadlines | 0 | 0 |
| Maintenance throughput/s | 0 | 23,129.683 |
| Maintenance successful writes | 0 | 46,261 |
| Cancelled queued calls | 0 | 86 |
| Peak foreground queue | 1 | 1 |
| Peak maintenance queue | 0 | 36 |
| Peak combined active writes | 1 | 2 |
| Peak active maintenance writes | 0 | 1 |
| Foreground / maintenance rejections | 0 / 0 | 0 / 0 |
| Drain time | 0 ms | 10 ms |

The local run passed every gate: p99 ratio 0.668, queues and pending operations
drained, open transactions/iterators/range cursors were zero, native-handle leak
count was zero, and shutdown was clean. The HDD/cold-cache row must be generated
on the rotational reference host with the exact command above before release;
no HDD result is inferred from this tmpfs baseline.
