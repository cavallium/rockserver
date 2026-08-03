# Rockserver

## Workload admission

Rockserver schedules requests by the explicit `LATENCY`, `INGEST`, `CDC`,
`ANALYTICAL`, `BATCH`, `CONTROL`, and `PHYSICAL_MAINTENANCE` profiles. Data
profiles share hard read/write worker limits with borrowable reservations;
CONTROL and physical maintenance use isolated pools.

```hocon
database.parallelism {
  read = 20
  write = 36
  workload {
    latency-queue-capacity = 4096
    ingest-queue-capacity = 4096
    cdc-queue-capacity = 1024
    analytical-queue-capacity = 512
    batch-queue-capacity = 512
  }
}
```

Each queue rejects overflow immediately as `SERVER_OVERLOADED`; gRPC exposes it
as `RESOURCE_EXHAUSTED`. Admission metrics use bounded `database`, `resource`,
`profile`, and `operation` tags where applicable:

- `rockserver.workload.queued`
- `rockserver.workload.active`
- `rockserver.workload.queue.wait`
- `rockserver.workload.execution`
- `rockserver.workload.quantums`
- `rockserver.workload.outcomes`
- `rockserver.workload.cancellations`
- `rockserver.workload.rejections`
- `rockserver.workload.failures`
- `rockserver.workload.worker.failures`
- `rockserver.workload.storage.pressure`
- `rockserver.workload.retained.snapshots`
- `rockserver.workload.cdc.lag`

See [workload profiles](docs/workload-profiles.md) for admission rules and
[workload configuration](docs/workload-configuration.md) for every setting,
default, and startup invariant.

## gRPC overload regression benchmark

`GrpcOverloadBenchmark` is the opt-in, disk-backed whole-path complement to the
embedded `SevenProfileWorkloadBenchmark`. It sends real loopback gRPC traffic for
all seven profiles during a harsh mixed phase. Every measured RPC is counted at
client-call start and exactly one terminal close; an independent integrity phase
writes unique INGEST/BATCH values and reads every acknowledged value back through
LATENCY. Scheduler snapshots prove all four pools drain with balanced
started/completed counters and no worker failures, while sampled sleeping-worker
state detects eligible queued work that leaves capacity idle for more than a short
wake-up streak. The result also gates foreground p99/throughput regression,
priority ordering, the eight-millisecond cooperative queue bound, cancellation,
profile progress, resources, native handles, errors, rejections, and shutdown.

This gRPC runner does not replace the release tuner: pressured-BATCH acceptance,
candidate selection, and controlled hardware comparisons still belong to
`SevenProfileWorkloadBenchmark` and its selector. A smoke run proves structure and
correctness only; it is not hardware acceptance.

Use the prepare/reopen workflow for a real cold page cache. The current command,
options, and acceptance rules are documented in
[`docs/grpc-mixed-workload-benchmark.md`](docs/grpc-mixed-workload-benchmark.md).
The dated
[`benchmarks/grpc-overload-2026-07-23.md`](benchmarks/grpc-overload-2026-07-23.md)
is historical two-profile evidence and is not comparable to the current schema.
The release hardware workflow and deterministic 5% throughput/10% p99 selector
are documented in [`docs/workload-tuning.md`](docs/workload-tuning.md).

`GrpcRawScanBenchmark` is the paired whole-gRPC raw-SST gate. It creates one
explicitly flushed dataset, runs the untouched and candidate production classpaths
in alternating order, validates every streamed key/value and gRPC terminal, proves
READ-pool saturation and bounded avoidable idle time, and evaluates paired 95%
confidence intervals for raw throughput, scheduler queue p99, and complete-scan p99.
The exact one-shot command and acceptance boundary are documented in
[`docs/grpc-raw-scan-benchmark.md`](docs/grpc-raw-scan-benchmark.md).

## Fast unary GET

Embedded databases with `database.global.enable-fast-get=true` use the owned
native GET API from `it.cavallium:rocksdbjni:11.1.2.6`. The public synchronous
API always returns an independent heap `Buf`. Unary gRPC current-value reads may
instead retain a RocksDB pin only for synchronous response framing; transactions,
bucketed columns, and proxy backends keep the ordinary implementation.

The default gRPC strategy is `automatic`: it inspects the pinned result size and
either streams it directly or uses the JNI `copyAndReset` path for independently
owned heap output.
The measured default uses pinned streaming through 128 bytes, from 512 bytes
through 4 KiB, and at or above 32 KiB. The remaining bands use independently
owned heap output. Operators can replace that table with one cutoff by setting
`-Drockserver.grpc.fast-get.pinned-min-bytes=<bytes>`. The
`rockserver.grpc.fast-get.strategy` property accepts `legacy`, `exact-heap`,
`pinned`, and `automatic`; the non-automatic values exist for the
performance matrix and operational comparison.

Run the five-round real-RocksDB/local-gRPC release gate with:

```shell
mvn -DskipTests test-compile org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.classpathScope=test \
  -Dexec.mainClass=it.cavallium.rockserver.core.impl.benchmark.GrpcFastGetBenchmark
```

## Package fat jar
```shell
mvn -Pfatjar -Dagent -DskipTests clean package
```

## Package native
```shell
GRAALVM_HOME=/usr/lib/jvm/xx;JAVA_HOME=/usr/lib/jvm/xx mvn -Pnative -Dagent -DskipTests clean package
```

## Deploy the library
```shell
mvn -Plibrary -Dagent -DperformRelease=true -DskipTests -Dgpg.skip=true -Drevision=1.0.0-SNAPSHOT deploy
```
