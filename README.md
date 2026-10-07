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
confidence intervals for throughput, latency, CPU, allocation, memory, and exact
resource non-increase.
The exact one-shot command and acceptance boundary are documented in
[`docs/grpc-raw-scan-benchmark.md`](docs/grpc-raw-scan-benchmark.md).

`GrpcRetainedReadBenchmark` is the paired whole-gRPC non-regression gate for exact
counts, streamed ranges, split `existsMulti`, and long explicit-iterator continuations.
It runs ten predetermined baseline/candidate pairs for isolated and foreground-mixed
scenarios, validates every result and terminal resource count, and applies strict
log-space confidence bounds to throughput, latency, CPU, allocation, and memory.
The release-only workflow is documented in
[`docs/grpc-retained-read-benchmark.md`](docs/grpc-retained-read-benchmark.md).

The same-build explicit-iterator service-bound mechanism and its idle/non-idle causal ablation are
documented in [`docs/iterator-quantum-ablation.md`](docs/iterator-quantum-ablation.md).

Both read gates and the paired seven-profile gate share the v1.3.11 Pareto rules and
versioned result schemas documented in
[`docs/v1.3.11-performance-contract.md`](docs/v1.3.11-performance-contract.md).

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

## SST table properties

Read a column's persisted table statistics using an analytical context:

```java
var api = connection.getSyncApi(RequestContext.analytical(Duration.ofSeconds(30)));
var properties = api.getTableProperties(columnId);
long dataBytes = properties.dataSize();
long physicalEntries = properties.numEntries();
// Also available: getTablePropertiesAsync(columnId).
```

Embedded, gRPC and Thrift return the same immutable `ColumnTableProperties` model;
Rust exposes `get_table_properties`. Use `getAllColumnDefinitions()` to enumerate
columns. Each column is observed independently. The result contains SST counts,
data/index/filter and raw sizes, block/entry/deletion/merge counts, compression
estimates, timestamp bounds, and distributions of formats, fixed key lengths,
index flags, column-family IDs, compression and other table configuration names.
Distribution values count files; empty strings mean unspecified names. Sizes are
bytes and timestamps are Unix seconds (zero means unknown). Creation-time bounds
refer to RocksDB's oldest-ancestor timestamps, not filesystem creation times.
Compression estimates sum available samples only and can cover a subset of files.

This call does not flush, compact, or scan data rows. It excludes memtables, WALs,
blob files and custom collector byte strings. Entries can include tombstones and
obsolete versions, and bucketed columns count physical entries rather than logical
rows. Reading metadata can still perform I/O for every SST, so use ANALYTICAL for
interactive requests and BATCH for periodic work. Cancellation cannot interrupt a
native metadata read; its database and column leases remain held until it returns.

Optional background Micrometer collection is disabled by default:

```hocon
database.metrics {
  table-properties-enabled: true
  table-properties-interval-seconds: 300
}
```

`rocksdb.table.properties` exports numeric values tagged by `database`,
`column_family` and `property_name` (for example `data.size` or `num.entries`).
There are no per-file or arbitrary string-value labels. Collection runs sequentially
in the BATCH lane, at most once per configured interval (minimum 60 seconds), and
scrapes read cached values. Check `rocksdb.table.properties.collection.success`
and `rocksdb.table.properties.last.success.time` for failure and freshness. Failed
collections remove the old value series; successful collections remove dropped
columns. JMX and Influx exporters use the existing metrics configuration.

## Package fat jar
```shell
mvn -Pfatjar -Dagent -DskipTests clean package
```

## Package desktop UI

Select `desktop` instead of `fatjar`, `library`, or `native` (including in the IDE's Maven profiles).
It includes the server/client code, standalone logging, and the desktop UI in a shaded executable JAR:

```shell
mvn -Pdesktop -DskipTests package
java --enable-native-access=ALL-UNNAMED -jar target/rockserver-core-1.0.0-SNAPSHOT-desktop.jar
```

GUI sources and tests live under `src/desktop/java` and `src/desktop-test/java`.
`mvn -Pdesktop test` includes the GUI tests; use `xvfb-run -a` on a headless Linux machine.
The other profiles omit the GUI sources and runtime dependencies. Each build variant uses
its own class/test output directories, so switching profiles cannot reuse the wrong module descriptor.

## Package native
```shell
GRAALVM_HOME=/usr/lib/jvm/xx;JAVA_HOME=/usr/lib/jvm/xx mvn -Pnative -Dagent -DskipTests clean package
```

## Deploy the library
```shell
mvn -Plibrary -Dagent -DperformRelease=true -DskipTests -Dgpg.skip=true -Drevision=1.0.0-SNAPSHOT deploy
```
