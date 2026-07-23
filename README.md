# Rockserver

## Write admission

Mutating APIs default to the `FOREGROUND` write class. Rebuildable/background
callers may select `MAINTENANCE`; both classes share the hard `write` worker
limit, while maintenance also obeys its smaller sub-limit. Foreground work can
use every worker when maintenance is absent. With both queues populated, one
maintenance task is selected after at most 32 foreground dequeues.

```hocon
database.parallelism {
  read = 20
  write = 36
  maintenance-write = 1
  foreground-write-queue-capacity = 4096
  maintenance-write-queue-capacity = 512
}
```

Each queue rejects overflow immediately as `SERVER_OVERLOADED`; gRPC exposes it
as `RESOURCE_EXHAUSTED`. Admission publishes the following metrics with
`database` and `lane` (`foreground` or `maintenance`) tags:

- `rockserver.write.admission.queued`
- `rockserver.write.admission.active`
- `rockserver.write.admission.queue.wait`
- `rockserver.write.admission.execution`
- `rockserver.write.admission.completed`
- `rockserver.write.admission.cancelled`
- `rockserver.write.admission.rejected`
- `rockserver.write.admission.worker.limit`
- `rockserver.write.admission.queue.limit`

## gRPC overload regression benchmark

`GrpcOverloadBenchmark` is an opt-in, disk-backed runner separate from ordinary
CI and the embedded `FastGetBenchmark`. It compares foreground-only and
maintenance-flood phases against one preloaded database while keeping every
foreground and first/last request on the fixed five-second deadline. Each run
emits `results.json` and `results.md` and enforces foreground latency,
maintenance progress, admission limits, cancellation, drain, native-handle,
and shutdown checks.

Use the prepare/reopen workflow for a real cold page cache. The exact reference
command, options, acceptance rules, and current local baseline are documented in
[`benchmarks/grpc-overload-2026-07-23.md`](benchmarks/grpc-overload-2026-07-23.md).

## Fast unary GET

Embedded databases with `database.global.enable-fast-get=true` use the owned
native GET API from `it.cavallium:rocksdbjni:11.1.2.5`. The public synchronous
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
