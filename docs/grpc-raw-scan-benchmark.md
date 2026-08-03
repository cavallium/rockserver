# Paired whole-path raw-SST benchmark

`GrpcRawScanBenchmark` closes the raw-scan performance and conservation gap left by
the mixed-workload runner. It exercises this path for both an untouched baseline and
the release candidate:

```text
generated gRPC client -> Netty server -> BATCH admission -> cooperative SST readers
  -> RocksDB SstFileReader -> serialized wire batches -> client decode
```

The controller creates one database with deterministic keys and values, disables
automatic compaction, and explicitly flushes eight SST groups. Both builds reopen
that exact database sequentially. Each child uses the same Java binary, JVM options,
configuration, client count, scheduler parallelism, host, and storage. Odd rounds run
baseline first; even rounds run candidate first. Every child completes one full warm
scan per client before timing, so this gate intentionally measures warmed raw scans.

Five clients each expose Rockserver's existing four-way per-stream SST parallelism,
enough to saturate the default 20 READ workers. During every measured interval the
candidate's strict scheduler sampler requires observed 20/20 activity and rejects an
eligible queued task coexisting with a sleeping worker for more than the eight-ms
cooperative quantum plus one sampling period. No scheduler sleep or rate cap is
introduced for raw-only work.

Every response batch is bounded, decoded, and checked against the complete expected
key set and deterministic value. A missing, duplicate, corrupt, out-of-range, or
malformed entry fails immediately. The gRPC interceptor independently requires every
measured `ClientCall.start` to receive exactly one `onClose(OK)`, with no in-flight
call left behind. Scheduler accepted/started/completed/outcome counters, queues,
active/parked/outstanding work, database resources, native handles, and client/server
shutdown must all balance. At least one ordinary full-sized wire batch must be observed
in a full run.

The controller verifies both production-class directories against their declared Git
SHAs and clean-state declarations before creating the dataset. It fingerprints the
complete classpath contents and deterministic dataset, passes those fingerprints into
every child, and rejects any worker artifact with missing, extra, or mismatched
provenance fields. The candidate must expose Wave 1's exact submission-attempt
accounting. For immutable `bb4f1a7`, which predates those snapshot accessors, the
benchmark derives aggregate outstanding as accepted minus terminal outcomes and parked
as outstanding minus queued minus active; the artifact marks that accounting as legacy
rather than claiming the new exact API.

The v3 controller fixes exactly ten paired rounds and computes geometric
candidate/baseline ratios with two-sided Student-t 95% intervals in log space. It gates
MiB/s, entries/s, scheduler queue/execution p99, complete-stream p99, CPU ns/entry,
allocated bytes/entry, peak live heap/direct memory/RSS, GC, thread count, native handles,
and peak parked/outstanding work. Automatic acceptance requires every geometric mean to
be no worse than 1.0 and no confidence interval to demonstrate a regression. GC,
threads, native handles, parked work, and outstanding work allow no increase in any pair;
allocation and those exact resource gates have no exception path.

The candidate must also prove at least one predeclared material improvement: throughput
lower 95% at least 1.02, or latency/CPU/allocation/memory upper 95% at most 0.98. Ratios
inside 0.99 throughput or 1.02 latency/CPU/memory are reported only as exception
candidates. They still fail automatically and require commit ablation, bounded causal
proof, profiles showing no lower-cost implementation, a compensating gain, and explicit
user approval. There is no adaptive stopping.

## Run the release comparison

Build the untouched baseline in a separate checkout or extracted archive, then
compile the candidate's test sources and runtime classpath:

```bash
mvn -q -DskipTests test-compile dependency:build-classpath \
  -Dmdep.outputFile=target/raw-scan-benchmark.classpath

raw_classpath="target/test-classes:target/classes:$(<target/raw-scan-benchmark.classpath)"
raw_main="it.cavallium.rockserver.core.impl.benchmark.GrpcRawScanBenchmark"
baseline_sha="bb4f1a7e90db1fdfd785936594d080e8c4a0ba4e"
candidate_sha="REPLACE_WITH_CLEAN_CANDIDATE_FULL_SHA"
raw_root="/mnt/rockserver-hdd/raw-scan-${candidate_sha}"

java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
  -cp "${raw_classpath}" "${raw_main}" \
  "--root=${raw_root}" \
  --baseline-classes=/tmp/rockserver-baseline/target/classes \
  --candidate-classes=target/classes \
  "--build-baseline=${baseline_sha}" "--build-candidate=${candidate_sha}" \
  --build-state-baseline=clean --build-state-candidate=clean \
  --storage-label=hdd-btrfs --host-state=dedicated --enforce=true
```

The default full shape is 1,000,000 256-byte values, flushes every 125,000 keys,
five scan clients, 20 READ workers, one complete warmup pass per client, and at least
15 measured seconds per child. Pareto mode defaults to and requires exactly ten pairs.
`--enforce=true` requires the immutable v1.3.11 baseline and clean full Git SHAs,
dedicated non-CI hardware, and those dimensions. The root is one-shot and is never
overwritten. Preserve `results.json`, `results.md`, metadata, the immutable dataset,
and every per-round worker file.

Use `--smoke=true --enforce=false` only to validate structure and binary compatibility.
A tmpfs/shared-host smoke result, a dirty build, a short interval, or an incomplete
metric set is not release evidence. This warmed raw-only gate
complements, but does not replace, the cold-cache mixed gRPC gate, seven-profile
hardware selector, full Maven validation, or the Yotsuba production canary.
