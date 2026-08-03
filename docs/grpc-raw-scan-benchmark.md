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
active work, database resources, native handles, and client/server shutdown must all
balance. At least one ordinary full-sized wire batch must be observed in a full run.

The controller computes paired candidate/baseline ratios and two-sided 95% Student-t
confidence intervals for serialized-byte throughput, scheduler BATCH/READ queue p99,
and complete-stream p99. It rejects the candidate when the complete throughput
interval is below 1.0 or the complete latency interval is above 1.0. An interval that
crosses equality is reported as inconclusive rather than mislabeled as a regression;
use longer runs when a release decision needs a narrower interval.

## Run the release comparison

Build the untouched baseline in a separate checkout or extracted archive, then
compile the candidate's test sources and runtime classpath:

```bash
mvn -q -DskipTests test-compile dependency:build-classpath \
  -Dmdep.outputFile=target/raw-scan-benchmark.classpath

raw_classpath="target/test-classes:target/classes:$(<target/raw-scan-benchmark.classpath)"
raw_main="it.cavallium.rockserver.core.impl.benchmark.GrpcRawScanBenchmark"
baseline_sha="REPLACE_WITH_UNTOUCHED_FULL_SHA"
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
five scan clients, 20 READ workers, five paired rounds, one complete warmup pass per
client, and at least 15 measured seconds per child. `--enforce=true` requires clean
full Git SHAs, dedicated non-CI hardware, and those minimum dimensions. The root is
one-shot and is never overwritten. Preserve `results.json`, `results.md`, metadata,
the immutable dataset, and every per-round worker file.

Use `--smoke=true --enforce=false` only to validate structure and binary compatibility.
A tmpfs/shared-host smoke result, a dirty build, a short interval, or an inconclusive
confidence interval is not release evidence. This warmed raw-only gate complements,
but does not replace, the cold-cache mixed gRPC gate, seven-profile hardware selector,
full Maven validation, or the Yotsuba production canary.
