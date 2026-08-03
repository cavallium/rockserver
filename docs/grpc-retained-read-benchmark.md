# Paired retained-read non-regression benchmark

`GrpcRetainedReadBenchmark` is the exclusive release-performance gate for cooperative
retained reads. It compares an untouched baseline with a release candidate through the
generated gRPC client and Netty server, using the same immutable RocksDB dataset and a
fresh JVM for every measurement.

The controller writes the complete schedule before preparing the dataset. It always runs
ten paired rounds, alternates baseline/candidate order by round, and never stops early.
Each of these operations has an isolated scenario and a scenario mixed with foreground
`LATENCY` point reads:

- exact range count;
- streamed range, including time to first item;
- multi-chunk `existsMulti` with deterministic hits and misses;
- explicit-iterator skip/take with both counts greater than 4,096.

Each child records a process-first cold probe separately, completes a fixed warmup, and
then enters the steady-state window. Results are accepted only after exact count, order,
hit/miss, value, and checksum validation. Worker files use a strict property schema:
unknown, duplicate, missing, empty, non-finite, or provenance-mismatched fields fail the
run. The controller records the exact 40-character Git SHAs, normalized classpaths and
their SHA-256 digests, dataset digest, JVM, RocksDB version, OS, hardware, storage, cache
state, execution order, and command-relevant dimensions.

## Metrics and acceptance

For each scenario, the controller computes the candidate/baseline paired log-ratio. It
forms a two-sided Student-t 95% confidence interval in log space and exponentiates the
three reported bounds. Every scenario must satisfy all applicable limits:

| Metric | Required candidate / baseline bound |
|---|---:|
| entries/s and MiB/s | lower 95% >= 0.99 |
| completion p99 | upper 95% <= 1.02 |
| streamed first-item p99 | upper 95% <= 1.02 |
| mixed foreground p99 | upper 95% <= 1.02 |
| cold completion and streamed cold first item | upper 95% <= 1.02 |
| process CPU ns/item | upper 95% <= 1.02 |
| allocated bytes/item | upper 95% <= 1.00 |
| peak live heap and direct memory | upper 95% <= 1.02 |

An interval crossing its threshold is inconclusive and therefore fails. Missing or
malformed samples also fail; there is no adaptive extension of the ten rounds. GC counts
and time are reported alongside the gated metrics.

Every optimized candidate request must record exactly one accepted, one started, and one
terminal scheduler task. Mixed scenarios must force more quantums than logical tasks.
Scheduler failures, rejections, cancellations, duplicate terminals, native leaks, or a
nonzero final queue/resource count fail the worker. Retained snapshots, permits, waiters,
range cursors, iterators, and iterator leases must not peak above the paired baseline,
and the configured retained-resource limit must not change.

## Run the release comparison

Build the exact baseline and candidate commits in separate clean worktrees. Compile the
candidate test harness and capture its dependency classpath:

```bash
mvn -q -DskipTests test-compile dependency:build-classpath \
  -Dmdep.outputFile=target/retained-read-benchmark.classpath

retained_classpath="target/test-classes:target/classes:$(<target/retained-read-benchmark.classpath)"
retained_main="it.cavallium.rockserver.core.impl.benchmark.GrpcRetainedReadBenchmark"
baseline_sha="REPLACE_WITH_RAW_CHECKPOINT_SHA"
candidate_sha="REPLACE_WITH_FINAL_RC_SHA"
retained_root="/mnt/rockserver-hdd/retained-read-${candidate_sha}"

java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
  -cp "${retained_classpath}" "${retained_main}" \
  "--root=${retained_root}" \
  --baseline-classes=/tmp/rockserver-baseline/target/classes \
  --candidate-classes=target/classes \
  "--build-baseline=${baseline_sha}" "--build-candidate=${candidate_sha}" \
  --build-state-baseline=clean --build-state-candidate=clean \
  --storage-label=nvme-ext4 --host-state=dedicated --cache-state=warmed \
  --enforce=true
```

Full defaults use 262,144 deterministic 256-byte values, 16,384 `existsMulti`
items, 8,192-element iterator skip and take stages, four READ workers, four foreground
workers, two warmup operations, ten measured seconds, and a four-resource retained
limit. The resulting fixed matrix contains 160 fresh worker processes: eight scenarios,
two implementations, and ten rounds.

The root is one-shot and is never overwritten. Preserve `metadata.properties`,
`schedule.tsv`, `results.json`, `results.md`, the shared dataset metadata, and all worker
artifacts. `--enforce=true` requires clean full SHAs, dedicated hardware, explicit
storage/cache provenance, and full dimensions. Run baseline and candidate sequentially
on an otherwise quiet machine; do not overlap them with Maven, IDE indexing, another
benchmark, or background I/O work.

`--smoke=true --enforce=false` is for structural validation only. It still requires exact
SHAs and executes the fixed schedule, so it is not a quick unit test and is never release
evidence. Ordinary Maven validation runs only the deterministic parser and acceptance
tests; Plan P owns authoritative measurements.
