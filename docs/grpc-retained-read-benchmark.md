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
run. The controller derives each 40-character Git SHA from the checkout that owns the
selected production-classes directory and rejects a mismatch or a checkout declared
clean that is dirty. Its classpath SHA-256 covers the normalized entry order, paths,
relative file names, sizes, and every file byte; each child recomputes it before loading
the database. The report also records the dataset digest, JVM, RocksDB version, OS,
hardware, storage, cache state, execution order, and command-relevant dimensions.

## Metrics and acceptance

For each scenario, the controller computes the candidate/baseline paired log-ratio. It
forms a two-sided Student-t 95% confidence interval in log space and exponentiates the
three reported bounds. Every geometric-mean estimate must be no worse than equality,
and its interval must not demonstrate a regression:

| Metric | Automatic gate | Exception ceiling |
|---|---:|---:|
| entries/s and MiB/s | ratio >= 1.00 | ratio >= 0.99 |
| queue, execution, completion, first-item, foreground, and cold p99 | ratio <= 1.00 | ratio <= 1.02 |
| process CPU ns/item | ratio <= 1.00 | ratio <= 1.02 |
| allocated bytes/item | ratio <= 1.00 | none |
| peak live heap, direct memory, and RSS | ratio <= 1.00 | ratio <= 1.02 |
| GC collections/time, peak threads, and peak native handles | no increase in any pair | none |
| parked/outstanding and retained/native lifetime peaks | no increase in any pair | none |

At least one predeclared primary metric must also demonstrate a material improvement:
throughput lower 95% at least 1.02, or latency/CPU/allocation/memory upper 95% at most
0.98. An exception-ceiling result is never an automatic pass; the report only identifies
it for the separate ablation, profiling, compensating-gain, and explicit-approval process.
Missing or malformed samples fail, and there is no adaptive extension of the ten rounds.

Every optimized candidate request must record exactly one accepted, one started, and one
terminal scheduler task. Mixed scenarios must force more quantums than logical tasks.
Scheduler failures, rejections, cancellations, duplicate terminals, native leaks, or a
nonzero final queue/resource count fail the worker. Candidate workers must expose the
Wave 1 exact scheduler accounting, and after drain submission attempts must equal terminal
outcomes; the immutable baseline is explicitly reported with the conservative legacy
fallback used by the binary-compatible harness. Parked and outstanding scheduler tasks,
retained snapshots, permits, waiters,
range cursors, iterators, iterator leases, and `existsMulti` logical requests, snapshots,
read options, and per-call arenas must not peak above the paired baseline. Every one of
those resources must drain to zero, and the configured retained-resource limit must not
change.

The measurement-only sampler also records scheduler queue/execution p99 and process CPU,
allocation, live heap, direct memory, Linux RSS, live threads, open native descriptors,
and GC deltas. It runs on the benchmark sampler thread and never adds a scheduler lock,
registry lookup, queue scan, or per-yield callback to production hot paths.

Every worker instruments the selected `EmbeddedDB.existsMultiStatusOnly` bytecode before
opening the database. The harness requires exactly one `Arena.ofConfined` call in that
method, replaces that exact call with a tracked wrapper, and then requires zero original
calls and exactly one replacement call in the transformed bytes. Baseline and candidate
therefore measure the same real arena opens and successful closes with symmetric overhead;
double-close, close failure, underflow, or a nonzero final count fails the run. Each worker
is launched with the pinned Byte Buddy artifact as a startup agent (no dynamic attach) and
records a SHA-256 over the transformed bytecode, harness bytecode, and Byte Buddy agent
artifact, and all fixed rounds for an implementation must report the same digest. The
controller accepts only immutable v1.3.11
`bb4f1a7e90db1fdfd785936594d080e8c4a0ba4e` as the baseline SHA; there is no inferred or
hookless fallback.

## Run the release comparison

Build the exact baseline and candidate commits in separate clean worktrees. Keep each
`target/classes` directory inside its owning worktree: the controller and every worker
verify its checkout `HEAD`, and enforced runs verify the worktree is still clean.
Classpath entries and their contents must not be symbolic links. Compile the candidate
test harness and capture its dependency classpath:

```bash
mvn -q -DskipTests test-compile dependency:build-classpath \
  -Dmdep.outputFile=target/retained-read-benchmark.classpath

retained_classpath="target/test-classes:target/classes:$(<target/retained-read-benchmark.classpath)"
retained_main="it.cavallium.rockserver.core.impl.benchmark.GrpcRetainedReadBenchmark"
baseline_sha="bb4f1a7e90db1fdfd785936594d080e8c4a0ba4e"
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
