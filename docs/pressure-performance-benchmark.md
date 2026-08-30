# Scheduler and backfill pressure performance proof

This suite separates executable structure from hardware acceptance. CI compiles the workers,
exercises strict parsing/controller failures, and runs a small real-RocksDB workload. A release
claim requires the full prepared schedule on an otherwise idle representative host.

## Immutable paired scheduler and signal workflow

`PressurePerformancePairedBenchmark` compares `SchedulerHighContentionBenchmark` and
`StoragePressureSignalBenchmark`. Preparation freezes all workload parameters and writes exactly
40 ordered rows: ten counterbalanced baseline/candidate pairs for each of the two suites. Every row
must be a fresh JVM; a worker refuses to run before any preceding artifact exists or after its own
artifact exists. Evaluation rejects missing, duplicate, unknown, or non-finite metrics; build,
configuration, runtime, host, hardware, classpath, and production-byte drift; reused process IDs;
and reordered or overlapping timestamps.

After compiling the final harness, obtain the test classpath from the current Surefire report as
described in `docs/workload-tuning.md`. Prepare once:

```bash
main=it.cavallium.rockserver.core.impl.benchmark.PressurePerformancePairedBenchmark
java -cp "${workload_classpath}" "${main}" \
  --mode=prepare --root=/mnt/bench/pressure-paired \
  --baseline-sha=<full-baseline-sha> --candidate-sha=<full-candidate-sha> \
  --host-state=dedicated --hardware-description=<stable-host-label> --enforce=true
```

Follow `schedule.tsv` serially. For each row launch the worker from the scheduled checkout and
production classpath, passing its suite, round, implementation, and exact build SHA:

```bash
java -cp "${selected_classpath}" "${main}" \
  --mode=worker --root=/mnt/bench/pressure-paired \
  --suite=<scheduler-or-signal> --round=<1-10> \
  --implementation=<baseline-or-candidate> --build-sha=<scheduled-sha>
```

Then evaluate with `--mode=evaluate --root=...`. `results.json` follows
`benchmarks/schemas/pressure-performance-comparison-v1.schema.json`; `results.md` is the human
review. Worker artifacts follow `pressure-performance-worker-v1.schema.json`. Scheduler automatic
acceptance requires ten complete pairs, no protected regression, and a material predeclared gain.
The signal suite requires strict non-regression and zero warmed allocation but does not require a
speed gain for a correctness-driven change.

## Statistical contract v2

The v1 result is immutable. Its point-estimate rule remains authoritative for every schedule
prepared without `--contract-version=v2`; a completed v1 `FAIL` is never re-evaluated under v2.
At true equality, the sign of one symmetric noisy point estimate is favorable with probability
one half. The default pressure matrix contains 237 v1 ratio metrics, so under independence the
probability that equality passes every point-estimate check is `2^-237`; arbitrary correlation
does not provide a usable error bound. Searching all predeclared primaries for a 2% confidence
gain also needs multiplicity control.

V2 uses the existing operational ceilings as non-inferiority margins, fixed before measurement:

- throughput candidate/baseline must be at least `0.99`;
- latency, CPU, allocation, memory, depth, GC, thread, and handle cost ratios must be at most `1.02`;
- non-negative count metrics use the predeclared transform `(candidate + 1) / (baseline + 1)` so
  zero GC/handle/direct-memory samples remain defined without selecting a pseudocount from results;
- warmed pressure-evaluator allocation remains an exact pairwise no-increase ceiling. Worker
  correctness, terminal conservation, progress, drain, shutdown, and leak checks remain hard
  structural gates and cannot be overridden by statistics.

Every stochastic metric uses the paired log ratio and Student-t model. V2 has three decisions:

- `FAIL`: a structural/deterministic gate failed, or a regression beyond its operational margin
  was demonstrated after Holm-Bonferroni correction over the complete stochastic metric set.
  Holm controls the probability of any false statistical `FAIL` at family-wise alpha `0.05` under
  arbitrary metric dependence.
- `PASS`: no failure occurred and every stochastic metric proves one-sided non-inferiority at
  alpha `0.05`. This is an intersection-union claim: if any metric is truly outside its margin,
  the probability of falsely claiming that all are non-inferior is at most `0.05`; no independence
  assumption or multiplicity discount is used.
- `INCONCLUSIVE`: no toleranced regression was demonstrated, but at least one metric lacks enough
  precision to prove non-inferiority. Inconclusive evidence is not relabeled as failure or pass.

Reciprocal-margin TOST equivalence is reported for diagnosis. A 2% material improvement remains
evidence only and its primary-metric search is Holm-adjusted; v2 does not require an improvement
to accept a correctness-driven refactor. These rules are fixed in the v2 schema and configuration
fingerprint. There is no adaptive stopping.

Prepare an entirely new, non-existing root:

```bash
java -cp "${workload_classpath}" "${main}" \
  --mode=prepare --contract-version=v2 --root=/mnt/bench/pressure-paired-v2 \
  --baseline-sha=<full-baseline-sha> --candidate-sha=<full-candidate-sha> \
  --host-state=dedicated --hardware-description=<stable-host-label> --enforce=true
```

The v2 metadata and schedule schemas, plus a configuration hash containing the alpha, margins,
multiplicity method, and version, prevent v1 worker artifacts from being reused. V2 still runs ten
fixed counterbalanced pairs per suite (40 fresh JVMs). Results follow
`benchmarks/schemas/pressure-performance-comparison-v2.schema.json`. Worker measurements retain
the v1 worker serialization format; they are raw measurements, and their v2 configuration hash
binds them exclusively to the fresh v2 schedule.

## Read-only precision planner

An `INCONCLUSIVE` v2 result is not a failure and must not be re-evaluated with selected metrics or
new margins. `PressurePerformancePrecisionPlanner` reads the immutable v2 metadata, schedule,
worker artifacts, and comparison result; revalidates every worker identity, configuration,
correctness flag, process ID, order, host, and runtime; and verifies that the workers still
reproduce the recorded `INCONCLUSIVE` decision. It refuses v1, `PASS`, `FAIL`, incomplete,
tampered, or already-planned roots.

The planner uses all predeclared stochastic metrics. For each metric it computes the sample
standard deviation `s` of the ten paired log ratios. It deliberately plans at true equality,
not at the observed favorable or unfavorable mean. For a log non-inferiority margin `m`, fixed
pair count `n`, and one-sided alpha `0.05`, version 1 reports the transparent normal-power
approximation

```text
power(n) = Phi(m * sqrt(n) / s - t(0.95, n - 1)).
```

It reports the smallest fixed `n` reaching the target and the alternative worker-duration scale
assuming variance falls inversely with measured work. This assumption is explicit evidence
planning, not a guarantee; the fresh run remains authoritative.

The desired probability that every metric proves non-inferiority is `0.90`. A union-bound design
spends beta `0.03` on 54 scheduler-quality metrics and beta `0.07` on all remaining stochastic
metrics. Queue p99, end-to-end p99, and maximum progress-gap metrics are predeclared critical, so
their per-metric target is stricter. Deadline accounting, terminal conservation, progress, drain,
shutdown, and leaks remain hard correctness gates rather than variance-planned metrics. The
planner never recommends relaxing the `0.99`/`1.02` margins.

The original planner's duration-only recommendation could overflow the scheduler's integer
operation count on real evidence. Precision-plan v2 therefore caps each worker at a predeclared
`64x` measured-duration scale, additionally bounded by every integer field. It then recomputes each
metric's required pair count using `s / sqrt(chosen-duration-scale)` and selects the maximum. This
mixed design never pretends the capped duration alone reaches target power. The cap is recorded in
v2.1 metadata and its configuration hash. The source v2 configuration and result SHA-256 values
are also mandatory v2.1 metadata, so copying or changing a recommendation changes the fresh
configuration hash and invalidates its prewritten schedule.

The executable next run uses contract version `v2.1`, the bounded scheduler/signal duration scales,
and enough fixed pairs to satisfy every predeclared metric under the planning approximation. It
still has no adaptive stopping. Existing v1 and v2 metadata remain unchanged; v2 without a
`fixed-pairs` field continues to mean exactly ten. It writes:

- `precision-plan.json`, schema
  `benchmarks/schemas/pressure-performance-precision-plan-v2.schema.json`;
- `precision-plan.md`, with the limiting metrics first;
- `next-run-v2.properties`, the complete predeclared fixed configuration.

The v2.1 schedule, metadata, worker, and comparison formats are separately versioned as
`pressure-performance-{schedule,metadata}-v2.1`, `pressure-performance-worker-v2`, and
`pressure-performance-comparison-v2.1`. Pair count, round, ordinal, vector length, and the total
`4 * pairs` fresh-process count remain exact and tamper-checked.

Run it against the baseline and candidate production class roots:

```bash
java -cp "${workload_classpath}" \
  it.cavallium.rockserver.core.impl.benchmark.PressurePerformancePrecisionPlanner \
  --root=/mnt/bench/pressure-paired-v2 \
  --baseline-classes=/path/to/baseline/target/classes \
  --candidate-classes=/path/to/candidate/target/classes
```

The planner hashes exactly
`it/cavallium/rockserver/core/impl/StoragePressureSignal.class` in both roots. Equality is recorded
only as component-byte provenance for the unchanged signal evaluator. It never changes the source
decision, removes signal metrics, reduces required evidence, or supplies a statistical PASS.

Prepare the recommended root directly from the emitted option file:

```bash
mapfile -t planned_options < <(sed 's/^/--/' /mnt/bench/pressure-paired-v2/next-run-v2.properties)
java -cp "${workload_classpath}" "${main}" \
  --mode=prepare --root=/mnt/bench/pressure-paired-v2.1 "${planned_options[@]}"
```

Metrics-on/off is not folded into this controller: the high-contention harness currently constructs
an unmetered scheduler, and changing the measured workload to register Micrometer only on one side
would no longer be an ablation of identical code paths. Use the existing fresh-process
`SchedulerHotPathBenchmark` `normal`/`normal-metrics` and `latency`/`latency-metrics` scenarios for
diagnosis; promote that comparison only after it has the same immutable artifact/provenance contract.

## Real RocksDB mixed backfill pressure gate

`MixedBackfillPressureBenchmark` owns a new, non-existing root and never deletes or reuses it. Its
correctness prelude creates several real SSTs, runs a resumable raw scan, cancels upstream exactly
after the first completion token, persists every acknowledged token with `DSYNC` and file/directory
`force`, resumes with exact row conservation, closes/reopens the database, and proves completed SSTs
are skipped.

The timed phase concurrently runs full raw-SST BATCH scans, INGEST writes, LATENCY gets, CDC poll and
durable commit, flush/compaction, and alternating injected pressure. It gates configured minimum
backfill and ingest throughput, maximum zero-progress gap, CDC lag, LATENCY p99, CPU/allocation/GC,
queue/parked/outstanding peaks, heap/direct/RSS/thread/handle peaks, final drain, clean shutdown, and
native leaks. A bounded smoke run is part of Maven; a representative run is opt-in:

The driver precomputes and reuses the entire immutable key space and value payload, so measured
allocation is not dominated by key construction. Allocations intentionally left in the contract are
the real API/scheduler task wrappers, raw-scan event and wire-batch objects, CDC event/page objects,
and RocksDB/JNI result ownership needed by the production paths.

```bash
java -cp "${workload_classpath}" \
  it.cavallium.rockserver.core.impl.benchmark.MixedBackfillPressureBenchmark \
  --root=/mnt/bench/mixed-pressure-<candidate-sha> \
  --preload-keys=50000 --flush-every=5000 --measure-ms=10000
```

The result schemas are `mixed-backfill-pressure-v1.schema.json`. Injected pressure proves scheduler
transitions; production observation is still required to prove RocksDB's native pressure signals,
storage latency, NUMA behavior, and the deployed Yotsuba backfill SLO.
