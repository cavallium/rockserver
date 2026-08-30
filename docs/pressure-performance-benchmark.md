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
