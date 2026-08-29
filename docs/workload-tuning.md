# Workload tuning and hardware selection

`SevenProfileWorkloadBenchmark` is the release tuning harness for Rockserver's
seven-profile scheduler. It is an opt-in `main` class under test sources; ordinary CI
may compile it and run the pure selector tests, but CI output is never hardware
acceptance and must not change production defaults.

The result keeps `workload_checks_passed` separate from
`hardware_acceptance_passed`. The latter is true only for an `--enforce=true` run
that satisfies every workload, pressure, metric-presence, shutdown, and leak check.

The harness keeps these producers active at the same time:

| Profile | Benchmark operation | Primary family |
|---|---|---|
| `CONTROL` | Open then protected rollback | `CONTROL` |
| `LATENCY` | Point lookup with a fresh five-second absolute deadline | `POINT_LOOKUP` |
| `ANALYTICAL` | Bounded fallback range scan | `RANGE_PAGE` |
| `INGEST` | Live point mutation | `MUTATION` |
| `CDC` | Poll and commit against concurrent writes | `WAL_PAGE` |
| `BATCH` | Recovery point mutation | `MUTATION` |
| `PHYSICAL_MAINTENANCE` | Protected flush | `FLUSH` |

A separate LATENCY bounded-fan-out producer deliberately cancels requests so the
same result also records cancellation behavior. During the measured interval the
harness can assert storage pressure, keep it asserted for a fixed interval, and then
clear it. This records BATCH progress before/during/after pressure, verifies BATCH
recovery after pressure, and confirms that parked physical work resumes after pressure
clears.

## What every result records

`results.json`, `results.md`, `selection-input.properties`, and
`pareto-worker.properties` contain or reference:

- throughput by profile and by profile/family;
- queue, execution, and end-to-end p99;
- scheduler rejection/cancellation counters, client-observed rejected/cancelled
  attempts, deadlines, and scheduler quantum counts;
- maximum retained snapshots, CDC lag, observed pressure, and
  queue/active/parked/outstanding depths;
- process CPU ns/operation, allocated bytes/operation, GC collections/time, peak live
  heap, direct memory, RSS, thread count, and native handles;
- post-run pending operations, transactions, iterators, range cursors, snapshots,
  native-handle leaks, and shutdown status;
- the deterministic seed, exact build ID, dataset and comparison-shape fingerprints,
  cache-state assertion, storage label, candidate, and each per-profile SLO verdict;
- the exact isolated INGEST throughput used by the mixed-run acceptance gate;
- up to the first 32 unexpected error details, rather than only their count.

The queue and execution p99 values come from Rockserver's own
`rockserver.workload.queue.wait` and `rockserver.workload.execution` timers. The
harness attaches a private percentile-enabled registry only for the measurement
window. It neither changes production metric configuration nor production
scheduling.

The candidate SLO gates are intentionally conservative:

- LATENCY makes progress, has no five-second deadlines, and has p99 below five
  seconds;
- CONTROL and the ANALYTICAL fallback both make progress with p99 below one second;
- mixed INGEST throughput is at least 95% of its candidate's isolated baseline;
- CDC makes progress, exposes its lag meter, and stays below `--cdc-lag-limit`;
- ANALYTICAL and BATCH continue, and BATCH completes work after injected pressure;
- injected storage pressure is observed, PHYSICAL work progresses, every profile has
  zero unexpected errors, all logical resources drain, no native handle leaks are
  detected, intentional cancellation appears in both client outcomes and scheduler
  metrics, and shutdown is clean.

The LATENCY and ANALYTICAL rows are scheduler-level chat/fallback proxies. They do
not replace the Yotsuba one-hour canary or prove the end-to-end message path.
Run the complementary
[`GrpcOverloadBenchmark`](grpc-mixed-workload-benchmark.md) to exercise all seven
profiles through the generated gRPC client and server, prove exact RPC terminal
accounting and acknowledged-write round trips, and gate avoidable sleeping workers
plus paired foreground confidence intervals. Neither harness replaces the canary.

## Direct high-contention scheduler suite

`SchedulerHighContentionBenchmark` isolates `RWScheduler` from RocksDB and transport
cost so admission, dispatch, arbitration, and cooperative transitions can be driven
far harder than a disk-backed benchmark. Its default fresh-process run submits one
million operations from at least 64 platform threads across twelve profile/family
lanes, all seven profiles, and all four pools. It mixes:

- normal and byte-weighted DRR work;
- already-expired deadlines and bounded overload;
- cancellation racing dispatch;
- injected one-shot and `context.fail` failures;
- cooperative `YIELD` and real `PARK`/resume cycles;
- alternating storage pressure with concurrent read/write BATCH work.

Correctness is not inferred from throughput. The process fails unless every attempt
has exactly one terminal outcome, cancellation and deadline arbitration balances,
every profile makes useful progress, failure telemetry matches injected failures,
no command executes twice, every sampled queue/outstanding value stays inside the
scheduler's explicit bounds, and every pool drains with exact conservation. The
machine-readable report then records attempts/useful runs per second, per-profile
queue and execution p50/p95/p99, peak pool depths, and whether pressured BATCH limits
were observed.

After obtaining `workload_classpath` below, run:

```bash
contention_main="it.cavallium.rockserver.core.impl.benchmark.SchedulerHighContentionBenchmark"
java -cp "${workload_classpath}" "${contention_main}" \
  1000000 64 16 16 65536 104376208116478 5
```

The positional values are operations, submitter threads, read workers, write workers,
foreground/BATCH queue capacity, deterministic seed, and injected-failure percentage.
Use zero injected failures only when comparing performance against a known older build
whose failure telemetry is itself under test; correctness runs should retain injection.
The main class performs an unreported deterministic warm-up (10% of the requested
operation count, capped at 200,000) before creating a fresh measured scheduler.
For performance decisions,
run immutable baseline and candidate in fresh, serial, counterbalanced JVMs with the
same arguments and host state. Absolute throughput from ordinary CI is only a smoke
measurement; it is never proof of optimality or release acceptance.

## Obtain the direct-launch classpath

N's consolidated Rockserver Maven gate compiles the harness and writes the test
runtime classpath into its Surefire reports. Reuse that output after the gate; do
not invoke Maven again just to launch the hardware harness:

```bash
surefire_report="target/surefire-reports/TEST-it.cavallium.rockserver.core.impl.test.GrpcControlCleanupCancellationTest.xml"
workload_classpath="$(sed -n 's/.*<property name="java.class.path" value="\([^"]*\)".*/\1/p' \
  "${surefire_report}" | head -n 1)"
test -n "${workload_classpath}"
workload_main="it.cavallium.rockserver.core.impl.benchmark.SevenProfileWorkloadBenchmark"
selector_main="it.cavallium.rockserver.core.impl.benchmark.WorkloadBenchmarkSelection"
```

The Java command for every step is:

```bash
java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
  -cp "${workload_classpath}" "${workload_main}" ...
```

## Paired v1.3.11 Pareto gate

Candidate selection and code-regression acceptance are separate decisions. After the
scheduler candidate/configuration is fixed, `SevenProfilePairedBenchmark` compares that
same shape at immutable v1.3.11
`bb4f1a7e90db1fdfd785936594d080e8c4a0ba4e` and the final candidate. It writes the
ten-pair schedule before measurement; odd pairs run baseline first and even pairs run
candidate first. Every listed measurement must be a fresh JVM, and no other benchmark,
build, validator, IDE indexer, or workload may overlap it.

Prepare the one-shot schedule:

```bash
paired_main="it.cavallium.rockserver.core.impl.benchmark.SevenProfilePairedBenchmark"
candidate_sha="REPLACE_WITH_FINAL_CANDIDATE_SHA"
paired_root="/mnt/rockserver-hdd/seven-profile-paired-${candidate_sha}"

java -cp "${workload_classpath}" "${paired_main}" \
  --mode=prepare "--root=${paired_root}" "--candidate-sha=${candidate_sha}" \
  --host-state=dedicated --storage-label=hdd-btrfs --cache-state=cold --enforce=true
```

Follow `schedule.tsv` exactly. For each row, prepare a deterministic fresh dataset and
run `SevenProfileWorkloadBenchmark` once with the fixed options from this document and
the row's build classpath/SHA. Place its generated `pareto-worker.properties` at the
scheduled relative path. Cache eviction and hardware-state verification remain explicit
operator steps; the Java harness does not claim to perform them.

After all 20 artifacts exist, run the same command with `--mode=evaluate`. The strict
parser rejects missing/extra/duplicate fields, build or configuration drift, failed
workload checks, leaks, nonzero final resources, or submission-attempt/outcome mismatch.
It gates every profile's throughput and queue/execution/end-to-end p99 plus global
CPU/allocation/heap/direct/RSS metrics. GC, thread/native-handle counts, retained
snapshots, per-profile queue/active peaks, and aggregate parked/outstanding peaks allow
no increase. The candidate artifact must expose exact Wave 1 scheduler accounting; the
immutable baseline is explicitly marked as using the legacy accepted/outcome fallback.
Enforced workers verify the production classes against their clean Git checkout and
record stable full-classpath, production-bytecode, RocksDB artifact, JDK, and hardware
fingerprints. An enforced paired evaluation rejects any artifact not produced with the
worker's own `--enforce=true` validation. The evaluator also rejects reused process IDs or any overlap/reordering in
the predeclared 20-process schedule.
Automatic acceptance requires every point estimate to be no worse, no interval to
demonstrate regression, and at least one predeclared primary metric to meet the 2%
material-improvement confidence bound. Reported exception candidates remain failures
until the user explicitly approves the documented ablation/profiling case.

## Generate candidates

Candidate values are the read and write data-pool capacities. Candidate generation
is powers-of-two only and is machine-readable:

```bash
java -cp "${workload_classpath}" "${workload_main}" \
  --print-candidates=true --candidate-min=4 --candidate-max=64 \
  > workload-candidates.json
```

The smallest candidate is four because production pools reserve borrowable slots
for LATENCY, INGEST, and CDC and require at least three workers. Use the same
candidate list, seed, workload rates, dataset dimensions, JVM, and Rockserver SHA
throughout one comparison.

## Reusable cold-cache datasets

Use a separate root for every candidate and for every candidate's isolated INGEST
baseline. A root is prepared once and reopened once. The preparation process closes
the database before asking the operator to evict the page cache. The harness never
drops host caches itself. Opening a prepared root atomically creates
`run-attempt.properties`; even an interrupted or failed attempt consumes that root,
because its database may already have changed.

The following options are the reference comparison shape; do not silently change
them between candidates:

Use `hdd-zfs` or `hdd-btrfs` according to the verified rotational mount. The
reference below uses `hdd-btrfs`; never apply a storage label that does not match
the measured filesystem.

```bash
rockserver_rc_sha="REPLACE_WITH_EXACT_ROCKSERVER_RC_SHA"
common_options=(
  "--build-id=${rockserver_rc_sha}"
  "--storage-label=hdd-btrfs"
  "--seed=5931033225068892758"
  "--preload-keys=1000000"
  "--preload-flush-keys=50000"
  "--value-bytes=256"
  "--range-width=4096"
  "--write-key-space=65536"
  "--warmup-seconds=15"
  "--measure-seconds=60"
  "--pressure-seconds=5"
  "--cdc-lag-limit=100000"
  "--control-workers=1"
  "--latency-workers=8"
  "--analytical-workers=1"
  "--ingest-workers=4"
  "--cdc-workers=1"
  "--batch-workers=2"
  "--physical-workers=1"
  "--cancellation-workers=1"
  "--control-rate=50"
  "--latency-rate=0"
  "--analytical-rate=10"
  "--ingest-rate=1000"
  "--cdc-rate=50"
  "--batch-rate=100"
  "--physical-rate=1"
  "--cancellation-rate=25"
  "--direct-io=false"
  "--spinning=false"
)
```

A rate of zero means unpaced, not disabled. Every producer remains present because
each worker count must be positive.

For candidate 8, first prepare and close the isolated-baseline root:

```bash
baseline_root="/mnt/rockserver-hdd/workload-candidate-8-ingest-baseline"

java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
  -cp "${workload_classpath}" "${workload_main}" \
  "--root=${baseline_root}" --candidate=8 "${common_options[@]}" \
  --prepare-only=true
```

Verify the mount really has the intended HDD/Btrfs topology, then evict caches only
after the Java process is closed:

```bash
findmnt -T "${baseline_root%/*}"
lsblk -d -o NAME,ROTA,SIZE,MODEL
sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
```

Reopen it exactly once for the isolated baseline:

```bash
java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
  -cp "${workload_classpath}" "${workload_main}" \
  "--root=${baseline_root}" --candidate=8 "${common_options[@]}" \
  --reuse-prepared=true --cache-state=cold --ingest-baseline-only=true
```

Keep the generated `ingest-baseline.properties`. Then repeat the prepare, hardware
verification, and cache eviction workflow with a fresh mixed root:

```bash
candidate_root="/mnt/rockserver-hdd/workload-candidate-8-mixed"

java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
  -cp "${workload_classpath}" "${workload_main}" \
  "--root=${candidate_root}" --candidate=8 "${common_options[@]}" \
  --prepare-only=true

sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'

java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
  -cp "${workload_classpath}" "${workload_main}" \
  "--root=${candidate_root}" --candidate=8 "${common_options[@]}" \
  --reuse-prepared=true --cache-state=cold --enforce=true \
  "--ingest-isolated-baseline-file=${baseline_root}/ingest-baseline.properties"
```

The mixed runner validates the baseline schema, exact throughput, zero baseline
leaks, candidate, build ID, storage/cache labels, and dataset/comparison fingerprints
before starting. Repeat this exact two-root procedure for every power-of-two candidate.
Hardware baseline and mixed runs require `--build-id` to be the full lowercase
40-character Rockserver release-candidate Git SHA.
Do not reuse a mutated candidate root, mix warm and cold results, merge different
dataset or comparison fingerprints, or merge results from different storage labels.

## Select on the HDD reference filesystem

Pass every provenance-matched HDD `selection-input.properties` file to the pure selector:

```bash
java -cp "${workload_classpath}" "${selector_main}" \
  --output=/mnt/rockserver-hdd/workload-selection.json \
  /mnt/rockserver-hdd/workload-candidate-*/selection-input.properties
```

Selection is mechanical:

1. Reject inputs that were not enforced cold-cache hardware runs.
2. Find maximum aggregate throughput across the seven profiles.
3. Retain candidates within 5% of that maximum.
4. Retain candidates whose worst relevant p99 is within 10% of the minimum such p99.
5. Reject any candidate with a per-profile SLO failure or any leak.
6. Reject any candidate whose remaining pressure, metric-presence, error, or shutdown
   checks failed.
7. Choose the smallest remaining candidate.

The selector also rejects gaps in the powers-of-two sequence so a missing candidate
cannot be mislabeled as an adjacent setting.

`workload-selection.json` includes the winner, every gate decision, and the lower
and upper adjacent candidates available in the measured set. No candidate is
selected if all of them fail.

## Verify winner and adjacent settings on NVMe

Prepare fresh NVMe baseline and mixed roots for the HDD winner and every value
in `adjacent_verification_candidates`. Change only `--storage-label=nvme` and the
root mount; retain the exact SHA, seed, JVM, dimensions, rates, and cache workflow.

Each NVMe mixed run must pass its own per-profile SLOs and leak/drain gates. Keep all
three machine-readable results with the release evidence. The adjacent runs verify
the selection boundary; they do not automatically retune defaults or override the
HDD winner. Any proposed winner change is a new tuning decision and requires a
fresh complete comparison.

## Acceptance boundary

The hardware evidence is incomplete until all candidates on the chosen HDD reference
filesystem, the selected winner, and its available NVMe neighbors finish with comparable cold-cache inputs.
Do not claim acceptance from a smoke run, tmpfs, CI, a single candidate, an
incomplete round, a warm cache, or an operator-supplied label that was not checked
against the real mount.

After hardware selection, N still runs the consolidated repository gates and the
one-hour Rockserver/Yotsuba canary. The canary—not this harness—provides the final
zero chat deadline, duplicate-message, and end-to-end rollout evidence.
