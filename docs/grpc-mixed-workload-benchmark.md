# Whole-path gRPC mixed-workload benchmark

`GrpcOverloadBenchmark` is the opt-in whole-system regression gate for the path

```text
generated gRPC client -> Netty server -> workload admission/scheduler -> RocksDB -> gRPC response
```

It complements `SevenProfileWorkloadBenchmark`, which has broader release-tuning
and storage-pressure coverage but uses the embedded API. Together with the
deterministic scheduler tests and the
[`GrpcRawScanBenchmark`](grpc-raw-scan-benchmark.md), a passing run supplies the
correctness half of the release gate. Release-level optimality additionally requires
the strict `GrpcOverloadComparison` result against immutable v1.3.11
`bb4f1a7e90db1fdfd785936594d080e8c4a0ba4e`. Request loss, wrong priority,
avoidable worker idleness, scheduler imbalance, resource leaks, and corrupt or
missing acknowledged writes remain explicit failures in every measured subprocess.

## Mixed workload

The foreground-only and mixed phases share one preloaded, flushed database. A full
run executes five paired rounds and alternates phase order to reduce order and
thermal bias. The mixed phase keeps every scheduler profile active:

| Profile | Real gRPC operation |
|---|---|
| `CONTROL` | Open a transaction, then use the protected rollback RPC |
| `LATENCY` | Point GET and bounded first/last range reduction |
| `ANALYTICAL` | Unpaced bounded first/last range reduction |
| `INGEST` | Rate-limited point PUT |
| `CDC` | Poll a server-owned CDC subscription and commit its cursor |
| `BATCH` | Unpaced point PUT flood and cancellable PUT bursts |
| `PHYSICAL_MAINTENANCE` | Protected flush |

The default client concurrency exceeds both data-pool capacities. ANALYTICAL is
unpaced and BATCH is flooded so priority, the analytical active cap, DRR progress,
borrowable reservations, and idle-worker reclamation are exercised under actual
contention rather than inferred from a lightly loaded request stream.

## Acceptance evidence

Every measured client call is intercepted at gRPC `ClientCall.start`. Its terminal
`onClose` is guarded against duplicates and classified by status. Each phase must
finish with:

- submitted calls equal to terminal calls;
- no tracked call in flight;
- no duplicate terminal callback;
- terminal status totals equal to terminal calls.

After stress, a separate integrity window sends unique values on disjoint keys,
alternating INGEST and BATCH. Every acknowledged key is fetched over the same gRPC
channel under LATENCY and compared byte-for-byte. This distinguishes transport
completion from server-side request correctness; repeated writes to an old key cannot
hide a lost acknowledged write.

The scheduler exposes immutable per-pool counters and the exact number of workers
sleeping on its work condition. The benchmark samples those values without running
commands or looking up metrics. For READ and WRITE it requires:

- eligible queued demand and samples capable of saturating the pool;
- no more than the eight-millisecond cooperative quantum plus one sampling period
  where an eligible queued task coexists with a sleeping worker.

Instantaneous active-task counts are retained as diagnostics, not used as the
work-conservation verdict: sub-millisecond calls can finish between sampling
instants. The exact sleeping-worker counter plus elapsed avoidable-idle bound is
authoritative. Backlogged BATCH work beyond the configured foreground-contention
allowance is recorded separately as policy-limited capacity, not eligible work.

ANALYTICAL-only queue depth is excluded from eligible READ demand when checking
idleness because the configured analytical active limit is intentional. PHYSICAL
work parked by storage pressure is likewise not called avoidable idleness. The
reported utilization ratio remains diagnostic; the bounded consecutive-sample
gate is used because sub-millisecond task completion/wake transitions make a raw
active-task ratio misleading.

After every phase, every queue, active count, pending operation, transaction,
iterator, range cursor, and iterator lease must drain. For every scheduler pool,
started tasks must equal completed tasks, terminal outcomes must cover accepted
tasks, and worker failure count must remain zero.

Priority evidence comes from a private percentile-enabled Micrometer registry
attached only during each measured window. Acceptance requires:

- LATENCY queue p99 no greater than foreground LATENCY execution p99 plus the
  fixed eight-millisecond cooperative quantum;
- LATENCY queue p99 no greater than contended ANALYTICAL queue p99;
- INGEST queue p99 no greater than flooded BATCH queue p99;
- the two profile-ordering relationships in at least four of five paired rounds,
  allowing one correlated host-jitter outlier while still requiring the median
  ordering;
- successful end-to-end progress for all seven profiles.

For each paired round, the runner computes mixed/foreground-only throughput and
p99 ratios with a two-sided 95% Student-t confidence interval. These within-build
ratios are availability and diagnostic evidence only. The former `2.0` p99 and
`0.80` throughput margins are not acceptance gates and cannot hide a cross-build
regression. With one smoke round the interval is only the point estimate and is
not hardware evidence.

The existing admission-monitor thread also samples process resources every 100 ms;
it adds no scheduler lock acquisition, queue scan, registry lookup, or request-path
work. Each phase reports process CPU nanoseconds and allocated bytes per useful
successful operation, peak post-collection live heap, peak direct memory, peak RSS,
GC collections/time, peak JVM threads, and peak native handles. Strict runs fail if
the HotSpot CPU/allocation counters, Linux RSS evidence, or Unix handle counter are
unavailable.

Cancellation must be observed. No concrete non-cancellation operation may exceed
its deadline, foreground rejections must remain zero, no unexpected error or
native-handle leak is allowed, and shutdown must be clean.

## Run the hardware gate

Release acceptance is deliberately provenance-strict. `--enforce=true` requires
a clean full lowercase Git SHA, a `host-state=dedicated` operator assertion,
`hdd-zfs`, `hdd-btrfs`, or `nvme` storage, at least
8 GiB `MemAvailable` at process preflight, the full dataset and saturation profile,
five paired rounds, and a prepared dataset reopened under an operator-asserted
cold cache. Before creating or consuming the root, the runner resolves its actual
mount from `/proc/self/mountinfo`; Btrfs HDD and NVMe labels must also match Linux
block-device rotational evidence. The runner records that mount, source,
filesystem, rotational bit and model alongside `/proc/meminfo`, JVM vendor/home,
arguments, library path and maximum heap, CPU model, RocksDB native version,
dependency and benchmark-harness hashes, fingerprints, and exact options. It also
enumerates other JVMs and refuses enforced timing when a JMH or benchmark command is active;
only PID plus a SHA-256 command hash is recorded, so command-line contents are not
copied into artifacts.

Strict instrumentation reads the scheduler's exact sleeping-worker counter and
is mandatory for enforced release acceptance. `--instrumentation-mode=portable`
exists only for running the current benchmark class against an older production
classpath during baseline/candidate diagnosis; portable results cannot satisfy
the work-conservation or release gates.

Compile test sources and obtain the test runtime classpath:

```bash
mvn -q -DskipTests test-compile dependency:build-classpath \
  -Dmdep.outputFile=target/overload-benchmark.classpath

overload_classpath="target/test-classes:target/classes:$(<target/overload-benchmark.classpath)"
overload_main="it.cavallium.rockserver.core.impl.benchmark.GrpcOverloadBenchmark"
rockserver_rc_sha="$(git rev-parse HEAD)"

test "$(git status --porcelain)" = ""
test "${#rockserver_rc_sha}" = 40
```

Prepare a fresh database, close it, verify the target storage, and only then evict
the host page cache:

```bash
overload_root="/mnt/rockserver-hdd/grpc-mixed-REPLACE_ME"

java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
  -cp "${overload_classpath}" "${overload_main}" \
  "--root=${overload_root}" \
  "--build-id=${rockserver_rc_sha}" --build-state=clean \
  --storage-label=hdd-btrfs --cache-state=unknown --host-state=dedicated \
  --minimum-host-available-gib=8 --prepare-only=true --enforce=true

findmnt -T "${overload_root%/*}"
lsblk -d -o NAME,ROTA,SIZE,MODEL
sudo sh -c 'sync; echo 3 > /proc/sys/vm/drop_caches'
```

Reopen the prepared root exactly once with the identical workload and database
options:

```bash
java --enable-native-access=ALL-UNNAMED -Xms4g -Xmx4g \
  -cp "${overload_classpath}" "${overload_main}" \
  "--root=${overload_root}" \
  "--build-id=${rockserver_rc_sha}" --build-state=clean \
  --storage-label=hdd-btrfs --cache-state=cold --host-state=dedicated \
  --minimum-host-available-gib=8 --reuse-preloaded=true --enforce=true
```

`run-attempt.properties` is created atomically before RocksDB is reopened. The
root is consumed even if the JVM is interrupted, killed, or the benchmark fails;
prepare a new root instead of retrying biased cache or database state.

## Paired baseline/candidate Pareto gate

Compile the final candidate benchmark harness once, compile the baseline and
candidate production classes in their isolated worktrees, and use the same candidate
`target/test-classes` and dependency jars for both builds. Put the selected
production `target/classes` directory second on the classpath:

```bash
candidate_root=/tmp/rockserver-v131-scheduler-integration
baseline_root=/tmp/rockserver-v131-perf-baseline
deps="$(<"${candidate_root}/target/overload-benchmark.classpath")"
harness="${candidate_root}/target/test-classes"

baseline_cp="${harness}:${baseline_root}/target/classes:${deps}"
candidate_cp="${harness}:${candidate_root}/target/classes:${deps}"
```

Do not rebuild or alter the harness between measurements. Its content hash is part
of the environment fingerprint, while `--build-id` identifies the production
classes selected by the second classpath entry. The comparator requires every run
to have the same logical dataset, workload/config/cache fingerprint, JDK, native
libraries, dependency jars, harness, CPU, memory, and storage evidence.

Before the first measurement, create a manifest and predeclare one or more material
primary metrics. Use at least ten fresh-subprocess pairs and alternate which build
runs first. Every run needs a separately prepared, one-use root; execute all
preparation, cache eviction, and measurement commands serially with no benchmark,
build, validator, or workload running at the same time.

```properties
schema=rockserver-grpc-overload-comparison-manifest-v1
declared-at=2026-08-03T18:00:00Z
baseline-build=bb4f1a7e90db1fdfd785936594d080e8c4a0ba4e
candidate-build=0123456789abcdef0123456789abcdef01234567
primary-metrics=mixed.useful-throughput,mixed.cpu-nanos-per-operation
pairs=10
pair.1.order=baseline-first
pair.1.baseline=/results/pair-01-baseline/comparison-input.properties
pair.1.candidate=/results/pair-01-candidate/comparison-input.properties
pair.2.order=candidate-first
pair.2.baseline=/results/pair-02-baseline/comparison-input.properties
pair.2.candidate=/results/pair-02-candidate/comparison-input.properties
pair.3.order=baseline-first
pair.3.baseline=/results/pair-03-baseline/comparison-input.properties
pair.3.candidate=/results/pair-03-candidate/comparison-input.properties
pair.4.order=candidate-first
pair.4.baseline=/results/pair-04-baseline/comparison-input.properties
pair.4.candidate=/results/pair-04-candidate/comparison-input.properties
pair.5.order=baseline-first
pair.5.baseline=/results/pair-05-baseline/comparison-input.properties
pair.5.candidate=/results/pair-05-candidate/comparison-input.properties
pair.6.order=candidate-first
pair.6.baseline=/results/pair-06-baseline/comparison-input.properties
pair.6.candidate=/results/pair-06-candidate/comparison-input.properties
pair.7.order=baseline-first
pair.7.baseline=/results/pair-07-baseline/comparison-input.properties
pair.7.candidate=/results/pair-07-candidate/comparison-input.properties
pair.8.order=candidate-first
pair.8.baseline=/results/pair-08-baseline/comparison-input.properties
pair.8.candidate=/results/pair-08-candidate/comparison-input.properties
pair.9.order=baseline-first
pair.9.baseline=/results/pair-09-baseline/comparison-input.properties
pair.9.candidate=/results/pair-09-candidate/comparison-input.properties
pair.10.order=candidate-first
pair.10.baseline=/results/pair-10-baseline/comparison-input.properties
pair.10.candidate=/results/pair-10-candidate/comparison-input.properties
```

`comparison-input.properties` is emitted only after both phases complete in every
round. It records correctness, integrity, conservation, drain, shutdown,
cancellation and telemetry gates; full run/process intervals; exact fingerprints;
and all absolute metric inputs. The comparator checks that the manifest predates
measurement, process identities are unique, declared order matches the timestamps,
and no measured subprocesses overlap.

Run the comparator from the unchanged candidate harness:

```bash
comparison_main="it.cavallium.rockserver.core.impl.benchmark.GrpcOverloadComparison"

java -cp "${candidate_cp}" "${comparison_main}" \
  --manifest=/results/overload-comparison.properties \
  --output=/results/overload-comparison --enforce=true
```

For throughput, latency, CPU, allocation, live heap, direct memory, RSS, and the
mixed/foreground degradation ratios, the paired candidate/baseline geometric-mean
estimate must be no worse and the 95% interval must not demonstrate a regression.
GC time/count, peak threads, and peak native handles may not increase in any pair.
At least one predeclared primary metric must demonstrate a material bound:
throughput lower 95% bound at least `1.02`, or a latency/CPU/allocation/memory upper
95% bound at most `0.98`. Exception ceilings are reported as diagnostics only and
never convert a failed automatic gate into acceptance.

`comparison.json` and `comparison.md` contain absolute paired values, ratios,
confidence intervals, environment evidence, exception-ceiling diagnostics, and
every failure. Any exception still requires separate ablation, profiles, bounded
causal explanation, compensating gain, and explicit approval outside this tool.

The deterministic `GrpcBatchCancellationIntegrationTest` supplies the cancellation
proof: it occupies every WRITE worker with latch-controlled INGEST work, queues
unique BATCH puts over loopback gRPC, cancels them, observes exact server-side
terminal removal and drain, then proves that none of their keys was written.

Use `--help` for the complete defaults. Important full-run defaults are five
alternating rounds, 15 seconds of warmup and 60 seconds of measurement per phase,
40 point readers, four analytical readers, 64 BATCH writers, 20 READ workers, 36
WRITE workers, a 250 microsecond scheduler sample interval, and 1,024 unique
integrity writes plus 1,024 matching reads.

`results.json` is the machine-readable source of truth. `results.md` contains the
same provenance plus per-round operation, CPU/allocation/resource, latency,
request-conservation, scheduler, utilization, integrity, drain, and acceptance
evidence. The result schema is `rockserver-grpc-overload-v6`. CPU and allocation
totals exclude the registered admission/resource observer thread so a faster or
slower sampler cannot be attributed to useful operations; its CPU and allocation
are reported separately as absolute observer counters. GC and peak resources remain
whole-process measurements and therefore retain any indirect sampler impact. `--smoke=true
--enforce=false` shortens the dataset and run to verify structure; tmpfs, CI, one
round, warm cache, a dirty build, low-memory startup, or uncontrolled hardware
cannot be reported as release acceptance.

The dated `benchmarks/grpc-overload-2026-07-23.md` file is historical evidence for
the original two-profile runner. Its measurements are not comparable to schema v5
and must not be used as a current baseline.
