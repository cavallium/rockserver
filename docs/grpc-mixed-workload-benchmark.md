# Whole-path gRPC mixed-workload benchmark

`GrpcOverloadBenchmark` is the opt-in whole-system regression gate for the path

```text
generated gRPC client -> Netty server -> workload admission/scheduler -> RocksDB -> gRPC response
```

It complements `SevenProfileWorkloadBenchmark`, which has broader release-tuning
and storage-pressure coverage but uses the embedded API. Together with the
deterministic scheduler tests and the
[`GrpcRawScanBenchmark`](grpc-raw-scan-benchmark.md), a passing run is
the release-level optimality claim for the declared hardware, release SHA, dataset,
and workload envelope: request loss, wrong priority, latency or throughput
regression, avoidable worker idleness, scheduler imbalance, resource leaks, and
corrupt or missing acknowledged writes are all explicit failures.

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
p99 ratios. A two-sided 95% Student-t confidence interval is computed over the
paired ratios. The upper p99 bound must be at most `2.0`, and the lower throughput
bound must be at least `0.80`. With one smoke round the interval is the point
estimate and is not hardware evidence.

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
filesystem, rotational bit and model alongside `/proc/meminfo`, JVM arguments and
maximum heap, OS/CPU data, fingerprints, and exact options. It also enumerates
other JVMs and refuses enforced timing when a JMH or benchmark command is active;
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

Use `--help` for the complete defaults. Important full-run defaults are five
alternating rounds, 15 seconds of warmup and 60 seconds of measurement per phase,
40 point readers, four analytical readers, 64 BATCH writers, 20 READ workers, 36
WRITE workers, a 250 microsecond scheduler sample interval, and 1,024 unique
integrity writes plus 1,024 matching reads.

`results.json` is the machine-readable source of truth. `results.md` contains the
same provenance plus per-round operation, request-conservation, scheduler,
utilization, integrity, drain, and acceptance evidence. `--smoke=true
--enforce=false` shortens the dataset and run to verify structure; tmpfs, CI, one
round, warm cache, a dirty build, low-memory startup, or uncontrolled hardware
cannot be reported as release acceptance.

The dated `benchmarks/grpc-overload-2026-07-23.md` file is historical evidence for
the original two-profile runner. Its measurements are not comparable to schema v4
and must not be used as a current baseline.
