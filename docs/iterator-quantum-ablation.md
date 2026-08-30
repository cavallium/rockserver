# Explicit-iterator service-bound ablation

`IteratorQuantumAblationBenchmark` is a causal, same-build check for the explicit-iterator
cooperative service-bound mechanism. It answers two separate questions:

1. Does checking the live scheduler signal after each RocksDB iterator step preserve the idle
   primitive's throughput and allocation behavior?
2. Once a `LATENCY` peer queues behind a running `BATCH` iterator, does the bounded primitive
   materially reduce handoff delay while completing exactly the same scan?

It is not a whole-service, transport, storage-hardware, NUMA, or production acceptance gate.
The mixed RocksDB and fresh-process suites remain authoritative at those wider evidence levels.

## Fixed design

The idle arm uses one real RocksDB iterator, identical 8,192-entry data, seek/reset path, values,
checksum, and thread. It alternates the legacy fixed-page primitive and the bounded primitive for
forty predetermined pairs. This fixed count was planned from the variance of an earlier independent
ten-pair `INCONCLUSIVE` run, without changing either acceptance margin. Each paired arm is divided
into sixteen fixed alternating sub-blocks;
their raw elapsed time and allocation totals are combined before calculating the pair ratio. This
reduces host-drift variance without selecting or discarding samples. Thread allocation is measured
but remains diagnostic; a resource-only change is not a reason to accept worse scheduling quality.

The competitive arm uses a real three-worker `RWScheduler`. Two workers are deterministically held
by `LATENCY` tasks, the scan starts as `BATCH` on the third, and a new `LATENCY` task is queued before
the scan gate opens. Each arm validates all 4,096 8-KiB values:

- `LEGACY` performs the old indivisible 4,096-value call;
- `BOUNDED` uses the production 8-MiB/8-ms service checkpoint;
- `ONE_STEP` sets both bounds below one value and must still consume exactly one value before
  yielding. This is the unavoidable native iterator-step term, not an assertion that arbitrary
  JNI calls have a finite universal latency bound.

Checkpoint state is carried out of the primitive. A checkpoint already reached therefore ends the
dispatch even if scheduler pressure clears before the primitive returns. Cancellation retains the
pre-existing item-quantum granularity and is checked before and after a primitive call; it is not a
second per-item signal read on the latency-sensitive loop.

The reported decision is:

- `PASS` when the lower 95% confidence bound for bounded/legacy idle throughput is at least `0.98`
  and the upper bound for bounded/legacy peer handoff is at most `0.50`;
- `FAIL` only when a throughput regression beyond 2% or a failure to reach 2x handoff improvement
  is statistically demonstrated;
- `INCONCLUSIVE` otherwise.

## Reproducible run

Compile and run the exact committed checkout. The final positional argument is a non-existing
artifact path; the benchmark uses `CREATE_NEW` and refuses to overwrite evidence.

```shell
mvn -q -DskipTests test-compile \
  org.codehaus.mojo:exec-maven-plugin:3.5.0:java \
  -Dexec.classpathScope=test \
  -Dexec.mainClass=it.cavallium.rockserver.core.impl.benchmark.IteratorQuantumAblationBenchmark \
  -Dexec.args='8192 256 64 256 8 /tmp/iterator-quantum-<full-sha>.results'
```

The arguments are idle entries, idle value bytes, warmup scans, measured scans per paired arm,
competitive samples per arm, and artifact path. The result records the full Git SHA, all fixed
dimensions, paired log-ratio confidence intervals, the exact one-step comparison, and the decision.
Run on an otherwise idle host; do not reinterpret an `INCONCLUSIVE` result by selecting favorable
samples or changing the margins after measurement.
