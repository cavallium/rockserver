# Adversarial BATCH liveness proof

`AdversarialBatchLivenessPairedBenchmark` isolates the scheduler state transition that caused
prolonged sender-index backfill zero-progress windows. It compares immutable production class trees
from a historical baseline and a candidate while using one shared, hashed test harness.

## Measured state machine

Each fresh JVM performs the same sequence:

1. Start three WRITE `LATENCY` tasks and hold each on its own barrier. A scheduler snapshot must
   prove that all WRITE workers are active.
2. Enable storage pressure with a global BATCH cap of one.
3. Start one READ BATCH and hold it after permit acquisition.
4. Queue one WRITE BATCH. A snapshot must prove that it is queued behind the fully occupied WRITE
   pool, so it cannot dispatch despite being queued.
5. Queue 96 READ BATCH tasks and prove that the READ queue is continuously runnable.
6. Release the priming READ barrier. Its completion starts a 240 ms measured interval with a scaled
   10 ms pressure-pacing interval. A condition records every useful READ completion and wakes at the
   phase deadline; sleeps are not correctness evidence.
7. Release exactly one WRITE foreground barrier. The queued WRITE BATCH must start within 250 ms,
   proving bounded fairness after the peer becomes dispatchable.
8. Cancel remaining queued work, release all barriers, disable pressure, and require both pools to
   drain with exact ownership conservation.

The historical oracle requires zero READ completions after the priming completion and a zero-progress
gap spanning at least 90% of the nondispatchable phase. The candidate must complete at least two READ
tasks, sustain at least 20 useful READ ops/s, and keep its maximum zero-progress gap at or below
100 ms. Every baseline and candidate run must independently satisfy the 250 ms WRITE fair-turn bound.
Across ten counterbalanced pairs, the upper end of the candidate/baseline maximum-gap 95% Student-t
confidence interval must be at most `0.5`.

The short interval scales time, not scheduler topology or ordering. It makes the proof suitable for
fresh-process repetition; production telemetry remains necessary to validate deployed storage and
backfill behavior at the configured production interval.

## Immutable fresh-process workflow

Build production classes from both trees. The baseline tree must be exactly the requested production
SHA. The candidate may contain later test or documentation commits, but its `pom.xml`, `src/main`,
and `src/library` trees must match the requested candidate SHA and be clean.

```bash
cd /path/to/baseline
mvn clean -Dmaven.test.skip=true package

cd /path/to/candidate-with-harness
mvn clean -DskipTests package
mvn -q dependency:build-classpath \
  -Dmdep.outputFile=/tmp/rockserver-adversarial-liveness-cp.txt \
  -DincludeScope=test
```

Construct a classpath from the candidate harness. `execute` replaces only the candidate production
classes entry for baseline workers.

```bash
workload_cp="target/test-classes:target/classes:$(tr -d '\n' \
  </tmp/rockserver-adversarial-liveness-cp.txt)"
main=it.cavallium.rockserver.core.impl.benchmark.AdversarialBatchLivenessPairedBenchmark

java -cp "$workload_cp" "$main" \
  --mode=prepare --root=/non-existing/path/liveness-proof \
  --baseline-sha=<full-baseline-production-sha> \
  --candidate-sha=<full-candidate-production-sha> \
  --baseline-worktree=/path/to/baseline \
  --candidate-worktree=/path/to/candidate-with-harness \
  --baseline-classes=/path/to/baseline/target/classes \
  --candidate-classes=/path/to/candidate-with-harness/target/classes \
  --hardware-description=<stable-single-line-host-description> \
  --enforce=true

java -cp "$workload_cp" "$main" \
  --mode=execute --root=/non-existing/path/liveness-proof
```

Preparation freezes the source identities, production directory hashes, paths, workload, bounds,
and hardware description. It writes an immutable 20-row schedule with alternating baseline/candidate
order. `execute` launches every row in a separate JVM and refuses missing predecessors, existing or
premature artifacts, wrong production bytes, source drift, or a wrong build identity.

Evaluation additionally rejects reused process IDs, overlapping workers, runtime/host/harness or
per-implementation classpath drift, altered timing configuration, malformed or incomplete artifacts,
failed topology/conservation, a baseline that does not reproduce the stall, a candidate that is not
work-conserving, and either implementation missing the bounded fair handoff. `results.json` and
`results.md` are created only after all twenty immutable worker artifacts exist.

## Evidence boundary

This benchmark proves the scheduler mechanism and its liveness/fairness transition in real scheduler
threads. It does not claim deployed Yotsuba acceptance, RocksDB/native I/O performance, production
NUMA behavior, or end-to-end sender-index throughput. Those remain separate live-observation gates.
