# Finite-deadline contention evidence

`FiniteDeadlineContentionBenchmark` is the immutable scheduler-only controller for the
`SchedulerHighContentionBenchmark` finite-deadline workload. Preparation fixes the workload,
the eight metrics, both production builds, the alternating pair order, and the statistical
contract before any worker starts. Execution launches one fresh JVM at a time. Evaluation rejects
missing, duplicated, reordered, overlapping, drifted, or non-conserving evidence before applying
`PairedPerformanceContractV2`.

The decision is `PASS`, `FAIL`, or `INCONCLUSIVE`. The contract uses a `0.99` minimum throughput
ratio, a `1.02` maximum cost ratio, Holm family-wise alpha `0.05` for demonstrated regressions,
and no adaptive stopping or result-selected metrics. Hard gates cover terminal conservation,
per-profile and per-family progress, cooperative transitions, queue bounds, drain, and both
pressured and unpressured work.

## Exact invocation

Build the baseline production classes, then compile the candidate and its test harness. Generate
the dependency classpath from the candidate:

```bash
cd "$BASELINE_WORKTREE"
mvn -DskipTests package

cd "$CANDIDATE_WORKTREE"
mvn -DskipTests test-compile dependency:build-classpath \
  -Dmdep.includeScope=test -Dmdep.outputFile=/tmp/rockserver-deadline-classpath.txt
DEADLINE_CP="$CANDIDATE_WORKTREE/target/test-classes:$CANDIDATE_WORKTREE/target/classes:$(tr -d '\n' </tmp/rockserver-deadline-classpath.txt)"
```

Prepare the fixed ten-pair schedule. Use full 40-character commit IDs and a concrete single-line
hardware description:

```bash
java -cp "$DEADLINE_CP" \
  it.cavallium.rockserver.core.impl.benchmark.FiniteDeadlineContentionBenchmark \
  --mode=prepare --root="$RESULT_ROOT" \
  --baseline-sha="$BASELINE_SHA" --candidate-sha="$CANDIDATE_SHA" \
  --baseline-worktree="$BASELINE_WORKTREE" --candidate-worktree="$CANDIDATE_WORKTREE" \
  --baseline-classes="$BASELINE_WORKTREE/target/classes" \
  --candidate-classes="$CANDIDATE_WORKTREE/target/classes" \
  --hardware-description="$HARDWARE_DESCRIPTION" --enforce=true --pairs=10 \
  --operations=1000000 --warmup-operations=100000 --submitters=64 \
  --read-workers=8 --write-workers=8 --analytical-limit=2 \
  --foreground-capacity=65536 --batch-capacity=65536 --work-tokens=256 \
  --cooperative-yields=4 --cooperative-parks=2 --expired-deadline-percent=5 \
  --cancellation-percent=10 --failure-percent=5 --cooperative-percent=30 \
  --alternate-storage-pressure=true --seed=104372305701837 --timeout-seconds=180

java -cp "$DEADLINE_CP" \
  it.cavallium.rockserver.core.impl.benchmark.FiniteDeadlineContentionBenchmark \
  --mode=execute --root="$RESULT_ROOT"
```

Do not edit or replace a prepared directory. Every output uses `CREATE_NEW`; start a new root for
another experiment. The earlier rejected ten-pair raw evidence remains external immutable evidence
and is not rewritten by this controller.

## Fixed precision follow-up

Only a complete canonical `INCONCLUSIVE` result can be planned. The planner uses the paired log
standard deviation of all eight metrics but sets the assumed effect to exact equality. It allocates
the global `0.10` beta budget equally, keeps every acceptance margin unchanged, and emits a single
fixed pair count:

```bash
java -cp "$DEADLINE_CP" \
  it.cavallium.rockserver.core.impl.benchmark.FiniteDeadlineContentionPrecisionPlanner \
  --root="$RESULT_ROOT"

java -cp "$DEADLINE_CP" \
  it.cavallium.rockserver.core.impl.benchmark.FiniteDeadlineContentionBenchmark \
  --mode=prepare --root="$NEXT_RESULT_ROOT" \
  --next-run-properties="$RESULT_ROOT/deadline-next-run-v2.properties"

java -cp "$DEADLINE_CP" \
  it.cavallium.rockserver.core.impl.benchmark.FiniteDeadlineContentionBenchmark \
  --mode=execute --root="$NEXT_RESULT_ROOT"
```

The consumable next-run file is bound to SHA-256 hashes of the source metadata, schedule, and
canonical result. Planning rejects a `PASS`, a `FAIL`, incomplete workers, any source tamper, or
pre-existing planner outputs.
