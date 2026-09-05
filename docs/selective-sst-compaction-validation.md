# Initial selective SST compaction validation

This records the implementation-stage checks before release preparation. For the
final release checks and resolved test issues, see [Rockserver 2.1.0](releases/2.1.0.md).

Local implementation in `/home/cavallium/IdeaProjects/rockserver-selective-compact-files`,
branch `codex/selective-compact-files`, based on `6e221da8ff53f8204e0a33170b6c1d91f40371da`.
No commit, push, package publication, deployment or production compaction was performed.
The original Rockserver master checkout remains clean. No RocksDB/JNI source or artifact
was changed.

## Passing acceptance checks

```bash
mvn -q -P library \
  -Dtest=SelectiveCompactionTest,SelectiveCompactionIntegrationTest,WorkloadAdmissionTest,ManualCompactionTest,GrpcPhysicalMaintenanceIdentityTest,GrpcRetryPolicyTest,GrpcErrorMappingTest,GrpcShutdownTest \
  test
python3 -B scripts/test_compact_adjacent_ssts.py
git diff --check
```

Maven exit **0**. Surefire reports **51 tests, zero failures/errors/skips**.
The POM's two Surefire executions both select those tests when `-Dtest` is supplied;
51 is the unique test count, not twice-counted executions. Python planner: **4 tests,
zero failures**, exit 0. Script `--help` also passed. Source hashes and per-suite
counts are in [selective-sst-compaction-test-results.json](selective-sst-compaction-test-results.json).

New integration coverage includes L5 and L6 same-level compaction over gRPC, one
storage path and two paths with explicit output placement on path 1, validation-only
behavior, stale/disappeared inputs, native snapshots plus newer updates/deletions,
reopening the resulting DB, default disk and in-memory paths, and cancellation while
column deletion waits for the in-flight operation's lease. The planner tests check
intervening large/busy files, bounds, key ordering, unsplittable shared boundaries,
and excluding one-file groups.

The updated exhaustive command-admission matrix covers both new command types.
Existing gRPC retry, error, maintenance identity and shutdown checks pass in the
final selected run. The sample script's actual grpcurl subprocess was not exercised
against a running endpoint because grpcurl is not installed here; gRPC interoperability
was tested through the Java client/server integration tests. The script's selector
and argument handling were exercised locally. No production performance claim follows
from these tests.

## Wider-run limitations

An initial `mvn -q -P library test` reached 2,982 reported tests with one failure:
the command-admission test's explicit subtype inventory had not yet been updated.
That inventory was corrected and the final focused command-admission tests passed.
The wider run was terminated (exit 143) while executing the unchanged, long-running
`WorkloadPressureControllerConcurrentExhaustiveTest` state-space exploration. The
entire test suite, including all exhaustive models/fuzz cases, is **not certified green**.

A subsequent expanded focused selection ran 55 tests and encountered one failure in
the unchanged `ColumnHandleLifecycleTest.deleteWaitsForReactiveRangeCursorLease`
during the second Surefire execution: deletion had already completed when its
buffered range-delivery assertion expected a retained native cursor. That test had
passed in earlier runs. A clean baseline checkout at the exact base commit passed
its full four-test class and three additional targeted classpath executions of the
range test. Therefore the one-off failure was **not reproduced on baseline**; it is
recorded as an unresolved intermittent wider-test observation, not asserted to be
proven pre-existing or silently counted as a pass. No range-cursor implementation
was changed. The new selective-compaction ownership test passes deterministically
using a barrier inside the retained column lease.

Logs remain local at `/tmp/rockserver-selective-full-test.log`,
`/tmp/rockserver-selective-final-focused.log`, `/tmp/rockserver-selective-acceptance.log`,
and `/tmp/rockserver-compact-baseline-test.log` plus the three baseline probe logs.
