# Distributed implementation evidence

Task: `rockserver_distributed_20261007_01a116c1`; implementation chat:
`01a116fa-5a6c-7833-85de-e5ad8a4dd28e`.

## Source and scope

- Worktree: `/home/cavallium/.codex/worktrees/b7a7/rockserver`.
- Branch: `codex/distributed-rockserver`.
- Design/implementation base: `c431f88884a6735ff25f4538377d811fb2ea7832`.
- Canonical architecture SHA-256:
  `25a11514aa77aede6e0f7936acc8e3c9e6830906c5103f1559c242f54839ddd2`.
- Consumer evidence SHA-256:
  `799594fc42fc61e5aabf4302d7e262f8ee0f12c08a98a44edee89ba63d20d7b9`.

The input hashes were verified before copying. The checkout started clean at
the supplied base; cached `origin/master` and the main checkout also identify
that base. No main-checkout changes were imported. Final integration belongs
to the user. Production changes and artifact publication are outside scope.

## Acceptance state

Setup is complete. Distributed runtime acceptance is **not established**.
Implementation follows the six milestones in `architecture.md`, section 12.
The first gate is real three-process quorum persistence, ordered atomic
cross-CF apply, linearizable reads, original-result retries, leader loss and
snapshot recovery. Local child seeding must be proven against released Ratis
before online split/merge depends on it.

IntelliJ's index does not cover this worktree (exact-root lookup returned
`project_not_found`). Source inspection uses this checkout directly.

Representative hardware, equal-durability performance, storage power-loss
simulation, dynamic topology, transactions, export/CDC and backup restoration
remain required gates. Local smoke tests cannot establish independent host
failure domains or production acceptance.

## Verified initial slice

JDK 25, pinned JNI 11.8.1.4, Apache Ratis 3.3.1. The optional classpath adapter
is built by `mvn -f distributed/pom.xml -pl :rockserver-distributed-ratis -am`;
the standalone core's dependency graph and modular build remain separate.
Ratis server/server-api have overlapping automatic-module packages, so they
cannot be added directly to the current JPMS module graph. Its protobuf,
gRPC and Netty are relocated in thirdparty-misc 1.1.0; the resolved core
versions remain protobuf 3.25.8, gRPC 1.80.0, Netty 4.2.12.Final and custom JNI
11.8.1.4, without a second RocksDB artifact.

Passing command (2026-10-07):

```
mvn -f distributed/pom.xml -pl :rockserver-distributed-ratis -am \
  -Dtest=DistributedCommandCodecTest,ReplicatedRangeStoreTest,DistributedMergeOperatorRegistryTest,RangeOwnershipTest,RatisDurableProcessesTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Twenty-four core tests and one three-process test pass. The root POM runs selected
tests again in its fuzz execution; those are repeated checks, not additional
independent tests. Native io_uring initialization emits an ENOMEM warning and
falls back; this is not storage power-loss evidence.

The process test verifies disk-backed quorum writes, cross-column native atomic
apply, noncommutative merge order, saved previous/merged results, dropped
application-result handling, leader kill, original-identity retry, node restart
and a new write with a second original voter stopped. All processes share a
host; independent host/disk failure domains and power-loss recovery remain
unverified. The reply is discarded at the application boundary, not dropped by
a network proxy. The test uses a qualified built-in append operator and a
fixture pool; normal loader/config integration remains required.

Native tests also verify same-batch result evaluation, response-budget rejection
without partial writes, one native snapshot cut for data/receipts/marker, WAL
enabled despite `rocksdb.migrate`, and deterministic uploaded-operator identity
across instances. Explicit operator qualification does not prove arbitrary Java
pure or sandboxed. Distributed callbacks require further cross-JVM/property
and compaction-schedule testing.

Independent review found and corrected pre-append MERGE admission on plain
columns. The native store now requires an immutable set of admitted deterministic
merge columns; a trusted pool/schema owner must populate it from actual qualified
operators. The process gate rejects plain-column MERGE and malformed/payload-
mismatched commands, then verifies continued valid writes/reads. It does not
prove arbitrary custom operand validation.

Snapshot pin release now waits for its native readers/iterators to return even
when close/cancellation arrives during export. A controlled reader barrier
verifies that another apply can proceed while the original cut stays retained;
the pool must remain open until `activeSnapshotPins()` returns to zero.
`RangeOwnership` additionally enforces unsigned intervals, stale identity checks,
one live local owner and exclusive inactive install/cleanup leases. Its six tests
cover metadata-only local split/merge and overlap races. This guard is not durable
ownership state and does not establish distributed split/merge behavior.

The current adapter applies synchronously. It explicitly accounts for Ratis
configuration/metadata entries before publishing completed progress. Log data
is disk-backed, unsafe/async flush and leader-lease reads are disabled, reads
are LINEARIZABLE/COMMIT_INDEX. No durable snapshot is advertised and automatic
snapshot triggering is disabled. Retaining logs is an initial correctness
boundary, not the final bounded-retention implementation.

Still missing: installable snapshots/seed initialization, range-aware result
overlay, durable session receipt GC/fences, conditional predicates and scoped
transactions, reservations/disjoint pipelining, production node/client/API
integration, catalogue/topology, long workflows, transport parity and the final
resource/performance/fault gates. A command combining range deletion with point
return modes is currently rejected before append. The task remains active.

Next acceptance sequence: implement streamed checksummed images and fenced,
recoverable native installation; prove fresh-node recovery and local child seed
initialization against Ratis; then measure idle 1/8/32-group cohorts on one shared
pool per node. Density fixtures must cap per-division and gRPC executors, use
fresh directories for group-add FORMAT, collect RSS/thread/FD samples after
warmup, and report local results without extrapolating to production NUMA or
quorum-durable throughput. This density measurement has not been run.
