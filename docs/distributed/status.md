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

The initial process test verified file-backed quorum writes, cross-column native atomic
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

## Portable native recovery checkpoint

Native image/import implementation is committed at `878bedc`, with persistent
test storage at `63b5299`. A streamed versioned image includes the exact native
cut's data, receipts, configuration and completed marker; ordering, ownership,
row/image limits and embedded/whole-file SHA-256 hashes are verified. It is
published by an atomic no-overwrite hard link after file/directory sync.

Installation acquires an inactive interval lease and persists `INSTALLING`
before cleanup. User and group metadata are restored with bounded WAL-enabled
batches, followed by a WAL sync and one final marker/recovery-state batch.
Interrupted, corrupt or changing images leave the range fenced. Reopen and retry
are tested, including untouched neighboring range data/receipts and original
result retention. Status reads reject transitions rather than expose partial
receipt state. Physical ownership bounds now reject cross-range access.

Regular RocksDB and optimistic borrowed-base modes pass range-tombstone,
cross-CF batch, snapshot and reopen tests. The optimistic wrapper itself rejects
range-tombstone batches; the distributed store retains its original owner while
using the borrowed base. Pessimistic TransactionDB is rejected before distribution
because the pinned Java API lacks the required raw optimized range-write route.
Existing standalone modes are unchanged.

Storage verification corrected an earlier assumption: host `/tmp` is **tmpfs**,
so earlier default JUnit temp fixtures prove file-backed process/reopen behavior,
not physical disk durability. Native durability fixtures now use the worktree's
`target/native-disk-tests` on XFS `/dev/nvme1n1p1`. All 41 core tests pass there;
the four installer cases also pass after status-transition fencing was tightened.
Filesystem sync tests remain distinct from hardware power-loss simulation.

The initial Ratis snapshot integration passed native-pool-loss restoration and
retained-tail replay in three JVMs. Its latest lifecycle/remote-publication changes
are still uncommitted and unverified: rerun process/lifecycle gates on XFS before
claiming their disk-backed acceptance. Released Ratis publishes received snapshot
directories without fsync and acknowledges before native reload. Staging recovery
and a gRPC final-reply durability barrier through the supported service customizer
are under implementation. Failed pause/shutdown must settle deferred callbacks
before Ratis joins its updater. Real remote installation and local child seed
proof remain next gates.

This initial image format stores canonical streamed logical rows. Canonical SST
export/import and representative migration/backup throughput remain required;
the row-based importer is not evidence of efficient large-range movement.

## Historical Ratis integration failures

An intermediate XFS-focused optional-module run passed the real empty LISTENER
snapshot-transfer/tail/promotion case, two controlled lifecycle cases and seven
reply-barrier tests. It failed the existing leader-loss retry case and one
snapshot-restore assertion. The full gate is therefore **not passing** and the
Ratis edits remain uncommitted pending diagnosis and regression validation.

The restore assertion used `SnapshotManagementApi.create(long,long)` incorrectly:
its first argument is `creationGap`, not a minimum required applied index.
Peer apply progress must be verified before requesting `create(true,timeout)`.
Failed process logs are retained on XFS; a test-only SLF4J provider was added to
make actual Ratis failures observable. Fixture startup's `purgeGap=0` was also
invalid in released Ratis (minimum is one), and is corrected without increasing
timeouts. Diagnostics and passing whole-process results are required before the
snapshot adapter is committed.

## Current verified checkpoint

The previous failures are resolved. Ratis integration is committed at `24a6863`,
conditional commands at `3c7a1e3`, and the actual loader-backed shared pool owner
at `8c0842b`. The combined XFS gate passes 68 core/loader tests and 12 optional
Ratis tests: three independent-JVM process cases, two controlled lifecycle cases
and seven transport-barrier boundary cases. Root selected tests repeat in the
fuzz execution; repeated executions are not extra independent cases.

Initial-voter readiness proved decisive: Ratis ignores votes from an empty-log
voter once the candidate has committed entries. Fixtures now prove initial
configuration apply on every initial voter before user writes; individual writes
still return on quorum, with no all-voter apply wait. The production node/catalogue
must enforce this initial activation gate; the fixture is not that runtime authority.

Received images are verified and synced in staging, retained through stock Ratis's
directory replacement, and synced again through the released gRPC customization
hook before final SUCCESS. Complete orphan images recover after restart. Native
reload/serving remains separate from transport acknowledgement. Recovery-image
pruning and reference accounting remain required before bounded-retention acceptance.

New command v2 carries bounded equality/absence predicates. Native apply evaluates
them in committed order and saves either success or a conflict receipt without
partial user effects. Delayed retries retain their original outcome even when
conditions later change. Decoded v1 commands re-encode identical bytes/hashes.
This is autocommit condition checking; snapshot transactions/ABA/history and 2PC
remain unimplemented. Feature-epoch activation for v2 is still runtime work.

`DistributedStoragePool` retains the whole LoadedDb resource graph, creates shared
CF bindings, and reopens its explicitly reserved internal CFs with compatibility
options. Standalone loader calls retain their original reserved-name behavior.
Range admission closes before native draining; a held pin prevents pool closure
and keeps all native options/caches/WBM/handles live for retry. Actual process
fixtures now open through this pool/ConfigParser/RocksDBLoader path rather than
handmade DBOptions pools. HOCON fixture substitutions are resolved within one
input source containing defaults; separate config sources do not resolve those
substitutions against each other.

Passing combined command:

```
mvn -f distributed/pom.xml -pl :rockserver-distributed-ratis -am \
  -Dtest=DistributedCommandCodecTest,DistributedMergeOperatorRegistryTest,ReplicatedRangeStoreTest,RangeOwnershipTest,NativeRangeImageTest,NativeRangeInstallTest,ReplicatedPoolModeTest,DistributedStoragePoolTest,RocksDBLoaderComplexConfigTest,RatisDurableProcessesTest,RatisSnapshotLifecycleTest,RatisSnapshotAckInterceptorTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Whole distribution is still incomplete: catalogue/bootstrap authority and real
client/API routing, durable session GC/fences, scoped snapshots/transactions and
disjoint proposal reservations, child seed proof, dynamic topology and migration,
SST/bulk/export/CDC/backup workflows, transport parity, deterministic/fault models,
rolling feature negotiation, bounded resource/performance and power-loss gates.
