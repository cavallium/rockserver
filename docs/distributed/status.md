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

## Catalogue and routing checkpoint

The next combined XFS gate passes 76 core/loader tests and 23 optional Ratis
cases. The eight new core cases validate bounded canonical directory images,
unsigned full-domain coverage, fixed-prefix colocation, immutable identities,
forward/reverse intersections and malformed inputs. Optional additions cover
catalogue validation (three), routed-client contracts (seven), and one real
three-JVM catalogue/two-data-group workflow. Core cases repeat in the root fuzz
execution; repetitions are not independent cases. The passing log is
`/tmp/rockserver-catalogue-full-gate.log` (exit zero, 2026-10-07).

The catalogue stores a domain directory in a dedicated native pool and Raft group.
A pure validator runs at admission and replica apply: one conditional canonical
replacement, matching cluster/database/domain, initial generation zero or exactly
one generation advance, unchanged schema/columns/projection. Native ordered
predicates arbitrate competing proposals; both success and conflict receipts
retain their original outcome. The catalogue CAS limit is 1 MiB minus 256 bytes
so both expected and replacement images fit one bounded command. This initial
whole-image model is not final large-catalogue paging or schema migration.

The routed client caches an immutable directory and supports points and atomic
same-range mutations/predicates. Cross-group operations fail before submission.
A prepared command preserves its operation ID, complete payload, group/epoch and
original descriptor through directory refresh and explicit retry. No ambiguous
mutation is automatically rerouted. Actual calls use bounded read-only leader
probes and one user submission; stopped-peer gRPC status exceptions are normalized
to IOException so discovery can inspect surviving voters. Calls are bounded to
32 with no waiting queue; per-operation clients/probes still require performance
work and are not the final efficient connection model.

The three-JVM fixture uses an independent catalogue pool and two disjoint data
ranges sharing one loader-backed user CF/native pool per JVM. It checks competing
CAS proposals and original outcomes, direct malformed-write rejection, both data
routes, actual catalogue read unavailability after two local catalogue groups
are removed, cached data writes, a data leader kill, original-command retry and
continued writes/reads on the surviving quorum. Initial configuration apply is
proven on every voter before directory publication. This fixture does not yet
implement catalogue-authorized production activation, dynamic topology, durable
node/incarnation authorization, genuinely lost CAS network replies or host-level
failure domains.

A separate source review identified a **candidate partition stale-read defect**
in pinned Ratis 3.3.1: ReadIndexHeartbeats reuses an acked commit index without a
fresh heartbeat round, while old-leader expiry runs separately. Immediate read
success after local follower-group removal is not itself stale-read evidence;
those removals leave no replacement writable quorum. The decisive pending gate
keeps the old leader's client endpoint reachable, isolates its Raft traffic,
acknowledges a newer write through the other voters, then reads the old leader
with a fresh client. The read must fail or see that write. Existing process/read
successes and eventual leader expiry do not establish this partition invariant.
No executed stale-read result or remediation is claimed at this checkpoint.

Pinned source anchors for that candidate:
[ReadIndexHeartbeats](https://raw.githubusercontent.com/apache/ratis/ratis-3.3.1/ratis-server/src/main/java/org/apache/ratis/server/impl/ReadIndexHeartbeats.java),
[LeaderStateImpl](https://raw.githubusercontent.com/apache/ratis/ratis-3.3.1/ratis-server/src/main/java/org/apache/ratis/server/impl/LeaderStateImpl.java),
[RaftServerImpl](https://raw.githubusercontent.com/apache/ratis/ratis-3.3.1/ratis-server/src/main/java/org/apache/ratis/server/impl/RaftServerImpl.java).

Passing combined command:

```
mvn -f distributed/pom.xml -pl :rockserver-distributed-ratis -am \
  -Dtest=DistributedCommandCodecTest,DistributedMergeOperatorRegistryTest,ReplicatedRangeStoreTest,RangeOwnershipTest,NativeRangeImageTest,NativeRangeInstallTest,ReplicatedPoolModeTest,DistributedStoragePoolTest,RocksDBLoaderComplexConfigTest,RangeDirectoryCodecTest,RatisDurableProcessesTest,RatisSnapshotLifecycleTest,RatisSnapshotAckInterceptorTest,RatisCatalogueValidationTest,RatisCatalogueProcessesTest,RatisRoutedClientTest \
  -Dsurefire.failIfNoSpecifiedTests=false test
```

Surefire's native-stream warning is the same io_uring ENOMEM fallback recorded in
its dumpstream, not a failed assertion. Strict read-safety/fault acceptance and
all remaining architecture milestones remain open. No merge, push, publication,
deployment or live acceptance occurred.

## Partition read failure reproduced

`RatisReadPartitionTest` reproduced the preceding candidate against stock Ratis
3.3.1 on its first executed run. Three independent JVMs used the XFS test factory.
A test-only gRPC interceptor isolated Raft server traffic to/from the old leader;
client/admin RPC remained reachable. Every initial voter applied configuration
before writes. The fixture configured election timing to 100 ms–3 s; this is not
a production timeout change.

Evidence from `/tmp/rockserver-read-partition-gate.log` (Maven exit one):

- 3005 ms: old leader n2 had a warmed stable committed cut at index 2.
- 3006 ms: Raft traffic to/from n2 was isolated.
- 5584 ms: the other quorum acknowledged the replacement value through n0 at index 5.
- 5588 ms: n2 still reported LEADER, native applied index 2.
- 5597 ms: a fresh no-retry client targeted n2 directly and received SUCCESS with
  the old value; n2 still reported LEADER afterward.

The failed assertion compared the acknowledged new value (26 bytes) with the
returned old value (16 bytes). The preserved fixture is
`distributed/ratis/target/ratis-disk-tests/ratis-10169440901303389205`, including
`partition-evidence.txt` and each child process log. This is an executed strict
linearizability violation, not eventual-unavailability or a missing timing window.
The earlier 76-core/23-Ratis passing gate does not establish this invariant.

Remediation is in progress: replace cached/index-keyed read acknowledgements with
bounded fresh quorum rounds, safely batch requests before each round starts,
preserve current/joint membership and completed native apply, and retain one
patched server artifact with the reviewed 3.3.1 input checksum. No remediation
or patched-runtime acceptance is claimed yet. Stock behavior must fail this
regression; the patched artifact must pass it and the existing durability,
snapshot, catalogue and routing gates before it is retained as a fix.

The user subsequently excluded fuzzing and security-oriented testing. The
traffic-isolation test was retired from the automated source/test output trees;
its source and already-produced evidence remain in the preserved failure fixture.
It will not be rerun. Subsequent validation uses ordinary deterministic database
unit and integration checks, with Maven `test-compile surefire:test@default-test`
to bypass the root's separate `fuzz-regressions` execution. The previously executed
failure remains evidence; no post-fix traffic-isolation acceptance is claimed.

## Transaction scope foundation

The active reactor/dependency wiring remains at the last validated 3.3.1 server;
the unfinished `distributed/ratis-server` draft is preserved outside that reactor.
Its workers were stopped following the user's testing restriction. It is not a
compiled or accepted replacement, and the documented read-consistency defect
remains unresolved. The opt-in density fixture is also an unexecuted draft.

`TransactionScope` now supplies an immutable bounded physical declaration of
point keys, half-open ranges and prefixes. Prefix upper bounds use unsigned
carry with correct truncation/infinity handling. Point declarations do not grant
scan access; range coverage can combine adjacent/overlapping declarations but
rejects gaps and other columns. The full participant plan validates routing and
its budget before any native snapshot allocation. Bounds are 256 spans and
2 MiB of declared key/bound bytes.

This is a model/planning foundation, not an executed transaction or coordinated
snapshot. Native pin acquisition, term binding, mutation history/ABA validation,
accepted commit records and cross-group decisions remain required.

Ordinary unit validation uses only the default Surefire execution:

```
mvn test-compile surefire:test@default-test \
  -Dtest=TransactionScopeTest,RangeDirectoryCodecTest \
  -Dsurefire.failIfNoSpecifiedTests=false
```

No fuzz-regressions execution, security-oriented tests, retired traffic-isolation
tests or additional network probes are part of this gate.

This gate passed five scope cases and eight directory cases (13 total, zero
failures/errors/skips, Maven exit zero); log `/tmp/rockserver-transaction-scope-gate.log`.

## Scoped native reader

`ReplicatedRangeStore.capture(TransactionScope)` now validates every declared
column/key/range against its native owner before allocating a snapshot pin.
The returned `ScopedSnapshot` exposes scoped point reads and scans, the local
cut/identity, and close. It does not expose the unrestricted pin. Reads copy
caller bounds before validation; scans require complete declared range coverage.
Null scan bounds request the owned interval, not automatic clipping to a smaller
scope. Captured readers retain their historical cut after a local ownership fence
and preserve the underlying native reader/drain ownership.

Ordinary native validation passes 21 cases: scope five, native range twelve,
loader-backed pool four. New cases verify one cross-CF historical cut, rejection
before pin allocation, scoped scan coverage, close/reopen and pin release.
The affected optional adapter compiles and its ten pure client/catalogue cases
pass. Both commands use `test-compile surefire:test@default-test`; no fuzz
execution, security-oriented testing, network probes or process fixtures ran.
Logs: `/tmp/rockserver-scoped-native-gate.log` and
`/tmp/rockserver-scoped-adapter-compile.log`, both exit zero.

This is a local native snapshot facility. The caller still must establish the
read barrier; distributed scoped snapshot coordination, writable transaction
history/validation/reservations and the documented read-consistency fix remain
unfinished. The native-stream warning is the io_uring ENOMEM fallback, with no
failed test assertion.

## Bounded physical pages

Scoped native snapshots now provide forward/reverse physical pages with exclusive
full-key continuation, native iteration bounds, item/byte limits and immutable
rows/continuation. Their initial ceilings match existing `RangeBudget` defaults:
4096 items and 8 MiB. Payload accounting includes row lengths and continuation
framing, while transport identity metadata belongs to the eventual protocol.
Native row lengths are checked before Java key/value allocation; an oversized
first row produces an explicit minimum-byte error, never an empty continuing page.
No latest-state snapshot is reopened between pages.

The final ordinary native gate passes 23 cases (14 native range, five scope, four
loader-backed pool); log `/tmp/rockserver-physical-pages-native-final.log`, exit zero.
New cases verify forward/reverse continuation, exclusive upper bounds, absent
continuation keys, retained historical values after later writes, immutable page
results, exact byte limits, empty physical keys/values and exhaustion. The affected
adapter compiled and ten pure client/catalogue cases passed in
`/tmp/rockserver-physical-pages-full-gate.log`; its unchanged paths remain supported
by that gate. Both use the default Surefire execution, with no fuzzing, process
fixtures or network probes.

This is physical native pagination. Full logical bucket-member continuation,
opaque cursor identity/lease handling, cross-range page coordination, frontend
API integration and distributed read-barrier acceptance remain incomplete.

## Bound local page cursor

A scoped snapshot can now open a local `PageCursor` with immutable copied bounds,
column and direction. The cursor advances its exclusive continuation internally;
failed byte-budget requests leave continuation unchanged. Successive requests are
serialized, and exhaustion avoids reopening an iterator. Cursor close stops its
own admission while allowing an already-started page to finish. It borrows its
parent's native pin: closing one cursor does not close sibling readers, while
closing the parent invalidates all its cursors, including exhausted ones. No
latest-cut fallback occurs.

The ordinary combined gate passes 24 core/native cases and ten pure adapter cases
(Maven exit zero), log `/tmp/rockserver-local-cursor-full-gate.log`. Added workflow
coverage includes two directions on one cut, bounds-array mutation, later writes,
close/reopen, budget-error retry and parent/cursor ownership. The command uses
`test-compile surefire:test@default-test`; no fuzzing, process fixtures or network
probes ran. This is a local borrowed cursor; remote cursor leases, logical bucket
members, cross-range coordination and API integration remain required.
