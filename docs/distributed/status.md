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

## Native user-column SST export

`NativeRangeSstExport` now streams materialized physical rows from one scoped
snapshot into an SST using copies of the source DB/column options. An idempotent
native-use loan retains the source pool while writer/options cleanup completes.
The exporter borrows, rather than closes, the caller's snapshot. It enforces row,
payload, file-size and duration limits, uses bounded native pages, and publishes
only a finished, forced, hashed file through no-overwrite hard-link publication
and directory forcing. Empty exports produce an explicit no-file manifest;
failed pre-publication jobs remove their temporary file. As with image publication,
a failure after the link is created must not be advertised as completed.

The manifest carries group/epoch/column/schema identity, actual completed cut,
effective bounds, row/payload/file sizes and file digest. This is a user-column
artifact, not a complete replicated snapshot, backup or range install: receipts,
configuration and topology metadata remain separate work. It is not the split
child-seed protocol and does not put a full copy beneath a topology fence.

Ordinary validation passes 28 core/native cases and ten pure adapter cases;
`/tmp/rockserver-sst-export-full-gate.log`, Maven exit zero. The new round trip
loads an old exported cut into a fresh native DB, verifies materialized merge
values and continued merge behavior, checks range exclusion and preserves the
existing output file. Empty and over-budget jobs leave no published SST or temp
file. This uses `test-compile surefire:test@default-test`, with no fuzzing, process
fixtures or network probes. The earlier registry prototype, its methods and
release-tracking diff are preserved under `target/paused-registry-draft` outside
the active source/build tree.

## Pool-owned SST export and worktree recovery

`DistributedStoragePool.exportSst` selects the exact loader-owned DB/column
options, verifies snapshot ownership by store identity, and retains the native
resource graph before releasing the schema semaphore. SST I/O holds no pool
schema lock. Pool close stops export admission and drains the retained range pins
before releasing DB/options/operators/caches. The caller still owns the cut.

The ordinary combined gate passes 29 core/native and ten pure adapter cases,
Maven exit zero: `/tmp/rockserver-pool-sst-export-recovered-gate.log`. New coverage
exports a historical cut through the pool, ingests it into a fresh database,
rejects foreign cuts and wrong columns, and verifies close/retry admission.
No fuzzing, process fixtures, density fixtures or network probes ran.

During the first gate, the app archived the worktree at 2026-10-08 01:03 Europe/Rome.
Source, class files and native fixture files disappeared mid-run; that failed
run is not behavioral acceptance. Snapshot `62fb3115d8c1aac709019d00e317dfc8cef4079b`
retained the tracked edits and both untracked drafts. The same path and branch
were restored using Git worktree registration and the snapshot diff; HEAD
remained `5d32852`. Residual failed-run reports are preserved at
`/tmp/rockserver-interrupted-gate-target-20261008`. Ignored target contents,
including the paused registry draft, were not retained by the archive snapshot;
that work remains inactive and is not reconstructed or validated here.

Full distribution remains incomplete, including the Ratis fresh-read-barrier
fix, public API/runtime/bootstrap integration, transactions/2PC, topology,
receipt GC, long workflows and representative performance acceptance.

## Bounded reusable range clients

`RatisRoutedClient` now borrows long-lived clients from a 32-slot LRU cache keyed
by group, range epoch, configuration version and complete replica descriptors.
Concurrent creation of the same route shares one result; active leases prevent
eviction. Replacement closes the idle client before opening its successor,
outside the cache monitor. All-busy capacity fails explicitly. Failed setup
releases its slot; failed retirement retains the resource and stops further
admission/growth. Shutdown drains routed calls, closes cached resources outside
the monitor, and supports retry of failed close without reopening admission.
Prepared commands still use their original descriptors and operation identity;
there is no new automatic mutation retry or ambiguous rerouting.

The focused ordinary gate passes 16 pure adapter cases (five cache, eight routed
client, three catalogue validation), Maven exit zero; log
`/tmp/rockserver-client-pool-final-gate.log`. Tests force blocked setup/retirement,
concurrent borrowers, close/retry and capacity boundaries without Raft RPCs,
sockets, server processes, density runs or excluded workflows. Core/native
behavior is unchanged and retains the preceding 29-case passing evidence.
This amortizes client setup for repeated range operations; it does not yet share
all channels/executors across groups by node identity, establish a tested group
density, or demonstrate measured throughput/resource acceptance.

## Independent review of the inactive fresh-read draft

Read-only architecture review verified the pinned server checksum and the stock
cached `ackedCommitIndex` shortcut against local Ratis 3.3.1 bytecode. The draft's
sealed batches and next-call-ID watermarks appear consistent with the pinned
gRPC allocation order, current-term checks and Ratis joint-majority predicate.
A concrete flaw remains: quorum-decided waiters leave deadline coverage before
success publication on the server executor. An accepted but stalled executor
can retain every admission indefinitely. Keep these waiters covered until
publication, allowing timeout to defeat delayed success. The 16-appender bound
also currently stops the tracker permanently when exceeded; treat an oversize
transitional configuration as recoverable admission failure or prove its limit
against supported topology. No draft source was changed or activated here.

Next: repair those boundaries, add deterministic ordinary read-round tests for
fresh ACKs, sealing, generations/joint quorum, startup/commit/apply order,
timeout/publication, cancellation and shutdown/rejection. Compile and inspect
replacement-class packaging and runtime dependency uniqueness before activation.
Strict read acceptance and excluded failure exercises remain unverified; this
review does not establish the deployed/runtime fix.


## Fresh-read replacement implemented and locally validated

The previously inactive server draft is repaired, tested and included in the
optional distributed reactor. Its checksum-pinned upstream input remains Ratis
3.3.1. The consumer now depends on one
`rockserver-ratis-server:3.3.1-rockserver.1` implementation; the provided original
server is not transitive to its runtime. Standalone does not gain Ratis dependencies.
Sealed batches require fresh call-ID acknowledgements and actual Ratis joint
majorities; the existing native completed-apply barrier remains intact.

Repaired deadlines cover decided/unpublished futures, generation invalidation,
and a later capacity error. Success also checks expiry at publication. Appender
capacity is 32 for bounded joint sets, and over-capacity failure is recoverable.
Failure publication is isolated from shared timer threads using node-bounded
virtual-thread tasks; 4,096 publication permits stay owned until synchronous
observers return. Cancellation and rejected publishers have explicit ownership
and fallback. This is an initial publication budget, not final whole-node
payload/admission/performance acceptance.

The combined ordinary gate passes 29 core/native, 16 read-round and 17 adapter
cases, Maven exit zero, `/tmp/rockserver-fresh-read-reactor-gate.log`. It uses
`test-compile surefire:test@default-test`, explicit selectors and a runtime
`dependency:tree`; the latter contains only the replacement server. Round tests
use a manual clock/executor, actual Ratis joint configuration and ReadRequests
apply barrier, and controlled virtual-thread publication. The runtime check
finds exactly one implementation of each replacement class and one patch marker.
No fuzzing, security work, traffic-isolation tests, network probes, process
fixtures, density runs or paused-registry validation ran.

Clean packaging was limited to the new server module's own target. Its output
has 162 unchanged upstream classes byte-for-byte, 23 replacement classes, no
obsolete read-listener classes, no missing method references from unchanged
classes, and no missing non-private LeaderStateImpl signatures. Upstream LICENSE
and NOTICE are unchanged. The inventory is in
`/tmp/rockserver-fresh-read-package-inventory.json`; raw verification helper is
`/tmp/rockserver-read-jar-inventory.py`. Clean packaging and forced repackaging
with a fixed output timestamp produce identical SHA-256:
`a9a903228c44ef865509a3b134d296418d8681e1d8d27fc590832c6fc7bc2e3d`.
Logs: `/tmp/rockserver-fresh-read-clean-package.log` and
`/tmp/rockserver-fresh-read-repro-package.log`. Packaging reused the passing tests
for unchanged sources; it did not rerun excluded work.

This fixes the identified cached-read-barrier implementation path locally;
strict multi-process read acceptance of the new artifact is not established.
Older stock-server process gates cannot be reused for it. Full distribution
remains incomplete: bootstrap/API integration, shared node channels/executors,
durable session receipt GC/floors, term-bound serializable transactions and
reservations, 2PC/scoped fences, child seeds/dynamic topology, complete long
workflows and representative equal-durability performance still require work.
The unexecuted density fixture remains preserved and excluded.


## Replicated receipt acknowledgements and sequence floors

Command v3 explicitly carries a contiguous `acknowledgedThrough` for the issuing
operation's own session UUID/epoch, strictly below its sequence. It supports
acknowledgement-only and data-piggyback commands. Existing constructors stay v2;
decoded v1/v2 bytes and hashes are preserved, including a frozen v2 digest check.
The v3 header consumes the same 2 MiB encoded-command budget; item limits remain
256. `SESSION_EXPIRED` is appended as result code 3 without changing prior codes.

Ordered native apply reads a group/session/epoch floor before looking up receipts
or evaluating business effects. A new advancing acknowledgement atomically writes
the floor, deletes only receipt keys from sequence zero through the floor, saves
the new issuer's receipt above that floor, and advances A in one WAL-enabled
sync=false native batch. A conflict or oversized result still acknowledges prior
outcomes without partially applying business data. Duplicate commands preserve
original results; new regressive acknowledgements do not lower or rewrite the
floor. An identity at/below the floor, including one never submitted to this
range, returns canonical expiry, never executes or creates a new receipt, and
only advances the marker if its committed position is new and contiguous.

Live status serializes floor and receipt lookup in the apply lane. Native pins
read both through their actual captured cut, so old pins keep old receipts while
new pins see expiry. Portable images carry floors in their existing group-prefixed
STATE section and validate the tagged key/value shape, version and sequence.
Reopen and fenced install retain the floor. Explicit routed and catalogue ACK
preparation preserve operation identity, canonical bytes and original descriptor
through refresh/retry; no automatic mutation retry or rerouting is added.

Ordinary validation passes 50 core/native, 16 read-round and 19 pure adapter cases,
`/tmp/rockserver-session-floor-full-gate.log`, Maven exit zero. Source/codec tests
cover legacy identity, v3 bounds, old/new pins, receipt removal, conflict and
oversized-result ACKs, duplicate/regressive ACKs, group/session/epoch isolation,
maximum sequences, reopen, image validation and prepared routes. The concurrent
status case exercises racing readers/apply but does not force every interleaving.
Updated boundary assertions and the native adapter reload fixture pass in
`/tmp/rockserver-session-floor-boundary-final-gate.log` (22 native + one adapter).
The unchanged checks from the full gate remain reusable.

The new adapter fixture exercises actual RatisRangeStateMachine initialization,
apply, snapshot publication, pause, reinitialize and resumed tail against native
pool and file-backed RaftStorage, without starting transport or a server. An
installed floor resolves a deferred command with a collected receipt as expiry;
uncovered tail remains pending until replay and applies once. This is native
reload composition, not quorum/failure-domain acceptance. Its initial fixture
omitted required RaftStorage builder fields; after correcting that setup, the
focused gate also passes in
`/tmp/rockserver-session-floor-adapter-reload-final-gate.log`. Native io_uring
ENOMEM fallback accounts for the Surefire native-stream warning; assertions pass.

No fuzzing, security work, traffic-isolation exercises, network probes, server
process fixtures, density runs or paused-registry validation ran. Standalone
application paths are unchanged; no publication, push or deployment occurred.

This is acknowledgement GC for a fixed range/session epoch, not the complete
session protocol. Session expiry/coarse epoch floors, catalogue-issued lifetime
authority, outstanding receipt-count/result-byte admission, floor/table cleanup,
lineage transfer and rolling v3 feature negotiation remain required. Unacknowledged
receipts and floor records are not yet given a complete retention bound. Frontend
and public Java/Rust/Thrift session/status APIs are also still pending. Next is
expiry/coarse-floor state and bounded reclamation, preserving this replay fence.


## Coarse session epoch fences and bounded local reclamation

Command v4 is an explicit control-only `minimumSessionEpoch` fence. Its issuer
must belong to a surviving epoch; it has no data mutations, predicates or ACK.
Old constructors remain v2 and v1/v2/v3 bytes/hashes remain unchanged. Applying a
new control atomically saves a monotonic group-wide tag3 floor, its original
receipt and A. Identities from lower epochs return SESSION_EXPIRED before any
sequence-floor/hash/business lookup; duplicates preserve original outcomes and
regressive controls cannot lower the fence.

New v4 proposals require a separate admission-only lifetime-authority callback,
which rejects them by default. The catalogue's structural validator recognizes
the envelope, but mutable authority never runs during committed follower apply
or replay. Already saved outcomes and fenced identities need no new authority
because they cannot advance the floor. Catalogue epoch issuance/lifetime proof is
not yet implemented, so default runtime activation remains disabled. No arbitrary
coarse-floor preparation helper was added to the routed client.

`reclaimExpiredSessions` is local physical maintenance, with immutable store-bound
progress and fixed cutoff, an installation-generation check on every page,
exclusive last-scanned-key continuation, and aggregate ceilings of 4,096 scanned
rows and 8 MiB scanned key/value payload per call across both phases. Retained
rows count against both budgets. Receipt values are sized without copying them.
Only this group's tag2 floors and receipt prefix are touched; data, marker,
configuration, tag3 and other metadata remain intact. A first-page WAL force
persists the fence before deletion can become durable; subsequent pages reuse
that barrier only in the same installation. Failed pages publish no progress.
An older sweep keeps its cutoff if a newer fence arrives; a later sweep collects
the newly obsolete epochs. Reopen restarts cleanup from the beginning safely.

Native image export filters obsolete metadata using the pin's own epoch fence.
Cleanup therefore preserves the exact logical image bytes at one A. Old native
pins retain physically deleted receipts; new images preserve the fence on fresh
installation. Restoring an image invalidates old maintenance cursors, preventing
them from deleting records made live by recovery at an earlier cut. This is local
recovery mechanics; exposing a restored range still requires the adapter's full
committed-tail/ownership readiness protocol.

The combined ordinary gate passes 53 core/native, 16 read-round and 20 adapter
cases, Maven exit zero: `/tmp/rockserver-epoch-floor-full-gate.log`. Added coverage
checks v1–v3 identity, v4 shape/issuer bounds, bounded scan/deleted-key/skipped-row
continuation, byte-limit retry, image-byte identity, reopen, newer floor during a
sweep, stale cursor after installation and neighboring-group isolation. The final
targeted gate passes 24 native plus one adapter case:
`/tmp/rockserver-epoch-floor-install-final-gate.log`; updated assertions prove
old-pin survival, fresh image install, mutable authority changing before apply,
committed follower replay and duplicate proposal outcome without reauthorization.
Unchanged full-gate cases remain reusable. The native-stream warning is the
previously observed io_uring ENOMEM fallback, with no failed assertions.

No fuzzing, security work, traffic-isolation exercises, network probes, server
process or density fixtures, or paused-registry validation ran. No main-checkout
changes, push, publication, deployment or production action occurred.

This implements coarse fencing and reclamation mechanics for a fixed group.
Catalogue lifetime authority, automatic scheduled reclamation/pressure integration,
outstanding receipt-count/result-byte admission, rolling feature activation and
lineage transfer remain required. Native WAL ordering and reopen checks do not
establish power-loss acceptance. The full distributed goal remains incomplete,
including bootstrap/frontend/API routing, shared node resources, term-bound
transactions/reservations, 2PC/snapshot fences, dynamic topology, long workflows
and representative equal-durability performance.


## Catalogue session epoch ledger and opt-in floor authority

`SessionEpochLedger` adds bounded catalogue lifetime state in a dedicated logical
column: cluster/database identity, generation, persisted monotonic logical time,
minimum/next globally unique epochs, and complete contiguous grants above the
minimum. Issuance starts at epoch 1; the initial image is empty generation zero.
At most 1,024 grants are retained, with positive lifetimes bounded to 24 hours in
catalogue logical time. Epoch/generation exhaustion fails explicitly. Issuing a
grant preserves all history/time and appends one epoch. Advancing time clamps
clock rollback and removes only the maximal expired prefix. An expired later
grant remains present, invalid and capacity-counted until its older prefix closes.
No expiry or epoch can be rewritten/reused through a valid transition.

The canonical ledger codec is versioned, bounded to 41,036 bytes and validates
exact count/length/order/lifetime invariants. `RatisSessionEpochs` provides a
borrowed-client catalogue read/CAS facade. Its deterministic validator requires
one matching-key PUT/NONE and equality predicate, fixed database key, identity,
schema and the exact permitted successor. The optional combined catalogue rule
dispatches by logical column; directory-only rules and existing constructors are
unchanged. CAS uses the existing v2/v3 commands and original durable outcomes.
`PreparedGrant` freezes the allocated grant and command, so a successful retry
never derives its epoch from current nextEpoch. ACK-only/piggyback operations
collect the issuer's covered receipts without altering grant history. The facade
normalizes transport/protocol failures and never closes the caller's client.

The opt-in floor-admission callback binds an immutable directory's cluster,
database, active group, range epoch and schema, then uses one fresh catalogue
read to check the live issuer and requested floor against the same image. Missing,
malformed or unavailable state rejects new admission. Committed apply does not
repeat this mutable lookup. This connects the previously abstract lifetime seam
without automatically enabling it in the default runtime.

Ordinary validation passes 57 core/native, 16 read-round and 23 adapter cases,
Maven exit zero: `/tmp/rockserver-session-epoch-authority-full-gate.log`. The final
focused gate passes four model and eight adapter cases:
`/tmp/rockserver-session-epoch-ack-final-gate.log`; it includes the added malformed/
unavailable reply and issuer ACK assertions. Reused unchanged cases give 97
unique checks overall. Tests cover canonical bounds/truncation, exact transitions,
out-of-order expiry/prefix closure, clock rollback, capacity/counter boundaries,
competing native CAS, original grant/result retry, column/key/identity dispatch,
borrowed ownership, unavailable/malformed replies and no automatic submission.

A native adapter fixture explicitly attaches the real facade callback to
RatisRangeStateMachine: new floor admission consumes one catalogue image, the
catalogue becomes unavailable, committed apply still succeeds, and an old-epoch
data entry produces expiry without effects. Catalogue RPC is represented by an
in-process native-backed proxy, not real transport/quorum; no live authority or
independent failure-domain acceptance is implied. The existing io_uring ENOMEM
fallback explains the native-stream warning; assertions pass.

No excluded fuzzing/security/traffic-isolation/network-probe/process/density or
paused-registry workflows ran; no main-checkout edits, push, publication or
production action occurred. This establishes bounded lifetime state in persisted
logical time. Automatic time advancement, bootstrap/admin authority, first-use
session/data-admission binding, floor propagation, receipt-count/result-byte
admission, scheduled reclamation, rolling feature activation and lineage remain
unfinished. Default floor activation is still disabled. Full architecture,
frontend parity, transactions/2PC/topology/long workflows and representative
performance/recovery acceptance remain required.


## Durable retained receipt usage and term-bound admission readiness

`ReceiptUsage` exposes an atomic completed-position/count/encoded-byte snapshot.
Native state now has a total at tag0x10 and epoch-first session buckets at tag0x11.
Bytes count the 48-byte receipt key plus version/hash/result value. New unique
results, ACK deletion and coarse epoch removal update usage in the same WAL-enabled
sync=false batch as data/receipts/floors/A. Duplicates and expired retries add no
usage; local physical cleanup leaves logical usage unchanged. Ordinary apply is
constant work; partial ACKs stream covered receipts and coarse fences stream
metadata buckets rather than user data.

Totals include their exact term/index in a 36-byte value; buckets remain 20 bytes.
Every marker advance, including duplicate, expired and config/no-op entries,
updates that binding atomically. This prevents an older writer advancing A and
receipts while leaving a deceptively valid old total. Missing, legacy20, stale or
interrupted counters rebuild with bounded memory and a synced restart flag; one
session/epoch accumulator and one receipt row are retained. Final synced totals
clear the flag. Installation recomputes bucket allocation while fenced and binds
it to the final marker. New image totals must match their header position;
legacy unbound totals are auxiliary input normalized before activation. Image
checks compare bound aggregate totals, bucket sums and retained receipt rows.

Pinned Ratis bytecode confirms that leader-ready notification precedes the native
current-term config/no-op marker. Admission therefore requires both current
leader readiness and completed native apply in that term. It does not require A
to catch every later commit/log entry or drain disjoint steady proposals.
Admissions capture term and an invalidation generation. Ratis's pre-append hook
rechecks term/role/readiness, native term and exact owner without native IO or the
adapter gate. Rejection releases ownership itself (Ratis does not cancel this
failure path) and uses StateMachineException with leaderShouldStepDown=false.
Pause/reload/shutdown/not-leader invalidate prior generations; cancellation and
release remain idempotent. Earlier-term contexts cannot append merely because a
replacement leader is now ready.

The final ordinary gate passes 66 core/native, 16 read-round and 25 adapter cases,
Maven exit zero: `/tmp/rockserver-retention-bound-position-full-gate.log`. New tests
cover exact bytes/counts, duplicate/expired/ACK/floor paths, cleanup invariance,
restartable legacy/partial counter recovery, legacy writer stale totals, no-op
position binding, old/new image metadata and fresh install. Leadership tests use
the production DivisionInfo lookup through controlled interfaces: ready-before-
native startup, inherited old term, multiple steady proposals, stale term with a
natively ready replacement, nonleader/not-ready state, generation invalidation,
non-stepdown rejection and exact release. Native session fixtures now explicitly
supply current-term startup accounting before proposal admission.

No excluded security/fuzzing/traffic-isolation/network-probe/server-process/density
or paused-registry workflow ran. Tests are controlled native/interface fixtures;
actual Ratis callback concurrency, independent failure domains and physical power
loss remain unverified. No push, publication, deployment, production or main-
checkout mutation occurred. The density fixture remains untracked and unexecuted.

This is the accounting/readiness foundation for retention limits, not enforcement:
proposal count/result-byte reservations and ACK/control progress at full capacity
remain to integrate. Bootstrap/lifetime/data-session binding, automatic clock/floor
propagation, scoped transactions/reservations, 2PC, topology, public API parity,
long workflows and representative equal-durability/resource acceptance are still
required by the full active goal.
