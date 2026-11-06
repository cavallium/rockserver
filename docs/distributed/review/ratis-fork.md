# Review: Rockserver's patched Apache Ratis 3.3.1 (`distributed/ratis-server`, `distributed/ratis-grpc`)

> **Historical, superseded.** This reviews the Codex implementation (`distributed/ratis`, the patched Ratis fork
> and its tests), which was removed and replaced by the design in [design.md](../design.md). Nothing here describes the
> current code. Its references to `status.md` and to Codex classes point to that branch: `status.md` is no longer
> in the tree; read it with `git show 9b2d16d:docs/distributed/status.md`.

Reviewer stance: independent, read-only. No builds run, no files or git state changed.
Upstream baseline: GitHub tag `ratis-3.3.1`, fetched into `/tmp/claude-1000/ratis-review-513402/up/`.
The SHA-256 of the fetched `RaftServerImpl`, `StateMachineUpdater`, `LogSegment`, `SegmentedRaftLog`,
`GrpcServerProtocolService` and `GrpcLogAppender` sources matches the hashes the fork records in its
`META-INF/rockserver-ratis-*.properties` and in `status.md` (line 1528). So the diffs below are against the
exact base the fork claims. Upstream `master` was also fetched, into `.../master/`.

## 0. How the fork is put together

* `rockserver-ratis-server:3.3.1-rockserver.1` (pom: `distributed/ratis-server/pom.xml`) checks the
  SHA-256 of the released `ratis-server-3.3.1.jar` (antrun `verify-release-input`). It then unpacks that jar
  into `target/classes` **excluding** 19 class families, and compiles replacement sources over them. Stock
  `ratis-server` is declared `provided`. The result is one jar that claims to be ratis-server, published
  under a different GAV.
* `rockserver-ratis-grpc` does the same for `GrpcLogAppender*` and `GrpcServerProtocolService*`.
* The adapter (`distributed/ratis/pom.xml`) depends on both patched artifacts. It does not depend on stock
  `ratis-server`/`ratis-grpc`. Stock `ratis-netty` only references `ratis-server` as `provided`/test, so
  today there is no duplicate on the classpath. Nothing enforces this, though (see section 3).
* Compiled with `--release 25` and mixed with upstream Java-8 bytecode. The stated runtime is JDK 25 only.

The stated purposes, from the properties files and docs:
`patch.purpose=fresh-quorum-read-rounds,committed-head-admission`, `manual-bounded-follower-append-intake`,
`resource.hooks=pre-effect-append,resident-before-insert,retained-headers,owned-io-drain,bounded-requested-read`,
`listener-before-materialization,current-and-previous-voter-exclusion,actual-transport-owner-drain`.

**The docs understate what was changed:**
* `docs/distributed/ratis-integration.md:82-89` says the artifact has "replacement `LeaderStateImpl` and
  `ReadIndexHeartbeats` classes". In fact 19 upstream classes are replaced.
* The header comment added to the forked `RaftServerImpl.java` says Rockserver "changes only the completed
  linearizable-read query dispatch and adds package-local test seams". That is false. The class also gains
  vote, candidate and leader gating, source-retirement admission, bounded follower intake, and an
  image-readiness append rejection.

## 1. What changed in each class (lines are `diff -w` counts, +added / -removed)

| Class | +/- | What changed |
|---|---|---|
| `impl/ReadIndexHeartbeats` | +389/-122 (rewrite, 191 -> 458 lines) | New "sealed read rounds": a 1 ms batch timer; captures the call-ids of all appenders before triggering heartbeats; requires fresh acks with `followerCommit >= target`; checks term/configuration/appender "generation". Adds per-leader limits (1024 waiters, 128 KiB), a **JVM-global static** `Semaphore(4096)`, a **static** 2-thread timer pool, a virtual-thread failure publisher and a 32-appender cap. |
| `impl/LeaderStateImpl` | +17/-38 | `getReadIndex` now delegates entirely to the new tracker. Removed: the singleton fast path, the `!isReady()` path, the **leader-lease / `leaderHeartbeatCheckEnabled` path** (both config keys are now silently ignored), and `ackedCommitIndex` reuse. Calls `invalidate()` on conf/appender changes. |
| `impl/RaftServerImpl` | +271/-40 | (a) `RecoveryEligibilityStateMachine.Gate` gating of `requestVote`, `changeToCandidate`, `changeToLeader`, `initElection`, `startLeaderElection`, `setConfiguration`, under a new lock order (division -> Gate, metadata fsync under Gate). (b) "Source retirement" admission counting around every client, append, readIndex and installSnapshot RPC, plus role shutdown, frontier freeze, seal and resume. (c) Appends are rejected with `INCONSISTENCY, nextIndex=0` while the image is not ready. (d) A per-append `LogResourceStateMachine` reservation and scope. (e) Stale read at `minIndex==0` bypasses the commit check for `RecoveryOwnerQueryStateMachine`. (f) `LinearizableQueryStateMachine.queryLinearizable` dispatch after the barrier, plus an optional leader-only check. |
| `impl/StateMachineUpdater` | +323/-17 | Adds a `CommittedApplyStateMachine.Admission` two-stage read/describe/dispatch before materializing each committed entry. Adds "covered prefix" (advances `appliedIndex` **without applying** entries), "discovery drain", "source retirement", "genesis metadata only" and "observed local reload" state machines inside the updater loop. These use about 16 `awaitForSignal.await(100ms)` polling points. With admission enabled, stop no longer waits for apply futures and snapshots are skipped while applies are pending. |
| `impl/SnapshotInstallationHandler` | +77/-17 | Refuses install during discovery or genesis. Revokes or restores eligibility around replacement. Keeps a newer committed configuration instead of truncating to the snapshot's configuration when the initial image is missing. Replies now carry the post-update term. |
| `impl/FollowerState`, `impl/LeaderElection` | +4/-2, +2/-2 | Election timeout and candidate checks go through the eligibility gate. |
| `leader/LogAppenderBase` | +26/-1 | For LISTENER followers: requires a `MigrationLogStateMachine` lease before materializing, and sends **one entry per request**. |
| `leader/InstallSnapshotRequests` | +87/-5 | Adds a recovery `SnapshotPermit` re-checked per chunk, a closeable iterator, and a single-file "source" mode. Exposed through the new public `RockserverSnapshotRequests`. |
| `raftlog/RaftLogBase` | +52/-5 | Every append path (`append(term,trx)`, `appendMetadata`, conf, `appendEntry`, `append(list)`) is wrapped in a reservation plus a thread-local `LogResourceScope`. |
| `segmented/SegmentedRaftLog` | +119/-7 | Footprint computation; preallocation disk reservation; metadata-row "promises"; follower append reservation; **an extra full read of every segment at open**; an **extra `stateMachine.data().flush(index)` per appended entry**; `onSnapshotInstalled` completion observer; reordered close. |
| `segmented/SegmentedRaftLogWorker` | +126/-25 | Bounded queue accounting (own CAS counters, `offer` instead of blocking `put`); the write buffer is charged to the budget; removed the `writeBuffer >= bufferByteLimit+8` check; `close()` now throws if the writer does not drain in 3 s; **`syncWithSnapshot` keeps `flushIndex`/`lastWrittenIndex` = max(snapshot, flushIndex)** instead of resetting them to the snapshot index. |
| `segmented/LogSegment` | +100/-7 | Per-record and per-cache leases. Cold load reads each segment **twice**. Bounded `loadCache` **re-scans the segment from the start for every missing entry and caches only that entry** (upstream loads the whole segment once). Empty open segments are deleted on load. |
| `segmented/SegmentedRaftLogCache`, `BufferedWriteChannel`, `SegmentedRaftLogOutputStream`, `SegmentedRaftLogReader` | +15/-2, +13, +29, +8/-6 | Plumbing; streaming of frames larger than the write buffer; reader mark/limit margin of +9 bytes (varint+CRC); negative-length guard. |
| `grpc/server/GrpcServerProtocolService` | +266/-2 | Replaces `bindService()` with a custom marshaller that reserves decode budget **before** protobuf parse. Hand-off to the listener goes through a ThreadLocal (`ADMITTED_DECODE`), and correctness relies on grpc-java parsing and calling `onMessage` on the same thread. Adds `AdmittedAppendObserver` (manual `request(1)` flow control, per-reply leases). |
| `grpc/server/GrpcLogAppender` | +78/-15 | Migration lease transport ownership. `getWaitTimeMs` uses `super.shouldSendAppendEntries()` to avoid an idle spin when the stream is absent. Unsent requests are rolled back. `onError`/`onCompleted` from a stale stream handler no longer reset the current stream. `stopAsync` waits for the owners to drain. |
| New: `CommittedApplyStateMachine`, `LinearizableQueryStateMachine`, `RecoveryEligibilityStateMachine` (+Gate), `RecoveryOwnerQueryStateMachine`, `LogResourceStateMachine` (+Registration), `LogResourceScope`, `LogMetadataReservations`, `MigrationLogStateMachine`, `MigrationTransportOwners`, `RockserverSnapshotRequests`, `RockserverRaftServers` (411 lines: create/coverLocalPrefix/holdDiscovery/reloadLocalCheckpoint/beginSourceRetirement) | about 1,000 lines | Application-specific SPI injected into `org.apache.ratis.server.*` packages. |

In total, about 2,100 changed lines across 21 replaced classes, plus about 1,000 lines of new core-package
classes.

**How the adapter depends on it:** `RatisRangeStateMachine` (6,572 lines) implements **six** fork
interfaces: `MigrationLogStateMachine`, `LinearizableQueryStateMachine`, `CommittedApplyStateMachine`,
`LogResourceStateMachine`, `RecoveryEligibilityStateMachine` and `RecoveryOwnerQueryStateMachine`. Other
users:
* `RatisCommittedApplyDispatcher` (Admission/permits).
* `RatisLogResourceBudget` (Resources/reservations).
* `RatisNodeRuntime`: `RockserverRaftServers.create`, `hasAppliedPrefix`, `ownsLocalUpdater`.
* `RatisRangeStateMachine`: `holdDiscovery`, `coverLocalPrefix`, `reloadLocalCheckpoint`, `hasGenesisPrefix`.
* `RatisTopologySourceSnapshotService`: `RockserverSnapshotRequests`.

The adapter config (`RatisDurability.java:35-38`) is `Read.Option.LINEARIZABLE`, lease **off**, heartbeat
check **on**, `ReadIndex.Type.COMMIT_INDEX`. The adapter **already uses the upstream
`GrpcConfigKeys.Server.setServicesCustomizer` hook** (`RatisSnapshotAckInterceptor.java:44`).

## 2. Is each change necessary?

### 2a. Real upstream bug: the linearizable ReadIndex can be stale (the fork's core idea is justified, but not its size)

Upstream 3.3.1 (unchanged on `master`, 0 diff lines):
* `ReadIndexHeartbeats.java:173-178`: `addAppendEntriesListener` returns `null` (so the read completes
  immediately) whenever `commitIndex <= ackedCommitIndex`. `ackedCommitIndex` is raised at line 156 whenever
  an earlier round reached a majority.
* `LeaderStateImpl.java:1209-1215`: a `null` listener means `completedFuture(readIndex)`.
* `ReadIndexHeartbeats.java:136`: `sorted.computeIfAbsent(commitIndex, ...)` makes a new read **join a
  round that was triggered before the read arrived**. Its `minCallId` (line 51) was captured when the earlier
  read arrived.

The consequence: with no new writes (so the commit index does not move), every later read reuses a
leadership confirmation from the past. Picture a deposed but not yet stepped-down leader. Its partition may
have elected a new leader that has already committed writes. The old leader keeps serving stale "linearizable"
reads until its own leadership check makes it step down, which takes about one election timeout. That violates
Raft §6.4 / ReadIndex, which requires a heartbeat round started after the read arrived.

**This is a genuine upstream defect and the strongest reason for any patch.** The fork fixes it correctly in
principle: fresh call-ids captured before the trigger, rounds sealed per batch, no reuse. But it does so with
a 458-line rewrite that adds new hazards:

* **Liveness:** any exception in `work()`, or one `RejectedExecutionException` from the server executor in
  `requestWork()` (line 210), calls `failListeners`. That sets `stopped` permanently (line 319). From then
  until leadership changes, the leader rejects **every** linearizable read for the rest of its term.
* JVM-global statics (`PUBLICATIONS` semaphore, `SHARED` timer pool, `Thread.startVirtualThread`) are shared
  by all groups and all `RaftServer`s in the process, which couples tenants. `MAX_APPENDERS=32` is a hard cap.
* The upstream singleton fast path is gone, and every read pays at least 1 ms of batching.
* The lease path and `leaderHeartbeatCheckEnabled` are deleted rather than kept. Users of those config keys
  get no error and no effect.
* `captureReadContext` takes the `RaftServerImpl` monitor three times per round.

**Minimal upstream-quality fix:** about 30-60 lines in `ReadIndexHeartbeats` plus about 2 lines in
`LeaderStateImpl`. Delete the `ackedCommitIndex` shortcut. Key pending listeners by a "round generation"
instead of `commitIndex`, so that a request only joins a round whose heartbeats have not yet been triggered.
Keep the singleton, lease and startup paths. This should be proposed as an upstream JIRA/PR.

**No-patch workaround (if a fork is unacceptable):** make reads go through the log. Batch reads behind a
no-op write committed after their arrival, then query at that index. Commit proves leadership after arrival.
It costs one fsync per batch. Lease mode does **not** help, because when the lease expires it falls back to the
same buggy path.

### 2b. Real upstream gap: completion of async apply vs. the read barrier (the fork does NOT fix this)

Upstream `StateMachineUpdater.java:265-267` calls `f.thenAccept(m -> notifyAppliedIndex(i))` per entry.
Non-data entries (configuration, and the frequent METADATA entries, because `logMetadataEnabled` defaults to
true) call `notifyAppliedIndex` **immediately** (`RaftServerImpl.java:1972-1974` + updater :267).
`ReadRequests.ReadIndexQueue.complete` advances by **maximum** (`ReadRequests.java:107`) and releases every
waiter at or below that index.

So with truly asynchronous `applyTransaction` futures, a linearizable read can be released before an earlier
data entry has finished applying. `ratis-integration.md:7-15` identifies this correctly. **But the fork's
updater still notifies the same way** (`delivered=f.thenAccept(notifyAppliedIndex)`, immediate when
`f==null`). The adapter avoids the problem by applying synchronously, not because of the fork.

* **Minimal upstream patch:** about 5 lines. Notify from the chained `applyLogFutures`, which is already
  contiguous by construction, instead of from each `f`.
* **No-patch workaround:** in `query()`, wait until the state machine's own contiguous-completed index is at
  least `division.getInfo().getLastAppliedIndex()` sampled on entry. The updater increments that index at
  dispatch, so it is at least the read index.

`CommittedApplyStateMachine` (two-stage read/describe/permit/re-read) is **memory admission**, not ordering.
It is not needed for correctness.

### 2c. Not necessary: application lifecycle logic pushed into consensus internals

* **Recovery-eligibility vote gating** (`RaftServerImpl`, `FollowerState`, `LeaderElection`,
  `SnapshotInstallationHandler`, `InstallSnapshotRequests`). This protects against "amnesia": a voter that
  lost its durable state.
  * The concern is real. Ratis, like Raft generally, assumes voters do not lose state.
  * The standard solution is at the membership and lifecycle level. Do not start or add the division until
    its state is verified (Ratis supports runtime `groupAdd`/`groupRemove`). Re-add a wiped member as a
    `LISTENER` and promote it with `setConfiguration` once it has caught up. Remove a member that lost
    state, then re-add it.
  * Patching vote/candidate/leader transitions and the metadata-fsync lock order is the riskiest possible
    place to edit Raft. The added `INCONSISTENCY, nextIndex=0` reply sent before term recognition also makes
    leaders rewind replication for that follower over and over.
* **Covered prefix / discovery drain / source retirement / genesis-metadata-only / observed local reload**
  (`StateMachineUpdater`, `RaftServerImpl`, `RockserverRaftServers`).
  * Upstream already covers this. The state machine reports native progress through
    `getLastAppliedTermIndex()`/`reinitialize()` and skips indexes at or below its durable native cut
    idempotently.
  * A runtime image swap uses the supported `pause()` -> `reinitialize()` path, via
    `notifyInstallSnapshotFromLeader` when `installSnapshotEnabled=false`.
  * To retire a member, use `transferLeadership`, `setConfiguration`, then `groupRemove`.
  * "Covered prefix" advances `appliedIndex` without calling the state machine at all. That bypasses the
    apply contract.
  * The updater thread now runs a polling (100 ms) multi-mode protocol of application concepts (GENESIS,
    CAT, META, source receipts).
* **Log/transport resource admission** (`LogResourceStateMachine` across `RaftLogBase`, the entire segmented
  log, and grpc intake). This is node-wide memory accounting, not correctness. Upstream offers:
  * `Log.queueByteLimit/queueElementLimit`, `Log.Appender.bufferByteLimit/bufferElementLimit`,
    `Write.elementLimit/byteLimit`, `Log.segmentCacheNumMax/SizeMax`.
  * `GrpcConfigKeys.messageSizeMax`/`flowControlWindow`, and the leader's outstanding-appends limit.
  * `GrpcServices.Customizer` (present in 3.3.1, and already used by the adapter). Through it a
    `ServerInterceptor` can wrap `ServerCall.request(n)` to gate demand, or use
    `ServerInterceptors.useInputStreamMessages` to budget before parse, without replacing
    `GrpcServerProtocolService`.

  A cross-group node budget is not upstream. Per-group caps times the group cap are a reasonable
  approximation, and the general feature should go upstream as a proposal. The fork also introduces
  **performance regressions**:
  * `LogSegment.loadCache` re-scans the segment from the start for each missing entry
    (`LogSegment.java:475`). That is O(n²) I/O when a lagging follower catches up from evicted segments.
  * Every segment is read 2-3 times at startup (`SegmentedRaftLog.loadLogSegments` + `LogSegment.loadSegment`).
  * There is an extra `stateMachine.data().flush(index)` per entry (`SegmentedRaftLog.java:503`). That
    changes the `DataApi` contract for any state machine that implements it.
* **Listener tail admission** (`LogAppenderBase`, `GrpcLogAppender`, `MigrationTransportOwners`). This
  throttles to one entry per request for listeners. Same resource-only rationale. Upstream alternatives:
  catch learners up by snapshot, use appender buffer limits, or an upstream "per-follower appender limits"
  proposal.
* **`LinearizableQueryStateMachine`.** `query()` is reached without a barrier only for `DEFAULT` or
  `preferNonLinearizable` (upstream `RaftServerImpl.java:1123-1130`), and the adapter controls its own
  client. Encode intent in the message or never set `preferNonLinearizable`. For leader-only queries, check
  the role inside `query()` or route to the leader on the client side.
* **`RecoveryOwnerQueryStateMachine`.** By its own javadoc it "does not require any committed log entry" and
  grants no Raft freshness. It does not belong in the Raft read path. Serve it through the adapter's own gRPC
  service.
* **`RockserverSnapshotRequests`** exposes the package-private `InstallSnapshotRequests` so the adapter can
  reuse Ratis's internal chunk producer for its own transfer protocol. Use your own file streaming or Ratis
  DataStream instead.
* **`RockserverRaftServers.create`** closes the proxy when `initGroups` fails. That is a plausible minor
  upstream resource leak. File it upstream; it is not worth a fork.

### 2d. Small upstream issues that may be real; file JIRAs, don't fork
* `GrpcLogAppender.getWaitTimeMs`: the overridden `shouldSendAppendEntries()` (upstream :321-323) returns true
  whenever the stream is absent. That gives zero wait while `newAppendEntriesRequest` returns null until the
  heartbeat deadline, a CPU spin after a stream reset.
* A stale stream handler's `onError`/`onCompleted` resets the current stream (upstream behaviour). Benign
  churn.
* `SegmentedRaftLogReader.decodeEntry` uses `mark(max)`/`checkBufferSize(total,max)` without the 5+4 bytes of
  varint and CRC. An entry close to `bufferByteLimit` may be writable but not readable. The adapter already
  leaves 1 MiB headroom (`status.md:237`), which is the config-level workaround.
* `SegmentedRaftLogWorker.syncWithSnapshot` resets `flushIndex` to the snapshot index even if retained
  entries above it are already durable. The fork's `max()` is arguably more accurate, but it changes a
  durability-related invariant with no upstream tests. Discuss upstream before relying on it.

## 3. Risks of keeping the fork

1. **Upgrades.**
   * Upstream `master` already differs from 3.3.1 by **658 lines in `RaftServerImpl`**: the read path moved
     into a `readRequestHandler`, the stale-read path changed, and pending-request permits were added. It
     differs by 121 lines in `GrpcLogAppender` and 25 in `LeaderStateImpl`.
   * The fork edits all of these, so the next upgrade is a manual re-port of about 2,100 lines across 21
     classes. Every upstream bug or security fix in these classes needs a manual port too.
   * The checksum pin stops silent drift but also blocks patch releases.
2. **Correctness evidence.**
   * Upstream's `ratis-test` suite (election, reconfiguration, snapshot, leader-lease, MiniRaftCluster
     fault tests) is never run against the patched jar. The fork has 13 unit-level test classes.
   * `status.md` says repeatedly: "No real server process, network/quorum probe, power-loss run or deployment
     was performed" (around line 1535). Also: "controlled no-socket checks", "not multi-node quorum or
     network evidence".
   * The edited areas are exactly where Raft bugs are subtle: voting and fsync ordering, flush index after
     install, conf retention during install, appender rollback.
3. **Split-brain class loading.**
   * The patched jar contains the full `org.apache.ratis.server.*` / `org.apache.ratis.grpc.server.*` under
     a different GAV, so Maven cannot mediate it.
   * Any dependency that pulls in stock `ratis-server`/`ratis-grpc` at compile/runtime scope (ratis-shell,
     examples, a future ratis-netty pom, an embedding application) puts two copies on the classpath. The
     first one wins per class, which can mix patched and stock classes and fail with `NoSuchMethodError`,
     `IllegalAccessError` or wrong behaviour.
   * There is no `maven-enforcer` `banDuplicateClasses` or dependency ban.
   * Global statics in `ReadIndexHeartbeats` and the static registry in `LogResourceStateMachine` are
     JVM-wide.
4. **JPMS.**
   * Ratis already has split packages across automatic modules: `org.apache.ratis.server` exists in both
     `ratis-server-api` and `ratis-server`. The fork adds public SPI types into those same internal packages,
     which locks in classpath-only use (the docs admit the "optional classpath adapter").
   * The upstream manifest is excluded and replaced.
   * The `--release 25` classes are mixed with Java 8 classes.
5. **Coupling and maintainability.**
   * Application concepts (GENESIS/MISSING/RECOVERY, source receipts, CAT/META, native drains) now live inside
     the Raft core and its updater thread.
   * The code style is dense one-line methods (for example `StateMachineUpdater.consumeSourceRetirement`) and
     many 100 ms polling waits, which makes it hard to review.
   * Six fork interfaces are implemented by one 6,572-line state machine.
6. **Licensing hygiene.** Apache-2.0 §4(b) requires prominent change notices in modified files. Only
   `RaftServerImpl` has one, and it is inaccurate.

## 4. Verdict

**Can the adapter use stock Ratis? Almost.** 3.3.1 is still the newest 3.x tag (checked against the GitHub
tags list). Stock 3.3.1 is enough for everything the fork does **except one thing**: linearizable
ReadIndex freshness (2a). That is a real upstream safety bug, still present on `master`. Everything else is
either:
* resource or flow-control policy, achievable with upstream config keys plus `GrpcServices.Customizer`
  interceptors; or
* adapter lifecycle logic that belongs in the state machine or membership layer (eligibility, retirement,
  discovery, covered prefix, owner queries, snapshot transfer); or
* minor upstream polish worth a JIRA (2d).

**Minimal patch set truly needed:**
1. **ReadIndex freshness**: `ReadIndexHeartbeats` + 1-2 lines in `LeaderStateImpl`, about 50 lines. Remove
   `ackedCommitIndex` reuse and never join a round triggered before the request arrived. Keep the lease,
   singleton and startup paths. Submit it upstream. Until it is released, either carry this single small
   patch, or use the log-read workaround (no-op write per read batch).
2. **Optional, only if apply becomes asynchronous**: `StateMachineUpdater`, about 5 lines. Notify the applied
   index from the contiguous `applyLogFutures` chain. Submit upstream, or use the state-machine-side wait
   described in 2b.

**How to deliver it, if patches are carried at all:** do not unpack and overlay released jars. Keep a real
git branch of `apache/ratis` on tag `ratis-3.3.1` with these one or two commits, run the full upstream test
suite, and publish all Ratis modules under a single distinct version. Add a duplicate-class enforcer rule to
the adapter. Then remove the other 19 class replacements and the about 1,000 lines of core-package SPI, and
rebuild those features in the adapter on top of stock APIs.
