# Review: `distributed/ratis` adapter (main sources)

> **Historical, superseded.** This reviews the Codex implementation (`distributed/ratis`, the patched Ratis fork
> and its tests), which was removed and replaced by the design in [design.md](../design.md). Nothing here describes the
> current code. Its references to `status.md` and to Codex classes point to that branch: `status.md` is no longer
> in the tree; read it with `git show 9b2d16d:docs/distributed/status.md`.

Scope: `distributed/ratis/src/main/java/it/cavallium/rockserver/distributed/ratis/` (worktree `distributed-rockserver-rewrite-e46da4`, HEAD `9b2d16d`). Read-only review; nothing was built or run. Paths below are relative to that package directory unless stated otherwise. `SM` = `RatisRangeStateMachine.java`.

Bar used: *a production single-node RocksDB server becomes replicated, so the Yotsuba client survives node restart or loss with high uptime*. The simplest design that meets that bar is the yardstick.

---

## 0. Headline numbers

| Metric | Value |
|---|---|
| Files | 241, one flat package, no sub-packages |
| Lines (`wc -l`) | 40,377 |
| **Bytes** | **4,638,033** (average about 115 chars/line; 5,540 lines are longer than 200 chars; the longest is 2,317 chars) |
| Normal-formatted equivalent | roughly 100k+ lines. Line count understates the code by about 2.5x. |
| `RatisRangeStateMachine` | 6,572 lines, **717 KB**, 132 fields, 58 nested types, about 590 member declarations, 300 `synchronized` blocks, 480 `require*` calls |
| Hand-rolled binary codecs | 36 `*Rpc` classes and 15 `*Record` classes (2,620 lines), plus 44 files with their own `MAGIC` ints. There is no protobuf for internal messages. |
| Logging | **Zero.** One `System.err.println` (`RatisNodeMain:25`) and one logger passed into the native pool (`RatisNodeRuntime:491`). No metrics. |
| Branch | 427 commits on top of master. 142 of the subjects are about recovery, retirement, home, source or fence. |
| Tests (other reviewer) | 47k lines, 5.4 MB |
| Class coupling | 198 of 241 classes form one transitively connected component reachable from `SM`, `RatisRoutedClient`, `RatisNodeRuntime` and `RatisRocksDBDirect` alike. |

### 30 largest files (lines / KB / longest line)

| File | Lines | KB | Max line |
|---|---|---|---|
| RatisRangeStateMachine | 6572 | 717 | 1886 |
| RatisNodeRuntime | 2229 | 315 | 1676 |
| RatisRoutedClient | 1837 | 176 | 1106 |
| RatisDurableSnapshot | 759 | 76 | 520 |
| RatisDurableTransaction | 735 | 71 | 583 |
| RatisTopologySourceSnapshotArtifacts | 553 | 91 | 1932 |
| RatisRocksDBAdapter | 546 | 51 | 409 |
| RatisSnapshotClient | 489 | 46 | 464 |
| RatisSchemaClient | 469 | 48 | 432 |
| RatisAtomicClient | 441 | 42 | 434 |
| RatisExportClient | 392 | 37 | 620 |
| RatisTopologySourceSnapshotService | 384 | 44 | 704 |
| RatisRocksDBConnection | 381 | 62 | 1337 |
| RatisExportJournal | 377 | 56 | 1144 |
| RatisSnapshotPins | 353 | 27 | 448 |
| RatisSchemaAdministration | 353 | 36 | 296 |
| RatisScopedTransactions | 350 | 25 | 380 |
| RatisRocksDBIterators | 349 | 28 | 350 |
| RatisAtomicMutationOwner | 347 | 45 | 807 |
| RatisParticipantRecoveryRegistry | 340 | 24 | 286 |
| RatisCdcSubscriptionController | 340 | 32 | 451 |
| RatisDurableParticipantRecovery | 337 | 32 | 326 |
| RatisSessionPropagation | 319 | 33 | 2317 |
| RatisTopologyCatalogue | 312 | 41 | 791 |
| RatisConfiguredRecovery | 307 | 51 | 1215 |
| ScopedTransactionRpc | 300 | 23 | 1346 |
| RatisRangeImportController | 289 | 29 | 498 |
| RatisDurableLogicalSession | 285 | 25 | 504 |
| RatisOperationRerouteCoordinator | 282 | 42 | 1948 |
| RatisSessionClient | 273 | 20 | 414 |

### Code volume by subsystem (bytes; approximate bucketing by file name)

| Subsystem | KB | Share |
|---|---|---|
| Topology, split/merge, relocation, import, configured recovery, source/home closing, retirement, reroute | 962 | 21% |
| State machine (single file, mostly holds for the rows above) | 717 | 16% |
| Client, adapter, routing, catalogue | 572 | 12% |
| Export, backup, restore, CDC | 477 | 10% |
| Node runtime, plan, manifest, service | 466 | 10% |
| Transactions, 2PC, homes, atomic | 410 | 9% |
| Snapshots, cohort, fences | 295 | 6% |
| Schema jobs | 261 | 6% |
| Sessions, receipts, client journals | 258 | 6% |
| Resource budgets, disk runway, committed-apply lanes | 113 | 2% |

---

## 1. Architecture map

### Raft groups

There is one Ratis `RaftServer` per node, hosting many divisions (`RatisNodeRuntime.buildServer:2214`). Each group has one of four roles (`RatisNodePlan.Role:16`):

- **CATALOGUE**: one group. It holds the range directory, session epochs, the topology/schema/import/CDC catalogue, and the column/operator catalogue.
- **DATA**: one group per key range per "domain". Up to `RangeDirectoryCodec.MAX_RANGES`.
- **TRANSACTION_HOME**: up to 64 groups. 2PC decision records ("home-record CAS").
- **SNAPSHOT_HOME**: snapshot-fence home records.

The frontend backend factory refuses to start unless both HOME roles exist (`RatisDatabaseBackendFactory:38,43`). **The minimum deployment is therefore 4 Raft groups** (catalogue, data, transaction home, snapshot home) to serve a single-range database.

### Key classes

| Class | Lines / KB | Role |
|---|---|---|
| `RatisRangeStateMachine` | 6572 / 717 | One class serves all four roles, switching on `role`. It implements six *forked* Ratis interfaces (`SM:100`): `MigrationLogStateMachine`, `LinearizableQueryStateMachine`, `CommittedApplyStateMachine`, `LogResourceStateMachine`, `RecoveryEligibilityStateMachine`, `RecoveryOwnerQueryStateMachine`. It cannot run on stock Ratis. |
| `RatisNodeRuntime` | 2229 / 315 | Node lifecycle, pools, groups, recovery, seeds, relocation sidecars, disk runway. `initialize()` (`:408-729`) is one 43 KB method. There are 12 telescoping `open(...)` overloads (`:248-300`). |
| `RatisRoutedClient` | 1837 / 176 | Per-domain router plus the Raft transport (`RaftTransport:1699`). |
| `RatisRocksDBConnection`, `RatisRocksDBAdapter`, `RatisRocksDBDirect`, `RatisPointOwnerManager` | — | Maps the Rockserver `RocksDBConnection` API onto distributed commands. |
| `RatisSessionClient`, `RatisDurableSessionClient`, `RatisDurableLogicalSession`, `SessionWindowJournal`, `RatisDirectRequestJournal`, `RatisRemoteMutationReceipts` | — | **Client-side** (frontend-side) durable sessions and request journals. |
| `RatisDurableTransaction`, `RatisScopedTransactions`, `RatisTransactionCoordinator`, `RatisTransactionStatuses`, `RatisParticipant*`, `ScopedTransactionRpc` | — | Scoped transactions and cross-group 2PC. |
| `RatisTopologyController`, `RatisRelocationController`, `RatisTopology*Rpc/Record`, `RatisConfiguredRecovery*`, `RatisRangeImport*` | — | Split/merge/relocate/import, driven by durable phase journals. |
| `RatisDurability` | 43 | Ratis property set. This is the one clean file. |

### Write flow (single `put`)

1. **A plain `put` is rejected.** `RatisRocksDBAdapter.write:399` does `if(id==0) throw explicitMutationRequired()`, which returns `NOT_IMPLEMENTED` with the message "Distributed mutations require the explicit RemoteMutationAsyncAPI owner, request identity and result ACK" (`:537`). `DeleteRange`, `PutBatch` and `MergeBatch` are rejected the same way (`:155`). Clients must switch to a new protocol (`rocksdb.proto:595-607`): `owners` → `preparePoint` → `execute` → `results` → `acknowledge`. That is 3-4 client RPCs per write.
2. `RatisRocksDBConnection:311` → `Generation.point(owner)` → `RatisRocksDBDirect.prepareSequence` (`:67`). This builds a `RemoteMutationToken` and `DistributedCommand`s, then `journal.begin()` does an atomic file write with fsync of the file and the directory (`RatisDirectRequestJournal:44,58`; `SessionJournalFiles.write:41-50`).
3. `execute` → `advance` (`RatisRocksDBDirect:107-121`) → `session.prepareAt` (session window checkpoint, another fsync pair, `RatisDurableSessionClient:78,192`) → `journal.bind` (fsync) → `session.executeAsync` → `RatisSessionClient.executeAsync` → `routed::executeAsync` (`RatisRoutedClient:1543`) → `transport.commandAsync` → **`metadataWriteAfterLeaderBarrier`** (`:1679`). That last step first sends a linearizable `STATE` read-only query. `STATE` takes a RocksDB snapshot pin and encodes a full `RangeStateView` (`SM:6404-6408`). Only then is the write sent. **Every write therefore costs an extra ReadIndex round plus a native snapshot.**
4. Leader: `startTransaction` (`SM:3745`) sniffs the magic prefix across about 20 record kinds, reserves node resources, and runs admission. Raft appends and replicates the entry (fsync on 3 nodes; `RatisDurability:28-30`).
5. `applyTransaction` (`SM:4379`) runs under `synchronized(gate)`. `applyRecord` dispatches on the magic prefix (`SM:4252-4317`) to `store.apply` (`ReplicatedRangeStore`, core), which writes user effects, the receipt and the applied marker in one WAL batch. RocksDB's WAL is on as well, so there are two local logs.
6. Back on the frontend: `journal.result` (fsync), `session.acknowledgeLocally` (checkpoint fsync), `journal.acknowledged` (fsync), then later `acknowledgeResult` → `receipts.acknowledge` + `journal.retire` (fsync + unlink + directory fsync).

That is **about 7-8 atomic file rewrites (about 15 fsyncs) on the frontend** per point write, on top of Raft. `SessionWindowJournal` rewrites the whole session snapshot each time (`journal.write(client.grant(),limits,client.snapshot())`).

**Concurrency cap:** `RatisRocksDBDirect.reserve:144` has `... || write && writing) throw SERVER_OVERLOADED "Direct request capacity exhausted"`. That allows one in-flight write per point owner. The default is `pointSlots=2` (`RemoteMutationOwnerLimits.defaults()`, max 16). **A frontend therefore processes at most 2 concurrent point writes, and extra writes are rejected, not queued.**

### Read flow (point `get`)

`RatisRocksDBDirect.get:54` → `routed.read` (`RatisRoutedClient:1160`) → `transport.read` → `sendReadOnly(POINT)`. This is a Ratis ReadIndex read with `Read.Option.LINEARIZABLE`, leader lease disabled, and `ReadIndex.Type.COMMIT_INDEX` (`RatisDurability:35-38`). It is served by leader or follower; `POINT` is not in `requiresLeaderForLinearizableQuery`. It then goes `query` → `readablePoint` (`SM:3375`), which is **`synchronized (gate)`**: the same monitor `applyTransaction` holds. All reads on a range serialize with each other and with apply.

Reads are retried on `NotLeader` up to 5 times (`snapshotRetryPolicy:1657`). Writes never retry.

---

## 2. Consistency model as implemented

| Aspect | Verdict |
|---|---|
| Linearizable point reads | **Yes**, via Ratis ReadIndex (`RatisDurability:35-38`, `sendReadOnly`). Correct but expensive: monitor-serialized at the state machine. |
| Stale/follower reads | `queryStale` (`SM:5755`) checks `minIndex` against applied. Not exposed through the public adapter. |
| Apply ordering | Strict log order inside `synchronized(gate)`. Some record kinds (schema install, topology freeze, range import, referenced file) are "async native" (`asyncNativeRecord:4448`). These park every *later* entry in a `deferred` TreeMap and finish them in `finishNativeApply` (`:4555-4590`). Ordering is preserved, but one slow schema install stalls the whole group's apply. In the non-production path the deferred queue is capped at 1024 entries / 16 MB, and overflowing it poisons the group (`:4404`). In the production path the cap is delegated to the forked `committedGroup`. |
| Idempotency / dedup | Three layers: (a) Ratis' own retry cache (clientId/callId), which is unused because the client never retries writes; (b) server receipts keyed by `OperationId(session, epoch, seq)` and `receiptDigest`, stored in RocksDB (`store.operationResult`, `recoveredRecord:4374`); (c) **frontend-local** journals (`RatisDirectRequestJournal`, `RatisRemoteMutationReceipts`, `SessionWindowJournal`). Layer (b) alone, plus Ratis client retry, would have been enough. |
| Ambiguous write outcome | On a transport error, `executeAsync` (`RatisRoutedClient:1543-1567`) queries `RatisTopologyOperationOutcomeRpc`. `ABSENT` becomes `OriginalOperationAbsent` and is surfaced to the caller. `ABSENT` at query time does not prove the entry will never commit: an old-term entry can still be committed by the new leader. Safety then rests on the server receipt dedup when the user `resume`s the same token. That is fine if it holds, but the result is that **transient leader changes surface to Yotsuba as errors** rather than being retried. |
| Session ownership vs node loss | The token's owner is a **frontend-local** durable session (`RatisPointOwnerManager`, journals under the frontend's root). `status()` and `resume` read only local journals (`RatisRocksDBDirect:96,105`). Another frontend reports "Original point owner unavailable" (`RatisRocksDBConnection:51`). **If the frontend node is lost, its in-flight requests become unresolvable from any other node.** This is the opposite of the stated goal. |
| Snapshot | `takeSnapshot` (`SM:3339`) writes a **full logical image** of the range to one file (`NativeRangeImage.write`, "portable logical range image"). It then computes MD5, validates, and fsyncs. The image is bounded by `maxImageBytes` (manifest-configured). Cost is O(dataset) per snapshot, where a RocksDB checkpoint would be O(1) via hard links. Auto-trigger is off (`RatisDurability:40`). Snapshots are triggered by disk-runway pressure through `SnapshotManagementRequest` (`RatisNodeRuntime:174-184`). |
| Snapshot install | `reinitialize` (`SM:3619`) reloads the store, then resolves every deferred entry covered by the image against saved receipts (`recoveredRecord`), and fails if one is missing. It also refuses outright when `storageFailure != null` (`:3632`), so a poisoned replica cannot even be repaired by snapshot install. |
| Restart recovery | `initialize` (`SM:2985`) plus `RatisNodeRuntime.initialize` plus "configured recovery discovery" (`RatisConfiguredRecovery*`, `RecoveryEligibilityStateMachine`). DATA groups refuse apply and queries until "discovery settles" (`requireDiscoverySettled:6560`). status.md:2427 says "independent multi-node cold restart remains a separate gate", and status.md:2457 says "Independent quorum/restart/fault ... gates remain outside the currently authorized controlled-fixture validation". **The core goal has not been validated, by the code's own admission.** |
| Fatal-poison semantics | Any throwable from apply sets `storageFailure` (`SM:4429`, `:5722`) and every later operation fails with "Range requires repair" (`requireProgressHealthy:6567-6571`). It is never cleared. Apply contains many deterministic validation throws, for example `throw new IllegalArgumentException("Activation original installation changed")` (`SM:4277`) and the committed installation fence check (`:4529`). A deterministic throw on a committed entry poisons **every replica, every restart** (it replays). That makes a whole-group outage a one-bad-entry event. Business rejections should be results, not exceptions. |

---

## 3. Topology, placement, split, merge, movement

- **Model:** `TopologyChange.Kind {SPLIT, MERGE, RELOCATE}` (core). Phases: `RatisTopologyController.Phase` has 13 values (`DEFINITION … CONFLICT`) and `RatisRelocationController.Phase` has 14 (`DEFINITION, BEGIN, JOINING, AUTHORIZE, REGISTER, TRANSFER, RELEASE, CONFIGURE, READY, PROOF, PUBLISH, COMPLETE, ACTIVE, CONFLICT`). Each phase is persisted by its own journal, and each step needs prepare and cancel query RPCs (`ScopedTransactionRpc:24-30`), FREEZE/SEED/ACTIVATE records, proofs, and source/home "closing" fences.
- **Drivers are not wired into production.** `RatisTopologyController` is constructed only in tests; the only production reference is the `DataPlane` interface type in `RatisTopologyOwnerClient:44`. `RatisRelocationController` has **0** production references. `RatisNodeMain` (the only entry point) offers no command to split, merge, relocate or replace a node. The server-side data plane (holds, records, seeds) is wired into the state machine and `RatisNodeRuntime`, so this is a large amount of live code with no production driver.
- **Replacing a dead node is impossible in production:** there is no production path to change membership. The only `setConfiguration` is in `RatisRelocationDataClient:80`, which only the test-only relocation controller uses. After permanently losing one node a group runs 2-of-3 forever, and a second loss is an outage.
- **Cluster bootstrap is impossible in production:** `RatisDatabaseBootstrap` and `RatisCatalogueGenesis` are reachable only from tests. `RatisNodeService.recover` requires an "established node root" (`RatisNodeService:66`, "Never grants placement-creation authority"). There is no production way to create a cluster.
- status.md:2432 lists as open: "Catch-up from a stale pre-relocation membership and leader-change source transfer". It also lists DAG reconciliation, quorum retirement, source quiescence and parent reclamation as "unfinished".
- **Verdict:** heavy scaffolding. Static placement is the only thing that works, and even that needs a test harness to bootstrap.

---

## 4. Failure handling

| Scenario | Code | Assessment |
|---|---|---|
| Leader loss | `notifyNotLeader:4608` bumps `admissionGeneration`, invalidates candidates and scoped snapshots, and releases admissions. Leader-local "holds" (about 25 kinds: `schemaHold`, `topologyHold`, `importApplyHold`, `relocationHold`, `sourceClosingHold` …) are lost, so their callers must redo prepare. Open transactions are leader-term-bound and are aborted with a retryable error (design doc). | Correct in intent. Every control operation is two round trips (query PREPARE, then write) with volatile leader state, and the code to reconcile those holds is a large share of `SM`. |
| Client during failover | `RetryPolicies.noRetry()` for writes (`RatisRoutedClient:1810`), 3 s request timeout (`:1807`), no write retry on `NotLeader` (`:1668-1674`, "Retrying ... an already-appended WRITE on step-down, is forbidden"). | Fear of double-apply despite having receipts. **Every failover becomes user-visible errors.** For a high-uptime goal this is the most damaging design choice in the client. |
| Partitions | Ratis handles them. ReadIndex without lease is safe. Nothing in the adapter does extra harm, but `STATE` before each write doubles leader dependence. | OK. |
| Restart | Long discovery/eligibility path. DATA refuses everything until discovery settles. Multi-node cold restart is not validated (status.md:2427). | Unproven, see §2. |
| Apply exception | Permanent poison (§2). No logging, so an operator would not even see why. | **Bug class.** |
| Frontend loss | In-flight request state lives only on that frontend (§2). | **Defeats the goal.** |
| Disk full | `RatisDiskRunway`, `RatisLogResourceBudget` and `RatisNodeResourceBudget` (about 115 KB) do extensive accounting; snapshots are triggered by runway pressure. | Over-engineered. A full-image snapshot under disk pressure needs `2*maxImageBytes` free (`SM:3345`), the most expensive time to need it. |

Concurrency hazards:

- `gate` and `admissions` are two monitors with 73 and 215 `synchronized` sites, with `gate→admissions` nesting (4 textual sites plus many via calls). I did not prove a deadlock; the lock graph is not auditable.
- Point reads hold `gate` (`SM:3375-3377`), so a long apply (schema install, import) blocks all reads.
- `RatisRocksDBDirect.status:96` returns `PENDING` whenever `persistence.tryLock()` fails, even if the request is terminal. That is racy status reporting.

---

## 5. Code quality: concrete examples

1. **God class.** `SM` is 717 KB with 132 fields and 58 nested types, and serves four unrelated roles via `role` checks (`requireRoleRecord`, `role==RatisNodePlan.Role.CATALOGUE && ...` everywhere, e.g. `SM:173-176`).
2. **Unreadable formatting.** Entire classes sit on one line, for example `SM:1402`:
   ```java
   private static final class RelocationHold extends RetainedProof {final RatisTopologyRelocationRecord record;final long term,generation,stamp;boolean reading=true,qualified,submitted,invalidated,maybeAppended,released;int owners;RelocationHold(...){super(graph);this.record=record;...}}
   ```
   Lines run up to 2,317 chars (`RatisSessionPropagation`). This defeats review, diffs and IDE structure views, and is probably why so much went unreviewed.
3. **Byte-offset protocol parsing inside policy code** (`SM:5832-5836`):
   ```java
   case RatisRelocationOwnerRpc.KIND -> bytes.size()>=ScopedTransactionRpc.HEADER_BYTES+72
       && bytes.byteAt(bytes.size()-68)==0
       && ((bytes.byteAt(bytes.size()-51)&255)==RatisRelocationOwnerRpc.Expected.SOURCE.ordinal() || ...
   ```
4. **Magic-number record sniffing instead of a typed envelope.** `startTransaction:3750-3765` and `applyRecord:4252-4317` chain about 20 checks of the form `if(RatisXxxRecord.isXxx(prefix))` (112 `isXxx(encoded|prefix|bytes)` calls in `SM`). 51 bespoke codecs and 44 `MAGIC` constants replace what one protobuf `oneof` (already a dependency via Ratis) would do.
5. **Query path used for mutations.** `ScopedTransactionRpc:22-30` defines `*_PREPARE`/`*_CANCEL` kinds sent via `sendReadOnly`. They create or destroy leader-local holds, e.g. `TOPOLOGY_FREEZE_PREPARE` → `proofQueries.submit(...)` (`SM:6100`) and `RatisRangeImportFileClient:37,63` (`sendReadOnly(...prepare/cancel...)`). Read-only RPCs with side effects break the Ratis contract and invite bugs on retry and reroute.
6. **Pointless overload ladders.** `RatisNodeRuntime.open` has 12 overloads (`:248-300`), each adding one nullable parameter. `RatisDurableSnapshot.create` has 4 more (`:58-74`).
7. **Non-retryable write client** (`RatisRoutedClient:1810`): `setRetryPolicy(RetryPolicies.noRetry())`. Then about 300 lines of outcome-query and reroute machinery (`RatisOperationRerouteCoordinator` 282 lines / 42 KB, `RatisOperationRerouteJournal`, `RatisRerouteReclamation`, `RatisOperationRerouteAdmission*`) compensate for not retrying.
8. **An extra read barrier per write** (`RatisRoutedClient:1679-1687`, `metadataWriteAfterLeaderBarrier`) is used for every async data command, not only metadata, despite its name.
9. **Single-writer slot with rejection** (`RatisRocksDBDirect:144`): `if (closed || renewing || ... || active>=MAX_TASKS || write && writing) throw RocksDBException.of(SERVER_OVERLOADED,...)`.
10. **Fsync storm in the client** (`RatisDirectRequestJournal:44-58`): `begin`, `bind`, `result`, `acknowledged`, `rejectUnsubmitted` and `retire` each do a full atomic rewrite with file and directory fsync, and `RatisDurableSessionClient.checkpoint:192` rewrites the whole window on prepare, result and ack.
11. **Absurd domain vocabulary**, which hides simple concepts: "source result debt" (commit `9b2d16d`), `RatisDiskRunway.Debt` (`:33`), `RatisRequestBudget` "retained path debt" (`:78`), `requireRecoveryRegistrationDrained` (17 uses), `metadataRecoveryRegistrationForced`, `validatePendingCurrentRangeRecoveryConfiguration`, `RatisSnapshotHomeRetirementRecord`, `RatisTopologyLearnerRecoveryRpc`, `RatisHomeSourceClosingRecord`, `NativeTopologyLearnerImagePermission`. Doc comments are negations rather than descriptions: "Never consumes.", "grants no authority …", "does not prove …" (e.g. `RatisTransactionStatuses`, `RatisSnapshotFences`, `RatisSourceReferenceClosure`).
12. **Copy-paste FQNs despite imports.** `new it.cavallium.rockserver.core.common.distributed.CommandResult(it.cavallium.rockserver.core.common.distributed.CommandResult.Status.SUCCESS,List.of()).encode()` appears 8 times in `SM`, while `CommandResult` is imported (`SM:58`). There are 429 fully qualified `it.cavallium.rockserver.core.*` names across the package, and `OperatorUpload` is imported twice (`SM:10,88`).
13. **Dead catch clauses:** `catch (IOException failure) { throw failure; }` at `RatisRocksDBConnection:61,377` and `RatisTransactionCoordinator:106`. Swallowed exceptions: `catch(IllegalArgumentException different){}` at `RatisConfiguredRecovery:152-153`, `RatisDatabaseBackupDriver:84`.
14. **Exceptions for control flow in apply** poison the group permanently (`SM:4277`, `:4429`; §2).
15. **Fourteen bespoke exception types** (`RatisSnapshotHomeRetiredException`, `RatisParticipantRetiredException`, `RatisStagedResultTooLargeException` …), mostly 8-12 lines each.
16. **No observability:** zero log statements across 4.6 MB of distributed-systems code. Leader changes, poison events, snapshot failures and recovery decisions are all silent.
17. **Hard dependency on a forked Ratis.** Six custom server interfaces (`SM:100`) plus `org.apache.ratis.server.impl.CommittedApplyStateMachine.Retry/Stopped` in the apply hot path (`SM:4429`). Upgrading Ratis is effectively impossible.
18. **Magic constants everywhere**, e.g. `RatisNodeRuntime:61`: `new RatisNodeResourceBudget.Limits(new RatisNodeResourceBudget.Lane(128L*1024*1024,512),new RatisNodeResourceBudget.Lane(512L<<20,128)),1L<<30,1L<<40,16L<<30,16L<<30,...`.

---

## 6. Complexity vs value

What "3-node replicated Rockserver with automatic failover" actually needs:

1. One Raft group (or a few fixed ones), with a state machine applying `put/delete/merge/batch/transaction-commit` commands deterministically to RocksDB and storing `lastApplied` in the same batch.
2. Linearizable reads through ReadIndex (or lease) on the leader. Ratis already provides this.
3. Snapshots via a RocksDB checkpoint (hard links) plus Ratis file-chunk install.
4. Client dedup with Ratis clientId/callId retry cache, plus a session/sequence receipt in RocksDB for cross-restart idempotency. Normal client retry on `NotLeader`.
5. Node bootstrap from a static peer list, add/remove peer via `setConfiguration`, and a CLI/admin RPC for it.
6. Transactions: either forward them to the leader and execute them inside the state machine (single group means no 2PC), or keep optimistic transactions leader-bound as designed.
7. Unchanged public gRPC API: the frontend forwards to the leader.

A competent implementation is **3-6k normal lines** (about 150-250 KB). The current package is 4.6 MB, which **puts the essential fraction at about 5%**. Generously crediting the parts of `SM`, `RatisNodeRuntime`, `RatisRoutedClient`, `RatisRocksDBAdapter`, `RatisRocksDBIterators`, `RatisRocksDBRanges`, `RatisClientPool`, `RatisDurability` and `RatisNodePlan` that implement items 1-4 gives at most **10-15%**. The other **85-95%** is speculative or self-inflicted:

- multi-range, multi-domain routing, split/merge/relocate/import, configured recovery DAGs, source/home closing and retirement (about 21% plus most of `SM`);
- cross-group 2PC with dedicated HOME groups, snapshot cohorts and fences across groups (about 15%);
- distributed schema jobs with fence/install/activate phases (about 6%);
- aggregate export, backup, restore and CDC lineage (about 10%);
- frontend-side durable request journals and the reroute machinery (about 6%), which exists because writes are not retried;
- byte-level resource budgets and disk runway (about 2%).

Meanwhile, items 5 (bootstrap and replace node) and 7 (plain API) are **missing**: the pieces needed for the actual goal are absent, and the speculative pieces are present.

---

## 7. Verdict per component

| Component | Verdict | Reason |
|---|---|---|
| `RatisDurability` | **KEEP** | Explicit, correct safety settings (sync log, LINEARIZABLE ReadIndex, no lease). |
| `RatisRangeStateMachine` | **REWRITE** | 717 KB god class, four roles, fatal-poison apply, monitor-serialized reads, depends on forked Ratis. Replace with a role-free, about 1k-line SM over a typed command envelope. |
| `RatisNodeRuntime` / `RatisNodePlan` / `RatisNodeManifest` / `RatisNodeService` / `RatisNodeMain` | **REWRITE** | No production bootstrap or membership change. 43 KB `initialize()`, 12 `open` overloads. Needs a simple static-peer bootstrap plus admin add/remove peer. |
| `RatisRoutedClient` / `RatisClientPool` | **REWRITE** (keep transport ideas) | `noRetry` writes and a `STATE` barrier before every write. Use the standard Ratis client with a retry policy and the retry cache. |
| `RatisRocksDBAdapter` / `RatisRocksDBConnection` | **REWRITE** | Rejects plain `put`, `PutBatch` and `DeleteRange`. Must implement the existing API surface transparently. |
| `RatisRocksDBIterators` / `RatisRocksDBRanges` / `RatisLogicalRangePages` | **KEEP, simplify** | Range paging over ReadIndex is needed. Strip the generation and owner coupling. |
| `RatisRocksDBDirect`, `RatisPointOwnerManager`, `RatisDirectRequestJournal`, `RatisRemoteMutationReceipts`, `SessionWindowJournal`, `RatisDurableSessionClient`, `RatisDurableLogicalSession`, `RatisSessionClient`, `RatisSessionPropagation`, `RatisSessionMaintenance`, `RatisSessionEpochs`, `RatisGrantIssuer`, `RatisBatchRequestJournal`, `RatisRequestBudget` | **DELETE** | Frontend-local fsync journals (about 15 fsyncs per write) and a 2-writer cap. State is lost with the frontend node, so this does not serve the goal. Use the Ratis retry cache plus a server-side session receipt. |
| `RatisOperationReroute*`, `RatisRerouteReclamation` | **DELETE** | Compensates for not retrying writes. |
| `RatisTopology*`, `RatisRelocation*`, `RatisMigrationController`, `RatisRangeImport*`, `RatisImportPublicationRpc`, `RatisConfiguredRecovery*`, `RatisRecovery*`, `RatisSource*`, `RatisHomeSource*`, `RatisRetirement*`, `RatisReferencedFile(s)`, `RatisLocalNoneDiscovery`, `RatisNativeParticipantDiscovery`, `RatisGenerationRegistry`, `SourceReadRate` | **DELETE** | Split/merge/relocate drivers are test-only, and the status doc lists them as unfinished. Replace with `setConfiguration`-based replica replacement. |
| `RatisDurableTransaction`, `RatisTransaction*`, `RatisParticipant*`, `RatisDurableParticipantRecovery`, `RatisHome*`, `RatisAtomic*`, TRANSACTION_HOME role | **DELETE** (keep single-group transactions) | Cross-group 2PC is unnecessary with one data group. Execute transactions inside one SM. |
| `RatisScopedTransactions` | **REWRITE** | Leader-bound handles are fine. Simplify to leader-forwarded transactions. |
| `RatisSnapshot*`, `RatisDurableSnapshot`, `RatisLogicalSnapshotPages`, SNAPSHOT_HOME role | **DELETE** | Cross-group snapshot fences are unnecessary. A RocksDB snapshot on the leader inside one group is enough. |
| Raft snapshot (`takeSnapshot`/`reinitialize` + `NativeRangeImage`) | **REWRITE** | Use a RocksDB checkpoint with hard links and Ratis file install, not a full logical dump with MD5. |
| `RatisSchema*`, `RatisColumnAllocation`, `RatisColumnIdentities`, `RatisOperatorCatalogue` | **REWRITE (small)** | Column create/delete can be one replicated command applied in order. The fence/install/activate jobs are unnecessary. |
| `RatisCatalogue*`, `RatisTopologyCatalogue`, `RatisDatabaseDomainRoster`, CATALOGUE role | **DELETE** for v1 | With a static single group there is no directory to replicate separately. |
| `RatisExport*`, `RatisAggregateExport*`, `RatisDatabaseBackup*`, `RatisDatabaseRestore*`, `RatisDatabaseCapture*`, `RatisRemoteExportSource`, `RatisBackend*` | **DELETE** for v1 | Backup can be a leader-side RocksDB checkpoint. Restore can be "bootstrap from checkpoint". |
| `RatisCdc*` | **DELETE** | CDC was removed from Rockserver altogether. |
| `RatisNodeResourceBudget`, `RatisLogResourceBudget`, `RatisDiskRunway*`, `RatisPoolPressureService`, `RatisMetadataClaims`, `RatisCommittedApplyDispatcher`, `RatisProofQueryService` | **DELETE** | Byte-level accounting that couples to forked Ratis internals. Use Ratis queue limits and an RocksDB write-stall/disk alarm. |
| `ScopedTransactionRpc` and the other 50 `*Rpc`/`*Record` codecs | **REWRITE** | Replace with one protobuf envelope. |
| `RatisStreamingTransport` | **KEEP if needed** | Only useful for large snapshot transfer. |

**Overall: restart from scratch, using `RatisDurability` and the range-page logic as salvage.** The current code is not a foundation that can be trimmed down. Its correctness-critical paths (apply, write retry, frontend ownership) are designed against the goal, and the parts the goal needs (bootstrap, replace a node, transparent API, logging) are missing.
