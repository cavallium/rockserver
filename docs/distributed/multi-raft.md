# Multi-Raft for replicated Rockserver: design (read-only study)

Status: **decided, not built.** Follower reads and the leader hint, the leaner apply path (`EmbeddedDB.ApplyWriter`),
client sessions, snapshots by log bytes (`--snapshot-every-bytes`) and partitioned apply inside one group
(`--parallel-apply-threads`) have all landed (section 6 marks them).
Partitioned apply spreads the CPU of the single apply thread over lanes split by first key part, including inside
`messages-v2`, so it lifts the ceiling that Multi-Raft by column would lift (at most 1.75x, section 0), without a
second fsync stream, a second election, per-group membership, snapshots and monitoring. Multi-Raft is therefore
not built. The gate to reopen it is measured, not argued: run `LoadBench` at 4x production with
`--parallel-apply-threads 4` on every node (performance.md item 19); build phase 1 (section 5) only if the apply
thread, or the single group commit it keeps, is still above 80% busy there, or if fsync latency of the one Raft log
is the limit. Measure the idle cost per group (threads, RSS, CPU, file handles at 1/2/8 groups) first.

Abbreviations:

- `R/` is `/home/cavallium/IdeaProjects/rockserver/.claude/worktrees/distributed-rockserver-rewrite-e46da4/`.
- `D/` is `R/distributed/server/src/main/java/it/cavallium/rockserver/distributed/`.
- `Y/` is `/home/cavallium/IdeaProjects/Yotsuba-private/server/source/core/src/main/java/org/yotsuba/core/`, at
  Yotsuba commit `217cb0b55`.

## 0. Verdict first

**Shard key: per column.** Columns are assigned statically to Raft groups, and the columns that share a transaction
go to the same group. With that assignment Yotsuba makes **0 cross-group operations**: no transaction, update or
multi-key call spans two groups. Key-range sharding would create 2 cross-group transaction sites plus every
multi-key call that crosses a boundary. Hash sharding would scatter nearly every multi-key call and every range.

**Not worth it yet.** Column sharding has a hard limit. `messages-v2` is one column and carries about 57% of the
replicated write requests (5.6k of about 9.9k per second). Two or three groups therefore raise the apply-bound
ceiling by about 1.75x at best (1/0.57), before the extra fsync streams and leader CPU are paid for. The cluster
already serves 2x production and saturates at about 3.5x (37.4k to 38.6k ops/s at 4x offered load, 2048 writes in
flight). The first limit with Yotsuba's real 256 to 512 writes in flight is in-flight divided by commit latency,
and Multi-Raft does not change that. The cheaper items come first: the leaner apply, follower reads, a lighter
`ConflictTracker`, raising client in-flight limits, and partitioned apply inside one group (section 6).
Multi-Raft is the right step only if, after those, the measured target load is above about 4x production with the
apply thread still above 80% busy. If it is ever built, use the plan in section 5 (2 groups, `messages-v2` alone).

## 1. What Yotsuba does with Rockserver (evidence)

### 1.1 Columns

| Column | Created at | Writes | Reads |
|---|---|---|---|
| `messages-v2` (key: chat 8 B, message 4 B) | `Y/database/data/MessagesImpl.java:354` | single-key update `get(forUpdate)` then `put(updateId)` or `delete(updateId)` or `closeFailedUpdate`, `:521-549` | `get` `:932,949,993,1052`; `getMulti` of a search page, keys from many chats, `:986`; `existsMulti` of sampled keys `:2428,2455`; `reduceRange firstAndLast` per chat `:1112,2489`; whole-column `entriesCount` or `estimateNumKeys` `:1581-1587`; `getRange` streams `:2968` |
| `poll-index` | `MessagesImpl.java:360` | autocommit `put` `:2854` | range `:2930` |
| `messages-by-sender-blocks-v1` | `Y/database/data/SentMessageSenderIndex.java:49,117` | autocommit `put(ensure)` `:228-233` | `getRange` `:274,323` |
| `history-archive-chunks` | `Y/database/data/HistoryArchive.java:55` | autocommit `putMulti` `:95`; `deleteRange` of an owner prefix `:160` | `reduceRange` `:125`, `estimateNumKeys` `:100` |
| `file_locations_8`, `file_locations_migration_state`, legacy `file_locations_5` | `Y/database/data/FileLocationsImpl.java:73-75,134,141,496` | autocommit `put` and `putMulti` `:198,245-258,315-330,604,628`; `delete` `:643`; `deleteColumn` of the old column `:524` | `get` `:537`; `reduceRange` or `estimateNumKeys` `:454-460` |
| `monitors-v1`, `monitor-log-v1`, `monitor-seen-v1` | `Y/monitors/MonitorStore.java:91-93` | `put` and `delete` `:116,120`; `deleteRange` `:125,128,210`; `deleteMulti` `:208`; **transaction over log and seen** `:168-173` | `existsMulti` `:158`; `getRangePage` `:216`; `reduceRange` `:139` |
| `invite-evidence-links-v2`, `-sources-v2`, `-fields-v1`, `-members-v2` | `Y/database/data/invites/InviteEvidenceStore.java:86-91` | **one transaction per batch over all four columns and up to 256 links** `:95-110,475-485`, with `getForUpdate` `:467`, `put` `:472`, `delete` `:181`, `putMulti` `:239` | `get`, range pages `:262-275` |
| `stories`, `polls`, `stickers`, `sticker-sets`, `communities`, `group-calls`, `forum-topics`, `direct-message-topics` | `StoriesImpl.java:85`, `PollsImpl.java:80`, `StickersImpl.java:73`, `StickerSetsImpl.java:75`, `CommunitiesImpl.java:80`, `GroupCallsImpl.java:78`, `ForumTopicsImpl.java:88,100` | single-key update through `RockserverUpdateTransactions.usingForUpdate` (`StoriesImpl.java:140-150`, `PollsImpl.java:135-145`, `StickersImpl.java:103-113`, `StickerSetsImpl.java:134-144`, `CommunitiesImpl.java:143-162`, `GroupCallsImpl.java:137-147`, `ForumTopicsImpl.java:179-192,265-278`); `delete` `StoriesImpl.java:201` | `get`, `getRange`, `reduceRange`, `estimateNumKeys` |
| `chat_photo_recovery_v1` | `Y/database/data/PhotoRecoveryArchive.java:32,41` | `putMulti` `:68`; `deleteRange` of an owner `:81` | |
| `association_observations_v1` | `Y/database/data/AssociationObservationArchive.java:54` | `put` `:64` | `estimateNumKeys` `:70` |

No call site uses `merge*`, `mergeBatch`, `putBatch` or `uploadMergeOperator`; the earlier study found the same
(`R/docs/distributed/consumer-contracts.md:47`). `scanRaw` and `MessagesV2Migration` are no longer in the tree
(`git ls-files` finds no `MessagesV2Migration`). The monitor plugin storage that consumer-contracts.md cites
(`RockserverMonitorStorage`) is gone too: `Y/monitors/MonitorStore.java` replaced it.

### 1.2 Atomicity that spans more than one column or key range

There are exactly two sites, both explicit transactions:

1. **`MonitorStore.append`** (`Y/monitors/MonitorStore.java:161-185`): `putMulti` on `monitor-log-v1` (key `(monitor,
   seq)`) and `putMulti` on `monitor-seen-v1` (key `(monitor, chat, message)`), then commit. Two columns, one
   monitor prefix per batch.
2. **`InviteEvidenceStore.record`** (`Y/database/data/invites/InviteEvidenceStore.java:95-110`, `transaction` at
   `:475-485`): one transaction for a whole sink batch (`InviteEvidenceSink.java:76-77`, `bufferTimeout(batchSize)`,
   at most 256 observations). Each observation does `getForUpdate` on `links`, reads and writes on `fields` and
   `sources`, and `putMulti` on `members`. That is four columns and many different link hashes, so many key ranges
   per column.

Everything else is single-column:

- Every `getForUpdate` update (messages and the eight entity columns) reads and writes one key of one column
  (`Y/database/data/RockserverUpdateTransactions.java:48-58`).
- The message write path touches other columns only through separate autocommit calls:
  - sender presence, `put(ensure)` with transaction 0, is issued inside the update window but is not part of the
    update (`MessagesImpl.java:549,704-708`, `SentMessageSenderIndex.java:228-233`);
  - the history spill and the invite evidence run after the commit (`MessagesImpl.java:507-508`).

  consumer-contracts.md:25 records that those derived writes are deliberately weaker.
- FileLocations' v5 to v8 migration copies pages from the old column into the new one and persists a cursor
  (`FileLocationsImpl.java:494-530`). Each step is autocommit, with no transaction across columns.
- The Rockserver API itself is single-column for every call except transactions: `openTransaction` names no column
  (`R/src/main/java/it/cavallium/rockserver/core/common/RocksDBAPICommand.java:213`); `closeTransaction` and
  `closeFailedUpdate` name only an id (`:242,265`).

### 1.3 Production rates per column

From `R/docs/distributed/performance.md:11-35`. Mean rates per second:

| Op | Rate | Column |
|---|---|---|
| update commits (`closeTransaction`) | 5.6k | almost all `messages-v2` (message RMW 5.9k/s, about 5% no-change) |
| `put` | 7.8k | 5.6k are the update puts above (counted again); about 2.2k `put(ensure)` on `messages-by-sender-blocks-v1` |
| `putMulti` | 2.1k (p99 8.2k) | about 1.8k `history-archive-chunks`, the rest file locations, monitors, photos |
| `get` | 6.1k | mostly `messages-v2` |
| `existsMulti` (256 keys) | 0.14k | file locations, monitors, messages density |
| explicit `openTransaction` | 0.03k (peak 0.2k) | monitors and invites |
| `getRange` | 2 | various |

Replicated write requests come to about 9.9k/s. `messages-v2` has about 5.6k of them (57%), sender index 2.2k (22%),
history 1.8k (18%), everything else about 0.3k (3%). Counted in bytes, messages (0.5-2 KiB values) and history
chunks (1-3 KiB) make up nearly all of it. **Before deciding anything, measure apply time per column** (see Phase 0);
these shares are derived from operation counts.

## 2. Question 1: the shard key

| Choice | Cross-group transactions | Other cross-group calls | Ceiling gained | Cost |
|---|---|---|---|---|
| **Per column**, transaction partners co-located | **0** | **0** (every non-transaction call names one column) | about 1.75x on apply, bounded by `messages-v2` | low: a static map, one DB per group |
| Key range per column (first key part: chat, monitor, link hash, owner) | 2 sites: invites (nearly every batch, since one batch spans many links), monitors (0 if `monitor-log-v1` and `monitor-seen-v1` split on the same monitor-id boundaries); about 30/s mean, 200/s peak | `putMulti` of file locations (hashed URL keys, so spread); `existsMulti` of 256 keys (0.14k/s); the `getMulti` message page across chats (`MessagesImpl.java:986`); whole-column counts and scans; invite summary pages; FileLocations migration scans; ranges that cross a boundary | unbounded in principle | range table, splits, scatter-gather reads, non-atomic `putMulti`, cross-group transaction protocol |
| Hash of the key | invites and monitors on every batch | every multi-key call and every range: `reduceRange firstAndLast` (chat extremes) and `getRangePage` with `resumeAfter` both need ordered merges across shards | unbounded | everything above, plus ordered scatter-gather for every range read |

**Recommendation: per column**, in this static assignment:

- **G0** (the default group, which also holds every column the map does not name, and is the existing group id):
  everything except `messages-v2`, which includes the four invite columns and the three monitor columns. Both
  transaction sites therefore stay inside G0.
- **G1**: `messages-v2` alone.
- Optional **G2**: `history-archive-chunks`, or `history-archive-chunks` plus `messages-by-sender-blocks-v1`. That
  would leave G0 with the sender index or the small columns. It gains little, because G1 stays the bottleneck.

Constraint to enforce: the map takes **co-location sets**, not single columns. `{monitor-log-v1, monitor-seen-v1}`
and `{invite-evidence-*}` must share a group. The server refuses a configuration that splits a set (section 4.2), and
a transaction that touches two groups is refused at commit anyway (section 4.1).

Key-range sharding is worth revisiting only for `messages-v2` (Phase 3), where every write is a single-key update.
Splitting it by chat id creates zero cross-group writes. What it costs is scatter-gather for the `getMulti` page,
`existsMulti`, and whole-column counts.

## 3. Question 2: mechanics

### 3.1 Ratis: one `RaftServer`, several divisions

Ratis 3.3.1 supports this with public APIs. The jar in `~/.m2` has `RaftServer$Builder.setStateMachineRegistry`,
`RaftServer.getGroupIds()`, `getGroups()` and `getDivision(RaftGroupId)`. `RaftServerProxy` keeps an `ImplMap` of
divisions.

- **Shared ports.** One gRPC Raft port serves every division, and requests are dispatched by group id. `TermService`
  already takes a group id on the wire (`D/TermService.java:117,146,208-233`), so it needs no protocol change. The
  API port (GrpcServer) also stays one per node.
- **Per-group state machines.** Replace `.setStateMachine(stateMachine)` (`D/ReplicatedNode.java:120-128`) with
  `.setStateMachineRegistry(gid -> machines.get(gid))`. Each `RockserverStateMachine` gets its own `NodeStore`
  (its own DB, see 3.3), `Applier`, apply thread, completer thread, `AppliedIndex`, `LeaderSession` and
  `ConflictTracker`.
- **Per-group logs and snapshots.** Ratis lays them out as `raft/<group-uuid>/current/` and `raft/<group-uuid>/sm/`
  under the one storage dir set in `D/RaftSettings.java:51`. On start, `RaftServerProxy` recovers every group
  directory it finds in storage. `ReplicatedNode` adds the groups missing at the first start (and at `--join`)
  through `server.groupManagementAsync(GroupManagementRequest.newAdd(...))`, in process.
  - **Spike needed:** check that adding an empty group (`RaftGroup.valueOf(gid)` with no peers) gives Ratis'
    initializing state, as `setGroup` does today for `--join` (`D/ReplicatedNode.java:124`).
  - The `existing` check at `:110` and the `raft-meta.bootstrap` logic (`:111-119`) become per group.
  - `RaftLogRepair.run(dataDir, groupId, …)` (`D/ReplicatedServerMain.java:325-328`) loops over the group dirs.
- **Properties are per server.** `RaftSettings.server` is shared by every division, and every limit in it applies
  per division. Divide the byte budgets by N, or the worst case multiplies:
  - pending writes, `Write.setByteLimit` 64 x 16 MiB = 1 GiB per division (`:82`);
  - log queue (`:72`), retry cache (`:88`), apply backlog 256 MiB per state machine.
- **One log worker per division.** Each division has its own `SegmentedRaftLogWorker` and fsyncs its own segment, so
  N groups mean N fsync streams on the same disk. Batching amortizes less: at production load each group's batches
  are smaller, and on the consumer SSDs (1-3 ms per fdatasync idle, 10-30 ms during compaction, performance.md
  "Deployment notes") that can raise commit latency. **This is the main hidden cost; measure it**
  (`LoadBench --groups 1,2,3`).
- **Leader spreading.**
  - Give each group's `RaftPeer`s a priority (`RaftPeer.newBuilder().setPriority`) so that group i prefers node
    (i mod 3). Ratis' leader then yields to a caught-up higher-priority peer by itself.
  - Belt and braces: a `LeaderPlacement` task in the maintenance loop. When a node leads more groups than its share
    for longer than a few election timeouts, it calls `transferLeadershipAsync` (already used at
    `D/ReplicatedNode.java:388-410,591-600`) towards the preferred peer, at most one transfer per group per
    minute, so a flapping node does not churn.
  - Each transfer costs that group about 200-300 ms of writes (RollingRestartTest).
  - Graceful stop (`:546-600`) hands over every group the node leads, one after another (or in parallel), and waits
    for a commit by each new leader.
- **Membership per group.**
  - `admin add-peer` and `remove-peer` (`D/ReplicatedServerMain.java:158-200`) run `setConfiguration` on each group
    in turn, and print OK only when every group has the target configuration.
  - The state in between, with some groups changed and others not, is safe: every group is an independent Raft
    group with a majority in both halves. The command is idempotent, so running it again completes the rest.
  - The one-voter guard of round 14 (`VoterChange`, `MembershipGuard`) applies per group: `admin` reads each group's
    leader configuration, refuses before the first group if any group would change more than one voter (a group that a run
    interrupted earlier already changed counts zero), and sends each group a compare-and-set. The leader's guard checks each group's request on its own.
  - `requireFitToJoin` asks `nodeState` for every group.
  - `--join` gives every group an empty configuration.
  - A node that lost one group's Raft state but kept the others must still be replaced under a new id, as it is today.
- **Snapshots per group.** Each state machine takes and installs its own checkpoint (3.3).
  - Auto-trigger counts per division (`RaftSettings.java:112-113`), so every group snapshots on its own schedule,
    and the pause of 100-300 ms stops only that group's apply.
  - Retention, purge gap and the heap rule for sending snapshots (design.md "Heap") apply per group. Two groups
    sending a snapshot at once add up.

### 3.2 How the safety machinery generalizes

| Mechanism | Today | With N groups |
|---|---|---|
| `ReadBarrier` (`D/ReadBarrier.java`) | one per node, rounds of term queries | One per group. A later optimization batches the queries of all groups to the same peer into one RPC, a `TermService` request with several group ids. Follower reads (arch/routing) work per group: a follower asks the group leader for its read index and waits for its own applied index of that group. |
| `TermService` | `currentTerm(gid)`, `nodeState(gid, cluster)` | Unchanged on the wire. `NodeState` answers per group. |
| `ApiGate` (`D/ApiGate.java`) | pause the API when no leader is known for 3 max election timeouts | Pause only when **every** group has been leaderless past the bound: that is what an isolated node looks like. If only some groups lack a leader, as in a stuck election, calls for those groups fail retryably, like during an election. Pausing the whole node for them would only move clients to another node that has the same problem. Resume when at least one group has had a leader for one max election timeout, and then all groups do within the usual bound. |
| `LeaderStall` (`D/LeaderStall.java`) | two rules on the one division | One instance per division, the same rules. Its step-down targets are per group. The bound-doubling state is per group. |
| `ClusterCheck` (`D/ClusterCheck.java:31-65`) | the leader tells followers its cluster id once per term | One per group (it already takes a group id). A follower stops (exit 3) if **any** group reports a foreign cluster. |
| Genesis (`LogEntry.Genesis(clusterId, seeded)`, `D/LogEntry.java:176`) | first leader proposes, random cluster id | `Genesis(clusterId, groupIndex, seeded, assignmentHash, g0ClusterId)`. G0's genesis creates the cluster id. A group k>0's first leader proposes its genesis only after G0's genesis has been applied locally, and copies G0's cluster id, so two clusters' groups can never be mixed. `assignmentHash` is the hash of the column-to-group map: a replica whose own configuration hashes differently stops (exit 3), the same rule as "same config file on every node". |
| Column-id verification (`VerifyColumn`, `D/LogEntry.java:182`, design.md "Column ids") | the leader proposes `VerifyColumn(name, nativeId)` after `createColumn` | The same inside the owning group, with the group-local native id. The client-visible id is the encoded id of 3.4. |
| `LeaderForwarder` | one leader | A leader per group. It finds it from `server.getDivision(gid).getInfo()`, and the 20 ms polling (review 3.8) is replaced by `notifyLeaderChanged` per state machine. |
| Proposer, exactly-once floors | one `Proposer` per node, random proposer id per process | One `Proposer` per group: its own id, sequences, floor and batching (see section 4.3 for client sessions). |
| `NodeMetrics` | node-wide | A `group` tag on every replication timer and gauge. |

### 3.3 RocksDB: one DB per group (recommended) or column families per group in one DB

**One DB per group (recommended).** The layout is `data/db/` for G0, which is the existing database, and
`data/db-g1/`, `data/db-g2/` for the others. Each is an `EmbeddedDB` with its own internal `__rockserver_replication`
column (`D/NodeStore.java:75-85`), atomic flush, applied position, floors, records and checkpoints. Every argument of
the current design holds per DB without change:

- **Atomic-flush prefix per group.** A crash leaves each DB at a consistent prefix of its own group's log, because
  its data and its `APPLIED` row are flushed together. Groups need not agree with each other, since no entry spans
  them.
- **Snapshots and installs per group.** `CheckpointStorage` checkpoints one DB and `reinitialize` restores one DB,
  both as today. Installing G1's snapshot never touches G0.
- **Isolated stalls.** `deleteRange` and schema changes flush all memtables and sync the WAL (`D/Applier.java`;
  review 3.3). In a shared DB those stall every group; per DB they stall one.
- **Disk-layout checks.** The checks extend from `db/` to `db*/`. Everything must still be on one file system with
  `raft/`, for hard links.

Costs, and what to do about them:

- **Memory and threads.** Separate block caches, memtable budgets and background threads. RocksJava can share a
  `Cache`, a `WriteBufferManager` and the default `Env` thread pools between DBs. The core change is that
  `EmbeddedDB` accepts injected shared `Cache`/`WriteBufferManager` objects; config parsing stays as is. Without
  it, split the configured budgets by N.
- **Column ids collide across DBs.** That is handled by the encoding in 3.4.
- **Merge operators are registered per DB.** `uploadMergeOperator` is therefore proposed to every group (each
  upload is atomic and idempotent, `MergeOperatorRegistry.upload`, design.md "Shape", "What this module needs from
  core"). Yotsuba uses none.

**Column families per group in one DB (rejected).**

- Each group would need its own meta column for its applied position. That part works: atomic flush across all column
  families still keeps each group a consistent prefix, because every apply commit is one atomic write.
- The blocker is the per-group snapshot. A checkpoint is DB-wide and holds other groups at other positions. Installing
  one group would mean `dropColumnFamily` plus `createColumnFamilyWithImport` of that group's column families.
- That gives **new native column-family ids on the installing follower**, which breaks the "column id = native CF id"
  invariant (design.md "Column ids", in "Writes, exactly once"). The fix would be a replicated logical-id table and
  remapping in every apply.
- Every schema-change or `deleteRange` flush, and every snapshot flush, would also pause all groups.
- Atomic flush across even more column families makes review 3.5 (tiny L0 files in cold columns) worse, not better.

### 3.4 Client-visible ids and routing

- **Column id:** `clientId = (group << 40) | nativeId`, with G0 being the identity.
  - G0's existing columns keep the ids they have today.
  - Columns moved by the seed split (5.2) get new ids in G1 and later groups. Yotsuba resolves ids with
    `createColumn` at every start and caches them only per process (all the `createColumnAsync` sites in 1.1), so a
    restart picks them up.
  - The one persisted id is in invite page cursors (`InviteEvidenceStore.java:423-434` writes the column id into the
    cursor). Since the invite columns stay in G0, those cursors stay valid.
  - `ReplicatedConnection` decodes the group from every command's `columnId()` (`D/ReplicatedConnection.java:578-598`
    already extracts it) and strips it before calling the group's `EmbeddedDB`.
- **Name-addressed commands** (`getColumnId`, `createColumn`, `deleteColumnIfExists`) go to the group that the
  static map gives for the name. `getAllColumnDefinitions` asks every group and merges the answers, with no
  cross-group snapshot, which nobody needs.
- **Update ids.** The group goes into the high bits:
  - native ids are at most -2, and chained ids are `(term & 0x7FFFFFFF) << 32 | counter`
    (`D/LeaderSession.java:616`), so reserve 4 group bits below the sign bit in both forms;
  - `closeFailedUpdate(updateId)` then routes by bits;
  - iterator ids are encoded the same way.
- **Transactions.** `openTransaction` has no column, so the transaction is **bound lazily**:
  - The receiving node returns a token from a distinct id space, with a flag bit, and opens nothing yet.
  - The first call that names a column goes to that group's leader with "bind token". The leader opens the native
    transaction then, with its snapshot taken at bind time. That is legal for an optimistic transaction, and both
    Yotsuba sites call nothing between open and first use (`MonitorStore.java:168-171`,
    `InviteEvidenceStore.java:106,465-467`).
  - `closeTransaction(token, commit)` names no group. The receiving node asks the leaders of all groups whether they
    hold the token: N RPCs, at 30/s mean.
    - Exactly one holds it: that group commits as today.
    - More than one holds it (a cross-group transaction): every group rolls it back, and the call fails with
      `INVALID_REQUEST`, "transaction spans groups". It fails before anything is appended, so it is deterministic.
    - None holds it: `TRANSACTION_NOT_FOUND`, as after a leader change.
- **Leader hint per group.** Extend the arch/routing trailer to a group-to-leader map. The core client keeps one
  subchannel per node and picks one by the group bits of the column id or the update id. Without the hint, about
  (N-1)/N of the leader-bound traffic takes a forwarding hop, because leaders are spread on purpose.

## 4. Questions 3 and 4: cross-group operations, schema, sessions

### 4.1 Cross-group operations

With the co-located column map, Yotsuba issues none. Options for a future consumer:

| Option | What it takes | Verdict |
|---|---|---|
| **Reject** (detect at commit, 3.4) | already designed above | **Recommended.** The fail-closed default. |
| 2PC with parallel commits (CockroachDB): a transaction record in a coordinator group, write intents in each participant group, intent resolution, recovery of abandoned records | intents visible to reads and conflict checks in every group, a status-record protocol, recovery after leader loss, and the `ConflictTracker` extended to intents | Months of work, and a new correctness surface (the Jepsen class of bugs). Not justified at 30 tx/s. |
| Deterministic ordering (Calvin): a sequencer log orders multi-group transactions, and every group applies them at an agreed slot | a global sequencer, which is a single group again; read and write sets known up front (Yotsuba's invite transaction discovers its keys as it reads) | It does not fit interactive optimistic transactions. |
| Co-location by configuration | the map is the tool | Use it. A new transaction that spans columns means putting those columns in one set. |

### 4.2 Schema operations and column ids

- **The assignment is static in Phase 1.** A `replication.groups` list in the Rockserver config, the same file on
  every node: `[{columns: ["messages-v2"]}, …]`, with unnamed columns in G0. It can also name co-location sets.
  - The node checks at start that no set is split, and records `assignmentHash` in every genesis (3.2).
  - Changing the map for an existing column is a migration (5.2), not a config edit. Nodes refuse to start if the
    hash differs from their genesis.
  - Adding a name that does not exist yet is allowed, and also changes the hash. So record the hash of the
    *existing* columns' assignment instead, re-checked when the owning group applies `createColumn`. The simpler
    alternative is to require that all new columns go to G0 in Phase 1.
- **Commands:**
  - `createColumn(name)` is proposed to the group the map gives for the name. That group creates it and proposes
    `VerifyColumn` (design.md "Column ids"), and the encoded id is returned.
  - `deleteColumn(id)` routes by the id bits. `deleteColumnIfExists(name)` routes by the map.
  - A name exists in at most one group, by construction, since the map is a function.
  - `uploadMergeOperator` goes to every group in turn. A `createColumn` that names an operator checks it in the
    target group only.
- **Later (Phase 3, dynamic placement):** a column directory replicated in G0 (name to group). A create then becomes
  two idempotent steps: reserve the name in G0, then create in the target group, with a retry finishing a
  half-done create. Not needed for a static map.

### 4.3 Exactly-once and client sessions

The session work in progress registers sessions through the log and keys records by session.

**Recommendation: keep sessions per group, with ids global and registration lazy.**

- The client and session id comes from the client (or is issued once by any node). It is not tied to a group.
- A group registers a session the first time one of its requests reaches that group's log. The registration is an
  entry in that group's log, and expiry is decided on that group's log time, deterministically.
- Records are `(session, seq)` in the group's own meta column (one DB per group), written in the same apply
  transaction as the effects, exactly as today.
- Each group sees a sparse subsequence of a session's sequence numbers. That is fine:
  - the dedupe check is set membership on `(session, seq)`;
  - the client's `completedBelow` floor is global and monotonic ("every request below n has a final answer"), so it
    is valid in every group, and each group deletes its records below it.
  - A group that never sees a session's requests never registers it.
- **Global sessions in G0 (rejected):** every first write of a session to another group would wait on a G0 entry,
  and every expiry would have to be propagated. That is the cross-group coordination this design avoids.
- **Internal proposers** (`Proposer` ids per process) become one per group, as in 3.2.
- **Constraint for the sessions work now:** do not make the session table a single global object owned by "the"
  state machine. Key it by the state machine instance (the group), and keep `completedBelow` semantics global,
  meaning "all below n are done". If the sessions branch puts session state in `NodeStore`'s meta column, it moves
  with the group automatically.

## 5. Question 5: phased plan

### Phase 0: decide with data (no Multi-Raft code)

1. Land arch/apply (the leaner apply), arch/routing (follower reads and the leader hint) and arch/sessions.
2. Add per-column apply metrics (`apply.execute` and bytes, tagged by column) and run `LoadBench` (performance.md:42-61) at 1x to 8x
   with 2048 in flight, leader on NVMe and then on SATA. Record:
   - apply busy %;
   - the share of apply time per column;
   - leader CPU in `preAppendTransaction` and `ConflictTracker`;
   - fsync latency.
3. **Go/no-go:** go on only if the target load is at least 4x production (about 42k ops/s), the apply thread stays
   above about 80% busy at it after the leaner apply, and the cheaper items of section 6 are exhausted. Otherwise
   stop here.

### Phase 1: the smallest step with real headroom (2 groups, static)

1. **Config and model:**
   - `replication.groups` with co-location sets;
   - `NodeConfig.groupIds()`, derived as G0 = the existing `--group-id` and Gk = the name-based UUID of
     `(group-id, k)`, so there is no new flag to get wrong;
   - the id encoding helpers of 3.4.
2. **Per-group runtime:**
   - turn `ReplicatedNode` into a node plus N `GroupRuntime` (state machine, `NodeStore`, `Proposer`, `ReadBarrier`,
     `LeaderStall`, `ClusterCheck`);
   - `setStateMachineRegistry`, add the missing groups at start, per-group bootstrap and `--join`;
   - per-group budgets in `RaftSettings` (divided by N).
3. **Routing:**
   - `ReplicatedConnection` dispatches by group bits or by name;
   - update and iterator ids carry the group;
   - lazy transaction binding and the commit-time fan-out with refusal;
   - `getAllColumnDefinitions` fans out;
   - local maintenance (`flush`, `compact`) runs on every DB.
4. **Placement:** priorities per group, plus the `LeaderPlacement` maintenance rule. Graceful stop hands over every
   group.
5. **Gate and identity:** `ApiGate` on "all groups leaderless"; genesis with `groupIndex`, `assignmentHash` and G0's
   cluster id.
6. **Seed split** (new clusters, and the single-node migration that production needs anyway):
   `ReplicatedServerMain split-seed --data-dir D --config C`, run offline on the seed copy before the first start.
   - It leaves `D/db` as G0.
   - For every k>0 it makes `D/db-gk` as a hard-linked checkpoint of `D/db` (the same code as `CheckpointStorage`).
     It then opens each copy and drops the column families that the map does not assign to it, so each copy keeps
     its own columns plus a fresh meta column.
   - Dropping is metadata only. The files a DB no longer references go away as it deletes obsolete files, and the
     disk cost is only the space shared through hard links. Native ids survive because a checkpoint keeps CF ids.
   - It writes a manifest of `(group, column, nativeId)` and a fingerprint.
   - Run it on one node and copy the result to every node, or run it on identical seed copies: it is
     deterministic. Each group's seeded genesis records the fingerprint, and a replica whose DB does not match it
     stops, which extends today's "replays the seeded Genesis with an empty database" check.
   - Every group takes its post-genesis snapshot, as design.md "Migrating from a single node" (in "Snapshots,
     catch-up and membership") requires today.
7. **Admin:**
   - `add-peer`/`remove-peer` loop over the groups, with the one-voter check per group;
   - `status` per group;
   - `snapshot ID [--group k]`;
   - `transfer-leader --group k`;
   - `repair-log` over every group dir.

Expected result, to be checked in Phase 0 and 1 measurements:

- G1 (`messages-v2`) takes about 57% of the write requests and G0 about 43%, each on its own apply thread and each
  led by a different node.
- The apply-bound saturation could rise from about 38k to about 55-65k ops/s, assuming apply is the only limit
  and the second fsync stream costs little.
- Latency per write stays the same, the commit round trip of 10-16 ms on these SSDs (performance.md "Results"), so
  at 512 writes in flight throughput stays near 2.3x production.
- **Phase 1 only pays off if Yotsuba's in-flight limits rise too** (performance.md, "Client concurrency").

### Phase 2: operations and migration of a deployed single-group cluster

- **Metrics:** per-group role, term, lags, snapshot age, and leader counts per node. Add an alert on a node that leads
  every group for more than 10 minutes.
- **Migration of a running single-group replicated cluster** (only if Multi-Raft lands after the first deployment;
  the replicated cluster has never been deployed, design.md status line):
  1. Gracefully stop every node. Graceful stops flush, and the leader hands over first.
  2. Pick the node with the highest applied index, from `admin status` before the stop or from the applied row in
     its `db/`.
  3. Run `export-seed`: a checkpoint of its `db/` without `__rockserver_replication` (or with it cleared), as a plain
     database.
  4. Run `split-seed`, then copy the result to every node with fresh `raft/` directories, and start them as a new
     seeded cluster with a new cluster id.
  - **Downtime:** dominated by the copy, 171 GB at 100-200 MB/s per link is about 15-30 min, unless the other
    nodes' own `db/` copies are reused. They are logically identical after a graceful stop of a cluster where all
    applied the same index; the tests show replicas byte-identical (design.md "Tests": `LeaderKillTest`,
    `ColdRestartTest`, `ProtocolPointKillTest`).
  - Reuse is allowed only if every node reports the same applied index and the same cluster id. Otherwise copy.
  - Every client retries through the outage. Run it in a maintenance window, not during a deploy.
- **Online column move (optional, only if downtime is unacceptable):** have Yotsuba copy into a new column name
  that the map assigns to the new group, with the same pattern as FileLocations v5 to v8
  (`FileLocationsImpl.java:494-530`: paged copy, a persisted cursor, then delete the old column). It needs no
  server-side migration code, but it costs a Yotsuba change per moved column and a double-write window.

### Phase 3: only if a single column becomes the limit

- Split `messages-v2` by chat-id ranges into K groups. A range table is replicated in G0. Point calls route by
  range; `getMulti`, `existsMulti`, whole-column counts and `getRange` do scatter-gather (no cross-range snapshot);
  `reduceRange firstAndLast` per chat stays in one range. Writes stay single-key, so there is still no cross-group
  transaction.
- Splits and merges need a range-move protocol (snapshot plus catch-up, then a cut-over entry). This is the TiKV and
  CockroachDB machinery, and the expensive part. Do not start it without measurements that require it.

### Test plan

- **Unit:**
  - id encoding round-trips, and native, chained and token id spaces never colliding;
  - routing of every `RocksDBAPICommand` kind;
  - assignment parsing and refusal of a split co-location set;
  - genesis checks (wrong `assignmentHash`, foreign G0 cluster id);
  - `split-seed` determinism (same seed gives byte-identical manifests) and the native ids kept.
- **The existing suites, run with N=2:**
  - `ReplicationTest`, `ApplierTest` (per group), `LeaderSessionTest`, `PipelinedUpdatesTest`;
  - `LeaderKillTest`, `PartitionTest`, `PauseTest`, `RandomFaultTest`, `ColdRestartTest`, `RollingRestartTest`,
    `IsolatedNodeTest`, `ClusterIdentityTest`, `LeaderStallTest`, `MembershipTest`, `RaftLogRepairTest`.
  - Linearizability is checked per group. Replicas must be identical per group DB.
  - Run them with leaders forced onto the same node and onto different nodes.
- **New:**
  - `CrossGroupTransactionTest`: a transaction that touches two groups is refused at commit and leaves no write. One
    whose first call goes to a group whose leader then dies gets `UPDATE_RETRY` or `TRANSACTION_NOT_FOUND`.
  - `LeaderPlacementTest`: leaders spread after start and after a restart, with no transfer loop under a flapping
    node.
  - `MultiGroupMembershipTest`: `add-peer` interrupted between groups, then run again, ends OK.
  - `GroupSnapshotInstallTest`: install of G1 while G0 serves writes, without touching G0's DB.
  - `ApiGateMultiGroupTest`: one group leaderless pauses nothing; the node isolated from all groups pauses.
  - `SplitSeedTest`: Yotsuba-shaped seed, split, cluster serves, ids as expected.
  - `SessionAcrossGroupsTest`: one session's requests across groups, retried after an unknown outcome, are applied
    once each, and the records are purged by the global floor in both groups.
- **Bench:** `LoadBench --groups 1,2,3` at 1x to 8x, 512 and 2048 in flight, NVMe and SATA leaders. Report device
  fsyncs per second per group and p99 per op. Also run it with the leaders spread and with them co-located.

### Risks

- **fsync streams.** N logs on one consumer SSD can raise commit latency enough to eat the gain at Yotsuba's in-flight
  limits. This is the first thing to measure.
- **Leader CPU** may still be the limit if the `ConflictTracker` lock and the Ratis append lock are not fixed (review
  2.3). Multi-Raft does halve them per leader.
- **Memory budgets** multiply per division (pending requests, retry cache, apply queue, snapshot send heap).
- **Operations** become N times as much: N snapshots in flight, N memberships to finish, and a partial `add-peer` to
  understand.
- **Ratis multi-division behavior** that the tests have not covered:
  - empty-group `--join`;
  - recovery of all group dirs at start;
  - priorities combined with the pre-vote setting;
  - the Ratis client's channel-shutdown delay of 3 s (`GrpcUtil.shutdownManagedChannel`; design.md, "Left before a
    first deployment", item 4, where `Proposer.resetConnection` takes it off the proposer's path) once per group.
- **The lazy transaction binding** changes when the snapshot is taken (at the first call, not at open). That is legal
  for optimistic transactions, but it is observable by a client that opens, sleeps, then reads.
- **The ceiling is fixed by `messages-v2`** until Phase 3.

## 6. Cheaper items, ranked (do these first)

| Item | Where | Expected effect |
|---|---|---|
| ~~Leaner apply (`WriteBatchWithIndex`, no `EmbeddedDB` transaction API on the apply thread)~~ done: `EmbeddedDB.ApplyWriter`; the headroom it gave is still to measure (PF-3) | review 3.1, arch/apply | removes per-op bookkeeping and commit validation; the earlier apply changes already took execute from 0.40 to 0.35 ms and commit from 0.22 to 0.12 ms per entry (performance.md "What limited it, and what changed", item 10); expect tens of percent more apply headroom |
| ~~**Partitioned apply inside one group**~~ done: `--parallel-apply-threads N`, off by default (design.md, "Partitioned apply"), by first key part of fixed-key columns, else by column | new | within one apply group, execute entries in K partitions by column (or by key hash for single-key entries) on K threads, each into its own indexed batch, then merge them into **one** atomic `db.write` with the position. Entries that touch several columns, and schema changes, run as barriers. Results stay deterministic because a request reads only its own column's keys (guards, previous values) in log order within its partition. Crash consistency and the single log stay. This captures more apply parallelism than column groups (it is not bounded by `messages-v2`) with none of the membership, snapshot or fsync costs. |
| ~~Follower reads and leader hint~~ done: follower point reads are off by default until measured with the Yotsuba mix (design.md, "Follower reads") | review 2.1, arch/routing | takes 6.1k gets/s and `existsMulti` off the leader and removes the forwarding hop for about 2/3 of the traffic |
| `ConflictTracker` off the append lock for plain writes (not done: still under the lock, without a per-key allocation since; unmeasured, performance.md item 16) | review 2.3 | less leader CPU per write, inside the Ratis lock |
| ~~Raise Yotsuba's `maxConcurrentWriteRequests` and read limit~~ done: 1024 each on a cluster (Yotsuba `456b8f8b7`, `df8acf015`); the production value is still to measure | performance.md, "Client concurrency" | 256 in flight caps the cluster near 2x; 2048 reach about 3.5x |
| ~~Snapshots by log bytes~~ done: `--snapshot-every-bytes`, 4 GiB by default (design.md, "Snapshots, catch-up and membership") | review 3.7 | fewer apply pauses at high batch sizes |

**Numbers for the verdict** (performance.md):

- **Production today:** 10.6k ops/s mean.
- **The cluster now:**
  - 1x and 2x served with `UPDATE` p99 of 16-33 ms in the latest runs;
  - 3.5x saturation with 2048 in flight (37.4k to 38.6k ops/s), apply 70-81% busy;
  - at 512 in flight, about 2.3x, bound by latency and not by apply.
- **Multi-Raft by column** raises only the apply-bound part, by at most about 1.75x, and adds N fsync streams.
- **The leaner apply plus partitioned apply** attack the same limit with no new failure modes. Multi-Raft is the
  right next step only if growth targets exceed about 4x with those done. Then it is Phase 1 as written:
  2 groups, `messages-v2` alone, one DB per group, transactions refused across groups (Yotsuba has none),
  sessions per group.

### Critical files for implementation

- `D/ReplicatedNode.java` (server builder, per-group runtime, maintenance, graceful stop)
- `D/ReplicatedConnection.java` (routing by group, id encoding, lazy transaction binding)
- `D/RockserverStateMachine.java` and `D/NodeStore.java` (per-group state machine, one DB per group, seed checks)
- `D/RaftSettings.java` and `D/NodeConfig.java` (per-division budgets, group ids, assignment)
- `D/ReplicatedServerMain.java` (admin over groups, `split-seed`, `repair-log` per group)
