# Replicated Rockserver: design

Status: implemented on `claude/distributed-rockserver-rewrite-e46da4`, never deployed. It replaces the Codex
design; [review.md](review.md) explains why. Code: `distributed/server`, package
`it.cavallium.rockserver.distributed`. Throughput work, measurements and the load benchmark:
[performance.md](performance.md).

## Goal and non-goals

**Goal.** Yotsuba becomes a stateless client of Rockserver. Rockserver must keep serving through:

- rolling restarts (about 3 deploys a day);
- the crash or permanent loss of one node;
- a network partition that isolates one node.

No acknowledged write may be lost or applied twice, and reads are linearizable.

**Non-goals for v1:**

- horizontal write scaling or partitioning;
- multi-datacenter deployment;
- cross-shard transactions;
- online resharding;
- CDC, which was removed from Rockserver altogether.

Every node holds the full dataset, like the single node today. If partitioning is ever needed, it can be
added later as several independent copies of this design, with routing in the client.

## Shape

```
              Yotsuba (unchanged Rockserver client API, gRPC)
                 │ any node; requests that need the leader are forwarded to it
   ┌─────────────┼─────────────────────────────┐
   ▼             ▼                             ▼
 node A        node B (leader)               node C
 GrpcServer    GrpcServer                    GrpcServer        ← core server, unchanged
 ReplicatedConnection (implements RocksDBConnection)            ← routes every API command
 EmbeddedDB    EmbeddedDB                    EmbeddedDB        ← core engine, unchanged
 Ratis server ═══ Raft log replication (stock Apache Ratis 3.3.1, gRPC) ═══
```

**One Raft group, three voting replicas, stock Ratis.** There are no Ratis patches, and Ratis APIs are used
only as published. Each replica is an ordinary `EmbeddedDB` with the same column schemas, merge operators,
bucket hashing, workload scheduler and native JNI as a single node. Every node must run the same Rockserver
config file.

**What this module needs from core.** Single-node behavior and the wire contract are unchanged, apart from the
separate removal of CDC from Rockserver. The core additions are used only by replicas or by a server that
forwards to a non-embedded connection:

- `MergeOperatorRegistry.upload` is atomic, and validates the operator before writing anything.
- `GrpcServer` runs requests to a non-embedded connection on virtual threads of their own (a proxy scheduler,
  at most 16384 at once, beyond which requests get `SERVER_OVERLOADED`); streamed `putMulti`, `deleteMulti`
  and `mergeMulti` calls, rollbacks and maintenance use the workload scheduler instead
  (`RWScheduler.withVirtualWorkers`, `WorkloadSettings.proxy`), which runs at most 256 of these streams at
  once and refuses more once its queues are full. A stream that waits in a queue past its request-context
  timeout ends with `DEADLINE_EXCEEDED`, even when the client set no gRPC deadline.
- `EmbeddedDB.openUnloggedTransaction` (a transaction without snapshot, untracked, optionally without the WAL),
  `EmbeddedDB.deleteRangeUnlogged` (a range deletion without the WAL) and the `atomic-flush` global option (off
  by default; it turns pipelined writes off).
- `GrpcConnection.forHostAndPortWithoutRetry`, for callers that retry on their own.

| Class | Role |
|---|---|
| `ReplicatedNode` | Wiring and lifecycle: database, Raft server, API server, cluster initialization, graceful hand-off. |
| `ReplicatedConnection` | Routes each `RocksDBAPICommand` (see below). |
| `Proposer` | Batches numbered requests into log entries and submits them to the leader: straight to the local Raft server on the leader, through a Ratis client elsewhere. |
| `RockserverStateMachine` | Ratis state machine. Leader admission in `startTransaction`/`preAppendTransaction`, synchronous apply, snapshots. |
| `Applier` | Applies the requests of one log entry to the local database in one transaction without the WAL, with their result records and the applied position. |
| `LeaderSession`, `ConflictTracker` | Transactions bound to the leader's term, per-key queues of single-key updates, and conflict validation. |
| `NodeMetrics` | Timers and gauges of the replication path, logged every 10 s. |
| `ReadBarrier`, `TermService` | Linearizable reads. |
| `NodeStore`, `CheckpointStorage` | Local database, internal metadata, checkpoint snapshots, restore. |
| `LeaderForwarder` | Finds the leader's API and forwards to it. |
| `LogEntry`, `Request`, `LogEntryCodec`, `ResultCodec` | Log entry model and its binary encoding. |
| `ReplicatedServerMain` | Command line: `run` and `admin`. |

## Routing

`ReplicatedConnection` sends every API command down one of four paths:

| Path | Commands | Handling |
|---|---|---|
| Replicated write | `put*`, `delete*`, `merge*` without a transaction id; `deleteRange`; `putBatch`/`mergeBatch` (split into ordered chunks of at most 4 MiB); `createColumn`, `deleteColumn*`, `uploadMergeOperator` | Proposed from the node that received it. The result is computed when the leader applies it. |
| Linearizable read | `get`, `exists*`, `getMulti`, `reduceRange*`, `getRange*`, `getRangePage`, iterator open, `getColumnId`, `getAllColumnDefinitions`, `checkMergeOperator`, `estimateNumKeys`, `scanRaw*` | On the leader, after the read barrier, against its local database. Forwarded to the leader from the other nodes. |
| Leader-bound | `openTransaction`, `getForUpdate`, writes and reads with a transaction or update id, `closeTransaction`, `closeFailedUpdate`, iterator seek, next and close | Served by the leader of the current term. Forwarded to it from the other nodes. |
| Local | `flush`, `compact`, `compactFiles`, `getSstMetadata`, `getTableProperties` | The node that received the request does it on its own database. |

Forwarding uses the leader's ordinary Rockserver API through the core `GrpcConnection` (4 connections per
peer, without gRPC's own retries; a port probe first, so a dead peer fails in milliseconds). When the leader
cannot be reached, reads and transaction or update opens are sent again to whichever node leads then, until the
request timeout; a request that was never sent can always be retried; a commit whose connection broke has an
unknown outcome. A `getForUpdate` whose connection broke is not sent again but fails with `UPDATE_RETRY`: the
leader may still be alive and hold the key for that update until its lease ends. A forwarded stream is sent again
only if it was never sent, or if it is not in a transaction and returned nothing yet. Otherwise it fails with
`UPDATE_RETRY`, or with `SERVER_OVERLOADED` once part of it was returned. A node learns the API address of a
member that joined later from that member's `TermService`. The internal column
`__rockserver_replication` is hidden from `getAllColumnDefinitions`, and every command that names it is
rejected.

## Writes, exactly once

A proposal is a `Request`: a random proposer id (one per node process), a sequence number, `completedBelow`,
and the `LogEntry`.

- **Batches.** Requests that arrive while 32 batches are in flight wait, then go together as one log entry
  (`LogEntryCodec.encodeBatch`: the proposer id and `completedBelow` once, then each sequence and entry; up to
  4096 requests or 8 MiB; schema changes, `deleteRange` and genesis always alone). One entry costs one Ratis
  append, one place in a group fsync and one apply transaction, whatever it holds. Transaction commits have a
  queue of their own, and commits of different leader terms never share an entry.
- **Retries.** A batch is resent with the same bytes across leader changes until the latest deadline of its
  requests (at most one attempt past the oldest one's). On the leader an attempt goes straight to the local Raft
  server; elsewhere it is an independent blocking Ratis client call on a virtual thread. Ratis' ordered async
  stream closes for good after one exhausted request, so it is not used. A batch whose attempt ended without
  saying whether it was appended can only end with its results or an unknown outcome, never "not applied".
- **Results are recorded.** Applying a request writes its encoded result under
  `(proposerId, sequence)` in the internal column, in the same local transaction as its effects. If a
  later copy of the same request reaches the log (a retry that went to a new leader, or to a restarted node
  whose in-memory Ratis retry cache is empty), applying it returns the recorded result and does nothing else.
  So a write takes effect at most once and its caller always gets the first result.
- **Late copies never run.** Each proposer reports `completedBelow`: every request below it has its final
  answer (a result, or "outcome unknown" after the deadline), so the proposer sends none of them again. Apply
  keeps the highest value per proposer as that proposer's **floor**. A copy numbered below the floor, for
  example an attempt that was still queued when the proposer gave up on it, is refused without being executed
  and without reading any record. Sequence numbers are allocated and registered atomically, so
  `completedBelow` never passes a request that is still in flight.
- **Records are deleted in bulk.** Records below the floor are never read again, so they are not deleted in
  the entry's write. Once a proposer's floor has moved 1024 sequences, or the proposer has recorded 4 MiB of
  results, since the last deletion, one range deletion without the WAL removes everything below its floor,
  after the entry that moved the floor. Before applying its first entry after a start, a replica also scans
  every proposer's floor and deletes the records below it, which covers proposers of processes that stopped
  (a proposer id lives as long as its process). With atomic flush a crash keeps a deletion only together with
  the floor that covers it; if it loses it, the records come back below the floor, unread, until the next
  one. Loading a proposer's records skips those below its floor.
- **Duplicates are found in memory.** Each replica keeps, per proposer, its floor and the sequences recorded
  at or above it, loaded from the internal column the first time the proposer is seen: checking a request for
  an earlier copy costs no read.

**Apply.** Every replica applies entries in log order on the Ratis apply thread, through the `EmbeddedDB`
API.

- **Data writes** (puts, deletes, merges, transaction commits) of one entry run in one local transaction
  without the RocksDB WAL. It also writes their result records, the proposer's floor and the applied position,
  so a crash keeps all of the entry or none of it. If one request is rejected for a logical reason, the entry
  is applied again one request at a time, and only its last request records the position.
- **Schema changes and `deleteRange`** cannot run in a transaction. All memtables are flushed first, so that
  everything applied without the WAL is on disk before a logged write follows it; their record and position
  are then written in one logged transaction, and the WAL is synced, because RocksDB writes column family
  changes to the MANIFEST synchronously: a crash cannot keep one without the other. A column deletion first
  records, synced, whether the column existed, so that a crash before its record makes the replayed entry
  give the same result as on the other replicas. Creating a column that exists with the same schema changes
  nothing and is recorded without the flush.
- **Return values** (previous value, delta, merged value...) are computed by apply, so they are the same on
  every replica. The results of an entry go back in one reply of at most 17 MiB (the Raft client's gRPC limit
  is 18 MiB). A result too large for a reply by itself, or one that cannot be encoded, is replaced before it
  is recorded, the same on every replica: by an `INVALID_REQUEST` failure saying that the write was applied
  (an `INTERNAL_ERROR` one if it cannot be encoded). That failure is both the record and the reply. If the
  results of an entry do not fit together, the largest ones are replaced in the reply by a "resend alone"
  marker. The proposer then sends each of those requests again in an entry of its own: its sequence is still
  in flight, so the copy does not run and gets the recorded result, which fits in a reply by itself. A
  transaction commit is not sent again (the copy would conflict with itself); the leader that answered is
  this node, which applied it, so its record is read locally. Building the reply never fails. A reply too
  large to receive anyway fails its requests with an "applied" `INVALID_REQUEST` instead of being resent.

**Failures during apply.**

- **Logical rejections** (missing column, malformed keys...) are identical on every replica. They become the
  request's result and the log moves on.
- **Local failures** (native or IO errors, a lost local transaction) stop the replica:
  - its Raft server stops first, so a failed leader stops leading;
  - the process exits with code 3;
  - it never skips the entry.

**Column ids** are native column family ids. Every replica gets the same ones because each runs the same
history from the same starting point. After a `createColumn` the leader proposes `VerifyColumn(name, id)`,
and a replica that assigned a different id stops. The internal column is created when a database is first
opened, before any entry is applied, and no other column is ever created locally.

**Durability.**

- **Raft log.** It is synced (`fsync`) on a majority before a write is acknowledged. It is the only log of
  data writes: they are applied without the RocksDB WAL, which would only write everything a second time.
- **RocksDB.** Replicas open it with `atomic-flush` (every column flushed together) and point-in-time WAL
  recovery (`absolute-consistency` forced off). After a crash or power loss the database is the last flushed
  state, a consistent point where the applied position matches the data, and Raft replays the rest: the log
  is purged only up to a snapshot, and a snapshot flushes. So after a kill, a replica's files are behind until
  it starts again; a graceful stop flushes.
- **Restart.** Ratis feeds entries from the last snapshot onwards. Entries already applied are skipped, and
  their recorded results are returned.

## Reads: our own ReadIndex

Stock Ratis 3.3.1 ReadIndex can reuse a leadership confirmation obtained before a read arrived
(`ReadIndexHeartbeats.addAppendEntriesListener` returns null when the commit index did not move). A deposed
leader can therefore serve stale reads. `ReadBarrier` does the check itself, with public APIs only:

1. When a read arrives, the leader must be ready, meaning it committed an entry of its own term. It records
   the commit index `c`.
2. The read joins the next confirmation round whose queries have not been sent yet. That round asks every
   voting peer for its current term, using `TermService` on the Ratis gRPC port. The read may proceed once
   a majority of the current configuration reports the leader's term, counting the leader itself. During a
   membership change, a majority of the previous configuration is needed too. A higher term fails the read.
3. The read then runs against the local database. `ReadBarrier.await` also waits until the local applied
   index reaches `c`; replicated reads use `awaitLeadership`, which does not: every write is acknowledged only
   after the leader applied it (the result comes from the leader's apply), and a ready leader has applied
   every entry of earlier terms, so whatever a client saw acknowledged is already in the leader's database.

**Why this is safe.** Suppose a leader with a higher term had committed something before the read arrived.
Then a majority had already moved to that higher term, and terms never decrease. A majority answering after
the read arrived must overlap it, so it could not report the old term. One round costs one network round
trip, with no fsync, and it serves every read that arrived before it started.

## Transactions

Production uses `OptimisticTransactionDB`, and replicas force it (`optimistic` on, whatever the config says): the
leader's native transactions only stage writes and serve reads, and with a pessimistic database their row locks
would block the apply of their own commits. Yotsuba relies on conflicts spanning several columns,
`getForUpdate` fences, `false` from commit meaning "conflict", and retrying `UPDATE_RETRY` and
`TRANSACTION_NOT_FOUND`.

**On the leader.** Transactions run on the leader, bound to its term (`LeaderSession`):

- **Open.** `openTransaction` and `getForUpdate` wait for the read barrier, then open a native transaction or
  update on the leader's `EmbeddedDB`. That gives the same snapshot and read-your-writes behavior as on one
  node. A `getForUpdate` starts the barrier when it arrives, so the barrier runs while it waits for its key.
- **Single-key updates queue.** A `getForUpdate` without a transaction first waits for the earlier updates of
  the same key (a FIFO lock per key hash, held until the update's commit is applied or the update is closed or
  expires after its 10 s lease; a waiter gives up after 12 s, or at its caller's deadline, with `UPDATE_RETRY`).
  It then reads what the previous one committed, instead of every concurrent update of a hot key reading the
  same value and all but one conflicting. An update whose caller is gone (its deadline passed, or it cancelled)
  when it gets the key releases the key at once, without opening the native update. If the caller cancels after
  the update opened but before the id reached it, the leader closes the update and releases the key at once
  instead of holding it for the lease. While an update waits, the
  leader expires abandoned updates every 100 ms, so the waiter gets the key at the end of the lease even when no
  other request comes.
- **Staged writes** happen natively. The leader also records them, with their conflict keys: the written keys
  plus the `getForUpdate` keys.
- **Commit** proposes one `Guarded` entry that carries the writes, the snapshot index and the conflict keys.
  It is submitted straight to the local Raft server in a single attempt, so a refusal really means the entry
  was not appended.
- **Open** also checks that the leader session belongs to the term the read barrier confirmed. A node that lost
  and regained leadership meanwhile might not have applied, or tracked, what the other leader committed.

**Validation.** Ratis calls `preAppendTransaction` under its log write lock, in log order.

- **Check.** It checks the conflict keys against `ConflictTracker`, a map from key hash (or whole column) to
  the index of the last entry that wrote it, filled as entries are appended. A key written after the snapshot
  index, including by an entry still in flight, is a conflict.
- **On conflict** commit returns `false` and the transaction stays open, as on a single node. Commits share
  entries: the leader rejects the entry before appending it and names the commits that conflict (with history,
  or with an earlier write of the same entry); those fail and the others go again in the next entry.
- **Outcome of a commit.** It is submitted once, to the local Raft server. Only a refusal that Ratis gives before
  appending (leader not ready, no pending-request permit, or the state machine's own refusals) means "not
  committed"; `NotLeaderException`, which a leader stepping down also returns for appended entries, is an
  unknown outcome.
- **Otherwise** the entry is appended and later applied atomically on every replica. The leader's native
  transaction is then rolled back, because its effects arrive through the log.
- **Hash collisions** can only cause false conflicts.
- **History is kept** as long as any open transaction, reservation or in-flight commit pins a snapshot. A commit
  whose snapshot is older than history that was already forgotten always conflicts, whatever released it, so a
  timing mistake can only cause a false conflict, never a missed one.

**Leader changes.** Transactions, update ids and iterators do not survive a leader change. Clients get
`UPDATE_RETRY` and run the transaction again, which Yotsuba already does. A commit of a transaction that
another leader opened is refused before it is appended (the term check), so it can never be validated
against an incomplete history.

## Snapshots, catch-up and membership

**Snapshot.**

- **Building it.** `takeSnapshot` builds a checkpoint from RocksDB's live-file APIs, with file deletions
  disabled:
  - memtables are flushed;
  - SST and blob files are hard-linked (copied across file systems);
  - the MANIFEST prefix is copied and `CURRENT` written;
  - the WALs still needed are copied.
- **Why not `org.rocksdb.Checkpoint`.** It refuses the column volumes (`cf_paths`) that the default config
  uses.
- **When.** Every `snapshotEveryEntries` applied entries (default 500k). Taking one flushes the memtables and
  stops the apply for about 100–300 ms. Files that are hard links of durable files are not fsynced again.
- **Log retention.** The Raft log is purged only up to the snapshot index and the lowest commit index of any
  member, and keeps `snapshotEveryEntries` entries more. A node restarted during a deploy, or one that just
  installed a snapshot, therefore catches up from the log. A member that is gone for good holds the log back,
  so remove it from the configuration promptly, and alert on Raft log size.

**Catch-up and install.**

- **Install.** A follower too far behind gets the snapshot files through stock Ratis snapshot installation.
  Ratis pauses the state machine. `reinitialize` then restores the checkpoint into the database directory:
  immutable files are hard-linked, the swap is crash-safe, and the database is reopened. If the local
  database is already ahead of the installed snapshot, it is kept.
- **Leader-side retention.** The leader keeps the previous snapshot, and any snapshot whose file list was read
  in the last 10 minutes, so a long install is not cut short by newer snapshots.
- **Damaged installs.** A snapshot that a crash damaged during install makes the replica fail loudly; it is not
  silently ignored.

**Membership.**

- **Initial cluster.** All nodes start with the same peer list. The first leader proposes `Genesis`, which
  binds every replica's database to the cluster and records whether the cluster was seeded from an existing
  database.
- **Migrating from a single node.** Stop it and copy its database directory to every node. A seeded cluster
  takes a snapshot right away. Before the log is purged, a node that joins with an empty database stops at
  `Genesis` instead of diverging.
- **Replacing a node.** `ReplicatedServerMain admin ... add-peer / remove-peer` (Ratis `setConfiguration`).
  New members catch up from a snapshot. A node that lost its Raft state but kept its id refuses to start,
  because it could vote twice in one term. Replace it under a new id.
- **Graceful stop.** On SIGTERM a leader hands leadership to the most up-to-date live follower before
  stopping. A deploy restart costs a few hundred milliseconds without writes (see the tests).

## Behavior differences from a single node

- **Leader changes.** Transactions, update ids and iterators do not survive one. Clients get `UPDATE_RETRY`.
- **Outcome unknown.** When no leader answers within the request timeout, the error is `SERVER_OVERLOADED`
  with "outcome unknown", and the write may still take effect later. Internal retries are exactly once, but
  a client that resends the same write after such an error can apply it twice.
- **`putBatch`/`mergeBatch`** run as ordered replicated write batches, whatever the requested SST mode.
- **Size limit.** One replicated entry (one write, multi-write or transaction commit) is limited to 16 MiB.
- **`scanRaw` SST tokens** describe the leader's own files, so a leader change invalidates them.
- **Merge operators** must be deterministic.
- **Maintenance.** `flush` and `compact` act only on the node that receives them.
- **Workload limits.** The replicated server's `GrpcServer` runs requests on virtual threads of their own (at
  most 16384 waiting on the leader), not through each column's workload scheduler. Streamed mutations run at
  most 256 at once.
- **Updates of one key run one at a time** on the leader. Nested or crossed updates of the same keys by one
  client block (up to 12 s) instead of conflicting at once.
- **After a kill** (not a graceful stop), a replica's database files are an older consistent point until it
  starts again and replays its log.

## Tests

All tests use real Ratis servers over real sockets. The multi-process tests start separate JVMs, each with its
own hosts file. Every directed link between two nodes goes through its own TCP proxy on a distinct loopback
address, so a test can partition any set of links.

| Test | What it proves |
|---|---|
| `ReplicationTest` (in-process) | Replication through any node; results; leader loss; snapshot catch-up; batches; transactions, conflicts and updates; updates whose caller timed out or cancelled while queued, or cancelled while the update opened, do not keep the key; transactions lost on a leader change; whole-cluster restart; recovery after a proposal timeout. |
| `ApplierTest` | At-most-once retries, including after a restart; deletion of records, by count, by bytes and at start; results too large for a reply; rejections; atomic commits; detection of column id divergence; batches with duplicates and rejections; crash consistency of applies without the WAL. |
| `LeaderSessionTest` | Per-key update queue (order, release on commit, expiry also without other traffic, and end of leadership); history kept for open transactions and in-flight commits. |
| `LeaderKillTest` | Repeated SIGKILL of the leader under concurrent writes. No acknowledged merge is lost or doubled, and replicas are byte-identical. Failover takes about 1 s. |
| `ForwardingFailoverTest` | A client of a follower sees only retryable errors (point reads, range streams, writes) while the leader is killed, and every kind of request succeeds again after the election. |
| `PartitionTest` | An isolated leader acknowledges nothing. The majority takes over. A leader that keeps its majority keeps serving. The history is linearizable. |
| `PauseTest` | SIGSTOP of the leader and of a follower. The history stays linearizable. |
| `RollingRestartTest` | Graceful restart of every node, twice, under load. No failed operation. A leader restart stops writes for about 200–300 ms. |
| `MembershipTest` | A node replaced through `add-peer`/`remove-peer` catches up from a snapshot. Seeded bootstrap works. An empty node joining a seeded cluster stops at `Genesis`. |
| `ColdRestartTest` | SIGKILL of all three nodes under load, 5 cycles, plus a 2-of-3 restart. Nothing acknowledged is lost, and replicas are identical. |
| `RandomFaultTest` | A seeded random schedule of kills, partitions, pauses and restarts. Register histories are linearizable, and the transactional bank-transfer invariant holds. |
| Component tests | Codecs (including fuzzing), `ConflictTracker` (checked against a reference model), `ReadBarrier` (deposed and isolated leaders never pass), `NodeStore`/`CheckpointStorage`, `LeaderForwarder` (leader lookups still waiting fail on close), fault proxy, linearizability checker. |

Run them all (about 10 minutes):

```bash
mvn -o -B -f distributed/pom.xml -pl server -am test -Dtest='it.cavallium.rockserver.distributed.**' -Dsurefire.failIfNoSpecifiedTests=false
```

## Left before a first deployment

1. **Client side.**
   - Yotsuba does not compile against this version as is: CDC was removed from the client API, and
     `server/source/api/src/main/java/org/yotsuba/database/RockserverAsyncApiViews.java:133` still overrides
     `cdcPollBatchAsync` (and imports `it.cavallium.rockserver.core.common.cdc.CdcBatch`). It is a pass-through
     that nothing calls: delete it, and the few Yotsuba tests that mention it. telegramdb has no CDC references.
   - Yotsuba must use several node addresses and fail over between them. Today `GrpcConnection` takes a
     single address.
   - The core client's automatic gRPC retry of reads holds requests against a stopping node for up to its
     backoff. Tune or disable it for cluster use.
2. **Exactly-once for client retries.** A client request id in `RequestContext`, mapped onto `Request`
   identities, would make a client-level retry after an "outcome unknown" error safe. Also consider a
   dedicated error type for unknown outcomes instead of `SERVER_OVERLOADED`, which Yotsuba retries.
3. **Capacity.** See [performance.md](performance.md) for the measurements on one PC and the load benchmark.
   Still to measure on the production machines and data: their fsync latency, snapshot creation and install
   time at 171 GB and at the multi-TB datasets, and log retention in bytes. Raise Yotsuba's in-flight request
   limits for the cluster (see performance.md).
4. **Write failover after a frozen leader.** When the leader freezes (SIGSTOP, a long GC pause, a hung disk),
   writes resume about 4 s later, although the election takes about 1 s and reads resume sooner. The cause is
   Ratis' client closing the channel to the frozen leader, which waits a hard-coded 3 s
   (`GrpcUtil.shutdownManagedChannel`). Crashes and graceful restarts are not affected (failover about 1 s,
   hand-off about 250 ms). Fix by keeping per-peer clients or by reporting it upstream.
5. **Snapshot transfer at TB scale.** Ratis installs restart from scratch after a failure, and are not rate
   limited. If that proves too slow, switch to install notifications plus a resumable pull of checkpoint
   files.
6. **Operations.**
   - Metrics and alerts: role, term, commit and apply lag, snapshot age, follower lag, replica failure exit.
   - A seeding runbook (copy the single-node database to all nodes before the first start).
   - A node replacement runbook.
   - Placement on separate hosts and disks.
   - The same Rockserver config file on every node.
7. **Known limits to decide on.**
   - `deleteColumn` waits for open iterators on the leader, which stalls apply there until they close or
     expire.
   - The result records that a stopped proposer process had in flight (at or above its floor) are never
     deleted; those below its floor are deleted when each replica next starts. Add a cleanup entry if crashes
     turn out to be frequent.
   - A result larger than 17 MiB (for example a multi-key previous value) is replaced by an
     `INVALID_REQUEST` failure: the write is applied, but its caller does not get the result.
   - Snapshots are triggered by entry count only, and a failing snapshot is retried at every apply step. Each
     one stops the apply for its memtable flush (100–300 ms measured).
   - Iterator calls after a leader change fail with an internal "iterator not found" error, not `UPDATE_RETRY`.
   - The snapshot taken right after a seeded genesis is attempted once, on the leader only.
   - A SIGTERM that races a replica failure can exit with 0 instead of 3.
8. **Upstream.** Report the Ratis ReadIndex reuse bug, and switch back to Ratis ReadIndex once it is fixed
   upstream.
