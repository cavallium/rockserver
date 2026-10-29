# Replicated Rockserver: design

Status: implemented on `claude/distributed-rockserver-rewrite-e46da4`, never deployed. It replaces the Codex
design; [review.md](review.md) explains why. Code: `distributed/server`, package
`it.cavallium.rockserver.distributed`.

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

**What core needs from this module.** Nothing changed in core except
`MergeOperatorRegistry.upload`, which was made atomic. Single-node behavior and the wire contract are
unchanged (CDC aside).

| Class | Role |
|---|---|
| `ReplicatedNode` | Wiring and lifecycle: database, Raft server, API server, cluster initialization, graceful hand-off. |
| `ReplicatedConnection` | Routes each `RocksDBAPICommand` (see below). |
| `Proposer` | Submits numbered requests to the leader through a Ratis client. |
| `RockserverStateMachine` | Ratis state machine. Leader admission in `startTransaction`/`preAppendTransaction`, synchronous apply, snapshots. |
| `Applier` | Applies one request to the local database, with its result record and applied position. |
| `LeaderSession`, `ConflictTracker` | Transactions bound to the leader's term, and their conflict validation. |
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

Forwarding uses the leader's ordinary Rockserver API through the core `GrpcConnection`. A node learns the API
address of a member that joined later from that member's `TermService`. The internal column
`__rockserver_replication` is hidden from `getAllColumnDefinitions`, and every command that names it is
rejected.

## Writes, exactly once

A proposal is a `Request`: a random proposer id (one per node process), a sequence number, `completedBelow`,
and the `LogEntry`.

- **Retries.** The Ratis client retries a proposal across leader changes until the request timeout, always
  with the same number. Each attempt is an independent blocking call on a virtual thread. Ratis' ordered
  async stream closes for good after one exhausted request, so it is not used.
- **Results are recorded.** Applying a request writes its encoded result under
  `(proposerId, sequence)` in the internal column, in the same local transaction as its effects. If a
  later copy of the same request reaches the log (a retry that went to a new leader, or to a restarted node
  whose in-memory Ratis retry cache is empty), applying it returns the recorded result and does nothing else.
  So a write takes effect at most once and its caller always gets the first result.
- **Records are deleted.** Each proposer reports `completedBelow`, the sequence below which it will never
  retry. Every 256 sequences, apply deletes that proposer's older records, the same way on every replica.

**Apply.** Every replica applies entries in log order on the Ratis apply thread, through the `EmbeddedDB`
API.

- **Data writes** (puts, deletes, merges, transaction commits) run in one short local transaction. That
  transaction also writes the result record and the applied position, so a crash keeps either all three or
  none.
- **Schema changes and `deleteRange`** cannot run in a transaction. They are idempotent at their log
  position, so their record and position are written right after them: RocksDB recovers a prefix of its WAL.
- **Return values** (previous value, delta, merged value...) are computed by apply, so they are the same on
  every replica.

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

- **Raft log.** It is synced (`fsync`) on a majority before a write is acknowledged.
- **RocksDB WAL.** It stays enabled but unsynced. Replicas open RocksDB with point-in-time WAL recovery
  (`absolute-consistency` forced off): after a crash or power loss they recover a consistent prefix, and
  Raft replays the rest.
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
3. The read then waits until the local applied index reaches `c`, and runs against the local database.

**Why this is safe.** Suppose a leader with a higher term had committed something before the read arrived.
Then a majority had already moved to that higher term, and terms never decrease. A majority answering after
the read arrived must overlap it, so it could not report the old term. One round costs one network round
trip, with no fsync, and it serves every read that arrived before it started.

## Transactions

Production uses `OptimisticTransactionDB`. Yotsuba relies on conflicts spanning several columns,
`getForUpdate` fences, `false` from commit meaning "conflict", and retrying `UPDATE_RETRY` and
`TRANSACTION_NOT_FOUND`.

**On the leader.** Transactions run on the leader, bound to its term (`LeaderSession`):

- **Open.** `openTransaction` and `getForUpdate` wait for the read barrier, then open a native transaction or
  update on the leader's `EmbeddedDB`. That gives the same snapshot and read-your-writes behavior as on one
  node.
- **Staged writes** happen natively. The leader also records them, with their conflict keys: the written keys
  plus the `getForUpdate` keys.
- **Commit** proposes one `Guarded` entry that carries the writes, the snapshot index and the conflict keys.
  It goes to the local leader in a single attempt, so a refusal really means the entry was not appended.

**Validation.** Ratis calls `preAppendTransaction` under its log write lock, in log order.

- **Check.** It checks the conflict keys against `ConflictTracker`, a map from key hash (or whole column) to
  the index of the last entry that wrote it, filled as entries are appended. A key written after the snapshot
  index, including by an entry still in flight, is a conflict.
- **On conflict** commit returns `false` and the transaction stays open, as on a single node.
- **Otherwise** the entry is appended and later applied atomically on every replica. The leader's native
  transaction is then rolled back, because its effects arrive through the log.
- **Hash collisions** can only cause false conflicts.
- **History is kept** as long as any open transaction, reservation or in-flight commit pins a snapshot.

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
- **When.** Every `snapshotEveryEntries` applied entries.
- **Log retention.** The Raft log keeps `snapshotEveryEntries` entries behind the latest snapshot, so a node
  restarted during a deploy catches up from the log.

**Catch-up and install.**

- **Install.** A follower too far behind gets the snapshot files through stock Ratis snapshot installation.
  Ratis pauses the state machine. `reinitialize` then restores the checkpoint into the database directory:
  immutable files are hard-linked, the swap is crash-safe, and the database is reopened. If the local
  database is already ahead of the installed snapshot, it is kept.
- **Leader-side retention.** The leader keeps the snapshot that a running install reads.

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
- **Workload limits.** The replicated server's `GrpcServer` uses its own scheduler, not each column's
  workload scheduler.

## Tests

All tests use real Ratis servers over real sockets. The multi-process tests start separate JVMs, each with its
own hosts file. Every directed link between two nodes goes through its own TCP proxy on a distinct loopback
address, so a test can partition any set of links.

| Test | What it proves |
|---|---|
| `ReplicationTest` (in-process) | Replication through any node; results; leader loss; snapshot catch-up; batches; transactions, conflicts and updates; transactions lost on a leader change; whole-cluster restart; recovery after a proposal timeout. |
| `ApplierTest` | At-most-once retries, including after a restart; deletion of records; rejections; atomic commits; detection of column id divergence. |
| `LeaderKillTest` | Repeated SIGKILL of the leader under concurrent writes. No acknowledged merge is lost or doubled, and replicas are byte-identical. Failover takes about 1 s. |
| `PartitionTest` | An isolated leader acknowledges nothing. The majority takes over. A leader that keeps its majority keeps serving. The history is linearizable. |
| `PauseTest` | SIGSTOP of the leader and of a follower. The history stays linearizable. |
| `RollingRestartTest` | Graceful restart of every node, twice, under load. No failed operation. A leader restart stops writes for about 200–300 ms. |
| `MembershipTest` | A node replaced through `add-peer`/`remove-peer` catches up from a snapshot. Seeded bootstrap works. An empty node joining a seeded cluster stops at `Genesis`. |
| `ColdRestartTest` | SIGKILL of all three nodes under load, 5 cycles, plus a 2-of-3 restart. Nothing acknowledged is lost, and replicas are identical. |
| `RandomFaultTest` | A seeded random schedule of kills, partitions, pauses and restarts. Register histories are linearizable, and the transactional bank-transfer invariant holds. |
| Component tests | Codecs (including fuzzing), `ConflictTracker` (checked against a reference model), `ReadBarrier` (deposed and isolated leaders never pass), `NodeStore`/`CheckpointStorage`, fault proxy, linearizability checker. |

Run them all (about 10 minutes):

```bash
mvn -o -B -f distributed/pom.xml -pl server -am test -Dtest='it.cavallium.rockserver.distributed.**' -Dsurefire.failIfNoSpecifiedTests=false
```

## Left before a first deployment

1. **Client side.**
   - Yotsuba must use several node addresses and fail over between them. Today `GrpcConnection` takes a
     single address.
   - The core client's automatic gRPC retry of reads holds requests against a stopping node for up to its
     backoff. Tune or disable it for cluster use.
2. **Exactly-once for client retries.** A client request id in `RequestContext`, mapped onto `Request`
   identities, would make a client-level retry after an "outcome unknown" error safe. Also consider a
   dedicated error type for unknown outcomes instead of `SERVER_OVERLOADED`, which Yotsuba retries.
3. **Capacity and performance.** Benchmark against the single node on production-like data:
   - write latency and throughput, given the Raft fsync plus the one local transaction per entry;
   - reads behind the barrier;
   - forwarding overhead;
   - snapshot creation time and install time at 171 GB, and at the multi-TB message and file datasets;
   - the `RaftSettings` values: segment size, snapshot interval, entry size limit, and log retention in
     bytes, not only in entries.
4. **Snapshot transfer at TB scale.** Ratis installs restart from scratch after a failure, and are not rate
   limited. If that proves too slow, switch to install notifications plus a resumable pull of checkpoint
   files.
5. **Operations.**
   - Metrics and alerts: role, term, commit and apply lag, snapshot age, follower lag, replica failure exit.
   - A seeding runbook (copy the single-node database to all nodes before the first start).
   - A node replacement runbook.
   - Placement on separate hosts and disks.
   - The same Rockserver config file on every node.
6. **Known limits to decide on.**
   - `deleteColumn` waits for open iterators on the leader, which stalls apply there until they close or
     expire.
   - The result records of a proposer process that crashed are never deleted. Only the few that were in flight
     remain per crash; add a cleanup entry if crashes turn out to be frequent.
   - A result larger than the Ratis message limit makes a committed write look failed.
   - Snapshots are triggered by entry count only.
7. **Upstream.** Report the Ratis ReadIndex reuse bug, and switch back to Ratis ReadIndex once it is fixed
   upstream.
