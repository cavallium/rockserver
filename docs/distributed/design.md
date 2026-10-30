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

**Disk layout.** A node's data directory holds `db/` (the database) and `raft/` (the Raft log and snapshots).
Keep all of it on one file system and mount the data directory itself, not `db/`, `raft/` or a column volume.
A node refuses to start if the RocksDB `wal-path` is outside `db/`, or if anything under `db/` (column volumes
included, also through symbolic links) is on another file system. A column volume (`cf_paths`) outside `db/`
makes every snapshot fail. If `raft/` is on another file system, every snapshot copies the whole database
instead of hard-linking its files, and the apply waits for the copy; restoring a snapshot copies it too.

**What this module needs from core.** Single-node behavior and the wire contract are unchanged, apart from the
separate removal of CDC from Rockserver. The core additions are used only by replicas or by a server that
forwards to a non-embedded connection:

- `MergeOperatorRegistry.upload` is atomic, and validates the operator before writing anything.
- `GrpcServer`, for a non-embedded connection, runs the calls' callbacks on virtual threads instead of the
  Netty event loop (so one client connection is not limited to one thread), and runs requests on virtual
  threads of their own (a proxy scheduler,
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
| `RockserverStateMachine` | Ratis state machine. Leader admission in `startTransaction`/`preAppendTransaction`; entries are queued to an apply thread of its own, which applies them in groups; snapshots. |
| `Applier` | Applies the requests of log entries to the local database, a group of entries in one transaction without the WAL, with their result records and the applied position. |
| `LeaderSession`, `ConflictTracker` | Transactions bound to the leader's term, per-key queues of single-key updates, and conflict validation. |
| `NodeMetrics` | Timers and gauges of the replication path, logged every 10 s. |
| `ReadBarrier`, `TermService` | Linearizable reads. |
| `NodeStore`, `CheckpointStorage` | Local database, internal metadata, checkpoint snapshots, restore. |
| `LeaderForwarder` | Finds the leader's API and forwards to it. |
| `LogEntry`, `Request`, `LogEntryCodec`, `ResultCodec` | Log entry model and its binary encoding. |
| `ReplicatedServerMain` | Command line: `run`, `admin` and `repair-log`. |
| `RaftLogRepair` | Offline repair of a torn last write in the open segment of the Raft log. |

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

A client survives the loss of its node by listing every node (`GrpcConnection.forHostsAndPorts`, or
`http://a:5333,b:5333,c:5333`). One gRPC channel holds the whole list: a static name resolver hands it to the
`pick_first` policy unshuffled, so the channel uses the first address that works and, when that connection is lost,
starts again from the top. Nothing above the channel changes: a call in flight on the lost connection fails with
`UNAVAILABLE`, the reads allowed by the retry policy run again on the next node, and writes reach the caller, whose
outcome is unknown as it is for any lost call. Transactions and updates live on the leader, so a client that moves
between followers keeps them. A connection attempt to a node that does not answer ends at the connect timeout,
including a frozen process, whose kernel accepts the connection. See the
[README](../../README.md#client-connections) for timings and settings.

`pick_first` reacts only to the transport, so a node that stays reachable but cannot serve has to shed its clients
itself. A node cut off from the Raft majority answers HTTP/2 and keepalive while every request fails. So a node that
has known a leader and has had none for 3 maximum election timeouts, and at least 6 s, stops serving its API
(`ApiGate`, checked by the maintenance loop of `ReplicatedNode`): `GrpcServer.pause` closes the port and sends GOAWAY
to every connection, and running calls end as they do. Clients move on, because the port refuses them. The bound
leaves room for two more voting rounds lost to a split vote, so it is not reached by an election, and an isolated
leader first has to step down, about one more election timeout. The minimum is for a node that stopped for a moment:
a leader that got no answer from it sleeps before it tries again, and the node has no leader until then. Ratis
multiplies each step of the appender retry policy by a random number from 0.5 to 1.5, so the last step of its default
policy, `1ms,10, 1s,20, 5s,1000`, sleeps 2.5 to 7.5 s. `RaftSettings.APPENDER_RETRY_POLICY` lowers that step to 2 s,
at most 3 s of sleep, and the minimum is twice that, which leaves room for one more try if the first is lost. The
count of the last step is about a billion because past the end of the policy Ratis no longer sleeps at all: with the
default policy a follower down for about 40 minutes (a failed try counted about twice in a test run) would then get an
attempt at every heartbeat interval. A dead follower now gets about 2.5 times the attempts, which cost its leader about
0.15% of a core more and 20% more log lines, because Ratis batches those warnings every 5 s.

The gate lives in memory, so a node that restarts learns from its disk that a leader existed: its database has a
cluster id or its Raft log has an entry (neither alone is enough: a kill can lose a recent cluster id, and a purge can
empty a log). Such a node has known a leader, and the bound counts from its start, or a restart while cut off from the
majority would keep clients on a node that cannot serve them. Raft has its own port, so a paused node keeps voting,
and elections, genesis and the read barrier's term checks use only Raft ports: a whole cluster that takes longer than
the bound to elect a leader pauses until it does, and it then serves by itself. None of its nodes could serve before
that. What it costs is the capability handshake of a client that starts meanwhile, which needs an answer before
`GrpcConnection` returns: with several addresses it keeps asking while the answer is `UNAVAILABLE`, for 30 s by
default (`handshake-wait-ms`), and with one address it fails at once, as a client of a plain server that is down does.
A cluster that elects within the bound pauses nothing. A member of a new cluster has neither, and is not judged until
it first knows a leader. Once a node
has known a leader for one maximum election timeout, long enough for a heartbeat to have confirmed it, `GrpcServer.resume` builds
a new gRPC server on the same port. Both conditions must hold without a break, so a short election never pauses a
node, and a pause and the next are at least 4 maximum election timeouts apart. A cluster where no node has a leader
pauses every node and recovers by itself: elections go on, and every node resumes after the new leader appears. A
single-node embedded server never pauses.

Placement is deliberately simple, and the client knows nothing about the leader. All the clients with the same list
sit on its first node. After a leader change they stay on what is now a follower, and calls that need the leader
take a forwarding hop, until the connection is lost. A node that returns gets its clients back the same way, not at
once: a client stays on the node it failed over to until that node goes away.

A maximum connection age (`maxConnectionAge`, off by default, gRPC adds +/-10%, with 10 minutes of grace for running
calls) would bring clients back, because after the GOAWAY a client starts again from the top of its list. That is its
only benefit, and it is off because of its two costs. A silent first address then costs the connect timeout at every
age cycle, for every client, so a one-time cost becomes a recurring stall. And the grace cuts any call longer than it,
including `Compact` and long scans, on a healthy cluster too.

A hung disk is not detected: the process still answers HTTP/2 and keepalive, and calls wait for their deadline. It
would take a probe through the request path.

## Writes, exactly once

A proposal is a `Request`: a random proposer id (one per node process), a sequence number, `completedBelow`,
and the `LogEntry`.

- **Batches.** Requests that arrive while 32 batches are in flight wait, then go together as one log entry
  (`LogEntryCodec.encodeBatch`: the proposer id and `completedBelow` once, then each sequence and entry; up to
  4096 requests or 8 MiB; schema changes, `deleteRange` and genesis always alone). One entry costs one Ratis
  append, one place in a group fsync and one apply transaction, whatever it holds. Transaction commits have a
  queue of their own, and commits of different leader terms never share an entry.
- **Retries.** A batch is resent with the same bytes across leader changes until the latest deadline of its
  requests (at most one attempt past the oldest one's, and never past a member's caller deadline). A resend
  goes out only if it can start before that deadline: 50 ms after a "not ready", "not leader" or busy answer,
  100 ms after other failures. On the leader an attempt goes straight to the local Raft server. Elsewhere it is
  an independent blocking call on a virtual thread, through the Ratis client's RPC layer, to the leader that the
  node's own Raft server knows. Ratis' ordered async stream closes for good after one exhausted request, so it
  is not used. An attempt already sent waits for its reply for up to one attempt timeout (the larger of 3 s and
  twice the maximum election timeout, 4 s by default; on the leader, up to the later of that and the deadline),
  even past the deadline. Every attempt of a batch is the same
  Ratis invocation (client id and call id). A leader that has an earlier attempt pending, or applied it, answers
  from its retry cache, for as long as the cache keeps the entry (`RaftSettings`), instead of appending the
  batch again. Every replica fills that cache when it applies an entry, and every replica computes the same
  results, so a new leader answers the same way. A node that never saw the batch appends it again, and the apply
  deduplicates it.
- **When resends stop** without an answer, at the batch's earliest caller deadline or at the node's request
  timeout. A request with a caller deadline only shares a batch with requests whose deadlines are within an
  attempt timeout of its own. If no copy of the batch can have been appended (every attempt was refused before
  the append, as during an election), the requests whose own deadline came fail as not applied
  (`READ_DEADLINE_EXCEEDED` for a caller's deadline, `SERVER_OVERLOADED` for the node's, or a retry error if
  the last copy was refused by a leader that was not ready), and the others go back to the queue and are sent
  again under their own deadlines. A batch whose attempt ended without saying whether it was appended ends
  with an unknown outcome for every request instead: sending its requests again as a new batch would append
  them a second time while the first copy may still be pending.
- **Caller deadlines.** When the caller's request-context timeout ends before the node's request timeout, a
  write is resent only until the caller's deadline, less a margin of 20 ms (at most a tenth of the timeout). A
  write still queued at that point, or whose caller cancelled it before it was sent, is never sent; nor is one
  with less than 100 ms left (at most a quarter of the caller's timeout), which would get one hurried attempt. It fails with
  `READ_DEADLINE_EXCEEDED` and a message that says it was not applied, as on a single node, where a command that
  waited past its deadline fails without running. A write already sent cannot be withdrawn. The synchronous API,
  which `GrpcServer` uses for writes, returns at the caller's deadline with `READ_DEADLINE_EXCEEDED` and "outcome
  unknown", and the write may still be applied later. A transaction commit (or the write that ends an update)
  still queued at its caller's deadline is not sent either, but the synchronous API does not return at the
  caller's deadline for it: once submitted, its single attempt waits for its reply for up to one attempt
  timeout, even past the caller's deadline. The
  chunks of a `putBatch` or `mergeBatch` share one deadline, counted from the start of the call.
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

**Apply.** Every replica applies entries in log order, through the `EmbeddedDB` API, on an apply thread of
its own. `applyTransaction` decodes the entry on the Ratis thread, queues it and returns a future; the apply
thread takes everything queued (up to 256 entries or about 16 MiB) and applies it as one group; a third
thread then completes the group's futures, in log order. Ratis replies to a client, or counts an entry applied,
only when its future completes, after the group commits,
and it waits for these futures before it takes a snapshot; replacing the database with an installed snapshot,
taking a snapshot and closing first wait until the apply thread is idle. At most 256 MiB of entries wait for
the apply thread: beyond that, Ratis' thread waits, so a replica that falls behind keeps its backlog in the
Raft log on disk instead of decoded on the heap. Any error on the apply or completion thread, an
`OutOfMemoryError` included, stops the replica and fails every waiting reply.

- **Data writes** (puts, deletes, merges, transaction commits) of a group run in one local transaction
  without the RocksDB WAL. It also writes their result records, each proposer's floor and the applied
  position, so a crash keeps all of the group or none of it, a consistent prefix of the log either way. Each
  entry starts at a savepoint: if one of its requests is rejected for a logical reason, its writes are rolled
  back, the group so far is committed, and the entry is applied again on its own, one request at a time, only
  its last request recording the position. Positions without data (configuration entries, leader no-ops) are
  written in the group too.
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
  marker. The proposer then sends those requests again, in entries that hold only such copies: their
  sequences are still in flight, so the copies do not run and get their recorded results, each of which fits in
  a reply by itself. An entry of copies holds at most as many as the reply that marked them carried, and at least
  one: a reply can mark every result of an entry when they are all close to the limit, and the copies then go one
  per entry, several entries in flight together.
  A transaction commit is not sent again (the copy would conflict with itself); the leader that answered is
  this node, which applied it, so its record is read locally. Building the reply never fails. A reply too
  large to receive anyway fails its requests with an "applied" `INVALID_REQUEST` instead of being resent.

**Failures during apply.**

- **Logical rejections** (missing column, malformed keys...) are identical on every replica. They become the
  request's result and the log moves on.
- **Local failures** (native or IO errors, a lost local transaction) stop the replica:
  - its Raft server stops first, so a failed leader stops leading;
  - the process exits with code 3 (also when Raft closes under a running node: see "Long pauses");
  - it never skips the entry.

**Column ids** are native column family ids. Every replica gets the same ones because each runs the same
history from the same starting point. After a `createColumn` the leader proposes `VerifyColumn(name, id)`,
and a replica that assigned a different id stops. The internal column is created when a database is first
opened, before any entry is applied, and no other column is ever created locally.

**Durability.**

- **Raft log.** It is synced (`fsync`) on a majority before a write is acknowledged. It is the only log of
  data writes: they are applied without the RocksDB WAL, which would only write everything a second time.
  After a power loss in the middle of a group commit, the unsynced tail of the open segment (preallocated with
  zeros) reads back as a corrupt entry, and the default corruption policy keeps the node from starting.
  `WARN_AND_RETURN` would drop the tail, but in Ratis 3.3.1 it also truncates closed segments, which can hold
  committed entries. Instead, `ReplicatedServerMain repair-log --data-dir DIR [--dry-run]`, run on the stopped
  node, truncates the open segment at the first invalid entry. It does so only if that entry looks like a torn
  last write: every byte after the end it declares is zero; the size it declares agrees with the first fields of
  its proto, unless a lost sector took those; the end of the file cuts it, or one of its 512-byte sectors (aligned
  in the file) is all zeros, since a torn write misses at least one whole sector and a complete entry that fails
  its checksum is damaged media; and no frame with a valid checksum starts inside it. It checks
  every closed segment and that each segment starts right after the one before it, before it changes anything. It
  refuses and changes nothing (exit code 1) otherwise: replace such a node, or see "No quorum" under Membership
  if no leader is left to do that.

  The repair's safety rests on one assumption, which the bytes cannot prove and which its output states: the
  dropped entry was never fsynced, so this node never acknowledged it. The repair removes this node's copy from
  whatever quorum counted it. If the storage lost an entry it had reported as synced, or damage passed the checks
  (a damaged entry whose payload holds a zero sector), a committed entry has one copy less. With three nodes it can be left on the leader alone, and if that leader then fails, or
  loses its memory of the acknowledgement in the same power cut, the other two can elect a leader without it: an
  acknowledged write is gone, with no error. A repaired node stuck behind a leader that remembers its match index
  shows this fault, but only while that leader lives, so a node that catches up proves nothing. Without the repair
  such a node stays down and the cluster stalls instead. So keep the Raft log on storage that keeps fsynced data,
  and repair only after a power loss.
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
   index reaches `c`; replicated reads use `awaitLeadership`, which waits only for the entries of earlier
   leaders: every write is acknowledged only after the leader applied it (the result comes from the leader's
   apply), so once the entries of earlier terms are applied, whatever a client saw acknowledged is in the
   leader's database. Ratis counts a leader ready when it hands its first entry to the state machine, which
   only queues it for the apply thread; the state machine records, in `notifyLeaderReady`, the last index it
   handed to the apply thread until then (not Ratis' `getLastAppliedIndex`, which is the state machine's own,
   lagging, applied index), and `awaitLeadership` waits, at most a round timeout, for the applied index to
   reach it (the commit index, if Ratis has not reported the term ready yet). A new leader session also treats any transaction snapshot older than
   that index as conflicting, so no commit validates against a snapshot that misses those entries.

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
  node. A `getForUpdate` starts the barrier when it arrives, so the barrier runs while it waits for its key
  (with pipelined updates, below, before: it holds no key during the barrier).
  The caller's timeout counts from the request's arrival, barrier included: a transaction that finishes opening
  after its caller's deadline is rolled back at once instead of being held for its lease.
- **Lease cap.** A transaction's lease is at most 60 s (`NodeConfig.maxTransactionLease`,
  `--max-transaction-lease-ms`); a longer requested lease is shortened to it, for the native transaction too.
  An open transaction keeps the write history after its snapshot on the leader, so an abandoned one must not
  hold it for hours.
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
- **Pipelined updates** (`NodeConfig.pipelinedUpdates`, `--pipelined-updates`, off by default; it only changes how
  a leader behaves, so nodes may differ). A hot key's cycle above is a whole commit: fsync, replication and apply.
  With the flag on, the key passes to the next waiter when the commit of the update holding it passes
  `preAppendTransaction`, after the conflict check and `register`, not when it is applied. Acknowledgement still
  comes after apply.
  - **Chained read.** While its predecessor's commit is unsettled, the next update is *chained*: it reads the value
    that commit writes from leader memory, with no native transaction, under a positive id (native ids are at
    most -2, and chained ids hold the term). The value is **provisional**: the update reads something that may
    never be written, so its read counts only if the update ends well, by a commit that applies or a chained
    close that succeeds. A failed update's read is not part of the history. Plain reads never see that value.
  - **Guard.** A chained commit carries a `Guard`: the key, the value's length and its SHA-256. Every replica
    checks it at the commit's log position, inside the apply transaction, before any write (`Applier.execute`).
    A mismatch is a deterministic `UPDATE_RETRY` with no write. So a chained commit at position p is an atomic
    read-modify-write at p, whatever happened to its predecessor: the leader's chain state decides only who gets
    the key, and when it is wrong, the cost is a retry. The verdict depends only on the entry and the state at
    that position, so restarts and snapshots need nothing new.
  - **Grant.** One method grants a key. With unsettled links, it grants the newest one if the chain is not broken,
    has fewer than 64 links, and `ConflictTracker.chainIntact` holds: the link is the key's last writer, and no
    column or global write is newer than the applied index. Otherwise waiters stay queued until the links settle,
    and the next one is a head, which opens natively. A link settles after `applied.advance`, on the thread that
    completes applied entries, so a head always reads a state that holds it. A failed or unknown link
    (`broken`, only ever true while links are unsettled) stops chaining until the links drain.
  - **Chained close.** `closeFailedUpdate` of a chained update writes no log entry. It hands the key on at once,
    then waits at most 2 s for the commit it read from, on a copy of that link's future. It succeeds only if that
    commit applied; a dead base, a timeout, or an unknown positive id (after a leader change, or a lease that ended)
    is `UPDATE_RETRY`, and so is `closeTransaction` of such an id, a rollback included: a clean end would confirm a
    value that may never be written. A no-change close hands the key on without making a link, so the next update
    reads the same one: 64 updates may read a link, and more queue for the key until it settles, like with the flag
    off. A proxy `GrpcServer`, with the flag on or off, serves `closeFailedUpdate` on the async API, on the call's own
    virtual thread, with must-complete semantics: the wait holds no CONTROL worker, and no queue can refuse a close,
    since a refused close would keep its key for the whole lease. So does its close of an update whose caller was gone
    by the time it opened.
    It linearizes right after that commit: every writer ordered after it was registered after the grant, which
    came after the read barrier, so after the close's update was invoked. That is why the barrier stays on both
    paths, and why the grant check is needed for this and not only as an optimisation.
  - **What falls back.** A commit that is not one put or delete of the locked key (a merge, several keys, another
    key) or a value over 1 MiB gives no link: the key is held until the reply, as with the flag off. Any operation
    that needs a snapshot on a chained id (`get` with the id, ranges, iterators, a second `getForUpdate`,
    `closeTransaction`) ends the update with `UPDATE_RETRY` and makes the next one a head. A second write on an update
    whose commit is in flight fails alone with `UPDATE_RETRY`: an update has one commit, so a failed commit can only
    fail the link it made itself. A cold key costs only the bookkeeping.
  - **Transactions.** A transaction that touches a key with an unsettled link conflicts at commit, whatever it waits
    for: its snapshot is fixed when it opens, and a link that is unsettled after that is newer than the snapshot even
    once applied, so only a new transaction can pass. What it needs is a moment with no unsettled links, and a key
    that chains without a pause has none. So a commit that conflicts on such a key also stops the chain there until
    its links settle (`LeaderSession.yieldChainsTo`, counted by `transaction.chainConflict`, among
    `transaction.conflict`), and the transaction's retry finds room, as it does with the flag off. A key's updates pay
    for that only while transactions collide with them. In `HotKeyThroughputTest`, in-process, with one client
    running transactions on the key in a tight loop, four runs of one commit on a busy machine (load 8 to 15) gave,
    beside 32 update clients, 42–137 commits/s with the flag off and 27–39 with it, and updates of the key at 268–767/s
    off and 1455–1981/s on. Beside 4 update clients, 68–129 commits/s off and 30–42 on, and updates at 387–799/s off
    and 960–1250/s on. Without the pause, in one earlier run, 0.2 commits/s and 13400 updates/s beside 32 clients, and
    13 commits/s beside 4. Single runs differ by up to three times: read these as orders of magnitude.
- **Staged writes** happen natively. The leader also records them, with their conflict keys: the written keys
  plus the `getForUpdate` keys. It counts the encoded size of the commit entry as they arrive. A write or
  `getForUpdate` that would take it past the 16 MiB entry limit fails at once with `INVALID_REQUEST` and is not
  staged; the transaction stays open without it, as after a failed write on a single node.
- **Commit** proposes one `Guarded` entry that carries the writes, the snapshot index and the conflict keys.
  It is submitted straight to the local Raft server in a single attempt, so a refusal really means the entry
  was not appended.
- **Open** also checks that the leader session belongs to the term the read barrier confirmed. A node that lost
  and regained leadership meanwhile might not have applied, or tracked, what the other leader committed.

**Validation.** Ratis calls `preAppendTransaction` under its log write lock, in log order.

- **Check.** It checks the conflict keys against `ConflictTracker`, a map from key hash (or whole column) to
  the index of the last entry that wrote it, filled as entries are appended. A key written after the snapshot
  index, including by an entry still in flight, is a conflict. A chained commit reads no snapshot: on the key it
  read it conflicts only with a last writer other than its base, and its index only pins the history for the
  rest (columns, other keys). Its base may be forgotten once applied, which is no conflict: the guard decides.
  One whose base failed, or whose update expired, always conflicts.
- **On conflict** commit returns `false` and the transaction stays open, as on a single node. Commits share
  entries: the leader rejects the entry before appending it and names the commits that conflict (with history,
  or with an earlier write of the same entry); those fail and the others go again in the next entry.
- **Outcome of a commit.** It is submitted once, to the local Raft server. Only a refusal that Ratis gives before
  appending (leader not ready, no pending-request permit, or the state machine's own refusals) means "not
  committed"; `NotLeaderException`, which a leader stepping down also returns for appended entries, is an
  unknown outcome.
- **Otherwise** the entry is appended and later applied atomically on every replica (unless a chained commit's
  guard rejects it: see above). The leader's native transaction is then rolled back, because its effects arrive
  through the log.
- **Hash collisions** can only cause false conflicts.
- **History is kept** as long as any open transaction, reservation or in-flight commit pins a snapshot. A commit
  whose snapshot is older than history that was already forgotten always conflicts, whatever released it, so a
  timing mistake can only cause a false conflict, never a missed one.
- **History is bounded.** Once the applied entries in the tracker hold more than 4M keys (about 300 MB), the
  oldest are forgotten even if a snapshot still needs them, and the transactions with that snapshot conflict at
  commit. Pruning forgets about 64k keys per call, and runs after every applied entry, so it never holds the
  tracker lock (which appends need) for long.

**Leader changes.** Transactions, update ids and iterators do not survive a leader change. Clients get
`UPDATE_RETRY` and run the transaction again, which Yotsuba already does. A commit of a transaction that
another leader opened is refused before it is appended (the term check), so it can never be validated
against an incomplete history.

## Snapshots, catch-up and membership

**Snapshot.**

- **Building it.** `takeSnapshot` builds a checkpoint from RocksDB's live-file APIs, with file deletions
  disabled:
  - memtables are flushed;
  - SST and blob files are hard-linked (copied, while the apply waits, if `raft/` is on another file system;
    see Disk layout);
  - the MANIFEST prefix is copied and `CURRENT` written;
  - the WALs still needed are copied.
- **Why not `org.rocksdb.Checkpoint`.** It refuses the column volumes (`cf_paths`) that the default config
  uses.
- **When.** Every `snapshotEveryEntries` applied entries (default 500k), not at stops (a graceful stop flushes
  the database instead). Taking one flushes the memtables and copies the live files, 100–300 ms, during which
  Ratis hands over no entry: about every 100 s at 2×, writes wait that long. (Taking it on a thread of its own,
  answering Ratis with the previous snapshot meanwhile, was tried and dropped: a result finished before a
  snapshot install could then be returned after it, behind Ratis' snapshot index, and a copy finishing during
  an install could land among the installed snapshot's files.) Files that are hard links of durable files are
  not fsynced again.
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
- **Disk cost of snapshots.** A retained snapshot keeps the table files it links, also after compaction
  replaced them in the database. Budget disk space for the database plus at least one older snapshot's
  compacted-away files, more while installs run. On a follower, a failed install leaves its partial copy until
  the server restarts or a newer install starts.
- **Damaged installs.** A snapshot that a crash damaged during install makes the replica fail loudly; it is not
  silently ignored.

**Membership.**

- **Initial cluster.** All nodes start with the same peer list. The first leader proposes `Genesis`, which
  binds every replica's database to the cluster and records whether the cluster was seeded from an existing
  database.
- **Migrating from a single node.** Stop it and copy its database directory to every node. A seeded cluster
  takes a snapshot right away, and a member added later through `add-peer` receives the leader's latest snapshot
  before any log entry, so it gets the seed. A node that replays the seeded `Genesis` with an empty database (an
  initial member without the copy, before the log is purged, or a new member while the leader has no snapshot
  because that one attempt failed) stops instead of diverging.
- **Replacing a node.** Start the new member first, under a new id, with an empty data directory, a peer list
  that includes itself and `--join`. Then run `ReplicatedServerMain admin --peer ... add-peer` and `remove-peer`
  (Ratis `setConfiguration`), whenever you like: the new node waits, also across restarts. `admin` does not read the
  current configuration: it sets it to exactly its own `--peer` list plus or minus one peer, so `--peer` must list
  every current member, or the ones left out are removed. New members catch up from a snapshot. A node that lost its
  Raft state but kept its id refuses to start, because it could vote twice in one term. Replace it under a new id.

  `--join` gives Ratis an empty group, so the node starts in Ratis' initializing state: no election timer, until a
  leader sends it a request that is not part of bringing it up to date. Without the option the node would start
  elections. A leader that is not of the first term answers a pre-vote from a node outside its configuration with a
  request to shut down (its configuration entry has an index, which it compares with the candidate's log), Ratis
  closes that node's division, and `add-peer` then hangs with no end. So a node that replaces a lost leader had 1 to 2 s
  to get its `add-peer`. Now such a node exits (see "Long pauses"), and `--join` keeps it from getting there. The
  option does not change a node that has a configuration of its own, from its log or from a snapshot, so it can stay in
  the node's command line; a node of a running cluster also restarts without it. It leaves nothing on disk: a node
  restarted without it before its `add-peer` is an ordinary new member, with the election timer above.

  A node still running when `remove-peer` removes it closes Raft by itself and exits with 3. Take it out of its
  supervisor first, or the supervisor keeps restarting a node that has no group to join.
- **No quorum.** Replacing a node takes a leader (`setConfiguration`), so it does not help when too few nodes
  can start: two of three after a shared power cut, for example, each with a hole in a group commit followed by
  later data, which `repair-log` refuses. Writes through a volatile device cache can complete out of order within
  one fdatasync, so this can happen under load. There is no override, on purpose: a log that `repair-log` refuses
  may hold the only copy of an acknowledged write, and forcing it could elect a leader without that write and hide
  the loss. Meanwhile the cluster stays down and loses nothing. What exists:
  1. Stop every node and copy every data directory before anything else.
  2. Run `repair-log --dry-run` on every node, repair the ones it accepts and start every node that can start. Any
     majority of the members elects a leader, repaired or not, and the refused nodes can then be replaced as above,
     with `--join`: the new node waits for as long as the leader takes.
     A refused node stays a member until then, so it still counts in the size of the majority.
  3. Otherwise, restore a copy of the whole cluster: every node's complete data directory, from one moment, under
     the same ids. The writes since then are lost. Restoring only some nodes from an older copy is the fault this
     section is about, losing fsynced entries and votes. Nothing takes such copies today: they have to come from the
     storage layer (see "Left before a first deployment").
  4. Without a copy, each replica's `data/db` still holds its last flushed state, which a plain Rockserver can open.
     It cannot seed a new cluster as it is, since it belongs to the old cluster and a node without Raft state refuses
     it, so its keys would have to be exported into a new database that seeds one ("Migrating from a single node").
     What the replica had not flushed, and all that only a Raft log holds, is lost: no tool reads it out. No tool or
     test covers this way, which is a last resort.
- **Graceful stop.** On SIGTERM a leader hands leadership to the most up-to-date live follower before
  stopping. A deploy restart costs a few hundred milliseconds without writes (see the tests).
- **Long pauses.** Ratis closes its server after a JVM pause longer than a limit, and a division also closes when a
  leader tells a node outside its configuration to shut down. Both leave the process up: the API listens, the Raft
  port refuses connections, and a closed division still reports its last leader, which it can no longer follow. So
  the maintenance loop of `ReplicatedNode` watches the division, and a node whose Raft closed while it was running
  stops like a failed replica: it exits with code 3, with `Raft closed` in its error, so that a supervisor restarts it.
  Its clients then move at once, and it comes back from its disk like any restart. The limit stays at Ratis' 60 s, set
  by `--jvm-pause-limit-ms`: a pause that long comes from a machine in trouble (swap, a stuck disk, a stopped VM), and
  a restart is the recovery path that every test covers. Raft stays safe through any pause, so a higher limit is safe
  too: it trades that restart for a node that resumes by itself. Without a supervisor the node stays down until an
  operator starts it.
- **A new leader whose first entry gets no reply.** A leader appends an entry when its term starts and refuses writes
  until that entry is committed. Ratis moves a follower's match index only on the reply to a request with entries:
  `AppendEntriesReply.matchIndex` is -1 on a heartbeat. It moves the next index when it sends an entry, and keeps it
  when a stream fails with no call id (`keep nextIndex unchanged`). So with one node of three down, if the other
  follower wrote the entry and its stream died before the reply, as when it restarts just then, no heartbeat can ever
  report the entry, and no write can carry a later one. On a real disk this took 189 s and more, with no end, and fsync
  decides the race: on a tmpfs the reply gets out first. `LeaderStall`, in the same maintenance loop, makes such a
  leader step down, with a `TransferLeadershipRequest` with no new leader, which works on a leader that is not ready.
  The rule: no entry of the term committed, the commit index not moving for 5 maximum election timeouts and at least
  10 s, and the whole log sent to a majority of members that still answer. It does not look at what is applied, so a
  long replay after a restart is no stall. A follower still catching up, from the log or a snapshot, has not been sent
  everything, so it does not count; a step down would start it over. A leader without such a majority does not step
  down this way: Ratis does that by itself after one election timeout. The cost is about 12 s without writes, in a case
  that had no end. The log says `stepping down to restart replication`, and the metric `leader.stall.stepdowns` counts.
  After its first commit a leader cannot stall like this: the next write it accepts has entries, and its reply moves
  the match index past the entry whose reply was lost. An uncommitted entry waits for that write, and only the client
  that wrote it waits with it, until its deadline. If a partner's disk hangs and heartbeats still pass, the leader
  steps down once per bound and the other side does the same: elections, and no writes, which is what that fault
  gives anyway.

## Behavior differences from a single node

- **Leader changes.** Transactions, update ids and iterators do not survive one. Clients get `UPDATE_RETRY`.
- **Outcome unknown.** When no leader answers within the request timeout, the error is `SERVER_OVERLOADED`.
  Its outcome is unknown, and the write may still take effect later, unless its message says that the write
  was not applied (no copy was ever appended). Internal retries are exactly once, but a client that resends the
  same write after an unknown outcome can apply it twice. A deadline error (`READ_DEADLINE_EXCEEDED`, or gRPC's
  `DEADLINE_EXCEEDED`) on a replicated write also has an unknown outcome, unless its message says that the write
  was not applied.
- **`putBatch`/`mergeBatch`** run as ordered replicated write batches, whatever the requested SST mode. They
  are not atomic in any mode: each chunk of at most 4 MiB is applied on its own, and the chunks applied before a
  failure stay applied. When a chunk fails after an earlier one was applied, the call fails with
  `INVALID_REQUEST`, whose message gives the number of chunks and keys applied, instead of the chunk's own error.
  That error could be retryable, and resending the whole batch would apply the first chunks twice. A failure of
  the first chunk keeps its own error.
- **Size limit.** One replicated entry (one write, multi-write or transaction commit) is limited to 16 MiB. A
  transaction write that would take its commit past that fails with `INVALID_REQUEST` when it is made.
- **Transaction leases** are at most 60 s by default (`--max-transaction-lease-ms`), whatever the client asks
  for: a transaction that runs longer expires, and its next operation or commit fails with a retry error, so a
  client that retries it may never finish it; raise the cap for such clients. A transaction whose snapshot is
  older than the last 4M keys written on the leader conflicts at commit.
- **`scanRaw` SST tokens** describe the leader's own files, so a leader change invalidates them.
- **Merge operators** must be deterministic.
- **Maintenance.** `flush` and `compact` act only on the node that receives them.
- **Workload limits.** The replicated server's `GrpcServer` runs requests on virtual threads of their own (at
  most 16384 waiting on the leader), not through each column's workload scheduler. Streamed mutations run at
  most 256 at once. On the leader, point reads, single-key writes staged in a transaction and transaction
  opens run each on a virtual thread of their own (one that waited past its deadline fails without running).
  Rollbacks (`closeTransaction` without commit, `closeFailedUpdate`) run at once on the thread that received
  them. Ranges, multi-key reads, multi-key staged writes and iterator calls use the embedded workload scheduler.
- **Updates of one key run one at a time** on the leader, unless `--pipelined-updates` is on: their reads and
  commits then overlap, and only the time up to each commit's admission is exclusive. Nested or crossed updates
  of the same keys by one client block (up to 12 s) instead of conflicting at once, as long as the first has not
  committed.
- **With `--pipelined-updates`, a `getForUpdate` value is provisional** until the update ends well, so nothing
  may act on it first. `closeFailedUpdate` of such an update can wait (up to 2 s) and can fail with `UPDATE_RETRY`,
  which on a single node it never does: code that uses a no-change close to confirm what it read must retry on
  it. Its update id is positive, and a call that needs a native snapshot on it fails with `UPDATE_RETRY`. A hot key
  almost always has a pending write, so transactions that touch one conflict more often.
- **After a kill** (not a graceful stop), a replica's database files are an older consistent point until it
  starts again and replays its log.

## Tests

All tests use real Ratis servers over real sockets. The multi-process tests start separate JVMs, each with its
own hosts file. Every directed link between two nodes goes through its own TCP proxy on a distinct loopback
address, so a test can partition any set of links. The cluster tests run with 400:800 ms election timeouts
(`--election-timeout-ms`); the default is 1000:2000 ms, so failover takes longer in production (see "Write
failover" below).

| Test | What it proves |
|---|---|
| `ReplicationTest` (in-process) | Replication through any node; results; leader loss, including a new leader that has not applied what the old one acknowledged yet; snapshot catch-up; batches; transactions, conflicts and updates; a transaction write too large for the commit refused at once; the capped transaction lease; updates whose caller timed out or cancelled while queued, or cancelled while the update opened, do not keep the key; transactions lost on a leader change; whole-cluster restart; recovery after a proposal timeout. |
| `ApplierTest` | At-most-once retries, including after a restart; deletion of records, by count, by bytes and at start; results too large for a reply; rejections; atomic commits; detection of column id divergence; batches with duplicates and rejections; groups of entries, with copies and rejections in them; crash consistency of applies without the WAL, and of groups. |
| `LeaderSessionTest` | Per-key update queue (order, release on commit, expiry also without other traffic, and end of leadership); chains (handoff at admission, the caps on links and on the readers of one, breaks and their end, a pause for a conflicting transaction, duplicates, expiry, close, no session monitor under the log lock); history kept for open transactions and in-flight commits, up to the key cap; the commit size limit of staged writes. |
| `PipelinedUpdatesTest` (in-process, flag on) | With the leader's apply stopped: chained updates see each other's values and nothing is acknowledged; wrong guards; plain writes, transactions and `deleteRange` against a chain; closes in mid-chain, and more of them waiting than a server's CONTROL workers and queue hold; refused operations on chained ids; second writes; merges and multi-key commits; caller gone, abandon; a snapshot and a restart inside a chain, around a rejected entry; a leader change with a chain in flight; a commit that ends without an outcome; the flag on and off at restarts, and a chained id handed to a leader without it; concurrent counters. With the flag off: the commit of an update, read back from the Raft log, has the old bytes, and a second write goes to the log. |
| `PipelinedLeaderKillTest`, `PipelinedPartitionTest`, `PipelinedRandomFaultTest` | The suites of the same name, with the nodes on `--pipelined-updates`. With `PipelinedUpdatesTest`, the only tests that chain in a default run: the suites below run the plain per-key queue unless the property below is set, and fail if an update chains there. After each fault these three wait for an update that chains. |
| `LeaderKillTest` | Repeated SIGKILL of the leader under concurrent writes. No acknowledged merge is lost or doubled, no acknowledged increment of a hot counter is lost, and replicas are byte-identical. Writes resume in about 1 s (the test only requires under 10 s). |
| `ForwardingFailoverTest` | A client of a follower sees only retryable errors (point reads, range streams, writes) while the leader is killed, and every kind of request succeeds again after the election. |
| `ClientFailoverTest` | One client connection listing all three nodes, while the node it uses is killed (the leader, or a follower): only retryable errors, new calls succeed on another node of the same connection (about 1 s with an election, under 200 ms without), a new connection skips the dead node, and a transaction opened through a dead follower still commits. |
| `IsolatedNodeTest` | A client listing every node, connected to a node that gets isolated (the leader, or a follower), moves to the majority and keeps working, and the isolated node listens and serves again after the heal. With every node isolated all pause, and after the heal all resume and the client recovers by itself. An election shorter than the bound pauses no node. A follower restarted while cut off pauses too, and the client that lists it first moves. A cluster killed or stopped as a whole and restarted at once serves again, a paused node takes part in the election, and a node of a new cluster does not pause. A node that lost its database and kept its log pauses, and a client that starts during a cold start waits for the cluster. |
| `PartitionTest` | An isolated leader acknowledges nothing. The majority takes over. A leader that keeps its majority keeps serving. The history, hot counters included, is linearizable. |
| `PauseTest` | SIGSTOP of the leader and of a follower. The history stays linearizable. A freeze longer than `--jvm-pause-limit-ms` ends the node with exit code 3, and it starts again. |
| `LeaderStallTest` | The rule of `LeaderStall`. With one node down, the reply to a new leader's first entry is lost on a link, after the entry reached the follower's disk: nothing commits and the leader stays, until it steps down and a write gets through. The same loss after the first commit heals with the next write. A leader behind in applying does not step down. |
| `RollingRestartTest` | Graceful restart of every node, twice, under load. No failed operation. A leader restart stops writes for about 200–300 ms. |
| `MembershipTest` | A node started with `--join` replaces a lost leader, however long before its `add-peer` it started and after a restart in between, catches up from a snapshot, and restarts with or without the option. A node removed while it runs exits. Seeded bootstrap works. An empty node added right after a seeded start receives the seed through the snapshot taken after genesis, and never replays the seeded `Genesis`. |
| `ColdRestartTest` | SIGKILL of all three nodes under load, 5 cycles, plus a 2-of-3 restart. Nothing acknowledged is lost, and replicas are identical. |
| `RandomFaultTest` | A seeded random schedule of kills, partitions, pauses and restarts. Register histories, with read-modify-write updates and hot counters in them, are linearizable, and the transactional bank-transfer invariant holds. |
| `RaftLogRepairTest` | Segments written by Ratis' own output stream, torn at sector boundaries: torn entries are dropped, and Ratis then opens and appends to the log; anything else is refused and left unchanged: a damaged complete entry, a size that hides a valid entry, a gap or overlap between segments, a problem in a closed segment; a dry run changes nothing. A leader, and a follower that never acknowledged its last entry, refuse to start with a torn entry and rejoin after `repair-log`. So does a leader that loses a whole open segment after closed segments, a snapshot and a purge. Two or three nodes torn by one power cut, in entries no quorum confirmed, repair and elect a leader among themselves with every acknowledged write. No test interrupts a real device: the torn bytes are written by the test. |
| Component tests | Codecs (including fuzzing), `ConflictTracker` (checked against a reference model, also with a capped history; pruning in bounded steps), `ReadBarrier` (deposed and isolated leaders never pass), `NodeStore`/`CheckpointStorage`, `LeaderForwarder` (leader lookups still waiting fail on close), fault proxy, linearizability checker. In the core project, `GrpcProxyCloseTest`: closes that answer late, on a proxy `GrpcServer`, take no worker, are not cancelled with their caller, and hold up shutdown after their caller left; the close of an update whose caller left while it opened is asynchronous too; a retry of either close, with no caller left, is no late failure. |

Run them all (about 15 minutes):

```bash
mvn -o -B -f distributed/pom.xml -pl server -am test -Dtest='it.cavallium.rockserver.distributed.**' -Dsurefire.failIfNoSpecifiedTests=false
```

The suite must also pass with its directories on a real disk: JUnit puts them in `/tmp`, and fsync costs nothing on the
tmpfs that `/tmp` often is, which hid a stall that a real disk shows every time (see "A new leader whose first entry gets
no reply"). Timing on a tmpfs is not timing on a disk, so run it both ways before calling a change done:

```bash
mkdir -p /var/tmp/rockserver-test
mvn -o -B -f distributed/pom.xml -pl server -am test -Dtest='it.cavallium.rockserver.distributed.**' -Dsurefire.failIfNoSpecifiedTests=false -Drockserver.test.tmpdir=/var/tmp/rockserver-test
```

`-Drockserver.test.pipelinedUpdates=true` runs every test cluster with pipelined updates on. Hot counters also
require, in both modes, that every key takes a new update well within a lease once the cluster settled, and, with the
flag on, that an update chained after each fault, and with the flag off that none did. `HotKeyThroughputTest` runs only with `-Drockserver.test.slow=true`.

## Left before a first deployment

0. **Pipelined updates** stay off until the benchmark on hot keys (see [performance.md](performance.md)) and a full
   review of the Yotsuba call sites: a chained value must not be acted on before the update ended well, and
   `UPDATE_RETRY` from a no-change close must be retried. Watch `update.guardRejected`, `update.chainConflict`,
   `transaction.conflict` and `transaction.chainConflict`. Every node must run a version with the flags of `Guarded`
   entries before any node turns the option on: an older one would fail on the entries of a pipelined leader. With the
   option off, entries keep the bytes they always had, and logs written before these flags stay readable.
1. **Client side.**
   - Yotsuba does not compile against this version as is: CDC was removed from the client API, and
     `server/source/api/src/main/java/org/yotsuba/database/RockserverAsyncApiViews.java:133` still overrides
     `cdcPollBatchAsync` (and imports `it.cavallium.rockserver.core.common.cdc.CdcBatch`). It is a pass-through
     that nothing calls: delete it, and the few Yotsuba tests that mention it. telegramdb has no CDC references.
   - Yotsuba must create its connection with every node's address (`GrpcConnection.forHostsAndPorts`, see
     "Routing"): the core client fails over between them, but Yotsuba passes a single address today.
   - The core client's automatic gRPC retry of reads waits for its backoff (0.5 s, then growing) before it sends
     a read to the next node. Tune it for cluster use if that matters.
2. **Exactly-once for client retries.** A client request id in `RequestContext`, mapped onto `Request`
   identities, would make a client-level retry after an "outcome unknown" error safe. Also consider a
   dedicated error type for unknown outcomes instead of `SERVER_OVERLOADED`, which Yotsuba retries.
3. **Capacity.** See [performance.md](performance.md) for the measurements on one PC and the load benchmark.
   Still to measure on the production machines and data: their fsync latency, snapshot creation and install
   time at 171 GB and at the multi-TB datasets, and log retention in bytes. Raise Yotsuba's in-flight request
   limits for the cluster (see performance.md).
4. **Write failover after a frozen leader.** When the leader freezes (SIGSTOP, a long GC pause, a hung disk),
   writes resume about 4 s later in the tests, although the election takes about 1 s there and reads resume
   sooner. The cause is Ratis' client closing the channel to the frozen leader, which waits a hard-coded 3 s
   (`GrpcUtil.shutdownManagedChannel`). Crashes and graceful restarts are not affected (in the tests, failover
   about 1 s, hand-off about 250 ms). Fix by keeping per-peer clients or by reporting it upstream. These figures
   are for 400:800 ms election timeouts. With the defaults (`--election-timeout-ms 1000:2000`,
   `--request-timeout-ms 10000`), the benchmark saw about 3 s without a successful operation after a SIGKILL
   of the leader (performance.md), and an attempt sent to a frozen leader waits 4 s instead of 3 s before it
   is resent.
5. **Snapshot transfer at TB scale.** Ratis installs restart from scratch after a failure, and are not rate
   limited. If that proves too slow, switch to install notifications plus a resumable pull of checkpoint
   files.
6. **Operations.**
   - Start nodes with `-Djava.io.tmpdir` on a directory of the node. RocksDB's Java binding extracts its 17 MB
     native library there at every start, and a killed JVM leaves it behind. On a test machine 2,099 such files
     filled a `/tmp` quota, and new nodes then could not start.
   - Metrics and alerts: role, term, commit and apply lag, snapshot age, follower lag, replica failure exit.
     Today the replication timers (`NodeMetrics`) are only logged. The existing `rocksdb_operation_timer`
     changes meaning on a cluster: replicated writes are counted on every node when applied (with the internal
     result, floor and position puts and one `closeTransaction` per applied group), and reads, staged
     transactional writes, opens, rollbacks and ranges only on the leader, with their local database time only.
   - A seeding runbook (copy the single-node database to all nodes before the first start).
   - A node replacement runbook.
   - Consistent copies of the whole cluster, and a runbook to restore them: with no quorum (see Membership) they
     are the only way back that keeps a cluster.
   - Placement on separate hosts, each node's data directory on one file system (see Disk layout).
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
