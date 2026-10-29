# Replicated Rockserver: target design (proposal)

Status: proposal, 2026-10-29. It replaces the Codex design (`architecture.md`);
the reasons are in [review.md](review.md).

## Goal and non-goals

**Goal.** Yotsuba becomes a stateless client of Rockserver. Rockserver itself
must therefore keep serving through:

- rolling restarts (deploys happen about 3 times a day);
- the crash or permanent loss of one node;
- a network partition that isolates one node.

No acknowledged write may be lost, and reads must stay linearizable.

**Non-goals for v1:**

- horizontal write scaling or data partitioning (sharding);
- multi-datacenter deployment;
- cross-shard transactions;
- online resharding.

Every node holds the full dataset, just as the single node does today. If
partitioning is ever needed, it can be added later as N independent copies of
this design with routing in the client. Nothing below prevents that.

## Shape

```
              Yotsuba (unchanged Rockserver client API, gRPC)
                 │ any node; followers forward to the leader
   ┌─────────────┼───────────────────────────┐
   ▼             ▼                           ▼
 node A        node B (leader)             node C
 GrpcServer    GrpcServer                  GrpcServer       ← existing core server, unchanged
 ReplicatedConnection (RocksDBConnection)                    ← new; routes each API command
 EmbeddedDB    EmbeddedDB                  EmbeddedDB       ← existing engine, unchanged
 Ratis server ─── Raft log replication (stock Apache Ratis 3.3.1, gRPC) ───
```

- **One Raft group with three voting replicas.** Each replica is a normal
  `EmbeddedDB` holding the full dataset, with the same column schemas, merge
  operators, bucket hashing, workload scheduler and native JNI as today.
- **Stock Apache Ratis, unmodified.** We work around the one upstream read bug in
  the read barrier (below) and report it upstream.
- **New code lives in a single module, `distributed/`.** It depends on
  `rockserver-core` and Ratis. Core changes are limited to small, additive hooks,
  if any are needed at all. The single-node server, its config and its wire
  contract stay byte-for-byte unchanged.

## Write path

The core API is already a sealed set of commands (`RocksDBAPICommand`), and
`RocksDBConnection` dispatches them. `ReplicatedConnection` sorts each command
into one of five classes:

| Class | Commands | Handling |
|---|---|---|
| Replicated write | `put*`, `putMulti*`, `delete*`, `deleteRange`, `merge*`, `putBatch`/`mergeBatch` (as chunked write batches), `createColumn`, `deleteColumn*`, `uploadMergeOperator` | Encode as a log entry, submit to the local Ratis server, return the result computed by the leader's apply. |
| Linearizable read | `get`, `exists*`, `getMulti`, `reduceRange*`, `getRange*`, `getRangePage`, `getColumnId`, `getAllColumnDefinitions`, `checkMergeOperator`, `estimateNumKeys`, `scanRaw*` | Wait for the read barrier, then run against the leader's local `EmbeddedDB`. |
| Leader-bound session | `openTransaction`, `getForUpdate`, operations inside a transaction or update, `closeTransaction`, `closeFailedUpdate`, iterators | Leader-local, bound to the leader's term (below). |
| Local maintenance | `flush`, `compact`, `compactFiles`, `getSstMetadata`, `getTableProperties` | Run on the node that receives the request. Not replicated. |
| Unsupported in v1 | `cdc*` | `NOT_IMPLEMENTED`, with a clear message (see open items). |

**Followers.** A follower forwards every non-local command to the current leader
over the leader's ordinary client port, using the existing `GrpcConnection`.
Clients can therefore connect to any node; the extra hop costs latency only.

**Apply.** All three replicas apply the same entries in the same order through
the existing `EmbeddedDB` API.

- **One local transaction per entry.** Each entry runs inside a short local native
  transaction. That transaction also writes the Raft `appliedIndex` into an
  internal column, then commits. So the entry's effects and its index become
  visible together, and a crash cannot leave one without the other. Replaying the
  log after a crash then skips every entry `≤ appliedIndex`, so merges are never
  applied twice.
- **Commands that cannot run in a transaction.** Schema operations and
  `deleteRange` are idempotent at a given log position, so they write the index
  in a separate, following step.
- **Deterministic results.** Return values such as previous value, delta or
  merged value are computed by apply, so every replica computes the same result.
- **Column IDs.** IDs are native column family IDs. They are identical on every
  replica because every replica runs the same column-creation history from the
  same starting point. Apply checks this on every `createColumn`, and a mismatch
  stops the replica. That is the same rule as for any other non-deterministic
  apply.
- **Failures.** A business error inside apply, such as "column exists with a
  different schema", is a result: it is returned to the caller and the log moves
  on. An unexpected storage or native failure stops the replica (its Ratis
  server is shut down and the error logged) instead of skipping the entry. It
  then has to be repaired from a snapshot. It never poisons the group.

**Durability.**

- **Two logs.** The Raft log is synced to disk (`fsync`) on a majority before a
  write is acknowledged. The RocksDB WAL stays on, unsynced (`sync=false`), for
  crash-safe recovery across column families. Both logs are kept on purpose: the
  RocksDB WAL is unsynced, so this costs no extra durability barrier.
- **After a power loss.** RocksDB recovers a consistent prefix, and Raft replays
  the rest.

## Read path and the read barrier

Upstream Ratis 3.3.1 ReadIndex can return stale reads: it reuses an old
heartbeat round when the commit index has not moved. See
[review/ratis-fork.md §2a](review/ratis-fork.md). We therefore implement the
ReadIndex check ourselves, using only public Ratis APIs.

1. A read arrives at the leader at time `t0`. The leader waits until it has
   committed an entry in its own term (`isLeaderReady`), then records
   `c = commitIndex`.
2. A confirmation round that **starts after `t0`** asks every peer for its current
   term. A tiny service on the Ratis gRPC port answers with
   `DivisionInfo.getCurrentTerm()`.
3. If a majority of the current configuration report the leader's term `T`, the
   read may proceed. During a membership change this must hold in both the old
   and new configurations, and the leader counts itself. If any peer reports a
   higher term, the read fails as retryable.
4. Wait until local `appliedIndex ≥ c`, then read from the local `EmbeddedDB`.

Why this is safe: if a leader with term above `T` had committed anything before
`t0`, a majority would have moved to that higher term before `t0`. Terms never
go back. Any majority that reports `T` after `t0` overlaps that majority, which
is a contradiction. So `c` covers every write committed before the read began.

Reads that arrive while a round is in flight wait for the next round, so one
round serves a whole batch of reads. A round costs one network round trip and
no `fsync`. A lease-based mode can be added later as an opt-in.

Snapshot isolation for iterators and range pages is RocksDB's own, on the
leader's local `EmbeddedDB`.

## Transactions

Production uses `OptimisticTransactionDB`. Yotsuba relies on:

- conflicts across columns;
- `getForUpdate` fences;
- `false` from commit meaning "conflict, retry".

Transactions run on the leader, bound to its term:

- `openTransaction` waits for the read barrier, then opens a native transaction on
  the leader's `EmbeddedDB`. That gives the snapshot and read-your-writes exactly
  as today. Ops inside the transaction are staged natively, and the leader also
  records them as logical ops together with their conflict keys: the keys
  written, plus `getForUpdate` keys.
- At commit, a single leader-side sequencer checks the conflict keys against the
  keys written by every entry proposed after the transaction's snapshot. Every
  replicated write goes through this sequencer, which keeps a key-hash → last
  proposal sequence map, pruned below the oldest open transaction.
  - On a conflict, commit returns `false`, exactly like the single-node optimistic
    path.
  - Otherwise the recorded ops are proposed as one log entry and applied
    atomically on every replica. The leader then rolls back its native
    transaction, because the effect arrives through apply.
- Hash collisions can only cause false conflicts, never missed ones. A write
  proposed by a leader that has already lost its term cannot commit, so a
  deposed leader cannot commit a stale transaction.
- When the leader changes, open transactions, update ids and iterators are gone.
  Clients get a retryable error and retry the whole transaction, which is what
  they already do for conflicts.

## Snapshots, catch-up and node replacement

**Taking a snapshot.** `takeSnapshot` syncs the WAL to disk and creates a RocksDB
checkpoint (hard links, O(1)) of the live database in the state machine's
snapshot directory. The `appliedIndex` stored inside the checkpoint is the
snapshot index. Ratis then purges the log up to that index. Only the latest
checkpoint is kept.

**Catch-up.** A follower too far behind receives the checkpoint files through the
stock Ratis snapshot install. In `reinitialize` it closes its `EmbeddedDB`,
swaps in the installed checkpoint, reopens and resumes from the stored index. If
stock Ratis install turns out to be unsuitable for multi-GB files, we use the
notification mode instead, with the follower pulling files from the leader.

**Restart.** Open `EmbeddedDB`, read `appliedIndex`, and Ratis replays from its
snapshot index. Entries already applied are skipped.

**Membership.**

- **Bootstrap.** All nodes start with the same static peer list and either empty
  data or identical data. The migration from today's single node is: stop it, and
  copy its database directory to all three nodes.
- **Changing members.** Add, remove or replace a node with standard Ratis
  `setConfiguration`, through a small admin command. A new node catches up by
  snapshot install.
- **Graceful shutdown.** A leader that is shutting down first transfers leadership
  to an up-to-date follower. A deploy restart then costs about one RPC of
  unavailability, not an election timeout.

## Behaviour differences from single-node (documented contract)

- **Leader-bound handles.** Transactions, update ids and iterators do not survive a
  leader change. Clients get a retryable error.
- **Uncertain outcomes on failover.** If the leader fails after a write was
  submitted, the client may see an error even though the write commits. That is
  the same as a timeout today. Exactly-once retry, through a request id carried
  in `RequestContext`, is an open item.
- **Batch writes.** The `putBatch`/`mergeBatch` SST ingestion modes are executed as
  replicated write batches. There is no SST ingestion into replicas.
- **Physical scans.** `scanRaw` SST tokens are physical to the leader. A
  leader change invalidates them.
- **Merge operators.** They must be deterministic. The example
  `MessagePatchMergeOperator`, which reads the clock, is not.
- **CDC.** Not available in v1.

## Testing

Tests run real Ratis servers over real sockets. They are in-process for speed,
plus multi-process tests for `kill -9`. A TCP fault proxy sits between every pair
of peers, to drop traffic or cut links for partitions. Scenarios:

1. Basic replication: every replica holds identical contents and column IDs.
2. Leader killed in the middle of writes: no acknowledged write is lost, and
   clients keep working through retries.
3. Follower restarted: it catches up from the log.
4. Lagging follower, with the log purged: it catches up by snapshot install.
5. Partitioned leader in the minority: it serves no stale read and commits no
   write. The majority side elects a new leader and continues.
6. Rolling restart of all nodes under load: no failed requests for clients that
   retry retryable errors.
7. Transactions: conflicts detected across replicated writes, and aborted on a
   leader change.
8. Permanent node loss and replacement through `setConfiguration`.
9. `kill -9` of every process and a cold restart: data identical, nothing lost.
10. Linearizability: concurrent clients on register keys under random faults
    (leader kills, partitions). The recorded history is checked with a
    Wing–Gong linearizability checker.

## Open items before a first deployment (expected)

- Exactly-once retries through a client request id, using the Ratis retry cache.
- Multi-endpoint failover in the Java client, or a load balancer in front.
- CDC on the replicated log, if a consumer needs it.
- Snapshot transfer at TB scale: resumable, rate-limited, measured.
- Benchmarks against the single node: write latency and throughput, read latency
  with the barrier.
- Metrics and alerts: role, term, commit/apply lag, snapshot age, follower lag.
- Production seeding runbook and failure-domain placement: separate hosts and disks.
- An upstream report and fix for the Ratis ReadIndex reuse bug.
