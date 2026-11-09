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
added later as several independent copies of this design, with routing in the client. [multi-raft.md](multi-raft.md)
studies that step: columns assigned to groups, no cross-group operation for Yotsuba. It is decided against for
now: partitioned apply inside one group lifts the same apply ceiling without a second Raft log, and Multi-Raft is
reopened only if a benchmark with partitioned apply on still shows the apply or the one log as the limit above about
4x production load (see its status section).

## Shape

```
              Yotsuba (unchanged Rockserver client API, gRPC)
                 │ any node; requests that need the leader are forwarded to it
   ┌─────────────┼─────────────────────────────┐
   ▼             ▼                             ▼
 node A        node B (leader)               node C
 GrpcServer    GrpcServer                    GrpcServer        ← core server, unchanged
 ReplicatedConnection (implements RocksDBConnection; Router)    ← routes every API command
 EmbeddedDB    EmbeddedDB                    EmbeddedDB        ← core engine (facade + package-private parts)
 Ratis server ═══ Raft log replication (stock Apache Ratis 3.3.1, gRPC) ═══
```

**One Raft group, three voting replicas, stock Ratis.** There are no Ratis patches, and Ratis APIs are used
only as published. Each replica is an ordinary `EmbeddedDB` with the same column schemas, merge operators,
bucket hashing, workload scheduler and native JNI as a single node. Every node must run the same Rockserver
config file. A leader compares a digest of each follower's config with its own once per term (`ClusterCheck`, over
`TermService`), and both nodes log an error on a difference: the digest is taken over the parsed and resolved file, so
comments, layout and key order do not count. It does not stop anything: a rolling change of the config passes through
a difference, and some settings, such as cache sizes on machines of other sizes, may differ on purpose.

**Disk layout.** A node's data directory holds `db/` (the database) and `raft/` (the Raft log and snapshots).
Keep all of it on one file system and mount the data directory itself, not `db/`, `raft/` or a column volume.
A node refuses to start if the RocksDB `wal-path` is outside `db/`, if anything under `db/` (column volumes
included, also through symbolic links) is on another file system, if a column volume (`volumes[].volume-path`, RocksDB's
`cf_paths`, of `fallback-column-options` or of any `column-options` entry) resolves outside `db/`, which would make every
snapshot fail (checked before the database opens, so the directory is not created), or if `raft/` or anything under it
is on another file system than the data directory: every snapshot would then copy the whole database instead of
hard-linking its files, while the apply waits, and restoring one would copy it too.
The check under `db/` runs again on every open, after the database opened (a column volume is created on the first
open), and after a restore. RocksDB deletes files there from its own threads meanwhile (the inputs of a compaction,
obsolete WALs), so an entry that is gone before the check reads it is skipped, a file or a directory alike (core
deletes only files of an open database, but nothing depends on that).

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
  These parts live in classes of their own next to `GrpcServer`, which keeps only the lifecycle: `GrpcProxyMode`
  (the proxy scheduler, its limits and the connection age), `GrpcTransport` (the listening gRPC server that
  `pause` stops and `resume` replaces), `MustCompleteOperationTracker` (the rollbacks and closes that `close`
  waits for), `GrpcRequestContexts` (wire context to `RequestContext` and deadline), `GrpcScheduledCalls` and
  `GrpcIteratorOperations` (scheduling, cancellation and iterator leases), and `GrpcServerImpl` (the service
  methods). The split moved code only: behavior, status mapping and the `rockserver-leader` header are the same.
- `EmbeddedDB` (12k lines) is split the same way. It keeps its public API, every field (state, locks, executors,
  timers, test observers), the constructor, `close`, transactions with `ApplyWriter`, the shared read helpers
  (`dbGet`, column leases) and `scheduleTracked`; its other public methods delegate to package-private parts in
  the same package, each holding only a reference to the database: `EmbeddedDBColumns` (schemas, merge-operator
  resolution, create/delete column), `EmbeddedDBWrites` (put/delete/merge, multi and range deletes),
  `EmbeddedDBBatchWrites` (putBatch/mergeBatch, SST ingestion), `EmbeddedDBWriteElision`, `EmbeddedDBPointReads`
  (get, fast get, getMulti), `EmbeddedDBExistsMulti`, `EmbeddedDBIterators`, `EmbeddedDBRanges` (reduce/get/count
  range, range cursors, the retained query limiter), `EmbeddedDBRawScan` (SST pinning and raw scans),
  `EmbeddedDBMaintenance` (flush, compaction, table properties) and `EmbeddedDBStorageMetrics` (timers, storage
  pressure, compaction-I/O sampling). Code was moved, not rewritten: a sorted, trimmed comparison of the old
  file's lines with the new files' lines (`scripts/embeddeddb-split-compare.py`) finds no old line missing once
  call-site prefixes (`edb.`, `writes.`, ...) and `private` are ignored; what is new is the parts' headers,
  imports and the one-line delegates. The only edited line is `EmbeddedDB.varHandle`, which now takes a private
  lookup in the field's own class, because the classes that own those fields are no longer nested in
  `EmbeddedDB`. `ApplyWriter` and the transaction map stay in `EmbeddedDB`: `ApplyWriter` is a public nested
  type that `Applier` imports as `EmbeddedDB.ApplyWriter`, and the transaction methods are the facade's own
  bookkeeping that every part uses. Changes made to `EmbeddedDB` while it was being split (`closeIteratorsOfColumn`,
  the logging fixes, the removal of `openUnloggedTransaction`) were applied to the part that now holds the code.
- `EmbeddedDB.openApplyWriter` (the apply thread's transaction: no snapshot, untracked, optionally without the WAL),
  `EmbeddedDB.deleteRangeUnlogged` (a range deletion without the WAL) and the `atomic-flush` global option (off
  by default; it turns pipelined writes off).
- `GrpcConnection.forHostAndPortWithoutRetry`, for callers that retry on their own.

| Class | Role |
|---|---|
| `ReplicatedNode` | Wiring and lifecycle: database, Raft server, API server, cluster initialization, graceful hand-off. |
| `ReplicatedConnection` | The node's `RocksDBConnection`: client sessions, the leader hint, and the API, which hands each `RocksDBAPICommand` to `Router`. |
| `Router` | Chooses each command's path (see below), including follower reads. |
| `Forwarder` | Forwards a command or stream to the leader: deadlines, idle timeout of streams, which commands are sent again after the leader became unreachable, and the retryable or unknown outcome of the others. |
| `UpdateProtocol`, `CommitOutcomes` | Transactions and updates on the leader: opening, staged writes, commits, chained updates; which commits of client session writes are unsettled, and the answer to a copy of one. |
| `ReplicatedWrites`, `LocalCommands` | Writes proposed to the log, with batches split into chunks; commands on the node's own database, and the leadership checks the others share. |
| `Proposer` | Batches numbered requests into log entries and submits them to the leader: straight to the local Raft server on the leader, through a Ratis client elsewhere. |
| `RockserverStateMachine` | Ratis state machine. Leader admission in `startTransaction`/`preAppendTransaction`; entries are queued to an apply thread of its own, which applies them in groups; snapshots. |
| `Applier` | Applies the requests of log entries to the local database, a group of entries in one transaction without the WAL, with their result records and the applied position. |
| `ApplyResult`, `AppliedIndex`, `ApplyLanes` | The outcome of one entry, identical on every replica; the highest applied index and the reads waiting for it; the platform threads of the partitioned apply (see "Partitioned apply"). |
| `LeaderSession`, `ConflictTracker` | Transactions bound to the leader's term, per-key queues of single-key updates, and conflict validation. |
| `NodeMetrics` | Timers and gauges of the replication path, logged every 10 s and exported to the node's metrics registry (see "Metrics and alerts"). |
| `ByteAdmission` | The byte-weighted semaphore that never waits, behind `Proposer`'s bound on the bytes of writes a node holds and has in flight. |
| `ReadBarrier`, `TermService` | Linearizable reads. `TermService` also answers what a node holds (`NodeState`), a follower's read index, and a node's configurations (`Configuration2`, and the older `Configuration`, for `RemovalCheck`). |
| `RemovalRule`, `RemovalCheck`, `MembershipFacts`, `CommittedConfigurations`, `RaftTraffic` | Whether a node was removed: the rule, one check that asks the members, a node's own facts and answers, the committed configurations it applied, and when a leader last sent it a Raft request. See "Removal and waiting outside". |
| `FollowerReads` | A follower's read indexes from the leader, for the point reads it serves itself (off by default). |
| `ClusterCheck` | A leader tells its followers which cluster it leads, and a node of another cluster stops. |
| `PeerChannels` | The gRPC channels to other members' Raft ports: one cached channel per address, with a fresh connection attempt when it failed. Each node has one shared cache for `ReplicaOps` (checksum results, capabilities) and `LeaderForwarder`'s address lookups, pruned by the Raft watch thread to the members, the peers being brought in and the peer list, and closed last when the node stops. `ReadBarrier` and `FollowerReads` keep one each: they close theirs to fail their requests in flight when they close, and the barrier prunes its own to the voters of each round. `ClusterCheck` (once per follower and term), `RemovalCheck` (at start) and the `admin` commands use a one-off channel per call, on purpose: `RemovalCheck` runs once before the node exists, keeps one channel per member across its retries and closes them when it decides, so a cache would hold nothing past it; `ClusterCheck` makes one call, and after a match one configuration comparison, per follower and term, a few channels per election, and it shuts its channel to a follower of another cluster, which on the shared cache would also cut that address's calls of `ReplicaOps`; the `admin` commands run in another process. The pruning (`ReplicatedNode.dropChannelsOfFormerMembers`, every 200 ms) keeps the current and previous configuration, the followers a leader replicates to besides it (a peer being added) and the addresses of `--peer`, and shuts down any other address: `PeerChannelsPruningTest` (unit, a stub configuration) and `PeerChannelsMembershipTest` (in-process: a node joins with `--join`, gets channels on every node's shared cache, is removed with `setConfiguration`, and every node shuts its channel down within the pruning period while keeping the others'). An address that a caller of the shared cache takes from elsewhere would be rebuilt at every pruning, so its callers use only these addresses (`ReplicaOps.pollPeers`, `PeerConfig.raftAddress`). |
| `ApiGate`, `CommitHeartbeats` | Whether a node serves its API, from whether it knows a leader; the leader sending its commit index to every follower at once instead of with its next heartbeat (its own gRPC log appender, `CommitHeartbeats.Rpc`). |
| `LeaderStall` | Tells a leader that cannot commit to step down. |
| `NodeThreads` | The node's control-plane platform threads, the proposer's submit threads and the virtual threads of the leader's short local commands (see "Threads"). |
| `ConflictPositions` | The text that carries a conflict's positions from the leader's refusal to the proposer. |
| `NodeStore`, `CheckpointStorage` | Local database, internal metadata, checkpoint snapshots, restore. |
| `LeaderForwarder` | Finds the leader's API and forwards to it; knows the leader's API address that nodes send to clients. |
| `LogEntry`, `Request`, `LogEntryCodec`, `ResultCodec` | Log entry model and its binary encoding. |
| `ReplicatedServerMain`, `NodeConfig`, `PeerConfig`, `RaftSettings` | Command line: `run`, `admin` and `repair-log`; the node's settings and its members; the Ratis settings, every safety-relevant one set explicitly. |
| `RaftLogRepair` | Offline repair of a torn last write in the open segment of the Raft log. |

**Internal encodings are hand-written, not protobuf.** Log entries, batches, results and the `TermService`
messages use small binary codecs of their own (`LogEntryCodec`, `ResultCodec` and the like), the
entry formats with a version or marker byte. Protobuf (already on the classpath for the client API) was not used for them,
because the apply path depends on what a generated codec cannot do: the apply decodes an entry in place, so each key
and value is a slice of the log entry's buffer that goes to RocksDB without a copy, and the leader encodes each
request once, straight into the batch buffer, at an offset it computed before (see "Encoded once"). Protobuf-java
parses into objects of its own and copies each `bytes` field into a `ByteString`, and its encoder writes a message
of its own: one copy and one allocation per key and value on every replica, which is the cost the leaner apply
removed. The decode is in place only when the entry's `ByteString` is backed by one heap array
(`RockserverStateMachine.decodeBatch`), as Ratis 3.3.1 hands it over from its log and from a request: it parses its
messages with protobuf's copying parser and does not use the zero-copy marshaller of its gRPC transport. An entry
made of several pieces, or of `ByteBuffer`-backed ones (what that marshaller would produce, if a later Ratis turned it
on), is decoded from a read-only view instead, with a copy of each key and value: correct, only slower. The price of hand-written codecs is that every format change needs a version and a test for old bytes:
`CodecEquivalenceTest` (against the previous codecs, kept as test classes) and the codec tests, fuzzing included,
pin them, and a format change must keep reading the old bytes. The client API and Ratis' own messages stay protobuf.

**Core changes considered and not taken.**

- **Puts without the apply copy.** A put still copies its key and value once at apply, into the `byte[]` that
  RocksJava's `Transaction.putUntracked` takes. RocksJava has a `ByteBuffer` put only for tracked writes, which would
  lock every key and track it for a validation the apply does not need, so removing this copy needs a change to
  rocksdbjni itself. rocksdbjni is already the project's own published fork (`it.cavallium:rocksdbjni`), so an
  untracked `ByteBuffer` put (`putUntracked(ColumnFamilyHandle, ByteBuffer, ByteBuffer)`) would be a small addition
  there, released as a new published version; what the Codex branch risked was depending on an unpublished commit
  of the JNI submodule, missing from the published jar. The change is deferred only because the copy has not been
  measured as a bottleneck (performance.md, item 15, lists the apply timers to compare first).
- **A presence check with an exists call instead of `getForUpdate`.** `EmbeddedDB`'s previous-presence path (a put
  or delete that returns whether the key existed) reads the whole value with `getForUpdate` and keeps only whether
  it was found. It stays: the call also locks the key and records it for conflict validation, which no exists-style
  call of a RocksJava `Transaction` does, and `keyMayExist` may answer a false positive.
- **Table-properties mapping de-duplication** (`TablePropertiesAccumulator`, `GrpcPortableWire`,
  `ThriftTablePropertiesWire` on the Codex branch). A faithful refactor of single-node code that replication does not
  use: `getTableProperties` is a local command on a cluster. Master changed the same regions since, so it is better
  re-done on master on its own, with the single-node tests, than carried in this branch.
- Two Codex core fixes were kept in a simpler form: a merge operator whose constructor fails destroys the native
  object it created, and the JNI library is extracted to a temporary file, checked against the jar entry's CRC-32,
  forced to disk and renamed into place atomically, so concurrent starts never truncate a library another process
  has loaded and a power loss never leaves a library of the right size with wrong content. The file lives in a
  directory named after the jar entry's size and CRC-32 (inside the version directory), both read from the jar's
  directory: a fork rebuilt under the same version, even with the same size, is extracted again instead of reusing
  the stale library, and no hashing happens at start. Temporary files older than ten minutes, left by a process
  killed mid-copy, are removed.

## Routing

`Router` (behind `ReplicatedConnection`) sends every API command down one of four paths:

| Path | Commands | Handling |
|---|---|---|
| Replicated write | `put*`, `delete*`, `merge*` without a transaction id; `deleteRange`; `putBatch`/`mergeBatch` (split into ordered chunks of at most 4 MiB); `createColumn`, `deleteColumn*`, `uploadMergeOperator` | Proposed from the node that received it. The result is computed when the leader applies it. |
| Linearizable read | `get`, `exists*`, `getMulti`, `reduceRange*`, `getRange*`, `getRangePage`, iterator open, `getColumnId`, `getAllColumnDefinitions`, `checkMergeOperator`, `estimateNumKeys`, `scanRaw*`, `getSstMetadata`, `getTableProperties` | On the leader, after the read barrier, against its local database. Forwarded to the leader from the other nodes, except point reads (`get`, `exists`, `getMulti`, `existsMulti` outside a transaction) on a node with follower reads on: see "Follower reads". |
| Leader-bound | `openTransaction`, `getForUpdate`, writes and reads with a transaction or update id, `closeTransaction`, `closeFailedUpdate`, iterator seek, next and close, `compactFiles` | Served by the leader of the current term. Forwarded to it from the other nodes. |
| Local | `flush`, `compact` | The node that received the request does it on its own database. |

**SST maintenance.** `scanRaw*`, `getSstMetadata`, `getTableProperties` and `compactFiles` all go to the leader, so
that file names, metadata sessions and raw-scan tokens refer to one database whichever node a client calls (before,
the last three ran on the receiving node, and a client that mixed them with `scanRaw` combined two databases). A
dry run of `compactFiles` and the metadata reads are sent again to a new leader after a lost answer; an executed
`compactFiles` is not, and the new leader's metadata has another session anyway. The internal replication column is
refused to all of them, `compactFiles` included. A resumable raw scan tags each token it returns with the node and
the Raft term that issued it (`<node>~t<term>@/000123.sst`; `RawSstToken.tagged`): RocksDB file numbers mean nothing
on another node, nor on the same node after a snapshot install replaced its database, which happens only while it
follows. Tokens that come back with another tag, or none, are dropped, so their tables are scanned again (the
consumer gets those rows twice) instead of a different table of the same name being skipped. The core client and the
rust client accept tagged tokens and say so in the request (`ScanRawRequest.acceptsTaggedSstTokens`); a plain server
never issues them and ignores them. Clients built before tagged tokens reject them (their token parser takes a
filename only), so the gRPC server sends a client that does not set the flag the plain filename instead: it can
parse it and resume, but the replicated server drops the untagged token when it comes back, so every resume of such a
client scans the column again. Upgrade order: servers first is safe (old clients keep working, without skipping),
then the clients, which get their skips back. The admission estimate of a resumable scan counts its tokens' own
lengths, so a plain server's scans are not charged for tags they never carry.

Forwarding uses the leader's ordinary Rockserver API through the core `GrpcConnection` (4 connections per
peer, without gRPC's own retries; a port probe first, so a dead peer fails in milliseconds). The forwarding
connections of a node share one `GrpcClientTransport`: 4 Netty event loop threads, which Netty hands to new
connections in turn, so the 4 connections to the leader get one each, and one callback executor. `LeaderForwarder`
closes it after the connections, when the node stops. Before, each connection made its own event loop and executor. A request that finds no
leader waits for one: the state machine tells `LeaderForwarder` of every leader change, and of this node becoming a
ready leader, so it goes on at once. A request registers for that wake-up before its last check, so a change between
the two is not missed; a 250 ms poll only enforces the deadline. Each forwarded call takes at most the
caller's deadline or the request timeout, whichever comes first (for a `getForUpdate`, the request timeout plus its
12 s key wait), and the leader gets that deadline as the call's own: a leader that is alive but stuck, such as one
whose apply waits, or one in a long pause, holds neither the call nor the follower's proxy permit longer. A call that
times out is treated as lost, as below, but its connection is kept, since other calls use it. A forwarded stream fails
the same way once no element comes for a request timeout while its consumer waits for one; a consumer that asks for
nothing stops that clock. When the leader cannot be reached, reads and transaction or update opens are sent again to whichever node leads then, until the
request timeout; a request that was never sent can always be retried; a commit whose connection broke has an
unknown outcome. A request never sent by the end of the request timeout fails with `UPDATE_RETRY`, except a write of a
client session (it carries a request id): that one fails with `SERVER_OVERLOADED` flagged not applied, for this copy
only, since an earlier copy may have reached a leader that appended it and then became unreachable, and may commit
under the next one (R25-O; `UPDATE_RETRY` would say that no copy applied, and the client would run the transaction a
second time). The client sends it again if an earlier copy may have applied, and otherwise ends it as not applied. A `getForUpdate` whose connection broke is not sent again but fails with `UPDATE_RETRY`: the
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
`UNAVAILABLE`, and the reads allowed by the retry policy run again on the next node. Writes are sent again by the
client with their session request id, so they apply once (see "Client sessions" under "Writes, exactly once"). Transactions and updates live on the leader, so a client that moves
between followers keeps them. A connection attempt to a node that does not answer ends at the connect timeout,
including a frozen process, whose kernel accepts the connection. See the
[README](../../README.md#client-connections) for timings and settings.

`pick_first` reacts only to the transport, so a node that stays reachable but cannot serve has to shed its clients
itself. A node cut off from the Raft majority answers HTTP/2 and keepalive while every request fails. So a node that
has known a leader and has had none for 3 maximum election timeouts, and at least 6 s, stops serving its API
(`ApiGate`, checked by the API gate thread of `ReplicatedNode`, see "Threads"): `GrpcServer.pause` closes the port and sends GOAWAY
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
`GrpcConnection` returns: with several addresses it keeps asking while the answer is `UNAVAILABLE` or no answer comes
within 10 s, for 30 s by default (`handshake-wait-ms`), and with one address it fails as a client of a plain server that
is down does, after the retries of the call, about 8 s. Any other answer fails it at once.
A cluster that elects within the bound pauses nothing. A member of a new cluster has neither, and is not judged until
it first knows a leader. Once a node
has known a leader for one maximum election timeout, long enough for a heartbeat to have confirmed it, `GrpcServer.resume` builds
a new gRPC server on the same port. Both conditions must hold without a break, so a short election never pauses a
node, and a pause and the next are at least 4 maximum election timeouts apart. A cluster where no node has a leader
pauses every node and recovers by itself: elections go on, and every node resumes after the new leader appears. A
single-node embedded server never pauses.

Placement is deliberately simple. All the clients with the same list sit on its first node, and stay on what is now
a follower after a leader change. A node that returns gets its clients back the same way, not at once: a client stays
on the node it failed over to until that node goes away. What spares them the forwarding hop is the leader hint (as
etcd's clientv3 and the TiKV client's region cache do): every response of a replicated node carries the leader's API
address, as the members know it (`rockserver-leader`, in the headers, or in the trailers of a call that fails before
them; `ReplicatedConnection` implements the core `LeaderHint`, and `GrpcServer` adds it). A client of several nodes
(`LeaderHintRouter`, inside `GrpcConnection`) keeps a second channel to that address and sends it the calls the leader
serves: writes, transactions, updates, iterators, ranges, column calls and `openClientSession` (registered through
the log, so a follower would only forward it). Point reads stay on the client's node, which serves them itself with
follower reads on, and so do maintenance calls and node statistics, which concern that node, and the handshake. Both
lists are written out (`LEADER_METHODS`, `ORDINARY_METHODS`), and a test checks that every method of the service is in
exactly one of them, so a new RPC cannot fall to the ordinary channel unnoticed. The
second channel takes calls only while it is connected; before that, and after a call on it ends with `UNAVAILABLE`
(the channel is then dropped, and the same address is not taken again for 2 s; each failed address is remembered on
its own, so two nodes that each name the other while both fail do not make the client open a channel at every
hint), calls go through the ordinary channel
and a follower forwards them. It is also dropped when the hinted node answers without naming itself as the leader: a
deposed leader, or one cut off from the majority, names another node or none, and fails its calls with errors other
than `UNAVAILABLE`. The second channel has no gRPC retries of its own, which would go to the same, perhaps dead,
node: a read that the ordinary channel's retry policy would send again (`getRangePage`, `reduceRange*`,
`getColumnId`, `estimateNumKeys`, `getAllColumnDefinitions`, `checkMergeOperator`) and that fails with `UNAVAILABLE`
before any answer runs again on the ordinary channel, which fails over to a live node. Other calls fail as on any lost
connection. A hint moves with the leader, since every response carries the current one. By default
the client takes only an address of its own list, compared by host name and port: otherwise a server could send it to
an address its user never listed, and the address the members use among themselves (an internal name, a NAT address)
may not be reachable from the client (`it.cavallium.rockserver.grpc.client.leader-hint`: `listed`, `any` or `off`). A
plain single-node server sends no hint, and a client with one address never uses one.

A maximum connection age (`maxConnectionAge`, off by default, gRPC adds +/-10%, with 10 minutes of grace for running
calls) would bring clients back, because after the GOAWAY a client starts again from the top of its list. That is its
only benefit, and it is off because of its two costs. A silent first address then costs the connect timeout at every
age cycle, for every client, so a one-time cost becomes a recurring stall. And the grace cuts any call longer than it,
including `Compact` and long scans, on a healthy cluster too.

A hung disk pauses the API too. The process would still answer HTTP/2 and keepalive while every write waits for an
fsync of the Raft log that does not end, until its deadline. Reads and writes through the database do not show it
(they are served from the block cache and the memtables), so `DiskProbe` does what every write waits for: once a
second it writes a few bytes to `disk-probe` in the data directory (the file system of the database and the log) and
fsyncs it, on a thread of its own. A probe that has not ended after the request timeout pauses the API like a missing
leader does (the log says `the disk hangs`), and the node serves again once it ends. A leader on that disk also
stops flushing its log, which `LeaderStall` handles (see "A leader whose log stops"). A disk that fails at once is
not a hung one: the database and the log report their own errors.

## Writes, exactly once

A proposal is a `Request`: a random proposer id (one per node process), a sequence number, `completedBelow`,
and the `LogEntry`.

- **Batches.** Requests that arrive while 32 batches are in flight wait, then go together as one log entry
  (`LogEntryCodec.encodeBatch`: the proposer id and `completedBelow` once, then each sequence and entry; up to
  4096 requests or 8 MiB; schema changes, `deleteRange`, genesis and session registrations always alone). A batch
  that holds a write of a client session has version 3 instead of 2: after each sequence, a byte says whether a client
  request id (session, sequence, `completedBelow`) follows. Every other batch keeps version 2 and its bytes, so logs
  written before sessions read as they did. One entry costs one Ratis
  append, one place in a group fsync and one apply transaction, whatever it holds. Transaction commits have a
  queue of their own, and commits of different leader terms never share an entry.
- **Encoded once.** `Proposer.submit` computes a request's exact encoded size (`LogEntryCodec.entrySize`, which
  also rejects what would not encode: strings that are not valid Unicode, request types with no log code) and keeps
  the entry itself; the batch is sized from those sizes, and each entry is written straight into it when the batch
  goes out, with no body of its own. The bytes are those of the bodies encoded one by one and joined (versions 2
  and 3 alike; `CodecEquivalenceTest`). A caller's buffers are read only until its write is first encoded into a
  batch, which happens while the caller still waits for its answer. A caller that gives up on a write in the short
  time between a batch taking the write and encoding it is answered only once that encoding is done, so a caller
  may reuse its buffers as soon as it is answered. The caller's future is a `Proposer.CallerCopy`, which overrides
  the two ways of giving up: `cancel()` returns only once the encoding is done (`Pending.awaitEncoded`: a flag set
  before the write is marked sent and cleared when the batch no longer reads it, at most 1 s of waiting, in practice
  the encoding of one batch); `completeExceptionally()`, which `orTimeout` calls on the JDK's single delayer thread,
  does not wait there but completes the future later, on a caller thread, once the encoding ended
  (`Pending.whenEncoded`, also at most 1 s), so that no continuation of the caller runs earlier. Both also take a
  write that was not sent yet out of its queue. `complete` with a value, `completeOnTimeout` and `obtrude*` are not
  covered (nothing uses them on a write). Checked through the proposer
  (`ProposerCopiesTest.aCallerGivingUpWhileItsBatchIsPausedBeforeEncodingIsAnsweredAfterIt`: a batch paused by a test
  hook just before it encodes; fails if the overrides are removed), and the 1 s cap of the later completion when
  the encoding does not end (`aCallerGivingUpWhileTheEncodingDoesNotEndIsAnsweredAfterTheCap`: the future completes
  with the caller's error after the cap, the encoding still paused). A caller must therefore not change a
  write's buffers while it waits; if it does, the batch fails to encode, nothing of it is sent, the writes that no
  longer encode to their size fail as invalid (or, resent alone after an earlier batch applied them, with an unknown
  outcome) and the others go back to the queue, as never taken unless resent alone, so that their callers' deadlines
  and cancellations apply again. No request in the queue may have been appended by an earlier batch without knowing
  it: such a request fails with an unknown outcome when its batch's resends stop (below), never goes back (R17-3: a
  `maybeAppended` flag for requeued ones was never set and is removed). The encoder also returns the batch's requests as decoding
  the bytes in place would read them: each key and value a slice of the batch, each array of a commit a copy,
  nothing parsed. Each request then keeps that copy instead of its caller's entry, so a write that goes back to the
  queue still sent (a commit after another commit's conflict, a copy resent alone), which its caller may already
  have given up on and whose buffers it may reuse, is encoded again from the batch's immutable bytes, never from the
  caller's memory. A batch with a schema change returns no requests; its members never go back to the queue still
  sent (`ProposerCopiesTest.aWriteTakenIntoABatchKeepsTheBatchCopyNotItsCallersBuffer` fails if the copy is not
  taken). A follower's resend of a batch whose reply is late (past an attempt timeout) goes under the same Ratis
  invocation and is answered by the leader's retry cache: one log entry, applied once
  (`aResendOfALateBatchIsAnsweredByTheRetryCacheAndAppliesOnce`). On the leader (and for every batch of commits, which only go to the local Raft server) the proposer offers
  the requests to its own state machine under the
  batch's Ratis invocation (client id, call id) before submitting it locally, and withdraws the offer when Ratis
  answers. `startTransaction` takes the offer instead of decoding the batch, only if the request carries the very
  `ByteString` the requests were encoded into; anything else is decoded as before: batches from other nodes, a
  resend whose offer was already taken, an offer withdrawn, a batch with a schema change (`CreateColumn`,
  `UploadMergeOperator`, whose fields could change under it), and every entry after a restart. The stamp, the bytes
  in the log and what followers decode do not change.
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
  with an unknown outcome (`OUTCOME_UNKNOWN`) for every request instead: sending its requests again as a new batch would append
  them a second time while the first copy may still be pending.
- **Byte admission.** A node holds at most a fixed number of bytes of writes, queued or in flight, from
  `Proposer.submit` to their answer: an eighth of the maximum heap by default (each body is held about twice while
  in flight: as its entry, and in its batch), at least room for two entries of the largest size, or the system
  property `it.cavallium.rockserver.distributed.write-admission-bytes` (the old name
  `it.cavallium.rockserver.replicated.write-admission-bytes` is still read, with a warning). Each request counts its encoded size plus 256
  bytes. A
  write that does not fit fails at once with `SERVER_OVERLOADED` and a message that says it was not applied
  (metric `writes.admissionRejects`), instead of filling the heap: an out of memory error ends the node (see "Heap").
  A queued write whose caller gives up (deadline or cancellation) drops its body at once and gives it back; its
  256 bytes stay counted until a batch takes it out of the queue, which may wait for a free batch slot.
- **Attempt permits.** Before an attempt is handed to Ratis, local or remote, it takes a permit for its bytes from
  the proposer's own count: at most 32 MiB of attempts in flight per node and per queue, one for data writes and one
  for transaction commits, so that bulk writes never take the room of commits. An attempt without a permit is not
  sent: it is refused before the append by the node's own decision, and sent again after 50 ms (a commit goes back
  through its queue, under its caller's deadline). A local attempt keeps its permit until Ratis answers it, not
  only until the node stops waiting, since the entry stays pending in Ratis until then; a remote attempt gives it
  back when its node stops waiting. The leader's Ratis pending-request limits (`RaftSettings`: Ratis' default 4096
  requests, and 32 MiB per node of `--peer`, at least 4 nodes: 160 MiB instead of the 1 GiB this used to be) hold
  the data writes of every node and the leader's own commits. Ratis reads them at the start, so a cluster that grew
  past its peer list by `add-peer` keeps the smaller limit until its nodes restart with the longer list; the leader
  warns meanwhile. A long commit stall can still fill them with remote attempts that their nodes gave up on (the
  leader cannot know which: the attempt carries no deadline it could check before the append, and the copy may still
  be the one that commits). That is accepted: Ratis then refuses before the append and each proposer resends, a
  liveness cost while the stall lasts, never a lost or doubled write. Ratis' refusal then (`ResourceUnavailableException`, "Failed to acquire a pending write
  request") is a refusal before the append, resent like the node's own; Ratis raises the same type after an append
  ("add a pending write request"), an unknown outcome, and only the message tells them apart (Ratis 3.3.1 has no type
  or field for it): `RatisContractTest` pins it, and a changed message falls back to an unknown outcome, the safe
  side. The retry cache of Ratis 3.3.1
  has no size bound, only its expiry.
- **Caller deadlines.** When the caller's request-context timeout ends before the node's request timeout, a
  write is resent only until the caller's deadline, less a margin of 20 ms (at most a tenth of the timeout). A
  write still queued at that point, or whose caller cancelled it before it was sent, is never sent; nor is one
  with less than 100 ms left (at most a quarter of the caller's timeout), which would get one hurried attempt. It fails with
  `READ_DEADLINE_EXCEEDED` and a message that says it was not applied, as on a single node, where a command that
  waited past its deadline fails without running. A write already sent cannot be withdrawn. The synchronous API,
  which `GrpcServer` uses for writes, returns at the caller's deadline with `OUTCOME_UNKNOWN`, and the write may
  still be applied later. A transaction commit (or the write that ends an update)
  still queued at its caller's deadline is not sent either, but the synchronous API does not return at the
  caller's deadline for it: once submitted, its single attempt waits for its reply for up to one attempt
  timeout, even past the caller's deadline. The
  chunks of a `putBatch` or `mergeBatch` share one deadline, counted from the start of the call.
  A queued write is failed by a timer, not only by the next batch that looks at the queue, so it fails on time
  also while every batch slot is taken by batches that get no answer. The timer fires when that batch would fail
  it: at its deadline less the least time left to be sent (100 ms, or a quarter of the caller's time), not at the
  deadline itself, where the caller would get the clean answer only 20 ms before its own deadline (R22-F2). Whichever
  of the timer and the batch fails it first, the write leaves the "queued" state once, so the admission gets its
  whole weight back exactly once (R22-F3). A write without a caller
  deadline has the node's request timeout as its deadline, and fails at it the same way, as not applied
  (`SERVER_OVERLOADED`, "No leader accepted the write in time"). The timer is armed when the write is queued and
  again whenever it comes back to the queue unsent, but a write holds at most one waiting timer: its deadline never
  changes, so a requeue that finds a timer still waiting arms none, and a write refused again and again under load
  does not pile timers onto the control threads (R21-2, R21-4).
- **Submitting off the control plane.** On the leader, Ratis 3.3.1's `submitClientRequestAsync` waits
  (`supplyAsync(..., clientExecutor).join()`) until `writeAsyncImpl` has appended the entry, and the append
  (`appendTransaction`, under the server's lock) waits without giving up for room in the log worker's queue
  (`SegmentedRaftLogWorker.addIOTask`). That queue's byte limit (64 MiB) is below the pending-request limit
  (160 MiB), so during a disk stall local submissions block for as long as the stall lasts. The proposer therefore
  never submits from the control threads, which would stop replies, expiry timers, resends and read-barrier rounds,
  nor from the proposing caller, whose `propose` would not return and whose synchronous deadline could not end the
  call (R22-F1). A batch is still taken from the queue on whichever thread dispatches (a caller, a reply handler, a
  timer: only the lane's lock), so batches form as before, and is then encoded and submitted on the node's submit
  threads (`NodeThreads.submit()`); the timer of a resend hands the resend to them too. While the node last saw that
  it does not lead (a hint updated by every leadership check, which itself can wait while the Raft group starts and
  so never runs on the dispatching thread), a data batch and its resends go straight to the sender's virtual
  threads instead, which encode the batch and send it to the leader: there an attempt cannot block in Ratis, and
  the submit threads would only add a second hand-off. An attempt on the sender that finds the node leading after
  all hands its submission to a submit thread (R22-2). Commits are only ever submitted locally, on the submit
  threads. A batch that a submit thread reaches once the proposer is closing is not sent: `close()` answers the
  writes in flight only after closing its client, which can take seconds, and the batch's writes are answered as
  stopping instead (R22-4; a batch reached after that no longer encodes, since its sequences are below
  `completedBelow`). Any failure while a batch is sent, an `Error` too (out of memory encoding 8 MiB), answers its
  writes with an unknown outcome and gives back its slot (R22-3). No order between batches is
  relied on. Between dispatching threads there never was any: `send()` already ran outside the lane's lock, so a
  batch taken first could reach Ratis second. Within one thread there now is none either: each batch is its own
  task on the submit threads, and a batch B taken after A can reach Ratis first. Before R22 a leader thread that
  proposed A and then B had A appended first only for A's first attempt, and only if A got its permits: a refusal
  before the append (`NoPermitException` when the attempt bytes in flight are full, Ratis'
  `ResourceUnavailableException` when its pending limits are), `NotLeader` and a timeout all resend A 50 ms or more
  later, after a smaller B that fit. Nor was that order ever a contract anywhere (R23-A):
  **writes that are not awaited have no relative order, on one node or replicated; order comes from awaiting each
  write before sending the next, from an update of the key (the per-key FIFO lock), a transaction, or a batch.**
  A replicated node's gRPC server runs each call on a virtual thread of its own (`GrpcServer.callExecutor`, and in
  proxy mode one virtual thread per write through `ProxyScheduler`), so each gRPC write is its own
  `writes.propose`, racing the next; the single-node server runs writes on its write pool (at least 3 threads in
  production) under DRR and deadline scheduling, and `EmbeddedConnection` on the column's scheduler executors; the
  core and Rust client sessions resend a copy whose outcome is unknown after later writes succeeded; on a follower
  attempts always went through the sender's executor. Restoring submission order per lane (a ticket taken in
  `dispatch()`, encoding in parallel, a drainer submitting in ticket order) would cost little, but would not cover
  resends, followers (one ordered stream to the leader per lane), leader changes, client resends or the gRPC
  front: "usually in order", worse than a clear contract. No client relies on it: Yotsuba's non-update puts and
  deletes are idempotent markers or writes of distinct keys, its value writes go through updates, its one ordered
  pair (`senderPresence` before the ending put) is awaited, and it uses no merges; the Rust client's calls are
  awaited, `put_batch` chunks are sent one after another. `GrpcProxyWritesUnorderedTest` (core) shows a later put
  to a key completing while an earlier, not awaited one still runs, independent of the proposer. Order
  comes from elsewhere, independent of submission order: successive attempts of one batch are sequential (the
  next is scheduled from the previous one's reply, same call id); single-key updates are ordered by the per-key
  FIFO lock and pipelined chains, admitted in `preAppendTransaction` in log order; commits by conflict validation
  in log order; `putBatch` chunks by sending each only after the previous one completes; requests requeued at the
  head are back before the dispatch that follows; and `completedBelow`, computed at encode time and kept as a
  maximum per proposer at apply, never passes a sequence still in flight, so a batch overtaken by a later one is
  still applied (a copy of requests already answered is refused as a late copy, as intended;
  `ProposerSubmitThreadsTest.aBatchOvertakenByALaterOneIsStillApplied`).
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

**Client sessions.** The proposer's numbering makes a write exactly once only while the same node process resends
it. A client that loses its connection and sends the write again, through any node, needs its own numbering: client
sessions, as in the Raft dissertation (section 6.3), the way etcd and TiKV deduplicate by request id.

- **Opening.** A client opens a session with the `openClientSession` call, which nodes advertise in their
  capabilities (`clientSessions`). The node proposes a `RegisterClient` entry, applied alone but without a flush;
  the session id is the entry's log index, the same on every replica, and the registration is itself a proposer
  request, so a resent registration returns the same id.
- **Numbering.** The client numbers its writes in the session and sends each with its request id (session, sequence,
  `completedBelow`) in the request context; forwarding keeps it. Apply identifies such a request by that id instead
  of its proposer's numbering, with the session's own floor and records (`SESSION_REQUEST` keys), loaded like a
  proposer's, deleted in bulk like a proposer's. So a copy proposed by another node, after a leader change, or in an
  entry with writes of many sessions, gets the result of the first copy.
- **What is numbered.** Writes outside transactions, commits, the write that ends an update, `deleteRange` (a copy
  sent after later writes into the range would delete them), schema changes, and batch chunks. Schema changes:
  `createColumn` (same name and schema) and `uploadMergeOperator` (same content) are idempotent at apply already, so
  numbering them only lets the client resend them itself after an unknown outcome instead of failing with it; a copy
  of `deleteColumn` or `deleteColumnIfExists` would answer "not found" or `false` for a deletion that took effect, so
  they need it. Each costs one small record under the session. `putBatch` and `mergeBatch` are numbered per chunk:
  the client splits its batches with the same rule as the server (`KVBatchChunks`: greedy, at most 4 MiB counting 16
  bytes per key segment and value, so a chunk split again is that chunk) and sends each chunk as a call of its own,
  one at a time, in order, all within the call's deadline. A node that gets a numbered batch call proposes it as one
  entry under its request id, and refuses one that is more than one chunk (`INVALID_REQUEST`) before anything is
  proposed, as soon as the second chunk arrives: the rest of the stream is cancelled, not read into memory (byte
  admission only counts proposals, so a large numbered stream would otherwise sit in the heap). Batches are numbered
  only by a server that advertises `numberedBatches` in its capabilities (besides `clientSessions`); a client talking
  to a server without it (an older one, which ignores the id of a batch and would apply a resent chunk twice) sends
  batches unnumbered, streamed once and never resent (`NumberedWritesTest.aServerThatDoesNotNumberBatchesGetsThemUnnumbered`
  checks both ends through a server that offers sessions without numbered batches). A chunk that fails for good after
  an earlier one took effect ends the call with `INVALID_REQUEST` ("partly applied"), as on the server. A long
  numbered batch stream is refused after its second chunk without being read further
  (`aLongNumberedBatchStreamIsRefusedWithoutReadingIt`: an endless stream, a handful of batches produced).
- **Streamed multi writes under a number.** The gRPC server applies a streamed `putMulti`, `deleteMulti` or
  `mergeMulti` (and their variants with results) of a non-LATENCY caller item by item, as it reads the stream. Each
  item then carried the call's one request id, so a replicated server applied the first item and answered every
  later one from the first's record, without applying it: the Java client numbers these calls, and a BATCH or
  INGEST caller lost all but the first item of a list too large for `putMultiList` (and of every `deleteMulti` and
  `mergeMulti`, which always stream). A numbered streamed multi write is now collected and applied as one multi call,
  like a LATENCY one (`GrpcServerImpl.collectsWhole`): one numbered write, applied at most once, whose copies answer
  with its results. Outside LATENCY, instead of the LATENCY fan-out limits, it is bounded by 16 MiB counted
  (`GrpcServerImpl.numberedItemBytes`: keys and values, 16 bytes per key segment and value as the log encoding counts
  them, and 96 bytes per item for its proto objects on the heap; so also at most 16 MiB / 112, about 150 000, items;
  more is refused as invalid, nothing applied). The collected items sit on the heap before the node's write
  admission sees the write, so while it is collected each item also takes its counted bytes from the server's
  collect budget (`rockserver.grpc.numbered-multi-collect-bytes`, 64 MiB by default, for every such stream at once),
  given back when the collection ends, when the write's bytes pass to the proposer's admission. A cancel can end the
  collection while a late item still arrives on another thread: the item then takes nothing (`GrpcServerImpl.CollectHold`,
  one lock per collection), so no byte stays taken whatever the order (`CollectHoldTest`, which races items against
  the end on several threads, and drives the Reactor pipeline itself, `GrpcServerImpl.collectNumbered`, which
  `collectMulti` uses: a stream that keeps sending items on its own thread while its subscription is disposed, 500
  times, leaves the budget at zero; it fails if the hold is ended only on completion or error and not on a cancel, or
  if an item after the end may take bytes). A write that does
  not fit is refused as `SERVER_OVERLOADED` with the not-applied flag, at once, without reading the rest of its
  stream; the caller knows it can send it again, as after a full admission (a client resends it under the same
  number only when an earlier copy may have applied) (`NumberedWritesTest.numberedStreamedMultiWritesTakeTheServersCollectBudget`, with a 64 KiB budget). Before
  this, collecting counted only key and value bytes, up to 2^20 items, with no bound on how many streams were
  collected at once: a few million-item `deleteMulti` calls could hold gigabytes before admission refused them. The collected path also maps
  each result before streaming it, so a missing previous value (null) no longer fails the call
  (`NumberedWritesTest.numberedStreamedMultiWritesOfABatchCallerApplyEveryItem`, which fails without the change).
  The Java client numbers a streamed `putMulti`, `deleteMulti` or `mergeMulti` only up to
  `SessionWrites.NUMBERED_MULTI_MAX_BYTES` (4 MiB counted as the server counts it, a quarter of its limit, as the Rust
  client does); a larger one is sent unnumbered, streamed and applied item by item as before numbering, once (a lost
  call leaves its outcome unknown). It used to number every one, so a BATCH or INGEST call over 16 MiB counted (or
  about 150 000 items) that was applied before numbering was refused as invalid with nothing applied (R6-3). A
  `putMulti` without results that goes as one unary `putMultiList` (at most 65 536 items and 8 MiB) is not collected
  by the server and stays numbered. Splitting a large call into numbered chunks was not done, for the reasons given
  for the Rust client below. `SessionWritesTest.largeStreamedMultiWritesAreNotNumbered` checks the rule;
  `NumberedWritesTest.aStreamedMultiWriteOverTheNumberingLimitIsSentUnnumberedAndApplied` (a BATCH `putMulti` with
  previous values of 20 values of 1 MiB) fails with `PUT_INVALID_REQUEST` without the change. Such a write can end an
  update (its transaction or update id is one this client's `getForUpdate` opened): the connection then forgets the
  update once the call ended, as it does after a numbered write (`SessionWrites.endsUpdateUnnumbered`, called by
  `GrpcConnection` for the synchronous and asynchronous paths), as the Rust client does. Before, nothing forgot it,
  and for up to 10 minutes a transaction that got the same id would have its staged writes numbered and refused as
  `INVALID_REQUEST` (`NumberedWritesTest.anUnnumberedMultiWriteThatEndsAnUpdateForgetsIt`: a 5 MiB `putMulti` with
  previous values that ends an update, which fails without the change; `SessionWritesTest.anUnnumberedMultiWriteThatEndsAnUpdateIsRecognized`).
- **Leader time.** The leader prepends its clock (`LogEntryCodec.stamp`: a marker byte and 8 bytes, without copying
  the batch) to every entry that holds a session write or registration. A proposal that already starts with the
  stamp marker is refused as malformed: only the leader stamps. Apply keeps the highest stamp seen as the
  replicated clock (`CLOCK`), written with the applied position of the entry that moved it, so it never goes back,
  even after a leader whose clock is behind, and every replica has the same clock at the same position. A leader's
  wall clock may be wrong, and one far ahead would expire every session at once. So the leader caps what it stamps
  (`LeaderClock`): the first stamp of its term is its wall clock, but not behind the replicated clock it applied and at
  most 60 s past it, and later stamps move with the time elapsed on its monotonic clock, so a wall clock that jumps
  during the term changes nothing. The cap is applied where the stamp is made, so the log holds the capped time: apply
  and replay read the same value everywhere, and no apply rule changed that a replica of an older version would apply
  differently during an upgrade (a cap at apply, `clock <= previous + step`, would: an old replica would take the
  uncapped time). Its cost: a period without session writes that ends with a new leader (a whole cluster stopped for a
  day, say) counts at most 60 s toward expiry, so sessions live longer, never shorter. A leader of an older version
  still stamps its wall clock. This stamp is the only wall-clock time the replicated state reads: leader election,
  the read barrier, transaction and update leases, forwarding deadlines and timeouts all run on `System.nanoTime()`,
  which a skewed wall clock does not move. For tests the wall clock that `LeaderClock` reads can be skewed per node (`-Drockserver.test.clockSkewMillis`, or the state machine's test
  hook); `ClockSkewTest` and `ClockSkewRandomFaultTest` use it. A different clock *rate* on one node (a monotonic
  clock running fast or slow) is not simulated: the JVM gives no per-process hook for `nanoTime`, and since no
  decision depends on a time-based lease (Ratis' lease reads are off, see "Reads"), a rate error can only make a
  timeout fire early or late, which the fault suites already cover with pauses and slow links.
- **Expiry.** A session's row holds its timeout, the clock of its last applied write and its floor. The timeout is
  in the `RegisterClient` entry (`-Dit.cavallium.rockserver.distributed.client-session-timeout-ms` of the node that
  opened it, the old name `rockserver.client-session-timeout-ms` still read, 1 hour by
  default, and also when the value is not an integer, with a warning, as `Long.getLong` read it before, at least 10 minutes and 30 request timeouts: well above the time a copy of a write may still be resent),
  so replicas agree whatever their own settings. It stays a system property and not a `NodeConfig` option (or
  `--client-session-timeout-ms` flag), by choice: it is not a setting the members must share, since each session
  carries the value of the node that opened it and no replica ever reads its own; the part that matters for safety,
  the floor of 10 minutes and 30 request timeouts, already follows `NodeConfig.requestTimeout`; and it is a rarely
  touched tuning knob, read through `DistributedProperties` like the other two of its kind (write admission bytes,
  follower reads), with the documented prefix, the old name as an alias and a warning on a bad value. A `NodeConfig`
  component would change the record's canonical constructor and its every caller for a value that does not need
  to be in the node's configuration. A proposer that gets `SESSION_EXPIRED` for a copy of a write whose
  earlier copy may have been appended answers `OUTCOME_UNKNOWN` (or, for a write resent alone after it applied,
  "applied, result lost"), never the "not applied" that would make the client write it again. A session idle longer than its timeout at an entry's
  clock is gone for good: its requests are rejected with `SESSION_EXPIRED` and not recorded, copies of writes it
  applied too. Each replica deletes expired sessions and their records without the WAL, once the entry that expired
  them committed, when it likes: what a request sees depends only on the clock and the row, and a lost deletion
  leaves a session that is expired anyway.
- **Commits.** A transaction commit, or the write that ends an update, carries the request id into its `Guarded`
  entry, and the record goes under the session. A copy that reaches a leader where its transaction or update is
  gone (it was committed, it expired, or another leader had it) waits for the read barrier, so every entry of
  earlier leaders is applied, and answers from the record. Without one, the commit never applied, unless a copy is
  still unsettled on this leader in this term (in flight, or ended without an answer; a mark of a later term counts
  too, since the term is read before the barrier and the node may have lost and won leadership meanwhile), which gives
  `OUTCOME_UNKNOWN`; otherwise `UPDATE_RETRY` as before, or `SESSION_EXPIRED` if the session is gone: its row is
  missing, or it is expired at the replicated clock (`CommitOutcomes.sessionLive` reads the row, then the clock, with
  the applier's rule `NodeStore.sessionExpired`), which the applier already treats as gone before its sweep deletes the
  row. Either answer says the copy was not applied, so reading the two rows apart can only err toward "expired". A copy that
  arrives while the first is in flight gets `OUTCOME_UNKNOWN` instead of a second entry, which would conflict with
  the first. Only a barrier that passed proves anything: when it fails (the node lost leadership, is stopping, could
  not confirm its leadership in time, or lags in applying), or the store fails while the copy is settled, the copy
  gets `OUTCOME_UNKNOWN` (R25-O, `CommitOutcomes.settledCommit`). An earlier copy may then be in this node's log,
  replicated, and commit under the next leader; the client sends the write again, a deposed node forwards it, and
  the next leader's barrier settles it. Until round 25 a barrier that failed with `NotLeader` answered `UPDATE_RETRY`
  (the reads' mapping, `LocalCommands.barrierFailure`), and the client ran the transaction a second time; a timeout
  or an apply lag answered `SERVER_OVERLOADED`, which the client resent only when an earlier copy was its own. The
  leader marks a numbered commit unsettled before anything can refuse it, and clears the mark only
  after the transaction or update left its session, unless the outcome was unknown: then the mark stays until the
  record exists or the term ends. A copy reads the mark before the record, so "no mark, no record" proves that no
  copy applied. The first mark of a newer term drops the marks of earlier terms at once (every entry of an earlier
  term is applied before the new term serves, so they say nothing more); marks of earlier terms and marks whose
  record exists are also dropped once the map doubled, on a virtual thread. A mark left by an unknown outcome is not
  dropped by a timeout, nor by the applied index passing its entry: the proposer does not learn the log index of an
  entry whose reply was lost, and while the term lasts such an entry may still be appended and applied, so a timeout
  could turn a copy's answer into a wrong "not applied". It costs only `OUTCOME_UNKNOWN` answers to that write's
  copies until the term ends. A transaction or update whose commit is in flight does not expire
  (`OpenTransaction.commitProposed`): its key lock belongs to that commit, and the commit's handler removes it once
  the outcome is known; expiring it earlier would hand the key to the next update while the commit may still apply. The lookups run on the node's virtual threads, never on the control threads that complete the read
  barrier. A write staged in a transaction that carries a request id is refused (`INVALID_REQUEST`): a resent copy
  would be staged twice.
- **Writes staged in a transaction are not numbered, by design.** A staged write changes nothing until the commit,
  and the commit is numbered. So the only thing a numbered staged write would buy is resending it after a lost
  answer, inside the same transaction on the same leader, which holds the transaction in memory only: after a leader
  change the transaction is gone anyway (the client gets `UPDATE_RETRY` and runs it again). Numbering it would need a
  record per staged write, keyed by transaction, kept on the leader until the transaction ends, and a copy arriving
  after a leader change would still have nothing to answer from. The rule instead: a staged write is sent once, and
  if its outcome is unknown (`UNAVAILABLE`, a deadline), the caller rolls the transaction back and runs it again
  from the start; it must not send the staged write again, which could stage a `merge` twice. Nothing of the
  abandoned transaction was applied, so this is always safe.
- **Where the session RPC lives.** `openClientSession`, `RequestContext.requestId`, `CapabilitiesResponse.clientSessions`
  and `numberedBatches` are in the one `rocksdb.proto` and `RocksDBService`, not in a separate service: the client
  API is the same for a single server and a cluster (a single server answers `clientSessions = false` and the default
  `NOT_IMPLEMENTED`), and a client must not need to know which one it talks to. The additions only add fields and an
  RPC; removed numbers are `reserved` (`WorkloadProfile` 5 and `CDC`, `RequestContext` 2), and `timeoutNanos` stays
  field 4.
- **The client** (`GrpcConnection`, `SessionWrites`) opens the session on its first write, numbers the writes listed
  above, the write that ends an update only when the update was opened through it. On an unknown outcome (`UNAVAILABLE`,
  `CANCELLED`, `OUTCOME_UNKNOWN`, a deadline error that does not say "not applied", gRPC's own `DEADLINE_EXCEEDED`
  included, which says nothing about the copy) it sends the same numbered
  write again, after 50 ms doubling to 1 s, to whichever node its channel reaches, until the caller's deadline (30 s
  without one). After such an outcome, a "not applied" answer (overload, deadline) also leads to another copy. A first
  copy refused with `SESSION_EXPIRED` was not applied, and goes again under a new session (at once the first time, then
  after the same growing pause, and within the deadline: a server whose sessions keep expiring ends the write with
  `SESSION_EXPIRED`, not applied); a later copy refused so ends with `OUTCOME_UNKNOWN`. Opening a session is retried
  like a write after an unknown outcome (two openings only leave an unused session, which expires); writes waiting
  for an opening that another write started open again within their own deadline, with the same growing pause, when
  that opening ended without refusing sessions for good: an unknown outcome, overload, or a "not applied" deadline
  error (the opener's deadline, for example while the proposer still had the registration queued). Only a definitive
  refusal (`NOT_IMPLEMENTED`, for example) ends every write waiting for it. A write that ends without a session was
  never sent, so it ends flagged not applied whatever the opening answered: the opening's own error with the flag
  (gRPC's deadline on the opening, mapped to `READ_DEADLINE_EXCEEDED`, would otherwise read as an unknown outcome), or
  `SERVER_OVERLOADED` flagged for an opening of unknown outcome (R26-3, as the Rust client; before, the error came
  back unflagged). Nothing is sent once the deadline or the window has passed, for either kind of caller (R26-4, the
  Java side of R20-1): each copy checks the time before it is sent, so a session that opens only after the 30 s
  window of a caller without a deadline ends the write flagged not applied instead of sending a copy that, lost,
  would end `OUTCOME_UNKNOWN`: `SERVER_OVERLOADED` at the window itself while it still waits for the opening (since
  R27-3), `READ_DEADLINE_EXCEEDED` if the session came and the window passed before the copy. A caller with a deadline got that already from the
  connection's own "already expired" check; a caller without one has no gRPC deadline, so nothing stopped the copy.
  No session is opened once the deadline or the window has passed either (as Rust's `remaining` before every
  opening): the write ends `SERVER_OVERLOADED` flagged not applied without an opening (`noSessionIsOpenedOnceTheDeadlineHasPassed`;
  before the R26 review the opening went out with a 1 ns gRPC deadline). An opening of a caller without a deadline,
  sent with no gRPC deadline, is ended at the window by a local timer, as Rust's `within_window`: that write, and one
  without a deadline waiting for the same opening, ends `SERVER_OVERLOADED` flagged not applied
  (`anOpeningThatNeverAnswersEndsACallerWithoutDeadlineUnsentAtTheWindow`, hung before; a late answer only leaves an
  unused session, which expires). A write that waits for an opening another write started is bounded by its own
  deadline, not the opener's (R27-3, as Rust's `timeout_at` on the opening lock): a local timer at its deadline ends
  it `SERVER_OVERLOADED` flagged not applied if it still has no session and no copy of it may have run, and the
  opening goes on for the opener (`aWriteJoiningAnOpeningEndsAtItsOwnDeadlineNotApplied`; before, a 300 ms write
  joined to a 30 s opening waited 30 s, and a synchronous caller got `READ_DEADLINE_EXCEEDED` unflagged at its
  deadline plus 5 s). That covers the usual joiner too: a write whose first copy was refused `SESSION_EXPIRED` (not
  run) and that is re-issued into the new session another write is opening. The timer tests `maybeApplied`, not
  "a copy was sent", and a refused copy clears the sent flag, so a cancel or close before the next copy also ends it
  not applied (`aWriteReissuedAfterAnExpiredSessionThatJoinsAnOpeningEndsAtItsOwnDeadlineNotApplied`; in the R27
  round the timer tested `sent`, which the refused copy had set, and such a write waited for the opener's deadline,
  then ended `OUTCOME_UNKNOWN` in a synchronous call). That timer is cancelled once the opening completes or the write
  ends (R28-7: it was a timer of the JDK's shared queue without a handle, which held the write, its command and its
  payload until its deadline, for ever with a "forever" timeout; `aWriteThatWaitedForAnOpeningIsNotHeldUntilItsDeadline`).
  A write is numbered in its session under its own lock, and never once it ended (R28-6): the numbering set the
  session and then the sequence without it, so a cancel, a close or that timer landing in between gave back the
  stale sequence (0, the number of another write still in flight, which the server's floor then passed, refusing that
  write's later copy as stale) and left the new number in flight for the life of the session, holding back the floor
  below which every replica purges its records (`aWriteCancelledWhileItIsNumberedLeavesNoNumberBehind`). A synchronous wait
  that cancels a numbered write before any copy was sent now throws `READ_DEADLINE_EXCEEDED` flagged not applied
  (`aSynchronousWaitThatEndsBeforeACopyWasSentIsNotApplied`); a cancel after a copy was sent still ends
  `OUTCOME_UNKNOWN`. Unlike Rust, a copy of a caller without a deadline is not cut at the window by a
  local timer (that would end the write `OUTCOME_UNKNOWN` while the copy may still be applied): the connection's
  keepalive (10 s, then 5 s for the ping's answer) ends a call to a silent node. One difference in error type stays:
  an opening that a caller with a deadline sent and that met gRPC's deadline ends `READ_DEADLINE_EXCEEDED` flagged
  not applied, where Rust gives `SERVER_OVERLOADED` flagged. The Java connection maps gRPC's deadline to
  `READ_DEADLINE_EXCEEDED` before `SessionWrites` sees it and cannot tell it from the server's own; Rust keeps an
  unprefixed tonic status. Both are flagged, so a caller that checks `isNotApplied()` sees the same outcome. The caller sees
  `OUTCOME_UNKNOWN` only when the deadline ended the copies without an
  answer. Only a `getForUpdate` that opens an update (no transaction id) makes its id one whose write is numbered.
  Closing the connection ends the writes waiting for a resend: `OUTCOME_UNKNOWN` if a copy was sent, else
  `SERVER_OVERLOADED` flagged not applied (not sent; also a write sent on a closed connection, or closed while it waited
  between openings of a session: before the R26 review these two came back unflagged, `aWriteEndedByCloseBeforeItWasSentIsNotApplied`). A caller that cancels the future of a write stops it the same way: no copy is sent
  any more and the copy in flight is cancelled; if a copy was sent the future is not cancelled but ends with
  `OUTCOME_UNKNOWN` (`cancel` returns false), else it is cancelled, not applied. A batch call does the same for its
  chunks: it counts as sent once a chunk was applied or the chunk in progress was handed to the channel, not when
  the chunk merely waits for its session. The cancel and the send cannot cross: a cancel marks the future
  `stopping` before it reads whether a copy was sent, and a copy sets `sent` before it reads `stopping` (the batch's
  too, for a chunk), both volatile, so either the caller sees the copy as sent (unknown), or the copy is not sent
  (cancelled). A synchronous call with a deadline waits for its answer at most that deadline plus 5 s (saturated, so
  a timeout of "forever", which `RequestContext` clamps to `Long.MAX_VALUE - 1` ns, does not overflow), then cancels
  it so: a lost callback cannot block its thread for ever. A synchronous call without a deadline (batch, ingest and
  analytical profiles) waits until the write ends, interruptibly: its copies have no gRPC deadline and a batch sends
  its chunks one after the other with no bound on the whole, so any fixed bound would cut a long but healthy write
  (a bulk ingest through the synchronous API) and end it unknown partway. At most 65,536 open update ids are remembered; past that the
  oldest is forgotten (counted, and logged once a minute), and the write that ends it is sent unnumbered, once, as
  without sessions. The connection reads the server's capabilities once, at its handshake, and logs whether it uses
  sessions: a connection made before the cluster offered them does not start using them, and must be made again
  after the upgrade.
- **"Not applied" on the wire.** Whether a failure says that the write was not applied is a flag of the exception
  (`RocksDBException.notApplied`, `isNotApplied`), set where the server knows it: the proposer's deadline and
  admission refusals, a forward whose deadline passed before it was sent, a request context already expired (also
  the client's own check before it sends, in the gRPC, Thrift and embedded connections: a numbered write that meets
  it on a first copy ends with the deadline error, not `OUTCOME_UNKNOWN`). The
  gRPC server carries it in a trailer, `rockserver-applied: no` (any other RocksDB failure gets `maybe`;
  `AppliedTrailerInterceptor`, which reads the cause the status keeps until the call closes), and the client sets the
  flag from it. The message is unchanged, so older clients, which read "not applied", "already expired" and "before
  downstream dispatch" in it, behave as before; a client reads those words only from a server that sends no trailer
  (`GrpcConnectionDelegate.LEGACY_NOT_APPLIED_MESSAGES`, pinned by a test). Rewording a message can therefore no
  longer turn a "maybe applied" into a "not applied": a deadline error without the flag is an unknown outcome.
  `SERVER_OVERLOADED` keeps meaning "not applied" by its type. `SESSION_EXPIRED` carries the flag too (R4-1): the
  copy it answers was refused before it ran. The proposer sets it when the refused copy is its only one; when one of
  its own earlier attempts may have been appended it answers `OUTCOME_UNKNOWN` instead (see "Expiry"), and a copy of
  a commit refused by `CommitOutcomes` because the session is gone gets it as well. The flag speaks of that copy only:
  copies the client sent earlier are the client's to judge, so the core client still ends a write whose earlier copy
  may have applied with `OUTCOME_UNKNOWN`, and a `SESSION_EXPIRED` it passes to its caller (no copy applied, the
  sessions kept expiring until the deadline) always has the flag, also from a server without the trailer. A server
  of round 3 sent `maybe` on `SESSION_EXPIRED`; the client does not read the flag on it, so that changes nothing. A
  follower that forwards keeps the flag: the leader's trailer sets it on the exception the follower's server fails
  with (a forwarded write is sent again only when the earlier forward was not sent, so the flag stays true of it).
  `NumberedWritesTest.aWriteRefusedByTheLeaderBehindAForwardIsFlaggedNotApplied` checks it on a three-node cluster:
  a commit sent to a follower, forwarded, refused by the leader's full byte admission, reaches the client as
  `SERVER_OVERLOADED` with the flag.
- **The Rust client.** `rust-client` numbers its writes the same way (`sessions.rs`, the counterpart of
  `SessionWrites`), when the server advertised `clientSessions` at connection: the same writes are numbered (outside
  transactions, commits, the write ending an update opened through the client, schema changes, `delete_range`, batch
  chunks when the server also advertises `numberedBatches`, split by `sessions::split_batch` with the server's rule),
  with the same resend rule (an unknown outcome is a lost call, `OUTCOME_UNKNOWN`, or a deadline error without the
  `rockserver-applied: no` trailer; after one, a "not applied" answer resends; pauses from 50 ms doubling to 1 s; the
  caller's deadline, or a 30 s window for a context without one), the same `SESSION_EXPIRED` handling (a first copy
  goes again under a new session, a copy after an unknown outcome ends `OUTCOME_UNKNOWN`), the same window per batch
  chunk (a context without a deadline gives each chunk the 30 s window from when it is sent, as `SessionWrites` does,
  not one window for the whole call, which a long batch would use up before a late chunk needs a resend:
  `each_chunk_of_a_batch_without_a_deadline_gets_its_own_retry_window`) and the same opening rule
  (writes wait for the one opening the session; each opens again in its own time if that fails). Its own failures
  carry the server's message format, `RocksDBError: [uid:TYPE] ...`, and the trailer when not applied, so a caller
  reads them like the server's (`sessions::is_outcome_unknown`, `is_not_applied`, `rocksdb_error_type`). Where Rust
  differs: dropping a write's future is its cancel (its number is final, and nothing more is sent, but whether a sent
  copy applied stays unknown, since there is no future left to answer); a streamed multi write that is numbered reads
  its whole input stream first, and one with results reads every result before returning them, since a copy may
  have to be sent again. Its deadline is taken before it reads the input (R6-4: it used to be taken after, so a
  slow input stream got a full timeout more), as when the stream is sent while it is read: an input that outlasts the
  deadline ends the write with `READ_DEADLINE_EXCEEDED` flagged not applied, nothing sent, and an input over the
  numbering limit is sent with what is left of it (`client::numbered_multi_tests`, which fail without the change).
  A caller without a deadline reads its input without a bound, and its retry window (`with_session_retry_window`,
  30 s by default) starts when the write is sent, once the input was read, as for each chunk of a batch and in the
  Java client: an input that took longer than the window to read still gets a full window of resends
  (`a_numbered_multi_write_without_a_deadline_starts_its_retry_window_when_sent`, a 1 s window and 1.5 s of input,
  which without this ends unknown after one copy). It does so only up to `sessions::NUMBERED_MULTI_MAX_BYTES` (4 MiB, counted as the server
  counts it, a quarter of the server's limit): a larger stream is sent unnumbered, streamed item by item as before
  numbering existed, so a large `put_multi` of an INGEST caller still has no size limit and is not held in the
  client's memory, but is sent once (a lost call leaves its outcome unknown, as before). Splitting it into numbered
  chunks, as batches are, was not done: a multi write is one call with one result list and no "partly applied"
  answer, and the Java client does not split these either (it has the same 4 MiB fallback); a caller that wants every item numbered uses
  `put_batch`/`merge_batch` or smaller calls; a view whose context already carries a request id (`RequestContext::with_request_id`) is
  numbered by its caller and sends once; `without_client_sessions()` sends everything once, as before. A caller
  without a deadline sends each copy and each session opening with no gRPC timeout, so that the server still sees a
  caller without one, and awaits it under a local `tokio::time::timeout_at` of its window (R19-1). Before, nothing
  else ended such a copy: a silent node (power loss, SIGSTOP, a partition with the host still up) held the write, and
  the opening, forever instead of for the window, since the window only decides whether to resend after an answer. A
  copy that outlasts the window ends `OUTCOME_UNKNOWN`, an opening that does ends not sent (`SERVER_OVERLOADED`,
  flagged not applied): `a_copy_that_never_answers_ends_a_caller_without_deadline_at_the_window` and
  `an_opening_that_never_answers_ends_a_caller_without_deadline_unsent_at_the_window` (both hung before). Nothing
  is sent once the window has passed, for either kind of caller: `remaining` checks the time before every opening and
  every copy, so a session that opens only at the end of the window (a late lock or a late answer) ends the write as
  `READ_DEADLINE_EXCEEDED` flagged not applied, like a caller whose deadline passed, instead of sending a copy that
  could still reach the server and end `OUTCOME_UNKNOWN`
  (`a_session_opened_after_the_window_ends_a_caller_without_deadline_unsent`, R20-1, red before). The current
  session sits in a slot of its own (a `std::sync::Mutex`, held only to read or swap it), apart from the lock a write
  holds while it opens a session (a `tokio::sync::Mutex<()>`, awaited under `timeout_at` of each write's deadline).
  Until R27 both were one async mutex: a write whose copy met `SESSION_EXPIRED` forgot the session under that lock
  with no deadline, so while another write was opening a session (for up to its deadline or the 30 s window) a 300 ms
  write waited that long (R27-4,
  `a_write_whose_session_expired_is_not_held_by_another_write_opening_one`, 3 s against a 3 s opening before). Unlike the
  Java client, `RockserverClient::connect` uses one address and adds no settings of its own: given a string or URI it
  has tonic's defaults, with no HTTP/2 keepalive or connect timeout, so resends go back to the same node, through the
  server's forwarding, and a dead peer is seen only by a call's deadline or the window. `connect` takes any
  `TryInto<Endpoint>` and uses an `Endpoint` as given, so a caller that wants dead-peer detection passes one set up
  with `http2_keep_alive_interval`, `keep_alive_timeout` or `connect_timeout`. Only failover over several nodes needs
  a tonic `Channel` built by the caller (`Channel::balance_list`) and passed to `RockserverClient::new`. The unit tests
  (`sessions::tests`) drive the resend loop with scripted answers, as `SessionWritesTest` does; the live test
  `tests/live_server.rs` runs every numbered call against a follower of a three-node in-process cluster
  (`RustClientLiveTest`, on when `-Drockserver.rust-client.dir` names a buildable copy of the crate), sends two copies
  of one numbered merge under the same request id (applied once), and a multi write over the numbering limit
  (streamed unnumbered); the Java side then checks every node. The live test is `#[ignore]`: a plain `cargo test`
  reports it as ignored, not as passed, and `cargo test -- --include-ignored` (what `RustClientLiveTest` runs) fails
  if `ROCKSERVER_TEST_ADDR` is unset. Neither runs in CI: the crate's workspace lists `rust-data-generator`, a git
  submodule (`.gitmodules`) that a checkout without `git submodule update --init` lacks, and `examples-runner`, the
  other member, depends on it by path, so the member cannot be dropped or made optional (Cargo has no optional
  members); with the submodule checked out the crate builds in place. Wiring cargo into the Maven build is left to
  whoever sets up CI with a Rust toolchain. A resend across a real leader loss is tested too
  (`numbered_writes_resent_across_a_leader_loss`, driven by `RustClientLiveTest` through a proxy and signal files):
  a merge, a multi merge and a multi put with results, sent to the leader, commit on the followers while the leader's
  apply is stopped; the leader is halted, which drops the calls, and the proxy moves to a follower on the same port;
  the client sends them again under their numbers (`RockserverClient::numbered_resends` counts the copies), and they
  answer with the first copy's results, applied once on every node. The live test also sends two copies of a
  numbered multi merge and multi put with results under one request id (the second answers from the record with
  the first's results) and every result variant over the numbering limit (streamed unnumbered, with its results).
  Not tested in Rust: the `delete_multi` variants over the limit (about 37 000 keys of the test's 8 bytes, which a
  replicated server applies one by one), which share the code of the others.

**Apply.** Every replica applies entries in log order, on an apply thread of its own, through
`EmbeddedDB.ApplyWriter`: a native transaction held by the apply thread itself, which runs the same code as the
client API's writes with a transaction (key encoding, schemas, merges, previous values and deltas are the same
byte for byte), without what a client call adds around it (the lookup and lock of the transaction id, column use
counting, action logging, operation counters and timers). Its writes are untracked: the apply thread is the only
writer, so the commit validates nothing. `applyTransaction` decodes the entry on the Ratis thread (on the leader it
reuses the requests `startTransaction` admitted, which for its own batches are those its proposer encoded, see
"Encoded once"), queues it and returns a future; the apply
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
- **Partitioned apply** (`parallelApplyThreads`, `--parallel-apply-threads N`, off by default) runs the data
  writes of a group on N lanes, each a platform thread with an `ApplyWriter` of its own, and gives the results
  and the database of the serial apply, byte for byte, whatever the timing. Each request gets a lane from the
  RocksDB keys it can touch: the first key part of a column whose schema has fixed key parts (every RocksDB key
  of such a column starts with it, a bucket too), or else the whole column. Two requests that can touch the same
  key are in the same lane, so each reads every earlier write to its keys, in log order, in its lane's writer. An
  entry with a request that touches two lanes (a multi-key write over different first key parts, a transaction
  over two columns), a `VerifyColumn` and every entry applied alone take the serial path, after what came before
  them is committed. The apply thread sorts each entry out as the serial path does (floors, records, sessions,
  clock), writes down its floor, session and position writes and hands its requests to their lanes, then goes on
  with the next entry: none of that depends on the results, which only the records hold. At the end of the
  group, or before an entry that needs the serial path, it waits for the lanes, writes the records in log order,
  and commits the lanes' writers and its own in **one** atomic write (`ApplyWriter.commitWith` joins their
  native batches as RocksDB's `WriteBatchInternal::Append` does), so a crash keeps a prefix of the log as
  before. RocksJava has no call for that append, so `ApplyWriter.joinWriteBatches` does it on the serialized
  batches (R2-PA-5): a batch is a 12-byte header (sequence, which the database assigns on write, and count) and its
  records, each one self-delimiting and independent of where it sits, so the records of every batch in turn under a
  header with the summed count are the batch of all their operations in order. `ParallelApplyTest` checks it byte
  for byte against a RocksJava batch made of the same puts, deletes, merges, range and single deletes in that order,
  and with the batches `commitWith` really joins: two apply writers (transactions over a batch with index) on two
  user columns (column-family records), one with writes rolled back to a save point, against one writer that made
  the kept operations, and the database after `commitWith`. A transaction's batch has no other markers: those come
  only with a two-phase prepare, which the apply never uses;
  a wrong count would also be refused by RocksDB's own check on write, and a lost or doubled record would show in the
  randomized comparison with the serial apply. A rejected request rolls back its whole entry in the serial path; here its lane marks the entry as a
  cut and the lanes skip what follows. At the end the lanes roll back to their save point before the cut (one
  per entry and lane), the entries before it commit, the in-memory changes of the entries from the cut on are
  undone, and those entries are applied again: the cut one on the serial path, which rejects it exactly as the
  serial apply does (commit, then one request at a time), the next ones in lanes. A failure is logged only where it
  is recorded (R2-PA-4): not by the lane that cut, nor by a group or multi-request attempt that the serial path runs
  again one request at a time, so a failing merge operator gives one warning per failed request, as without lanes
  (`PoisonEntryTest` counts them). Concurrent writers cannot share
  one RocksDB transaction, and committing each lane on its own would let a crash keep a lane's writes without the
  position, whose replay would apply them twice: hence one writer per lane, joined at commit. Replies wait for
  the group's commit as before. A node in lanes and a serial one hold the same data, so nodes may differ in this
  setting. What it costs: a rejected request (a guard that no longer holds, a failing merge operator) runs its
  entry twice and the entries after it in the same group again, and each change between lanes and the serial
  path adds a commit; `apply.parallelCuts` counts the cuts. See [performance.md](performance.md), item 16.
- **`deleteRange`** cannot run in a transaction either, but it is a plain write: the group before it is
  committed, the range deletion is written without the WAL, and its record and position go into the group
  that follows, without a flush and without a WAL sync. A crash replay is still correct: with atomic flush a crash
  keeps a prefix of the writes made without the WAL, so the deletion survives either with its record (the entry is
  not replayed) or without it, and then without anything written after it. The replay deletes the same range
  again from the state right after the first deletion, which removes nothing more, and computes the same result
  (a range deletion returns nothing, or a rejection that depends only on the column and the keys).
- **Schema changes** cannot run in a transaction. All memtables are flushed first, so that
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

**Validation before the log.** What the proposing node checks before an entry reaches the log is what does not
depend on the log: the internal column (`requireUserColumn`), the shape of each entry (its constructor: counts of keys
and values, kinds of request types, which the typed core API already guarantees), and on the leader that the batch
decodes, holds only data writes when it holds several, and that a guarded entry belongs to this term. Nothing more is
checked there on purpose. Whether a column exists, a key fits its schema or a merge operator loads depends on the
entries before it in the log, which the proposing node has not applied yet (a `createColumn` may be in flight just
ahead of the write): a check there would refuse writes that are valid at their position, or repeat what apply checks
anyway. Apply rejects such entries the same way on every replica (below), at the cost of one log entry.

**Failures during apply.** A failure is classified by where it comes from, not by its exception type.

- **Deterministic failures** are identical on every replica: they become the request's result, the entry's
  writes are rolled back, and the log moves on. They are:
  - logical rejections (missing column, malformed keys, a wrong request type...);
  - a native status that follows from the request and the data (`InvalidArgument`, `NotFound`, `NotSupported`);
  - a **merge operator that fails** (throws, or returns no value), as `MERGE_OPERATOR_FAILED`. RocksDB reports a
    failed merge as a `Corruption` status, which alone would look like a broken disk. The operator records its
    failure on the thread that ran it (a read of a merged key runs the operator on the reading thread), and the
    apply names it. Bucketed columns run the operator from Java, and report it directly. A failure of the JVM
    itself inside the operator (out of memory, stack overflow, an `IOError`, or a class whose initialization one
    of these ended) stays local, even when the operator wraps it: the whole cause chain is checked first;
  - an uploaded merge operator that cannot be loaded or instantiated (a missing class, a constructor or static
    initializer that throws, an `Error` included, but not a failure of the JVM), as `INVALID_REQUEST`. Nothing is
    stored for it, so a replay after a restart rejects it the same way;
  - any other exception of the apply code (a bug: a `NullPointerException`, a bad cast), as `INTERNAL_ERROR`.
    Every replica runs the same binary, so it fails the same way everywhere.

  The recorded text of these failures is the same on every replica: the exception's class and message, without
  native addresses, identity hashes (as in class loader names), thread names, which differ between processes, or
  absolute file paths (a RocksDB status names the node's own data directory), which differ between nodes: each
  becomes `<path>`. A path under the node's own directories (database, WAL, and the directory holding the database)
  is removed whole even when it contains white space; any other path ends at white space. Only these recorded texts
  lose their paths: the error messages of a single node (`Failures.sanitize`) keep them. The full failure is only logged. The messages themselves come from the JDK, RocksDB and the
  Rockserver code, so every replica must run the same Rockserver build on the same JDK build: during a rolling
  upgrade a deterministic failure may be recorded with different text on the replicas of the two versions (its
  error type is still the same).
- **Local failures** report this machine, not the entry: an `IOException`, an `Error` (out of memory...), a
  native status of the storage or the process (`IOError`, `Corruption`, `NoSpace`, `Busy`, `TimedOut`,
  `Aborted`, `ShutdownInProgress`, or no status at all), a lost local transaction, a failed commit of the group,
  or a failed write of the result record. They stop the replica:
  - its Raft server stops first, so a failed leader stops leading;
  - the process exits with code 3 (also when Raft closes under a running node: see "Long pauses");
  - it never skips the entry.
- **Every merge is checked at apply.** A merge whose request type does not return the merged value (`none()`)
  would store its operand without running the operator; if the operator failed on it later, every read of the
  key would fail, and so would the memtable flush that merges it: RocksDB stops the database with a background
  error, on each replica in turn, and replaying the log stores the operand again, a crash loop. So right after
  staging the operand (single merges, multi-merges and the chunks of `mergeBatch`, in columns with a merge
  operator), the apply reads the key through its transaction (the batch over the database), which runs the
  operator. If it fails, the entry is rolled back to its savepoint and the request gets `MERGE_OPERATOR_FAILED`,
  as a merge that returns its value does. This costs one read per merge (in the memtable or block cache when the
  key is hot, a disk read otherwise); merges that return the merged value, and bucketed columns, which merge in
  Java, already merge and skip it. The load bench has a `MERGE` operation to measure this cost (`--only MERGE`,
  `--merge-rate`, see performance.md; WA-15); that run has not been done. Its values stay small: with the
  string-append operator each merge adds 9 bytes, so a key takes 64 merges only (16 keys of a chat at a time, then
  the next 16), and the read at apply costs the same at the end of a run as at its start. A later flush or compaction then only merges operand sequences that a read
  already merged at apply: each operand was checked over the base and every operand before it, which is what a
  full merge at flush or compaction computes, and a partial merge that the operator declines leaves the operands
  as they are. With a deterministic operator, that cannot fail. An operator that is not deterministic (it depends
  on time, randomness or state outside its inputs) can still fail at flush or compaction on an operand that passed
  at apply, or pass on one replica and fail on another: the replicas then diverge or stop, which is why merge
  operators must be deterministic.

Runbook: **every replica stops at the same entry** (each exits with code 3, its log names the same position and
the same cause, and each restart stops there again). That is a deterministic failure still classified as local.

1. Read the cause in the log of any replica. A storage cause that is real on all of them at once (every disk full)
   is fixed in place: free the space, restart, and the entry applies.
2. Otherwise fix the code, not the data: a build that classifies the cause as a result (or fixes the bug that
   raises it), or a merge operator that no longer fails on that input. Install it on **every** node, then start
   them: each replays the entry and records the same result.
3. Do not skip the entry, and there is no switch to do so. A replica that skips it lacks its effects and records
   no result, while the others (or any later replica that replays the log from an older snapshot, or a new member
   that catches up from the log) apply it. The entries after it then run on different data, and the replicas
   diverge silently, with nothing that ever detects it. A skip is only safe if every replica, present and
   future, makes the same decision at the same position, which only the code can do: that is step 2.
4. Never edit the Raft log or the applied position by hand. A single replica that stops while the others go on
   has a local fault: replace it (see "Replacing a node").

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
  recovery (`absolute-consistency` forced off). With atomic flush, whatever triggers a flush flushes every column
  that has data, so a replica raises every column's `write-buffer-size` to the write buffer budget
  (`database.global.write-buffer-manager`, default 128 MiB, or half the block cache when it is 0 or null): the
  `WriteBufferManager`, which counts the memtables of all columns, decides when to flush, instead of the hottest
  column filling its own 64 MiB and flushing every cold column into a tiny file each time. A per-column
  `write-buffer-size` in the Rockserver config is replaced on replicas; raise the budget to flush less often. After a crash or power loss the database is the last flushed
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
   voting peer for its current term, using `TermService` on the Ratis gRPC port. Two rounds may run at once, so a
   read that arrives just after a round was sent starts the next one at once; reads that arrive while two run share
   the one after. Each peer has at most two term queries in flight, and rounds that find both taken share the next
   one: a paused peer does not pile up calls. The read may proceed once
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
   With follower reads (below) that shortcut is unsafe, and `awaitLeadership` waits for the commit index `c`, as
   `await` does.

**Why this is safe.** Suppose a leader with a higher term had committed something before the read arrived.
Then a majority had already moved to that higher term, and terms never decrease. A majority answering after
the read arrived must overlap it, so it could not report the old term. One round costs one network round
trip, with no fsync, and it serves every read that arrived before it started.

**Why not a leader lease.** Ratis 3.3.1 has one (`RaftServerConfigKeys.Read.leaderLeaseEnabled`, off here), and its
idea is sound: `LeaderLease.extend` takes, for a majority, the send time of the last `appendEntries` each follower
answered (`FollowerInfo.getLastRespondedAppendEntriesSendTime`), and the lease lasts `leaderLeaseTimeoutRatio` (0.9)
of the minimum election timeout from there; with PreVote a follower that heard from its leader within the minimum
election timeout refuses pre-votes (`VoteContext.checkLeader`, `FollowerState.isCurrentLeaderValid`). It is not the
`ReadIndexHeartbeats` reuse. But it is not safe as shipped, from the 3.3.1 class files:

- `GrpcLogAppender.AppendLogResponseHandler.onNext` records that send time for every reply before it looks at the
  result, so a `NOT_LEADER` reply from a follower that already moved to a higher term extends the lease too. The
  step-down that follows is an event queued for the leader's event processor (`LeaderStateImpl.onFollowerTerm` ->
  `submitStepDownEvent`), and only if that follower is caught up: until then a read checks a lease that a deposed
  leader's former follower just renewed.
- It is not reachable through public APIs. `LeaderStateImpl.hasLease()` is package-private; the only way in is Ratis'
  own ReadIndex, which falls back to `ReadIndexHeartbeats`, with the reuse above, whenever the lease is not valid.

A lease of our own, built on `TermService` replies the way CheckQuorum leases are, is not done either: a term reply
says nothing about when the follower stops refusing pre-votes, and a follower would have to report how long ago it
heard from its leader. Even then, a follower that restarts forgets its leader and grants pre-votes at once, and Ratis
can move leadership by itself (a peer of higher priority, or `admin transfer-leader` sent to any node) with an
election that skips the pre-vote. The barrier costs one round trip, and that is not worth those cases.

### Follower reads

With `-Dit.cavallium.rockserver.distributed.follower-reads=true` (off by default; nodes may differ, see below), a
follower serves `get`, `exists`, `getMulti` and `existsMulti` outside a transaction from its own database (Raft
dissertation §6.4, TiKV follower read). Updates, transactions, iterators and ranges still go to the leader.

1. The follower asks the node it knows as leader for a read index (`TermService` `ReadIndex`, on the Raft port). The
   leader records its commit index `c` when the request arrives, confirms its leadership with a barrier round that
   starts after that (`ReadBarrier.readIndex`, which waits for no apply), and answers `c` and the term `t` in which it
   confirmed its leadership (16 bytes; the 8-byte answer of a round-1 node is refused, and the follower forwards).
2. The follower waits until its own applied index reaches `c`, then reads its own database.
3. It keeps the answer only if its database then holds no entry of a term later than `t` (below), and forwards the
   read otherwise.

Every write acknowledged before the read arrived at the follower was committed before the request reached the
leader, so its index is at most `c`, and a follower that applied `c` has it. The follower must wait for the whole of
`c`, not for the entries of earlier leaders as a leader does (`awaitLeadership`): it did not acknowledge those writes
itself. Reads share requests, with at most one in flight per follower: a read that arrives while one is in flight
waits for the next, because the one in flight may reach the leader before a write that was acknowledged before the
read arrived. The answer of a node that is no longer the leader is a failure, as for any barrier.

**The leader waits for its apply too.** A follower applies an entry as soon as it learns that it is committed, which
can be before the leader applied it, and so before the leader acknowledged the write (its apply thread may be behind,
or paused). A client can then read the new value on the follower, and a read that starts after that one ended must
not answer the old value on the leader. So with follower reads the leader's `awaitLeadership` waits for its applied
index to reach its commit index at the read's arrival, like `await`, for every leader read, update and transaction
open. It does so when the node has the option on, and from the first read index it answers, in case a follower has it
on while the leader does not. `FollowerReadTest.leaderReadSeesWhatAFollowerAlreadyServed` holds the leader's apply and
fails without this.

**A follower serves no entry of a later term than its read index's.** A leader that answered no read index yet keeps
the shortcut. A follower whose read index came from an earlier leader, in term `t`, may meanwhile apply an entry of the
new leader's term, committed but not yet applied there, as during a rolling restart that turns the option on (or any
mix of settings). Served, a later read on the new leader could answer the older value. So the state machine raises a
"visible term" (`RockserverStateMachine.visibleTerm`) to the term of the last entry of each apply group before the
group's writes are committed, and to the restored position's term before a restored database is used. The follower
checks it after its read, since the apply goes on meanwhile, and forwards the read if it exceeds `t`. Every entry it
serves is then of a term whose leader turned the wait on before answering it, or of an earlier term, which every later
leader applied before serving (its ready index) and whose own leader can no longer confirm its leadership. This costs
only the reads that straddle a change of leader. So nodes may differ in the option, and it can be turned on (or the
nodes upgraded) one node at a time; a round-1 node answers no usable read index, so a follower forwards to it.
`FollowerReadTest.followerForwardsAReadThatMaySeeAnEntryOfALaterTerm` and
`followerReadsStayLinearizableAcrossALeaderChange` cover it, with the option set on every node as in production.

**The commit index reaches the follower at once.** The follower cannot apply an entry it does not know is committed,
and Ratis sends the commit index only with appends: at once while writes flow, but on a quiet cluster only with the
next heartbeat, a quarter to a half of the minimum election timeout later. Ratis wakes every appender on every commit
and on every change of a follower's commit index (`LeaderStateImpl.updateCommit` and `onFollowerCommitIndex` call
`commitIndexChanged`, which calls `notifySenders`), and an appender on every reply to it; an appender whose follower
lags in commit index then tries to send (`GrpcLogAppender.run` checks `isFollowerCommitBehindLastCommitIndex`), but
`LogAppenderBase.newAppendEntriesRequest` builds nothing without new entries or a due heartbeat. So the node's
Raft server uses Ratis' gRPC transport with an appender of its own (`CommitHeartbeats.Rpc`, set as `raft.rpc.type` by
`RaftSettings`; the node's `CommitHeartbeats` reaches it through Ratis' `Parameters`). When the leader answers a read
index request it records the commit index `c` as wanted and wakes the appenders whose follower is known to lag below
`c` and that have no append in flight (`CommitHeartbeats.send`). An appender with nothing else to send then sends a
heartbeat, which carries the commit index, when its follower lags below `c`, no request carrying `c` went to it on its
current stream, and no append to it waits for a reply. The last condition matters: a heartbeat sent while an append is
in flight goes without its previous entry (`LogAppenderBase.newAppendEntriesRequest`), so it carries commit index 0
(`ServerImplUtils.effectiveCommitIndex`) and tells the follower nothing. The appender waits for that append's reply
instead, which wakes it again. For the same reason `send` does not wake an appender with appends in flight: woken with
too many in flight, `GrpcLogAppender.run` calls `appendLog(true)`, which sends exactly that empty heartbeat to the
follower that is already behind. The read index itself still comes from `ReadBarrier`.

If the stream to the follower breaks before the heartbeat arrives, Ratis drops the requests in flight
(`GrpcLogAppender.resetClient`) and opens a new stream with the next request, but does not wake the appender, which
would send nothing more for `c` until its own heartbeat is due. So while a request that carried `c` waits for its
reply, and for at most half the minimum election timeout after it, the appender waits at most 10 ms at a time
(`Appender.getWaitTimeMs`); when it finds its stream gone (Ratis' `shouldSendAppendEntries` true with no entries to
send and no heartbeat due, which leaves only the missing stream), it sends the heartbeat again on the new stream. A
heartbeat that only times out is not sent again: Ratis' request timeout (the maximum election timeout) is longer than
the interval of its own heartbeats, which carry the commit index too, and a gRPC stream that loses a message breaks.
The resend is bounded (R7-2, `CommitHeartbeats.CommitWindow`): the window of half the minimum election timeout opens
when a request first carries `c`, a resend does not open it again, and `c` goes again at most 3 times. Before, each
resend counted as a new first send and restarted the window, so a stream that kept ending was reopened every 10 ms for
as long as it kept ending: errors are spaced by Ratis' retry policy (`RaftSettings.APPENDER_RETRY_POLICY`), but a
stream that ends with `onCompleted` is not, about 100 streams a second. After the bound, Ratis' own heartbeat carries `c`.

The appender also has a wake signal of its own (R7-1, `CommitHeartbeats.WakeSignal`, returned by
`Appender.getEventAwaitForSignal`, the only way Ratis reaches it). Ratis 3.3.1's `AwaitForSignal.signal()` sets the
flag that it replaces (`signaled.getAndSet(new AtomicBoolean()).set(true)`) and `await` reads the new one, so a wake
given while the appender is not parked in `GrpcLogAppender.mayWait` (between building its requests and the wait) is
lost, and the appender waits its whole wait, up to a heartbeat interval. A `send` that lands in that moment, or the
reply to the append it waited for, was then lost, and later `send` calls for the same `c` are skipped: the follower
learned `c` with the next heartbeat, the R6-1 symptom through another door. The signal here keeps a wake given while
the appender is not waiting until the next wait takes it, which then returns at once, but only when that wake would
make the appender send a commit heartbeat (`Appender.wantsCommitHeartbeat`: its follower lags below the wanted `c`,
no request carrying `c` went out on the current stream, and no append is in flight, the same condition under which
`newAppendEntriesRequest` builds that heartbeat). Every other early wake is dropped, as Ratis drops it. The first
version kept every early wake, and Ratis wakes every appender on every commit (`LeaderStateImpl.notifySenders`) and an
appender on every reply; under write saturation, with the appender at its limit of appends in flight
(`haveTooManyPendingRequests`, also true with any request pending before a stream's first reply), each kept wake
bought one more pass of its loop, and that pass an empty heartbeat (`GrpcLogAppender.run` calls
`appendLog(haveTooManyPendingRequests())`, and `newAppendEntriesRequest(callId, true)` with an append in flight builds
a heartbeat with commit index 0) to a follower that is already loaded. That cost was never measured; rather than
measure it, the condition removes it: with an append in flight no early wake is kept, so under load the appender wakes
exactly as Ratis' own does, and Ratis recovers from the early wakes it drops by itself (its wait is 0 while it has
entries to send, `GrpcLogAppender.getWaitTimeMs`). The condition is evaluated on the signalling thread, outside the
signal's lock (it reads Ratis state, and that thread may hold a Ratis lock); `CommitWindow.sentCommit` is volatile for
it. A wake given while the appender waits always wakes it. The cases it keeps are the ones in which the appender would
otherwise have sent nothing until Ratis' heartbeat: the reply wakes of R6-1 (Ratis removes the request from its map,
updates the follower's commit index and only then wakes the appender, `AppendLogResponseHandler.onNextImpl`, so the
condition sees the state after the reply) and `send`. Shortening `getWaitTimeMs` while a commit heartbeat is wanted but not
sent was not enough: a wake between that call and the wait is still lost, and the wait would be 0 for as long as the
heartbeat cannot be built (a follower that needs a snapshot), a busy loop. `CommitHeartbeatsTest` shows the lost wake
on Ratis' signal (a wait after a signal parks for its whole timeout) and not on this one, and that an early wake is
kept only while the condition holds. `RatisContractTest.aCommitHeartbeatAskedForJustBeforeTheAppenderWaitsIsNotLost`
drives the race through the real appender: a test hook (`CommitHeartbeats.whenNothingToSend`) calls `send` inside
`newAppendEntriesRequest`, on the appender's thread, after it built nothing for a commit its follower does not know
yet, so the wake lands exactly between building and waiting; the other follower is stopped so that no follower commit
change wakes the parked appender by accident. The appender may also not run at all between the commit and Ratis'
heartbeat: the reply to the write's append wakes it before the leader commits (Ratis updates the commit later, on its
event thread), and the commit's own wake can land between that pass and the wait, where it is dropped, since no commit
heartbeat is wanted yet; the hook then never ran and the round timed out (R8-3, seen once in a full-class run). So if
the hook has not run within 100 ms of the write, the test wakes the appender itself, under a lock that the hook takes
before it calls `send`, so that no wake of the test can stand in for one the appender lost; a round that Ratis'
heartbeat told first is not counted. It passed in 7 runs of 5 rounds; in the two whose output was kept
the follower learned the commit index 0.6-19 ms after the `send`, and 3.0 s after it, the next heartbeat, with the signal made to drop every early wake as Ratis' does.
The test also asserts that Ratis waited on the appender's `WakeSignal` (its wait count), so a Ratis upgrade that waits
on `LogAppenderBase.eventAwaitForSignal` directly instead of through the getter fails it rather than only slowing
follower reads.

Until R6-1 the trigger was a Ratis read with its dummy message on the leader, whose linearizable read path asks the
appenders for a heartbeat round (`LogAppender.triggerHeartbeat`, from `ReadIndexHeartbeats.AppendEntriesListener`).
That round is the heartbeat above: with 3 nodes the leader commits and answers on the first follower's reply, so the
other follower often still had its append in flight, got commit index 0, and learned the commit index only with the
next heartbeat, 3 s later in `RatisContractTest` (one or more of its 5 writes in 4 of 18 runs, which watched one
follower). The appender needs no read on the leader, so nothing waits for the leader's apply and no bound on reads in
flight is needed: the cost is at most one heartbeat per follower per wanted commit index (and per broken stream), only
while follower reads ask for it, and none for a follower that an append of new entries already told. `RatisContractTest.aCommitHeartbeatSendsTheCommitIndexAtOnce`
pins it with the node's own Raft settings, with heartbeats every 3 s: over 20 writes in a row both followers learned
the commit index of each within 0.5-7.3 ms of the trigger (20 runs out of 20), and 3.0 s after a write without one
(the next heartbeat; no commit metadata entries, which `RaftSettings` turns off, would have carried it).
`RatisContractTest.aCommitHeartbeatLostWithItsStreamIsSentAgain` holds the heartbeat in a `LinkProxy` and then resets
the link: the follower learned the commit index 5-30 ms after the reset (10 runs of 5 writes), and 2.85 s after it
without the resend (`-Drockserver.test.commitResend=false`). With the old dummy read in place of `send`,
`aCommitHeartbeatSendsTheCommitIndexAtOnce` fails every run (5 of 5). `FollowerReadLatencyTest` after the change:
quiet follower reads 100/100 served by the follower in every run (p50 1.2-2.5 ms), see the table below.

The read is forwarded to the leader instead, as without follower reads, when no read index comes (no leader known, the
leader unreachable or not confirmed within a maximum election timeout, or a leader that predates the method), when the
follower is more than 1024 entries behind `c`, or when its apply does not reach `c` within 20 ms of the answer
(`Router.FOLLOWER_READ_WAIT`). A follower read is therefore never slower than a forwarded one by more than that wait
and the read index round trip. A read that fails while a snapshot install replaces the database is forwarded too: the
install raises a generation counter before it closes the old database and again once the new one is open
(`RockserverStateMachine.storeGeneration`, odd in between), and a read that fails with the counter odd or changed is
forwarded; a read that throws at once counts as failed. The database waits for the reads it admitted before it
closes, so a read under way when the install closes it either completes or fails as closed, never on freed memory.
`SnapshotInstallTest` holds an install and checks that a read seeing the odd counter is forwarded; the other case, a
read that saw the counter even and then lost its database, needs a second install on a running member, which in
process would need a partition (only the process suites have one): its decision (`Router.replacedDuringRead`) is
checked on its own, where removing either half fails. Right after a cluster starts, the first follower read or two
are forwarded (R2-RT-3; `FollowerReadTest` warms up first, which is why): traced on a fresh 3-node cluster in process,
the read index (4) arrived 16-27 ms after the read, the first one paying for the connection to the leader, and the
follower already knew that it was committed and had handed it to its apply thread, but its first apply group (the
genesis, the column's creation, the first write) took another 30 ms: a schema change flushes the memtables and syncs
the WAL, and the apply code runs cold. The 20 ms wait then ran out, and the read was forwarded, correctly. From the
second read on every read was served (49 of 50, 3 runs alike); neither leader readiness nor the commit index was
late. A read that arrives while the counter is odd is forwarded
without touching the database: during the install the applied index already reaches the snapshot once the new
database is open, before the counter is even again (`SnapshotInstallTest` holds an install there).

**Why still off by default.** Measured in process (3 nodes on one machine and one disk, `FollowerReadLatencyTest`,
100 rounds of a write on the leader and a read of it on a follower, in turn per mode), with the appender above
(R6-1); three runs, on a machine busier than for the earlier figures (the forwarded rows are about 1 ms slower than
they were, so compare rows within this table, not with older numbers):

| Read after write | Quiet cluster, p50 / p90 | One writer in the background, p50 / p90 |
|---|---|---|
| Follower read | 1.2-2.5 / 3.8-5.1 ms, 100% served by the follower | 3.5 / 6.5-7.0 ms, 100% served |
| Follower read, without `CommitHeartbeats` | 26.6-27.3 / 31.4-32.8 ms, 0% served (waits, then forwards) | 6.3 / 7.2-8.0 ms, 100% served |
| Forwarded, leader waits for its apply (follower reads on) | 3.6-4.0 / 7.7-7.9 ms | 3.3-3.6 / 4.9-5.6 ms |
| Forwarded, leader shortcut (follower reads off) | 2.5-3.7 / 6.5-8.6 ms | 3.0 / 4.5-5.2 ms |

(The quiet follower read: 2.2 / 3.8 ms in the R6-1 run, 1.2 / 5.1 and 2.5 / 5.1 ms in two later runs; the other
rows are from the two later runs.) Before R6-1 the trigger was a Ratis read on the leader, and the follower served
97-98% of the quiet reads, the rest waiting for the next heartbeat; now it serves all of them. On a quiet cluster the
follower serves reads after writes and beats forwarding. With writes flowing, a follower read waits for the follower
to receive, flush and apply the latest entries; with the appender it now ties with a forwarded read at the median
(3.5 against 3.3-3.6 ms; before R6-1, and still without `CommitHeartbeats`, about 6 ms, 3-4 ms more than forwarding)
and is slower in the tail. Turning the option on also makes every leader read wait for the leader's apply, about
0.5-1 ms more here. Under
Yotsuba's steady writes that is a latency loss on every read in exchange for taking point reads off the leader, which
only a benchmark of the Yotsuba mix can weigh. The bound of 20 ms plus a round trip is large against a forwarded read
of 3-4 ms here, so the option stays off; measure it with the load benchmark before turning it on.

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
- **End of a term.** When this node stops leading (`notifyLeaderChanged`, `notifyServerShutdown`, or a newer term's
  session replacing the old one in `leaderSession(term)`), the session of the ended term is retired (R23-B): Ratis
  calls the first two holding its server's monitor (from `changeToFollower`, and inside `appendEntriesAsync` and
  `requestVote`) and the third under its append lock, while a rollback takes the native transaction's monitor,
  which a point read or a staged write in that transaction holds for a whole native call, as long as a stalled
  disk makes it last. So on Ratis' thread the session is only sealed (`LeaderSession.seal()`: nothing registers in
  it or queues for one of its keys afterwards); a platform thread of that session (`<node>-session-close-<term>`,
  one per session, so that a stuck rollback of one term does not delay the waiters of a later one) then answers
  its key waiters with `UPDATE_RETRY` and its unsettled links, and only then rolls back its native transactions
  and updates. Their ids are already unknown to the new term (a new session starts empty, and `EmbeddedDB` ids
  are random and never reuse a live one); the replicated store is an `OptimisticTransactionDB`, so the old
  transactions hold no row locks meanwhile, only snapshots. `installSnapshot` and `close()` wait for these threads
  before closing the database (the database's own close would wait for the same monitors anyway; a session
  retired while an install swaps the store rolls back on the old database object, whose closed transaction map
  makes that a no-op). If that thread cannot be created (R23-1: an `OutOfMemoryError` "unable to create native
  thread" on a host short of memory), `retire` throws nothing into Ratis, whose thread holds its monitor or append
  lock: it answers the waiters and the links there (no native work), then runs the drain on a virtual thread,
  which needs only a carrier of the shared pool (a rollback stuck in a native call pins that carrier, a cost
  accepted only on this path). Should that fail too, the transactions stay open until they expire or the database
  closes, and the session counts as drained, so that `installSnapshot` and `close()` do not wait forever. Rolling
  back on Ratis' thread instead is what R23-B removed.
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

**Validation.** Ratis calls `preAppendTransaction` under its log write lock, in log order. Nothing it runs there,
nor under the state machine monitor it may wait for (`leaderSession`), reads the disk: the cluster version and the
cluster id are in memory (`NodeStore` reads the id when it opens and `Genesis` sets it, R24-2; before, choosing
whether a term pipelines its updates read it from the meta column under the monitor, and `startTransaction` read it
natively for every proposal). A closed store still refuses with "Closed", as the database read did: a node
installing a snapshot has closed its old store, and `getCapabilities` takes the refusal to mean "being replaced"
and offers no client sessions, where a cached null would offer them before the new store says which cluster
the node belongs to.

- **Check.** It checks the conflict keys against `ConflictTracker`, a map from key hash (or whole column) to
  the index of the last entry that wrote it, filled as entries are appended. A key written after the snapshot
  index, including by an entry still in flight, is a conflict. A chained commit reads no snapshot: on the key it
  read it conflicts only with a last writer other than its base, and its index only pins the history for the
  rest (columns, other keys). Its base may be forgotten once applied, which is no conflict: the guard decides.
  One whose base failed, or whose update expired, always conflicts.
- **On conflict** commit returns `false` and the transaction stays open, as on a single node. Commits share
  entries: the leader rejects the entry before appending it and names the commits that conflict (with history,
  or with an earlier write of the same entry); those fail and the others go again in the next entry. Rejected with
  the entry before its append, the others go back to the queue as never sent, as after a refusal for lack of room:
  their deadlines and cancellations apply again and their expiry timer is armed, so they cannot wait past their
  deadline when another requeue gets ahead of them and takes the freed slot (R21-3).
- **Outcome of a commit.** It is submitted once, to the local Raft server. Only a refusal that is known to come
  before appending (Ratis' leader not ready, or the state machine's own refusals) means "not committed"; no room
  (no attempt permit, Ratis' full pending queue) sends it again through its queue until its caller's deadline
  (back in the queue, it fails at that deadline as not applied even while every commit slot stays taken by commits
  Ratis has not answered, as any queued request does: a timer, not only the next free slot, ends it; a commit
  without a caller deadline fails so at the request timeout);
  `NotLeaderException`, which a leader stepping down also returns for appended entries, and any other
  `ResourceUnavailableException` (see "Attempt permits") are an unknown outcome
  (`OUTCOME_UNKNOWN`). A client with a session sends the commit again with the same request id, and the copy
  answers from the commit's record (see "Client sessions"). The positions
  of the conflicting commits travel as the text of the state machine's `ConflictException`, the only channel
  Ratis offers (it rebuilds the exception from its class name and message): `ConflictPositions` formats and parses
  it, and a text that does not parse, or names a position the entry does not have, fails the whole entry as a
  conflict.
- **Otherwise** the entry is appended and later applied atomically on every replica (unless a chained commit's
  guard rejects it: see above). The leader's native transaction is then rolled back, because its effects arrive
  through the log.
- **Hash collisions** can only cause false conflicts. Keys are tracked by their 64-bit hash alone, which folds in
  the column id, in a primitive open-addressing map (`Long2ObjectOpenHashMap`): registering a written key under the
  append lock allocates no object per key. Two keys with the same hash, of the same column or not, share a slot.
- **History is kept** as long as any open transaction, reservation or in-flight commit pins a snapshot. A commit
  whose snapshot is older than history that was already forgotten always conflicts, whatever released it, so a
  timing mistake can only cause a false conflict, never a missed one.
- **History is bounded.** Once the applied entries in the tracker hold more than 4M keys (about 130 MB), the
  oldest are forgotten even if a snapshot still needs them, and the transactions with that snapshot conflict at
  commit. Pruning forgets about 64k keys per call and runs after every applied entry, on the thread that completes
  applied entries. It takes the tracker lock (which appends need) once per 4096 keys, not for the whole call, so an
  append waits for one such chunk at most. It stays on that thread, synchronous: `LeaderSession` computes the
  pruned index under its own monitor (a reservation reads the applied index under the same monitor), and an
  incremental prune with a bounded lock hold gives what a background pruner would, without a hand-off per entry.

**Leader changes.** Transactions, update ids and iterators do not survive a leader change. Clients get
`UPDATE_RETRY` (an iterator call that the leader does not know, because another leader opened it, too: the database
reports it as an internal "no iterator" error, which the router turns into `UPDATE_RETRY`) and run the transaction again, which Yotsuba already does. A commit whose answer was lost with the
leader is not one of them: its client sends it again with its request id and gets its recorded result. A commit of a transaction that
another leader opened is refused before it is appended (the term check), so it can never be validated
against an incomplete history.

### Hot keys: why not parallel commits or deterministic ordering

A hot key's throughput is bounded by one commit cycle per update (the per-key FIFO lock held until apply), or less
with pipelined updates. Two newer techniques were considered and rejected for this design:

- **Parallel commits** (CockroachDB) make a transaction that spans several ranges commit in one consensus round
  instead of two, by writing its intents and a staging record in parallel. With one Raft group every commit is
  already one consensus round, so there is nothing to save. It becomes relevant only if [Multi-Raft](multi-raft.md)
  is built and transactions cross groups, which Yotsuba's do not.
- **Deterministic ordering** (Calvin) removes locks by ordering whole transactions before execution, which needs
  their read and write sets up front. Rockserver updates are interactive: the client reads, decides, then writes,
  so the sets are not known until the client has run. The part of Calvin that does fit is already here: a merge
  operator is a deterministic read-modify-write ordered by the log, with no lock and no round trip back to the
  client. A hot counter is best written as a merge, not as an update.

So the per-key lock stays, with pipelined updates as the measured option to shorten the cycle.

## Snapshots, catch-up and membership

**Snapshot.**

- **Building it.** `takeSnapshot` builds a checkpoint from RocksDB's live-file APIs, with file deletions
  disabled:
  - memtables are flushed;
  - SST and blob files are hard-linked (a node with `raft/` on another file system, where they would be copied while
    the apply waits, refuses to start; see Disk layout);
  - the MANIFEST prefix is copied and `CURRENT` written;
  - the WALs still needed are copied.
- **Why not `org.rocksdb.Checkpoint`.** It refuses the column volumes (`cf_paths`) that the default config
  uses.
- **When.** Every `snapshotEveryEntries` applied entries (default 500k, Ratis' own trigger), and also once
  `snapshotEveryBytes` of log entries (default 4 GiB, `--snapshot-every-bytes`) were applied since the latest
  snapshot: an entry holds up to 4096 requests or 8 MiB, so a count alone does not bound the log a snapshot lets
  Ratis purge. At a start the count begins from the bytes of the Raft log segments that the database applied past
  the latest snapshot (a segment only partly in that range counts in proportion to its entries), not from 0:
  otherwise restarts more frequent than `snapshotEveryBytes` of writes, a few deploys a day, would leave only the
  entry-count trigger. Ratis replays the entries up to the database's applied position after the start; they count
  0, since the seed holds them (counting them again doubled the count and took a snapshot at half the threshold,
  `SnapshotByBytesTest.theCountSurvivesARestart`). The byte trigger asks through Ratis' snapshot management API, like `admin
  snapshot`, from the thread that completes applied groups, so Ratis still takes the snapshot on its own thread.
  Not at stops (a graceful stop flushes the database instead). Taking one flushes the memtables and copies the live files, 100–300 ms, during which
  Ratis hands over no entry: about every 100 s at 2×, writes wait that long. (Taking it on a thread of its own,
  answering Ratis with the previous snapshot meanwhile, was tried and dropped: a result finished before a
  snapshot install could then be returned after it, behind Ratis' snapshot index, and a copy finishing during
  an install could land among the installed snapshot's files.) Files that are hard links of durable files are
  not fsynced again.
- **Log retention.** The Raft log is purged only up to the snapshot index and the lowest commit index of any
  member, and keeps `snapshotEveryEntries` entries more (Ratis counts that margin in entries only: with large
  entries, lower `--snapshot-every` to bound it in bytes). A node restarted during a deploy, or one that just
  installed a snapshot, therefore catches up from the log. This holds on the followers too: Ratis takes the lowest
  commit index from every member's commit index, which the leader sends to each follower, so a follower that later
  leads still has the log of a member that lags. An existing member therefore never needs an install (only a new
  member does, or one whose log was lost). A member that is gone for good holds the log back,
  so remove it from the configuration promptly, and alert on Raft log size.

**Catch-up and install.**

- **Install.** A follower too far behind gets the snapshot files through stock Ratis snapshot installation.
  Ratis pauses the state machine. `reinitialize` then restores the checkpoint into the database directory:
  immutable files are hard-linked, the swap is crash-safe, and the database is reopened. If the local
  database is already ahead of the installed snapshot, it is kept.
  A restored database with many level-0 files starts a compaction as soon as it opens, which unlinks the
  compaction's inputs while the open checks the disk layout. Until round 12 that check (`Files.walk`) failed on such a
  file (`NoSuchFileException` on an `.sst` of the follower's own `db/volume`) and the install stopped the replica for
  good; it now skips entries deleted under it (R12-0, seen as a flake of `CommittedConfigurationsTest`; the bug was
  there since the check first ran on an open database).
- **Leader-side retention.** The leader keeps the previous snapshot, and any snapshot whose file list was read
  in the last 10 minutes, so a long install is not cut short by newer snapshots.
- **Disk cost of snapshots.** A retained snapshot keeps the table files it links, also after compaction
  replaced them in the database. Budget disk space for the database plus at least one older snapshot's
  compacted-away files, more while installs run. On a follower, a failed install leaves its partial copy until
  the server restarts, a newer install starts, or its files did not change for 10 minutes (the leader's own read
  lease of a snapshot being sent): a cleanup thread of its own then deletes it. It is not one of the threads that
  pause the API on a hung disk and step down a leader whose log stopped (see "Threads"): the API gate thread does no
  disk I/O and takes no lock that a thread waiting for a disk can hold, so a hung disk cannot hold it back. Nor is it a Ratis thread (R24-1): Ratis reads the
  latest snapshot holding its server's monitor on every append that carries entries (a follower checks the append
  against its snapshot index: `appendEntriesAsync`, `checkInconsistentAppendEntries`, `getSnapshotIndex`) and on the
  leader's appender threads, so `CheckpointStorage.getLatestSnapshot()` only reads a field. Before R24 it ran the
  scan itself, at most once a second (a listing of `tmp/`, and every 10 s a walk of each current installation), so
  the appends did most scans under that monitor; with a hung disk, the cleanup thread held the install lock in its
  walk, the next append waited for that lock holding the monitor, and the follower stopped answering every Raft
  RPC, votes included. Only the cleanup thread (`discardFailedInstalls`, every 200 ms, at most one scan a second)
  looks now. An install that is only that slow fails at its next chunk and Ratis starts it over. Because nothing
  else covers for it any more, a run that fails, an `Error` included (out of memory in the walk of a large
  install, or when the thread that deletes a directory cannot start), is logged and the next run retries: a
  scheduled task that throws is cancelled for good, silently, and installs would then leak until a restart.
  What is tested is that `getLatestSnapshot` neither scans nor waits for the install lock
  (`CheckpointStorageTest`): Ratis' monitor then has no path to the cleanup's disk I/O. No cluster test hangs the
  disk under the cleanup walk while checking heartbeats and votes: `HungDiskTest`'s shim hangs syncs, not
  listings or walks, and a hook to block the walk would test that Ratis does not call code that it no longer
  reaches.
- **Damaged installs.** A snapshot that a crash damaged during install makes the replica fail loudly; it is not
  silently ignored.

**Membership.**

- **Initial cluster.** All nodes start with the same peer list and without `--join`. The first leader proposes
  `Genesis`, with a random cluster id, which binds every replica's database to the cluster and records whether the
  cluster was seeded from an existing database. A node started with `--join` has no election timer, so a cluster of
  such nodes never elects a leader, though each prints `READY`; a node that stays outside says so in its log after 15
  maximum election timeouts, and every as many after that.
- **One cluster id per genesis.** The group id is shared by name: by default it is the same in every cluster. Raft
  compares log entries by term and index only, so a node that elected itself writes entries that look like the
  entries of a real cluster, and they are never replaced: 41 of 700 acknowledged keys read null after such a node
  was added, with no error. Every genesis therefore gets its own id, and two things keep the histories apart:
  `add-peer` asks the new node what it holds and refuses it (see below), and a leader tells each of its followers
  once per term which cluster it leads, also a follower still catching up to join (`ClusterCheck`). A follower of
  another cluster exits with 3, so a node that the Ratis tools add, or that gets a log right after `add-peer` asked,
  stops before the configuration changes, and so does a member named in the peer list of the others that started a
  cluster of its own. A second `Genesis` in a log, from a resent batch of a former leader,
  changes nothing: the first one wins on every replica. Other ways for two histories to meet are closed: a database
  that holds a cluster id with no Raft state is refused at start, whichever cluster it comes from, so a copy of a
  replica cannot seed a new cluster or rejoin; and a new member takes a seeded cluster's data from a snapshot, which
  replaces its database.
- **One seed.** At its first start, before anything opens the database, a node records a digest of the files in
  `db/` next to it (`db.seed-digest`): by path, table and blob files by size, every other file (MANIFEST, CURRENT,
  OPTIONS, WAL files) by content, without `LOCK` and the info logs (`LOG*` in `db/`, and `*_LOG`, `*_LOG.old.*` in its
  `logs/` directory, whose names hold the database's absolute path; before R26-1 those were digested). That is the only moment when copies of one seed are
  alike: opening a database writes a new MANIFEST and starts compactions at times that differ on each node. The leader
  of a seeded cluster puts its digest in `Genesis`, and a replica that recorded another one stops at genesis (exit 3)
  instead of keeping another database for good. A missing file, an extra one, a truncated copy or a copy taken at
  another moment differ; a table file damaged without a change of size does not. Copies made with `cp -a` or `rsync`
  of the stopped database agree. A replica that recorded nothing (its first start was of an older version) logs that
  it cannot check and goes on. A first start can fail after the digest is recorded but before Raft made its group
  directory (a partial copy that RocksDB refuses, a refused config, the start-up check found a cluster id): the
  operator may then copy the seed again. So the start-up check, after it opened and closed the database, also records
  the digest of the files as it left them, and a later first start keeps the recorded digest only while `db/` holds the
  files of either digest; otherwise it takes the digest again and logs it. `restore-backup` writes both lines
  itself, the backup's digest and the restored files' (see `Backup` in "Checksums and backups"). Older `Genesis` entries, and those of clusters that are not seeded, carry no digest, and
  keep their bytes (the digest comes with a new entry tag that older versions cannot read, which matters only for a
  cluster created while nodes of an older version run).
- **Migrating from a single node.** Stop it and copy its database directory to every node. Every replica takes a
  snapshot as soon as it has applied the seeded `Genesis`, and on each start until it has one, because a member added
  later needs the seed from whichever node leads by then: a leader sends a new member its latest snapshot before any
  log entry, and without a snapshot it would send a log that does not hold the seed. `add-peer` waits until every
  member that answers has its snapshot. A node that replays the seeded `Genesis` with an empty database stops with
  exit code 3 instead of diverging: an initial member without the copy, before the log is purged, or a new member
  whose leader still has no snapshot. For the last one to happen, that leader must be a member that `add-peer` could
  not reach, in the moments after it starts. Such a node is a member by then: take it out with `remove-peer`, then wipe
  it and add it again. Do not wipe it first, because a member with an empty log gets no vote from Ratis
  (`LeaderElection.waitForResults`), and with a node lost as well the cluster then has no leader; if the cluster has
  already lost its majority, restart the node as it is until `remove-peer` gets through. `admin snapshot ID` makes a
  member take a snapshot at any time.
- **Replacing a node.** Start the new member first, under a new id, with an empty data directory, a peer list that
  includes itself and `--join`. It waits, also across restarts, for as long as you like. Then change one voter per
  command (round 14): `admin` refuses a call that adds and removes a voter together, or changes two, and so does the
  leader.

  - **A lost node:** `remove-peer` of the lost node with every current member in `--peer`, then `add-peer` of the new
    one with the members that stay:

    ```bash
    ReplicatedServerMain admin --peer <every current member> remove-peer <lost node>
    ReplicatedServerMain admin --peer <the members that stay> add-peer <new node>
    ```

    This needs the same live members as the joint change that the one-step replacement made before round 14: with one
    node of three lost, the two others are 2 of 3 during the removal (both of them, as the joint change also needed)
    and 2 of 2, then 2 of 2 and 2 of 3 during the addition, so they are a majority of both halves all along and a
    failure of the new node stops nothing. It works the same when the lost node was the leader or a follower, seeded
    or not (`MembershipTest.aLostNodeIsReplacedRemoveThenAdd`).
  - **A running node (a move), the leader too:** `add-peer` with every member in `--peer`, then `remove-peer`; it keeps
    serving until it is out, and exits with 3 (or 4) then (`MembershipTest.aRunningLeaderIsReplacedInTwoSteps`).

  Why one voter at a time: with Ratis 3.3.1 a leader elected while a joint configuration is uncommitted writes only the
  new half in its term-start entry and commits with it alone (see the residuals of "Removal and waiting outside"). With
  one voter added or removed every majority of the new half meets every majority of the old half; with two voters
  changed (old o for new n) a majority of the old half and one of the new half can be disjoint while o still votes.
  The one-step replacement was safe only while o could not vote, which `admin` cannot tell from a partition, so it is
  refused rather than left to the operator. `admin` reads the leader's configuration, refuses a change of more than
  one voter before it proposes anything (exit 1, with the two orders above in the message), and sends the change as a
  compare-and-set against what it read; the leader's `MembershipGuard` checks every `setConfiguration` that reaches it
  through gRPC the same way (also an older `admin`, or the Ratis tools, once every node that can lead runs round 14:
  during a rolling upgrade a leader of an older version has no guard, so the runbook says to change one voter per
  call with every `admin` until the upgrade ends), and rewrites what it lets through into a
  compare-and-set against the configuration it checked, so that a change committed between its check and Ratis'
  handling makes Ratis refuse instead. `--unsafe-multi-voter-change` skips both (the leader logs a WARN); it is never
  needed for recovery, since any change that can commit needs a live majority of the old voters, and with one the
  same result is reached one voter at a time; with none nothing commits, flag or not ("No quorum"). It is safe only if
  every majority of the old voters that can still vote meets every majority of the new ones, which the leaving nodes
  looking dead does not ensure ({1,2,3,4,5} to {3,4,5,6} with 1 and 2 partitioned, not dead: {1,2,3} misses
  {4,5,6}). Ratis itself (3.3.1, `memberMajorityAddEnabled` off) refuses, flag or not, a change whose new voters are at
  least as many as the kept ones (`changes a majority set`; `RaftConfigurationImpl.changeMajority`), so most
  multi-voter replacements never reach the term-start problem; `admin` fails at once on such a refusal: after a
  `SetConfigurationException` without the guard's marker it reads the configuration again, and goes on only if it
  changed (a lost compare-and-set race) or another leader, or the same one in a later term, answers, else it fails
  with Ratis' message and says to run the command again if the leader changes.

  Which leader `admin` reads (R16-1): it asks every `--peer` in each round, all at once (a down or unroutable member
  costs one request timeout per round, not one each, as for the dead node of a `remove-peer`), and takes the answer of
  the LEADER with the
  highest term (`RoleInfoProto.leaderInfo.term`), and only if no member that answered shows a higher one as the term
  of its last entry (a follower's answer carries no term); else it waits, and a joint configuration of that leader is
  waited for, never replaced by another leader's answer. Before, it took the first `--peer` that answered LEADER: a
  deposed leader answers LEADER with the configuration of its term until Ratis' `checkLeadership` steps it down
  (about an election timeout), so a valid change could fail at once ("did not change since", without advice), and
  `add-peer` could print stale voters, those that its advice to wipe a node refers to. The guard kept that safe; it
  failed a valid command, and misled. What is left: the new leader and every member that holds an entry of its term
  do not answer while the deposed one does (its own term-start entry is the first of the new term, so a member that
  answers and holds it shows the term). The compare-and-set then fails on the real leader, `admin` reads the same stale
  answer and fails at once with the advice to run it again; the printed voters can be stale only in that window, which
  the runbook's "wipe only if it fails the same way again" covers. `LeaderConfigurationTest` (`settle`, the fast fail
  of `changeVoters` after a refusal, `standingRefusal`, and the members asked at once, `askAll`); not checked against
  a real deposed leader in a cluster.

  A re-run of `add-peer` for a node that is a voter already (after a timeout, or of a call that succeeded) answers
  `already a voter` and `OK` from the leader's settled configuration, before it asks the node anything: until the node
  has applied the genesis or its snapshot, its `nodeState` looks like a log of its own, and the old order (fitness
  check first) told the operator to wipe a voter, which can lose committed entries (voters {n2,n3,n4}, an entry on n3
  and n4 only, n4 wiped, n2 elected). A voter at another address fails with "never wipe a voter". When the fitness
  check does refuse, `add-peer` reads the configuration again before failing, and the message says to wipe nothing
  while an earlier `add-peer` of the node may still run (the leader may be catching it up, in Ratis' staging, which
  the settled configuration does not show yet).

  Adding first while a lost node is still in the configuration is the risky order: it passes through 4 members with 1
  lost, where all 3 others are needed: any failure of the new node stops writes until it returns, and an appender that
  dies during the catch-up leaves the cluster without a leader, with no way back (see "Heap"). Adding first is for
  growing a cluster and for a node that runs. Every member must be in `--peer`, or the change removes the ones left
  out, which the one-voter check refuses unless it is just one.

  New members catch up from a snapshot. A node that lost its Raft state but kept its id refuses to start, because it
  could vote twice in one term. Replace it under a new id.

  `add-peer` asks the new node first, and exits with 1 and says why if it does not answer, if it runs another group
  id, if it was started without `--join`, or if it has a log of its own (see "One cluster id per genesis"). It then
  waits until a majority of the members answer, and every one that answers applied the genesis (with a snapshot on a
  seeded cluster): before, a check that no member answered passed. It accepts
  a node of this cluster, so it can run again. `add-peer` and `remove-peer` retry for `--timeout-ms`, 10 minutes by
  default, sleeping between tries, and then exit with 1. The limit must cover the catch-up of the new node, at about
  100 MB/s. A leader that started a change goes on with it after the command ended: see `admin status`, and run the
  command again, which ends with `OK` once the change is done. The catch-up ends by itself if the new node stops
  answering for 3 maximum election timeouts.

  `--join` gives Ratis an empty group, so the node starts in Ratis' initializing state: no election timer, until a
  leader sends it a request that is not part of bringing it up to date. Without the option the node would start
  elections. A leader that is not of the first term answers a pre-vote from a node outside its configuration with a
  request to shut down (its configuration entry has an index, which it compares with the candidate's log), Ratis
  closes that node's division, and `add-peer` then hangs. So a node that replaces a lost leader had 1 to 2 s to get its
  `add-peer`. Now such a node exits (see "Long pauses"), and `--join` keeps it from getting there. The option does not
  change a node that has a configuration of its own, from its log or from a snapshot, so it can stay in the node's
  command line; a node of a running cluster also restarts without it.

  `--join` must be there from the first start until `add-peer`. A start without it writes `raft-meta.bootstrap` in the Raft directory,
  and Ratis uses that peer list whatever a later start says, so adding the option afterwards would change nothing: the
  node would keep exiting. A start with `--join` is refused when that file exists and no log gave the node a
  configuration: wipe the data directory and start again with the option. Do that before an `add-peer` reaches the
  node. `add-peer` itself refuses such a node, but an `add-peer` of the Ratis tools that lands while it runs, in the
  seconds between its exits, makes it a member, and a wipe then loses that member: with another node lost, that
  leaves no majority, and neither `add-peer` nor `remove-peer` can end. Such a node must get its data back as a member
  would: keep its data directory, and restart it until a `remove-peer` gets through.

  The peer list of a node is read at the start. Before its first connection to a member's API, a node asks the member
  for its API address at its Raft address (`TermService`), and uses the answer, logging an error if it differs from the
  list; a member that does not answer within 500 ms is likely down, and the listed address is used until it answers. A
  member that is not in the list is asked the same way. So a node given wrong API ports for its peers still forwards
  to the right ones: fix its list anyway, since the address it gives clients as a leader hint comes from the same
  place until the member answered. The answer is used without asking again until a connection to the member fails;
  then the member is asked again at once, and again before the next connection after the 200 ms backoff, and leader
  hints follow the new answer. So a member restarted with another API port while the others keep running is found at
  the new port at once; before R18-2 the first answer was kept until this node restarted
  (`LeaderForwarderTest.aListedMemberThatMovesItsApiPortIsAskedAgainOnceAConnectionFails` fails without the fix).
  The old answer stays until a new one replaces it, for connections and hints alike: after a one-off failure at the
  same port (a pause, a restart on the same port) it is still right, while the listed port may be the wrong one that
  made the answer necessary. The first R18-2 fix removed the answer on any failure, so hints named the listed port
  until this node next forwarded to the member, which clients that follow hints may make rare
  (`aListedMemberThatFailsOnceAtTheSamePortKeepsItsAnsweredAddress` fails with that fix). A member that is down and
  does not answer keeps its last answer. One failure asks once: every request waiting on the failed connection used to
  send its own ask, each resetting the backoff of the channel to the member's Raft port, and the connections made
  together after the backoff each asked again. Now only the request that marks the answer stale asks, and asks to a
  member share the one in flight (R19-2;
  `requestsSharingAFailedConnectionAskTheMemberForItsApiAddressOnce`: 17 asks instead of 2 without the fix, and 4
  instead of 1 without the sharing; the test accepts up to one ask per failed connection, 5 in all, since an answer
  that returns before a later connection fails lets that failure mark the member stale and ask again, which is
  correct). The leader hint of every response is cached by
  `LeaderForwarder.addressVersion()`, which changes only when an answer or a learned address changes, so the hot path
  reads one counter instead of looking the address up (`LeaderHintCache`, keyed by leader and version, the version
  read before the lookup; `LeaderHintCacheTest`).

  A node still running when `remove-peer` removes it closes Raft by itself and exits with 3, or, if its Raft server
  stays up outside the cluster, is stopped by the check below with exit 4. With systemd,
  `RestartPreventExitStatus=2 4 5` keeps a node that exited with 4 down (2: wrong usage or no
  `-XX:+ExitOnOutOfMemoryError`; 5: a database of a newer cluster version; see "Exit codes" in runbooks.md for the exit
  and stop settings of the unit, with `SuccessExitStatus=143` for a stop by SIGTERM and `TimeoutStopSec=150` for a
  stop longer than systemd's default of 90 s).

  **Removal and waiting outside (R11).** This replaces the rules of rounds 4 to 10 (R4-6, R3-G2b-4, R10-1), which
  judged the latest configuration of the node's own log, and two of which were wrong: a stable configuration without
  the node was taken as a committed removal, but Ratis' term-start entry copies only the new half of a joint
  configuration (`LeaderStateImpl` builds it from `getRaftConf().getConf()`), so a leader re-elected with an
  uncommitted joint configuration that removes X writes a stable one without X, which the next leader may drop, and
  X, a member, was told to wipe itself (R11-1); and configuration indexes were compared across two logs, so a removed
  node whose own log had a longer, uncommitted tail was judged a member and ran as a follower that no leader sends
  to, forever (R11-2).

  The rule (`RemovalRule.decide`, pure). A node concludes that it was removed, exits with 4 and is told to wipe its
  data directory, only when all of these hold: a committed configuration K* leaves it out of its new half; K* is the
  newest committed configuration that anyone reports, its own included; a majority of K*'s new half each report K*
  committed in their own log, and none of them holds a later configuration that includes the node and may still
  commit; the node's own log holds no later configuration that includes it and may still commit; and the node was a
  voter once. Only committed configurations are compared by index: committed entries form one log in every node (a
  follower's commit index covers only entries that match the leader's log, and a leader commits only entries of its
  own term). An uncommitted configuration only ever blocks a decision. It is known never to commit when it cannot be in
  a log that holds K*: its term is lower than K*'s (terms never decrease along a log), or the log that holds it has
  another term at K*'s index, or a probe of K*'s (term, index) answers "absent" there; a higher term alone is not enough
  (Raft's Figure 8). "A voter once" is `--join` not given (the node's own peer list holds it), or `raft/member`: the
  node writes it when Ratis runs it (it left its initializing state, which only an append that is not part of bringing
  it up to date does) and its latest configuration holds it in the new half. A node that is brought up to date for
  `add-peer` replays the configurations of an earlier membership of the same id while it is initializing, and they do
  not count (otherwise a node added again after its removal would be told it was removed in the middle of its own add).
  Files written by earlier versions (from a stable configuration that held the node) are kept.

  Each check asks, in rounds, the members of the halves of the node's own latest and committed configurations and of
  the current K*, plus the nodes of its peer list, for `TermService` `Configuration2`: whether the node runs, its term
  and commit index, its committed configuration and its latest one (each with its term and both halves), the
  addresses of its members, and the answers to two probes, the asker's own latest configuration entry and K*
  (committed, present, absent, or unknown). The verdict is MEMBER, NOT_ADDED (never a voter), REMOVED, or undecided
  with a reason: INCONSISTENT (two answers disagree about a committed configuration on a field both know),
  NO_COMMITTED_CONFIGURATION, OWN_LATER_CONFIGURATION, NEEDS_TERM (K* known only from members that predate the method,
  which tell no terms), MEMBER_LATER_CONFIGURATION, NO_MAJORITY, and HEARD_LEADER (below). A check returns as soon as
  the verdict is decided, or when asking again cannot change it, or after two election timeouts (at least 1 s). A
  member that answers `UNIMPLEMENTED` is asked the version-1 `Configuration` (its committed configuration, with no term
  and no old half), and one that predates both, or cannot be reached, is no answer. A version-1 committed flag
  compares Ratis' configuration with the commit index, which is wrong for the stale configuration of finding 1 below
  (R12-1). So a version-1 answer raises K* or blocks, but confirms only a K* that the node itself or a member of this
  version reports committed: that member's log then holds K* committed at that index, and the old member's committed
  entry there is the same entry. Otherwise the verdict is NEEDS_TERM. A K* that only version-1 answers report has no
  known term, so neither step 4's branch tests nor step 5's exclusions can drop a blocker on its account. The cost is
  liveness in a rolling upgrade: a removed node often never applied its own removal, so with every remaining member on
  a version without `Configuration2` it waits outside (NEEDS_TERM) until one of them is upgraded, where the round-11
  rule exited with 4. Upgrade the members before removing nodes.

  The committed configurations come from the node's own records, not from Ratis' notification: on a snapshot install
  Ratis reports the leader's latest configuration, committed or not, with term 0, on every chunk
  (`SnapshotInstallationHandler`), and it sends nothing at all when the follower already holds that configuration
  uncommitted. So `RockserverStateMachine` records a configuration only when the apply path passes it on together with
  its position (`notifyConfigurationChanged` then `notifyTermIndexUpdated` of the same term and index on the same
  thread), and the apply thread writes it into the meta column (`NodeStore.CONFIGURATION`, key index and term) in the
  same atomic write as the applied position: a crash keeps both or neither, Ratis applies the entry again after a crash
  that lost it, and a snapshot carries the history, so a follower that installs one gets the leader's committed
  configurations up to the snapshot (`CommittedConfigurations`). The history is complete from the first index this
  version applied on that database (`CONFIGURATION_HISTORY`), and kept: one entry per term start and per membership
  change, about a hundred bytes each. A node's committed configuration is the newest it recorded, or its latest
  configuration if that is at or below its commit index and in its log or snapshot (a configuration a snapshot install
  took from the leader, past the snapshot and not in the log, does not count; one in the snapshot counts only if the
  history recorded it at its index, or does not cover that index). A probe of an entry in the log answers
  from the log and the commit index (or the history, for a restarted node whose commit index has not caught up), and
  of an entry in the snapshot from the history: committed if it recorded that configuration, absent if it recorded
  another one there, unknown otherwise (also during a snapshot install, while Ratis' snapshot index is ahead of the
  history, R12-2: Ratis moves it before `reinitialize` swaps in the snapshot's history, and "absent" there would drop a
  blocker; for a K* that really is committed, a complete history with no record cannot happen otherwise). Seeding from `raft-meta.conf` was not needed: the
  derived committed configuration covers a database of an older version.

  Ratis' configuration of a node is not always the entry at its index (round-11 review, finding 1). A snapshot install
  writes the leader's latest configuration to `raft-meta.conf`, committed or not; at a start `ServerState.initialize`
  loads that file first and then replays the log through `ConfigurationManager.addConfiguration`, which makes an entry
  current only if its index is the highest seen, and nothing at start removes configurations. So a follower F that
  installed a snapshot from a leader holding the uncommitted joint configuration J (index c) of X's removal, then took
  the entries of a later leader E that dropped J (E's term-start configuration C_old at e < c, ordinary entries past
  c), and restarted before applying e (only that apply rewrites the file), runs with J again, until a configuration
  above c reaches it (checked in the 3.2.2 sources; the 3.3.1 bytecode of `ServerState` and `ConfigurationManager` is the
  same). The commit index then covers c and the log has E's term there, so the round-11 code took J as committed with
  E's term, and a probe of (E's term, c) answered "committed" on every member: X, a member of the committed C_old, was
  refused with exit 4 at a cold restart or while it heard no leader. Now `MembershipFacts.entry` reads the entry at the
  configuration's index: the latest configuration takes its term from the log only if that entry is the same
  configuration (both halves), from the history only if it recorded the same configuration at that index, and is
  unknown otherwise; it is the committed configuration only if that entry is the same configuration, or the index is in
  the snapshot; and a probe whose (term, index) is an ordinary entry answers "absent" (that entry is the only one with
  that term and index in any log, so no configuration has them). Once the node purges its log past c (purging follows
  the members' lowest commit index, so it does, while no configuration above c is appended), the entry is gone and J
  sits in the snapshot: there it counts as committed only if the history recorded J at c, or does not cover c (a
  database of an older version, which keeps the round-11 behaviour), round-13 review. A stale J is then only a latest
  configuration with an unknown term, which can neither be K* nor confirm one, for a history complete over c. `MembershipFactsTest` builds F's state (a stub log and
  configuration) and the members' answers, and fails without the change (J committed with term 3, X REMOVED by four
  confirmers). An in-process reproduction needs F's Ratis updater stalled between appending E's entries and applying e,
  which stock Ratis has no hook for; it is not written. At the Ratis level the same stale state can do more: if F is
  elected with J, its term-start entry copies J's new half (R11-1) and the removal of X becomes real. That removal is
  one an operator asked for with `remove-peer` (whose reply then failed or timed out), the rule then reports it
  correctly, and Ratis is left as it is.

  The history must not say that it is complete where it misses a committed configuration (round-11 review, finding 2),
  or a probe of a purged entry answers "absent" for it and stops a member from blocking (since R12-2 a missing record
  answers "unknown", so completeness no longer feeds any probe answer; it still decides whether a configuration of
  Ratis in the snapshot counts as committed). Two ways led there, both
  closed. Ratis calls `notifyConfigurationChanged` from the install path on a gRPC thread with term 0; that call cleared
  the pending configuration of the updater thread, so one landing between the updater's notification of a
  configuration entry and its `notifyTermIndexUpdated` lost the entry while the applied position moved past it. A call
  with term 0 now leaves the pending configuration alone. And a version without the history (a rolling upgrade or a
  downgrade) applies configurations without recording them under a `CONFIGURATION_HISTORY` it does not know, and its
  snapshot carries that key on: `CONFIGURATION_HISTORY` now holds the first complete index and the applied index it was
  written with, rewritten with every applied position in the same write as `APPLIED`, and an open that finds another
  applied index (or the 8-byte value of the first round-11 commits) takes the history as complete only from the index
  after the applied position, as for a database without the key. `ConfigurationHistoryGapTest` fails without either
  change.

  Where it runs. At the start of a node with an existing data directory, after its Raft server started (it needs its
  own log, and its own `TermService` must answer the others in a cold start): REMOVED throws `RemovedException`, exit
  4; any other verdict lets it start, and an undecided or not-added verdict logs a warning that names the reason and
  what the operator does. Then, while the node is detached, every three election timeouts (at least 1 s): detached
  means that no leader sent it any Raft request for three election timeouts (appends, heartbeats and snapshot chunks,
  including those that bring it up to date for `add-peer`, seen by `RaftTraffic`, an interceptor on the Raft gRPC
  server: Ratis shows that time only for a running follower) and it does not lead. Ratis' initializing state is not
  detachment, so a node being caught up is never checked during its own add; and a node that runs as a follower with
  a configuration its log never saw removed (R11-2) is checked too, which the round-10 gate on the initializing state
  missed. `RaftTraffic` counts a request before Ratis checks its term, so an append or snapshot chunk from a deposed
  leader counts too (R12-7, accepted): Ratis rejects it (`NOT_LEADER`) only when its term is below the receiver's, or
  equal with another leader known (`ServerState.recognizeLeader`, and `SnapshotInstallationHandler` for chunks), and
  that leader steps down on the reply (`GrpcLogAppender`, `onFollowerTerm`); when its term is not lower, Ratis does
  accept it as leader, so a check based on the reply would change nothing there, and it stops within about an election
  timeout once a majority of its configuration stops answering (`LeaderStateImpl.checkLeadership`). It cannot be
  elected again: the members that committed the removal have a later last term, and the removed nodes whose logs
  missed the joint configuration are fewer than a majority of its old half, through which it committed. The cost is
  one more quiet period (at most about three election timeouts) and at most one REMOVED turned into HEARD_LEADER; the
  next check decides. The warning is chosen by the verdict (never by `--join`, R11-5) and repeated every 15 election
  timeouts. A
  removal found while a leader sent the node anything during the check is not trusted (HEARD_LEADER): the answers may
  be older than that leader's configuration. `ReplicatedNode#membership` keeps the last verdict, and the
  `membership.state` gauge says 0 (member, or not checked), 1 (never added; also while `add-peer` catches the node
  up, since it is not detached then and keeps its last verdict until its configuration holds it), 2 (outside,
  undecided) or 3 (removed: reported only if a push of the gauges happens between the verdict and exit 4, which the
  shutdown does not wait for, and never for a removal found at the start, which exits before the gauges report; no
  alert relies on it). The alerts `rockserver-node-outside` (2 or more for
  10 minutes) and `rockserver-node-not-added` (1 for 6 hours: a catch-up of a large snapshot can take an hour or more)
  watch it. While undecided
  the node runs in whatever Ratis state its own log gives: a log whose latest configuration leaves it out of a half
  keeps it initializing (no election timer, no vote: `RaftServerImpl.requestVote` needs a running server); one that
  holds it in both halves (a log that missed its removal) makes it a follower, whose votes cannot elect a leader that
  lacks K* (Leader Completeness) and whose own campaigns cannot win (the members that committed K* have a later last
  term). It cannot serve stale data: a follower read needs a read index from a leader, and its applied index grows only
  from entries a leader sends it. `RemovedException` carries the verdict, and its message names K*, the members that
  confirmed it (a majority of its new half) and what the node's own log holds at K*'s index (R11-6).

  What the operator does about each warning: NO_MAJORITY or NO_COMMITTED_CONFIGURATION, start or repair the named
  members of K*'s new half (members of a version without the method count as unreachable); the node decides by itself
  once a majority of them answer; if they are gone for good, check with `status` on a member, and if this node is not
  in its configuration, stop it, wipe it, start it with `--join` and run `add-peer`. NEEDS_TERM, upgrade the named
  members (their version tells no terms, and their committed flag does not confirm a removal by itself), or start more
  members of this version; until then judge with `status`. It is given only when the members of this version that
  answered cannot make a majority of K*'s new half without the older ones: members of this version that answered
  without confirming yet (lagging) make it NO_MAJORITY, which the check asks again within its budget (round-13
  review, finding 3). MEMBER_LATER_CONFIGURATION, normally clears within seconds; if it persists,
  the named member is stuck or was removed itself. OWN_LATER_CONFIGURATION, its own log holds a later configuration
  that includes it and may still commit: normally an add of this node in flight (let `add-peer` finish), or a
  configuration a leader appended just before it lost leadership, which clears once the next leader's configuration
  reaches this node; if it persists while a leader runs, check `status` on a member. NOT_ADDED, run `add-peer`; if
  the add was abandoned, or the node was added and removed long ago, stop it and wipe it (`add-peer` refuses a node
  that has a log). INCONSISTENT, a bug or a data directory restored from elsewhere:
  wipe nothing, collect the logs.

  Why it is safe (S1: never exit 4 while the node is, or may still become, a member). REMOVED needs a K* that is
  committed (it is the applied configuration of a confirmer, or covered by a commit index) and leaves the node out of
  its new half. Let F be the first committed configuration after K* that holds the node in its new half: term-start
  entries copy only the new half, so F descends from a `setConfiguration` that added the node, and Ratis brings new
  peers up to date before it appends the joint configuration of an add, so the node's current data directory heard a
  leader then. If it applied F or later, its own committed configuration would be at least F, so K* would hold it. If it
  holds F unapplied, its log holds K* (a leader that holds K* brought it up to date), and step 4 finds a later
  configuration of its own that holds it. If it does not hold F, the data directory was made for this add (`add-peer`
  refuses a node that has a log), so it was never a voter: NOT_ADDED. A configuration that may still commit in a
  member's log is excluded by step 5, in the node's own log by step 4. A configuration D that Ratis took from a
  snapshot install, past the end of the node's own log and with no known term, or a stale one over another entry of
  its log, blocks too, unless D is older than K* (R12-3, narrowed in round 13, review finding 1). A leader sends its
  configuration with each snapshot chunk without a term (`LeaderProtoUtils` passes none, checked in the 3.3.1
  bytecode), so `RaftTraffic` records, before Ratis sees a chunk, the chunk's configuration (index and halves) with the
  leader's term in `raft/installed-configuration` (`InstalledConfiguration`): a bound per configuration, the highest
  term of a chunk that carried it. A leader, deposed or not, holds no entry of a later term than its own, so that term
  bounds the term of whichever entry D came from. Every chunk is recorded, whatever its term: Ratis 3.3.1 takes the
  configuration of a chunk it rejects too (`SnapshotInstallationHandler.installSnapshotImpl` applies it for every
  reply that is not an exception: NOT_LEADER for a leader's term below its own, SNAPSHOT_EXPIRED, ALREADY_INSTALLED,
  and the install itself), dropping its configurations from the chunk's index on and taking the chunk's
  (`RatisStaleInstallConfigurationTest` pins it). Until round 15 the file held one record, which a chunk of another
  configuration replaced whatever its term: after the leader of term 6 installed D, a chunk of a leader deposed in term
  4 with D', then one of a leader deposed in term 2 whose log holds an uncommitted entry with D's index and halves,
  left Ratis with D and a bound of 2, so step 4 could skip a D that may still commit (R14-1; the model example
  `aStaleChunkOfAnotherConfigurationDoesNotLowerTheBoundOfTheOneRatisHolds`). Now a chunk raises only its own
  configuration's bound; at most 32 configurations are kept, the lowest index going first, and its bound raises a
  floor that a configuration recorded later starts from. Updates are serialized per Raft directory (install streams of
  several leaders may run at once), and the file, synced with its directory, is replaced before the chunk is
  forwarded. Invariant: for every chunk forwarded to Ratis since the file was written at this node's start, the file
  holds the chunk's configuration with a bound not below the chunk's term, or does not hold it and the floor is not
  below it. At each start, before the Raft server runs, a missing or unreadable file, or one of the round-13 single
  record (whose bound R14-1 may have left too low; a version of round 13 cannot read the new format either, so it
  bounds nothing after a downgrade), is replaced by an empty one whose floor is the term in Ratis' `raft-meta`: Ratis
  persists a term before it acts on a chunk of that term, and a chunk it rejects has a lower one, so no chunk handled
  before exceeds it. A file of the new format is kept, with its floor and every bound above that term lowered to it,
  for the same reason. If `raft-meta` cannot be read, the floor of a new file is the highest term and nothing kept is
  lowered: until a start that reads `raft-meta` again, which lowers that floor, nothing a chunk adds is skipped
  (review of round 15, finding 3: before, a floor of the highest term stayed for good). While the node runs, a file
  that is missing or cannot be read when a chunk comes (deleted or damaged since the start) is written again with
  Ratis' current term as its floor (the highest term if the division is not known yet), which no chunk Ratis handled
  exceeds; before, a missing file was read as an empty record of floor 0, so a later stale chunk of a configuration
  Ratis took before got its own lower term (findings 2 and 4). If the record cannot be written even so (a read-only
  Raft directory, a non-empty directory in the file's place), `RaftTraffic` fails the install stream with UNAVAILABLE
  instead of forwarding the chunk, counts it in the `install.recordFailures` series, and the leader starts the install
  again after its retry wait (R14-3; Ratis' leader never sends a single chunk again, so a dropped chunk would show only
  at the next one, or after the leader's request timeout; `GrpcLogAppender.InstallSnapshotResponseHandler.onError`
  resets the client and closes, checked in the bytecode). Such a node never catches up by a snapshot, and its log
  and snapshot writes, in the same directory, fail as well: the series and Ratis' own errors show it. gRPC ends a call
  that the interceptor closed with `onComplete`, which the listener of Ratis' stream handler does not pass on
  (`ServerCalls.StreamingServerCallListener.onComplete` runs only an on-close handler); the interceptor passes it on as
  `onCancel`, which tells Ratis' request observer of the end as for a leader's cancel (finding 7, checked in the
  bytecode of the gRPC that Ratis shades; with a fake call only). A chunk of a leader whose term is below the node's
  current term, a deposed leader's, does not reach Ratis at all: `RaftTraffic` answers it NOT_LEADER with the current
  term, as Ratis would, and does not record it (N-R15-1 below). Nor does a chunk whose configuration index is below
  the node's install floor (`MembershipFacts.installFloor`: the higher of its newest committed configuration and its
  latest configuration if that one's term is the chunk's leader term, committed or not): `RaftTraffic` fails its stream with
  FAILED_PRECONDITION, as for a failed record (R16-2 below), and counts it in the `install.staleChunks` series. Each
  series has an alert (R17-6): a live stream refused by mistake would otherwise show only as a WARN and a follower that
  never catches up. `rockserver-install-record-failed` fires on the first failed record (a disk fault);
  `rockserver-install-stale-chunks` fires only when more than 3 stale chunks come in every 30 minutes for 30 minutes,
  since the chunks still in flight of a stream its leader gave up on are refused too, and a few after a leader change
  or a restarted install are expected (R17 review, finding 2). When the bound is below K*'s term, D has an entry of a lower term than K* at iStar before it, which is not
  K*, and Leader Completeness keeps it from committing: step 4 skips it. Otherwise D may be K*'s successor: a re-add of
  this incarnation, whose staging ended within Ratis' catch-up gap, so that the leader then installed a snapshot with
  the joint add D past it; restarted, Ratis runs the node with D and it writes `raft/member`. If the members of K*'s
  new half answer before that leader replicated D to them, step 5 sees nothing, and a leader that cannot reach the
  node can still commit D: the round-12 rule (skip with no bound) said REMOVED there, which
  `RemovalRulePropertyTest.aReAddFromAnInstallPastTheEndOfTheLogKeepsTheNodeWaiting` reproduces. A crash between the
  record and Ratis' write leaves a record of a configuration Ratis may not hold, which bounds nothing it holds. Without
  a record of D (an install before this node started with this version, or a configuration that was dropped from the
  file to make room) the node waits with OWN_LATER_CONFIGURATION, as before R12-3, until a leader's configuration
  reaches it. The majority of K*'s new half matters because
  every configuration after K* needs a majority of K*'s new half to commit (the joint one after a stable K* has K*'s new
  half as its old half; the one after a joint K* is its new half), and that majority meets the confirmers. Answers come
  at different times over a round, so the guarantee is: no configuration that includes the node commits during the
  round unless its current data directory is brought up to date, and a leader sending to it during the round makes
  the verdict HEARD_LEADER. S1 and S2 are checked against a model of Raft logs and Ratis' configurations by
  `RemovalRulePropertyTest` (R12-6), with answers taken atomically per round. The term-start case (R11-1) and the
  joint case (R3-G2b-4, R10-1) need nothing special: an
  uncommitted configuration never enters the committed set. Liveness (S2): with membership quiescent and a majority of
  the newest committed configuration's new half up, of this version and applied, the next check finds that
  configuration as K*, those members confirm it, other removed nodes do not block (lower term, or "absent" for K*), and
  the node's own tail does not either (its log lacks K*, which the term at K*'s index shows, or holds it with no later
  configuration that includes it): it exits with 4, at its start or while detached, since no leader of K* sends to it.

  Residuals: an `add-peer` of a node id to a data directory that still has a log of its earlier membership (which
  `add-peer` refuses, but Ratis' own tools do not) may end with exit 4 during that add; the operator then wipes it, the
  documented procedure. A node writes `raft/member` on its monitor thread, every 200 ms, once Ratis runs it and
  its latest configuration holds it (round-11 review, finding 3): a `--join` node that crashes within that interval
  after it became a voter, and is then removed while down, gets NOT_ADDED at every start instead of exit 4, with the
  advice to stop and wipe it; writing the file from the apply path would not close the window (the configuration that
  makes it a voter may arrive uncommitted) and would count configurations replayed during an add again, so it is left.
  `JoinRemovedWhileDownTest` waits for the file instead of sleeping. A node whose add is
  older than the members' records (a database of an older version) may stay NOT_ADDED the same way. Members of a
  version without `Configuration2` leave R11-2 undecided (NEEDS_TERM). An old `raft/member` may come from a
  configuration that was never committed: at worst a node whose add was dropped exits with 4 and the advice to wipe it
  and add it again, which add-peer needs anyway. Stock Ratis keeps a removed node with a stale configuration as a
  follower until the check fires; Ratis' own shutdown signal (`shouldSendShutdown`, which compares only indexes) is
  left as it is. Ratis' own safety, Leader Completeness included, is assumed, for changes of one node at a time:
  the model of `RemovalRulePropertyTest` found that with a change of several nodes at once it does not hold. A leader
  elected while a joint configuration is uncommitted writes a term-start entry with only the new half
  (`LeaderStateImpl.StartupLogEntry` takes `getRaftConf().getConf()`, checked in the 3.3.1 bytecode) and then commits
  by the new half alone; when a majority of the new half and a majority of the old half can be disjoint (two nodes of
  five removed at once, say), a majority of the old half that never saw the joint configuration can elect another
  leader, which overwrites those committed entries. Round 14 enforces one voter per change ("Replacing a node"):
  `admin add-peer` and `remove-peer` read the leader's configuration, refuse a change of more than one voter
  (`VoterChange`) before proposing anything, and send a compare-and-set; the leader's `MembershipGuard`, an
  interceptor on the gRPC server of Ratis' admin service, refuses the same for any client (with
  `SetConfigurationException`, which Ratis clients do not retry), answers `ReconfigurationInProgressException` while
  its configuration is joint, and rewrites what it lets through into a compare-and-set against the configuration it
  checked, with the same call id. The header `rockserver-unsafe-membership-change` (`admin
  --unsafe-multi-voter-change`) lets a change through with a WARN. In-process calls
  (`Division.getRaftServer().setConfigurationAsync`, tests only) skip gRPC and the guard, on purpose, and so do the raw
  Ratis servers of `ReadBarrierTest`. On a node that is not the leader the guard checks too, against that node's own
  configuration (its current half, if joint): Ratis runs the unary handler only at half-close and then queues the
  request on the division's executor (`GrpcAdminProtocolService.setConfiguration`, `RaftServerProxy
  .setConfigurationAsync`, `getImplFuture().thenCompose(...)`), so the gap between the guard's look and Ratis' handling
  includes that executor's backlog, in which the node can win an election and commit its term-start entry. A refusal
  there goes back as `NotLeaderException` (with the leader the node knows), so the client asks the leader, which checks
  again; what passes is rewritten into a compare-and-set against the node's configuration. If the node leads by the
  time Ratis handles it, Ratis applies it only if its configuration is still the one checked (if it differs, Ratis
  refuses it as different, and `admin` reads again); if it is still a follower, Ratis answers `NotLeaderException`
  itself. So no unchecked request goes through on a node of this version. What remains: in-process calls, and leaders
  of an older version during a rolling upgrade (above). The upstream report about the term-start entry is for the
  user. A second one is for the user too (N-R15-1, round 15): a snapshot chunk of a deposed leader, which Ratis
  rejects with NOT_LEADER, still drops Ratis' configurations from the chunk's index on, those of its own log and
  committed ones included, and makes the stale configuration current (`raft-meta.conf` too). A node can so lose the
  evidence of a later configuration that includes it: after the leader installed D on it, a deposed leader's chunk
  with a configuration at a lower index leaves the node with that one, and if the leader that holds D does not answer
  the check, the node may be told REMOVED while D may still commit; on a leader the same chunk replaces the
  configuration it commits with. `RaftTraffic` sees every chunk before Ratis does, so it answers a chunk whose
  leader's term is below the division's current term itself: NOT_LEADER with the current term, the reply Ratis builds
  (`ServerProtoUtils.toInstallSnapshotReplyProto`), on which the sender's appender steps down; the chunk is neither
  recorded nor forwarded, and the stream goes on (its half-close reaches Ratis). Terms only grow, so a chunk stale at
  that check would be rejected by Ratis too: nothing Ratis would accept is refused. What is left is a term that rises
  between that check and Ratis' own (the node votes or times out in between): Ratis then rejects the chunk and takes
  its configuration as before. The chunk was recorded with its term, so R14-1 holds there, but the configurations
  dropped are lost as before; the model example `aStaleChunkOfAnotherConfigurationDoesNotLowerTheBoundOfTheOneRatisHolds`
  steps through that window (`RacedStaleInstall`), and random histories never do. Closing it would take Ratis' own
  lock; the upstream report stays for the user. `aChunkOfADeposedLeaderDoesNotReplaceTheConfigurationRatisHolds`
  shows the wrong REMOVED without the check (it fails at `009b698`).

  The same hazard has a form of the current term, which the term check cannot see (R16-2): a delayed chunk of the
  current leader, from an install stream it already gave up on (S1), that reaches the node after a later stream (S2)
  installed and the appends after it brought a later configuration C2, committed or not, known to be committed here or
  not yet (the node learns that from the leader's next `leaderCommit`, up to a heartbeat later). Ratis answers it
  ALREADY_INSTALLED, or installs it, and takes its configuration either way, dropping C2 and any later one from its
  memory; the record is right (same term), but Ratis'
  configuration is not, with the consequence of N-R15-1 (a wrong REMOVED if the leader that holds the later
  configuration does not answer). It needs gRPC to deliver S1's queued message after S2 and the appends (S1 held for
  seconds behind a record's sync or Ratis' lock, with no fair ordering): contrived, but not excluded. `RaftTraffic`
  narrows it as well: a leader's chunk carries the leader's latest configuration (`LeaderProtoUtils` reads
  `getRaftConf()` when it builds the chunk), whose index is at least that of every committed configuration (the
  leader holds every committed entry: Leader Completeness, and those committed in its term it appended itself), and a
  follower learns of a later commit only from that leader's appends, which its appender sends after the install
  ends (`GrpcLogAppender.run` waits for the install's replies). The same holds for a configuration entry of the
  chunk's leader term in the follower's log, committed or not: only that leader appends entries of its term, it
  appended the entry to its own log before sending it, and so every chunk it builds afterwards carries that
  configuration or a later one, while no append reaches the follower during a live install. So a chunk whose
  configuration index is below the floor, the higher of the node's newest committed configuration and its newest
  configuration of the chunk's `leaderTerm` (`MembershipFacts.installFloor`), comes from an abandoned stream, and
  `RaftTraffic` fails that stream with FAILED_PRECONDITION before Ratis sees the chunk (no leader waits on it; were
  the argument wrong for a live stream, that leader would retry the install, and the WARN shows it). This covers C2
  before it commits and before the node learns that it did (R16 review, finding 1): the first form of the check,
  against committed configurations only, missed both. Committed is `MembershipFacts.committed` (the history applied,
  or Ratis' configuration if at or below the commit index and in the log); the newest of the leader's term is Ratis'
  latest configuration (`getRaftConf`, the newest in the log) when its term, from the log or the history, is the
  chunk's: a node whose term is above that is answered NOT_LEADER first, and the configuration of a later leader
  gives no floor (that leader may not hold it). Both are unknown (no check) before the division runs; a stale
  `raft-meta.conf` of unknown term (see R11 review, finding 1) only hides the second. Left: a configuration that
  enters the log, or commits, between that check and Ratis' handling of the chunk, as for the term, which needs the
  leader's appends while the stale chunk is between the two on its listener thread; the upstream report (N-R16-1)
  covers both forms. `RaftTrafficTest.aChunkOfAConfigurationOlderThanTheCommittedOneFailsTheStream` and
  `theFloorIsTheNewestConfigurationOfTheLeadersTermCommittedOrNot`; how a running node wires it,
  `InstallStreamTest.theRunningNodesFloorFailsAReplayedChunkOfAnOlderConfiguration`.

  Tests (in-process; each of the first three fails at `0d8bc71`, which has the round-10 rule and the test filter):
  `FlattenedConfigurationStartTest` (R11-1: five nodes; with `RaftTraffic.dropForTesting`, L's appends reach X only,
  L appends the joint configuration that removes X, loses its leadership and is elected again, so its term-start
  entry, the stable configuration without X, reaches X; X runs with a 60 s election timeout meanwhile, since Ratis
  shuts down a candidate outside the new half of its configuration; then the others elect a leader that drops both,
  and X starts and becomes a follower; at `0d8bc71` it is refused at once), `DivergentTailRemovalTest` (R11-2: X leads
  term after term with appends that reach nobody, then the others remove it at a lower index and a higher term; X's
  start is refused, and, started while the members are down, it stops once they answer; at `0d8bc71` it starts and
  never stops), `JoinRemovedWhileDownTest` (R11-3: a `--join` node gets only the joint configuration of its add, goes
  down and is removed; its start is refused; at `0d8bc71` it waited for `add-peer` forever), `ReAddTest` (a removed id
  wiped and added again, replaying its earlier membership, through checks while it hears no leader and a restart in
  the middle of its add: NOT_ADDED, never exit 4, then a member; and a fresh `--join` node through six completed checks;
  both wait for a count of completed checks, not a fixed sleep),
  `ColdRestartOrderTest` (every start order of three members; a removed fourth node started first, in the middle or
  last exits with 4 once two members are up), `CommittedConfigurationsTest` (records with terms, kept across a restart,
  probes; a snapshot install from a leader that holds an uncommitted joint configuration records the leader's
  history and not that configuration, which the next leader drops), `MembershipFactsTest` (a stale configuration of
  Ratis over another leader's entry: not committed, no term, its probe absent; X not removed; also once the entry is
  purged; a probe in the snapshot with no record is unknown, R12-2), `ConfigurationHistoryGapTest`
  (an install notification between a configuration and its position; a history left behind by an older version),
  `JointRemovalStartTest` (now asserts the verdict's
  reason code, R11-8), `RemovalRuleTest` (one case per scenario, with the R11-1 and R11-2 inputs, Figure-8 tails,
  quorum arithmetic for odd, even and joint halves, version-1 answers that do not confirm alone (R12-1), a latest
  configuration outside the node's own log, skipped only with a term bound below K*'s (R12-3, round 13), the
  "voter once" cases and INCONSISTENT), `RemovalRulePropertyTest` (R12-6, the model above, with the R11-1, R11-2,
  finding-1, R12-1, R12-2 and R12-3 scenarios, the round-13 re-add from an install and the R14-1 chunks of deposed
  leaders as examples, and N-R15-1), `InstalledConfigurationTest` (a bound per configuration that a chunk of another
  one does not lower, concurrent updates, the floor of evicted configurations, the start that discards a round-13
  record, the format tag, a file lost or damaged while running written again with the floor given, a start that
  lowers a floor of the highest term, a file replaced behind the cached record), `RaftTrafficTest` (a chunk whose
  configuration cannot be recorded fails the install stream, what Ratis sends on it after that is dropped, and Ratis
  is told of the end as a cancel; a chunk below the current term is answered NOT_LEADER without Ratis), `RatisStaleInstallConfigurationTest` (Ratis takes the configuration of a
  chunk it rejects for its term) and `SnapshotInstallTest` (a real install records the configuration),
  `MembershipGaugeTest` (the gauge values, R12-4) and
  `RemovalCheckTest` (rounds, unreachable members, the `Configuration2` wire through the real handler and client, a
  member with only version 1, and one with neither). Not run here: the process suites (`MembershipTest`,
  `MembershipFaultTest`, the random-fault suites), which would check the exit codes and a supervisor's restarts; a
  rolling upgrade with an old member has no harness.
- **Heap.** Run every node with `-XX:+ExitOnOutOfMemoryError`: `run` refuses to start without it (exit code 2), unless
  given `--allow-without-exit-on-oom`, which is for development only. A leader that
  sends a snapshot keeps each chunk on the heap for 4 s, whether or not its reply came: Ratis' `StreamObserverWithTimeout`
  holds it in the timer of the chunk's timeout, which is 8 times `raft.grpc.server.install_snapshot.request.timeout`, and
  we set that to 500 ms. So add to the heap of a leader what it sends in 4 s. Measured live `byte[]` after a full GC on a
  leader sending a 1 GB snapshot, on this machine's disk at about 200 MB/s:

  | Setting | Live heap | Send |
  |---|---|---|
  | Ratis' 3 s (24 s per chunk) | 1038 MB: all of it, and it would keep growing | 8.5 s |
  | 500 ms (4 s per chunk), the setting | 792 MB | 7.6 s |
  | 250 ms (2 s per chunk) | 540 MB | 6.0 s |
  | 500 ms, chunks of 1 MB | 413 MB | 16.4 s |

  What stays is what goes out in that time, so a smaller chunk saves nothing per byte sent: it only slowed the send, and
  chunks stay at 8 MB. A shorter timeout costs more than it saves: a follower that answers no chunk for that long
  makes the leader start the whole snapshot over. The rule is about 1 GB of heap per 250 MB/s of sending, on top of what
  the node needs anyway. With Ratis' 3 s a send at 200 MB/s holds up to 4.8 GB, derived, not measured. With 500 ms the
  same send also passed with `-Xmx1g` and `-Xmx700m`, where it is slower, and `-Xmx1g` at 3 s ran out of memory in 5
  of 5 runs. Only an error that the JVM itself throws makes the option exit: Netty's limit on direct memory is not one,
  and neither is the JDK's `-XX:MaxDirectMemorySize`, both of which Java code checks and throws. They are the same
  hazard on a thread of Ratis (gRPC encodes a message into direct buffers on the thread that sends it, an appender's), so
  they are fatal too: `run` watches JFR's throwable events, without stack traces, and a node that made Netty's
  `OutOfDirectMemoryError` or the JDK's "direct buffer memory" error exits at once with 3, within about a second
  (`DirectMemoryWatch`). The limit is the heap size unless `-XX:MaxDirectMemorySize` (or `-Dio.netty.maxDirectMemory`)
  says otherwise; leave room for the snapshot chunks and the forwarded calls in flight. Without JFR in the JVM the node
  warns and runs without the watch.

  Only such an error on a thread of replication is fatal: a thread of Ratis (`<peer>@group-...`: appenders, log
  worker, state machine updater, leader and follower state; `<peer>-server-thread<n>` and the other executors of its
  server; `grpc-default-...`, or `grpc-nio-...` where Netty runs without epoll, its shaded gRPC transport). Client traffic makes the same error on other threads: the
  API's event loop (`rockserver-grpc-server-loop-...`, named for this) reading a large message, a thread encoding a large
  reply, a forward to the leader (`rockserver-grpc-<node>-forward-...`). That error fails the one request, and halting
  for it would do harm: the client retries on the next node, which halts too, until no majority is left. So it is
  logged as an error, with its thread, and the node goes on (`DirectMemoryWatchTest` pins the names on a running
  cluster, `DirectMemoryProcessTest` sends a message larger than the direct memory of every node). This does not keep
  client traffic from using up the memory Ratis needs next: on this JDK both Netty copies, the API's and the shaded one
  of Ratis, allocate through the JDK's direct buffers (neither uses a counter of its own: measured, `noCleaner` is
  false for both), so they share one limit, and an append that fails for lack of it halts the node. A hard cap of the
  API's direct memory below the limit would need a pooled allocator with its own limit for the API transport, which
  gRPC's Netty transport does not offer per server; the bound instead is the configuration: set
  `-XX:MaxDirectMemorySize` to at least the snapshot chunks in flight plus `max-inbound-message-bytes` (512 MiB by
  default; lower it to what the clients really send) times the large requests expected at once.

  An out of memory error on a thread of Ratis does not stop the JVM. In the appender of a new member it ends the
  catch-up as if it had finished: `LeaderStateImpl.restart` starts a new appender only for a peer in the configuration,
  and `checkStaging` then finds nobody lagging and appends the joint configuration, with a member that has nothing.
  - With `-XX:+ExitOnOutOfMemoryError` the leader exits at once (code 3, like a failed replica), before any of this. Start
    it again with more heap, and run `add-peer` again. The new node keeps waiting. Run once with the literal `add-peer`
    below: replicas identical at the end.
  - Without the option, after `remove-peer` of the lost node and `add-peer`, nothing is lost: `add-peer` says `OK` too
    early, but the two old members are a majority of both configurations, the leader stays, writes go on, and a new appender starts the
    snapshot again. Run once.
  - Without the option, which now takes `--allow-without-exit-on-oom`, with `add-peer` that leaves a lost node in the
    configuration, the cluster ends with no leader,
    in all 5 runs of the review and 1 of ours. The joint configuration needs the vote of the new node, which is
    only in the new half, so Ratis starts it in the initializing state, where it refuses votes (a member with an empty
    log would not count anyway). Restarting nodes does not help, with or without `--join`, and neither does a copy of a
    survivor's data under the new node's id, which starts in the initializing state too (run once). What worked (run
    once, replicas identical at 16,100 keys): stop every node, find the survivor with the highest last log entry, copy
    its data directory to the lost node's place, and start it under the lost node's id. This is outside what Raft
    proves. With one node of three lost, that copy holds everything committed, since every majority holds a survivor,
    and a leader that the lost node helped elect is a survivor that wrote an entry of that term, so the highest
    last entry covers that term too. Start the survivors
    and the new node (wiped, with `--join`), which gets a snapshot, then run `remove-peer` for the lost id. Do this only
    with the lost node's disk really gone. With two nodes lost see "No quorum".
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
     section is about, losing fsynced entries and votes. Such copies have to come from the storage layer. A `Backup`
     entry gives the other kind: a copy of the data, the same on every node, from which a new cluster starts (see
     "Checksums and backups", and the runbook in [runbooks.md](runbooks.md)).
  4. Without a copy, each replica's `data/db` still holds its last flushed state. It cannot seed a new cluster as it
     is, since it belongs to the old cluster and a node without Raft state refuses it. On a stopped node (a running
     one is refused with 1 before anything of it is touched: `export-seed` first takes `raft/<group>/in_use.lock`,
     which a node holds for as long as it runs, as `repair-log` does; R32-5),
     `ReplicatedServerMain export-seed --data-dir DIR --target NEW [--rockserver-config FILE] [--group-id UUID]` writes a copy (table files
     hard-linked when `NEW` is on the same file system) without the replication metadata, cluster id, applied position,
     results and sessions, and prints the applied index and term of the replica. It builds the copy's config before
     it writes anything (an absolute `volume-path` under the source `db/` becomes the same path relative to the copy),
     so a refused config leaves no half-made target. It refuses, with exit 5, a replica whose cluster version has a
     capability the binary lacks (R18-1): the copy leaves the version out with the rest of the metadata. A replica
     that crashed during a snapshot installation can hold, in `raft/<group>/sm/snapshots`, a received snapshot ahead
     of `db/`; `export-seed` verifies it and restores it into `db/` first, as the node's start would, so the copy and
     the printed index are the snapshot's (R31-2; the output says so). A snapshot that the crash damaged fails
     `export-seed` with 1 and nothing written, naming the snapshot's index and the database's (R32-1): the replica
     may have applied entries up to it that `db/` lacks, so its printed index would understate it.
     `--without-damaged-snapshot` copies `db/` as it is, and the output names the snapshot left out. `--group-id` is
     the node's, as for `run` (the default group otherwise); a group, given or not, without a directory in a `raft/`
     that holds other groups is refused with 2 (R31-2, R32-2). Pick the replica with the highest one,
     and seed a new cluster with its copy ("Migrating from a single node"). What the replica had not flushed, and all that
     only a Raft log holds, is lost: no tool reads it out. `StartupChecksTest` checks that the copy holds the data and
     no cluster; seeding a whole cluster from it is the seeded start that `MembershipTest` covers. This is a last
     resort.
- **Graceful stop.** On SIGTERM a leader hands leadership to the most up-to-date live follower. It then waits until
  it knows of a commit of the new leader, for at most 3 s, and stops. A deploy restart with all nodes up costs a few
  hundred milliseconds without writes (see the tests). With one node of three down the stopping node is the only
  partner of the new leader, so writes wait for it to come back. The new leader loses its majority while the old one
  restarts, steps down after one maximum election timeout, and a new election follows once the node is back. Measured
  with real processes on a real disk, 6 rounds each: 4.6 to 6.1 s without writes at the default election timeouts, and
  2.3 s at 3000:6000. Before this wait, and before a leader that lost its majority could stand for election again
  after one maximum election timeout instead of Ratis' default of 10 s (`leaderStepDownWaitTime`), the same rounds took
  3.0 to 15.6 s, and once 35 s at 3000:6000.
- **Long pauses.** Ratis closes its server after a JVM pause longer than a limit, and a division also closes when a
  leader tells a node outside its configuration to shut down. Both leave the process up: the API listens, the Raft
  port refuses connections, and a closed division still reports its last leader, which it can no longer follow. So
  the Raft watch thread of `ReplicatedNode` watches the division, and a node whose Raft closed while it was running
  stops like a failed replica: it exits with code 3, with `Raft closed` in its error, so that a supervisor restarts it.
  Ratis' log worker does the same after a disk error (`hit exception`, then it closes the division). The loop runs
  every 200 ms, so a graceful stop (a SIGTERM) can come first: `close()` checks the division too, before and after the
  leadership hand-off, and if Raft closed by itself it stops as the failure's stop, with exit code 3, not 0 (DD-7h).
  It waits for the database first, which Ratis closes on a thread of its own: a JVM that exits during that close does
  not end, because the destructors of RocksDB join background threads that wait for the JVM to detach them (seen in a
  thread dump, in about 1 run in 10). Its clients then move at once, and it comes back from its disk like any restart. The limit stays at Ratis' 60 s, set
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
  decides the race: on a tmpfs the reply gets out first. `LeaderStall`, on the same Raft watch thread, makes such a
  leader step down, with a `TransferLeadershipRequest` with no new leader, which works on a leader that is not ready.
  The rule: no entry of the term committed, the commit index not moving for 5 maximum election timeouts and at least
  10 s, and the whole log sent to a majority of members that still answer. It does not look at what is applied, so a
  long replay after a restart is no stall. A follower still catching up, from the log or a snapshot, has not been sent
  everything, so it does not count; a step down would start it over. A leader without such a majority does not step
  down this way: Ratis does that by itself after one election timeout. The cost is about 12 s without writes, in a case
  that had no end. The log says `stepping down to restart replication`, and the metric `leader.stall.stepdowns` counts.
  After its first commit a leader cannot stall like this: the next write it accepts has entries, and its reply moves
  the match index past the entry whose reply was lost. An uncommitted entry waits for that write, and only the client
  that wrote it waits with it, until its deadline.

  A disk that is slow looks the same: the reply exists, and takes longer than the bound on every leader, and a step
  down only starts the wait over. So the bound doubles after each step down of a node, up to 8 times its first value,
  and goes back when the commit index moves, under any leader. Before the doubling, with both remaining disks slowed
  to 12 s per write round, leaders stepped down every 11.5 s and committed nothing in 90 s, against 7 commits without
  the rule; the doubling has not been measured on such disks yet. If a partner's disk hangs and heartbeats
  still pass, the leader steps down at growing intervals and the other side does the same: elections and no writes,
  which is what a hung disk gives anyway, when the majority needs it.
- **A leader whose log stops.** Ratis commits on a leader only up to what the leader itself flushed
  (`RaftLogBase.updateCommitIndex`), whatever its followers hold, and no setting changes that. The heartbeats come from
  other threads, so a leader whose log worker hangs stops all writes, and its followers have no reason for an election,
  with two healthy followers that are a majority. Without more, the wait ended only when the queue of the log worker
  filled up: Ratis then blocks with the log lock held, and the appenders that need that lock send no more heartbeats.
  That took 38 to 45 s with 8 writers of 1 MiB values, and with a few small writes it would never happen. So a
  `LeaderStall` rule on the Raft watch thread looks at the leader's log: entries to flush, and a flush index that does
  not move for 3 maximum election timeouts and at least 5 s, makes it hand over to its most up-to-date follower, or
  step down if it has none: a leader that only stepped down often won the next election with the same log. The bound
  doubles here too, and goes back when a flush takes less than its first value, so that disks that are all this slow
  do not end every leader in turn. `LeaderStallTest` stops the log worker of a leader and checks that writes resume
  on another node; how long that takes on a real disk under load has not been measured yet. The log says
  `stepping down`, and the metric `leader.log.stepdowns` counts. It does nothing on a leader with nothing to flush, and
  it cannot help once the queue is full under a heavy load, which blocks the server lock: then the heartbeats stop
  anyway, within seconds at that load. The node stays up with its hung disk, as a follower that does not answer, and
  its disk probe pauses its API (see "Routing"), so its clients move. That pause runs on a thread that reads neither
  the log nor the disk (round 25, R25-W): with the log's write lock held for good, every read of the log waits,
  the Raft watch's and the node monitor's too, and until round 25 the pause shared their thread and never came
  (`WatchdogThreadsTest`). The Raft watch starts each step-down on a virtual thread, at most one at a time, and asks
  `LeaderStall` for a decision only while none is being started: a bound that passes meanwhile gives no answer, and is
  neither doubled nor started again, so the next check after the previous step-down got going decides at once. Until
  the round 25 review the decision was taken and then dropped, which doubled the next bound (up to 8 times) and
  started it again for a step-down that never happened.

## Cluster version

Some features write log entries, or entry flags, that an older node cannot read: it would stop on the first one, and a
majority of such nodes would stop the cluster. So the cluster has a version (`Capabilities`): the capabilities that
every member supports, kept in the meta column of every replica. A leader asks every member of its configuration what it
supports (the `Ops` call of `TermService`, on the Raft port), every 10 s while the cluster lacks something that this
version supports, and proposes a `ClusterVersion` entry once all of them answered with more; a member that does not
answer, or that predates the question, holds the version where it is. Replicas apply it as a union, so it never goes
down. A cluster created by a version with the gate starts at `Capabilities.GENESIS` (its `Genesis` writes it), 0x1F:
the capabilities this branch's builds had when the value was frozen (R17-1), which later versions support and never
change; the leader's poll raises anything newer. It is not what the first gate build wrote: `c756b00` had
`GENESIS = SUPPORTED` = 0xF, and `CHECKSUM_SCHEMAS` (bit 4) came at `729fa06`. A cluster created before the gate has no
version until its leader raises it.

The `Genesis` entry carries no version, and each replica applies it with its own code, so its value must not depend on
the version that applies it (R17-1). It used to be everything the applying node supports: a node of a later version
that replays genesis from the log, because it joined a young unseeded cluster whose log still starts at index 0 (`admin
add-peer` accepts a node that supports more than the cluster) or because it was upgraded before it applied genesis,
would hold a version above the other replicas', and applying `ClusterVersion` as a union never lowers it. With a bit
the others lack it would, as leader, write entries they cannot read, and its checksums would digest differently.
`ApplierTest.genesisWritesTheFrozenVersionNotTheApplyingNodesCapabilities` pins the literal: genesis writes 0x1F, and
`applyGenesis` does not start writing what the applying node supports (`Maintenance.supported()`). It does not
reproduce a newer node: the old code read the constant `GENESIS = SUPPORTED` and never asked `supported()`, so the
test passes before the fix (`b3df680`); it fails only with one more bit in `SUPPORTED` and `GENESIS = SUPPORTED`
restored (checked by hand: 31 expected, 63 written). Not done: a version in the entry itself (a new codec tag). The
frozen value gives every replica the same version without a format change that the replicas of a cluster being
created would all have to read.

Residual: a cluster created by any build before `729fa06` replays its genesis differently on a current node. Builds
before the gate (`c756b00`) wrote no version; gate builds from `c756b00` to `729fa06^` wrote 0xF (`GENESIS =
SUPPORTED` without `CHECKSUM_SCHEMAS`). A current node that applies that genesis from the log writes 0x1F, so it holds
`CHECKSUM_SCHEMAS` while the other replicas do not: its checksums digest the schemas and theirs do not, so every check
reports a false divergence, and as leader it may use any capability of 0x1F the others lack. It lasts until every
older member was upgraded to a build with `CHECKSUM_SCHEMAS` and one of those upgraded members leads, whose poll then
raises the others to 0x1F: a 0xF-build leader supports nothing beyond 0xF and never polls past it, and a current node
that leads never polls at all (`ReplicaOps.onLeader` returns while `supported() & ~clusterVersion` is 0, and its version
is already 0x1F). If a current node keeps the leadership, the residual never ends: transfer the leadership to an
upgraded former 0xF member, or better, recreate the cluster. Such a cluster exists only if a development build
of this branch created it and its log still starts at genesis (nothing was deployed). Wipe and recreate it rather than
add a current node to it by log replay.

What waits for it:

- `--pipelined-updates`: a leader chains updates only when the cluster version has them at the start of its term, or
  when it starts the term that creates the cluster (its genesis brings `Capabilities.GENESIS`, which has them). One raised during a term
  takes effect at the next term; a node with the option logs a warning at each term without it.
- Client sessions: a node advertises them to its clients (`RockserverCapabilities.clientSessions`) only when the cluster
  version has them, or before genesis. A client reads that when it connects, so one connected before stays without
  sessions until it reconnects. So no moment of a node's life may answer "no sessions" on a cluster that has them
  (R16b-1; r16b first named it R16-1, the id of `admin`'s stale-leader read above): `Genesis` makes the version known to the node before it writes the cluster id (it wrote the id first, and
  a client whose handshake fell in between, typically on the leader of a cluster being created, ran without sessions
  for good; `ApplierTest.genesisMakesTheVersionKnownBeforeTheClusterId`), and a database opened at a snapshot install
  gives its version before it replaces the old one. A crash between the two writes of genesis leaves the version
  without the id: genesis is applied again, and a version without an id answers as before genesis.
- Checksums and backups (next section): refused until the version has them. Each member decides by the version it
  has applied, so a member that lags behind the leader refuses after the leader raised it; `admin` then asks the next
  member that answered (R32-4, next section). Before the leader raised it (up to `VERSION_CHECK_EVERY`, 10 s, after
  the last member of a rolling upgrade restarts), every member refuses, the leader too, and `admin` fails with 1 and
  says to retry in about 10 s (R32 review 1).
- `admin add-peer` refuses a node that lacks a capability of the cluster, or one that every member that answered
  supports: the leader may raise the version to it at any moment, also while the new node catches up and before the
  CLI's check could see it. The leader closes the same race from its side: it also asks every peer it sends the log to,
  which includes a peer that `add-peer` is bringing in and that is still catching up (Ratis' staging peers, which are
  not in the configuration yet), and does not raise the version past what that peer supports (`PollPeersTest`, in
  process with a stub division that lists a real member as staging: R3-G3-4; the node's own check with the real
  division runs meanwhile, and the test asserts that a check polled the stub and ended before it looks). A member must not be downgraded below
  it.
- The leader's question and proposal run on the threads of the answers, without blocking them; the flag that tells
  the next round whether one is running is volatile.
- A node refuses to start on a database whose cluster version has a capability it lacks (exit code 5): see "Exit
  codes" in runbooks.md (CR2-6). The check at the start sees only what the database kept, so it runs in two more
  places. A member of a newer version that answered the leader's poll and was then rolled back can meet the raise in
  its log: its database lost the unsynced WAL tail that held it and replays it, or it had not applied it yet and
  catches up. The apply of a `ClusterVersion` entry to bits the node lacks stops the replica (the version stays as it
  was; the failure carries `NewerVersionException`, and `run` exits with 5, not 3, also through its shutdown hook: see
  R6-2 under "Left before a first deployment"). The same member lagging past the
  leader's log installs the leader's snapshot, whose version has the raise: the install checks the database it just
  opened and stops the replica the same way. Not an entry failure: a replica of the newer version applies the entry.
- `checksum-schemas` (CR2-9) puts the column schemas into the checksum digest: a bit of its own though it writes no new
  entry, since replicas digest a position alike only if all of them include the schemas.

The upgrade path from version 0 is tested in process (`CapabilityUpgradeTest`, R3-G3-1): a cluster whose replicas lost
their version, with one member answering an older version (a test hook, `ReplicaOps.SUPPORTED_FOR_TESTING`), is raised
only to what that member supports, and stays there across further checks: no sessions advertised, no pipelining. Once
that member is upgraded the leader raises the version to everything within its 10 s check, sessions are advertised at
once, the leader of the current term still does not pipeline, and the next leader does.

Rejected: a check of the members' versions at each use instead of a version in the log. A node restarted during an
upgrade would not know whether it may serve sessions until a leader asked everyone again, and pipelining would turn on
and off with members coming and going; the log gives every replica the same answer at the same entry, also after a
restart. The upgrade procedure is in [runbooks.md](runbooks.md).

## Checksums and backups

Two entries act outside the database at one log position on every replica (`ReplicaOps`). Both are applied alone, and
their result is the entry's log index on every replica, whatever happened locally: a local failure is logged and
reported, and stops nothing.

- **`Checksum`** (DD-32). Each replica opens a database transaction with a snapshot at that position, which takes no
  time on the apply thread, and lists its user columns. A thread of its own then reads every key and value of those
  columns at the snapshot, in key order, and digests them: SHA-256 over each column's name and entries, columns in name
  order, each key part and value with its length; with the capability `checksum-schemas` (CR2-9) also each column's
  schema, as `ReplicaOps.describeSchema` writes it: key layout, whether it has values, and the merge operator's name,
  version and class, so replicas whose columns were created or altered differently differ even with equal data. That is
  a capability bit although it writes no new entry: replicas digest a position alike only if all include the schemas
  or none does, and the cluster version, read at the entry's position on each replica, says which. An older member
  would otherwise report every check as a divergence. Equal histories give equal digests, whatever the files and
  compactions of each replica. The meta column is left out: it holds things that are local by design (results purged
  at a start). Each replica then asks the others for their digest of that position every 10 s, until each one answered,
  skipped it (a snapshot install jumped over it, or it restarted while digesting: results are kept in
  `<data-dir>/replica-ops.log`) or for 24 hours. A difference is logged on every replica that sees it as
  `REPLICAS DIVERGED` and counted in `checksum.mismatches`, with an alert. This is what finds a merge operator that is
  not deterministic, which nothing else detects. A check reads the whole database on every node at once and holds a
  snapshot meanwhile, which keeps the files that compactions replace: it runs when asked (`admin checksum`) or every N
  hours with `-Dit.cavallium.rockserver.distributed.checksum-every-hours=N` (off by default). The N hours count from
  the last check the replica applied, whoever proposed it (CR2-2): each replica keeps the wall-clock time it applied
  one, also in `replica-ops.log`, so a new leader goes on with the schedule instead of starting it over, and checks
  still run when the leadership moves more often than every N hours. A leader that knows of no check counts from its
  first start with the property set, which every node writes once to `<data-dir>/checksum-schedule` (R6-5): restarts
  more frequent than N hours, a few deploys a day, would otherwise keep the first check from ever running. A periodic
  check whose proposal fails before any leader accepted it (the error says "not applied": no leader in time, a full
  queue) is tried again a minute later (at most N hours), not N hours later. Any other failure (a timeout after the
  entry was sent, a leadership lost after it was appended: `OUTCOME_UNKNOWN`) waits the full N hours as before: the
  entry may still commit and be applied more than a minute later (a long apply backlog, a leadership flap and back),
  and a retry in that window would run a second full-database digest right after the first (`ReplicaOpsTest`). Applying the same checksum entry again (a restart that replays it after a lost WAL tail, or the entries
  after the snapshot) keeps the time of its first apply, the earliest line of that index in the file, instead of the
  replay's. A member that applies an old check for the first time (catching up, or after an install) still takes its
  own time for it: checksum entries carry no leader stamp (only session entries do, and adding one changes the log
  format), so that member, if it leads next, waits at most one interval more than the history says. Nothing that
  happens more often than the interval postpones checks for good. `admin checksum` prints
  `OK` only when every member it was given returned a digest and all are equal: a member down, failed, not done in
  time, or that skipped the position makes it exit with 4 ("incomplete"; `--allow-skipped` accepts a skipped one), a
  difference with 1. It proposes the check again through another member only after an answer that proves nothing was
  proposed (UNAVAILABLE, NOT_FOUND): a second check of the same data would cost a second full read of the database.
  Proposing never waits on one member for the whole admin time limit (R28-1): the members are first asked at once a
  question that proposes nothing (their capabilities, within 3 s or the time left, if less), the proposal goes to them
  in the order they answered, and one that does not answer (down, frozen with its port still accepting connections,
  not in the group) is skipped, nothing having been sent to it. Asking them one after the other (R28 review 1) gave
  each its full 3 s whatever was left: the command ran up to 3 s past `--timeout-ms`, and with a limit of 3 s or less
  a frozen member listed first used it all, so the command ended "no member took the request" without asking the
  healthy ones. In answer order, the member asked is no longer the first of `--peer`, so the member asked can be one that has not
  applied the cluster version yet: right after a cluster's start, a member that `add-peer` is bringing in or that
  catches up on a slow disk. It refuses by its own view, which is behind the committed version, and that
  refusal proposed nothing (`ReplicaOps.versionRefusal`: FAILED_PRECONDITION whose description begins `Not every
  member supports `, in this version's words and the older ones'), so `proposeOp` prints it and asks the next member
  that answered, also for `checksum`, which is still proposed at most once (R32-4). Before, it ended with 1 on that
  first refusal and advised `Upgrade every member first`, although the leader would have taken it; which member
  answered first varied, so the same command passed or failed from run to run. Only when every member that answered
  refuses for the version does it fail with 1, naming each with the version it gave; any other refusal (another
  FAILED_PRECONDITION, INVALID_ARGUMENT, UNIMPLEMENTED) still ends it at once. The end of a rolling upgrade is not a
  case of lag and is not passed over (R32 review 1): until the leader's next version check (`VERSION_CHECK_EVERY`,
  10 s) after the last member restarts, no member has the new version committed, the leader included, so every
  member refuses and the command fails with 1; the message names that case apart ("the leader has not raised the
  cluster version yet ... run the command again in about 10 s") from members that lag ("once they caught up").
  "Every member that answered" is decided by the capabilities question, which waits for each member until
  `PEER_ANSWER_TIMEOUT` (3 s, or the time left) runs out (R32 review 2): so the all-refused failure comes no sooner
  than the slowest member's answer or 3 s, and a member that would take the proposal but answers the question after
  3 s is left out: if all those that answered refuse for the version, the command fails with 1 rather than asking
  again within `--timeout-ms`. Asking again would turn the plain "not every member supports it" into a wait until
  the time limit and exit 4 whenever a member is down, so it stays a failure with 1, which the runbook tells to run
  again. A member
  that took a `checksum` is waited for as long as the time limit allows (R28 review 2): it bounds the proposal itself
  with its own `--request-timeout-ms`, which `admin` does not know, so the former fixed 30 s ("longer than a node's
  request timeout with an election") held only for the 10 s default, and an answer cut short there exits 4 while the
  entry may still commit, and "run it again" could start a second full-database digest. `backup` keeps the 30 s per
  member: it then proposes the same request id through the next member, harmless if the first proposal is still
  pending, so a member that took it and hangs (a leader on a hung disk) does not hold it. Before, a frozen member
  listed first in `--peer` held `checksum` and `backup` for `--timeout-ms` and left the others 1 ms, so every backup
  failed for as long as it stayed frozen. A proposal that no member took within the time limit, and a checksum whose
  answer leaves open whether it was proposed, exit with 4 ("incomplete", R28-2; they exited with 1, which the
  runbooks give to a divergence or a failed backup); a refusal (a member that does not support the operation, an
  invalid name) stays 1. `admin` also refuses unknown options, an option of another command (`--allow-skipped` outside
  `checksum`; `--unsafe-multi-voter-change` outside `add-peer` and `remove-peer`, R28 review 5) and malformed values
  with exit 2 (R28-3: a misspelled `--timout-ms` ran with the 10-minute default and swallowed the next word, and a
  non-numeric value threw out of `main` with 1). So does `run` (R28 review 3): a malformed value (a non-numeric
  number, an `--election-timeout-ms` without MIN:MAX, a `--peer` or `--group-id` that does not parse) or one that
  `NodeConfig` refuses exits 2 with the usage, as an unknown option did already; only the malformed `--peer` did
  before, the others threw out of `main` with 1, which a supervisor restarts for nothing. Words are checked too
  (R29-4): every command refuses a word that is no option's value and that it does not take, with 2. `run`,
  `repair-log`, `export-seed` and `restore-backup` take none, `admin` the command and its arguments, counted per
  command (`status` none). Before, the parser checked only `--name value` pairs, so a flag given a value
  (`--join false`, `--pipelined-updates false`, `--dry-run yes`) turned the flag on, its value ignored, and exited 0;
  the usage now says the flag takes no value. A malformed `add-peer` argument (`PeerConfig.parse` refused it) and a
  member id with `=` or `,` for `remove-peer`, `transfer-leader` or `snapshot` are wrong usage too: the first threw
  out of `main` with 1 and a stack trace, the second read the leader's configuration and proposed nothing useful.
  The results are asked as the proposal is (R29-3): after the proposal, `checksum`, `backup` and `backup-status` ask
  every member for its result on a virtual thread of its own, again every 200 ms until it gives a final one, each
  question within 3 s or the time left; a member that never answers holds only its own thread. Before,
  `awaitResults` asked the members one after another with a fixed 3 s each, so k frozen members listed first cost
  3k s per round and the command overran `--timeout-ms` by that much (three made a 4 s limit take more than 9 s),
  which the runbooks' "ends within `--timeout-ms`" did not say. When the proposal used up the limit, the members
  are still asked for 1 s (`LAST_RESULT_ROUND`), so those that have their result print it. An answer that cannot be
  read (R29 review 1: `ReplicaOps.decodeResult` throws, as on the answer of a member of another or mismatched
  version) is that member's failure: printed as `<id>: cannot read its answer (<error>)`, that member not asked again,
  and the command fails with 1 (`unreadableVerdict`), as asking again would not help; a verdict that fails with 1
  anyway (different digests, a failed backup) is kept. The first version of R29-3 let the error end that member's
  virtual thread, the future completed exceptionally and `awaitResults` swallowed the `ExecutionException` with the
  timeouts: "no result within the time limit", exit 4, nothing logged. Before R29-3 the error escaped and the command
  failed with 1 and a stack trace. `requireCapabilities`
  (`add-peer`) still asks the members one after another, once.
- **`Backup`** (RA-7.2, DD-6f). Each replica writes a checkpoint of its database (`NodeStore.checkpoint`, as for a
  snapshot: a memtable flush and hard links, 100–300 ms during which its apply waits) to `<data-dir>/backups/<name>/db`,
  with `backup.properties` (log index, request id, cluster, node, time). All of them hold the data of every entry before
  the backup entry: a consistent copy of the whole cluster, on every node. The same entry applied again after a crash
  finds its backup and changes nothing. The entry carries a request id that `admin backup` draws at random: after an
  answer that was lost (a timeout, a leader change) it proposes the backup again through another member, and the second
  entry finds the backup of the same request and reports it done instead of "exists already". Results are kept and
  asked for by log index (the name is checked too), so a name used again after its backup was deleted is never
  answered with the older result, and a later failure with a name never replaces the earlier backup's result.
  `backup.properties` is written to a temporary file, synced, and renamed (R26-2: it was not synced, so a power loss
  could leave it empty after the rename; the entry applied again then found a backup "at index -1" and failed). A
  `backup.properties` without an index, as such a power loss left it, is the trace of an interrupted attempt, which
  is taken again, as one without the file. A backup that fails on a node deletes what it wrote (R29-1): the
  checkpoint deletes its `db.tmp` on any failure, as `CheckpointStorage.take` deletes its building directory, and
  the backup deletes `backups/<name>` when it fails after it started writing there (an existing backup of that name
  is refused before, and stays). Before, a failure in the MANIFEST or WAL copy left `db.tmp`, and one in writing
  `backup.properties` a complete `db/` without it; both hard-link every live table and blob file of the moment, so
  on a node short of space (the likely cause) its usage grew by up to the database's size as compactions replaced
  them, until a backup of the same name ran again, which a date-named one never does. A cleanup that fails is logged
  ("delete it by hand"). `export-seed` deletes its target, and the checkpoint its `<target>.tmp`, on any failure.
  `admin backup` prints `OK` only when every member it was given wrote the backup: it exits with 1 if one failed, and
  with 4 if one gave no result in time; `admin backup-status NAME INDEX` asks again later, without proposing anything.
  `restore-backup` prepares an empty data directory from a copy: it restores the checkpoint and deletes the meta
  column, so the database belongs to no cluster, and every node of a new cluster is prepared from the same backup and
  started as a seeded cluster (see "Migrating from a single node"). It renames a `replica-ops.log` and a `backups/`
  left in the data directory by an old cluster (suffix `.before-restore-<time>`): their results, by log index and by
  name, would otherwise answer for the new cluster, whose log starts again from 1. Client sessions go with the meta
  column: clients open new ones. The cluster version goes with it too, so `restore-backup` reads it first and refuses
  (exit 5, and no `db/` left behind) a backup whose version has a capability this binary lacks: the new cluster would
  start at `Capabilities.GENESIS` holding data a newer version wrote, and no later check would see it (R18-1;
  `export-seed` refuses such a replica the same way; `NodeStoreTest` and `CommandLineTest` check both; before the fix
  both commands dropped the version unread, while the docs promised the refusal). Any other failure of `restore-backup`
  after the copy (a configuration that does not parse, a column that does not open, the renames) also deletes the
  `db/` it made, so the same command can run again (R19-3; before, only the version refusal did, and a rerun refused
  the directory as holding a database). Only `db/` is undone: if `replica-ops.log` was already renamed when the
  rename of `backups/` fails, it stays renamed, which the rerun does not mind. A run killed midway still leaves its
  `db/`; runbooks.md says to delete it. A restore opens the database once (to read the cluster version and drop the
  meta column), and the files that open leaves differ on each node: `OPTIONS` holds the node's absolute paths, and
  the info log its own lines (R26-1: `NodeStoreTest.restoresOfTheSameBackupGiveTheSameSeedDigest` restored one
  backup into three data directories and found three digests, with only the two `OPTIONS` files and the info log
  differing; the followers of a cluster restored as the runbook said stopped at genesis with exit 3). So
  `restore-backup` records the seed digest itself ("One seed"), from the backup's files before anything opens them,
  with the digest of the restored files as the start-up check's `opened` line: the first start keeps it, and every
  node restored from a copy of the same backup puts the same digest in `Genesis` or compares with it. A stale
  `db.seed-digest` in the data directory is replaced, and removed if the restore fails. Since the recorded digest is
  the backup's and not the restored `db/`'s, a restored `db/` copied to other nodes without `db.seed-digest` records
  the `db/` digest there at the first start, and those nodes stop at genesis: the runbook's "restore once, copy the
  stopped `db/`" variant copies `db.seed-digest` with it, or deletes it on every node, the restoring one included (R26
  review; before, the runbook said "with or without `db.seed-digest`"). The node cannot find the backup's digest by
  itself, so this stays an operator step.
  Residual: the `db/` that either
  command writes, whichever binary writes it, has no meta column and so no cluster version, so an older binary started
  on it is not refused and starts at `Capabilities.GENESIS` (for example a new cluster seeded from a newer backup with
  some nodes still on the older version). Every node of the new cluster must run a version at least as new as the
  backup's (runbooks.md says so). Keeping the version in the restored database, for the start-up check to read, would
  close it; that changes what a seeded genesis starts from and was not done in R18. Restoring under the old cluster's ids and Raft state was rejected:
  each node's Raft log and vote are local and not taken at the backup's position, and an older log with a newer vote,
  or the reverse, is the fault that "No quorum" describes. A new cluster has neither problem.

## Metrics and alerts

`NodeMetrics` exports its series and gauges into the registry of the node's database, the core server's metrics
pipeline (JMX, and InfluxDB with `database.metrics.influx`): every series as `rockserver.replication.duration` (a
function timer) or `rockserver.replication.count` and `.amount` (function counters), tagged `name`, `node` and `kind`;
every gauge as `rockserver.replication.<name>`. The values are cumulative; the log line keeps its own interval sums. A
snapshot install opens a new database, with a new registry, and the meters move to it. Every meter of a replica's
database, the core ones included, also gets the tag `node`: a replica sets `database.metrics.node-name` to its node id
unless the config sets a name that is not blank (the default config's `""` means unset), since the nodes of a cluster
share one config and would otherwise write the same series. The replication meters take the tag the database resolved,
so a configured name (a host name, say) tags both families the same way.

`NodeMonitor` reads, once per second on the node's monitor thread, what the alerts watch: `raft.role` (2 leader, 1
candidate, 0 follower, -1 unknown), `raft.term`, `raft.last_index`, `raft.commit_index`, `raft.applied_index`,
`raft.flush_index`, `raft.start_index`, the lags between them (`raft.commit_lag`, `raft.apply_lag`, `raft.flush_lag`),
on the leader `raft.follower_lag` per follower (tag `follower`, from the commit index the leader knows of it) and
`raft.follower_lag_max`, `raft.log.bytes` and `raft.log.entries` (DD-22), `snapshot.index` and `snapshot.age_seconds`,
`disk.data.*` and `disk.raft.*` (usable and total bytes, usable percent), `replica.failed` and `membership.state` (0 a
member, 1 never added, 2 outside and undecided, 3 removed: see "Removal and waiting outside"), and
`monitor.refresh_age_seconds`; and from `ReplicaOps`, `cluster.version` and the last checksum matched or mismatched.
A refresh lists, sizes and stats files, reads free space and reads Ratis' log under its read lock, so on a hung disk
the monitor thread waits for good: every gauge it sets then keeps its last value, which looks healthy (lags of 0, a
recent snapshot, free space). The metrics keep refreshing only while the disk answers. `monitor.refresh_age_seconds`,
the seconds since the last refresh ended, is computed by the metrics thread at each read, so it grows exactly then
(round 25 review); the alert `rockserver-monitor-stale` fires on it, and the API pause of the disk probe is the other
sign. The metrics thread only reads fields; the lag samples
of the log line (`metrics.lag-sample-ms`) read Ratis' log under its read lock, on a sampler thread of their own, so a
hung disk stops the samples, not the log line. The node also logs
the alarms it can tell by itself, once when they start, every minute while they last and once when they end: less than
10% free on either disk, a Raft log above 4 snapshots' worth of `--snapshot-every-bytes` (a member gone for good holds
it back), and RocksDB stopping or delaying writes (the core server's `rockserver.workload.storage.pressure.reason`
gauges). The alert rules are [alerts/grafana-alert-rules.yaml](alerts/grafana-alert-rules.yaml), with what each means
and what to do in [runbooks.md](runbooks.md). A replica failure ends the process with exit code 3, so the alert for it is
a node that stops reporting.

**Reading `rocksdb.operation.timer` on a cluster** (DD-6c). It times calls into the local database: on every node the
apply of replicated writes (as `Put`, `PutMulti`, ..., with the internal record, floor and position puts, and one
`CloseTransaction` per applied group), and on the leader (or a follower serving a follower read) the reads, staged
transactional writes, opens and rollbacks of clients, with their database time only. Group it by `node` and keep the
apply apart by its actions, or read the replication timers instead, whose `kind` tag says `apply` (the apply of
committed entries and snapshots, on every node), `raft` (lags and step downs) or `client` (the work of client requests:
queues, Raft round trips, read barriers, forwarding, local commands). Rejected: tagging the core timer itself by role and
by apply or client. Each of its 22 recording sites would have to learn which caller it serves, through a thread-local
read on every database call of the hot path, in the core module that is shared with the single-node server; and a role
tag on a cumulative timer would split one series at every leader change. The node tag and the replication timers give
the same answers without touching the core's calls.

**Sampling of Raft positions** (CR-11). The lag series of the log line (`lag.uncommitted`, `lag.unapplied`,
`lag.unflushed`) were sampled every millisecond on every node, always. They are now sampled every 10 ms, set by
`-Dit.cavallium.rockserver.distributed.metrics.lag-sample-ms` (0 turns them off): the alerts read the gauges above, and
the samples only add averages and maxima to the log line, which 100 samples per second give as well. The overhead of
either rate under load is not measured (it needs a LoadBench run).

## Threads

Virtual threads run what waits for I/O: gRPC calls, the blocking remote attempts of the proposer, forwarded calls
and the leader's short local commands (`NodeThreads.local()`, one executor per node, shut down with it). A RocksDB
call pins the carrier of its virtual thread (JEP 491 removed pinning by `synchronized`, not by native frames), so
slow disk reads can hold every carrier at once. The Raft control plane does not wait behind them: the proposer's
reply handling, resends and expiries and the read barrier's rounds run on `NodeThreads.control()`, 2 to 4 platform
threads per node that run no database calls and never submit to Ratis, whose append on the leader can wait for a
full log queue during a disk stall. The proposer encodes and submits its batches on `NodeThreads.submit()`, a cached
pool of platform threads (`<node>-submit-`, idle ones stop after 60 s; one hand-off per batch, at most one
blocked per batch slot of each lane during a stall); while the node does not lead, its data batches skip them and
are encoded on the sender's virtual threads, which send them to the leader (see "Submitting off the control
plane"). The proposer hands each answer back
to a new virtual thread, so that its callers' continuations never run on the control plane; the read barrier's callers hop to the local executor
before touching the database. The leader forwarder's poll timer has a platform thread of its own. Cluster
initialization (the genesis proposal, which can wait up to the request timeout) has a thread of its own, so that
the API gate and the Raft watch (`LeaderStall`) keep running meanwhile. Those two have a platform thread each, and the
node monitor a third (round 25, R25-W): `rockserver-<node>-api-gate` runs `gateApi` only, which reads fields and
atomics (Ratis' division info, the disk probe's start time) and pauses or resumes the API server, so it waits for no
disk and for no lock that a thread waiting for a disk can hold; `-raft-watch` runs `watchRaft`, which reads the Raft
log under its read lock (an append holds the write lock while it waits for a stopped log worker) and starts the
step-downs it decides on a virtual thread, at most one at a time, since a hand-over takes the Raft server's monitor,
which Ratis holds while it waits for its log or fsyncs its metadata; `-monitor` runs `NodeMonitor.refresh` (file
listings and sizes, free space, the snapshot's age, the log) and the write of `raft/member`. Until round 25 one
maintenance thread ran all three: the node monitor read the snapshot directory under the storage's monitor, which the
state machine updater holds for a whole checkpoint or snapshot install, and the Raft watch read the log, so a hung
disk parked that thread for good, and the API was never paused nor a leader with a stopped log stepped down.
`CheckpointStorage.latestSnapshotDir` now reads a volatile field without the monitor. Every periodic task (the API
gate, the Raft watch, the node monitor and the `raft/member` write, cluster initialization, the install cleanup, the
lag sampler and the metrics log line) and the disk probe's loop catch `Throwable` and log it: an executor cancels every
later run of a task that throws, without a word, and one `OutOfDirectMemoryError` from `apiServer.resume()` (which
builds and binds a new Netty server) would otherwise end the API gate for good, and with it every later pause on a hung
disk (R24-1, round 25 review). A start that fails once these tasks run stops all of them, the disk probe and the
metrics threads before it closes what it started; until the round 25 review only the Raft watch stopped them, as a
replica failure once it found the Raft server closed, and not at all while it waited for the log. Why the local commands stay on virtual
threads, and what to measure, is in [performance.md](performance.md), item 11. The partitioned apply, when on,
adds its lanes: platform threads (`<node>-apply-lane-N`) that run RocksDB calls only for the apply thread. A leader
session of a term that ended is drained on a platform thread of its own (`<node>-session-close-<term>`, once per
term): its rollbacks are native calls that can wait for a stalled disk, and must not run on the Ratis thread that
reported the change while it holds the server's monitor (see "Transactions", "End of a term"). For the same
reason the state machine answers Ratis' frequent calls under its locks from memory: the latest snapshot
(`getLatestSnapshot`, under the server's monitor on every append with entries) and the cluster id (under the append
lock). The partial copies of failed snapshot installs are looked for on a platform thread of their own
(`rockserver-<node>-install-cleanup`), the only thread that lists, walks or renames them (see "Disk cost of
snapshots").

## Behavior differences from a single node

- **Leader changes.** Transactions, update ids and iterators do not survive one. Clients get `UPDATE_RETRY`.
- **Outcome unknown.** A write that may or may not have been applied fails with `OUTCOME_UNKNOWN`: no leader
  answered after a copy may have been appended, the leader was lost with a forwarded commit, the node stopped, or
  the caller's deadline came first. `SERVER_OVERLOADED` and `READ_DEADLINE_EXCEEDED` from a replicated write now
  mean "not applied". The core client resends numbered writes after an unknown outcome, so a write applies once, and
  passes `OUTCOME_UNKNOWN` to its caller only when its deadline ended that. A client that resends such a write itself,
  as a new write, can apply it twice. Over gRPC the error has status `UNKNOWN` (and `SESSION_EXPIRED` has
  `FAILED_PRECONDITION`): a client built before these types sees a plain gRPC error, not one it would resend as an
  overload. A gRPC `DEADLINE_EXCEEDED` seen by a client without sessions still has an unknown outcome.
- **`putBatch`/`mergeBatch`** run as ordered replicated write batches, whatever the requested SST mode. They
  are not atomic in any mode: each chunk of at most 4 MiB is applied on its own, and the chunks applied before a
  failure stay applied. When a chunk fails after an earlier one was applied, the call fails with
  `INVALID_REQUEST`, whose message gives the number of chunks and keys applied, instead of the chunk's own error.
  That error could be retryable, and resending the whole batch would apply the first chunks twice. A failure of
  the first chunk keeps its own error. The core client with sessions sends each chunk as a numbered call of its own
  (see "Client sessions"), so a lost connection mid-batch resends the chunk instead of failing the batch.
- **Size limit.** One replicated entry (one write, multi-write or transaction commit) is limited to 16 MiB. A
  transaction write that would take its commit past that fails with `INVALID_REQUEST` when it is made.
- **Transaction leases** are at most 60 s by default (`--max-transaction-lease-ms`), whatever the client asks
  for: a transaction that runs longer expires, and its next operation or commit fails with a retry error, so a
  client that retries it may never finish it; raise the cap for such clients. A transaction whose snapshot is
  older than the last 4M keys written on the leader conflicts at commit.
- **`scanRaw` SST tokens** describe the leader's own files and carry its node and term: after a leader change the
  tables they name are scanned again (see "SST maintenance").
- **Merge operators** must be deterministic. One that fails makes the write fail with
  `MERGE_OPERATOR_FAILED` on every replica, see "Failures during apply".
- **Maintenance.** `flush` and `compact` act only on the node that receives them; `compactFiles`,
  `getSstMetadata` and `getTableProperties` go to the leader, like `scanRaw`.
- **Workload limits.** The replicated server's `GrpcServer` runs requests on virtual threads of their own (at
  most 16384 waiting on the leader), not through each column's workload scheduler. Streamed mutations run at
  most 256 at once. The 16384 is a count: the bytes of waiting writes are bounded by the byte admission (see
  "Writes, exactly once"), and a waiting read holds only its key. A gRPC message may be up to 512 MiB, as on a
  single node; the system property `it.cavallium.rockserver.grpc.server.max-inbound-message-bytes` lowers it.
  On the leader, point reads, single-key writes staged in a transaction and transaction
  opens run each on a virtual thread of their own (one that waited past its deadline fails without running).
  Rollbacks (`closeTransaction` without commit, `closeFailedUpdate`) run at once on the thread that received
  them. Ranges, multi-key reads, multi-key staged writes and iterator calls use the embedded workload scheduler.
- **Updates of one key run one at a time** on the leader, unless `--pipelined-updates` is on: their reads and
  commits then overlap, and only the time up to each commit's admission is exclusive. Nested or crossed updates
  of the same keys by one client block (up to 12 s) instead of conflicting at once, as long as the first has not
  committed.
- **With `--pipelined-updates`, a `getForUpdate` value is provisional** until the update ends well, so nothing
  may act on it first. (One exception is argued harmless: Yotsuba writes its sender-index presence entry, an idempotent
  put that is never deleted, from a provisional read; the writer of that value already wrote the same entry, so the
  write adds nothing new. See item 0 of "Left before a first deployment".) `closeFailedUpdate` of such an update can wait (up to 2 s) and can fail with `UPDATE_RETRY`,
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
| `ParallelApplyTest` | Randomized logs over five columns (fixed keys, buckets whose tails all collide, a column without fixed key parts), with rejections, copies, sessions, `deleteRange` and positions without data, applied serially and in lanes: the results of every entry and every row of every column, the internal one included, are the same after every group. A 3-node cluster, two nodes in lanes and one serial, holds the same data, also after a restart. Crashes of a replica in lanes: a flush right after one of its commits (the serial prefix before a segment, a segment's joined batch, a group's end, an entry applied alone), more entries applied, then the database closed without a flush; reopened, it holds exactly what was committed at the flush, its position included, and replaying the rest of the log (serially or in lanes) gives the results and user rows of a replica that never crashed, and every row of a serial replica restarted at the same position. The joined batch of `commitWith` equals, byte for byte, a RocksJava batch of all the operations in order. |
| `SnapshotInstallTest` (in-process) | Every node applies in lanes, the log is purged up to each snapshot (test setting), and a member that was down past the leader's log comes back through a snapshot install. Held inside the install, after the new database opened and applied up to the snapshot but before it is published (`storeGeneration` odd), a follower read on it is forwarded, not served (a mutation that skips the generation check serves it and fails the test). After the install the member holds every write, the applier of the installed database sorts new entries into lanes (its segment counter grows), and it serves follower reads again. A read that failed locally is forwarded when the counter was odd or changed, and keeps its error otherwise. A member running a version without `checksum-schemas` (its database too) that installs the leader's snapshot stops, with `NewerVersionException` in its failure (removing the check after the install fails the test). A snapshot cannot meet an open segment of the lanes: segments end with their apply group, and the drain before a snapshot is handled between groups. |
| `ApplierTest` | At-most-once retries, including after a restart; deletion of records, by count, by bytes and at start; results too large for a reply; rejections; atomic commits; detection of column id divergence; batches with duplicates and rejections; groups of entries, with copies and rejections in them; crash consistency of applies without the WAL, and of groups; a logged raise of the cluster version to a capability the node lacks stops the replica (`NewerVersionException` in the failure) and leaves the version and the position; genesis makes the version known before the cluster id (R16b-1); genesis writes the frozen version 0x1F and not what the applying node supports (R17-1; it pins the literal and does not reproduce a newer node, since the old code never asked `supported()`: it passes at `b3df680`, and fails there only with one more bit in `SUPPORTED`, 31 expected, 63 written; R17 review, finding 3). |
| `SessionApplierTest` | Client sessions at apply: a write resent through other proposers, alone or in an entry with other sessions' writes, takes effect once, also after a restart; the session's floor refuses late copies; unknown and expired sessions are rejected and not recorded, a leader clock behind does not revive them, and expired sessions are deleted; a leader whose wall clock is a year ahead, or jumps within its term, expires nothing (`LeaderClock`); two replicas, one restarting midway, give the same results. |
| `SessionWritesTest` (core) | The client's numbering: which writes are numbered (staged writes not, schema changes, `deleteRange` and batches yes, multi writes over 4 MiB counted not), update ids forgotten once the update ended, also by an unnumbered multi write that ends one; a first copy refused by an expired session re-issued under a new one, a later copy so refused ending unknown (also when that copy is flagged not applied), sessions that keep expiring ending at the deadline, flagged not applied; opening retried after an unknown outcome, and again by a write that waited on another write's opening, also when that opening ended "not applied" at the opener's deadline or overloaded; batches unnumbered when the server does not advertise `numberedBatches`; batches sent as numbered chunks in order, a chunk resent with its number, a later chunk's failure reported as partly applied; closing ends waiting writes; each answer of a copy (value, recorded failure, overload, flagged deadline: ends; unknown, then "not applied": same number again; a sender that throws: lost copy); a deadline without the "not applied" flag, gRPC's own included, on a first or a resent copy, ends unknown, whatever its message; a cancel after a copy was sent ends unknown and stops the resends and the copy in flight, before one was sent cancels it unsent, also for a batch (a batch whose chunk only waits for its session counts as unsent); a synchronous wait is bounded, unbounded without a deadline, and never overflows; the client's own expired-deadline check on a first copy ends "not applied"; past 65,536 open updates the oldest is forgotten and counted; a Reactor caller (`Mono.fromFuture`) that disposes its subscription or times out stops the resends and cancels the copy in flight. R26-3: a write that got no session (gRPC's deadline on every opening, a refusal, an opening of unknown outcome) ends flagged not applied, and nothing is sent (`aWriteThatGotNoSessionEndsNotApplied`, unflagged at `31e06ad`). R26-4: for a caller without a deadline, a session that opens after the window sends no copy and the write ends flagged not applied, `SERVER_OVERLOADED` at the window since R27-3 (`READ_DEADLINE_EXCEEDED` before) (`aSessionOpenedAfterTheWindowSendsNoCopyForACallerWithoutDeadline`, the copy was sent at `31e06ad`; since the R26 review the late session is opened by a caller with a deadline, which the window's timer on an opening does not end). R26 review: a write ended by close before it was sent, waiting between openings or sent on a closed connection, is flagged not applied (`aWriteEndedByCloseBeforeItWasSentIsNotApplied`); no session is opened after the deadline (`noSessionIsOpenedOnceTheDeadlineHasPassed`); an opening that never answers ends a caller without a deadline, and one waiting for the same opening, `SERVER_OVERLOADED` flagged at the window (`anOpeningThatNeverAnswersEndsACallerWithoutDeadlineUnsentAtTheWindow`). R27-3: a write joined to another write's opening ends at its own deadline, `SERVER_OVERLOADED` flagged, while the opener goes on (`aWriteJoiningAnOpeningEndsAtItsOwnDeadlineNotApplied`), also when it is re-issued after `SESSION_EXPIRED` (`aWriteReissuedAfterAnExpiredSessionThatJoinsAnOpeningEndsAtItsOwnDeadlineNotApplied`); a synchronous wait that cancels a numbered write before a copy was sent throws `READ_DEADLINE_EXCEEDED` flagged not applied, a plain future stays unflagged (`aSynchronousWaitThatEndsBeforeACopyWasSentIsNotApplied`). R28-6: a write cancelled while the opening it waits for completes, at the moment it is numbered, leaves no number behind: the write still in flight keeps `completedBelow` at its number, and once every write ended `completedBelow` reaches the next sequence (`aWriteCancelledWhileItIsNumberedLeavesNoNumberBehind`, the test holds the session's lock; at `f00426b` `completedBelow` was 1 with number 0 in flight). R28-7: a write that waited for an opening is collectable once it ended, not held by its deadline timer (`aWriteThatWaitedForAnOpeningIsNotHeldUntilItsDeadline`, held at `f00426b`). |
| `ClientSessionTest` (in-process) | A merge and a transaction commit whose leader dies after its followers applied them and before it answered: the client sends them again with their request ids through the next node, the merge applies once and the commit answers `true` from its record. Copies of a commit and of the write that ends an update answer "unknown" while the first is in flight, then from the record. Copies of a numbered `mergeBatch` chunk, `deleteColumnIfExists`, `deleteColumn` and `deleteRange` take effect once; a numbered batch of two chunks is refused with nothing applied; the client library sends batches as numbered chunks over gRPC, a three-chunk batch of items that fill a chunk to the byte taking exactly one number per chunk (the client's and the server's split agree after encoding); a client batch whose leader dies after committing its first chunk, unanswered, resends that chunk with its number and every key is merged exactly once. |
| `NumberedWritesTest` (in-process) | Streamed multi writes of a BATCH caller with sessions apply every item, results included; copies of a numbered `createColumn` and `uploadMergeOperator` answer from the record (no column re-created after a deletion, no version added); a server offering sessions without `numberedBatches` gets a three-chunk batch unnumbered, taking no number; an endless numbered batch stream is refused after a few batches; a commit refused by the leader behind a follower's forward is flagged not applied at the client; a numbered streamed multi write over the server's collect budget is refused as `SERVER_OVERLOADED`, applies nothing, and the budget is given back after each collection; a BATCH `putMulti` with results over 16 MiB counted is sent unnumbered and applied, on every node (R6-3); an unnumbered `putMulti` over the numbering limit that ends an update makes the client forget the update. |
| `ProposerCopiesTest` | A write's entry becomes its batch's copy once encoded, so a later re-encode never reads the caller's changed buffer; a follower's resend of a late batch is answered from the leader's retry cache, one log entry, applied once; a caller giving up during its batch's encoding waits for it, bounded; through the proposer, with a batch paused before its encoding, `cancel()` returns and an `orTimeout` answers only after the encoding, without holding the JDK's delayer thread; a caller that gives up while the encoding does not end is answered after the 1 s cap. |
| `ProposerSubmitThreadsTest` | R22: against a `StubRaft` leader whose submissions block as Ratis' append does behind a full log queue, a write Ratis answers at once is still answered while as many resends as there are control threads block in Ratis' submit (F1); `propose` returns at once while its submission blocks (F1b); a request that `take()` drops past its deadline while its expiry timer abandons it gives its whole weight back to the write admission (F3); a follower's batches and resends skip the submit threads (at most one hand-off to them while about 18 attempts go to the sender), and once it leads a batch handed to the sender is submitted on a submit thread (R22-2); an `Error` while encoding answers the write and frees the slot (R22-3); a batch taken before the proposer closes is not submitted while it closes (R22-4); a batch held while it encodes is overtaken by one taken after it, and is still applied: the later batch's `completedBelow` does not pass its sequence (R23-A). |
| `StepDownRollbackOffRaftTest` | R23-B: while a test thread holds the monitor of an open native transaction on the leader (a read stuck on a stalled disk), a leadership transfer makes the old leader a follower that applies the new leader's entries, an update waiting for a key of the ended term is answered, and the transaction's id is unknown to the new leader; the transaction and the update are rolled back once the monitor is free. A node stopped during such a stuck rollback closes its database only after it (a regression check: the database's own close takes the same monitor). R23-1: when the session's drain thread cannot start (the test makes it throw `OutOfMemoryError`), the old leader still follows and applies the new leader's entries, the key waiter is answered, the transaction and update are rolled back, and the node stops (red before the fix: the run hung). |
| `RustClientLiveTest` (in-process, off by default) | The Rust client's live test against a follower of a three-node cluster: every numbered call, merge operator upload/check/ensure, batches as chunks, two copies of one numbered merge applied once, two copies of numbered multi writes with results answered alike, the multi writes and their result variants over the numbering limit streamed unnumbered, then every node checked. A second run: numbered writes (with results) lost with a halted leader behind a proxy, resent by the client to a follower, answered with their first results and applied once. |
| `CommitOutcomesTest` | The answer to a copy of a numbered commit whose transaction is gone: unknown while a mark of this term exists, also for a mark of a term that started while the copy's leadership barrier ran (the term is read before the barrier), "run it again" with no mark or a mark of an earlier term, the record when there is one; the first mark of a newer term drops the older ones; a copy whose barrier fails with `NotLeader` is told "unknown", with or without a mark (R25-O). `NodeStoreTest` checks that a session expired at the replicated clock is not live while its row is still there. |
| `NodeStoreTest` (disk layout) | Opening a database is not failed by files, or directories, created and deleted in its volume directory while the open checks the disk layout (R12-0); both tests fail without the fix. R18-1: `restore-backup` (`ReplicaOps.restore`) and `export-seed` refuse a database whose cluster version has a bit this version lacks, and leave no `db/` or seed (both failed at `d385ecd`). R19-3: a `restore-backup` that fails after the copy for another reason (a `--rockserver-config` that does not parse) leaves no `db/` either, and the same command then succeeds (`aFailedRestoreBackupLeavesNoDatabaseAndCanRunAgain`; at `d1b98b0` the half-made `db/` stayed and a rerun refused the directory). R26-1: one backup restored into three data directories, each from its own copy, gives one seed digest (`restoresOfTheSameBackupGiveTheSameSeedDigest`; at `31e06ad` three digests, the two `OPTIONS` files and the info log differing). R29-1: an `export-seed` whose checkpoint fails (a test hook throws once the live files are in place) leaves no target and no `<target>.tmp`, and then succeeds (`aFailedExportLeavesNoTemporaryCopy`; `seed.tmp` stayed at `eefe6d5`). R29-2: the randomly named configs that killed opens left in `DATA-DIR` are deleted by the next open, an unrelated file with the same prefix stays, and no config is left after the open (`aConfigLeftByAKilledOpenIsRemovedByTheNextOpen`; they stayed at `eefe6d5`). R30-1: an `export-seed` into an all-digit sibling of the source (`node/20261108` next to `node/db`) succeeds and leaves no config (`anExportIntoAnAllDigitSiblingOfTheSourceWorks`; at `ebe9ae0` the source's open deleted the target's `rockserver-replica-20261108.conf` and the copy's open failed with "File does not exist"), and the config of an all-digit database survives the open of another database in the same directory while a legacy one is deleted (`theConfigOfAnAllDigitDatabaseIsNotTakenForALeftover`; deleted at `ebe9ae0`). R31-1: the replica config is rw------- when new and when it replaces a 0644 leftover (`theReplicaConfigIsOwnerOnly`), and so is the `export-seed` target's config while the source is copied, replacing a 0664 leftover (`theExportTargetConfigIsOwnerOnly`); `rw-r--r--` and `rw-rw-r--` at `9309dc4`. |
| `CheckpointStorageTest` (`export-seed`) | R31-2: a stopped node whose `db/` is at 1:5 and whose received snapshot at 4:40 was never verified (a crash after Ratis renamed it into place): `export-seed` verifies the snapshot (`installed.verified` appears), restores it into `db/` as a start would, and exports and reports 4:40 with the snapshot's data (`exportSeedRestoresAnInstalledSnapshotAheadOfTheDatabase`); with a table of that snapshot emptied by the crash it refuses (`DamagedSnapshotNotExportedException`, snapshot index 40, database 1:5, nothing written, no `rebuild it from the other members`), and with `withoutDamagedSnapshot` exports `db/` at 1:5 and reports the damaged snapshot by name and index (`exportSeedWithADamagedInstalledSnapshotRefusesUnlessToldToExportTheDatabase`, R32-1). Both red at `9309dc4` (1:5 exported, nothing reported); R32-1's refusal red at `74271ac`. |
| `AppliedFlagWireTest`, `AppliedTrailerInterceptorTest`, `GrpcServerDeadlineErrorTest` (core) | The "not applied" flag on the wire: the server's trailer follows the exception's flag (a real server round trip included), the client reads it, and the legacy messages only without it; gRPC's own deadline is never "not applied". |
| `LeaderSessionTest` | Per-key update queue (order, release on commit, expiry also without other traffic, and end of leadership); chains (handoff at admission, the caps on links and on the readers of one, breaks and their end, a pause for a conflicting transaction, duplicates, expiry, close, no session monitor under the log lock); history kept for open transactions and in-flight commits, up to the key cap; an update whose commit is in flight does not expire; the commit size limit of staged writes. |
| `PipelinedUpdatesTest` (in-process, flag on) | With the leader's apply stopped: chained updates see each other's values and nothing is acknowledged; wrong guards; plain writes, transactions and `deleteRange` against a chain; closes in mid-chain, and more of them waiting than a server's CONTROL workers and queue hold; refused operations on chained ids; second writes; merges and multi-key commits; caller gone, abandon; a snapshot and a restart inside a chain, around a rejected entry; a leader change with a chain in flight; a commit that ends without an outcome; the flag on and off at restarts, and a chained id handed to a leader without it; concurrent counters. With the flag off: the commit of an update, read back from the Raft log, has the old bytes, and a second write goes to the log. |
| `PipelinedLeaderKillTest`, `PipelinedPartitionTest`, `PipelinedRandomFaultTest` | The suites of the same name, with the nodes on `--pipelined-updates`. With `PipelinedUpdatesTest`, the only tests that chain in a default run: the suites below run the plain per-key queue unless the property below is set, and fail if an update chains there. After each fault these three wait for an update that chains. |
| `LeaderKillTest` | Repeated SIGKILL of the leader under concurrent writes. No acknowledged merge is lost or doubled, no acknowledged increment of a hot counter is lost, and replicas are byte-identical. Writes resume in about 1 s (the test only requires under 10 s). |
| `FollowerReadTest` (in-process, follower reads on) | A follower serves reads of writes acknowledged on the leader, with writes flowing meanwhile and on a quiet cluster (thanks to `CommitHeartbeats`), some by itself (the share is printed, not asserted beyond a loose bound: it depends on the disk and load); once it knows the commit index it serves by itself. The option is set on every node through the property. A follower whose database may hold an entry of a term later than its read index forwards, also after a real leader change: a read index of term T whose apply (held by the test) is reached in one group with the entries of the next leader's term is forwarded, not served (a mutation without the check serves it); reads stay linearizable on the follower and the new leader across a leader change. A read on the leader whose apply is held does not answer the old value after a follower served the new one. A follower whose apply is held forwards instead of answering with the old value, and one cut off from the rest fails instead of answering. A follower names the leader in its responses, and a client of several nodes listing the follower first reads its writes and its transaction through it. |
| `PeerChannelsMembershipTest` (in-process, 3 nodes and a fourth that joins) | R2-RT-2: after a real membership change the shared `PeerChannels` of every node drop the channel to a member removed after it joined (no startup peer list holds its address) and keep the other members' channels (with the pruning call removed, the channel stays and the test fails). `InProcessCluster.join` starts a node with `--join` on ports of its own, with every configured node as a peer and none of their ports, also a stopped node's (R9-9, `InProcessClusterTest`: two joins and more, no node started; with the old peer list the second join leaves out the first). |
| `CommitHeartbeatsTest` | Each commit index is asked for once and a higher one again, nothing before a commit index or when disabled; Ratis finds the transport by class name and builds it with and without the node's instance. A wake given before the appender waits is kept for that wait (R7-1; Ratis' `AwaitForSignal` loses it), but only when it would make the appender send a commit heartbeat; any other early wake is dropped, a wake while waiting always wakes. Resends after broken streams do not reopen the window of the first send and stop at 3 per commit index (R7-2). R26-5: the wake given while the appender waits is given once the waiter holds `WakeSignal`'s count (`WakeSignal.waiting()`), not after a 50 ms sleep, which a descheduled waiter could outlast and then park for 10 s. |
| `AppenderMetricsRaceTest` | R9-1: Ratis' `GrpcServerMetrics`, made while another of the same division unregisters, gets no registry and its constructor throws a `NullPointerException` (a few thousand generations, under a second); that failure is recognized and the appender made again, up to 5 times; a `NullPointerException` from elsewhere is not retried, one without a stack trace is. Without the retry the tests of it fail. |
| `JointRemovalStartTest` (in-process, 5 nodes, then 3) | R3-G2b-4, R10-1, R11: a node whose log ends with a joint configuration of its own removal that the next leader dropped starts and rejoins, whether it starts after the members of the new half or before them, while none answers (it waits in Ratis' initializing state, with the verdict MEMBER from its own committed configuration); one whose removal was committed is refused, and the test asserts the verdict (REMOVED, K* without it, a majority of its new half confirming) rather than a message (R11-8). |
| `FlattenedConfigurationStartTest`, `DivergentTailRemovalTest`, `JoinRemovedWhileDownTest` (in-process, 5, 5 and 3+1 nodes) | R11-1, R11-2, R11-3, see "Removal and waiting outside": a dropped stable configuration without the node from a term-start entry does not refuse it; a removed node with a longer tail of a lower term is refused at its start and stops while it waits once the members answer; a `--join` node that was a voter of its add's joint configuration and was removed while down is refused. Each fails at `0d8bc71`. They use `RaftTraffic.dropForTesting` to choose which Raft requests reach which node. |
| `ReAddTest`, `ColdRestartOrderTest`, `CommittedConfigurationsTest` (in-process) | A node id added again after its removal, replaying its earlier membership, is never told it was removed during its add (checks while it hears no leader, a restart mid-add), and a fresh `--join` node waits without exiting; no start order of a cold restart refuses a member, and a removed node exits with 4 once a majority is up, whether it starts first, in the middle or last; the committed configurations are recorded with their terms, kept across a restart, carried by a snapshot install, and never take the uncommitted configuration that Ratis reports on an install; in that install scenario the leader holds back only the uncommitted configuration and later entries from the others, not its heartbeats, so it still leads after a wait longer than the largest election timeout before the install. |
| `FollowerReadLatencyTest` (in-process, prints numbers) | Read-after-write latency on a follower, with and without `CommitHeartbeats`, against forwarded reads with and without the leader's apply wait, quiet and with a writer in the background; with the heartbeats the follower serves more than half of the quiet reads and some of the busy ones itself (loose bounds; the printed shares are the measurement). The numbers in "Follower reads" come from it. |
| `ForwardingFailoverTest` | A client of a follower sees only retryable errors (point reads, range streams, writes) while the leader is killed, and every kind of request succeeds again after the election. |
| `ClientFailoverTest` | One client connection listing all three nodes, while the node it uses is killed (the leader, or a follower): only retryable errors, new calls succeed on another node of the same connection (about 1 s with an election, under 200 ms without), a new connection skips the dead node, and a transaction opened through a dead follower still commits. |
| `IsolatedNodeTest` | A client listing every node, connected to a node that gets isolated (the leader, or a follower), moves to the majority and keeps working, and the isolated node listens and serves again after the heal. With every node isolated all pause, and after the heal all resume and the client recovers by itself. An election shorter than the bound pauses no node. A follower restarted while cut off pauses too, and the client that lists it first moves. A cluster killed or stopped as a whole and restarted at once serves again, a paused node takes part in the election, and a node of a new cluster does not pause. A node that lost its database and kept its log pauses, and a client that starts during a cold start waits for the cluster, also past a frozen node. |
| `PartitionTest` | An isolated leader acknowledges nothing. The majority takes over. A leader that keeps its majority keeps serving. The history, hot counters included, is linearizable. |
| `PauseTest` | SIGSTOP of the leader and of a follower. The history stays linearizable. A freeze longer than `--jvm-pause-limit-ms` ends the node with exit code 3, and it starts again. |
| `StartupChecksTest`, `StartupClusterTest` (in-process) | The config digest ignores layout and key order; a column volume outside `db/` is refused before it is created; copies of one seed share a digest that a truncated copy and an opened database do not, and a digest recorded by a failed first start is kept after the start-up check opened the database but taken again after `db/` is replaced, and a replica with another seed than the leader stops at genesis while copies of one seed start a cluster; `export-seed`'s copy holds the data with no cluster and no position, also with an absolute volume path inside `db/`, and a refused target config leaves no target; a direct-memory error made in Java code reaches the JFR watch; `run` refuses a JVM without `-XX:+ExitOnOutOfMemoryError`, and a malformed or refused option value (`--snapshot-every often`, `--election-timeout-ms 1000`, `--parallel-apply-threads 1`, a malformed `--peer`) with 2 and the usage (R28 review 3, `runRefusesMalformedValuesWithTwo`; 1 at `8e4502a`), and so a flag given a value (`--join false`, `--pipelined-updates false`) and a stray word (R29-4; at `eefe6d5` `--join false` started the node as a joiner, still running after 60 s); a removed node refuses to start again, also with `--join`, and so does one stopped before its removal, whose log never received it (the members tell it); a node with a wrong API port for the leader forwards to the port the leader names. The RocksDB library goes to `DATA-DIR/tmp`, whose old copies are deleted, without and with `-Djava.io.tmpdir` (R27-2); a given directory inside `DATA-DIR` loses its old copies, one outside is not touched; a `DATA-DIR/tmp` or given directory that is a symlink to a shared one keeps the copies there (R27 review, `aSymlinkedTemporaryDirectoryIsNotCleaned`). |
| `RemovalRuleTest`, `RemovalCheckTest` | The removal rule, one case per scenario of "Removal and waiting outside" (the R11-1 and R11-2 inputs, Figure-8 tails that block until shown off K*'s branch, quorum arithmetic for odd, even and joint halves, version-1 answers that confirm only a K* a member of this version reports committed (R12-1), a latest configuration past the end of the node's own log or stale over another entry, which does not block only with a term bound below K*'s (R12-3, round 13), the "voter once" cases, INCONSISTENT and an empty committed set); one check's rounds (pending members and new members of K* asked again, a blocker asked until the budget ends, unreachable members leave it undecided at once), the `Configuration2` wire through the real handler and client, and a member with only `Configuration` (asked that) or neither (no answer). |
| `RemovalRulePropertyTest` (jqwik, pure) | R12-6: the removal rule and the real membership facts (on stub divisions, `ModelDivision`) against a model of Raft logs, Ratis' in-memory configurations and `raft-meta.conf` (kept apart), histories, snapshots and installs, with elections, one-node changes, replication, commits, crashes and restarts. After every step: committed prefixes agree, committed facts and probe answers are true, no false INCONSISTENT, and S1 for REMOVED verdicts under answers of version 2, version 1, pending or none; S2 after quiescing. Examples for R11-1, R11-2, finding 1 after a purge, R12-1, R12-2 and R12-3 (the last four fail with the round-13 fixes reverted), the round-13 review's re-add whose joint configuration arrives by an install past the log's end (it fails with the R12-3 skip that ignores the term bound), and R14-1: chunks of two deposed leaders, the second with the configuration Ratis held (it fails with the single record of round 13). The model records every chunk in `raft/installed-configuration` before the follower acts on it, whatever Ratis answers, and applies the chunk's configuration as Ratis 3.3.1 does for NOT_LEADER and ALREADY_INSTALLED as well as for an install (round 15); chunks of deposed leaders, or of leaders cut off from the others, are generated (`StaleInstall`). Since the review of round 15 a chunk below the follower's term is answered by `RaftTraffic` and reaches neither the record nor Ratis; a chunk that passed that check before the term rose (`RacedStaleInstall`) is in examples only, and the example of N-R15-1 fails without the check. Not generated (finding 6): a chunk Ratis would install over a log that already holds the snapshot's prefix, SNAPSHOT_EXPIRED, and chunks that Ratis fails with an exception. The first comes only from the current leader of the follower's term (an older one is answered NOT_LEADER, a later one raises the term first), whose configuration is the one its next appends would bring; the follower's entries past the snapshot that conflict with it are of earlier terms, uncommitted, and the leader drops them anyway. SNAPSHOT_EXPIRED (notifications only) takes the configuration as ALREADY_INSTALLED does, which is modelled. A chunk Ratis fails is recorded but not applied, which only adds a key with a bound at least its sender's term, an upper bound of any entry with that configuration the sender holds. So those orderings fall under cases the model has; the argument, not the model, covers them. One ordering the argument missed (R16-2): a delayed chunk of the current leader from a stream it gave up on, with an older configuration than one committed since, answered ALREADY_INSTALLED; `StaleInstall` builds chunks with the sender's configuration at the step, never an older one. `RaftTraffic` now fails such a chunk's stream (see N-R15-1's residuals), and the model does not generate it. It writes the member file also when a restart runs the node with a configuration that holds it, and lets a staging end after a snapshot install (Ratis' catch-up gap). It found that a change of several nodes at once is unsafe in Ratis (see the residuals of "Removal and waiting outside"), so it generates one-node changes. Answers are atomic per round: the asynchrony of `RemovalCheck`'s rounds and HEARD_LEADER are not modelled. |
| `InstalledConfigurationTest`, `RaftTrafficTest` | R14-1, R14-3: a bound per configuration, the highest term of a chunk that carried it, never lowered by a chunk of another configuration; eight threads recording at once lose no update; an evicted configuration comes back with a bound not below its old one; a start replaces a round-13 record by an empty one with Ratis' term as floor, and the file's first line is a tag an older version cannot read; a chunk whose configuration cannot be recorded fails the install stream with UNAVAILABLE and reaches neither Ratis nor does a later message, and what Ratis' handler sends on the call after that is dropped. All but one fail with the round-13 record. Review of round 15: a chunk below the node's term is answered NOT_LEADER (with the term, the request index and the ids of the request) and reaches neither Ratis nor the record (N-R15-1); a failed stream reaches Ratis as a cancel; a record file deleted or damaged while running is written again with the floor given; a start that reads Ratis' term lowers a floor of the highest term; a file replaced behind the cached record is read again. Each fails at `94a5795` but the last, which fails, as the concurrent test does (on the shared temporary file), with the cache's check and the lock taken out. R16-2: a chunk of the current term with a configuration older than the committed one fails its stream and reaches neither Ratis nor the record (fails at `67781b4`). R16 review, finding 1: the floor is also the newest configuration of the chunk's leader term, committed or not, and a chunk below it fails its stream (fails at `9201372`, whose `installFloor` has the committed configuration only). R17-6: the refused chunk is counted in `install.staleChunks`, a forwarded one is not (fails at `47cc7e2`, 0 counted). |
| `LeaderConfigurationTest` (pure) | R16-1: `admin` settles on the LEADER of the highest term whatever the `--peer` order, not on one below a term another member's log shows, and waits for a change under way on the leader of the highest term rather than taking a deposed leader's settled answer. Three of five fail at `67781b4`. R16 review: after a refusal `changeVoters` fails at once only for the same leader in the same term with the same members, with the advice to run the command again if the leader changes, and goes on against another leader or term (`standingRefusal`, extracted unchanged: it pins behaviour); the members are asked at once (`askAll`; it fails at `9201372`, by its 10 s wait, with the members asked one after another). |
| `InstallStreamTest` (in-process, three nodes) | R16-3: a real Ratis install through `RaftTraffic`. A chunk seen below the follower's term (`RaftTraffic.termForTesting`) is answered NOT_LEADER and the leader steps down (its term rises, another leader installs); a record that fails (`recordFailureForTesting`) fails the stream, the same leader installs again in the same term and the follower catches up. A damaged `raft-meta`: the start keeps a record it has, writes the floor `Long.MAX_VALUE` without one, and fails (Ratis cannot read it either); once repaired, the next start lowers the floor to the term in it. It pins behaviour that was already there: it passed at `67781b4`. R16 review, finding 2: the running node's floor (`ReplicatedNode.installFloor`) is known and is its configuration's index for the leader's term, and a chunk of that install replayed through the node's own `RaftTraffic` with a configuration one index older fails with FAILED_PRECONDITION; it fails if the node's floor is always unknown. R17 review, finding 4: the refusal is counted in the node's own `install.staleChunks` series (fails, 0 counted, with `ReplicatedNode` passing no series). R16-3 is therefore only partly verified: not checked is that Ratis' server keeps no request observer or handler of a failed install stream (nothing exposes it; the tests show only that the next stream installs). |
| `RatisStaleInstallConfigurationTest` (one in-process Ratis server) | Pins Ratis 3.3.1: a snapshot chunk with a leader's term below the current one is answered NOT_LEADER and still replaces Ratis' configuration with its own. Why every chunk is recorded; if it fails after an upgrade, revisit that and N-R15-1. |
| `MembershipGaugeTest` | R12-4: `membership.state` is 0 for a member or not checked, 1 never added, 2 undecided, 3 removed. Fails with the round-11 mapping (REMOVED was 0). |
| `MembershipFactsTest`, `ConfigurationHistoryGapTest` | Round-11 review, findings 1 and 2: a stale configuration of Ratis (loaded from `raft-meta.conf`) over another leader's ordinary entry at its index is not committed, has no term, and its probe answers absent, and a node asking about it is not removed; the same once the entry is purged (round-13 review); a probe in the snapshot with no record in the history is unknown, never absent (R12-2); a snapshot install's notification between the updater's configuration and its position does not drop it; a history whose applied position moved without it (an older version) is complete only after that position. Each fails without its fix. |
| `ClusterIdentityTest` | Each genesis gets its own id, and a second `Genesis` in a log changes nothing. A node that elected itself under the same group id, added with Ratis' own client, stops before the configuration changes, and writes go on. What `add-peer` refuses. |
| `RatisContractTest` | The Ratis 3.3.1 behaviors that the design relies on beyond its API, against real servers in the JVM: the bootstrap and configuration meta files that `--join` safety reads, and the initializing state of a server started with an empty group (no election, no bootstrap file); the open segment format that `RaftLogRepair` rewrites; the type of the full-pending-queue refusal; a state machine refusal that reaches a remote client by class name and message, without a step down; `NotLeaderException` for an entry appended by a leader that then steps down (three servers); a transfer without a new leader that steps down; the 3 s wait of `GrpcUtil.shutdownManagedChannel` with a call in flight; a snapshot chunk held on the heap until its timeout by `StreamObserverWithTimeout`; a leader whose log worker is stopped (`LogWorkerGate`) commits nothing until it flushes; the CPU of the appender to a dead follower (printed, bounded at half a core); with the node's Raft settings, a follower learns a new commit index only with the next append or heartbeat (3 s), and within 600 ms of `CommitHeartbeats.send` on the leader, for both followers, 20 writes in a row, also when the link to a follower drops that heartbeat and breaks its stream (sent again on the new stream), and when `send` lands between the appender building nothing and its wait (R7-1, through a test hook in the real appender; the appender waits on its own `WakeSignal`); a client proxy made while the one before it to the same peer closes can get a metric interceptor without a registry (R8-4: every call of it and its close then throw a `NullPointerException`), and `PeerProxyMap.close` lets such a close failure out as a `CompletionException` after closing the other proxies. The match index moving only on replies with entries, and the follower order of `LeaderStall`, are pinned by `LeaderStallTest`. |
| `RaftSettingsTest`, `ByteAdmissionTest`, `ConflictPositionsTest` | Snapshot chunks stay at most 4 s on the heap; Ratis' pending-request limits hold every node's attempts and stay near the defaults, and the byte limit grows with a peer list of more than 4 nodes; one entry of the largest size fits every limit on its way. The byte admission never exceeds its limit and is sized from the heap. Conflict positions survive the exception that Ratis rebuilds, and a text that does not parse is a conflict of the whole entry. |
| `SstMaintenanceTest` (in-process) | `scanRawResumable`, `getSstMetadata` and `getTableProperties` through a follower describe the leader's database; raw-scan tokens carry the leader and its term and skip their tables through any node, while tokens of another node or term, or untagged ones, scan them again; over gRPC a client that does not set `acceptsTaggedSstTokens` gets plain filenames; `compactFiles` of the internal column is refused. |
| `ColumnIteratorsTest`, `SnapshotBackoffTest`, `DiskProbeTest`, `AddPeerCheckTest`, `ProposerResetTest`, `ProposerBrokenProxyTest`, `ShutdownSignalTest` | Applying a column deletion closes the column's iterators and ends its range streams with `COLUMN_NOT_FOUND` instead of waiting for them (a stream of another column goes on; one started during the deletion fails), and so does a replicated deletion for `scanRaw` and `scanRawResumable` served through a node's API (R3-G2a-5), and an unknown iterator answers `UPDATE_RETRY` (the database's error for it is pinned); a failing snapshot is tried again only after its delay (1 s doubling to a minute), on a cluster too; a disk probe that does not end within the limit means a hung disk, until it ends; a probe that throws an Error once does not end the probe thread (round 25 review: before, the thread died and looked idle for good); `add-peer` waits for a majority of the members to answer; a proposer's connection reset returns once the failed proxy left Ratis' map, before its slow close, so a resend gets a new one; later attempts that failed on a proxy already replaced keep its replacement (R9-2), and a failure reported with an older proxy than the map's keeps the map's, while one reported with the map's own proxy replaces it, the reset rule that R10-5's bound relies on (the test calls `resetBrokenProxy` itself and sends no attempt, so it does not exercise where `Proposer.send` reads the proxy, and passes without that change); a follower's proposer replaces a client proxy to the leader whose calls throw (a metric interceptor without a registry, R8-4) instead of failing every write with an unknown outcome, and a node stops without an error although its proposer holds such a proxy to another peer; the shutdown hook's stop waits for a node whose replica failure was already stopping it, and gives 3; a node stopped by a removal or a newer cluster version exits with 4 or 5 from both the main thread and the shutdown hook's halt (R6-2).; a signal during the start waits for it and stops the node it returned (R28-4) without printing `READY` (R28 review 4, printed at `8e4502a`), and an `Error` of the start exits with 1 (R28-5). |
| `VoterChangeTest`, `MembershipGuardTest` (in-process, 3 nodes and joined ones, gRPC client) | Round 14: what counts as a voter change (by id; listeners, addresses and priorities count nothing; voter to listener and back count one) and the refusal message. A two-voter change through gRPC is refused at once (well within the client's retry time) by the leader, also when the client asks a follower first, and the configuration stays; single changes, an address-only and a priority-only change and a re-run go through; the unsafe header lets a two-voter change through; what the guard lets through is a compare-and-set against the configuration it checked, ADD keeps the current members, and a compare-and-set against another configuration is refused without the marker. The two refusal tests and the rewrite check fail without the guard. Review of round 14: on a non-leader the guard checks against that node's configuration, answers `NotLeaderException` for what it refuses (also a compare-and-set against another configuration), and rewrites what it lets through into a compare-and-set against that configuration (`onANonLeaderTheGuardChecksAgainstThatNodesConfiguration`, fails at `e709a90`). |
| `CommandLineTest`, `FaultFsTest`, `LogTermsTest` | The operator commands through `main`, in this JVM with the exit replaced (`ReplicatedServerMain.exiter`): `admin checksum` and `admin backup` against a running cluster print `OK`, wrong arguments exit with 2, `export-seed` of a stopped replica writes the seed and refuses a target that exists, `restore-backup` restores into an empty directory (which then starts and serves the data) and refuses a used one with 1, and both refuse a newer cluster's database or backup with 5 (R18-1). Round 14: `admin` refuses the one-step replacement of a stopped node with exit 1 and the two-step advice, leaving the configuration as it was; `remove-peer` then `add-peer` pass, a re-run prints `OK`, a stale `--peer` with `remove-peer` is refused. Review of round 14: a re-run of `add-peer` right after it succeeded, and one with the new voter stopped, print `already a voter` and `OK` without asking the node (both fail at `e709a90`: the first said "has a log of its own ... Wipe its data directory"); an unsafe replacement of two of three voters, which Ratis refuses (`changes a majority set`), fails at once with Ratis' message (`aRefusalThatNoReReadChangesFailsAtOnce`; at `e709a90` it re-read until the limit). R27-1: `admin status` with a member that accepts but never answers and one on a closed port, listed first, answers within seconds under a 60 s admin limit, in the `--peer` order, the dead ones `unreachable` (`statusAnswersAtOnceWhenMembersAreDown`). R28-1: with a member that accepts but never answers listed first and a 15 s limit, `backup` and `checksum` are proposed through the next member, every live member gives its result, and both exit 4 for the silent one (`aSilentMemberListedFirstDoesNotHoldAProposalForTheTimeLimit`; at `f00426b` the silent member held the proposal for the whole limit and the command failed with 1, no member asked). R28-2: `checksum` and `backup` that no member took (a silent and a closed port) exit 4 (`aProposalThatNoMemberTookIsIncomplete`; 1 at `f00426b`). R28-3: an unknown option, `--allow-skipped` outside `checksum`, and a malformed `--timeout-ms`, `--group-id`, `--peer` or backup-status index exit 2 with the usage (`adminRefusesUnknownOptionsAndMalformedValuesWithTwo`; at `f00426b` the misspelled option ran `status` with 0, the others threw with 1); R28 review 5: `--unsafe-multi-voter-change` with `status` exits 2 (0 at `8e4502a`). R28 review 1: with the silent member first and `--timeout-ms 2500`, `backup` and `checksum` are still proposed through a healthy member and end within 4.5 s, the limit plus 2 s (R29 review 3: 06c6ec9 had tightened it to 4 s, too little room for the admin's own setup and the channels' closing on a loaded host; `resultsAreAskedOfAllMembersAtOnceWithinTheTimeLimit` covers the results round) (at `8e4502a` "no member took the request", exit 4, after the silent member's 3 s). R28 review 2: only an idempotent proposal is cut at 30 s per member, a `checksum` gets the time left (`onlyAnIdempotentProposalIsCutShortPerMember`, a unit check of `proposalWait`; 30 s for both at `8e4502a`; a member that answers and then holds a proposal is exercised since R28-L2, below). R28-L1: four members that accept but never answer, listed first, cost `status` one question time (about 2-3 s here, bound 6 s) under a 60 s limit (`statusDoesNotAskSilentMembersOneAfterAnother`; 8.1 s at `5318f6b`, before R27-1, where `statusAnswersAtOnceWhenMembersAreDown` passes too). R28-L2: a fake member (`HangingMember`: it answers the capabilities question at once and nothing else) gets the proposal first, the healthy members being behind links with 400 ms of latency each way: `backup` waits for it `proposeAnswerTimeout` (shortened to 2 s) and proposes the same request id through the next member, whose backups carry that id, and `checksum` waits the whole `--timeout-ms 4000` (between 3 s and 6 s) and exits 4 "does not tell", proposing nothing elsewhere (`aMemberThatTookTheProposalAndHangsHoldsOnlyWhatItMay`; green at `eefe6d5` too, a test gap: red with `proposalWait` returning the time left for both, backup "no member took the request", and with the per-member cut for both, checksum ended after 2 s). R29-3: three silent members listed first and `--timeout-ms 4000`: `backup` and `checksum` give every live member's result and end within 6 s, the limit plus 2 s (R29 review 3, 5.5 s before) (`resultsAreAskedOfAllMembersAtOnceWithinTheTimeLimit`; 9.3 s at `eefe6d5`). R29 review 1: a fake member (`GarbledMember`) answers the capabilities question after 500 ms, so the real members take the proposal, and every result question with a state byte that names no state: `backup` and `checksum` print `g1: cannot read its answer (java.lang.ArrayIndexOutOfBoundsException ...)` and the real members' results, and fail with 1 (`anAnswerThatCannotBeReadIsThatMembersFailure`; at `e4ce92b` "g1: no result within the time limit" and exit 4). R29-4: `status now`, a malformed `add-peer` argument or one without `=`, a `remove-peer` argument with `=`, `repair-log --dry-run yes`, a stray word after `export-seed` and `restore-backup` exit 2 with the usage and write nothing (`unexpectedWordsAndMalformedPeerArgumentsExitWithTwo`; at `eefe6d5` `status now` ran with 0, add-peer threw with 1). R31-2: `export-seed` of a stopped node whose `db/` (1:5) is behind its snapshot (4:40) prints index 40 and that the snapshot was restored first; for a node of another group it exits 2 naming the group directory found and `--group-id`, and with `--group-id` exports at 40 (`exportSeedRestoresASnapshotAheadOfTheDatabase`; index 5 at `9309dc4`). R32-2: a mistyped `--group-id` next to another group's directory exits 2 naming both and writes nothing (`exportSeedRefusesAGroupIdWithoutRaftState`; 0 at `74271ac`). R32-1: a damaged snapshot at 40 (a digest that matches nothing) exits 1 naming index 40 and `--without-damaged-snapshot`, writing nothing; with the flag it exports index 5 with 0 and stdout says `damaged snapshot at index 40` (`exportSeedRefusesADamagedSnapshotUnlessToldTo`; 0 at `74271ac`). R32-3: `adminChecksumAndBackupThenExportSeedAndRestoreBackup` waits until every member has applied `CHECKSUM | BACKUP` before `checksum` (a flake before: the third member could lack the genesis). R32-4: a follower whose applied version lacks `CHECKSUM | BACKUP` answers first (the others behind 400 ms links): it prints `<id> refused`, and `checksum` and `backup` succeed through the next member with every member `DONE` (`aLaggingMemberThatAnswersFirstDoesNotStopTheCommand`); two fake members (`RefusingMember`) refusing for the version in the older wording and a silent one: exit 1 naming both, each asked once (`everyMemberRefusingForItsVersionFailsNamingThem`); both red at `d90c2c6` (exit 1 on the first refusal); another FAILED_PRECONDITION still ends the command at once, the next member not asked (`anotherRefusalStillEndsTheCommandAtOnce`, green there too). R32-5: with `raft/<group>/in_use.lock` held, `export-seed` exits 1 with `repair-log`'s message, keeps the `db.restore-tmp` it found and writes no target; released, it exports at 40 (`exportSeedRefusesWhileTheNodeHoldsItsRaftLock`; exit 0 and the directory deleted at `d90c2c6`). The storage shim fails writes and syncs under its directory only while triggered, and only the kinds the trigger names. `LogTerms` reads the terms of a stopped cluster's logs, and they match. |
| `WatchdogThreadsTest` | R25-W, in process: a follower whose snapshot storage monitor is held (as by a checkpoint waiting for a hung disk), or whose Raft log write lock is held (`LogWorkerGate.holdWriteLock`, as by an append waiting for a stopped log worker), still pauses its API once its disk probe hangs, and the API gate thread is not blocked; a leader whose storage monitor is held and whose log worker stops still steps down (`leader.log.stepdowns`) and another node leads. Each fails on one shared maintenance thread. Round 25 review: the API gate, the Raft watch, the node monitor, the `raft/member` write and the lag sampler of every node each throw `OutOfMemoryError` twice (`ReplicatedNode.FAILURES_FOR_TESTING`; the second only from a run after the first), and the API still pauses on a hung disk afterwards (fails without the `Throwable` catches: every task stops after one Error); while the log write lock is held the exported `monitor.refresh_age_seconds` grows past 3 s, and falls once it is released; a start that fails after the background tasks are scheduled, with the Raft watch unable to stop them, leaves none of their threads, nor the disk probe's or the metrics', and the node starts again (fails without `stopBackgroundTasks` in the constructor). |
| `LeaderStallTest` | The rule of `LeaderStall`; follower next indices, which Ratis gives without ids, pair with ids only when the follower list read before and after them is the same; the bound that grows and comes back, and the rule of a log that stops, which a real leader with a stopped log worker follows within seconds. A graceful stop of the leader with one node down, followed by a restart at once, loses no reply and costs no step down. With one node down, the reply to a new leader's first entry is lost on a link, after the entry reached the follower's disk: nothing commits and the leader stays, until it steps down and a write gets through. The same loss after the first commit heals with the next write. A leader behind in applying does not step down. Round 25 review: a decision that the caller cannot act on (`canStepDown` false, a step-down still being started) is not taken, for both rules: no step-down while it cannot start, one at the next check that can, and the next bound is twice the first, not four times (fails when the flag is ignored). |
| `RollingRestartTest` | Graceful restart of every node, twice, under load. No failed operation. A leader restart stops writes for about 200–300 ms. |
| `MembershipTest` | A node started with `--join` replaces a lost leader, however long before its `add-peer` it started and after a restart in between, catches up from a snapshot, and restarts with or without the option. A node removed while it runs exits. Seeded bootstrap works. An empty node added right after a seeded start receives the seed through the snapshot taken after genesis, and never replays the seeded `Genesis`. A node of a seeded cluster is replaced whichever node leads. A lost leader or follower, seeded or not, replaced by `remove-peer` then `add-peer`, after `admin` refused the one-step replacement (exit 1, no `receive setConfiguration` on any member); a leader that runs replaced by `add-peer` then `remove-peer`. `add-peer` refuses a node with a log of its own and a node started without `--join`, and fails at once without its node; a change without a majority ends at its limit; `--join` after a start without it is refused; `admin snapshot`; a removed node that a supervisor starts again exits with 4 and never prints `READY` again (DD-30, R4-6), while the others serve on; the warning of a new cluster whose nodes all have `--join`. |
| `ColdRestartTest` | SIGKILL of all three nodes under load, 5 cycles, plus a 2-of-3 restart. Nothing acknowledged is lost, and replicas are identical. |
| `RandomFaultTest` | A seeded random schedule of kills, restarts, pauses, isolations, partitions, one-way and single-link cuts, slow links (20–200 ms each way with as much jitter), throttled links (64–512 KiB/s), blackholes (bytes held, nothing reset, as with dropped packets), flapping links (isolated and healed every 50–400 ms) and client-side faults (a node's client link that drops every answer, so committed writes go unacknowledged while the leader runs on, or that is cut). Register histories, with read-modify-write updates and hot counters in them, are linearizable; the transactional bank-transfer invariant holds; list-append transactions (every read a `getForUpdate`) show no anomaly at all to `ListAppendChecker` and lose no committed append; every node's persisted Raft term (`raft-meta`) never decreases. Adding faults changed which schedule a given seed produces. |
| `OverlappingRandomFaultTest`, `ClockSkewRandomFaultTest` | `RandomFaultTest` with a second fault starting while the first holds (for example a pause plus a one-way cut, or two kills: the majority lost, then back), where availability is reported but not asserted and every safety check stays; and with node 1's wall clock 90 s ahead and node 2's 90 s behind. |
| `ClockSkewTest` (in-process) | Leaders whose clocks are two hours behind or ahead of the others: the replicated clock never goes back and is the same on every replica, and session writes across the leader changes apply at most once (exactly once when acknowledged). |
| `ListAppendCheckerTest` | The Elle-style checker finds and classifies known anomalous histories (G0, G1a, G1b, G1c, G-single, G2 as allowed by snapshot isolation, incompatible orders, duplicates, garbage, internal reads), treats an unknown transaction as committed only once it was seen, and passes random serial histories. |
| `RaftLogRepairTest` | Segments written by Ratis' own output stream, torn at sector boundaries: torn entries are dropped, and Ratis then opens and appends to the log; anything else is refused and left unchanged: a damaged complete entry, a size that hides a valid entry, a gap or overlap between segments, a problem in a closed segment; a dry run changes nothing. A leader, and a follower that never acknowledged its last entry, refuse to start with a torn entry and rejoin after `repair-log`. So does a leader that loses a whole open segment after closed segments, a snapshot and a purge. Two or three nodes torn by one power cut, in entries no quorum confirmed, repair and elect a leader among themselves with every acknowledged write. No test interrupts a real device: the torn bytes are written by the test. |
| `ClusterOpsTest` (in-process) | A cluster created by this version has the full cluster version, and advertises sessions. A checksum proposed through a follower gives the same digest and key count on every replica, and every replica finds the others equal; the Raft port answers it. A backup asked through the Raft port is written by every replica at the same log index, and the port answers it by name and index; the same request proposed again is done again, another request with the name fails without changing the first result, and a query by another name or index never returns an older result. Restored with `restore-backup` into a new seeded cluster, through a data directory holding an old `replica-ops.log` and `backups/` (moved aside), it holds every key written before it and none written after. The monitor's gauges (one leader, a Raft log with bytes) and the replication timers reach each node's database registry, and the core meters carry the node tag. R26-1: the restored cluster has three nodes, each restored with `restore-backup` from its own copy of the backup as the runbook says; all three apply the seeded genesis with the same seed digest (at `31e06ad` n1 and n2 stopped at genesis, "started from another seed"). R26 review: n2 instead gets a copy of the `db/` that n0's restore left, with `db.seed-digest`, the runbook's other variant, and the cluster still starts; a copy of that `db/` without `db.seed-digest` records another digest (the runbook said "with or without"). |
| `CapabilityUpgradeTest` (in-process) | A version-0 cluster with one member of an older version is raised only to what that member has (no sessions advertised, no pipelining, also after another check); once that member is upgraded, the version reaches everything, sessions are advertised, the current term still runs without pipelining and the next leader pipelines. |
| `ChecksumTest` (in-process) | A row written past the log on one replica makes its digest and key count differ, and every replica reports the divergence (`checksum.mismatches`, the index gauge). A column with the same name and no data but another merge operator on one replica digests differently (schemas are in the digest). With checks every 6 s and the leader stopped 3 s after one, the next check comes about 6 s after the last one, not 6 s after the change. A check applied again (its recorded time moved 3 hours back while the node was down) keeps that time, also across the next start. |
| `ReplicaOpsTest` | The new entries round-trip and are applied alone; backup names; the wire form of results; capability names; digests that tell key parts and values apart; series and gauges exported cumulative, with their kind, and moved to a new registry; the lag sampling period; Raft log bytes counted from segment files only; the alarm's start, repeat and end; the verdicts of `admin checksum` and `admin backup` (OK only with a final result from every member), which failed proposals are retried, the capabilities `add-peer` requires (current and pending), the replication meters following a configured node name, and a blank `node-name` meaning the node id. R26-2: a backup whose `backup.properties` is empty, as a power loss before the sync left it, is taken again (`aBackupWhosePropertiesAreEmptyIsTakenAgain`; FAILED "exists already, at index null" at `31e06ad`). R29-1: a backup whose checkpoint fails leaves neither `db.tmp` nor `backups/<name>` (`aBackupWhoseCheckpointFailsLeavesNothingBehind`), one whose `backup.properties` cannot be written leaves no `db/` without it, and a backup that exists already is kept when another of its name is refused (`aBackupWhosePropertiesCannotBeWrittenLeavesNoDatabase`); both left their directory at `eefe6d5` (the failure is injected by `NodeStore.checkpointBuiltForTesting`). |
| Component tests | Codecs (including fuzzing), `ConflictTracker` (checked against a reference model, also with a capped history; pruning in bounded steps, and appends and checks while a long prune runs), `ReadBarrier` (deposed and isolated leaders never pass; a second round starts while the first is in flight; a read index needs no apply), `LeaderHintRouter` (which calls go to the hinted leader, and when the hint is taken or dropped; every service method classified; two failing addresses each back off), the idle timeout of forwarded streams, `NodeStore`/`CheckpointStorage` (partial installs deleted once replaced, at the start, and after 10 minutes without a change; R24-1: `getLatestSnapshot` deletes none of them and does not wait for the install lock while another thread holds it, `latestSnapshotNeverScansTheInstallations` and `latestSnapshotDoesNotWaitForTheInstallLock`, both red before the fix; R24-2: the cluster id is answered from memory, a value written to the database behind the store is not seen until it opens again, `clusterIdIsAnsweredFromMemory`, and a closed store still refuses with "Closed", `clusterIdRefusesAClosedStore`, red after R24-2 cached it; the install cleanup task keeps running after a run that threw an `OutOfMemoryError`, `InstallCleanupTaskTest`, red while it caught only `RuntimeException`), `LeaderForwarder` (leader lookups still waiting fail on close; see also "Targeted faults and component tests" below), fault proxy (cuts, delay and jitter keeping byte order, bandwidth caps, blackholes that release held bytes, dropped answers resetting their connections when healed), linearizability checker. In the core project, `GrpcProxyCloseTest`: closes that answer late, on a proxy `GrpcServer`, take no worker, are not cancelled with their caller, and hold up shutdown after their caller left; the close of an update whose caller left while it opened is asynchronous too; a retry of either close, with no caller left, is no late failure. `GrpcProxyWritesUnorderedTest`: a later put to a key, sent without awaiting an earlier one, reaches the connection and completes while the earlier one still runs (writes not awaited have no relative order, R23-A). |

Run them all (about 20 minutes on a real disk):

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

### Fault harness

`LinkProxy` sits on every directed node-to-node link (`Network`) and, since round 3, in front of every node's API
port for the test clients (`ProcessCluster.clientLink`). Besides cutting a link (a TCP reset, so failures are seen
at once), it can delay each chunk by a latency plus jitter, cap the bandwidth per direction, blackhole the link, or
drop the answers. Bytes are never reordered or lost in the middle of a stream: TCP does neither, so reordering below
it cannot be observed by the nodes, and a blackhole holds bytes (as TCP retransmissions would) and releases them on
heal unless an endpoint's own timeout closed the connection first. A blackhole holds a half-close (FIN) as well, so
no connection end gets through it either. Held and queued bytes always follow the link's current state, not the one
they were read under: after a heal a throttled or delayed backlog (up to 4 MiB per direction and connection) is
forwarded at once, so a healed node is not still effectively throttled when the next fault starts. A connection that
did lose answers is reset when the link heals, since its stream is garbage after the gap. The client link makes the "committed but unanswered"
case reproducible without killing the leader: the node commits and applies, only the reply is lost.

**Why `LinkProxy` and not `tc netem` (RT-50).** A netem harness, in a user and network namespace (`unshare -rn`,
which gives `CAP_NET_ADMIN` inside it without root) or with iptables on real hosts, would add what happens below and
inside the kernel's TCP: packets lost, reordered, duplicated or corrupted at the IP level, and the kernel's own
reaction to them (retransmission timeouts that double from 200 ms, congestion control shrinking the window,
`tcp_retries2` closing a connection after about 15 minutes of unanswered retransmissions, SYN retries on a connect
to a host that does not answer, keepalive probes). It is not built, because none of that reaches the nodes as
anything but what `LinkProxy` already produces:

- Loss, reordering, duplication and corruption are repaired by TCP (the checksum drops a corrupted segment, which
  is then lost and resent). The nodes see only bytes that arrive later, in order, and a lower throughput: the
  proxy's latency with jitter and its bandwidth cap. A burst of loss long enough to matter is a stall, which is a
  blackhole that heals.
- A connection that the kernel gives up on fails like a cut link (a reset, or an error on the next read or write),
  after a time far beyond every timeout the nodes and clients set themselves (election 1-2 s, the request timeout,
  gRPC keepalive, the forwarding deadlines). Those timeouts are what decides the outcome, and the blackhole
  exercises them: a blackholed link keeps its connections open and moves nothing, as a dead network does.
- What the nodes rely on is TCP's contract (order, no loss, no duplication inside a connection), never its timing.
  Raft's safety does not depend on time, and the time-bound claims (the availability bound of the random fault
  suites) are tested under latency, jitter, throttling, blackholes and flapping links.

Two differences remain, both judged not worth a privileged harness. A blackholed proxy accepts a new connection
(the TCP handshake with the proxy succeeds) and then holds it, where a real dead host leaves the connect hanging on
SYN retries: a client then waits for the HTTP/2 preface instead of the connect, and both end on the same gRPC
deadline or keepalive. And the gradual shape of a lossy link (throughput collapsing under congestion control,
stalls of a few RTOs) is approximated by fixed jitter and a fixed cap, not reproduced. netem in a namespace would
also need a one-time step by root on each machine that runs the tests: the kernel does not load a module on request
from inside a user namespace, so `sch_netem` must be loaded beforehand (`modprobe sch_netem`, or a line in
`/etc/modules-load.d/`). After that, `tc qdisc ... netem` inside `unshare -rn` should need no privilege. On the
development machine `tc` in `unshare -rn` answers "Specified qdisc kind is unknown" for a reason of its own: the
running kernel (7.3.0-5) has no module tree after an upgrade without a reboot (`modinfo sch_netem` finds nothing,
while the installed 7.3.1-1 ships `sch_netem.ko.zst` and its config has `CONFIG_NET_SCH_NETEM=m`). So netem is
feasible here after a reboot and that root step (needs-user), but the argument above does not depend on it. Faults
that need several machines (a real switch, failure domains) stay with the multi-host item (RT-28).

When two faults overlap, a flapping link cuts and restores only the links of its node that no other network fault of
the step holds, so the other fault keeps its links for its whole planned hold instead of being healed by the flapper
within 400 ms. The term check is not allowed to pass vacuously: every node's `raft-meta` term must have been read at
least once, and a run that stopped the leader must have seen a term increase.

`ListAppendChecker` is a port of Elle's list-append analysis: unique appended elements recover each key's version
order from the longest read, from which it derives `ww`, `wr` and `rw` dependencies and searches for G0, G1c,
G-single and G2 cycles, plus the cycle-free anomalies (G1a, G1b, garbage, duplicates, incompatible orders, internal
reads). A list append is implemented by the workload as `getForUpdate` and put of the whole list, so every append
also reads; that only adds constraints. Real-time order (strict serializability) is left to the register
linearizability checks.

A client link refuses connections while its node is down (`LinkProxy.refuseWhileTargetDown`), as the node's own
closed port would. Before this, the proxy accepted the connection and then reset it, which a client cannot tell from
a reset after its request was forwarded, so `ClusterClient` recorded such a write as unknown instead of failed: the
histories stayed correct but weaker. Now `ProcessCluster` closes the link's listening socket before it kills or stops
the node and binds the same port again (picked below the ephemeral range, like the node ports) once the node accepts
connections; a node that exits by itself is noticed by the first connection that finds it down, which is still reset,
and only that one. A kill right after a restart can ask for a new watch while the previous watcher, which just bound
the port, is still marked running; that request is counted, and the watcher starts another one as it ends, so the
link never stays refusing while its node is up (`LinkProxyTest.aWatchRequestedWhileTheLastWatcherEndsIsNotLost`).

Storage faults need no root: `faultfs.c` (a test resource, built by `FaultFs` with `gcc` in the test's directory; a
test is skipped without `gcc`, or fails with `-Drockserver.test.requireFaultFs=true`, as in CI) is preloaded into a node's JVM with `LD_PRELOAD`. While a trigger file next to the
node's data directory exists, every `write`, `pwrite`, `writev`, `pwritev`, `fsync`, `fdatasync`, `sync_file_range`
and `fallocate` of a file under that data directory fails with the error the trigger names (`EIO` or `ENOSPC`), on
every call or only on the writes (a full disk) or only on the syncs (a disk that fails a flush), and changes nothing.
It finds the file through `/proc/self/fd`, reads the trigger at most every 2 ms, and leaves every other file alone, so
the node's logs and sockets work. The JDK's file channels, Ratis' log and RocksDB (a JNI library calling libc through
its PLT) all go through it; writes inside libc itself (stdio), through `mmap`, `io_uring` or a raw `syscall` do not,
and opening, renaming or deleting files never fails (on a real full disk the first write to a new file fails, which
the shim reproduces). `/proc/self/fd` gives canonical paths, so the faulty directory must be one too: `FaultFs`
passes it canonical (its deepest existing ancestor through `toRealPath`, the rest normalized), since a node's data
directory does not exist before its first start and the CI temp directory is reached through `..`; the shim also
resolves it with `realpath` itself, at load or at the first faulted call once it exists. The trigger `HANG` (with the
same operation filter) makes those calls block instead, without failing, until the trigger goes or names an error: a
hung disk (R3-G2a-3). `FaultFsTest` checks it from a small JVM, also with a directory created after the JVM started
and named through `..`, and with a sync that stays blocked under `HANG sync` while writes there and syncs elsewhere go
on, and ends once the trigger goes. Faults below the file system (`dm-error`,
`dm-flakey`, a disk that lies about fsync) need root and stay with the user.

The process tests poll with wall-clock bounds (RT-41): they wait for a condition (`READY` in the log, a leader in
every node's group info, a write acknowledged after the fault) every 50 to 100 ms, up to a bound far above the
expected time, and fail with the bound in the message. The bounds: 60 s per phase (`ScenarioSupport.PHASE_TIMEOUT`,
also each node's start), 20 s of client retries, 60 s for a node to exit, a few minutes for whole tests. Raft's
safety does not depend on time, so these bounds only decide when a test gives up: a slower machine makes a test
slower, not wrong. Where the claim itself is about time it is stated as one: the availability bound of the random
fault suites (15 s without an acknowledged write) under the slow, throttled, blackholed and flapping links,
`MembershipFaultTest`'s 15 s join window, and `LaggingInstallTest`'s 3 minutes for the leader to purge past the
lagging node. Those are the known risks of flakiness, unverified until the process suites run on the runner; a test
that turns out flaky gets an event to wait for instead of a longer bound (as `aRemovedNodeDoesNotStartAgain` did).
Timing is measured with `System.nanoTime`, never the wall clock, so a clock step does not end or extend a wait.

### Targeted faults and component tests

The suites above kill and cut at moments the test picks, but a kill there lands at a random point of the protocol.
`CrashPoints` names the points that matter, and a test can halt a node exactly there: the node prints `CRASH POINT
<name>` and calls `Runtime.halt` with exit code 99, which skips shutdown hooks, flushes and the leadership hand-off,
as a SIGKILL at that instruction would. The points are: on the leader, in `preAppendTransaction` before Ratis appends
a proposal (`BEFORE_APPEND`); a group of committed entries on the apply thread before it is applied (`BEFORE_APPLY`,
on the leader the entries a majority holds and no client has an answer for); after the group was committed to the
local database and before its replies and applied index are published (`AFTER_APPLY_BEFORE_REPLY`); as the node
learns it won an election (`ELECTED`); a snapshot's checkpoint built and synced but not yet published
(`SNAPSHOT_BUILT`); a snapshot published before Ratis learns of it (`SNAPSHOT_TAKEN`); a snapshot received from the
leader before the database is closed for it (`INSTALL_RECEIVED`), and with the old database closed and the new one not
restored yet (`INSTALL_RESTORING`); a joint configuration applied (`JOINT_CONFIGURATION_APPLIED`). Nothing is armed in
production: each point costs one volatile read. The process tests start their nodes through `CrashPointMain`, which
arms the point named in a file next to the node's data directory, so a test arms the node that holds a role at that
moment (the current leader, the follower that won). The point "appended on the leader but not replicated" needs no
hook: the test cuts the leader's links, lets clients write to it directly, and kills it.

| Test | What it proves |
|---|---|
| `ProtocolPointKillTest` (processes) | Rounds drawn from a seeded generator (`-Drockserver.test.seed`, `-Drockserver.test.protocolKillRounds`, 12 by default): the leader halted before an append, between commit and apply, between apply and replies; any node halted at those points or in the middle of a snapshot; the winner of an election halted as it learns it won (both followers armed, the leader killed). After each round writes resume and the node restarts and catches up; at the end every acknowledged write is readable, no write answered "not applied" took effect, and the replicas are identical. A leader killed with entries in its log that no follower has loses only unacknowledged writes, and drops them from its log when it rejoins: its links are cut, the followers' last entries are read 100 ms later (still of the leader's term, so before any election), and the leader is killed only once its last entry is past both. At the end, with every node stopped, the log files are read (`LogTerms`, as a starting node reads them): wherever two logs hold an index they hold the same term (log matching), and every entry the killed leader had appended past the followers' last one is gone from its log or replaced by a later term (the truncation itself, not only its effect on the data). |
| `LaggingInstallTest` (processes) | An existing member that lags past what the leader still holds catches up through a snapshot install. With the production settings no node purges past a member that lags (see "Log retention"), so the nodes run with a test-only setting (`rockserver.test.purgeUpToSnapshot`, set through `CrashPointMain`) that purges up to each snapshot. The lagging follower is cut off, and the link heals only once the leader's log, read from its segment files, starts past the follower's last entry. Variants: the lagging member halted after receiving the snapshot (`INSTALL_RECEIVED`) and while restoring it (`INSTALL_RESTORING`) restarts and catches up; the sending leader killed as soon as the lagging member starts receiving files leaves it to the next leader. The test fails if the lagging member caught up without an install. The leader-kill variant stays timed by the receiver's first file, without a crash point in the sender (R3-G4b-3): Ratis' snapshot sender (`GrpcLogAppender` reading the snapshot files) calls nothing of the state machine between the chunks, so a hook there means patching Ratis. What the kill does to the receiver, which is what Rockserver code handles, is pinned exactly by `INSTALL_RECEIVED` and `INSTALL_RESTORING` on the receiving side, and a sender that dies between chunks only breaks the stream, which a receiver sees the same way at any chunk; the timed kill lands somewhere in the transfer, which is the one case left, and prints where. |
| `HungDiskTest` (processes) | R3-G2a-3 (DD-9): the shim's `HANG sync` on the leader and on a follower. The node does not exit, stops listening on its API port once its disk probe has not ended for the request timeout (3 s here), and the other two keep acknowledging writes; a leader on that disk is replaced (its log stops flushing, `LeaderStall`). Once the trigger goes, the node listens again and catches up; every acknowledged write is readable and the replicas are identical. Skipped without `gcc`. Written in round 6 and never run, so it runs only with `-Drockserver.test.hungDisk=true`, not in the nightly job: open are whether a leader whose log fsync hangs steps down within the phase timeout (while it still says it is the leader, `ScenarioSupport.leaderOrNull` sees two leaders and waits; each node's group-info query has a 1 s deadline, so a hung node does not block it), and whether teardown is clean. It joins the nightly job after one passing run on the user's machine. `DiskProbeTest` checks the API pause in process, with a test hook that holds one node's probe (`DiskProbe.HANGS_FOR_TESTING`): that node stops listening, a client given every address goes on writing, and it listens again once the probe ends. |
| `DiskFaultTest` (processes) | A full disk and failing fsyncs, with the shim above preloaded into every node: ENOSPC on every write, and EIO on every fsync, of the leader and of a follower (and EIO on everything, of a follower). The node stops as a failed replica with exit code 3, the other two keep acknowledging writes, and once the trigger is gone the node starts again (after `repair-log` if Ratis refuses a torn tail of its open segment; safe here, since the write or fsync of that entry failed, so this node never acknowledged it) and catches up; every acknowledged write is readable and the replicas are identical. A write is acknowledged only once a majority fsynced it, so the node whose fsync failed never counted. DD-7h: a follower's disk starts failing right after a SIGCONT, and a SIGTERM follows either as soon as its log shows the failure (`hit exception`, `replica failed to`, `Raft closed`) or after 0-30 ms; whenever the failure showed before the signal the exit code is 3, never the 143 of a graceful stop (the JVM's status after SIGTERM), and at least one round must signal a node that was failing and still alive. A disk error that first shows during a graceful stop (the stop's own flush hits the failed disk) may exit 143: the stop was asked for, nothing acknowledged depends on that flush (an entry is acknowledged only once a majority fsynced it to its Raft log, and the database is rebuilt from the log), and the next start on that disk fails at its first write. Skipped without `gcc`. |
| `PublicApiRecoveryTest` (processes) | Salvaged from the old public-quorum suite, through the public client (`GrpcConnection` with every node's address, sessions on). RT-70: the client link to the leader drops the reply of `deleteColumnIfExists` and of `uploadMergeOperator`; once applied, another client creates a column of the same name (or uploads another version), then the link is reset, or the leader is SIGKILLed: the client sends the request again under the same id and gets the first answer (`true`, the first version), and the new column stays, so the deletion ran once. The registry deduplicates uploads by content, so a second apply would answer the first version too; the nodes run with the registry's log at info, and no node may log the upload as applied again (`already exists as version`). RT-72: a transaction opened through a follower is refused at commit after its leader's SIGKILL, as safe to run again, its writes stay absent seconds later (never rebased onto the new leader), and the same transaction run again commits. |
| `MembershipFaultTest` (processes) | Node 4 joins under writes and a node stops during the joint configuration. Halted where it applies the committed joint configuration: the leader (node 4 ends a member), or node 4 (the others complete the change without it, it rejoins after a restart). Killed while the joint configuration is appended and not committed: the old followers are paused before `add-peer` (long election timeouts keep the leader in place), so the leader catches node 4 up and appends the joint configuration, which cannot commit; the leader, or node 4, is killed once its own configuration shows the joint one, then the followers resume. The configuration ends plain, old or new, and node 4 can be added afterwards. The configuration never stays joint, no acknowledged write is lost, all members end identical. |
| `SlowFollowerDiskTest` (in-process) | One follower's log worker sleeps 180 ms of every 200 ms (`LogWorkerGate.pause`) under 20 s of writes: the leader keeps its term, writes are acknowledged in every second, the follower falls behind (hundreds of entries) and catches up once its disk is fast again. Memory: the nodes use 1 MiB log segments, and the leader's Ratis log cache, read directly, must stay within Ratis' cached-segment limit plus three segments (9 MiB) while the load writes far more (about 200 MiB; the test fails below four times the bound, where the check could not fail). The appender's buffers are not measured. |
| `LogWorkerGateContractTest` | The Ratis-private members that `LogWorkerGate` uses still exist, and on a real single node a blocked worker holds a write back and a pause delays one: a Ratis upgrade that breaks the gate fails here by name. |
| `LongStreamSemanticsTest` (in-process) | A range stream outside a transaction is one point-in-time view of the leader's database, taken when its cursor opens: writes acknowledged while it runs (updates, deletes, inserts ahead of the cursor) are not in it, also when a follower forwards the stream. |
| `ProposerCommitRetryTest` | Commits refused before the append for lack of room (Ratis' pending-request limit, or the commit lane's attempt permits held by commits Ratis has not answered) go back to the queue: they succeed once there is room, are not sent while there is none, and fail at their caller's deadline as not applied (`READ_DEADLINE_EXCEEDED`), never as unknown, by an expiry timer that fires when `take()` would fail them, at the deadline less the least time left (880 ms for a 1 s caller deadline, R22-F2), also when every commit slot is held by unanswered commits by the time a refused commit is back in the queue (R21-1), and a commit without a caller deadline fails so at the request timeout (R21-4); two commits taking turns in the one free slot, each refused again and again, hold one expiry timer each on the control threads, not one per requeue (R21-2). |
| `LeaderForwarderTest` | Leader lookups: another node is answered at once, this node only as a ready leader; a change wakes waiting lookups well before the poll; a leader that appears between the first check and the registration (its notification finding nobody) is seen by the check after the registration; no leader within the request timeout is a retryable overload. Connections: four per peer, used in turn; an invalidated connection is replaced while the other slots stay, and a stale invalidation does not drop a newer one; a refused port fails at once (the probe) and the peer is not tried again for 200 ms; a listed member that moved its API port is asked again after a failed connection, and reached at the new port (R18-2); a listed member that fails once at the same port keeps its answered address, for hints and the next connection, and the address version stays (R18 review finding 1); sixteen requests waiting on four failing connections ask the member at most once per failed connection, not once per request (2 to 5 asks in all, counting the first connect: an answer that comes back before a later connection fails lets that one ask again), and connections made together to a stale member share one ask (R19-2). A numbered commit that could not be sent to the leader (its port refuses) by the end of the request timeout fails with `SERVER_OVERLOADED` flagged not applied, not `UPDATE_RETRY`; one without a request id still gets `UPDATE_RETRY` (R25-O). |
| `LeaderHintCacheTest` | The leader hint cache of `ReplicatedConnection.leaderApiAddress`: the same leader and address version are not looked up again; a new version is, and follows a moved port; another leader is looked up at the same version; an unknown address is cached as null until the version changes; an address recorded during a lookup (the version raised meanwhile) is looked up at the next call. |
| `ClusterCheckTest`, `CommitOutcomesPathsTest`, `LocalCommandsTest`, `ReplicatedWritesTest`, `UpdateProtocolPathsTest` | The classes split out of `ReplicatedConnection`: the cluster check tells each follower once per term, retries a silent one only after its pause and asks again in a new term; commit outcomes (a second copy marked in the same term, settling by outcome, pruning of marks, every kind of record, a gone session, flagged not applied, every kind of barrier failure and a failing store answering "unknown", never "not applied", R25-O); who leads and gets the session of its term; column creation and its check, and batches split, refused when numbered and too large, reported when partly applied; the update protocol's answers for transactions and updates that are gone (also for copies of numbered commits, and "unknown" for such a copy, of a commit or of the write that ends an update, whose barrier fails with `NotLeader`), staged writes carrying a request id, empty commits, the end of an update and a failed update. |
| `MergeErrorKindsTest`, `CrashPointsTest`, `ConflictTrackerPropertyTest` | A merge operator's `AssertionError` or `LinkageError` is a recorded `MERGE_OPERATOR_FAILED` result and the replica goes on, a `StackOverflowError` stops it. The arming rules of the crash points. `ConflictTracker` against a model of the log, with jqwik (failures shrink to the shortest sequence of steps): a write after a transaction's snapshot always conflicts, also with dropped entries, pruning and tiny history caps, and without those it conflicts exactly then. |

`FollowerReadsTest` also has a deterministic version of its stack test: reads made from each other's completions,
50,000 deep, while the reader finishes its batch; the recursion that the loop replaced overflowed there.

The crash-point, install and membership process tests are written and compile; like the other process suites they
run only in the nightly job below (and they were not yet run on the development machine, for lack of memory).

**Kept as they are, with reasons.**

- *Tests that pin mechanisms rather than behavior* (exact RPC counts, timer counts, pins in `LeaderStallTest`,
  `LeaderSessionTest`, `ProposerQueueTest` and the like). They are component tests: their job is to hold a mechanism
  in place (a lost wake-up, a slot given back, a bound that comes back), which a behavior test sees only as a rare
  timing. Each such mechanism also has a behavior test above it (the scenario suites, `ReplicationTest`), so a
  refactor that rewrites a component rewrites its component test and keeps the behavior suites as the check. Turning
  them into behavior tests would lose exactly the cases they were written for.
- *Hooks that hold a thread at a call site* (the state machine's `applyGate` latch, `LogWorkerGate.block`, the core's
  test observers on range continuations). They depend on the call structure by design: they stop a thread at one step to make an interleaving
  happen every time. Where they reach into Ratis-private code, `LogWorkerGateContractTest` now fails by name when
  Ratis changes; the crash points above halt instead of holding a thread, so they cannot deadlock a test.
- *Codecs stay on seeded random fuzzing* (`CodecEquivalenceTest`, `LogEntryCodecTest`), not jqwik: they compare whole
  random batches against the old codec, where shrinking adds little, and a failure prints its seed. jqwik is used
  where a minimal failing sequence matters: the conflict tracker's steps.

### Continuous integration

`.gitea/workflows/ci.yml` runs on the Gitea instance that hosts the repository (Gitea Actions). On every push and pull
request: the core module's tests, then the distributed module's tests without the multi-process suites (they are listed
in the file, and excluded by name), on the runner's disk. Every night, and on demand: the whole distributed suite, the
process suites included, twice (pipelined updates off and on), keeping the surefire reports and the node logs (the
job sets `junit.jupiter.tempdir.cleanup.mode.default=ON_SUCCESS`, so a failed test's `@TempDir`, where its nodes
write their logs, is not deleted; only there, so that failed runs on a developer's disk do not pile up). The
runner needs Linux (the process tests give every link its own loopback address), Docker for the
`maven:3.9-eclipse-temurin-27` image, access to the Maven repositories of the root `pom.xml`, and several GiB of
memory for the nightly job; the storage-fault tests (`FaultFsTest`, `DiskFaultTest`) also need `gcc`: both jobs
install it (`apt-get install gcc libc6-dev`) and run with `-Drockserver.test.requireFaultFs=true`, so a missing
`gcc` fails them instead of skipping them (a developer's run without the property still skips). Nothing was pushed: the runner and the schedule are for the repository owner to enable.

## Left before a first deployment

0. **Pipelined updates** stay off until the benchmark on hot keys (see [performance.md](performance.md)). The review
   of the Yotsuba call sites is done, on Yotsuba's `replicated-rockserver` branch (commit `53d400ef4`, see
   [consumer-contracts.md](consumer-contracts.md)): every update path acts on a chained value only after the update
   ended well, and retries `UPDATE_RETRY` from a no-change close. One exception, argued harmless (Yotsuba
   `ffb742b77`, its workload mapping's "Side writes before the ending write"): `MessagesImpl.patchCanonicalMessage`
   writes the sender-index presence entry (an idempotent `ensure` put, never deleted) before its ending put, from a
   read that may be provisional, and the entry stays after a failed update or a rerun. The order keeps the lossy
   index a superset of the committed rows; the sender of a provisional value is one whose writer already wrote the
   same entry, and an extra entry would only cost a filtered block scan. Every other update body ends with its only
   write. Watch `update.guardRejected`, `update.chainConflict`,
   `transaction.conflict` and `transaction.chainConflict`. A leader chains only once the cluster version has pipelined
   updates (see "Cluster version"): an older node would fail on the entries of a pipelined leader. With the
   option off, entries keep the bytes they always had, and logs written before these flags stay readable.
1. **Client side.** Yotsuba's changes for this version are on its local branch `replicated-rockserver`, not merged
   and not deployed: `0427455d1` removes the dead `cdcPollBatchAsync` override and its test stubs (Yotsuba did not
   compile without it, CDC being gone from the client API), `9cb3a9d6f` builds the connection with every node
   (`database.rockserver.nodes`, the host names of the cluster's `--peer`, expected leader first; empty keeps the
   single address), the commits of item 2, and the round-5 commits `456b8f8b7` and `df8acf015` (column
   registrations retry an unknown outcome, the update cleanup closes once, in-flight limits for a cluster; below
   and in item 3), and the round-6 commits `9b69ebb15` and `6358380bc` (an embedded test that the update
   cleanup leaves no transaction open and closes each update once, the in-flight limit warning from one tested helper,
   also in the settings comments), the round-7 commits `b0f42c8bd` and `414f3109f` (that test checks double closes
   per update id and drives the cancel and overload close paths), and the round-8 commits `b8b713488` (a body's own
   error after an unconfirmed chained read runs the update again, below), `520170f65` (its review: resent
   closes, the exhausted retry's message) and `ffb742b77` (R8-5: documents the one side write before an ending put,
   see item 0; no code change), and the round-9 commits `2cb7df603`, `f4ded3df4`, `f0b80643b` and `f1a857d1b`
   (a Rockserver or gRPC error kept only as a suppressed exception also stops a rerun; a timing-free cleanup test;
   javadocs and the workload mapping match the pipelined close answers), and the round-10 commit `8cf79428e`
   (javadoc only). Left on the Yotsuba side:
   - Merge the branch, build it against a released `rockserver-core` of this version (the branch was compiled and
     tested against local builds of it; Yotsuba's pom still pins 2.0.3), and deploy it.
   - Set `database.rockserver.nodes` in the deployed settings once the cluster exists.
   - Done on the branch: the `createColumn` sites no longer retry only a raw `UNAVAILABLE` (`456b8f8b7`;
     `createColumn` is idempotent). The ones that wait in place (`CommunitiesImpl`, `FileLocationsImpl`,
     `ForumTopicsImpl` and others) retry while Rockserver is unavailable and after an unknown outcome
     (`RockserverRequests.isColumnRegistrationWaitable`), never after a deterministic failure; overload and
     timeouts still reach their caller. The bounded ones (the history archive, the old file-location column during
     its migration) use `ReactiveGuard.isRetryableIdempotentFailure`, as `SentMessageSenderIndex` already did.
   - Done on the branch: an update is closed once (`456b8f8b7`, narrowed by `df8acf015`). When the body's own
     no-change close fails with `UPDATE_RETRY` (`NoChangeCloseRetryException`, still an `UPDATE_RETRY` for the
     callers' retries), the server has already ended the update, so the cleanup does not close it a second time
     and the double-close warning is gone. Every other failure, including an `UPDATE_RETRY` from the put or delete
     that ends the update, and a cancel still close it: a single node keeps the native transaction open after a
     conflicting ending write. The cluster answers such a close quietly for an update without `--pipelined-updates`
     (`UpdateProtocol.closeFailedUpdate` rolls back a native id and answers at once, whether or not it still holds
     it), but not for a chained one. With `--pipelined-updates`, `UpdateProtocol.closeChained` answers `UPDATE_RETRY`
     whenever it cannot confirm the chained read (see "Chained close"), and `RockserverUpdateTransactions` turns
     that into a `NoChangeCloseRetryException`. The cleanup close of a chained update therefore answers
     `UPDATE_RETRY`:
     - when the leader no longer holds the update: the commit of any chained ending write removes it, whatever its
       error (`UpdateProtocol.stagedWrite`: `UPDATE_RETRY` from a chain conflict or a failed guard, a deadline or
       `OUTCOME_UNKNOWN`, `NOT_LEADER`, a cancel), and so does the refusal of an ending write whose base commit
       failed; the same when the update was opened under a previous leader, the close reaches a new one, or its
       lease ended;
     - when the leader still holds it but the commit it read from failed or is not known to have applied within
       the close's wait: a body that throws its own error, or is cancelled, after reading a chained value whose
       predecessor failed.

     The client cannot tell the two cases apart from the answer. Before round 8 the cleanup logged every such
     answer ("Failed to close RocksDB update transaction", at most once a minute) and dropped it, so in the second
     case the caller got the body's own error while the close said the read was not real: a body that fails because
     of what it read (a validation error on a value that was never written) has acted on a provisional value, which
     "Behavior differences from a single node" forbids. Done on the branch (round 8, `b8b713488`): the
     cleanup after a body failure (`closeAfterBodyFailure`) looks at what failed. If the body failed with its own
     error, with no Rockserver error, timeout, cancel, gRPC status, rejected or circuit-broken call in its causes
     (`isBodyOwnFailure`), and the close answers `UPDATE_RETRY`, the update fails with an
     `UnconfirmedReadRetryException` instead: an `UPDATE_RETRY` with the body's error suppressed in it, which the
     callers' optimistic retries (`optimisticUpdateRetry`, the messages' own) run again from a new read. If the new
     read is real and the body fails again, its close succeeds and the caller gets the body's error; if the
     retries run out, the retry exhausted error carries it. That last case is left as it is, on purpose: when the
     base commit does not settle for all 17 attempts, no read was confirmed, so the body's error (say
     `CommunitiesImpl`'s `IllegalArgumentException`, "Community patch changed source or identity") may be about a
     value that never existed, and the caller gets an `UPDATE_RETRY` exhaustion (Reactor's retry exhausted error,
     its cause the `UnconfirmedReadRetryException`) with the body's error in the cause's suppressed list and,
     since the review commit `520170f65`, in its message, so a log line that prints the cause still shows it. It is
     bounded, and no caller branches on the body's error type: the importers' `patchCommunity` calls (bot API,
     TDLib) pass the error on, and the web API's mapping of `IllegalArgumentException` to HTTP 400 never reaches an
     update body (the web API makes no update calls). A resent close is the other place where the cluster's answer
     is not quiet: `retryCloseFailedUpdateOnOverload` sends a close again after `SERVER_OVERLOADED`, which a
     forwarding follower also answers when its stream to the leader broke after the close ran. For a chained
     update the resend finds nothing held and answers `UPDATE_RETRY`; after a body's own error that is one extra
     run (the body fails again on a real read, that close succeeds and the caller gets its error;
     `RockserverUpdateTransactionsTest.aLostCloseAnswerAfterABodyErrorRunsTheUpdateOnceMore`), and nothing was
     written. Any other failure keeps the old behaviour (the close's
     answer is logged, the caller gets the body's failure). That covers the first case: there the body's failure is
     the ending write's own Rockserver error, and an unknown outcome or a deadline must never become a rerun, since
     the write may have applied. The rule relies on one contract of Yotsuba's bodies: the call with the update id
     (one put or delete, or the no-change close) is their last step that can fail, so a body's own error always
     comes before its ending write and the update wrote nothing. A cancel, by the caller or a caller's timeout,
     goes to the cancel close, which only closes and never reruns. Rockserver needs no change for this: the close
     already answers `UPDATE_RETRY` whenever the read is not confirmed, and answering these closes quietly would be
     wrong (a clean close of an unconfirmed chained read would confirm it). Nothing is left open in any of these
     cases, since a chained update has no native transaction and the leader removes it before it answers, and
     nothing is written through the update; the one side write of a body before its ending write (the sender-index
     presence entry, item 0) is an idempotent `ensure` put outside the update, which stays, harmlessly.
     Under `--pipelined-updates` the remaining warning follows leader changes, timeouts and refused chained writes:
     it counts failed chained updates, and a pipelined cluster under contention makes some on purpose. Tested in
     Yotsuba with fakes (`RockserverUpdateTransactionsTest`) and against an embedded single node whose close is made
     to answer `UPDATE_RETRY` (`RockserverUpdateCleanupEmbeddedTest`), not against a running replicated server. The
     same commit fixes the javadoc of `RockserverUpdateTransactions.updateAlreadyEnded`, which said the cluster
     answers a close of a dropped update quietly: it now says so only without `--pipelined-updates`, and
     `520170f65` does the same for `retryCloseFailedUpdateOnOverload` and the workload mapping's paragraph on
     resent closes.

   On the Rockserver side, the core client's automatic gRPC retry of reads waits for its backoff before it sends a
   read to the next node: 50 ms, then growing, on a connection with several addresses (the next node is another one,
   so there is nothing to wait for), 0.5 s with one address (`it.cavallium.rockserver.grpc.client.initial-retry-backoff`
   overrides both).
2. **Exactly-once for client retries** (client sessions, see "Writes, exactly once"). The contract for a caller, and
   where Yotsuba's branch meets it:
   - A caller must not retry `OUTCOME_UNKNOWN` as a new write: the core client already resent the write until its
     deadline, and a new copy could apply it twice. It treats it as a failed call whose effect is unknown (for a
     counter or an update, re-read before deciding), or gives the call a longer deadline. `SESSION_EXPIRED` reaches
     it only when sessions kept expiring until the deadline, never after a copy that may have applied, and then with
     the not-applied flag (`isNotApplied()`, the `rockserver-applied: no` trailer; also set when the refused copy was
     the only one sent, R4-1): safe to retry. `SERVER_OVERLOADED` and `READ_DEADLINE_EXCEEDED` from replicated writes
     mean "not applied" and stay safe to retry. Yotsuba: `2cb001186` (its `ReactiveGuard` keeps an unknown outcome or
     a `MERGE_OPERATOR_FAILED` out of every generic retry classification; a call site whose operation is idempotent,
     a put of a fixed value, a delete that does not return presence, a column creation or a read, opts back in with
     `ReactiveGuard.isRetryableIdempotentFailure` and does retry an unknown outcome, never a
     `MERGE_OPERATOR_FAILED`: `SentMessageSenderIndex`'s column setup and, since `456b8f8b7`, every column
     registration, see item 1; an audit of double applies checks those sites, not only the guard),
     `5e648b895` (reads the structured flag instead of the message).
   - A local timeout around a write must not be shorter than the write's client deadline: the client keeps resending
     a write its caller cancelled, so an outer retry after a shorter local timeout could apply the write twice.
     Yotsuba: `53d400ef4` sizes its local timeouts from the client's deadline.
   - Writes staged in a transaction are not numbered, by design (see "Writes staged in a transaction"): a caller
     whose staged write or commit has an unknown outcome rolls the transaction back and runs it again, never resends
     the write. Yotsuba: `5e648b895` reruns its two transactions (invite evidence, monitors) that way, and `c73697eae`
     sends the search update of an invite commit whose outcome was unknown when the rerun finds the change already
     stored (R4-2). The Rust client numbers and resends its writes outside transactions mostly the same way as the
     Java client, and leaves staged writes unnumbered too; the differences (a streamed multi write over
     `NUMBERED_MULTI_MAX_BYTES` is sent unnumbered and only once, and dropping the future is the cancel) are in
     "The Rust client".
   - A client needs every node's address (item 1): a single address has nowhere to resend to while its node is
     down, so its writes end with `OUTCOME_UNKNOWN` at their deadline.
   - Every node must run this version before clients use sessions: an older node cannot read a version 3 batch,
     a stamp or a `RegisterClient` entry. The cluster version enforces it from this version on (see "Cluster
     version"): restart the clients once the upgrade is done, since they read it when they connect, or start them
     with `-Dit.cavallium.rockserver.grpc.client.session-writes=false` until it is done. Sessions need no Yotsuba
     change otherwise: the client turns them on by itself.
3. **Capacity.** See [performance.md](performance.md) for the measurements on one PC and the load benchmark.
   Still to measure on the production machines and data: their fsync latency, snapshot creation and install
   time at 171 GB and at the multi-TB datasets, and log retention in bytes. Yotsuba's in-flight request limits
   are raised for a cluster on its branch (since `456b8f8b7` and `df8acf015`); the value for production is still to measure
   with the Yotsuba mix (see performance.md).

   *Upgrade note, Yotsuba's in-flight limits* (behaviour as of `6358380bc`; the warning and the settings comments
   come from `SettingsImpl.rockserverInFlightLimitMigration` since `9b69ebb15`). `database.rockserver.maxConcurrentReadRequests` and
   `maxConcurrentWriteRequests` now default to 0, automatic: 256 each with one node, 1024 each when
   `database.rockserver.nodes` lists several. A positive value is used as is, with one exception: with several
   nodes an explicit 256 also means automatic, so it becomes 1024. Earlier versions wrote 256, their default, into
   every settings file, and a cluster would otherwise keep the single-node limit without anyone choosing it. The
   change is silent apart from one warning per limit at startup (`SettingsImpl`, "... treated as automatic,
   1024"). So with several nodes the limit 256 itself cannot be configured: set 0 to keep the automatic value
   without the warning, or another value (255, 257, ...) to fix the limit. A negative value is rejected at startup.
   With one node nothing changes. Yotsuba documents the same rule in `docs/rockserver-workload-mapping.md`
   ("In-flight limits").
4. **Write failover after a frozen leader.** When the leader freezes (SIGSTOP, a long GC pause, a hung disk),
   writes resume about 4 s later in the tests, although the election takes about 1 s there and reads resume
   sooner. The cause is Ratis' client closing the channel to the frozen leader, which waits a hard-coded 3 s
   (`GrpcUtil.shutdownManagedChannel`). Crashes and graceful restarts are not affected (in the tests, failover
   about 1 s, hand-off about 250 ms). The proposer now resets its connection to a peer on a thread of its own
   (`Proposer.resetConnection`): Ratis takes the peer's proxy out of its map at once, and only the closing of the
   old channel waits, so the failed attempt is resent to the next leader without the 3 s. The map's lock is not held
   while a channel closes, so other peers never waited for it; `RatisContractTest` pins the 3 s wait itself. Not
   measured yet with a frozen leader (`PauseTest`, the benchmark): the figures above are from before. These figures
   are for 400:800 ms election timeouts. With the defaults (`--election-timeout-ms 1000:2000`,
   `--request-timeout-ms 10000`), the benchmark saw about 3 s without a successful operation after a SIGKILL
   of the leader (performance.md), and an attempt sent to a frozen leader waits 4 s instead of 3 s before it
   is resent.
5. **Snapshot transfer at TB scale.** Ratis installs restart from scratch after a failure, and are not rate
   limited. If that proves too slow, switch to install notifications plus a resumable pull of checkpoint
   files.
6. **Operations.**
   - RocksDB's Java binding extracts its 17 MB native library into the temporary directory at every start, and a
     killed JVM leaves it behind: on a test machine 2,099 such files filled a `/tmp` quota, and new nodes then could
     not start. So `run` extracts the library into `DATA-DIR/tmp` itself
     (`NativeLibraryLoader.loadLibrary(dir)`, before any RocksDB class loads; `RocksDB.loadLibrary()` then finds it
     loaded), and deletes the copies that earlier runs left there (a loaded library stays usable when its file is
     deleted). Setting `java.io.tmpdir` while the JVM runs would not do this: the JDK reads the property once, at
     start-up, and `File.createTempFile`, which RocksDB uses when no directory is given, keeps the start-up value.
     `run` still sets the property to `DATA-DIR/tmp`, for libraries that read it later (Netty's native transports);
     the replica's own config file is written next to the database, not in the temporary directory. A given
     `-Djava.io.tmpdir` outside the data directory is warned about. Until R27 a given `-Djava.io.tmpdir` skipped all
     of this, and the runbooks' alias gave one (`/srv/rockserver/tmp`): every unclean exit (halt 3/4/5,
     `ExitOnOutOfMemoryError`, SIGKILL) left a copy there again. Now the library goes to `DATA-DIR/tmp`, cleaned at
     every start, whatever `-Djava.io.tmpdir` says; only the property is left as given. A given directory inside the
     data directory is the node's own and its old copies are deleted too; one outside it may be shared by other nodes
     of the host, one of which could be between extracting and loading its copy, so the node never deletes there. The
     alias no longer gives the option. `StartupChecksTest.anExplicitTemporaryDirectoryGetsNoCopyOfTheRocksDbLibrary`.
     "Inside the data directory" is decided on real paths (`toRealPath`): a `DATA-DIR/tmp`, or a given directory
     under the data directory, that is a symlink to a shared directory such as `/tmp` counts as outside and is not
     cleaned, with a warning, since a copy there may be another JVM's between extracting and `System.load`
     (`aSymlinkedTemporaryDirectoryIsNotCleaned`).
   - `admin status` asks every member at once, once each (a client without retries) and for at most 3 s
     (`PEER_ANSWER_TIMEOUT`), and prints them in the `--peer` order. Until R27 it asked them one after the other
     through the shared admin client, which retries every 500 ms until the admin time limit (10 minutes by default):
     one dead member listed first held the status for 10 minutes, exactly when the runbooks tell to look at it.
     `CommandLineTest.statusAnswersAtOnceWhenMembersAreDown` (a socket that accepts and never answers, and a closed
     port). The 3 s bound the answers, not the command: after printing, closing the client shuts its channels down
     (Ratis's `GrpcUtil.shutdownManagedChannel`, per member in parallel: up to 3 s graceful, then 2 s after
     `shutdownNow`), so with a member whose connect is still pending (an unroutable address) the command may exit
     about 5 s after its output, about 8 s after it started. Still bounded, and the output comes first; an unroutable
     member is not tested, since a blackholed address behaves differently per host and network.
   - Load [alerts/grafana-alert-rules.yaml](alerts/grafana-alert-rules.yaml) and set its placeholders, and follow
     [runbooks.md](runbooks.md): seeding, node replacement, backups and restore, checksums, upgrades (see "Metrics and
     alerts" and "Checksums and backups"). Test a restore of a backup once on the production data, and time a checksum
     there before running one periodically.
   - Placement on separate hosts, each node's data directory on one file system (see Disk layout).
   - The same Rockserver config file on every node (a difference is logged, see Shape).
7. **Known limits, decided.**
   - `deleteColumn` no longer waits for the leader's iterators: apply closes the iterators of the column first
     (`EmbeddedDB.closeIteratorsOfColumn`; only the leader has any), and their next call gets `UPDATE_RETRY`, then
     a missing column. Nor for its streams (DD-7a): every range stream and raw scan a node serves is tracked by column
     (`ColumnStreams`, around `LocalCommands.localStream`), and the apply of the deletion cancels the column's streams
     first, as a client's cancel would, which releases their use of the column; each one ends with
     `COLUMN_NOT_FOUND`, never as a complete range, and one that starts while the deletion runs fails at once. Before,
     a slow client or a long scan held the deletion, and with it the leader's whole apply, until the stream ended or
     its deadline passed. A stream that acquired its use of the column in the instant between the cancel and the
     deletion's own closing of admission is not cancelled; it is subscribed before then, so its start would have to
     race the apply thread within microseconds, and it still ends at its deadline.
   - The result records that a stopped proposer process had in flight (at or above its floor) are never
     deleted; those below its floor are deleted when each replica next starts. Kept as is: a crash leaves at most
     the records of the requests that process had in flight, which its byte admission and attempt permits bound
     (typically hundreds of small records), and client writes with sessions, the default of the core client, are
     recorded under their session and deleted when it expires. A cleanup entry would need the id of the dead
     proposer, which is random per process and not kept: the design, if crashes turn out to be frequent, is to keep
     the id in the data directory and have the restarted node propose its retirement.
   - A result larger than 17 MiB (for example a multi-key previous value) is replaced by an
     `INVALID_REQUEST` failure: the write is applied, but its caller does not get the result. Kept: the result
     travels in Ratis' reply (at most 18 MiB on the wire) and is recorded for copies of the write, so a larger one
     would need a channel of its own from the leader's apply to the caller's node, and a record kept outside the
     database. A caller that needs previous values of that size splits the write.
   - The log kept behind a snapshot is counted in entries, not bytes. Ratis 3.3.1 keeps the last N entries of the
     log at every purge (`purgePreservation`, read once at the start and counted from the end of the log) and has no
     byte limit of its own; N is this project's choice, set to `--snapshot-every`. A count derived from a byte
     budget, min(`--snapshot-every`, budget / 16 MiB), would bound the worst case, but only through the largest
     entry: a 4 GiB budget gives 256 entries, which with the usual entries of a few KiB to a few hundred KiB is
     seconds of writes, and a node restarted during a deploy would then need a snapshot install instead of catching
     up from the log, which is what the margin is for. Ratis cannot size the count from the entries actually
     written, so the margin stays a count tied to the snapshot interval. A deployment whose entries are near the
     16 MiB maximum bounds the retained log by lowering `--snapshot-every` (at most that many entries of that size,
     besides what the members that are behind hold back), at the cost of more frequent checkpoints, each a
     100-300 ms pause of the apply. A failing snapshot is no longer tried at every applied entry: after a failure,
     `takeSnapshot` answers "no snapshot" (which Ratis passes over without an error or a purge) for 1 s, doubling with
     each failure in a row up to a minute; the snapshots by bytes wait for the same delay.
   - Ratis' retry cache has no size bound (DD-12), only an expiry: `max(6 s, 4 × --election-timeout-ms max)`, 8 s
     with the defaults (`RaftSettings`). Kept, because the expiry bounds it by traffic: it holds one entry per Ratis
     invocation applied in the last 8 s, on every replica, and an invocation is one proposer batch, not one write. Each
     entry keeps the batch's reply, the encoded results of its writes (`ResultCodec.encodeAll`: a few bytes per write,
     plus the previous values a write asked for, at most 17 MiB per result). So the cache holds about 8 s of the
     results the cluster answers: with a status per write, at the production peaks of `performance.md` (`put`,
     update commits and `putMulti` together, about 30k writes/s), about 30k × 8 × ~20 bytes, under 5 MiB, plus a
     few hundred bytes of Ratis per batch. Only
     writes that ask for large previous values make it large, and those cost the same bytes again as replies to their
     clients: 100 MB/s of previous values keeps about 800 MB on every replica, which such a workload must count in its
     heap. A size bound is not offered by Ratis 3.3.1, and a shorter expiry would miss the resends it exists for (a
     resend of a copy still pending, see `RaftSettings`); Rockserver deduplicates requests itself, so the cache is an
     optimization, not what makes resends safe. To report upstream with the others (below).
   - Bulk bandwidth is not profiled: 1 MiB `putMulti` calls reach 103 MiB/s replicated against 183 MiB/s on one
     node (performance.md, "Bandwidth"). Open, needs the user: whether Yotsuba's bulk writers come near this rate is
     not measured. Its bulk writers on the `replicated-rockserver` branch are: the `FileLocationsImpl` migration of
     the old column (pages of the old column, then one `putAsync` per entry with concurrency 1, keys only, empty
     values: bound by one replicated write's latency, a few hundred small writes a second, far below the limit);
     `putMultiAsync` of new file locations (keys only, one call per request batch); `HistoryArchive` chunk writes
     (one `putMultiAsync` per observation batch, rows of up to 32 observations each) and `PhotoRecoveryArchive`
     (one `putMultiAsync` of profile-photo observations), both paced by the observations coming in; and the dump
     plugin's import of exported chats, which goes message by message through the messages layer (a read and an
     update per message), not through a bulk call. None of them writes at the pace of a source scan with large
     values. Production (the single-node Rockserver, `rocksdb_operation_timer`, 7 days to 2026-11-06) peaks at
     about 6,000 `Put` and 5,300 `PutMulti` calls a second (1-minute rates); the byte counters have no recent data,
     so the bytes per call are not known. At those call rates the 103 MiB/s limit is reached only if the calls
     average about 9 KiB, which keys-only file locations do not and history chunks could only with large rows. To
     close it: measure the bytes of `PutMulti` in production (or of `HistoryArchive` rows) and compare with
     103 MiB/s; before a bulk import or migration of the multi-TB datasets, profile the path
     (`load-bench.sh --only BULK_PUT --jfr profile`): a replicated byte is written to the Raft log of every node and
     sent to two followers, on top of the work of a single node.
   - Iterator calls after a leader change get `UPDATE_RETRY` (see "Leader changes").
   - The snapshot taken right after a seeded genesis is attempted once, on the leader only.
   - A SIGTERM that races a replica failure exits with 3: the shutdown hook closes the node, then waits until it
     stopped (also when the failure's own stop was already closing it, for at most twice the database's 1-minute
     close timeout), so that the JVM does not exit while RocksDB still closes, and halts with 3 if the replica had
     failed meanwhile. A signal that comes after Ratis closed the division by itself (its log worker after a disk
     error) but before the Raft watch noticed is the failure's stop too: `close()` checks the division. Still
     open until `DiskFaultTest` runs: `ShutdownSignalTest` checks both in process (a failed apply, and a division
     closed right before the signal); `DiskFaultTest` sends a real SIGTERM to a node whose disk is failing (written,
     not run yet: it is a process suite).
   - A SIGTERM during the start of `run` (R28-4): the shutdown hook is registered before `ReplicatedNode.start`, waits
     for the start to end, and stops the node it returned as above; a start that fails (also because the shutdown
     refuses what it registers) closed what it opened, and the hook has nothing to stop. Before, the hook was
     registered after the start, so a stop while a large node opened its database or checked its membership exited
     the JVM with RocksDB and Ratis open, whose destructors can hang it until SIGKILL. Any `Throwable` of the start
     exits with 1 (R28-5; an `Error` such as `NoClassDefFoundError` from a half-replaced jar escaped `main`, and the
     threads the start left behind could keep a JVM without a node alive, which a supervisor does not restart).
     A node that the signal is already stopping when its start returns does not print `READY` (R28 review 4: it
     did, so a harness or supervisor waiting for `READY` saw a ready node that went away at once); a signal after
     that check is one after `READY`.
     `ShutdownSignalTest.aSignalDuringTheStartWaitsForItAndStopsTheNode` and `anErrorDuringTheStartExitsWithOne`
     run `startAndServe` with the start, the exit, the hook registration and the halt replaced.
   - A process killed while its database opens (R29-2) leaves the config file it opens it with, which is deleted only
     once the open returned. It was `Files.createTempFile` in the database's parent (`DATA-DIR`), one random name per
     start, never removed: a node in a crash loop killed during each WAL replay left one per start. Now the file has
     one name per database (`rockserver-replica-for-<db dir name>.conf`, `NodeStore.replicaConfigFile`), so a
     killed open leaves at most one, which the next open replaces and deletes, and the open deletes the randomly
     named `rockserver-replica-<digits>.conf` that released versions left in that directory
     (`NodeStore.deleteLeftConfigs`). Only that pattern, which no per-database name matches: the `for-` makes the
     part between the prefix and `.conf` contain letters whatever the directory is called, so the config of an
     `export-seed` target next to the source, or of another database in the same directory, stays. Two processes
     opening the same database at once would share the name, but they would fail on RocksDB's lock anyway. Not
     checked with a real kill: `NodeStoreTest` creates the files a killed open left.
   - R30-1: round 29 first named the file `rockserver-replica-<db dir name>.conf`, which the legacy pattern matched
     when the directory's name is all digits. `export-seed` writes its target's config before it opens the source,
     and that open deleted it as a leftover, so `export-seed --target DATA-DIR/20261108` (a date-named seed next to
     `db/`, kept there so its files hard-link) failed on every run with a config error ("File does not exist") from
     the copy's open, deleting the target; an all-digit database opened while another one in the same directory
     opened would have lost its config the same way. The `for-` infix makes the two name sets disjoint; the legacy
     pattern still matches every name released versions made (`Files.createTempFile` puts digits only between prefix
     and suffix). Round 29 was not released, so no `rockserver-replica-<db dir name>.conf` is left in the field (a
     build of it that a killed open interrupted could leave one, which nothing deletes: harmless, delete by hand).
     The other opens (the node's start, a snapshot install, `restore-backup`) only run this cleanup and write and
     delete their own file, so none can delete another database's config any more; `repair-log` reads the Raft log
     only and opens no database. `NodeStoreTest.anExportIntoAnAllDigitSiblingOfTheSourceWorks`
     and `theConfigOfAnAllDigitDatabaseIsNotTakenForALeftover` (both red at `ebe9ae0`, before the fix).
   - R31-1: that config is the resolved Rockserver config, with `database.metrics.influx.token` and every
     `${?ENV}` substitution in plain text. R29-2's `Files.createTempFile` made it rw------- (0600) by chance; the
     fixed name was written with `Files.writeString`, which gives the umask's mode (0644 under the usual 0022), so
     any local user could read the InfluxDB token while a database opened, and for good after a killed open. Both
     the replica's config and the `export-seed` target's are now written by `NodeStore.writeOwnerOnly`: an existing
     file (a killed open's, possibly 0644) is deleted, the new one is created rw------- and its mode set again (the
     umask could only have removed bits), then written; on a file system without POSIX permissions the default
     applies. RocksDB's own files and the Raft storage keep the umask's mode: keep `DATA-DIR` at 0700 or 0750 for
     those. `NodeStoreTest.theReplicaConfigIsOwnerOnly` (a new file, and one replacing a 0644 leftover) and
     `theExportTargetConfigIsOwnerOnly` (the target's config seen while the source is copied, replacing a 0664
     leftover) were red at `9309dc4` (`rw-r--r--`, `rw-rw-r--`).
   - R31-2: `export-seed` opened `db/` and copied it without looking at the snapshots in `raft/<group>/sm`. After a
     crash between Ratis renaming a received snapshot into place (it may reset the log then) and its restore into
     `db/` (`INSTALL_RECEIVED` to `INSTALL_RESTORING`), `db/` is behind the snapshot; a start repairs it
     (`openStore`), but an operator who exports every node of a cluster that lost its majority without starting them
     got the older index printed and could pick another replica, losing the entries only that snapshot held. Now
     `export-seed` opens `db/` (R32-5 below: a running node is refused first, by its group's lock), then runs
     `CheckpointStorage.init(Path)` on `raft/<group>/sm`, the recovery and digest verification of a start without
     Ratis, and `NodeStore.restoreSnapshotIfAhead`, which `openStore` now calls too, so both restore through the same
     code. A snapshot damaged by the crash fails a start (`DamagedSnapshotException`); for the last resort it is
     reported (`ExportedSeed.damagedSnapshot`, a `WARNING` on stderr) and `db/` is exported as it is. The CLI takes
     `--group-id` (default the default group, like `run` and `repair-log`); without it, a `raft/` that holds only
     other groups' directories is refused with 2 rather than exported without its snapshot.
     `CheckpointStorageTest.exportSeedRestoresAnInstalledSnapshotAheadOfTheDatabase` (an unverified received
     snapshot at 4:40 over `db/` at 1:5: verified, restored, exported at 40) and
     `exportSeedWithADamagedInstalledSnapshotExportsTheDatabaseAndSaysSo`, and
     `CommandLineTest.exportSeedRestoresASnapshotAheadOfTheDatabase` (through `main`, with a local snapshot, and the
     `--group-id` refusal), were red at `9309dc4` (index 5 exported).
   - R32-1 (review of R31-2): with a damaged snapshot `export-seed` warned on stderr, exported `db/` and exited 0, so
     a script reading the exit code and stdout's `applied up to index N` saw no sign that the replica had held more;
     and the reason printed was `DamagedSnapshotException`'s, whose advice (`rebuild it from the other members`)
     fits a start, not the no-quorum case. Now it refuses by default: `NodeStore.DamagedSnapshotNotExportedException`
     (the snapshot's index, from its `snapshot-<term>-<index>` name since its properties file may be what was
     damaged, and the database's applied position), thrown after the open and before the checkpoint, so nothing is
     written; the CLI prints `Not exported: ...` and exits 1 (not 2: the command line is right, and the same one
     works on another replica; not 5, which means a newer version). `--without-damaged-snapshot` keeps the old
     behaviour, and stdout then says `without its damaged snapshot at index N`. The digest check and the refusal to
     restore a damaged snapshot were already right and are unchanged.
     `CheckpointStorageTest.exportSeedWithADamagedInstalledSnapshotRefusesUnlessToldToExportTheDatabase` and
     `CommandLineTest.exportSeedRefusesADamagedSnapshotUnlessToldTo` were red at `74271ac` (exported, exit 0).
   - R32-2 (review of R31-2): an explicit `--group-id` with no `raft/<group>` directory (a typo) skipped the
     `raft/ holds only other groups` refusal, and `exportSeedInto` skips the snapshot check for a missing directory,
     so `db/` was exported at its possibly older index without a word. The refusal (exit 2, naming the groups found)
     now applies whether the group was given or not; a `raft/` with no group at all is still exported from `db/`.
     `CommandLineTest.exportSeedRefusesAGroupIdWithoutRaftState` was red at `74271ac` (exit 0).
   - R32-3: `CommandLineTest.adminChecksumAndBackupThenExportSeedAndRestoreBackup` ran `admin checksum` right after
     puts that commit on a majority, so its third member could still lack the genesis (cluster version none), answer
     first and refuse: a flake (r31's first run). It now waits, as its siblings do, until every member has applied
     `CHECKSUM | BACKUP`. With R32-4 the wait is no longer needed for the command to pass, and keeps the test about the
     commands.
   - R32-4: a member that lags behind the leader refused `checksum` and `backup` by the cluster version it had
     applied, and `admin` ended with 1 on that first refusal (see "Checksums and backups"). Now the refusal says the
     member may lag (`... as far as <id> knows: it has applied cluster version X (the leader raises it within about
     10 s of the last member's upgrade, and a member that lags behind the leader applies it later)`; r32 said only
     the lag), `proposeOp` asks the next member that answered, and fails with 1
     only when every member that answered refuses for the version. `CommandLineTest.aLaggingMemberThatAnswersFirstDoesNotStopTheCommand`
     (a follower's applied version stubbed without `CHECKSUM | BACKUP`, the others behind 400 ms links, so it answers
     first: both commands exit 0, every member `DONE`) and `everyMemberRefusingForItsVersionFailsNamingThem` (two fake
     members refusing in the older wording, a silent one: exit 1 naming both and their version, each asked once,
     not the silent one) were red at `d90c2c6` (exit 1 `n1 refused`; the second member never asked);
     `anotherRefusalStillEndsTheCommandAtOnce` (another FAILED_PRECONDITION: exit 1, the next member not asked) guards
     the rest and was green there too.
   - R32 review 1-3: r32's runbook, this section and the all-refused message listed the end of a rolling upgrade
     among the cases of a member that lags, as if `admin` went on with the next member; but until the leader's next
     version check every member refuses, the leader too, and nobody lags. The message now says `either the leader has
     not raised the cluster version yet (...): run the command again in about 10 s; or those that answered lag behind
     the leader: run it again once they caught up`, and the docs keep the two apart. The javadoc of
     `everyMemberRefusingForItsVersionFailsNamingThem` said the silent member is not waited for; it is, until the 3 s
     capabilities question times out (the test takes about 3.3 s), and the runbook now says what that means for a
     member slower than that. `proposeOp`'s javadoc lists every refusal that ends at once with 1 (any
     FAILED_PRECONDITION but the version's, INVALID_ARGUMENT, UNIMPLEMENTED). The test was red at `573820e`
     (`some lag behind the leader`).
   - R32-5 (review of R31-2): R31-2 said `export-seed`'s open of `db/` fails while the node runs, before the Raft
     directory is touched. But `NodeStore.open` runs `RestoreDirs.recover()` (deleting `<db>.restore-tmp`, swapping in
     `<db>.restore-ready`) and `deleteLeftConfigs` before RocksDB takes its LOCK, and a running node holds no LOCK on
     `db/` while it installs a snapshot (`installSnapshot`: close, `NodeStore.restore`, open; `restoreSnapshotIfAhead`
     at a start likewise), so an export run on a node thought stopped could delete the directory the node was
     copying into (the node then stops for good) or finish its swap, then run `CheckpointStorage.init` on its live
     `sm/`. Now `NodeStore.exportSeed`, when the group directory exists, takes its `in_use.lock` (Ratis' lock, held
     for the node's lifetime) with `tryLock` first, as `repair-log` does, and keeps it to the end; held (by another
     process, or a channel of this JVM), it throws `NodeStore.NodeRunningException` with `repair-log`'s message
     (`RaftLogRepair.nodeRunning`), and the CLI prints `Not exported: ...` and exits 1, nothing of the node touched and
     nothing written. The lock file stays, as a killed node leaves it. A node with no `raft/<group>` yet (one that has
     never started) is exported without the lock, as before. `CommandLineTest.exportSeedRefusesWhileTheNodeHoldsItsRaftLock`
     (the lock held, a `db.restore-tmp` with a file in it: exit 1 with the message, the file kept, no target; then,
     released, exported at 40) was red at `d90c2c6` (exit 0, the directory deleted).
   - The exit status after a start is the same on both paths (R6-2): the main thread's exit is a `System.exit`,
     which runs the shutdown hook, and the hook ends with `Runtime.halt`. The hook used to halt with 3 for any
     failure, which replaced the 4 of a node that found out, while it waited outside the cluster, that it was
     removed, and the 5 of a replica that met a newer cluster version in its log or in an installed snapshot, so
     `RestartPreventExitStatus=4 5` never saw them after a start. Both now take the status from the failure that
     stopped the node (`ReplicatedServerMain.failedExitStatus`, looking through its causes: 4, 5, else 3).
     `ShutdownSignalTest.aFailureWithItsOwnStatusExitsWithItAlsoThroughTheShutdownHook` runs the main thread's part
     (`serve`) with the exit, the hook registration and the halt replaced, then the hook as `System.exit` would, for 4
     and 5 (it fails with 3 from the hook without the change). A real process exit is checked by
     `MembershipTest.aRemovedNodeThatRestartsExitsWithFour` only for the start path, and that suite has not run.
8. **Upstream.** Report the Ratis ReadIndex reuse bug, and switch back to Ratis ReadIndex once it is fixed
   upstream. Also to report (drafts in the round-3 notes), none of them blocking:
   - `AwaitForSignal.signal()` loses a wake given while nobody waits (R7-1, see "Commit heartbeats" above): it sets
     the flag that it replaces (`signaled.getAndSet(new AtomicBoolean()).set(true)`), and `await` reads the new one.
     So a `LogAppender` woken between building its requests and its wait in `GrpcLogAppender.mayWait` waits its
     whole wait, up to a heartbeat interval, and the follower learns a commit index late. Worked around by
     `CommitHeartbeats.WakeSignal` (returned by `Appender.getEventAwaitForSignal`), which keeps such a wake for the
     next wait when it would send a commit heartbeat; `RatisContractTest` drives the lost-wake moment through a test
     hook. On a Ratis that fixes it, `WakeSignal` can go.
   - `GrpcUtil.shutdownManagedChannel` waits 3 s, then 2 s more, on the thread that resets a peer's proxy; worked
     around by `Proposer.resetConnection`, which resets on a thread of its own and waits only until Ratis took the
     failed proxy out of its map (so a resend to the same peer gets a new channel, not the one being closed).
   - Client proxies can get a metric registry of `null` (R8-4): `RatisMetrics.create` shares a registry that already
     exists without counting the new holder, and `RefCountingMap.put` counts a new one only after putting it in the
     map, so the close of an older proxy to the same peer (the same `MessageMetrics` name, `clientId->peer`) can
     remove it in between. Every call of that proxy then throws a `NullPointerException` at its start, which is not a
     connection error, so Ratis never replaces it; its close throws too, and `PeerProxyMap` catches only an
     `IOException` of a proxy's close. `Proposer.resetConnection` closes old proxies while new ones are made, so a
     node met it after a leader loss (`RustClientLiveTest`: the stop failed with "Failed to stop node"). Worked around
     in `Proposer`: an attempt whose call throws a runtime exception replaces the proxy (`resetBrokenProxy`), and a
     runtime failure of the client's close is logged (`closeClient`); Ratis has closed the other proxies and its
     threads by then. `RatisContractTest` pins the race. A reset replaces only the proxy its attempt was sent on
     (read before the call): attempts in flight on one broken proxy fail together, and each would otherwise reset the
     replacement made by the one before, closing a healthy channel under its resends, which then fail `UNAVAILABLE`
     and may end with an unknown outcome (R9-2). Ratis' `PeerProxyMap` has no compare-and-remove, so the check and the
     reset are two steps, checked again just before the reset: a reset by another attempt between them can still
     replace one healthy proxy, once. `ProposerResetTest` fails without the check. The proxy is read just before
     `rpc.sendRequest`, which reads it from the map once more (`GrpcClientRpc.sendRequest` calls
     `PeerProxyMap.getProxy`; its overload that takes a proxy is private), so a reset by another attempt between the
     two reads sends the attempt on the replacement while it holds the old proxy (R10-5). If the call fails there, the
     reset keeps the replacement and the resend to that peer can fail on it once more; the next attempt reads it first
     and replaces it. The cost is bounded by one more failed attempt and its resend, and the window is the few
     instructions between the two reads (`GrpcClientRpc.sendRequest` reads the map as its first step, checked in the
     3.3.1 bytecode). No test sends an attempt into that window; `ProposerResetTest` only pins the reset rule the bound
     relies on. Accepted (round 13): Ratis 3.3.1 offers no public way to send on a given proxy
     (`GrpcClientRpc.sendRequest(RaftClientRequest, GrpcClientProtocolClient)` is private,
     `GrpcClientProtocolClient.unorderedWithTimeout` is package-private, and its only public members are `close()` and
     `getTarget()`, checked with javap); reflection or a helper in Ratis' package would tie the Proposer to Ratis
     internals, and a per-peer lock held across the call would hold resets back for an attempt timeout, which
     `resetConnection` exists to avoid.
   - The same race on the server side (R9-1): every `GrpcLogAppender` of a division makes a `GrpcServerMetrics` with
     one shared registry name (member id, `ratis_grpc`, `log_appender`), and its stop unregisters it. An appender made
     while another one stops (`LeaderStateImpl.restart` is stop then add, and is not synchronized; a removed follower;
     the appenders of an earlier term) can get `null`, and its constructor throws a `NullPointerException` at its
     first counter; the old appender of that follower is already gone, so the follower would get no appends or
     heartbeats until the leadership changes. Worked around in `CommitHeartbeats.Factory.newLogAppender`: an appender
     whose constructor throws a `NullPointerException` from a Ratis metrics class is made again, up to 5 times (the
     constructor has no effect before its metrics, and the next try makes a new registry; any other
     `NullPointerException` is thrown at once, except one without a stack trace: HotSpot's fast throw
     (`OmitStackTraceInFastThrow`) or `-XX:-StackTraceInThrowable` can drop the trace, and an exception from elsewhere
     only fails all 5 attempts). `AppenderMetricsRaceTest` pins the race against Ratis' own
     `GrpcServerMetrics` (a few thousand generations) and checks the retry. Other Ratis objects with shared metric
     names were checked in the 3.3.1 bytecode: the server's `MetricServerInterceptor` (one per server), and a
     division's `LogAppenderMetrics` (per leader term), `RaftServerMetricsImpl`, `LeaderElectionMetrics`,
     `SegmentedRaftLogMetrics` and `StateMachineMetrics` are made and unregistered once per server, division or term,
     one after the other on the division's own transitions, never two of one name at once; `GrpcServerProtocolClient`
     (the leader's channel to a follower) has no metric interceptor. The `admin` command's `RaftClient` builds its
     proxies once, in a process of its own: a broken proxy there fails the command, which is run again.
   - The two `ResourceUnavailableException`s of the pending-request limit (before and after the append) differ only
     by their message: a typed refusal before the append would let `Proposer` stop matching text.
   - The retry cache has an expiry but no size bound (`RaftServerConfigKeys.RetryCache`): a bound in entries or
     bytes would cap its heap under replies with large values (see "Known limits", DD-12).
   - `GrpcLogAppender.getWaitTimeMs` waits 0 ms while it has no stream to a follower, a spin until the next
     heartbeat after a stream reset (found in a review of the code). Not seen in practice: with a follower down,
     the leader's appender threads for it used 0.0% of a core over 3 s (`RatisContractTest`), since the retry policy
     sleeps between failed tries. A reset with the follower alive lasts until its next request.
   - Decided not to report: in `GrpcLogAppender`, the response handler of a stream that was already replaced still
     resets the current one when its `onError` or `onCompleted` arrives (`resetClient` stops whatever stream is
     current, with no check that the event belongs to it; checked in the 3.3.1 bytecode). The cost is one extra
     reset: the pending requests are dropped, and the leader sends again from the follower's next index on a new
     stream. Neither safety nor the log is affected, and nothing in the tests showed it. If filed anyway, one line:
     "AppendLogResponseHandler.onError/onCompleted of a stale stream reset the current appendLogRequestObserver;
     compare the handler's stream with the current one first."
