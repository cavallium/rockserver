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
| `ReadBarrier`, `TermService` | Linearizable reads. `TermService` also answers what a node holds (`NodeState`), a follower's read index, and a node's latest configuration (for `RemovalCheck`). |
| `FollowerReads` | A follower's read indexes from the leader, for the point reads it serves itself (off by default). |
| `ClusterCheck` | A leader tells its followers which cluster it leads, and a node of another cluster stops. |
| `PeerChannels` | The gRPC channels to other members' Raft ports: one cached channel per address, with a fresh connection attempt when it failed. Each node has one shared cache for `ReplicaOps` (checksum results, capabilities) and `LeaderForwarder`'s address lookups, pruned by the maintenance thread to the members, the peers being brought in and the peer list, and closed last when the node stops. `ReadBarrier` and `FollowerReads` keep one each: they close theirs to fail their requests in flight when they close, and the barrier prunes its own to the voters of each round. `ClusterCheck` (once per follower and term), `RemovalCheck` (at start) and the `admin` commands use a one-off channel per call, on purpose: `RemovalCheck` runs once before the node exists, keeps one channel per member across its retries and closes them when it decides, so a cache would hold nothing past it; `ClusterCheck` makes one call, and after a match one configuration comparison, per follower and term, a few channels per election, and it shuts its channel to a follower of another cluster, which on the shared cache would also cut that address's calls of `ReplicaOps`; the `admin` commands run in another process. The pruning (`ReplicatedNode.dropChannelsOfFormerMembers`, every 200 ms) keeps the current and previous configuration, the followers a leader replicates to besides it (a peer being added) and the addresses of `--peer`, and shuts down any other address: `PeerChannelsPruningTest` (unit, a stub configuration) and `PeerChannelsMembershipTest` (in-process: a node joins with `--join`, gets channels on every node's shared cache, is removed with `setConfiguration`, and every node shuts its channel down within the pruning period while keeping the others'). An address that a caller of the shared cache takes from elsewhere would be rebuilt at every pruning, so its callers use only these addresses (`ReplicaOps.pollPeers`, `PeerConfig.raftAddress`). |
| `ApiGate`, `CommitHeartbeats` | Whether a node serves its API, from whether it knows a leader; the leader sending its commit index to every follower at once instead of with its next heartbeat (its own gRPC log appender, `CommitHeartbeats.Rpc`). |
| `LeaderStall` | Tells a leader that cannot commit to step down. |
| `NodeThreads` | The node's control-plane platform threads and the virtual threads of the leader's short local commands (see "Threads"). |
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
`UNAVAILABLE`, and the reads allowed by the retry policy run again on the next node. Writes are sent again by the
client with their session request id, so they apply once (see "Client sessions" under "Writes, exactly once"). Transactions and updates live on the leader, so a client that moves
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
  longer encode to their size fail as invalid (or with an unknown outcome if an earlier batch may have appended
  them) and the others go back to the queue, as never taken unless an earlier batch may have appended them, so that
  their callers' deadlines and cancellations apply again. The encoder also returns the batch's requests as decoding
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
  the first. The leader marks a numbered commit unsettled before anything can refuse it, and clears the mark only
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
  refusal (`NOT_IMPLEMENTED`, for example) ends every write waiting for it. The caller sees `OUTCOME_UNKNOWN` only when the deadline ended the copies without an
  answer. Only a `getForUpdate` that opens an update (no transaction id) makes its id one whose write is numbered.
  Closing the connection ends the writes waiting for a resend: `OUTCOME_UNKNOWN` if a copy was sent, else
  `SERVER_OVERLOADED` (not sent). A caller that cancels the future of a write stops it the same way: no copy is sent
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
  numbered by its caller and sends once; `without_client_sessions()` sends everything once, as before. The unit tests
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
change wakes the parked appender by accident. It passed in 7 runs of 5 rounds; in the two whose output was kept
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
- **Outcome of a commit.** It is submitted once, to the local Raft server. Only a refusal that is known to come
  before appending (Ratis' leader not ready, or the state machine's own refusals) means "not committed"; no room
  (no attempt permit, Ratis' full pending queue) sends it again through its queue until its caller's deadline;
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
- **Leader-side retention.** The leader keeps the previous snapshot, and any snapshot whose file list was read
  in the last 10 minutes, so a long install is not cut short by newer snapshots.
- **Disk cost of snapshots.** A retained snapshot keeps the table files it links, also after compaction
  replaced them in the database. Budget disk space for the database plus at least one older snapshot's
  compacted-away files, more while installs run. On a follower, a failed install leaves its partial copy until
  the server restarts, a newer install starts, or its files did not change for 10 minutes (the leader's own read
  lease of a snapshot being sent): a cleanup thread of its own then deletes it. It is not the maintenance thread,
  which pauses the API on a hung disk and steps down a leader whose log stopped: that thread does no disk I/O and
  takes no install lock, so a hung disk cannot hold it back. An install that is only that slow fails at
  its next chunk and Ratis starts it over.
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
  OPTIONS, WAL files) by content, without `LOCK` and the info logs. That is the only moment when copies of one seed are
  alike: opening a database writes a new MANIFEST and starts compactions at times that differ on each node. The leader
  of a seeded cluster puts its digest in `Genesis`, and a replica that recorded another one stops at genesis (exit 3)
  instead of keeping another database for good. A missing file, an extra one, a truncated copy or a copy taken at
  another moment differ; a table file damaged without a change of size does not. Copies made with `cp -a` or `rsync`
  of the stopped database agree. A replica that recorded nothing (its first start was of an older version) logs that
  it cannot check and goes on. A first start can fail after the digest is recorded but before Raft made its group
  directory (a partial copy that RocksDB refuses, a refused config, the start-up check found a cluster id): the
  operator may then copy the seed again. So the start-up check, after it opened and closed the database, also records
  the digest of the files as it left them, and a later first start keeps the recorded digest only while `db/` holds the
  files of either digest; otherwise it takes the digest again and logs it. Older `Genesis` entries, and those of clusters that are not seeded, carry no digest, and
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
  includes itself and `--join`. It waits, also across restarts, for as long as you like. Then replace the node in one
  step, leaving it out of `--peer`:

  ```bash
  ReplicatedServerMain admin --peer <the other members> add-peer <new node>
  ```

  `admin` does not read the current configuration: it sets it to exactly its own `--peer` list plus or minus one
  peer. So this command adds the new node and removes the old one together (Ratis `setConfiguration`, a joint
  configuration), and the members that stay form a majority of the old configuration and of the new one all along.
  With one node of three lost that is 2 of 3 before, during and after the change, and a failure of the new node stops
  nothing. It works the same when the lost node was the leader or a follower, seeded or not (`MembershipTest`), and
  for a node that still runs, the leader too: that node exits with 3 once it is out.

  The other way, `add-peer` with every member in `--peer` and `remove-peer` afterwards, is for growing a cluster. With
  a lost node in `--peer` it passes through 4 members with 1 lost, where all 3 others are needed: any failure of the
  new node stops writes until it returns, and an appender that dies during the catch-up leaves the cluster without a
  leader, with no way back (see "Heap"). Every member must be in `--peer`, or the ones left out are removed.

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
  place until the member answered.

  A node still running when `remove-peer` removes it closes Raft by itself and exits with 3. A removed node that a
  supervisor starts again exits with 4 before it serves anything: the configuration in its log leaves it out (usually
  the joint configuration of its removal, with the node in the old half only, since the final one goes to the members
  that stay), and it was started without `--join` or was a member once (it writes `raft/member` when a configuration
  from the log makes it one, after the leader checks of each maintenance tick and on its own, so that a failed write,
  such as on a full disk, does not keep a stalled leader in place). With systemd, `RestartPreventExitStatus=2 4 5` keeps it down (2: wrong usage or no `-XX:+ExitOnOutOfMemoryError`;
  5: a database of a newer cluster version; see "Exit codes" in runbooks.md for the exit and stop settings of the
  unit, with `SuccessExitStatus=143` for a stop by SIGTERM and `TimeoutStopSec=150` for a stop longer than systemd's
  default of 90 s). A node of a version before
  this, started with `--join`, has no `raft/member` and still stays up in the initializing state, printing `READY`; it
  says so in its log as above. One start is refused wrongly: a node that holds a joint configuration of its own
  removal that its leader never committed before losing its leadership. Only a node that was being removed gets there;
  run `remove-peer` again.

  The configuration that removes a node commits with the other members alone (a majority of the old members need not
  include it), and then the leader stops sending to it: a node that was stopped, slow or cut off during its removal
  may never receive it, and its log still holds it. (`StartupClusterTest.aRemovedNodeDoesNotStartAgain` failed about
  one run in four for this: the removed node's Raft closed before the joint configuration reached its log.) So a node
  that starts again, with a Raft directory, and was started without `--join` or was a member once, also asks the
  other members of the configuration in its log for theirs (`TermService` `Configuration` on the Raft port:
  log index, whether it is committed there, members; `RemovalCheck`). It exits with 4 once a majority of that
  configuration (itself included, which never says so) holds a committed configuration, newer than its own, without
  it. A committed configuration is never undone, so a member is never refused this way; one that is not committed
  yet, or a member still starting, is asked again every 100 ms, for at most two election timeouts (at least 1 s). A
  member that cannot be reached, or of an older version, does not count, and does not hold up the start: a node whose
  members are down starts as before. Such a node, if it then waits outside the cluster (the warning above), asks
  again each time it warns, and stops with exit code 4 once a majority says it was removed. A first start, and a
  `--join` node that never was a member, which legitimately is not in the configuration yet, skip the question.
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
  - Without the option, after a one step replacement, nothing is lost: `add-peer` says `OK` too early, but the two old
    members are a majority of both configurations, the leader stays, writes go on, and a new appender starts the
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
     is, since it belongs to the old cluster and a node without Raft state refuses it. On a stopped node,
     `ReplicatedServerMain export-seed --data-dir DIR --target NEW [--rockserver-config FILE]` writes a copy (table files
     hard-linked when `NEW` is on the same file system) without the replication metadata, cluster id, applied position,
     results and sessions, and prints the applied index and term of the replica. It builds the copy's config before
     it writes anything (an absolute `volume-path` under the source `db/` becomes the same path relative to the copy),
     so a refused config leaves no half-made target. Pick the replica with the highest one,
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
  the maintenance loop of `ReplicatedNode` watches the division, and a node whose Raft closed while it was running
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
  `LeaderStall` rule in the maintenance loop looks at the leader's log: entries to flush, and a flush index that does
  not move for 3 maximum election timeouts and at least 5 s, makes it hand over to its most up-to-date follower, or
  step down if it has none: a leader that only stepped down often won the next election with the same log. The bound
  doubles here too, and goes back when a flush takes less than its first value, so that disks that are all this slow
  do not end every leader in turn. `LeaderStallTest` stops the log worker of a leader and checks that writes resume
  on another node; how long that takes on a real disk under load has not been measured yet. The log says
  `stepping down`, and the metric `leader.log.stepdowns` counts. It does nothing on a leader with nothing to flush, and
  it cannot help once the queue is full under a heavy load, which blocks the server lock: then the heartbeats stop
  anyway, within seconds at that load. The node stays up with its hung disk, as a follower that does not answer, and
  its disk probe pauses its API (see "Routing"), so its clients move.

## Cluster version

Some features write log entries, or entry flags, that an older node cannot read: it would stop on the first one, and a
majority of such nodes would stop the cluster. So the cluster has a version (`Capabilities`): the capabilities that
every member supports, kept in the meta column of every replica. A leader asks every member of its configuration what it
supports (the `Ops` call of `TermService`, on the Raft port), every 10 s while the cluster lacks something that this
version supports, and proposes a `ClusterVersion` entry once all of them answered with more; a member that does not
answer, or that predates the question, holds the version where it is. Replicas apply it as a union, so it never goes
down. A cluster created by this version starts with everything this version supports (its `Genesis` writes it): every
node of a new cluster must run the same version. A cluster created before has no version until its leader raises it.

What waits for it:

- `--pipelined-updates`: a leader chains updates only when the cluster version has them at the start of its term, or
  when it starts the term that creates the cluster (its genesis brings this node's version). One raised during a term
  takes effect at the next term; a node with the option logs a warning at each term without it.
- Client sessions: a node advertises them to its clients (`RockserverCapabilities.clientSessions`) only when the cluster
  version has them, or before genesis. A client reads that when it connects, so one connected before stays without
  sessions until it reconnects.
- Checksums and backups (next section): refused until the version has them.
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
- **`Backup`** (RA-7.2, DD-6f). Each replica writes a checkpoint of its database (`NodeStore.checkpoint`, as for a
  snapshot: a memtable flush and hard links, 100–300 ms during which its apply waits) to `<data-dir>/backups/<name>/db`,
  with `backup.properties` (log index, request id, cluster, node, time). All of them hold the data of every entry before
  the backup entry: a consistent copy of the whole cluster, on every node. The same entry applied again after a crash
  finds its backup and changes nothing. The entry carries a request id that `admin backup` draws at random: after an
  answer that was lost (a timeout, a leader change) it proposes the backup again through another member, and the second
  entry finds the backup of the same request and reports it done instead of "exists already". Results are kept and
  asked for by log index (the name is checked too), so a name used again after its backup was deleted is never
  answered with the older result, and a later failure with a name never replaces the earlier backup's result.
  `admin backup` prints `OK` only when every member it was given wrote the backup: it exits with 1 if one failed, and
  with 4 if one gave no result in time; `admin backup-status NAME INDEX` asks again later, without proposing anything.
  `restore-backup` prepares an empty data directory from a copy: it restores the checkpoint and deletes the meta
  column, so the database belongs to no cluster, and every node of a new cluster is prepared from the same backup and
  started as a seeded cluster (see "Migrating from a single node"). It renames a `replica-ops.log` and a `backups/`
  left in the data directory by an old cluster (suffix `.before-restore-<time>`): their results, by log index and by
  name, would otherwise answer for the new cluster, whose log starts again from 1. Client sessions go with the meta
  column: clients open new ones. Restoring under the old cluster's ids and Raft state was rejected:
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

`NodeMonitor` reads, once per second on the maintenance thread, what the alerts watch: `raft.role` (2 leader, 1
candidate, 0 follower, -1 unknown), `raft.term`, `raft.last_index`, `raft.commit_index`, `raft.applied_index`,
`raft.flush_index`, `raft.start_index`, the lags between them (`raft.commit_lag`, `raft.apply_lag`, `raft.flush_lag`),
on the leader `raft.follower_lag` per follower (tag `follower`, from the commit index the leader knows of it) and
`raft.follower_lag_max`, `raft.log.bytes` and `raft.log.entries` (DD-22), `snapshot.index` and `snapshot.age_seconds`,
`disk.data.*` and `disk.raft.*` (usable and total bytes, usable percent) and `replica.failed`; and from `ReplicaOps`,
`cluster.version` and the last checksum matched or mismatched. The metrics thread only reads fields. The node also logs
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
threads per node that run no database calls. The proposer hands each answer back to a new virtual thread, so that
its callers' continuations never run on the control plane; the read barrier's callers hop to the local executor
before touching the database. The leader forwarder's poll timer has a platform thread of its own. Cluster
initialization (the genesis proposal, which can wait up to the request timeout) has a thread of its own, so that
the API gate and the Raft watch (`LeaderStall`) keep running meanwhile. Why the local commands stay on virtual
threads, and what to measure, is in [performance.md](performance.md), item 11. The partitioned apply, when on,
adds its lanes: platform threads (`<node>-apply-lane-N`) that run RocksDB calls only for the apply thread.

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
| `ParallelApplyTest` | Randomized logs over five columns (fixed keys, buckets whose tails all collide, a column without fixed key parts), with rejections, copies, sessions, `deleteRange` and positions without data, applied serially and in lanes: the results of every entry and every row of every column, the internal one included, are the same after every group. A 3-node cluster, two nodes in lanes and one serial, holds the same data, also after a restart. Crashes of a replica in lanes: a flush right after one of its commits (the serial prefix before a segment, a segment's joined batch, a group's end, an entry applied alone), more entries applied, then the database closed without a flush; reopened, it holds exactly what was committed at the flush, its position included, and replaying the rest of the log (serially or in lanes) gives the results and user rows of a replica that never crashed, and every row of a serial replica restarted at the same position. The joined batch of `commitWith` equals, byte for byte, a RocksJava batch of all the operations in order. |
| `SnapshotInstallTest` (in-process) | Every node applies in lanes, the log is purged up to each snapshot (test setting), and a member that was down past the leader's log comes back through a snapshot install. Held inside the install, after the new database opened and applied up to the snapshot but before it is published (`storeGeneration` odd), a follower read on it is forwarded, not served (a mutation that skips the generation check serves it and fails the test). After the install the member holds every write, the applier of the installed database sorts new entries into lanes (its segment counter grows), and it serves follower reads again. A read that failed locally is forwarded when the counter was odd or changed, and keeps its error otherwise. A member running a version without `checksum-schemas` (its database too) that installs the leader's snapshot stops, with `NewerVersionException` in its failure (removing the check after the install fails the test). A snapshot cannot meet an open segment of the lanes: segments end with their apply group, and the drain before a snapshot is handled between groups. |
| `ApplierTest` | At-most-once retries, including after a restart; deletion of records, by count, by bytes and at start; results too large for a reply; rejections; atomic commits; detection of column id divergence; batches with duplicates and rejections; groups of entries, with copies and rejections in them; crash consistency of applies without the WAL, and of groups; a logged raise of the cluster version to a capability the node lacks stops the replica (`NewerVersionException` in the failure) and leaves the version and the position. |
| `SessionApplierTest` | Client sessions at apply: a write resent through other proposers, alone or in an entry with other sessions' writes, takes effect once, also after a restart; the session's floor refuses late copies; unknown and expired sessions are rejected and not recorded, a leader clock behind does not revive them, and expired sessions are deleted; a leader whose wall clock is a year ahead, or jumps within its term, expires nothing (`LeaderClock`); two replicas, one restarting midway, give the same results. |
| `SessionWritesTest` (core) | The client's numbering: which writes are numbered (staged writes not, schema changes, `deleteRange` and batches yes, multi writes over 4 MiB counted not), update ids forgotten once the update ended, also by an unnumbered multi write that ends one; a first copy refused by an expired session re-issued under a new one, a later copy so refused ending unknown (also when that copy is flagged not applied), sessions that keep expiring ending at the deadline, flagged not applied; opening retried after an unknown outcome, and again by a write that waited on another write's opening, also when that opening ended "not applied" at the opener's deadline or overloaded; batches unnumbered when the server does not advertise `numberedBatches`; batches sent as numbered chunks in order, a chunk resent with its number, a later chunk's failure reported as partly applied; closing ends waiting writes; each answer of a copy (value, recorded failure, overload, flagged deadline: ends; unknown, then "not applied": same number again; a sender that throws: lost copy); a deadline without the "not applied" flag, gRPC's own included, on a first or a resent copy, ends unknown, whatever its message; a cancel after a copy was sent ends unknown and stops the resends and the copy in flight, before one was sent cancels it unsent, also for a batch (a batch whose chunk only waits for its session counts as unsent); a synchronous wait is bounded, unbounded without a deadline, and never overflows; the client's own expired-deadline check on a first copy ends "not applied"; past 65,536 open updates the oldest is forgotten and counted; a Reactor caller (`Mono.fromFuture`) that disposes its subscription or times out stops the resends and cancels the copy in flight. |
| `ClientSessionTest` (in-process) | A merge and a transaction commit whose leader dies after its followers applied them and before it answered: the client sends them again with their request ids through the next node, the merge applies once and the commit answers `true` from its record. Copies of a commit and of the write that ends an update answer "unknown" while the first is in flight, then from the record. Copies of a numbered `mergeBatch` chunk, `deleteColumnIfExists`, `deleteColumn` and `deleteRange` take effect once; a numbered batch of two chunks is refused with nothing applied; the client library sends batches as numbered chunks over gRPC, a three-chunk batch of items that fill a chunk to the byte taking exactly one number per chunk (the client's and the server's split agree after encoding); a client batch whose leader dies after committing its first chunk, unanswered, resends that chunk with its number and every key is merged exactly once. |
| `NumberedWritesTest` (in-process) | Streamed multi writes of a BATCH caller with sessions apply every item, results included; copies of a numbered `createColumn` and `uploadMergeOperator` answer from the record (no column re-created after a deletion, no version added); a server offering sessions without `numberedBatches` gets a three-chunk batch unnumbered, taking no number; an endless numbered batch stream is refused after a few batches; a commit refused by the leader behind a follower's forward is flagged not applied at the client; a numbered streamed multi write over the server's collect budget is refused as `SERVER_OVERLOADED`, applies nothing, and the budget is given back after each collection; a BATCH `putMulti` with results over 16 MiB counted is sent unnumbered and applied, on every node (R6-3); an unnumbered `putMulti` over the numbering limit that ends an update makes the client forget the update. |
| `ProposerCopiesTest` | A write's entry becomes its batch's copy once encoded, so a later re-encode never reads the caller's changed buffer; a follower's resend of a late batch is answered from the leader's retry cache, one log entry, applied once; a caller giving up during its batch's encoding waits for it, bounded; through the proposer, with a batch paused before its encoding, `cancel()` returns and an `orTimeout` answers only after the encoding, without holding the JDK's delayer thread; a caller that gives up while the encoding does not end is answered after the 1 s cap. |
| `RustClientLiveTest` (in-process, off by default) | The Rust client's live test against a follower of a three-node cluster: every numbered call, merge operator upload/check/ensure, batches as chunks, two copies of one numbered merge applied once, two copies of numbered multi writes with results answered alike, the multi writes and their result variants over the numbering limit streamed unnumbered, then every node checked. A second run: numbered writes (with results) lost with a halted leader behind a proxy, resent by the client to a follower, answered with their first results and applied once. |
| `CommitOutcomesTest` | The answer to a copy of a numbered commit whose transaction is gone: unknown while a mark of this term exists, also for a mark of a term that started while the copy's leadership barrier ran (the term is read before the barrier), "run it again" with no mark or a mark of an earlier term, the record when there is one; the first mark of a newer term drops the older ones. `NodeStoreTest` checks that a session expired at the replicated clock is not live while its row is still there. |
| `AppliedFlagWireTest`, `AppliedTrailerInterceptorTest`, `GrpcServerDeadlineErrorTest` (core) | The "not applied" flag on the wire: the server's trailer follows the exception's flag (a real server round trip included), the client reads it, and the legacy messages only without it; gRPC's own deadline is never "not applied". |
| `LeaderSessionTest` | Per-key update queue (order, release on commit, expiry also without other traffic, and end of leadership); chains (handoff at admission, the caps on links and on the readers of one, breaks and their end, a pause for a conflicting transaction, duplicates, expiry, close, no session monitor under the log lock); history kept for open transactions and in-flight commits, up to the key cap; an update whose commit is in flight does not expire; the commit size limit of staged writes. |
| `PipelinedUpdatesTest` (in-process, flag on) | With the leader's apply stopped: chained updates see each other's values and nothing is acknowledged; wrong guards; plain writes, transactions and `deleteRange` against a chain; closes in mid-chain, and more of them waiting than a server's CONTROL workers and queue hold; refused operations on chained ids; second writes; merges and multi-key commits; caller gone, abandon; a snapshot and a restart inside a chain, around a rejected entry; a leader change with a chain in flight; a commit that ends without an outcome; the flag on and off at restarts, and a chained id handed to a leader without it; concurrent counters. With the flag off: the commit of an update, read back from the Raft log, has the old bytes, and a second write goes to the log. |
| `PipelinedLeaderKillTest`, `PipelinedPartitionTest`, `PipelinedRandomFaultTest` | The suites of the same name, with the nodes on `--pipelined-updates`. With `PipelinedUpdatesTest`, the only tests that chain in a default run: the suites below run the plain per-key queue unless the property below is set, and fail if an update chains there. After each fault these three wait for an update that chains. |
| `LeaderKillTest` | Repeated SIGKILL of the leader under concurrent writes. No acknowledged merge is lost or doubled, no acknowledged increment of a hot counter is lost, and replicas are byte-identical. Writes resume in about 1 s (the test only requires under 10 s). |
| `FollowerReadTest` (in-process, follower reads on) | A follower serves reads of writes acknowledged on the leader, with writes flowing meanwhile and on a quiet cluster (thanks to `CommitHeartbeats`), some by itself (the share is printed, not asserted beyond a loose bound: it depends on the disk and load); once it knows the commit index it serves by itself. The option is set on every node through the property. A follower whose database may hold an entry of a term later than its read index forwards, also after a real leader change: a read index of term T whose apply (held by the test) is reached in one group with the entries of the next leader's term is forwarded, not served (a mutation without the check serves it); reads stay linearizable on the follower and the new leader across a leader change. A read on the leader whose apply is held does not answer the old value after a follower served the new one. A follower whose apply is held forwards instead of answering with the old value, and one cut off from the rest fails instead of answering. A follower names the leader in its responses, and a client of several nodes listing the follower first reads its writes and its transaction through it. |
| `PeerChannelsMembershipTest` (in-process, 3 nodes and a fourth that joins) | R2-RT-2: after a real membership change the shared `PeerChannels` of every node drop the channel to a member removed after it joined (no startup peer list holds its address) and keep the other members' channels (with the pruning call removed, the channel stays and the test fails). `InProcessCluster.join` starts a node with `--join` on ports of its own. |
| `CommitHeartbeatsTest` | Each commit index is asked for once and a higher one again, nothing before a commit index or when disabled; Ratis finds the transport by class name and builds it with and without the node's instance. A wake given before the appender waits is kept for that wait (R7-1; Ratis' `AwaitForSignal` loses it), but only when it would make the appender send a commit heartbeat; any other early wake is dropped, a wake while waiting always wakes. Resends after broken streams do not reopen the window of the first send and stop at 3 per commit index (R7-2). |
| `FollowerReadLatencyTest` (in-process, prints numbers) | Read-after-write latency on a follower, with and without `CommitHeartbeats`, against forwarded reads with and without the leader's apply wait, quiet and with a writer in the background; with the heartbeats the follower serves more than half of the quiet reads and some of the busy ones itself (loose bounds; the printed shares are the measurement). The numbers in "Follower reads" come from it. |
| `ForwardingFailoverTest` | A client of a follower sees only retryable errors (point reads, range streams, writes) while the leader is killed, and every kind of request succeeds again after the election. |
| `ClientFailoverTest` | One client connection listing all three nodes, while the node it uses is killed (the leader, or a follower): only retryable errors, new calls succeed on another node of the same connection (about 1 s with an election, under 200 ms without), a new connection skips the dead node, and a transaction opened through a dead follower still commits. |
| `IsolatedNodeTest` | A client listing every node, connected to a node that gets isolated (the leader, or a follower), moves to the majority and keeps working, and the isolated node listens and serves again after the heal. With every node isolated all pause, and after the heal all resume and the client recovers by itself. An election shorter than the bound pauses no node. A follower restarted while cut off pauses too, and the client that lists it first moves. A cluster killed or stopped as a whole and restarted at once serves again, a paused node takes part in the election, and a node of a new cluster does not pause. A node that lost its database and kept its log pauses, and a client that starts during a cold start waits for the cluster, also past a frozen node. |
| `PartitionTest` | An isolated leader acknowledges nothing. The majority takes over. A leader that keeps its majority keeps serving. The history, hot counters included, is linearizable. |
| `PauseTest` | SIGSTOP of the leader and of a follower. The history stays linearizable. A freeze longer than `--jvm-pause-limit-ms` ends the node with exit code 3, and it starts again. |
| `StartupChecksTest`, `StartupClusterTest` (in-process) | The config digest ignores layout and key order; a column volume outside `db/` is refused before it is created; copies of one seed share a digest that a truncated copy and an opened database do not, and a digest recorded by a failed first start is kept after the start-up check opened the database but taken again after `db/` is replaced, and a replica with another seed than the leader stops at genesis while copies of one seed start a cluster; `export-seed`'s copy holds the data with no cluster and no position, also with an absolute volume path inside `db/`, and a refused target config leaves no target; a direct-memory error made in Java code reaches the JFR watch; `run` refuses a JVM without `-XX:+ExitOnOutOfMemoryError`; a removed node refuses to start again, also with `--join`, and so does one stopped before its removal, whose log never received it (the members tell it); a node with a wrong API port for the leader forwards to the port the leader names. |
| `RemovalCheckTest` | Only a committed configuration newer than the node's own and without it says "removed"; one not committed, or a member with no group yet, is asked again; a majority of the configuration the node knows decides; members that cannot be reached let the node start at once. |
| `ClusterIdentityTest` | Each genesis gets its own id, and a second `Genesis` in a log changes nothing. A node that elected itself under the same group id, added with Ratis' own client, stops before the configuration changes, and writes go on. What `add-peer` refuses. |
| `RatisContractTest` | The Ratis 3.3.1 behaviors that the design relies on beyond its API, against real servers in the JVM: the bootstrap and configuration meta files that `--join` safety reads, and the initializing state of a server started with an empty group (no election, no bootstrap file); the open segment format that `RaftLogRepair` rewrites; the type of the full-pending-queue refusal; a state machine refusal that reaches a remote client by class name and message, without a step down; `NotLeaderException` for an entry appended by a leader that then steps down (three servers); a transfer without a new leader that steps down; the 3 s wait of `GrpcUtil.shutdownManagedChannel` with a call in flight; a snapshot chunk held on the heap until its timeout by `StreamObserverWithTimeout`; a leader whose log worker is stopped (`LogWorkerGate`) commits nothing until it flushes; the CPU of the appender to a dead follower (printed, bounded at half a core); with the node's Raft settings, a follower learns a new commit index only with the next append or heartbeat (3 s), and within 600 ms of `CommitHeartbeats.send` on the leader, for both followers, 20 writes in a row, also when the link to a follower drops that heartbeat and breaks its stream (sent again on the new stream), and when `send` lands between the appender building nothing and its wait (R7-1, through a test hook in the real appender; the appender waits on its own `WakeSignal`). The match index moving only on replies with entries, and the follower order of `LeaderStall`, are pinned by `LeaderStallTest`. |
| `RaftSettingsTest`, `ByteAdmissionTest`, `ConflictPositionsTest` | Snapshot chunks stay at most 4 s on the heap; Ratis' pending-request limits hold every node's attempts and stay near the defaults, and the byte limit grows with a peer list of more than 4 nodes; one entry of the largest size fits every limit on its way. The byte admission never exceeds its limit and is sized from the heap. Conflict positions survive the exception that Ratis rebuilds, and a text that does not parse is a conflict of the whole entry. |
| `SstMaintenanceTest` (in-process) | `scanRawResumable`, `getSstMetadata` and `getTableProperties` through a follower describe the leader's database; raw-scan tokens carry the leader and its term and skip their tables through any node, while tokens of another node or term, or untagged ones, scan them again; over gRPC a client that does not set `acceptsTaggedSstTokens` gets plain filenames; `compactFiles` of the internal column is refused. |
| `ColumnIteratorsTest`, `SnapshotBackoffTest`, `DiskProbeTest`, `AddPeerCheckTest`, `ProposerResetTest`, `ShutdownSignalTest` | Applying a column deletion closes the column's iterators and ends its range streams with `COLUMN_NOT_FOUND` instead of waiting for them (a stream of another column goes on; one started during the deletion fails), and so does a replicated deletion for `scanRaw` and `scanRawResumable` served through a node's API (R3-G2a-5), and an unknown iterator answers `UPDATE_RETRY` (the database's error for it is pinned); a failing snapshot is tried again only after its delay (1 s doubling to a minute), on a cluster too; a disk probe that does not end within the limit means a hung disk, until it ends; `add-peer` waits for a majority of the members to answer; a proposer's connection reset returns once the failed proxy left Ratis' map, before its slow close, so a resend gets a new one; the shutdown hook's stop waits for a node whose replica failure was already stopping it, and gives 3; a node stopped by a removal or a newer cluster version exits with 4 or 5 from both the main thread and the shutdown hook's halt (R6-2). |
| `CommandLineTest`, `FaultFsTest`, `LogTermsTest` | The operator commands through `main`, in this JVM with the exit replaced (`ReplicatedServerMain.exiter`): `admin checksum` and `admin backup` against a running cluster print `OK`, wrong arguments exit with 2, `export-seed` of a stopped replica writes the seed and refuses a target that exists, `restore-backup` restores into an empty directory (which then starts and serves the data) and refuses a used one with 1. The storage shim fails writes and syncs under its directory only while triggered, and only the kinds the trigger names. `LogTerms` reads the terms of a stopped cluster's logs, and they match. |
| `LeaderStallTest` | The rule of `LeaderStall`; follower next indices, which Ratis gives without ids, pair with ids only when the follower list read before and after them is the same; the bound that grows and comes back, and the rule of a log that stops, which a real leader with a stopped log worker follows within seconds. A graceful stop of the leader with one node down, followed by a restart at once, loses no reply and costs no step down. With one node down, the reply to a new leader's first entry is lost on a link, after the entry reached the follower's disk: nothing commits and the leader stays, until it steps down and a write gets through. The same loss after the first commit heals with the next write. A leader behind in applying does not step down. |
| `RollingRestartTest` | Graceful restart of every node, twice, under load. No failed operation. A leader restart stops writes for about 200–300 ms. |
| `MembershipTest` | A node started with `--join` replaces a lost leader, however long before its `add-peer` it started and after a restart in between, catches up from a snapshot, and restarts with or without the option. A node removed while it runs exits. Seeded bootstrap works. An empty node added right after a seeded start receives the seed through the snapshot taken after genesis, and never replays the seeded `Genesis`. A node of a seeded cluster is replaced whichever node leads. One step replacement of a lost leader, a lost follower, seeded or not, and of a leader that runs. `add-peer` refuses a node with a log of its own and a node started without `--join`, and fails at once without its node; a change without a majority ends at its limit; `--join` after a start without it is refused; `admin snapshot`; a removed node that a supervisor starts again exits with 4 and never prints `READY` again (DD-30, R4-6), while the others serve on; the warning of a new cluster whose nodes all have `--join`. |
| `ColdRestartTest` | SIGKILL of all three nodes under load, 5 cycles, plus a 2-of-3 restart. Nothing acknowledged is lost, and replicas are identical. |
| `RandomFaultTest` | A seeded random schedule of kills, restarts, pauses, isolations, partitions, one-way and single-link cuts, slow links (20–200 ms each way with as much jitter), throttled links (64–512 KiB/s), blackholes (bytes held, nothing reset, as with dropped packets), flapping links (isolated and healed every 50–400 ms) and client-side faults (a node's client link that drops every answer, so committed writes go unacknowledged while the leader runs on, or that is cut). Register histories, with read-modify-write updates and hot counters in them, are linearizable; the transactional bank-transfer invariant holds; list-append transactions (every read a `getForUpdate`) show no anomaly at all to `ListAppendChecker` and lose no committed append; every node's persisted Raft term (`raft-meta`) never decreases. Adding faults changed which schedule a given seed produces. |
| `OverlappingRandomFaultTest`, `ClockSkewRandomFaultTest` | `RandomFaultTest` with a second fault starting while the first holds (for example a pause plus a one-way cut, or two kills: the majority lost, then back), where availability is reported but not asserted and every safety check stays; and with node 1's wall clock 90 s ahead and node 2's 90 s behind. |
| `ClockSkewTest` (in-process) | Leaders whose clocks are two hours behind or ahead of the others: the replicated clock never goes back and is the same on every replica, and session writes across the leader changes apply at most once (exactly once when acknowledged). |
| `ListAppendCheckerTest` | The Elle-style checker finds and classifies known anomalous histories (G0, G1a, G1b, G1c, G-single, G2 as allowed by snapshot isolation, incompatible orders, duplicates, garbage, internal reads), treats an unknown transaction as committed only once it was seen, and passes random serial histories. |
| `RaftLogRepairTest` | Segments written by Ratis' own output stream, torn at sector boundaries: torn entries are dropped, and Ratis then opens and appends to the log; anything else is refused and left unchanged: a damaged complete entry, a size that hides a valid entry, a gap or overlap between segments, a problem in a closed segment; a dry run changes nothing. A leader, and a follower that never acknowledged its last entry, refuse to start with a torn entry and rejoin after `repair-log`. So does a leader that loses a whole open segment after closed segments, a snapshot and a purge. Two or three nodes torn by one power cut, in entries no quorum confirmed, repair and elect a leader among themselves with every acknowledged write. No test interrupts a real device: the torn bytes are written by the test. |
| `ClusterOpsTest` (in-process) | A cluster created by this version has the full cluster version, and advertises sessions. A checksum proposed through a follower gives the same digest and key count on every replica, and every replica finds the others equal; the Raft port answers it. A backup asked through the Raft port is written by every replica at the same log index, and the port answers it by name and index; the same request proposed again is done again, another request with the name fails without changing the first result, and a query by another name or index never returns an older result. Restored with `restore-backup` into a new seeded cluster, through a data directory holding an old `replica-ops.log` and `backups/` (moved aside), it holds every key written before it and none written after. The monitor's gauges (one leader, a Raft log with bytes) and the replication timers reach each node's database registry, and the core meters carry the node tag. |
| `CapabilityUpgradeTest` (in-process) | A version-0 cluster with one member of an older version is raised only to what that member has (no sessions advertised, no pipelining, also after another check); once that member is upgraded, the version reaches everything, sessions are advertised, the current term still runs without pipelining and the next leader pipelines. |
| `ChecksumTest` (in-process) | A row written past the log on one replica makes its digest and key count differ, and every replica reports the divergence (`checksum.mismatches`, the index gauge). A column with the same name and no data but another merge operator on one replica digests differently (schemas are in the digest). With checks every 6 s and the leader stopped 3 s after one, the next check comes about 6 s after the last one, not 6 s after the change. A check applied again (its recorded time moved 3 hours back while the node was down) keeps that time, also across the next start. |
| `ReplicaOpsTest` | The new entries round-trip and are applied alone; backup names; the wire form of results; capability names; digests that tell key parts and values apart; series and gauges exported cumulative, with their kind, and moved to a new registry; the lag sampling period; Raft log bytes counted from segment files only; the alarm's start, repeat and end; the verdicts of `admin checksum` and `admin backup` (OK only with a final result from every member), which failed proposals are retried, the capabilities `add-peer` requires (current and pending), the replication meters following a configured node name, and a blank `node-name` meaning the node id. |
| Component tests | Codecs (including fuzzing), `ConflictTracker` (checked against a reference model, also with a capped history; pruning in bounded steps, and appends and checks while a long prune runs), `ReadBarrier` (deposed and isolated leaders never pass; a second round starts while the first is in flight; a read index needs no apply), `LeaderHintRouter` (which calls go to the hinted leader, and when the hint is taken or dropped; every service method classified; two failing addresses each back off), the idle timeout of forwarded streams, `NodeStore`/`CheckpointStorage` (partial installs deleted once replaced, at the start, and after 10 minutes without a change), `LeaderForwarder` (leader lookups still waiting fail on close; see also "Targeted faults and component tests" below), fault proxy (cuts, delay and jitter keeping byte order, bandwidth caps, blackholes that release held bytes, dropped answers resetting their connections when healed), linearizability checker. In the core project, `GrpcProxyCloseTest`: closes that answer late, on a proxy `GrpcServer`, take no worker, are not cancelled with their caller, and hold up shutdown after their caller left; the close of an update whose caller left while it opened is asynchronous too; a retry of either close, with no caller left, is no late failure. |

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
| `ProposerCommitRetryTest` | Commits refused before the append for lack of room (Ratis' pending-request limit, or the commit lane's attempt permits held by commits Ratis has not answered) go back to the queue: they succeed once there is room, are not sent while there is none, and fail at their caller's deadline as not applied (`READ_DEADLINE_EXCEEDED`), never as unknown. |
| `LeaderForwarderTest` | Leader lookups: another node is answered at once, this node only as a ready leader; a change wakes waiting lookups well before the poll; a leader that appears between the first check and the registration (its notification finding nobody) is seen by the check after the registration; no leader within the request timeout is a retryable overload. Connections: four per peer, used in turn; an invalidated connection is replaced while the other slots stay, and a stale invalidation does not drop a newer one; a refused port fails at once (the probe) and the peer is not tried again for 200 ms. |
| `ClusterCheckTest`, `CommitOutcomesPathsTest`, `LocalCommandsTest`, `ReplicatedWritesTest`, `UpdateProtocolPathsTest` | The classes split out of `ReplicatedConnection`: the cluster check tells each follower once per term, retries a silent one only after its pause and asks again in a new term; commit outcomes (a second copy marked in the same term, settling by outcome, pruning of marks, every kind of record, a gone session, flagged not applied, barrier failures); who leads and gets the session of its term; column creation and its check, and batches split, refused when numbered and too large, reported when partly applied; the update protocol's answers for transactions and updates that are gone (also for copies of numbered commits), staged writes carrying a request id, empty commits, the end of an update and a failed update. |
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
   ended well, and retries `UPDATE_RETRY` from a no-change close. Watch `update.guardRejected`, `update.chainConflict`,
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
   per update id and drives the cancel and overload close paths), and the round-8 commit `b8b713488` (a body's own
   error after an unconfirmed chained read runs the update again, below). Left on the Yotsuba side:
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
     retries run out, the retry exhausted error carries it. Any other failure keeps the old behaviour (the close's
     answer is logged, the caller gets the body's failure). That covers the first case: there the body's failure is
     the ending write's own Rockserver error, and an unknown outcome or a deadline must never become a rerun, since
     the write may have applied. The rule relies on one contract of Yotsuba's bodies: the call with the update id
     (one put or delete, or the no-change close) is their last step that can fail, so a body's own error always
     comes before its ending write and the update wrote nothing. A cancel, by the caller or a caller's timeout,
     goes to the cancel close, which only closes and never reruns. Rockserver needs no change for this: the close
     already answers `UPDATE_RETRY` whenever the read is not confirmed, and answering these closes quietly would be
     wrong (a clean close of an unconfirmed chained read would confirm it). Nothing is written or left open in any
     of these cases, since a chained update has no native transaction and the leader removes it before it answers.
     Under `--pipelined-updates` the remaining warning follows leader changes, timeouts and refused chained writes:
     it counts failed chained updates, and a pipelined cluster under contention makes some on purpose. Tested in
     Yotsuba with fakes (`RockserverUpdateTransactionsTest`) and against an embedded single node whose close is made
     to answer `UPDATE_RETRY` (`RockserverUpdateCleanupEmbeddedTest`), not against a running replicated server. The
     same commit fixes the javadoc of `RockserverUpdateTransactions.updateAlreadyEnded`, which said the cluster
     answers a close of a dropped update quietly: it now says so only without `--pipelined-updates`.

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
     not start. So, unless `-Djava.io.tmpdir` is given, `run` extracts the library into `DATA-DIR/tmp` itself
     (`NativeLibraryLoader.loadLibrary(dir)`, before any RocksDB class loads; `RocksDB.loadLibrary()` then finds it
     loaded), and deletes the copies that earlier runs left there (a loaded library stays usable when its file is
     deleted). Setting `java.io.tmpdir` while the JVM runs would not do this: the JDK reads the property once, at
     start-up, and `File.createTempFile`, which RocksDB uses when no directory is given, keeps the start-up value.
     `run` still sets the property to `DATA-DIR/tmp`, for libraries that read it later (Netty's native transports);
     the replica's own config file is written next to the database, not in the temporary directory. A given
     `-Djava.io.tmpdir` outside the data directory is warned about.
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
     error) but before the maintenance loop noticed is the failure's stop too: `close()` checks the division. Still
     open until `DiskFaultTest` runs: `ShutdownSignalTest` checks both in process (a failed apply, and a division
     closed right before the signal); `DiskFaultTest` sends a real SIGTERM to a node whose disk is failing (written,
     not run yet: it is a process suite).
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
   - `GrpcUtil.shutdownManagedChannel` waits 3 s, then 2 s more, on the thread that resets a peer's proxy; worked
     around by `Proposer.resetConnection`, which resets on a thread of its own and waits only until Ratis took the
     failed proxy out of its map (so a resend to the same peer gets a new channel, not the one being closed).
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
