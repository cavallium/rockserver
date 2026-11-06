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
studies that step: columns assigned to groups, no cross-group operation for Yotsuba, and a verdict that it pays
only once the apply thread is still the limit above about 4x production load.

## Shape

```
              Yotsuba (unchanged Rockserver client API, gRPC)
                 │ any node; requests that need the leader are forwarded to it
   ┌─────────────┼─────────────────────────────┐
   ▼             ▼                             ▼
 node A        node B (leader)               node C
 GrpcServer    GrpcServer                    GrpcServer        ← core server, unchanged
 ReplicatedConnection (implements RocksDBConnection; Router)    ← routes every API command
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
  These parts live in classes of their own next to `GrpcServer`, which keeps only the lifecycle: `GrpcProxyMode`
  (the proxy scheduler, its limits and the connection age), `GrpcTransport` (the listening gRPC server that
  `pause` stops and `resume` replaces), `MustCompleteOperationTracker` (the rollbacks and closes that `close`
  waits for), `GrpcRequestContexts` (wire context to `RequestContext` and deadline), `GrpcScheduledCalls` and
  `GrpcIteratorOperations` (scheduling, cancellation and iterator leases), and `GrpcServerImpl` (the service
  methods). The split moved code only: behavior, status mapping and the `rockserver-leader` header are the same.
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
| `NodeMetrics` | Timers and gauges of the replication path, logged every 10 s. |
| `ByteAdmission` | The byte-weighted semaphore that never waits, behind `Proposer`'s bound on the bytes of writes a node holds and has in flight. |
| `ReadBarrier`, `TermService` | Linearizable reads. `TermService` also answers what a node holds (`NodeState`), and a follower's read index. |
| `FollowerReads` | A follower's read indexes from the leader, for the point reads it serves itself (off by default). |
| `ClusterCheck` | A leader tells its followers which cluster it leads, and a node of another cluster stops. |
| `PeerChannels` | The gRPC channels to other members' Raft ports used by `ReadBarrier`, `FollowerReads` (one cached channel per address, a fresh connection attempt when it failed) and `ClusterCheck` (a one-off channel per call). |
| `ApiGate`, `CommitHeartbeats` | Whether a node serves its API, from whether it knows a leader; the leader sending its commit index to every follower at once instead of with its next heartbeat. |
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
removed. The price of hand-written codecs is that every format change needs a version and a test for old bytes:
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
| Linearizable read | `get`, `exists*`, `getMulti`, `reduceRange*`, `getRange*`, `getRangePage`, iterator open, `getColumnId`, `getAllColumnDefinitions`, `checkMergeOperator`, `estimateNumKeys`, `scanRaw*` | On the leader, after the read barrier, against its local database. Forwarded to the leader from the other nodes, except point reads (`get`, `exists`, `getMulti`, `existsMulti` outside a transaction) on a node with follower reads on: see "Follower reads". |
| Leader-bound | `openTransaction`, `getForUpdate`, writes and reads with a transaction or update id, `closeTransaction`, `closeFailedUpdate`, iterator seek, next and close | Served by the leader of the current term. Forwarded to it from the other nodes. |
| Local | `flush`, `compact`, `compactFiles`, `getSstMetadata`, `getTableProperties` | The node that received the request does it on its own database. |

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
serves: writes, transactions, updates, iterators, ranges and column calls. Point reads stay on the client's node, which
serves them itself with follower reads on, and so do maintenance calls, which concern that node, and the handshake. The
second channel takes calls only while it is connected; before that, and after a call on it ends with `UNAVAILABLE`
(the channel is then dropped, and the same address is not taken again for 2 s), calls go through the ordinary channel
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

A hung disk is not detected: the process still answers HTTP/2 and keepalive, and calls wait for their deadline. It
would take a probe through the request path.

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
  batch, which happens while the caller still waits for its answer (except in the short time between a batch
  taking the write and encoding it, if the caller gives up right then). A caller must therefore not change a
  write's buffers while it waits; if it does, the batch fails to encode, nothing of it is sent, the writes that no
  longer encode to their size fail as invalid (or with an unknown outcome if an earlier batch may have appended
  them) and the others go back to the queue, as never taken unless an earlier batch may have appended them, so that
  their callers' deadlines and cancellations apply again. The encoder also returns the batch's requests as decoding
  the bytes in place would read them: each key and value a slice of the batch, each array of a commit a copy,
  nothing parsed. Each request then keeps that copy instead of its caller's entry, so a write that goes back to the
  queue still sent (a commit after another commit's conflict, a copy resent alone), which its caller may already
  have given up on and whose buffers it may reuse, is encoded again from the batch's immutable bytes, never from the
  caller's memory. A batch with a schema change returns no requests; its members never go back to the queue still
  sent. On the leader (and for every batch of commits, which only go to the local Raft server) the proposer offers
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
  property `it.cavallium.rockserver.replicated.write-admission-bytes`. Each request counts its encoded size plus 256
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
  requests, and 160 MiB instead of the 1 GiB this used to be) hold the data writes of every node of a cluster of up
  to 4 nodes and the leader's own commits. A long commit stall can still fill them with remote attempts that their
  nodes gave up on. Ratis' refusal then (`ResourceUnavailableException`, "Failed to acquire a pending write
  request") is a refusal before the append, resent like the node's own; Ratis raises the same type after an append
  ("add a pending write request"), an unknown outcome, and only the message tells them apart:
  `RatisContractTest` pins it, and a changed message falls back to an unknown outcome. The retry cache of Ratis 3.3.1
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
  batches unnumbered, streamed once and never resent. A chunk that fails for good after an earlier one took effect ends the call with `INVALID_REQUEST`
  ("partly applied"), as on the server.
- **Leader time.** The leader prepends its clock (`LogEntryCodec.stamp`: a marker byte and 8 bytes, without copying
  the batch) to every entry that holds a session write or registration. A proposal that already starts with the
  stamp marker is refused as malformed: only the leader stamps. Apply keeps the highest stamp seen as the
  replicated clock (`CLOCK`), written with the applied position of the entry that moved it, so it never goes back,
  even after a leader whose clock is behind, and every replica has the same clock at the same position.
- **Expiry.** A session's row holds its timeout, the clock of its last applied write and its floor. The timeout is
  in the `RegisterClient` entry (`-Drockserver.client-session-timeout-ms` of the node that opened it, 1 hour by
  default, at least 10 minutes and 30 request timeouts: well above the time a copy of a write may still be resent),
  so replicas agree whatever their own settings. A proposer that gets `SESSION_EXPIRED` for a copy of a write whose
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
  `OUTCOME_UNKNOWN`; otherwise `UPDATE_RETRY` as before, or `SESSION_EXPIRED` if the session is gone. A copy that
  arrives while the first is in flight gets `OUTCOME_UNKNOWN` instead of a second entry, which would conflict with
  the first. The leader marks a numbered commit unsettled before anything can refuse it, and clears the mark only
  after the transaction or update left its session, unless the outcome was unknown: then the mark stays until the
  record exists or the term ends. A copy reads the mark before the record, so "no mark, no record" proves that no
  copy applied. Marks of earlier terms and marks whose record exists are dropped once the map doubled, on a virtual
  thread. The lookups run on the node's virtual threads, never on the control threads that complete the read
  barrier. A write staged in a transaction that carries a request id is refused (`INVALID_REQUEST`): a resent copy
  would be staged twice.
- **The client** (`GrpcConnection`, `SessionWrites`) opens the session on its first write, numbers the writes listed
  above, the write that ends an update only when the update was opened through it. On an unknown outcome (`UNAVAILABLE`,
  `CANCELLED`, `OUTCOME_UNKNOWN`, a deadline error that does not say "not applied") it sends the same numbered
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
  `SERVER_OVERLOADED` (not sent).

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
  before. A rejected request rolls back its whole entry in the serial path; here its lane marks the entry as a
  cut and the lanes skip what follows. At the end the lanes roll back to their save point before the cut (one
  per entry and lane), the entries before it commit, the in-memory changes of the entries from the cut on are
  undone, and those entries are applied again: the cut one on the serial path, which rejects it exactly as the
  serial apply does (commit, then one request at a time), the next ones in lanes. Concurrent writers cannot share
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
  Java, already merge and skip it. A later flush or compaction then only merges operand sequences that a read
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
next heartbeat, a quarter to a half of the minimum election timeout later. An appender whose follower lags in commit
index wakes on every commit (`LeaderStateImpl.commitIndexChanged` notifies it) but sends nothing without new entries
or a due heartbeat. So when the leader answers a read index request it also asks for a heartbeat round
(`CommitHeartbeats`): Ratis 3.3.1 has no public way to call `LogAppender.triggerHeartbeat`, but its linearizable read
path does it (`raft.server.read.option=LINEARIZABLE`, set in `RaftSettings`, with the default leader heartbeat check),
so the leader submits a Ratis read with its dummy message, which skips the state machine, and ignores the reply.
Each heartbeat carries the commit index when it is sent, at least `c`. Ratis skips the round when one already
confirmed a commit index at least as high, and coalesces triggers per follower, so it costs at most one empty append
per follower per read index request (at most one in flight per follower). The read index itself still comes from
`ReadBarrier`, never from Ratis' heartbeat confirmation.

The read is forwarded to the leader instead, as without follower reads, when no read index comes (no leader known, the
leader unreachable or not confirmed within a maximum election timeout, or a leader that predates the method), when the
follower is more than 1024 entries behind `c`, or when its apply does not reach `c` within 20 ms of the answer
(`Router.FOLLOWER_READ_WAIT`). A follower read is therefore never slower than a forwarded one by more than that wait
and the read index round trip. A read that fails while a snapshot install replaces the database is forwarded too: the
install raises a generation counter before it closes the old database and again once the new one is open
(`RockserverStateMachine.storeGeneration`, odd in between), and a read that fails with the counter odd or changed is
forwarded; a read that throws at once counts as failed.

**Why still off by default.** Measured in process (3 nodes on one machine and one disk, `FollowerReadLatencyTest`,
100 rounds of a write on the leader and a read of it on a follower, in turn per mode):

| Read after write | Quiet cluster, p50 / p90 | One writer in the background, p50 / p90 |
|---|---|---|
| Follower read | 1.1-1.3 / 2.6-3.1 ms, 97-98% served by the follower | 6.0 / 7.4-10.1 ms, 100% served |
| Follower read, without `CommitHeartbeats` | 24-25 / 27-28 ms, 0% served (waits, then forwards) | 5.9-6.2 / 7.2-10.5 ms, 100% served |
| Forwarded, leader waits for its apply (follower reads on) | 2.1-2.6 / 3.9-5.0 ms | 1.8-3.0 / 2.8-5.0 ms |
| Forwarded, leader shortcut (follower reads off) | 1.7-2.4 / 3.3-4.8 ms | 1.4-2.4 / 2.5-4.6 ms |

On a quiet cluster the follower now serves reads after writes and beats forwarding. With writes flowing, a follower
read waits for the follower to receive, flush and apply the latest entries, and takes about 3-4 ms more than a
forwarded read at the median (with a wait of 5 ms instead of 20, half of these reads fell back to forwarding and paid
both). Turning the option on also makes every leader read wait for the leader's apply, about 0.5 ms more here. Under
Yotsuba's steady writes that is a latency loss on every read in exchange for taking point reads off the leader, which
only a benchmark of the Yotsuba mix can weigh. The bound of 20 ms plus a round trip is large against a 2-3 ms
forwarded read, so the option stays off; measure it with the load benchmark before turning it on.

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
`UPDATE_RETRY` and run the transaction again, which Yotsuba already does. A commit whose answer was lost with the
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
  - SST and blob files are hard-linked (copied, while the apply waits, if `raft/` is on another file system;
    see Disk layout);
  - the MANIFEST prefix is copied and `CURRENT` written;
  - the WALs still needed are copied.
- **Why not `org.rocksdb.Checkpoint`.** It refuses the column volumes (`cf_paths`) that the default config
  uses.
- **When.** Every `snapshotEveryEntries` applied entries (default 500k, Ratis' own trigger), and also once
  `snapshotEveryBytes` of log entries (default 4 GiB, `--snapshot-every-bytes`) were applied since the latest
  snapshot or since the start: an entry holds up to 4096 requests or 8 MiB, so a count alone does not bound the
  log a snapshot lets Ratis purge. The byte trigger asks through Ratis' snapshot management API, like `admin
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
  replaces its database. Copies of a seed that differ from each other are not detected: the seed must be one database.
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
  id, if it was started without `--join`, or if it has a log of its own (see "One cluster id per genesis"). It accepts
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

  The peer list of a node is read at the start, and the API addresses in it are the ones forwarded calls use (a member
  that is not in the list is asked for its own). So a `--join` node given wrong API ports for its peers joins, and
  then fails the calls that it forwards: restart it with the right list.

  A node still running when `remove-peer` removes it closes Raft by itself and exits with 3. Take it out of its
  supervisor first. A removed node that a supervisor starts again does not exit: its configuration does not contain it,
  so it stays up in the initializing state, printing `READY` and with an API that has no leader to serve from, and it
  says so in its log as above.
- **Heap.** Run every node with `-XX:+ExitOnOutOfMemoryError`: a node without it warns at its start. A leader that
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
  of 5 runs. Only an error that the JVM itself throws makes it exit: Netty's limit on direct memory is not one.

  An out of memory error on a thread of Ratis does not stop the JVM. In the appender of a new member it ends the
  catch-up as if it had finished: `LeaderStateImpl.restart` starts a new appender only for a peer in the configuration,
  and `checkStaging` then finds nobody lagging and appends the joint configuration, with a member that has nothing.
  - With `-XX:+ExitOnOutOfMemoryError` the leader exits at once (code 3, like a failed replica), before any of this. Start
    it again with more heap, and run `add-peer` again. The new node keeps waiting. Run once with the literal `add-peer`
    below: replicas identical at the end.
  - Without the option, after a one step replacement, nothing is lost: `add-peer` says `OK` too early, but the two old
    members are a majority of both configurations, the leader stays, writes go on, and a new appender starts the
    snapshot again. Run once.
  - Without the option, with `add-peer` that leaves a lost node in the configuration, the cluster ends with no leader,
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
     section is about, losing fsynced entries and votes. Nothing takes such copies today: they have to come from the
     storage layer (see "Left before a first deployment").
  4. Without a copy, each replica's `data/db` still holds its last flushed state, which a plain Rockserver can open.
     It cannot seed a new cluster as it is, since it belongs to the old cluster and a node without Raft state refuses
     it, so its keys would have to be exported into a new database that seeds one ("Migrating from a single node").
     What the replica had not flushed, and all that only a Raft log holds, is lost: no tool reads it out. No tool or
     test covers this way, which is a last resort.
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
  anyway, within seconds at that load. The node stays up with its hung disk, as a follower that does not answer.

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
- **`scanRaw` SST tokens** describe the leader's own files, so a leader change invalidates them.
- **Merge operators** must be deterministic. One that fails makes the write fail with
  `MERGE_OPERATOR_FAILED` on every replica, see "Failures during apply".
- **Maintenance.** `flush` and `compact` act only on the node that receives them.
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
| `ParallelApplyTest` | Randomized logs over five columns (fixed keys, buckets whose tails all collide, a column without fixed key parts), with rejections, copies, sessions, `deleteRange` and positions without data, applied serially and in lanes: the results of every entry and every row of every column, the internal one included, are the same after every group. A 3-node cluster, two nodes in lanes and one serial, holds the same data, also after a restart. Crashes of a replica in lanes: a flush right after one of its commits (the serial prefix before a segment, a segment's joined batch, a group's end, an entry applied alone), more entries applied, then the database closed without a flush; reopened, it holds exactly what was committed at the flush, its position included, and replaying the rest of the log (serially or in lanes) gives the results and user rows of a replica that never crashed, and every row of a serial replica restarted at the same position. |
| `ApplierTest` | At-most-once retries, including after a restart; deletion of records, by count, by bytes and at start; results too large for a reply; rejections; atomic commits; detection of column id divergence; batches with duplicates and rejections; groups of entries, with copies and rejections in them; crash consistency of applies without the WAL, and of groups. |
| `SessionApplierTest` | Client sessions at apply: a write resent through other proposers, alone or in an entry with other sessions' writes, takes effect once, also after a restart; the session's floor refuses late copies; unknown and expired sessions are rejected and not recorded, a leader clock behind does not revive them, and expired sessions are deleted; two replicas, one restarting midway, give the same results. |
| `SessionWritesTest` (core) | The client's numbering: which writes are numbered (staged writes not, schema changes, `deleteRange` and batches yes), update ids forgotten once the update ended; a first copy refused by an expired session re-issued under a new one, a later copy so refused ending unknown, sessions that keep expiring ending at the deadline; opening retried after an unknown outcome, and again by a write that waited on another write's opening, also when that opening ended "not applied" at the opener's deadline or overloaded; batches unnumbered when the server does not advertise `numberedBatches`; batches sent as numbered chunks in order, a chunk resent with its number, a later chunk's failure reported as partly applied; closing ends waiting writes. |
| `ClientSessionTest` (in-process) | A merge and a transaction commit whose leader dies after its followers applied them and before it answered: the client sends them again with their request ids through the next node, the merge applies once and the commit answers `true` from its record. Copies of a commit and of the write that ends an update answer "unknown" while the first is in flight, then from the record. Copies of a numbered `mergeBatch` chunk, `deleteColumnIfExists`, `deleteColumn` and `deleteRange` take effect once; a numbered batch of two chunks is refused with nothing applied; the client library sends batches as numbered chunks over gRPC, a three-chunk batch of items that fill a chunk to the byte taking exactly one number per chunk (the client's and the server's split agree after encoding); a client batch whose leader dies after committing its first chunk, unanswered, resends that chunk with its number and every key is merged exactly once. |
| `CommitOutcomesTest` | The answer to a copy of a numbered commit whose transaction is gone: unknown while a mark of this term exists, also for a mark of a term that started while the copy's leadership barrier ran (the term is read before the barrier), "run it again" with no mark or a mark of an earlier term, the record when there is one. |
| `LeaderSessionTest` | Per-key update queue (order, release on commit, expiry also without other traffic, and end of leadership); chains (handoff at admission, the caps on links and on the readers of one, breaks and their end, a pause for a conflicting transaction, duplicates, expiry, close, no session monitor under the log lock); history kept for open transactions and in-flight commits, up to the key cap; the commit size limit of staged writes. |
| `PipelinedUpdatesTest` (in-process, flag on) | With the leader's apply stopped: chained updates see each other's values and nothing is acknowledged; wrong guards; plain writes, transactions and `deleteRange` against a chain; closes in mid-chain, and more of them waiting than a server's CONTROL workers and queue hold; refused operations on chained ids; second writes; merges and multi-key commits; caller gone, abandon; a snapshot and a restart inside a chain, around a rejected entry; a leader change with a chain in flight; a commit that ends without an outcome; the flag on and off at restarts, and a chained id handed to a leader without it; concurrent counters. With the flag off: the commit of an update, read back from the Raft log, has the old bytes, and a second write goes to the log. |
| `PipelinedLeaderKillTest`, `PipelinedPartitionTest`, `PipelinedRandomFaultTest` | The suites of the same name, with the nodes on `--pipelined-updates`. With `PipelinedUpdatesTest`, the only tests that chain in a default run: the suites below run the plain per-key queue unless the property below is set, and fail if an update chains there. After each fault these three wait for an update that chains. |
| `LeaderKillTest` | Repeated SIGKILL of the leader under concurrent writes. No acknowledged merge is lost or doubled, no acknowledged increment of a hot counter is lost, and replicas are byte-identical. Writes resume in about 1 s (the test only requires under 10 s). |
| `FollowerReadTest` (in-process, follower reads on) | A follower serves reads of writes acknowledged on the leader, with writes flowing meanwhile and on a quiet cluster (thanks to `CommitHeartbeats`), some by itself (the share is printed, not asserted beyond a loose bound: it depends on the disk and load); once it knows the commit index it serves by itself. The option is set on every node through the property. A follower whose database may hold an entry of a term later than its read index forwards; reads stay linearizable on the follower and the new leader across a leader change. A read on the leader whose apply is held does not answer the old value after a follower served the new one. A follower whose apply is held forwards instead of answering with the old value, and one cut off from the rest fails instead of answering. A follower names the leader in its responses, and a client of several nodes listing the follower first reads its writes and its transaction through it. |
| `FollowerReadLatencyTest` (in-process, prints numbers) | Read-after-write latency on a follower, with and without `CommitHeartbeats`, against forwarded reads with and without the leader's apply wait, quiet and with a writer in the background; with the heartbeats the follower serves more than half of the quiet reads and some of the busy ones itself (loose bounds; the printed shares are the measurement). The numbers in "Follower reads" come from it. |
| `ForwardingFailoverTest` | A client of a follower sees only retryable errors (point reads, range streams, writes) while the leader is killed, and every kind of request succeeds again after the election. |
| `ClientFailoverTest` | One client connection listing all three nodes, while the node it uses is killed (the leader, or a follower): only retryable errors, new calls succeed on another node of the same connection (about 1 s with an election, under 200 ms without), a new connection skips the dead node, and a transaction opened through a dead follower still commits. |
| `IsolatedNodeTest` | A client listing every node, connected to a node that gets isolated (the leader, or a follower), moves to the majority and keeps working, and the isolated node listens and serves again after the heal. With every node isolated all pause, and after the heal all resume and the client recovers by itself. An election shorter than the bound pauses no node. A follower restarted while cut off pauses too, and the client that lists it first moves. A cluster killed or stopped as a whole and restarted at once serves again, a paused node takes part in the election, and a node of a new cluster does not pause. A node that lost its database and kept its log pauses, and a client that starts during a cold start waits for the cluster, also past a frozen node. |
| `PartitionTest` | An isolated leader acknowledges nothing. The majority takes over. A leader that keeps its majority keeps serving. The history, hot counters included, is linearizable. |
| `PauseTest` | SIGSTOP of the leader and of a follower. The history stays linearizable. A freeze longer than `--jvm-pause-limit-ms` ends the node with exit code 3, and it starts again. |
| `ClusterIdentityTest` | Each genesis gets its own id, and a second `Genesis` in a log changes nothing. A node that elected itself under the same group id, added with Ratis' own client, stops before the configuration changes, and writes go on. What `add-peer` refuses. |
| `RatisContractTest` | The Ratis 3.3.1 behaviors that the design relies on beyond its API, against a real single-node server: the bootstrap and configuration meta files that `--join` safety reads, the open segment format that `RaftLogRepair` rewrites, the type of the full-pending-queue refusal, and a state machine refusal that reaches a remote client by class name and message, without a step down. |
| `RaftSettingsTest`, `ByteAdmissionTest`, `ConflictPositionsTest` | Snapshot chunks stay at most 4 s on the heap; Ratis' pending-request limits hold every node's attempts and stay near the defaults; one entry of the largest size fits every limit on its way. The byte admission never exceeds its limit and is sized from the heap. Conflict positions survive the exception that Ratis rebuilds, and a text that does not parse is a conflict of the whole entry. |
| `LeaderStallTest` | The rule of `LeaderStall`; follower next indices, which Ratis gives without ids, pair with ids only when the follower list read before and after them is the same; the bound that grows and comes back, and the rule of a log that stops, which a real leader with a stopped log worker follows within seconds. A graceful stop of the leader with one node down, followed by a restart at once, loses no reply and costs no step down. With one node down, the reply to a new leader's first entry is lost on a link, after the entry reached the follower's disk: nothing commits and the leader stays, until it steps down and a write gets through. The same loss after the first commit heals with the next write. A leader behind in applying does not step down. |
| `RollingRestartTest` | Graceful restart of every node, twice, under load. No failed operation. A leader restart stops writes for about 200–300 ms. |
| `MembershipTest` | A node started with `--join` replaces a lost leader, however long before its `add-peer` it started and after a restart in between, catches up from a snapshot, and restarts with or without the option. A node removed while it runs exits. Seeded bootstrap works. An empty node added right after a seeded start receives the seed through the snapshot taken after genesis, and never replays the seeded `Genesis`. A node of a seeded cluster is replaced whichever node leads. One step replacement of a lost leader, a lost follower, seeded or not, and of a leader that runs. `add-peer` refuses a node with a log of its own and a node started without `--join`, and fails at once without its node; a change without a majority ends at its limit; `--join` after a start without it is refused; `admin snapshot`; the warnings of a node that stays outside. |
| `ColdRestartTest` | SIGKILL of all three nodes under load, 5 cycles, plus a 2-of-3 restart. Nothing acknowledged is lost, and replicas are identical. |
| `RandomFaultTest` | A seeded random schedule of kills, partitions, pauses and restarts. Register histories, with read-modify-write updates and hot counters in them, are linearizable, and the transactional bank-transfer invariant holds. |
| `RaftLogRepairTest` | Segments written by Ratis' own output stream, torn at sector boundaries: torn entries are dropped, and Ratis then opens and appends to the log; anything else is refused and left unchanged: a damaged complete entry, a size that hides a valid entry, a gap or overlap between segments, a problem in a closed segment; a dry run changes nothing. A leader, and a follower that never acknowledged its last entry, refuse to start with a torn entry and rejoin after `repair-log`. So does a leader that loses a whole open segment after closed segments, a snapshot and a purge. Two or three nodes torn by one power cut, in entries no quorum confirmed, repair and elect a leader among themselves with every acknowledged write. No test interrupts a real device: the torn bytes are written by the test. |
| Component tests | Codecs (including fuzzing), `ConflictTracker` (checked against a reference model, also with a capped history; pruning in bounded steps, and appends and checks while a long prune runs), `ReadBarrier` (deposed and isolated leaders never pass; a second round starts while the first is in flight; a read index needs no apply), `LeaderHintRouter` (which calls go to the hinted leader, and when the hint is taken or dropped), the idle timeout of forwarded streams, `NodeStore`/`CheckpointStorage`, `LeaderForwarder` (leader lookups still waiting fail on close), fault proxy, linearizability checker. In the core project, `GrpcProxyCloseTest`: closes that answer late, on a proxy `GrpcServer`, take no worker, are not cancelled with their caller, and hold up shutdown after their caller left; the close of an update whose caller left while it opened is asynchronous too; a retry of either close, with no caller left, is no late failure. |

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
2. **Exactly-once for client retries** (client sessions, see "Writes, exactly once"). What Yotsuba must change:
   - `ReactiveGuard` retries `SERVER_OVERLOADED`. It must not retry `OUTCOME_UNKNOWN` as a new write: the core client
     already resent the write until its deadline, and a new copy could apply it twice. Treat it as a failed call
     whose effect is unknown (for a counter or an update, re-read before deciding), or give the call a longer
     deadline. `SESSION_EXPIRED` does not reach it (the core client handles it). `SERVER_OVERLOADED` and
     `READ_DEADLINE_EXCEEDED` from replicated writes now mean "not applied" and stay safe to retry.
   - The dead `cdcPollBatchAsync` override (item 1) must go before it compiles at all.
   - It must build its connection with every node (item 1): a single address has nowhere to resend to while its
     node is down, so its writes end with `OUTCOME_UNKNOWN` at their deadline.
   - Every node must run this version before clients use sessions: an older node cannot read a version 3 batch,
     a stamp or a `RegisterClient` entry. Start the clients after the upgrade, or with
     `-Dit.cavallium.rockserver.grpc.client.session-writes=false` until it is done.
   - Not done: writes staged in a transaction are not numbered (a resent `merge` in a transaction could stage it
     twice); a caller that cancels its future does not stop the resends; the rust client has the proto fields,
     `open_client_session` and `RequestContext::with_request_id`, but numbers and resends nothing by itself.
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
   - The log kept behind a snapshot is counted in entries only, and a failing snapshot is retried at every apply step. Each
     one stops the apply for its memtable flush (100–300 ms measured).
   - Iterator calls after a leader change fail with an internal "iterator not found" error, not `UPDATE_RETRY`.
   - The snapshot taken right after a seeded genesis is attempted once, on the leader only.
   - A SIGTERM that races a replica failure can exit with 0 instead of 3.
8. **Upstream.** Report the Ratis ReadIndex reuse bug, and switch back to Ratis ReadIndex once it is fixed
   upstream.
