# Replicated Rockserver: performance

How replicated Rockserver behaves under a Yotsuba-shaped load, what limited it, what changed, and how to
measure it again. Measured on one PC (32 cores, 123 GiB RAM) with the three nodes on three different disks,
standing in for three machines. Everything here ran locally; nothing touched production.

## The production workload

Rockserver's own operation timers in Grafana (InfluxDB `rocksdb_operation_timer`, database `yotsuba.local`,
7 days to 2026-10-30), per second at 1-minute resolution:

| Operation | Mean | p99 | Peak | Mean latency |
|---|---|---|---|---|
| `put` | 7.8k | 9.7k | 10.8k | 0.07 ms |
| `get` | 6.1k | 7.7k | 8.9k | 0.15 ms |
| `closeTransaction` (update commits) | 5.6k | 6.7k | 7.0k | 0.04 ms |
| `putMulti` | 2.1k | 8.2k | 10.5k | 2 ms |
| `closeFailedUpdate` | 0.3k | 0.4k | 2.0k | |
| `existsMulti` | 0.14k | 0.16k | 0.17k | 25 ms |
| `openTransaction` (explicit only) | 0.03k | 0.1k | 0.2k | |
| `getRange` | 2 | 25 | 161 | |

What Yotsuba does to produce it (source: `Yotsuba-private`, `MessagesImpl`, `SentMessageSenderIndex`,
`HistoryArchive`, `FileLocationsImpl`, `MonitorStore`):

- **Message read-modify-write**, about 5.9k/s: `get(forUpdate)` on `messages-v2` (key: 8-byte chat, 4-byte
  message), then `put(updateId)` with the whole serialized message (0.5–2 KiB), or `closeFailedUpdate` when
  nothing changed (about 5%). Each update commit is counted as one `closeTransaction`. Interaction-info updates
  of popular channel posts make some keys hot.
- **Sender index**, about 2.2k/s: `put(ensure)` of a 19-byte key without value.
- **History chunks**, about 1.8k/s: `putMulti`, mostly of one 1–3 KiB chunk.
- **File locations, monitors, invites**: small `putMulti` calls, `existsMulti` of 256 keys, explicit
  transactions of two `putMulti`.
- **Client limits**: one gRPC channel, at most 256 point reads and 256 writes in flight
  (`maxConcurrentReadRequests`, `maxConcurrentWriteRequests`).

`YotsubaWorkload` (in `distributed/server/src/test/.../bench`) replays this mix on the same column layouts,
with skewed chats so that hot keys conflict, at multiples of the production rate.

## The benchmark

`LoadBench` starts the servers as separate JVMs (4 GiB heap, ZGC each): either a 3-node cluster, every node on
its own disk, or one single-node Rockserver with the same settings. It preloads 1M messages, then runs
open-loop steps (every operation kind arrives as a Poisson process at `scale` × its production rate; latency is
measured from the planned start, so a stalled server shows as latency) or closed-loop steps (a fixed number of
clients back to back). The client allows 512 reads and 512 writes in flight by default, like Yotsuba with
some margin.

```bash
mvn -o -q -f distributed/pom.xml -pl server -am test-compile
distributed/bench/load-bench.sh --target replicated --root /path/on/a/real/disk/run1 \
    --node-dirs /disk1/bench,/disk2/bench,/disk3/bench --scales 1,2,4,8 --step-seconds 30
```

Other options: `--target single`, `--closed 64,512`, `--only UPDATE,GET` (or `BULK_PUT`, 1 MiB `putMulti`
calls for bandwidth), `--max-in-flight N`, `--route all` (clients spread over every node, so most requests are
forwarded), `--route failover` (spread over every node too, each connection listing every node, starting from a
different one), `--kill-leader-after S` (SIGKILL of the leader during the step, with a per-second timeline), `--jfr
profile` (or a `.jfc` file) to record every node. Each node logs its own timers every 10 s (`NodeMetrics`:
proposer queue and batch round trip, append-to-apply, apply phases, read barrier, update lock wait, log lags,
tracked keys, heap).

**Measure on a quiet machine.** Other processes' CPU and disk load moves these numbers a lot; compare variants
back to back, more than once. Process `write_bytes` is misleading for the Raft log: XFS charges about 2 MiB to
the process for each fdatasync of a zero-preallocated segment while the device writes about 4 KiB, so the
benchmark reports device writes (`/proc/diskstats`) instead.

## What limited it, and what changed

Measured at 1× production (10.6k operations/s) before any change: the cluster served about 1k operations/s
with 11–18 s write latency. In order of discovery:

1. **API threads blocked for the whole Raft round trip.** `GrpcServer` ran replicated requests on its workload
   scheduler (one platform thread per core), and each blocked for a full round trip, so at most 32 writes were
   ever in flight. Non-embedded connections now run each request on a virtual thread of its own (at most 16384
   at once), without the workload queues: admission happens on the leader. 700 → 7k writes/s.
2. **One fsync per write on followers.** Followers fsync once per AppendEntries call, and the leader kept 8
   calls of one entry each in flight. Two outstanding calls per follower make each call carry everything
   appended meanwhile. 8 clients: 0.8k → 3k writes/s.
3. **One Raft entry per write.** Every entry costs the Ratis append lock, a log write, its replication and an
   apply transaction. The `Proposer` now batches the requests that arrive together into one entry (up to 4096
   requests or 8 MiB, at most 32 batches in flight per queue), sent straight to the local Raft server on the
   leader. 29k → 93k writes/s (closed loop, `ensure` puts).
4. **Hot-key update conflicts.** With a 10–30 ms window between `getForUpdate` and its commit, concurrent
   updates of a popular post kept conflicting (1.08M retries in a minute at 4×), and Yotsuba backs off 10–250
   ms each time. Single-key updates now queue per key on the leader: each reads what the previous one
   committed. Retries went to zero.
5. **The RocksDB WAL duplicated the Raft log.** Entries are now applied without the WAL, in one local
   transaction per entry, with atomic flush so that a crash keeps a consistent prefix that Raft completes.
   Saturation at 4×: 15k → 25k operations/s; p99 at 1× from 300–400 ms to about 40 ms.
6. **Reads waited for the apply.** The read barrier waited until everything committed was applied (8–16 ms
   under load). Since a write is acknowledged only after the leader applied it, reads now wait only for the
   leadership confirmation (0.2 ms).
7. **The proxy scheduler's lock.** 13% of the leader's CPU was lock and condition traffic of the workload
   executor, plus virtual-thread churn; the direct proxy path gave 25–30% more throughput at 3×.
8. **Failover of forwarded requests.** A follower forwarding to a dead leader kept failing (or hanging in gRPC
   retries for up to 9 s) until the election. Reads (also range streams that returned nothing yet) and
   transaction opens are now re-sent to whichever node leads, over connections without gRPC's own retries that
   probe the port first.
9. **Dedupe records on the apply thread.** Each applied request wrote its record and, once the floor passed it,
   a point delete in a later entry's transaction: two of every three writes of a single put, each through the
   full `EmbeddedDB` path. In the fs1 profile, writing records and finishing entries (the deletes, the floor and
   position writes, two scans of a hash set of recorded sequences) took about a fifth of the apply thread's Java
   samples. Records below the floor are now deleted with one range deletion per proposer every 1024 sequences
   or 4 MiB of recorded results, and the recorded sequences are kept in a sorted set.
10. **Conflict checks nobody needed.** At 4× with 2048 writes in flight the leader's apply thread was the
    limit (about 81% busy, 37 entries waiting to apply on average). `perf` on that thread: 40% in the RocksDB
    commit, half of it validating every written key against the memtables, as an optimistic transaction does,
    although the apply thread is the only writer of those keys. Apply transactions now write untracked and read
    with `get` instead of `getForUpdate`: per entry, commit 0.22 → 0.12 ms and execute 0.40 → 0.35 ms.
11. **The embedded scheduler on the leader's reads.** The leader's local reads (a get, the read of an update, a
    single-key transactional write, opening native transactions) went through the embedded workload scheduler,
    whose queues cost 2.6–8.6 ms per request on average at 2–4×, and up to 1.4 s. Those requests were admitted
    by `GrpcServer` already: they now run straight on the database. Ranges, multi-key reads, multi-key
    transactional writes and iterator calls keep the scheduler; rollbacks run at once on the caller's thread.
    Local time 0.8–1.4 ms; `UPDATE` p99 at 2× from 876 to 232 ms.
    They run on virtual threads. A pool of four platform threads per core, tried to keep RocksDB calls (which
    pin a virtual thread's carrier) off the carriers, gave mixed results in back-to-back pairs: better tails
    with the leader on an NVMe disk, but with the leader on the SATA disk its local commands stalled for 2–3.6 s
    at times (against at most 0.9 s on virtual threads), once slow reads blocked all its threads.
    What starved carriers could hold up besides data reads was the Raft control plane: the proposer's replies
    and resends and the read barrier's rounds ran on virtual threads too. They now run on a few platform threads
    of their own (see "Threads" in [design.md](design.md)), so slow reads can at worst slow other data reads, not
    leadership confirmation or write replies. The local commands stay on virtual threads, one executor per node
    instead of one shared by every node of a JVM: a bounded platform pool is the configuration that stalled, and
    the control plane no longer depends on the carriers. **Not measured yet** (the control-plane move costs one
    hand-off per write reply, a few microseconds). To check it, and whether more carriers help, run back to
    back, more than once, on a quiet machine, with the leader on the SATA disk and then on an NVMe disk:
    `load-bench.sh --target replicated --root <dir> --node-dirs <three disks> --scales 1,2,4 --step-seconds 30`
    on the commit before this change and on this one, then on this one with `--jvm "-Xms4g -Xmx4g -XX:+UseZGC
    -XX:+AlwaysPreTouch -XX:+ExitOnOutOfMemoryError -Djdk.virtualThreadScheduler.parallelism=<2 × cores>
    -Djdk.virtualThreadScheduler.maxPoolSize=<4 × cores>"`. Compare throughput, `GET` and `UPDATE` p99/max, the
    `read.barrier` and `writes.batch` timers, and the longest local command (`local` max). Keep the default
    parallelism unless the larger one cuts the stalls without costing throughput.
12. **One commit per entry.** Each entry paid its own local transaction: open, commit (0.12 ms) and the floor
    and position writes. Entries are now handed to an apply thread of the state machine's own, which applies
    whatever queued up meanwhile, up to 256 entries, in one transaction, with a savepoint per entry for
    rejections; Ratis' thread only decodes and queues them, and a third thread completes the replies. Groups
    hold about 15 entries at 4×: 29.2k → 38.9k operations/s with 2048 writes in flight.
13. **One client connection, one event loop.** Yotsuba uses one gRPC channel, so one connection, which Netty
    serves on one event loop thread. With `directExecutor` that thread also ran every call's handler and
    started its virtual thread; against a replicated node, whose calls last milliseconds, one connection
    reached only about 21k operations/s, with an `UPDATE` p99 of 112 ms already at 1× (20 ms over four
    connections). In proxy mode the calls' callbacks now run on virtual threads: one connection at 1× has a
    p99 of 20 ms, at 2× 167 ms instead of 984 ms, and at 3× it serves 24.2k instead of 20.7k, limited by
    the client's 512 writes in flight like several connections. Single-node servers keep `directExecutor`.

14. **One update per key at a time.** A hot key's update waits for the previous update's fsync, replication and
    apply, so one key should see about 30–70 updates/s: an estimate from those latencies, not a measurement.
    `--pipelined-updates` (off by default, see [design.md](design.md)) hands the key on when the commit is admitted to
    the log instead, at the cost of one point read and one SHA-256 per chained commit on every replica, and of 8 bytes
    on the update commits of a pipelined leader plus about 60–100 on chained ones. With the option off, entries are
    unchanged. Not measured yet: `LoadBench --hot-keys` compares it on and off, with `update.keyHold`,
    `update.lockWait`, `update.guardRejected`, `update.chainConflict`, `transaction.conflict` and
    `transaction.chainConflict` in the node logs. In its report, `lockretry` counts the updates that failed because
    `getForUpdate` said to retry, such as a lock wait that ran out: they are errors, as they always were in this
    benchmark, while `retry` counts commits and no-change closes told to retry.
15. **Leaner apply.** Not measured yet; compare `apply.open`, `apply.execute` and `apply.commit` with the figures
    above. The apply thread writes through `EmbeddedDB.ApplyWriter` (a native transaction it holds itself) instead
    of the client API with a transaction id: no id lookup and lock, column use counting, action logging, operation
    counters or timers per write. `deleteRange` no longer flushes every memtable and syncs the WAL (hundreds of
    milliseconds on the apply thread, and small L0 files in every column). Keys and values are decoded as slices
    of the log entry instead of one array each, and merges pass their slice to RocksDB without a copy.
16. **Leader-side copies.** Not measured yet. A local write was copied into a body of its own at submit, then into
    the batch, and the leader then decoded the batch it had just encoded, on Ratis' request thread. Each entry is
    now written once, straight into the batch, from its size computed at submit; on the leader the encoder returns
    the requests as slices of the batch, and `startTransaction` takes them instead of decoding (see "Encoded once" in
    [design.md](design.md)). Batches from followers are still decoded on the leader, as before. Every node, leader
    or not, builds those requests (objects and slices, no byte copies), so that a request sent again is encoded from
    its first batch and not from its caller's buffers. Compare the
    `writes.batch` and `commits.batch` timers, the leader's CPU per operation and its allocation rate at 4×.

## Results

Suite `s2` (commit `cdca0b2`), 2026-10-30, on the PC described above, with other workloads on it at times;
the values marked * come from later runs that include `cd8904b`, and ** from runs at `a5b555e` (caller
deadlines; its background snapshots were later reverted). Results move by 10–15% with the node that wins the election: the one on the
SATA disk serves fewer operations at saturation than those on NVMe disks. Open loop, 30 s steps
after 10 s of warmup, 512 reads and 512 writes in flight unless noted, four client connections. Latencies are
from the planned start of each operation, so a client queue shows up as latency. `UPDATE` is the whole
`get(forUpdate)` plus commit.

| Setup | Load | Served ops/s | `UPDATE` p50 / p99 | `GET` p50 / p99 |
|---|---|---|---|---|
| Single node | 1× | 10.6k | 0.19 / 2.8 ms | 0.15 / 2.9 ms |
| Single node | 2× | 21.2k | 0.21 / 37 ms | 0.14 / 4.0 ms |
| Single node | 4× | 42.5k | 0.33 / 127 ms | 0.21 / 24 ms |
| Single node | 8× | 54.6k (saturated) | queued | 2.6 / 19 ms |
| Replicated, leader | 1× | 10.6k | 4.4 / 19 ms (5.8 / 16 ms**) | 0.28 / 1.8 ms |
| Replicated, leader | 2× | 21.2k | 11.9 / 105 ms (9.1 / 28 ms*, 11.7 / 33 ms**) | 0.33 / 5.4 ms |
| Replicated, leader | 4× | 24.7k (512 writes in flight) | queued | 1.1 / 8.8 ms |
| Replicated, leader, 2048 in flight | 4× | 37.4k (38.6k*) | queued | 1.8 / 21 ms |
| Replicated, leader, 2048 in flight | 8× | 36.7k | queued | 2.7 / 19 ms |
| Replicated, one connection* | 1× | 10.6k | 5.9 / 20 ms | 0.27 / 2.9 ms |
| Replicated, one connection* | 2× | 21.2k | 13.3 / 167 ms | 0.45 / 5.8 ms |
| Replicated, one connection* | 3× | 24.2k (512 writes in flight) | queued | 1.9 / 12 ms |
| Replicated, all nodes, 2 connections each | 2× | 24.5k (draining a backlog) | 1.7 s / 6.1 s | 1.4 / 8.0 ms |
| Replicated on tmpfs (no fsync cost) | 1× | 10.6k | 2.1 / 8.3 ms | |

Where a replicated write's time goes at 2× (leader metrics): the commit round trip (append, replication, the
fsync of the Raft log on the leader and one follower, the apply) is 10–16 ms on these consumer SSDs, which
write 80–100 MiB/s each meanwhile; on tmpfs it is about 2 ms. The read barrier costs 0.3–0.7 ms and the local
read of an update about 1 ms. Throughput is then in-flight writes divided by that latency: with 512 writes in
flight the cluster serves about 2.3× today's production mean, with 2048 about 3.5×, and the apply thread
(about 70% busy at 38k operations/s) is the next limit. Routing through followers adds a hop to each call
and the commit's forwarding, so with 512 writes in flight it saturates near 2×: clients should talk to the
leader, or have more writes in flight.

Before this work (commit `7493365`, suite `s1`), the replicated cluster had a p99 of 129 ms at 1× and 405 ms
at 2×, and served 33.2k operations/s at 4× with 2048 in flight; before batching, about 1k operations/s.

Other results:

- **Failover.** With clients spread over all nodes at 2× and the default election timeouts (1000:2000 ms), a
  leader killed with SIGKILL leaves about 3 s with no successful operation, then the new leader serves. With
  `--route all` the dead node's share of the client connections then keeps failing, a third of all requests.
  With `--route failover` those connections move to another node: in one short run on tmpfs on a loaded PC (1×,
  6 connections, a small dataset, not a throughput measurement) the second of the kill had 289 errors and the
  11 s after the 4 s gap had 1 error in 58k requests, against a third of the requests with `--route all`.
- **Bandwidth.** Sixteen clients writing 1 MiB `putMulti` calls: 183 MiB/s on one node, 103 MiB/s
  replicated, where each device writes 280–390 MiB/s (Raft log, flushes and compactions).

## Deployment notes

- **Disks.** Every write waits for an fsync of the Raft log on the leader and on one follower. Consumer SSDs
  without power-loss protection take 1–3 ms per fdatasync when idle and 10–30 ms when RocksDB compaction writes
  heavily to the same disk. Prefer SSDs with power-loss protection. Do not split the data directory over
  several disks: a node refuses to start if `data/db`, or a column volume under it, is on another file system
  (a volume outside `data/db` passes the start but makes every snapshot fail), and with
  `data/raft` on another disk every snapshot copies the whole database (instead of hard-linking it) while the
  apply waits. Keep `data/raft`, `data/db`, every column volume and the RocksDB WAL on one file system.
- **Power loss.** After any failure reading the Raft log at startup, stop the node, run
  `ReplicatedServerMain repair-log --data-dir DIR --dry-run` and follow its verdict, not the error message: a torn
  last write can show up as `Log entry corrupted` or as an `InvalidProtocolBufferException`, and damage it refuses
  can show up the same way or as `Read extra bytes`. If it would repair, run it again without `--dry-run` and start
  the node: the leader sends it what it lost. That is safe only if the dropped entry was never fsynced, as the
  command says, so disks that lose fsynced data void it (see Durability in the design document). If it refuses,
  replace the node under a new id. If too few nodes are left to elect a leader, which replacing a node needs, see
  "No quorum" in the design document: there is no override.
- **Disk space.** Each retained snapshot keeps the table files it links, also after compaction replaced them.
  Budget the database plus at least one older snapshot's compacted-away files, more while installs run.
- **Client concurrency.** Throughput is in-flight requests divided by latency. A replicated write takes
  5–15 ms instead of a fraction of one, so Yotsuba's 256 writes in flight cap it near 2× today's mean (512
  reach about 2.3×, 2048 about 3.5×). Raise `maxConcurrentWriteRequests` (and the read limit) for the cluster.
- **Talk to the leader.** A client connected to a follower has every update forwarded (its open and its
  commit), one more hop each. One connection is enough: its calls no longer share one event loop thread.
- **Client failover.** Create clients with every node's address (`GrpcConnection.forHostsAndPorts`, or
  `http://node-1:5333,node-2:5333,node-3:5333`), the expected leader first: without it, a dead node takes the
  clients pinned to it down with it. All the clients with the same list sit on its first node. After a leader change
  they stay there, but the nodes name their leader in every response, and the client sends the calls the leader
  serves straight to it, on a second channel, once that is connected (see "Routing" in the design document): only
  point reads still take the forwarding hop through what is now a follower, unless follower reads are on. This
  needs the clients to list the nodes by the host names of the cluster's `--peer` configuration. Not measured yet. A
  client stays on the node it failed over to until that node goes away. The benchmark's `--route failover` spreads the lists, so it does not measure this. A
  server option brings clients back to the first address, `max-connection-age-ms`, which is off by default. With it
  every client starts again from the top of its list at every age cycle, so a silent first address costs each of them
  the connect timeout at every cycle instead of once, and the grace of 10 minutes cuts any longer call, including
  `Compact` and long scans, on a healthy cluster too. A crash is noticed at once. A host that goes down or a process
  that freezes is noticed by keepalive, in at most 15 s (10 s of silence, 5 s for the answer), which is the price of
  not closing connections on shorter pauses; each such node before the first live one in the list then costs 3 s
  (the connect timeout), once, when the client moves past it. A node cut off from the Raft majority stops listening
  after 3 maximum election timeouts without a leader, at least 6 s (about 8 s in all by default), so its clients move,
  and listens again once it knows a leader. A hung disk is not detected. See the
  [README](../../README.md#client-connections).
- **Snapshots.** One every 500k entries (`--snapshot-every`), about every 100 s at 2×, or every 4 GiB of
  applied log entries (`--snapshot-every-bytes`), whichever comes first. A snapshot flushes the
  memtables and stops the apply for about 150–200 ms; the Raft log keeps up to twice the interval.
- **Memory.** In a 10-minute run at 2× the leader's heap stayed between 1.3 and 2.8 GiB (4 GiB heap). The
  bounds are larger: at most 256 MiB of committed entries wait for the apply thread, the leader holds up to
  65536 pending Raft requests (proposer batches of up to 4096 writes each) or 1 GiB of them, the Raft log write queue 64 MiB, every replica keeps every reply
  (up to 17 MiB each) in Ratis' retry cache for 8 s by default, and each node admits 16384 requests at once,
  with messages up to 512 MiB. Size the heap for bursts of large `putMulti` calls and large results: an
  `OutOfMemoryError` on the apply thread stops the replica.
- **Metrics.** On a cluster, `rocksdb_operation_timer` counts the calls made on each node's own database.
  Replicated writes are counted once per node when they are applied, together with the internal result, floor
  and position puts and one `closeTransaction` per applied group, and never with the client's latency. Reads,
  staged transactional writes, transaction opens, rollbacks and range calls are counted only on the leader,
  with their local database time only (no read barrier, forwarding or replication). The replication timers
  (`NodeMetrics`) are only in the log, every 10 s.
