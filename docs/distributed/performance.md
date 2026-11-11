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
calls for bandwidth, or `MERGE`, single merges of an 8-byte operand that each cost a read at apply; each key takes 64
merges only, so values stay under 600 bytes and the cost per merge does not drift during a run), `--merge-rate R`
(adds `MERGE` to the mix at R per second at scale 1; whenever it runs, every column gets the string-append merge
operator through `<root>/merge.conf`, unless a `--server-arg --rockserver-config` is given), `--delete-range-rate R`
(WA-1: adds `DELETE_RANGE` to the mix at R calls per second at scale 1, or `--only DELETE_RANGE` alone; each call
takes the next window of 16 keys of a skewed chat in the `range-deletes` column, fills it with a `putMulti`, waits,
and then empties it with a `deleteRange`, so a window belongs to one call, every delete removes live keys and the
column does not grow even when calls of a hot chat overlap; a call's latency is that of both writes in a row, and the
`range-deletes` column is created only in runs that have `DELETE_RANGE`, so other runs keep the earlier column set.
Not run yet: compare it with `--only HISTORY_PUT_MULTI` at the same rate for the cost of the deletes through the log
and at apply), `--max-in-flight N`, `--route all` (clients spread over every node, so most requests are
forwarded), `--route failover` (spread over every node too, each connection listing every node, starting from a
different one), `--route same-list` (every connection lists every node in one order, as production clients do,
starting from `--first follower|leader|ID`, a follower by default; with `--leader-hint listed|any|off` for the
client's hint mode, so that a run with `listed` and one with `off` show what the leader hint gains over forwarding
through a follower), `--kill-leader-after S` (SIGKILL of the leader during the step, with a per-second timeline), `--jfr
profile` (or a `.jfc` file) to record every node. Each node logs its own timers every 10 s, and exports them as
`rockserver.replication.*` (`NodeMetrics`:
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
    the control plane no longer depends on the carriers. Batches are also encoded (up to 8 MiB each) and
    submitted to Ratis on the proposer's submit threads, not on the 2–4 control threads, since a local submission
    can block in Ratis' append during a disk stall (R22): one more hand-off per batch on the leader, never per
    request, and none at all while every batch slot is taken. On a node that does not lead, a data batch goes
    straight to the sender's virtual threads, as its attempts did before, and is encoded there: no extra hand-off,
    but the encoding now waits for a carrier too, behind pinned database reads (R22-2). **Not measured yet** (the control-plane move costs one
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
    of the log entry instead of one array each, and merges pass their slice to RocksDB without a copy. Puts still copy
    key and value once, into the arrays of `putUntracked` (see "Core changes considered and not taken" in design.md).
16. **The conflict tracker under the append lock.** Not measured yet. `preAppendTransaction` registers every
    written key in `ConflictTracker`, under Ratis' append lock and the tracker's monitor. Each key used to allocate
    a record and a `HashMap` node there; keys are now `long` hashes in an open-addressing map and in one `long`
    array per entry. Pruning, on the completer thread after every applied entry, holds the monitor for at most
    4096 keys at a time instead of up to 64k. To check: JFR `jdk.JavaMonitorEnter` on `ConflictTracker` and
    allocation samples under `LoadBench` with transactions, before and after.
17. **Every column flushed with the hottest one.** Not measured yet. Replicas use atomic flush, so the hottest
    column reaching its 64 MiB `write-buffer-size` flushed every column with data, each into a small L0 file.
    Every column's limit is now the shared write buffer budget, so flushes come once per budget of writes to all
    columns together (see "RocksDB" under "Durability" in [design.md](design.md)). To check: L0 file count and size
    per column (`rocksdb.num-files-at-level0`, the SST table properties call) after a `LoadBench` run, before and
    after.
18. **Leader-side copies.** Not measured yet. A local write was copied into a body of its own at submit, then into
    the batch, and the leader then decoded the batch it had just encoded, on Ratis' request thread. Each entry is
    now written once, straight into the batch, from its size computed at submit; on the leader the encoder returns
    the requests as slices of the batch, and `startTransaction` takes them instead of decoding (see "Encoded once" in
    [design.md](design.md)). Batches from followers are still decoded on the leader, as before. Every node, leader
    or not, builds those requests (objects and slices, no byte copies), so that a request sent again is encoded from
    its first batch and not from its caller's buffers. Compare the
    `writes.batch` and `commits.batch` timers, the leader's CPU per operation and its allocation rate at 4×.
19. **One apply thread per replica.** Not measured yet (benchmarks are off for now). `--parallel-apply-threads N`
    (off by default) runs the data writes of each group on N lanes split by first key part, with one commit per
    group as before: see "Partitioned apply" in the design document. It parallelizes what costs on the apply
    thread (key encoding, reads for guards, previous values and merge checks, the merge operator, the index of
    the native batch); the commit (memtable inserts) and the records stay on one thread. Yotsuba's writes are
    single-key updates and single-key or one-owner multi-key puts, so nearly all of them run in lanes; the
    multi-column transactions (monitors, invites, about 0.03k/s) take the serial path. What can make it slower
    than the serial apply: rejected requests (`update.guardRejected`), each of which cuts its group
    (`apply.parallelCuts`), and the commits added between lanes and the serial path. Compare `apply`,
    `apply.execute` and `apply.commit` per entry with and without it, at 3.5× with 2048 in flight, where the
    apply was 70–81% busy.

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
  replicated, where each device writes 280–390 MiB/s (Raft log, flushes and compactions). Not profiled, and whether
  Yotsuba's bulk writers (file-location migration, history and photo-recovery archive chunks, chat imports) come near
  this rate is not measured: their call rates are known, their bytes per call are not (see "Known limits, decided"
  in the design document).

### Suite s3 (commit `f506d2a`)

Run on 2026-11-11, about 21:48 to 22:01, on the same PC, with nothing else running that the logs show (load average
2.3 at the start, 80 GiB of RAM available). Same suite definition as s2 (`final-suite.sh`), same options: open loop, 30 s
steps after 10 s of warmup, four client connections (two per node for `--route all`), 512 reads and 512 writes in
flight unless noted. Raw results: `/home/cavallium/rockserver-bench/s3-*/results.md` and `s3-*.log`. Each number below is
copied from those files. "Not reported" means the benchmark does not print it. At the saturated steps (4× and 8×
replicated, 8× single) latencies of the write operations are the client queue's, as in s2 (the planned start is the
clock), and p50 of seconds is a backlog, not a service time. The size of that backlog is set by the benchmark, not by
the server: `LoadBench` keeps at most 100000 requests waiting for one of its in-flight slots and drops the rest
("dropped (client queue full)"), so a saturated step's p50 is about 100000 divided by the served rate (100000 / 27738
= 3.6 s for 3.83 s measured at 4× replicated, 100000 / 67790 = 1.5 s for 1.59 s at 8× single, 100000 / 39253 = 2.5 s
for 2.87 s at 8× with 2048 in flight). See "Reading the saturated steps and the failover" below.

**Disks, and what differs from s2 there.** In s3 the nodes' data directories are on three different devices:
`df` after the suite says node 1 (`/home/cavallium/.rockserver-bench`) is on `/dev/nvme1n1p1`, node 2
(`/var/tmp/rockserver-bench`) on `/dev/nvme0n1p4`, node 3 (`/media/datissd/rockserver-bench`) on `/dev/sdb1` (SATA),
and the replicated runs' device writes show all three busy (at 1× 36.2, 35.3 and 35.7 MiB/s on `nvme0n1`, `nvme1n1`,
`sdb`). So nodes 1 and 2 were not on the same partition in s3. The s2 logs hold no `df`; they suggest the opposite
for s2: at 1× its replicated run wrote 65.4 MiB/s on `nvme0n1`, 34.1 on `nvme1n1` and 37.0 on `sdb`, about twice the
others on one NVMe, which fits two nodes sharing it, and its single-node run (directory of node 1) wrote on `nvme0n1`
where s3's wrote on `nvme1n1`. That is an inference from `/proc/diskstats`, not a check, and the machine rebooted in
between (s3's boot is about 20 h 59 min old at 22:03). If it holds, s2's leader (node 2) shared a disk with node 1 and
s3's did not, which favors s3. The leader also differs: node 2 in every s2 replicated run, node 1 in `s3-repl` and
`s3-repl-2048` (a NVMe disk), node 3 in `s3-bw-repl` (the SATA disk); in `s3-route-all` the heaviest node (CPU) was node 3.
s2 says results move 10–15% with the elected node.

**Single node** (`s3-single`; preload 1000000 messages in 1.1 s, 937117 keys/s, 546.9 MiB/s; node RSS 7863 MiB):

| Load | Served ops/s (offered) | `UPDATE` p50 / p99 / p999 ms | `GET` p50 / p99 / p999 ms | Dropped | Server cores | Device writes MiB/s |
|---|---|---|---|---|---|---|
| 1× | 10627 (10614) | 0.18 / 0.34 / 0.75 | 0.13 / 0.24 / 0.48 | 0 | 2.0 | `nvme1n1` 19.5 |
| 2× | 21259 (21228) | 0.17 / 0.98 / 18.43 | 0.12 / 0.40 / 2.61 | 0 | 2.8 | `nvme1n1` 50.3 |
| 4× | 42440 (42456) | 0.20 / 55.14 / 104.77 | 0.13 / 4.37 / 14.86 | 0 | 4.6 | `nvme1n1` 121.6 |
| 8× | 67790 (84912), saturated | 1591.30 / 2232.32 / 2467.84 | 2.37 / 14.24 / 32.16 | 476353 | 6.8 | `nvme1n1` 223.5 |

Errors 0 at every step. `UPDATE` retries (commits told to retry): 4, 16, 281, 10657. No `lockretry`.

**Replicated, 512 writes in flight** (`s3-repl`, leader node 1; node RSS 1=7502, 2=6958, 3=6916 MiB):

| Load | Served ops/s | `UPDATE` p50 / p99 / p999 ms | `GET` p50 / p99 / p999 ms | Dropped | Cores (nodes 1, 2, 3) | Device writes MiB/s (`sdb`, `nvme0n1`, `nvme1n1`) |
|---|---|---|---|---|---|---|
| 1× | 10601 | 4.99 / 16.04 / 28.85 | 0.23 / 2.87 / 5.80 | 0 | 3.0, 0.6, 0.6 | 35.7, 36.2, 35.3 |
| 2× | 21207 | 10.82 / 51.74 / 139.65 | 0.26 / 5.87 / 9.57 | 0 | 6.0, 1.3, 1.3 | 90.8, 91.1, 76.6 |
| 4× | 27738 (saturated) | 3833.86 / 4628.48 / 4685.82 | 0.95 / 11.68 / 21.42 | 472468 | 6.6, 1.1, 1.1 | 123.9, 124.2, 102.1 |
| 8× | 26411 (saturated) | 4272.13 / 4694.02 / 4976.64 | 1.48 / 12.73 / 24.77 | 2238410 | 6.5, 1.0, 1.1 | 122.4, 112.9, 122.7 |

Errors 0 at every step. The saturation point is between 2× (21207 served, p99 51.74 ms) and 4× (27738 served, a client
queue of about four seconds).

**Replicated, 2048 writes in flight** (`s3-repl-2048`, leader node 1; node RSS 1=7380, 2=6564, 3=6661 MiB):

| Load | Served ops/s | `UPDATE` p50 / p99 / p999 ms | `GET` p50 / p99 / p999 ms | Dropped | Cores (nodes 1, 2, 3) | Device writes MiB/s (`sdb`, `nvme0n1`, `nvme1n1`) |
|---|---|---|---|---|---|---|
| 4× | 42725 (offered 42456) | 21.70 / 154.88 / 1087.49 (max 2699.3) | 0.62 / 7.36 / 14.02 | 0 | 12.0, 2.1, 2.0 | 153.7, 163.7, 150.3 |
| 8× | 39253 (saturated) | 2865.15 / 3958.78 / 6705.15 | 2.88 / 22.02 / 31.70 | 1680934 | 9.1, 1.6, 1.5 | 157.5, 176.4, 165.4 |

Errors 0. At 4× the cluster kept up with the offered load in this run (nothing dropped); the saturation point with 2048
in flight is between 4× and 8×.

**All nodes, 2 connections each, 2×** (`s3-route-all`; preload 1000000 messages in 5.8 s, 172250 keys/s, 100.5 MiB/s;
node RSS 1=5836, 2=5839, 3=6332 MiB): 21609 served, 16257 dropped, errors 0. `UPDATE` 4186.11 / 5562.37 / 5607.42 ms
(p50 / p99 / p999), `GET` 4.46 / 128.45 / 210.43 ms (max 352.8). Cores 3.1, 3.1, 5.8; device writes `sdb` 77.0,
`nvme0n1` 74.4, `nvme1n1` 77.0 MiB/s. Offered load of 21228 operations/s was not sustained: the cluster is behind by about
four seconds, as with `--route all` in s2.

**Failover** (`s3-failover`, same setup as `s3-route-all`, SIGKILL of the leader, node 1, at second 10; preload 4.3 s,
230919 keys/s, 134.8 MiB/s). Node 2 logged `leading term 2`. The log has no timestamp for the election and the benchmark
does not report the time without a leader. The per-second timeline (successful operations per second; `ok/errors`)
from the kill second on is: 29103/11, 0/0, 0/0, 2306/346, 648/90, 0/0, 0/0, 3/7, 161/86, 127/71, 116/83, 466/212,
126/61, 0/0, 1/3, 75/37, 158/83, 88/94, 0/133, 0/132. So there are two seconds with nothing (seconds 11–12), a
trickle of 2306 and 648, and then no recovery to the 24k–33k per second before the kill within the 20 s that follow:
at most 466 operations/s succeed, and the last two seconds have none. In the step table the whole step served 11095 ok/s
(mean over 30 s), 315695 requests were dropped by the client queue, and errors totalled 1449 (`grpc-UNAVAILABLE` 367 on
`EXISTS_MULTI`, 108 on `FIRST_LAST`, 509 on `GET`, 10 on `RANGE_PAGE`, 4 on `MONITOR_TX`; on `UPDATE` 444
`grpc-UNAVAILABLE` and 7 `UPDATE_RETRY`). The per-operation write rows (`ENSURE`, `HISTORY_PUT_MULTI`,
`FILE_PUT_MULTI`) show 0 errors, with p50 about 2.8 s, p99 5.77 s and p999 5.78 s (max 11739.1 ms on `ENSURE`). The log's error
legend also lists `OUTCOME_UNKNOWN` ("the write was sent, and no answer settled its outcome before its deadline"),
`CLIENT_CLOSED` and a `StatusRuntimeException: UNKNOWN: Uncaught exception in the SynchronizationContext`, without
counts: **the report does not say how many writes ended with an unknown outcome, and does not check lost or duplicated
writes (not reported).** Server cores `{1=-4.5, 2=2.2, 3=2.0}` and RSS `{1=-1, 2=5856, 3=5624}`: the negative values are
the killed node's. After the kill node 2 applied only 1 to 545 entries per 10 s (before: thousands), so what the
timeline shows is that the clients did not reach the new leader, not that it was slow.

**Bulk writes** (16 clients, 1 MiB `putMulti`, closed loop, 20 s after 5 s of warmup, 1000 chats × 10 messages):

| Run | MiB/s | p50 / p99 / p999 ms (max) | Server cores | Device writes MiB/s | Node RSS MiB |
|---|---|---|---|---|---|
| `s3-bw-single` | 218.59 (218 calls/s) | 8.74 / 1051.65 / 1396.74 (1396.7) | 0.9 | `nvme1n1` 520.7 (`nvme0n1` 0.1, `sda` and `sdb` 0.0) | 5959 |
| `s3-bw-repl`, leader node 3 | 122.73 (123 calls/s) | 116.16 / 343.04 / 393.98 (394.0) | 0.6, 0.7, 1.4 | `sdb` 357.7, `nvme0n1` 437.0, `nvme1n1` 318.7 | 5356, 5404, 5531 |

Errors 0 in both. The single node's rate swings between 14 and 595 calls/s from second to second.

**s3 against s2** (same step, same metric; s2 from `/home/cavallium/rockserver-bench/s2-*/results.md`, commit
`cdca0b2`, 2026-10-30; the change is s3 against s2 and only the cells that move by more than about 10% are worth reading,
given the caveats below). In s2 `repl` and `repl-2048` the leader was node 2, in s3 node 1.

| Run | Step | Metric | s2 | s3 | Change |
|---|---|---|---|---|---|
| single | 1× | served ops/s | 10631 | 10627 | 0% |
| single | 1× | `UPDATE` p50 / p99 / p999 ms | 0.19 / 2.80 / 26.54 | 0.18 / 0.34 / 0.75 | -5% / -88% / -97% |
| single | 1× | `GET` p50 / p99 / p999 ms | 0.15 / 2.92 / 26.18 | 0.13 / 0.24 / 0.48 | -13% / -92% / -98% |
| single | 2× | served ops/s | 21247 | 21259 | 0% |
| single | 2× | `UPDATE` p50 / p99 / p999 ms | 0.21 / 36.70 / 205.44 | 0.17 / 0.98 / 18.43 | -19% / -97% / -91% |
| single | 2× | `GET` p50 / p99 / p999 ms | 0.14 / 4.00 / 21.97 | 0.12 / 0.40 / 2.61 | -14% / -90% / -88% |
| single | 4× | served ops/s | 42461 | 42440 | 0% |
| single | 4× | `UPDATE` p50 / p99 / p999 ms | 0.33 / 127.30 / 161.54 | 0.20 / 55.14 / 104.77 | -39% / -57% / -35% |
| single | 4× | `GET` p50 / p99 / p999 ms | 0.21 / 24.00 / 61.18 | 0.13 / 4.37 / 14.86 | -38% / -82% / -76% |
| single | 8× | served ops/s | 54598 | 67790 | +24% |
| single | 8× | `UPDATE` p50 / p99 ms | 1953.79 / 3145.73 | 1591.30 / 2232.32 | -19% / -29% |
| single | 8× | `GET` p50 / p99 ms | 2.59 / 19.04 | 2.37 / 14.24 | -8% / -25% |
| repl | 1× | served ops/s | 10625 | 10601 | 0% |
| repl | 1× | `UPDATE` p50 / p99 / p999 ms | 4.40 / 18.69 / 29.95 | 4.99 / 16.04 / 28.85 | +13% / -14% / -4% |
| repl | 1× | `GET` p50 / p99 / p999 ms | 0.28 / 1.75 / 3.84 | 0.23 / 2.87 / 5.80 | -18% / +64% / +51% |
| repl | 2× | served ops/s | 21217 | 21207 | 0% |
| repl | 2× | `UPDATE` p50 / p99 / p999 ms | 11.93 / 104.90 / 174.46 | 10.82 / 51.74 / 139.65 | -9% / -51% / -20% |
| repl | 2× | `GET` p50 / p99 / p999 ms | 0.33 / 5.42 / 9.28 | 0.26 / 5.87 / 9.57 | -21% / +8% / +3% |
| repl | 4× | served ops/s | 24685 | 27738 | +12% |
| repl | 4× | `UPDATE` p50 / p99 ms | 4333.57 / 5218.30 | 3833.86 / 4628.48 | -12% / -11% |
| repl | 4× | `GET` p50 / p99 / p999 ms | 1.06 / 8.81 / 18.27 | 0.95 / 11.68 / 21.42 | -10% / +33% / +17% |
| repl | 8× | served ops/s | 27245 | 26411 | -3% |
| repl | 8× | `UPDATE` p50 / p99 ms | 4067.33 / 5943.30 | 4272.13 / 4694.02 | +5% / -21% |
| repl | 8× | `GET` p50 / p99 / p999 ms | 1.52 / 9.34 / 17.66 | 1.48 / 12.73 / 24.77 | -3% / +36% / +40% |
| repl, 2048 | 4× | served ops/s | 37374 | 42725 | +14% |
| repl, 2048 | 4× | `UPDATE` p50 / p99 / p999 ms | 2938.88 / 3895.30 / 6737.92 | 21.70 / 154.88 / 1087.49 | -99% / -96% / -84% |
| repl, 2048 | 4× | `GET` p50 / p99 / p999 ms | 1.83 / 20.58 / 32.10 | 0.62 / 7.36 / 14.02 | -66% / -64% / -56% |
| repl, 2048 | 8× | served ops/s | 36735 | 39253 | +7% |
| repl, 2048 | 8× | `UPDATE` p50 / p99 / p999 ms | 3149.82 / 4145.15 / 6451.20 | 2865.15 / 3958.78 / 6705.15 | -9% / -4% / +4% |
| repl, 2048 | 8× | `GET` p50 / p99 / p999 ms | 2.71 / 18.62 / 28.38 | 2.88 / 22.02 / 31.70 | +6% / +18% / +12% |
| route-all | 2× | served ops/s (dropped) | 24505 (21016) | 21609 (16257) | -12% |
| route-all | 2× | `UPDATE` p50 / p99 / p999 ms | 1745.92 / 6127.62 / 6148.10 | 4186.11 / 5562.37 / 5607.42 | +140% / -9% / -9% |
| route-all | 2× | `GET` p50 / p99 / p999 ms | 1.39 / 7.95 / 31.25 | 4.46 / 128.45 / 210.43 | +221% / +1516% / +573% |
| failover | 2× | served ops/s (step mean) | 16976 | 11095 | -35% |
| failover | 2× | errors / dropped | 109757 / 57272 | 1449 / 315695 | |
| failover | 2× | seconds with 0 ok after the kill | 3 (seconds 11–13) | 7 of the 20 after it, never back to the pre-kill rate | |
| bw-single | 16 clients | MiB/s; p50 / p99 / p999 ms | 183.12; 12.65 / 1000.96 / 1252.35 | 218.59; 8.74 / 1051.65 / 1396.74 | +19%; -31% / +5% / +12% |
| bw-repl | 16 clients | MiB/s; p50 / p99 / p999 ms | 103.21; 144.13 / 299.78 / 364.54 | 122.73; 116.16 / 343.04 / 393.98 | +19%; -19% / +14% / +8% |

Other s2 and s3 figures side by side (not percentages): server cores at 1× single 1.8 and 2.0, replicated leader 3.2 (node 2)
and 3.0 (node 1); node RSS, single 7640 and 7863 MiB, replicated (1, 2, 3) 6908, 7673, 6724 and 7502, 6958, 6916 MiB.
Device writes in s2 and s3 runs are totals for the device over all processes and are not comparable for the single
node at 1× (115.4 and 19.5 MiB/s, on different devices and with other workloads on the PC in s2).

**Improvements above about 10%** (s3 against s2):

- Single node tails: `UPDATE` and `GET` p99 at 1× and 2× are 88–97% lower (for example `UPDATE` p99 at 2× 36.70 to 0.98 ms),
  p99 at 4× 57% (`UPDATE`) and 82% (`GET`) lower; throughput at 8× +24% (54598 to 67790 ops/s) with 476353 requests
  dropped against 993004. Medians at 2× and 4× are 14–39% lower. The s2 single-node tails were the ones the old
  runs said came from "other workloads" on the PC, so the size of this gain is not a measurement of the code.
- Replicated, 2048 in flight, 4×: 42725 against 37374 ops/s (+14%) with nothing dropped (146045 before), `UPDATE` p50 / p99
  21.70 / 154.88 ms against 2938.88 / 3895.30 ms. s2 had a backlog at this step, s3 had none. Node 1 was the leader and the
  nodes were on three devices, so part of this is the layout (see above).
- Replicated, 512 in flight: `UPDATE` p99 at 2× 51.74 against 104.90 ms (-51%) and at 1× -14%, throughput at 4× +12%
  (27738 against 24685; both saturated). Against the later s2-era runs of the same step in the table above (28 and 33 ms
  at 2×, `cd8904b` and `a5b555e`) s3 is slower, so this is a gain only against the suite's own s2.
- `GET` p50 is 10–21% lower at 1×, 2× and 4× (replicated) and 66% lower at 4× with 2048 in flight.
- Bulk writes: 218.59 against 183.12 MiB/s on a single node (+19%) and 122.73 against 103.21 MiB/s replicated (+19%),
  with a lower median (-31% and -19%).

**Regressions above about 10%**:

- **Failover.** Worse than s2 as measured, and the result of the suite to look at first: s2 had 3 seconds with no successful
  operation and then mostly 8k–24k per second; s3 has two empty seconds, then at most 2306 per second and 7 of the 20
  seconds after the kill empty, the last two included, with 315695 requests dropped (57272 in s2) and no recovery.
  Fewer errors counted (1449 against 109757) does not mean better: the writes of the dead leader's connections do not
  appear as errors in the per-operation rows and their outcome is not reported. The other variable is which node died:
  node 2 in s2 (on `/var/tmp`), node 1 in s3 (on `/home`, also the leader's disk of `s3-repl`). The likely cause is
  in the client and the benchmark, a change of behaviour between the suites, not a slower election (see "Reading the
  saturated steps and the failover" below): as it is, this run no longer measures the cluster's failover.
- **`--route all` at 2×**: served 21609 against 24505 ops/s (-12%), `UPDATE` p50 4186.11 against 1745.92 ms
  (+140%), and `GET` p99 128.45 against 7.95 ms (+1516%; p50 4.46 against 1.39 ms, +221%). In s3 the leader was the
  SATA node 3 by CPU use (5.8 cores), in s2 node 2 (7.8 cores).
- Replicated `GET` p99 at 1× 2.87 against 1.75 ms (+64%), at 4× 11.68 against 8.81 ms (+33%) and at 8× 12.73 against
  9.34 ms (+36%); p999 up 51%, 17% and 40%. Also `GET` p99 +18% and p999 +12% at 8× with 2048 in flight. The `GET` p99 at
  2× is within 8%. The absolute values are 3–13 ms.
- Replicated `UPDATE` p50 at 1× 4.99 against 4.40 ms (+13%).
- Bulk writes: p99 +14% replicated (343.04 against 299.78 ms) and p999 +12% single (1396.74 against 1252.35 ms).

Every other cell is within 10%: served ops/s at 1× and 2× in every run, and at 4× single (42440 against 42461).

**Caveats.** One run per cell, on a shared PC (the s2 runs also had other workloads on it at times, s3 had load average
2.3 at the start and the suite's own load after that). The leader node, and with it its disk (NVMe or SATA), differs
between the suites, and the s2 and s3 disk layouts probably differ too (see above); s2 itself says results move by
10–15% with the elected node. Single-run tails at saturation and in the failover are noisy. Treat differences under
about 15% as noise, and the large ones (single-node tails, failover) as a reason to repeat the run, not as a
measurement of the code change alone. Node 1 and node 2 were not on the same partition in s3, contrary to what was
assumed before this analysis (see "Disks" above).

**Reading the saturated steps and the failover.** Two results look like a problem under high load. Both readings
below come from the code and the logs, without a new run.

- *Multi-second p50 with requests dropped at saturation.* This is the load generator's backlog, as designed, and the
  server never saw the excess. The client holds at most 512 (or 2048) writes in flight, like Yotsuba's limit, so the
  cluster is offered no more than that window whatever the planned rate: no step of the six runs without a kill has an
  error, so no request was refused with `SERVER_OVERLOADED` and the nodes' own admission (16384 requests at once, the
  byte admission of writes) was never reached. Requests beyond the window wait in the benchmark, up to 100000 of them,
  and their wait counts from the planned start: the p50 is that queue's length over the served rate (see the top of
  this section). With the window full a write took about 19 ms at 4× with 512 in flight (512 over about 26.7k
  writes/s), and `GET`, which has a window of its own, kept a p50 of 0.95–2.88 ms and a p99 of 11.68–22.02 ms in every
  saturated replicated step. So these numbers say where the capacity ends (between 2× and 4× of the production mean
  with 512 writes in flight, between 4× and 8× with 2048), not that overload is shed too late. What they do not show
  is how the nodes behave when the concurrency offered exceeds their own limits (many clients, or windows far larger
  than 2048): no step of the suite reaches the server's admission, so shedding by the server is covered by tests
  only, not by a load run. A caller with a bounded window and a queue in front of it, as Yotsuba has, builds the same
  backlog while the load stays above capacity; bounding that queue, or the wait in it, is the caller's job.
- *Failover with no recovery.* After the kill the cluster was healthy and idle: node 2 logged `leading term 2`, node 3
  followed with no lag, and in one 10 s interval node 2 applied 3 entries and served 396 local reads. What stalled was
  the benchmark. With `--route all` every connection has one address, and a third of them pointed at the dead node. The
  client's numbered writes (`SessionWrites`, added after s2, in `a1120d2`) are sent again until the caller's deadline,
  30 s here, where in s2 they failed at once, and a connection with one address has nowhere else to send them. Each of
  those writes keeps one of the 512 write slots, which all the connections share, until the step ends, so after about
  1500 writes every slot is held by a write to the dead node and the two live nodes get none. The log fits: the write
  rows have 0 errors (those writes were still waiting when the step ended, which is also why their outcome is not
  reported), the successes that remain are about twice the errors second by second (reads, a third of which go to the
  dead node and fail), and what trickles through are updates whose first call failed `UNAVAILABLE` and gave its slot
  back (444). In s2 the dead node's share of the requests failed fast and the rest were served. So this is the
  documented limit of a client pinned to one node ("a dead node takes the clients pinned to it down with it", see
  "Client failover" below), made worse for a caller with a shared window, since its writes to the dead node now wait
  for their deadline. A production client lists every node and is not in this case. To measure failover again, the
  suite's failover run has to use `--route failover` or `--route same-list`. Both were run right after the suite
  (2026-11-11, commit `670ce11`, 2× load, leader killed at second 10 of 30, results in `s3-failover-samelist` and
  `s3-failover-rot`): one second with no successful operation, the offered rate served again 3 to 4 s after the
  kill, then up to 33744 ops/s while the backlog cleared; 232 and 196 failed updates, nothing dropped at the client
  queue; `UPDATE` p99 2705 ms and 1957 ms, `GET` p99 1852 ms and 1113 ms over the step (the outage itself). The
  suite script's failover step now uses `--route same-list`. Not checked: whether a
  connection with one address should give up a numbered write sooner when its only node refuses connections (it
  cannot know that the node will not come back), and what the reads' own retries add to the stall.

**Odd in the s3 logs.**

- Failover: no recovery after the election (above, and "Reading the saturated steps and the failover"). Besides the errors listed there, the client log has 42 lines
  `gRPC call to ... reached terminal status UNAVAILABLE. No further automatic retry will be attempted` (24 in s2) and, at
  21:59:50, after the step, `SEVERE: RuntimeException while executing runnable CallbackListener ...
  RejectedExecutionException` from `ThreadPerTaskExecutor.ensureNotShutdown` (client closing while calls were still
  completing; it appears at shutdown, so it is probably harmless, but it is a client-side error in the log). Node 2's
  stderr, after the kill: 52 `Connection refused: /127.0.0.1:32959` stack traces from `GrpcLogAppender` (node 1's port),
  `Failed appendEntries` repeated about 6 times per 5 s, and at shutdown `leadership transfer to 3 failed`.
- `s3-repl-2048` 4×: `UPDATE` p999 1087.49 ms and max 2699.3 ms while p99 is 154.88 ms: a tail event in the 30 s
  step, one run. Leader node 1 used 12.0 cores, the highest of any step.
- `s3-route-all`: the CPU was on node 3 (5.8 cores) while its device wrote like the others; the step never caught up.
- Single node 8× has a throughput swing of 41282 to 82865 ok/s from second to second; the bulk single node 14 to 595
  calls/s.
- **Disk space alarm on node 1**: `NodeMonitor` logged `ALARM disk-space: 55.4 GiB free of 931.1 GiB` (49.2 GiB at the
  lowest, in `s3-repl`; 51.1 GiB in `s3-repl-2048`) for node 1's data directory in `s3-repl`, `s3-repl-2048`, `s3-route-all`, `s3-failover` and
  `s3-bw-repl`: `/home/cavallium` was 95% full. Node 1 was the leader in `s3-repl` and `s3-repl-2048` and the killed node
  in `s3-failover`. A nearly full NVMe disk can slow writes; this is not measured here.
- `s3-repl` node 3 logged one `ERROR ... RaftServer$Division - 3@group-...: Failed appendEntries* 1->3 ... HEARTBEAT`
  (at startup, while the cluster formed). The nodes' only other startup noise is `Connection refused` traces (2 per
  node in several runs: a peer not yet listening), the JDK's `sun.misc.Unsafe` and incubator-module warnings, and the
  `java.io.tmpdir` is outside the data directory warning. Snapshots were taken at index about 500k in 2 to 3 runs (114
  to 138 ms); nothing else warns.
- The negative `server cores` and `-1` RSS of the killed node in `s3-failover` are the benchmark's marker for a dead
  process, as in `s2-failover`.

## Deployment notes

- **Disks.** Every write waits for an fsync of the Raft log on the leader and on one follower. Consumer SSDs
  without power-loss protection take 1–3 ms per fdatasync when idle and 10–30 ms when RocksDB compaction writes
  heavily to the same disk. Prefer SSDs with power-loss protection. Do not split the data directory over
  several disks: a node refuses to start (exit 1) if anything under `data/db`, a column volume included, is on
  another file system, if a column volume or the RocksDB `wal-path` is outside `data/db` (a volume there would make
  every snapshot fail), or if `data/raft` is on another file system than the data directory (every snapshot would
  copy the whole database instead of hard-linking it, while the apply waits). Keep `data/raft`, `data/db`, every
  column volume and the RocksDB WAL on one file system (see "Disk layout" in the design document).
- **Power loss.** After any failure reading the Raft log at startup (the failed start prints the command), stop the
  node, run `ReplicatedServerMain repair-log --data-dir DIR --dry-run` (with the node's `--group-id` if it has one)
  and follow its verdict, not the error message: a torn
  last write can show up as `Log entry corrupted` or as an `InvalidProtocolBufferException`, and damage it refuses
  can show up the same way or as `Read extra bytes`. If it would repair, run it again without `--dry-run` and start
  the node: the leader sends it what it lost. That is safe only if the dropped entry was never fsynced, as the
  command says, so disks that lose fsynced data void it (see Durability in the design document). If it answers `OK`,
  the log is not torn and the start failed for another reason (an I/O error, a permission, memory): repairing would
  change nothing. If it refuses because the log is damaged, replace the node under a new id; a refusal for a missing
  directory (a wrong `--data-dir` or `--group-id`) or a running node says nothing about the log. The verdict is the
  line that starts with `OK:`, `WOULD REPAIR`, `REPAIRED:` or `REFUSED:`, not always the last one (a repair is followed
  by the assumption above); a stack trace instead of a verdict means the command could not read the log (a
  permission, a disk error): fix that first. From the repair itself, not the dry run, a stack trace can come after
  the file was cut (the sync or the check after it failed): run the dry run again to see where the log stands. If too few nodes are left to elect a leader, which replacing a node needs, see
  "No quorum" in the design document: there is no override. The runbooks have it step by step ("Node does not start
  after a power loss").
- **Disk space.** Each retained snapshot keeps the table files it links, also after compaction replaced them.
  Budget the database plus at least one older snapshot's compacted-away files, more while installs run.
- **Client concurrency.** Throughput is in-flight requests divided by latency. A replicated write takes
  5–15 ms instead of a fraction of one, so Yotsuba's 256 writes in flight cap it near 2× today's mean (512
  reach about 2.3×, 2048 about 3.5×). Yotsuba's `replicated-rockserver` branch raises both limits to 1024 when
  `database.rockserver.nodes` lists several nodes (`456b8f8b7`, `df8acf015`; an explicit 256, the old default,
  counts as automatic there, see "Left before a first deployment" in the design document); the production value
  is still to measure with the Yotsuba mix.
- **Talk to the leader.** A client connected to a follower has every update forwarded (its open and its
  commit), one more hop each. One connection is enough: its calls no longer share one event loop thread.
- **Client failover.** Create clients with every node's address (`GrpcConnection.forHostsAndPorts`, or
  `http://node-1:5333,node-2:5333,node-3:5333`), the expected leader first: without it, a dead node takes the
  clients pinned to it down with it. All the clients with the same list sit on its first node. After a leader change
  they stay there, but the nodes name their leader in every response, and the client sends the calls the leader
  serves straight to it, on a second channel, once that is connected (see "Routing" in the design document): only
  point reads still take the forwarding hop through what is now a follower, unless follower reads are on (off by
  default: under steady writes a follower read ties with a forwarded one at the median in process and is slower in
  the tail, and turning the option on adds the wait for the leader's apply to every leader read, see "Follower reads"
  in the design document). This
  needs the clients to list the nodes by the host names of the cluster's `--peer` configuration. Not measured yet. A
  client stays on the node it failed over to until that node goes away. The benchmark's `--route failover` spreads the lists, so it does not measure this; `--route same-list` does (DD-25,
  PF-5: forwarding through followers saturated near 2x of production), with `--leader-hint listed` against `off`. Not
  run yet: it needs the user's go-ahead for a benchmark. A
  server option brings clients back to the first address, `max-connection-age-ms`, which is off by default. With it
  every client starts again from the top of its list at every age cycle, so a silent first address costs each of them
  the connect timeout at every cycle instead of once, and the grace of 10 minutes cuts any longer call, including
  `Compact` and long scans, on a healthy cluster too. A crash is noticed at once. A host that goes down or a process
  that freezes is noticed by keepalive, in at most 15 s (10 s of silence, 5 s for the answer), which is the price of
  not closing connections on shorter pauses; each such node before the first live one in the list then costs 3 s
  (the connect timeout), once, when the client moves past it. A node cut off from the Raft majority stops listening
  after 3 maximum election timeouts without a leader, at least 6 s (about 8 s in all by default), so its clients move,
  and listens again once it knows a leader. A node whose disk hangs (an fsync of its probe that does not end within
  the request timeout) stops listening too, until the probe ends. See the
  [README](../../README.md#client-connections).
- **Batch writes with client sessions.** The core client sends a `putBatch` or `mergeBatch` as one call per chunk
  of at most 4 MiB, one after the other, so each chunk costs a client round trip on top of its commit, which the
  server already waited for chunk by chunk. Each batch the caller publishes is split on its own and chunks are not
  merged across batches: publish batches of a few MiB, not many tiny ones. Not measured; Yotsuba does not use these
  calls today.
- **Partitioned apply.** Off. Turn it on (`--parallel-apply-threads 4`) only after a back-to-back run shows it
  helps, see item 19; nodes may differ in this setting.
- **Snapshots.** One every 500k entries (`--snapshot-every`), about every 100 s at 2×, or every 4 GiB of
  applied log entries (`--snapshot-every-bytes`), whichever comes first. A snapshot flushes the
  memtables and stops the apply for about 150–200 ms; the Raft log keeps up to twice the interval.
- **Memory.** In a 10-minute run at 2× the leader's heap stayed between 1.3 and 2.8 GiB (4 GiB heap). The
  bounds are larger: at most 256 MiB of committed entries wait for the apply thread; the leader holds up to
  4096 pending Raft requests (Ratis' default; proposer batches of up to 4096 writes each) or 160 MiB of them
  (the attempts in flight of 4 nodes, 32 MiB each, plus the leader's own commits); the Raft log write queue holds
  up to 64 MiB (65536 entries); each node holds the writes it proposed up to an eighth of its heap (Proposer's
  byte admission, `-Dit.cavallium.rockserver.distributed.write-admission-bytes`); every replica keeps every reply
  (up to 17 MiB each) in Ratis' retry cache for 4 election timeouts, at least 6 s (8 s by default); and each node
  admits 16384 requests at once, with messages up to 512 MiB. Size the heap for bursts of large `putMulti` calls and large results: an
  `OutOfMemoryError` on the apply thread stops the replica.
- **Memtables.** Replicas flush when the memtables of all columns together reach the write buffer budget,
  `database.global.write-buffer-manager` in the Rockserver config (128 MiB by default, charged to the block
  cache); a per-column `write-buffer-size` is replaced by that budget. A larger budget means fewer and larger L0
  files, and more memory and replay after a crash (the Raft log, not the RocksDB WAL, replays it). The cap on range
  tombstones per memtable (`memtable-max-range-deletions`, unless set) follows the write buffer size, one per 64 KiB,
  and is clamped between 64 and 65,536: 2,048 with the default 128 MiB, 65,536 from 4 GiB up. A memtable with that many
  range deletions is flushed early; `deleteRange` is rare here, so the clamp does not bind in practice, and a deployment
  with many range deletions sets the option itself. `NodeStoreTest` reads both values back from the OPTIONS file
  RocksDB writes (32 MiB gives 512; 8 GiB gives the clamp).
- **Metrics.** On a cluster, `rocksdb_operation_timer` counts the calls made on each node's own database.
  Replicated writes are counted once per node when they are applied, together with the internal result, floor
  and position puts and one `closeTransaction` per applied group, and never with the client's latency. Reads,
  staged transactional writes, transaction opens, rollbacks and range calls are counted only on the leader,
  with their local database time only (no read barrier, forwarding or replication). The replication timers
  (`NodeMetrics`) are logged every 10 s and exported with the core metrics (JMX, InfluxDB) as
  `rockserver.replication.*`, tagged `node` and `kind` (see design.md, "Metrics and alerts").
