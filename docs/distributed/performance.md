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
forwarded), `--kill-leader-after S` (SIGKILL of the leader during the step, with a per-second timeline), `--jfr
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
    transactional put, opening and closing native transactions) went through the embedded workload scheduler,
    whose queues cost 2.6–8.6 ms per request on average at 2–4×, and up to 1.4 s. Those requests were admitted
    by `GrpcServer` already: they now run on a virtual thread of their own, straight on the database. Ranges
    and multi-key reads keep the scheduler. Local time 0.8–1.4 ms; `UPDATE` p99 at 2× from 876 to 232 ms.

## Results

<!-- RESULTS -->

## Deployment notes

- **Disks.** Every write waits for an fsync of the Raft log on the leader and on one follower. Consumer SSDs
  without power-loss protection take 1–3 ms per fdatasync when idle and 10–30 ms when RocksDB compaction writes
  heavily to the same disk. Prefer SSDs with power-loss protection, or a separate disk for the Raft log
  (`data/raft`) and the database (`data/db`).
- **Client concurrency.** Throughput is in-flight requests divided by latency. A replicated write takes a few
  milliseconds instead of a fraction of one, so Yotsuba's 256 writes in flight cap it near 2–3× today's mean.
  Raise `maxConcurrentWriteRequests` (and the read limit) for the cluster; 2048 still keeps latency in the tens
  of milliseconds.
- **Client failover.** Until Yotsuba's client fails over between node addresses, a dead node takes its share of
  requests with it (a third, when clients are spread over three nodes).
- **Snapshots.** One every 500k entries (`--snapshot-every`). A snapshot flushes the memtables and stops the
  apply for about 200 ms; the Raft log keeps up to twice the interval.
