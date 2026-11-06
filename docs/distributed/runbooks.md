# Replicated Rockserver: runbooks

What an operator does to run a cluster: start one, seed it, replace a node, take and restore backups, check that the
replicas agree, react to the alerts, and upgrade. The reasons behind each step are in [design.md](design.md); this page
only says what to do and what to check.

In the commands, `rs` stands for the node's launcher:

```bash
alias rs='java -XX:+ExitOnOutOfMemoryError -Djava.io.tmpdir=/srv/rockserver/tmp \
  -cp rockserver-distributed.jar it.cavallium.rockserver.distributed.ReplicatedServerMain'
```

and `PEERS` for the peer list of the cluster, one `--peer` per member, the same on every node and in every `admin`
command:

```bash
PEERS="--peer n1=10.0.0.1:9001/10.0.0.1:5333 --peer n2=10.0.0.2:9001/10.0.0.2:5333 --peer n3=10.0.0.3:9001/10.0.0.3:5333"
```

## Before the first start

- One node per host, each with its data directory (`--data-dir`) on one file system, its own disk (see "Disk layout").
- The same Rockserver config file (`--rockserver-config`) on every node, with the metrics pipeline on
  (`database.metrics.influx`), and the alert rules of [alerts/grafana-alert-rules.yaml](alerts/grafana-alert-rules.yaml)
  loaded (see "Alerts" below). Each node tags its meters with its node id by itself.
- Every node runs the same version (see "Upgrades").
- A supervisor (systemd or similar) restarts a node that exits with code 3: that is how a node stops after a replica
  failure or after Raft closed.
- `-XX:+ExitOnOutOfMemoryError` and `-Djava.io.tmpdir` on a directory of the node, as in the alias.

## Starting an empty cluster

1. Start every node with the same `PEERS` and without `--join`: `rs run --node-id n1 $PEERS --data-dir /srv/rockserver/data`.
2. Each prints `READY n1` once it serves the API. Check: `rs admin $PEERS status` shows one `LEADER` and the others
   `FOLLOWER`, with the same commit index soon after.

## Seeding a cluster from a single-node database

The data of a single Rockserver becomes the first state of the cluster. Every node must start from a copy of the
same database. Each node records a digest of its copy at its first start, and a replica whose digest differs from the
leader's stops at genesis with exit code 3 (see "One seed" in the design document): that catches a missing, extra or
truncated file and a copy taken at another moment, but not a table file damaged without a change of size, which only
the check of step 3 finds.

1. Stop the single node (a clean stop: its database must be closed).
2. Copy its database directory to every node, as `<data-dir>/db` (for example `rsync -a --delete old-db/
   n1:/srv/rockserver/data/db/`). The data directories must hold nothing else: no `raft/`.
3. Check that the copies are equal, for example `find db -type f -exec sha256sum {} + | sort -k2 | sha256sum` on
   each node, the same result everywhere. Do not skip it: the digest of the first start compares table files by size
   only.
4. Start every node as for an empty cluster. The first leader proposes a seeded genesis; every replica then takes a
   snapshot of the seed (the log says `snapshot taken`).
5. Check: `rs admin $PEERS checksum` prints `OK` and the same digest for every node.
6. Only then point clients at the cluster.

If a node stops with "seeded with an existing database, but this replica's database is empty", it started without the
copy: see "Membership" in the design document, and replace it as below.

## Replacing a node

For a node that is lost (dead disk, dead host) or that must move. One node at a time, and only while the others are a
majority and healthy (`status`).

1. Pick a new id that was never used, say `n4`. On the new host, with an empty data directory, start it with
   `--join` and a peer list that has the members that stay plus itself:
   `rs run --node-id n4 --peer n2=... --peer n3=... --peer n4=... --data-dir /srv/rockserver/data --join`.
   It prints `READY` and waits for as long as it takes.
2. From any host, in one step, add `n4` and remove the lost node, by leaving it out of `--peer`:
   `rs admin --peer n2=... --peer n3=... --peer n4=... add-peer n4=10.0.0.4:9001/10.0.0.4:5333`.
   It ends with `OK` once `n4` caught up from a snapshot and the configuration changed (the limit, `--timeout-ms`, is
   10 minutes by default: raise it for a large database, about 100 MB/s).
3. Check: `status` shows `n4` with the others' commit index; `checksum` prints `OK`; the `rockserver-node-missing`
   alert threshold and the clients' address lists include `n4` and not the old node.
4. If the old node still runs, it exits with code 3 once removed: take it out of its supervisor first.

Failure branches:

- `add-peer` says `FAILED: ... has a log of its own` or `was started without --join`: wipe the new node's data
  directory and start it again with `--join`, then run `add-peer` again.
- `add-peer` says the new node `runs a version without ...`: upgrade it to the cluster's version first.
- `add-peer` ends with no result within the limit: the leader goes on with the change. Run the same command again until
  it says `OK`; `status` shows the progress.
- The lost node was the leader: the same steps, after the election (`status` shows a new leader).
- Two nodes of three lost: there is no majority, `add-peer` cannot work. See "No quorum" in the design document, and
  "Restoring a backup" below.

## Backups

A backup is a checkpoint of every replica's database taken at the same log position: every node holds a copy of the
whole cluster at one moment. It is the way back when no majority can start again.

Take one (from any host, through any member):

```bash
rs admin $PEERS backup 2026-11-06
```

It prints the log index of the backup and, for each node, `DONE` and the directory, `<data-dir>/backups/<name>`, or
why it failed (disk full, a backup of that name exists), and `OK` only if every node of `$PEERS` wrote it. It exits
with 1 if a node failed, and with 4 if a node gave no result within `--timeout-ms` (down, or slow): then ask again
later with `rs admin $PEERS backup-status 2026-11-06 <index it printed>`, which proposes nothing. Each node's apply stops for
the flush (100–300 ms); the files are hard links, so a backup costs space only as compactions replace the files it
holds.

Then, for each backup:

1. Copy one node's `<data-dir>/backups/<name>` off the cluster (all of them hold the same data; `backup.properties`
   says the log index and cluster).
2. Delete `<data-dir>/backups/<name>` on every node once it is copied: until then the hard links keep the replaced
   files on the node's disk.
3. Now and then, check a copy by restoring it (below) on a spare host.

### Restoring a backup

A restore starts a new cluster, seeded from the backup: the writes after the backup are lost, and client sessions start
over (clients open new ones by themselves).

1. Stop every node of the old cluster, and copy their data directories aside before anything else.
2. On every node of the new cluster, into an empty data directory, from a copy of the same backup:
   `rs restore-backup --backup /path/to/<name> --data-dir /srv/rockserver/data [--rockserver-config ...]`.
   It prints `OK` and the log index the data reaches. It refuses a data directory with a `db/` or a Raft log, and
   renames a `replica-ops.log` and a `backups/` of the old cluster found there (suffix `.before-restore-<time>`).
3. Start every node as for a new cluster, without `--join`; the nodes of the old cluster stay stopped. The first leader proposes
   a seeded genesis, as in "Seeding a cluster".
4. Check: `status`, then `checksum` prints `OK`, then a few known keys read back.

## Checksums

`rs admin $PEERS checksum` makes every replica digest its user data at one log position and compare it with the others.
It prints each node's digest and number of keys, and `OK` only if every node of `$PEERS` gave a digest and all are
equal. It exits with 1 if they differ, and with 4 if a node gave none (down, failed, not done within `--timeout-ms`,
or it skipped the position because a snapshot install jumped over it): run it again. `--allow-skipped` accepts nodes
that skipped the position. The digest reads every key of the
database on every node at once, holding a snapshot meanwhile: run it at a quiet time on large databases. With
`-Dit.cavallium.rockserver.distributed.checksum-every-hours=N` on the nodes, the leader also runs one every N hours,
counted from the last check of the cluster (also one run by hand), so a leader change does not start the count over.
Before the first check, it counts from the first start with the property set (`<data-dir>/checksum-schedule`), so
restarts do not start it over either.
The digest covers each column's schema and merge operator too, once every member runs a version that does (the
capability `checksum-schemas`): a column created or altered differently on one replica is a divergence even with the
same data. A malformed value of this property, as of every `it.cavallium.rockserver.distributed.*` property, is logged
as a warning at the start and the default is used.

A difference (the log says `REPLICAS DIVERGED`, the alert `rockserver-checksum-mismatch` fires):

1. Stop the clients' writes, and copy every data directory aside (the evidence).
2. Find the cause: a merge operator that is not deterministic (the usual one: see [merge-operators](../merge-operators.md)),
   a damaged disk (the system log, SMART), or a bug. Compare a few keys between the nodes with a local Rockserver on
   the copies.
3. Rebuild the replicas that are wrong: replace them one at a time as in "Replacing a node" (a new member gets the
   leader's data). If the leader is the one that is wrong, transfer the leadership first (`admin transfer-leader`).
4. Run `checksum` again.

## Raft log grows

The alert `rockserver-raft-log-size`, or the log line `ALARM raft-log-size`. Ratis purges the log only up to the lowest
commit index of any member, so a member that is gone holds it back.

1. `status`: a member that does not answer, or whose commit index stays behind.
2. If it is gone for good, replace it (above), or remove it: `rs admin --peer <the others> remove-peer <id>`.
3. If every member is there, check the snapshots: the alert `rockserver-snapshot-old` and the log (`snapshot taken`,
   or errors). A failing snapshot is usually a full disk.

## Disk space and write stalls

The alerts `rockserver-disk-low`, `rockserver-write-stopped`, `rockserver-write-delayed`, or `ALARM disk-space`,
`ALARM write-stopped`, `ALARM write-delayed` in the node's log.

- Disk space: delete backups that were copied off (`<data-dir>/backups`), then old snapshots are deleted by the node by
  itself once newer ones exist. Budget the database, one older snapshot's replaced files, the Raft log (up to
  `--snapshot-every-bytes` plus the margin) and the backups. Grow the disk if that is not enough.
- A write stall on one node slows only that node's apply; on a majority, it stops the cluster's writes. It comes from
  compactions behind (look at the pending compaction gauges) or a full disk.

## Upgrades

Upgrade one node at a time (a rolling restart): stop it (SIGTERM: a leader hands over first), replace the binary, start
it, wait until `status` shows it with the others' commit index, then the next. Clients see a few hundred milliseconds
without writes per leader restart.

The cluster version keeps the new kinds of log entries off until every member runs a version that reads them (see
"Cluster version" in the design document):

- After the last node is upgraded, the leader raises the cluster version within about 10 seconds (the log says
  `cluster version is now ...`, and the gauge `rockserver_replication_cluster_version` changes).
- `--pipelined-updates` takes effect from the next leader term after that: run `admin transfer-leader <a follower>`
  once, or wait for the next leader change. A node with the option on logs a warning at each term that runs without it.
- Clients use sessions only through nodes whose cluster version allows them. A client connected before that runs without
  sessions until it reconnects: restart the clients after the upgrade (or start them with
  `-Dit.cavallium.rockserver.grpc.client.session-writes=false` until then).
- `admin checksum` and `admin backup` are refused until every member supports them.
- Never downgrade a node below the cluster version: it would stop on the first entry it cannot read. A node whose
  database has a cluster version with a capability its binary lacks (a downgrade, or a backup of a newer cluster restored
  here) refuses to start, with exit code 5 and a message naming the unknown capabilities: start the newer version
  again. A node rolled back after it took part in a raise can also meet it after the start, in the log it replays or
  catches up on, or in a snapshot it installs: it stops then, with exit code 5 as well. `add-peer` refuses
  a new node that lacks a capability of the cluster, or one that every member supports already (the leader may turn it
  on at any moment): upgrade the new node first.
- A cluster created before the cluster version has none until every member was upgraded to a version that has it and the
  leader raised it.

Upgrades across these versions need more care, because no gate covers them:

- **`MERGE_OPERATOR_FAILED`.** A version that records a failed merge operator as `MERGE_OPERATOR_FAILED` must not run
  in a cluster beside nodes of an older version that apply merges: the older nodes would apply such an entry their own
  way (and cannot decode such a record), and the replicas would diverge. Upgrade all nodes together (stop the cluster,
  upgrade, start), or keep the clients from merging during the rolling upgrade. Clients built before that version see
  such a failure as a plain gRPC `INTERNAL` error: upgrade them too.
- **Client sessions** need every node on a version that reads version 3 batches, stamps and `RegisterClient` entries;
  the cluster version covers this from this version on.
- **Pipelined updates** need every node on the version with the flags of `Guarded` entries; the cluster version covers
  this from this version on.

## Exit codes

Each command has its own codes; the same number can mean different things for different commands. Supervise `run`
only: restart it on 1 and 3, never on 2, 4 or 5, which need an operator.

| Code | `run` | `admin` | `repair-log` | `export-seed`, `restore-backup` |
|---|---|---|---|---|
| 0 | Stopped by SIGTERM (or SIGINT), after its hand-off | `OK` | The log is valid, or was (with `--dry-run`: could be) repaired | Done |
| 1 | Did not start: a bad option value, a port in use, a refused data directory (another file system, a cluster's database without its Raft state, a pointless `--join`) | `FAILED: ...`: refused, a change failed, the replicas differ (`checksum`), a member failed (`backup`) | Refused: see its output | Failed: see the exception |
| 2 | Wrong usage, or no `-XX:+ExitOnOutOfMemoryError` | Wrong usage | Wrong usage | Wrong usage |
| 3 | The node stopped after a failure: its replica failed, Raft closed, a JVM pause beyond `--jvm-pause-limit-ms`, out of direct memory on a thread of replication; also a failure that raced a SIGTERM | - | - | - |
| 4 | Removed from the cluster: refused at the start, or found out while waiting outside the cluster. Do not start it again | Incomplete: a member gave no result in time (`checksum`, `backup`, `backup-status`): run it again | - | - |
| 5 | Its database, its log or a snapshot it installed has a cluster version with capabilities this version lacks: run the newer version | - | - | - |

Code 4 is kept for both meanings: renumbering either would change what deployed supervisors and scripts, and the
process tests, already expect, and a supervisor never runs `admin`, nor a script `run`.

## Alerts

[alerts/grafana-alert-rules.yaml](alerts/grafana-alert-rules.yaml) holds the rules, in Grafana's provisioning format,
on the InfluxDB data source that receives the metrics. Replace `CLUSTER`, `INFLUX_UID` and the node count as its header
says. The rules and what to do:

| Rule | Severity | Means | Runbook |
|---|---|---|---|
| `rockserver-no-leader` | critical | No node leads: writes stop | "No quorum" in design.md |
| `rockserver-node-missing` | critical | A node stopped reporting (down, or exited with 3 after a replica failure) | Its supervisor and log; "Replacing a node" |
| `rockserver-replica-failed` | critical | A replica could not apply the log | Its log; "Replacing a node" |
| `rockserver-term-churn` | warning | Elections keep happening | Links, pauses, `leader.stall.stepdowns` |
| `rockserver-apply-lag` | warning | A node applies far behind its commit index | Disk, write stalls |
| `rockserver-commit-lag` | warning | The leader's entries wait for a majority | Followers' disks and links |
| `rockserver-follower-lag` | warning | A follower is far behind | That follower |
| `rockserver-raft-log-size` | warning | The Raft log is not purged | "Raft log grows" |
| `rockserver-snapshot-old` | warning | No snapshot for a day with a large log | "Raft log grows", disk space |
| `rockserver-disk-low` | critical | Under 10% free | "Disk space and write stalls" |
| `rockserver-write-stopped` | critical | RocksDB stopped writes | "Disk space and write stalls" |
| `rockserver-write-delayed` | warning | RocksDB slows writes | "Disk space and write stalls" |
| `rockserver-checksum-mismatch` | critical | The replicas diverged | "Checksums" |
| `rockserver-leader-stall` | warning | A leader stepped down to restart replication | "A leader whose log stops" in design.md |
