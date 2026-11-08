# Replicated Rockserver: runbooks

What an operator does to run a cluster: start one, seed it, replace a node, take and restore backups, check that the
replicas agree, react to the alerts, and upgrade. The reasons behind each step are in [design.md](design.md); this page
only says what to do and what to check.

In the commands, `rs` stands for the node's launcher:

```bash
alias rs='java -XX:+ExitOnOutOfMemoryError \
  -cp rockserver-distributed.jar it.cavallium.rockserver.distributed.ReplicatedServerMain'
```

`run` extracts the RocksDB library into `DATA-DIR/tmp` by itself, with or without `-Djava.io.tmpdir`, and deletes
there the 17 MB copies that killed runs leave behind. It also points `java.io.tmpdir` there for libraries that read the
property later (Netty), but the JDK's own temporary files (`File.createTempFile`, `Files.createTempFile`) keep the
directory the JVM started with, `/tmp` by default. To move everything, pass `-Djava.io.tmpdir=DATA-DIR/tmp` (for
example `/srv/rockserver/data/tmp`): a given directory inside the data directory is the node's own, and the node deletes
the RocksDB copies in it at every start. A given directory outside the data directory, or one that is a symlink to a
shared directory such as `/tmp`, is warned about at the start and never cleaned by the node, since another JVM of the
host may be loading its copy there.

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
- `-XX:+ExitOnOutOfMemoryError`, as in the alias, and either no `-Djava.io.tmpdir` or one inside the data directory
  (see above). A node upgraded from an alias that set `-Djava.io.tmpdir=/srv/rockserver/tmp`: drop the option (or set
  it to `/srv/rockserver/data/tmp`), stop the node, delete `/srv/rockserver/tmp/*rocksdbjni*` (copies of killed runs,
  17 MB each), start it.

## Starting an empty cluster

1. Start every node with the same `PEERS` and without `--join`: `rs run --node-id n1 $PEERS --data-dir /srv/rockserver/data`.
2. Each prints `READY n1` once it serves the API. Check: `rs admin $PEERS status` shows one `LEADER` and the others
   `FOLLOWER`, with the same commit index soon after. `status` asks every member at once, waits up to 3 s in all for
   the answers, and prints them in the `--peer` order: a member that is down prints `unreachable` and does not hold the
   others. With an unroutable member the command may take about 5 s more to exit after printing, while it closes its
   connections.

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
2. From any host, change one voter per command. `admin` reads the leader's configuration first (it waits while a change
   is under way) and refuses, with exit code 1 and before it proposes anything, a call that would add and remove
   voters together or change two of them: see "One voter per change" below. Old node `n1`:
   - **The old node is down (lost):** remove it first, with `--peer` listing all current members, then add the new one
     with `--peer` listing the members that stay:
     `rs admin --peer n1=... --peer n2=... --peer n3=... remove-peer n1`, then
     `rs admin --peer n2=... --peer n3=... add-peer n4=10.0.0.4:9001/10.0.0.4:5333`.
     The two members that stay are needed throughout, as with any replacement of one node of three.
   - **The old node runs (a move):** add the new one first, with every member, the old one included, in `--peer`, then
     remove the old one with all of them in `--peer`:
     `rs admin --peer n1=... --peer n2=... --peer n3=... add-peer n4=10.0.0.4:9001/10.0.0.4:5333`, then
     `rs admin --peer n1=... --peer n2=... --peer n3=... --peer n4=... remove-peer n1`.
     The old node exits with code 3 or 4 once removed: take it out of its supervisor before the `remove-peer`.

   Do not add first while the old node is down: with 4 voters and one lost, all 3 others are needed, and a failure of
   the new node during its catch-up stops the cluster (see "Heap" in the design document). `add-peer` ends with `OK`
   once `n4` caught up from a snapshot and the configuration changed (the limit, `--timeout-ms`, is 10 minutes by
   default: raise it for a large database, about 100 MB/s).
3. Check: `status` shows `n4` with the others' commit index; `checksum` prints `OK`; the `rockserver-node-missing`
   alert threshold and the clients' address lists include `n4` and not the old node.

Failure branches:

- `add-peer` says `FAILED: ... has a log of its own` or `was started without --join`: wipe the new node's data
  directory and start it again with `--join`, then run `add-peer` again. **Never wipe a node that is a voter**: the
  line `leader ...: voters [...]` that `add-peer` prints first lists them (a voter at the given address gets `already
  a voter ... nothing to do` and `OK`, without its own state being asked). If an earlier `add-peer` of the same node
  ended without `OK`, the leader may still be catching it up: run `add-peer` again first, and wipe only if it fails
  the same way with the node still not among the voters printed. Wiping a voter's log and vote can lose committed
  entries (with voters n2, n3, n4, an entry on n3 and n4 alone is gone once n4 is wiped and n2 is elected).
- `add-peer` says the new node `runs a version without ...`: upgrade it to the cluster's version first.
- `add-peer` or `remove-peer` says `FAILED: refused: changes more than one voter: 2 voters would change at once (adds
  [n4], removes [n1]; the voters now are [...])`: `--peer` does not match the leader's configuration (the line before
  prints it), or the call is the old one-step replacement. Nothing was proposed. Fix `--peer` to the voters printed,
  and change one voter per call as in step 2. If the leader itself refused it (`FAILED: the leader refused: changes
  more than one voter ...`; an `admin` of an older version prints `FAILED: ... or the leader refused: refused: changes
  more than one voter ...`), the same applies.
- `add-peer` ends with no result within the limit: the leader goes on with the change. Run the same command again until
  it says `OK` (once the node is a voter it says `already a voter` and `OK`); `status` shows the progress. Do not wipe
  the new node meanwhile, whatever a re-run says about its log (see the first branch).
- The lost node was the leader: the same steps, after the election (`status` shows a new leader).
- Two nodes of three lost: there is no majority, `add-peer` cannot work. See "No quorum" in the design document, and
  "Restoring a backup" below.

## Removed node

A node that was removed (`remove-peer`, or the replace above) and is started again exits with 4, and its log says
which committed configuration left it out and which members confirmed it. It does so only once a majority of the new
half of the newest committed configuration answer; until then it prints `READY` and waits outside the cluster: it
serves nothing (its clients' calls are forwarded to a leader, or time out), and checks again every three election
timeouts while no leader sends to it. Whether it votes depends on its own log: one whose latest configuration leaves
it out keeps it in Ratis' initializing state, where it votes for nobody; one that missed its removal (it still holds
the node in both halves) makes it a follower that answers vote requests and starts elections of its own until the
check stops it. Neither can elect a leader that lacks the removal, nor win an election itself (see "Removal and
waiting outside" in the design document); the elections it starts show in the members' logs as rejected pre-votes. Every 15 election timeouts it logs `hears no leader` or, at its
start, `starting, not known to be a member`, with `Membership <reason> (K* ..., confirmed by ..., blocked by ..., pending
..., unreachable ...)` and what to do. The `membership.state` gauge is 2 meanwhile (1 for a `--join` node before its
`add-peer` and while `add-peer` catches it up, 0 for a member, 3 if a sample happens to be pushed between the verdict and exit 4, which is not guaranteed); the alert
`rockserver-node-outside` fires after 10 minutes at 2, and `rockserver-node-not-added` after 6 hours at 1. By reason:

| Reason | What to do |
|---|---|
| `NO_MAJORITY`, `NO_COMMITTED_CONFIGURATION` | Start or repair the pending and unreachable members (a member of a version without `Configuration` counts as unreachable). The node decides by itself once a majority of them answer. If they are gone for good (wiped, replaced), check with `status` on a member: if this node is not in its configuration, stop it, take it out of its supervisor, wipe its data directory, and add it again with `--join` and `add-peer` if you want it back. |
| `NEEDS_TERM` | The members that answered run a version without `Configuration2`: upgrade them. Their answers tell no terms, and their committed flag does not confirm a removal unless a member of this version (or the node itself) reports the same configuration committed. Until then judge with `status`, as above. |
| `MEMBER_LATER_CONFIGURATION` | Clears by itself within seconds. If it lasts, the blocking members are stuck or were removed themselves: look at their logs and `status`. |
| `OWN_LATER_CONFIGURATION` | Its own log holds a later configuration that includes it and may still commit: normally an `add-peer` of this node in flight (let it finish), or a configuration a leader appended just before it lost leadership, which clears once the next leader's configuration reaches this node. If it persists while a leader runs, check `status` on a member. |
| `NOT_ADDED` | A `--join` node that never was a voter: run `add-peer`. If the add was abandoned, or the node was added and removed long ago, stop it and wipe its data directory (`add-peer` refuses a node that has a log). |
| `INCONSISTENT` | A bug, or a data directory restored from elsewhere: wipe nothing, collect the logs of every node. |
| `HEARD_LEADER` | A leader sent the node something during the check: it is a member again, or will be checked again. |
| `MEMBER` (with `hears no leader`) | It is a member waiting for a leader; in Ratis' initializing state if it was started with `--join` and no node started without it. |

`RestartPreventExitStatus=4` stays: a node that exited with 4 is removed, and a restart only repeats the check.

### One voter per change

`add-peer` and `remove-peer` build the new voters from the `--peer` given to `admin`, plus or minus the one node, and
compare them with the leader's configuration, which they read first and print (`leader n2: voters [...]; this change
adds [...] and removes [...]`). A call that changes more than one voter is refused: Ratis' first entry of a new leader
keeps only the new half of a joint configuration and commits with it alone, so a change of several voters at once can
lose committed entries if the leader changes in the middle (see "Replacing a node" and the residuals of "Removal and
waiting outside" in the design document). A voter that becomes a listener, or the reverse, counts as one change;
listeners, addresses and priorities count nothing. The change is sent as a compare-and-set against the configuration
read: if another change commits first, `admin` reads it again and checks again. The leader refuses the same on its
own, at once (`FAILED: the leader refused: changes more than one voter ...`), also for an `admin` of an older version
(which prints `FAILED: ... or the leader refused: refused: changes more than one voter ...`) or the Ratis tools, but
only a leader of this version does: during a rolling upgrade a leader of an older version has no such check, and an
older `admin` gets its one-step replacement through. Until every node that can become leader is upgraded, change one
voter per call with every `admin` (see "Upgrades"). Several dead nodes are removed one per call; a re-run of a call
that succeeded prints `OK` again (for `add-peer`, `already a voter`). If the leader refuses for another reason that a
new read of its configuration does not change (Ratis' own `changes a majority set`, say), `admin` fails at once with
its message (`the leader n2 (term 7) refused, and its configuration did not change since: ...`).

`admin` asks every `--peer` at once (a down member delays each round by one request timeout, not one each) and takes the configuration of the leader of the highest term; it waits while a member
shows a later term than the leader that answered. A leader that has just been deposed answers as the leader for about
an election timeout, with its old configuration: if neither the new leader nor any member that knows its term
answers, `admin` can still read that one. A change then fails at once with `did not change since` and the advice to
run it again, and the voters printed by `add-peer` can be stale: run the command again before acting on them, and
never wipe a node on one such answer (wipe only if `add-peer` fails the same way again with the node not among the
voters printed).

`--unsafe-multi-voter-change` skips both checks: `admin` prints a warning and sends the change straight to the leader
with a header that its guard lets through (it logs a WARN with the old and new voters). It is **never needed for
recovery**: any change that can commit needs a live majority of the old voters, and with one, the same result can be
reached one voter at a time; with no live majority nothing commits, flag or not: see "No quorum" in the design
document, `export-seed` and "Restoring a backup". The change is safe only if every majority of the old voters that can
still vote meets every majority of the new voters, and that the leaving nodes look dead is not enough: going from five
voters n1..n5 to n3, n4, n5, n6 in one change while n1 and n2 are partitioned rather than dead, the old majority n1, n2,
n3 and the new majority n4, n5, n6 do not meet. Ratis itself refuses, flag or not, a change whose new voters are at
least as many as the kept ones (`changes a majority set of the current configuration`, for example two of three voters
replaced at once, or three voters with four new ones added), and `admin` then fails at once with that message. It is for
tests and experts.

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
   It refuses a backup of a newer cluster version than this binary supports with exit code 5, and leaves no `db/`:
   restore it with the newer version. Run that newer version on every node of the new cluster as well: the restored
   `db/` carries no cluster version, so an older binary started on it is not refused, and would hold data written by a
   newer version under the oldest cluster version. On any other failure (a `--rockserver-config` that does not parse,
   a column that does not open, a full disk) it also removes the `db/` it made, so fix the cause and run the same
   command again. A `db/` left there by an interrupted run (power loss, `kill -9`) is not removed: delete
   `DATA-DIR/db` and run it again.
   Use copies of one node's backup on every node (`cp -a`, `rsync`, `scp` of the whole `<name>` directory), not the
   backup each node took itself: those hold the same data, but not the same files. `restore-backup` records the seed
   digest of the backup's own files (`DATA-DIR/db.seed-digest`), before it opens the database, because the restored
   `db/` differs on each node (each restore opens it once and writes its own `OPTIONS` and info log, R26-1): the
   followers compare that digest with the leader's at genesis and stop with exit 3 if it differs. Running
   `restore-backup` once and copying the stopped `DATA-DIR/db` to every node with `cp -a` or `rsync` works as well,
   as in "Seeding a cluster", but only together with `DATA-DIR/db.seed-digest`: that file holds the backup's digest,
   and the restored `db/` (opened once by the restore) digests to another value, so a node that gets `db/` alone
   records that other value at its first start and the nodes stop at genesis with exit 3. Either copy both to every
   node, or delete `db.seed-digest` on every node, the one that ran the restore included, before the first start.
3. Start every node as for a new cluster, without `--join`; the nodes of the old cluster stay stopped. The first leader proposes
   a seeded genesis, as in "Seeding a cluster".
4. Check: `status`, then `checksum` prints `OK`, then a few known keys read back.

## Checksums

`rs admin $PEERS checksum` makes every replica digest its user data at one log position and compare it with the others.
It prints each node's digest and number of keys, and `OK` only if every node of `$PEERS` gave a digest and all are
equal. It exits with 1 if they differ, and with 4 if a node gave none (down, failed, not done within `--timeout-ms`,
or it skipped the position because a snapshot install jumped over it), or if no node took the request within
`--timeout-ms` (no leader, every node down) or the answer leaves open whether it was proposed: run it again. A node of
`$PEERS` that does not answer at all (down, or frozen with its port still accepting connections) is skipped after a
few seconds, and the request goes to the next one; a node that took it gets at most 30 s to answer. So a frozen node
listed first delays the command by seconds, not by `--timeout-ms`, and `backup` behaves the same way.
`--allow-skipped` accepts nodes that skipped the position (it is refused with any other command). The digest reads every key of the
database on every node at once, holding a snapshot meanwhile: run it at a quiet time on large databases. With
`-Dit.cavallium.rockserver.distributed.checksum-every-hours=N` on the nodes, the leader also runs one every N hours,
counted from the last check of the cluster (also one run by hand), so a leader change does not start the count over.
Before the first check, it counts from the first start with the property set (`<data-dir>/checksum-schedule`), so
restarts do not start it over either. A periodic check that no leader accepted is tried again a minute later; one
whose outcome is unknown (it may still apply) waits the full N hours, so it never runs twice in a row.
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

## Snapshot installs refused

Two alerts count the ways a follower fails a leader's snapshot install stream (design.md, R16-2 and the install
record). The leader starts the install again in both cases.

- `install.recordFailures` (`rockserver-install-record-failed`, on the first one): the follower could not write the chunk's configuration to `raft/installed-configuration`.
  Its log says `cannot record the configuration of a snapshot chunk`, with the I/O error. Check the Raft disk (space,
  permissions, errors in the kernel log) as in "Disk space and write stalls".
- `install.staleChunks` (`rockserver-install-stale-chunks`, only when more than 3 come in every 30 minutes for 30
  minutes): the follower refused a chunk whose configuration is older than its install floor. Its log says
  `a snapshot chunk of term ... carries the configuration at index ..., older than the one at ...`. A few right after a
  leader change or a restarted install are expected: they are chunks of a stream its leader gave up on. If they keep
  coming while the same follower's lag never drops (`rockserver-follower-lag`), the follower may be refusing a live
  stream, which design.md argues cannot happen. Keep that follower's log and Raft directory and report it, then replace
  the node ("Replacing a node").

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

Do not change the membership in the middle of an upgrade if you can avoid it. If you must, change one voter per call
(`remove-peer` then `add-peer` for a lost node, as in "Replacing a node"), whatever the version of `admin`: until every
node that can become leader runs round 14 or later, a leader of an older version does not refuse a change of several
voters, so an older `admin`'s one-step replacement goes through.

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
  database has a cluster version with a capability its binary lacks (a downgrade, or a newer cluster's database copied
  here) refuses to start, with exit code 5 and a message naming the unknown capabilities: start the newer version
  again. A node rolled back after it took part in a raise can also meet it after the start, in the log it replays or
  catches up on, or in a snapshot it installs: it stops then, with exit code 5 as well. `add-peer` refuses
  a new node that lacks a capability of the cluster, or one that every member supports already (the leader may turn it
  on at any moment): upgrade the new node first. `restore-backup` and `export-seed` refuse a newer cluster's backup or
  database the same way, with exit code 5 and no `db/` or seed written: run them with the newer version, and run it on
  every node that starts on their output too, since that output carries no cluster version and an older binary is not
  refused there.
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
- **Removing nodes** (`remove-peer`, or a replacement) during an upgrade from a version without `Configuration2`:
  upgrade the remaining members first. A removed node started again decides that it was removed only once the node
  itself or an upgraded member reports its removal committed; with only old members it waits outside with
  `NEEDS_TERM` (see "Removed node").

## Exit codes

Each command has its own codes; the same number can mean different things for different commands. Supervise `run`
only: restart it on 1 and 3, never on 2, 4 or 5, which need an operator. With systemd, the exit and stop settings of
the unit:

```ini
[Unit]
StartLimitIntervalSec=300
StartLimitBurst=5

[Service]
Restart=on-failure
RestartPreventExitStatus=2 4 5
SuccessExitStatus=143
TimeoutStopSec=150
```

A restart on 4 or 5 is what those codes exist to prevent: the node would refuse to start again, or start a version
that cannot read its data. 2 is always a fault of the command line or of the JVM options, so it fails the same way on
every restart. 1 is not always transient either: of its causes only a port in use can pass by itself, while a bad or
non-numeric option value, a refused data directory or a pointless `--join` fail the same way each time. 1 stays
restarted because it mixes both kinds and the exit code cannot tell which one it was; the start limit ends a loop of
the deterministic kind. The limit is set wider than systemd's default (5 starts in 10 s), which a node whose failed
start takes a few seconds (the JVM, opening the database) never reaches; 5 in 5 minutes also stops restarting a node
that fails with 3 over and over, which needs an operator too.

`SuccessExitStatus=143` is there because the JVM, not `run`, chooses the status of a stop by signal (below): without
it, every `systemctl stop` leaves the unit "failed". `TimeoutStopSec=150` is there because a stop can outlast
systemd's default of 90 s. The shutdown hook hands over leadership (up to 5 s for the transfer and 3 s for the
successor's commit), then closes the node in place: the apply thread finishes its entry, its completer gets up to a
minute, and the database closes (with its flush). When another thread had already started the close (a replica
failure racing the signal), the hook waits up to 2 minutes for it. A stop past `TimeoutStopSec` gets SIGKILL, exit
137, and the unit is "failed" whatever `SuccessExitStatus` says; for the node that is a crash (it replays its log at
the next start), not a loss. 150 s covers the waits the node bounds itself; a database close slower than that is a
disk problem to look at.

| Code | `run` | `admin` | `repair-log` | `export-seed`, `restore-backup` |
|---|---|---|---|---|
| 0 | Not on a stop by signal: see below | `OK` | The log is valid, or was (with `--dry-run`: could be) repaired | Done |
| 1 | Did not start: a bad option value, a port in use, a refused data directory (another file system, a cluster's database without its Raft state, a pointless `--join`), any other failure of the start (an `Error` too, such as a class that does not load) | `FAILED: ...`: refused (a change of more than one voter included, or a member refused `checksum`/`backup`: not every member supports it), a change failed, the replicas differ (`checksum`), a member failed (`backup`) | Refused: see its output | Failed: see the exception |
| 2 | Wrong usage, or no `-XX:+ExitOnOutOfMemoryError` | Wrong usage: also an unknown option (a misspelled one), an option of another command (`--allow-skipped` without `checksum`), a malformed value (`--timeout-ms`, `--peer`, `--group-id`, the index of `backup-status`) | Wrong usage | Wrong usage |
| 3 | The node stopped after a failure: its replica failed, Raft closed, a JVM pause beyond `--jvm-pause-limit-ms`, out of direct memory on a thread of replication; also a failure that raced a SIGTERM, and out of heap (the JVM's own status for `-XX:+ExitOnOutOfMemoryError`) | - | - | - |
| 4 | Removed from the cluster, confirmed by a majority of the current configuration: refused at the start, or found out while it heard no leader. Do not start it again (see "Removed node"; until a majority answers, a removed node waits outside instead) | Incomplete: a member gave no result in time (`checksum`, `backup`, `backup-status`), no member took the request in time, or a `checksum` answer that does not tell whether it was proposed: run it again | - | - |
| 5 | Its database, its log or a snapshot it installed has a cluster version with capabilities this version lacks: run the newer version | - | - | The replica's database (`export-seed`) or the backup (`restore-backup`) has a cluster version with capabilities this version lacks; nothing is written: run the newer version |

A stop of `run` by SIGTERM, after its hand-off, ends with 143 and one by SIGINT with 130: the JVM's
status for a signal (128 plus its number), since the shutdown hook halts with a status of its own only after a
failure (then 3, 4 or 5, as in the table). The hook is registered before the node starts: a SIGTERM while it is still
starting (opening its database, loading its log, checking its membership) waits for the start to end and then stops
the node as above, so a stop during a long start takes as long as the rest of the start, which `TimeoutStopSec`
must cover for a large node (or it ends with SIGKILL, a crash for the node). A node that is killed (SIGKILL, 137 in a shell) or crashes in native code
(SIGABRT, 134) ends by a signal, and an uncaught exception before the node starts, such as a non-numeric option
value, ends with 1, as in the table. A supervisor restarts all of these except a stop it asked for. So the codes a
supervisor must not restart are 2, 4 and 5, and 143 is a clean stop.

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
| `rockserver-install-record-failed` | warning | A follower could not record a snapshot chunk's configuration (`install.recordFailures`) | "Snapshot installs refused", "Disk space and write stalls" |
| `rockserver-install-stale-chunks` | warning | A follower kept refusing stale snapshot chunks: more than 3 in every 30 minutes for 30 minutes (`install.staleChunks`). A few after a leader change are expected, hence the threshold and the wait | "Snapshot installs refused" |
| `rockserver-node-outside` | warning | A node is outside the cluster and undecided for 10 minutes (`membership.state` 2) | "Removed node" (the reason is in its log) |
| `rockserver-monitor-stale` | warning | A node's monitor has not refreshed for over a minute (`monitor.refresh_age_seconds`): its monitor thread waits for a hung disk, and every other gauge of that node shows its last value, however healthy it looks | Its disk (the API is likely paused too: "Routing" in design.md, on a hung disk), its log |
| `rockserver-node-not-added` | warning | A `--join` node is not a voter after 6 hours (`membership.state` 1): a forgotten or abandoned `add-peer`. It is 1 during a normal catch-up too, hence the long wait | "Removed node", `NOT_ADDED` |
