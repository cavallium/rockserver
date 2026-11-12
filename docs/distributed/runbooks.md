# Replicated Rockserver: runbooks

What an operator does to run a cluster: start one, seed it, replace a node, take and restore backups, check that the
replicas agree, react to the alerts, and upgrade. The reasons behind each step are in [design.md](design.md); this page
only says what to do and what to check.

In the commands, `rs` stands for the node's launcher. The build makes no runnable jar: the launcher runs the classes
of a checkout of the version to run, with the JVM flags the in-repo launchers pass (README.md; the process tests'
`ProcessCluster`, `BenchNodes` and `distributed/bench/load-bench.sh`). Build them once per version, in its checkout,
through the reactor (`-am`): Maven then takes `rockserver-core` from the checkout (`target/library/classes`) and every
other jar from the checkout's own poms. Run on `distributed/server/pom.xml` alone, it would look for
`rockserver-core:<version>` and that version's pom in `~/.m2` or a remote repository: it fails where none of that
version was ever installed, and elsewhere picks the third-party jars of whichever build was. Nothing of the checkout is
installed into `~/.m2`, but a run without `-o` downloads the third-party jars and the Maven plugins it lacks into
`~/.m2/repository`; `-o` works once they are there.

```bash
mvn -q -f distributed/pom.xml -pl server -am compile dependency:build-classpath \
  -Dmdep.outputFile=target/runtime.classpath -DincludeScope=runtime
# The server has no SLF4J provider at runtime (logging would go to a no-op logger): add the process tests' one.
mvn -q -f distributed/pom.xml -pl server -am compile dependency:build-classpath \
  -Dmdep.outputFile=target/logging.classpath -DincludeArtifactIds=slf4j-simple
T=$PWD/distributed/server/target
RS_CP="$T/classes:$(<"$T/runtime.classpath"):$(<"$T/logging.classpath")"
alias rs="java -XX:+ExitOnOutOfMemoryError --enable-native-access=ALL-UNNAMED --add-modules jdk.incubator.vector \
  -cp '$RS_CP' it.cavallium.rockserver.distributed.ReplicatedServerMain"
```

Each module of the reactor writes its own `target/*.classpath`; the server's are the ones read. The scope filter is
`-DincludeScope`, not `-Dmdep.includeScope`, which the plugin ignores (it would add the test jars). Any SLF4J 2
provider can replace `slf4j-simple`, which writes to standard error.

`rs` without a command prints the usage and exits with 2. That proves only that the JVM flags are accepted and the main
class is found: the JVM loads classes when first used, and the usage loads none of RocksDB, gRPC or Netty. A missing
jar shows when a command first needs it, for `run` possibly part-way through a start (exit 1: an `Error`, a class that does not load). To check the
classpath, start a throwaway single node on a scratch directory and loopback ports, which loads them all, and stop it
once it logs `started` and becomes leader:

```bash
rs run --node-id n1 --peer n1=127.0.0.1:47911/127.0.0.1:47912 --data-dir /tmp/rs-check \
  --raft-bind-host 127.0.0.1 --api-bind-host 127.0.0.1   # then Ctrl-C; rm -r /tmp/rs-check
```

Without `--enable-native-access=ALL-UNNAMED` the JDK warns about RocksDB's native access (a later
JDK may refuse it); `--add-modules jdk.incubator.vector` is the README's command line (no Rockserver code of this
version needs it). For a service unit, put the same `java` command line, with the classpath written out, in `ExecStart`.

`run` extracts the RocksDB library into `DATA-DIR/tmp` by itself, with or without `-Djava.io.tmpdir`, and deletes
there the 17 MB copies that killed runs leave behind. It also points `java.io.tmpdir` there for libraries that read the
property later (Netty), but the JDK's own temporary files (`File.createTempFile`, `Files.createTempFile`) keep the
directory the JVM started with, `/tmp` by default. To move everything, pass `-Djava.io.tmpdir=DATA-DIR/tmp` (for
example `/srv/rockserver/data/tmp`): a given directory inside the data directory is the node's own, and the node deletes
the RocksDB copies in it at every start. A given directory outside the data directory, or one that is a symlink to a
shared directory such as `/tmp`, is warned about at the start and never cleaned by the node, since another JVM of the
host may be loading its copy there. While the database opens, the node keeps the config it opens it with in
`DATA-DIR/rockserver-replica-for-db.conf` and deletes it once the database is open; a node killed during the open (a
long WAL replay) leaves it, and its next start replaces and deletes it. The randomly named
`rockserver-replica-<digits>.conf` files that older versions left there are deleted at the start too; nothing else is.
`export-seed` keeps the config of its target next to it, as `rockserver-replica-for-<target name>.conf`, until it ends,
whatever the target is called (a date such as `DATA-DIR/20261108` is fine). Both files hold the resolved Rockserver
config, the InfluxDB token and every `${?ENV}` substitution included, so they are created owner-only (rw-------)
whatever the umask; RocksDB's and Raft's own files are not, so keep `DATA-DIR` itself at 0700 or 0750
(`chmod 0750 /srv/rockserver/data`). The node also logs the resolved config at INFO each time it opens its database
(`Database configuration: {...}`: at the start, after each snapshot install, and in `restore-backup` and
`export-seed`), into the journal under systemd. The InfluxDB `token` and `user`, and a `user:password@` in its `url`,
print there as `<redacted>` (R60-1); every other value prints as it is, `${?ENV}` substitutions included, so keep
secrets out of other fields, or start the node with `-Drockserver.core.print-config=false` to leave the line out.

Run the offline commands (`export-seed`, `repair-log`, `restore-backup`) as the user the node runs as (the service
unit's `User=`), not as root through `sudo`. They create and rewrite files in `DATA-DIR`: `restore-backup` builds
`db/`, `export-seed` may finish a restore or restore a snapshot into `db/` and opens the database (RocksDB writes its
`LOG`, `OPTIONS`, `MANIFEST` and WAL files), `repair-log` rewrites a log segment, and `export-seed` and a `repair-log`
that is not a dry run create `raft/<group>/in_use.lock`, which stays. Each file gets the owner and the umask of whoever
ran the command, and a node of another user then fails to start on a denied permission that names the file. If that
happened, give the data directory back to the node's user (`chown -R <node user>: /srv/rockserver/data`, with the
unit's `User=`) before the start.

The commands below use `PEERS` for the peer list of the cluster, one `--peer` per member, the same on every node and
in every `admin` command:

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
   `FOLLOWER`, with the same commit index soon after. Each line reads `<id>: <role>, applied index A, commit index C,
   commit infos [...]`: the member's own applied and commit index, then the commit index it knows of each member. Every
   member gives the two indexes, whatever its version (Ratis' group-info answer carries them); only an `rs admin` of a
   version before R54-4 prints none. A member's line can lack its commit index, rarely, when the entry at that index is
   not in its log: its applied index is then enough. `status` asks every member at once, waits up to 3 s in all for
   the answers, and prints them in the `--peer` order: a member that is down prints `unreachable` and does not hold the
   others. With an unroutable member the command may take about 5 s more to exit after printing, while it closes its
   connections.

## Seeding a cluster from a single-node database

The data of a single Rockserver becomes the first state of the cluster. Every node must start from a copy of the
same database. Each node records a digest of its copy at its first start, and a replica whose digest differs from the
leader's stops at genesis with exit code 3 (see "One seed" in the design document): that catches a missing, extra or
truncated file and a copy taken at another moment, but not a table file damaged without a change of size, which only
the check of step 3 finds.

1. Stop the single node (a clean stop: its database must be closed). Do not copy a database that is running, and
   if the node crashed, start it once and stop it cleanly before you copy it: a crashed or running database can hold
   table files its MANIFEST does not reference, which the first start's check deletes as it opens the copy, and a
   first start killed during that check then no longer recognizes the seed and stops at genesis (Review of R43, 2).
2. Copy its database directory to every node, as `<data-dir>/db` (for example `rsync -a --delete old-db/
   n1:/srv/rockserver/data/db/`). The data directories must hold nothing else: no `raft/`. Then run `sync` on every
   node (or `sync -f <data-dir>/db`): `rsync`, `cp` and `scp` do not fsync, and a power loss before writeback can
   leave copied table files empty or with holes. The first start fsyncs `db/` too, before it records the digest
   (R34-1), with every column volume (a node refuses to start with one outside `db/`), but not before
   the start.
   If the nodes' `wal-path` is outside `db/`, the seed's WAL files do not go there: put them in `db/wal.detached/`
   on every node (create it; the node moves them into its WAL directory at the first start, and the seed digest covers
   them), and leave the WAL directory itself without `*.log` files. A first start refuses a seed without
   `wal.detached/` while the WAL directory holds WAL files.
3. Check that the copies are equal, for example `find db -type f -exec sha256sum {} + | sort -k2 | sha256sum` on
   each node, the same result everywhere. Do not skip it: the digest of the first start compares table files by size
   only.
4. Start every node as for an empty cluster. The first leader proposes a seeded genesis; every replica then takes a
   snapshot of the seed (the log says `snapshot taken`).
   A first start that fails or is stopped before the node joins (a refused config, or a kill or a start timeout while
   it opens a large seed to check it) can simply be started again: it keeps the digest it recorded before it opened the
   database (R41-1; the log says `the seed digest recorded before it is kept`). If you copy the seed again in between,
   always delete `<data-dir>/db` and `<data-dir>/db.seed-digest` first, whatever tool you copy with: the node then
   records the digest of the new copy. `rsync -a --delete` over the old `db/` is not enough (R43-1): `db.seed-digest`
   sits next to `db/`, outside what it deletes, and a later copy of the seed's own source database keeps the same
   `IDENTITY` and the table files the source did not compact, which `rsync` leaves untouched. The node finds such a
   copy only when the source compacted away a table file of the seed, and another database copied over `db/` only by
   its `IDENTITY`. If the log says it kept the digest and you put another database in place after such a stop, delete
   `<data-dir>/db.seed-digest` before the next start. Hard links to the seed's files coming and going in between are
   fine (R45-1): deleting a backup copy that `restore-backup` linked into `db/`, or another copy made with `cp -al`,
   or linking one. The node tells such a change from a copy made again by the files' birth time, which ext4, XFS,
   btrfs and tmpfs keep; on a file system without birth times, or if a link to one file is removed and another added
   back, it takes the files for a new copy, records a new digest and stops at genesis with exit 3: copy the seed again
   as above.
5. Check: `rs admin $PEERS checksum` prints `OK` and the same digest for every node.
6. Only then point clients at the cluster.

If a node stops with "seeded with an existing database, but this replica's database is empty", it started without the
copy: see "Membership" in the design document, and replace it as below.

### Building the seed from some columns (`build-seed`)

Use `build-seed` instead of copying the single node's `db/` when that database cannot be a seed as it is: its column
volumes or its WAL are outside its directory (a node refuses a volume outside `db/`), or some of its columns must not
go to the cluster. It builds a new database directory in the cluster's layout that holds only the chosen columns,
with their schemas, by hard-linking their table files. It does not read the tables, so the time does not depend on
their size when source volume, export directory and target are on one file system.

```
rs build-seed --source-db OLD-DB --source-config old-rockserver.conf --target /disk/seed \
              --rockserver-config cluster-rockserver.conf --exclude-column big1 --exclude-column big2
```

- `--source-db` and `--source-config` are the stopped single node's database directory and config (step 1 above
  applies). `--rockserver-config` is the config the cluster's nodes run with: the seed gets its volumes inside it,
  and, when the cluster's `wal-path` is outside `db/`, a `wal.detached/` directory. `--column NAME` (repeatable)
  takes only those columns, `--exclude-column NAME` all but those; not both.
- The source is opened read-write, with its config but without automatic compactions: the open replays its WAL and
  its memtables are flushed, as a start and stop of the single node would. No table file of it is rewritten or
  deleted. It is refused while a Rockserver runs on it. Run it on a snapshot or clone of the volumes if the source
  must stay untouched.
- While a column is exported, the source database directory holds a symbolic link for each of the column's table
  files that is in a volume outside it, and a `column-export.<pid>.<id>` entry; RocksDB's export finds table files
  only in the database directory. They are removed before the command ends. If the command is killed, delete the
  `*.sst` symbolic links and `column-export.*` in the source database directory, the `*.build-seed-export`
  directory next to the volume (or `build-seed-*` in `--export-dir`) and `<target>.tmp`.
- Hard links need one file system. The table files are first linked into an export directory, by default
  `<volume>.build-seed-export` next to the column's volume (pass `--export-dir DIR` when the volume is a mount point,
  whose sibling is on another file system), then into the target. Where a step crosses file systems, the files are
  copied, and the command prints how many bytes before it copies them. Each column ends with a line
  `column NAME: N files, B bytes, linked|copied` (`empty` for a column without table files), and the command with
  `OK`. On a failure it prints `Not built: ...`, exits with 1 and leaves no target.
- Pass a `--rockserver-config` without the per-column `column-options` stanzas of the columns being built (keep
  `default` and the internal ones): a stanza makes the new database create that column when it opens, and the import
  then refuses it (`Column exists, table files cannot be imported into it`). The nodes run with the full config.
- Refused: a column whose schema names a merge operator (an uploaded one, or a class): leave it out. A column merged
  by the `merge-operator-class` of the source's config needs the same one in `--rockserver-config`. A column with
  blob files. An existing target or `<target>.tmp`.
- The linked seed shares its table files with the source. Copy it to the nodes before the single node runs again if
  you want the three copies taken from one unchanging directory; the source deleting its own files later does not
  change the seed.

**Do not carry `adaptive-compaction-io` over from a single node's config.** A replica runs without core's
compaction I/O limiter whatever its config says (since 3.0.1; in 3.0.0 set `adaptive-compaction-io: false` in the
cluster's config). The limiter takes the write rate of its first active five-second window as its budget, so a small
first flush leaves it at its floor of 40 KiB/s, and it then grows by a tenth per window. A replica's snapshot flushes
its memtables while its applies wait: on the cluster seeded on 2026-11-12 with a config copied from the single node
(`adaptive-compaction-io: true`), the first snapshot took 206 to 229 s on each of the three nodes at once, and writes
timed out for four minutes. Later snapshots took 1 to 3 s only because the budget had grown by then; every restart
starts the limiter again. Reproduced by `NodeStoreTest.aCheckpointIsNotThrottledByTheAdaptiveCompactionIoLimiter`
(125 s for 16 MiB with the limiter, under a second without).

Give each node its own snapshot thresholds all the same, so that they never snapshot together: for example
`--snapshot-every` 2000000, 2500000 and 3000000 with `--snapshot-every-bytes` 6, 7 and 8 GiB.

Then continue with steps 2 to 6 above, with the built seed as the database directory to copy. Build one seed and copy
it to every node: two builds do not give the same files.

Not tested: columns larger than a few megabytes (the time on terabytes is not measured), a column with several
volumes, a source with a crashed (unreplayed) WAL, a kill of the command, and the source config's less common
options (per-column table formats or comparators differing from the cluster's config are not compared: the seed's
columns get the cluster's options, as columns created on the cluster do).

## Replacing a node

For a node that is lost (dead disk, dead host) or that must move. One node at a time, and only while the others are a
majority and healthy (`status`).

1. Pick a new id that was never used, say `n4`. On the new host, with an empty data directory, start it with
   `--join` and a peer list that has the members that stay plus itself:
   `rs run --node-id n4 --peer n2=... --peer n3=... --peer n4=... --data-dir /srv/rockserver/data --join`.
   It prints `READY` and waits for as long as it takes.
2. From any host, change one voter per command. `admin` reads the leader's configuration first (it waits while a change
   is under way) and refuses, with exit code 1 and before it proposes anything, a call that would add and remove
   voters together or change two of them, or whose `--peer` gives a member that stays another address than the
   configuration (the addresses in `--peer` are written into the new configuration): see "One voter per change" below. Old node `n1`:
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

   `remove-peer` of an id that is not a voter of the leader's configuration proposes nothing. An id that differs from
   a voter only by case (`N1` for `n1`: ids are case-sensitive) fails with exit code 1, `FAILED: N1 is not a voter
   (voters [...]): likely a typo of n1 ... Nothing was proposed`: run it again with the id as printed. Any other id
   prints `n11 is not a voter: nothing to remove (voters [...])` and `OK` with exit code 0, because that is also what
   the re-run of a removal that already committed prints (its id is gone from the configuration): `OK` after that line
   means nothing was removed by this call, so check the voters it printed.

   Do not add first while the old node is down: with 4 voters and one lost, all 3 others are needed, and a failure of
   the new node during its catch-up stops the cluster (see "Heap" in the design document). `add-peer` ends with `OK`
   once `n4` caught up from a snapshot and the configuration changed (the limit, `--timeout-ms`, is 10 minutes by
   default: raise it for a large database, about 100 MB/s).
3. Check, with `--peer` listing the members that stay and `n4` (`--peer n2=... --peer n3=... --peer n4=...`: `status`
   and `checksum` ask only the members listed): `status` shows `n4` with the others' commit index; `checksum` prints `OK`; the `rockserver-node-missing`
   alert threshold and the clients' address lists include `n4` and not the old node. Check that the old id is gone
   from the voters before step 4, from a call made after the removal: run the `remove-peer n1` again, with the same
   `--peer`. It reads the configuration first and prints `n1 is not a voter: nothing to remove (voters [n2, n3, n4])`
   and `OK`: the voters it lists must not include `n1`. (`add-peer n4` again, which prints `already a voter` and `OK`,
   also reads it after the change: its `leader ...: voters [...]` line must not list `n1`.) Do not read the voters
   from the call that removed: it prints the configuration read before its proposal, the old id in it,
   `leader n2: voters [n1, n2, n3, n4]; this change adds [] and removes [n1]`, then `OK` when the removal committed.
   If the check's run instead prints `removes [n1]` and `OK`, the earlier call had not removed it and this one did. If
   the voters printed by the first `remove-peer` after `nothing to remove` still list the old id, its id was mistyped:
   run it again with the id as listed, with `--peer` still holding the old node. These re-runs work with the lists of
   step 2 also when `n4`, which they do not list, leads by now: the command asks the leader that the listed members
   name and prints `NOTE: the leader n4 is not in --peer: asked at ...` first (R61-11; before, it waited the whole
   `--timeout-ms` and failed with `still waiting for a leader (n2 is FOLLOWER, n3 is FOLLOWER)`: on a version that
   does that, add `--peer n4=...` to the re-run).
4. Update `PEERS` (every node's `--peer`, and the `admin` commands) to the new members: `n4` added, the old node
   gone, only once step 3 shows it is no longer a voter. The running nodes need no restart for it (they learned `n4` from the configuration, and forward to it by
   asking it at that Raft address); use the new list at their next restart. Never reuse the old id at another
   address (see "One voter per change").

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
- `add-peer` or `remove-peer` says `FAILED: refused: --peer gives members that stay another address than the leader's
  configuration: ...`: a `--peer` address is mistyped, or names another spelling of the host. Nothing was proposed.
  Fix `--peer` to the addresses printed and run it again; `--change-addresses` is only for a configuration that
  records a wrong address of a member that stays, on a call that adds or removes a voter (see "One voter per
  change"); it never moves a member.
- `add-peer` ends with no result within the limit: the leader goes on with the change. Run the same command again until
  it says `OK` (once the node is a voter it says `already a voter` and `OK`); `status` shows the progress. If the id
  was a member before at another address, it never ends (see "An id keeps its address" in "One voter per change"):
  check with `status` that it is not a voter, then stop that node, wipe it and add it under a new id. Do not wipe
  the new node meanwhile, whatever a re-run says about its log (see the first branch).
- The lost node was the leader: the same steps, after the election (`status` shows a new leader).
- Two nodes of three lost: there is no majority, `add-peer` cannot work. See "No quorum" in the design document, and
  "Restoring a backup" below. Without a backup, `rs export-seed --data-dir DATA-DIR --target NEW [--rockserver-config FILE] [--group-id UUID]` on each stopped
  node prints the index its copy holds; pick the highest. It refuses a node that runs (exit 1, `Not exported:
  .../raft/<group>/in_use.lock is locked: stop the node first.`, the message of `repair-log`), also one that is
  installing a snapshot with `db/` closed, before it touches anything of the node. A node that crashed while installing a snapshot can have
  no `db/` at all, its complete new database in `db.restore-ready` (a crash in the middle of swapping it in):
  `export-seed` finishes the swap, as a start of the node would, and exports that database; it prints `No database in
  .../db` only when neither `db/` nor `db.restore-ready` holds one. `--target` must be a new path outside the node's
  `db/`, its `db.restore-*` directories and `raft/`: `export-seed` refuses one inside them (also through a symbolic
  link, a dangling one too, and `DATA-DIR/db` itself when it is missing) with `a directory of the node being exported`,
  before it touches anything (R34-2). It builds the copy in `NEW.tmp` and refuses a `NEW.tmp` that already exists
  (`The target's temporary directory ... already exists`) rather than delete it: remove it, or choose another target
  (then also delete the `rockserver-replica-for-<name>.conf` next to it that the refusal names, if any: see
  "Backups"). It can also have `db/` behind that snapshot: `export-seed` verifies the snapshot and restores it into
  `db/` first, as a start of the
  node would, and its output then says `its database was behind its snapshot at index N, restored first`. If the
  crash damaged that snapshot, `export-seed` exits 1 with `Not exported: The replica's latest snapshot ... (at index N)
  is damaged`, the database's index, and writes nothing: that replica may have applied entries up to N that its `db/`
  lacks. Export the other nodes first; if none reaches N, `--without-damaged-snapshot` exports this `db/` as it is,
  and the output then says `without its damaged snapshot at index N`. Pass the node's `--group-id` if it runs with
  one; `export-seed` refuses (exit 2) a group, given or the default, that has no directory in a `raft/` holding other
  groups (a mistyped `--group-id` included).

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
| `NO_MAJORITY`, `NO_COMMITTED_CONFIGURATION` | Start or repair the pending and unreachable members (a member of a version without `Configuration` counts as unreachable). The node decides by itself once a majority of them answer. If they are gone for good (wiped, replaced), check with `status` on a member: if this node is not in its configuration, stop it, take it out of its supervisor, wipe its data directory, and add it again with `--join` and `add-peer` if you want it back, at its old address (at another one, add it under a new id: see "One voter per change"). |
| `NEEDS_TERM` | The members that answered run a version without `Configuration2`: upgrade them. Their answers tell no terms, and their committed flag does not confirm a removal unless a member of this version (or the node itself) reports the same configuration committed. Until then judge with `status`, as above. |
| `MEMBER_LATER_CONFIGURATION` | Clears by itself within seconds. If it lasts, the blocking members are stuck or were removed themselves: look at their logs and `status`. |
| `OWN_LATER_CONFIGURATION` | Its own log holds a later configuration that includes it and may still commit: normally an `add-peer` of this node in flight (let it finish), or a configuration a leader appended just before it lost leadership, which clears once the next leader's configuration reaches this node. If it persists while a leader runs, check `status` on a member. |
| `NOT_ADDED` | A `--join` node that never was a voter: run `add-peer`. If the add was abandoned, or the node was added and removed long ago, stop it and wipe its data directory (`add-peer` refuses a node that has a log). A removed id comes back only at its old address (at another one, add it under a new id: see "One voter per change"). |
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
listeners, addresses and priorities count nothing toward that rule (addresses are checked on their own, below). The
change is sent as a compare-and-set against the configuration read: if another change commits first, `admin` reads it again and checks again. The leader refuses the same on its
own, at once (`FAILED: the leader refused: changes more than one voter ...`), also for an `admin` of an older version
(which prints `FAILED: ... or the leader refused: refused: changes more than one voter ...`) or the Ratis tools, but
only a leader of this version does: during a rolling upgrade a leader of an older version has no such check, and an
older `admin` gets its one-step replacement through. Until every node that can become leader is upgraded, change one
voter per call with every `admin` (see "Upgrades"). Several dead nodes are removed one per call; a re-run of a call
that succeeded prints `OK` again (for `add-peer`, `already a voter`; for `remove-peer`, `is not a voter: nothing to
remove`, which a mistyped id prints too: check the voters listed; an id that differs from a voter only by case fails
as a likely typo, R59-1). If the leader refuses for another reason that a
new read of its configuration does not change (Ratis' own `changes a majority set`, say), `admin` fails at once with
its message (`the leader n2 (term 7) refused, and its configuration did not change since: ...`).

The new configuration is written from `--peer`: the address given there for every member that stays is the address
the configuration records for it, which a node started with `--join` and a node that finds the member missing from its
own `--peer` use (a running node keeps the address it knows for a member id, so a wrong address shows only then). `add-peer` and `remove-peer` therefore refuse,
with exit code 1 and before they propose anything, a `--peer` address of a member that stays that differs from its
address in the leader's configuration: `FAILED: refused: --peer gives members that stay another address than the
leader's configuration: n3 (at 10.0.0.3:9001 in the configuration, 10.0.0.3:9010 in --peer)`. Addresses are compared
as written, so `localhost:9001` is not `127.0.0.1:9001`. Fix `--peer` to the addresses the refusal names as those of
the configuration.

**An id keeps its address.** To move a member to another address, replace it as in "Replacing a node" (the old node
runs: a move): start a node under a new id at the new address, with an empty data directory and `--join`, `add-peer`
it, then `remove-peer` the old id. The member that moves is the one added or removed, never one that stays. Do not
bring an id back at another address (`remove-peer n3`, n3 wiped and started at a new address with `--join`,
`add-peer n3=<new address>`) while the other nodes run: Ratis 3.3.1 keeps one connection per member id on every
node, made from the address it first knew for that id (its `--peer`, then its configuration) and never replaced
while the node runs, so the leader keeps sending to the old address. n3 gets nothing, `add-peer` ends without `OK` at
its limit and every re-run does the same, and the cluster stays one voter short; a restart of the other nodes does
not help while their `--peer` gives the old address, which they read first, and their calls forwarded to n3 as
leader go there too. A removed id comes back only at its old address ("Removed node"). Only when the configuration
itself holds a wrong address of a member that stays (written by an `admin` without this check, say), pass
`--change-addresses` to an `add-peer` or `remove-peer` call that adds or removes a voter and whose `--peer` gives the
right one: it prints `--change-addresses: this change also moves ...` and writes the `--peer` addresses. For a
node started without `--join` that fixes the record only: it already uses the address of its `--peer`. A node
started with `--join` takes its addresses from the configuration it first applies and keeps them while it runs, so
every running node started with `--join` (the one this same `add-peer` adds included) still sends to the old address
(R53-4). Once the call printed `OK`, run `status` and note the leader's `commit index`: the configuration entry is
at or below it. Then restart those nodes one at a time, each one only when `status` shows its own `commit index` at
or past that number (its `applied index` there is enough too, it is never past the commit index), and the next one
only when the restarted node is back and its `commit index` has reached the leader's again. A node's commit index
never passes the end of its own log, so its log then holds the configuration entry, and a restart loads every
configuration entry of the log past the snapshot after `raft-meta.conf` (Ratis 3.3.1 `ServerState.initRaftLog`), so
an apply backlog does not matter (review of R54, 1). If a node was restarted before its commit index reached the
number, restart it once more after it did. Ratis
writes nothing for a configuration whose voters do not change, so `admin` refuses `--change-addresses` on a call
that adds and removes no voter (a re-run of a `remove-peer`, or an `add-peer` of a voter already) with exit code 1: `FAILED: refused:
--change-addresses on a call that adds and removes no voter ... Nothing was written.` Wait for the next replacement
to write it.

`admin` asks every `--peer` at once (a down member delays each round by one request timeout, not one each) and takes the configuration of the leader of the highest term; it waits while a member
shows a later term than the leader that answered. The leader need not be in `--peer` for `add-peer` and `remove-peer`
(R61-11): when the members of `--peer` follow a leader that is not listed, the command asks that leader too, at its
Raft address in their configuration, and prints `NOTE: the leader n4 is not in --peer: asked at ...` (once per command) before the
voters. If that leader does not answer at that address from the host that runs `admin`, the command waits until
`--timeout-ms` and fails with `still waiting for a leader (n2 is FOLLOWER, n3 is FOLLOWER); n2 names n4 as the leader,
which is not in --peer and did not answer at ...: add it with --peer ID=RAFTHOST:PORT/APIHOST:PORT`: run it again with
that member in `--peer` (nothing was proposed). A change is still built from `--peer`: with the leader left out, the
only change that passes the one-voter rule is the removal of that leader itself; anything else is refused with the
voters printed, and nothing is proposed. The other commands are unchanged; `status` and `checksum` report on the
members listed only, so list every member for those. A leader that has just been deposed answers as the leader for about
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
later with `rs admin $PEERS backup-status 2026-11-06 <index it printed>`, which proposes nothing. It also exits with 4
when no node gave the log index in time. If it says `Nothing was proposed`, no node took the request (or each one that did refused it as not applied, such
as when no leader accepted it in time, or was still starting: its answer begins with `Not proposed: `, or, from a node
of an older version during a rolling upgrade, is exactly `The node is starting`): run it again.
If it says a node `took the request ... it may have been proposed`, the backup may still be written (a slow leader
appended it, or the node's connection broke after it took the request, such as a node restarted or killed mid-call:
it may have sent it to the leader already): do not run it again under the same name, which is refused (`A backup named 2026-11-06 exists already`,
exit 1) once the first one is written. Look on the nodes for `<data-dir>/backups/2026-11-06/backup.properties` with
the `request=` the message printed (its `index=` is the log index), or for `backup 2026-11-06 at log index` in their
logs, and check it with `backup-status 2026-11-06 <index>`; or take the backup under another name. Each node's apply stops for
the flush (100–300 ms); the files are hard links, so a backup costs space only as compactions replace the files it
holds.

A node on which the backup failed (a full disk, say) deletes what it had written of it, `backups/<name>/db.tmp` or a
`db/` without `backup.properties`, since those hard-link the live files too. If it cannot, its log says `cannot delete
... delete it by hand`: a `<data-dir>/backups/<name>` without `backup.properties` is always a failed or interrupted
attempt, and can be deleted. A backup of that name that existed already is never touched by the failure of another
one. `export-seed` likewise leaves neither its target nor `<target>.tmp` when it fails. Its target appears only
complete, without the old cluster's metadata: an export killed before its last rename (OOM, SIGKILL, power loss)
leaves `<target>.tmp`, which a rerun refuses (`The target's temporary directory ... already exists`): delete
`<target>.tmp` and run the export again. An export killed after it leaves a complete target but printed no applied
index: delete that target too and export again. Either kill also leaves the copy's config next to the target,
`rockserver-replica-for-<target name>.conf` (owner-only, the resolved Rockserver config with its secrets, such as the
InfluxDB token and the `${?ENV}` values) (R53-6): both refusals name it when it is there. A rerun to the same target
replaces and deletes it; if you export to another target instead, delete it by hand with `<target>.tmp`.

Then, for each backup:

1. Copy one node's `<data-dir>/backups/<name>` off the cluster (all of them hold the same data, that of one log
   position; `backup.properties` says the log index and cluster). A backup that `admin` proposed twice (after a lost
   answer) is the copy taken at the first entry on every node: the nodes that have it say `DONE` with `taken at log
   index <first>`, and a node that installed a snapshot past that entry says it holds no copy, which makes the command
   fail naming it; copy the backup from a node that has it, or take one under another name to have it everywhere.
2. Delete `<data-dir>/backups/<name>` on every node once it is copied: until then the hard links keep the replaced
   files on the node's disk.
3. Now and then, check a copy by restoring it (below) on a spare host.

### Restoring a backup

A restore starts a new cluster, seeded from the backup: the writes after the backup are lost, and client sessions start
over (clients open new ones by themselves).

1. Stop every node of the old cluster, and copy their data directories aside before anything else.
2. On every node of the new cluster, into an empty data directory, from a copy of the same backup:
   `rs restore-backup --backup /path/to/<name> --data-dir /srv/rockserver/data [--rockserver-config ...]`.
   It prints `OK`, the data directory and the log index the data reaches. It refuses a data directory with a `db/`, a
   Raft log, or a restore directory of `db/` (`db.restore-tmp`, `db.restore-ready`, `db.restore-old`, which a crash
   leaves in a restore into `db/`: a snapshot install of the old cluster, see "export-seed" above, or a `restore-backup`
   of a version before the build aside, which restored straight into `db/`) with exit code 1, before it looks
   at the backup and touching nothing; the message names the directory. "Empty" means without any of these: with the
   copy of step 1 made, delete the named directories (or empty the whole data directory) and run the command again.
   Since the database is built aside, nothing would roll such a directory in before the restore, and the first start
   would swap an old `db.restore-ready` over the restored `db/` and delete it. It reads the backup and builds the database aside, in `DATA-DIR/db.restore-backup`, checking its cluster
   version; only then does it rename a `replica-ops.log` and a `backups/` of the old cluster found there (suffix
   `.before-restore-<time>`) and fsync the directory, and then rename the built database to `db/`, so that their
   results never answer for the new cluster. A backup inside `DATA-DIR/backups` (the node's own
   `--backup DATA-DIR/backups/<name>`, also through a link) works: it is read completely before it is renamed with
   `backups/`, and is then found, intact, under `backups.before-restore-<time>/<name>`. Every command takes `--data-dir` absolute and normalized (`/x/link/..` is `/x`, not the parent
   of the link's target): use the same `--data-dir` for `restore-backup` and for the node's start, and the `OK` line
   names the directory both use.
   It refuses a backup of a newer cluster version than this binary supports with exit code 5 before it renames
   anything, and leaves no `db/`: restore it with the newer version. Run that newer version on every node of the new cluster as well: the restored
   `db/` carries no cluster version, so an older binary started on it is not refused, and would hold data written by a
   newer version under the oldest cluster version. On any other failure (a `--rockserver-config` that does not parse,
   a column that does not open, a full disk) it also removes what it built, and renames nothing if it failed before
   the renames, so fix the cause and run the same command again. A run interrupted (power loss, `kill -9`) before
   `db/` appeared leaves at most `db.restore-backup` (with possibly `db.restore-backup.restore-tmp`, `.restore-ready`
   or `.restore-old`, and `rockserver-replica-for-db.restore-backup.conf`, which holds the resolved config with its
   secrets) and the renames: run the same command again, which deletes all of those and finds nothing more to rename;
   if that run had already renamed the `backups/` your `--backup` pointed into, the rerun says where the backup is
   now (`run again with --backup .../backups.before-restore-<time>/<name>`). A `wal-path`, `log-path` or volume path
   of the `--rockserver-config` that is absolute inside `DATA-DIR/db` is fine: the database built aside keeps them
   under `db.restore-backup`, and they are in `db/` after the rename.
   A `db/` left by a run interrupted in its last step (recording `db.seed-digest`) is not removed: delete
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
   Run `sync` after copying the backup (and after copying a restored `db/`): the copy tools do not fsync. On the
   same file system `restore-backup` hard-links the backup copy's table files and fsyncs each one (R34-1), so a power
   loss right after it cannot damage both `db/` and the copy, but a sync right after the copy keeps that from
   depending on the command. The backup copy can be deleted once the restore printed OK, also before a first start
   that was stopped during its start-up check is started again (R45-1, see "Seeding a cluster", step 4).
3. Start every node as for a new cluster, without `--join`; the nodes of the old cluster stay stopped. The first leader proposes
   a seeded genesis, as in "Seeding a cluster".
4. Check: `status`, then `checksum` prints `OK`, then a few known keys read back.

## Checksums

`rs admin $PEERS checksum` makes every replica digest its user data at one log position and compare it with the others.
It prints each node's digest and number of keys, and `OK` only if every node of `$PEERS` gave a digest and all are
equal. It exits with 1 if they differ, and with 4 if a node gave none (down, failed, not done within `--timeout-ms`,
or it skipped the position because a snapshot install jumped over it), or if no node took the request within
`--timeout-ms` (no leader, every node down) or the answer leaves open whether it was proposed: run it again. An
answer leaves that open also when the connection to the node that took the request broke before it answered (the node
was restarted or killed, a reset): it may have sent it to the leader already, so the request is not sent to another
node, which could start a second digest on every replica. Only a node's own refusal that begins with `Not proposed: `
(no leader accepted it in time, the node is starting) moves the request to the next node. The
nodes of `$PEERS` are first asked at once whether they answer (up to 3 s, or what is left of `--timeout-ms`), and the
request goes to them in the order they answered; one that does not answer (down, or frozen with its port still
accepting connections) gets nothing. Then the results are asked of every node at once, again and again until each has
its own, each question within 3 s or what is left of `--timeout-ms` (as for `backup` and `backup-status`). So a frozen
node delays neither the proposal nor the other nodes' results, also with a short `--timeout-ms`, and the command ends
within `--timeout-ms`, plus at most 1 s when the proposal itself used the limit up (the nodes are asked for their
results once more for that long), and the time to close its connections. A node that took the `checksum` request is waited for
up to `--timeout-ms`, since it bounds the request itself with its own `--request-timeout-ms` and an answer cut short
would leave open whether the digest runs; `backup` waits at most 30 s per node and then proposes the same backup through
the next one, which is harmless (its request id makes the second proposal a no-op if the first commits: every node
keeps the copy taken at the first entry, see Backups).
`--allow-skipped` accepts nodes that skipped the position (it is refused with any other command, as
`--unsafe-multi-voter-change` is with any command but `add-peer` and `remove-peer`). The digest reads every key of the
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

## Node does not start after a power loss

A node that lost power (or crashed) while writing can have a torn last entry in its Raft log, which Ratis refuses:
`run` exits with 1, and the stack trace shows `Log entry corrupted`, an `InvalidProtocolBufferException` or `Read
extra bytes`. The start failure then also prints the `repair-log` command for that node: it does so for any failure
thrown while Ratis read a log segment, which includes I/O errors that are not damage (a denied permission, an `EIO`
of the disk, memory running out), so read the cause in the stack trace too. The error does not tell a torn tail from
damage; `repair-log` does, so follow its verdict:

1. Stop the node's supervisor, so that it does not restart it meanwhile (`repair-log` refuses while the node runs:
   its `in_use.lock` is locked).
2. `rs repair-log --data-dir DATA-DIR --dry-run`, with the node's `--group-id` if it runs with one. It prints a
   few lines on what it found, then exactly one verdict line, the one that starts with `OK:`, `WOULD REPAIR` or
   `REFUSED:`. `OK` and `REFUSED` end the output; `WOULD REPAIR` is followed by one more line, the assumption the
   repair rests on ("This is safe only if that entry was never fsynced, ..."), so its output does not end on the
   verdict. Act on the verdict line:
   - `WOULD REPAIR (dry run, nothing changed): would drop the torn entry expected to have index N ...` (exit 0): go
     on with step 3.
   - `OK: every entry is valid` or `OK: no open segment` (exit 0): the log has no torn tail, so repairing changes
     nothing and the same start would fail again. The failure is elsewhere: read the stack trace's cause (a permission,
     a disk error in the kernel log, memory, the Raft disk full), fix it and start the node; if nothing explains it,
     replace the node ("Replacing a node").
   - `REFUSED: ... does not exist: wrong --data-dir or --group-id?` (exit 1): nothing was read. Check the paths and the
     `--group-id` against the node's `run` command and try again: this says nothing about the log.
   - `REFUSED: ... is locked: stop the node first.` (exit 1): the node, or its supervisor's restart, is running. Go
     back to step 1.
   - Any other `REFUSED` (exit 1, its line says "Nothing was changed: replace the node"): the log is damaged, not
     torn; do not force it, replace the node ("Replacing a node").
   - No verdict line, but a Java stack trace (exit 1, usually an `IOException` such as `AccessDeniedException`): the
     command itself could not read or lock the log, a denied permission or a disk error (`EIO`), the same causes that
     can fail the start. Nothing was changed. Fix the cause (the files' owner and mode, the kernel log), then run step
     2 again and follow its verdict.
3. `rs repair-log --data-dir DATA-DIR` (the same `--group-id`): its verdict line is `REPAIRED: dropped the torn entry
   expected to have index N ...`, followed by the same assumption line. A `REFUSED` here (the log changed since the
   dry run, or the node was started meanwhile) is read as in step 2: it is printed before anything is written. A stack
   trace without a verdict is not: the repair cuts the file, syncs it and reads it again with Ratis, so a failure of
   the sync or of that read (a disk error, `EIO`) can come after the file was already cut. Do not assume nothing
   changed: fix the cause, then run step 2 again, which shows where the log stands (`OK` if the cut was made and is
   sound, so start the node; `WOULD REPAIR` if not, so repair again). A stack trace whose message is `Truncated ... at
   offset ..., but Ratis then read ... entries instead of ...` is different: the file was cut, but Ratis then reads it
   differently than the tool. Do not start the node; replace it.
4. Start the node: the leader sends it what it dropped, and `status` shows it catching up.

The repair is safe only if the dropped entry was never fsynced, as its output says: disks that lose fsynced data (a
volatile write cache) void it. If too few nodes can start to elect a leader, which replacing a node needs, see "No
quorum" in [design.md](design.md): there is no override. [performance.md](performance.md) ("Power loss") has the
same procedure, with the disks it assumes.

A start that fails with `Installed snapshot .../snapshot-<term>-<index> is damaged (...), probably by a crash before it
was synced` is a different case: the snapshot the node received from the leader lost data in the power loss, and its
Raft log may already start after it, so the node cannot catch up by itself. Do not delete the snapshot (it is kept for
inspection) nor run `repair-log`: replace the node ("Replacing a node"). When the message goes on with `the Raft log
starts at index N, past index M after the newest snapshot moved aside (...)` (R58-3), Ratis had also failed to delete
the node's old snapshots after the install (a `Failed to delete existing directory` warning earlier in its log,
usually a disk error): check the disk (the kernel log, SMART) before you reuse it. The same start without that addition
and with a `snapshots.*` directory next to `snapshots/` does not fail: the node goes back to its old snapshots (the
install was cut short) and catches up from the leader.

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
3. If every member is there, check the snapshots: the alert `rockserver-snapshot-old` (its age is the time since the
   node started when it has no snapshot at all, R60-3, so it fires a day after a start too, with a log over 8 GiB;
   a restart starts that count again) and the log (`snapshot taken`, or `snapshot failed`). A failing snapshot is usually a full disk. `rs admin $PEERS snapshot <id>` asks a member for
   one (`<id>` must be one of the `--peer` ids, spelled as there: another is refused as wrong usage, exit code 2, R58-2): it waits up to 2 minutes (or the time left of `--timeout-ms`) in one call, as long as the snapshot ends
   within the wait (R57-1: before, a snapshot slower than 2 s could run twice for one command, or end it with
   `FAILED` although the member took it; the request is still sent again after a broken connection or when Ratis
   ended it early, R56-2, and a snapshot that ends in the 500 ms before it is sent again is not reported: run it again), and,
   when the snapshot fails there, ends with
   `FAILED: <id> took no snapshot within ... ms: its snapshot failed ...`, exit code 1: see that member's log. Once
   the disk is freed it takes one: a failed snapshot asked by `--snapshot-every-bytes` holds the member's snapshot
   requests for at most twice its slowest snapshot, at least 30 s and at most 2 minutes (R56-1), and the command asks
   again within its wait. With a short `--timeout-ms` (30 s or less) the command can still end with `took no snapshot
   within`: run it again; a run at least 2 minutes after the failed snapshot takes one.

## Disk space and write stalls

The alerts `rockserver-disk-low`, `rockserver-write-stopped`, `rockserver-write-delayed`, or `ALARM disk-space`,
`ALARM write-stopped`, `ALARM write-delayed` in the node's log.
The three `ALARM` lines need no metrics backend: the node reads the disks and RocksDB's stall state itself (R61-8;
before, `write-stopped` and `write-delayed` were never logged on a node with JMX and InfluxDB both off or failed).
The alerts need the InfluxDB pipeline.
A line `Failed to initialize jmx metrics` or `Failed to initialize influx metrics` in the log carries the cause as its
stack trace (R61-14); the credentials of the InfluxDB configuration are masked in it.

- Disk space: delete backups that were copied off (`<data-dir>/backups`), then old snapshots are deleted by the node by
  itself once newer ones exist. Budget the database, one older snapshot's replaced files, the Raft log (up to
  `--snapshot-every-bytes` plus the margin) and the backups. Grow the disk if that is not enough.
- A write stall on one node slows only that node's apply; on a majority, it stops the cluster's writes. It comes from
  compactions behind (look at the pending compaction gauges) or a full disk.
- A disk that is full, or that fails a write or an fsync, stops the node with exit code 3 within seconds, as a
  leader or as a follower (R62-2): `the Raft log failed` or `replica failed to` in its log, and `Node N stopped after
  a failure: ... The Raft log of node N failed while the node was running` on stderr. The other nodes serve on. A
  supervisor restarts it, and it stops again the same way for as long as the disk refuses writes, so free the space
  (or replace the disk) first. If the start then stops before `READY` on a Raft log that Ratis cannot read, the
  failed write left a torn entry at the end of the open segment: follow "Node does not start after a power loss"
  (`repair-log`), which is safe here because the node never acknowledged the entry whose write failed. Before
  R62-2 such a node stayed up without storing anything: a leader with `failed to check Raft, retrying` in its log
  every 200 ms, a follower with `the disk probe failed` every second.

## Numbered multi writes refused as overloaded

A client's streamed multi write (`putMulti`, `deleteMulti`, `mergeMulti`) that carries a request id is collected
whole by the node that receives it, and all such collections of a node share a heap budget (64 MiB,
`-Drockserver.grpc.numbered-multi-collect-bytes`). A write that does not fit is refused `SERVER_OVERLOADED`, flagged
not applied. The gauge `rockserver.replication.api.multi_collect.bytes` (per node) shows the bytes taken.

- A gauge that returns to 0 between bursts is load: raise the budget if the heap allows, or send smaller calls.
- A gauge that stays up without traffic means streams whose client went silent. Each is ended once it got no item
  for 30 s (`-Drockserver.grpc.numbered-multi-collect-idle-ms`, 0 turns that off) and answered
  `READ_DEADLINE_EXCEEDED` flagged not applied, so it should fall by itself; if it does not, check that property.
- A healthy client on a slow link can meet the same limit. The limit counts the time between whole items as the
  node receives them, from the start of the call, and an item still in transfer does not count: one item of 4 MiB
  (the most a client numbers) needs more than 30 s below about 1.1 Mbit/s, and a stalled link the same. Such a call
  ends `READ_DEADLINE_EXCEEDED` "no item arrived for 30000 ms while the server was collecting the write", flagged
  not applied, and every resend of it ends the same way. If clients report that text while they are alive and
  sending, raise `-Drockserver.grpc.numbered-multi-collect-idle-ms` on every node to above the time their largest
  item takes (a rolling restart; a silent client then holds its share that much longer), or have them send smaller
  values per call.
- Client hosts that vanish without closing their connections (power loss, a cut link) also keep their other calls
  open until TCP gives up. `-Dit.cavallium.rockserver.grpc.server.keep-alive-time-ms=60000` (off by default; the
  wait for a ping's answer is `...keep-alive-timeout-ms`, 20 s) makes the node ping silent connections and close
  those that do not answer. It also closes the connection of a client frozen for longer than the two together.

## Upgrades

Upgrade one node at a time (a rolling restart): stop it (SIGTERM: a leader hands over first), replace the binary, start
it, wait until `status` shows it with the others' commit index, then the next. Clients see a few hundred milliseconds
without writes per leader restart.

A node that was down for a while needs 2.5 to 10 s after its process started before it has applied what it missed
(about 1.2 s of that is its start) (median 5 s in 23 runs of a test of three nodes): the leader's appender for it is sleeping between tries, up to
three times for 0.5 to 3 s (design document, R66-4). `status` shows the node as `FOLLOWER` with its old indices meanwhile,
and nothing is wrong: wait for the commit index, as above, before restarting the next node.

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
- `admin checksum` and `admin backup` are refused until every member supports them. The leader raises the cluster
  version only after every member answered that it supports more, and it asks them every 10 s (and at each new term).
  So right after the last member of a rolling upgrade restarts, every member refuses, the leader too: `admin` fails
  with 1 (`every member that answered refused for the cluster version it has applied: ...`); run it again in about
  10 s, or once `cluster version is now ...` is logged. Apart from that, a member decides by the cluster version it
  has applied, which lags behind the leader's on a member that is catching up (after `add-peer`, on a slow disk):
  `admin` then prints `<id> refused: Not every member supports ... as far as <id> knows: it has applied cluster
  version ...; asking the next member` and goes on with the next member that answered (nothing was proposed). It
  fails with 1 only when every member that answered refuses so, naming each with the version it gave: if every member
  runs a version that supports the command, run it again once they caught up; otherwise upgrade every member first.
  Which members answered is decided by a first question that proposes nothing, waited for up to 3 s (or the time
  left): a member that answers it later is left out, so if the only member that would take the command (typically the
  leader) is that slow while every member that answered refuses for the version, the command fails with 1 instead of
  asking again within `--timeout-ms`. Run it again.
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
- A node may be rolled back to an older version that the cluster version allows. Its `replica-ops.log`, the cache of
  its checksum and backup results, stays readable: from R55-1 on each line is written as the versions before R54 wrote
  it, followed by a line holding its check, which they skip (R54 alone wrote the check at the end of the line, where
  R53 read it as part of the digest or the backup's directory, and a later version keeps R54's lines, since it only
  appends to the file: before rolling a node back to R53 from R54 or from any later version, if the node ever ran
  R54, move its `replica-ops.log` aside). A version from R54 on that finds, after its own checked lines, lines without the check
  it knows (an older version's appends after a rollback and the upgrade again, or R55's after a rollback to R54)
  forgets those results, which only costs a check's comparison or a `backup-status` answer. As before R54, a
  rolled-back R53 still loads a fragment of a line: a torn last line, or one that R55 leaves in the middle of the file
  (an append cut while the node ran, or a line whose check line was cut off after a power loss), which R55 itself
  rejects. A fragment cut inside a checksum's digest can then show as a false `REPLICAS DIVERGED`: if R53 reports one
  right after the rollback, move `replica-ops.log` aside and run `checksum` again.
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
TimeoutStopSec=600
```

A restart on 4 or 5 is what those codes exist to prevent: the node would refuse to start again, or start a version
that cannot read its data. 2 is always a fault of the command line or of the JVM options, so it fails the same way on
every restart (a malformed or refused option value of `run` included). 1 is not always transient either: of its
causes only a port in use can pass by itself, while a refused data directory or a pointless `--join` fail the same way
each time. 1 stays
restarted because it mixes both kinds and the exit code cannot tell which one it was; the start limit ends a loop of
the deterministic kind. The limit is set wider than systemd's default (5 starts in 10 s), which a node whose failed
start takes a few seconds (the JVM, opening the database) never reaches; 5 in 5 minutes also stops restarting a node
that fails with 3 over and over, which needs an operator too.

`SuccessExitStatus=143` is there because the JVM, not `run`, chooses the status of a stop by signal (below): without
it, every `systemctl stop` leaves the unit "failed". `TimeoutStopSec=600` is there because a stop can outlast
systemd's default of 90 s. The shutdown hook hands over leadership (up to 5 s for the transfer and 3 s for the
successor's commit), then closes the node in place. First the API server (R53-2): its gRPC transport waits for the
calls in flight up to `shutdown-graceful-timeout-ms` (1 minute), then cancels them and waits up to
`shutdown-forced-timeout-ms` (1 minute); the writes it accepted get up to `scheduler-shutdown-timeout-ms`
(2 minutes), its event loop 5 s, and its scheduler up to `scheduler-shutdown-timeout-ms` again. Then the apply thread
finishes its entry, its completer gets up to a minute, and the database closes (with its flush; up to a minute more
when it is closing on another thread). When another thread had already started the close (a replica failure racing
the signal), the hook waits up to 2 minutes for it instead. A stop past `TimeoutStopSec` gets SIGKILL, exit 137, and
the unit is "failed" whatever `SuccessExitStatus` says; for the node that is a crash (it replays its log at the next
start), not a loss.

The API server's close normally takes well under a second. It reaches its bounds only with a call that does not end:
a client stream without a deadline holds the graceful minute and is then cancelled, and the two scheduler waits last
only for writes or closes held up by the disk. With the defaults the waits the node bounds itself add up to about
8 s + 6 min 5 s + 2 min, a little over 8 minutes, so 600 s leaves room for the database's flush; a stop slower than
that is a disk problem to look at. To stop faster, lower the three properties with `-D` in the JVM options (for example
`-Dit.cavallium.rockserver.grpc.server.shutdown-graceful-timeout-ms=10000`, and likewise
`it.cavallium.rockserver.grpc.server.shutdown-forced-timeout-ms` and
`it.cavallium.rockserver.grpc.server.scheduler-shutdown-timeout-ms`) and set `TimeoutStopSec` to at least
8 s + graceful + forced + 2 × scheduler + 5 s + 2 min, plus the flush. A lower graceful wait cuts client streams
sooner at a planned stop: they fail over to another node either way.

| Code | `run` | `admin` | `repair-log` | `export-seed`, `restore-backup` |
|---|---|---|---|---|
| 0 | Not on a stop by signal: see below | `OK` | The log is valid, or was (with `--dry-run`: could be) repaired | Done |
| 1 | Did not start: a port in use, a refused data directory (another file system, a cluster's database without its Raft state, a pointless `--join`), a Raft log that Ratis cannot read (after a power loss: see "Node does not start after a power loss"; the output then names the `repair-log` command), a `raft-meta.conf` that Ratis cannot parse (`RaftServerProxy has failed` with `InvalidProtocolBufferException`, `IllegalArgumentException` or `IllegalStateException`; never seen outside a test, review of R66: replace the node, see "Replacing a node"; a file that holds the bytes of two writers and still parses is read as one of their two complete configurations, the later writer's, and the node starts: if it exited 3 with `raft-meta.conf.tmp` in its log and a configuration change happened while it was down or being installed, replace the node, see "Replacing a node": `status` prints each member's role and indexes, not its member list, so the configuration it started with cannot be checked from outside, R66-3), any other failure of the start (an `Error` too, such as a class that does not load) | `FAILED: ...`: `snapshot` took none (`FAILED: <id> took no snapshot (...)`: the member is in its retry backoff after a failed snapshot, still replays its log after a start, or installs a snapshot; run it again in a while, R54-3; or `FAILED: <id> took no snapshot within <ms> ms: ...`: no answer within 2 minutes or the time left (the command waits for the answer in one call, so a slow snapshot that ends within the wait is reported, R57-1; a member that does not answer at all can make it end up to 2 s after the wait), as when the snapshot fails on the member (its log says `snapshot failed`), R55-3 (a request ended early by a timer that an earlier snapshot request left in Ratis is sent again within the wait, R56-2; one that joined a failed snapshot of `--snapshot-every-bytes` is too, R56-1, unless the wait ends first, as with a `--timeout-ms` of 30 s or less: a run at least 2 minutes after the failed snapshot takes one); or `FAILED: <id> took no snapshot: it refused the request (...)`: the member refused at once, for example one started with another `--group-id`), refused (a change of more than one voter included, a `remove-peer` id that differs from a voter only by case (`FAILED: N1 is not a voter ...: likely a typo of n1`, nothing proposed, R59-1), a `--peer` address of a member that stays that differs from the leader's configuration without `--change-addresses`, or every member that answered refused `checksum`/`backup` for the cluster version it has applied: not every member supports it, the leader has not raised the version yet (retry in about 10 s), or they lag behind the leader, see "Upgrades"), a change failed, the replicas differ (`checksum`), a member failed (`backup`), a member's answer cannot be read (`<id>: cannot read its answer (...)`: a member of another version, asking again would not help) | Refused: see its output (also while the node runs: its `in_use.lock` is locked) | Failed: see the exception; `export-seed` also while the node runs (`Not exported: ... in_use.lock is locked: stop the node first.`, nothing touched) and when the replica's latest snapshot is damaged (`Not exported: ...`, nothing written; see "Replacing a node", `--without-damaged-snapshot`); `restore-backup` also for a data directory that is not empty (`FAILED: ...`: a `db/`, a Raft log, or a `db.restore-tmp`, `db.restore-ready` or `db.restore-old` it names; nothing touched, see "Restoring a backup") |
| 2 | Wrong usage, or no `-XX:+ExitOnOutOfMemoryError`: also an unknown option, a word that is no option's value (a flag given a value, `--join false` or `--pipelined-updates false`, which turned the flag on before), a malformed value (a non-numeric number, an `--election-timeout-ms` that is not MIN:MAX, a `--peer` or `--group-id` that does not parse, a `--peer` port outside 1 to 65535, R61-12, a `--peer` id given twice, or two members at one Raft or API address, R61-15: also a placeholder address that two members share, and two spellings of one IP address) or one the node's configuration refuses (`--parallel-apply-threads 1`, a node id not among the peers, a minimum election timeout not below the maximum, a `--request-timeout-ms` or an election timeout that is zero, negative or above 1000 days, R61-3) | Wrong usage: also an unknown option (a misspelled one), an option of another command (`--allow-skipped` without `checksum`, `--unsafe-multi-voter-change` or `--change-addresses` without `add-peer` or `remove-peer`), a word the command does not take (`status now`), a malformed value (`--timeout-ms`, also one above about 146 years, R60-6, or zero or negative, R61-12, `--peer`, also an id given twice, R60-6, or a port outside 1 to 65535, R61-12, `--group-id`, the index of `backup-status`, the argument of `add-peer`, an `add-peer` or `remove-peer` whose group would have two members at one Raft or API address (R61-15: the member that is replaced or removed is left out, so a wrong recorded address can still be repaired), a member id with `=` for `remove-peer`, `transfer-leader` or `snapshot`) | Wrong usage: also an unknown option or a stray word (`--dry-run yes`), a `--group-id` that is not a UUID (any case is accepted, as by `run`) | Wrong usage: also an unknown option or a stray word; `export-seed` with a group (`--group-id`, or the default without it) that has no directory in a `raft/` holding other groups |
| 3 | The node stopped after a failure: its replica failed, Raft closed, its Raft log failed (a write or an fsync of it failed: a full disk, an I/O error; `the Raft log failed` in the log, R62-2), Ratis could not write `raft-meta.conf` while applying an entry (`caught a Throwable` with `raft-meta.conf.tmp` in the log; a restart applies it again, review of R66), a JVM pause beyond `--jvm-pause-limit-ms`, out of direct memory on a thread of replication; also a failure that raced a SIGTERM, and out of heap (the JVM's own status for `-XX:+ExitOnOutOfMemoryError`) | - | - | - |
| 4 | Removed from the cluster, confirmed by a majority of the current configuration: refused at the start, or found out while it heard no leader. Do not start it again (see "Removed node"; until a majority answers, a removed node waits outside instead) | Incomplete: a member gave no result in time (`checksum`, `backup`, `backup-status`), no member took the request in time (`Nothing was proposed`), or a `checksum` answer that does not tell whether it was proposed (cut by the time limit, or a connection that broke after the member took it): run it again. A `backup` that a member took without answering in time (`may have been proposed`) may still be written: check it with `backup-status` (see "Backups") or use another name | - | - |
| 5 | Its database, its log or a snapshot it installed has a cluster version with capabilities this version lacks: run the newer version | - | - | The replica's database (`export-seed`) or the backup (`restore-backup`) has a cluster version with capabilities this version lacks; nothing is written (`restore-backup` renames nothing, and deletes the database it built aside to read the version): run the newer version |

A stop of `run` by SIGTERM, after its hand-off, ends with 143 and one by SIGINT with 130: the JVM's
status for a signal (128 plus its number), since the shutdown hook halts with a status of its own only after a
failure (then 3, 4 or 5, as in the table). A halt ends the process through libc's `_exit` (R62-1): with the JVM's
own halt a node whose database was still writing could stay forever, not serving and deaf to SIGTERM, until a
SIGKILL. A node prints `WARNING: libc's _exit could not be linked` at its start if it has to fall back to the JVM's
halt (that is where native access is denied, `--illegal-native-access=deny`; without
`--enable-native-access=ALL-UNNAMED` Java 27 only warns and still links it). The hook is registered before the node starts: a SIGTERM while it is still
starting (opening its database, loading its log, checking its membership) waits for the start to end and then stops
the node as above, so a stop during a long start takes as long as the rest of the start, which `TimeoutStopSec`
must cover for a large node (or it ends with SIGKILL, a crash for the node). Such a node does not print `READY`.

A Raft port that cannot be bound (in use, or `--raft-bind-host` is no address of the machine) ends `run` with 1 within
seconds: the log has `Terminating with exit status 1: Failed to start Grpc server` with
`Failed to bind to address /<host>:<port>` and the cause (`Address already in use`, or `Cannot assign requested
address` for a host the machine lacks), then
`Node <id> does not start: its start called System.exit ...; halting with 1`. Ratis exits by itself there, inside the
start, so the node halts instead of closing what it had opened, which is safe before it serves (R61-13; until then such
a node never exited: it logged the first line and stayed, without its API and deaf to SIGTERM, until a SIGKILL). A taken
API port also ends with 1: `Failed to start node <id>:` with the same `Failed to bind to address` and cause, after
the start closed what it opened. Find the holder with `ss -ltnp 'sport = :<port>'`; when it is the node's previous
process still stopping, the supervisor's next restart passes. A node that prints
`... a System.exit on another thread started the shutdown, and the start did not end within PT30S ...; halting with 1`
was asked to exit by a library while it started: the cause is above that line in its log. The same holds for
`... a System.exit on another thread waits behind the shutdown that a signal started, and the start did not end within
PT30S ...; halting with 1`: a SIGTERM reached the node while it started, something then called `System.exit`, and the
start did not end within 30 s of that (R66-5; without such an exit a SIGTERM during a start waits for the start,
however long). The single-node server (`it.cavallium.rockserver.core.Main`) has one line of this kind:
`Rockserver cannot shut down: the thread that runs it called System.exit ...; halting with 1`, with exit code 1 and
its database left as after a crash (the next start recovers it); the stack below the line shows what called the
exit. The single-node server also ends with 1, after closing everything, when its start fails: a database that does
not open (the exception ends `main`), or a failure after it opened, such as a listening port that is taken
(`Unexpected error` with the cause, then `Shut down after the error above`). The second kind ended with 0 until R67,
so a unit with `Restart=on-failure` did not restart it and now does. A stop by SIGTERM is 143 there too. A node that is killed (SIGKILL, 137 in a shell) or crashes in native code
(SIGABRT, 134) ends by a signal, and any other failure before the node serves (a malformed option value ends with 2
instead) ends with 1, as in the table. A supervisor restarts all of these except a stop it asked for. So the codes a
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
| `rockserver-node-missing` | critical | A node stopped reporting (down, or exited with 3 after a replica failure) | Its supervisor and log; a start that fails reading its Raft log: "Node does not start after a power loss"; else "Replacing a node" |
| `rockserver-replica-failed` | critical | A replica could not apply the log | Its log; "Replacing a node" |
| `rockserver-term-churn` | warning | A node's term rose by more than 5 in 15 minutes: elections keep happening, or a node came back after more than 5 terms away (then it fires once, for that node, and clears within 15 minutes). The term 0 that a `--join` node reports before `add-peer` is left out, so a replacement does not fire it (R61-4) | Links, pauses, `leader.stall.stepdowns` |
| `rockserver-apply-lag` | warning | A node applies far behind its commit index | Disk, write stalls |
| `rockserver-commit-lag` | warning | The leader's entries wait for a majority | Followers' disks and links |
| `rockserver-follower-lag` | warning | A follower is far behind | That follower |
| `rockserver-raft-log-size` | warning | The Raft log is not purged | "Raft log grows" |
| `rockserver-snapshot-old` | warning | No snapshot for a day (or none since a start a day ago) with a large log | "Raft log grows", disk space |
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
