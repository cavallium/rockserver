# Review of the Codex distributed Rockserver branch

Reviewed 2026-10-29. Subject: `codex/source-quiescence` at `9b2d16d`, which is
`codex/distributed-rockserver` (`820461f`) plus one commit. The other `codex/*`
branches are subsets of this line. Baseline: merge-base `c431f88` with `master`.
Current `master` is `ee505d7`.

Detailed per-area reports, with file:line evidence, are in `review/`:

| Report | Scope |
|---|---|
| [review/adapter.md](review/adapter.md) | `distributed/ratis` main sources (state machine, runtime, client, topology, transactions) |
| [review/ratis-fork.md](review/ratis-fork.md) | `distributed/ratis-server`, `distributed/ratis-grpc` (patched Apache Ratis) |
| [review/core-changes.md](review/core-changes.md) | Changes to single-node core, proto/Thrift, Java and Rust clients |
| [review/tests.md](review/tests.md) | All tests added by the branch, plus uncommitted Codex work |

## Verdict

**Do not continue this branch. Start again from `master`.**

The branch is big: 426 commits, about 1,143 files and +250k lines. A good part of
that is generated Thrift code, and much of the handwritten code is packed several
statements to a line, so line counts understate it. Worse, it is built against the
goal. The goal is a 3-node replicated Rockserver that keeps Yotsuba serving through
deploy restarts and node loss. The branch instead builds a range-partitioned,
multi-group database with cross-group two-phase commit (2PC), split/merge/relocation,
frontend-side durable sessions, export/backup/CDC lineage, and byte-level resource
budgets.

The parts the goal needs are missing or broken:

- there is no production way to bootstrap a cluster;
- there is no production way to replace a failed node;
- plain `put` is rejected;
- writes are not retried across leader changes;
- in-flight requests die with the frontend that accepted them.

The branch also:

- breaks the production gRPC and Thrift wire contract;
- depends on an unpublished RocksDB fork with about 6k lines of new C++;
- ships an overlay fork of 21 Apache Ratis classes;
- does not compile from the repository.

It has never run as a cluster outside hand-driven test fixtures.

Little is worth carrying over:

- the explicit Ratis safety settings (`RatisDurability`);
- the observation that upstream Ratis linearizable reads can be stale (below);
- the idea behind one test, the public-API quorum workflow in uncommitted Codex work, as a specification.

## 1. Architecture

**What it is.** One Ratis server per node hosts many groups, each in one of four roles:

- CATALOGUE: directory, sessions, schema and operator catalogue;
- DATA: one group per key range per "domain";
- TRANSACTION_HOME: up to 64 groups holding 2PC decisions;
- SNAPSHOT_HOME: cross-group snapshot fences.

Serving even a single-range database needs at least four Raft groups
(`RatisDatabaseBackendFactory:38,43`). One 717 KB class,
`RatisRangeStateMachine`, serves all four roles by switching on `role`. It has
132 fields, 58 nested types and 300 `synchronized` blocks. It implements six
interfaces that exist only in the forked Ratis, so it cannot run on stock Ratis.

**Storage.** `distributed/` never uses `EmbeddedDB`. It reimplements storage in
core as `ReplicatedRangeStore` (6,289 lines), `DistributedStoragePool` (2,210)
and `NativeRangeImage` (1,276), plus 107 codec and value classes in
`core/common/distributed`. So the distributed engine does not inherit the
production engine's column schemas, bucket hashing, merge operators, iterators,
range pages or workload scheduler. Instead it duplicates part of them.

**Native layer.** The `rocksdbjni` submodule moved to a commit carrying patches
0013–0017, about 6k lines of RocksDB C++. They add "extent admission",
disk-quota arenas and bounded metadata inventory. None of them is needed for
replication. `master` has since switched to the published JNI 11.8.1.13, which
lacks these APIs. The branch therefore no longer compiles
(`DistributedStoragePool.java:642`: `cannot find symbol NativeExtentEnv`).

**Ratis fork.** The build unpacks the released 3.3.1 jars, removes 21 class
families and compiles replacements over them, about 2,100 changed lines plus
1,000 new lines inside `org.apache.ratis.*`. The changes put application
lifecycle into consensus internals: recovery gates on voting and election,
"source retirement", "covered prefix" advancement and memory reservations.
`docs/distributed/ratis-integration.md` says only two classes are replaced;
that is false. See [review/ratis-fork.md](review/ratis-fork.md).

**Documentation.** `architecture.md` is 82 KB and reads as a specification for a
general-purpose distributed database. `status.md` is a 2.5k-line changelog of
log-file names and case counts. Neither says what a deployer must do to run a
cluster, because no such path exists.

## 2. Consistency model as implemented

| Aspect | Finding |
|---|---|
| Point reads | Linearizable via Ratis ReadIndex, with a patched read path. They hold the same monitor as apply (`SM:3375`), so a long schema install or import blocks every read on that range. |
| Upstream read bug | Real. Stock Ratis 3.3.1 lets a read reuse an earlier heartbeat confirmation when the commit index has not moved (`ReadIndexHeartbeats.java:173-178`, `LeaderStateImpl.java:1209-1215`). A deposed leader can therefore serve stale "linearizable" reads until it steps down, for about one election timeout. Enabling the lease does not help: when the lease expires, reads fall back to the same path. The fork's fix is right in principle but adds liveness bugs. One rejected executor task disables linearizable reads for the rest of that leader's term. |
| Writes | Plain `put`, `PutBatch` and `DeleteRange` without an explicit owner/token return `NOT_IMPLEMENTED` (`RatisRocksDBAdapter:155,399,537`). Clients must use a new 3-4 RPC protocol: owners, prepare, execute, results, acknowledge. Each write first runs an extra linearizable `STATE` query that pins a RocksDB snapshot and encodes the range state (`RatisRoutedClient:1679`, `SM:6404`). |
| Write cost | About 15 fsyncs per point write on the frontend (request and session journals), on top of Raft. At most 2 concurrent writes per frontend by default; extras are rejected, not queued (`RatisRocksDBDirect:144`). |
| Dedup | Three layers: the Ratis retry cache (unused, because writes use `RetryPolicies.noRetry()`), server receipts in RocksDB, and frontend-local journals. In-flight state lives on the frontend's local disk, so another node cannot resolve it ("Original point owner unavailable", `RatisRocksDBConnection:51`). |
| Apply failure | Any exception in apply sets a `storageFailure` that is never cleared (`SM:4429`). Apply throws on deterministic validation checks (`SM:4277`), so one bad committed entry poisons every replica on every restart. Snapshot install also refuses to run on a poisoned replica (`SM:3632`). |
| Snapshots | Each snapshot writes a full logical image of the range plus an MD5. The cost grows with the dataset, where a RocksDB checkpoint would cost O(1) using hard links. Snapshots are triggered by disk pressure, which is the moment the extra space is least available. |
| Restart | Long "configured recovery discovery" path. DATA groups refuse to serve until discovery settles. Multi-node cold restart is listed as an open gate (`status.md:2427`). |

## 3. Replication and topology

The split/merge driver (`RatisTopologyController`) and relocation driver
(`RatisRelocationController`) are constructed only in tests. There is no
production path to:

- create a cluster: `RatisDatabaseBootstrap` and `RatisCatalogueGenesis` are reachable only from tests;
- change membership: the only `setConfiguration` call is in a test-only controller.

After a permanent node loss a group stays at 2-of-3 forever. About 21% of the
adapter (topology, relocation, import, recovery, retirement, reroute) is
therefore live code with no production driver.

## 4. Failure handling

- **Leader change.** Writes are never retried (`RatisRoutedClient:1810`), so every
  failover reaches Yotsuba as an error. This is the most damaging single choice
  for an uptime goal, and about 300 lines of reroute code exist to compensate.
- **Frontend loss.** In-flight requests become unresolvable from other nodes.
- **Partition.** Handled by Ratis, apart from the stale-read window above.
- **Disk full.** About 115 KB of runway and budget accounting. A full-image
  snapshot under disk pressure needs twice the image size free.
- **Observability.** The adapter contains no logging and no metrics.

## 5. Transactions

Transactions are scoped, eagerly captured, and cross-group via 2PC with
dedicated HOME groups, snapshot fences and cohorts. That is about 15% of the
adapter. With one data group, none of the 2PC machinery is needed. The
`openTransaction` API was changed to require a scope (below). `EmbeddedDB`
transactional reads were changed to read the transaction's snapshot instead of
the latest committed value. That changes Yotsuba-visible results, and no test
pins it.

## 6. API surface and the production contract

These changes break the gRPC and Thrift contracts used in production. Details
are in [review/core-changes.md](review/core-changes.md).

- **Context timeout field.** `RequestContext.timeoutNanos` moved from field 4 to the
  previously `reserved` field 2. Old clients would be rejected on every
  context-bearing call ("timeoutNanos must be positive").
- **Thrift.** `2: required timeoutNanos` was added, so old Thrift clients fail to parse.
- **Capabilities handshake.** `workloadContractVersion` changed from `int32` to an
  enum, which breaks the handshake in both directions. The field was also removed
  from about 11 messages without reserving the numbers.
- **Transactions.** `openTransaction(Duration)` was removed in favour of
  `openTransaction(scope, Duration)` in Java and Rust.
- **Pages.** New server-side limits on range budgets, and changed page byte accounting.
- **Error codes.** The new Thrift error codes 45 and 46 collide with `master`'s codes.
- **Unconditional services.** Five distributed gRPC services are registered on
  the production server whether or not distributed mode is used.

## 7. Code quality

- **Formatting.** Code is packed onto very long lines: 5,540 lines over 200
  characters in the adapter alone, with the longest at 2,317. Entire classes sit
  on one line.
- **Codecs.** 51 hand-rolled binary codecs, with record types picked by magic
  numbers and raw byte offsets (`bytes.byteAt(bytes.size()-68)==0`, `SM:5835`).
  No protobuf is used for internal messages.
- **Duplication.** The "write batch + applied marker" pattern is copy-pasted
  about 40 times, choosing sync on or off ad hoc.
- **Naming and API shape.** Names hide meaning: "source result debt",
  `requireRecoveryRegistrationDrained`, `RatisPublicQuorumWorkflowChild`.
  `RatisNodeRuntime.open` has 12 overloads, and `initialize()` is a single 43 KB
  method.
- **Errors.** Business rejections are thrown as exceptions inside apply instead
  of being returned as results, which is what causes the poisoning in §2.
- **Coupling.** 198 of the 241 adapter classes form one connected dependency
  component.

## 8. Tests

About 1,950 test methods in 376 files, plus about 800 new core tests. Only five
methods, in three classes, run a real multi-process cluster over real gRPC with
kill -9. Even those advance recovery by hand, through reflection into private
fields and a manual timer, and the main one checks 4 user keys. Nothing
simulates a network partition. No test installs a snapshot on a lagging
follower. Nothing checks linearizability. The forked Ratis code is tested only
with stubs, and upstream's own test suite is never run against it. Roughly
1,250 assertions compare an internal counter with a literal; only about 25
check actual user values. There are 493 reflective reads of private fields. See
[review/tests.md](review/tests.md).

## 9. Complexity versus value

A competent implementation of the goal needs about 3–6k lines of normally
formatted code. Against the adapter alone, the essential fraction of the branch
is about 5–15%. The rest is speculative scope:

- multi-range topology;
- cross-group 2PC;
- schema jobs;
- export, backup and CDC lineage;
- frontend journals;
- byte budgets;
- native disk-quota patches.

Meanwhile the parts the goal needs are absent: bootstrap, node replacement, the
unchanged API, retried writes and logging.

## 10. Keep, rewrite, delete

| Item | Decision |
|---|---|
| Ratis safety settings (`RatisDurability`: synchronous log flush, no unsafe flush) | Keep the idea; rewrite as a small, readable config. |
| Stale-ReadIndex finding | Keep. Work around it without forking (see `design.md`), and report it upstream. |
| `RatisPublicQuorumWorkflowTest` scenario (uncommitted) | Keep as a specification for the new failure tests; do not reuse the code. |
| Range paging and iterator semantics | Not needed. The rewrite serves reads from the leader's own `EmbeddedDB`, so the existing implementation is reused unchanged. |
| Everything else in `distributed/` (adapter, Ratis fork) | Delete. |
| `core/common/distributed`, `ReplicatedRangeStore`, `DistributedStoragePool`, `NativeRangeImage` and the other new `impl/rocksdb` classes | Delete. |
| Proto, Thrift, Java and Rust API changes | Delete. Return to `master`'s contract. |
| `rocksdbjni` patches 0013–0017 | Delete. Use the published JNI from `master`. |
| Small independent core improvements: `TablePropertiesAccumulator`, wire-mapping cleanup, Thrift `checkMergeOperator`, merge-operator constructor leak fix, safe JNI extraction | Not part of this rewrite. They can be proposed separately against `master` if wanted. |
| `docs/distributed/architecture.md`, `status.md`, `ratis-integration.md`, `consumer-contracts.md` | Replace with `design.md`. Keep `consumer-contracts.md`: its Yotsuba evidence is useful. |
