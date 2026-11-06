# Review: Codex test suite for the distributed (Raft/Ratis) Rockserver

> **Historical, superseded.** This reviews the Codex implementation (`distributed/ratis`, the patched Ratis fork
> and its tests), which was removed and replaced by the design in [design.md](../design.md). Nothing here describes the
> current code. Its references to `status.md` and to Codex classes point to that branch: `status.md` is no longer
> in the tree; read it with `git show 9b2d16d:docs/distributed/status.md`.

Worktree reviewed: `/home/cavallium/IdeaProjects/rockserver/.claude/worktrees/distributed-rockserver-rewrite-e46da4` (HEAD 9b2d16d).
This was a read-only review. I ran no builds and no tests, so every statement about whether a test passes comes from the docs or the source, not from running anything.

## TL;DR

- The suite is very large: about 50k lines and 5.4 MB of test source, about 1,950 distributed test methods plus about 800 new core ones. Almost all of it checks internal state-machine bookkeeping in-process, with Raft faked or stubbed out.
- The real failure tests are **5 multi-process test methods** spread over 3 classes. These are the only tests that run real Ratis gRPC between separate JVMs and kill processes. They are genuinely useful, but each runs one fixed, sequential script with a single client.
- **No partition is simulated anywhere.** There is no message delay, reordering or asymmetric partition, and no clock skew. Nothing checks linearizability or keeps a history of operations (nothing Jepsen-like). "Fault injection" means throwing `IOException` from a hand-written transport wrapper, flipping a fake `leader=true` flag, or sending `destroyForcibly()` (kill -9).
- The headline "three-process cold recovery / leader loss / return" test is real in its processes and networking. However, the child processes drive recovery themselves through reflection and a manual timer, so the production recovery loop never runs on its own. It also checks only 4 user keys, alongside 14 assertions on internal counters.
- The forked Ratis modules (`ratis-server`, `ratis-grpc`; about 13k lines of patched `org.apache.ratis.*` code, including a 1,355-line copy of `LeaderStateImpl` and a new `ReadIndexHeartbeats`) are covered only by tests built on `java.lang.reflect.Proxy` stubs. The upstream Ratis test suite is not run against the fork. This is the highest-risk gap: the consensus read path has been changed and nothing checks it against a real cluster.
- There is no CI. The team validates by running hand-picked surefire selections and "reusing unchanged passing cases", with logs written to `/tmp` (which is now empty). The whole suite has probably never been run as one unit.
- The uncommitted Codex work contains the single best test of the whole effort: `RatisPublicQuorumWorkflowTest`, which goes through the public gRPC API, drops a committed reply, kills the leader, checks exactly-once behaviour and compares replica digests. Its scenario is worth keeping as a specification; its code is not. The other two files have little value.

---

## 1. Inventory

### Size

| Area | Files | Lines | Test methods |
|---|---|---|---|
| `distributed/ratis/src/test/java` | 360 | 47,355 (5.4 MB) | ~1,840 (26 parameterized) |
| `distributed/ratis-server/src/test` | 13 | 2,351 | 95 |
| `distributed/ratis-grpc/src/test` | 3 | 500 | 14 |
| core `src/test` changed since c431f88 | 188 changed (142 added, 46 modified) | +18,262 / −211 | ~800 new |

Line counts understate the size. The average line in the distributed tests is 115 characters. 2,641 lines are longer than 300 characters, 88 are longer than 1,000, and the longest is 2,886 characters. Entire test methods are often packed onto one line with semicolons. By comparison, the distributed production code is 241 files and about 40k lines in `distributed/ratis/src/main`. `RatisRangeStateMachine.java` alone is 734 KB in 6,572 lines and `RatisNodeRuntime.java` is 322 KB, so the line counts understate production too.

Test method names are long and repeat the same words: the median name is 68 characters and the longest 106. "Original" appears in 234 names, "Exact" in 208, "Actual" in 208, "Reject" in 236 and "Reopen" in 184. The names read like proof obligations, not user-level behaviours.

### The 20 largest test files (by bytes; line count in parentheses)

1. RatisNodeRuntimeTest 112 KB (1,002)
2. RatisTwoPoolWorkflowFixture 74 KB (445; a fixture with no tests)
3. RatisAtomicClientTest 64 KB (551)
4. RatisConfiguredRecoveryRuntimeTest 59 KB (334)
5. RatisOperationRerouteCoordinatorTest 56 KB (248)
6. RatisLocalMetadataCompletionWorkflowTest 55 KB (372)
7. RatisDatabaseExportWorkflowTest 54 KB
8. RatisExportSourceRegistryTest 48 KB
9. RatisRocksDBConnectionTest 45 KB (341)
10. RatisDiscoveryInventoryTest 45 KB (249)
11. RecoveryEligibilityTest (ratis-server) 45 KB (634)
12. RatisParticipantPrepareTest 44 KB (394)
13. RatisDurableTransactionTest 44 KB (369)
14. RatisSchemaClientWorkflowTest 43 KB (411)
15. RatisNodeManifestTest 42 KB (325)
16. RatisDurableSnapshotTest 41 KB (405)
17. RatisRangeImportApplyTest 41 KB (313)
18. RatisDatabaseRestoreControllerTest 38 KB
19. RatisTopologySourceHostTest 37 KB
20. RatisRelocationControllerTest 36 KB

### Categories (by class name; the groups overlap)

| Category | Classes | What they test |
|---|---|---|
| Recovery, supersession, genesis, bootstrap, discovery, retirement | ~72 | Phase transitions of the "configured recovery" machinery (PREPARE → REGISTER → CLEANUP_OLD → LOCAL_DONE), genesis/CURRENT/META bootstrap classification, obsolete-artifact disposition |
| Transactions (atomic, participant prepare/finalize, HOME, rejected, scoped, accepted) | ~52 | Cross-group commit protocol state, journaled intents, lost-reply replay, receipt identity |
| Node, runtime, pool, manifest, plan, service | ~58 | Wiring and lifecycle of `RatisNodeRuntime`, almost always with `FakeServer` |
| Snapshot, fence, pins, capture, gate | ~47 | Snapshot fences and pin accounting, capture-request codecs, Raft snapshot lifecycle callbacks |
| Topology, relocation, migration, reroute, learner, placement | ~44 | Split/merge controller phases, freeze RPCs, source snapshot transfer, reroute journals |
| Schema, operator, catalogue | ~38 | Catalogue CAS, schema fencing and installation, merge-operator jar replication |
| Export, import, backup, restore | ~34 | Bulk SST import/export jobs, backup cohort, restore planner |
| Budget, quota, capacity, resource, pressure, disk runway, admission | ~32 | Byte and slot accounting: "charged bytes", "retained requests", queue capacities |
| Session, epoch, grant, clock | ~21 | Session-epoch ledger and grant issuance |
| CDC | 11 | CDC bridge and subscription controller |
| Real multi-process tests | 3 classes, 5 methods | See section 2 |
| Forked Ratis internals | 16 classes | ReadIndex heartbeats, log appender, recovery eligibility gate (all with Proxy stubs) |
| New core tests (`src/test/.../impl/test`) | 142 | Single-node native `ReplicatedRangeStore`, `NativeRangeImage`, `DistributedStoragePool`, wire/codec contracts (gRPC/Thrift), schema/session/snapshot gate codecs |

About 15 files contain no test methods. They are fixtures, or in one case a former test whose tests were removed (`RatisLogicalExportMigrationTest`, test removed in d9ea1a1). The test classes depend heavily on one another: there are **1,631 references to static members of other `*Test` classes** and 58 static imports from other tests. For example, `RatisAtomicClientTest.` is referenced 344 times, `RatisNodeRuntimeTest.` 244 times and `RatisTopologySeedRuntimeTest.` 143 times. The test classes behave as one tightly coupled library.

---

## 2. Do the tests exercise real failure scenarios?

### How the cluster is built in practice

| Mechanism | Files |
|---|---|
| `RaftServer` replaced by a `java.lang.reflect.Proxy` (e.g. `"getId" -> ...; default -> throw new AssertionError(method)`) | 89 |
| Hand-set `new RatisRangeStateMachine.Leadership(1,true)` (a fake leader) | 56 |
| `RatisNodeRuntimeTest.FakeServer` (`volatile boolean leader=true,ready=true; volatile long term=1;`) | 38 |
| `notifyTermIndexUpdated(...)` and `applyTransaction`/`startTransaction` called directly by the test | 74 / 66 |
| Opens `RatisNodeRuntime` or a real `RaftServer` in-process | ~52 (most with FakeServer, single voter, no sockets) |
| Separate JVMs talking over real Ratis gRPC | **3 classes / 5 methods** |
| Mockito | 38 |

The string `127.0.0.1` appears in 87 files, but in most of them it is only a descriptor value. One example is `"atomic-fixture","127.0.0.1:12345"` in `RatisAtomicClientTest.Fixture`, where the `RaftServer` is a proxy and the leader is hard-coded. Many tests are titled "workflow" or "runtime" but say outright that consensus is not real, for example:

- `RatisLocalMetadataCompletionWorkflowTest`: "single-voter socket-free Raft; no remote endpoint/quorum acceptance claim."
- `RatisTwoPoolWorkflowFixture`: "One actual node owns both pools. Only the remote catalogue and Ratis division transport are controlled."
- `RatisTopologyLearnerRecoveryRuntimeTest`: "only source Raft transport and divisions are controlled."

### The only real multi-process tests

| Test | What is real | Assessment |
|---|---|---|
| `RatisDurableProcessesTest.durableReceiptSurvivesLeaderKillAndNodeRestart` | 3 JVMs on gRPC Ratis with `DistributedStoragePool` (the bare range state machine, not the full Runtime). The leader is killed with kill -9, the retry gets the original outcome byte-for-byte, the node restarts, then a second node is killed and the cluster still commits through the restarted node. | Good and readable. Single client, kills only at quiet points. |
| `RatisDurableProcessesTest.portableSnapshotRestoresEmptyNativePoolsWithRetainedRaft` | All voters take snapshots, the whole cluster is killed, the native pools are deleted and orphaned snapshot directories are planted, then the cluster restarts and the data is checked. | Good on-disk recovery test. |
| `RatisDurableProcessesTest.emptyListenerReceivesDurableSnapshotAndTailBeforePromotion` | Logs are purged, then a 4th JVM joins as a listener, receives an **installed snapshot** plus the log tail, is promoted, and a voter is killed. | **The only snapshot-install-to-lagging-replica test**, and it covers a brand-new replica, not a lagging one. |
| `RatisCatalogueProcessesTest.catalogueCasAndCachedDataRoutingSurviveCatalogueQuorumLossAndDataLeaderKill` | 3 JVMs. The catalogue loses quorum and the DATA leader is killed. | Reasonable. |
| `RatisRuntimeRecoveryProcessesTest.allOriginalSourcesFencedRecoverMetadataAndDescendantsAcrossColdQuorumAndLeaderLoss` | This is the "three-process workflow" from the docs; see below. | Real processes and network, but recovery is driven by the test. |

There are no in-JVM multi-voter clusters with real sockets in the rest of the suite. `RatisSnapshotLifecycleTest` builds a real `RaftServer`, but the reload gap is controlled by "manual callback injection".

### How faults are simulated

- **Process crash:** `destroyForcibly()` (kill -9), 14 call sites, all in the 5 tests above. No test stops a node gracefully (0 uses of `destroy()`).
- **Leader change:** in-process, a fake flag is flipped (`.leader=false` or `Leadership(n,false)`, 29 sites). `transferLeadership`/`stepDown` appear in only 1 file.
- **Lost RPC replies:** hand-written anonymous `Transport` or `BlockingApi` wrappers throw `IOException("Lost exact native STAGE reply")` after the real call. Mockito `doAnswer` is used for lost cleanup. These wrappers are everywhere ("lose…" appears 2,786 times). They test retry and journal idempotence at the boundary of a single component, which is legitimate unit-level fault injection.
- **Network partition:** **not simulated anywhere.** No test drops or delays traffic in one direction, isolates a minority leader, splits a quorum, or reorders messages. No test uses a proxy such as Toxiproxy or iptables. Every "partition" match in the suite refers to transaction partitions.
- **Disk faults:** about 43 files truncate, corrupt or plant files, mostly to test that codecs reject bad journals and snapshots. No test injects fsync failures, ENOSPC during apply (beyond budget accounting) or torn Raft log segments under a running cluster.
- **Timers:** `RatisNodeRuntimeTest.ManualTimer` replaces the Reactor scheduler; `schedulePeriodically` only records the task, which then runs when the test calls `fire()`. This is used in the multi-process tests too.

### The "three-process cold recovery / leader loss / return" test

File: `distributed/ratis/src/test/java/it/cavallium/rockserver/distributed/ratis/RatisRuntimeRecoveryProcessesTest.java` (208 lines; the single `@Test` is about 40 physical lines but several thousand characters). `docs/distributed/status.md` cites it at lines 41, 72, 147–162 and 174 (57–67 s per run).

What it does:

1. Starts 3 child JVMs, each running a real `RatisNodeRuntime` with native pools and Raft directories, connected over loopback gRPC.
2. Bootstraps the catalogue, schema and directory through real Raft clients.
3. Writes `a=original` and `z=right` and keeps the receipt bytes.
4. Asks each child to "PREPARE" a CURRENT recovery, performs a real topology split (`RatisTopologyController.advance` in a loop), then "FENCE"s.
5. Kills all 3 with kill -9, relaunches them, and sends "DRIVE" to every child.
6. Checks that `a`, `z` and the original receipt replay byte-for-byte, then writes `b`.
7. Kills the LEFT leader with kill -9, replays the `b` receipt, writes `c` with the two remaining voters, restarts the old leader, and polls until a stale read on the restarted node returns `c`.

What is real: separate processes, real gRPC transport, Raft elections, on-disk recovery after kill -9, and a voter catching up after rejoining.

What undermines it:

- **Recovery is driven by the test, not by the system.** The child sets `var timer=new RatisNodeRuntimeTest.ManualTimer();`. The "DRIVE" command runs a loop that calls `node.bootstrapDeferredRecovery()` and `manager(node).step()` through **reflection** (`field(node,"configuredRecoveries",...)`) until the phase is `LOCAL_DONE`. The docs admit this: "Recovery phase timers are caller-driven". So nothing shows that a cluster recovers by itself after a crash. Restarted nodes only recover because the test reaches into private fields and turns the crank.
- **Phase changes are coordinated through files.** Commands such as `command-<run>-<seq>` and `reply-<run>-<seq>` are written as Java property files. Every fault happens at a quiet point chosen by the test. No crash happens during a write, a split, an election or a snapshot.
- **Very little user data is checked.** Four keys (`a`, `z`, `b`, `c`) and two receipt replays. There is one client, every step is sequential, no load runs during the kills, and nobody checks that a set of acknowledged writes all survived.
- **Most assertions check internal counters.** `assertRecovered` checks 14 internal properties: `aliases=1`, `dispositions=3`, `obsolete=3`, `groups=4`, `sourceLeases=0`, `pins=0`, `originKind=LOCAL_SEED`, digests, `mode=RECOVERY` and others. These values are read in the child through `getDeclaredField`/`setAccessible`, including a private method on the Ratis division: `division.getClass().getDeclaredMethod("isInitialImageReady")`. Any refactor breaks the test even when behaviour is correct. The docs already record a failure caused by "an obsolete timer-count fixture".
- **Generous retry loops hide availability problems.** `execute()` and `read()` swallow every `IOException` for 45 s, `awaitRecovered` waits 150 s, and the whole test has 420 s. A client-visible outage of 40 s would still pass.
- **It is one host.** Its own Javadoc says "one host, not failure domains."

Verdict: it is a valuable smoke test showing that the code can come back after kill -9 when it is driven by hand. It is **not** evidence of automatic recovery, of liveness under faults, or of correctness under concurrency.

The "durability check" that the status doc lists next to it (`RatisDurabilityTest`, 24 lines) only asserts that `RaftServerConfigKeys` values are set, for example `assertFalse(RaftServerConfigKeys.Log.asyncFlushEnabled(properties))`. Confirming the configuration is fine, but it does not test durability.

---

## 3. Assertion quality

Approximate counts across `distributed/ratis/src/test`:

- `assertEquals`: 8,090. Of these, **about 1,250 compare a literal number with a counter**, for example `assertEquals(1,allocations.count.get())`, `assertEquals(charged,budget.chargedBytes())`, `assertEquals(0,budget.retainedRequests(...))`, `assertEquals(3,requests.size())`.
- `assertThrows`: 3,309. About 2,500 of them expect `IllegalArgument`, `IO` or `IllegalState` exceptions, mostly from codecs and frame validators rejecting malformed input or exceeded budgets. The suite is dominated by negative-path validation.
- `assertArrayEquals(bytes(...))` on actual user values: **25**.
- Encode/decode round-trips: about 300. These check that a codec agrees with itself.
- Reflective access to private fields through the `field(obj,"name")` helper: 493 uses in 56 files. The most common fields are `store` (65), `pools` (31), `recoveryGraph` (28) and `configuredRecoveries` (23).

On user-visible correctness:

- **Linearizability:** no checker and no recorded history. No test runs concurrent clients and checks reads against a model. The ReadIndex patch (`ReadIndexHeartbeatsTest`) is checked only with proxy appenders and a fake timer.
- **No lost acknowledged writes:** checked only point by point, a few keys per process test. No test runs a set of acknowledged writes through a crash or failover and compares the final state with that set.
- **Exactly-once:** this is the suite's real strength. Many tests replay a request after a lost reply or a cold reopen and assert that the outcome bytes are identical and that a merge was applied once (e.g. `seed,first,later`). That is a meaningful user-level property, but it is checked with one request at a time.
- **Snapshot isolation and serializability:** checked in a few scripted cases (an outside write, then conflict on commit). There is no randomized or concurrent coverage.
- **Jepsen-like testing:** none. There is no generator, nemesis, history or checker such as Knossos, Porcupine or Elle. No property-based testing either: no jqwik, and only 7 files use `Random`.

---

## 4. Brittleness

- **Timing:** 62 `Thread.sleep` calls in 29 files, mostly 10–50 ms inside polling loops. There are 51 hand-rolled `System.nanoTime()+deadline` loops in 26 files, 14 `assertTimeoutPreemptively` calls (90–600 s), and about 2,000 `get(N,SECONDS)`/`await(N,SECONDS)` calls. In-process concurrency uses `CountDownLatch`/`CyclicBarrier` (539 uses in 130 files), often to hold a thread inside a hook such as `beforeSnapshotCapture`. That is deterministic but tightly tied to the current call structure.
- **Giant fixtures:** `RatisAtomicClientTest.Fixture` has about 30 mutable hook fields (`beforeAuthorize`, `beforeHomeRead`, `losePrepareGroup`, `losePromotion`, ...) and builds proxy servers per range. `RatisTwoPoolWorkflowFixture` (74 KB) has `loseRecoveryAck`, `losePrepareReply`, `loseFinalizeReply`, `loseCommitReply` and two `FakeServer`s. `RatisTopologySourceSnapshotFixture` is constructed 82 times. Because of the 1,631 cross-test static references, changing one fixture ripples through dozens of classes.
- **Tests that replicate production logic:** the fixtures re-implement production validation inline. For example, the atomic fixture's promotion validator checks `allowedReservation.origin().equals(next.origin()) || next.reserved() || next.generation()!=1 || opens.get()!=2`, so the test hard-codes the protocol it is supposed to check. The child processes rebuild the runtime's internal stepping loop (`drive`, `awaitFence`, `awaitGenesis`) through reflection. The forked-Ratis tests construct `RaftConfigurationImpl` and appender proxies by hand to mimic leader internals.
- **Tests that check implementation instead of behaviour:** assertions on exact RPC counts (`assertEquals(1,requests.size(),"A failed CURRENT observation must not issue an OUTCOME query")`), timer counts, "charged bytes" and pin counts. A correct refactor fails these tests.
- **Readability:** the code is effectively minified. Lines up to 2,886 characters and whole tests on a single line make review almost impossible, which is probably part of why the owner finds the code bad.
- **Process hygiene:** the core tests had API-change churn, with `openTransaction(null, …)` added everywhere. One existing negative test, `fastGetRejectsMissingAndNonV3RequestContexts`, which checked rejection of non-v3 workload contexts, was removed together with the workload contract version. Nothing in the status doc flags this.
- **No CI and selective verification:** `status.md` §"Validation and evidence" says verification runs "`test-compile surefire:test@default-test` selections, not root `mvn test`", and many entries say "reusing unchanged passing cases". The evidence logs were in `/tmp` and are gone. Regressions in tests that were not selected would go unnoticed.

---

## 5. Failure tests a rewrite should have instead

Build one reusable harness and keep the scenarios declarative:

**Harness**
- Spawn N real server processes (or containers) from the production entry point and configuration, not from a test `main` that reflects into internals. Each process should have its own data directory and run **all of its production timers**.
- Put a fault-injecting TCP proxy between every pair of peers and between clients and peers (Toxiproxy, or a small Netty proxy). It should support drop, delay, jitter, bandwidth limits, one-way blackholes and reset. For multi-host runs, optionally use `iptables`/`tc netem` inside network namespaces.
- Inject disk faults with a FUSE or LD_PRELOAD shim, or a dm-flakey / dm-error device: fsync errors, ENOSPC, slow disk, and torn writes at the last segment.
- Run several concurrent clients through the **public** gRPC/Thrift API, recording every operation as invoke/ok/fail/info with timestamps.
- Check the histories: linearizability per key with Porcupine or Knossos, transaction anomalies with Elle (G0, G1c, G-single, G2), plus set-, register- and bank-style invariant workloads.

**Scenarios (each run with randomized timing and repeated, with a fixed seed recorded):**

1. Kill the leader with kill -9 **during** a stream of writes, and do it at random points: before the append, after replication but before apply, after apply but before the reply. Every acknowledged write must survive and no write may apply twice.
2. Isolate the leader with a minority partition while clients keep writing to it. There must be no acknowledged writes on the minority side, no stale linearizable reads from the old leader (this exercises the ReadIndex/lease patch directly), and the cluster must recover after the partition heals.
3. Majority/minority split with clients on both sides, followed by healing. Run register and bank workloads.
4. Asymmetric (one-way) partitions and flapping links that cause repeated elections. Check that the cluster stays live and that no term regresses.
5. Clock skew and pauses: freeze a leader process with SIGSTOP for longer than the election timeout, then resume it. The old leader must not serve reads or acknowledge writes.
6. Restart every node at once (a cold restart) under load, with no manual stepping. Recovery must complete on its own within a stated SLO.
7. Make a follower lag past log purge with a slow or partitioned link, then heal: it must catch up through **InstallSnapshot**. Also kill the follower or leader in the middle of the install and check that it resumes or restarts cleanly.
8. Membership change under faults: add or remove a voter, or promote a learner, while the leader is killed during the joint-configuration phase.
9. Range split or merge (topology change) under load, with the leader killed at each controller phase. No key may be lost or duplicated, and routing must converge.
10. Cross-range transaction (2PC/HOME) under coordinator crash, participant crash and partition. Check atomicity: all participants commit or none do. Check with Elle.
11. Client failover: a client sending to a dead node must fail over to the new leader, and retrying the same request ID must return the original outcome. This includes losing the committed reply (as `RatisPublicQuorumWorkflowTest` does), but under concurrency.
12. Disk-full and fsync-error on the leader and on followers. The node should fail-stop or step down, never acknowledge without durability, and recover once space returns.
13. Torn or corrupt last Raft log segment and a corrupt snapshot after a crash. Detect the damage, truncate or refuse, and catch up from peers.
14. Slow-disk follower causing back-pressure. Throughput should degrade while the leader stays alive and memory stays bounded.
15. Soak test: a randomized fault schedule ("nemesis") running for hours, with the checkers above.

Keep the unit tests for codecs, budgets and journals, but far fewer and readable: one behaviour per test and normal formatting. Assert on observable outcomes, not on private fields or call counts.

---

## 6. Uncommitted Codex work

### `/home/cavallium/.codex/worktrees/b7a7/rockserver` (branch `codex/distributed-rockserver`, HEAD 820461f)

- Untracked `RatisGroupDensityTest.java` (444 lines). mtime **2026-10-08 01:04**, three weeks old and unchanged since. The same file was archived in commit `62fb311` ("Codex worktree snapshot: archive-cleanup", 2026-10-08), which is not on any branch.
- It is an opt-in benchmark (`@EnabledIfSystemProperty rockserver.density=true`). It starts 3 JVMs with 1, 8 and 32 groups per node, runs 512 sequential put/read pairs, and writes a JSON report with latency, CPU, RSS and thread counts. It assumes an XFS test root.
- It was written against base `6287867` (2026-10-07), about 411 commits behind HEAD. It uses a raw `RatisRangeStateMachine` and pool instead of `RatisNodeRuntime`, so it no longer measures the real stack. Its own limitations list calls it "an early architecture density gate".
- **Salvage:** the idea only. Per-group idle cost (threads, RSS, CPU, file handles) is a key question for a multi-Raft design and should go into a rewrite's benchmark plan. Don't keep the code.

### `/home/cavallium/.codex/worktrees/topology-recovery-obligations/rockserver` (branch `codex/public-quorum-workflow`, HEAD 000d47b)

000d47b is a divergent twin of 820461f with the same message and a different SHA. It is based on 85ddefc and lacks 9b2d16d.

- mtimes: all on **2026-10-16 between 09:29 and 09:55**, untouched for 13 days.
- Modified `RatisNodeRuntime.java` (+10/−14): `readSchemaAuthority` no longer falls back to reading the retained OUTCOME when the CURRENT read fails. It now reads the domain head once and then requalifies with `outcome=!current`. The stated reason is that a failed read after a no-retry leader redirect was repeatedly undoing route discovery. The method was also made package-private for testing.
- Modified `RatisSchemaJobBridge.java` (+2): a `LOG.debug` call when a schema request pauses. This looks like debugging of a hang.
- Untracked `RatisSchemaAuthorityRoutingTest.java` (58 lines, 4 tests). A mocked `RaftClient`/`BlockingApi` (Proxy) unit test that asserts the exact sequence and count of authority requests. It locks in the fix above but only checks call structure. **Low value.**
- Untracked `RatisPublicQuorumWorkflowTest.java` (258 lines, 1 test, 600 s timeout) and `RatisPublicQuorumWorkflowChild.java` (146 lines). **The best test in the effort:**
  - 3 Runtime JVMs, each with a **real public gRPC frontend**. The test uses `GrpcConnection`, the async API, `RemoteMutationApi` and `SchemaOperationsApi`, as a real client would.
  - Uploads a merge-operator jar and creates columns through public schema operations, recovering by request UUID.
  - Runs an atomic cross-domain mutation, where a `ClientInterceptor` (`CommittedReplyLoss`) **drops the committed TERMINAL SUCCESS reply**. Then it kills the leader with kill -9, settles the original token, and asserts the saved result and **exactly-once merges** (`Bseed+one+two`, unchanged by later writes). Re-preparing with the same sequence returns the same token, followed by ACK.
  - Opens a scoped transaction and checks snapshot isolation against an outside atomic write, which must make the read-only cut conflict on commit. A leader-bound transaction must refuse to commit after its leader is killed, and must never be rebased onto newer data.
  - Restarts the whole cluster, re-verifies values and ACK status, and checks that **replica digests agree on all 3 replicas** (`assertReplicas`, which runs INSPECT on every node).
  - Weaknesses: the child still uses `ManualTimer`, reflection (`field(service,"node")`, `field(node,"machines")`) and the file-command protocol. It has one sequential client and no partitions. Whether it passed is unknown: there is no log, the status doc does not mention it, and the debug logging suggests it was still being diagnosed.
- **Salvage:** keep `RatisPublicQuorumWorkflowTest` as an executable specification of the user-level guarantees (lost committed reply plus leader kill gives exactly-once, scoped snapshot conflicts, leader-loss refusal, cold restart, replica agreement). Port the scenario to the new harness from section 5. The `CommittedReplyLoss` interceptor pattern is directly reusable. Commit the production fix only if the existing code is kept. Drop `RatisSchemaAuthorityRoutingTest` and `RatisGroupDensityTest`, keeping the density idea as a benchmark requirement.
