# Review: Codex changes to single-node Rockserver core (branch `claude/distributed-rockserver-rewrite-e46da4`)

> **Historical, superseded.** This reviews the core changes of the Codex implementation, which was removed and
> replaced by the design in [design.md](../design.md). Most of these core changes were reverted with it; the ones
> kept, and the fixes taken from them, are listed in design.md ("Shape") and its rejections. Line numbers are those
> of `9b2d16d`, not of the current code. Its references to `status.md` and to Codex classes point to that branch: `status.md` is no longer
> in the tree; read it with `git show 9b2d16d:docs/distributed/status.md`.

Worktree: `/home/cavallium/IdeaProjects/rockserver/.claude/worktrees/distributed-rockserver-rewrite-e46da4`
Range: merge-base `c431f88` .. `9b2d16d` (427 commits). Read-only review; nothing was built or run.
Line numbers are at HEAD `9b2d16d` unless marked `base:`.

## 0. Size of the change

| Area | Files | Lines |
|---|---|---|
| `common/api` (checked-in Thrift-generated Java) | 74 | +91,265 / -2,718 |
| `impl/rocksdb` (new storage engine, in core) | 16 | +11,691 / -91 |
| `common/distributed` (value types and codecs) | 107 | +7,060 |
| `src/test` | 188 | +18,262 / -211 |
| `rust-client` | 19 | +9,363 / -164 |
| `distributed/` module (Ratis, outside core) | 656 | +104,138 |

The handwritten Thrift IDL change is small: `rocksdb.thrift` +43/-15, plus 5 new `.thrift` files totalling 226 lines (`cdc`, `exports`, `rocksdb-mutations`, `rocksdb-physical`, `rocksdb-schema-operations`). Those 269 lines of IDL generate about 91k lines of checked-in Java (`RocksDBRemoteMutations.java` 18.7k, `RocksDBExports.java` 12.2k, `RocksDBDistributedCdc.java` 8.6k, `RocksDBReplicaPhysical.java` 7.1k, `RocksDB.java` +2952/-2375 after regeneration). On the proto side, `rocksdb.proto` is +138/-16, and three files are new: `cdc.proto` (60), `exports.proto` (87) and `schema-operations.proto` (25).

The `distributed/` module **does not use `EmbeddedDB` at all**. It runs on a second, parallel storage engine (`DistributedStoragePool` + `ReplicatedRangeStore` + `NativeRangeImage`), built on `RocksDBLoader`, and that engine lives inside the core `impl/rocksdb` package. So most of the single-node API changes are not needed to execute distributed writes. They exist to make the *client contract* backend-agnostic: capabilities, transaction scope, and new service registrations.

---

## 1. Public API and wire-contract changes (single-node paths)

### 1a. gRPC (`src/main/proto/rocksdb.proto`). Production contract: BREAKING

| # | Change | Location | Breaking? |
|---|---|---|---|
| G1 | `RequestContext`: `reserved 2; int32 workloadContractVersion = 3; int64 timeoutNanos = 4;` became `int64 timeoutNanos = 2;`. The field is **renumbered from 4 to 2**, field 2 is **un-reserved and reused**, and fields 3 and 4 are dropped **without `reserved`**. | proto:83-86 | **Yes, fatal.** Every existing client (master/production, base, the current Rust and Java clients) sends timeout in field 4. The new server reads field 2, gets 0, and rejects **every request that has a context** with `PUT_INVALID_REQUEST "Request timeoutNanos must be positive"` (`GrpcServer.java:977-981`). Yotsuba would fail on every get/put/range call. |
| G2 | `CapabilitiesResponse.1` changed from `int32 workloadContractVersion` to `BackendMode backendMode`, with fields 2-5 added. | proto:89-112 | **Yes.** An old client reads field 1 = `STANDALONE(1)`, compares it with `REQUIRED_WORKLOAD_CONTRACT_VERSION = 3` (base `RockserverCapabilities.java:13`), and throws `NOT_IMPLEMENTED` at connect. A new client talking to an old server sees `backendMode=3` (an unknown enum value), and `decodeCapabilities` fails (`GrpcConnection.java:356-367`). Both directions are broken. |
| G3 | `workloadContractVersion` was removed from `CloseFailedUpdate`, `CloseIterator`, `Compact`, `Flush`, `CdcCreate(7)`, `CdcDelete`, `CdcGetEarliest`, `CdcGetLastCommitted`, `CdcPoll(5)`, `CdcCommit`, `CompactFiles(1)`. None of the removed numbers is `reserved`. | proto:165, 213, 302-304, 404-445, 483 | Old clients still parse, because the server ignores unknown fields. But the deliberate v3 contract (master commits `ee2d18a`, `52ce503`) is gone, and the freed numbers can be reused silently later. |
| G4 | `OpenTransactionRequest.scope = 3` (optional `TransactionOpenScope`) was added. | proto:151-159 | Wire-compatible, because absent means standalone. |
| G5 | Two new services, `RemoteMutationService` (13 RPCs) and `ReplicaPhysicalService` (6 RPCs), are appended to the **same `rocksdb.proto`**, plus `cdc.proto`, `exports.proto` and `schema-operations.proto`. `GrpcServer` registers **five extra services unconditionally** on the production listener (`GrpcServer.java:377-381`). | proto:540-636 | Not breaking, but adds attack surface and distributed-only types to the production server. |
| G6 | **New semantic limit:** `getRangePage` now calls `client.getCapabilities().requireRangeBudget(budget)` (`GrpcServer.java:2350`). STANDALONE caps the budget at `RangeBudget.DEFAULT_*` (4096 items / 8 MiB) (`RockserverCapabilities.java:14-16, 53-57`). Base accepted any positive budget. | | **Yes, behaviour change:** a client asking for more than 4096 items or more than 8 MiB now gets `PUT_INVALID_REQUEST`. |
| G7 | **New semantic limit, client side:** `GrpcConnection` sets `capabilities.withTransportLimit(maxInboundMessageSize)` (`GrpcConnection.java:308`), which lowers `maximumRangePageBytes` to `(maxInbound-64KiB)/2`. It then enforces that limit before sending (`:1191`). | | Java clients with a smaller inbound limit now refuse budgets that used to work. |
| G8 | **Page accounting changed:** `EmbeddedDB.getRangePage` now sizes items with `RangeBudget.itemBytes` (16 + 6 per key component + payload) instead of `decodedKVBytes` (payload only) (`EmbeddedDB.java:8724`). The other range path still uses `decodedKVBytes` (`:8828`, `:10747`). | | **Yes:** pages contain fewer items, and an item that fit exactly before now raises `RANGE_ITEM_TOO_LARGE`. The test was adjusted to make it pass (`RangePageContractTest.java:59`, `LATENCY_MAX_BYTES` 64 → 76). |
| G9 | `scanRaw` now calls `requirePhysicalSst()` (`GrpcServer.java:2404`). | | Standalone returns `PHYSICAL_SST`, so this is OK. |

### 1b. Thrift (`rocksdb.thrift`). BREAKING

- T1: `RequestContext {1: profile, 3: required i32 workloadContractVersion, 4: required i64 timeoutNanos}` became `{1, 2: required i64 timeoutNanos}` (thrift:46-49). Old clients never send field 2, and the generated `read()` throws `TProtocolException` for a missing required field. **Every context-bearing call from an old client fails.**
- T2: `Capabilities.1` type changed from `i32` to `enum BackendMode`, and new required fields 2-5 were added (thrift:51-70). Old and new clients and servers cannot interoperate.
- T3: Method argument lists shrank (`closeFailedUpdate`, `closeIterator`, `flush`, `compact`, `cdc*`). An old client sending extra arg fields is tolerated. A new client calling an old server **fails**, because the old server expects `required` arg 2.
- T4: New error enum values `TX_SCOPE_REQUIRED=45, SCHEMA_OPERATION_NOT_FOUND=46, SCHEMA_OPERATION_NOT_RESUMABLE=47` (thrift:212-214) **collide with master's** `GET_MULTI_ITEM_TOO_LARGE=45, INVALID_REQUEST=46`.
- T5: The server now uses a `TMultiplexedProcessor` with `registerDefault` (`ThriftServer.java` ctor). That should keep plain clients working, but it is an untested change to the transport path.
- T6: The Thrift server advertises `withoutRawScan()` and a CDC-frame-derived page byte cap (`ThriftServer.effectiveCapabilities`, and `:1249` enforces it).
- T7: `checkMergeOperator` was added to Thrift. It is a genuine parity fix, because gRPC already had it at base (`base:proto:344`).

### 1c. Java API (`RocksDBSyncAPI` / `RocksDBAsyncAPI` / `RocksDBAPICommand` / `RocksDBConnection`). Source- and binary-breaking

- J1: `openTransaction(Duration)` became `openTransaction(TransactionOpenScope scope, Duration)`, and `openTransactionAsync` changed the same way (`RocksDBSyncAPI.java:55`, `RocksDBAsyncAPI.java:66`). The `OpenTransaction` record gained a leading component (`RocksDBAPICommand.java:226`). The old overload was **not kept**, so every Java caller has to change, and 20+ existing tests were edited to pass `null`.
- J2: `RockserverCapabilities` was redesigned. `workloadContractVersion()`, `CURRENT`, `REQUIRED_WORKLOAD_CONTRACT_VERSION` and `requireCompatible()` were removed, and five components were added. The class reaches into `common.cdc.DistributedCdcSubscriptions` for constants (`RockserverCapabilities.java:39-41`).
- J3: `RocksDBConnection` gained the default methods `getDistributedCdcApi()`, `getRemoteMutationApi()` and `getSchemaOperationsApi()` (`RocksDBConnection.java:19-36`).
- J4: New error types `TX_SCOPE_REQUIRED`, `SCHEMA_OPERATION_NOT_FOUND` and `SCHEMA_OPERATION_NOT_RESUMABLE` were added. `TX_SCOPE_REQUIRED` was inserted in the *middle* of the enum, which shifts the ordinal of every later value. Master appended different values, so the two branches conflict.
- J5: Module exports were widened (`common.distributed`, `common.schema`, `common.export`, `api.cdc(.proto)`, `api.export.proto`). `library` exports `common.export` but `fatjar` does not (`src/library|fatjar/java/module-info.java`), which is inconsistent.
- J6: `ServerBuilder.setExportAPI` and `setReplicaPhysicalAPI` were added. `GrpcServer` and `ThriftServer` gained new constructors.

### 1d. Rust client

- `REQUIRED_WORKLOAD_CONTRACT_VERSION` and its check were removed.
- `open_transaction(scope: Option<..>, ttl)` changed signature, which is breaking.
- `check_merge_operator` and `ensure_merge_operator` were added. `ensure_merge_operator` is a good addition.
- Modules were added: `cdc.rs` (3.2k), `remote_mutation.rs` (1.5k), `export.rs` (1.5k), `aggregate_export.rs` (1.1k), `replica_physical.rs`.
- The `rust-client/proto/rocksdb.proto` copy is identical to the Java one, so it inherits G1 and G2.

---

## 2. What `common/distributed` and `impl/rocksdb` contain

### `common/distributed`: 107 files, 7,060 lines

These are distributed-only value types and hand-rolled binary codecs for Raft commands and 2PC state:

- **Commands and codecs:** `DistributedCommand(Codec)`, `Mutation`, `KeyPredicate`, `AtomicMutation`.
- **Schema and topology state machines:** `SchemaChange` (318), `TopologyChange` (289), `DistributedSchema(Codec)`, `RangeDirectory(Codec)`.
- **2PC and fencing:** `PreparedParticipant`, `ParticipantVote/Outcome/Finalization/Retirement`, `Snapshot*` (fences, gates, homes), `Home*`, `TransactionStatus/Acceptance`.
- **Native recovery and topology bookkeeping (about 25 `Native*` records):** recovery supersession, learner registration, SST import manifests, and similar.
- **Remote-mutation tokens** and the **CDC checkpoint/subscription** model.

**None of these files is referenced by `EmbeddedDB`.** They are in core only because `ReplicatedRangeStore` (also in core) and the client/server wire adapters import them. A second `TransactionScope` model sits here (UUID columns, `Arrays.compareUnsigned` ordering) next to `common/TransactionOpenScope` (long columns) and the Thrift and proto `TransactionOpenScope` types, so the same concept exists in four forms with **two different key orderings** (see 4.6).

### `impl/rocksdb`: +11.7k lines across 12 new files

| File | Lines | Purpose | Used by single-node? |
|---|---|---|---|
| `ReplicatedRangeStore.java` | 6,289 | Per-range replicated state machine: apply(ApplyPosition, command), 2PC prepare/finalize, snapshot gates, topology freeze/relocation, schema fence/activation, CDC log, receipts, recovery | No |
| `DistributedStoragePool.java` | 2,210 | Owns one RocksDB per node pool; CF lifecycle per range; internal `_state_`/`_receipts_` CFs; physical maintenance | No |
| `NativeRangeImage.java` | 1,276 | Range snapshot images (SST export/import) for Raft snapshots and learners | No |
| `RangeOwnership.java` | 531 | Per-CF ownership/lease claims | No |
| `DistributedOperatorArtifacts.java` | 357 | Merge-operator artifact persistence for distributed | No |
| `NativeRangeCdc.java`, `NativeRangeSst{Export,Import}.java`, `PoolSnapshotAdmission`, `SchemaCapacityClaims`, `DistributedBucketMergeOperator` | ~1,150 | distributed helpers | No |
| `NativeCacheCapacity.java` | 40 | Block-cache and WBM sizing, extracted from `RocksDBLoader` | **Yes** (`RocksDBLoader.java:732, 849, 857, 905`) |

**Verdict:** this is a second database engine embedded in core. It should be a separate module (`rockserver-distributed-storage`) that depends on core, not the other way round. The only reason it is in `impl.rocksdb` is package-level access to `RocksDBLoader`, `TransactionalDB` and `RocksDBObjects` (`RocksDBObjects.addFirst` was added just for it). `module-info` even `opens impl.rocksdb to rockserver.core.test`.

### Core files changed only to serve the distributed engine

- **`RocksDBLoader`:**
  - New `load(..., extraInternalColumns, OperatorResolver, ColumnFamilyInventoryLimits, NativeExtentEnv)` overloads (`:640-668`).
  - `configureNativeExtent*`, `readOnly*Options` and `nativeExtentDirectories` (`:1021-1092`) reference `DistributedStoragePool.STATE_COLUMN` and `RECEIPTS_COLUMN` directly from the generic loader (`:1057, 1085`).
  - When `extraInternalColumns` is non-empty, configured column families are **dropped** if they do not exist (`:1200-1203`). This only affects distributed, but it is policy living inside the loader.
- **`MergeOperatorRegistry`:**
  - `DistributedOperatorDescriptor`, `DeterministicContract`, `getDistributed` (`:180`, used only by a test) and `instantiateDistributed`.
  - A second sandboxing class-loader mode (`InMemoryClassLoader(..., distributed=true)`).
- **`FFMAbstractMergeOperator`:** `ConstructionScope` and the `retainOwnedDependency` ownership graph (`:114-168`).
- **`SelectiveCompaction.metadata`** now routes through `boundedMetadata` and **`db.getBoundedColumnFamilyInventory(...)`** (`SelectiveCompaction.java:42`).
- **`ConfigParser.addSourceBytes`.**

---

## 3. `EmbeddedDB` behaviour changes and production risk

1. **Transaction reads switched from read-latest to read-at-snapshot (visible semantics change).**
   - A new helper, `useTransactionSnapshot` (`EmbeddedDB.java:2542`), is now applied to:
     - `dbGet` inside any tx (`:10405`): plain `get` in a transaction now reads the tx snapshot instead of the latest committed value plus own writes;
     - every `getForUpdate` in put-with-previous and bucket paths (`:4699, 4715, 4727, 4864, 4881, 4895, 5027`);
     - `openIterator` in a tx (`:6322`) and the second iterator path (`:6932`);
     - multi-chunk `existsMulti` (`:5944-5954`).
   - Internal update transactions do not capture a snapshot (`openTransactionInternal(…, false)`, `:2565-2566`), so non-transactional puts are unaffected.
   - User transactions from `openTransaction` *do* capture one (`:2466`, `captureSnapshot=true`), so any Yotsuba code that opens a transaction and then gets or iterates **will see different data** for keys written by others after the transaction opened. For pessimistic `GetForUpdate` the commit and validation outcome is unchanged, because validation already used the txn snapshot. The returned values change, though, and so do iterator contents.
   - Snapshot isolation is arguably more correct, but it is a contract change hidden inside a "distributed" branch, and **it has no dedicated test.**
2. **Range page accounting** changed (G8), which shifts page boundaries and can raise `RANGE_ITEM_TOO_LARGE` for items that fit before.
3. **SST metadata (`getSstMetadata`)** now goes through `boundedMetadata`. That path:
   - charges 4 bytes per raw key byte plus 96 bytes per file against the 64 MiB budget, and gives the native inventory only 1/8 of the remainder (`SelectiveCompaction.java:27-50`);
   - so large CFs that previously returned metadata can now fail with "Physical metadata exceeds response limit";
   - and it calls a JNI method that is **not present in the published `rocksdbjni` 11.8.1.4 jar declared in `pom.xml`**, nor in 11.8.1.13. I searched the `RocksDB.class` constant pool for `getBoundedColumnFamilyInventory` and `listColumnFamiliesBounded`, and the jar for `NativeExtentEnv`, and found none of them.
   - The branch moves the `rocksdbjni` submodule to `69caf44`, while master is at `dfca199`. Single-node core therefore **depends on an unpublished JNI fork**. I could not verify how the build resolves this (the submodule is not checked out in the worktree).
4. **Scope checks** add `requireTransactionPoints` / `requireRange` to every tx read and write path. When the scope is null they return immediately. However, `openIterator` now computes `transactionRangeBound(...)`, a duplicate `calculateKey` and array copy, on every tx iterator open even when the scope is null (`:6321, 6931`). The cost is small, but it lands on the hot path for nothing.
5. **`TransactionalDB.close`** (`TransactionalDB.java:153-203`): `closed=true` and `afterCloseAndIsLast()` now happen only when `db.isOwningHandle()` is false.
   - If `closeE()` fails, `close()` becomes re-entrant: it re-runs `databaseTasks.close()` and `flushWal`, and the env refcount is never decremented, so env thread pools leak.
   - `closeE` failures used to be counted as closed. This is a behaviour change on the shutdown path.
6. **`FFMAbstractMergeOperator`:**
   - The constructor now destroys the raw native object on failure (`:213-218`). That is a real leak fix.
   - `disposeBridge` changed its guard from `sharedPtrContainer.address()==handle` to `instanceArena.scope().isAlive()` (`:285-287`), so it now ignores the handle argument.
   - It also cascades `close()` into "owned dependencies". On the standalone path the dependency list is always empty, so the risk is low.
7. **JNI extraction (`RocksDBLoader.java:91-132`):** content-addressed (`sha256-<hash>/`) atomic extraction replaces copy-into-place. This fixes a real race (concurrent processes overwriting a loaded `.so`). The cost is that **every startup computes SHA-256 over the embedded library 2-3 times** (select, verify existing, verify copy). It is also written as dense one-liners.
8. **Block cache and WBM sizing** (`NativeCacheCapacity`): I checked this and it is semantically equivalent to base (cache = blockCache + WBM; WBM defaults to half of that). It adds validation for negative values.
9. **CDC and merge semantics in `EmbeddedDB`:** unchanged. The only CDC changes are client-side `requireScalarCdc()` gates.

**Regression risk for production:** **high**, driven by G1/T1 (total wire break) and G2/T2. Once the wire is fixed, the residual risk is medium: 3.1 (tx snapshot reads), G6/G7/G8 (range limits and accounting), 3.3 (SST metadata limits plus the JNI fork) and 3.5 (close semantics).

---

## 4. Code-quality findings

1. **Unreadable formatting at scale.**
   - `ReplicatedRangeStore.java` has 631 lines over 200 characters, the longest 1,214 (`:351`).
   - `DistributedStoragePool` has 276 lines over 200 (max 1,365 at `:1193`). `NativeRangeImage` has 156 (max 905).
   - Records such as `NativeCurrentRangeRecovery.java:35` (2,234 characters), `DatabaseBackupManifest.java:33` (2,098) and `NativeSstImportManifest.java:79` (1,592) put entire decode methods on one line.
   - This style also leaks into core: `RockserverCapabilities.java:22, 36`, `RocksDBLoader.java:103-132`, `MergeOperatorRegistry.InMemoryClassLoader` and `FFMAbstractMergeOperator:114-168`.
2. **God class:** `ReplicatedRangeStore` is 6,289 lines with 438 `public` declarations and 186 `synchronized`/`lock()` sites. All state machines live in one class (source closing, home closing, outcome credits, learner images, snapshot gates, 2PC, schema fences, CDC).
3. **The "WriteBatch + applied marker" pattern is copy-pasted, not abstracted.** There are 40 `new WriteBatch()` blocks and 26 inline `batch.put(state, markerKey, encodePosition(...))` calls. `setSync(true)` (19 occurrences) and `setSync(false)` (23) are chosen ad hoc per call site (for example `:256` vs `:428` vs `:1652`). This is exactly the one primitive that should exist once.
4. **Pervasive fully-qualified names instead of imports** in core edits, for example `it.cavallium.rockserver.core.common.RockserverCapabilities.CdcMode.VECTOR_SUBSCRIPTIONS` inline (`GrpcServer.java:117-123, 378, 1163-1167`; `ThriftServer` ctor; `RocksDBConnection.java:20-35`).
5. **Duplicated helper:** `remoteMutationBackend(RocksDBConnection)` is copy-pasted in `GrpcServer.java:116-123` and `ThriftServer.java:80-87`.
6. **Inconsistent key ordering.** `Tx.compareBounds` treats trailing zero bytes as equal (`Tx.java:51-58`, so `[01]` equals `[01 00]`). That ordering differs from RocksDB's bytewise comparator and from `common/distributed/TransactionScope` (`Arrays.compareUnsigned`). The result is two scope models with different semantics for the same concept.
7. **Dead code in core:**
   - `MergeOperatorRegistry.getDistributed` and `distributedCache` (`:180-188`) are referenced only by `DistributedMergeOperatorRegistryTest`.
   - `RockserverCapabilities.withoutRawScan` and `maximumVectorCdc*` each have one caller.
   - `EmbeddedDB.transactionRangeBound` duplicates the `calculateKey` work that was already done.
8. **Inconsistent delegation:** `LoggingClient` forwards `getDistributedCdcApi` and `getSchemaOperationsApi` but **not `getRemoteMutationApi`**, which then silently falls back to the `NOT_IMPLEMENTED` default (`LoggingClient.java:35-38`).
9. **Over-abstracted capabilities record.** `RockserverCapabilities` mixes backend identity, transport-derived limits (`withTransportLimit`, which halves the message size for an assumed duplicated cursor key), and CDC-vector constants from another package. Its cross-field invariants throw from the record constructor (`:21-28`), so an *unknown future server mode* becomes a hard connect failure.
10. **Proto hygiene:** removed fields are not `reserved`, a reserved number (2) was **reused** for a different meaning, and distributed services were added to the production `rocksdb.proto` instead of separate files (the branch already created `cdc.proto` and `exports.proto` for that purpose).
11. **Tests changed to fit behaviour changes** instead of guarding the contract:
    - `RangePageContractTest.java:59` (64 → 76 bytes);
    - `WorkloadWireContractTest` deleted the `thriftServerRejectsMissingAndNonV3Contexts` test and rewrote the v3 assertions;
    - `RangePageContractTest.grpcConstructionRejectsAnIncompatibleWorkloadContract` was repurposed.
12. **Ownership machinery with no standalone need.** `FFMAbstractMergeOperator.ConstructionScope` combines a ThreadLocal stack, nesting depth 8, a budget of 16 allocations, an identity-set graph check and `synchronized(FFMAbstractMergeOperator.class)` (`:114-168`). That is a lot of global-lock code on the operator construction path, written to police arbitrary uploaded operator code.
13. **Generic loader knows distributed CF names** (`RocksDBLoader.java:1057, 1085` → `DistributedStoragePool.STATE_COLUMN`), which inverts the layering.
14. **Runtime-variable defensive limits everywhere** (`MAX_*` constants in nearly every record: 256 spans, 2 MiB bounds, 4096 entries, 64 MiB expanded jar, and so on). The bounds are not wrong individually, but they are arbitrary and untested at their edges, and they now gate single-node calls too (G6, 3.3).

---

## 5. Recommendation

### Revert from core (single-node paths)

- **All wire-contract changes to `rocksdb.proto` and `rocksdb.thrift`** for existing messages: `RequestContext`, `CapabilitiesResponse`/`Capabilities`, removal of `workloadContractVersion`, the error-enum renumbering. Restore master's v3 contract exactly.
- If capability discovery is wanted, add **new** fields (`CapabilitiesResponse.backendMode = 2…`) and keep `workloadContractVersion = 1`.
- `OpenTransaction` signature change: keep `openTransaction(Duration)`. If scope is ever needed, add an overload.
- Server-side `requireRangeBudget` and client-side `withTransportLimit` gating (G6, G7) and `RangeBudget.itemBytes` accounting (G8).
- Transaction snapshot reads in `EmbeddedDB` (3.1). If they are desired, do them as a separate, explicitly tested and announced change.
- The `SelectiveCompaction.boundedMetadata` / bounded-JNI dependency on the single-node path (3.3), and the `TransactionalDB.close` semantics change (3.5).
- Unconditional registration of the five distributed services on the production gRPC and Thrift servers. Register them only when the backend is distributed.
- Distributed-only code in `MergeOperatorRegistry`, `FFMAbstractMergeOperator.ConstructionScope`, and `RocksDBLoader`'s native-extent and internal-CF overloads. Move them into the distributed storage module.
- `TransactionOpenScope` and `Tx` scope checks. Distributed uses its own engine and does not need them in `EmbeddedDB`.

### Keep (genuinely independent improvements; small and reviewable)

- `TablePropertiesAccumulator` extraction, plus `GrpcPortableWire` and `ThriftTablePropertiesWire` mapping dedup. These are faithful refactors that remove triplicated mapping code (check the diff carefully when merging, because master also touched these regions).
- Thrift `checkMergeOperator` parity, and Rust `ensure_merge_operator`.
- The `FFMAbstractMergeOperator` constructor failure cleanup (destroy the raw object, disown the handle), *without* `ConstructionScope`.
- Content-addressed atomic JNI extraction, simplified: hash once and key the directory by jar version plus size, or by the resource's own checksum. Re-hashing on every start is unnecessary.
- `NativeCacheCapacity`, optional: equivalent semantics, a reusable pure function.

### The minimal core hook distributed actually needs

Distributed already bypasses `EmbeddedDB`. What it needs from core is:

1. **A storage-open SPI.** `RocksDBLoader.load(path, config, LoadHooks hooks)`, where `LoadHooks` provides extra internal CF names and options, an `OperatorResolver`, an optional `Env`, and bounded CF listing. This replaces the five overloads and the hard-coded `DistributedStoragePool` names.
2. **One atomic apply primitive**, owned by the distributed module:
   `void applyAtomically(WriteBatch userEffects, ColumnFamilyHandle stateCf, byte[] appliedKey, ApplyPosition pos, @Nullable byte[] result, Durability d)`.
   It writes the user effects, the result receipt and the applied-index marker in **one** WAL-enabled `WriteBatch`, with an explicit durability policy. Recovery reads `appliedKey` to resume. That one helper replaces the 40 inline batches in `ReplicatedRangeStore`. If the goal were to reuse `EmbeddedDB` as the replica state machine, the equivalent hook would be `EmbeddedDB.applyReplicated(WriteBatch, appliedIndex)` with the marker in a reserved internal CF. Today neither exists.
3. **Read-only snapshot/SST export and import utilities** (current `NativeRangeSst{Export,Import}`), also outside core.
4. **Client contract:** distributed should ship its own services and protos (`rocksdb-distributed.proto`, which `cdc.proto` and `exports.proto` already start), and expose capabilities through **new** proto fields, never by changing existing ones.

---

## 6. Divergence from master (`c431f88..master`, 56 commits, tip `ee505d7`)

There are 26 files changed on both sides. A legacy `git merge-tree c431f88 master HEAD` (stdout only) shows textual conflicts in the following files. Counts are numbers of conflict hunks.

| File | Master | Branch | Conflict hunks |
|---|---|---|---|
| `common/api/RocksDB.java` (generated) | +3973/-2522 | +2952/-2375 | 241 (regenerate, don't merge) |
| `impl/EmbeddedDB.java` | +817/-94 | +123/-91 | 9 (incl. same `getForUpdate` readOptions lines 4608-4900 where master switched to synchronous read options) |
| `impl/rocksdb/RocksDBLoader.java` | +179/-52 | +207/-84 | 6 |
| `common/api/RocksDBErrorType.java` | +7/-1 | +10/-1 | 2 (value 45/46 collision) |
| `rocksdb.thrift`, `rocksdb.proto`, `rust-client/proto/rocksdb.proto` | +10/-1, +5/-1, +1/-1 | +43/-15, +138/-16, +138/-16 | 1 each (`FlushRequest`: master adds `wal_only=2` and keeps `workloadContractVersion=1`; branch empties it) |
| `server/GrpcServer.java` | +327/-22 | +33/-78 | 1 |
| `client/GrpcConnection.java` | +46/-0 | +48/-66 | 1 |
| `common/RocksDBException.java` | +3/-1 | +4/-1 | 1 (enum append conflict) |
| `rust-client/src/client.rs` | +12 | +206/-113 | 1 |
| `pom.xml` | +137/-5 (version 2.0.3-SNAPSHOT, JNI 11.8.1.13, bson test scope, build dirs) | +1/-1 | 1 |
| `rocksdbjni` submodule | → `dfca199` | → `69caf44` | gitlink conflict |

These files are changed on both sides and merge without a textual conflict, but may still conflict semantically: `EmbeddedConnection`, `LoggingClient`, `ThriftConnection`, `ThriftServer`, `RocksDBAPICommand`, `RocksDBSyncAPI`/`AsyncAPI` (master adds `getMulti`, `flushWal`), `ConfigParser`, the three `module-info.java` files, `GrpcPhysicalMaintenanceIdentityTest`, `WorkloadAdmissionTest`.

**Semantic conflicts to expect:**
- Master's new `GetMulti` command, `getMulti` RPC and `flushWal` rely on `RequestContext` with field 4 and on the v3 contract.
- Master's new tests call `openTransaction(Duration)`.
- Master pins JNI 11.8.1.13, which lacks the bounded or native-extent APIs the branch's core calls.
