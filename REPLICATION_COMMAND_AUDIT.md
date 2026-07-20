# Rockserver Replication Command Audit

This audit classifies the current `RocksDBAPICommand` surface for the first Ratis-backed production cut.

Code hooks:

- `ReplicatedCommandClassifier` is the current enforcement hook for commands that may enter a replicated log.
- `ReplicatedCommandKind` is the routing-level classification used by `ClusterRequestRouter`.
- `ReplicatedWriteRetryPolicy` is the current policy hook for ack-only versus response-body dedupe.
- `WriteDurability.applyBasedFallback()` is the first-cut rule for exact commit-only acknowledgement when a pre-apply Ratis proposal/watch path is not available.

## Command Classification

| Command(s) | Replicated-mode class | First-cut handling |
| --- | --- | --- |
| `Put`, `Delete`, `Merge` with `transactionOrUpdateId == 0` | Replicated single-key write | Encode into one deterministic partition command after key-to-partition validation. `Merge` must execute inside the state machine because merge output depends on current value and configured operator. |
| `Put`, `Delete`, `Merge` with `transactionOrUpdateId != 0` | Unsupported local handle write | Reject before command encoding. Transaction and update IDs are local handles and cannot cross the Ratis boundary. |
| `PutMulti`, `DeleteMulti`, `MergeMulti` with `transactionOrUpdateId == 0` | Replicated multi-key write | Accept only after bounding mutation count to `PartitionWriteCommand.MAX_MUTATIONS` and prevalidating every calculated key is in one partition. Reject cross-partition requests in the first cut. |
| `PutMulti`, `DeleteMulti`, `MergeMulti` with `transactionOrUpdateId != 0` | Unsupported local handle write | Reject before command encoding. |
| `PutBatch` with `WRITE_BATCH` | Replicated multi-key write | Accept only after streaming input is materialized into bounded deterministic single-partition payloads. Reject if any batch crosses a partition. |
| `PutBatch` with `WRITE_BATCH_NO_WAL`, `SST_INGESTION`, or `SST_INGEST_BEHIND` | Unsafe local write path | Reject before command encoding. These modes bypass the durability path that replicated mode must control. |
| `MergeBatch` with `MERGE_WRITE_BATCH` | Replicated multi-key write | Accept only after `PartitionWriteCommand.MAX_MUTATIONS`, single-partition, and deterministic merge-operator validation. |
| `MergeBatch` with `MERGE_WRITE_BATCH_NO_WAL`, `MERGE_SST_INGESTION`, or `MERGE_SST_INGEST_BEHIND` | Unsafe local write path | Reject before command encoding. |
| `CreateColumn`, `DeleteColumn` | Replicated metadata write | Encode as metadata-group commands, not data-partition commands. Column IDs must be cluster-assigned and stable. |
| `UploadMergeOperator` | Unsupported artifact upload | Reject until replicated artifact distribution and activation preflight can make operator uploads deterministic. |
| `CheckMergeOperator`, `GetColumnId`, `GetAllColumnDefinitions` | Metadata read | Route through metadata-group read path. Linearizable is the default; stale reads require explicit policy and metadata epoch checks. |
| `Get` with `CURRENT` or `EXISTS` | Point read | Linearizable by default; stale/local reads are allowed only when caller requests them and supplies an acceptable metadata/applied-index policy. |
| `Get` with `FOR_UPDATE` | Unsupported local transaction read | Reject in replicated mode until distributed transactions exist. |
| `Get` with `NOTHING` | Invalid point read shape | Reject as unsupported for replicated public APIs. |
| `ReduceRange` | Range read/reduce | Route to the owning partition for single-partition ranges. Multi-partition ranges need the Phase 5 wide-scan contract. |
| `GetRange` | Stream/range read | Replace local server-side streams with bounded, resumable partition cursors. Do not expose raw local stream handles. |
| `OpenTransaction`, `CloseTransaction` | Local-only transaction handle | Reject at the public boundary. |
| `OpenIterator`, `CloseIterator`, `SeekTo`, `Subsequent` | Local-only iterator handle | Reject at the public boundary. Use stateless/resumable cursor APIs later. |
| `CloseFailedUpdate` | Local-only update handle | Reject at the public boundary. |
| `Flush`, `Compact` | Node-local admin | Keep as node-local admin operations; never encode as client-visible replicated writes. |
| `ScanRaw` | Local/admin raw storage scan | Reject from normal replicated client APIs. Reserve for internal backup/repair tooling with explicit manifest checks. |
| `CdcCreate`, `CdcDelete`, `CdcCommit`, `CdcPoll` | Unsupported in first replicated cut | Reject until replicated CDC tokens and partition-local event streams are implemented. |

## Local Handles

| Handle kind | Commands/fields | Replicated-mode rule |
| --- | --- | --- |
| Transaction ID | `CloseTransaction.transactionId`, `OpenIterator.transactionId`, `Get.transactionOrUpdateId`, `ReduceRange.transactionId`, `GetRange.transactionId`, write `transactionOrUpdateId` fields | Reject nonzero values before encoding. |
| Update ID | `CloseFailedUpdate.updateId`, write `transactionOrUpdateId` fields, `Get.transactionOrUpdateId` for retry/update flows | Reject before encoding. |
| Iterator ID | `CloseIterator.iteratorId`, `SeekTo.iterationId`, `Subsequent.iterationId` | Reject all iterator-handle commands. |
| Local stream cursor | `GetRange`/`ScanRaw`/`CdcPoll` returned streams | Do not expose local stream handles. Use future resumable cursor tokens shaped by metadata epoch, partition ID, partition epoch, applied index, and offset. |

## Public Rejection Table

| Input at public boundary | Error class |
| --- | --- |
| Any transaction, update, or iterator handle command | Unsupported in replicated mode |
| Any nonzero transaction/update field on otherwise replicated write/read commands | Unsupported in replicated mode |
| `Get` with `FOR_UPDATE` | Unsupported until a real transaction protocol exists |
| `PutBatch`/`MergeBatch` unsafe or SST-ingestion modes | Unsafe local mode rejected |
| Oversized `putMulti`, `deleteMulti`, `mergeMulti`, `putBatch`, or `mergeBatch` expansion | Resource-limit request rejected before proposal encoding |
| Cross-partition `putMulti`, `deleteMulti`, `mergeMulti`, `putBatch`, or `mergeBatch` | Cross-partition write rejected |
| `Flush`, `Compact`, `ScanRaw` from normal client APIs | Local/admin command rejected |
| CDC commands | Unsupported until replicated CDC is implemented |
| `UploadMergeOperator` | Unsupported until replicated artifact distribution and activation preflight are implemented |

## Response-Producing Writes And Dedupe

Retryable writes that return data must persist the response with the mutation and dedupe record in the same partition state machine.

| Command | Response-producing request types |
| --- | --- |
| `Put`/`PutMulti` | `PREVIOUS`, `DELTA`, `CHANGED`, `PREVIOUS_PRESENCE` |
| `Delete`/`DeleteMulti` | `PREVIOUS`, `PREVIOUS_PRESENCE` |
| `Merge`/`MergeMulti` | `MERGED` |
| `CreateColumn` | Column ID response |

`NOTHING` writes still need dedupe to prevent duplicate application, but they do not need response-body persistence beyond acknowledgement status.

## Retry And Dedupe Policy

`ReplicatedCommandClassifier.retryPolicy(...)` is the executable first-cut policy gate.

| Policy | Applies to | State-machine persistence rule |
| --- | --- | --- |
| `ACK_ONLY_DEDUPE` | Blind `Put`, `Delete`, `Merge`, `PutMulti`, `DeleteMulti`, `MergeMulti`, safe `PutBatch`, safe `MergeBatch`, and `DeleteColumn`. | Persist `(clientId, requestId)` acknowledgement metadata with the mutation so duplicate retries do not apply twice. No response body is required. |
| `RESPONSE_DEDUPE` | `CreateColumn` and writes with response-producing request types: `PREVIOUS`, `DELTA`, `CHANGED`, `PREVIOUS_PRESENCE`, or `MERGED`. | Persist the response body with the dedupe record and mutation in the same state-machine transaction before acknowledging. |
| `UNSUPPORTED` | Local handles, routed reads, CDC, local admin commands, unsafe WAL/SST modes, and commands outside the first-cut replicated log. | Reject before command encoding or Ratis proposal. |

## Read-Modify-Write Commands

These commands must execute inside one partition state machine because correctness depends on the current partition value:

- `Put`/`PutMulti` with `PREVIOUS`, `DELTA`, `CHANGED`, or `PREVIOUS_PRESENCE`.
- `Delete`/`DeleteMulti` with `PREVIOUS` or `PREVIOUS_PRESENCE`.
- `Merge`, `MergeMulti`, and `MergeBatch`.

The first cut rejects cross-partition variants rather than attempting distributed read-modify-write.

## Merge Operator Determinism

Replicated merge operators must be deterministic across nodes and versions:

- Operator activation must be metadata-driven, with artifact hash, class name, version, and schema epoch in replicated metadata.
- Operators must not depend on wall-clock time, local paths, local random sources, process identity, environment variables, or non-replicated external services.
- Activation must be fenced by schema epoch so replay uses the same operator for a given log entry.
- Unknown or missing operator artifacts make a replica unhealthy for that partition until repaired; they must not silently fall back to local behavior.

## Deterministic Command Envelope

The state-machine payload may include:

- cluster ID, partition ID, partition epoch, metadata epoch, schema epoch;
- client ID and request ID;
- command type/version;
- column ID and calculated keys;
- values, merge operands, and deterministic request type;
- write durability after `WriteDurability.applyBasedFallback()`;
- dedupe lease/watermark metadata needed by the state machine.

The state-machine payload must exclude:

- client deadline and retry budget;
- route-cache state, leader hints, selected endpoint, and transport metadata;
- local transaction/update/iterator IDs;
- local file paths, SST file handles, or RocksDB local WAL choices;
- client connection identity and server-side stream handles.

## RequestType Compatibility

| RequestType | Replicated-mode handling |
| --- | --- |
| `NOTHING` | Allowed for `Put`, `Delete`, and `Merge`; no response body. Invalid as a replicated point read. |
| `PREVIOUS` | Allowed for `Put` and `Delete`; response must be deduped with mutation. |
| `CURRENT` | Allowed for `Get`. |
| `FOR_UPDATE` | Rejected; creates local transaction/update semantics. |
| `EXISTS` | Allowed for `Get` and iterator-like future cursor reads. |
| `DELTA` | Allowed for `Put`; response must be deduped with mutation. |
| `MULTI` | Allowed only for future resumable cursor APIs, not local iterator handles. |
| `CHANGED` | Allowed for `Put`; response must be deduped with mutation. |
| `PREVIOUS_PRESENCE` | Allowed for `Put` and `Delete`; response must be deduped with mutation. |
| `MERGED` | Allowed for `Merge`; response must be deduped with mutation. |
| `FIRST_AND_LAST` | Allowed for single-partition `ReduceRange`; wide ranges need the Phase 5 scan contract. |
| `ALL_IN_RANGE` | Allowed for `GetRange` only through future resumable partition cursor semantics. |
| `ENTRIES_COUNT` | Allowed for single-partition `ReduceRange`; wide ranges need the Phase 5 scan contract. |

## Safety Checks Beyond `isReadOnly()`

`RocksDBAPICommand.isReadOnly()` is not enough for replicated safety:

- `CloseFailedUpdate`, `CloseIterator`, `SeekTo`, and `Subsequent` return `true` for `isReadOnly()` but contain local handles and must be rejected.
- `Get` returns `false` for `FOR_UPDATE`, but all nonzero `transactionOrUpdateId` values also need rejection.
- `Flush`, `Compact`, and `ScanRaw` are read-only or admin-like but must not be routed as ordinary replicated reads.
- CDC poll is read-only but not safe until replicated CDC tokens exist.

## Transport Parity

| Surface | First-cut replicated behavior |
| --- | --- |
| Java API | Must classify commands before encoding and reject local handles synchronously. |
| gRPC/protobuf | Must expose equivalent rejected/unsupported status codes before forwarding to Ratis. `ReplicatedGrpcBoundaryTest` covers transaction, update, iterator, and nonzero write-handle calls mapping back to `RocksDBErrorType.NOT_IMPLEMENTED` without proposing to the log. |
| Thrift | If retained, must mirror Java/gRPC rejection behavior or be disabled in replicated mode. `ReplicatedThriftBoundaryTest` covers transaction, update, iterator, and nonzero write-handle calls mapping back to `RocksDBErrorType.NOT_IMPLEMENTED` without proposing to the log. |
| Rust client | Must receive the same unsupported/local-handle/cross-partition errors through the public protocol. `RockserverStatusExt` parses the Java gRPC `RocksDBError: [uid:...]` status description so Rust callers can detect `NOT_IMPLEMENTED`; covered by `cargo test -p rockserver-client`. |

## Retired Raft Spike Cleanup

The old custom Raft and MicroRaft spikes have been deleted from main sources. Remaining replicated write tests exercise the stable `ReplicatedLogClient` abstraction directly instead of binding to the deleted implementation.

| Area | Files |
| --- | --- |
| Deleted custom Raft core/store/RPC | `src/main/java/it/cavallium/rockserver/core/cluster/raft/*`, `GrpcRaftService`, gRPC Raft methods in `GrpcServer`, protobuf Raft RPCs |
| Retained production write surface | Sealed `PartitionWriteCommand` routing/admission/submission, `ReplicatedMutationCodec`, `ReplicatedMutationApplier`, `ReplicatedLogClient` |
| Retired raw-mutation facade | Main-source `ReplicatedWriteSyncAPI` removed; test-only `LegacyReplicatedWriteSyncFixture` preserves initial classifier and transport-boundary fixture coverage without exposing an obsolete production proposal path. |
| Deleted MicroRaft spike | `src/main/java/it/cavallium/rockserver/core/cluster/microraft/*`, `io.microraft:microraft` dependency |
| Current tests as fixtures | `ReplicatedCommandClassifierTest`, `ClusterRequestRouterTest`, `ReplicatedWriteSyncAPITest`, `RocksDBCheckpointSnapshotSupportTest`, `RatisPackagingBoundaryTest` |
