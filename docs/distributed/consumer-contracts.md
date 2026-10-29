# Existing Yotsuba consumer contracts

Read-only evidence for distributed Rockserver, inspected 2026-10-07 against Yotsuba commit `61ee2eb79b0a5738d9df2829563f49a21523fc21` in `/home/cavallium/IdeaProjects/Yotsuba-private`. Relevant inspected paths had no working-tree diff. This is source evidence, not a benchmark or production measurement.

A read-only refresh on 2026-10-15 found that this checkout is now at
`321817a7d831f6571a7903d3ed5a878ec5b5e355`, pins Rockserver `2.0.2` in
`server/source/pom.xml`, and has no tracked `MessagesV2Migration` file. A tracked
Java-source search found no `DatabaseExportDefinition`, `exportDatabase` or
`ExportWitness` references. The evidence below remains specific to `61ee2eb`;
it does not establish aggregate-export adoption by the current checkout. No
Yotsuba files were changed.

Path prefixes used below:

- `core/`: `server/source/core/src/main/java/org/yotsuba/core/database/data/`
- `monitor/`: `server/source/plugins/monitor/src/main/java/org/yotsuba/plugin/monitor/storage/`
- `api/`: `server/source/api/src/main/java/org/yotsuba/database/`

## Atomicity and conflict detection

`monitor/RockserverMonitorStorage.java:389` acquires an identity with `forUpdate` and writes identity and expiration columns in one transaction. Lines 446 and 459 establish rollback on failure/cancellation, optimistic retries, and false commit results as conflicts. Lines 764, 771, 799, and 854–911 implement conditional replacement across identity, identity-expiration, contextual-result, and result-expiration columns. The chosen identity and all corresponding rows must commit together.

`core/MessagesImpl.java:724` opens an optimistic transaction, validates a migration fence, reads the canonical previous value, and stages its patch. `core/MessagesV2Migration.java:350` stages source/destination writes and the overlay; line 381 stages deletes and overlay tombstones; line 410 reads the epoch/shard fence using `forUpdate`. Conflicts on the fence and on canonical/migration columns must participate in the same transaction.

Some derived writes intentionally have weaker semantics. `core/MessagesImpl.java:1024` establishes monotonic sender presence outside the hot transaction; stale positives are safe. `core/SentMessageSenderIndex.java:188` uses key-only `RequestType.ensure()`. `monitor/RockserverMonitorStorage.java:501` writes expiration then queued match as separate autocommit operations. Do not impose cross-shard transactions on every derived index.

## Logical reads and retained resources

`core/MessagesImpl.java:1785` uses `firstAndLast()` for chat extremes; line 2715 uses bounded `existsMultiAsync`. The distributed API must preserve comparator order, range boundaries, reverse direction, and first/last results.

`docs/rockserver-workload-mapping.md:53` sets a hard limit of 4,096 logical items and 8 MiB of encoded key/value bytes per range page. `resumeAfter` is exclusive in either direction. Autocommit pages are independently consistent; explicit-transaction pages retain that transaction snapshot. `api/RockserverRangePages.java:48–79` fetches pages lazily with original bounds, direction and transaction ID; lines 115 and 122 require finite timeouts and reject excessive limits instead of clamping.

`docs/rockserver-workload-mapping.md:65` retains demand-driven streams and server iterators with cancellation-driven resource release. `monitor/RockserverMonitorStorage.java:1025` uses streaming ranges; lines 1033 and 1051 use bounded pages. The exact snapshot semantics of a nontransactional long-lived stream still need server-side verification.

`core/FileLocationsImpl.java:109` uses fixed URL hashes plus variable hashed keys, exact URL-collision filtering and key-only records. Line 351 and the workload mapping at line 57 prohibit partial success for a single logical bucket: `hasMore` causes overload, without continuation. A physical shard fragment cannot be returned as a complete logical bucket.

## Physical migration API

`core/MessagesV2Migration.java:1259` calls `scanRawResumableAsync(sourceColumnId, 0, 1, acknowledgedSsts)`. Line 1789 handles raw batches and `SstCompleted`; line 1839 waits for prior work before publishing completion. Line 1269 performs destination writes before ordered acknowledgement; line 1878 persists completed tokens only after those writes. Line 1918 performs lease-validated transactional logical `putMultiAsync`; line 1953 persists checkpoints in a fence-validated transaction.

Physical SST-token identity, complete source coverage, completion barriers and durable acknowledgement ordering are observable dependencies. Distribution must provide stable generation/coverage semantics or explicitly adapt this consumer; it cannot silently reinterpret tokens after movement or splitting.

## Search limits

The scoped search found no direct native merge/operator-registration or `putBatchAsync` usage in non-test Yotsuba Java. A mapping-document reference to merge setup does not prove an operand algebra. The observed migration destination uses logical `putMultiAsync`, not SST ingestion. CDC is delegated by `api/RockserverAsyncApiViews.java:128`, but no active polling consumer was found; CDC has since been removed from Rockserver altogether. These absences do not remove Rockserver's general merge or bulk-ingestion contracts; inspect the server API and tests.
