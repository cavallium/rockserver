include "rocksdb.thrift"
namespace java it.cavallium.rockserver.core.common.api

enum ExportPhase { CAPTURING = 0, EXPORTING = 1, MATERIALIZED = 2, READY = 3, SNAPSHOT_LOST = 4, CANCELLED = 5, RELEASED = 6 }
struct ExportLimits {
 1: required i32 maximumChunks, 2: required i64 maximumJournalBytes, 3: required i64 maximumDiskBytes,
 4: required i32 maximumRows, 5: required i64 maximumPayloadBytes, 6: required i64 maximumFileBytes,
 7: required i64 maximumChunkDurationNanos, 8: required i64 snapshotTtlMillis
}
struct ExportCreate { 1: required binary requestId, 2: required list<i64> columns, 3: required ExportLimits limits }
struct ExportChunkId {
 1: required binary jobId, 2: required binary groupId, 3: required binary rangeLineage,
 4: required i64 rangeEpoch, 5: required i64 snapshotTerm, 6: required i64 snapshotIndex,
 7: required i64 ordinal, 8: required binary contentHash
}
struct ExportStatus {
 1: required binary jobId, 2: required ExportPhase phase, 3: required list<i64> columns,
 4: required i32 participantCount, 5: required i32 chunkCount, 6: required i32 acknowledgedCount,
 7: required i64 fileBytes, 8: required bool cleanupPending, 9: required binary schema
}
struct ExportChunk {
 1: required ExportChunkId id, 2: required i64 columnId, 3: optional binary lowerInclusive,
 4: optional binary upperExclusive, 5: required i64 rows, 6: required i64 payloadBytes,
 7: required i64 fileBytes, 8: required bool hasMore
}
struct ExportManifestRequest { 1: required binary jobId, 2: required i32 offset, 3: required i32 maximumItems, 4: required i32 maximumBytes }
struct ExportManifestPage { 1: required list<ExportChunk> chunks, 2: required i32 next, 3: required bool hasMore }
struct ExportReadRequest { 1: required ExportChunkId id, 2: required i64 offset, 3: required i32 maximumBytes }
struct ExportFilePage { 1: required ExportChunkId id, 2: required i64 offset, 3: required binary data, 4: required bool hasMore }
struct ExportCapabilities {
 1: required bool available, 2: required rocksdb.RawScanMode rawScanMode,
 3: required i32 maximumColumns, 4: required i32 maximumChunks, 5: required i32 maximumManifestItems,
 6: required i32 maximumManifestBytes, 7: required i32 maximumFilePageBytes
}
service RocksDBExports {
 ExportCapabilities getExportCapabilities(),
 ExportStatus createExport(1: required ExportCreate request, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ExportStatus recoverExport(1: required binary jobId, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ExportStatus stepExport(1: required binary jobId, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ExportStatus exportStatus(1: required binary jobId, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ExportManifestPage exportManifest(1: required ExportManifestRequest request, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ExportFilePage readExport(1: required ExportReadRequest request, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 void acknowledgeExport(1: required ExportChunkId id, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 void releaseExport(1: required binary jobId, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 void cancelExport(1: required binary jobId, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e)
}
