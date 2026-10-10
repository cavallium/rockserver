include "rocksdb.thrift"
namespace java it.cavallium.rockserver.core.common.api.cdc

enum CdcPhase { CREATE = 0, ACTIVE = 1, CLOSING = 2, CLOSED = 3, CONFLICT = 4 }
enum CdcOutcome { DATA = 0, GAP = 1, TOO_LARGE = 2 }
enum CdcMutationKind { PUT = 0, DELETE = 1, MERGE = 2, DELETE_RANGE = 3, BUCKET_PUT = 4, BUCKET_DELETE = 5, BUCKET_MERGE = 6 }
enum CdcReturnMode { NONE = 0, PREVIOUS = 1, CURRENT = 2 }
struct CdcPosition { 1: required i64 term, 2: required i64 index }
struct CdcCursor { 6: required CdcPosition position, 7: required i32 ordinal }
struct CdcLineage { 1: required binary rangeId, 2: required binary groupId, 6: required i64 rangeEpoch }
struct CdcOperation { 1: required binary sessionId, 6: required i64 epoch, 7: required i64 sequence }
struct CdcMutation {
 1: required binary columnId, 6: required CdcMutationKind kind, 7: required CdcReturnMode returnMode,
 21: required binary key, 22: optional binary value, 32: optional list<binary> memberKeys
}
struct CdcEvent { 7: required i32 ordinal, 8: required CdcMutation mutation }
struct CdcEnvelope {
 6: required CdcPosition position, 20: required binary schemaDigest, 8: required CdcOperation operation,
 26: optional binary finalization, 31: required list<CdcEvent> events
}
struct CdcBatch {
 1: required binary batchId, 24: required binary definition, 6: required CdcLineage lineage,
 7: required CdcCursor fromCursor, 8: required CdcCursor nextCursor, 9: required CdcPosition cut,
 30: required list<CdcEnvelope> envelopes
}
struct CdcPoll {
 6: required CdcLineage lineage, 7: required CdcOutcome outcome, 8: optional CdcBatch batch,
 9: required i32 requiredBytes, 10: required CdcCursor floor, 11: required CdcPosition cut
}
struct CdcStatus { 23: required binary state, 6: required CdcPhase phase }
struct CdcContext { 25: required binary directory }
service RocksDBDistributedCdc {
 CdcContext context(1: required binary domainId, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 CdcStatus create(24: required binary definition, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 CdcStatus recover(1: required binary subscriptionId, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 CdcStatus status(1: required binary subscriptionId, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 CdcPoll poll(1: required binary subscriptionId, 6: required i32 maxRecords, 7: required i32 maxBytes, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 CdcStatus acknowledge(1: required binary subscriptionId, 3: required binary batchId, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 CdcStatus closeSubscription(1: required binary subscriptionId, 2: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e)
}
