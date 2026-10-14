include "rocksdb.thrift"
namespace java it.cavallium.rockserver.core.common.api

enum ThriftMutationKind { POINT = 0, RANGE = 1, BULK = 2 }
enum ThriftPointMutationKind { PUT = 0, DELETE = 1, MERGE = 2 }
enum ThriftMutationCallback {
 NOTHING = 0, PREVIOUS = 1, CURRENT = 2, FOR_UPDATE = 3, EXISTS = 4, DELTA = 5,
 MULTI = 6, CHANGED = 7, PREVIOUS_PRESENCE = 8, MERGED = 9, FIRST_AND_LAST = 10,
 ALL_IN_RANGE = 11, ENTRIES_COUNT = 12, ALL_IN_RANGE_NO_CACHE = 13, ENSURE = 14
}
enum ThriftMutationState { PREPARED = 0, PENDING = 1, TERMINAL = 2, ACKNOWLEDGED = 3, EXPIRED = 4, OWNER_UNAVAILABLE = 5, UNAVAILABLE = 6 }
enum ThriftMutationOutcome { SUCCESS = 0, RESULT_TOO_LARGE = 1, CONFLICT = 2, SESSION_EXPIRED = 3, STALE_SCHEMA = 4 }
struct ThriftMutationOwner {
 1: required binary clusterId, 2: required binary databaseId, 3: required binary domainId, 4: required binary ownerId,
 6: required ThriftMutationKind kind, 7: required i64 ownerEpoch, 8: required i64 nextRequestSequence,
 10: required binary schemaDigest, 11: required binary directoryDigest
}
struct ThriftMutationEntry { 40: required list<binary> keys, 41: optional binary value }
struct ThriftMutationObservation {
 20: required binary token, 60: required ThriftMutationState state, 61: required i64 appliedMutations,
 62: required i64 copiedBatches, 63: required i32 retainedResults,
 64: optional ThriftMutationOutcome outcome, 22: optional binary resultDigest
}
struct ThriftMutationPage {
 70: required ThriftMutationObservation status, 71: required i32 offset, 72: required i32 nextOffset,
 45: required list<binary> rows, 73: required bool hasMore
}
service RocksDBRemoteMutations {
 list<ThriftMutationOwner> owners(32: required i64 column, 100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 binary preparePoint(30: required ThriftMutationOwner owner, 5: required binary requestId, 31: required i64 requestSequence,
   32: required i64 column, 42: required list<ThriftMutationEntry> entries, 43: required bool valuesPresent,
   44: required ThriftPointMutationKind kind, 46: required ThriftMutationCallback callback, 47: required bool multi,
   100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 binary prepareRange(30: required ThriftMutationOwner owner, 5: required binary requestId, 31: required i64 requestSequence,
   32: required i64 column, 51: list<binary> lower, 52: list<binary> upper,
   100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 binary prepareBulk(30: required ThriftMutationOwner owner, 5: required binary requestId, 31: required i64 requestSequence,
   32: required i64 column, 53: required bool merge, 100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ThriftMutationObservation execute(20: required binary token, 100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ThriftMutationObservation resume(20: required binary token, 100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ThriftMutationObservation status(20: required binary token, 100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ThriftMutationPage results(20: required binary token, 71: required i32 offset, 74: required i32 maximumItems,
   75: required i32 maximumBytes, 100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ThriftMutationObservation acknowledge(20: required binary token, 22: required binary resultDigest,
   100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ThriftMutationObservation appendBatch(20: required binary token, 76: required i64 ordinal, 42: required list<ThriftMutationEntry> entries,
   23: required binary inputDigest, 100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ThriftMutationObservation finishInput(20: required binary token, 100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e)
}
