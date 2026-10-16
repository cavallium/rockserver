include "rocksdb.thrift"
namespace java it.cavallium.rockserver.core.common.api

enum ThriftSchemaOperationKind { CREATE = 0, DELETE = 1, DELETE_IF_EXISTS = 2, UPLOAD = 3 }
enum ThriftSchemaOperationState { QUEUED = 0, NOT_STARTED = 1, RUNNING = 2, PAUSED = 3, DONE = 4, ABORTED = 5, CONFLICT = 6 }
enum ThriftSchemaOperationPhase {
 ALLOCATE = 0, SCHEMA = 1, DEFINITION = 2, BEGIN = 3, ARTIFACTS = 4, CAPACITY = 5, ADMISSIONS = 6,
 FENCING = 7, FENCE = 8, INSTALL = 9, PUBLISH = 10, ACTIVATE = 11, RETIRE = 12, CLEANUP = 13,
 DONE = 14, ABORTED = 15, CONFLICT = 16, OPERATOR_BEGIN = 17, OPERATOR_CHUNKS = 18, OPERATOR_PUBLISH = 19
}
struct ThriftSchemaOperationResult {
 5: required binary requestId, 6: required ThriftSchemaOperationKind kind, 9: required i64 value,
 10: required bool changed, 11: required bool aborted
}
struct ThriftSchemaOperationStatus {
 5: required binary requestId, 6: required ThriftSchemaOperationKind kind, 7: required ThriftSchemaOperationState state,
 8: optional ThriftSchemaOperationPhase phase, 12: optional ThriftSchemaOperationResult result
}
service RocksDBSchemaOperations {
 list<ThriftSchemaOperationStatus> inventory(100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ThriftSchemaOperationStatus status(5: required binary requestId, 100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ThriftSchemaOperationStatus resume(5: required binary requestId, 100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e),
 ThriftSchemaOperationStatus abort(5: required binary requestId, 100: required rocksdb.RequestContext context) throws (1: rocksdb.RocksDBThriftException e)
}
