include "rocksdb.thrift"
namespace java it.cavallium.rockserver.core.common.api

struct ThriftPhysicalContext { 108: required i64 timeoutNanos }
struct ThriftPhysicalScope { 104: optional binary pool, 106: optional binary column }
struct ThriftPhysicalTarget {
 101: required binary node, 102: required binary incarnation, 103: required binary process,
 104: required binary pool, 105: required binary session, 106: required binary column, 107: required i64 nativeColumnId
}
struct ThriftPhysicalObservation {
 120: required ThriftPhysicalTarget target, 121: required i64 estimatedKeys,
 201: required string actualCFComparator, 122: required rocksdb.ColumnTablePropertiesData tables
}
struct ThriftPhysicalFile {
 202: required string name, 130: required i32 level, 131: required i32 pathId, 132: required i64 sizeBytes,
 203: required string smallestKeyHex, 204: required string largestKeyHex, 133: required bool beingCompacted
}
struct ThriftPhysicalMetadata {
 205: required string session, 107: required i64 columnId, 206: required string columnName,
 134: required i32 numLevels, 135: required i32 baseLevel,
 210: required list<string> paths, 211: required list<ThriftPhysicalFile> files
}
struct ThriftPhysicalCompactRequest {
 107: required i64 columnId, 205: required string session, 212: required list<string> files,
 130: required i32 level, 131: required i32 outputPathId, 136: required i64 outputFileSizeLimit,
 137: required i64 maxInputBytes, 138: required i32 maxSubcompactions, 139: required bool execute
}
struct ThriftPhysicalCompactResult {
 139: required bool executed, 212: required list<string> inputFiles, 213: required list<string> outputFiles,
 140: required i64 validatedInputBytes, 141: required i64 outputBytes, 142: required i64 elapsedNanos
}
struct ThriftQualifiedPhysicalMetadata { 120: required ThriftPhysicalTarget target, 123: required ThriftPhysicalMetadata metadata }
struct ThriftQualifiedPhysicalCompaction { 120: required ThriftPhysicalTarget target, 124: required ThriftPhysicalCompactResult result }
service RocksDBReplicaPhysical {
 list<ThriftPhysicalTarget> discover(111: required ThriftPhysicalScope scope, 110: required ThriftPhysicalContext context) throws (1: rocksdb.RocksDBThriftException e),
 list<ThriftPhysicalObservation> observe(111: required ThriftPhysicalScope scope, 110: required ThriftPhysicalContext context) throws (1: rocksdb.RocksDBThriftException e),
 void flush(111: required ThriftPhysicalScope scope, 110: required ThriftPhysicalContext context) throws (1: rocksdb.RocksDBThriftException e),
 void compact(111: required ThriftPhysicalScope scope, 110: required ThriftPhysicalContext context) throws (1: rocksdb.RocksDBThriftException e),
 ThriftQualifiedPhysicalMetadata metadata(120: required ThriftPhysicalTarget target, 130: required i32 level, 110: required ThriftPhysicalContext context) throws (1: rocksdb.RocksDBThriftException e),
 ThriftQualifiedPhysicalCompaction compactFiles(120: required ThriftPhysicalTarget target, 125: required ThriftPhysicalCompactRequest request, 110: required ThriftPhysicalContext context) throws (1: rocksdb.RocksDBThriftException e)
}
