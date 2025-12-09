namespace java it.cavallium.rockserver.core.common.api

struct ColumnSchema {
  1: list<i32> fixedKeys,
  2: list<ColumnHashType> variableTailKeys,
  3: bool hasValue,
  4: optional string mergeOperatorName,
  5: optional i64 mergeOperatorVersion
}

struct Column {
  1: string name,
  2: ColumnSchema schema
}

enum ColumnHashType {
  XXHASH32 = 1,
  XXHASH8 = 2,
  ALLSAME8 = 3,
  FIXEDINTEGER32 = 4
}

enum Operation {
  NOTHING = 1,
  PREVIOUS = 2,
  CURRENT = 3,
  FOR_UPDATE = 4,
  EXISTS = 5,
  DELTA = 6,
  MULTI = 7,
  CHANGED = 8,
  PREVIOUS_PRESENCE = 9
}

enum PutBatchMode {
  WRITE_BATCH = 0,
  WRITE_BATCH_NO_WAL = 1,
  SST_INGESTION = 2,
  SST_INGEST_BEHIND = 3
}

enum MergeBatchMode {
  MERGE_WRITE_BATCH = 0,
  MERGE_WRITE_BATCH_NO_WAL = 1,
  MERGE_SST_INGESTION = 2,
  MERGE_SST_INGEST_BEHIND = 3
}

struct Delta {
  1: optional binary previous,
  2: optional binary current
}

struct OptionalBinary {
  1: optional binary value
}

struct UpdateBegin {
  1: optional binary previous,
  2: optional i64 updateId
}

struct KV {
  1: list<binary> keys,
  2: binary value
}

struct FirstAndLast {
  1: optional KV first,
  2: optional KV last
}

enum RocksDBErrorType {
  PUT_UNKNOWN_ERROR = 0,
  PUT_2 = 1,
  UNEXPECTED_NULL_VALUE = 2,
  PUT_1 = 3,
  PUT_3 = 4,
  GET_1 = 5,
  COLUMN_EXISTS = 6,
  COLUMN_CREATE_FAIL = 7,
  COLUMN_NOT_FOUND = 8,
  COLUMN_DELETE_FAIL = 9,
  CONFIG_ERROR = 10,
  ROCKSDB_CONFIG_ERROR = 11,
  VALUE_MUST_BE_NULL = 12,
  DIRECTORY_DELETE = 13,
  KEY_LENGTH_MISMATCH = 14,
  UNSUPPORTED_HASH_SIZE = 15,
  RAW_KEY_LENGTH_MISMATCH = 16,
  KEYS_COUNT_MISMATCH = 17,
  COMMIT_FAILED_TRY_AGAIN = 18,
  COMMIT_FAILED = 19,
  TX_NOT_FOUND = 20,
  KEY_HASH_SIZE_MISMATCH = 21,
  RESTRICTED_TRANSACTION = 22,
  PUT_INVALID_REQUEST = 23,
  UPDATE_RETRY = 24,
  ROCKSDB_LOAD_ERROR = 25,
  WRITE_BATCH_1 = 26,
  SST_WRITE_1 = 27,
  SST_WRITE_2 = 28,
  SST_WRITE_3 = 29,
  SST_WRITE_4 = 30,
  SST_GET_SIZE_FAILED = 31,
  UNSUPPORTED_COLUMN_TYPE = 32,
  NOT_IMPLEMENTED = 33,
  GET_PROPERTY_ERROR = 34,
  INTERNAL_ERROR = 35,
  TRANSACTION_NOT_FOUND = 36,
  NULL_ARGUMENT = 37
}

exception RocksDBThriftException {
  1: required RocksDBErrorType errorType,
  2: required string message
}

service RocksDB {

   i64 openTransaction(1: required i64 timeoutMs) throws (1: RocksDBThriftException e),

   bool closeTransaction(1: required i64 transactionId, 2: required bool commit) throws (1: RocksDBThriftException e),

   void closeFailedUpdate(1: required i64 updateId) throws (1: RocksDBThriftException e),

   i64 createColumn(1: required string name, 2: required ColumnSchema schema) throws (1: RocksDBThriftException e),

   void deleteColumn(1: required i64 columnId) throws (1: RocksDBThriftException e),

   i64 getColumnId(1: required string name) throws (1: RocksDBThriftException e),

   oneway void putFast(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value),

   void put(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value) throws (1: RocksDBThriftException e),

   void putMulti(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti) throws (1: RocksDBThriftException e),

   OptionalBinary putGetPrevious(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value) throws (1: RocksDBThriftException e),

   Delta putGetDelta(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value) throws (1: RocksDBThriftException e),

   bool putGetChanged(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value) throws (1: RocksDBThriftException e),

   bool putGetPreviousPresence(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value) throws (1: RocksDBThriftException e),

   OptionalBinary get(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys) throws (1: RocksDBThriftException e),

   UpdateBegin getForUpdate(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys) throws (1: RocksDBThriftException e),

   bool exists(1: required i64 transactionOrUpdateId, 3: required i64 columnId, 4: required list<binary> keys) throws (1: RocksDBThriftException e),

   i64 openIterator(1: required i64 transactionId, 2: required i64 columnId, 3: required list<binary> startKeysInclusive, 4: list<binary> endKeysExclusive, 5: required bool reverse, 6: required i64 timeoutMs) throws (1: RocksDBThriftException e),

   void closeIterator(1: required i64 iteratorId) throws (1: RocksDBThriftException e),

   void seekTo(1: required i64 iterationId, 2: required list<binary> keys) throws (1: RocksDBThriftException e),

   void subsequent(1: required i64 iterationId, 2: required i64 skipCount, 3: required i64 takeCount) throws (1: RocksDBThriftException e),

   bool subsequentExists(1: required i64 iterationId, 2: required i64 skipCount, 3: required i64 takeCount) throws (1: RocksDBThriftException e),

   list<OptionalBinary> subsequentMultiGet(1: required i64 iterationId, 2: required i64 skipCount, 3: required i64 takeCount) throws (1: RocksDBThriftException e),

   void putBatch(1: required i64 columnId, 2: required list<KV> data, 3: required PutBatchMode mode) throws (1: RocksDBThriftException e),

   void merge(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value) throws (1: RocksDBThriftException e),

   void mergeMulti(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti) throws (1: RocksDBThriftException e),

   void mergeBatch(1: required i64 columnId, 2: required list<KV> data, 3: required MergeBatchMode mode) throws (1: RocksDBThriftException e),

   OptionalBinary mergeGetMerged(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value) throws (1: RocksDBThriftException e),

   list<OptionalBinary> mergeMultiGetMerged(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti) throws (1: RocksDBThriftException e),

   FirstAndLast reduceRangeFirstAndLast(1: required i64 transactionId, 2: required i64 columnId, 3: required list<binary> startKeysInclusive, 4: list<binary> endKeysExclusive, 5: required bool reverse, 6: required i64 timeoutMs) throws (1: RocksDBThriftException e),

   i64 reduceRangeEntriesCount(1: required i64 transactionId, 2: required i64 columnId, 3: required list<binary> startKeysInclusive, 4: list<binary> endKeysExclusive, 5: required bool reverse, 6: required i64 timeoutMs) throws (1: RocksDBThriftException e),

   list<KV> getAllInRange(1: required i64 transactionId, 2: required i64 columnId, 3: required list<binary> startKeysInclusive, 4: list<binary> endKeysExclusive, 5: required bool reverse, 6: required i64 timeoutMs) throws (1: RocksDBThriftException e),

   i64 uploadMergeOperator(1: required string operatorName, 2: required string className, 3: required binary jarPayload) throws (1: RocksDBThriftException e),

   list<OptionalBinary> putMultiGetPrevious(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti) throws (1: RocksDBThriftException e),

   list<Delta> putMultiGetDelta(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti) throws (1: RocksDBThriftException e),

   list<bool> putMultiGetChanged(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti) throws (1: RocksDBThriftException e),

   list<bool> putMultiGetPreviousPresence(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti) throws (1: RocksDBThriftException e),

   void flush() throws (1: RocksDBThriftException e),

   void compact() throws (1: RocksDBThriftException e),

   list<Column> getAllColumnDefinitions() throws (1: RocksDBThriftException e),

}
