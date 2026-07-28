namespace java it.cavallium.rockserver.core.common.api

struct ColumnSchema {
  1: list<i32> fixedKeys,
  2: list<ColumnHashType> variableTailKeys,
  3: bool hasValue,
  4: optional string mergeOperatorName,
  5: optional i64 mergeOperatorVersion,
  6: optional string mergeOperatorClass
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

enum WorkloadProfile {
  CONTROL = 1,
  LATENCY = 2,
  ANALYTICAL = 3,
  INGEST = 4,
  CDC = 5,
  BATCH = 6,
  PHYSICAL_MAINTENANCE = 7
}

struct RequestContext {
  1: required WorkloadProfile profile,
  2: required i64 deadlineEpochMillis
}

struct Capabilities {
  1: required i32 workloadContractVersion,
  2: required bool boundedRange
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

struct OptionalLongValue {
  1: optional i64 value
}

enum CDCOperation {
  PUT = 0,
  DELETE = 1,
  MERGE = 2
}

struct CDCEvent {
  1: required i64 seq,
  2: required i64 columnId,
  3: required binary key,
  4: optional binary value,
  5: required CDCOperation op
}

struct CdcPollBatchResult {
  1: required list<CDCEvent> events,
  2: required i64 nextSeq
}

struct CdcCreateRequest {
  1: required string id,
  2: optional i64 fromSeq,
  3: optional list<i64> columnIds,
  4: optional bool emitLatestValues,
  // Both absent means unchecked. expectAbsent=true requires no metadata;
  // expectedLastCommittedSeq requires an exact durable checkpoint.
  5: optional bool expectAbsent,
  6: optional i64 expectedLastCommittedSeq
}

struct CdcPollRequest {
  1: required string id,
  2: optional i64 fromSeq,
  3: required i64 maxEvents,
  4: optional i32 maxResponseBytes
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

enum RangeRequestType {
  ALL_IN_RANGE = 1,
  ALL_IN_RANGE_NO_CACHE = 2
}

struct RangeBudget {
  1: required i32 maxItems,
  2: required i64 maxBytes
}

struct RangePage {
  1: required list<KV> items,
  2: optional list<binary> resumeAfter,
  3: required bool hasMore
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
  NULL_ARGUMENT = 37,
  READ_DEADLINE_EXCEEDED = 38,
	CDC_GAP_DETECTED = 39,
	CDC_RESPONSE_TOO_LARGE = 40,
	CDC_SUBSCRIPTION_CHANGED = 41,
	CDC_SUBSCRIPTION_NOT_FOUND = 42,
	SERVER_OVERLOADED = 43,
	RANGE_ITEM_TOO_LARGE = 44
}

exception RocksDBThriftException {
  1: required RocksDBErrorType errorType,
  2: required string message
}

service RocksDB {

   i64 openTransaction(1: required i64 timeoutMs, 2: required RequestContext context) throws (1: RocksDBThriftException e),

   bool closeTransaction(1: required i64 transactionId, 2: required bool commit, 3: required RequestContext context) throws (1: RocksDBThriftException e),

   void closeFailedUpdate(1: required i64 updateId) throws (1: RocksDBThriftException e),

   i64 createColumn(1: required string name, 2: required ColumnSchema schema, 3: required RequestContext context) throws (1: RocksDBThriftException e),

   void deleteColumn(1: required i64 columnId, 2: required RequestContext context) throws (1: RocksDBThriftException e),

   bool deleteColumnIfExists(1: required string name, 2: required RequestContext context) throws (1: RocksDBThriftException e),

   i64 getColumnId(1: required string name, 2: required RequestContext context) throws (1: RocksDBThriftException e),

   i64 estimateNumKeys(1: required i64 columnId, 2: required RequestContext context) throws (1: RocksDBThriftException e),

   void putFast(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   void put(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   void putMulti(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   OptionalBinary putGetPrevious(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   Delta putGetDelta(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   bool putGetChanged(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   bool putGetPreviousPresence(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   void delete(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   OptionalBinary deleteGetPrevious(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   bool deleteGetPreviousPresence(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   void deleteMulti(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   list<OptionalBinary> deleteMultiGetPrevious(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   list<bool> deleteMultiGetPreviousPresence(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   OptionalBinary get(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   UpdateBegin getForUpdate(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   bool exists(1: required i64 transactionOrUpdateId, 3: required i64 columnId, 4: required list<binary> keys, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   list<bool> existsMulti(1: required i64 transactionId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required i64 timeoutMs, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   i64 openIterator(1: required i64 transactionId, 2: required i64 columnId, 3: list<binary> startKeysInclusive, 4: list<binary> endKeysExclusive, 5: required bool reverse, 6: required i64 timeoutMs, 7: required RequestContext context) throws (1: RocksDBThriftException e),

   void closeIterator(1: required i64 iteratorId) throws (1: RocksDBThriftException e),

   void seekTo(1: required i64 iterationId, 2: required list<binary> keys, 3: required RequestContext context) throws (1: RocksDBThriftException e),

   void subsequent(1: required i64 iterationId, 2: required i64 skipCount, 3: required i64 takeCount, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   bool subsequentExists(1: required i64 iterationId, 2: required i64 skipCount, 3: required i64 takeCount, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   list<OptionalBinary> subsequentMultiGet(1: required i64 iterationId, 2: required i64 skipCount, 3: required i64 takeCount, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   void putBatch(1: required i64 columnId, 2: required list<KV> data, 3: required PutBatchMode mode, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   void merge(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   void mergeMulti(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   void mergeBatch(1: required i64 columnId, 2: required list<KV> data, 3: required MergeBatchMode mode, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   void deleteRange(1: required i64 columnId, 2: list<binary> startKeysInclusive, 3: list<binary> endKeysExclusive, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   OptionalBinary mergeGetMerged(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<binary> keys, 4: required binary value, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   list<OptionalBinary> mergeMultiGetMerged(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   FirstAndLast reduceRangeFirstAndLast(1: required i64 transactionId, 2: required i64 columnId, 3: list<binary> startKeysInclusive, 4: list<binary> endKeysExclusive, 5: required bool reverse, 6: required i64 timeoutMs, 7: required RequestContext context) throws (1: RocksDBThriftException e),

   i64 reduceRangeEntriesCount(1: required i64 transactionId, 2: required i64 columnId, 3: list<binary> startKeysInclusive, 4: list<binary> endKeysExclusive, 5: required bool reverse, 6: required i64 timeoutMs, 7: required RequestContext context) throws (1: RocksDBThriftException e),

   list<KV> getAllInRange(1: required i64 transactionId, 2: required i64 columnId, 3: list<binary> startKeysInclusive, 4: list<binary> endKeysExclusive, 5: required bool reverse, 6: required i64 timeoutMs, 7: required RequestContext context) throws (1: RocksDBThriftException e),

   list<KV> getAllInRangeNoCache(1: required i64 transactionId, 2: required i64 columnId, 3: list<binary> startKeysInclusive, 4: list<binary> endKeysExclusive, 5: required bool reverse, 6: required i64 timeoutMs, 7: required RequestContext context) throws (1: RocksDBThriftException e),

   i64 uploadMergeOperator(1: required string operatorName, 2: required string className, 3: required binary jarPayload, 4: required RequestContext context) throws (1: RocksDBThriftException e),

   list<OptionalBinary> putMultiGetPrevious(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   list<Delta> putMultiGetDelta(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   list<bool> putMultiGetChanged(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   list<bool> putMultiGetPreviousPresence(1: required i64 transactionOrUpdateId, 2: required i64 columnId, 3: required list<list<binary>> keysMulti, 4: required list<binary> valueMulti, 5: required RequestContext context) throws (1: RocksDBThriftException e),

   void flush() throws (1: RocksDBThriftException e),

   void compact() throws (1: RocksDBThriftException e),

   list<Column> getAllColumnDefinitions(1: required RequestContext context) throws (1: RocksDBThriftException e),

   i64 cdcCreate(1: required CdcCreateRequest request) throws (1: RocksDBThriftException e),

   void cdcDelete(1: required string id) throws (1: RocksDBThriftException e),

   i64 cdcGetEarliestAvailableSequence() throws (1: RocksDBThriftException e),

   OptionalLongValue cdcGetLastCommittedSequence(1: required string id) throws (1: RocksDBThriftException e),

   CdcPollBatchResult cdcPollBatch(1: required CdcPollRequest request) throws (1: RocksDBThriftException e),

   void cdcCommit(1: required string id, 2: required i64 seq) throws (1: RocksDBThriftException e),

   Capabilities getCapabilities(),

   RangePage getRangePage(1: required i64 transactionId, 2: required i64 columnId, 3: list<binary> startKeysInclusive, 4: list<binary> endKeysExclusive, 5: required bool reverse, 6: list<binary> resumeAfter, 7: required RangeRequestType requestType, 8: required i64 timeoutMs, 9: required RangeBudget budget, 10: required RequestContext context) throws (1: RocksDBThriftException e),

}
