package it.cavallium.rockserver.core.common;

public class RocksDBException extends RuntimeException {

	private final RocksDBErrorType errorUniqueId;

	public enum RocksDBErrorType {
		PUT_UNKNOWN_ERROR,
		PUT_2,
		UNEXPECTED_NULL_VALUE,
		PUT_1, PUT_3,
		GET_1,
		COLUMN_EXISTS,
		COLUMN_CREATE_FAIL,
		COLUMN_NOT_FOUND,
		COLUMN_DELETE_FAIL,
		CONFIG_ERROR,
		ROCKSDB_CONFIG_ERROR,
		VALUE_MUST_BE_NULL,
		DIRECTORY_DELETE,
		KEY_LENGTH_MISMATCH,
		UNSUPPORTED_HASH_SIZE,
		RAW_KEY_LENGTH_MISMATCH,
		KEYS_COUNT_MISMATCH,
		COMMIT_FAILED_TRY_AGAIN,
		COMMIT_FAILED,
		TX_NOT_FOUND,
		KEY_HASH_SIZE_MISMATCH, RESTRICTED_TRANSACTION, PUT_INVALID_REQUEST, UPDATE_RETRY, ROCKSDB_LOAD_ERROR,
		WRITE_BATCH_1,
		SST_WRITE_1,
		SST_WRITE_2,
		SST_WRITE_3,
		SST_WRITE_4,
		SST_GET_SIZE_FAILED,
		UNSUPPORTED_COLUMN_TYPE,
		NOT_IMPLEMENTED,
		GET_PROPERTY_ERROR,
		INTERNAL_ERROR,
		TRANSACTION_NOT_FOUND,
		NULL_ARGUMENT,
		READ_DEADLINE_EXCEEDED,
		SERVER_OVERLOADED,
		RANGE_ITEM_TOO_LARGE,
		COMPACTION_CONFLICT,
		COMPACTION_FAILED,
		GET_MULTI_ITEM_TOO_LARGE,
		INVALID_REQUEST,
		/**
		 * A merge operator failed: it threw, or returned no value, while merging the operands of a key. The failure
		 * follows from the operator and the data, not from the machine: a replicated write gets it as its result on
		 * every replica.
		 */
		MERGE_OPERATOR_FAILED,
		/**
		 * A write may or may not have been applied: the server lost track of it (leader change, lost connection,
		 * deadline) after it may have been appended. Resending it as a new write can apply it twice. A replicated
		 * server's client library resends it with its own request id until its deadline, and reports this only when
		 * that did not settle the outcome.
		 */
		OUTCOME_UNKNOWN,
		/**
		 * The client session of a write is unknown to the replicated server, or expired: this copy of the write was
		 * not applied. A write sent for the first time can be sent again under a new session; for a resent write,
		 * whose earlier copy may have been applied, the outcome is unknown.
		 */
		SESSION_EXPIRED
	}

	public static RocksDBException of(RocksDBErrorType errorUniqueId, String message) {
		checkStaticInvocationType(errorUniqueId);
		return new RocksDBException(errorUniqueId, message);
	}

	public static RocksDBException of(RocksDBErrorType errorUniqueId, Throwable ex) {
		checkStaticInvocationType(errorUniqueId);
		if (ex instanceof org.rocksdb.RocksDBException e) {
			return new RocksDBException(errorUniqueId, e);
		} else {
			return new RocksDBException(errorUniqueId, ex);
		}
	}

	public static RocksDBException of(RocksDBErrorType errorUniqueId, String message, Throwable ex) {
		checkStaticInvocationType(errorUniqueId);
		if (ex instanceof org.rocksdb.RocksDBException e) {
			return new RocksDBException(errorUniqueId, message, e);
		} else {
			return new RocksDBException(errorUniqueId, message, ex);
		}
	}

	private static void checkStaticInvocationType(RocksDBErrorType errorUniqueId) {
		if (errorUniqueId == RocksDBErrorType.UPDATE_RETRY) {
			throw new UnsupportedOperationException("Please use new RocksDBRetryException()");
		}
	}

	protected RocksDBException(RocksDBErrorType errorUniqueId, String message) {
		super(message);
		this.errorUniqueId = errorUniqueId;
	}

	protected RocksDBException(RocksDBErrorType errorUniqueId, String message, Throwable ex) {
		super(message, ex);
		this.errorUniqueId = errorUniqueId;
	}

	protected RocksDBException(RocksDBErrorType errorUniqueId, Throwable ex) {
		super(ex.toString(), ex);
		this.errorUniqueId = errorUniqueId;
	}

	protected RocksDBException(RocksDBErrorType errorUniqueId, org.rocksdb.RocksDBException ex) {
		this(errorUniqueId, ex.getMessage());
		super.initCause(ex);
	}

	protected RocksDBException(RocksDBErrorType errorUniqueId, String message, org.rocksdb.RocksDBException ex) {
		this(errorUniqueId, message + ": " + ex.getMessage());
		// Kept: the native status tells a storage failure from a logical one.
		super.initCause(ex);
	}

	public RocksDBErrorType getErrorUniqueId() {
		return errorUniqueId;
	}

	@Override
	public String getLocalizedMessage() {
		return "RocksDBError: [uid:" + errorUniqueId + "] " + getMessage();
	}
}
