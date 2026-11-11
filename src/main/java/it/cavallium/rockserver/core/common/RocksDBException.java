package it.cavallium.rockserver.core.common;

public class RocksDBException extends RuntimeException {

	/**
	 * gRPC trailer that a server adds to every call that fails with a {@link RocksDBException}: {@link #NOT_APPLIED} if
	 * the failure says that the write was not applied ({@link #isNotApplied()}), {@link #MAYBE_APPLIED} otherwise. A
	 * client reads whether a write took effect from it, not from the message, which stays the same for older clients.
	 */
	public static final String APPLIED_METADATA_KEY = "rockserver-applied";
	/** {@link #APPLIED_METADATA_KEY} value: this copy of the write was not applied. */
	public static final String NOT_APPLIED = "no";
	/** {@link #APPLIED_METADATA_KEY} value: the failure does not say that the write was not applied. */
	public static final String MAYBE_APPLIED = "maybe";

	private final RocksDBErrorType errorUniqueId;
	/** The failure says that this copy of the write was not applied: see {@link #notApplied}. */
	private final boolean notApplied;

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
		SESSION_EXPIRED,
		/**
		 * The client was closed before it sent the call: a failure of the client library itself, flagged
		 * {@link #isNotApplied() not applied}. It is permanent for that client, so it has a type of its own:
		 * {@link #SERVER_OVERLOADED}, which it was before, is a state of the server that callers wait out, for as long
		 * as it takes (R61-7). A new client can send the call.
		 */
		CLIENT_CLOSED
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

	/**
	 * A failure that says that this copy of the write was not applied, for example a deadline that came before the
	 * write was sent: a client may send it again without applying it twice. Carried on the wire by
	 * {@link #APPLIED_METADATA_KEY}.
	 */
	public static RocksDBException notApplied(RocksDBErrorType errorUniqueId, String message) {
		checkStaticInvocationType(errorUniqueId);
		return new RocksDBException(errorUniqueId, message, true);
	}

	private static void checkStaticInvocationType(RocksDBErrorType errorUniqueId) {
		if (errorUniqueId == RocksDBErrorType.UPDATE_RETRY) {
			throw new UnsupportedOperationException("Please use new RocksDBRetryException()");
		}
	}

	protected RocksDBException(RocksDBErrorType errorUniqueId, String message) {
		this(errorUniqueId, message, false);
	}

	protected RocksDBException(RocksDBErrorType errorUniqueId, String message, boolean notApplied) {
		super(message);
		this.errorUniqueId = errorUniqueId;
		this.notApplied = notApplied;
	}

	protected RocksDBException(RocksDBErrorType errorUniqueId, String message, Throwable ex) {
		super(message, ex);
		this.errorUniqueId = errorUniqueId;
		this.notApplied = false;
	}

	protected RocksDBException(RocksDBErrorType errorUniqueId, Throwable ex) {
		super(ex.toString(), ex);
		this.errorUniqueId = errorUniqueId;
		this.notApplied = false;
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

	/** True if this failure says that this copy of the write was not applied: see {@link #notApplied}. */
	public boolean isNotApplied() {
		return notApplied;
	}

	@Override
	public String getLocalizedMessage() {
		return "RocksDBError: [uid:" + errorUniqueId + "] " + getMessage();
	}
}
