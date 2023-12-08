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
		KEY_HASH_SIZE_MISMATCH,
		ROCKSDB_LOAD_ERROR
	}

	public static RocksDBException of(RocksDBErrorType errorUniqueId, String message) {
		return new RocksDBException(errorUniqueId, message);
	}

	public static RocksDBException of(RocksDBErrorType errorUniqueId, Throwable ex) {
		if (ex instanceof RocksDBException e) {
			return new RocksDBException(errorUniqueId, e);
		} else {
			return new RocksDBException(errorUniqueId, ex);
		}
	}

	public static RocksDBException of(RocksDBErrorType errorUniqueId, String message, Throwable ex) {
		if (ex instanceof RocksDBException e) {
			return new RocksDBException(errorUniqueId, message, e);
		} else {
			return new RocksDBException(errorUniqueId, message, ex);
		}
	}

	private RocksDBException(RocksDBErrorType errorUniqueId, String message) {
		super(message);
		this.errorUniqueId = errorUniqueId;
	}

	private RocksDBException(RocksDBErrorType errorUniqueId, String message, Throwable ex) {
		super(message, ex);
		this.errorUniqueId = errorUniqueId;
	}

	private RocksDBException(RocksDBErrorType errorUniqueId, Throwable ex) {
		super(ex.toString(), ex);
		this.errorUniqueId = errorUniqueId;
	}

	private RocksDBException(RocksDBErrorType errorUniqueId, org.rocksdb.RocksDBException ex) {
		this(errorUniqueId, ex.getMessage());
	}

	private RocksDBException(RocksDBErrorType errorUniqueId, String message, org.rocksdb.RocksDBException ex) {
		this(errorUniqueId, message + ": " + ex.getMessage());
	}

	public RocksDBErrorType getErrorUniqueId() {
		return errorUniqueId;
	}

	@Override
	public String getLocalizedMessage() {
		return "RocksDBError: [uid:" + errorUniqueId + "] " + getMessage();
	}
}
