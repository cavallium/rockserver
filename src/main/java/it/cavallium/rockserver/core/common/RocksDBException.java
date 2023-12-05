package it.cavallium.rockserver.core.common;

public class RocksDBException extends RuntimeException {

	private final RocksDBErrorType errorUniqueId;

	public enum RocksDBErrorType {
		PUT_UNKNOWN, PUT_2, UNEXPECTED_NULL_VALUE, PUT_1, PUT_3, GET_1, COLUMN_EXISTS, COLUMN_CREATE_FAIL, COLUMN_NOT_FOUND, COLUMN_DELETE_FAIL, CONFIG_ERROR, VALUE_MUST_BE_NULL

	}

	public RocksDBException(RocksDBErrorType errorUniqueId, String message) {
		super(message);
		this.errorUniqueId = errorUniqueId;
	}

	public RocksDBException(RocksDBErrorType errorUniqueId, String message, Throwable ex) {
		super(message, ex);
		this.errorUniqueId = errorUniqueId;
	}

	public RocksDBException(RocksDBErrorType errorUniqueId, Throwable ex) {
		super(ex.toString(), ex);
		this.errorUniqueId = errorUniqueId;
	}

	public RocksDBException(RocksDBErrorType errorUniqueId, org.rocksdb.RocksDBException ex) {
		this(errorUniqueId, ex.getMessage());
	}

	public RocksDBErrorType getErrorUniqueId() {
		return errorUniqueId;
	}

	@Override
	public String getLocalizedMessage() {
		return "RocksDBError: [uid:" + errorUniqueId + "] " + getMessage();
	}
}
