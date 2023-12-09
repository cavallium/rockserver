package it.cavallium.rockserver.core.common;

public class RocksDBRetryException extends RocksDBException {

	public RocksDBRetryException() {
		super(RocksDBErrorType.UPDATE_RETRY, "Please, retry the transaction");
	}
}
