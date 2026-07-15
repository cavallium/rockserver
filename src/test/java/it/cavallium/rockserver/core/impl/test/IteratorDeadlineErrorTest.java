package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;

import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import org.junit.jupiter.api.Test;
import org.rocksdb.Status;
import org.rocksdb.Status.Code;
import org.rocksdb.Status.SubCode;

class IteratorDeadlineErrorTest {

	@Test
	void timedOutIteratorStatusHasStableDeadlineType() {
		var nativeError = nativeError(Code.TimedOut, "Deadline exceeded");

		var mapped = EmbeddedDB.mapIteratorStatusException(nativeError);

		assertEquals(RocksDBErrorType.READ_DEADLINE_EXCEEDED, mapped.getErrorUniqueId());
		assertSame(nativeError, mapped.getCause());
	}

	@Test
	void unrelatedIteratorErrorWithDeadlineTextRemainsGetError() {
		var nativeError = nativeError(Code.IOError, "Deadline exceeded");

		var mapped = EmbeddedDB.mapIteratorStatusException(nativeError);

		assertEquals(RocksDBErrorType.GET_1, mapped.getErrorUniqueId());
		assertSame(nativeError, mapped.getCause());
	}

	@Test
	void iteratorErrorWithoutStructuredStatusRemainsGetError() {
		var nativeError = new org.rocksdb.RocksDBException("Deadline exceeded");

		var mapped = EmbeddedDB.mapIteratorStatusException(nativeError);

		assertEquals(RocksDBErrorType.GET_1, mapped.getErrorUniqueId());
		assertSame(nativeError, mapped.getCause());
	}

	private static org.rocksdb.RocksDBException nativeError(Code code, String state) {
		return new org.rocksdb.RocksDBException(state, new Status(code, SubCode.None, state));
	}
}
