package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;

import java.util.List;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

class EmbeddedConnectionAsyncErrorTest {

	@Test
	void deleteMultiAsyncPreservesRocksDBExceptionAsFutureCause() throws Exception {
		try (var connection = new EmbeddedConnection(null, "embedded-async-error", null)) {
			var response = connection.deleteMultiAsync(
					0L,
					42L,
					List.of(new Keys(new Buf[]{Buf.wrap(new byte[]{1})})),
					RequestType.none());

			var failure = assertThrows(CompletionException.class, response::join);
			var cause = assertInstanceOf(RocksDBException.class, failure.getCause());

			assertEquals(RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND, cause.getErrorUniqueId());
		}
	}
}
