package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class WorkloadResourceBindingTest {

	@TempDir
	Path tempDir;

	@Test
	void transactionsUpdatesAndIteratorsInheritTheirOpeningProfile() throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "profile-binding", null)) {
			var latency = connection.getSyncApi(RequestContext.latency(Duration.ofSeconds(30)));
			var batch = connection.getSyncApi(RequestContext.batch());
			long columnId = latency.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var key = new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(1).array()));
			latency.put(0, columnId, key, Buf.wrap(new byte[] {1}), RequestType.none());

			long transactionId = latency.openTransaction(30_000);
			assertDoesNotThrow(() -> latency.get(transactionId, columnId, key, RequestType.current()));
			assertThrows(RocksDBException.class,
					() -> batch.get(transactionId, columnId, key, RequestType.current()));

			long iteratorId = latency.openIterator(transactionId, columnId, null, null, false, 30_000);
			assertDoesNotThrow(() -> latency.seekTo(iteratorId, key));
			assertThrows(RocksDBException.class, () -> batch.seekTo(iteratorId, key));
			assertDoesNotThrow(() -> batch.closeIterator(iteratorId),
					"server-owned cleanup must not require changing the bound profile");
			assertDoesNotThrow(() -> latency.closeTransaction(transactionId, false));

			var update = latency.get(0, columnId, key, RequestType.forUpdate());
			assertThrows(RocksDBException.class,
					() -> batch.put(update.updateId(), columnId, key, Buf.wrap(new byte[] {2}), RequestType.none()));
			assertDoesNotThrow(() -> batch.closeFailedUpdate(update.updateId()),
					"failed-update cleanup is protected CONTROL work");
		}
	}
}
