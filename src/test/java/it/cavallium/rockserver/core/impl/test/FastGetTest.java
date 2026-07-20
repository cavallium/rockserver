package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
class FastGetTest {

	@TempDir
	Path tempDir;

	@Test
	void nativePinnedGetMatchesOrdinaryGet() throws Exception {
		System.setProperty("rockserver.core.print-config", "false");
		Path configFile = writeConfig("rockserver.conf", true);

		Path databasePath = tempDir.resolve("db");
		Keys nonEmptyKey = key(1);
		Keys emptyKey = key(2);
		Keys missingKey = key(3);
		Buf nonEmptyValue = value(42);
		Buf emptyValue = Buf.wrap(new byte[0]);

		try (var connection = new EmbeddedConnection(databasePath, "fast-get-load", configFile)) {
			var api = connection.getSyncApi();
			long columnId = api.createColumn("fast-get",
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			api.put(0, columnId, nonEmptyKey, nonEmptyValue, RequestType.none());
			api.put(0, columnId, emptyKey, emptyValue, RequestType.none());

			Buf retainedValue = api.get(0, columnId, nonEmptyKey, RequestType.current());
			assertEquals(nonEmptyValue, retainedValue);
			assertEquals(emptyValue, api.get(0, columnId, emptyKey, RequestType.current()));
			assertEquals(nonEmptyValue, retainedValue,
					"a returned Buf must remain valid after its pooled PinnedGet is reused");
			api.flush();
		}

		try (var connection = new EmbeddedConnection(databasePath, "fast-get-reopen", configFile)) {
			var api = connection.getSyncApi();
			long columnId = api.getColumnId("fast-get");
			assertEquals(nonEmptyValue, api.get(0, columnId, nonEmptyKey, RequestType.current()));
			assertEquals(emptyValue, api.get(0, columnId, emptyKey, RequestType.current()),
					"native pinned Get must distinguish a real zero-length value from a missing key");
			assertNull(api.get(0, columnId, missingKey, RequestType.current()));
		}

		Path ordinaryConfigFile = writeConfig("rockserver-ordinary.conf", false);
		try (var connection = new EmbeddedConnection(databasePath, "ordinary-get-reopen", ordinaryConfigFile)) {
			var api = connection.getSyncApi();
			long columnId = api.getColumnId("fast-get");
			assertEquals(nonEmptyValue, api.get(0, columnId, nonEmptyKey, RequestType.current()));
			assertEquals(emptyValue, api.get(0, columnId, emptyKey, RequestType.current()));
			assertNull(api.get(0, columnId, missingKey, RequestType.current()));
		}
	}

	@Test
	void nativePinnedStatePoolHandlesVirtualThreadChurn() throws Exception {
		System.setProperty("rockserver.core.print-config", "false");
		Path configFile = writeConfig("rockserver-concurrent.conf", true);
		Path databasePath = tempDir.resolve("concurrent-db");
		Keys key = key(11);
		Buf expected = value(99);
		try (var connection = new EmbeddedConnection(databasePath, "fast-get-concurrent", configFile)) {
			var api = connection.getSyncApi();
			long columnId = api.createColumn("fast-get-concurrent",
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			api.put(0, columnId, key, expected, RequestType.none());
			int concurrentReaders = 24;
			int readsPerReader = 64;
			var start = new CountDownLatch(1);
			try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
				var futures = new ArrayList<java.util.concurrent.Future<?>>(concurrentReaders);
				for (int reader = 0; reader < concurrentReaders; reader++) {
					futures.add(executor.submit(() -> {
						start.await();
						for (int read = 0; read < readsPerReader; read++) {
							assertEquals(expected, api.get(0, columnId, key, RequestType.current()));
						}
						return null;
					}));
				}
				start.countDown();
				for (var future : futures) {
					future.get();
				}
			}

			int churnReads = 256;
			try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
				for (int read = 0; read < churnReads; read++) {
					executor.submit(() -> assertEquals(expected,
							api.get(0, columnId, key, RequestType.current()))).get();
				}
			}
		}
	}

	private Path writeConfig(String fileName, boolean fastGet) throws Exception {
		Path configFile = tempDir.resolve(fileName);
		Files.writeString(configFile, """
				database: {
				  global: {
				    enable-fast-get: %s
				    ingest-behind: false
				    optimistic: false
				    use-direct-io: false
				    maximum-open-files: -1
				    fallback-column-options: {
				      cache-index-and-filter-blocks: true
				    }
				  }
				}
				""".formatted(fastGet));
		return configFile;
	}

	private static Keys key(long value) {
		byte[] backing = new byte[Long.BYTES + 4];
		ByteBuffer.wrap(backing, 2, Long.BYTES).putLong(value);
		return new Keys(Buf.wrap(backing, 2, 2 + Long.BYTES));
	}

	private static Buf value(long value) {
		return Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
	}
}
