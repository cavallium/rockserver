package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.ThriftConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionConfig;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionMethod;
import it.cavallium.rockserver.core.impl.test.DBTest.ConnectionType;
import it.cavallium.rockserver.core.server.ThriftServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Flux;

@Timeout(60)
class CdcLastCommittedSequenceTransportTest {

	@TempDir
	Path tempDir;

	static Stream<Arguments> transportImplementations() {
		return Stream.of(ConnectionMethod.GRPC, ConnectionMethod.THRIFT)
				.flatMap(method -> Stream.of(ConnectionType.SYNC, ConnectionType.ASYNC)
						.map(type -> new ConnectionConfig(type, method)))
				.map(config -> Arguments.arguments(config.getName(), config));
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("transportImplementations")
	void reportsMissingCreatedCommittedAndDeletedSubscription(
			String name,
			ConnectionConfig connection) throws Exception {
		Path configFile = tempDir.resolve("cdc-watermark-" + safeName(name) + ".conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		Path databasePath = tempDir.resolve("db-" + safeName(name));

		try (var embedded = new EmbeddedConnection(databasePath, "cdc-watermark", configFile);
				var testDB = new TestDB(embedded, connection)) {
			RocksDBAPI api = testDB.getAPI();
			String subscriptionId = "replica-watermark";

			assertEquals(embedded.cdcGetEarliestAvailableSequence(), readEarliest(api, connection.type()));
			assertEquals(OptionalLong.empty(), read(api, connection.type(), subscriptionId));

			create(api, connection.type(), subscriptionId, 1L, null, false);
			assertEquals(OptionalLong.of(0L), read(api, connection.type(), subscriptionId));

			commit(api, connection.type(), subscriptionId, 42L);
			assertEquals(OptionalLong.of(42L), read(api, connection.type(), subscriptionId));

			delete(api, connection.type(), subscriptionId);
			assertEquals(OptionalLong.empty(), read(api, connection.type(), subscriptionId));
		}
	}

	@ParameterizedTest(name = "{0}")
	@MethodSource("transportImplementations")
	void conditionalCreateIsAtomicAndExactRetriesAreIdempotent(
			String name,
			ConnectionConfig connection) throws Exception {
		Path configFile = tempDir.resolve("cdc-conditional-create-" + safeName(name) + ".conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		Path databasePath = tempDir.resolve("conditional-db-" + safeName(name));

		try (var embedded = new EmbeddedConnection(databasePath, "cdc-conditional-create", configFile);
				var testDB = new TestDB(embedded, connection)) {
			RocksDBAPI api = testDB.getAPI();
			String existingId = "existing";
			String absentId = "absent";

			assertEquals(100L, create(api, connection.type(), existingId, 100L, null, false));
			assertEquals(OptionalLong.of(99L), read(api, connection.type(), existingId));

			assertEquals(100L, api.cdcCreate(
					existingId, null, List.of(), true, OptionalLong.of(99L)));
			assertEquals(100L, api.cdcCreate(
					existingId, null, List.of(), true, OptionalLong.of(99L)),
					"retrying after a lost exact-check response must return the same start sequence");

			var wrongCheckpoint = assertThrows(RocksDBException.class,
					() -> api.cdcCreate(existingId, 1_000L, null, false, OptionalLong.of(98L)));
			assertEquals(RocksDBException.RocksDBErrorType.CDC_SUBSCRIPTION_CHANGED,
					wrongCheckpoint.getErrorUniqueId());
			var unexpectedlyExisting = assertThrows(RocksDBException.class,
					() -> api.cdcCreate(existingId, 1_000L, null, false, OptionalLong.empty()));
			assertEquals(RocksDBException.RocksDBErrorType.CDC_SUBSCRIPTION_CHANGED,
					unexpectedlyExisting.getErrorUniqueId());
			assertEquals(OptionalLong.of(99L), read(api, connection.type(), existingId),
					"failed preconditions must not mutate the existing checkpoint");

			var unexpectedlyAbsent = assertThrows(RocksDBException.class,
					() -> api.cdcCreate(absentId, 200L, null, false, OptionalLong.of(99L)));
			assertEquals(RocksDBException.RocksDBErrorType.CDC_SUBSCRIPTION_CHANGED,
					unexpectedlyAbsent.getErrorUniqueId());
			assertEquals(OptionalLong.empty(), read(api, connection.type(), absentId),
					"an exact-check mismatch must not create a missing subscription");

			assertEquals(200L, api.cdcCreate(absentId, 200L, null, false, OptionalLong.empty()));
			assertEquals(OptionalLong.of(199L), read(api, connection.type(), absentId));
			var secondAbsentCreate = assertThrows(RocksDBException.class,
					() -> api.cdcCreate(absentId, 300L, null, true, OptionalLong.empty()));
			assertEquals(RocksDBException.RocksDBErrorType.CDC_SUBSCRIPTION_CHANGED,
					secondAbsentCreate.getErrorUniqueId());
			assertEquals(OptionalLong.of(199L), read(api, connection.type(), absentId));
		}
	}

	@Test
	void thriftPollBatchPreservesFilteredEmptyCursorProgress() throws Exception {
		Path configFile = tempDir.resolve("cdc-thrift-poll.conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		int port = freePort();

		try (var embedded = new EmbeddedConnection(tempDir.resolve("cdc-thrift-poll-db"),
				"cdc-thrift-poll", configFile);
				var server = new ThriftServer(embedded, "127.0.0.1", port)) {
			server.start();
			try (var client = new ThriftConnection("cdc-thrift-poll-client", "127.0.0.1", port)) {
				var api = client.getSyncApi();
				var schema = ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true);
				long selectedColumn = api.createColumn("selected", schema);
				long ignoredColumn = api.createColumn("ignored", schema);
				long startSeq = api.cdcCreate("filtered", null, List.of(selectedColumn), false);

				api.put(0,
						ignoredColumn,
						new Keys(Buf.wrap(new byte[] {0, 0, 0, 1})),
						Buf.wrap(new byte[] {1}),
						RequestType.none());

				var batch = client.cdcPollBatchAsync("filtered", startSeq, 100)
						.block(Duration.ofSeconds(10));
				assertNotNull(batch);
				assertTrue(batch.events().isEmpty());
				assertTrue(batch.nextSeq() > startSeq,
						"filtered mutations must still advance the exact CDC cursor");

				var selectedKey = new Keys(Buf.wrap(new byte[] {0, 0, 0, 2}));
				var selectedValue = Buf.wrap(new byte[] {2});
				api.put(0, selectedColumn, selectedKey, selectedValue, RequestType.none());
				var selectedBatch = client.cdcPollBatchAsync("filtered", batch.nextSeq(), 100)
						.block(Duration.ofSeconds(10));
				assertNotNull(selectedBatch);
				assertEquals(1, selectedBatch.events().size());
				var event = selectedBatch.events().getFirst();
				assertEquals(selectedColumn, event.columnId());
				assertEquals(selectedValue, event.value());
				assertEquals(it.cavallium.rockserver.core.common.cdc.CDCEvent.Op.PUT, event.op());
			}
		}
	}

	@Test
	void missingSubscriptionErrorsRemainTypedOverThrift() throws Exception {
		Path configFile = tempDir.resolve("cdc-thrift-missing.conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		int port = freePort();

		try (var embedded = new EmbeddedConnection(tempDir.resolve("cdc-thrift-missing-db"),
				"cdc-thrift-missing", configFile);
				var server = new ThriftServer(embedded, "127.0.0.1", port)) {
			server.start();
			try (var client = new ThriftConnection("cdc-thrift-missing-client", "127.0.0.1", port)) {
				var commitError = assertThrows(RocksDBException.class,
						() -> client.getSyncApi().cdcCommit("missing", 1L));
				assertEquals(RocksDBException.RocksDBErrorType.CDC_SUBSCRIPTION_NOT_FOUND,
						commitError.getErrorUniqueId());

				var batchError = assertThrows(RocksDBException.class,
						() -> client.getAsyncApi().cdcPollBatchAsync("missing", null, 10).block());
				assertEquals(RocksDBException.RocksDBErrorType.CDC_SUBSCRIPTION_NOT_FOUND,
						batchError.getErrorUniqueId());

				var streamError = assertThrows(RocksDBException.class,
						() -> Flux.from(client.getAsyncApi().cdcPollAsync("missing", null, 10))
								.collectList()
								.block());
				assertEquals(RocksDBException.RocksDBErrorType.CDC_SUBSCRIPTION_NOT_FOUND,
						streamError.getErrorUniqueId());
			}
		}
	}

	@Test
	void thriftPollBatchTruncatesAtSequenceBoundariesWithoutLoss() throws Exception {
		Path configFile = tempDir.resolve("cdc-thrift-budget.conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		int port = freePort();
		int maxFrameSize = ThriftTransportLimits.MIN_MAX_FRAME_SIZE;
		int maxResponseSize = 256;

		try (var embedded = new EmbeddedConnection(tempDir.resolve("cdc-thrift-budget-db"),
				"cdc-thrift-budget", configFile);
				var server = new ThriftServer(embedded, "127.0.0.1", port, maxFrameSize)) {
			server.start();
			try (var client = new ThriftConnection("cdc-thrift-budget-client",
					"127.0.0.1",
					port,
					maxFrameSize,
					maxResponseSize)) {
				var schema = ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true);
				long columnId = embedded.createColumn("budgeted", schema);
				long startSeq = embedded.cdcCreate("budgeted", null, List.of(columnId), false);
				for (int i = 0; i < 3; i++) {
					embedded.put(0,
							columnId,
							new Keys(Buf.wrap(new byte[] {0, 0, 0, (byte) i})),
							Buf.wrap(new byte[128]),
							RequestType.none());
				}

				var expected = embedded.cdcPollBatchAsync("budgeted", startSeq, 100)
						.block(Duration.ofSeconds(10));
				assertNotNull(expected);
				assertEquals(3, expected.events().size());

				var received = new ArrayList<it.cavallium.rockserver.core.common.cdc.CDCEvent>();
				long cursor = startSeq;
				for (int poll = 0; poll < 10 && cursor != expected.nextSeq(); poll++) {
					var page = client.cdcPollBatchAsync("budgeted", cursor, 100)
							.block(Duration.ofSeconds(10));
					assertNotNull(page);
					assertEquals(1, page.events().size(), "the configured boundary should fit one event group");
					received.addAll(page.events());
					if (received.size() < expected.events().size()) {
						assertEquals(expected.events().get(received.size()).seq(), page.nextSeq(),
								"nextSeq must identify the first complete sequence group not returned");
					}
					assertTrue(page.nextSeq() > cursor, "a truncated response must advance its cursor");
					cursor = page.nextSeq();
				}

				assertEquals(expected.nextSeq(), cursor);
				assertEquals(expected.events(), received);
			}
		}
	}

	@Test
	void thriftPollBatchRejectsAnOversizedSequenceGroupWithTypedError() throws Exception {
		Path configFile = tempDir.resolve("cdc-thrift-oversized.conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		int port = freePort();
		int maxFrameSize = ThriftTransportLimits.MIN_MAX_FRAME_SIZE;

		try (var embedded = new EmbeddedConnection(tempDir.resolve("cdc-thrift-oversized-db"),
				"cdc-thrift-oversized", configFile);
				var server = new ThriftServer(embedded, "127.0.0.1", port, maxFrameSize)) {
			server.start();
			var schema = ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true);
			long columnId = embedded.createColumn("oversized", schema);
			long startSeq = embedded.cdcCreate("oversized", null, List.of(columnId), false);
			embedded.put(0,
					columnId,
					new Keys(Buf.wrap(new byte[] {0, 0, 0, 1})),
					Buf.wrap(new byte[512]),
					RequestType.none());

			try (var constrainedClient = new ThriftConnection("cdc-thrift-oversized-client",
					"127.0.0.1",
					port,
					maxFrameSize,
					256)) {
				var error = assertThrows(RocksDBException.class,
						() -> constrainedClient.cdcPollBatchAsync("oversized", startSeq, 100)
								.block(Duration.ofSeconds(10)));
				assertEquals(RocksDBException.RocksDBErrorType.CDC_RESPONSE_TOO_LARGE,
						error.getErrorUniqueId());
				assertTrue(error.getMessage().contains(
						ThriftTransportLimits.CLIENT_MAX_CDC_RESPONSE_SIZE_PROPERTY));
			}

			try (var largerClient = new ThriftConnection("cdc-thrift-recovery-client",
					"127.0.0.1",
					port,
					maxFrameSize,
					2048)) {
				var recovered = largerClient.cdcPollBatchAsync("oversized", startSeq, 100)
						.block(Duration.ofSeconds(10));
				assertNotNull(recovered);
				assertEquals(1, recovered.events().size(),
						"a larger advertised budget must resume from the unchanged cursor");
			}
		}
	}

	@Test
	void committedSequenceSurvivesDatabaseReopen() throws Exception {
		Path configFile = tempDir.resolve("cdc-watermark-reopen.conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		Path databasePath = tempDir.resolve("db-reopen");

		try (var embedded = new EmbeddedConnection(databasePath, "cdc-watermark", configFile)) {
			embedded.cdcCreate("replica-watermark", 1L, null);
			embedded.cdcCommit("replica-watermark", 42L);
		}

		try (var reopened = new EmbeddedConnection(databasePath, "cdc-watermark-reopened", configFile)) {
			assertEquals(OptionalLong.of(42L),
					reopened.cdcGetLastCommittedSequence("replica-watermark"));
		}
	}

	private static OptionalLong read(RocksDBAPI api, ConnectionType type, String subscriptionId) {
		return switch (type) {
			case SYNC -> api.cdcGetLastCommittedSequence(subscriptionId);
			case ASYNC -> api.cdcGetLastCommittedSequenceAsync(subscriptionId).join();
		};
	}

	private static long create(RocksDBAPI api,
			ConnectionType type,
			String subscriptionId,
			Long fromSeq,
			List<Long> columnIds,
			Boolean emitLatestValues) {
		return switch (type) {
			case SYNC -> api.cdcCreate(subscriptionId, fromSeq, columnIds, emitLatestValues);
			case ASYNC -> api.cdcCreateAsync(subscriptionId, fromSeq, columnIds, emitLatestValues).join();
		};
	}

	private static void commit(RocksDBAPI api,
			ConnectionType type,
			String subscriptionId,
			long sequence) {
		switch (type) {
			case SYNC -> api.cdcCommit(subscriptionId, sequence);
			case ASYNC -> api.cdcCommitAsync(subscriptionId, sequence).join();
		}
	}

	private static void delete(RocksDBAPI api, ConnectionType type, String subscriptionId) {
		switch (type) {
			case SYNC -> api.cdcDelete(subscriptionId);
			case ASYNC -> api.cdcDeleteAsync(subscriptionId).join();
		}
	}

	private static long readEarliest(RocksDBAPI api, ConnectionType type) {
		return switch (type) {
			case SYNC -> api.cdcGetEarliestAvailableSequence();
			case ASYNC -> api.cdcGetEarliestAvailableSequenceAsync().join();
		};
	}

	private static String safeName(String name) {
		return name.replaceAll("[^a-zA-Z0-9]", "-");
	}

	private static int freePort() throws Exception {
		try (var socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}
}
