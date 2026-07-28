package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.client.ThriftConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.cavallium.rockserver.core.server.ThriftServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;

class WorkloadResourceBindingTest {

	@TempDir
	Path tempDir;

	static Stream<Arguments> transportsAndApiModes() {
		return Stream.of(Transport.values())
				.flatMap(transport -> Stream.of(false, true)
						.map(async -> Arguments.of(transport, async)));
	}

	@ParameterizedTest(name = "{0}, async={1}")
	@MethodSource("transportsAndApiModes")
	void serverResourceStateIsAuthoritativeAcrossEveryTransport(Transport transport,
			boolean async) throws Exception {
		try (var fixture = openFixture(transport, "authority-" + transport + "-" + async)) {
			var latency = new Api(fixture.connection(),
					RequestContext.latency(Duration.ofSeconds(30)), async);
			var batch = new Api(fixture.connection(), RequestContext.batch(), async);
			long columnId = batch.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var key = key(1);
			latency.put(0, columnId, key, Buf.wrap(new byte[] {1}));

			long transactionId = latency.openTransaction(30_000);
			assertDoesNotThrow(() -> latency.get(transactionId, columnId, key),
					"the profile must be installed before the transaction id is observable");
			assertRocksFailure(() -> batch.get(transactionId, columnId, key));
			assertRocksFailure(() -> batch.existsMulti(transactionId, columnId, List.of(key)));
			assertRocksFailure(() -> batch.closeTransaction(transactionId, true));
			assertDoesNotThrow(() -> batch.closeTransaction(transactionId, false),
					"rollback is protected CONTROL cleanup");

			long iteratorTransaction = latency.openTransaction(30_000);
			long iteratorId = latency.openIterator(iteratorTransaction, columnId);
			assertDoesNotThrow(() -> latency.seekTo(iteratorId, key),
					"the profile must be installed before the iterator id is observable");
			assertRocksFailure(() -> batch.seekTo(iteratorId, key));
			assertRocksFailure(() -> batch.subsequent(iteratorId));
			assertDoesNotThrow(() -> batch.closeIterator(iteratorId),
					"iterator close is protected CONTROL cleanup");
			assertDoesNotThrow(() -> batch.closeTransaction(iteratorTransaction, false));

			UpdateContext<Buf> update = latency.forUpdate(columnId, key);
			assertRocksFailure(() -> batch.put(update.updateId(), columnId, key, Buf.wrap(new byte[] {2})));
			latency.put(update.updateId(), columnId, key, Buf.wrap(new byte[] {3}));
			UpdateContext<Buf> abandonedUpdate = latency.forUpdate(columnId, key);
			assertDoesNotThrow(() -> batch.closeFailedUpdate(abandonedUpdate.updateId()),
					"failed-update close is protected CONTROL cleanup");
			assertEquals(0, fixture.embedded().getInternalDB().getOpenTransactionsCount(),
					"self-committing updates must remove their authoritative profile with the resource");
			assertEquals(0, fixture.embedded().getInternalDB().getOpenIteratorsCount());
		}
	}

	@ParameterizedTest(name = "configured fan-out: {0}, async={1}")
	@MethodSource("transportsAndApiModes")
	void configuredLatencyFanOutLimitsAreServerAuthoritativeAcrossEveryTransport(Transport transport,
			boolean async) throws Exception {
		Path config = tempDir.resolve("fan-out-" + transport + "-" + async + ".conf");
		java.nio.file.Files.writeString(config, """
				database.parallelism.workload.latency-fan-out-max-items = 3
				database.parallelism.workload.latency-fan-out-max-bytes = 8B
				""");
		try (var fixture = openFixture(transport, "fan-out-" + transport + "-" + async, config)) {
			var latency = new Api(fixture.connection(),
					RequestContext.latency(Duration.ofSeconds(30)), async);
			var batch = new Api(fixture.connection(), RequestContext.batch(), async);
			long fixedColumn = batch.createColumn("fixed",
					ColumnSchema.of(IntList.of(1), ObjectList.of(), true));

			assertEquals(2, latency.existsMulti(0, fixedColumn, keysOfSizes(1, 1)).size());
			assertEquals(3, latency.existsMulti(0, fixedColumn, keysOfSizes(1, 1, 1)).size());
			assertRocksFailure(() -> latency.existsMulti(0, fixedColumn, keysOfSizes(1, 1, 1, 1)));

			long variableColumn = batch.createColumn("variable",
					ColumnSchema.of(IntList.of(), ObjectList.of(ColumnHashType.XXHASH32), true));
			assertEquals(1, latency.existsMulti(0, variableColumn, keysOfSizes(7)).size());
			assertEquals(1, latency.existsMulti(0, variableColumn, keysOfSizes(8)).size());
			assertRocksFailure(() -> latency.existsMulti(0, variableColumn, keysOfSizes(9)));
		}
	}

	@Test
	void cancellationBetweenExistsMultiChunksReleasesAllLogicalState() throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("exists-cancel"),
				"profile-exists-cancel",
				null)) {
			var sync = connection.getSyncApi(RequestContext.batch());
			var async = connection.getAsyncApi(RequestContext.batch());
			long columnId = sync.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			var keys = new ArrayList<Keys>(4_097);
			for (int i = 0; i < 4_097; i++) {
				keys.add(key(i));
			}

			var chunks = new AtomicInteger();
			var firstChunkFinished = new CountDownLatch(1);
			var releaseFirstChunk = new CountDownLatch(1);
			connection.getInternalDB().setExistsMultiChunkObserverForTesting(() -> {
				if (chunks.incrementAndGet() == 1) {
					firstChunkFinished.countDown();
					awaitUninterruptibly(releaseFirstChunk);
				}
			});
			try {
				var request = async.existsMultiAsync(0, columnId, keys, 30_000);
				assertTrue(firstChunkFinished.await(5, TimeUnit.SECONDS));
				assertFalse(request.cancel(true),
						"a running native chunk must finish cleanup before cancellation becomes terminal");
				releaseFirstChunk.countDown();
				assertThrows(CancellationException.class, () -> request.get(5, TimeUnit.SECONDS));
				assertEquals(1, chunks.get(), "no continuation chunk may run after cancellation");
				assertEquals(0, connection.getInternalDB().getPendingOpsCount(),
						"cancellation must release the retained snapshot, column use, and operation");
			} finally {
				releaseFirstChunk.countDown();
				connection.getInternalDB().setExistsMultiChunkObserverForTesting(null);
			}
		}
	}

	@Test
	void expiryAndShutdownRemoveTheProfileWithTheResource() throws Exception {
		var embedded = new EmbeddedConnection(tempDir.resolve("expiry"), "profile-expiry", null);
		try {
			var latency = embedded.getSyncApi(RequestContext.latency(Duration.ofSeconds(30)));
			var batch = embedded.getSyncApi(RequestContext.batch());
			long columnId = batch.createColumn("entries",
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			long transactionId = latency.openTransaction(1);
			long iteratorId = latency.openIterator(0, columnId, null, null, false, 1);
			Thread.sleep(20);

			var cleanupTransactions = fixtureMethod("cleanupExpiredTransactionsNow");
			var cleanupIterators = fixtureMethod("cleanupExpiredIteratorsNow");
			cleanupTransactions.invoke(embedded.getInternalDB());
			cleanupIterators.invoke(embedded.getInternalDB());

			assertEquals(0, embedded.getInternalDB().getOpenTransactionsCount());
			assertEquals(0, embedded.getInternalDB().getOpenIteratorsCount());
			assertRocksFailure(() -> latency.get(transactionId, columnId, key(1), RequestType.current()));
			assertRocksFailure(() -> latency.seekTo(iteratorId, key(1)));

			latency.openTransaction(30_000);
			latency.openIterator(0, columnId, null, null, false, 30_000);
		} finally {
			// closeTesting asserts that shutdown leaves no transactions, iterators, or
			// resource leases behind. Profiles are fields of those registrations, so
			// they cannot outlive the resource in a second registry.
			embedded.closeTesting();
		}
	}

	private Fixture openFixture(Transport transport, String name) throws Exception {
		return openFixture(transport, name, null);
	}

	private Fixture openFixture(Transport transport, String name, Path config) throws Exception {
		var embedded = new EmbeddedConnection(tempDir.resolve(name), name, config);
		try {
			return switch (transport) {
				case EMBEDDED -> new Fixture(embedded, embedded, null);
				case GRPC -> {
					int port = freePort();
					var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", port));
					server.start();
					var connection = GrpcConnection.forHostAndPort(name,
							new HostAndPort("127.0.0.1", port));
					yield new Fixture(embedded, connection, server);
				}
				case THRIFT -> {
					int port = freePort();
					var server = new ThriftServer(embedded, "127.0.0.1", port);
					server.start();
					var connection = new ThriftConnection(name, "127.0.0.1", port);
					yield new Fixture(embedded, connection, server);
				}
			};
		} catch (Throwable failure) {
			embedded.closeTesting();
			if (failure instanceof Exception exception) {
				throw exception;
			}
			if (failure instanceof Error error) {
				throw error;
			}
			throw new AssertionError(failure);
		}
	}

	private static java.lang.reflect.Method fixtureMethod(String name) throws Exception {
		var method = it.cavallium.rockserver.core.impl.EmbeddedDB.class.getDeclaredMethod(name);
		method.setAccessible(true);
		return method;
	}

	private static int freePort() throws Exception {
		try (var socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static List<Keys> keysOfSizes(int... sizes) {
		var result = new ArrayList<Keys>(sizes.length);
		for (int size : sizes) {
			result.add(new Keys(Buf.createZeroes(size)));
		}
		return List.copyOf(result);
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private static RocksDBException assertRocksFailure(ThrowingRunnable action) {
		var failure = assertThrows(Throwable.class, action::run);
		for (var current = failure; current != null; current = current.getCause()) {
			if (current instanceof RocksDBException rocksFailure) {
				return rocksFailure;
			}
			if (current.getCause() == current) {
				break;
			}
		}
		throw new AssertionError("Expected RocksDBException, got " + failure, failure);
	}

	private enum Transport {
		EMBEDDED,
		GRPC,
		THRIFT
	}

	@FunctionalInterface
	private interface ThrowingRunnable {
		void run() throws Exception;
	}

	private record Fixture(EmbeddedConnection embedded,
			RocksDBConnection connection,
			AutoCloseable server) implements AutoCloseable {

		@Override
		public void close() throws Exception {
			Throwable failure = null;
			if (connection != embedded) {
				try {
					connection.close();
				} catch (Throwable closeFailure) {
					failure = closeFailure;
				}
			}
			if (server != null) {
				try {
					server.close();
				} catch (Throwable closeFailure) {
					failure = append(failure, closeFailure);
				}
			}
			try {
				embedded.closeTesting();
			} catch (Throwable closeFailure) {
				failure = append(failure, closeFailure);
			}
			if (failure instanceof Exception exception) {
				throw exception;
			}
			if (failure instanceof Error error) {
				throw error;
			}
		}

		private static Throwable append(Throwable current, Throwable next) {
			if (current == null) {
				return next;
			}
			current.addSuppressed(next);
			return current;
		}
	}

	private record Api(RocksDBSyncAPI sync, RocksDBAsyncAPI async, boolean asyncMode) {

		private Api(RocksDBConnection connection, RequestContext context, boolean asyncMode) {
			this(connection.getSyncApi(context), connection.getAsyncApi(context), asyncMode);
		}

		long createColumn(String name, ColumnSchema schema) {
			return asyncMode ? async.createColumnAsync(name, schema).join() : sync.createColumn(name, schema);
		}

		long openTransaction(long timeoutMs) {
			return asyncMode ? async.openTransactionAsync(timeoutMs).join() : sync.openTransaction(timeoutMs);
		}

		boolean closeTransaction(long transactionId, boolean commit) {
			return asyncMode
					? async.closeTransactionAsync(transactionId, commit).join()
					: sync.closeTransaction(transactionId, commit);
		}

		Buf get(long transactionId, long columnId, Keys key) {
			return asyncMode
					? async.getAsync(transactionId, columnId, key, RequestType.current()).join()
					: sync.get(transactionId, columnId, key, RequestType.current());
		}

		UpdateContext<Buf> forUpdate(long columnId, Keys key) {
			return asyncMode
					? async.getAsync(0, columnId, key, RequestType.forUpdate()).join()
					: sync.get(0, columnId, key, RequestType.forUpdate());
		}

		void put(long transactionId, long columnId, Keys key, Buf value) {
			if (asyncMode) {
				async.putAsync(transactionId, columnId, key, value, RequestType.none()).join();
			} else {
				sync.put(transactionId, columnId, key, value, RequestType.none());
			}
		}

		List<Boolean> existsMulti(long transactionId, long columnId, List<Keys> keys) {
			return asyncMode
					? async.existsMultiAsync(transactionId, columnId, keys, 10_000).join()
					: sync.existsMulti(transactionId, columnId, keys, 10_000);
		}

		long openIterator(long transactionId, long columnId) {
			return asyncMode
					? async.openIteratorAsync(transactionId, columnId, null, null, false, 30_000).join()
					: sync.openIterator(transactionId, columnId, null, null, false, 30_000);
		}

		void closeIterator(long iteratorId) {
			if (asyncMode) {
				async.closeIteratorAsync(iteratorId).join();
			} else {
				sync.closeIterator(iteratorId);
			}
		}

		void closeFailedUpdate(long updateId) {
			if (asyncMode) {
				async.closeFailedUpdateAsync(updateId).join();
			} else {
				sync.closeFailedUpdate(updateId);
			}
		}

		void seekTo(long iteratorId, Keys key) {
			if (asyncMode) {
				async.seekToAsync(iteratorId, key).join();
			} else {
				sync.seekTo(iteratorId, key);
			}
		}

		boolean subsequent(long iteratorId) {
			return asyncMode
					? async.subsequentAsync(iteratorId, 0, 1, RequestType.exists()).join()
					: sync.subsequent(iteratorId, 0, 1, RequestType.exists());
		}
	}
}
