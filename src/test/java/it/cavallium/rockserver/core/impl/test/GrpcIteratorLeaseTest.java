package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

@Timeout(60)
class GrpcIteratorLeaseTest {

	private static final int ITERATOR_COUNT = 48;

	@TempDir
	Path tempDir;

	@Test
	void expiredAndFailedIteratorOperationsDoNotRetainServerLeases() throws Exception {
		try (var embedded = new EmbeddedConnection(tempDir.resolve("db"), "grpc-iterator-leases", null);
				var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-iterator-leases",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var api = client.getSyncApi();
				long columnId = api.createColumn("entries",
						ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
				List<Long> iteratorIds = new ArrayList<>(ITERATOR_COUNT);
				try {
					for (int i = 0; i < ITERATOR_COUNT; i++) {
						iteratorIds.add(api.openIterator(0, columnId, new Keys(), null, false, 25));
					}

					assertEquals(0, server.getActiveIteratorOperationLeaseCountForTesting(),
							"idle or abandoned iterators must not be retained as operation leases");

					Thread.sleep(50);
					for (long iteratorId : iteratorIds) {
						assertThrows(RocksDBException.class,
								() -> api.subsequent(iteratorId, 0, 1, RequestType.exists()));
						assertEquals(0, server.getActiveIteratorOperationLeaseCountForTesting(),
								"an error must release its transient operation lease");
					}

					for (int i = 0; i < ITERATOR_COUNT; i++) {
						long missingIteratorId = Long.MAX_VALUE - i;
						assertThrows(RocksDBException.class,
								() -> api.subsequent(missingIteratorId, 0, 1, RequestType.exists()));
					}
					assertEquals(0, server.getActiveIteratorOperationLeaseCountForTesting(),
							"failed operations on unknown iterators must not accumulate leases");
					assertEquals(0, embedded.getInternalDB().getOpenIteratorsCount());
					assertEquals(0L, embedded.getInternalDB().getPendingOpsCount());
				} finally {
					for (long iteratorId : iteratorIds) {
						api.closeIterator(iteratorId);
					}
				}
			}
		}
	}

	@Test
	void cancellationKeepsLeaseUntilRunningNativeIteratorCallReturns() throws Exception {
		var backend = new BlockingSubsequentConnection();
		try (backend; var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-running-iterator-lease",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var asyncApi = client.getAsyncApi();
				var syncApi = client.getSyncApi();
				long iteratorId = syncApi.openIterator(0, 0, new Keys(), null, false, 30_000);
				var running = asyncApi.subsequentAsync(iteratorId, 0, 1, RequestType.exists());

				try {
					assertTrue(backend.entered.await(5, TimeUnit.SECONDS));
					assertEquals(1, server.getActiveIteratorOperationLeaseCountForTesting());
					assertTrue(running.cancel(true));
					assertTrue(backend.cancelObserved.await(5, TimeUnit.SECONDS),
							"the blocking server task did not observe RPC cancellation");

					var concurrent = assertThrows(RocksDBException.class,
							() -> syncApi.subsequent(iteratorId, 0, 1, RequestType.exists()));
					assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
							concurrent.getErrorUniqueId());
					assertEquals(1, server.getActiveIteratorOperationLeaseCountForTesting(),
							"RPC cancellation must not release a lease while JNI is still running");
				} finally {
					backend.release.countDown();
				}

				assertTrue(backend.finished.await(5, TimeUnit.SECONDS));
				awaitLeaseCount(server, 0);
				assertTrue(syncApi.subsequent(iteratorId, 0, 1, RequestType.exists()),
						"the iterator must accept a new operation after the cancelled task really terminates");
				assertEquals(0, server.getActiveIteratorOperationLeaseCountForTesting());
			}
		}
	}

	@Test
	void rejectedLateOpenCleanupFallsBackInline() throws Exception {
		var backend = new BlockingOpenIteratorConnection();
		try (backend; var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			try (var client = GrpcConnection.forHostAndPort("grpc-rejected-open-cleanup",
					new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
				var open = client.getAsyncApi().openIteratorAsync(0, 0, new Keys(), null, false, 30_000);
				try {
					assertTrue(backend.entered.await(5, TimeUnit.SECONDS));
					assertTrue(open.cancel(true));
					assertTrue(backend.cancelObserved.await(5, TimeUnit.SECONDS),
							"the blocking open did not observe RPC cancellation");
					backend.scheduler.control().dispose();
				} finally {
					backend.release.countDown();
				}

				assertTrue(backend.closed.await(5, TimeUnit.SECONDS),
						"a rejected control-lane cleanup must close the late iterator inline");
				assertFalse(backend.iteratorOpen.get());
			}
		}
	}

	private static void awaitLeaseCount(GrpcServer server, int expected) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		do {
			if (server.getActiveIteratorOperationLeaseCountForTesting() == expected) {
				return;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadline);
		assertEquals(expected, server.getActiveIteratorOperationLeaseCountForTesting());
	}

	private static final class BlockingSubsequentConnection implements RocksDBConnection, InternalConnection {

		private static final long ITERATOR_ID = 17;

		private final RWScheduler scheduler = new RWScheduler(2, 1, "grpc-blocking-subsequent");
		private final CountDownLatch entered = new CountDownLatch(1);
		private final CountDownLatch cancelObserved = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private final CountDownLatch finished = new CountDownLatch(1);
		private final AtomicInteger subsequentCalls = new AtomicInteger();
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {};
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator) {
					return (RS) Long.valueOf(ITERATOR_ID);
				}
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator) {
					return null;
				}
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<?>) {
					if (subsequentCalls.getAndIncrement() == 0) {
						entered.countDown();
						try {
							awaitIgnoringInterrupts(release, cancelObserved);
						} finally {
							finished.countDown();
						}
					}
					return (RS) Boolean.TRUE;
				}
				throw new UnsupportedOperationException("Unexpected request: " + request);
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://grpc-blocking-subsequent");
		}

		@Override
		public RocksDBSyncAPI getSyncApi() {
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi() {
			return asyncApi;
		}

		@Override
		public RWScheduler getScheduler() {
			return scheduler;
		}

		@Override
		public void close() {
			release.countDown();
			scheduler.dispose();
		}
	}

	private static final class BlockingOpenIteratorConnection implements RocksDBConnection, InternalConnection {

		private static final long ITERATOR_ID = 23;

		private final RWScheduler scheduler = new RWScheduler(1, 1, "grpc-blocking-open");
		private final CountDownLatch entered = new CountDownLatch(1);
		private final CountDownLatch cancelObserved = new CountDownLatch(1);
		private final CountDownLatch release = new CountDownLatch(1);
		private final CountDownLatch closed = new CountDownLatch(1);
		private final AtomicBoolean iteratorOpen = new AtomicBoolean();
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {};
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
			@Override
			@SuppressWarnings("unchecked")
			public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator) {
					entered.countDown();
					awaitIgnoringInterrupts(release, cancelObserved);
					iteratorOpen.set(true);
					return (RS) Long.valueOf(ITERATOR_ID);
				}
				if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator) {
					iteratorOpen.set(false);
					closed.countDown();
					return null;
				}
				throw new UnsupportedOperationException("Unexpected request: " + request);
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://grpc-blocking-open");
		}

		@Override
		public RocksDBSyncAPI getSyncApi() {
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi() {
			return asyncApi;
		}

		@Override
		public RWScheduler getScheduler() {
			return scheduler;
		}

		@Override
		public void close() {
			release.countDown();
			scheduler.dispose();
		}
	}

	private static void awaitIgnoringInterrupts(CountDownLatch release, CountDownLatch interrupted) {
		while (true) {
			try {
				release.await();
				return;
			} catch (InterruptedException ignored) {
				interrupted.countDown();
			}
		}
	}
}
