package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.server.GrpcServer;

import java.lang.reflect.InvocationTargetException;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;

class GrpcServerLifecycleTest {

	@Test
	void closeDoesNotDisposeBorrowedInternalScheduler() throws Exception {
		var scheduler = new RWScheduler(
				Schedulers.newSingle("borrowed-grpc-read"),
				Schedulers.newSingle("borrowed-grpc-write")
		);
		try {
			try (var server = new GrpcServer(
					new InternalTestConnection(scheduler),
					new InetSocketAddress("127.0.0.1", 0)
			)) {
				server.start();
			}

			assertFalse(scheduler.read().isDisposed());
			assertFalse(scheduler.write().isDisposed());
		} finally {
			scheduler.dispose();
		}
	}

	@Test
	void closeDisposesOwnedScheduler() throws Exception {
		RWScheduler scheduler;
		try (var server = new GrpcServer(new TestConnection(), new InetSocketAddress("127.0.0.1", 0))) {
			scheduler = scheduler(server);
			server.start();
		}

		assertTrue(scheduler.read().isDisposed());
		assertTrue(scheduler.write().isDisposed());
	}

	@Test
	void constructorCleanupDisposesOwnedScheduler() throws Exception {
		var scheduler = new RWScheduler(1, 1, "grpc-constructor-cleanup");
		scheduler.read().schedule(() -> {
		});
		scheduler.write().schedule(() -> {
		});

		closeConstructorResources(new IllegalStateException("build failed"), scheduler, true);

		assertTrue(scheduler.read().isDisposed());
		assertTrue(scheduler.write().isDisposed());
		assertTrue(((ExecutorService) scheduler.readExecutor()).isTerminated());
		assertTrue(((ExecutorService) scheduler.writeExecutor()).isTerminated());
	}

	@Test
	void constructorCleanupDoesNotDisposeBorrowedScheduler() throws Exception {
		var scheduler = new RWScheduler(
				Schedulers.newSingle("borrowed-grpc-constructor-read"),
				Schedulers.newSingle("borrowed-grpc-constructor-write")
		);
		try {
			closeConstructorResources(new IllegalStateException("build failed"), scheduler, false);

			assertFalse(scheduler.read().isDisposed());
			assertFalse(scheduler.write().isDisposed());
		} finally {
			scheduler.dispose();
		}
	}

	@Test
	void closeGrpcServerReportsUnterminatedForcedShutdown() {
		var server = new StuckGrpcServer();

		var failure = assertThrows(IllegalStateException.class, () -> closeGrpcServer(server));

		assertEquals("GRPC server did not terminate after forced shutdown", failure.getMessage());
		assertEquals(1, server.shutdowns.get());
		assertEquals(1, server.shutdownNowCalls.get());
		assertEquals(2, server.awaitTerminationCalls.get());
	}

	@Test
	void closeGrpcServerRetriesAwaitAfterInterruptionAndRestoresInterrupt() throws Exception {
		var server = new InterruptingThenTerminatingGrpcServer();

		try {
			closeGrpcServer(server);

			assertTrue(Thread.interrupted());
			assertEquals(1, server.shutdowns.get());
			assertEquals(1, server.shutdownNowCalls.get());
			assertEquals(2, server.awaitTerminationCalls.get());
		} finally {
			clearInterruptedStatus();
		}
	}

	private static void closeConstructorResources(Throwable failure,
	                                              RWScheduler scheduler,
	                                              boolean ownsScheduler) throws Exception {
		var eventLoopGroup = Class.forName("io.netty.channel.EventLoopGroup");
		var method = GrpcServer.class.getDeclaredMethod("closeConstructorResources",
				Throwable.class,
				eventLoopGroup,
				RWScheduler.class,
				boolean.class);
		method.setAccessible(true);
		method.invoke(null, failure, null, scheduler, ownsScheduler);
	}

	private static void closeGrpcServer(io.grpc.Server server) throws Exception {
		var method = GrpcServer.class.getDeclaredMethod("closeGrpcServer", io.grpc.Server.class);
		method.setAccessible(true);
		try {
			method.invoke(null, server);
		} catch (InvocationTargetException e) {
			var cause = e.getCause();
			if (cause instanceof RuntimeException runtimeException) {
				throw runtimeException;
			}
			if (cause instanceof Error error) {
				throw error;
			}
			throw e;
		}
	}

	private static RWScheduler scheduler(GrpcServer server) throws Exception {
		var field = GrpcServer.class.getDeclaredField("scheduler");
		field.setAccessible(true);
		return (RWScheduler) field.get(server);
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	private static void clearInterruptedStatus() {
		Thread.interrupted();
	}

	private static class TestConnection implements RocksDBConnection {

		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
		};
		private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
		};

		@Override
		public URI getUrl() {
			return URI.create("memory://grpc-server-lifecycle-test");
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
		public void close() {
		}
	}

	private static final class InternalTestConnection extends TestConnection implements InternalConnection {

		private final RWScheduler scheduler;

		private InternalTestConnection(RWScheduler scheduler) {
			this.scheduler = scheduler;
		}

		@Override
		public RWScheduler getScheduler() {
			return scheduler;
		}
	}

	private abstract static class TestGrpcServer extends io.grpc.Server {

		protected final AtomicInteger shutdowns = new AtomicInteger();
		protected final AtomicInteger shutdownNowCalls = new AtomicInteger();
		protected final AtomicInteger awaitTerminationCalls = new AtomicInteger();
		private volatile boolean shutdown;

		@Override
		public io.grpc.Server start() {
			return this;
		}

		@Override
		public io.grpc.Server shutdown() {
			shutdowns.incrementAndGet();
			shutdown = true;
			return this;
		}

		@Override
		public io.grpc.Server shutdownNow() {
			shutdownNowCalls.incrementAndGet();
			shutdown = true;
			return this;
		}

		@Override
		public boolean isShutdown() {
			return shutdown;
		}

		@Override
		public void awaitTermination() {
			throw new UnsupportedOperationException("awaitTermination is not used by this test");
		}
	}

	private static final class StuckGrpcServer extends TestGrpcServer {

		@Override
		public boolean isTerminated() {
			return false;
		}

		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) {
			awaitTerminationCalls.incrementAndGet();
			return false;
		}
	}

	private static final class InterruptingThenTerminatingGrpcServer extends TestGrpcServer {

		@Override
		public boolean isTerminated() {
			return awaitTerminationCalls.get() > 1;
		}

		@Override
		public boolean awaitTermination(long timeout, TimeUnit unit) throws InterruptedException {
			if (awaitTerminationCalls.incrementAndGet() == 1) {
				throw new InterruptedException("await interrupted");
			}
			return true;
		}
	}
}
