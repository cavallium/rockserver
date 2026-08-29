package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.ManagedChannelBuilder;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.api.proto.CompactRequest;
import it.cavallium.rockserver.core.common.api.proto.FlushRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

@Timeout(30)
class GrpcPhysicalMaintenanceIdentityTest {

	private static final String DATABASE = "grpc-physical-identity";

	@Test
	void successfulGrpcFlushAndCompactHaveDistinctSchedulerIdentity() throws Exception {
		try (var backend = new MaintenanceBackend();
				var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
					.usePlaintext()
					.build();
			try {
				var stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
				stub.flush(FlushRequest.getDefaultInstance()).block(Duration.ofSeconds(5));
				stub.compact(CompactRequest.getDefaultInstance()).block(Duration.ofSeconds(5));

				assertEquals(1, backend.flushCalls.get());
				assertEquals(1, backend.compactCalls.get());
				assertEquals(1.0, counter(backend.registry,
						"rockserver.workload.admission", "flush", "result", "accepted"));
				assertEquals(1.0, counter(backend.registry,
						"rockserver.workload.admission", "compaction", "result", "accepted"));
				assertEquals(1.0, counter(backend.registry,
						"rockserver.workload.outcomes", "flush", "outcome", "run"));
				assertEquals(1.0, counter(backend.registry,
						"rockserver.workload.outcomes", "compaction", "outcome", "run"));
			} finally {
				channel.shutdownNow();
				assertTrue(channel.awaitTermination(5, SECONDS));
			}
		}
	}

	@Test
	void maintenanceDispatchCancellationAndRejectionKeepRequestedOperationIdentity() throws Exception {
		try (var backend = new MaintenanceBackend()) {
			var blockerStarted = new CountDownLatch(1);
			var releaseBlocker = new CountDownLatch(1);
			var queuedFlushesRan = new CountDownLatch(16);
			try {
				backend.scheduler.maintenance(OperationFamily.COMPACTION).schedule(() -> {
					blockerStarted.countDown();
					awaitUninterruptibly(releaseBlocker);
				});
				assertTrue(blockerStarted.await(5, SECONDS));

				var cancelledFlush = backend.scheduler.maintenance(OperationFamily.FLUSH).schedule(() -> {});
				assertEventually(() -> backend.scheduler.poolSnapshot(RWScheduler.Pool.PHYSICAL)
						.queuedTasks() == 1);
				cancelledFlush.dispose();
				assertEventually(() -> counter(backend.registry,
						"rockserver.workload.cancellations", "flush") == 1.0);
				assertEquals(0.0, counter(backend.registry,
						"rockserver.workload.cancellations", "compaction"));

				for (int index = 0; index < 16; index++) {
					backend.scheduler.maintenance(OperationFamily.FLUSH).schedule(queuedFlushesRan::countDown);
				}
				assertEventually(() -> backend.scheduler.poolSnapshot(RWScheduler.Pool.PHYSICAL)
						.queuedTasks() == 16);

				var overload = assertThrows(RocksDBException.class,
						() -> backend.scheduler.maintenanceExecutor(OperationFamily.COMPACTION)
								.execute(() -> {}));
				assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,
						overload.getErrorUniqueId());
				assertEquals(1.0, counter(backend.registry,
						"rockserver.workload.rejections", "compaction", "reason", "queue_full"));
				assertEquals(0.0, counter(backend.registry,
						"rockserver.workload.rejections", "flush", "reason", "queue_full"));

				releaseBlocker.countDown();
				assertTrue(queuedFlushesRan.await(5, SECONDS));
			} finally {
				releaseBlocker.countDown();
			}
		}
	}

	@Test
	void maintenanceFamilyApiRejectsNonPhysicalOperations() {
		try (var backend = new MaintenanceBackend()) {
			var schedulerFailure = assertThrows(RocksDBException.class,
					() -> backend.scheduler.maintenance(OperationFamily.MUTATION));
			assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
					schedulerFailure.getErrorUniqueId());
			var executorFailure = assertThrows(RocksDBException.class,
					() -> backend.scheduler.maintenanceExecutor(OperationFamily.RANGE_PAGE));
			assertEquals(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
					executorFailure.getErrorUniqueId());
		}
	}

	private static double counter(SimpleMeterRegistry registry,
			String name,
			String operation,
			String... extraTags) {
		var tags = new ArrayList<String>();
		tags.add("database");
		tags.add(DATABASE);
		tags.add("resource");
		tags.add("physical");
		tags.add("profile");
		tags.add("physical_maintenance");
		tags.add("operation");
		tags.add(operation);
		for (var tag : extraTags) {
			tags.add(tag);
		}
		return registry.get(name).tags(tags.toArray(String[]::new)).counter().count();
	}

	private static void assertEventually(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(10L);
		}
		assertTrue(condition.getAsBoolean());
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

	private static final class MaintenanceBackend implements RocksDBConnection, InternalConnection, AutoCloseable {

		private final SimpleMeterRegistry registry = new SimpleMeterRegistry();
		private final RWScheduler scheduler = RWScheduler.forTesting(
				1, 1, 1, 2, 2, "grpc-physical-identity", registry, DATABASE);
		private final AtomicInteger flushCalls = new AtomicInteger();
		private final AtomicInteger compactCalls = new AtomicInteger();
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
			@Override
			public void flush() {
				flushCalls.incrementAndGet();
			}

			@Override
			public void compact() {
				compactCalls.incrementAndGet();
			}
		};

		@Override
		public URI getUrl() {
			return URI.create("test://grpc-physical-identity");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(RequestContext context) {
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(RequestContext context) {
			return new RocksDBAsyncAPI() {};
		}

		@Override
		public RWScheduler getScheduler() {
			return scheduler;
		}

		@Override
		public void close() {
			scheduler.dispose();
			registry.close();
		}
	}
}
