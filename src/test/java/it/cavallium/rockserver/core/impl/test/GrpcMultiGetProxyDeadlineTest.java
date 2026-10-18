package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.MultiGetPage;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.proto.GetMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.KeyTuple;
import it.cavallium.rockserver.core.common.api.proto.RangeBudget;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Mono;

@Timeout(30)
class GrpcMultiGetProxyDeadlineTest {

	private static final long TIMEOUT = TimeUnit.SECONDS.toNanos(1);

	@Test
	void proxyForwardsOnlyTimeRemainingAfterQueueAndCanBeReused() throws Exception {
		try (var fixture = new Fixture()) {
			var response = RocksDBServiceGrpc.newFutureStub(fixture.channel).getMulti(request());
			fixture.awaitQueued();
			fixture.clock.addAndGet(TimeUnit.MILLISECONDS.toNanos(700));
			fixture.release.countDown();

			var page = response.get(5, TimeUnit.SECONDS);
			assertEquals(ByteString.copyFrom(new byte[]{7}), page.getValues(0).getValue());
			assertEquals(TimeUnit.MILLISECONDS.toNanos(300), fixture.backend.timeout.get());
			assertEquals(TIMEOUT, fixture.backend.deadline.get());

			// The completed call must not leave its absolute deadline bound to the worker.
			assertEquals(page, RocksDBServiceGrpc.newBlockingStub(fixture.channel).getMulti(request()));
			assertEquals(TIMEOUT, fixture.backend.timeout.get());
			assertEquals(fixture.clock.get() + TIMEOUT, fixture.backend.deadline.get());
			assertEquals(2, fixture.backend.calls.get());
		}
	}

	@Test
	void queuedExpiredRequestNeverReachesDownstream() throws Exception {
		try (var fixture = new Fixture()) {
			var response = RocksDBServiceGrpc.newFutureStub(fixture.channel).getMulti(request());
			fixture.awaitQueued();
			fixture.clock.addAndGet(TIMEOUT + 1);
			fixture.release.countDown();

			var failure = assertThrows(ExecutionException.class, () -> response.get(5, TimeUnit.SECONDS));
			var status = assertInstanceOf(StatusRuntimeException.class, failure.getCause());
			assertEquals(Status.Code.DEADLINE_EXCEEDED, status.getStatus().getCode());
			assertEquals(0, fixture.backend.calls.get());
			assertEquals(1, RocksDBServiceGrpc.newBlockingStub(fixture.channel).getMulti(request()).getValuesCount());
			assertEquals(1, fixture.backend.calls.get());
		}
	}

	private static GetMultiRequest request() {
		return GetMultiRequest.newBuilder().setColumnId(1)
				.addKeysMulti(KeyTuple.newBuilder().addKeys(ByteString.copyFrom(new byte[]{1})))
				.setBudget(RangeBudget.newBuilder().setMaxItems(1).setMaxBytes(64))
				.setContext(it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
						.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
						.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
						.setTimeoutNanos(TIMEOUT)).build();
	}

	private static RWScheduler scheduler(AtomicLong clock) throws ReflectiveOperationException {
		var factory = RWScheduler.class.getDeclaredMethod("forTesting", int.class, int.class,
				int.class, int.class, int.class, String.class, java.util.function.LongSupplier.class);
		factory.setAccessible(true);
		return (RWScheduler) factory.invoke(null, 1, 1, 1, 8, 8,
				"multi-get-proxy-deadline", (java.util.function.LongSupplier) clock::get);
	}

	private static final class Fixture implements AutoCloseable {
		private final AtomicLong clock = new AtomicLong();
		private final CountDownLatch release = new CountDownLatch(1);
		private final RWScheduler scheduler = scheduler(clock);
		private final RecordingBackend backend = new RecordingBackend(scheduler);
		private final GrpcServer server;
		private final ManagedChannel channel;

		private Fixture() throws Exception {
			server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0));
			server.start();
			channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
			var entered = new CountDownLatch(1);
			scheduler.executor(WorkloadProfile.BATCH, OperationFamily.BOUNDED_FAN_OUT, Long.MAX_VALUE).execute(() -> {
				entered.countDown();
				try {
					if (!release.await(10, TimeUnit.SECONDS)) {
						throw new AssertionError("queue blocker was not released");
					}
				} catch (InterruptedException interrupted) {
					Thread.currentThread().interrupt();
					throw new AssertionError(interrupted);
				}
			});
			assertTrue(entered.await(5, TimeUnit.SECONDS));
		}

		private void awaitQueued() throws InterruptedException {
			long end = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
			while (scheduler.queuedTasks(WorkloadProfile.BATCH) == 0 && System.nanoTime() < end) {
				Thread.sleep(1);
			}
			assertEquals(1, scheduler.queuedTasks(WorkloadProfile.BATCH));
		}

		@Override
		public void close() throws Exception {
			release.countDown();
			try {
				channel.shutdownNow();
				assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
			} finally {
				try {
					server.close();
				} finally {
					scheduler.disposeNow();
				}
			}
		}
	}

	/** A downstream connection without an embedded fast path, using the proxy's clock. */
	private record RecordingBackend(RWScheduler scheduler, AtomicLong timeout, AtomicLong deadline,
			AtomicInteger calls) implements RocksDBConnection, InternalConnection {
		private RecordingBackend(RWScheduler scheduler) {
			this(scheduler, new AtomicLong(), new AtomicLong(), new AtomicInteger());
		}

		@Override
		public RWScheduler getScheduler() {
			return scheduler;
		}

		@Override
		public URI getUrl() {
			return URI.create("memory://multi-get-proxy");
		}

		@Override
		public RockserverCapabilities getCapabilities() {
			return RockserverCapabilities.CURRENT;
		}

		@Override
		public RocksDBSyncAPI getSyncApi(RequestContext context) {
			return new RocksDBSyncAPI() {
				@Override
				public MultiGetPage getMulti(long columnId, List<it.cavallium.rockserver.core.common.Keys> keys,
						it.cavallium.rockserver.core.common.RangeBudget budget) {
					calls.incrementAndGet();
					timeout.set(context.timeoutNanos());
					deadline.set(scheduler.resolveMonotonicDeadline(context));
					return new MultiGetPage(List.of(Buf.wrap(new byte[]{7})), false);
				}
			};
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(RequestContext context) {
			return (id, fromSeq, maxEvents) -> Mono.error(new UnsupportedOperationException("CDC is unused"));
		}

		@Override
		public void close() {
		}
	}
}
