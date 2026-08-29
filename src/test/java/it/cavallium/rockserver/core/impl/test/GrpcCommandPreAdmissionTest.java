package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import com.google.protobuf.ByteString;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.proto.DeleteRangeRequest;
import it.cavallium.rockserver.core.common.api.proto.GetRangeRequest;
import it.cavallium.rockserver.core.common.api.proto.GetRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchMode;
import it.cavallium.rockserver.core.common.api.proto.PutBatchRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

@Timeout(30)
class GrpcCommandPreAdmissionTest {

	@Test
	void invalidUnaryRangeAndBatchCommandsNeverEnterTheSchedulerQueue() throws Exception {
		Path root = Files.createTempDirectory("rockserver-grpc-pre-admission");
		Path config = Files.createTempFile("rockserver-grpc-pre-admission", ".conf");
		Files.writeString(config, """
				database: {
				  parallelism: {
				    read: 3
				    write: 3
				    workload: { competing-batch-read-maximum-active: 3 }
				  }
				  global: { ingest-behind: false, optimistic: false }
				}
				""");
		EmbeddedConnection embedded = null;
		GrpcServer server = null;
		ManagedChannel channel = null;
		var blockers = new ArrayList<Disposable>();
		var release = new CountDownLatch(1);
		try {
			embedded = new EmbeddedConnection(root, "grpc-pre-admission", config);
			server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
			server.start();
			channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
					.usePlaintext()
					.build();

			var scheduler = embedded.getScheduler();
			var entered = new CountDownLatch(3);
			for (int worker = 0; worker < 3; worker++) {
				blockers.add(scheduler.scheduler(WorkloadProfile.LATENCY,
						OperationFamily.MUTATION,
						Long.MAX_VALUE).schedule(() -> {
					entered.countDown();
					try {
						release.await();
					} catch (InterruptedException interrupted) {
						Thread.currentThread().interrupt();
					}
				}));
			}
			assertTrue(entered.await(5, TimeUnit.SECONDS), "write workers did not become occupied");
			assertEquals(0, scheduler.queuedTasks(WorkloadProfile.LATENCY));

			var stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
			var context = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
					.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.LATENCY)
					.setDeadlineEpochMillis(System.currentTimeMillis() + Duration.ofSeconds(10).toMillis())
					.build();

			assertInvalid(stub.deleteRange(DeleteRangeRequest.newBuilder()
					.setColumnId(1)
					.setContext(context)
					.build()));
			assertEquals(0, scheduler.queuedTasks(WorkloadProfile.LATENCY),
					"invalid unary command entered the LATENCY queue");

			assertInvalid(stub.getAllInRange(GetRangeRequest.newBuilder()
					.setColumnId(1)
					.setTimeoutMs(1_000)
					.setContext(context)
					.build()).then());
			assertEquals(0, scheduler.queuedTasks(WorkloadProfile.LATENCY),
					"invalid server-streaming range entered the LATENCY queue");

			var initial = PutBatchRequest.newBuilder()
					.setInitialRequest(PutBatchInitialRequest.newBuilder()
							.setColumnId(1)
							.setMode(PutBatchMode.WRITE_BATCH)
							.setContext(context))
					.build();
			assertInvalid(stub.putBatch(Flux.just(initial)));
			assertEquals(0, scheduler.queuedTasks(WorkloadProfile.LATENCY),
					"invalid client-streaming batch entered the LATENCY queue");
		} finally {
			release.countDown();
			for (Disposable blocker : blockers) {
				blocker.dispose();
			}
			if (channel != null) {
				channel.shutdownNow();
				channel.awaitTermination(5, TimeUnit.SECONDS);
			}
			if (server != null) {
				server.close();
			}
			if (embedded != null) {
				embedded.closeTesting();
			}
			Utils.deleteDirectory(root.toString());
			Files.deleteIfExists(config);
		}
	}

	@Test
	void getForUpdateAndFastGetUseConcretePreAdmissionBeforeScheduling() throws Exception {
		Path root = Files.createTempDirectory("rockserver-grpc-get-pre-admission");
		EmbeddedConnection embedded = null;
		GrpcServer server = null;
		ManagedChannel channel = null;
		try {
			embedded = new EmbeddedConnection(root, "grpc-get-pre-admission", null);
			server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
			server.start();
			channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
					.usePlaintext()
					.build();
			var stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);

			var analytical = wireContext(
					it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.ANALYTICAL);
			assertInvalid(stub.getForUpdate(GetRequest.newBuilder()
					.setColumnId(1)
					.addKeys(ByteString.copyFrom(new byte[] {1}))
					.setContext(analytical)
					.build()));

			var latency = wireContext(
					it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.LATENCY);
			assertInvalid(stub.get(GetRequest.newBuilder()
					.setColumnId(1)
					.addKeys(ByteString.copyFrom(new byte[
							(int) it.cavallium.rockserver.core.impl.WorkloadAdmission
									.MAX_LATENCY_ENCODED_INPUT_BYTES + 1]))
					.setContext(latency)
					.build()));
		} finally {
			if (channel != null) {
				channel.shutdownNow();
				channel.awaitTermination(5, TimeUnit.SECONDS);
			}
			if (server != null) {
				server.close();
			}
			if (embedded != null) {
				embedded.closeTesting();
			}
			Utils.deleteDirectory(root.toString());
		}
	}

	@Test
	void grpcScheduledWrappersExposeEstimatedWorkToDeficitAccounting() throws Exception {
		var estimatedWork = it.cavallium.rockserver.core.impl.RWScheduler.EstimatedWork.class;
		var rejectionAware = it.cavallium.rockserver.core.impl.RWScheduler.RejectionAwareTask.class;
		assertTrue(estimatedWork.isAssignableFrom(Class.forName(
				"it.cavallium.rockserver.core.server.GrpcServer$FastGetCallHandler$FastGetListener")));
		assertTrue(estimatedWork.isAssignableFrom(Class.forName(
				"it.cavallium.rockserver.core.server.GrpcServer$GrpcServerImpl$ScheduledCall")));
		assertTrue(rejectionAware.isAssignableFrom(Class.forName(
				"it.cavallium.rockserver.core.server.GrpcServer$FastGetCallHandler$FastGetListener")));
		assertTrue(rejectionAware.isAssignableFrom(Class.forName(
				"it.cavallium.rockserver.core.server.GrpcServer$GrpcServerImpl$ScheduledCall")));
	}

	@Test
	void grpcScheduledCallArbitratesRunCancelAndRejectionExactlyOnce() throws Exception {
		Path root = Files.createTempDirectory("rockserver-grpc-scheduled-terminal");
		try (var embedded = new EmbeddedConnection(root, "grpc-scheduled-terminal", null);
				var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			Object grpc = field(GrpcServer.class, "grpc").get(server);
			Method execute = grpc.getClass().getDeclaredMethod(
					"executeScheduled", Callable.class, Scheduler.class, long.class);
			execute.setAccessible(true);

			var rejectedScheduler = new CapturingScheduler();
			var rejectedCalls = new AtomicInteger();
			var rejected = invokeScheduled(execute, grpc, rejectedScheduler,
					() -> rejectedCalls.incrementAndGet(), 8L);
			var firstFailure = new java.util.concurrent.RejectedExecutionException("deadline");
			var rejectedTask = (it.cavallium.rockserver.core.impl.RWScheduler.RejectionAwareTask)
					rejectedScheduler.task();
			rejectedTask.reject(firstFailure);
			rejectedTask.reject(new java.util.concurrent.RejectedExecutionException("duplicate"));
			var rejectedResult = assertThrows(ExecutionException.class,
					() -> rejected.get(5, TimeUnit.SECONDS));
			assertEquals(firstFailure, rejectedResult.getCause());
			assertEquals(0, rejectedCalls.get());

			var runningScheduler = new CapturingScheduler();
			var runningCalls = new AtomicInteger();
			var running = invokeScheduled(execute, grpc, runningScheduler,
					runningCalls::incrementAndGet, 16L);
			runningScheduler.task().run();
			((it.cavallium.rockserver.core.impl.RWScheduler.RejectionAwareTask) runningScheduler.task())
					.reject(new java.util.concurrent.RejectedExecutionException("late"));
			assertEquals(1, running.get(5, TimeUnit.SECONDS));
			assertEquals(1, runningCalls.get());

			var cancelledScheduler = new CapturingScheduler();
			var cancelledCalls = new AtomicInteger();
			var cancelled = invokeScheduled(execute, grpc, cancelledScheduler,
					cancelledCalls::incrementAndGet, 32L);
			assertTrue(cancelled.cancel(true));
			((it.cavallium.rockserver.core.impl.RWScheduler.RejectionAwareTask) cancelledScheduler.task())
					.reject(new java.util.concurrent.RejectedExecutionException("cancelled"));
			cancelledScheduler.task().run();
			assertTrue(cancelled.isCancelled());
			assertEquals(0, cancelledCalls.get());
		} finally {
			Utils.deleteDirectory(root.toString());
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> CompletableFuture<T> invokeScheduled(Method method,
			Object grpc,
			Scheduler scheduler,
			Callable<T> callable,
			long estimatedBytes) throws Exception {
		return ((Mono<T>) method.invoke(grpc, callable, scheduler, estimatedBytes)).toFuture();
	}

	private static Field field(Class<?> owner, String name) throws Exception {
		var field = owner.getDeclaredField(name);
		field.setAccessible(true);
		return field;
	}

	private static final class CapturingScheduler implements Scheduler {

		private final AtomicReference<Runnable> task = new AtomicReference<>();

		@Override
		public Disposable schedule(Runnable submitted) {
			if (!task.compareAndSet(null, submitted)) {
				throw new AssertionError("scheduler received more than one task");
			}
			return Disposables.disposed();
		}

		@Override
		public Worker createWorker() {
			throw new UnsupportedOperationException();
		}

		@Override
		public void dispose() {
		}

		@Override
		public boolean isDisposed() {
			return false;
		}

		private Runnable task() {
			var captured = task.get();
			org.junit.jupiter.api.Assertions.assertNotNull(captured);
			return captured;
		}
	}

	private static it.cavallium.rockserver.core.common.api.proto.RequestContext wireContext(
			it.cavallium.rockserver.core.common.api.proto.WorkloadProfile profile) {
		return it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
				.setProfile(profile)
				.setDeadlineEpochMillis(System.currentTimeMillis() + Duration.ofSeconds(10).toMillis())
				.build();
	}

	private static void assertInvalid(Mono<?> response) {
		var error = assertThrows(StatusRuntimeException.class,
				() -> response.block(Duration.ofSeconds(5)));
		assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
		assertTrue(error.getStatus().getDescription().contains("PUT_INVALID_REQUEST"),
				() -> "Unexpected error description: " + error.getStatus().getDescription());
	}
}
