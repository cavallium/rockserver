package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.ServerCall;
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
import it.cavallium.rockserver.core.impl.ReactorResultOwnership;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Modifier;
import java.util.ArrayList;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
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

	@Test
	void grpcScheduledCallRunCancelRejectionCrossProductOwnsOneTerminalAndOneCleanup() throws Exception {
		Path root = Files.createTempDirectory("rockserver-grpc-scheduled-race");
		try (var embedded = new EmbeddedConnection(root, "grpc-scheduled-race", null);
				var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			Object grpc = field(GrpcServer.class, "grpc").get(server);
			Method execute = grpc.getClass().getDeclaredMethod(
					"executeScheduled",
					Callable.class,
					Scheduler.class,
					Consumer.class,
					Scheduler.class,
					long.class);
			execute.setAccessible(true);

			for (int repetition = 0; repetition < 64; repetition++) {
				int raceIndex = repetition;
				var scheduler = new CapturingScheduler();
				var calls = new AtomicInteger();
				var cleanups = new AtomicInteger();
				var successes = new AtomicInteger();
				var errors = new AtomicInteger();
				Mono<Integer> result = invokeScheduledWithCleanup(
						execute,
						grpc,
						scheduler,
						calls::incrementAndGet,
						ignored -> cleanups.incrementAndGet(),
						16L);
				Disposable subscription = result.subscribe(
						ignored -> successes.incrementAndGet(),
						ignored -> errors.incrementAndGet());
				Runnable task = scheduler.task();
				var rejectionAware = (it.cavallium.rockserver.core.impl.RWScheduler.RejectionAwareTask) task;
				var raceStart = new CountDownLatch(1);
				var run = CompletableFuture.runAsync(() -> {
					awaitUninterruptibly(raceStart);
					task.run();
				});
				var cancel = CompletableFuture.runAsync(() -> {
					awaitUninterruptibly(raceStart);
					subscription.dispose();
				});
				var reject = CompletableFuture.runAsync(() -> {
					awaitUninterruptibly(raceStart);
					rejectionAware.reject(new java.util.concurrent.RejectedExecutionException(
							"synthetic terminal race " + raceIndex));
				});
				raceStart.countDown();
				run.get(5, TimeUnit.SECONDS);
				cancel.get(5, TimeUnit.SECONDS);
				reject.get(5, TimeUnit.SECONDS);

				// Late/duplicate contenders must remain inert after the first state transition.
				task.run();
				rejectionAware.reject(new java.util.concurrent.RejectedExecutionException("late duplicate"));
				subscription.dispose();
				assertTrue(booleanField(task, "terminated"), "task did not reach a terminal state");
				assertFalse(booleanField(task, "running"), "task retained running ownership");
				boolean cancellationWon = booleanField(task, "cancelled");
				assertTrue(calls.get() <= 1, "callable ran more than once");
				assertTrue(successes.get() + errors.get() <= 1, "subscriber saw duplicate terminal signals");
				assertTrue(cleanups.get() <= 1, "result cleanup ran more than once");
				if (calls.get() == 1 && cancellationWon) {
					assertEquals(1, cleanups.get(), "cancellation-owned result was not cleaned");
				}
				assertTrue(field(task.getClass(), "pendingResult").get(task) == null,
						"terminal hooks retained a produced result");
			}
		} finally {
			Utils.deleteDirectory(root.toString());
		}
	}

	@Test
	void grpcScheduledResultOwnershipCoversCancellationSuccessNullableAndCleanupFailure() throws Exception {
		Path root = Files.createTempDirectory("rockserver-grpc-result-ownership");
		try (var embedded = new EmbeddedConnection(root, "grpc-result-ownership", null);
				var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			Object grpc = field(GrpcServer.class, "grpc").get(server);
			Method execute = grpc.getClass().getDeclaredMethod(
					"executeScheduled", Callable.class, Scheduler.class, Consumer.class, Scheduler.class, long.class);
			execute.setAccessible(true);
			String lateErrorKey = (String) field(GrpcServer.class,
					"GRPC_LATE_ERROR_HANDLER_CONTEXT_KEY").get(null);
			assertTrue(java.util.Arrays.stream(ReactorResultOwnership.class.getDeclaredFields())
					.allMatch(candidate -> Modifier.isStatic(candidate.getModifiers())));

			var cancelScheduler = new CapturingScheduler();
			var callableEntered = new CountDownLatch(1);
			var releaseCallable = new CountDownLatch(1);
			var cleanupCalls = new AtomicInteger();
			var cleanupFailure = new AtomicReference<Throwable>();
			Mono<Integer> cancelledResult = invokeScheduledWithCleanup(
					execute,
					grpc,
					cancelScheduler,
					() -> {
						callableEntered.countDown();
						awaitUninterruptibly(releaseCallable);
						return 7;
					},
					ignored -> {
						cleanupCalls.incrementAndGet();
						throw new IllegalStateException("synthetic late cleanup failure");
					},
					16L).contextWrite(context -> context.put(
						lateErrorKey, (Consumer<Throwable>) cleanupFailure::set));
			Disposable cancelledSubscription = cancelledResult.subscribe();
			Runnable cancelledTask = cancelScheduler.task();
			assertIntrusiveOwnershipState(cancelledTask);
			var running = CompletableFuture.runAsync(cancelledTask);
			assertTrue(callableEntered.await(5, TimeUnit.SECONDS));
			cancelledSubscription.dispose();
			assertTrue(booleanField(cancelledTask, "cancelled"));
			releaseCallable.countDown();
			running.get(5, TimeUnit.SECONDS);
			((it.cavallium.rockserver.core.impl.RWScheduler.RejectionAwareTask) cancelledTask)
					.reject(new java.util.concurrent.RejectedExecutionException("late duplicate"));
			cancelledSubscription.dispose();
			assertEquals(1, cleanupCalls.get());
			assertEquals("synthetic late cleanup failure", cleanupFailure.get().getMessage());
			assertTrue(field(cancelledTask.getClass(), "pendingResult").get(cancelledTask) == null);

			var successScheduler = new CapturingScheduler();
			var successes = new AtomicInteger();
			var successCleanups = new AtomicInteger();
			Disposable successSubscription = invokeScheduledWithCleanup(
					execute, grpc, successScheduler, () -> 11,
					ignored -> successCleanups.incrementAndGet(), 16L)
					.subscribe(ignored -> successes.incrementAndGet());
			Runnable successTask = successScheduler.task();
			successTask.run();
			assertEquals(1, successes.get());
			assertEquals(0, successCleanups.get());
			assertFalse(booleanField(successTask, "cancelled"));
			assertTrue(field(successTask.getClass(), "pendingResult").get(successTask) == null);
			successSubscription.dispose();

			var nullableScheduler = new CapturingScheduler();
			var nullableEntered = new CountDownLatch(1);
			var releaseNullable = new CountDownLatch(1);
			Disposable nullableSubscription = invokeScheduledWithCleanup(
					execute, grpc, nullableScheduler, () -> {
						nullableEntered.countDown();
						awaitUninterruptibly(releaseNullable);
						return 13;
					}, null, 16L).subscribe();
			Runnable nullableTask = nullableScheduler.task();
			var nullableRun = CompletableFuture.runAsync(nullableTask);
			assertTrue(nullableEntered.await(5, TimeUnit.SECONDS));
			nullableSubscription.dispose();
			releaseNullable.countDown();
			nullableRun.get(5, TimeUnit.SECONDS);
			assertTrue(field(nullableTask.getClass(), "pendingResult").get(nullableTask) == null);
		} finally {
			Utils.deleteDirectory(root.toString());
		}
	}

	@Test
	@SuppressWarnings({"rawtypes", "unchecked"})
	void fastGetQueuedCancelDeadlineDispatchRaceOwnsOneTerminalOutsideThePoolLock() throws Exception {
		Path root = Files.createTempDirectory("rockserver-fast-get-terminal-race");
		Path config = Files.createTempFile("rockserver-fast-get-terminal-race", ".conf");
		Files.writeString(config, """
				database: {
				  parallelism: {
				    read: 3
				    write: 3
				    workload: { competing-batch-read-maximum-active: 3 }
				  }
				  global: { enable-fast-get: true, ingest-behind: false, optimistic: false }
				}
				""");
		try (var embedded = new EmbeddedConnection(root, "fast-get-terminal-race", config);
				var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
			var scheduler = embedded.getScheduler();
			Class<?> handlerType = Class.forName(
					"it.cavallium.rockserver.core.server.GrpcServer$FastGetCallHandler");
			var constructor = handlerType.getDeclaredConstructor(GrpcServer.class);
			constructor.setAccessible(true);
			Object handler = constructor.newInstance(server);
			Method startCall = handlerType.getDeclaredMethod("startCall", ServerCall.class, Metadata.class);
			startCall.setAccessible(true);

			for (int repetition = 0; repetition < 24; repetition++) {
				boolean forceDeadlineWinner = repetition % 4 == 0;
				var blockersStarted = new CountDownLatch(3);
				var releaseBlockers = new CountDownLatch(1);
				for (int worker = 0; worker < 3; worker++) {
					scheduler.executor(WorkloadProfile.BATCH,
							OperationFamily.POINT_LOOKUP,
							it.cavallium.rockserver.core.common.RequestContext.NO_DEADLINE).execute(() -> {
						blockersStarted.countDown();
						awaitUninterruptibly(releaseBlockers);
					});
				}
				assertTrue(blockersStarted.await(5, TimeUnit.SECONDS));
				var before = scheduler.poolSnapshot(it.cavallium.rockserver.core.impl.RWScheduler.Pool.READ);
				long deadline = System.currentTimeMillis() + 100L;
				var call = new RecordingServerCall(scheduler);
				ServerCall.Listener<GetRequest> listener = (ServerCall.Listener<GetRequest>) startCall.invoke(
						handler, call, new Metadata());
				listener.onMessage(GetRequest.newBuilder()
						.setColumnId(Long.MAX_VALUE)
						.addKeys(ByteString.copyFrom(new byte[] {1}))
						.setContext(it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
								.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
								.setDeadlineEpochMillis(deadline))
						.build());
				listener.onHalfClose();
				assertTrue(field(listener.getClass(), "task").get(listener) instanceof Disposable);
				assertEventually(() -> scheduler.queuedTasks(WorkloadProfile.BATCH) == 1);

				var raceStart = new CountDownLatch(1);
				var cancel = CompletableFuture.runAsync(() -> {
					awaitUninterruptibly(raceStart);
					while (System.currentTimeMillis() < deadline) Thread.onSpinWait();
					if (forceDeadlineWinner) {
						long closeDeadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
						while (call.closeCount.get() == 0 && System.nanoTime() < closeDeadline) {
							Thread.onSpinWait();
						}
					}
					call.cancel();
					listener.onCancel();
				});
				var dispatchOrDeadline = CompletableFuture.runAsync(() -> {
					awaitUninterruptibly(raceStart);
					while (System.currentTimeMillis() < deadline) Thread.onSpinWait();
					releaseBlockers.countDown();
				});
				raceStart.countDown();
				cancel.get(5, TimeUnit.SECONDS);
				dispatchOrDeadline.get(5, TimeUnit.SECONDS);
				assertEventually(() -> scheduler.poolSnapshot(
						it.cavallium.rockserver.core.impl.RWScheduler.Pool.READ).drainedAndConserved());

				listener.onCancel();
				listener.onHalfClose();
				assertTrue(call.closeCount.get() <= 1, "Fast Get closed the call twice");
				if (forceDeadlineWinner) {
					assertEquals(1, call.closeCount.get(), "forced deadline did not reach Fast Get rejection");
				}
				assertTrue(call.callbackFailure.get() == null,
						() -> "Fast Get terminal callback held the scheduler lock: " + call.callbackFailure.get());
				var after = scheduler.poolSnapshot(it.cavallium.rockserver.core.impl.RWScheduler.Pool.READ);
				assertEquals(1L, after.submissionAttempts() - before.submissionAttempts());
				assertEquals(4L, after.terminalOutcomes() - before.terminalOutcomes());
				long targetRun = after.outcomes().get(
						it.cavallium.rockserver.core.impl.RWScheduler.TerminalOutcome.RUN)
						- before.outcomes().get(it.cavallium.rockserver.core.impl.RWScheduler.TerminalOutcome.RUN)
						- 3L;
				long selected = targetRun
						+ after.outcomes().get(it.cavallium.rockserver.core.impl.RWScheduler.TerminalOutcome.DEADLINE)
						- before.outcomes().get(it.cavallium.rockserver.core.impl.RWScheduler.TerminalOutcome.DEADLINE)
						+ after.outcomes().get(it.cavallium.rockserver.core.impl.RWScheduler.TerminalOutcome.CANCELLATION)
						- before.outcomes().get(it.cavallium.rockserver.core.impl.RWScheduler.TerminalOutcome.CANCELLATION);
				assertEquals(1L, selected, "Fast Get did not select exactly one scheduler terminal cause");
			}
		} finally {
			Utils.deleteDirectory(root.toString());
			Files.deleteIfExists(config);
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

	@SuppressWarnings("unchecked")
	private static <T> Mono<T> invokeScheduledWithCleanup(Method method,
			Object grpc,
			Scheduler scheduler,
			Callable<T> callable,
			Consumer<T> cleanup,
			long estimatedBytes) throws Exception {
		return (Mono<T>) method.invoke(
				grpc, callable, scheduler, cleanup, reactor.core.scheduler.Schedulers.immediate(), estimatedBytes);
	}

	private static boolean booleanField(Object owner, String name) throws Exception {
		var field = field(owner.getClass(), name);
		return field.getBoolean(owner);
	}

	private static void assertIntrusiveOwnershipState(Object task) {
		assertEquals(0L, java.util.Arrays.stream(task.getClass().getDeclaredFields())
				.filter(candidate -> candidate.getType() == ReactorResultOwnership.class
						|| candidate.getType() == java.util.concurrent.atomic.AtomicBoolean.class
						|| candidate.getType() == java.util.concurrent.atomic.AtomicReference.class)
				.count(), "gRPC result ownership allocated a per-request helper");
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

	private static final class RecordingServerCall extends ServerCall<GetRequest, Object> {

		private final it.cavallium.rockserver.core.impl.RWScheduler scheduler;
		private final AtomicInteger closeCount = new AtomicInteger();
		private final AtomicReference<Throwable> callbackFailure = new AtomicReference<>();
		private volatile boolean cancelled;

		private RecordingServerCall(it.cavallium.rockserver.core.impl.RWScheduler scheduler) {
			this.scheduler = scheduler;
		}

		private void cancel() {
			cancelled = true;
		}

		@Override
		public void request(int numMessages) {
			if (numMessages <= 0) throw new AssertionError("invalid requested message count");
		}

		@Override
		public void sendHeaders(Metadata headers) {
		}

		@Override
		public void sendMessage(Object message) {
		}

		@Override
		public void close(Status status, Metadata trailers) {
			closeCount.incrementAndGet();
			try {
				CompletableFuture.runAsync(() -> scheduler.poolSnapshot(
						it.cavallium.rockserver.core.impl.RWScheduler.Pool.READ)).get(2, TimeUnit.SECONDS);
			} catch (Throwable failure) {
				callbackFailure.set(failure);
			}
		}

		@Override
		public boolean isCancelled() {
			return cancelled;
		}

		@Override
		@SuppressWarnings({"unchecked", "rawtypes"})
		public MethodDescriptor<GetRequest, Object> getMethodDescriptor() {
			return (MethodDescriptor) it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc
					.getGetMethod();
		}
	}

	private static void assertEventually(java.util.function.BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(5L);
		}
		assertTrue(condition.getAsBoolean(), "condition did not become true before timeout");
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
