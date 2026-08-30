package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;

@Timeout(30)
class EmbeddedTrackedSchedulingCostTest {

	@Test
	void trackedNativeTaskPublishesItsConservativeByteCostWithoutAnExtraWrapper(@TempDir Path tempDir)
			throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "tracked-native-cost", null)) {
			var observedBytes = new AtomicLong(-1L);
			Scheduler probe = new ImmediateCostProbeScheduler(observedBytes);
			long expectedBytes = 7L * 1024L * 1024L;

			String result = invokeTracked(connection.getInternalDB(), probe, () -> "done", expectedBytes)
					.block();

			assertEquals("done", result);
			assertEquals(expectedBytes, observedBytes.get());
			assertEquals(0L, connection.getInternalDB().getPendingOpsCount(),
					"the cost-aware task must retain the existing exactly-once shutdown accounting");
		}
	}

	@Test
	void asynchronousQueuedRejectionPublishesOneErrorAndReleasesTheShutdownLease(@TempDir Path tempDir)
			throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("db-rejection"),
				"tracked-native-rejection", null)) {
			var scheduler = new AsynchronousRejectScheduler();
			var callableRan = new AtomicBoolean();
			var result = invokeTracked(connection.getInternalDB(), scheduler, () -> {
				callableRan.set(true);
				return "unexpected";
			}, 1024L).toFuture();
			var rejection = new RejectedExecutionException("synthetic queued shutdown rejection");
			try {
				if (!scheduler.submitted.await(5, TimeUnit.SECONDS)) {
					throw new AssertionError("tracked task was not submitted");
				}
				CompletableFuture.runAsync(() -> scheduler.reject(rejection)).get(5, TimeUnit.SECONDS);

				var completion = assertThrows(ExecutionException.class,
						() -> result.get(5, TimeUnit.SECONDS));
				assertSame(rejection, completion.getCause());
				assertEquals(false, callableRan.get());
				assertEquals(0L, connection.getInternalDB().getPendingOpsCount(),
						"queued scheduler rejection must release the SafeShutdown lease exactly once");

				scheduler.reject(new RejectedExecutionException("duplicate late rejection"));
				assertSame(rejection, assertThrows(ExecutionException.class, result::get).getCause());
				assertEquals(0L, connection.getInternalDB().getPendingOpsCount());
			} finally {
				result.cancel(true);
			}
		}
	}

	@Test
	void trackedNativeRunCancelRejectionCrossProductReleasesOneLeaseAndOneResult(@TempDir Path tempDir)
			throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("db-terminal-race"),
				"tracked-native-terminal-race", null)) {
			for (int repetition = 0; repetition < 64; repetition++) {
				var scheduler = new CapturingScheduler();
				var calls = new AtomicLong();
				var cleanups = new AtomicLong();
				var successes = new AtomicLong();
				var errors = new AtomicLong();
				Mono<String> result = invokeTracked(
						connection.getInternalDB(),
						scheduler,
						() -> {
							calls.incrementAndGet();
							return "value";
						},
						ignored -> cleanups.incrementAndGet(),
						1024L);
				Disposable subscription = result.subscribe(
						ignored -> successes.incrementAndGet(),
						ignored -> errors.incrementAndGet());
				Runnable task = scheduler.task();
				var rejectionAware = (RWScheduler.RejectionAwareTask) task;
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
					rejectionAware.reject(new RejectedExecutionException("synthetic tracked race"));
				});
				raceStart.countDown();
				run.get(5, TimeUnit.SECONDS);
				cancel.get(5, TimeUnit.SECONDS);
				reject.get(5, TimeUnit.SECONDS);

				// Duplicate late transitions must neither re-run native work nor release accounting twice.
				task.run();
				rejectionAware.reject(new RejectedExecutionException("late duplicate"));
				subscription.dispose();
				assertEquals(0L, connection.getInternalDB().getPendingOpsCount());
				assertEventually(() -> calls.get() == successes.get() + cleanups.get());
				assertEquals(calls.get(), successes.get() + cleanups.get(),
						"each native result must be delivered or cleaned exactly once");
				org.junit.jupiter.api.Assertions.assertTrue(calls.get() <= 1L, "native callable ran twice");
				org.junit.jupiter.api.Assertions.assertTrue(successes.get() + errors.get() <= 1L,
						"subscriber saw duplicate terminal signals");
			}
		}
	}

	@SuppressWarnings("unchecked")
	private static <T> Mono<T> invokeTracked(EmbeddedDB database,
			Scheduler scheduler,
			Callable<T> callable,
			long estimatedBytes) throws Exception {
		Method method = EmbeddedDB.class.getDeclaredMethod(
				"scheduleTracked",
				Scheduler.class,
				Callable.class,
				Consumer.class,
				long.class);
		method.setAccessible(true);
		return (Mono<T>) method.invoke(database, scheduler, callable, null, estimatedBytes);
	}

	@SuppressWarnings("unchecked")
	private static <T> Mono<T> invokeTracked(EmbeddedDB database,
			Scheduler scheduler,
			Callable<T> callable,
			Consumer<T> cleanup,
			long estimatedBytes) throws Exception {
		Method method = EmbeddedDB.class.getDeclaredMethod(
				"scheduleTracked",
				Scheduler.class,
				Callable.class,
				Consumer.class,
				long.class);
		method.setAccessible(true);
		return (Mono<T>) method.invoke(database, scheduler, callable, cleanup, estimatedBytes);
	}

	private static final class ImmediateCostProbeScheduler implements Scheduler {

		private final AtomicLong observedBytes;
		private boolean disposed;

		private ImmediateCostProbeScheduler(AtomicLong observedBytes) {
			this.observedBytes = observedBytes;
		}

		@Override
		public Disposable schedule(Runnable task) {
			observedBytes.set(((RWScheduler.EstimatedWork) task).estimatedBytes());
			task.run();
			return Disposables.disposed();
		}

		@Override
		public Worker createWorker() {
			throw new AssertionError("tracked scheduling must use the direct scheduler path");
		}

		@Override
		public void dispose() {
			disposed = true;
		}

		@Override
		public boolean isDisposed() {
			return disposed;
		}
	}

	private static final class AsynchronousRejectScheduler implements Scheduler {

		private final CountDownLatch submitted = new CountDownLatch(1);
		private final AtomicReference<Runnable> task = new AtomicReference<>();

		@Override
		public Disposable schedule(Runnable command) {
			task.set(command);
			submitted.countDown();
			return Disposables.disposed();
		}

		private void reject(RejectedExecutionException failure) {
			var rejectionAware = assertInstanceOf(RWScheduler.RejectionAwareTask.class, task.get());
			rejectionAware.reject(failure);
		}

		@Override
		public Worker createWorker() {
			throw new AssertionError("tracked scheduling must use the direct scheduler path");
		}

		@Override
		public void dispose() {
		}

		@Override
		public boolean isDisposed() {
			return false;
		}
	}

	private static final class CapturingScheduler implements Scheduler {

		private final AtomicReference<Runnable> task = new AtomicReference<>();

		@Override
		public Disposable schedule(Runnable command) {
			if (!task.compareAndSet(null, command)) {
				throw new AssertionError("tracked scheduling submitted more than one task");
			}
			return Disposables.disposed();
		}

		private Runnable task() {
			return assertInstanceOf(Runnable.class, task.get());
		}

		@Override
		public Worker createWorker() {
			throw new AssertionError("tracked scheduling must use the direct scheduler path");
		}

		@Override
		public void dispose() {
		}

		@Override
		public boolean isDisposed() {
			return false;
		}
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

	private static void assertEventually(java.util.function.BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(1L);
		}
	}
}
