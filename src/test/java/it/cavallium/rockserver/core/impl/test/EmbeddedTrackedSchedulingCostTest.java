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
}
