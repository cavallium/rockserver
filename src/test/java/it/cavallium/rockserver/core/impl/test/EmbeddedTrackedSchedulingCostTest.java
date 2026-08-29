package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.util.concurrent.Callable;
import java.util.concurrent.atomic.AtomicLong;
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
}
