package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.RWScheduler;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.AbstractExecutorService;
import java.util.concurrent.TimeUnit;

import org.jetbrains.annotations.NotNull;
import org.junit.jupiter.api.Test;
import reactor.core.scheduler.Schedulers;

class RWSchedulerTest {

	@Test
	void forcedExecutorTimeoutDoesNotSkipLaterExecutorCleanup() {
		var events = new ArrayList<String>();
		var readExecutor = new RecordingExecutorService("read", events, 2);
		var writeExecutor = new RecordingExecutorService("write", events, 0);
		var scheduler = new RWScheduler(
				Schedulers.immediate(),
				Schedulers.immediate(),
				readExecutor,
				writeExecutor
		);

		var error = assertThrows(IllegalStateException.class, scheduler::dispose);
		assertEquals("read executor did not terminate after forced shutdown", error.getMessage());
		assertEquals(List.of(
				"read-shutdown",
				"read-await",
				"read-shutdown-now",
				"read-await",
				"write-shutdown",
				"write-await"
		), events);
	}

	@Test
	void interruptedExecutorAwaitDoesNotSkipLaterExecutorCleanupAndRestoresInterrupt() {
		var events = new ArrayList<String>();
		var readExecutor = new RecordingExecutorService("read", events, 0, true);
		var writeExecutor = new RecordingExecutorService("write", events, 0);
		var scheduler = new RWScheduler(
				Schedulers.immediate(),
				Schedulers.immediate(),
				readExecutor,
				writeExecutor
		);

		try {
			scheduler.dispose();

			assertTrue(Thread.interrupted());
			assertEquals(List.of(
					"read-shutdown",
					"read-await",
					"read-shutdown-now",
					"read-await",
					"write-shutdown",
					"write-await"
			), events);
		} finally {
			clearInterruptedStatus();
		}
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	private static void clearInterruptedStatus() {
		Thread.interrupted();
	}

	private static final class RecordingExecutorService extends AbstractExecutorService {

		private final String name;
		private final List<String> events;
		private final int timeoutsBeforeTermination;
		private final boolean interruptFirstAwait;
		private boolean shutdown;
		private int awaits;

		private RecordingExecutorService(String name, List<String> events, int timeoutsBeforeTermination) {
			this(name, events, timeoutsBeforeTermination, false);
		}

		private RecordingExecutorService(String name,
		                                 List<String> events,
		                                 int timeoutsBeforeTermination,
		                                 boolean interruptFirstAwait) {
			this.name = name;
			this.events = events;
			this.timeoutsBeforeTermination = timeoutsBeforeTermination;
			this.interruptFirstAwait = interruptFirstAwait;
		}

		@Override
		public void shutdown() {
			events.add(name + "-shutdown");
			shutdown = true;
		}

		@Override
		public @NotNull List<Runnable> shutdownNow() {
			events.add(name + "-shutdown-now");
			shutdown = true;
			return List.of();
		}

		@Override
		public boolean isShutdown() {
			return shutdown;
		}

		@Override
		public boolean isTerminated() {
			return shutdown && awaits > timeoutsBeforeTermination;
		}

		@Override
		public boolean awaitTermination(long timeout, @NotNull TimeUnit unit) throws InterruptedException {
			events.add(name + "-await");
			if (interruptFirstAwait && awaits++ == 0) {
				throw new InterruptedException("await interrupted");
			}
			return awaits++ >= timeoutsBeforeTermination;
		}

		@Override
		public void execute(@NotNull Runnable command) {
			throw new UnsupportedOperationException();
		}
	}
}
