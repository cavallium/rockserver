package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.SafeShutdown;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

public class SafeShutdownTest {

	@Test
	@Timeout(10)
	@DisplayName("close() should wait indefinitely without overflowing its timeout")
	public void testCloseWaitsForPendingOperation() throws Exception {
		var shutdown = new SafeShutdown();
		shutdown.beginOp();

		try (var executor = Executors.newSingleThreadExecutor()) {
			var closeInvoked = new CountDownLatch(1);
			Future<?> close = executor.submit(() -> {
				closeInvoked.countDown();
				shutdown.close();
			});

			Assertions.assertTrue(closeInvoked.await(1, TimeUnit.SECONDS));
			awaitClosing(shutdown);
			Assertions.assertFalse(close.isDone(), "close returned while an operation was still pending");

			shutdown.endOp();
			close.get(1, TimeUnit.SECONDS);
		}

		Assertions.assertEquals(0, shutdown.getPendingOpsCount());
		Assertions.assertThrows(IllegalStateException.class, shutdown::beginOp);
	}

	@Test
	@Timeout(10)
	@DisplayName("A huge finite timeout should saturate instead of overflowing")
	public void testHugeFiniteTimeoutDoesNotOverflow() throws Exception {
		var shutdown = new SafeShutdown();
		shutdown.beginOp();

		try (var executor = Executors.newSingleThreadExecutor()) {
			Future<?> close = executor.submit(() -> {
				try {
					shutdown.closeAndWait(Long.MAX_VALUE - 1);
				} catch (TimeoutException exception) {
					throw new AssertionError(exception);
				}
			});

			awaitClosing(shutdown);
			Assertions.assertFalse(close.isDone(), "large timeout overflowed and expired immediately");
			shutdown.endOp();
			close.get(1, TimeUnit.SECONDS);
		}
	}

	@Test
	@Timeout(10)
	@DisplayName("A finite timeout should close admission but preserve the pending count")
	public void testFiniteTimeoutAndLaterDrain() throws Exception {
		var shutdown = new SafeShutdown();
		shutdown.beginOp();

		Assertions.assertThrows(TimeoutException.class, () -> shutdown.closeAndWait(1));
		Assertions.assertFalse(shutdown.isOpen());
		Assertions.assertEquals(1, shutdown.getPendingOpsCount());
		Assertions.assertThrows(IllegalStateException.class, shutdown::beginOp);

		shutdown.endOp();
		shutdown.waitForExit(100);
		Assertions.assertEquals(0, shutdown.getPendingOpsCount());
	}

	@Test
	@DisplayName("Invalid timeout and unmatched end should fail without corrupting state")
	public void testInvalidLifecycleCalls() {
		var shutdown = new SafeShutdown();

		Assertions.assertThrows(IllegalArgumentException.class, () -> shutdown.closeAndWait(-1));
		Assertions.assertTrue(shutdown.isOpen());
		Assertions.assertThrows(IllegalStateException.class, shutdown::endOp);
		Assertions.assertEquals(0, shutdown.getPendingOpsCount());
	}

	@Test
	@Timeout(20)
	@DisplayName("Concurrent begin and close should have no admitted operation after close returns")
	public void testBeginAndCloseAreLinearizable() throws Exception {
		int workerCount = 8;
		int rounds = 2_000;

		try (var executor = Executors.newFixedThreadPool(workerCount + 1)) {
			for (int round = 0; round < rounds; round++) {
				var shutdown = new SafeShutdown();
				var start = new CountDownLatch(1);
				var closeReturned = new AtomicBoolean();
				var admittedAfterClose = new AtomicBoolean();
				List<Future<?>> workers = new ArrayList<>(workerCount);

				for (int worker = 0; worker < workerCount; worker++) {
					workers.add(executor.submit(() -> {
						start.await();
						try {
							shutdown.beginOp();
						} catch (IllegalStateException ignored) {
							return null;
						}
						if (closeReturned.get()) {
							admittedAfterClose.set(true);
						}
						Thread.yield();
						shutdown.endOp();
						return null;
					}));
				}

				Future<?> close = executor.submit(() -> {
					start.await();
					shutdown.closeAndWait(Long.MAX_VALUE);
					closeReturned.set(true);
					return null;
				});

				start.countDown();
				for (Future<?> worker : workers) {
					worker.get(1, TimeUnit.SECONDS);
				}
				close.get(1, TimeUnit.SECONDS);

				Assertions.assertFalse(admittedAfterClose.get(), "beginOp linearized after close returned");
				Assertions.assertFalse(shutdown.isOpen());
				Assertions.assertEquals(0, shutdown.getPendingOpsCount());
			}
		}
	}

	@Test
	@Timeout(10)
	@DisplayName("Interrupting one shutdown waiter should not strand the remaining waiters")
	public void testInterruptedWaiterDoesNotConsumeFinalWakeup() throws Exception {
		var shutdown = new SafeShutdown();
		shutdown.beginOp();
		shutdown.beginOp();
		var interruptedThread = new AtomicReference<Thread>();
		var infiniteThread = new AtomicReference<Thread>();
		var finiteThread = new AtomicReference<Thread>();
		var interruptedStatusRestored = new AtomicBoolean();

		try (var executor = Executors.newFixedThreadPool(3)) {
			Future<TimeoutException> interruptedWaiter = executor.submit(() -> {
				interruptedThread.set(Thread.currentThread());
				try {
					shutdown.closeAndWait(Long.MAX_VALUE);
					return null;
				} catch (TimeoutException exception) {
					interruptedStatusRestored.set(Thread.currentThread().isInterrupted());
					return exception;
				}
			});
			Future<?> infiniteWaiter = executor.submit(() -> {
				infiniteThread.set(Thread.currentThread());
				shutdown.closeAndWait(Long.MAX_VALUE);
				return null;
			});
			Future<?> finiteWaiter = executor.submit(() -> {
				finiteThread.set(Thread.currentThread());
				shutdown.closeAndWait(5_000);
				return null;
			});

			awaitClosing(shutdown);
			awaitWaiting(interruptedThread);
			awaitWaiting(infiniteThread);
			awaitWaiting(finiteThread);

			interruptedThread.get().interrupt();
			TimeoutException interruption = interruptedWaiter.get(1, TimeUnit.SECONDS);
			Assertions.assertNotNull(interruption);
			Assertions.assertInstanceOf(InterruptedException.class, interruption.getCause());
			Assertions.assertTrue(interruptedStatusRestored.get());

			shutdown.endOp();
			Assertions.assertEquals(1, shutdown.getPendingOpsCount());
			Assertions.assertFalse(infiniteWaiter.isDone());
			Assertions.assertFalse(finiteWaiter.isDone());

			shutdown.endOp();
			infiniteWaiter.get(1, TimeUnit.SECONDS);
			finiteWaiter.get(1, TimeUnit.SECONDS);
		}

		Assertions.assertEquals(0, shutdown.getPendingOpsCount());
		Assertions.assertFalse(shutdown.isOpen());
	}

	@Test
	@DisplayName("Zero-timeout and repeated close calls should be idempotent after drain")
	public void testRepeatedImmediateCloseAfterDrain() throws Exception {
		var shutdown = new SafeShutdown();

		shutdown.closeAndWait(0);
		shutdown.closeAndWait(0);
		shutdown.waitForExit(0);

		Assertions.assertEquals(0, shutdown.getPendingOpsCount());
		Assertions.assertFalse(shutdown.isOpen());
		Assertions.assertThrows(IllegalStateException.class, shutdown::beginOp);
	}

	@Test
	@DisplayName("The pending-operation counter should reject saturation without wrapping into closed state")
	public void testPendingOperationCounterSaturation() throws Exception {
		var shutdown = new SafeShutdown();
		var stateField = SafeShutdown.class.getDeclaredField("state");
		stateField.setAccessible(true);
		var state = (AtomicLong) stateField.get(shutdown);
		state.set(Long.MAX_VALUE);

		IllegalStateException failure = Assertions.assertThrows(IllegalStateException.class, shutdown::beginOp);
		Assertions.assertEquals("Too many pending operations", failure.getMessage());
		Assertions.assertTrue(shutdown.isOpen(), "counter saturation must not set the closing bit");
		Assertions.assertEquals(Long.MAX_VALUE, shutdown.getPendingOpsCount());
	}

	@Test
	@DisplayName("close() should preserve interruption while adapting its checked timeout")
	public void testCloseAdaptsInterruptionWithoutLosingStatus() {
		var shutdown = new SafeShutdown();
		shutdown.beginOp();
		Thread.currentThread().interrupt();
		try {
			RuntimeException failure = Assertions.assertThrows(RuntimeException.class, shutdown::close);
			TimeoutException timeout = Assertions.assertInstanceOf(TimeoutException.class, failure.getCause());
			Assertions.assertInstanceOf(InterruptedException.class, timeout.getCause());
			Assertions.assertTrue(Thread.currentThread().isInterrupted());
			Assertions.assertFalse(shutdown.isOpen());
			Assertions.assertEquals(1, shutdown.getPendingOpsCount());
		} finally {
			Thread.interrupted();
			shutdown.endOp();
		}
	}

	@Test
	@Timeout(10)
	@DisplayName("A finite condition wait should report interruption and retain its pending operation")
	public void testFiniteConditionWaitInterruption() throws Exception {
		var shutdown = new SafeShutdown();
		shutdown.beginOp();
		var waitingThread = new AtomicReference<Thread>();
		var interruptedStatusRestored = new AtomicBoolean();

		try (var executor = Executors.newSingleThreadExecutor()) {
			Future<TimeoutException> waiter = executor.submit(() -> {
				waitingThread.set(Thread.currentThread());
				try {
					shutdown.waitForExit(5_000);
					return null;
				} catch (TimeoutException exception) {
					interruptedStatusRestored.set(Thread.currentThread().isInterrupted());
					return exception;
				}
			});

			awaitWaiting(waitingThread);
			waitingThread.get().interrupt();
			TimeoutException interruption = waiter.get(1, TimeUnit.SECONDS);
			Assertions.assertNotNull(interruption);
			Assertions.assertInstanceOf(InterruptedException.class, interruption.getCause());
			Assertions.assertTrue(interruptedStatusRestored.get());
			Assertions.assertEquals(1, shutdown.getPendingOpsCount());
		}

		shutdown.endOp();
		shutdown.waitForExit(100);
	}

	private static void awaitClosing(SafeShutdown shutdown) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
		while (shutdown.isOpen() && System.nanoTime() - deadline < 0) {
			Thread.onSpinWait();
		}
		Assertions.assertFalse(shutdown.isOpen(), "shutdown did not start closing");
	}

	private static void awaitWaiting(AtomicReference<Thread> threadReference) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(1);
		while (System.nanoTime() < deadline) {
			Thread thread = threadReference.get();
			if (thread != null
					&& (thread.getState() == Thread.State.WAITING
					|| thread.getState() == Thread.State.TIMED_WAITING)) {
				return;
			}
			Thread.onSpinWait();
		}
		Assertions.fail("shutdown waiter did not reach its condition wait");
	}
}
