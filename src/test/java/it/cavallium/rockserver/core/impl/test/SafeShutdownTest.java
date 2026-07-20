package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.SafeShutdown;

import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
class SafeShutdownTest {

	@Test
	void closeWithMaxTimeoutWaitsForPendingOperationInsteadOfOverflowing() throws Exception {
		var shutdown = new SafeShutdown();
		shutdown.beginOp();
		var completed = new AtomicBoolean();
		var failure = new AtomicReference<Throwable>();

		var closer = new Thread(() -> {
			try {
				shutdown.close();
				completed.set(true);
			} catch (Throwable t) {
				failure.set(t);
			}
		});
		closer.start();

		Thread.sleep(50);
		assertFalse(completed.get());
		assertNull(failure.get());

		shutdown.endOp();
		closer.join(5_000);

		assertFalse(closer.isAlive());
		assertTrue(completed.get());
		assertNull(failure.get());
		assertEquals(0, shutdown.getPendingOpsCount());
	}

	@Test
	void closeAndWaitTimesOutWhenPendingOperationsRemain() {
		var shutdown = new SafeShutdown();
		shutdown.beginOp();

		assertThrows(TimeoutException.class, () -> shutdown.closeAndWait(0));
	}

	@Test
	void closeAndWaitRejectsNewOperationsWhenPendingOperationsRemain() {
		var shutdown = new SafeShutdown();
		shutdown.beginOp();

		assertThrows(TimeoutException.class, () -> shutdown.closeAndWait(0));
		assertFalse(shutdown.isOpen());
		var beginFailure = assertThrows(IllegalStateException.class, shutdown::beginOp);

		assertEquals("Closed", beginFailure.getMessage());
		shutdown.endOp();
		assertEquals(0, shutdown.getPendingOpsCount());
	}

	@Test
	void endOpWithoutBeginRejectsUnbalancedOperation() {
		var shutdown = new SafeShutdown();

		var failure = assertThrows(IllegalStateException.class, shutdown::endOp);

		assertEquals("No pending operation to end", failure.getMessage());
		assertEquals(0, shutdown.getPendingOpsCount());
	}

	@Test
	void waitForExitRejectsNegativeTimeout() {
		var shutdown = new SafeShutdown();

		var failure = assertThrows(IllegalArgumentException.class, () -> shutdown.waitForExit(-1));

		assertEquals("timeoutMs must be non-negative", failure.getMessage());
	}

	@Test
	void interruptedWaitRestoresInterruptFlagAndPreservesCause() {
		var shutdown = new SafeShutdown();
		shutdown.beginOp();
		Thread.currentThread().interrupt();
		try {
			var failure = assertThrows(TimeoutException.class, () -> shutdown.waitForExit(1_000));

			assertTrue(Thread.currentThread().isInterrupted());
			assertInstanceOf(InterruptedException.class, failure.getCause());
			assertEquals("Interrupted while waiting for pending operations to finish", failure.getMessage());
		} finally {
			var interrupted = Thread.interrupted();
			shutdown.endOp();
			assertTrue(interrupted);
		}
	}
}
