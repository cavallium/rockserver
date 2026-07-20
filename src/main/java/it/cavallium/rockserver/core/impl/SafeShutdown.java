package it.cavallium.rockserver.core.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;

public class SafeShutdown implements AutoCloseable {

	// High bit marks closing; lower bits hold the pending operation count.
	private static final long CLOSING_FLAG = Long.MIN_VALUE;

	private static final long PENDING_MASK = Long.MAX_VALUE;

	private final AtomicLong state = new AtomicLong();

	private final Object exitMonitor = new Object();

	public void beginOp() {
		while (true) {
			var current = state.get();
			if (isClosing(current)) {
				throw new IllegalStateException("Closed");
			}
			if (pendingOps(current) == PENDING_MASK) {
				throw new IllegalStateException("Pending operation count overflow");
			}
			if (state.compareAndSet(current, current + 1)) {
				return;
			}
		}
	}

	public void endOp() {
		while (true) {
			var current = state.get();
			var pendingOps = pendingOps(current);
			if (pendingOps == 0) {
				throw new IllegalStateException("No pending operation to end");
			}
			var next = current - 1;
			if (state.compareAndSet(current, next)) {
				if (pendingOps(next) == 0) {
					notifyExitWaiters();
				}
				return;
			}
		}
	}

	public void closeAndWait(long timeoutMs) throws TimeoutException {
		validateTimeout(timeoutMs);
		markClosing();
		waitForExitAfterValidation(timeoutMs);
	}

	public void waitForExit(long timeoutMs) throws TimeoutException {
		validateTimeout(timeoutMs);
		waitForExitAfterValidation(timeoutMs);
	}

	private static void validateTimeout(long timeoutMs) {
		if (timeoutMs < 0) {
			throw new IllegalArgumentException("timeoutMs must be non-negative");
		}
	}

	private static boolean isClosing(long state) {
		return (state & CLOSING_FLAG) != 0;
	}

	private static long pendingOps(long state) {
		return state & PENDING_MASK;
	}

	private void markClosing() {
		while (true) {
			var current = state.get();
			if (isClosing(current) || state.compareAndSet(current, current | CLOSING_FLAG)) {
				return;
			}
		}
	}

	private void notifyExitWaiters() {
		synchronized (exitMonitor) {
			exitMonitor.notifyAll();
		}
	}

	private void waitForExitAfterValidation(long timeoutMs) throws TimeoutException {
		var timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
		var startNanos = System.nanoTime();
		synchronized (exitMonitor) {
			try {
				while (getPendingOpsCount() > 0) {
					if (timeoutMs == Long.MAX_VALUE) {
						exitMonitor.wait();
						continue;
					}
					var remainingNanos = timeoutNanos - (System.nanoTime() - startNanos);
					if (remainingNanos <= 0) {
						break;
					}
					var waitMillis = TimeUnit.NANOSECONDS.toMillis(remainingNanos);
					var waitNanos = (int) (remainingNanos - TimeUnit.MILLISECONDS.toNanos(waitMillis));
					exitMonitor.wait(waitMillis, waitNanos);
				}
				if (getPendingOpsCount() > 0) {
					throw new TimeoutException();
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				var timeout = new TimeoutException("Interrupted while waiting for pending operations to finish");
				timeout.initCause(e);
				throw timeout;
			}
		}
	}

	@Override
	public void close() {
		try {
			closeAndWait(Long.MAX_VALUE);
		} catch (TimeoutException e) {
			throw new IllegalStateException("Safe shutdown close did not finish", e);
		}
	}

	public boolean isOpen() {
		return !isClosing(state.get());
	}

	/**
	 * Expose current number of pending operations for diagnostics/metrics.
	 */
	public long getPendingOpsCount() {
		return pendingOps(state.get());
	}
}
