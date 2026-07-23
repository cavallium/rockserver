package it.cavallium.rockserver.core.impl;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

public class SafeShutdown implements AutoCloseable {

	private static final long CLOSING_MASK = Long.MIN_VALUE;
	private static final long PENDING_MASK = Long.MAX_VALUE;

	/**
	 * The sign bit marks closing and the remaining bits hold the exact pending-operation count.
	 * Keeping both values in one CAS word makes beginOp linearizable with closeAndWait.
	 */
	private final AtomicLong state = new AtomicLong();

	private final ReentrantLock waitLock = new ReentrantLock();
	private final Condition operationsFinished = waitLock.newCondition();
	private volatile int waiters;

	public void beginOp() {
		for (;;) {
			long current = state.get();
			if (current < 0) {
				throw new IllegalStateException("Closed");
			}
			if (current == PENDING_MASK) {
				throw new IllegalStateException("Too many pending operations");
			}
			if (state.compareAndSet(current, current + 1)) {
				return;
			}
		}
	}

	public void endOp() {
		long updated;
		for (;;) {
			long current = state.get();
			if ((current & PENDING_MASK) == 0) {
				throw new IllegalStateException("No pending operation to end");
			}
			updated = current - 1;
			if (state.compareAndSet(current, updated)) {
				break;
			}
		}

		if ((updated & PENDING_MASK) == 0 && waiters != 0) {
			signalWaiters();
		}
	}

	public void closeAndWait(long timeoutMs) throws TimeoutException {
		validateTimeout(timeoutMs);
		closeAdmission();
		waitForExit(timeoutMs);
	}

	/**
	 * Prevent new operations from starting without waiting for already admitted operations.
	 * This lets an owner cancel shutdown-aware work before waiting for the active set to drain.
	 */
	public void closeAdmission() {
		startClosing();
	}

	public void waitForExit(long timeoutMs) throws TimeoutException {
		validateTimeout(timeoutMs);
		if (getPendingOpsCount() == 0) {
			return;
		}

		long waitStartedNanos = System.nanoTime();
		try {
			waitLock.lockInterruptibly();
		} catch (InterruptedException exception) {
			throw interruptedTimeout(exception);
		}
		waiters++;
		try {
			if (timeoutMs == Long.MAX_VALUE) {
				awaitIndefinitely();
			} else {
				long timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMs);
				long lockWaitNanos = System.nanoTime() - waitStartedNanos;
				long remainingNanos = lockWaitNanos >= timeoutNanos ? 0 : timeoutNanos - lockWaitNanos;
				awaitFor(remainingNanos);
			}
		} finally {
			waiters--;
			waitLock.unlock();
		}
	}

	private static void validateTimeout(long timeoutMs) {
		if (timeoutMs < 0) {
			throw new IllegalArgumentException("timeoutMs must not be negative");
		}
	}

	private void startClosing() {
		for (;;) {
			long current = state.get();
			if (current < 0 || state.compareAndSet(current, current | CLOSING_MASK)) {
				return;
			}
		}
	}

	private void awaitIndefinitely() throws TimeoutException {
		while (getPendingOpsCount() != 0) {
			try {
				operationsFinished.await();
			} catch (InterruptedException exception) {
				throw interruptedTimeout(exception);
			}
		}
	}

	private void awaitFor(long timeoutNanos) throws TimeoutException {
		long remainingNanos = timeoutNanos;
		while (getPendingOpsCount() != 0) {
			if (remainingNanos <= 0) {
				throw new TimeoutException("Timed out with " + getPendingOpsCount() + " pending operations");
			}
			try {
				remainingNanos = operationsFinished.awaitNanos(remainingNanos);
			} catch (InterruptedException exception) {
				throw interruptedTimeout(exception);
			}
		}
	}

	private void signalWaiters() {
		waitLock.lock();
		try {
			operationsFinished.signalAll();
		} finally {
			waitLock.unlock();
		}
	}

	private static TimeoutException interruptedTimeout(InterruptedException cause) {
		Thread.currentThread().interrupt();
		var exception = new TimeoutException("Interrupted while waiting for pending operations to finish");
		exception.initCause(cause);
		return exception;
	}

	@Override
	public void close() {
		try {
			closeAndWait(Long.MAX_VALUE);
		} catch (TimeoutException exception) {
			throw new RuntimeException(exception);
		}
	}

	public boolean isOpen() {
		return state.get() >= 0;
	}

	/**
	 * Expose current number of pending operations for diagnostics/metrics.
	 */
	public long getPendingOpsCount() {
		return state.get() & PENDING_MASK;
	}
}
