package it.cavallium.rockserver.distributed;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;

/**
 * A byte-weighted semaphore that never waits: {@link #tryAcquire} either takes the bytes or refuses at once. Used by
 * {@link Proposer} to bound the bytes of writes a node holds, and the bytes of the attempts it has in flight.
 */
final class ByteAdmission {

	/** How long a reservation lasts after its waiter last asked (see {@link #tryAcquire(long, Object)}). */
	static final long RESERVATION_NANOS = TimeUnit.SECONDS.toNanos(1);

	private final long limit;
	private final AtomicLong used = new AtomicLong();
	private final LongSupplier clock;

	/** {@code limit} must be at least the largest weight ever asked for, or such a request is always refused. */
	ByteAdmission(long limit) {
		this(limit, System::nanoTime);
	}

	ByteAdmission(long limit, LongSupplier clock) {
		if (limit <= 0) {
			throw new IllegalArgumentException("limit must be positive: " + limit);
		}
		this.limit = limit;
		this.clock = clock;
	}

	/** Takes {@code bytes} if they fit under the limit with what is taken already; false, taking nothing, otherwise. */
	boolean tryAcquire(long bytes) {
		return tryAcquire(bytes, null);
	}

	/** Stub (red test): the same as {@link #tryAcquire(long)}. */
	boolean tryAcquire(long bytes, Object waiter) {
		if (bytes < 0) {
			throw new IllegalArgumentException("negative weight: " + bytes);
		}
		long current;
		do {
			current = used.get();
			if (current + bytes > limit) {
				return false;
			}
		} while (!used.compareAndSet(current, current + bytes));
		return true;
	}

	/** Stub (red test). */
	void cancel(Object waiter) {
	}

	/** Gives back {@code bytes} taken by {@link #tryAcquire}; exactly once for each successful call. */
	void release(long bytes) {
		long after = used.addAndGet(-bytes);
		assert after >= 0 : "released more than acquired";
	}

	long used() {
		return used.get();
	}

	long limit() {
		return limit;
	}
}
