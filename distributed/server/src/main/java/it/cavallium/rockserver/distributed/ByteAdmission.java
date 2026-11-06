package it.cavallium.rockserver.distributed;

import java.util.concurrent.atomic.AtomicLong;

/**
 * A byte-weighted semaphore that never waits: {@link #tryAcquire} either takes the bytes or refuses at once. Used by
 * {@link Proposer} to bound the bytes of writes a node holds, and the bytes of the attempts it has in flight.
 */
final class ByteAdmission {

	private final long limit;
	private final AtomicLong used = new AtomicLong();

	/** {@code limit} must be at least the largest weight ever asked for, or such a request is always refused. */
	ByteAdmission(long limit) {
		if (limit <= 0) {
			throw new IllegalArgumentException("limit must be positive: " + limit);
		}
		this.limit = limit;
	}

	/** Takes {@code bytes} if they fit under the limit with what is taken already; false, taking nothing, otherwise. */
	boolean tryAcquire(long bytes) {
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
