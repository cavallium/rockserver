package it.cavallium.rockserver.distributed;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.LongSupplier;

/**
 * A byte-weighted semaphore that never waits: {@link #tryAcquire} either takes the bytes or refuses at once. Used by
 * {@link Proposer} to bound the bytes of writes a node holds, and the bytes of the attempts it has in flight.
 *
 * <p>A caller that asks again after a refusal names itself ({@link #tryAcquire(long, Object)}), and the oldest such
 * waiter reserves its bytes (R50-1): until it gets in, a request by anyone else is taken only if it leaves room for
 * the reservation. Otherwise, under saturation, bytes freed one release at a time go to whoever asks first, which in
 * {@link Proposer} is the batch dispatched by the one that just finished, and an attempt larger than one release
 * could be refused until its deadline. A reservation lapses {@link #RESERVATION_NANOS} after its waiter last asked,
 * so a waiter that gave up without {@link #cancel} holds nothing for long. With no reservation, the only cost over a
 * plain compare-and-set is one volatile read.</p>
 */
final class ByteAdmission {

	/** How long a reservation lasts after its waiter last asked (see {@link #tryAcquire(long, Object)}). */
	static final long RESERVATION_NANOS = TimeUnit.SECONDS.toNanos(1);

	private final long limit;
	private final AtomicLong used = new AtomicLong();
	private final LongSupplier clock;
	/** The oldest refused waiter that asks again, or null. */
	private final AtomicReference<Reservation> reservation = new AtomicReference<>();

	/** {@code bytes} kept for {@code waiter} until it gets in, or until {@code lapsesNanos} on {@link #clock}. */
	private record Reservation(Object waiter, long bytes, long lapsesNanos) {}

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

	/**
	 * Takes {@code bytes} as {@link #tryAcquire(long)} does, but leaving room for the oldest refused waiter that still
	 * asks, unless that is {@code waiter} itself. A refused {@code waiter} that is not null reserves its bytes if no
	 * one does, or renews its own reservation; it gives it up by getting in or by {@link #cancel}. A null
	 * {@code waiter} never reserves.
	 */
	boolean tryAcquire(long bytes, Object waiter) {
		if (bytes < 0) {
			throw new IllegalArgumentException("negative weight: " + bytes);
		}
		var reserved = reservation.get();
		if (reserved != null && reserved.waiter != waiter && clock.getAsLong() - reserved.lapsesNanos >= 0) {
			// Its waiter stopped asking.
			reservation.compareAndSet(reserved, null);
			reserved = reservation.get();
		}
		boolean own = reserved != null && reserved.waiter == waiter;
		long room = reserved == null || own ? limit : limit - reserved.bytes;
		long current;
		do {
			current = used.get();
			if (current + bytes > room) {
				if (waiter != null) {
					reserve(reserved, own, waiter, bytes);
				}
				return false;
			}
		} while (!used.compareAndSet(current, current + bytes));
		if (own) {
			reservation.compareAndSet(reserved, null);
		}
		return true;
	}

	private void reserve(Reservation reserved, boolean own, Object waiter, long bytes) {
		if (reserved == null || own) {
			reservation.compareAndSet(reserved, new Reservation(waiter, bytes, clock.getAsLong() + RESERVATION_NANOS));
		}
	}

	/** {@code waiter} stopped asking: its reservation, if it holds one, is given up. */
	void cancel(Object waiter) {
		var reserved = reservation.get();
		if (reserved != null && reserved.waiter == waiter) {
			reservation.compareAndSet(reserved, null);
		}
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
