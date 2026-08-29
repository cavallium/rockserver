package it.cavallium.rockserver.core.impl;

import org.jetbrains.annotations.VisibleForTesting;

/**
 * Allocation-free evaluator for the RocksDB signals that protect background work from write stalls.
 *
 * <p>The write-stop and actual delayed-write-rate properties are authoritative database-wide signals.
 * Pending-compaction bytes are an optional proactive signal and must be compared with the effective
 * soft limit of the same column family. A positive explicit override replaces the per-column limit;
 * otherwise zero and {@link Long#MAX_VALUE} limits disable proactive pressure for that column.</p>
 *
 * <p>One instance is reusable by one polling thread. Observation getters are safe for concurrent metric
 * readers and do not allocate. This type is public only so the qualified Rockserver test/benchmark
 * module can exercise the exact production evaluator; the {@code impl} package is not exported as a
 * public library API.</p>
 */
public final class StoragePressureSignal {

	public static final int REASON_WRITE_STOPPED = 1;
	public static final int REASON_DELAYED_WRITE = 1 << 1;
	public static final int REASON_PENDING_COMPACTION = 1 << 2;
	public static final int REASON_SIGNAL_FAILURE = 1 << 3;

	private static final long NO_COLUMN = -1L;

	private final long pendingCompactionLimitOverride;
	private volatile int reasonMask;
	private volatile long actualDelayedWriteRate;
	private volatile long maximumPendingCompactionBytes;
	private volatile long triggeringColumnId = NO_COLUMN;
	private volatile long triggeringPendingCompactionBytes;
	private volatile long triggeringPendingCompactionLimit;

	/** Use every column family's effective configured soft limit. */
	public StoragePressureSignal() {
		this.pendingCompactionLimitOverride = 0L;
	}

	/**
	 * Use one explicit positive proactive threshold for every column family.
	 *
	 * @param pendingCompactionLimitOverride threshold in bytes; must be positive
	 */
	public StoragePressureSignal(long pendingCompactionLimitOverride) {
		if (pendingCompactionLimitOverride <= 0L) {
			throw new IllegalArgumentException("pendingCompactionLimitOverride must be positive");
		}
		this.pendingCompactionLimitOverride = pendingCompactionLimitOverride;
	}

	/** Begin a new poll with the two authoritative database-wide RocksDB properties. */
	public void reset(long writeStopped, long actualDelayedWriteRate) {
		this.actualDelayedWriteRate = actualDelayedWriteRate;
		this.maximumPendingCompactionBytes = 0L;
		this.triggeringColumnId = NO_COLUMN;
		this.triggeringPendingCompactionBytes = 0L;
		this.triggeringPendingCompactionLimit = 0L;

		int reasons = 0;
		if (writeStopped != 0L) {
			reasons |= REASON_WRITE_STOPPED;
		}
		if (actualDelayedWriteRate != 0L) {
			reasons |= REASON_DELAYED_WRITE;
		}
		this.reasonMask = reasons;
	}

	/**
	 * Observe one column family. Native byte counters are compared as unsigned longs.
	 * A negative configured limit is invalid and therefore fails closed.
	 */
	public void observeColumn(long columnId,
			long pendingCompactionBytes,
			long effectiveSoftPendingCompactionBytesLimit) {
		if (Long.compareUnsigned(pendingCompactionBytes, maximumPendingCompactionBytes) > 0) {
			maximumPendingCompactionBytes = pendingCompactionBytes;
		}

		long limit = pendingCompactionLimitOverride != 0L
				? pendingCompactionLimitOverride
				: effectiveSoftPendingCompactionBytesLimit;
		if (pendingCompactionLimitOverride == 0L
				&& (limit == 0L || limit == Long.MAX_VALUE)) {
			return;
		}
		if (limit < 0L) {
			markSignalFailure();
			return;
		}
		if (Long.compareUnsigned(pendingCompactionBytes, limit) >= 0) {
			if ((reasonMask & REASON_PENDING_COMPACTION) == 0) {
				triggeringColumnId = columnId;
				triggeringPendingCompactionBytes = pendingCompactionBytes;
				triggeringPendingCompactionLimit = limit;
			}
			reasonMask |= REASON_PENDING_COMPACTION;
		}
	}

	/** Fail closed when an authoritative native signal cannot be read safely. */
	public void markSignalFailure() {
		reasonMask |= REASON_SIGNAL_FAILURE;
	}

	public boolean pressured() {
		return reasonMask != 0;
	}

	public int reasonMask() {
		return reasonMask;
	}

	public boolean hasReason(int reason) {
		return (reasonMask & reason) != 0;
	}

	public long actualDelayedWriteRate() {
		return actualDelayedWriteRate;
	}

	public long maximumPendingCompactionBytes() {
		return maximumPendingCompactionBytes;
	}

	public long triggeringColumnId() {
		return triggeringColumnId;
	}

	public long triggeringPendingCompactionBytes() {
		return triggeringPendingCompactionBytes;
	}

	public long triggeringPendingCompactionLimit() {
		return triggeringPendingCompactionLimit;
	}

	@VisibleForTesting
	public long pendingCompactionLimitOverride() {
		return pendingCompactionLimitOverride;
	}
}
