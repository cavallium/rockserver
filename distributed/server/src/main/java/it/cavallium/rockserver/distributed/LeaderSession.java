package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * State that exists only while this node leads one term: the conflict tracker and the open transactions.
 *
 * <p>An open transaction (or {@code getForUpdate} update) is a native transaction on the leader's local database:
 * it provides the snapshot and read-your-writes exactly as on a single node. Its writes are also recorded here, so
 * that the commit can be proposed as one {@link LogEntry.Guarded} entry; the native transaction itself is always
 * rolled back, because the committed effects arrive through the log like every other write.</p>
 */
final class LeaderSession {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderSession.class);

	/** One open transaction or update. */
	static final class OpenTransaction {
		final long id;
		final boolean update;
		final long snapshotIndex;
		private final long deadlineNanos;
		private final List<LogEntry.Write> writes = new ArrayList<>();
		private final LongArrayList conflictColumns = new LongArrayList();
		private final LongArrayList conflictHashes = new LongArrayList();

		private OpenTransaction(long id, boolean update, long snapshotIndex, long deadlineNanos) {
			this.id = id;
			this.update = update;
			this.snapshotIndex = snapshotIndex;
			this.deadlineNanos = deadlineNanos;
		}

		/** Records a write staged in the transaction; its keys become conflict keys, as in RocksDB optimistic transactions. */
		synchronized void addWrite(LogEntry.Write write, List<Keys> keys) {
			writes.add(write);
			for (Keys key : keys) {
				addConflict(write.columnId(), key);
			}
		}

		/** Records a key read with {@code getForUpdate}. */
		synchronized void addConflict(long columnId, Keys keys) {
			conflictColumns.add(columnId);
			conflictHashes.add(ConflictTracker.keyHash(columnId, keys));
		}

		synchronized boolean hasWrites() {
			return !writes.isEmpty();
		}

		/** The commit entry of a transaction: all staged writes, returning true. */
		synchronized LogEntry.Guarded commitEntry(long term) {
			return new LogEntry.Guarded(term, snapshotIndex, conflictColumns.toLongArray(), conflictHashes.toLongArray(),
					writes, false);
		}

		/** The commit entry of an update: its single final write, returning that write's result. */
		synchronized LogEntry.Guarded updateEntry(long term, LogEntry.Write write, List<Keys> keys) {
			var columns = new LongArrayList(conflictColumns);
			var hashes = new LongArrayList(conflictHashes);
			for (Keys key : keys) {
				columns.add(write.columnId());
				hashes.add(ConflictTracker.keyHash(write.columnId(), key));
			}
			return new LogEntry.Guarded(term, snapshotIndex, columns.toLongArray(), hashes.toLongArray(), List.of(write),
					true);
		}

		boolean isExpired(long nowNanos) {
			return nowNanos - deadlineNanos > 0;
		}
	}

	private final long term;
	private final ConflictTracker tracker = new ConflictTracker();
	private final ConcurrentHashMap<Long, OpenTransaction> transactions = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Object, Long> pins = new ConcurrentHashMap<>();
	private long appliesSincePrune;
	private volatile boolean closed;

	LeaderSession(long term) {
		this.term = term;
	}

	long term() {
		return term;
	}

	ConflictTracker tracker() {
		return tracker;
	}

	boolean isClosed() {
		return closed;
	}

	/**
	 * Pins the current applied index as a snapshot index before the native transaction is opened, so that the write
	 * history after it is kept: the native snapshot, taken afterwards, can only be newer, which at worst causes false
	 * conflicts.
	 */
	synchronized Object reserve(AppliedIndex applied) {
		// Read the applied index under the same lock as pruning, so nothing newer can be pruned in between.
		return pin(applied.get());
	}

	/**
	 * Keeps the write history after {@code snapshotIndex} until {@link #unpin}: a commit holds one while its entry
	 * is in flight, so that rolling back, expiring or closing the transaction meanwhile cannot drop the history its
	 * validation still needs.
	 */
	synchronized Object pin(long snapshotIndex) {
		var pin = new Object();
		pins.put(pin, snapshotIndex);
		return pin;
	}

	synchronized void unpin(Object pin) {
		pins.remove(pin);
		tracker.prune(minSnapshotIndex());
	}

	/**
	 * Turns a reservation into an open transaction once the native transaction exists. Fails if leadership ended
	 * meanwhile: the caller must then roll the native transaction back itself.
	 */
	synchronized OpenTransaction register(Object reservation, long id, boolean update, long leaseNanos) {
		if (closed) {
			pins.remove(reservation);
			throw new IllegalStateException("Leadership of term " + term + " ended");
		}
		Long snapshotIndex = pins.get(reservation);
		if (snapshotIndex == null) {
			throw new IllegalStateException("Unknown reservation");
		}
		var transaction = new OpenTransaction(id, update, snapshotIndex, System.nanoTime() + leaseNanos);
		if (transactions.putIfAbsent(id, transaction) != null) {
			throw new IllegalStateException("Transaction id reused: " + id);
		}
		pins.remove(reservation);
		return transaction;
	}

	/** Releases a reservation whose transaction could not be opened. */
	void cancel(Object reservation) {
		unpin(reservation);
	}

	@Nullable OpenTransaction get(long id) {
		return transactions.get(id);
	}

	/** Forgets a finished transaction and the write history no open transaction needs any more. */
	synchronized void remove(OpenTransaction transaction) {
		transactions.remove(transaction.id, transaction);
		tracker.prune(minSnapshotIndex());
	}

	/** Called after each applied entry of this term: keeps the tracker bounded when there are no transactions. */
	synchronized void afterApply() {
		if (++appliesSincePrune >= 256) {
			appliesSincePrune = 0;
			tracker.prune(minSnapshotIndex());
		}
	}

	/** Drops expired transactions (their native transaction expires on its own) and prunes the tracker. */
	synchronized void expire() {
		long now = System.nanoTime();
		transactions.values().removeIf(transaction -> transaction.isExpired(now));
		tracker.prune(minSnapshotIndex());
	}

	private long minSnapshotIndex() {
		long min = Long.MAX_VALUE;
		for (OpenTransaction transaction : transactions.values()) {
			min = Math.min(min, transaction.snapshotIndex);
		}
		for (long pinned : pins.values()) {
			min = Math.min(min, pinned);
		}
		// Writes at or below the oldest snapshot can no longer conflict with anything.
		return min;
	}

	/** Leadership ended: roll back every open transaction. Their ids become unknown, so clients retry them. */
	void close(EmbeddedDB db) {
		synchronized (this) {
			closed = true;
		}
		for (OpenTransaction transaction : transactions.values()) {
			try {
				if (transaction.update) {
					db.closeFailedUpdate(transaction.id);
				} else {
					db.closeTransaction(transaction.id, false);
				}
			} catch (RuntimeException ex) {
				LOG.debug("Failed to roll back transaction {} of term {}", transaction.id, term, ex);
			}
		}
		transactions.clear();
	}
}
