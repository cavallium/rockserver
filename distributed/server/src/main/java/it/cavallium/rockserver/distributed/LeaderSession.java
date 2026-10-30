package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.unimi.dsi.fastutil.longs.Long2IntRBTreeMap;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
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

	/**
	 * The right to update one key with {@code getForUpdate}, held from before the read until the update's commit is
	 * applied or the update is closed. Released at most once.
	 */
	static final class KeyLock {
		final long key;
		private final AtomicBoolean released = new AtomicBoolean();

		private KeyLock(long key) {
			this.key = key;
		}
	}

	/** One open transaction or update. */
	static final class OpenTransaction {
		final long id;
		final boolean update;
		final long snapshotIndex;
		/** The key lock of a single-key update, released when the update is removed. */
		final @Nullable KeyLock keyLock;
		private final long deadlineNanos;
		private final List<LogEntry.Write> writes = new ArrayList<>();
		private final LongArrayList conflictColumns = new LongArrayList();
		private final LongArrayList conflictHashes = new LongArrayList();
		/**
		 * Encoded size of the commit entry ({@link LogEntryCodec#encodeEntryBody}) with every staged write and conflict
		 * key, plus the writes and keys being staged.
		 */
		private long commitBytes = LogEntryCodec.GUARDED_BASE_BYTES;

		private OpenTransaction(long id, boolean update, long snapshotIndex, @Nullable KeyLock keyLock,
				long deadlineNanos) {
			this.id = id;
			this.update = update;
			this.snapshotIndex = snapshotIndex;
			this.keyLock = keyLock;
			this.deadlineNanos = deadlineNanos;
		}

		/** What a write with {@code keyCount} keys adds to the commit entry: the write and its conflict keys. */
		static long stagedBytes(LogEntry.Write write, int keyCount) {
			return LogEntryCodec.writeSize(write) + (long) keyCount * LogEntryCodec.CONFLICT_KEY_BYTES;
		}

		/**
		 * Reserves room in the commit entry before a write or {@code getForUpdate} key is staged, so that a
		 * transaction that grows too large to commit is refused at that request instead of at commit, before the
		 * leader holds all of it. Release it with {@link #releaseBytes} if the request then fails.
		 *
		 * @throws RocksDBException {@code INVALID_REQUEST} if the commit entry would not fit in one log entry; the
		 *                          transaction stays open, without that request
		 */
		synchronized void reserveBytes(long bytes) {
			if (commitBytes + bytes > Proposer.MAX_ENTRY_BODY_BYTES) {
				throw RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
						"Transaction too large for one replicated entry: " + commitBytes + " bytes staged, " + bytes
								+ " more requested, at most " + Proposer.MAX_ENTRY_BODY_BYTES);
			}
			commitBytes += bytes;
		}

		synchronized void releaseBytes(long bytes) {
			commitBytes -= bytes;
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
	private final AppliedIndex applied;
	private final ConflictTracker tracker;
	private final ConcurrentHashMap<Long, OpenTransaction> transactions = new ConcurrentHashMap<>();
	private final ConcurrentHashMap<Object, Long> pins = new ConcurrentHashMap<>();
	/**
	 * How many open transactions and pins hold each snapshot index, so that the oldest one is found without a scan.
	 * Guarded by this.
	 */
	private final Long2IntRBTreeMap snapshots = new Long2IntRBTreeMap();
	/**
	 * Single-key updates by key hash ({@link ConflictTracker#keyHash}): a key is in the map while an update holds it,
	 * with the updates waiting for it in arrival order. Guarded by this.
	 */
	private final Long2ObjectOpenHashMap<ArrayDeque<CompletableFuture<KeyLock>>> keyQueues = new Long2ObjectOpenHashMap<>();
	private long appliesSincePrune;
	/**
	 * Whether {@link #expiryTick} is scheduled: it runs while some key is held after an update queued behind it, so
	 * that an abandoned update releases its key at the end of its lease even when no other request comes. Guarded by
	 * this.
	 */
	private boolean expiryScheduled;
	/** When {@link #expireIfDue} last expired transactions, in {@link System#nanoTime()} units. */
	private volatile long lastExpireNanos = System.nanoTime();
	private volatile boolean closed;

	LeaderSession(long term, AppliedIndex applied) {
		this(term, applied, new ConflictTracker());
	}

	LeaderSession(long term, AppliedIndex applied, ConflictTracker tracker) {
		this.term = term;
		this.applied = applied;
		this.tracker = tracker;
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
		hold(snapshotIndex);
		return pin;
	}

	synchronized void unpin(Object pin) {
		var snapshotIndex = pins.remove(pin);
		if (snapshotIndex != null) {
			release(snapshotIndex);
		}
		tracker.prune(minSnapshotIndex());
	}

	private void hold(long snapshotIndex) {
		snapshots.addTo(snapshotIndex, 1);
	}

	private void release(long snapshotIndex) {
		int holders = snapshots.get(snapshotIndex);
		if (holders > 1) {
			snapshots.put(snapshotIndex, holders - 1);
		} else {
			// Also after close(), which drops every hold at once.
			snapshots.remove(snapshotIndex);
		}
	}

	/**
	 * Turns a reservation into an open transaction once the native transaction exists. Fails if leadership ended
	 * meanwhile: the caller must then roll the native transaction back itself.
	 */
	/**
	 * Waits for the right to update {@code key}: single-key updates of one key run one after the other, each reading
	 * what the previous one committed, instead of all reading the same value and all but one failing to commit. The
	 * future completes on the thread that released the key, or here: chain work to it with an async stage. Fails with
	 * {@link RocksDBRetryException} if leadership ends first.
	 */
	synchronized CompletableFuture<KeyLock> lockKey(long key) {
		if (closed) {
			return CompletableFuture.failedFuture(new RocksDBRetryException());
		}
		var queue = keyQueues.get(key);
		if (queue == null) {
			keyQueues.put(key, new ArrayDeque<>());
			return CompletableFuture.completedFuture(new KeyLock(key));
		}
		var waiter = new CompletableFuture<KeyLock>();
		queue.addLast(waiter);
		if (!expiryScheduled) {
			expiryScheduled = true;
			EXPIRY_DELAY.execute(this::expiryTick);
		}
		return waiter;
	}

	/** Expires what is due, and runs again later while a key is held and leadership lasts. */
	private void expiryTick() {
		expireIfDue();
		synchronized (this) {
			if (closed || keyQueues.isEmpty()) {
				expiryScheduled = false;
				return;
			}
		}
		EXPIRY_DELAY.execute(this::expiryTick);
	}

	/** Open transactions and updates, for metrics. */
	int openTransactions() {
		return transactions.size();
	}

	/** Keys held by an update, for metrics. */
	synchronized int lockedKeys() {
		return keyQueues.size();
	}

	/** Releases {@code lock}, passing the key to the next waiting update that still waits. */
	synchronized void unlockKey(KeyLock lock) {
		if (!lock.released.compareAndSet(false, true)) {
			return;
		}
		var queue = keyQueues.get(lock.key);
		if (queue == null) {
			return;
		}
		while (true) {
			var next = queue.pollFirst();
			if (next == null) {
				keyQueues.remove(lock.key);
				return;
			}
			// A waiter that timed out or was cancelled is already complete: skip it.
			if (next.complete(new KeyLock(lock.key))) {
				return;
			}
		}
	}

	synchronized OpenTransaction register(Object reservation, long id, boolean update, @Nullable KeyLock keyLock,
			long leaseNanos) {
		if (closed) {
			unpin(reservation);
			throw new IllegalStateException("Leadership of term " + term + " ended");
		}
		Long snapshotIndex = pins.get(reservation);
		if (snapshotIndex == null) {
			throw new IllegalStateException("Unknown reservation");
		}
		var transaction = new OpenTransaction(id, update, snapshotIndex, keyLock, System.nanoTime() + leaseNanos);
		if (transactions.putIfAbsent(id, transaction) != null) {
			throw new IllegalStateException("Transaction id reused: " + id);
		}
		// The reservation's hold on the snapshot index passes to the transaction.
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
		if (transactions.remove(transaction.id, transaction)) {
			release(transaction.snapshotIndex);
		}
		if (transaction.keyLock != null) {
			unlockKey(transaction.keyLock);
		}
		tracker.prune(minSnapshotIndex());
	}

	/**
	 * Called after each applied entry of this term: keeps the tracker bounded when there are no transactions, and
	 * when transactions are abandoned without being closed.
	 */
	synchronized void afterApply() {
		// Pruning costs only what it drops; expiring scans every open transaction, so it is rarer.
		tracker.prune(minSnapshotIndex());
		if (++appliesSincePrune >= 256) {
			appliesSincePrune = 0;
			expire();
		}
	}

	/** Drops expired transactions (their native transaction expires on its own) and prunes the tracker. */
	synchronized void expire() {
		long now = System.nanoTime();
		lastExpireNanos = now;
		transactions.values().removeIf(transaction -> {
			if (transaction.isExpired(now)) {
				release(transaction.snapshotIndex);
				if (transaction.keyLock != null) {
					unlockKey(transaction.keyLock);
				}
				return true;
			}
			return false;
		});
		tracker.prune(minSnapshotIndex());
	}

	/** {@link #expire()}, at most every {@link #EXPIRE_EVERY_NANOS}: callers on the request path use this. */
	void expireIfDue() {
		if (System.nanoTime() - lastExpireNanos > EXPIRE_EVERY_NANOS) {
			expire();
		}
	}

	private static final long EXPIRE_EVERY_NANOS = 100_000_000L;
	private static final Executor EXPIRY_DELAY = CompletableFuture.delayedExecutor(EXPIRE_EVERY_NANOS,
			TimeUnit.NANOSECONDS);

	private long minSnapshotIndex() {
		long min = snapshots.isEmpty() ? Long.MAX_VALUE : snapshots.firstLongKey();
		// Writes at or below the oldest snapshot can no longer conflict with anything. Never beyond what is applied:
		// a transaction opened next takes its snapshot at the applied index.
		return Math.min(min, applied.get());
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
		List<CompletableFuture<KeyLock>> waiters = new ArrayList<>();
		synchronized (this) {
			snapshots.clear();
			keyQueues.values().forEach(waiters::addAll);
			keyQueues.clear();
		}
		for (var waiter : waiters) {
			waiter.completeExceptionally(new RocksDBRetryException());
		}
	}
}
