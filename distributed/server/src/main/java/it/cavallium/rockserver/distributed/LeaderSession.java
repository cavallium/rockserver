package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
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
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongConsumer;
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
 *
 * <p>With {@link NodeConfig#pipelinedUpdates()}, a key passes to its next update as soon as the commit of the previous
 * one is admitted to the log ({@link #admitted}), and that update is <em>chained</em>: it reads the value the admitted
 * commit writes, from memory, with no native transaction, under a positive id. Its commit carries a
 * {@link LogEntry.Guard}, which every replica checks at the commit's log position, so that chain state here decides
 * only what is granted: when it is wrong, the cost is a retry. See "Transactions" in the design document.</p>
 */
final class LeaderSession {

	private static final Logger LOG = LoggerFactory.getLogger(LeaderSession.class);

	/** Unsettled commits of one key that may be chained on. More wait: this bounds what one failure can reject. */
	static final int MAX_CHAIN_LINKS = 64;
	/**
	 * Updates that may read one link. A write makes a new link, so only updates that end with a close, which waits for
	 * the link, ever reach this: more wait for the key, so that a stalled apply cannot pile up closes.
	 */
	static final int MAX_LINK_READERS = 64;
	/** Largest value an update may read from memory; a larger write holds its key until it is applied. */
	static final int MAX_LINK_VALUE_BYTES = 1 << 20;

	/**
	 * The right to update one key with {@code getForUpdate}, held from before the read until the update's commit is
	 * admitted to the log (with {@link NodeConfig#pipelinedUpdates()}) or applied, or the update is closed. Released at
	 * most once.
	 */
	static final class KeyLock {
		final long key;
		/** The admitted commit the update should read, instead of the database; null for a head. */
		final @Nullable ChainLink base;
		private final long grantedNanos = System.nanoTime();
		private final AtomicBoolean released = new AtomicBoolean();

		private KeyLock(long key, @Nullable ChainLink base) {
			this.key = key;
			this.base = base;
		}
	}

	/** A commit admitted to the log that handed its key over, from admission until its outcome is known. */
	static final class ChainLink {
		final long columnId;
		final Keys keys;
		final long keyHash;
		/** What the commit writes, null for a delete; from the decoded entry, whose arrays nothing else changes. */
		final @Nullable Buf value;
		/** Index of the commit among the requests of its log entry. */
		final int position;
		final ConflictTracker.Token token;
		/** True if the commit applied, false if not or if that is unknown. Completed by {@link #finish}. */
		final CompletableFuture<Boolean> settled = new CompletableFuture<>();
		/** Guarded by {@link #chainLock}. */
		private boolean finished;
		/** Updates granted to read this link. Guarded by {@link #chainLock}. */
		private int readers;
		private volatile LogEntry.@Nullable Guard guard;

		private ChainLink(long columnId, Keys keys, long keyHash, @Nullable Buf value, int position,
				ConflictTracker.Token token) {
			this.columnId = columnId;
			this.keys = keys;
			this.keyHash = keyHash;
			this.value = value;
			this.position = position;
			this.token = token;
		}

		/** The guard of whoever reads this link's value. Computed on first use, which is never under the log lock. */
		LogEntry.Guard guard() {
			var result = guard;
			if (result == null) {
				result = LogEntry.Guard.of(columnId, keys, value);
				guard = result;
			}
			return result;
		}

		/** True once this commit is known not to have written its value. */
		boolean failed() {
			return !settled.getNow(true);
		}
	}

	/** Who holds one key, and who waits for it in arrival order. Guarded by {@link #chainLock}. */
	private static final class KeyState {
		final long columnId;
		final ArrayDeque<CompletableFuture<KeyLock>> waiters = new ArrayDeque<>();
		/** An update owns the key and its commit was not admitted to the log. */
		boolean held;
		/** Links not settled yet. */
		int links;
		/** The newest usable unsettled link. */
		@Nullable ChainLink tail;
		/** Nobody may chain until the links are settled. Only ever true while {@code links > 0}, so it always clears. */
		boolean broken;

		KeyState(long columnId) {
			this.columnId = columnId;
		}
	}

	/** One open transaction or update. */
	static final class OpenTransaction {
		final long id;
		final boolean update;
		final long snapshotIndex;
		/** The key lock of a single-key update, released when the update is removed. */
		final @Nullable KeyLock keyLock;
		/** The commit a chained update read from: it has no native transaction. Null for every other transaction. */
		final @Nullable ChainLink base;
		/** Its own commit, once admitted to the log and passed its key on. */
		private volatile @Nullable ChainLink link;
		private final AtomicBoolean commitStarted = new AtomicBoolean();
		/** Commits of it in flight ({@link #commitProposed}): it does not expire meanwhile. */
		private final AtomicInteger commitsInFlight =
				new AtomicInteger();
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
				@Nullable ChainLink base, long deadlineNanos) {
			this.id = id;
			this.update = update;
			this.snapshotIndex = snapshotIndex;
			this.keyLock = keyLock;
			this.base = base;
			this.deadlineNanos = deadlineNanos;
		}

		/** True for an update that read from a commit, and has no native transaction. */
		boolean chained() {
			return base != null;
		}

		@Nullable ChainLink link() {
			return link;
		}

		/**
		 * Claims the only commit of this update, false if it was claimed already. So a link has one commit behind it, and
		 * that commit's failure is the only one that can fail the link: a resend of the write must not take down a commit
		 * that may be applying, nor the updates chained on it.
		 */
		boolean startCommit() {
			return commitStarted.compareAndSet(false, true);
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

		/** The commit entry of a transaction: all staged writes, returning true. */
		synchronized LogEntry.Guarded commitEntry(long term) {
			return new LogEntry.Guarded(term, snapshotIndex, conflictColumns.toLongArray(), conflictHashes.toLongArray(),
					writes, false);
		}

		/**
		 * The commit entry of an update: its single final write, returning that write's result. Only a {@code pipelined}
		 * leader needs the entry to carry the update's id, in {@link #admitted}.
		 */
		synchronized LogEntry.Guarded updateEntry(long term, LogEntry.Write write, List<Keys> keys, boolean pipelined) {
			var columns = new LongArrayList(conflictColumns);
			var hashes = new LongArrayList(conflictHashes);
			for (Keys key : keys) {
				columns.add(write.columnId());
				hashes.add(ConflictTracker.keyHash(write.columnId(), key));
			}
			return new LogEntry.Guarded(term, snapshotIndex, columns.toLongArray(), hashes.toLongArray(), List.of(write),
					true, pipelined ? id : 0, base != null ? base.guard() : null);
		}

		boolean isExpired(long nowNanos) {
			return nowNanos - deadlineNanos > 0;
		}

		/**
		 * Its commit was proposed, until {@link #commitEnded}: {@link #expire} leaves it alone meanwhile. Its key lock
		 * belongs to that commit, and the commit's handler removes it from the session once the outcome is known; dropping
		 * it earlier would hand the key to the next update while the commit may still apply.
		 */
		void commitProposed() {
			commitsInFlight.incrementAndGet();
		}

		void commitEnded() {
			commitsInFlight.decrementAndGet();
		}

		boolean commitInFlight() {
			return commitsInFlight.get() > 0;
		}
	}

	private final long term;
	private final boolean pipelined;
	/** How long an update keeps a key: the real cycle of a hot key. */
	private final NodeMetrics.Series keyHold;
	private final NodeMetrics.Series chainLength;
	private final NodeMetrics.Series chainBroken;
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
	 * Guards the key locks. Separate from this object's monitor, which {@link #expire} holds for a whole scan, so that
	 * what runs under Ratis' log lock never waits for that scan. Lock order: this, then this lock, then the tracker's
	 * monitor; Ratis' log lock may come before this one. Nothing holding this lock takes this object's monitor.
	 */
	private final Object chainLock = new Object();
	/**
	 * Single-key updates by key hash ({@link ConflictTracker#keyHash}): a key is in the map while an update holds it or
	 * while it has unsettled links. Guarded by {@link #chainLock}.
	 */
	private final Long2ObjectOpenHashMap<KeyState> keys = new Long2ObjectOpenHashMap<>();
	/** Keys that an update holds now, for metrics. Guarded by {@link #chainLock}. */
	private int heldKeys;
	private long appliesSincePrune;
	/**
	 * Whether {@link #expiryTick} is scheduled: it runs while some key is held after an update queued behind it, so
	 * that an abandoned update releases its key at the end of its lease even when no other request comes. Guarded by
	 * {@link #chainLock}.
	 */
	private boolean expiryScheduled;
	/** Links not finished yet, of every key, so that {@link #close} can finish them. Guarded by {@link #chainLock}. */
	private final Set<ChainLink> unsettled = new HashSet<>();
	private final AtomicLong chainedIds = new AtomicLong(1);
	/** When {@link #expireIfDue} last expired transactions, in {@link System#nanoTime()} units. */
	private volatile long lastExpireNanos = System.nanoTime();
	private volatile boolean closed;

	LeaderSession(long term, AppliedIndex applied) {
		this(term, applied, new ConflictTracker());
	}

	LeaderSession(long term, AppliedIndex applied, ConflictTracker tracker) {
		this(term, applied, tracker, false);
	}

	LeaderSession(long term, AppliedIndex applied, ConflictTracker tracker, boolean pipelined) {
		this(term, applied, tracker, pipelined, null);
	}

	LeaderSession(long term, AppliedIndex applied, ConflictTracker tracker, boolean pipelined,
			@Nullable NodeMetrics metrics) {
		this.term = term;
		this.applied = applied;
		this.tracker = tracker;
		this.pipelined = pipelined;
		this.keyHold = metrics != null ? metrics.duration("update.keyHold") : new NodeMetrics.Series();
		this.chainLength = metrics != null ? metrics.amount("update.chainLength") : new NodeMetrics.Series();
		this.chainBroken = metrics != null ? metrics.amount("update.chainBroken") : new NodeMetrics.Series();
	}

	/** Whether {@link NodeConfig#pipelinedUpdates()} is on. */
	boolean pipelined() {
		return pipelined;
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
	CompletableFuture<KeyLock> lockKey(long key) {
		var waiter = new CompletableFuture<KeyLock>();
		lockKey(key, 0, waiter);
		return waiter;
	}

	/**
	 * {@link #lockKey(long)} with the caller's own future, which it may have made depend on a timeout or a cancellation
	 * before the key is asked for. A future that is complete by the time its turn comes is skipped.
	 */
	void lockKey(long key, long columnId, CompletableFuture<KeyLock> waiter) {
		synchronized (chainLock) {
			if (closed) {
				waiter.completeExceptionally(new RocksDBRetryException());
				return;
			}
			if (waiter.isDone()) {
				return;
			}
			var state = keys.computeIfAbsent(key, _ -> new KeyState(columnId));
			state.waiters.addLast(waiter);
			grantNext(key, state);
			if (!waiter.isDone() && !expiryScheduled) {
				expiryScheduled = true;
				EXPIRY_DELAY.execute(this::expiryTick);
			}
		}
	}

	/**
	 * The only way a key is granted: to the oldest update that still waits, if nobody holds it. With unsettled links,
	 * the update reads the newest one, if the chain is sound: it was not broken, it has room, too many updates are not
	 * reading that link already, and no write came after it. This check also makes a chained close correct: every write
	 * ordered after the link was registered after this grant, so after the update was invoked. Otherwise the waiters
	 * stay queued until the links settle, and the next one is then a head, which reads the database.
	 *
	 * <p>The grant runs under {@link #chainLock}, which appends can wait for, so a waiter's dependent stages must be
	 * cheap: {@link UpdateProtocol} has only its timeout's cancellation and an async stage there.</p>
	 */
	private void grantNext(long key, KeyState state) {
		if (state.held) {
			return;
		}
		if (state.waiters.isEmpty()) {
			if (state.links == 0) {
				keys.remove(key);
			}
			return;
		}
		ChainLink base = null;
		if (state.links > 0) {
			if (state.broken || state.tail == null || state.links >= MAX_CHAIN_LINKS
					|| state.tail.readers >= MAX_LINK_READERS) {
				return;
			}
			if (!tracker.chainIntact(state.columnId, key, state.tail.token, applied.get())) {
				breakChain(state);
				return;
			}
			base = state.tail;
		}
		CompletableFuture<KeyLock> next;
		while ((next = state.waiters.pollFirst()) != null) {
			// A waiter that timed out or was cancelled is already complete: skip it.
			if (next.complete(new KeyLock(key, base))) {
				state.held = true;
				heldKeys++;
				if (base != null) {
					base.readers++;
				}
				return;
			}
		}
		if (state.links == 0) {
			keys.remove(key);
		}
	}

	/**
	 * Hands over the keys of the updates whose commits {@code requests}, the requests of one log entry, were just
	 * admitted as {@code token}, and returns the new links, null if none. Called under Ratis' log lock, in log order.
	 *
	 * <p>Only a commit that is one put or delete of the locked key gives a link: nobody could read the value of any
	 * other. Its update keeps the key until it is removed, as without pipelining.</p>
	 */
	@Nullable List<ChainLink> admitted(List<Request> requests, ConflictTracker.Token token) {
		if (!pipelined) {
			return null;
		}
		List<ChainLink> links = null;
		for (int position = 0; position < requests.size(); position++) {
			if (!(requests.get(position).entry() instanceof LogEntry.Guarded commit) || commit.updateId() == 0) {
				continue;
			}
			var transaction = transactions.get(commit.updateId());
			if (transaction == null || transaction.keyLock == null) {
				continue;
			}
			var link = linkOf(commit, position, token, transaction.keyLock.key);
			if (link == null) {
				continue;
			}
			synchronized (chainLock) {
				// The release and the link together: whoever fails to release the lock then sees the link.
				if (closed || !transaction.keyLock.released.compareAndSet(false, true)) {
					continue;
				}
				var state = keys.get(link.keyHash);
				if (state == null) {
					continue;
				}
				keyHold.since(transaction.keyLock.grantedNanos);
				transaction.link = link;
				unsettled.add(link);
				state.links++;
				chainLength.record(state.links);
				state.tail = state.broken ? null : link;
				state.held = false;
				heldKeys--;
				grantNext(link.keyHash, state);
			}
			if (links == null) {
				links = new ArrayList<>();
			}
			links.add(link);
		}
		return links;
	}

	private static @Nullable ChainLink linkOf(LogEntry.Guarded commit, int position, ConflictTracker.Token token,
			long key) {
		Keys keys;
		Buf value;
		switch (commit.writes().getFirst()) {
			case LogEntry.Put put when put.value() != null -> {
				keys = put.keys();
				value = put.value();
			}
			case LogEntry.Delete delete -> {
				keys = delete.keys();
				value = null;
			}
			default -> {
				return null;
			}
		}
		long columnId = commit.writes().getFirst().columnId();
		if ((value != null && value.size() > MAX_LINK_VALUE_BYTES) || ConflictTracker.keyHash(columnId, keys) != key) {
			return null;
		}
		return new ChainLink(columnId, keys, key, value, position, token);
	}

	/** Called once the entry of {@code links} was applied, with its results: the commits' outcomes are now known. */
	void settle(List<ChainLink> links, List<ApplyResult> results) {
		for (var link : links) {
			finish(link, results.get(link.position) instanceof ApplyResult.Ok);
		}
	}

	/** The commit of {@code link} ended without an outcome: unknown counts as failed. */
	void linkFailed(ChainLink link) {
		finish(link, false);
	}

	private void finish(ChainLink link, boolean ok) {
		synchronized (chainLock) {
			if (link.finished) {
				return;
			}
			link.finished = true;
			unsettled.remove(link);
			var state = keys.get(link.keyHash);
			if (state != null) {
				state.links--;
				if (!ok && state.links > 0) {
					breakChain(state);
				}
				if (state.tail == link) {
					state.tail = null;
				}
				if (state.links == 0) {
					state.broken = false;
				}
				grantNext(link.keyHash, state);
			}
		}
		// Outside the lock: a closed update's reply goes on from here.
		link.settled.complete(ok);
	}

	/** Stops chaining on {@code key} until its links settle, so that the next update is a head. */
	void breakChain(long key) {
		synchronized (chainLock) {
			var state = keys.get(key);
			if (state != null && state.links > 0) {
				breakChain(state);
			}
		}
	}

	private void breakChain(KeyState state) {
		if (!state.broken) {
			chainBroken.record(1);
		}
		state.broken = true;
		state.tail = null;
	}

	/**
	 * What the tracker needs to validate a chained commit, null if it must conflict. Called inside the tracker's lock,
	 * so it takes no lock.
	 */
	ConflictTracker.@Nullable ChainCheck chainCheck(LogEntry.Guarded commit) {
		var transaction = transactions.get(commit.updateId());
		var base = transaction != null ? transaction.base : null;
		if (base == null || base.failed()) {
			return null;
		}
		return new ConflictTracker.ChainCheck(base.columnId, base.keyHash, base.token);
	}

	/** A new id for a chained update: positive, while native ids are at most -2, and unique across terms. */
	long nextChainedId() {
		return (term & 0x7FFFFFFFL) << 32 | (chainedIds.getAndIncrement() & 0xFFFFFFFFL);
	}

	/** Unsettled links of every key, for metrics. */
	int chainLinks() {
		synchronized (chainLock) {
			return unsettled.size();
		}
	}

	/** Expires what is due, and runs again later while a key is held and leadership lasts. */
	private void expiryTick() {
		expireIfDue();
		synchronized (chainLock) {
			if (closed || keys.isEmpty()) {
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

	/** Keys held by an update, for metrics; not those that only wait for their links to settle. */
	int lockedKeys() {
		synchronized (chainLock) {
			return heldKeys;
		}
	}

	/**
	 * Called after the transaction commit {@code commit} conflicted: stops chaining on its conflict keys that have
	 * unsettled links, and returns true if there was one. Its snapshot was older than those links, so no wait could save
	 * it, but on a key that chains without a pause, no transaction ever opened after the last link: the pause until the
	 * links settle is the room its retry needs, and it costs updates of that key only while transactions collide with
	 * them.
	 */
	boolean yieldChainsTo(LogEntry.Guarded commit) {
		boolean yielded = false;
		synchronized (chainLock) {
			for (long key : commit.conflictHashes()) {
				var state = keys.get(key);
				if (state != null && state.links > 0) {
					breakChain(state);
					yielded = true;
				}
			}
		}
		return yielded;
	}

	/** Releases {@code lock}, passing the key to the next waiting update that still waits. */
	void unlockKey(KeyLock lock) {
		synchronized (chainLock) {
			if (!lock.released.compareAndSet(false, true)) {
				return;
			}
			keyHold.since(lock.grantedNanos);
			var state = keys.get(lock.key);
			if (state == null) {
				return;
			}
			state.held = false;
			heldKeys--;
			grantNext(lock.key, state);
		}
	}

	OpenTransaction register(Object reservation, long id, boolean update, @Nullable KeyLock keyLock, long leaseNanos) {
		return register(reservation, id, update, keyLock, null, leaseNanos);
	}

	synchronized OpenTransaction register(Object reservation, long id, boolean update, @Nullable KeyLock keyLock,
			@Nullable ChainLink base, long leaseNanos) {
		if (closed) {
			unpin(reservation);
			throw new IllegalStateException("Leadership of term " + term + " ended");
		}
		Long snapshotIndex = pins.get(reservation);
		if (snapshotIndex == null) {
			throw new IllegalStateException("Unknown reservation");
		}
		var transaction = new OpenTransaction(id, update, snapshotIndex, keyLock, base, System.nanoTime() + leaseNanos);
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

	/**
	 * Drops expired transactions (their native transaction expires on its own) and prunes the tracker. A transaction
	 * whose commit is in flight stays: the commit's handler removes it.
	 */
	synchronized void expire() {
		long now = System.nanoTime();
		lastExpireNanos = now;
		transactions.values().removeIf(transaction -> {
			if (transaction.isExpired(now) && !transaction.commitInFlight()) {
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
		seal();
		drain(db);
	}

	/** {@link #close(EmbeddedDB)}, with what rolls back a native update and a native transaction. */
	void close(LongConsumer rollBackUpdate, LongConsumer rollBackTransaction) {
		seal();
		drain(rollBackUpdate, rollBackTransaction);
	}

	/**
	 * The first half of {@link #close}: from here on no transaction registers and no update queues for a key. Cheap,
	 * no native work: Ratis calls it holding its server's monitor (see {@code RockserverStateMachine.retire}).
	 */
	void seal() {
		synchronized (this) {
			closed = true;
		}
	}

	/** The second half of {@link #close}, after {@link #seal()}, on any thread: see {@link #drain(LongConsumer, LongConsumer)}. */
	void drain(EmbeddedDB db) {
		drain(id -> db.closeFailedUpdate(id), id -> db.closeTransaction(id, false));
	}

	/**
	 * Answers the updates waiting for a key with {@link RocksDBRetryException} and the unsettled links with false, then
	 * rolls back every open transaction. The waiters go first: a rollback takes the native transaction's monitor, which
	 * a read or write in that transaction holds for a whole native call, as long as a stalled disk makes it last.
	 */
	void drain(LongConsumer rollBackUpdate, LongConsumer rollBackTransaction) {
		answerWaiters();
		for (OpenTransaction transaction : transactions.values()) {
			if (transaction.chained()) {
				continue;
			}
			try {
				if (transaction.update) {
					rollBackUpdate.accept(transaction.id);
				} else {
					rollBackTransaction.accept(transaction.id);
				}
			} catch (RuntimeException ex) {
				LOG.debug("Failed to roll back transaction {} of term {}", transaction.id, term, ex);
			}
		}
		transactions.clear();
		synchronized (this) {
			snapshots.clear();
		}
	}
	/**
	 * The first part of {@link #drain(LongConsumer, LongConsumer)}: answers the updates waiting for a key with
	 * {@link RocksDBRetryException} and the unsettled links with false. No native work, so it may run where the
	 * rollbacks must not (see {@code RockserverStateMachine.retire}); a second call finds nothing to answer.
	 */
	void answerWaiters() {
		if (!closed) {
			throw new IllegalStateException("drain() before seal()");
		}
		List<CompletableFuture<KeyLock>> waiters = new ArrayList<>();
		List<ChainLink> links;
		synchronized (chainLock) {
			keys.values().forEach(state -> waiters.addAll(state.waiters));
			keys.clear();
			heldKeys = 0;
			links = List.copyOf(unsettled);
			unsettled.clear();
			links.forEach(link -> link.finished = true);
		}
		for (var waiter : waiters) {
			waiter.completeExceptionally(new RocksDBRetryException());
		}
		// A commit may still apply, but nobody here will hear of it.
		links.forEach(link -> link.settled.complete(false));
	}
}
