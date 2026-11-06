package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.longs.Long2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.longs.LongArrayList;
import it.unimi.dsi.fastutil.longs.LongOpenHashSet;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.PriorityQueue;
import java.util.function.Function;
import org.jetbrains.annotations.Nullable;

/**
 * Leader-side record of which keys were written by which log entries, used to validate optimistic transactions.
 *
 * <p>Only the leader that appends entries uses it. {@link #register} and {@link #conflicts} are called from
 * {@code StateMachine.preAppendTransaction}, which Ratis invokes under its append lock in log order, so they see
 * every earlier entry of this leadership. A registered entry is <em>pending</em> (treated as newer than any
 * snapshot) until {@link #resolve} records its log index at apply time. A {@link LogEntry.DeleteRange}, a column
 * deletion or a column creation marks the whole column as written.</p>
 *
 * <p>Ratis can still reject an entry after {@code preAppendTransaction} registered it (for example when the entry
 * exceeds the log buffer size), without telling the state machine. Such an entry would stay pending for good, so
 * resolving an entry also settles every entry registered before it: those precede it in the log, or were never
 * appended and write nothing.</p>
 *
 * <p>The history is bounded. Once resolved entries hold more than {@code maxKeys} keys and columns, {@link #prune}
 * forgets the oldest ones even if an open transaction's snapshot still needs them, and that transaction then
 * conflicts at commit. A long or abandoned transaction costs a false conflict instead of unbounded heap.</p>
 *
 * <p>Keys are tracked by their {@link #keyHash} alone, in a primitive long-keyed open-addressing map: the hash
 * already folds in the column id, and two keys (of any columns) with the same hash only share a slot, which can
 * cause a false conflict, never a missed one. Registering a key allocates no object per key.</p>
 *
 * <p>False conflicts are allowed (hash collisions, coarse column markers, entries that failed to append, history
 * forgotten past the cap); missed conflicts are not. Thread-safe.</p>
 */
final class ConflictTracker {

	/** Index of an entry that has not been applied yet: newer than any snapshot. */
	private static final long PENDING = Long.MAX_VALUE;
	/**
	 * Default {@code maxKeys}: about 130 MB of heap, and minutes of history at tens of thousands of written keys per
	 * second, well over the longest transaction lease.
	 */
	static final long DEFAULT_MAX_KEYS = 4_000_000;
	/**
	 * Keys and columns that one {@link #prune} call forgets at most (always at least one entry, if it can forget
	 * one), so that it never holds the lock {@code preAppendTransaction} needs for long. The next calls, one per
	 * applied entry, forget the rest.
	 */
	private static final int PRUNE_BATCH_KEYS = 65_536;
	/**
	 * Keys and columns that {@link #prune} forgets while holding the lock once (at least one entry): it releases the
	 * lock between such chunks, so {@code preAppendTransaction} waits for one chunk at most, not for a whole call.
	 */
	private static final int PRUNE_CHUNK_KEYS = 4_096;

	private static final ValueLayout.OfLong LONG_LE
			= ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
	private static final long HASH_SEED = 0x9E3779B97F4A7C15L;

	/**
	 * Every map value is the last entry that wrote that key or column. The key's write index is the index of that
	 * entry, so resolving an entry updates every key it still owns, and a later pending writer stays pending.
	 */
	private final Long2ObjectOpenHashMap<Token> keyWriters = new Long2ObjectOpenHashMap<>();
	private final Long2ObjectOpenHashMap<Token> columnWriters = new Long2ObjectOpenHashMap<>();
	/** Last column creation or name-based column deletion: its column id is unknown, so it covers every column. */
	private @Nullable Token globalWriter;
	/** Highest index whose older writes may have been forgotten. */
	private long prunedThrough = -1;
	/** Resolved entries, oldest index first, so that {@link #prune} only visits what it can drop. */
	private final PriorityQueue<Token> resolved = new PriorityQueue<>(Comparator.comparingLong(token -> token.index));
	/** Pending entries in registration order, which is log order. */
	private final ArrayDeque<Token> pending = new ArrayDeque<>();
	private final long maxKeys;
	/** Keys and columns held by the entries in {@link #resolved}, overwritten ones included. */
	private long resolvedKeys;

	ConflictTracker() {
		this(DEFAULT_MAX_KEYS);
	}

	/** {@code maxKeys}: how many keys and columns resolved entries may hold before history is forgotten early. */
	ConflictTracker(long maxKeys) {
		if (maxKeys <= 0) {
			throw new IllegalArgumentException("maxKeys must be positive");
		}
		this.maxKeys = maxKeys;
	}

	/** Opaque handle for one registered entry. */
	static final class Token {

		/** {@link #keyHash} of every key written, duplicates included. */
		private final LongArrayList keys;
		private final LongArrayList columns;
		private final boolean global;
		/** Log index once applied, guarded by the tracker's lock. */
		private long index = PENDING;

		private Token(LongArrayList keys, LongArrayList columns, boolean global) {
			this.keys = keys;
			this.columns = columns;
			this.global = global;
		}

		private boolean isEmpty() {
			return keys.isEmpty() && columns.isEmpty() && !global;
		}

		private int weight() {
			return keys.size() + columns.size() + (global ? 1 : 0);
		}
	}

	/**
	 * What the leader knows about a chained commit, see {@link #conflicts(List, Function)}.
	 *
	 * @param columnId column of the key it read
	 * @param hash {@link #keyHash} of the key it read
	 * @param base the entry whose value it read
	 */
	record ChainCheck(long columnId, long hash, Token base) {
		ChainCheck {
			java.util.Objects.requireNonNull(base, "base");
		}
	}

	/** 64-bit hash of a logical key: column id plus every key segment with its length. */
	static long keyHash(long columnId, Keys keys) {
		Buf[] segments = keys.keys();
		long hash = mix(HASH_SEED ^ columnId);
		hash = mix(hash ^ segments.length);
		for (Buf segment : segments) {
			MemorySegment bytes = segment.asMemorySegment();
			long size = bytes.byteSize();
			hash = mix(hash ^ size);
			long offset = 0;
			for (; offset + Long.BYTES <= size; offset += Long.BYTES) {
				hash = mix(hash ^ bytes.get(LONG_LE, offset));
			}
			if (offset < size) {
				long tail = 0;
				for (int shift = 0; offset < size; offset++, shift += Byte.SIZE) {
					tail |= (bytes.get(ValueLayout.JAVA_BYTE, offset) & 0xFFL) << shift;
				}
				hash = mix(hash ^ tail);
			}
		}
		return hash;
	}

	/** The murmur3 64-bit finalizer: a bijection where every input bit affects every output bit. */
	private static long mix(long value) {
		value ^= value >>> 33;
		value *= 0xFF51AFD7ED558CCDL;
		value ^= value >>> 33;
		value *= 0xC4CEB9FE1A85EC53L;
		value ^= value >>> 33;
		return value;
	}

	/**
	 * Records every key written by {@code entry} (for {@link LogEntry.Guarded}, by its writes) as pending.
	 * Entries that write no data (e.g. {@link LogEntry.Genesis}) return a token that resolves nothing.
	 */
	Token register(LogEntry entry) {
		return register(List.of(entry));
	}

	/**
	 * Records every key written by {@code entries}, the requests of one log entry, as one pending token. The keys are
	 * hashed before the lock is taken: only the map updates hold it.
	 */
	Token register(List<LogEntry> entries) {
		var keys = new LongArrayList();
		var columns = new LongArrayList();
		boolean global = false;
		for (var entry : entries) {
			switch (entry) {
				case LogEntry.Write write -> collectWrite(write, keys, columns);
				case LogEntry.Guarded guarded -> guarded.writes().forEach(write -> collectWrite(write, keys, columns));
				case LogEntry.DeleteColumn deleteColumn -> columns.add(deleteColumn.columnId());
				case LogEntry.CreateColumn _, LogEntry.DeleteColumnIfExists _ -> global = true;
				case LogEntry.UploadMergeOperator _, LogEntry.Genesis _, LogEntry.VerifyColumn _,
						LogEntry.RegisterClient _ -> {}
			}
		}
		var token = new Token(keys, columns, global);
		synchronized (this) {
			long[] keyArray = keys.elements();
			for (int i = 0, n = keys.size(); i < n; i++) {
				keyWriters.put(keyArray[i], token);
			}
			long[] columnArray = columns.elements();
			for (int i = 0, n = columns.size(); i < n; i++) {
				columnWriters.put(columnArray[i], token);
			}
			if (global) {
				globalWriter = token;
			}
			pending.add(token);
		}
		return token;
	}

	private static void collectWrite(LogEntry.Write write, LongArrayList keys, LongArrayList columns) {
		long columnId = write.columnId();
		switch (write) {
			case LogEntry.Put put -> keys.add(keyHash(columnId, put.keys()));
			case LogEntry.Delete delete -> keys.add(keyHash(columnId, delete.keys()));
			case LogEntry.Merge merge -> keys.add(keyHash(columnId, merge.keys()));
			case LogEntry.PutMulti putMulti -> collectKeys(columnId, putMulti.keys(), keys);
			case LogEntry.DeleteMulti deleteMulti -> collectKeys(columnId, deleteMulti.keys(), keys);
			case LogEntry.MergeMulti mergeMulti -> collectKeys(columnId, mergeMulti.keys(), keys);
			case LogEntry.DeleteRange _ -> columns.add(columnId);
		}
	}

	private static void collectKeys(long columnId, List<Keys> keyList, LongArrayList keys) {
		keys.ensureCapacity(keys.size() + keyList.size());
		for (Keys k : keyList) {
			keys.add(keyHash(columnId, k));
		}
	}

	/**
	 * True if an update may read the value written by {@code link} instead of the database: {@code link} is the last
	 * writer of the key, and no pending or applied write of its column or of all columns came after {@code
	 * appliedIndex}. Stricter than {@link #conflicts(List, Function)}: a forgotten {@code link} is not intact.
	 */
	synchronized boolean chainIntact(long columnId, long hash, Token link, long appliedIndex) {
		return !isNewer(globalWriter, appliedIndex)
				&& !isNewer(columnWriters.get(columnId), appliedIndex)
				&& keyWriters.get(hash) == link;
	}

	/** True if any conflict key of {@code guarded} (or its column) was written by an entry newer than its snapshot. */
	synchronized boolean conflicts(LogEntry.Guarded guarded) {
		return conflicts(guarded, null);
	}

	/**
	 * {@link #conflicts(LogEntry.Guarded)} for a commit that may be chained. The key a chained update read is judged by
	 * identity: only a writer other than the entry it read from conflicts. Its absence does not: that entry may be
	 * forgotten once applied, and the commit's guard decides anyway. A chained commit without a {@code check} conflicts:
	 * its update expired, or the entry it read from failed.
	 */
	private boolean conflicts(LogEntry.Guarded guarded, @Nullable ChainCheck check) {
		if (guarded.guard() != null && check == null) {
			return true;
		}
		long snapshotIndex = guarded.snapshotIndex();
		if (snapshotIndex < prunedThrough) {
			// Writes after this snapshot may have been forgotten already: it cannot be validated, so it conflicts.
			return true;
		}
		if (isNewer(globalWriter, snapshotIndex)) {
			return true;
		}
		long[] columns = guarded.conflictColumns();
		long[] hashes = guarded.conflictHashes();
		for (int i = 0; i < columns.length; i++) {
			if (isNewer(columnWriters.get(columns[i]), snapshotIndex)) {
				return true;
			}
			var writer = keyWriters.get(hashes[i]);
			if (check != null && columns[i] == check.columnId() && hashes[i] == check.hash()) {
				if (writer != null && writer != check.base()) {
					return true;
				}
			} else if (isNewer(writer, snapshotIndex)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * The positions of the {@link LogEntry.Guarded} entries among {@code entries} (the requests of one log entry, in
	 * order) that conflict: with a write tracked here, or with a write of an earlier entry of the same log entry that
	 * does not conflict itself, since that one is applied first. Without such a position, every entry can be
	 * appended and registered as one; with one, nothing is registered.
	 */
	IntList conflicts(List<LogEntry> entries) {
		return conflicts(entries, _ -> null);
	}

	/**
	 * {@link #conflicts(List)}, with {@code chains} telling what is known of each chained commit, one with a guard.
	 * {@code chains} runs inside this tracker's lock: it must take no other lock.
	 */
	IntList conflicts(List<LogEntry> entries, Function<LogEntry.Guarded, @Nullable ChainCheck> chains) {
		// Only commits can conflict: an entry of plain writes needs no lock here. The keys that a commit may conflict
		// with inside the entry are hashed before the lock is taken.
		int lastGuarded = -1;
		for (int i = 0; i < entries.size(); i++) {
			if (entries.get(i) instanceof LogEntry.Guarded) {
				lastGuarded = i;
			}
		}
		if (lastGuarded < 0) {
			return new IntArrayList();
		}
		var writes = new ArrayList<LongArrayList>(lastGuarded);
		var ignoredColumns = new LongArrayList();
		for (int i = 0; i < lastGuarded; i++) {
			// Only data writes share a log entry (see Applier.isBatchable), so keys cover everything they write.
			var keys = new LongArrayList();
			switch (entries.get(i)) {
				case LogEntry.Write write -> collectWrite(write, keys, ignoredColumns);
				case LogEntry.Guarded guarded -> guarded.writes().forEach(w -> collectWrite(w, keys, ignoredColumns));
				default -> {}
			}
			writes.add(keys);
		}
		synchronized (this) {
			return conflicts(entries, lastGuarded, writes, chains);
		}
	}

	private IntList conflicts(List<LogEntry> entries, int lastGuarded, List<LongArrayList> writes,
			Function<LogEntry.Guarded, @Nullable ChainCheck> chains) {
		assert Thread.holdsLock(this);
		var conflicting = new IntArrayList();
		LongOpenHashSet written = null;
		for (int i = 0; i <= lastGuarded; i++) {
			var entry = entries.get(i);
			if (entry instanceof LogEntry.Guarded guarded) {
				boolean conflict = conflicts(guarded, guarded.guard() != null ? chains.apply(guarded) : null);
				if (!conflict && written != null) {
					long[] hashes = guarded.conflictHashes();
					for (int k = 0; k < hashes.length && !conflict; k++) {
						conflict = written.contains(hashes[k]);
					}
				}
				if (conflict) {
					conflicting.add(i);
					continue;
				}
			}
			if (i < lastGuarded) {
				var keys = writes.get(i);
				if (!keys.isEmpty()) {
					if (written == null) {
						written = new LongOpenHashSet();
					}
					written.addAll(keys);
				}
			}
		}
		return conflicting;
	}

	private static boolean isNewer(@Nullable Token writer, long snapshotIndex) {
		return writer != null && writer.index > snapshotIndex;
	}

	/**
	 * Marks the entry of {@code token} as applied at {@code index}, unless a later entry wrote the same key.
	 * Resolving a token again has no effect.
	 *
	 * <p>Entries registered before this one and still pending are settled as if applied at {@code index - 1}: they
	 * are earlier in the log, or their append failed. A snapshot at or after {@code index - 1} already contains
	 * them.</p>
	 */
	synchronized void resolve(Token token, long index) {
		if (token.index != PENDING) {
			return;
		}
		Token earliest;
		do {
			earliest = pending.removeFirst();
			settle(earliest, earliest == token ? index : index - 1);
		} while (earliest != token);
	}

	private void settle(Token token, long index) {
		token.index = index;
		if (!token.isEmpty()) {
			resolved.add(token);
			resolvedKeys += token.weight();
		}
	}

	/**
	 * Forgets resolved writes at or below {@code index}: no open transaction has a snapshot older than that. A
	 * transaction whose snapshot is older than a pruned index is then always reported as conflicting, whatever
	 * pinned or forgot it, so pruning too eagerly can only cause false conflicts.
	 *
	 * <p>While resolved entries hold more than {@code maxKeys} keys and columns, it also forgets the oldest entries
	 * above {@code index}, and raises the pruned index to theirs. One call forgets about {@link #PRUNE_BATCH_KEYS}
	 * keys. Entries at or below the pruned index that are left for a later call make nothing conflict: every
	 * snapshot that is still validated is at or above that index.</p>
	 *
	 * <p>It takes the lock once per {@link #PRUNE_CHUNK_KEYS} keys, not once for the whole call: the leader's
	 * {@code preAppendTransaction} (under Ratis' append lock) and the completer thread calling this after every
	 * applied entry then never wait on each other for more than one chunk. The same rule as between two calls makes
	 * this safe.</p>
	 */
	void prune(long index) {
		long forgotten = 0;
		boolean first = true;
		while (forgotten < PRUNE_BATCH_KEYS) {
			long chunk;
			synchronized (this) {
				if (first) {
					prunedThrough = Math.max(prunedThrough, index);
					first = false;
				}
				chunk = pruneChunk(Math.min(PRUNE_CHUNK_KEYS, PRUNE_BATCH_KEYS - forgotten));
			}
			if (chunk == 0) {
				break;
			}
			forgotten += chunk;
		}
	}

	/**
	 * Forgets resolved entries that {@link #prune} may drop, about {@code budget} keys and columns (at least one
	 * entry, if one can be dropped), and returns how many it forgot: zero once nothing more can be dropped now.
	 * Between two chunks every structure is consistent: an entry is forgotten whole, and only after {@link
	 * #prunedThrough} covers it.
	 */
	private long pruneChunk(long budget) {
		assert Thread.holdsLock(this);
		long forgotten = 0;
		while (!resolved.isEmpty() && forgotten < budget) {
			Token token = resolved.peek();
			if (token.index > prunedThrough) {
				if (resolvedKeys <= maxKeys) {
					break;
				}
				// Over the cap: a transaction with an older snapshot will conflict instead of being validated.
				prunedThrough = token.index;
			}
			resolved.poll();
			resolvedKeys -= token.weight();
			// Count an empty-weight token too, so that the loop always makes progress.
			forgotten += Math.max(1, token.weight());
			// Only the keys this token still owns: a later writer of the same key keeps its own state.
			long[] keyArray = token.keys.elements();
			for (int i = 0, n = token.keys.size(); i < n; i++) {
				keyWriters.remove(keyArray[i], token);
			}
			long[] columnArray = token.columns.elements();
			for (int i = 0, n = token.columns.size(); i < n; i++) {
				columnWriters.remove(columnArray[i], token);
			}
			if (globalWriter == token) {
				globalWriter = null;
			}
		}
		return forgotten;
	}

	/** Highest index whose older writes may have been forgotten, for tests. */
	synchronized long prunedThrough() {
		return prunedThrough;
	}

	/** Keys and columns held by resolved entries, overwritten ones included, for tests. */
	synchronized long resolvedKeys() {
		return resolvedKeys;
	}

	/** Number of tracked keys and columns, for tests and metrics. */
	synchronized int size() {
		return keyWriters.size() + columnWriters.size() + (globalWriter != null ? 1 : 0);
	}
}
