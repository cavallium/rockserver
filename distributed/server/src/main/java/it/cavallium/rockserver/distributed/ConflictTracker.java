package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.ByteOrder;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Set;
import java.util.HashSet;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
 * <p>False conflicts are allowed (hash collisions, coarse column markers, entries that failed to append, history
 * forgotten past the cap); missed conflicts are not. Thread-safe.</p>
 */
final class ConflictTracker {

	/** Index of an entry that has not been applied yet: newer than any snapshot. */
	private static final long PENDING = Long.MAX_VALUE;
	/**
	 * Default {@code maxKeys}: about 300 MB of heap, and minutes of history at tens of thousands of written keys per
	 * second, well over the longest transaction lease.
	 */
	static final long DEFAULT_MAX_KEYS = 4_000_000;
	/**
	 * Keys and columns that one {@link #prune} call forgets at most (always at least one entry, if it can forget
	 * one), so that it never holds the lock {@code preAppendTransaction} needs for long. The next calls, one per
	 * applied entry, forget the rest.
	 */
	private static final int PRUNE_BATCH_KEYS = 65_536;

	private static final ValueLayout.OfLong LONG_LE
			= ValueLayout.JAVA_LONG_UNALIGNED.withOrder(ByteOrder.LITTLE_ENDIAN);
	private static final long HASH_SEED = 0x9E3779B97F4A7C15L;

	/**
	 * Every map value is the last entry that wrote that key or column. The key's write index is the index of that
	 * entry, so resolving an entry updates every key it still owns, and a later pending writer stays pending.
	 */
	private final Map<KeyId, Token> keyWriters = new HashMap<>();
	private final Map<Long, Token> columnWriters = new HashMap<>();
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

		private final List<KeyId> keys;
		private final List<Long> columns;
		private final boolean global;
		/** Log index once applied, guarded by the tracker's lock. */
		private long index = PENDING;

		private Token(List<KeyId> keys, List<Long> columns, boolean global) {
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

	private record KeyId(long columnId, long hash) {}

	/**
	 * What the leader knows about a chained commit, see {@link #conflicts(List, Function)}.
	 *
	 * @param columnId column of the key it read
	 * @param hash {@link #keyHash} of the key it read
	 * @param base the entry whose value it read; null if that entry is known to have failed, or is unknown
	 */
	record ChainCheck(long columnId, long hash, @Nullable Token base) {}

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
	synchronized Token register(LogEntry entry) {
		return register(List.of(entry));
	}

	/** Records every key written by {@code entries}, the requests of one log entry, as one pending token. */
	synchronized Token register(List<LogEntry> entries) {
		var keys = new ArrayList<KeyId>();
		var columns = new ArrayList<Long>();
		boolean global = false;
		for (var entry : entries) {
			switch (entry) {
				case LogEntry.Write write -> collectWrite(write, keys, columns);
				case LogEntry.Guarded guarded -> guarded.writes().forEach(write -> collectWrite(write, keys, columns));
				case LogEntry.DeleteColumn deleteColumn -> columns.add(deleteColumn.columnId());
				case LogEntry.CreateColumn _, LogEntry.DeleteColumnIfExists _ -> global = true;
				case LogEntry.UploadMergeOperator _, LogEntry.Genesis _, LogEntry.VerifyColumn _ -> {}
			}
		}
		var token = new Token(keys, columns, global);
		for (KeyId key : keys) {
			keyWriters.put(key, token);
		}
		for (Long column : columns) {
			columnWriters.put(column, token);
		}
		if (global) {
			globalWriter = token;
		}
		pending.add(token);
		return token;
	}

	private static void collectWrite(LogEntry.Write write, List<KeyId> keys, List<Long> columns) {
		long columnId = write.columnId();
		switch (write) {
			case LogEntry.Put put -> keys.add(keyId(columnId, put.keys()));
			case LogEntry.Delete delete -> keys.add(keyId(columnId, delete.keys()));
			case LogEntry.Merge merge -> keys.add(keyId(columnId, merge.keys()));
			case LogEntry.PutMulti putMulti -> putMulti.keys().forEach(k -> keys.add(keyId(columnId, k)));
			case LogEntry.DeleteMulti deleteMulti -> deleteMulti.keys().forEach(k -> keys.add(keyId(columnId, k)));
			case LogEntry.MergeMulti mergeMulti -> mergeMulti.keys().forEach(k -> keys.add(keyId(columnId, k)));
			case LogEntry.DeleteRange _ -> columns.add(columnId);
		}
	}

	private static KeyId keyId(long columnId, Keys keys) {
		return new KeyId(columnId, keyHash(columnId, keys));
	}

	/**
	 * True if an update may read the value written by {@code link} instead of the database: {@code link} is the last
	 * writer of the key, and no pending or applied write of its column or of all columns came after {@code
	 * appliedIndex}. Stricter than {@link #conflicts(List, Function)}: a forgotten {@code link} is not intact.
	 */
	synchronized boolean chainIntact(long columnId, long hash, Token link, long appliedIndex) {
		return !isNewer(globalWriter, appliedIndex)
				&& !isNewer(columnWriters.get(columnId), appliedIndex)
				&& keyWriters.get(new KeyId(columnId, hash)) == link;
	}

	/** True if any conflict key of {@code guarded} (or its column) was written by an entry newer than its snapshot. */
	synchronized boolean conflicts(LogEntry.Guarded guarded) {
		return conflicts(guarded, null);
	}

	/**
	 * {@link #conflicts(LogEntry.Guarded)} for a commit that may be chained. The key a chained update read is judged by
	 * identity: only a writer other than the entry it read from conflicts. Its absence does not: that entry may be
	 * forgotten once applied, and the commit's guard decides anyway. A chained commit without a usable {@code check}
	 * conflicts: its update expired, or the entry it read from failed.
	 */
	private boolean conflicts(LogEntry.Guarded guarded, @Nullable ChainCheck check) {
		if (guarded.guard() != null && (check == null || check.base() == null)) {
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
			var writer = keyWriters.get(new KeyId(columns[i], hashes[i]));
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
	synchronized IntList conflicts(List<LogEntry> entries) {
		return conflicts(entries, _ -> null);
	}

	/**
	 * {@link #conflicts(List)}, with {@code chains} telling what is known of each chained commit, one with a guard.
	 * {@code chains} runs inside this tracker's lock: it must take no other lock.
	 */
	synchronized IntList conflicts(List<LogEntry> entries, Function<LogEntry.Guarded, @Nullable ChainCheck> chains) {
		var conflicting = new IntArrayList();
		Set<KeyId> written = null;
		for (int i = 0; i < entries.size(); i++) {
			var entry = entries.get(i);
			if (entry instanceof LogEntry.Guarded guarded) {
				boolean conflict = conflicts(guarded, guarded.guard() != null ? chains.apply(guarded) : null);
				if (!conflict && written != null) {
					long[] columns = guarded.conflictColumns();
					long[] hashes = guarded.conflictHashes();
					for (int k = 0; k < columns.length && !conflict; k++) {
						conflict = written.contains(new KeyId(columns[k], hashes[k]));
					}
				}
				if (conflict) {
					conflicting.add(i);
					continue;
				}
			}
			if (i + 1 < entries.size()) {
				// Only data writes share a log entry (see Applier.isBatchable), so keys cover everything they write.
				var keys = new ArrayList<KeyId>();
				switch (entry) {
					case LogEntry.Write write -> collectWrite(write, keys, new ArrayList<>());
					case LogEntry.Guarded guarded -> guarded.writes().forEach(w -> collectWrite(w, keys, new ArrayList<>()));
					default -> {}
				}
				if (!keys.isEmpty()) {
					if (written == null) {
						written = new HashSet<>();
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
	 */
	synchronized void prune(long index) {
		prunedThrough = Math.max(prunedThrough, index);
		long forgotten = 0;
		while (!resolved.isEmpty() && forgotten < PRUNE_BATCH_KEYS) {
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
			forgotten += token.weight();
			// Only the keys this token still owns: a later writer of the same key keeps its own state.
			for (KeyId key : token.keys) {
				keyWriters.remove(key, token);
			}
			for (Long column : token.columns) {
				columnWriters.remove(column, token);
			}
			if (globalWriter == token) {
				globalWriter = null;
			}
		}
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
