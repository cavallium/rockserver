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
 * <p>False conflicts are allowed (hash collisions, coarse column markers, entries that failed to append);
 * missed conflicts are not. Thread-safe.</p>
 */
final class ConflictTracker {

	/** Index of an entry that has not been applied yet: newer than any snapshot. */
	private static final long PENDING = Long.MAX_VALUE;

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
	}

	private record KeyId(long columnId, long hash) {}

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

	/** True if any conflict key of {@code guarded} (or its column) was written by an entry newer than its snapshot. */
	synchronized boolean conflicts(LogEntry.Guarded guarded) {
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
			if (isNewer(columnWriters.get(columns[i]), snapshotIndex)
					|| isNewer(keyWriters.get(new KeyId(columns[i], hashes[i])), snapshotIndex)) {
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
		var conflicting = new IntArrayList();
		Set<KeyId> written = null;
		for (int i = 0; i < entries.size(); i++) {
			var entry = entries.get(i);
			if (entry instanceof LogEntry.Guarded guarded) {
				boolean conflict = conflicts(guarded);
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
		}
	}

	/**
	 * Forgets resolved writes at or below {@code index}: no open transaction has a snapshot older than that. A
	 * transaction whose snapshot is older than a pruned index is then always reported as conflicting, whatever
	 * pinned or forgot it, so pruning too eagerly can only cause false conflicts.
	 */
	synchronized void prune(long index) {
		prunedThrough = Math.max(prunedThrough, index);
		while (!resolved.isEmpty() && resolved.peek().index <= index) {
			Token token = resolved.poll();
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

	/** Number of tracked keys and columns, for tests and metrics. */
	synchronized int size() {
		return keyWriters.size() + columnWriters.size() + (globalWriter != null ? 1 : 0);
	}
}
