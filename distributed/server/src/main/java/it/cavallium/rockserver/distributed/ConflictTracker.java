package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.Keys;

/**
 * Leader-side record of which keys were written by which log entries, used to validate optimistic transactions.
 *
 * <p>Only the leader that appends entries uses it. {@link #register} and {@link #conflicts} are called from
 * {@code StateMachine.preAppendTransaction}, which Ratis invokes under its append lock in log order, so they see
 * every earlier entry of this leadership. A registered entry is <em>pending</em> (treated as newer than any
 * snapshot) until {@link #resolve} records its log index at apply time. A {@link LogEntry.DeleteRange}, a column
 * deletion or a column creation marks the whole column as written.</p>
 *
 * <p>False conflicts are allowed (hash collisions, coarse column markers, pending entries that never resolve);
 * missed conflicts are not. Thread-safe.</p>
 */
final class ConflictTracker {

	/** Opaque handle for one registered entry. */
	static final class Token {
		private Token() {}
	}

	/** 64-bit hash of a logical key: column id plus every key segment with its length. */
	static long keyHash(long columnId, Keys keys) {
		throw new UnsupportedOperationException("TODO");
	}

	/**
	 * Records every key written by {@code entry} (for {@link LogEntry.Guarded}, by its writes) as pending.
	 * Entries that write no data (e.g. {@link LogEntry.Genesis}) return a token that resolves nothing.
	 */
	Token register(LogEntry entry) {
		throw new UnsupportedOperationException("TODO");
	}

	/** True if any conflict key of {@code guarded} (or its column) was written by an entry newer than its snapshot. */
	boolean conflicts(LogEntry.Guarded guarded) {
		throw new UnsupportedOperationException("TODO");
	}

	/** Marks the entry of {@code token} as applied at {@code index}, unless a later entry wrote the same key. */
	void resolve(Token token, long index) {
		throw new UnsupportedOperationException("TODO");
	}

	/** Forgets resolved writes at or below {@code index}: no open transaction has a snapshot older than that. */
	void prune(long index) {
		throw new UnsupportedOperationException("TODO");
	}

	/** Number of tracked keys and columns, for tests and metrics. */
	int size() {
		throw new UnsupportedOperationException("TODO");
	}
}
