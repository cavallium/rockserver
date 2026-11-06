package it.cavallium.rockserver.distributed;

/**
 * The time a leader stamps on the entries that hold session writes ({@link LogEntryCodec#stamp}), from which every
 * replica takes the replicated clock that session expiry reads. A leader's wall clock may be wrong: one far ahead
 * would expire every session at once, on every replica, and the clock never goes back. So the stamps of a term start
 * at the leader's wall clock, but at most {@link #MAX_TERM_START_STEP_MILLIS} past the replicated clock this node
 * applied (and not behind it), and then move with the time elapsed on the leader's monotonic clock, not with its wall
 * clock: a wall clock that jumps during the term changes nothing.
 *
 * <p>The cap is applied where the stamp is made, so the log holds the capped time: apply and replay read the same
 * value on every replica, with no rule of their own that a replica of an older version would apply differently. A
 * leader of an older version still stamps its wall clock. The cost: across a change of leader, the clock moves at most
 * {@link #MAX_TERM_START_STEP_MILLIS} past the latest stamp the new leader applied, so a period without session writes
 * that ends with a new leader (a whole cluster stopped for a day, for example) does not count toward expiry. Sessions
 * then live longer, never shorter.</p>
 */
final class LeaderClock {

	/** How far the first stamp of a term may be past the replicated clock. */
	static final long MAX_TERM_START_STEP_MILLIS = 60_000;

	private long term = Long.MIN_VALUE;
	private long baseStamp;
	private long baseNanos;
	private long last;

	/**
	 * The stamp of an entry proposed now in {@code term}, given the replicated clock this node applied
	 * ({@code replicatedClock}, 0 if none yet), its wall clock and its {@link System#nanoTime()}. Never decreasing
	 * within a term.
	 */
	synchronized long stamp(long term, long replicatedClock, long wallMillis, long nanos) {
		if (term != this.term) {
			this.term = term;
			// No clock yet (a new cluster): the wall clock is all there is.
			baseStamp = replicatedClock <= 0 ? wallMillis
					: Math.min(Math.max(wallMillis, replicatedClock), replicatedClock + MAX_TERM_START_STEP_MILLIS);
			baseNanos = nanos;
			last = baseStamp;
			return baseStamp;
		}
		long stamp = baseStamp + Math.max(0, (nanos - baseNanos) / 1_000_000);
		if (stamp < last) {
			stamp = last;
		}
		last = stamp;
		return stamp;
	}
}
