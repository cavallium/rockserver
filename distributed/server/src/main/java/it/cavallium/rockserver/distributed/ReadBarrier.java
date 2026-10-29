package it.cavallium.rockserver.distributed;

import java.io.Closeable;
import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;

/**
 * Linearizable-read barrier for the leader, built on public Ratis APIs only.
 *
 * <p>Stock Ratis 3.3.1 ReadIndex can reuse a leadership confirmation obtained before the read arrived
 * ({@code ReadIndexHeartbeats.addAppendEntriesListener} returns null when the commit index did not move), so a
 * deposed leader may serve stale reads. This barrier instead:</p>
 * <ol>
 *   <li>requires this division to be the leader and ready (it committed an entry of its own term), and records
 *       the commit index {@code c} when {@link #await} is called;</li>
 *   <li>joins the next confirmation round that starts after the call: every voting peer of the current (and,
 *       during a membership change, previous) configuration is asked for its current term via
 *       {@link TermService}; the round succeeds when a majority of each configuration, counting this node,
 *       reports this leader's term; any reply with a higher term fails it;</li>
 *   <li>waits until the local applied index reaches {@code c}.</li>
 * </ol>
 * <p>Concurrent calls share rounds. A round costs one network round trip, no fsync.</p>
 */
final class ReadBarrier implements Closeable {

	/** Thrown (as the future's failure) when this node is not, or no longer, a ready leader. */
	static final class NotLeaderException extends Exception {
		NotLeaderException(String message) {
			super(message);
		}
	}

	ReadBarrier(RaftServer server, RaftGroupId groupId, AppliedIndex applied, Duration roundTimeout) {
		throw new UnsupportedOperationException("TODO");
	}

	/**
	 * Completes when every write committed before this call is visible in the local database and this node was
	 * the leader after this call. Fails with {@link NotLeaderException} or {@link java.util.concurrent.TimeoutException}.
	 */
	CompletableFuture<Void> await() {
		throw new UnsupportedOperationException("TODO");
	}

	@Override
	public void close() {
		throw new UnsupportedOperationException("TODO");
	}
}
