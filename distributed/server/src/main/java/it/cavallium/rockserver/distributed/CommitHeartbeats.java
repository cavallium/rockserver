package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Function;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ratis.client.impl.OrderedAsync;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes the leader send its commit index to every follower now, rather than with its next append or heartbeat.
 *
 * <p>A follower learns the commit index only from the leader's appends: Ratis notifies its appenders when the commit
 * index moves, but an appender with no new entries sends nothing until the next heartbeat (a quarter to a half of the
 * minimum election timeout). A follower that serves a read ({@link FollowerReads}) must apply up to the read index,
 * and cannot apply entries it does not know are committed; on a quiet cluster it would wait for that heartbeat.</p>
 *
 * <p>Ratis 3.3.1 offers no public call that makes the leader's appenders send a heartbeat
 * ({@code LogAppender.triggerHeartbeat()} is public, but the appenders are reachable only through package-private
 * classes). Its linearizable read path does it: with {@code raft.server.read.option=LINEARIZABLE} and
 * {@code raft.server.read.leader.heartbeat-check.enabled=true} (both set by {@link RaftSettings}, pinned by
 * {@code RatisContractTest}), a read on the leader asks its appenders for a heartbeat round, unless a round already
 * confirmed a commit index at least this high. Each heartbeat carries the leader's commit index at the time it is sent.
 * So the leader submits a read with Ratis' dummy message, which skips the state machine
 * ({@code RaftServerImpl.isDummyRead}), and ignores its reply. That read is only a trigger: the read index the follower
 * waits for still comes from {@link ReadBarrier}, never from Ratis' heartbeat confirmation, which has the ReadIndex
 * reuse bug that made this design use its own barrier (see the design document).</p>
 *
 * <p>The trigger rides on that path anyway, so it is kept small: at most one trigger in flight per commit index (a
 * second one would confirm nothing more), and at most {@link #MAX_IN_FLIGHT} in all. A dummy read completes only once
 * the leader applied up to its read index, so while the leader's apply lags they stay pending; without the bound, one
 * per follower read would pile up there. A follower whose read finds the bound reached waits for the next append or
 * heartbeat, as without this class; under a lagging apply, writes flow and appends carry the commit index anyway.</p>
 */
final class CommitHeartbeats {

	private static final Logger LOG = LoggerFactory.getLogger(CommitHeartbeats.class);
	/** Triggers in flight at most, over all commit indexes. */
	static final int MAX_IN_FLIGHT = 4;

	private final LongSupplier commitIndex;
	private final Function<RaftClientRequest, CompletableFuture<RaftClientReply>> submit;
	private final Supplier<@Nullable RaftServer> server;
	private final RaftGroupId groupId;
	private final ClientId clientId = ClientId.randomId();
	private final AtomicLong callIds = new AtomicLong();
	/** The highest commit index that a trigger in flight covers, -1 if none: guarded by this. */
	private long covered = -1;
	/** Guarded by this. */
	private int inFlight;
	private final AtomicLong sent = new AtomicLong();
	private final AtomicLong skipped = new AtomicLong();
	/** Tests only: false sends nothing, as before this class existed. */
	volatile boolean enabled = true;

	CommitHeartbeats(Supplier<@Nullable RaftServer> server, RaftGroupId groupId) {
		this.server = server;
		this.groupId = groupId;
		this.commitIndex = () -> {
			var raftServer = server.get();
			if (raftServer == null) {
				return -1;
			}
			try {
				return raftServer.getDivision(groupId).getRaftLog().getLastCommittedIndex();
			} catch (IOException e) {
				return -1;
			}
		};
		this.submit = request -> {
			var raftServer = server.get();
			if (raftServer == null) {
				return CompletableFuture.failedFuture(new IOException("No Raft server"));
			}
			try {
				return raftServer.submitClientRequestAsync(request);
			} catch (IOException e) {
				return CompletableFuture.failedFuture(e);
			}
		};
	}

	/** Tests only: the commit index and the submission given. */
	CommitHeartbeats(LongSupplier commitIndex, Function<RaftClientRequest, CompletableFuture<RaftClientReply>> submit,
			RaftGroupId groupId) {
		this.server = () -> null;
		this.groupId = groupId;
		this.commitIndex = commitIndex;
		this.submit = submit;
	}

	/** Asks the local leader to send its commit index to its followers; does nothing on a node that is not the leader. */
	void send() {
		if (!enabled) {
			return;
		}
		long commit = commitIndex.getAsLong();
		if (commit < 0) {
			return;
		}
		synchronized (this) {
			if (commit <= covered || inFlight >= MAX_IN_FLIGHT) {
				skipped.incrementAndGet();
				return;
			}
			covered = commit;
			inFlight++;
		}
		sent.incrementAndGet();
		CompletableFuture<RaftClientReply> reply;
		try {
			var request = RaftClientRequest.newBuilder()
					.setClientId(clientId)
					.setServerId(org.apache.ratis.protocol.RaftPeerId.valueOf(serverId()))
					.setGroupId(groupId)
					.setCallId(callIds.incrementAndGet())
					.setMessage(OrderedAsync.DUMMY)
					.setType(RaftClientRequest.readRequestType())
					.build();
			reply = submit.apply(request);
		} catch (RuntimeException e) {
			reply = CompletableFuture.failedFuture(e);
		}
		reply.whenComplete((result, error) -> {
			synchronized (this) {
				inFlight--;
				if (covered == commit) {
					// Done: a later read of the same commit index may trigger again (a follower that missed this round).
					covered = -1;
				}
			}
			if (error != null || (result != null && !result.isSuccess())) {
				// Not the leader any more, or a closing server: the follower waits for the next append instead.
				LOG.debug("Commit heartbeat request failed", error != null ? error : result.getException());
			}
		});
	}

	private String serverId() {
		var raftServer = server.get();
		return raftServer != null ? raftServer.getId().toString() : "test";
	}

	/** Triggers sent. */
	long sent() {
		return sent.get();
	}

	/** Calls of {@link #send} that sent nothing: one in flight covered the commit index, or the bound was reached. */
	long skipped() {
		return skipped.get();
	}
}
