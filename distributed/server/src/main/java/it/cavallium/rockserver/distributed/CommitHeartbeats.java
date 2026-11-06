package it.cavallium.rockserver.distributed;

import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Supplier;
import org.apache.ratis.client.impl.OrderedAsync;
import org.apache.ratis.protocol.ClientId;
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
 * classes). Its linearizable read path does it: with {@code raft.server.read.option=LINEARIZABLE} and the default
 * {@code raft.server.read.leader.heartbeat-check}, a read on the leader asks its appenders for a heartbeat round, unless
 * a round already confirmed a commit index at least this high. Each heartbeat carries the leader's commit index at the
 * time it is sent. So the leader submits a read with Ratis' dummy message, which skips the state machine
 * ({@code RaftServerImpl.isDummyRead}), and ignores its reply. That read is only a trigger: the read index the follower
 * waits for still comes from {@link ReadBarrier}, never from Ratis' heartbeat confirmation (see the design document).</p>
 */
final class CommitHeartbeats {

	private static final Logger LOG = LoggerFactory.getLogger(CommitHeartbeats.class);

	private final Supplier<@Nullable RaftServer> server;
	private final RaftGroupId groupId;
	private final ClientId clientId = ClientId.randomId();
	private final AtomicLong callIds = new AtomicLong();
	/** Tests only: false sends nothing, as before this class existed. */
	volatile boolean enabled = true;

	CommitHeartbeats(Supplier<@Nullable RaftServer> server, RaftGroupId groupId) {
		this.server = server;
		this.groupId = groupId;
	}

	/** Asks the local leader to send its commit index to its followers; does nothing on a node that is not the leader. */
	void send() {
		var raftServer = server.get();
		if (!enabled || raftServer == null) {
			return;
		}
		try {
			var request = RaftClientRequest.newBuilder()
					.setClientId(clientId)
					.setServerId(raftServer.getId())
					.setGroupId(groupId)
					.setCallId(callIds.incrementAndGet())
					.setMessage(OrderedAsync.DUMMY)
					.setType(RaftClientRequest.readRequestType())
					.build();
			raftServer.submitClientRequestAsync(request).whenComplete((reply, error) -> {
				if (error != null || (reply != null && !reply.isSuccess())) {
					// Not the leader any more, or a closing server: the follower waits for the next append instead.
					LOG.debug("Commit heartbeat request failed", error != null ? error : reply.getException());
				}
			});
		} catch (Exception e) {
			LOG.debug("Commit heartbeat request failed", e);
		}
	}
}
