package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.LongSupplier;
import java.util.function.Supplier;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.grpc.server.GrpcLogAppender;
import org.apache.ratis.proto.RaftProtos.AppendEntriesRequestProto;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.rpc.RpcFactory;
import org.apache.ratis.rpc.RpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.leader.FollowerInfo;
import org.apache.ratis.server.leader.LeaderState;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.util.LifeCycle;
import org.jetbrains.annotations.Nullable;

/**
 * Makes the leader send its commit index to every follower now, rather than with its next append or heartbeat.
 *
 * <p>A follower learns the commit index only from the leader's appends. Ratis 3.3.1 wakes an appender whose follower
 * lags in commit index on every commit and on every reply ({@code GrpcLogAppender.run} checks
 * {@code isFollowerCommitBehindLastCommitIndex}), but {@code LogAppenderBase.newAppendEntriesRequest(callId, false)}
 * builds nothing when there is no new entry and no heartbeat is due, so nothing is sent until the next heartbeat (a
 * quarter to a half of the minimum election timeout). A follower that serves a read ({@link FollowerReads}) must apply
 * up to the read index, and cannot apply entries it does not know are committed; on a quiet cluster it would wait for
 * that heartbeat.</p>
 *
 * <p>So the node's Raft server uses this class' appender ({@link Rpc}, set by {@link RaftSettings}: Ratis' gRPC
 * transport with {@link Appender} instead of {@link GrpcLogAppender}). When a follower read needs commit index
 * {@code c}, the leader calls {@link #send}, which records {@code c} as wanted and wakes its appenders. An appender with
 * nothing else to send then sends a heartbeat, which carries the commit index, if its follower is known to lag below
 * the wanted index, no request carrying that index went to it yet, and no append to it waits for a reply. The last
 * condition is the one that a heartbeat requested from Ratis ({@code LogAppender.triggerHeartbeat}, which its
 * linearizable read path calls) cannot meet: Ratis builds a heartbeat sent while an append is in flight without its
 * previous entry ({@code LogAppenderBase.newAppendEntriesRequest}), so it carries commit index 0
 * ({@code ServerImplUtils.effectiveCommitIndex}) and tells the follower nothing; the follower then waited a full
 * heartbeat interval. This appender instead waits for that append's reply, which wakes it again, and sends the
 * heartbeat then.</p>
 *
 * <p>The cost is at most one heartbeat per follower per wanted commit index, and only while follower reads ask for it:
 * an append of new entries carries the commit index by itself, and {@link #send} with an index already wanted only
 * counts a skip. The read index the follower waits for still comes from {@link ReadBarrier}; this class only hastens
 * the commit index. {@code RatisContractTest.aCommitHeartbeatSendsTheCommitIndexAtOnce} pins the Ratis behavior it
 * relies on.</p>
 */
final class CommitHeartbeats {

	/** The {@link Parameters} key under which a node's Raft server finds its instance of this class. */
	static final String PARAMETER = CommitHeartbeats.class.getName();

	private final LongSupplier commitIndex;
	/** The highest commit index that a follower read asked for, -1 if none. */
	private final AtomicLong wanted = new AtomicLong(-1);
	/** The appenders of this node while it leads: one per follower. */
	private final Set<Appender> appenders = ConcurrentHashMap.newKeySet();
	private final AtomicLong sent = new AtomicLong();
	private final AtomicLong skipped = new AtomicLong();
	private final AtomicLong heartbeats = new AtomicLong();
	/** Tests only: false sends nothing, as before this class existed. */
	volatile boolean enabled = true;

	CommitHeartbeats(Supplier<@Nullable RaftServer> server, RaftGroupId groupId) {
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
	}

	/** Tests only: the commit index given. */
	CommitHeartbeats(LongSupplier commitIndex) {
		this.commitIndex = commitIndex;
	}

	/** Asks the local leader to send its commit index to its followers; does nothing on a node that is not the leader. */
	void send() {
		if (!enabled) {
			return;
		}
		long commit = commitIndex.getAsLong();
		if (commit < 0 || wanted.getAndAccumulate(commit, Math::max) >= commit) {
			skipped.incrementAndGet();
			return;
		}
		sent.incrementAndGet();
		for (var appender : appenders) {
			appender.notifyLogAppender();
		}
	}

	/** The highest commit index that a follower read asked for, -1 if none. */
	long wanted() {
		return wanted.get();
	}

	/** Calls of {@link #send} that asked for a higher commit index. */
	long sent() {
		return sent.get();
	}

	/** Calls of {@link #send} that asked for nothing new: no commit index yet, or one already asked for. */
	long skipped() {
		return skipped.get();
	}

	/** Heartbeats that the appenders sent only to carry a wanted commit index. */
	long heartbeats() {
		return heartbeats.get();
	}

	/** The appenders of this node now: one per follower while it leads, none otherwise. */
	int appenders() {
		return appenders.size();
	}

	/**
	 * Ratis' gRPC transport with {@link Appender} as the leader's log appender. Ratis finds it by class name
	 * ({@code raft.rpc.type}, see {@link RpcType#valueOf}), so it is public, with a public constructor without arguments.
	 */
	public static final class Rpc implements RpcType {

		public Rpc() {}

		@Override
		public String name() {
			return Rpc.class.getName();
		}

		@Override
		public RpcFactory newFactory(Parameters parameters) {
			return new Factory(parameters);
		}
	}

	/** Ratis' gRPC factory, but with {@link Appender}. A server built without {@link #PARAMETER} sends no extra heartbeat. */
	static final class Factory extends GrpcFactory {

		private final @Nullable CommitHeartbeats heartbeats;

		Factory(Parameters parameters) {
			super(parameters);
			this.heartbeats = parameters.get(PARAMETER, CommitHeartbeats.class);
		}

		@Override
		public LogAppender newLogAppender(RaftServer.Division server, LeaderState state, FollowerInfo follower) {
			return new Appender(server, state, follower, heartbeats);
		}
	}

	/** Ratis' gRPC appender that also sends a heartbeat when its follower lags below a wanted commit index. */
	static final class Appender extends GrpcLogAppender {

		private final @Nullable CommitHeartbeats heartbeats;
		/**
		 * The highest commit index that a request built for this follower carried. Only the appender's own thread builds
		 * requests ({@code GrpcLogAppender.appendLog}), so it needs no synchronization.
		 */
		private long sentCommit = -1;

		Appender(RaftServer.Division server, LeaderState state, FollowerInfo follower,
				@Nullable CommitHeartbeats heartbeats) {
			super(server, state, follower);
			this.heartbeats = heartbeats;
			if (heartbeats != null) {
				heartbeats.appenders.add(this);
			}
		}

		@Override
		public AppendEntriesRequestProto newAppendEntriesRequest(long callId, boolean heartbeat)
				throws RaftLogIOException {
			var request = super.newAppendEntriesRequest(callId, heartbeat);
			if (request == null && !heartbeat && heartbeats != null) {
				// Nothing to send: no new entry, and no heartbeat due. Send one anyway if the follower must learn a wanted
				// commit index. Not while an append is in flight: Ratis would build the heartbeat without its previous
				// entry, so that it carried commit index 0; the reply to that append wakes this appender again.
				long target = Math.min(getRaftLog().getLastCommittedIndex(), heartbeats.wanted());
				if (target > getFollower().getCommitIndex() && target > sentCommit && !hasPendingDataRequests()) {
					request = super.newAppendEntriesRequest(callId, true);
					if (request != null) {
						heartbeats.heartbeats.incrementAndGet();
					}
				}
			}
			if (request != null) {
				sentCommit = Math.max(sentCommit, request.getLeaderCommit());
			}
			return request;
		}

		@Override
		public CompletableFuture<LifeCycle.State> stopAsync() {
			if (heartbeats != null) {
				heartbeats.appenders.remove(this);
			}
			return super.stopAsync();
		}
	}
}
