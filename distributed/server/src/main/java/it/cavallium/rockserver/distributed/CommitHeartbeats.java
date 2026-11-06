package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
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
 * <p>A follower learns the commit index only from the leader's appends. Ratis 3.3.1 wakes every appender on every
 * commit and on every change of a follower's commit index ({@code LeaderStateImpl.updateCommit} and
 * {@code onFollowerCommitIndex} call {@code commitIndexChanged}, which calls {@code notifySenders}) and an appender on
 * every reply to it, and {@code GrpcLogAppender.run} then tries to send when its follower lags in commit index
 * ({@code isFollowerCommitBehindLastCommitIndex}), but {@code LogAppenderBase.newAppendEntriesRequest(callId, false)}
 * builds nothing when there is no new entry and no heartbeat is due, so nothing is sent until the next heartbeat (a
 * quarter to a half of the minimum election timeout). A follower that serves a read ({@link FollowerReads}) must apply
 * up to the read index, and cannot apply entries it does not know are committed; on a quiet cluster it would wait for
 * that heartbeat.</p>
 *
 * <p>So the node's Raft server uses this class' appender ({@link Rpc}, set by {@link RaftSettings}: Ratis' gRPC
 * transport with {@link Appender} instead of {@link GrpcLogAppender}). When a follower read needs commit index
 * {@code c}, the leader calls {@link #send}, which records {@code c} as wanted and wakes its appenders. An appender with
 * nothing else to send then sends a heartbeat, which carries the commit index, if its follower is known to lag below
 * the wanted index, no request carrying that index went to it on its current stream, and no append to it waits for a
 * reply. The last condition is the one that a heartbeat requested from Ratis ({@code LogAppender.triggerHeartbeat}, which its
 * linearizable read path calls) cannot meet: Ratis builds a heartbeat sent while an append is in flight without its
 * previous entry ({@code LogAppenderBase.newAppendEntriesRequest}), so it carries commit index 0
 * ({@code ServerImplUtils.effectiveCommitIndex}) and tells the follower nothing; the follower then waited a full
 * heartbeat interval. This appender instead waits for that append's reply, which wakes it again, and sends the
 * heartbeat then.</p>
 *
 * <p>{@link #send} wakes only the appenders whose follower lags below the index and that have no append in flight: a
 * woken appender with appends in flight would send Ratis' heartbeat with commit index 0, extra load on a follower that
 * is already behind. If the stream to a follower breaks before the heartbeat's reply
 * ({@code GrpcLogAppender.resetClient} drops every request in flight, without waking the appender), the appender, which checks every
 * {@link Appender#RESEND_CHECK_MS} while that reply is due, sends the heartbeat again on the new stream. One that only
 * times out is not sent again: Ratis' request timeout ({@link RaftSettings}: the maximum election timeout) is longer
 * than the interval of Ratis' own heartbeats, which carry the commit index too.</p>
 *
 * <p>The cost is at most one heartbeat per follower per wanted commit index (and per broken stream), and only while
 * follower reads ask for it: an append of new entries carries the commit index by itself, and {@link #send} with an
 * index already wanted only counts a skip. The read index the follower waits for still comes from {@link ReadBarrier};
 * this class only hastens the commit index. {@code RatisContractTest.aCommitHeartbeatSendsTheCommitIndexAtOnce} and
 * {@code aCommitHeartbeatLostWithItsStreamIsSentAgain} pin the Ratis behavior it relies on.</p>
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
	private final AtomicLong woken = new AtomicLong();
	private final AtomicLong resent = new AtomicLong();
	/** Tests only: false sends nothing, as before this class existed. */
	volatile boolean enabled = true;
	/** Tests only: false does not send a heartbeat again after its stream broke. */
	volatile boolean resendOnNewStream = true;

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
			// Only an appender that would send the heartbeat now. One with an append in flight would build a heartbeat
			// with commit index 0 if woken while it has too many in flight (GrpcLogAppender.run calls appendLog(true)),
			// which tells its follower nothing; the reply to that append wakes it anyway, and it sends the heartbeat then.
			if (appender.lagsBelow(commit)) {
				woken.incrementAndGet();
				appender.notifyLogAppender();
			}
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

	/** Appenders that {@link #send} woke: those whose follower lagged and had no append in flight. */
	long woken() {
		return woken.get();
	}

	/** Heartbeats sent again because the stream that carried an earlier one broke before its reply. */
	long resent() {
		return resent.get();
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

		/** How often an appender whose commit heartbeat waits for its reply checks that its stream did not break. */
		static final long RESEND_CHECK_MS = 10;

		private final @Nullable CommitHeartbeats heartbeats;
		/**
		 * The highest commit index that a request built for this follower on its current stream carried, -1 after the
		 * stream broke. Only the appender's own thread builds requests ({@code GrpcLogAppender.appendLog}), so it needs
		 * no synchronization.
		 */
		private long sentCommit = -1;
		/** When {@link #sentCommit} last rose, in {@link System#nanoTime()} units. */
		private long sentCommitNanos;

		Appender(RaftServer.Division server, LeaderState state, FollowerInfo follower,
				@Nullable CommitHeartbeats heartbeats) {
			super(server, state, follower);
			this.heartbeats = heartbeats;
			if (heartbeats != null) {
				heartbeats.appenders.add(this);
			}
		}

		/** Whether this appender would send a heartbeat for {@code commit} if woken now. */
		boolean lagsBelow(long commit) {
			return getFollower().getCommitIndex() < commit && !hasPendingDataRequests();
		}

		@Override
		public AppendEntriesRequestProto newAppendEntriesRequest(long callId, boolean heartbeat)
				throws RaftLogIOException {
			var request = super.newAppendEntriesRequest(callId, heartbeat);
			if (request == null && !heartbeat && heartbeats != null) {
				// Nothing to send: no new entry, and no heartbeat due.
				boolean resend = false;
				if (sentCommit >= 0 && heartbeats.resendOnNewStream && noStream()) {
					// The stream broke (GrpcLogAppender.resetClient on an error or a completed stream drops every request in
					// flight, and opens a new stream with the next request): what it carried may never have arrived.
					sentCommit = -1;
					resend = true;
				}
				// Send a heartbeat anyway if the follower must learn a wanted commit index. Not while an append is in
				// flight: Ratis would build the heartbeat without its previous entry, so that it carried commit index 0;
				// the reply to that append wakes this appender again.
				long target = Math.min(getRaftLog().getLastCommittedIndex(), heartbeats.wanted());
				if (target > getFollower().getCommitIndex() && target > sentCommit && !hasPendingDataRequests()) {
					request = super.newAppendEntriesRequest(callId, true);
					if (request != null) {
						heartbeats.heartbeats.incrementAndGet();
						if (resend) {
							heartbeats.resent.incrementAndGet();
						}
					}
				}
			}
			if (request != null && request.getLeaderCommit() > sentCommit) {
				sentCommit = request.getLeaderCommit();
				sentCommitNanos = System.nanoTime();
			}
			return request;
		}

		/**
		 * Ratis' wait, but at most {@link #RESEND_CHECK_MS} while a request that carried the wanted commit index waits for
		 * its reply. Ratis wakes the appender on a reply but not when its stream breaks ({@code resetClient} only clears
		 * the requests in flight), so without this the appender would notice a broken stream only when Ratis' own
		 * heartbeat is due. Called only from the appender's thread ({@code GrpcLogAppender.mayWait}).
		 */
		@Override
		public long getWaitTimeMs() {
			long wait = super.getWaitTimeMs();
			if (heartbeats != null && heartbeats.resendOnNewStream && wait > RESEND_CHECK_MS && awaitsCommitReply()) {
				return RESEND_CHECK_MS;
			}
			return wait;
		}

		/**
		 * Whether a request carrying the wanted commit index went out, its follower has not confirmed it yet, and no
		 * append is in flight (an appender woken with appends in flight may send Ratis' heartbeat with commit index 0).
		 * Only for half the minimum election timeout after that request: by then Ratis' own heartbeat is due anyway.
		 */
		private boolean awaitsCommitReply() {
			long target = Math.min(getRaftLog().getLastCommittedIndex(), heartbeats.wanted());
			return target > getFollower().getCommitIndex() && sentCommit >= target && !hasPendingDataRequests()
					&& System.nanoTime() - sentCommitNanos
							< TimeUnit.MILLISECONDS.toNanos(getServer().properties().minRpcTimeoutMs() / 2);
		}

		/**
		 * Whether this appender has no stream to its follower: {@code GrpcLogAppender.shouldSendAppendEntries} is
		 * {@code appendLogRequestObserver == null || LogAppenderBase.shouldSendAppendEntries()}, and the latter is a
		 * heartbeat trigger (which makes the heartbeat wait 0), new entries, or a due heartbeat. With none of those, the
		 * stream is missing. Called only from the appender's thread, which is the only one that opens a stream.
		 */
		private boolean noStream() {
			return shouldSendAppendEntries() && !hasAppendEntries() && getHeartbeatWaitTimeMs() > 0;
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
