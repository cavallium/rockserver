package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;
import java.util.function.BooleanSupplier;
import java.util.function.LongSupplier;
import java.util.function.Predicate;
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
import org.apache.ratis.util.AwaitForSignal;
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
 * heartbeat then. Its wake signal ({@link WakeSignal}) keeps a wake given while it is not waiting, which Ratis' own
 * signal loses, when that wake would make it send a commit heartbeat.</p>
 *
 * <p>{@link #send} wakes only the appenders whose follower lags below the index and that have no append in flight: a
 * woken appender with appends in flight would send Ratis' heartbeat with commit index 0, extra load on a follower that
 * is already behind. If the stream to a follower breaks before the heartbeat's reply
 * ({@code GrpcLogAppender.resetClient} drops every request in flight, without waking the appender), the appender, which checks every
 * {@link Appender#RESEND_CHECK_MS} while that reply is due, sends the heartbeat again on the new stream, at most
 * {@link CommitWindow#MAX_RESENDS} times per commit index and only within half the minimum election timeout of its first
 * send ({@link CommitWindow}). One that only times out is not sent again: Ratis' request timeout ({@link RaftSettings}: the maximum election timeout) is longer
 * than the interval of Ratis' own heartbeats, which carry the commit index too.</p>
 *
 * <p>The cost is at most one heartbeat per follower per wanted commit index (and per broken stream), and only while
 * follower reads ask for it: an append of new entries carries the commit index by itself, and {@link #send} with an
 * index already wanted only counts a skip. The read index the follower waits for still comes from {@link ReadBarrier};
 * this class only hastens the commit index. {@code RatisContractTest.aCommitHeartbeatSendsTheCommitIndexAtOnce} and
 * {@code aCommitHeartbeatLostWithItsStreamIsSentAgain} pin the Ratis behavior it relies on.</p>
 */
final class CommitHeartbeats {

	private static final org.slf4j.Logger LOG = org.slf4j.LoggerFactory.getLogger(CommitHeartbeats.class);

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
	/**
	 * Tests only: called by an appender that built nothing to send, just before it returns to its loop and waits; the
	 * first call that returns true removes it. Lets a test call {@link #send} exactly in the moment a lost wake would
	 * be lost (R7-1).
	 */
	final AtomicReference<@Nullable Predicate<Appender>> whenNothingToSend = new AtomicReference<>();

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

	/** Tests only: the appenders of this node now. */
	Set<Appender> appenderSet() {
		return Set.copyOf(appenders);
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
			return retryingMissingRegistry(follower.getName(), () -> new Appender(server, state, follower, heartbeats));
		}
	}

	/** How many times {@link #retryingMissingRegistry} makes an appender whose metrics got no registry. */
	static final int APPENDER_ATTEMPTS = 5;

	/**
	 * R9-1: {@code make()}, made again (up to {@link #APPENDER_ATTEMPTS} times) when it fails because its metrics got no
	 * registry. Ratis 3.3.1 has the race of R8-4 ({@link Proposer#resetBrokenProxy}) on the server side too: every
	 * {@code GrpcLogAppender} of a division makes a {@code GrpcServerMetrics} with one shared registry name (the member
	 * id, {@code ratis_grpc}, {@code log_appender}), and the stop of another appender (a restart of an appender of
	 * another follower, a removed follower, the appenders of an earlier term) can unregister that registry between
	 * {@code RefCountingMap.put} and its count, so the new one gets {@code null} and its constructor throws a
	 * {@link NullPointerException} at its first counter. Ratis has then already taken the old appender of that follower
	 * out ({@code LeaderStateImpl.restart} is stop, then add), so the follower would get no appends or heartbeats until
	 * the leadership changes. The constructor has no other effect before that point, and the next attempt makes a new
	 * registry. A {@link NullPointerException} from elsewhere is not retried, except one without a stack trace (see
	 * {@link #fromMetrics}).
	 */
	static <T> T retryingMissingRegistry(String follower, Supplier<T> make) {
		for (int attempt = 1; ; attempt++) {
			try {
				return make.get();
			} catch (NullPointerException ex) {
				if (attempt >= APPENDER_ATTEMPTS || !fromMetrics(ex)) {
					throw ex;
				}
				LOG.warn("{}: the appender's metrics got no registry (a Ratis race), making it again", follower, ex);
			}
		}
	}

	/**
	 * Whether {@code ex} was thrown in Ratis' metrics: a metrics class, or a frame of one, on top of its stack. One
	 * without a stack trace counts too: HotSpot throws a preallocated one with none for an implicit exception at a hot
	 * compiled site ({@code OmitStackTraceInFastThrow}), and {@code -XX:-StackTraceInThrowable} drops every trace. It
	 * cannot be told apart, and a retry is bounded: an NPE from elsewhere fails each attempt and is thrown after the last.
	 */
	static boolean fromMetrics(NullPointerException ex) {
		var stack = ex.getStackTrace();
		if (stack.length == 0) {
			return true;
		}
		for (var frame : stack) {
			var name = frame.getClassName();
			if (name.startsWith("org.apache.ratis.") && name.contains(".metrics.")) {
				return true;
			}
			if (name.equals(GrpcLogAppender.class.getName()) || name.equals(Appender.class.getName())) {
				// Reached the appender's own constructor without a metrics frame: another failure.
				return false;
			}
		}
		return false;
	}

	/** Ratis' gRPC appender that also sends a heartbeat when its follower lags below a wanted commit index. */
	static final class Appender extends GrpcLogAppender {

		/** How often an appender whose commit heartbeat waits for its reply checks that its stream did not break. */
		static final long RESEND_CHECK_MS = 10;

		private final @Nullable CommitHeartbeats heartbeats;
		/**
		 * What went to this follower and when. Only the appender's own thread builds requests
		 * ({@code GrpcLogAppender.appendLog}) and waits ({@code GrpcLogAppender.mayWait}), so it needs no synchronization.
		 */
		private final CommitWindow window = new CommitWindow();
		/** Replaces Ratis' wake signal, which loses a wake given while the appender is not waiting: see {@link WakeSignal}. */
		private final WakeSignal wake;

		Appender(RaftServer.Division server, LeaderState state, FollowerInfo follower,
				@Nullable CommitHeartbeats heartbeats) {
			super(server, state, follower);
			this.wake = new WakeSignal(follower.getName(), this::wantsCommitHeartbeat);
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
				if (window.sentCommit() >= 0 && heartbeats.resendOnNewStream && noStream()) {
					// The stream broke (GrpcLogAppender.resetClient on an error or a completed stream drops every request in
					// flight, and opens a new stream with the next request): what it carried may never have arrived. Sent
					// again a bounded number of times, and only within the window of the first send (see CommitWindow).
					resend = window.streamBroke(System.nanoTime(), windowNanos());
				}
				// Send a heartbeat anyway if the follower must learn a wanted commit index. Not while an append is in
				// flight: Ratis would build the heartbeat without its previous entry, so that it carried commit index 0;
				// the reply to that append wakes this appender again.
				long target = Math.min(getRaftLog().getLastCommittedIndex(), heartbeats.wanted());
				if (target > getFollower().getCommitIndex() && target > window.sentCommit() && !hasPendingDataRequests()) {
					request = super.newAppendEntriesRequest(callId, true);
					if (request != null) {
						heartbeats.heartbeats.incrementAndGet();
						if (resend) {
							heartbeats.resent.incrementAndGet();
						}
					}
				}
			}
			if (request != null) {
				window.sent(request.getLeaderCommit(), System.nanoTime());
			} else if (!heartbeat && heartbeats != null) {
				var hook = heartbeats.whenNothingToSend.get();
				if (hook != null && hook.test(this)) {
					heartbeats.whenNothingToSend.compareAndSet(hook, null);
				}
			}
			return request;
		}

		/**
		 * Whether this appender, woken now, would send a commit heartbeat: its follower lags below the wanted commit
		 * index, no request carrying it went out on the current stream, and no append is in flight. The condition under
		 * which {@link WakeSignal} keeps a wake given while the appender is not waiting. Called from any thread.
		 */
		boolean wantsCommitHeartbeat() {
			if (heartbeats == null || !heartbeats.enabled) {
				return false;
			}
			long target = Math.min(getRaftLog().getLastCommittedIndex(), heartbeats.wanted());
			return target > getFollower().getCommitIndex() && target > window.sentCommit() && !hasPendingDataRequests();
		}

		/** Half the minimum election timeout: by then Ratis' own heartbeat, which carries the commit index, is due. */
		private long windowNanos() {
			return TimeUnit.MILLISECONDS.toNanos(getServer().properties().minRpcTimeoutMs() / 2);
		}

		/**
		 * The wake signal that {@code GrpcLogAppender.mayWait} waits on, and that {@link #notifyLogAppender} (from
		 * {@link CommitHeartbeats#send}, a reply, a commit) signals. Ratis reads it only through this method.
		 */
		@Override
		public AwaitForSignal getEventAwaitForSignal() {
			return wake;
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
			return target > getFollower().getCommitIndex() && window.sentCommit() >= target && !hasPendingDataRequests()
					&& window.open(System.nanoTime(), windowNanos());
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

	/**
	 * What an {@link Appender} sent to its follower for the commit index, and when (R7-2). The window that allows a quick
	 * check of the stream ({@link Appender#RESEND_CHECK_MS}) and a resend after the stream broke opens when a request
	 * first carries a commit index, and a resend does not open it again: before, a resend restarted it, so a stream that
	 * kept ending (gRPC {@code onCompleted} is not limited by Ratis' retry policy, unlike an error) was reopened every
	 * {@link Appender#RESEND_CHECK_MS}, about 100 streams a second, for as long as it kept ending. Now there are at most
	 * {@link #MAX_RESENDS} resends per commit index, all within half the minimum election timeout of its first send;
	 * after that Ratis' own heartbeat carries it.
	 */
	static final class CommitWindow {

		/** The most heartbeats sent again for one commit index. */
		static final int MAX_RESENDS = 3;

		/**
		 * The highest commit index that a request on the current stream carried, -1 after the stream broke. Written by
		 * the appender's thread only, read by others too ({@link Appender#wantsCommitHeartbeat}).
		 */
		private volatile long sentCommit = -1;
		/** The highest commit index ever sent; its first send opened the window. */
		private long windowCommit = -1;
		private long windowStartNanos;
		private int resends;

		/** A request that carried {@code leaderCommit} was built at {@code nanos}. */
		void sent(long leaderCommit, long nanos) {
			if (leaderCommit > sentCommit) {
				sentCommit = leaderCommit;
			}
			if (leaderCommit > windowCommit) {
				windowCommit = leaderCommit;
				windowStartNanos = nanos;
				resends = 0;
			}
		}

		long sentCommit() {
			return sentCommit;
		}

		/** Whether the first send of the highest commit index sent was less than {@code windowNanos} ago. */
		boolean open(long nanos, long windowNanos) {
			return windowCommit >= 0 && nanos - windowStartNanos < windowNanos;
		}

		/**
		 * The stream broke after a request carried {@link #sentCommit}: true if that commit index may be sent again (it
		 * then counts as unsent), false if it was sent again {@link #MAX_RESENDS} times or its window closed.
		 */
		boolean streamBroke(long nanos, long windowNanos) {
			if (sentCommit < 0 || resends >= MAX_RESENDS || !open(nanos, windowNanos)) {
				return false;
			}
			sentCommit = -1;
			resends++;
			return true;
		}
	}

	/**
	 * The wake signal of an {@link Appender}, in place of Ratis' {@link AwaitForSignal} (R7-1). In Ratis 3.3.1
	 * {@code signal()} sets the flag it replaces ({@code signaled.getAndSet(new AtomicBoolean()).set(true)}), and
	 * {@code await} reads the new one, so a wake given while the appender is not waiting (between building its requests
	 * and {@code GrpcLogAppender.mayWait}) is lost: the appender then waits its full wait, a heartbeat interval, and the
	 * commit heartbeat that {@link CommitHeartbeats#send} asked for in that moment goes out only with Ratis' heartbeat.
	 *
	 * <p>Here a wake given while the appender is not waiting is kept, so that the next wait takes it and returns at once,
	 * but only if {@code keepEarlyWake} says the appender would then send a commit heartbeat
	 * ({@link Appender#wantsCommitHeartbeat}). Every other early wake is dropped, as Ratis drops it: Ratis wakes every
	 * appender on every commit and an appender on every reply, and under write load, with the appender at its limit of
	 * appends in flight, each kept wake would buy one more pass of its loop, and that pass an empty heartbeat with commit
	 * index 0 ({@code GrpcLogAppender.run} calls {@code appendLog(haveTooManyPendingRequests())}). Ratis recovers from the
	 * wakes it drops by itself: its wait is 0 while it has entries to send ({@code GrpcLogAppender.getWaitTimeMs}). A
	 * wake given while the appender waits always wakes it.</p>
	 *
	 * <p>{@code keepEarlyWake} is called outside the lock: it reads Ratis state, and the thread that signals may hold a
	 * Ratis lock.</p>
	 */
	static final class WakeSignal extends AwaitForSignal {

		private final ReentrantLock lock = new ReentrantLock();
		private final Condition woken = lock.newCondition();
		private final BooleanSupplier keepEarlyWake;
		private boolean signaled;
		/** Threads in {@link #await}: one, the appender's, or none. */
		private int waiting;
		private final AtomicLong awaits = new AtomicLong();
		private final AtomicLong kept = new AtomicLong();

		WakeSignal(Object name, BooleanSupplier keepEarlyWake) {
			super(name);
			this.keepEarlyWake = keepEarlyWake;
		}

		@Override
		public void signal() {
			lock.lock();
			try {
				if (waiting > 0) {
					signaled = true;
					woken.signalAll();
					return;
				}
			} finally {
				lock.unlock();
			}
			if (!keepEarlyWake.getAsBoolean()) {
				return;
			}
			lock.lock();
			try {
				// The appender may be waiting by now; then this wakes it, which it would want anyway.
				signaled = true;
				kept.incrementAndGet();
				woken.signalAll();
			} finally {
				lock.unlock();
			}
		}

		@Override
		public void await() throws InterruptedException {
			awaits.incrementAndGet();
			lock.lock();
			waiting++;
			try {
				while (!signaled) {
					woken.await();
				}
				signaled = false;
			} finally {
				waiting--;
				lock.unlock();
			}
		}

		@Override
		public boolean await(long time, TimeUnit unit) throws InterruptedException {
			awaits.incrementAndGet();
			lock.lock();
			waiting++;
			try {
				long nanos = unit.toNanos(time);
				while (!signaled) {
					if (nanos <= 0) {
						return false;
					}
					nanos = woken.awaitNanos(nanos);
				}
				signaled = false;
				return true;
			} finally {
				waiting--;
				lock.unlock();
			}
		}

		/** Tests only: calls of {@code await}, to show that Ratis waits on this signal. */
		long awaits() {
			return awaits.get();
		}

		/** Tests only: wakes given while nobody waited that were kept for the next wait. */
		long kept() {
			return kept.get();
		}
	}
}
