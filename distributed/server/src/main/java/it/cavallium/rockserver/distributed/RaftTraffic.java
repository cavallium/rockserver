package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import java.util.function.ToLongBiFunction;
import java.util.function.ToLongFunction;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCall;
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Watches the Raft requests that reach this node's Raft server: when a leader last sent it anything (an append, a
 * heartbeat, a snapshot chunk), including the appends that bring a new peer up to date before {@code add-peer} makes it
 * a member. Ratis shows that time only for a running follower ({@code RoleInfoProto}), not for a node in its
 * initializing state, so the membership check that runs while a node hears no leader
 * ({@link ReplicatedNode}'s {@code checkWhileDetached}) reads it here.
 *
 * <p>Installed on the Raft gRPC server as an interceptor (through {@link TermService#customizer}); it reads the
 * method name, and forwards every message unchanged, but for snapshot chunks. A chunk whose leader's term is below the
 * node's current term (a deposed leader's) it answers NOT_LEADER itself, as Ratis would, since Ratis would still take
 * that chunk's configuration (round 15 review, N-R15-1). A chunk whose configuration is older than the node's newest
 * committed one, or than the newest configuration in its log of the chunk's leader term, comes from an install stream
 * its leader gave up on (R16-2): it fails that stream with
 * FAILED_PRECONDITION, since Ratis would answer it ALREADY_INSTALLED, or install it, and take its configuration either
 * way. Before it forwards any other chunk, it records the chunk's
 * configuration with the leader's term ({@link InstalledConfiguration}, round-13 review, finding 1), whatever its term:
 * Ratis takes the configuration of a chunk it rejects too (round 15), and the node's term may rise between this check
 * and Ratis'. If the record cannot be written, it fails the install stream with UNAVAILABLE instead of forwarding the
 * chunk (R14-3), and tells Ratis' handler of the end as a cancel: Ratis never takes that configuration, and the
 * leader, told at once, starts the install again after its retry wait (Ratis' leader never sends a single chunk
 * again). The record is synced on the gRPC listener thread of that stream only (Raft's
 * gRPC server has no executor of its own: the listeners of a call run one at a time on gRPC's shared pool), outside
 * Ratis' server lock, and only when it changes: about two syncs per install; Ratis' own handler writes each chunk
 * on that same thread.</p>
 */
final class RaftTraffic implements ServerInterceptor {

	private static final Logger LOG = LoggerFactory.getLogger(RaftTraffic.class);

	private static final String APPEND_ENTRIES = RaftServerProtocolServiceGrpc.getAppendEntriesMethod()
			.getFullMethodName();
	private static final String INSTALL_SNAPSHOT = RaftServerProtocolServiceGrpc.getInstallSnapshotMethod()
			.getFullMethodName();

	/**
	 * Tests only: drops a Raft request (append, vote, snapshot chunk, ...) that reaches node {@code receiver} when it
	 * returns true: Ratis never sees it. A unary request (a vote) times out at its sender, which sends it again; on a
	 * stream (appends, snapshot chunks) the follower sees the gap at the next message (a snapshot chunk of an unexpected
	 * index fails the stream) or the leader's request times out, and the leader starts over (appends from the next
	 * index it learns, a snapshot install from its first chunk). Static, for in-process clusters.
	 */
	static volatile @Nullable BiPredicate<String, Object> dropForTesting;
	/**
	 * Tests only: the current term that node {@code receiver} sees for a snapshot chunk, instead of its division's;
	 * {@link #NO_TERM}: its division's. Static, for in-process clusters.
	 */
	static volatile @Nullable ToLongBiFunction<String, RaftProtos.InstallSnapshotRequestProto> termForTesting;
	/**
	 * Tests only: the record of a snapshot chunk's configuration at node {@code receiver} fails when it returns true, as
	 * a write error would. Static, for in-process clusters.
	 */
	static volatile @Nullable BiPredicate<String, RaftProtos.InstallSnapshotRequestProto> recordFailureForTesting;

	/** What the current-term function answers when it does not know the term. */
	static final long NO_TERM = -1;
	/** What the configuration floor answers when it knows no floor. */
	static final long NO_INDEX = -1;

	/**
	 * The index below which a snapshot chunk's configuration shows a stream its leader gave up on (R16-2): see
	 * {@link MembershipFacts#installFloor}.
	 */
	@FunctionalInterface
	interface ConfigurationFloor {
		/**
		 * The floor for a chunk of a leader of {@code leaderTerm} to this node's division of {@code group}, {@link #NO_INDEX}
		 * if none is known (no such division, or the server is not built yet).
		 */
		long floor(RaftGroupId group, long leaderTerm);
	}

	private final String nodeId;
	/** Where {@link InstalledConfiguration} is kept: the Raft directory; null: not recorded (tests). */
	private final @Nullable Path raftDir;
	/** See the constructor. */
	private final ToLongFunction<RaftGroupId> currentTerm;
	/** See the constructor. */
	private final ConfigurationFloor configurationFloor;
	private final NodeMetrics.@Nullable Series recordFailures;
	/** {@link System#nanoTime} of the last leader request, 0 if none yet. */
	private final AtomicLong lastLeaderRequestNanos = new AtomicLong();

	RaftTraffic(String nodeId) {
		this(nodeId, null);
	}

	RaftTraffic(String nodeId, @Nullable Path raftDir) {
		this(nodeId, raftDir, _ -> NO_TERM, null);
	}

	/**
	 * @param currentTerm the current term of this node's division of a group, {@link #NO_TERM} if unknown (no such
	 *     division, or the server is not built yet)
	 * @param recordFailures counts the chunks whose configuration could not be recorded (null: not counted)
	 */
	RaftTraffic(String nodeId, @Nullable Path raftDir, ToLongFunction<RaftGroupId> currentTerm,
			NodeMetrics.@Nullable Series recordFailures) {
		this(nodeId, raftDir, currentTerm, (_, _) -> NO_INDEX, recordFailures);
	}

	/**
	 * @param currentTerm the current term of this node's division of a group, {@link #NO_TERM} if unknown (no such
	 *     division, or the server is not built yet)
	 * @param configurationFloor the index below which a chunk's configuration shows an abandoned stream (R16-2)
	 * @param recordFailures counts the chunks whose configuration could not be recorded (null: not counted)
	 */
	RaftTraffic(String nodeId, @Nullable Path raftDir, ToLongFunction<RaftGroupId> currentTerm,
			ConfigurationFloor configurationFloor, NodeMetrics.@Nullable Series recordFailures) {
		this.nodeId = nodeId;
		this.raftDir = raftDir;
		this.currentTerm = currentTerm;
		this.configurationFloor = configurationFloor;
		this.recordFailures = recordFailures;
	}

	/** {@link System#nanoTime} of the last append or snapshot chunk a leader sent to this node, 0 if none yet. */
	long lastLeaderRequestNanos() {
		return lastLeaderRequestNanos.get();
	}

	@Override
	public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
			ServerCallHandler<Q, R> next) {
		var method = call.getMethodDescriptor().getFullMethodName();
		boolean fromLeader = method.equals(APPEND_ENTRIES) || method.equals(INSTALL_SNAPSHOT);
		boolean raft = method.startsWith(RaftServerProtocolServiceGrpc.SERVICE_NAME + "/");
		if (!raft) {
			return next.startCall(call, headers);
		}
		var serialized = method.equals(INSTALL_SNAPSHOT) ? new SerializedCall<>(call) : null;
		return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(
				serialized != null ? serialized : call, headers)) {
			/** The install stream was failed: no message of it reaches Ratis any more. Listener calls are serial. */
			private boolean failed;

			@Override
			public void onMessage(Q message) {
				if (failed) {
					return;
				}
				var drop = dropForTesting;
				if (drop != null && drop.test(nodeId, message)) {
					// The stream handler asks for the next message only after it saw one.
					if (call.getMethodDescriptor().getType() != MethodDescriptor.MethodType.UNARY) {
						call.request(1);
					}
					return;
				}
				long term = NO_TERM;
				if (serialized != null && message instanceof RaftProtos.InstallSnapshotRequestProto request) {
					term = termOf(request);
					var termHook = termForTesting;
					if (termHook != null) {
						long tested = termHook.applyAsLong(nodeId, request);
						term = tested != NO_TERM ? tested : term;
					}
					if (term != NO_TERM && request.getLeaderTerm() < term) {
						// A deposed leader's chunk (R15 review, N-R15-1): Ratis would answer NOT_LEADER too, but still
						// drop its configurations from the chunk's index on and take the chunk's. Terms only grow, so
						// Ratis would reject it as well; the sender learns the term and steps down. Not a leader's
						// request, and not recorded: Ratis never sees it.
						serialized.reply(notLeader(request, term));
						call.request(1);
						return;
					}
				}
				if (serialized != null && message instanceof RaftProtos.InstallSnapshotRequestProto request
						&& request.hasLastRaftConfigurationLogEntryProto()) {
					long index = request.getLastRaftConfigurationLogEntryProto().getIndex();
					long floor = floorOf(request);
					if (floor != NO_INDEX && index < floor) {
						// R16-2: a leader sends its latest configuration, whose index is at least that of every
						// committed one and of every configuration it appended in its term, so this chunk comes from
						// a stream its leader gave up on (delayed behind a later stream and the appends that brought
						// the newer configuration). Ratis would answer it ALREADY_INSTALLED, or install it, and take
						// its configuration either way, dropping the later ones. Its leader no longer waits on it.
						LOG.warn("{}: a snapshot chunk of term {} carries the configuration at index {}, older than the"
								+ " one at {} that its leader already holds: failing its install stream, which its leader"
								+ " gave up on", nodeId, request.getLeaderTerm(), index, floor);
						fail(Status.FAILED_PRECONDITION.withDescription("the configuration of the chunk, at index " + index
								+ ", is older than the one at " + floor + " that its leader already holds"));
						return;
					}
				}
				if (fromLeader) {
					lastLeaderRequestNanos.set(Math.max(1, System.nanoTime()));
				}
				if (raftDir != null && serialized != null
						&& message instanceof RaftProtos.InstallSnapshotRequestProto request
						&& !recordOrFail(request, term)) {
					return;
				}
				super.onMessage(message);
			}

			/** Records the chunk's configuration; if that fails, fails the stream and returns false. */
			private boolean recordOrFail(RaftProtos.InstallSnapshotRequestProto request, long term) {
				{
					try {
						var failHook = recordFailureForTesting;
						if (failHook != null && failHook.test(nodeId, request)) {
							throw new IOException("recordFailureForTesting");
						}
						InstalledConfiguration.record(Objects.requireNonNull(raftDir),
								term == NO_TERM ? Long.MAX_VALUE : term, request);
					} catch (IOException | RuntimeException ex) {
						// Ratis must not take a configuration that is not recorded: the stream fails, so the leader
						// starts the install again (a dropped chunk would show only at the next chunk, or, for the
						// last one, after the leader's request timeout).
						var file = raftDir.resolve(InstalledConfiguration.FILE);
						LOG.warn("{}: cannot record the configuration of a snapshot chunk in {}; failing the install"
								+ " stream", nodeId, file, ex);
						if (recordFailures != null) {
							recordFailures.record(1);
						}
						fail(Status.UNAVAILABLE.withDescription("cannot record the configuration of a snapshot chunk in "
								+ file).withCause(ex));
						return false;
					}
				}
				return true;
			}

			/** Fails the install stream: no later message of it reaches Ratis, and its end reaches Ratis as a cancel. */
			private void fail(Status status) {
				failed = true;
				Objects.requireNonNull(serialized).close(status, new Metadata());
			}

			@Override
			public void onHalfClose() {
				if (!failed) {
					super.onHalfClose();
				}
			}

			/**
			 * gRPC ends a call that this interceptor closed with onComplete, which the listener of Ratis' stream handler
			 * does not pass to its request observer (R15 review, finding 7): Ratis is told of the end as of a leader's
			 * cancel instead, which it handles for any stream.
			 */
			@Override
			public void onComplete() {
				if (failed) {
					super.onCancel();
				} else {
					super.onComplete();
				}
			}
		};
	}

	/** The current term of the division the chunk {@code request} is for, {@link #NO_TERM} if unknown. */
	private long termOf(RaftProtos.InstallSnapshotRequestProto request) {
		if (!request.hasServerRequest() || !request.getServerRequest().hasRaftGroupId()) {
			return NO_TERM;
		}
		try {
			return currentTerm.applyAsLong(RaftGroupId.valueOf(request.getServerRequest().getRaftGroupId().getId()));
		} catch (RuntimeException e) {
			return NO_TERM;
		}
	}

	/** The {@link ConfigurationFloor} of the division the chunk is for, {@link #NO_INDEX} if unknown. */
	private long floorOf(RaftProtos.InstallSnapshotRequestProto request) {
		if (!request.hasServerRequest() || !request.getServerRequest().hasRaftGroupId()) {
			return NO_INDEX;
		}
		try {
			return configurationFloor.floor(RaftGroupId.valueOf(request.getServerRequest().getRaftGroupId().getId()),
					request.getLeaderTerm());
		} catch (RuntimeException e) {
			return NO_INDEX;
		}
	}

	/**
	 * What Ratis answers to a chunk of a term below {@code term} ({@code SnapshotInstallationHandler
	 * .checkAndInstallSnapshot}, {@code ServerProtoUtils.toInstallSnapshotReplyProto}): NOT_LEADER with the current
	 * term; the leader's appender steps down on it ({@code GrpcLogAppender.InstallSnapshotResponseHandler.onNext}).
	 */
	static RaftProtos.InstallSnapshotReplyProto notLeader(RaftProtos.InstallSnapshotRequestProto request, long term) {
		var server = request.getServerRequest();
		var reply = RaftProtos.InstallSnapshotReplyProto.newBuilder()
				.setServerReply(RaftProtos.RaftRpcReplyProto.newBuilder().setRequestorId(server.getRequestorId())
						.setReplyId(server.getReplyId()).setRaftGroupId(server.getRaftGroupId()).setSuccess(false))
				.setTerm(term)
				.setResult(RaftProtos.InstallSnapshotResult.NOT_LEADER);
		if (request.hasSnapshotChunk()) {
			reply.setRequestIndex(request.getSnapshotChunk().getRequestIndex());
		}
		return reply.build();
	}

	/**
	 * The call of an install stream, as Ratis' handler and this interceptor share it: a {@link ServerCall} is not
	 * thread-safe, and Ratis answers chunks from the callbacks of its futures, on other threads than the listener's.
	 * Once closed (by this interceptor, after a failed record), what Ratis still sends or closes on it is dropped
	 * instead of failing on a closed call.
	 */
	static final class SerializedCall<Q, R> extends ForwardingServerCall.SimpleForwardingServerCall<Q, R> {
		private boolean closed;
		private boolean headersSent;

		SerializedCall(ServerCall<Q, R> delegate) {
			super(delegate);
		}

		@Override
		public synchronized void sendHeaders(Metadata headers) {
			if (!closed && !headersSent) {
				headersSent = true;
				super.sendHeaders(headers);
			}
		}

		/** Sends {@code reply} (a reply of this call's method) from this interceptor, with the headers if not sent yet. */
		@SuppressWarnings("unchecked")
		synchronized void reply(Object reply) {
			if (!closed) {
				sendHeaders(new Metadata());
				super.sendMessage((R) reply);
			}
		}

		@Override
		public synchronized void sendMessage(R message) {
			if (!closed) {
				super.sendMessage(message);
			}
		}

		@Override
		public synchronized void close(Status status, Metadata trailers) {
			if (!closed) {
				closed = true;
				super.close(status, trailers);
			}
		}
	}
}
