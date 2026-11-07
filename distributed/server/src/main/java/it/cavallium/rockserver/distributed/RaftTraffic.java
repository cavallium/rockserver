package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
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
 * method name, and forwards every message unchanged. Before it forwards a snapshot chunk, it records the chunk's
 * configuration with the leader's term ({@link InstalledConfiguration}, round-13 review, finding 1): every chunk,
 * whatever its term, since Ratis takes the configuration of a chunk it rejects too (round 15). If the record cannot
 * be written, it fails the install stream with UNAVAILABLE instead of forwarding the chunk (R14-3): Ratis never takes
 * that configuration, and the leader, told at once, starts the install again after its retry wait (Ratis' leader
 * never sends a single chunk again). The record is synced on the gRPC listener thread of that stream only (Raft's
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

	private final String nodeId;
	/** Where {@link InstalledConfiguration} is kept: the Raft directory; null: not recorded (tests). */
	private final @Nullable Path raftDir;
	/** {@link System#nanoTime} of the last leader request, 0 if none yet. */
	private final AtomicLong lastLeaderRequestNanos = new AtomicLong();

	RaftTraffic(String nodeId) {
		this(nodeId, null);
	}

	RaftTraffic(String nodeId, @Nullable Path raftDir) {
		this(nodeId, raftDir, _ -> -1, null);
	}

	RaftTraffic(String nodeId, @Nullable Path raftDir, java.util.function.ToLongFunction<org.apache.ratis.protocol.RaftGroupId> currentTerm,
			NodeMetrics.@Nullable Series recordFailures) {
		this.nodeId = nodeId;
		this.raftDir = raftDir;
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
				if (fromLeader) {
					lastLeaderRequestNanos.set(Math.max(1, System.nanoTime()));
				}
				if (raftDir != null && message instanceof RaftProtos.InstallSnapshotRequestProto request) {
					try {
						InstalledConfiguration.record(raftDir, 0, request);
					} catch (IOException | RuntimeException ex) {
						// Ratis must not take a configuration that is not recorded: the stream fails, so the leader
						// starts the install again (a dropped chunk would show only at the next chunk, or, for the
						// last one, after the leader's request timeout).
						var file = raftDir.resolve(InstalledConfiguration.FILE);
						LOG.warn("{}: cannot record the configuration of a snapshot chunk in {}; failing the install"
								+ " stream", nodeId, file, ex);
						failed = true;
						Objects.requireNonNullElse(serialized, call).close(Status.UNAVAILABLE.withDescription(
								"cannot record the configuration of a snapshot chunk in " + file).withCause(ex),
								new Metadata());
						return;
					}
				}
				super.onMessage(message);
			}

			@Override
			public void onHalfClose() {
				if (!failed) {
					super.onHalfClose();
				}
			}
		};
	}

	/**
	 * The call of an install stream, as Ratis' handler and this interceptor share it: a {@link ServerCall} is not
	 * thread-safe, and Ratis answers chunks from the callbacks of its futures, on other threads than the listener's.
	 * Once closed (by this interceptor, after a failed record), what Ratis still sends or closes on it is dropped
	 * instead of failing on a closed call.
	 */
	static final class SerializedCall<Q, R> extends ForwardingServerCall.SimpleForwardingServerCall<Q, R> {
		private boolean closed;

		SerializedCall(ServerCall<Q, R> delegate) {
			super(delegate);
		}

		@Override
		public synchronized void sendHeaders(Metadata headers) {
			if (!closed) {
				super.sendHeaders(headers);
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
