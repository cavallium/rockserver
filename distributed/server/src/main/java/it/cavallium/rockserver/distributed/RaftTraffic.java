package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.nio.file.Path;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiPredicate;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
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
 * configuration with the leader's term ({@link InstalledConfiguration}, round-13 review, finding 1).</p>
 */
final class RaftTraffic implements ServerInterceptor {

	private static final Logger LOG = LoggerFactory.getLogger(RaftTraffic.class);

	private static final String APPEND_ENTRIES = RaftServerProtocolServiceGrpc.getAppendEntriesMethod()
			.getFullMethodName();
	private static final String INSTALL_SNAPSHOT = RaftServerProtocolServiceGrpc.getInstallSnapshotMethod()
			.getFullMethodName();

	/**
	 * Tests only: drops a Raft request (append, vote, snapshot chunk, ...) that reaches node {@code receiver} when it
	 * returns true, as a lossy link would: Ratis never sees it, and its sender times out and sends again. Static, for
	 * in-process clusters.
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
		return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
			@Override
			public void onMessage(Q message) {
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
						InstalledConfiguration.record(raftDir, request);
					} catch (IOException | RuntimeException ex) {
						// Ratis must not take a configuration whose term bound may be lower than recorded: the
						// chunk is dropped, as on a lossy link, and the leader sends it again.
						LOG.warn("{}: failed to record the configuration of a snapshot chunk in {}, dropping it", nodeId,
								raftDir.resolve(InstalledConfiguration.FILE), ex);
						if (call.getMethodDescriptor().getType() != MethodDescriptor.MethodType.UNARY) {
							call.request(1);
						}
						return;
					}
				}
				super.onMessage(message);
			}
		};
	}
}
