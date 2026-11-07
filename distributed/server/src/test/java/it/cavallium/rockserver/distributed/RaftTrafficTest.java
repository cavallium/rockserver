package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R14-3: what {@link RaftTraffic} does with a snapshot chunk whose configuration it cannot record: it fails the
 * install stream (Ratis never sees the chunk, and the leader restarts the install at once), and what Ratis' handler
 * sends on the call after that is dropped instead of failing on a closed call.
 */
class RaftTrafficTest {

	private static final List<String> ABX = List.of("a", "b", "x");
	private static final RaftGroupId GROUP = RaftGroupId.randomId();
	private static final RaftProtos.RaftGroupIdProto GROUP_PROTO = RaftProtos.RaftGroupIdProto.newBuilder()
			.setId(GROUP.toByteString()).build();

	/** A server call that records what is done on it, and fails as gRPC does on a second close. */
	static final class FakeCall<Q, R> extends ServerCall<Q, R> {
		final MethodDescriptor<Q, R> method;
		final List<String> events = Collections.synchronizedList(new ArrayList<>());
		final List<R> sent = Collections.synchronizedList(new ArrayList<>());
		volatile Status closed;

		FakeCall(MethodDescriptor<Q, R> method) {
			this.method = method;
		}

		@Override
		public void request(int numMessages) {
			events.add("request " + numMessages);
		}

		@Override
		public void sendHeaders(Metadata headers) {
			events.add("headers");
		}

		@Override
		public void sendMessage(R message) {
			if (closed != null) {
				throw new IllegalStateException("call is closed");
			}
			events.add("message");
			sent.add(message);
		}

		@Override
		public void close(Status status, Metadata trailers) {
			if (closed != null) {
				throw new IllegalStateException("call already closed");
			}
			closed = status;
			events.add("close " + status.getCode());
		}

		@Override
		public boolean isCancelled() {
			return false;
		}

		@Override
		public MethodDescriptor<Q, R> getMethodDescriptor() {
			return method;
		}
	}

	/** Ratis' handler: keeps the call it was given, the messages that reach it and how its stream ended. */
	static final class FakeHandler<Q, R> implements ServerCallHandler<Q, R> {
		volatile ServerCall<Q, R> call;
		final List<Q> messages = Collections.synchronizedList(new ArrayList<>());
		final List<String> ends = Collections.synchronizedList(new ArrayList<>());

		@Override
		public ServerCall.Listener<Q> startCall(ServerCall<Q, R> call, Metadata headers) {
			this.call = call;
			return new ServerCall.Listener<>() {
				@Override
				public void onMessage(Q message) {
					messages.add(message);
				}

				@Override
				public void onHalfClose() {
					ends.add("half-close");
				}

				@Override
				public void onCancel() {
					ends.add("cancel");
				}

				@Override
				public void onComplete() {
					ends.add("complete");
				}
			};
		}
	}

	private static RaftProtos.InstallSnapshotRequestProto chunk(long leaderTerm, long index, List<String> newHalf) {
		var conf = RaftProtos.RaftConfigurationProto.newBuilder();
		for (var id : newHalf) {
			conf.addPeers(RaftProtos.RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8(id)));
		}
		return RaftProtos.InstallSnapshotRequestProto.newBuilder().setLeaderTerm(leaderTerm)
				.setServerRequest(RaftProtos.RaftRpcRequestProto.newBuilder().setRaftGroupId(GROUP_PROTO))
				.setLastRaftConfigurationLogEntryProto(RaftProtos.LogEntryProto.newBuilder().setIndex(index)
						.setConfigurationEntry(conf))
				.build();
	}

	private static FakeCall<RaftProtos.InstallSnapshotRequestProto, RaftProtos.InstallSnapshotReplyProto> installCall() {
		return new FakeCall<>(RaftServerProtocolServiceGrpc.getInstallSnapshotMethod());
	}

	/**
	 * The record cannot be written (the Raft directory is a file): the stream fails with UNAVAILABLE, the chunk does
	 * not reach Ratis, nor does a later message of the same stream; what Ratis' handler sends after that is dropped.
	 */
	@Test
	void aChunkWhoseConfigurationCannotBeRecordedFailsTheStream(@TempDir Path dir) throws Exception {
		var raftDir = Files.createFile(dir.resolve("not-a-directory"));
		var traffic = new RaftTraffic("n1", raftDir);
		var call = installCall();
		var handler = new FakeHandler<RaftProtos.InstallSnapshotRequestProto, RaftProtos.InstallSnapshotReplyProto>();
		var listener = traffic.interceptCall(call, new Metadata(), handler);
		listener.onMessage(chunk(3, 40, ABX));
		assertNotNull(call.closed, () -> "the stream is not failed: " + call.events);
		assertEquals(Status.Code.UNAVAILABLE, call.closed.getCode());
		assertTrue(handler.messages.isEmpty(), "the chunk reached Ratis");
		// A later message of that stream (one with no configuration would need no record) does not reach Ratis.
		listener.onMessage(RaftProtos.InstallSnapshotRequestProto.newBuilder().setLeaderTerm(3).build());
		assertTrue(handler.messages.isEmpty(), "a later message reached Ratis");

		// Ratis' handler still answers an earlier chunk, or fails its side: dropped, without an exception.
		var events = List.copyOf(call.events);
		handler.call.sendMessage(RaftProtos.InstallSnapshotReplyProto.getDefaultInstance());
		handler.call.close(Status.CANCELLED, new Metadata());
		assertEquals(events, List.copyOf(call.events));
	}

	/**
	 * R15 review, finding 7: gRPC ends a call this interceptor closed with onComplete, which Ratis' stream listener
	 * does not pass to its request observer; Ratis must be told the stream ended, as for a cancel by the leader.
	 */
	@Test
	void aFailedStreamEndsRatisStreamAsACancel(@TempDir Path dir) throws Exception {
		var raftDir = Files.createFile(dir.resolve("not-a-directory"));
		var traffic = new RaftTraffic("n1", raftDir);
		var call = installCall();
		var handler = new FakeHandler<RaftProtos.InstallSnapshotRequestProto, RaftProtos.InstallSnapshotReplyProto>();
		var listener = traffic.interceptCall(call, new Metadata(), handler);
		listener.onMessage(chunk(3, 40, ABX));
		listener.onHalfClose();
		listener.onComplete();
		assertEquals(List.of("cancel"), List.copyOf(handler.ends));
	}

	/**
	 * R15 review, finding 1 (N-R15-1): a chunk of a term below the node's current term (a deposed leader's) is answered
	 * NOT_LEADER with the current term by the interceptor itself: Ratis, which would answer the same but still take
	 * the chunk's configuration, never sees it, and nothing is recorded. The stream goes on; its end reaches Ratis.
	 */
	@Test
	void aChunkBelowTheCurrentTermIsAnsweredNotLeaderWithoutRatis(@TempDir Path raftDir) throws Exception {
		var traffic = new RaftTraffic("n1", raftDir, group -> group.equals(GROUP) ? 5 : RaftTraffic.NO_TERM, null);
		var call = installCall();
		var handler = new FakeHandler<RaftProtos.InstallSnapshotRequestProto, RaftProtos.InstallSnapshotReplyProto>();
		var listener = traffic.interceptCall(call, new Metadata(), handler);
		var request = chunk(3, 40, ABX).toBuilder()
				.setServerRequest(RaftProtos.RaftRpcRequestProto.newBuilder().setRequestorId(ByteString.copyFromUtf8("a"))
						.setReplyId(ByteString.copyFromUtf8("n1")).setRaftGroupId(GROUP_PROTO))
				.setSnapshotChunk(RaftProtos.InstallSnapshotRequestProto.SnapshotChunkProto.newBuilder().setRequestIndex(2))
				.build();
		listener.onMessage(request);
		assertTrue(handler.messages.isEmpty(), "the chunk reached Ratis");
		assertEquals(null, call.closed);
		assertEquals(List.of("headers", "message", "request 1"), List.copyOf(call.events));
		var reply = call.sent.getFirst();
		assertEquals(RaftProtos.InstallSnapshotResult.NOT_LEADER, reply.getResult());
		assertEquals(5, reply.getTerm());
		assertEquals(2, reply.getRequestIndex());
		assertEquals(ByteString.copyFromUtf8("a"), reply.getServerReply().getRequestorId());
		assertEquals(ByteString.copyFromUtf8("n1"), reply.getServerReply().getReplyId());
		assertEquals(GROUP_PROTO, reply.getServerReply().getRaftGroupId());
		assertEquals(RemovalRule.UNKNOWN_TERM, InstalledConfiguration.boundOf(InstalledConfiguration.read(raftDir),
				new Conf(RemovalRule.UNKNOWN_TERM, 40, ABX, List.of())));
		// Ratis' handler answers on the same call afterwards: its headers are not sent twice.
		handler.call.sendHeaders(new Metadata());
		handler.call.sendMessage(RaftProtos.InstallSnapshotReplyProto.getDefaultInstance());
		assertEquals(List.of("headers", "message", "request 1", "message"), List.copyOf(call.events));
		listener.onHalfClose();
		assertEquals(List.of("half-close"), List.copyOf(handler.ends));
	}

	/**
	 * A chunk of a lower term than one recorded, that passed the term check (the node's term had not risen yet), is
	 * still recorded (at the highest term) and forwarded.
	 */
	@Test
	void aChunkOfALowerTermIsRecordedAndForwarded(@TempDir Path raftDir) throws Exception {
		var traffic = new RaftTraffic("n1", raftDir, _ -> 3, null);
		var call = installCall();
		var handler = new FakeHandler<RaftProtos.InstallSnapshotRequestProto, RaftProtos.InstallSnapshotReplyProto>();
		var listener = traffic.interceptCall(call, new Metadata(), handler);
		listener.onMessage(chunk(7, 40, ABX));
		listener.onMessage(chunk(3, 40, ABX));
		assertEquals(2, handler.messages.size());
		assertEquals(7, InstalledConfiguration.boundOf(InstalledConfiguration.read(raftDir),
				new Conf(RemovalRule.UNKNOWN_TERM, 40, ABX, List.of())));
		assertEquals(null, call.closed);
	}

	/**
	 * R16-2: a chunk of the current term whose configuration is older than the node's newest committed one (a delayed
	 * chunk of an install stream its leader gave up on, answered ALREADY_INSTALLED by Ratis, which would still drop the
	 * later configurations and take the chunk's) fails its stream: Ratis never sees it, and its end reaches Ratis as a
	 * cancel, and it is counted ({@code install.staleChunks}, R17-6). A chunk of the same configuration, or of a later
	 * one, is forwarded.
	 */
	@Test
	void aChunkOfAConfigurationOlderThanTheCommittedOneFailsTheStream(@TempDir Path raftDir) throws Exception {
		var staleChunks = new NodeMetrics.Series();
		var traffic = new RaftTraffic("n1", raftDir, _ -> 5,
				(group, _) -> group.equals(GROUP) ? 40 : RaftTraffic.NO_INDEX, null, staleChunks);
		var call = installCall();
		var handler = new FakeHandler<RaftProtos.InstallSnapshotRequestProto, RaftProtos.InstallSnapshotReplyProto>();
		var listener = traffic.interceptCall(call, new Metadata(), handler);
		listener.onMessage(chunk(5, 30, ABX));
		assertNotNull(call.closed, () -> "the stream is not failed: " + call.events);
		// R17-6: counted, so that a live stream refused by mistake shows in a metric and an alert, not only in a WARN.
		assertEquals(1, staleChunks.count(), "install.staleChunks");
		assertTrue(handler.messages.isEmpty(), "the chunk reached Ratis");
		assertEquals(RemovalRule.UNKNOWN_TERM, InstalledConfiguration.boundOf(InstalledConfiguration.read(raftDir),
				new Conf(RemovalRule.UNKNOWN_TERM, 30, ABX, List.of())));
		listener.onComplete();
		assertEquals(List.of("cancel"), List.copyOf(handler.ends));

		var current = installCall();
		var currentHandler = new FakeHandler<RaftProtos.InstallSnapshotRequestProto,
				RaftProtos.InstallSnapshotReplyProto>();
		var currentListener = traffic.interceptCall(current, new Metadata(), currentHandler);
		currentListener.onMessage(chunk(5, 40, ABX));
		currentListener.onMessage(chunk(5, 41, ABX));
		assertEquals(2, currentHandler.messages.size());
		assertEquals(null, current.closed);
		assertEquals(1, staleChunks.count(), "a forwarded chunk is not counted");
	}

	/**
	 * R16 review, finding 1: the floor is also the newest configuration of the chunk's leader term, committed or not
	 * ({@link MembershipFacts#installFloor}): a delayed chunk of an abandoned stream that arrives after the same leader's
	 * appends brought a later configuration, before the node learns it is committed, fails its stream. A configuration of
	 * another term, or of no known term, gives no floor beyond the committed one.
	 */
	@Test
	void theFloorIsTheNewestConfigurationOfTheLeadersTermCommittedOrNot(@TempDir Path raftDir) throws Exception {
		var committed = new Conf(4, 40, ABX, List.of());
		var latest = new Conf(5, 50, List.of("a", "b", "x", "y"), List.of());
		assertEquals(50, MembershipFacts.installFloor(committed, latest, 5));
		assertEquals(50, MembershipFacts.installFloor(null, latest, 5));
		assertEquals(40, MembershipFacts.installFloor(committed, latest, 6), "a later leader may not hold it");
		assertEquals(40, MembershipFacts.installFloor(committed,
				new Conf(RemovalRule.UNKNOWN_TERM, 50, ABX, List.of()), 5), "a configuration of no known term");
		assertEquals(RaftTraffic.NO_INDEX, MembershipFacts.installFloor(null,
				new Conf(4, 40, ABX, List.of()), 5));
		assertEquals(40, MembershipFacts.installFloor(committed, committed, 4));

		var traffic = new RaftTraffic("n1", raftDir, _ -> 5,
				(_, leaderTerm) -> MembershipFacts.installFloor(committed, latest, leaderTerm), null);
		var call = installCall();
		var handler = new FakeHandler<RaftProtos.InstallSnapshotRequestProto, RaftProtos.InstallSnapshotReplyProto>();
		var listener = traffic.interceptCall(call, new Metadata(), handler);
		listener.onMessage(chunk(5, 45, ABX));
		assertNotNull(call.closed, () -> "the stream is not failed: " + call.events);
		assertTrue(handler.messages.isEmpty(), "the chunk reached Ratis");

		var live = installCall();
		var liveHandler = new FakeHandler<RaftProtos.InstallSnapshotRequestProto, RaftProtos.InstallSnapshotReplyProto>();
		var liveListener = traffic.interceptCall(live, new Metadata(), liveHandler);
		liveListener.onMessage(chunk(5, 50, latest.newHalf()));
		assertEquals(1, liveHandler.messages.size());
		assertEquals(null, live.closed);
	}
}
