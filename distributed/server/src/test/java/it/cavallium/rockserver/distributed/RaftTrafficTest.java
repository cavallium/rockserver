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

	/** A server call that records what is done on it, and fails as gRPC does on a second close. */
	static final class FakeCall<Q, R> extends ServerCall<Q, R> {
		final MethodDescriptor<Q, R> method;
		final List<String> events = Collections.synchronizedList(new ArrayList<>());
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

	/** Ratis' handler: keeps the call it was given and the messages that reach it. */
	static final class FakeHandler<Q, R> implements ServerCallHandler<Q, R> {
		volatile ServerCall<Q, R> call;
		final List<Q> messages = Collections.synchronizedList(new ArrayList<>());

		@Override
		public ServerCall.Listener<Q> startCall(ServerCall<Q, R> call, Metadata headers) {
			this.call = call;
			return new ServerCall.Listener<>() {
				@Override
				public void onMessage(Q message) {
					messages.add(message);
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

	/** A chunk of a lower term than one recorded is still recorded (at the highest term) and forwarded. */
	@Test
	void aChunkOfALowerTermIsRecordedAndForwarded(@TempDir Path raftDir) throws Exception {
		var traffic = new RaftTraffic("n1", raftDir);
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
}
