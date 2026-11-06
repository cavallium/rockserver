package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.io.grpc.CallOptions;
import org.apache.ratis.thirdparty.io.grpc.Channel;
import org.apache.ratis.thirdparty.io.grpc.MethodDescriptor;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.stub.ClientCalls;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls;
import org.apache.ratis.thirdparty.io.grpc.stub.StreamObserver;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tiny gRPC service on the Ratis server port, using the shaded gRPC that Ratis ships
 * ({@code org.apache.ratis.thirdparty.io.grpc}). It answers:
 * <ul>
 *   <li>"what is your current term in group G?", for {@link ReadBarrier}. Request: the 16-byte group UUID.
 *   Response: 8-byte big-endian current term, or -1 if this server has no such group;</li>
 *   <li>"where is your Rockserver API?", for {@link LeaderForwarder}, which knows the Raft address of every member
 *   from the Raft configuration but the API address only of the members it was started with. Request: empty.
 *   Response: UTF-8 {@code host:port}, as other members reach it.</li>
 *   <li>"what do you hold?", for {@link ClusterCheck} and {@code admin add-peer}. Request: the 16-byte group UUID, then
 *   the UTF-8 id of the caller's cluster, if it speaks for one: a node of another cluster stops. Response: the
 *   {@link NodeState} encoded by {@link #encode}, or nothing if this server has no such group (or is not started
 *   yet).</li>
 *   <li>"what may a follower read up to?", for {@link FollowerReads}. Request: the 16-byte group UUID. Response: the
 *   8-byte big-endian read index then the 8-byte big-endian term of the leader ({@link ReadBarrier#readIndex()}),
 *   once this node confirmed its leadership after the request arrived (a node of round 1 answered the index alone, which
 *   followers now refuse: they forward their reads instead); {@code FAILED_PRECONDITION} if it is not the ready leader, {@code UNIMPLEMENTED} on a node that
 *   predates the method or cannot serve it.</li>
 *   <li>"which Rockserver config do you run?", for {@link ClusterCheck}. Request: the UTF-8 digest of the caller's
 *   config ({@link NodeStore#configDigest}), possibly empty. Response: the UTF-8 digest of this node's config. Each side
 *   logs a difference: every node must run the same config. {@code UNIMPLEMENTED} on a node that predates the method.</li>
 *   <li>"which configuration do you hold?", for {@link RemovalCheck}. Request: the 16-byte group UUID. Response: the
 *   8-byte big-endian log index of this node's latest configuration, one byte 1 if that index is committed here (else
 *   0), then the UTF-8 ids of its (new) members, one per line; nothing if this server has no such group or is not
 *   started yet. {@code UNIMPLEMENTED} on a node that predates the method.</li>
 *   <li>the cluster operations of {@link ReplicaOps} (capabilities, checksums, backups). Request: the 16-byte group
 *   UUID, then an operation byte and its payload. Response: see {@link ReplicaOps#answer}; {@code UNIMPLEMENTED} on a
 *   node that predates the method.</li>
 * </ul>
 */
final class TermService {

	static final String SERVICE_NAME = "rockserver.distributed.Term";

	/** Reply of a server that has no division for the requested group (or is not started yet). */
	static final long NO_GROUP = -1;

	static final MethodDescriptor<byte[], byte[]> CURRENT_TERM = MethodDescriptor.<byte[], byte[]>newBuilder()
			.setType(MethodDescriptor.MethodType.UNARY)
			.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "CurrentTerm"))
			.setRequestMarshaller(BytesMarshaller.INSTANCE)
			.setResponseMarshaller(BytesMarshaller.INSTANCE)
			.setIdempotent(true)
			.setSafe(true)
			.build();

	static final MethodDescriptor<byte[], byte[]> API_ADDRESS = MethodDescriptor.<byte[], byte[]>newBuilder()
			.setType(MethodDescriptor.MethodType.UNARY)
			.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ApiAddress"))
			.setRequestMarshaller(BytesMarshaller.INSTANCE)
			.setResponseMarshaller(BytesMarshaller.INSTANCE)
			.setIdempotent(true)
			.setSafe(true)
			.build();

	static final MethodDescriptor<byte[], byte[]> NODE_STATE = MethodDescriptor.<byte[], byte[]>newBuilder()
			.setType(MethodDescriptor.MethodType.UNARY)
			.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "NodeState"))
			.setRequestMarshaller(BytesMarshaller.INSTANCE)
			.setResponseMarshaller(BytesMarshaller.INSTANCE)
			.build();

	static final MethodDescriptor<byte[], byte[]> READ_INDEX = MethodDescriptor.<byte[], byte[]>newBuilder()
			.setType(MethodDescriptor.MethodType.UNARY)
			.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ReadIndex"))
			.setRequestMarshaller(BytesMarshaller.INSTANCE)
			.setResponseMarshaller(BytesMarshaller.INSTANCE)
			.setIdempotent(true)
			.setSafe(true)
			.build();

	static final MethodDescriptor<byte[], byte[]> CONFIG_DIGEST = MethodDescriptor.<byte[], byte[]>newBuilder()
			.setType(MethodDescriptor.MethodType.UNARY)
			.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "ConfigDigest"))
			.setRequestMarshaller(BytesMarshaller.INSTANCE)
			.setResponseMarshaller(BytesMarshaller.INSTANCE)
			.setIdempotent(true)
			.setSafe(true)
			.build();

	private static final Logger LOG = LoggerFactory.getLogger(TermService.class);

	static final MethodDescriptor<byte[], byte[]> CONFIGURATION = MethodDescriptor.<byte[], byte[]>newBuilder()
			.setType(MethodDescriptor.MethodType.UNARY)
			.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Configuration"))
			.setRequestMarshaller(BytesMarshaller.INSTANCE)
			.setResponseMarshaller(BytesMarshaller.INSTANCE)
			.build();

	static final MethodDescriptor<byte[], byte[]> OPS = MethodDescriptor.<byte[], byte[]>newBuilder()
			.setType(MethodDescriptor.MethodType.UNARY)
			.setFullMethodName(MethodDescriptor.generateFullMethodName(SERVICE_NAME, "Ops"))
			.setRequestMarshaller(BytesMarshaller.INSTANCE)
			.setResponseMarshaller(BytesMarshaller.INSTANCE)
			.build();

	/** Answers {@link #OPS} on a node. */
	@FunctionalInterface
	interface OpsSource {
		/** The answer to {@code request} (operation byte and payload) about {@code groupId}. */
		CompletableFuture<byte[]> answer(RaftGroupId groupId, byte[] request);
	}

	private static final OpsSource NO_OPS = (_, _) -> CompletableFuture.failedFuture(
			Status.UNIMPLEMENTED.withDescription("This node runs no cluster operations").asRuntimeException());

	private static final int GROUP_ID_BYTES = 16;
	private static final byte SEEDED = 1;
	private static final byte HAS_SNAPSHOT = 2;
	private static final byte STARTED_OUTSIDE_JOIN = 4;

	/** Answers {@link #NODE_STATE} on a node. */
	@FunctionalInterface
	interface NodeStateSource {
		/**
		 * @param callerClusterId the cluster the caller speaks for, null if none
		 * @return null if this node has no such group, or is not started yet
		 */
		@Nullable NodeState state(RaftGroupId groupId, @Nullable String callerClusterId);
	}

	/** Answers {@link #READ_INDEX} on a node. */
	@FunctionalInterface
	interface ReadIndexSource {
		/** The read index of {@code groupId}, once this node confirmed its leadership; fails if it is not the leader. */
		CompletableFuture<ReadBarrier.ReadIndex> readIndex(RaftGroupId groupId);
	}

	private TermService() {}

	/**
	 * A customizer that adds this service to the Ratis gRPC server; register it through Ratis {@code Parameters}.
	 *
	 * @param apiAddress this node's API address as other members reach it
	 */
	static GrpcServices.Customizer customizer(Supplier<RaftServer> server, HostAndPort apiAddress,
			NodeStateSource nodeState) {
		return customizer(server, apiAddress, nodeState, _ -> CompletableFuture.failedFuture(
				Status.UNIMPLEMENTED.withDescription("This node serves no read index").asRuntimeException()));
	}

	/** {@link #customizer(Supplier, HostAndPort, NodeStateSource)}, answering {@link #READ_INDEX} too. */
	static GrpcServices.Customizer customizer(Supplier<RaftServer> server, HostAndPort apiAddress,
			NodeStateSource nodeState, ReadIndexSource readIndex) {
		return customizer(server, apiAddress, nodeState, readIndex, null, NO_OPS);
	}

	/**
	 * {@link #customizer(Supplier, HostAndPort, NodeStateSource, ReadIndexSource)}, answering {@link #CONFIG_DIGEST} too,
	 * unless {@code configDigest} is null.
	 */
	static GrpcServices.Customizer customizer(Supplier<RaftServer> server, HostAndPort apiAddress,
			NodeStateSource nodeState, ReadIndexSource readIndex, @Nullable String configDigest) {
		return customizer(server, apiAddress, nodeState, readIndex, configDigest, NO_OPS);
	}

	/** {@link #customizer(Supplier, HostAndPort, NodeStateSource, ReadIndexSource)}, answering {@link #OPS} too. */
	static GrpcServices.Customizer customizer(Supplier<RaftServer> server, HostAndPort apiAddress,
			NodeStateSource nodeState, ReadIndexSource readIndex, OpsSource ops) {
		return customizer(server, apiAddress, nodeState, readIndex, null, ops);
	}

	/**
	 * {@link #customizer(Supplier, HostAndPort, NodeStateSource, ReadIndexSource)}, answering {@link #OPS} and, unless
	 * {@code configDigest} is null, {@link #CONFIG_DIGEST} too.
	 */
	static GrpcServices.Customizer customizer(Supplier<RaftServer> server, HostAndPort apiAddress,
			NodeStateSource nodeState, ReadIndexSource readIndex, @Nullable String configDigest, OpsSource ops) {
		ServerCalls.UnaryMethod<byte[], byte[]> handler = (request, response) -> answer(server, request, response);
		var address = (apiAddress.host() + ":" + apiAddress.port()).getBytes(StandardCharsets.UTF_8);
		ServerCalls.UnaryMethod<byte[], byte[]> addressHandler = (_, response) -> {
			response.onNext(address);
			response.onCompleted();
		};
		ServerCalls.UnaryMethod<byte[], byte[]> stateHandler = (request, response) -> answerState(nodeState, request, response);
		ServerCalls.UnaryMethod<byte[], byte[]> readIndexHandler = (request, response) ->
				answerReadIndex(readIndex, request, response);
		var definition = ServerServiceDefinition.builder(SERVICE_NAME)
				.addMethod(CURRENT_TERM, ServerCalls.asyncUnaryCall(handler))
				.addMethod(API_ADDRESS, ServerCalls.asyncUnaryCall(addressHandler))
				.addMethod(NODE_STATE, ServerCalls.asyncUnaryCall(stateHandler))
				.addMethod(READ_INDEX, ServerCalls.asyncUnaryCall(readIndexHandler))
				.addMethod(OPS, ServerCalls.asyncUnaryCall((request, response) -> answerOps(ops, request, response)))
				.addMethod(CONFIGURATION, ServerCalls.asyncUnaryCall((request, response) ->
						answerConfiguration(server, request, response)));
		if (configDigest != null) {
			ServerCalls.UnaryMethod<byte[], byte[]> digestHandler = (request, response) ->
					answerConfigDigest(configDigest, request, response);
			definition.addMethod(CONFIG_DIGEST, ServerCalls.asyncUnaryCall(digestHandler));
		}
		var service = definition.build();
		// Ratis may run admin and client services on separate ports; the peers' Raft address is the SERVER one.
		return (builder, types) -> types.contains(GrpcServices.Type.SERVER) ? builder.addService(service) : builder;
	}

	/** Asks the server behind {@code channel} for its current term in {@code groupId}. */
	static CompletableFuture<Long> currentTerm(Channel channel, RaftGroupId groupId, Duration timeout) {
		return call(channel, CURRENT_TERM, encodeGroupId(groupId), timeout).thenApply(value -> {
			if (value.length != Long.BYTES) {
				throw new CompletionException(new IOException("Bad term reply size " + value.length));
			}
			return ByteBuffer.wrap(value).getLong();
		});
	}

	/** Asks the leader behind {@code channel} for a read index of {@code groupId}: see {@link #READ_INDEX}. */
	static CompletableFuture<ReadBarrier.ReadIndex> readIndex(Channel channel, RaftGroupId groupId, Duration timeout) {
		return call(channel, READ_INDEX, encodeGroupId(groupId), timeout).thenApply(value -> {
			if (value.length != 2 * Long.BYTES) {
				throw new CompletionException(new IOException("Bad read index reply size " + value.length));
			}
			var buffer = ByteBuffer.wrap(value);
			return new ReadBarrier.ReadIndex(buffer.getLong(), buffer.getLong());
		});
	}

	/**
	 * Tells the server behind {@code channel} the digest of this node's Rockserver config, and asks for its own. Fails
	 * with {@code UNIMPLEMENTED} on a node that predates the method.
	 */
	static CompletableFuture<String> configDigest(Channel channel, String ownDigest, Duration timeout) {
		return call(channel, CONFIG_DIGEST, ownDigest.getBytes(StandardCharsets.UTF_8), timeout)
				.thenApply(value -> new String(value, StandardCharsets.UTF_8));
	}

	private static void answerConfigDigest(String own, byte[] request, StreamObserver<byte[]> response) {
		var callerDigest = new String(request, StandardCharsets.UTF_8);
		if (!callerDigest.isEmpty() && !callerDigest.equals(own)) {
			LOG.warn("A leader of this cluster runs another Rockserver config (digest {}) than this node ({}): every node"
					+ " must run the same config file", callerDigest, own);
		}
		response.onNext(own.getBytes(StandardCharsets.UTF_8));
		response.onCompleted();
	}

	/** Asks the server behind {@code channel} for the address of its Rockserver API. */
	static CompletableFuture<HostAndPort> apiAddress(Channel channel, Duration timeout) {
		return call(channel, API_ADDRESS, new byte[0], timeout).thenApply(value -> {
			var address = new String(value, StandardCharsets.UTF_8);
			int colon = address.lastIndexOf(':');
			try {
				return new HostAndPort(address.substring(0, colon), Integer.parseInt(address.substring(colon + 1)));
			} catch (RuntimeException e) {
				throw new CompletionException(new IOException("Bad API address reply: " + address, e));
			}
		});
	}

	/**
	 * Asks the server behind {@code channel} what it holds in {@code groupId}: null if it has no such group, or is not
	 * started yet.
	 *
	 * @param clusterId the cluster that the caller speaks for: a node of another cluster stops. Empty for a caller that
	 *                  only wants to know
	 */
	static CompletableFuture<@Nullable NodeState> nodeState(Channel channel, RaftGroupId groupId, String clusterId,
			Duration timeout) {
		var cluster = clusterId.getBytes(StandardCharsets.UTF_8);
		var request = ByteBuffer.allocate(GROUP_ID_BYTES + cluster.length).put(encodeGroupId(groupId)).put(cluster).array();
		return call(channel, NODE_STATE, request, timeout).thenApply(TermService::decode);
	}

	private static byte[] encode(NodeState state) {
		var cluster = state.clusterId() == null ? new byte[0] : state.clusterId().getBytes(StandardCharsets.UTF_8);
		byte flags = (byte) ((state.seeded() ? SEEDED : 0) | (state.hasSnapshot() ? HAS_SNAPSHOT : 0)
				| (state.startedOutsideJoin() ? STARTED_OUTSIDE_JOIN : 0));
		return ByteBuffer.allocate(Long.BYTES + 1 + cluster.length).putLong(state.nextLogIndex()).put(flags).put(cluster)
				.array();
	}

	private static @Nullable NodeState decode(byte[] value) {
		if (value.length == 0) {
			return null;
		}
		var buffer = ByteBuffer.wrap(value);
		long nextLogIndex = buffer.getLong();
		byte flags = buffer.get();
		var cluster = new byte[buffer.remaining()];
		buffer.get(cluster);
		return new NodeState(nextLogIndex, (flags & SEEDED) != 0, (flags & HAS_SNAPSHOT) != 0,
				(flags & STARTED_OUTSIDE_JOIN) != 0, cluster.length == 0 ? null : new String(cluster, StandardCharsets.UTF_8));
	}

	/**
	 * Asks the server behind {@code channel} for its latest configuration of {@code groupId}: null if it has no such
	 * group, or is not started yet. Fails at once, without waiting for {@code timeout}, if the server cannot be reached.
	 */
	static CompletableFuture<RemovalCheck.@Nullable PeerConfiguration> configuration(Channel channel, RaftGroupId groupId,
			Duration timeout) {
		return call(channel, CONFIGURATION, encodeGroupId(groupId), timeout, false).thenApply(value -> {
			if (value.length == 0) {
				return null;
			}
			if (value.length < Long.BYTES + 1) {
				throw new CompletionException(new IOException("Bad configuration reply size " + value.length));
			}
			var buffer = ByteBuffer.wrap(value);
			long index = buffer.getLong();
			boolean committed = buffer.get() != 0;
			var text = new String(value, buffer.position(), buffer.remaining(), StandardCharsets.UTF_8);
			return new RemovalCheck.PeerConfiguration(index, committed,
					text.isEmpty() ? List.of() : List.of(text.split("\n")));
		});
	}

	private static void answerConfiguration(Supplier<RaftServer> server, byte[] request,
			StreamObserver<byte[]> response) {
		if (request.length != GROUP_ID_BYTES) {
			response.onError(Status.INVALID_ARGUMENT
					.withDescription("Expected a 16-byte group id, got " + request.length + " bytes")
					.asRuntimeException());
			return;
		}
		var uuid = ByteBuffer.wrap(request);
		var groupId = RaftGroupId.valueOf(new UUID(uuid.getLong(), uuid.getLong()));
		byte[] reply;
		try {
			var raftServer = server.get();
			var division = raftServer == null ? null : raftServer.getDivision(groupId);
			if (division == null) {
				reply = new byte[0];
			} else {
				var conf = division.getRaftConf();
				boolean committed = conf.getLogEntryIndex() <= division.getRaftLog().getLastCommittedIndex();
				var members = String.join("\n", conf.getCurrentPeers().stream().map(peer -> peer.getId().toString())
						.toList()).getBytes(StandardCharsets.UTF_8);
				reply = ByteBuffer.allocate(Long.BYTES + 1 + members.length).putLong(conf.getLogEntryIndex())
						.put((byte) (committed ? 1 : 0)).put(members).array();
			}
		} catch (IOException | RuntimeException e) {
			reply = new byte[0];
		}
		response.onNext(reply);
		response.onCompleted();
	}

	private static CompletableFuture<byte[]> call(Channel channel, MethodDescriptor<byte[], byte[]> method,
			byte[] request, Duration timeout) {
		return call(channel, method, request, timeout, true);
	}

	/** {@code waitForReady}: wait for a connection being (re)established, within the deadline, rather than fail at once. */
	private static CompletableFuture<byte[]> call(Channel channel, MethodDescriptor<byte[], byte[]> method,
			byte[] request, Duration timeout, boolean waitForReady) {
		var result = new CompletableFuture<byte[]>();
		var options = CallOptions.DEFAULT.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS);
		if (waitForReady) {
			options = options.withWaitForReady();
		}
		ClientCalls.asyncUnaryCall(channel.newCall(method, options), request, new StreamObserver<>() {
			@Override
			public void onNext(byte[] value) {
				result.complete(value);
			}

			@Override
			public void onError(Throwable error) {
				result.completeExceptionally(error);
			}

			@Override
			public void onCompleted() {
				result.completeExceptionally(new IOException(method.getBareMethodName() + " reply missing"));
			}
		});
		return result;
	}

	private static void answer(Supplier<RaftServer> server, byte[] request, StreamObserver<byte[]> response) {
		if (request.length != 16) {
			response.onError(Status.INVALID_ARGUMENT
					.withDescription("Expected a 16-byte group id, got " + request.length + " bytes")
					.asRuntimeException());
			return;
		}
		var uuid = ByteBuffer.wrap(request);
		var groupId = RaftGroupId.valueOf(new UUID(uuid.getLong(), uuid.getLong()));
		long term;
		try {
			var raftServer = server.get();
			term = raftServer == null ? NO_GROUP : raftServer.getDivision(groupId).getInfo().getCurrentTerm();
		} catch (IOException | RuntimeException e) {
			term = NO_GROUP;
		}
		response.onNext(ByteBuffer.allocate(Long.BYTES).putLong(term).array());
		response.onCompleted();
	}

	private static void answerReadIndex(ReadIndexSource source, byte[] request, StreamObserver<byte[]> response) {
		if (request.length != GROUP_ID_BYTES) {
			response.onError(Status.INVALID_ARGUMENT
					.withDescription("Expected a 16-byte group id, got " + request.length + " bytes")
					.asRuntimeException());
			return;
		}
		var uuid = ByteBuffer.wrap(request);
		var groupId = RaftGroupId.valueOf(new UUID(uuid.getLong(), uuid.getLong()));
		CompletableFuture<ReadBarrier.ReadIndex> index;
		try {
			index = source.readIndex(groupId);
		} catch (RuntimeException e) {
			index = CompletableFuture.failedFuture(e);
		}
		index.whenComplete((value, error) -> {
			if (error == null) {
				response.onNext(ByteBuffer.allocate(2 * Long.BYTES).putLong(value.index()).putLong(value.term()).array());
				response.onCompleted();
				return;
			}
			var cause = ReplicatedConnection.unwrap(error);
			var status = Status.fromThrowable(cause);
			if (status.getCode() == Status.Code.UNKNOWN) {
				// Not the leader, leadership not confirmed in time, or closing: the follower forwards the read instead.
				status = Status.FAILED_PRECONDITION.withDescription(String.valueOf(cause));
			}
			response.onError(status.asRuntimeException());
		});
	}

	private static void answerState(NodeStateSource source, byte[] request, StreamObserver<byte[]> response) {
		if (request.length < GROUP_ID_BYTES) {
			response.onError(Status.INVALID_ARGUMENT
					.withDescription("Expected a 16-byte group id, got " + request.length + " bytes")
					.asRuntimeException());
			return;
		}
		var buffer = ByteBuffer.wrap(request);
		var groupId = RaftGroupId.valueOf(new UUID(buffer.getLong(), buffer.getLong()));
		var clusterId = request.length == GROUP_ID_BYTES ? null
				: new String(request, GROUP_ID_BYTES, request.length - GROUP_ID_BYTES, StandardCharsets.UTF_8);
		NodeState state;
		try {
			state = source.state(groupId, clusterId);
		} catch (RuntimeException e) {
			// Its database closes while it installs a snapshot: the caller asks again. A node that does not run is UNAVAILABLE.
			response.onError(Status.ABORTED.withDescription(e.toString()).asRuntimeException());
			return;
		}
		response.onNext(state == null ? new byte[0] : encode(state));
		response.onCompleted();
	}

	/** Runs a cluster operation on the node behind {@code channel}: see {@link #OPS}. */
	static CompletableFuture<byte[]> ops(Channel channel, RaftGroupId groupId, byte[] request, Duration timeout) {
		var message = ByteBuffer.allocate(GROUP_ID_BYTES + request.length).put(encodeGroupId(groupId)).put(request).array();
		return call(channel, OPS, message, timeout);
	}

	private static void answerOps(OpsSource source, byte[] request, StreamObserver<byte[]> response) {
		if (request.length < GROUP_ID_BYTES) {
			response.onError(Status.INVALID_ARGUMENT
					.withDescription("Expected a 16-byte group id, got " + request.length + " bytes")
					.asRuntimeException());
			return;
		}
		var buffer = ByteBuffer.wrap(request);
		var groupId = RaftGroupId.valueOf(new UUID(buffer.getLong(), buffer.getLong()));
		var rest = Arrays.copyOfRange(request, GROUP_ID_BYTES, request.length);
		CompletableFuture<byte[]> answer;
		try {
			answer = source.answer(groupId, rest);
		} catch (RuntimeException e) {
			answer = CompletableFuture.failedFuture(e);
		}
		answer.whenComplete((value, error) -> {
			if (error == null) {
				response.onNext(value);
				response.onCompleted();
				return;
			}
			var cause = ReplicatedConnection.unwrap(error);
			var status = Status.fromThrowable(cause);
			if (status.getCode() == Status.Code.UNKNOWN) {
				status = Status.ABORTED.withDescription(String.valueOf(cause));
			}
			response.onError(status.asRuntimeException());
		});
	}

	private static byte[] encodeGroupId(RaftGroupId groupId) {
		var uuid = groupId.getUuid();
		return ByteBuffer.allocate(16)
				.putLong(uuid.getMostSignificantBits())
				.putLong(uuid.getLeastSignificantBits())
				.array();
	}

	private enum BytesMarshaller implements MethodDescriptor.Marshaller<byte[]> {
		INSTANCE;

		@Override
		public InputStream stream(byte[] value) {
			return new ByteArrayInputStream(value);
		}

		@Override
		public byte[] parse(InputStream stream) {
			try {
				return stream.readAllBytes();
			} catch (IOException e) {
				throw new UncheckedIOException(e);
			}
		}
	}
}
