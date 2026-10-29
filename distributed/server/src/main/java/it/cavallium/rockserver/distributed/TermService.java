package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
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

/**
 * Tiny gRPC service on the Ratis server port, using the shaded gRPC that Ratis ships
 * ({@code org.apache.ratis.thirdparty.io.grpc}). It answers:
 * <ul>
 *   <li>"what is your current term in group G?", for {@link ReadBarrier}. Request: the 16-byte group UUID.
 *   Response: 8-byte big-endian current term, or -1 if this server has no such group;</li>
 *   <li>"where is your Rockserver API?", for {@link LeaderForwarder}, which knows the Raft address of every member
 *   from the Raft configuration but the API address only of the members it was started with. Request: empty.
 *   Response: UTF-8 {@code host:port}, as other members reach it.</li>
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

	private TermService() {}

	/**
	 * A customizer that adds this service to the Ratis gRPC server; register it through Ratis {@code Parameters}.
	 *
	 * @param apiAddress this node's API address as other members reach it
	 */
	static GrpcServices.Customizer customizer(Supplier<RaftServer> server, HostAndPort apiAddress) {
		ServerCalls.UnaryMethod<byte[], byte[]> handler = (request, response) -> answer(server, request, response);
		var address = (apiAddress.host() + ":" + apiAddress.port()).getBytes(StandardCharsets.UTF_8);
		ServerCalls.UnaryMethod<byte[], byte[]> addressHandler = (_, response) -> {
			response.onNext(address);
			response.onCompleted();
		};
		var service = ServerServiceDefinition.builder(SERVICE_NAME)
				.addMethod(CURRENT_TERM, ServerCalls.asyncUnaryCall(handler))
				.addMethod(API_ADDRESS, ServerCalls.asyncUnaryCall(addressHandler))
				.build();
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

	private static CompletableFuture<byte[]> call(Channel channel, MethodDescriptor<byte[], byte[]> method,
			byte[] request, Duration timeout) {
		var result = new CompletableFuture<byte[]>();
		// Wait for a connection being (re)established, within the deadline, rather than failing at once.
		var options = CallOptions.DEFAULT
				.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
				.withWaitForReady();
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
