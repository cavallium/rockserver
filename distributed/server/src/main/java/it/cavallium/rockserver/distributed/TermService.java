package it.cavallium.rockserver.distributed;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
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
 * Tiny gRPC service on the Ratis server port answering "what is your current term in group G?", using the
 * shaded gRPC that Ratis ships ({@code org.apache.ratis.thirdparty.io.grpc}). Used by {@link ReadBarrier}.
 * Request: the 16-byte group UUID. Response: 8-byte big-endian current term, or -1 if this server has no such group.
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

	private TermService() {}

	/** A customizer that adds this service to the Ratis gRPC server; register it through Ratis {@code Parameters}. */
	static GrpcServices.Customizer customizer(Supplier<RaftServer> server) {
		ServerCalls.UnaryMethod<byte[], byte[]> handler = (request, response) -> answer(server, request, response);
		var service = ServerServiceDefinition.builder(SERVICE_NAME)
				.addMethod(CURRENT_TERM, ServerCalls.asyncUnaryCall(handler))
				.build();
		// Ratis may run admin and client services on separate ports; the peers' Raft address is the SERVER one.
		return (builder, types) -> types.contains(GrpcServices.Type.SERVER) ? builder.addService(service) : builder;
	}

	/** Asks the server behind {@code channel} for its current term in {@code groupId}. */
	static CompletableFuture<Long> currentTerm(Channel channel, RaftGroupId groupId, Duration timeout) {
		var result = new CompletableFuture<Long>();
		// Wait for a connection being (re)established, within the deadline, rather than failing at once.
		var options = CallOptions.DEFAULT
				.withDeadlineAfter(timeout.toNanos(), TimeUnit.NANOSECONDS)
				.withWaitForReady();
		ClientCalls.asyncUnaryCall(channel.newCall(CURRENT_TERM, options), encodeGroupId(groupId),
				new StreamObserver<>() {
					@Override
					public void onNext(byte[] value) {
						if (value.length != Long.BYTES) {
							result.completeExceptionally(new IOException("Bad term reply size " + value.length));
						} else {
							result.complete(ByteBuffer.wrap(value).getLong());
						}
					}

					@Override
					public void onError(Throwable error) {
						result.completeExceptionally(error);
					}

					@Override
					public void onCompleted() {
						result.completeExceptionally(new IOException("Term reply missing"));
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
