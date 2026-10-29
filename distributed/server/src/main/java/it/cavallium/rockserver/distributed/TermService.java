package it.cavallium.rockserver.distributed;

import java.util.function.Supplier;
import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.server.RaftServer;

/**
 * Tiny gRPC service on the Ratis server port answering "what is your current term in group G?", using the
 * shaded gRPC that Ratis ships ({@code org.apache.ratis.thirdparty.io.grpc}). Used by {@link ReadBarrier}.
 * Request: the 16-byte group UUID. Response: 8-byte big-endian current term, or -1 if this server has no such group.
 */
final class TermService {

	static final String SERVICE_NAME = "rockserver.distributed.Term";

	private TermService() {}

	/** A customizer that adds this service to the Ratis gRPC server; register it through Ratis {@code Parameters}. */
	static GrpcServices.Customizer customizer(Supplier<RaftServer> server) {
		throw new UnsupportedOperationException("TODO");
	}
}
