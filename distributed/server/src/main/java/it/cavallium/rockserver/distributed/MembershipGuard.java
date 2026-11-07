package it.cavallium.rockserver.distributed;

import java.util.Collection;
import java.util.function.Supplier;
import org.apache.ratis.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
import org.jetbrains.annotations.Nullable;

/** Skeleton (round 14, test commit): lets every request through unchanged. */
final class MembershipGuard implements ServerInterceptor {

	static final String UNSAFE_HEADER = "rockserver-unsafe-membership-change";
	static final Metadata.Key<String> UNSAFE = Metadata.Key.of(UNSAFE_HEADER, Metadata.ASCII_STRING_MARSHALLER);

	private final Supplier<RaftServer> server;

	MembershipGuard(Supplier<RaftServer> server) {
		this.server = server;
	}

	record Decision(@Nullable SetConfigurationRequestProto forward, @Nullable Exception refusal) {}

	static Decision decide(SetConfigurationRequestProto request, Collection<RaftPeer> servers,
			Collection<RaftPeer> listeners, boolean unsafe) {
		return new Decision(request, null);
	}

	@Override
	public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
			ServerCallHandler<Q, R> next) {
		return next.startCall(call, headers);
	}
}
