package it.cavallium.rockserver.core.server;


import io.grpc.Grpc;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import java.net.SocketAddress;


class AdaptiveCompressionInterceptor implements ServerInterceptor {

	@Override
	public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
			Metadata headers,
			ServerCallHandler<ReqT, RespT> next) {
		SocketAddress remoteAddress = call.getAttributes().get(Grpc.TRANSPORT_ATTR_REMOTE_ADDR);
		call.setCompression(GrpcServer.shouldCompressGrpcResponse(remoteAddress) ? "gzip" : "identity");
		return next.startCall(call, headers);
	}
}
