package it.cavallium.rockserver.core.server;


import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;


/** Adds the leader's address to the response headers, or to the trailers of a call that sends no headers. */
final class LeaderHintInterceptor implements ServerInterceptor {

	private static final Metadata.Key<String> KEY = Metadata.Key.of(
			it.cavallium.rockserver.core.common.LeaderHint.METADATA_KEY, Metadata.ASCII_STRING_MARSHALLER);
	private final it.cavallium.rockserver.core.common.LeaderHint hint;

	LeaderHintInterceptor(it.cavallium.rockserver.core.common.LeaderHint hint) {
		this.hint = hint;
	}

	@Override
	public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
			Metadata headers,
			ServerCallHandler<ReqT, RespT> next) {
		return next.startCall(new io.grpc.ForwardingServerCall.SimpleForwardingServerCall<>(call) {
			private volatile boolean headersSent;

			@Override
			public void sendHeaders(Metadata responseHeaders) {
				add(responseHeaders);
				headersSent = true;
				super.sendHeaders(responseHeaders);
			}

			@Override
			public void close(io.grpc.Status status, Metadata trailers) {
				if (!headersSent) {
					add(trailers);
				}
				super.close(status, trailers);
			}
		}, headers);
	}

	private void add(Metadata metadata) {
		String address;
		try {
			address = hint.leaderApiAddress();
		} catch (RuntimeException e) {
			return;
		}
		if (address != null) {
			metadata.discardAll(KEY);
			metadata.put(KEY, address);
		}
	}
}
