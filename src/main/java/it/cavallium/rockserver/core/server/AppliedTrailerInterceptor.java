package it.cavallium.rockserver.core.server;

import io.grpc.ForwardingServerCall;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import it.cavallium.rockserver.core.common.RocksDBException;
import org.jetbrains.annotations.Nullable;

/**
 * Adds {@link RocksDBException#APPLIED_METADATA_KEY} to the trailers of a call that fails with a
 * {@link RocksDBException}: whether the failure says that the write was not applied
 * ({@link RocksDBException#isNotApplied()}). The client reads that, not the message text, to tell a write it may send
 * again from one whose outcome is unknown. The status keeps its cause until the call is closed here, and the message
 * is unchanged, so older clients read it as before.
 */
final class AppliedTrailerInterceptor implements ServerInterceptor {

	static final Metadata.Key<String> KEY = Metadata.Key.of(RocksDBException.APPLIED_METADATA_KEY,
			Metadata.ASCII_STRING_MARSHALLER);

	@Override
	public <Q, R> Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers, ServerCallHandler<Q, R> next) {
		return next.startCall(new ForwardingServerCall.SimpleForwardingServerCall<>(call) {
			@Override
			public void close(Status status, Metadata trailers) {
				if (!status.isOk()) {
					var applied = appliedValue(status);
					if (applied != null) {
						trailers.discardAll(KEY);
						trailers.put(KEY, applied);
					}
				}
				super.close(status, trailers);
			}
		}, headers);
	}

	/** The trailer's value for a failed call, or null if the failure is not a {@link RocksDBException}. */
	static @Nullable String appliedValue(Status status) {
		var rocks = GrpcRequestErrors.findRocksDBException(status.getCause());
		if (rocks == null) {
			return null;
		}
		return rocks.isNotApplied() ? RocksDBException.NOT_APPLIED : RocksDBException.MAYBE_APPLIED;
	}
}
