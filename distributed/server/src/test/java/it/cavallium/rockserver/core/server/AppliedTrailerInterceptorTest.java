package it.cavallium.rockserver.core.server;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.grpc.Status;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import java.util.concurrent.CompletionException;
import org.junit.jupiter.api.Test;

/**
 * The server's trailer for a failed call (CR-2): "no" for a failure flagged not applied, "maybe" for any other
 * {@link RocksDBException}, none for other failures. Read from the status the server builds
 * ({@link GrpcRequestErrors#handleError}), whose cause is kept until the call is closed.
 */
class AppliedTrailerInterceptorTest {

	@Test
	void theTrailerFollowsTheFlagOfTheFailure() {
		var notApplied = RocksDBException.notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
				"Request deadline already expired");
		assertEquals(RocksDBException.NOT_APPLIED,
				AppliedTrailerInterceptor.appliedValue(GrpcRequestErrors.handleError(notApplied)));
		assertEquals(RocksDBException.NOT_APPLIED, AppliedTrailerInterceptor.appliedValue(
				GrpcRequestErrors.handleError(new CompletionException(notApplied))));
		// Same type and message, not flagged: the message alone says nothing.
		assertEquals(RocksDBException.MAYBE_APPLIED, AppliedTrailerInterceptor.appliedValue(GrpcRequestErrors.handleError(
				RocksDBException.of(RocksDBErrorType.READ_DEADLINE_EXCEEDED, "Request deadline already expired"))));
		assertEquals(RocksDBException.MAYBE_APPLIED, AppliedTrailerInterceptor.appliedValue(GrpcRequestErrors.handleError(
				RocksDBException.of(RocksDBErrorType.OUTCOME_UNKNOWN, "lost"))));
		assertNull(AppliedTrailerInterceptor.appliedValue(Status.UNAVAILABLE));
	}
}
