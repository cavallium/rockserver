package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.junit.jupiter.api.Test;

class ExpectedGrpcStreamCloseLogFilterTest {

	@Test
	void suppressesOnlyTheExpectedWarningForAClosedStream() {
		var expected = warning("Stream Error",
				Http2Exception.streamError(7,
						Http2Error.STREAM_CLOSED,
						"Stream closed before write could take place"));
		assertTrue(GrpcServer.isExpectedGrpcClientCancellationForTesting(expected));

		var differentFailure = warning("Stream Error",
				Http2Exception.streamError(7, Http2Error.INTERNAL_ERROR, "unexpected write failure"));
		assertFalse(GrpcServer.isExpectedGrpcClientCancellationForTesting(differentFailure));

		var connectionFailure = warning("Stream Error",
				Http2Exception.connectionError(Http2Error.INTERNAL_ERROR,
						"Stream closed before write could take place"));
		assertFalse(GrpcServer.isExpectedGrpcClientCancellationForTesting(connectionFailure));

		var wrongMessage = warning("Unexpected transport failure", expected.getThrown());
		assertFalse(GrpcServer.isExpectedGrpcClientCancellationForTesting(wrongMessage));

		expected.setLevel(Level.SEVERE);
		assertFalse(GrpcServer.isExpectedGrpcClientCancellationForTesting(expected));
	}

	private static LogRecord warning(String message, Throwable failure) {
		var record = new LogRecord(Level.WARNING, message);
		record.setThrown(failure);
		return record;
	}
}
