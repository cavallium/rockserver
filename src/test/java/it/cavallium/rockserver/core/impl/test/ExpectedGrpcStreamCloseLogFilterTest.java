package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.netty.handler.codec.http2.Http2Error;
import io.netty.handler.codec.http2.Http2Exception;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.lang.ref.WeakReference;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import java.util.logging.Logger;
import org.junit.jupiter.api.Test;

class ExpectedGrpcStreamCloseLogFilterTest {
	private static final String NETTY_SERVER_LOGGER = "io.grpc.netty.NettyServerHandler";

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

	@Test
	void installedFilterAndLoggerSurviveUntilNettyInitializes() throws InterruptedException {
		var loggerReference = installAndReleaseLogger();

		for (int attempt = 0; attempt < 5; attempt++) {
			System.gc();
			Thread.sleep(10);
		}

		var logger = Logger.getLogger(NETTY_SERVER_LOGGER);
		assertSame(loggerReference.get(), logger,
				"Rockserver must strongly retain the filtered JUL logger");
		var filter = logger.getFilter();
		assertNotNull(filter);
		assertFalse(filter.isLoggable(warning("Stream Error",
				Http2Exception.streamError(7,
						Http2Error.STREAM_CLOSED,
						"Stream closed before write could take place"))));
		assertTrue(filter.isLoggable(warning("Stream Error",
				Http2Exception.streamError(7, Http2Error.INTERNAL_ERROR, "unexpected write failure"))));
	}

	private static WeakReference<Logger> installAndReleaseLogger() {
		GrpcServer.installExpectedGrpcClientCancellationLogFilterForTesting();
		return new WeakReference<>(Logger.getLogger(NETTY_SERVER_LOGGER));
	}

	private static LogRecord warning(String message, Throwable failure) {
		var record = new LogRecord(Level.WARNING, message);
		record.setThrown(failure);
		return record;
	}
}
