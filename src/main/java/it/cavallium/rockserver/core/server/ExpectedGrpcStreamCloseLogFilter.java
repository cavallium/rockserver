package it.cavallium.rockserver.core.server;

import io.netty.handler.codec.http2.Http2Exception;
import java.time.Duration;
import java.util.Collections;
import java.util.IdentityHashMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.logging.Filter;
import java.util.logging.Level;
import java.util.logging.LogRecord;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Removes one noisy JUL stack trace emitted by gRPC Netty after a peer has
 * already cancelled its HTTP/2 stream. The aggregate remains visible through
 * Rockserver's normal logger; every other gRPC JUL record is delegated
 * unchanged.
 */
final class ExpectedGrpcStreamCloseLogFilter implements Filter {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());
	private static final String NETTY_SERVER_LOGGER = "io.grpc.netty.NettyServerHandler";
	private static final String STREAM_ERROR_MESSAGE = "Stream Error";
	private static final String CLOSED_BEFORE_WRITE_MESSAGE = "Stream closed before write could take place";
	private static final long REPORT_INTERVAL_NANOS = Duration.ofMinutes(1).toNanos();
	private static final AtomicBoolean INSTALLED = new AtomicBoolean();
	private static final AtomicLong LAST_REPORT_NANOS = new AtomicLong();
	private static final AtomicLong SUPPRESSED = new AtomicLong();

	private final @Nullable Filter delegate;

	private ExpectedGrpcStreamCloseLogFilter(@Nullable Filter delegate) {
		this.delegate = delegate;
	}

	static void install() {
		if (!INSTALLED.compareAndSet(false, true)) {
			return;
		}
		try {
			var logger = java.util.logging.Logger.getLogger(NETTY_SERVER_LOGGER);
			logger.setFilter(new ExpectedGrpcStreamCloseLogFilter(logger.getFilter()));
		} catch (RuntimeException failure) {
			INSTALLED.set(false);
			LOG.warn("Could not install the expected gRPC client-cancellation log filter", failure);
		}
	}

	@Override
	public boolean isLoggable(LogRecord record) {
		if (delegate != null && !delegate.isLoggable(record)) {
			return false;
		}
		if (!isExpectedClientCancellation(record)) {
			return true;
		}
		reportExpectedClientCancellation();
		return false;
	}

	static boolean isExpectedClientCancellation(LogRecord record) {
		if (record == null
				|| record.getLevel().intValue() != Level.WARNING.intValue()
				|| !STREAM_ERROR_MESSAGE.equals(record.getMessage())) {
			return false;
		}
		var visited = Collections.newSetFromMap(new IdentityHashMap<Throwable, Boolean>());
		for (var failure = record.getThrown(); failure != null && visited.add(failure); failure = failure.getCause()) {
			if (failure instanceof Http2Exception.StreamException
					&& CLOSED_BEFORE_WRITE_MESSAGE.equals(failure.getMessage())) {
				return true;
			}
		}
		return false;
	}

	private static void reportExpectedClientCancellation() {
		SUPPRESSED.incrementAndGet();
		long now = System.nanoTime();
		long previous = LAST_REPORT_NANOS.get();
		if ((previous == 0L || now - previous >= REPORT_INTERVAL_NANOS)
				&& LAST_REPORT_NANOS.compareAndSet(previous, now)) {
			long suppressed = SUPPRESSED.getAndSet(0L);
			LOG.info("Client cancelled a gRPC stream before a pending write completed; "
					+ "suppressed {} expected Netty stack trace(s) since the previous report",
					suppressed);
		}
	}
}
