package it.cavallium.rockserver.core.server;


import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import java.time.Duration;

import java.util.concurrent.TimeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** System properties and timeout arithmetic of {@link GrpcServer}. */
final class GrpcServerSettings {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());

	private GrpcServerSettings() {
	}

	static long saturatedTimeoutNanos(Duration timeout) {
		if (timeout.isNegative()) {
			throw new IllegalArgumentException("timeout must not be negative");
		}
		try {
			return timeout.toNanos();
		} catch (ArithmeticException overflow) {
			return Long.MAX_VALUE;
		}
	}

	static long remainingTimeoutNanos(long timeoutNanos, long waitStartedNanos) {
		long elapsedNanos = System.nanoTime() - waitStartedNanos;
		if (elapsedNanos <= 0L) {
			return timeoutNanos;
		}
		return elapsedNanos >= timeoutNanos ? 0L : timeoutNanos - elapsedNanos;
	}

	static Duration durationProperty(String name, Duration defaultValue) {
		var value = System.getProperty(name);
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			long millis = Long.parseLong(value);
			if (millis < 0) {
				LOG.warn("Invalid negative duration for system property {}: {}", name, value);
				return defaultValue;
			}
			return Duration.ofNanos(TimeUnit.MILLISECONDS.toNanos(millis));
		} catch (NumberFormatException ex) {
			LOG.warn("Invalid duration in milliseconds for system property {}: {}", name, value);
			return defaultValue;
		}
	}
}
