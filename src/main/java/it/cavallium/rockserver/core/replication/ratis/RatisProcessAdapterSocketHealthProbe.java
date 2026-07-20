package it.cavallium.rockserver.core.replication.ratis;

import java.io.IOException;
import java.io.Serial;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.time.Duration;
import java.util.concurrent.TimeUnit;

import static it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterHealthStatus.STARTING;

public final class RatisProcessAdapterSocketHealthProbe implements RatisProcessAdapterHealthProbe {

    public static final String HEALTH_REQUEST = "HEALTH\n";
    private static final String TIMEOUT_MESSAGE = "Ratis adapter process did not become healthy before timeout";
    private static final int RETRY_DELAY_MILLIS = 100;

    private final InetSocketAddress address;
    private final int timeoutMillis;
    private final long timeoutNanos;
    private final int maxMessageBytes;

    public RatisProcessAdapterSocketHealthProbe(InetSocketAddress address, Duration timeout) {
        this(address, timeout, RatisProcessAdapterSocketWire.DEFAULT_MAX_HEALTH_MESSAGE_BYTES);
    }

    public RatisProcessAdapterSocketHealthProbe(InetSocketAddress address, Duration timeout, int maxMessageBytes) {
        this.address = RatisProcessAdapterSocketWire.requireNonNull(address, "address");
        this.timeoutMillis = RatisProcessAdapterSocketWire.requireSocketTimeoutMillis(timeout, "timeout");
        this.timeoutNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        this.maxMessageBytes = RatisProcessAdapterSocketWire.requirePositiveMaxMessageBytes(maxMessageBytes);
    }

    @Override
    public RatisProcessAdapterHealth check(RatisProcessAdapterCommand command) throws IOException {
        RatisProcessAdapterSocketWire.requireNonNull(command, "command");
        var startNanos = System.nanoTime();
        IOException lastFailure = null;
        RatisProcessAdapterHealth lastStartingHealth = null;
        while (System.nanoTime() - startNanos < timeoutNanos) {
            try {
                var health = checkOnce(startNanos);
                if (health.status() != STARTING) {
                    return health;
                }
                lastStartingHealth = health;
                lastFailure = null;
                sleepBeforeRetry(startNanos, null);
            } catch (RatisProcessAdapterSocketWire.MessageTooLargeException
                     | RatisProcessAdapterSocketWire.InvalidUtf8Exception
                     | InvalidHealthResponseException e) {
                throw e;
            } catch (IOException e) {
                lastFailure = e;
                sleepBeforeRetry(startNanos, e);
            }
        }
        throw lastFailure == null
                ? new IOException(timeoutMessage(lastStartingHealth))
                : lastFailure;
    }

    private RatisProcessAdapterHealth checkOnce(long startNanos) throws IOException {
        try (var socket = new Socket()) {
            socket.connect(address, remainingTimeoutMillis(startNanos));
            RatisProcessAdapterSocketWire.writeUtf8(
                    socket,
                    RatisProcessAdapterSocketWire.encodeUtf8(HEALTH_REQUEST, maxMessageBytes),
                    startNanos,
                    timeoutMillis,
                    "Ratis adapter process health request"
            );
            var payload = RatisProcessAdapterSocketWire.readUtf8(
                    socket,
                    maxMessageBytes,
                    startNanos,
                    timeoutMillis,
                    "Ratis adapter process health response"
            );
            try {
                return RatisProcessAdapterHealthCodec.decode(payload);
            } catch (RuntimeException e) {
                throw new InvalidHealthResponseException("Invalid Ratis adapter process health response", e);
            }
        }
    }

    private void sleepBeforeRetry(long startNanos, IOException retryFailure) throws IOException {
        var remainingMillis = remainingTimeoutMillisOrZero(startNanos);
        if (remainingMillis == 0) {
            return;
        }
        try {
            TimeUnit.MILLISECONDS.sleep(Math.min(RETRY_DELAY_MILLIS, remainingMillis));
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            var failure = new IOException("Interrupted while waiting for Ratis adapter process health", e);
            if (retryFailure != null) {
                failure.addSuppressed(retryFailure);
            }
            throw failure;
        }
    }

    private int remainingTimeoutMillis(long startNanos) throws IOException {
        var remainingMillis = remainingTimeoutMillisOrZero(startNanos);
        if (remainingMillis == 0) {
            throw new IOException(TIMEOUT_MESSAGE);
        }
        return remainingMillis;
    }

    private int remainingTimeoutMillisOrZero(long startNanos) {
        var remainingNanos = timeoutNanos - (System.nanoTime() - startNanos);
        if (remainingNanos <= 0) {
            return 0;
        }
        return (int) Math.min(timeoutMillis, TimeUnit.NANOSECONDS.toMillis(remainingNanos - 1) + 1);
    }

    private static String timeoutMessage(RatisProcessAdapterHealth lastStartingHealth) {
        if (lastStartingHealth == null || lastStartingHealth.message().isBlank()) {
            return TIMEOUT_MESSAGE;
        }
        return TIMEOUT_MESSAGE + ": " + lastStartingHealth.message();
    }

    private static final class InvalidHealthResponseException extends IOException {

        @Serial
        private static final long serialVersionUID = 1L;

        private InvalidHealthResponseException(String message, Throwable cause) {
            super(message, cause);
        }
    }

}
