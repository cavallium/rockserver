package it.cavallium.rockserver.core.replication.ratis;

import java.io.EOFException;
import java.io.IOException;
import java.io.Serial;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

final class RatisProcessAdapterSocketWire {

    static final int DEFAULT_MAX_RPC_MESSAGE_BYTES = 64 * 1024 * 1024;
    static final int DEFAULT_MAX_HEALTH_MESSAGE_BYTES = 8192;

    private static final int BUFFER_SIZE = 8192;
    private static final int MAX_WRITE_TIMEOUT_CLOSE_WAIT_MILLIS = 100;
    private static final ScheduledThreadPoolExecutor WRITE_TIMEOUT_EXECUTOR = writeTimeoutExecutor();

    private RatisProcessAdapterSocketWire() {
    }

    static String readUtf8(Socket socket,
                           int maxMessageBytes,
                           long startNanos,
                           int timeoutMillis,
                           String operation) throws IOException {
        requireNonNull(socket, "socket");
        requirePositiveMaxMessageBytes(maxMessageBytes);
        var input = socket.getInputStream();
        var output = new BoundedBytes(maxMessageBytes);
        var buffer = new byte[Math.min(maxMessageBytes, BUFFER_SIZE)];
        long total = 0;
        while (true) {
            var remaining = (int) (maxMessageBytes - total);
            var readLimit = remaining == 0 ? 1 : Math.min(buffer.length, remaining);
            socket.setSoTimeout(remainingTimeoutMillis(startNanos, timeoutMillis, operation));
            var read = input.read(buffer, 0, readLimit);
            if (read < 0) {
                return decodeUtf8(output, output.size());
            }
            total += read;
            if (total > maxMessageBytes) {
                throw oversized(maxMessageBytes);
            }
            output.write(buffer, read);
        }
    }

    static String readUtf8Line(Socket socket,
                               int maxMessageBytes,
                               long startNanos,
                               int timeoutMillis) throws IOException {
        requireNonNull(socket, "socket");
        requirePositiveMaxMessageBytes(maxMessageBytes);
        var input = socket.getInputStream();
        var output = new BoundedBytes(maxMessageBytes);
        long total = 0;
        while (true) {
            socket.setSoTimeout(remainingTimeoutMillis(
                    startNanos,
                    timeoutMillis,
                    "Ratis process adapter health request"
            ));
            var next = input.read();
            if (next < 0) {
                throw new EOFException("Ratis process adapter health request ended before line terminator");
            }
            total++;
            if (total > maxMessageBytes) {
                throw oversized(maxMessageBytes);
            }
            if (next == '\n') {
                return decodeLine(output);
            }
            output.write(next);
            if (total == maxMessageBytes) {
                throw oversized(maxMessageBytes);
            }
        }
    }

    static void writeUtf8(Socket socket,
                          byte[] payload,
                          long startNanos,
                          int timeoutMillis,
                          String operation) throws IOException {
        requireNonNull(socket, "socket");
        requireNonNull(payload, "payload");
        requireNonNull(operation, "operation");
        var timedOut = new AtomicBoolean();
        var timeoutCloseResult = new CompletableFuture<Throwable>();
        var timeoutTask = WRITE_TIMEOUT_EXECUTOR.schedule(() -> {
            timedOut.set(true);
            try {
                Thread.ofVirtual().name("rockserver-ratis-socket-write-timeout-close").start(() -> {
                    Throwable closeFailure = null;
                    try {
                        socket.close();
                    } catch (IOException | RuntimeException | Error e) {
                        closeFailure = e;
                    }
                    timeoutCloseResult.complete(closeFailure);
                });
            } catch (RuntimeException | Error e) {
                timeoutCloseResult.complete(e);
            }
        }, remainingTimeoutMillis(startNanos, timeoutMillis, operation), TimeUnit.MILLISECONDS);
        IOException failure = null;
        try {
            socket.getOutputStream().write(payload);
            socket.getOutputStream().flush();
        } catch (IOException e) {
            failure = e;
        } finally {
            if (!timeoutTask.cancel(false)) {
                timedOut.set(true);
            }
        }
        if (timedOut.get()) {
            var timeoutFailure = new SocketTimeoutException(operation + " timed out");
            if (failure != null) {
                timeoutFailure.initCause(failure);
            }
            var closeFailure = awaitTimeoutClose(timeoutCloseResult, operation);
            if (closeFailure != null) {
                timeoutFailure.addSuppressed(closeFailure);
            }
            throw timeoutFailure;
        }
        if (failure != null) {
            throw failure;
        }
    }

    private static Throwable awaitTimeoutClose(CompletableFuture<Throwable> timeoutCloseResult, String operation) {
        try {
            return timeoutCloseResult.get(MAX_WRITE_TIMEOUT_CLOSE_WAIT_MILLIS, TimeUnit.MILLISECONDS);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return new IOException("Interrupted while waiting for " + operation + " timed-out socket close", e);
        } catch (ExecutionException e) {
            return new IOException("Unexpected " + operation + " timed-out socket close completion failure", e);
        } catch (TimeoutException e) {
            return new IOException(operation + " timed-out socket close did not complete within "
                    + MAX_WRITE_TIMEOUT_CLOSE_WAIT_MILLIS + " milliseconds", e);
        }
    }

    static byte[] encodeUtf8(String payload, int maxMessageBytes) throws IOException {
        requireNonNull(payload, "payload");
        requirePositiveMaxMessageBytes(maxMessageBytes);
        var bytes = new byte[utf8Length(payload, maxMessageBytes)];
        var encoded = ByteBuffer.wrap(bytes);
        var encoder = StandardCharsets.UTF_8.newEncoder()
                .onMalformedInput(CodingErrorAction.REPORT)
                .onUnmappableCharacter(CodingErrorAction.REPORT);
        try {
            var result = encoder.encode(CharBuffer.wrap(payload), encoded, true);
            if (result.isError()) {
                result.throwException();
            }
            if (!result.isUnderflow()) {
                throw new IOException("Ratis adapter socket UTF-8 encoding exceeded its preflight size");
            }
            result = encoder.flush(encoded);
            if (result.isError()) {
                result.throwException();
            }
            if (!result.isUnderflow() || encoded.hasRemaining()) {
                throw new IOException("Ratis adapter socket UTF-8 encoding did not match its preflight size");
            }
        } catch (CharacterCodingException e) {
            throw new IOException("Ratis adapter socket message cannot be encoded as UTF-8", e);
        }
        return bytes;
    }

    static int requirePositiveMaxMessageBytes(int maxMessageBytes) {
        if (maxMessageBytes <= 0) {
            throw new IllegalArgumentException("maxMessageBytes must be positive");
        }
        return maxMessageBytes;
    }

    static void closeAfterFailedHandoff(Socket socket, Throwable failure) {
        requireNonNull(socket, "socket");
        requireNonNull(failure, "failure");
        try {
            socket.close();
        } catch (IOException | RuntimeException | Error closeFailure) {
            if (failure != closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
    }

    static int requireSocketTimeoutMillis(Duration timeout, String name) {
        requireNonNull(timeout, name);
        long timeoutMillis;
        try {
            timeoutMillis = timeout.toMillis();
        } catch (ArithmeticException e) {
            throw new IllegalArgumentException(name + " must fit socket timeout milliseconds", e);
        }
        if (timeoutMillis <= 0 || timeoutMillis > Integer.MAX_VALUE) {
            throw new IllegalArgumentException(name + " must be between 1 and " + Integer.MAX_VALUE
                    + " milliseconds");
        }
        return (int) timeoutMillis;
    }

    static int remainingTimeoutMillis(long startNanos, int timeoutMillis, String operation)
            throws SocketTimeoutException {
        var elapsedNanos = System.nanoTime() - startNanos;
        var remainingNanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis) - elapsedNanos;
        if (remainingNanos <= 0) {
            throw new SocketTimeoutException(operation + " timed out");
        }
        return (int) Math.min(timeoutMillis, TimeUnit.NANOSECONDS.toMillis(remainingNanos - 1) + 1);
    }

    static ServerSocket bindAndStartListener(ServerSocket listener,
                                             InetSocketAddress bindAddress,
                                             Consumer<ServerSocket> startListener) throws IOException {
        requireNonNull(listener, "listener");
        requireNonNull(bindAddress, "bindAddress");
        requireNonNull(startListener, "startListener");
        try {
            listener.bind(bindAddress);
            startListener.accept(listener);
            return listener;
        } catch (IOException | RuntimeException | Error e) {
            try {
                listener.close();
            } catch (IOException | RuntimeException | Error closeFailure) {
                if (e != closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            throw e;
        }
    }

    static void shutdownListener(ServerSocket listener,
                                 AcceptedSockets acceptedSockets,
                                 ExecutorService acceptExecutor,
                                 ExecutorService requestExecutor,
                                 ExecutorService operationExecutor,
                                 int timeoutMillis,
                                 String name) throws IOException {
        requireNonNull(acceptExecutor, "acceptExecutor");
        requireNonNull(acceptedSockets, "acceptedSockets");
        requireNonNull(requestExecutor, "requestExecutor");
        requireNonNull(name, "name");
        Throwable failure = null;
        try {
            if (listener != null) {
                listener.close();
            }
        } catch (IOException | RuntimeException | Error e) {
            failure = e;
        }
        try {
            acceptedSockets.close();
        } catch (IOException | RuntimeException | Error e) {
            failure = merge(failure, e);
        }
        var startNanos = System.nanoTime();
        var interrupted = new ShutdownInterruption();
        failure = shutdownNow(acceptExecutor, failure);
        failure = shutdown(requestExecutor, failure);
        failure = awaitTermination(acceptExecutor, startNanos, timeoutMillis, name + " accept executor", failure,
                interrupted);
        failure = shutdownAndAwait(requestExecutor, startNanos, timeoutMillis, name + " request executor", failure,
                interrupted);
        if (operationExecutor != null) {
            failure = shutdown(operationExecutor, failure);
            failure = shutdownAndAwait(operationExecutor, startNanos, timeoutMillis, name + " operation executor",
                    failure, interrupted);
        }
        interrupted.restore();
        throwFailure(failure);
    }

    private static Throwable awaitTermination(ExecutorService executor,
                                              long startNanos,
                                              int timeoutMillis,
                                              String name,
                                              Throwable failure,
                                              ShutdownInterruption interrupted) {
        while (true) {
            try {
                var remainingNanos = remainingShutdownTimeoutNanos(startNanos, timeoutMillis);
                if (remainingNanos <= 0) {
                    return merge(failure, shutdownTimeoutFailure(name, timeoutMillis));
                }
                if (!executor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                    return merge(failure, shutdownTimeoutFailure(name, timeoutMillis));
                }
                return failure;
            } catch (InterruptedException e) {
                interrupted.record();
            } catch (RuntimeException | Error e) {
                return merge(failure, e);
            }
        }
    }

    private static Throwable shutdownAndAwait(ExecutorService executor,
                                              long startNanos,
                                              int timeoutMillis,
                                              String name,
                                              Throwable failure,
                                              ShutdownInterruption interrupted) {
        try {
            if (executor.awaitTermination(remainingShutdownTimeoutNanos(startNanos, timeoutMillis), TimeUnit.NANOSECONDS)) {
                return failure;
            }
        } catch (InterruptedException e) {
            interrupted.record();
            failure = shutdownNow(executor, failure);
            return awaitTermination(executor, startNanos, timeoutMillis, name, failure, interrupted);
        } catch (RuntimeException | Error e) {
            return shutdownNow(executor, merge(failure, e));
        }
        failure = shutdownNow(executor, failure);
        return awaitTermination(executor, startNanos, timeoutMillis, name, failure, interrupted);
    }

    private static IOException shutdownTimeoutFailure(String name, int timeoutMillis) {
        return new IOException(name + " did not stop within " + timeoutMillis + " milliseconds");
    }

    private static long remainingShutdownTimeoutNanos(long startNanos, int timeoutMillis) {
        return Math.max(0, TimeUnit.MILLISECONDS.toNanos(timeoutMillis) - (System.nanoTime() - startNanos));
    }

    private static Throwable shutdown(ExecutorService executor, Throwable failure) {
        try {
            executor.shutdown();
        } catch (RuntimeException | Error e) {
            return merge(failure, e);
        }
        return failure;
    }

    private static Throwable shutdownNow(ExecutorService executor, Throwable failure) {
        try {
            executor.shutdownNow();
        } catch (RuntimeException | Error e) {
            return merge(failure, e);
        }
        return failure;
    }

    private static Throwable merge(Throwable failure, Throwable addition) {
        if (failure == null) {
            return addition;
        }
        if (failure != addition) {
            failure.addSuppressed(addition);
        }
        return failure;
    }

    private static void throwFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException ioException) {
            throw ioException;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IOException("Unexpected socket listener cleanup failure", failure);
        }
    }

    private static final class ShutdownInterruption {

        private boolean interrupted;

        void record() {
            interrupted = true;
        }

        void restore() {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }

    private static ScheduledThreadPoolExecutor writeTimeoutExecutor() {
        var executor = new ScheduledThreadPoolExecutor(1, task -> {
            var thread = new Thread(task, "rockserver-ratis-socket-write-timeout");
            thread.setDaemon(true);
            return thread;
        });
        executor.setRemoveOnCancelPolicy(true);
        return executor;
    }

    static final class AcceptedSockets implements AutoCloseable {

        private final Set<Socket> sockets = ConcurrentHashMap.newKeySet();
        private final AtomicBoolean closed = new AtomicBoolean();

        boolean rejectIfClosed(Socket socket) throws IOException {
            requireNonNull(socket, "socket");
            if (closed.get()) {
                closeTracked(socket);
                return true;
            }
            sockets.add(socket);
            if (closed.get()) {
                closeTracked(socket);
                return true;
            }
            return false;
        }

        private void closeTracked(Socket socket) throws IOException {
            sockets.add(socket);
            socket.close();
            sockets.remove(socket);
        }

        void removeAfterWorkerClose(Socket socket) {
            if (socket.isClosed()) {
                sockets.remove(socket);
            }
        }

        @SuppressWarnings("unused")
        int size() {
            return sockets.size();
        }

        @Override
        public void close() throws IOException {
            closed.set(true);
            Throwable failure = null;
            for (var socket : sockets) {
                try {
                    closeTracked(socket);
                } catch (IOException | RuntimeException | Error e) {
                    failure = merge(failure, e);
                }
            }
            throwFailure(failure);
        }
    }

    static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static String decodeLine(BoundedBytes output) throws IOException {
        var length = output.size();
        if (length > 0 && output.byteAt(length - 1) == '\r') {
            length--;
        }
        return decodeUtf8(output, length);
    }

    private static String decodeUtf8(BoundedBytes bytes, int length) throws IOException {
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes.buffer(), 0, length))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new InvalidUtf8Exception("Ratis adapter socket message is not valid UTF-8", e);
        }
    }

    private static int utf8Length(String payload, int maxMessageBytes) throws IOException {
        var encodedBytes = 0;
        for (int i = 0; i < payload.length(); i++) {
            var character = payload.charAt(i);
            final int characterBytes;
            if (character <= 0x7F) {
                characterBytes = 1;
            } else if (character <= 0x7FF) {
                characterBytes = 2;
            } else if (Character.isHighSurrogate(character)) {
                if (i + 1 >= payload.length() || !Character.isLowSurrogate(payload.charAt(++i))) {
                    throw new IOException("Ratis adapter socket message cannot be encoded as UTF-8");
                }
                characterBytes = 4;
            } else if (Character.isLowSurrogate(character)) {
                throw new IOException("Ratis adapter socket message cannot be encoded as UTF-8");
            } else {
                characterBytes = 3;
            }
            if (encodedBytes > maxMessageBytes - characterBytes) {
                throw oversized(maxMessageBytes);
            }
            encodedBytes += characterBytes;
        }
        return encodedBytes;
    }

    private static MessageTooLargeException oversized(int maxMessageBytes) {
        return new MessageTooLargeException(
                "Ratis adapter socket message exceeds max size of " + maxMessageBytes + " bytes"
        );
    }

    private static final class BoundedBytes {

        private final int maxBytes;
        private byte[] bytes;
        private int size;

        private BoundedBytes(int maxBytes) {
            this.maxBytes = requirePositiveMaxMessageBytes(maxBytes);
            this.bytes = new byte[Math.min(maxBytes, BUFFER_SIZE)];
        }

        private void write(byte[] source, int length) throws MessageTooLargeException {
            java.util.Objects.checkFromIndexSize(0, length, source.length);
            if (length > maxBytes - size) {
                throw oversized(maxBytes);
            }
            ensureCapacity(size + length);
            System.arraycopy(source, 0, bytes, size, length);
            size += length;
        }

        private void write(int value) throws MessageTooLargeException {
            if (size == maxBytes) {
                throw oversized(maxBytes);
            }
            ensureCapacity(size + 1);
            bytes[size++] = (byte) value;
        }

        private int size() {
            return size;
        }

        private int byteAt(int index) {
            return bytes[index] & 0xFF;
        }

        private byte[] buffer() {
            return bytes;
        }

        private void ensureCapacity(int requiredCapacity) throws MessageTooLargeException {
            if (requiredCapacity > maxBytes) {
                throw oversized(maxBytes);
            }
            if (requiredCapacity <= bytes.length) {
                return;
            }
            var doubledCapacity = Math.min(maxBytes, bytes.length * 2L);
            bytes = Arrays.copyOf(bytes, (int) Math.max(requiredCapacity, doubledCapacity));
        }
    }

    static final class MessageTooLargeException extends IOException {

        @Serial
        private static final long serialVersionUID = 1L;

        private MessageTooLargeException(String message) {
            super(message);
        }
    }

    static final class InvalidUtf8Exception extends IOException {

        @Serial
        private static final long serialVersionUID = 1L;

        private InvalidUtf8Exception(String message, Throwable cause) {
            super(message, cause);
        }
    }
}
