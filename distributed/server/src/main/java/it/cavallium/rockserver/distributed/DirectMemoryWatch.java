package it.cavallium.rockserver.distributed;

import java.util.Locale;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingStream;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Makes running out of direct memory as fatal as running out of heap.
 *
 * <p>{@code -XX:+ExitOnOutOfMemoryError} acts only on errors that the JVM itself throws. Direct memory is counted in Java
 * code: Netty's pooled allocator throws its own {@code OutOfDirectMemoryError} past its limit
 * ({@code -Dio.netty.maxDirectMemory}, by default {@code -XX:MaxDirectMemorySize}, by default the heap size), and
 * {@code ByteBuffer.allocateDirect} throws an {@link OutOfMemoryError} "Cannot reserve ... direct buffer memory". The JVM
 * goes on after either. On a thread of Ratis that is the same hazard as a heap error: Ratis catches it and goes on without
 * what it was doing, and in the appender of a new member that ends the catch-up as if it had finished (see "Heap" in the
 * design document). gRPC encodes an outgoing message into direct buffers on the thread that sends it, the appender's.</p>
 *
 * <p>No hook runs when such an error is made, so this watches JFR's {@code jdk.JavaExceptionThrow} event, which the JVM
 * records for every {@link Throwable} constructed, without stack traces, and calls back for one of those two. The event
 * arrives within about a second. {@code jdk.JavaErrorThrow}, which records errors only, leaves out every
 * {@link OutOfMemoryError} on purpose, but it must be on too: without it, the JDK 27 used here records no error at all in
 * {@code jdk.JavaExceptionThrow} (seen, not documented). Recording costs a small fixed amount per throwable made, which
 * the node does not make on its hot paths. The error may have been caught and retried meanwhile, but a node that hit
 * the limit once is likely to hit it again, and a restart is the recovery path that every test covers.</p>
 */
final class DirectMemoryWatch implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(DirectMemoryWatch.class);
	private static final String EVENT = "jdk.JavaExceptionThrow";
	private static final String ERROR_EVENT = "jdk.JavaErrorThrow";

	private final RecordingStream stream;

	private DirectMemoryWatch(RecordingStream stream) {
		this.stream = stream;
	}

	/**
	 * Starts watching; {@code onExhausted} gets the class and message of the first such error, once, on a thread of JFR.
	 *
	 * @return null if this JVM has no JFR (the node then runs without the watch, and says so)
	 */
	static @Nullable DirectMemoryWatch start(Consumer<String> onExhausted) {
		RecordingStream stream;
		try {
			stream = new RecordingStream();
		} catch (RuntimeException | Error ex) {
			LOG.warn("JFR is not available: running out of direct memory will not stop this node", ex);
			return null;
		}
		stream.enable(EVENT).withoutStackTrace();
		stream.enable(ERROR_EVENT).withoutStackTrace();
		var fired = new AtomicBoolean();
		stream.onEvent(EVENT, event -> {
			var error = describe(event);
			if (error != null && fired.compareAndSet(false, true)) {
				onExhausted.accept(error);
			}
		});
		stream.startAsync();
		return new DirectMemoryWatch(stream);
	}

	private static @Nullable String describe(RecordedEvent event) {
		var thrownClass = event.getClass("thrownClass");
		var className = thrownClass != null ? thrownClass.getName() : "";
		String message = event.getString("message");
		return isDirectMemoryError(className, message) ? className + ": " + message : null;
	}

	/** Netty's error past its own limit (shaded or not), or the JDK's past {@code -XX:MaxDirectMemorySize}. */
	static boolean isDirectMemoryError(String className, @Nullable String message) {
		if (className.endsWith(".OutOfDirectMemoryError")) {
			return true;
		}
		return className.equals(OutOfMemoryError.class.getName()) && message != null
				&& message.toLowerCase(Locale.ROOT).contains("direct buffer memory");
	}

	@Override
	public void close() {
		stream.close();
	}
}
