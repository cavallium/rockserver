package it.cavallium.rockserver.distributed;

import java.util.List;
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
 *
 * <p>Only an error on a thread of replication is fatal ({@link #fatalOn}): the threads of Ratis (its division threads,
 * appenders among them, its server executors, and its shaded gRPC transport). The same error on any other thread was
 * made for one request: the API's gRPC transport reading a large client message or encoding a large reply, a forward
 * to the leader, a thread that answers a stream. That request fails, and halting would only carry the same request to
 * the next node, and halt that one too. Such an error is logged, with its thread, and the node goes on. Every thread is
 * classified by its name, which the event carries; this JDK's Netty copies both allocate through the JDK's direct
 * buffers (no Netty-own counter), so a client can still use up the memory that the next allocation of Ratis needs:
 * see "Heap" in the design document for sizing the limit.</p>
 */
final class DirectMemoryWatch implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(DirectMemoryWatch.class);
	private static final String EVENT = "jdk.JavaExceptionThrow";
	private static final String ERROR_EVENT = "jdk.JavaErrorThrow";

	private final RecordingStream stream;

	private DirectMemoryWatch(RecordingStream stream) {
		this.stream = stream;
	}

	/** Prefixes of the threads of Ratis' own executors and of its shaded gRPC transport. */
	private static final List<String> REPLICATION_PREFIXES = List.of("grpc-default-");
	/** Suffixes of the executors of a Ratis server ({@code <peer>-server-thread3}, ...), digits removed. */
	private static final List<String> REPLICATION_SUFFIXES = List.of("-server-thread", "-client-thread", "-impl-thread",
			"-groupManagement");

	/**
	 * Starts watching; {@code onExhausted} gets the class and message of the first such error on a thread of
	 * replication ({@link #fatalOn}), once, on a thread of JFR. Such errors on other threads are only logged.
	 *
	 * @return null if this JVM has no JFR (the node then runs without the watch, and says so)
	 */
	static @Nullable DirectMemoryWatch start(Consumer<String> onExhausted) {
		return start(onExhausted, survived -> LOG.error("Out of direct memory on a thread outside replication: {}. The"
				+ " request it served failed; the node goes on. A node that keeps logging this needs a higher"
				+ " -XX:MaxDirectMemorySize, or its clients smaller messages (see \"Heap\" in the design document)", survived));
	}

	/** {@link #start(Consumer)}, with what to do with an error that is not fatal (each one, on a thread of JFR). */
	static @Nullable DirectMemoryWatch start(Consumer<String> onExhausted, Consumer<String> onSurvived) {
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
			if (error == null) {
				return;
			}
			var thread = event.getThread();
			String threadName = thread != null ? thread.getJavaName() : null;
			var described = error + " (thread " + threadName + ")";
			if (!fatalOn(threadName)) {
				onSurvived.accept(described);
			} else if (fired.compareAndSet(false, true)) {
				onExhausted.accept(described);
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

	/**
	 * Whether a direct memory error made on the thread named {@code threadName} stops the node: a thread of Ratis,
	 * whose code catches the error and goes on without what it was doing. Recognized by name: Ratis' division threads
	 * ({@code <peer>@group-<id>-...}: appenders, log worker, state machine updater, leader and follower state), the
	 * executors of its server ({@code <peer>-server-thread<n>}, ...) and the threads of its shaded gRPC
	 * ({@code grpc-default-...}; the API's gRPC transport names its own threads {@code rockserver-grpc-...}). An event
	 * without a thread is fatal. Pinned on a running cluster by {@code DirectMemoryWatchTest}.
	 */
	static boolean fatalOn(@Nullable String threadName) {
		if (threadName == null || threadName.isEmpty()) {
			return true;
		}
		if (threadName.contains("@group-")) {
			return true;
		}
		for (var prefix : REPLICATION_PREFIXES) {
			if (threadName.startsWith(prefix)) {
				return true;
			}
		}
		var withoutDigits = threadName.replaceAll("\\d+$", "");
		for (var suffix : REPLICATION_SUFFIXES) {
			if (withoutDigits.endsWith(suffix)) {
				return true;
			}
		}
		return false;
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
