package it.cavallium.rockserver.core.impl;

import java.lang.foreign.FunctionDescriptor;
import java.lang.foreign.Linker;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandle;
import org.jetbrains.annotations.Nullable;

/**
 * Ends this process at once, with libc's {@code _exit}: no shutdown hooks, and none of the exit handlers and static
 * destructors that {@link Runtime#halt} still runs through libc's {@code exit}.
 *
 * <p>{@link Runtime#halt} does not end a node that is writing (R62-1). It stops the Java threads at a safepoint and
 * calls {@code exit}; the destructors of the RocksDB library then join its background threads; and a flush or
 * compaction thread that is inside a call into Java (a merge operator, the logger, an event listener) waits for that
 * safepoint to end, which it never does. The process stays, with its ports open and nothing behind them, and answers
 * neither SIGTERM nor a thread dump: only SIGKILL ends it. {@code _exit} leaves the threads where they are and lets the
 * kernel take the process, which is what a halt is for.</p>
 *
 * <p>The call is linked when this class loads ({@link #prepare}), so that a halt on a node that is out of memory has
 * nothing left to build. Where it cannot be linked (no {@code _exit}, or native access refused), {@link #halt} falls
 * back to {@link Runtime#halt}.</p>
 *
 * <p>Used by the replicated server (its {@code ProcessHalt}) and by the shutdown hook of the single-node
 * {@code Main}.</p>
 */
public final class ProcessHalt {

	private static final @Nullable MethodHandle EXIT = link();

	private ProcessHalt() {}

	private static @Nullable MethodHandle link() {
		try {
			var linker = Linker.nativeLinker();
			var symbol = linker.defaultLookup().find("_exit");
			if (symbol.isEmpty()) {
				return null;
			}
			return linker.downcallHandle(symbol.get(), FunctionDescriptor.ofVoid(ValueLayout.JAVA_INT));
		} catch (Throwable notLinked) {
			return null;
		}
	}

	/** Loads this class, and so links the call. Whether {@link #halt} will use {@code _exit}. */
	public static boolean prepare() {
		return EXIT != null;
	}

	/** Ends the process with exit status {@code code}; does not return. */
	public static void halt(int code) {
		var exit = EXIT;
		if (exit != null) {
			try {
				exit.invokeExact(code);
			} catch (Throwable notCalled) {
				// Falls through.
			}
		}
		Runtime.getRuntime().halt(code);
	}
}
