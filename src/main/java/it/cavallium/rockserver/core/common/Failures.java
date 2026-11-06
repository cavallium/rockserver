package it.cavallium.rockserver.core.common;

import java.util.regex.Pattern;
import org.jetbrains.annotations.Nullable;

/**
 * Tells failures of the JVM from failures of the code that ran in it, and describes a failure with text that does
 * not depend on the process: replication records failures as results, which must be the same on every replica.
 */
public final class Failures {

	private static final int MAX_DEPTH = 64;
	/** Native addresses, as in {@code MemorySegment{ address: 0x7f12... }} or hidden class names. */
	private static final Pattern ADDRESS = Pattern.compile("0x[0-9a-fA-F]+");
	/** Identity hashes, as in {@code Object.toString()} or the class loader names of a {@code ClassCastException}. */
	private static final Pattern IDENTITY_HASH = Pattern.compile("@[0-9a-fA-F]+\\b");
	/** The thread named by the JDK in some messages: replicas run their apply on threads of different names. */
	private static final Pattern THREAD = Pattern.compile("\\[in thread \"[^\"]*\"]");
	private static final String[] JVM_ERRORS = {"java.lang.OutOfMemoryError", "java.lang.StackOverflowError",
			"java.lang.InternalError", "java.lang.UnknownError"};

	private Failures() {}

	/**
	 * True if {@code failure} or any of its causes is a failure of the JVM rather than of the code: a
	 * {@link VirtualMachineError} (out of memory, stack overflow...), an {@link java.io.IOError}, or a class whose
	 * initialization was ended by one (the JDK keeps only the text of that first failure in the
	 * {@link ExceptionInInitializerError} or {@link NoClassDefFoundError} of later uses). It may not happen on another
	 * machine running the same code on the same data.
	 */
	public static boolean isJvmFailure(@Nullable Throwable failure) {
		int depth = 0;
		for (Throwable t = failure; t != null && depth < MAX_DEPTH; t = t.getCause(), depth++) {
			if (t instanceof VirtualMachineError || t instanceof java.io.IOError) {
				return true;
			}
			if ((t instanceof ExceptionInInitializerError || t instanceof NoClassDefFoundError) && namesJvmError(t)) {
				return true;
			}
			if (t.getCause() == t) {
				break;
			}
		}
		return false;
	}

	private static boolean namesJvmError(Throwable t) {
		var message = t.getMessage();
		if (message == null) {
			return false;
		}
		for (String name : JVM_ERRORS) {
			if (message.contains(name)) {
				return true;
			}
		}
		return false;
	}

	/** Rethrows {@code failure} as an unchecked throwable, unchanged if it is one. */
	public static RuntimeException rethrow(Throwable failure) {
		if (failure instanceof Error error) {
			throw error;
		}
		if (failure instanceof RuntimeException runtime) {
			throw runtime;
		}
		throw new IllegalStateException(failure);
	}

	/** The class name of {@code failure} and its message, {@link #sanitize}d. */
	public static String describe(Throwable failure) {
		var message = failure.getMessage();
		return message == null ? failure.getClass().getName()
				: failure.getClass().getName() + ": " + sanitize(message);
	}

	/** {@code text} without native addresses, identity hashes and thread names, which differ between processes. */
	public static String sanitize(String text) {
		var result = ADDRESS.matcher(text).replaceAll("0x?");
		result = IDENTITY_HASH.matcher(result).replaceAll("@?");
		return THREAD.matcher(result).replaceAll("[in thread ?]");
	}
}
