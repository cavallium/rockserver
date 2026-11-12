package it.cavallium.rockserver.distributed;

/**
 * Ends this process at once, with libc's {@code _exit}: no shutdown hooks, and none of the exit handlers and static
 * destructors that {@link Runtime#halt} still runs through libc's {@code exit} (R62-1). The call itself is
 * {@link it.cavallium.rockserver.core.impl.ProcessHalt}, which the single-node server uses too; that class says why
 * {@link Runtime#halt} is not enough and what happens where {@code _exit} cannot be linked.
 */
final class ProcessHalt {

	private ProcessHalt() {}

	/** Loads the class that holds the call, and so links it. Whether {@link #halt} will use {@code _exit}. */
	static boolean prepare() {
		return it.cavallium.rockserver.core.impl.ProcessHalt.prepare();
	}

	/** Ends the process with exit status {@code code}; does not return. */
	static void halt(int code) {
		it.cavallium.rockserver.core.impl.ProcessHalt.halt(code);
	}
}
