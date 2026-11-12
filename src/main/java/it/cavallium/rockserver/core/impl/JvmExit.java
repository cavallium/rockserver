package it.cavallium.rockserver.core.impl;

/**
 * What a shutdown hook can find out about the exit of the JVM it runs in: which thread asked for it (R61-13, R66-5).
 *
 * <p>{@link System#exit} runs the shutdown hooks on the thread that called it and waits for them, holding a lock that
 * every later {@code System.exit} waits for, for good. A thread inside {@code System.exit} therefore never returns,
 * and a hook that waits for something only that thread can do waits for good too: the JVM never exits. A SIGTERM runs
 * the hooks on a thread of the JVM's own instead, which nothing else waits for.</p>
 *
 * <p>The JDK does not tell a hook who called it, so this reads the stack traces: {@code java.lang.Shutdown} is where
 * both {@code System.exit} and the signal handler ({@code java.lang.Terminator}) run and wait for the hooks. Those are
 * classes of the JDK's implementation, pinned for the JDK in use by the process tests that run real exits and a real
 * signal: {@code MainStartProcessTest} for the single-node server, {@code StartExitProcessTest} and
 * {@code PortInUseProcessTest} for the replicated one. A virtual thread that called {@code System.exit} has no stack
 * trace here: {@link #bySignal} is then false, as for any other thread.</p>
 */
public final class JvmExit {

	private static final String SHUTDOWN = "java.lang.Shutdown";
	private static final String RUN_HOOKS = "runHooks";
	private static final String SIGNAL_HANDLER = "java.lang.Terminator";

	private JvmExit() {}

	/**
	 * Whether {@code thread} is inside {@code System.exit}: it runs the shutdown hooks and waits for them, or it waits
	 * for the thread that does. Either way it does not return.
	 */
	public static boolean exiting(Thread thread) {
		return has(thread.getStackTrace(), SHUTDOWN, null);
	}

	/** Whether a signal (SIGTERM, SIGINT, SIGHUP) started the shutdown under way, and not a {@code System.exit}. */
	public static boolean bySignal() {
		for (var stack : Thread.getAllStackTraces().values()) {
			// The thread that runs the hooks, not one that waits behind it: a signal after a System.exit waits there.
			if (has(stack, SHUTDOWN, RUN_HOOKS) && has(stack, SIGNAL_HANDLER, null)) {
				return true;
			}
		}
		return false;
	}

	/**
	 * Whether a thread is inside {@code System.exit} without being the one that runs the hooks: it called it after the
	 * shutdown under way began, and waits behind it for good.
	 */
	public static boolean exitWaiting() {
		for (var stack : Thread.getAllStackTraces().values()) {
			if (has(stack, SHUTDOWN, null) && !has(stack, SHUTDOWN, RUN_HOOKS)) {
				return true;
			}
		}
		return false;
	}

	private static boolean has(StackTraceElement[] stack, String type, String method) {
		for (var frame : stack) {
			String name = frame.getClassName();
			// Also a nested class, such as the signal handler's (Terminator$1).
			if ((name.equals(type) || name.startsWith(type + "$")) && (method == null || method.equals(frame.getMethodName()))) {
				return true;
			}
		}
		return false;
	}
}
