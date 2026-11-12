package it.cavallium.rockserver.distributed;

/**
 * What the shutdown hook of {@code run} asks about the exit of the JVM it runs in: which thread asked for it (R61-13).
 * The answers for this JVM are read from the stack traces by {@link it.cavallium.rockserver.core.impl.JvmExit}, which
 * the single-node server uses too and which says what is read and why; {@code StartExitProcessTest} and
 * {@code PortInUseProcessTest} pin them for the JDK in use.
 */
final class JvmExit {

	private JvmExit() {}

	/** The questions of a shutdown hook; a test replaces the answers ({@link ReplicatedServerMain#exitProbe}). */
	interface Probe {

		/**
		 * Whether {@code thread} is inside {@code System.exit}: it runs the shutdown hooks and waits for them, or it
		 * waits for the thread that does. Either way it does not return.
		 */
		boolean exiting(Thread thread);

		/** Whether a signal (SIGTERM, SIGINT, SIGHUP) started the shutdown under way, and not a {@code System.exit}. */
		boolean bySignal();

		/**
		 * Whether a thread is inside {@code System.exit} without being the one that runs the hooks: it called it after
		 * the shutdown under way began, and waits behind it for good. Asked only after a signal (R66-5).
		 */
		default boolean exitWaiting() {
			return false;
		}
	}

	/** The answers for this JVM. */
	static final Probe JVM = new Probe() {
		@Override
		public boolean exiting(Thread thread) {
			return it.cavallium.rockserver.core.impl.JvmExit.exiting(thread);
		}

		@Override
		public boolean bySignal() {
			return it.cavallium.rockserver.core.impl.JvmExit.bySignal();
		}

		@Override
		public boolean exitWaiting() {
			return it.cavallium.rockserver.core.impl.JvmExit.exitWaiting();
		}
	};
}
