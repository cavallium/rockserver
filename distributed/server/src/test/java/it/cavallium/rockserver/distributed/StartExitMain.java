package it.cavallium.rockserver.distributed;

import java.time.Duration;

/**
 * The child JVM of {@link StartExitProcessTest}: {@link ReplicatedServerMain} with a start that the test controls, to
 * meet a real {@code System.exit} and a real SIGTERM while {@code run} is still starting its node (R61-13). The mode is
 * {@code -Drockserver.test.startExit}; the arguments are those of {@link ReplicatedServerMain}. Every mode prints
 * {@code STARTING} once the start runs, and shortens the hook's wait after another thread's exit to 2 s.
 * <ul>
 * <li>{@code slow}: the real start, 5 s late. The test sends SIGTERM during the wait.</li>
 * <li>{@code exit-after-signal}: after 3 s, during which the test sends SIGTERM, the start calls
 * {@code System.exit(9)}.</li>
 * <li>{@code exit-elsewhere}: another thread calls {@code System.exit(7)}, and the start waits for that thread.</li>
 * <li>{@code exit-elsewhere-after-signal}: the same, after 3 s during which the test sends SIGTERM.</li>
 * </ul>
 */
public final class StartExitMain {

	private StartExitMain() {}

	public static void main(String[] args) throws Exception {
		String mode = System.getProperty("rockserver.test.startExit");
		ReplicatedServerMain.startAfterExitTimeout = Duration.ofSeconds(2);
		ReplicatedServerMain.starter = config -> {
			System.out.println("STARTING");
			System.out.flush();
			try {
				switch (mode) {
					case "slow" -> Thread.sleep(5000);
					case "exit-after-signal" -> {
						Thread.sleep(3000);
						System.exit(9);
					}
					case "exit-elsewhere" -> {
						var exiting = Thread.ofPlatform().name("test-exit").start(() -> System.exit(7));
						exiting.join();
					}
					case "exit-elsewhere-after-signal" -> {
						Thread.sleep(3000);
						var exiting = Thread.ofPlatform().name("test-exit").start(() -> System.exit(7));
						exiting.join();
					}
					default -> throw new IllegalArgumentException(mode);
				}
			} catch (InterruptedException ex) {
				throw new IllegalStateException(ex);
			}
			return ReplicatedNode.start(config);
		};
		ReplicatedServerMain.main(args);
	}
}
