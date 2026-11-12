package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RequestContext;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * R66-5: how the single-node server's {@code Main} ends when its start does not reach "ready", with a real process
 * each: a listening port that is taken, a start that throws, a SIGTERM during the start, and a start that calls
 * {@code System.exit} (alone, and after a SIGTERM). The shutdown hook of {@code Main} waits for the thread that runs
 * it to close everything; a thread inside {@code System.exit} waits for the hook, so the two waited for each other and
 * the process never ended, deaf to SIGTERM. {@link MainShutdownTest} is the SIGTERM after the start.
 *
 * <p>The child has no SLF4J provider, so what {@code Main} logs is not in its output: the tests read the exit status,
 * the marker that {@code Main} writes once it closed everything, and the database.</p>
 *
 * <p>The last two also pin what the hook reads in the JDK's stack traces ({@code java.lang.Shutdown}): on a JDK where
 * the exit is not recognised they fail with the hang.</p>
 */
@Timeout(180)
public class MainStartProcessTest {

	private static final long EXIT_SECONDS = 60;

	@TempDir
	Path tempDir;

	private Path databasePath;
	private Path configPath;
	private Path childLog;
	private Path shutdownComplete;

	@BeforeEach
	void setUp() throws Exception {
		Assumptions.assumeTrue(System.getProperty("os.name").toLowerCase(Locale.ROOT).contains("linux"),
				"Process.destroy() is used as the production SIGTERM signal on Linux");
		databasePath = tempDir.resolve("database");
		configPath = writeConfig();
		childLog = tempDir.resolve("child.log");
		shutdownComplete = tempDir.resolve("shutdown-complete");
	}

	/**
	 * The gRPC port is held by another socket: the start fails after the database opened and the Thrift server
	 * started. The process ends by itself, having closed everything (it logs the cause, which this child does not show),
	 * with status 1: a supervisor that restarts on failure must see a failure. It ended with 0 before, the status of a
	 * {@code main} that returned.
	 */
	@Test
	void aTakenListeningPortEndsTheProcessWithTheDatabaseClosed() throws Exception {
		int thriftPort = reserveLoopbackPort();
		try (var held = new ServerSocket()) {
			held.setReuseAddress(true);
			held.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0), 1);
			int grpcPort = held.getLocalPort();
			var child = start("none", thriftPort, grpcPort);
			String log = awaitExit(child);
			assertFalse(log.contains("halting"), log);
			assertEquals("complete", Files.readString(shutdownComplete), log);
			assertEquals(1, child.exitValue(), log);
		}
		assertDatabaseOpens();
	}

	/** The start throws once the database is open: the same end as a taken port, status 1 with everything closed. */
	@Test
	void aStartThatThrowsEndsTheProcessWithTheDatabaseClosed() throws Exception {
		var child = start("fail", reserveLoopbackPort(), reserveLoopbackPort());
		String log = awaitExit(child);
		assertFalse(log.contains("halting"), log);
		assertEquals("complete", Files.readString(shutdownComplete), log);
		assertEquals(1, child.exitValue(), log);
		assertDatabaseOpens();
	}

	/**
	 * A SIGTERM while the start still runs (5 s more): the hook waits for the start, however long it takes, the servers
	 * are not started, and everything is closed before the JVM exits as a signal's shutdown does.
	 */
	@Test
	void aSignalDuringAStartStillClosesTheDatabase() throws Exception {
		var child = start("slow", reserveLoopbackPort(), reserveLoopbackPort());
		awaitStarting(child);
		long signalled = System.nanoTime();
		child.destroy();
		String log = awaitExit(child);
		assertEquals(128 + 15, child.exitValue(), log);
		assertTrue(System.nanoTime() - signalled >= TimeUnit.SECONDS.toNanos(4), "it did not wait for the start: " + log);
		assertFalse(log.contains("halting"), log);
		assertEquals("complete", Files.readString(shutdownComplete), log);
		assertDatabaseOpens();
	}

	/**
	 * The start calls {@code System.exit}: that thread runs the shutdown hook and waits for it, so it never closes what
	 * the hook waits for. The hook sees it and ends the process, with 1.
	 */
	@Test
	void aStartThatCallsSystemExitEndsWithOne() throws Exception {
		var child = start("exit", reserveLoopbackPort(), reserveLoopbackPort());
		String log = awaitExit(child);
		assertEquals(1, child.exitValue(), log);
		assertTrue(log.contains("called System.exit"), log);
		assertTrue(log.contains("halting with 1"), log);
		assertTrue(log.contains("MainStartProcessTest$StartProcess"), "where that thread stands: " + log);
		assertFalse(Files.exists(shutdownComplete), log);
		// What a crash during a start leaves: the next start recovers from it.
		assertDatabaseOpens();
	}

	/** The start calls {@code System.exit} after a signal started the shutdown: it waits behind the signal for good. */
	@Test
	void aStartThatExitsAfterASignalEndsWithOne() throws Exception {
		var child = start("exit-after-signal", reserveLoopbackPort(), reserveLoopbackPort());
		awaitStarting(child);
		child.destroy();
		String log = awaitExit(child);
		assertEquals(1, child.exitValue(), log);
		assertTrue(log.contains("called System.exit"), log);
		assertTrue(log.contains("halting with 1"), log);
		assertDatabaseOpens();
	}

	/**
	 * The caller of {@code run} exits once {@code run} returned (here after a start that threw): that thread is inside
	 * {@code System.exit} too, but it closed everything first. Its status stands.
	 */
	@Test
	void anExitOnceRunReturnedKeepsItsStatus() throws Exception {
		var child = start("fail-then-exit", reserveLoopbackPort(), reserveLoopbackPort());
		String log = awaitExit(child);
		assertEquals(7, child.exitValue(), log);
		assertFalse(log.contains("halting"), log);
		assertEquals("complete", Files.readString(shutdownComplete), log);
		assertDatabaseOpens();
	}

	private Process start(String mode, int thriftPort, int grpcPort) throws Exception {
		String classPath = System.getProperty("surefire.test.class.path", System.getProperty("java.class.path"));
		var command = new ArrayList<>(List.of(
				Path.of(System.getProperty("java.home"), "bin", "java").toString(),
				"--enable-native-access=ALL-UNNAMED",
				"-Drockserver.core.print-config=false",
				"-Drockserver.test.mainStart=" + mode,
				"-XX:ErrorFile=" + tempDir.resolve("hs_err_pid%p.log"),
				"-cp",
				classPath,
				StartProcess.class.getName(),
				shutdownComplete.toString(),
				"--database-url",
				databasePath.toUri().toString(),
				"--thrift-listen-url",
				"http://127.0.0.1:" + thriftPort,
				"--grpc-listen-url",
				"http://127.0.0.1:" + grpcPort,
				"--name",
				"main-start-test",
				"--config",
				configPath.toString()));
		return new ProcessBuilder(command)
				.directory(tempDir.toFile())
				.redirectErrorStream(true)
				.redirectOutput(childLog.toFile())
				.start();
	}

	private void awaitStarting(Process child) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(EXIT_SECONDS);
		while (!readLog().contains("STARTING")) {
			if (!child.isAlive() || System.nanoTime() > deadline) {
				child.destroyForcibly().waitFor();
				fail("the start did not begin: " + readLog());
			}
			Thread.sleep(20);
		}
	}

	private String awaitExit(Process child) throws Exception {
		try {
			if (!child.waitFor(EXIT_SECONDS, TimeUnit.SECONDS)) {
				fail("still running after " + EXIT_SECONDS + " s: " + readLog() + "\nIts threads:\n" + threads(child.pid()));
			}
		} finally {
			child.destroyForcibly().waitFor();
		}
		return readLog();
	}

	private static String threads(long pid) throws Exception {
		var jstack = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "jstack").toString(),
				String.valueOf(pid)).redirectErrorStream(true).start();
		var dump = new String(jstack.getInputStream().readAllBytes());
		jstack.waitFor(30, TimeUnit.SECONDS);
		return dump;
	}

	/** The database is not held by the process that ended, and opens: a column can be made and read back. */
	private void assertDatabaseOpens() throws Exception {
		try (var reopened = new EmbeddedConnection(databasePath, "main-start-reopen", configPath)) {
			var api = reopened.getSyncApi(RequestContext.batch());
			long columnId = api.createColumn("start-column", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			assertEquals(columnId, api.getColumnId("start-column"));
		}
	}

	private String readLog() throws Exception {
		return Files.exists(childLog) ? Files.readString(childLog) : "";
	}

	private Path writeConfig() throws Exception {
		Path path = tempDir.resolve("rockserver.conf");
		Files.writeString(path, """
				database: {
				  metrics: {
				    database-name: "main-start-test"
				    jmx: { enabled: false }
				  }
				  parallelism: {
				    read: 3
				    write: 3
				    workload: { competing-batch-read-maximum-active: 3 }
				  }
				  global: {
				    enable-fast-get: false
				    ingest-behind: false
				    optimistic: false
				    use-direct-io: false
				    maximum-open-files: 128
				    block-cache: "16MiB"
				    write-buffer-manager: "8MiB"
				    log-path: "%s"
				    wal-path: "%s"
				    temp-sst-path: "%s"
				    fallback-column-options: {
				      write-buffer-size: "4MiB"
				      memtable-memory-budget-bytes: "8MiB"
				      volumes: [{ volume-path: "%s", target-size: "1TiB" }]
				    }
				  }
				}
				""".formatted(
				escapeHocon(tempDir.resolve("logs")),
				escapeHocon(tempDir.resolve("wal")),
				escapeHocon(tempDir.resolve("temp-sst")),
				escapeHocon(tempDir.resolve("volume"))));
		return path;
	}

	private static int reserveLoopbackPort() throws Exception {
		try (var socket = new ServerSocket()) {
			socket.bind(new InetSocketAddress("127.0.0.1", 0));
			return socket.getLocalPort();
		}
	}

	private static String escapeHocon(Path path) {
		return path.toAbsolutePath().toString().replace("\\", "\\\\").replace("\"", "\\\"");
	}

	/**
	 * The child JVM: {@code Main.runAndExit}, which is {@code Main.main} with callbacks, with a start that the test
	 * controls through {@code -Drockserver.test.mainStart}, run once the database is open and before the servers are
	 * built. Every mode but {@code none} prints {@code STARTING} first.
	 * <ul>
	 * <li>{@code none}: the start as it is.</li>
	 * <li>{@code slow}: 5 s late. The test sends SIGTERM during the wait.</li>
	 * <li>{@code fail}: throws.</li>
	 * <li>{@code fail-then-exit}: throws, and the child, which calls {@code Main.run} here as an embedding caller
	 * would, calls {@code System.exit(7)} once {@code run} returned.</li>
	 * <li>{@code exit}: calls {@code System.exit(9)}.</li>
	 * <li>{@code exit-after-signal}: after 3 s, during which the test sends SIGTERM, calls {@code System.exit(9)}.</li>
	 * </ul>
	 */
	public static final class StartProcess {

		private StartProcess() {
		}

		public static void main(String[] args) throws Exception {
			String mode = System.getProperty("rockserver.test.mainStart");
			Path completionMarker = Path.of(args[0]);
			String[] mainArgs = Arrays.copyOfRange(args, 1, args.length);
			Runnable onShutdownComplete = () -> {
				try {
					Files.writeString(completionMarker, "complete", StandardOpenOption.CREATE_NEW);
				} catch (java.io.IOException ex) {
					throw new java.io.UncheckedIOException(ex);
				}
			};
			Runnable onConnected = () -> {
				if (mode.equals("none")) {
					return;
				}
				System.out.println("STARTING");
				System.out.flush();
				try {
					switch (mode) {
						case "slow" -> Thread.sleep(5000);
						case "fail", "fail-then-exit" -> throw new IllegalStateException("test start failure");
						case "exit" -> System.exit(9);
						case "exit-after-signal" -> {
							Thread.sleep(3000);
							System.exit(9);
						}
						default -> throw new IllegalArgumentException(mode);
					}
				} catch (InterruptedException ex) {
					throw new IllegalStateException(ex);
				}
			};
			var run = Class.forName("it.cavallium.rockserver.core.Main")
					.getDeclaredMethod(mode.equals("fail-then-exit") ? "run" : "runAndExit",
							String[].class, Runnable.class, Runnable.class);
			run.setAccessible(true);
			try {
				run.invoke(null, mainArgs, onShutdownComplete, onConnected);
				if (mode.equals("fail-then-exit")) {
					System.exit(7);
				}
			} catch (java.lang.reflect.InvocationTargetException ex) {
				if (ex.getCause() instanceof Exception cause) {
					throw cause;
				}
				if (ex.getCause() instanceof Error cause) {
					throw cause;
				}
				throw ex;
			}
		}
	}
}
