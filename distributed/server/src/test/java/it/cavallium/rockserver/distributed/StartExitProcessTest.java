package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R61-13: what the shutdown hook of {@code run} does about a start still running, with a real {@code System.exit} and
 * a real SIGTERM ({@link StartExitMain} is the node, one process each). These also pin what {@link JvmExit} reads in
 * the JDK's stack traces: a signal that is not recognised fails {@link #aSignalDuringAStartStillStopsTheNodeCleanly},
 * an exit that is not recognised fails the two others (and {@link PortInUseProcessTest}).
 */
class StartExitProcessTest {

	private static final long EXIT_SECONDS = 60;

	@TempDir
	Path tempDir;

	/**
	 * R28-4 with a real signal: the hook waits for the start, longer than the limit that an exit on another thread gets
	 * (2 s here, the start takes 5 s more), stops the node it returned, and the JVM exits as a signal's shutdown does.
	 */
	@Test
	void aSignalDuringAStartStillStopsTheNodeCleanly() throws Exception {
		var ports = ProcessCluster.freePorts(2, List.of());
		var process = start("slow", ports);
		awaitStarting(process);
		long signalled = System.nanoTime();
		process.destroy();
		var output = awaitExit(process);
		assertEquals(143, process.exitValue(), output);
		assertTrue(System.nanoTime() - signalled >= TimeUnit.SECONDS.toNanos(4), "it did not wait for the start: " + output);
		assertTrue(output.contains("a: started, Raft"), output);
		assertFalse(output.contains("READY"), output);
		assertFalse(output.contains("halting"), output);
		// The stop closed the database: its lock is free again.
		try (var channel = java.nio.channels.FileChannel.open(tempDir.resolve("node/db/LOCK"),
				java.nio.file.StandardOpenOption.WRITE); var lock = channel.tryLock()) {
			assertTrue(lock != null, "the database is still locked");
		}
	}

	/** The start calls {@code System.exit} after a signal started the shutdown: it waits behind the signal for good. */
	@Test
	void aStartThatExitsAfterASignalEndsWithOne() throws Exception {
		var ports = ProcessCluster.freePorts(2, List.of());
		var process = start("exit-after-signal", ports);
		awaitStarting(process);
		process.destroy();
		var output = awaitExit(process);
		assertEquals(1, process.exitValue(), output);
		assertTrue(output.contains("Node a does not start: its start called System.exit"), output);
		assertTrue(output.contains("StartExitMain"), "where the start stands: " + output);
	}

	/** Another thread calls {@code System.exit} and the start waits for that thread: the hook's wait has a limit. */
	@Test
	void anExitOnAnotherThreadThatTheStartWaitsForEndsWithOne() throws Exception {
		var ports = ProcessCluster.freePorts(2, List.of());
		var process = start("exit-elsewhere", ports);
		var output = awaitExit(process);
		assertEquals(1, process.exitValue(), output);
		assertTrue(output.contains("Node a does not start: a System.exit on another thread started the shutdown"), output);
	}

	/**
	 * R66-5: a signal started the shutdown, then another thread calls {@code System.exit} and the start waits for that
	 * thread. The exit waits behind the signal's shutdown for good, so the start cannot end: the hook's wait for a start
	 * after a signal, which has no limit otherwise, gets the limit of an exit on another thread.
	 */
	@Test
	void anExitOnAnotherThreadAfterASignalThatTheStartWaitsForEndsWithOne() throws Exception {
		var ports = ProcessCluster.freePorts(2, List.of());
		var process = start("exit-elsewhere-after-signal", ports);
		awaitStarting(process);
		process.destroy();
		var output = awaitExit(process);
		assertEquals(1, process.exitValue(), output);
		assertTrue(output.contains("Node a does not start: a System.exit on another thread waits behind the shutdown"
				+ " that a signal started"), output);
	}

	private Process start(String mode, List<Integer> ports) throws Exception {
		var command = PortInUseProcessTest.command(tempDir, StartExitMain.class, ports.get(0), ports.get(1),
				List.of("-Drockserver.test.startExit=" + mode));
		return new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(tempDir.resolve("run.log").toFile())
				.start();
	}

	private void awaitStarting(Process process) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(EXIT_SECONDS);
		while (!Files.readString(tempDir.resolve("run.log")).contains("STARTING")) {
			if (!process.isAlive() || System.nanoTime() > deadline) {
				process.destroyForcibly().waitFor();
				fail("the start did not begin: " + Files.readString(tempDir.resolve("run.log")));
			}
			Thread.sleep(20);
		}
	}

	private String awaitExit(Process process) throws Exception {
		try {
			if (!process.waitFor(EXIT_SECONDS, TimeUnit.SECONDS)) {
				fail("still running after " + EXIT_SECONDS + " s: " + Files.readString(tempDir.resolve("run.log"))
						+ "\nIts threads:\n" + PortInUseProcessTest.threads(process.pid()));
			}
		} finally {
			process.destroyForcibly().waitFor();
		}
		return Files.readString(tempDir.resolve("run.log"));
	}
}
