package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R61-13: {@code run} with a port that another socket holds. One real node process each, since the Raft case ends in a
 * {@link System#exit} that Ratis calls on the main thread inside the start: with a taken Raft port the process must
 * exit with 1 and print the cause, not wait for itself in its shutdown hook; with a taken API port the start fails
 * with an exception, which must give the same exit status.
 */
class PortInUseProcessTest {

	/** Far more than a JVM, an empty database and a Raft server that fails to bind take (a few seconds). */
	private static final long EXIT_SECONDS = 60;

	@TempDir
	Path tempDir;

	@Test
	void aTakenRaftPortEndsRunWithOneAndNamesTheCause() throws Exception {
		var ports = ProcessCluster.freePorts(2, List.of());
		try (var taken = hold(ports.get(0))) {
			var output = runExpectingExit(ports.get(0), ports.get(1));
			assertTrue(output.contains("Failed to start Grpc server"), output);
			assertTrue(output.contains("Address already in use"), output);
			assertTrue(output.contains("Failed to bind to address /127.0.0.1:" + ports.get(0)), output);
			assertTrue(output.contains("Node a does not start: its start called System.exit"), output);
			assertFalse(output.contains("READY"), output);
		}
	}

	@Test
	void aTakenApiPortEndsRunWithOneAndNamesTheCause() throws Exception {
		var ports = ProcessCluster.freePorts(2, List.of());
		try (var taken = hold(ports.get(1))) {
			var output = runExpectingExit(ports.get(0), ports.get(1));
			assertTrue(output.contains("Failed to start node a"), output);
			assertTrue(output.contains("Address already in use"), output);
			assertTrue(output.contains("Failed to bind to address /127.0.0.1:" + ports.get(1)), output);
			assertFalse(output.contains("halting"), output);
			assertFalse(output.contains("READY"), output);
		}
	}

	/**
	 * Review of R65: a Raft bind host that is no address of the machine (192.0.2.1 is reserved for documentation) takes
	 * the same exit of Ratis as a taken port.
	 */
	@Test
	void aRaftBindHostTheMachineLacksEndsRunWithOne() throws Exception {
		var ports = ProcessCluster.freePorts(2, List.of());
		var command = command(tempDir, ReplicatedServerMain.class, ports.get(0), ports.get(1), List.of());
		command.set(command.indexOf("--raft-bind-host") + 1, "192.0.2.1");
		var output = runExpectingExit(command);
		assertTrue(output.contains("Failed to start Grpc server"), output);
		assertTrue(output.contains("Failed to bind to address /192.0.2.1:" + ports.get(0)), output);
		assertTrue(output.contains("Cannot assign requested address"), output);
		assertTrue(output.contains("Node a does not start: its start called System.exit"), output);
		assertFalse(output.contains("READY"), output);
	}

	/** The thread dump of a node that did not exit, for the failure message: where its exit waits. */
	static String threads(long pid) throws Exception {
		var jstack = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "jstack").toString(),
				String.valueOf(pid)).redirectErrorStream(true).start();
		var dump = new String(jstack.getInputStream().readAllBytes());
		jstack.waitFor(30, TimeUnit.SECONDS);
		return dump;
	}

	private static ServerSocket hold(int port) throws Exception {
		var socket = new ServerSocket();
		// Like the node's own listening sockets: SO_REUSEADDR does not let a second socket listen on the port.
		socket.setReuseAddress(true);
		socket.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), port), 1);
		return socket;
	}

	/** The command of a single node {@code a} under {@code root}, with these ports, started through {@code main}. */
	static List<String> command(Path root, Class<?> main, int raftPort, int apiPort, List<String> jvmOptions)
			throws Exception {
		var command = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
				"-cp", System.getProperty("java.class.path"),
				"--enable-native-access=ALL-UNNAMED", "--add-modules=jdk.incubator.vector",
				"-XX:+ExitOnOutOfMemoryError", "-Djava.io.tmpdir=" + Files.createDirectories(root.resolve("tmp"))));
		command.addAll(jvmOptions);
		command.addAll(List.of(main.getName(), "run", "--node-id", "a",
				"--peer", "a=127.0.0.1:" + raftPort + "/127.0.0.1:" + apiPort,
				"--data-dir", root.resolve("node").toString(),
				"--raft-bind-host", "127.0.0.1", "--api-bind-host", "127.0.0.1"));
		return command;
	}

	/** Runs a single node with these ports; it must exit with 1 within {@link #EXIT_SECONDS}. Returns its output. */
	private String runExpectingExit(int raftPort, int apiPort) throws Exception {
		return runExpectingExit(command(tempDir, ReplicatedServerMain.class, raftPort, apiPort, List.of()));
	}

	private String runExpectingExit(List<String> command) throws Exception {
		var log = tempDir.resolve("run.log");
		var process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
		try {
			if (!process.waitFor(EXIT_SECONDS, TimeUnit.SECONDS)) {
				fail("still running after " + EXIT_SECONDS + " s: " + Files.readString(log) + "\nIts threads:\n"
						+ threads(process.pid()));
			}
		} finally {
			process.destroyForcibly().waitFor();
		}
		var output = Files.readString(log);
		assertEquals(1, process.exitValue(), output);
		return output;
	}
}
