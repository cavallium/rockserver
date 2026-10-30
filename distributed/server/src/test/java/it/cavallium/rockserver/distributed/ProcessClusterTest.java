package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/** {@link ProcessCluster} with {@link FakeNode} JVMs, which take the {@code run} options of the real server. */
@Timeout(120)
class ProcessClusterTest {

	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	/** A node whose data directory holds this file exits before printing {@code READY}. */
	private static final String EXIT_MARKER = "exit-before-ready";

	@TempDir
	Path root;

	@Test
	void waitReadyFailsWhenANodeCrashesAtStartup() throws Exception {
		var dataDir3 = root.resolve("node-3").resolve("data");
		Files.createDirectories(dataDir3);
		Files.createFile(dataDir3.resolve(EXIT_MARKER));
		try (var cluster = startFakeCluster(root, 3)) {
			// Exited before the wait starts, so it cannot be mistaken for a node that is merely not ready yet.
			awaitExit(cluster, "3");
			var error = assertThrows(IllegalStateException.class, () -> cluster.waitReady(READY_TIMEOUT));
			assertTrue(error.getMessage().startsWith("Node 3 exited with code 3 before printing READY"),
					error.getMessage());
			assertTrue(error.getMessage().contains("fake node 3: exiting as asked"), "log tail: " + error.getMessage());
		}
	}

	@Test
	void waitReadyAwaitsRestartedNodesAndSkipsStoppedOnes() throws Exception {
		try (var cluster = startFakeCluster(root, 3)) {
			cluster.waitReady(READY_TIMEOUT);

			cluster.kill("2");
			cluster.stop("3");
			assertFalse(cluster.isRunning("2"));
			assertFalse(cluster.isRunning("3"));
			cluster.waitReady(READY_TIMEOUT);

			// A restart that crashes fails the cluster-wide wait.
			Files.createFile(cluster.dataDir("2").resolve(EXIT_MARKER));
			cluster.start("2");
			awaitExit(cluster, "2");
			var error = assertThrows(IllegalStateException.class, () -> cluster.waitReady(READY_TIMEOUT));
			assertTrue(error.getMessage().startsWith("Node 2 exited with code 3 before printing READY"),
					error.getMessage());

			// The same ports bind again on a good restart.
			Files.delete(cluster.dataDir("2").resolve(EXIT_MARKER));
			cluster.start("2");
			cluster.start("3");
			cluster.waitReady(READY_TIMEOUT);
			assertTrue(cluster.isRunning("2"));
			assertTrue(cluster.isRunning("3"));
		}
	}

	/** Ports in the ephemeral range could be taken by an outgoing connection while their node is down. */
	@Test
	void nodePortsAreBelowTheEphemeralRange() throws IOException {
		var range = Path.of("/proc/sys/net/ipv4/ip_local_port_range");
		assumeTrue(Files.isReadable(range), "no ephemeral port range to compare with");
		int ephemeralStart = Integer.parseInt(Files.readAllLines(range).getFirst().strip().split("\\s+")[0]);
		try (var cluster = startFakeCluster(root, 3)) {
			cluster.waitReady(READY_TIMEOUT);
			for (var id : cluster.nodeIds()) {
				int port = cluster.apiAddress(id).port();
				assertTrue(port < ephemeralStart, "node " + id + " API port " + port + " >= " + ephemeralStart);
			}
		}
	}

	@Test
	void nodesDieWhenTheTestJvmExitsWithoutClosing() throws Exception {
		assertNodesDieWithOwner(false);
	}

	@Test
	void nodesDieWhenTheTestJvmIsTerminated() throws Exception {
		assertNodesDieWithOwner(true);
	}

	/** Runs an {@link Owner} JVM that leaks its cluster, ends it, and checks that its node JVMs end too. */
	private void assertNodesDieWithOwner(boolean terminate) throws Exception {
		var owner = new ProcessBuilder(javaCommand(Owner.class, root.resolve("owned").toString(),
				terminate ? "wait" : "exit"))
				.redirectError(root.resolve("owner-stderr.log").toFile())
				.start();
		var pids = new ArrayList<Long>();
		try {
			var reading = CompletableFuture.supplyAsync(() -> readNodePids(owner));
			pids.addAll(reading.get(READY_TIMEOUT.toSeconds(), TimeUnit.SECONDS));
			assertEquals(3, pids.size(), "node pids from the owner JVM");
			for (long pid : pids) {
				assertTrue(ProcessHandle.of(pid).map(ProcessHandle::isAlive).orElse(false), "node " + pid + " alive");
			}
			if (terminate) {
				owner.destroy();
			}
			assertTrue(owner.waitFor(30, TimeUnit.SECONDS), "owner JVM did not exit");
			for (long pid : pids) {
				var node = ProcessHandle.of(pid);
				if (node.isPresent()) {
					node.get().onExit().get(30, TimeUnit.SECONDS);
				}
			}
		} finally {
			owner.destroyForcibly();
			// Do not leak the nodes if the assertion failed.
			pids.forEach(pid -> ProcessHandle.of(pid).ifPresent(ProcessHandle::destroyForcibly));
		}
	}

	/** Waits until the node's process is gone, without killing it. */
	private static void awaitExit(ProcessCluster cluster, String id) throws Exception {
		var process = ProcessHandle.of(cluster.pid(id));
		if (process.isPresent()) {
			process.get().onExit().get(READY_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		}
		assertFalse(cluster.isRunning(id));
	}

	private static List<Long> readNodePids(Process owner) {
		var pids = new ArrayList<Long>();
		try (var reader = new BufferedReader(new InputStreamReader(owner.getInputStream(), StandardCharsets.UTF_8))) {
			for (String line; (line = reader.readLine()) != null; ) {
				if (line.equals("OWNER READY")) {
					return pids;
				}
				if (line.startsWith("NODE ")) {
					pids.add(Long.parseLong(line.substring("NODE ".length())));
				}
			}
			throw new IllegalStateException("Owner JVM ended before starting its cluster");
		} catch (IOException e) {
			throw new IllegalStateException(e);
		}
	}

	private static ProcessCluster startFakeCluster(Path root, int size) throws IOException {
		return ProcessCluster.start(root, size, List.of(), FakeNode.class.getName());
	}

	private static List<String> javaCommand(Class<?> mainClass, String... args) {
		var command = new ArrayList<String>();
		command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
		command.add("-cp");
		command.add(System.getProperty("java.class.path"));
		command.add(mainClass.getName());
		command.addAll(List.of(args));
		return command;
	}

	/** Test JVM stand-in: starts a cluster, prints its node pids and never closes it. */
	public static final class Owner {
		public static void main(String[] args) throws Exception {
			var cluster = startFakeCluster(Path.of(args[0]), 3);
			cluster.waitReady(READY_TIMEOUT);
			for (var id : cluster.nodeIds()) {
				System.out.println("NODE " + cluster.pid(id));
			}
			System.out.println("OWNER READY");
			System.out.flush();
			if (args[1].equals("wait")) {
				new CountDownLatch(1).await();
			}
		}
	}

	/**
	 * Stand-in for {@code ReplicatedServerMain run}: binds its Raft and API ports like the real server, prints
	 * {@code READY <id>} and runs until killed. Exits with code 3 if its data directory holds {@value #EXIT_MARKER}.
	 */
	public static final class FakeNode {
		public static void main(String[] args) throws Exception {
			String nodeId = null;
			String dataDir = null;
			String raftBindHost = null;
			String apiBindHost = null;
			var peers = new ArrayList<PeerConfig>();
			for (int i = 1; i < args.length; i += 2) {
				if (args[i].equals("--pipelined-updates")) {
					// The one option without a value.
					i--;
					continue;
				}
				switch (args[i]) {
					case "--node-id" -> nodeId = args[i + 1];
					case "--data-dir" -> dataDir = args[i + 1];
					case "--raft-bind-host" -> raftBindHost = args[i + 1];
					case "--api-bind-host" -> apiBindHost = args[i + 1];
					case "--peer" -> peers.add(PeerConfig.parse(args[i + 1]));
					default -> throw new IllegalArgumentException("Unexpected option " + args[i]);
				}
			}
			var id = nodeId;
			var self = peers.stream().filter(peer -> peer.id().equals(id)).findFirst().orElseThrow();
			Files.createDirectories(Path.of(dataDir));
			if (Files.exists(Path.of(dataDir, EXIT_MARKER))) {
				System.err.println("fake node " + id + ": exiting as asked");
				System.exit(3);
			}
			try (var raft = listen(raftBindHost, self.raftPort());
					var api = listen(apiBindHost, self.apiPort())) {
				System.out.println("READY " + id);
				System.out.flush();
				new CountDownLatch(1).await();
			}
		}

		private static ServerSocket listen(String host, int port) throws IOException {
			var socket = new ServerSocket();
			socket.setReuseAddress(true);
			socket.bind(new InetSocketAddress(host, port));
			return socket;
		}
	}
}
