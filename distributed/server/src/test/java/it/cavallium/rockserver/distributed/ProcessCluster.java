package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A cluster of {@link ReplicatedServerMain} JVMs on this machine, connected through a {@link Network}.
 *
 * <p>Node ids are {@code "1"} to {@code "N"}. Every node binds its Raft and API ports on {@code 127.0.0.1} and
 * names its peers {@code node-<id>} for both ports, so all node-to-node traffic goes through the network's link
 * proxies. The test JVM reaches each node's API directly through {@link #apiAddress}, bypassing the proxies.</p>
 *
 * <p>Everything lives under the root directory given at start: {@code <root>/node-<id>/data} (the node's data
 * directory, kept across restarts), {@code <root>/node-<id>/stdout.log} and {@code stderr.log} (appended to by
 * every start), and {@code <root>/network} (hosts files).</p>
 */
public final class ProcessCluster implements AutoCloseable {

	private static final String MAIN_CLASS = "it.cavallium.rockserver.distributed.ReplicatedServerMain";
	private static final String LOOPBACK = "127.0.0.1";
	private static final Duration GRACEFUL_STOP_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration READY_POLL_INTERVAL = Duration.ofMillis(50);
	private static final int FAILURE_LOG_LINES = 60;

	private final List<String> serverArgs;
	private final Network network;
	private final Map<String, Node> nodes;

	private static final class Node {
		final String id;
		final int raftPort;
		final int apiPort;
		final Path dir;
		Process process;
		/** Size of the stdout log when the current process started: its own output follows. */
		long stdoutStart;
		boolean paused;
		int starts;

		Node(String id, int raftPort, int apiPort, Path dir) {
			this.id = id;
			this.raftPort = raftPort;
			this.apiPort = apiPort;
			this.dir = dir;
		}

		Path dataDir() {
			return dir.resolve("data");
		}

		Path stdout() {
			return dir.resolve("stdout.log");
		}

		Path stderr() {
			return dir.resolve("stderr.log");
		}

		boolean running() {
			return process != null && process.isAlive();
		}
	}

	private ProcessCluster(List<String> serverArgs, Network network, Map<String, Node> nodes) {
		this.serverArgs = serverArgs;
		this.network = network;
		this.nodes = nodes;
	}

	/**
	 * Starts {@code size} nodes without waiting for them; call {@link #waitReady(Duration)} next.
	 *
	 * @param serverArgs extra {@code run} options passed to every node, e.g. {@code --snapshot-every 100}
	 */
	public static ProcessCluster start(Path root, int size, List<String> serverArgs) throws IOException {
		var nodes = new LinkedHashMap<String, Node>();
		var ports = new LinkedHashMap<String, List<Integer>>();
		var freePorts = freePorts(size * 2);
		for (int i = 0; i < size; i++) {
			var id = String.valueOf(i + 1);
			var node = new Node(id, freePorts.get(2 * i), freePorts.get(2 * i + 1), root.resolve(Network.hostname(id)));
			Files.createDirectories(node.dir);
			nodes.put(id, node);
			ports.put(id, List.of(node.raftPort, node.apiPort));
		}
		var network = Network.start(ports, root.resolve("network"));
		var cluster = new ProcessCluster(List.copyOf(serverArgs), network, nodes);
		try {
			for (var id : nodes.keySet()) {
				cluster.start(id);
			}
		} catch (IOException | RuntimeException e) {
			cluster.close();
			throw e;
		}
		return cluster;
	}

	public List<String> nodeIds() {
		return List.copyOf(nodes.keySet());
	}

	public Network network() {
		return network;
	}

	/** Address of the node's API, reachable directly from the test JVM. */
	public HostAndPort apiAddress(String id) {
		return new HostAndPort(LOOPBACK, node(id).apiPort);
	}

	public Map<String, HostAndPort> apiAddresses() {
		var result = new LinkedHashMap<String, HostAndPort>();
		nodes.keySet().forEach(id -> result.put(id, apiAddress(id)));
		return result;
	}

	public Path dataDir(String id) {
		return node(id).dataDir();
	}

	public synchronized boolean isRunning(String id) {
		return node(id).running();
	}

	/** Waits until every running node printed its {@code READY} line. */
	public void waitReady(Duration timeout) {
		long deadline = System.nanoTime() + timeout.toNanos();
		for (var id : nodes.keySet()) {
			if (isRunning(id)) {
				waitReady(id, Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
			}
		}
	}

	/** Waits until the node printed {@code READY <id>} since its last start; fails with its log tail otherwise. */
	public void waitReady(String id, Duration timeout) {
		var node = node(id);
		long deadline = System.nanoTime() + timeout.toNanos();
		while (true) {
			Process process;
			long stdoutStart;
			synchronized (this) {
				process = node.process;
				stdoutStart = node.stdoutStart;
			}
			if (process == null) {
				throw new IllegalStateException("Node " + id + " was never started");
			}
			// Check liveness before reading the log, so that a READY printed right before exiting still counts.
			boolean alive = process.isAlive();
			if (printedReady(node, stdoutStart)) {
				return;
			}
			if (!alive) {
				throw new IllegalStateException("Node " + id + " exited with code " + process.exitValue()
						+ " before printing READY\n" + logTail(id));
			}
			if (System.nanoTime() - deadline >= 0) {
				throw new IllegalStateException("Node " + id + " not ready after " + timeout + "\n" + logTail(id));
			}
			try {
				Thread.sleep(READY_POLL_INTERVAL);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException("Interrupted while waiting for node " + id, e);
			}
		}
	}

	/** Starts (or restarts, with the same directories and ports) a node that is not running. */
	public synchronized void start(String id) throws IOException {
		var node = node(id);
		if (node.running()) {
			throw new IllegalStateException("Node " + id + " is already running");
		}
		node.starts++;
		var marker = "---- harness: start #" + node.starts + " of node " + id + " ----\n";
		Files.writeString(node.stdout(), marker, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		Files.writeString(node.stderr(), marker, StandardOpenOption.CREATE, StandardOpenOption.APPEND);
		node.stdoutStart = Files.size(node.stdout());
		// The child writes straight to the files: reading its stdout through a pipe can lose the last lines, as
		// the JDK closes the pipe when the process exits.
		node.process = new ProcessBuilder(command(node))
				.redirectOutput(ProcessBuilder.Redirect.appendTo(node.stdout().toFile()))
				.redirectError(ProcessBuilder.Redirect.appendTo(node.stderr().toFile()))
				.start();
		node.paused = false;
	}

	/** SIGKILL: no shutdown hook, no leadership hand-off. */
	public synchronized void kill(String id) {
		var node = node(id);
		if (node.process == null) {
			return;
		}
		node.process.destroyForcibly();
		awaitExit(node);
	}

	/** SIGTERM, then waits for the node to stop gracefully; kills it and fails if it does not. */
	public synchronized void stop(String id) {
		var node = node(id);
		if (!node.running()) {
			return;
		}
		if (node.paused) {
			// A stopped process only acts on SIGTERM once continued.
			resume(id);
		}
		node.process.destroy();
		try {
			if (!node.process.waitFor(GRACEFUL_STOP_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS)) {
				node.process.destroyForcibly();
				awaitExit(node);
				throw new IllegalStateException("Node " + id + " did not stop within " + GRACEFUL_STOP_TIMEOUT
						+ " of SIGTERM\n" + logTail(id));
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while stopping node " + id, e);
		}
		awaitExit(node);
	}

	/** SIGSTOP: the process freezes with its sockets open, like a long GC pause or a hung host. */
	public synchronized void pause(String id) {
		var node = runningNode(id);
		signal(node, "-STOP");
		node.paused = true;
	}

	/** SIGCONT after {@link #pause}. */
	public synchronized void resume(String id) {
		var node = runningNode(id);
		signal(node, "-CONT");
		node.paused = false;
	}

	/** The last lines of the node's stdout and stderr, for failure messages. */
	public String logTail(String id) {
		var node = node(id);
		return "---- node " + id + " stdout (tail) ----\n" + tail(node.stdout())
				+ "---- node " + id + " stderr (tail) ----\n" + tail(node.stderr());
	}

	/** Kills every node and closes the network. Directories are left for inspection. */
	@Override
	public synchronized void close() {
		for (var node : nodes.values()) {
			if (node.process != null) {
				node.process.destroyForcibly();
			}
		}
		for (var node : nodes.values()) {
			if (node.process != null) {
				awaitExit(node);
			}
		}
		network.close();
	}

	private List<String> command(Node node) {
		var command = new ArrayList<String>();
		command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
		command.add("-cp");
		command.add(System.getProperty("java.class.path"));
		command.add("--enable-native-access=ALL-UNNAMED");
		command.add("-Djdk.net.hosts.file=" + network.hostsFile(node.id));
		// Same logging configuration as the test JVM (surefire sets the slf4j-simple levels).
		System.getProperties().stringPropertyNames().stream()
				.filter(name -> name.startsWith("org.slf4j.simpleLogger."))
				.sorted()
				.forEach(name -> command.add("-D" + name + "=" + System.getProperty(name)));
		command.add(MAIN_CLASS);
		command.add("run");
		command.add("--node-id");
		command.add(node.id);
		for (var peer : nodes.values()) {
			var host = Network.hostname(peer.id);
			command.add("--peer");
			command.add(new PeerConfig(peer.id, host, peer.raftPort, host, peer.apiPort).format());
		}
		command.add("--data-dir");
		command.add(node.dataDir().toString());
		command.add("--raft-bind-host");
		command.add(LOOPBACK);
		command.add("--api-bind-host");
		command.add(LOOPBACK);
		command.addAll(serverArgs);
		return command;
	}

	private static boolean printedReady(Node node, long stdoutStart) {
		var readyLine = "READY " + node.id;
		try {
			var bytes = Files.readAllBytes(node.stdout());
			int start = (int) stdoutStart;
			var output = new String(bytes, start, bytes.length - start, StandardCharsets.UTF_8);
			return output.lines().anyMatch(line -> line.strip().equals(readyLine));
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	private static void awaitExit(Node node) {
		try {
			node.process.waitFor();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while waiting for node " + node.id + " to exit", e);
		}
		node.paused = false;
	}

	private static void signal(Node node, String signal) {
		try {
			var kill = new ProcessBuilder("kill", signal, String.valueOf(node.process.pid()))
					.redirectErrorStream(true)
					.start();
			var output = new String(kill.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
			if (kill.waitFor() != 0) {
				throw new IllegalStateException("kill " + signal + " " + node.process.pid() + " failed: " + output);
			}
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while signalling node " + node.id, e);
		}
	}

	private static String tail(Path file) {
		try {
			if (!Files.exists(file)) {
				return "(no output)\n";
			}
			var lines = Files.readAllLines(file, StandardCharsets.UTF_8);
			var tail = lines.subList(Math.max(0, lines.size() - FAILURE_LOG_LINES), lines.size());
			return String.join("\n", tail) + "\n";
		} catch (IOException e) {
			return "(unreadable: " + e + ")\n";
		}
	}

	/**
	 * Picks distinct free ports. They are released before the nodes bind them, so another process could take one
	 * in between; that is unlikely enough for tests.
	 */
	private static List<Integer> freePorts(int count) throws IOException {
		var sockets = new ArrayList<ServerSocket>();
		try {
			for (int i = 0; i < count; i++) {
				sockets.add(new ServerSocket(0, 1, InetAddress.getByName(LOOPBACK)));
			}
			return sockets.stream().map(ServerSocket::getLocalPort).toList();
		} finally {
			for (var socket : sockets) {
				socket.close();
			}
		}
	}

	private synchronized Node runningNode(String id) {
		var node = node(id);
		if (!node.running()) {
			throw new IllegalStateException("Node " + id + " is not running");
		}
		return node;
	}

	private Node node(String id) {
		var node = nodes.get(id);
		if (node == null) {
			throw new IllegalArgumentException("Unknown node " + id);
		}
		return node;
	}
}
