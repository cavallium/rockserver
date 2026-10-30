package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * A cluster of {@link ReplicatedServerMain} JVMs on this machine, connected through a {@link Network}.
 *
 * <p>Node ids are {@code "1"} to {@code "N"}, followed by the ids of spare nodes, if any ({@link #create}): those
 * have ports, links and directories from the start but join the group only when the test starts them and changes
 * the membership ({@link #setMembers}, {@link #admin}). Every node binds its Raft and API ports on
 * {@code 127.0.0.1} and names its peers {@code node-<id>} for both ports, so all node-to-node traffic goes through
 * the network's link proxies. The test JVM reaches each node's API directly through {@link #apiAddress},
 * bypassing the proxies.</p>
 *
 * <p>Everything lives under the root directory given at start: {@code <root>/node-<id>/data} (the node's data
 * directory, kept across restarts), {@code <root>/node-<id>/stdout.log} and {@code stderr.log} (appended to by
 * every start), and {@code <root>/network} (hosts files).</p>
 *
 * <p>The node JVMs are killed by {@link #close()}, or by a shutdown hook if the test JVM exits (or gets SIGTERM)
 * without closing the cluster. Only a SIGKILL of the test JVM leaves them running.</p>
 */
public final class ProcessCluster implements AutoCloseable {

	private static final String MAIN_CLASS = "it.cavallium.rockserver.distributed.ReplicatedServerMain";
	private static final Duration ADMIN_TIMEOUT = Duration.ofSeconds(60);
	private static final String LOOPBACK = "127.0.0.1";
	private static final Duration GRACEFUL_STOP_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration READY_POLL_INTERVAL = Duration.ofMillis(50);
	private static final int FAILURE_LOG_LINES = 60;
	/** Lowest port handed to nodes; ports from here up to the kernel's ephemeral range are never auto-assigned. */
	private static final int FIRST_NODE_PORT = 10_000;
	private static final Path EPHEMERAL_PORT_RANGE = Path.of("/proc/sys/net/ipv4/ip_local_port_range");
	/** Linux default, used where the range cannot be read. */
	private static final int DEFAULT_EPHEMERAL_PORT_START = 32_768;

	private final String mainClass;
	private final List<String> serverArgs;
	private final Network network;
	private final Map<String, Node> nodes;
	/** The peer list given to nodes when they start. Guarded by this. */
	private List<String> members;
	private final Thread shutdownHook = new Thread(this::killAllNow, "process-cluster-shutdown");

	private static final class Node {
		final String id;
		final int raftPort;
		final int apiPort;
		final Path dir;
		/** Volatile for the shutdown hook, which must not take the cluster lock. */
		volatile Process process;
		/** Size of the stdout log when the current process started: its own output follows. */
		long stdoutStart;
		/** Started and not killed or stopped since: a node that should run but does not has crashed. */
		boolean shouldRun;
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

	private ProcessCluster(String mainClass, List<String> serverArgs, Network network, Map<String, Node> nodes,
			List<String> members) {
		this.mainClass = mainClass;
		this.serverArgs = serverArgs;
		this.network = network;
		this.nodes = nodes;
		this.members = members;
	}

	/**
	 * Starts {@code size} nodes without waiting for them; call {@link #waitReady(Duration)} next.
	 *
	 * @param serverArgs extra {@code run} options passed to every node, e.g. {@code --snapshot-every 100}
	 */
	public static ProcessCluster start(Path root, int size, List<String> serverArgs) throws IOException {
		return start(root, size, serverArgs, MAIN_CLASS);
	}

	/** Like {@link #start(Path, int, List)}, with another main class taking the same {@code run} options. */
	static ProcessCluster start(Path root, int size, List<String> serverArgs, String mainClass) throws IOException {
		var cluster = create(root, size, 0, serverArgs, mainClass);
		try {
			for (var id : cluster.members()) {
				cluster.start(id);
			}
		} catch (IOException | RuntimeException e) {
			cluster.close();
			throw e;
		}
		return cluster;
	}

	/**
	 * Prepares {@code size} member nodes and {@code spares} spare nodes without starting any, so that the test can
	 * fill their data directories first; start them with {@link #start(String)}.
	 *
	 * @param serverArgs extra {@code run} options passed to every node, e.g. {@code --snapshot-every 100}
	 */
	public static ProcessCluster create(Path root, int size, int spares, List<String> serverArgs) throws IOException {
		return create(root, size, spares, serverArgs, MAIN_CLASS);
	}

	private static ProcessCluster create(Path root, int size, int spares, List<String> serverArgs, String mainClass)
			throws IOException {
		int total = size + spares;
		var nodes = new LinkedHashMap<String, Node>();
		var ports = new LinkedHashMap<String, List<Integer>>();
		var linkAddresses = Network.linkAddresses(total);
		var freePorts = freePorts(total * 2, linkAddresses);
		for (int i = 0; i < total; i++) {
			var id = String.valueOf(i + 1);
			var node = new Node(id, freePorts.get(2 * i), freePorts.get(2 * i + 1), root.resolve(Network.hostname(id)));
			Files.createDirectories(node.dir);
			nodes.put(id, node);
			ports.put(id, List.of(node.raftPort, node.apiPort));
		}
		var network = Network.start(ports, root.resolve("network"));
		var members = List.copyOf(nodes.keySet()).subList(0, size);
		var cluster = new ProcessCluster(mainClass, List.copyOf(serverArgs), network, nodes, members);
		Runtime.getRuntime().addShutdownHook(cluster.shutdownHook);
		return cluster;
	}

	/** Every node, members and spares. */
	public List<String> nodeIds() {
		return List.copyOf(nodes.keySet());
	}

	/** The peer list that nodes get when they start: the initial members unless {@link #setMembers} changed it. */
	public synchronized List<String> members() {
		return members;
	}

	/**
	 * Sets the peer list given to nodes started from now on (a node not in it gets it plus itself). Running nodes
	 * keep theirs; the group's own membership changes only through {@link #admin}.
	 */
	public synchronized void setMembers(List<String> ids) {
		ids.forEach(this::node);
		members = List.copyOf(ids);
	}

	/** The node's address as nodes see it, in the {@code --peer} format of {@link ReplicatedServerMain}. */
	public PeerConfig peer(String id) {
		var node = node(id);
		var host = Network.hostname(id);
		return new PeerConfig(id, host, node.raftPort, host, node.apiPort);
	}

	/**
	 * Runs {@code ReplicatedServerMain admin --peer <each of peerIds> <command...>} in its own JVM, which reaches
	 * the nodes directly (see {@link Network#directHostsFile}), and returns its output. Fails with the output if
	 * it exits with an error or does not finish in time.
	 */
	public String admin(List<String> peerIds, String... command) throws IOException {
		var args = new ArrayList<String>();
		args.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
		args.add("-cp");
		args.add(System.getProperty("java.class.path"));
		args.add("--enable-native-access=ALL-UNNAMED");
		args.add("--add-modules=jdk.incubator.vector");
		args.add("-Djdk.net.hosts.file=" + network.directHostsFile());
		args.add(MAIN_CLASS);
		args.add("admin");
		for (var id : peerIds) {
			args.add("--peer");
			args.add(peer(id).format());
		}
		args.addAll(List.of(command));
		var process = new ProcessBuilder(args).redirectErrorStream(true).start();
		var output = new StringBuilder();
		var reader = Thread.ofVirtual().start(() -> {
			try (var in = process.getInputStream()) {
				output.append(new String(in.readAllBytes(), StandardCharsets.UTF_8));
			} catch (IOException e) {
				output.append("(output unreadable: ").append(e).append(")");
			}
		});
		try {
			if (!process.waitFor(ADMIN_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS)) {
				process.destroyForcibly();
				process.waitFor();
				reader.join();
				throw new IllegalStateException("admin " + String.join(" ", command) + " did not finish within "
						+ ADMIN_TIMEOUT + ":\n" + output);
			}
			reader.join();
		} catch (InterruptedException e) {
			process.destroyForcibly();
			Thread.currentThread().interrupt();
			throw new IllegalStateException("Interrupted while running admin " + String.join(" ", command), e);
		}
		if (process.exitValue() != 0) {
			throw new IllegalStateException("admin " + String.join(" ", command) + " exited with code "
					+ process.exitValue() + ":\n" + output);
		}
		return output.toString();
	}

	public Network network() {
		return network;
	}

	/** Address of the node's API, reachable directly from the test JVM. */
	public HostAndPort apiAddress(String id) {
		return new HostAndPort(LOOPBACK, node(id).apiPort);
	}

	/** Address of the node's Raft server, reachable directly from the test JVM (e.g. to query its group info). */
	public HostAndPort raftAddress(String id) {
		return new HostAndPort(LOOPBACK, node(id).raftPort);
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

	/** Process id of the node's current (or last) JVM. */
	synchronized long pid(String id) {
		var process = node(id).process;
		if (process == null) {
			throw new IllegalStateException("Node " + id + " was never started");
		}
		return process.pid();
	}

	/**
	 * Waits until every node that was started and not killed or stopped since printed its {@code READY} line.
	 * Fails if one of them exited instead, even if it printed {@code READY} first.
	 */
	public void waitReady(Duration timeout) {
		long deadline = System.nanoTime() + timeout.toNanos();
		List<String> expected;
		synchronized (this) {
			expected = nodes.values().stream().filter(node -> node.shouldRun).map(node -> node.id).toList();
		}
		for (var id : expected) {
			waitReady(id, Duration.ofNanos(Math.max(0, deadline - System.nanoTime())));
		}
	}

	/**
	 * Waits until the node printed {@code READY <id>} since its last start and is still alive; fails with its log
	 * tail otherwise.
	 */
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
			boolean ready = printedReady(node, stdoutStart);
			// Checked after reading the log: if the node is alive now, the READY line read above is current.
			if (!process.isAlive()) {
				throw new IllegalStateException("Node " + id + " exited with code " + process.exitValue()
						+ (ready ? " after" : " before") + " printing READY\n" + logTail(id));
			}
			if (ready) {
				return;
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
		node.shouldRun = true;
		node.paused = false;
	}

	/** SIGKILL: no shutdown hook, no leadership hand-off. */
	public synchronized void kill(String id) {
		var node = node(id);
		node.shouldRun = false;
		if (node.process == null) {
			return;
		}
		node.process.destroyForcibly();
		awaitExit(node);
	}

	/**
	 * SIGKILL to every node at once, like a power cut of the whole cluster: all signals are sent before waiting for
	 * any node to exit, so no node outlives another by more than the time it takes to send them.
	 */
	public synchronized void killAll() {
		for (var node : nodes.values()) {
			node.shouldRun = false;
		}
		killAllNow();
		for (var node : nodes.values()) {
			if (node.process != null) {
				awaitExit(node);
			}
		}
	}

	/** SIGTERM, then waits for the node to stop gracefully; kills it and fails if it does not. */
	public synchronized void stop(String id) {
		var node = node(id);
		node.shouldRun = false;
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

	/** The node's stderr log (where it logs), appended to by every start; each start begins with a marker line. */
	public Path stderrLog(String id) {
		return node(id).stderr();
	}

	/** Everything the node printed on stdout and stderr, over all its starts. */
	public String output(String id) {
		var node = node(id);
		try {
			var stdout = Files.exists(node.stdout()) ? Files.readString(node.stdout()) : "";
			var stderr = Files.exists(node.stderr()) ? Files.readString(node.stderr()) : "";
			return stdout + stderr;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
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
		killAllNow();
		for (var node : nodes.values()) {
			node.shouldRun = false;
			if (node.process != null) {
				awaitExit(node);
			}
		}
		network.close();
		try {
			Runtime.getRuntime().removeShutdownHook(shutdownHook);
		} catch (IllegalStateException shuttingDown) {
			// close() runs during shutdown: the hook is running or has run, nothing to remove.
		}
	}

	/** Sends SIGKILL to every node without waiting: also the shutdown hook, so it takes no lock. */
	private void killAllNow() {
		for (var node : nodes.values()) {
			var process = node.process;
			if (process != null) {
				process.destroyForcibly();
			}
		}
	}

	/**
	 * Tests only: {@code -Drockserver.test.pipelinedUpdates=true} runs every test cluster, of processes or in one JVM,
	 * with {@link NodeConfig#pipelinedUpdates()} on.
	 */
	static final boolean PIPELINED_UPDATES = Boolean.getBoolean("rockserver.test.pipelinedUpdates");
	private static final String PIPELINED_UPDATES_ARG = "--pipelined-updates";

	/** {@code serverArgs} with pipelined updates on, whatever the system property says. */
	static List<String> pipelined(List<String> serverArgs) {
		var args = new ArrayList<>(serverArgs);
		args.add(PIPELINED_UPDATES_ARG);
		return args;
	}

	/** Whether this cluster's nodes have pipelined updates on. */
	boolean pipelinedUpdates() {
		return PIPELINED_UPDATES || serverArgs.contains(PIPELINED_UPDATES_ARG);
	}

	private List<String> command(Node node) {
		var command = new ArrayList<String>();
		command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
		command.add("-cp");
		command.add(System.getProperty("java.class.path"));
		command.add("--enable-native-access=ALL-UNNAMED");
		command.add("--add-modules=jdk.incubator.vector");
		command.add("-Djdk.net.hosts.file=" + network.hostsFile(node.id));
		// Same logging configuration as the test JVM (surefire sets the slf4j-simple levels).
		System.getProperties().stringPropertyNames().stream()
				.filter(name -> name.startsWith("org.slf4j.simpleLogger."))
				.sorted()
				.forEach(name -> command.add("-D" + name + "=" + System.getProperty(name)));
		command.add(mainClass);
		command.add("run");
		command.add("--node-id");
		command.add(node.id);
		var peers = new ArrayList<>(members);
		if (!peers.contains(node.id)) {
			peers.add(node.id);
		}
		for (var peer : peers) {
			command.add("--peer");
			command.add(peer(peer).format());
		}
		command.add("--data-dir");
		command.add(node.dataDir().toString());
		command.add("--raft-bind-host");
		command.add(LOOPBACK);
		command.add("--api-bind-host");
		command.add(LOOPBACK);
		command.addAll(serverArgs);
		if (PIPELINED_UPDATES && !serverArgs.contains(PIPELINED_UPDATES_ARG)) {
			command.add(PIPELINED_UPDATES_ARG);
		}
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
	 * Picks distinct ports that are free on {@code 127.0.0.1} and on every link address.
	 *
	 * <p>A node keeps its ports across restarts, so they must stay free while it is down. Ports in the kernel's
	 * ephemeral range would not: any outgoing connection (a link proxy retrying the dead node, a client) may get
	 * one as its source port, and the restarted node would fail to bind it. So the ports come from below that
	 * range, starting at a random offset to keep parallel test JVMs apart. Requiring the link addresses to be free
	 * also skips the ports of other clusters on this machine, even while their node is down, since their link
	 * proxies keep those addresses bound. Another process could still bind a port between this check and the
	 * node's first start; that is unlikely enough for tests.</p>
	 */
	private static List<Integer> freePorts(int count, List<InetAddress> linkAddresses) throws IOException {
		var addresses = new ArrayList<InetAddress>();
		addresses.add(InetAddress.getByName(LOOPBACK));
		addresses.addAll(linkAddresses);
		int span = ephemeralPortStart() - FIRST_NODE_PORT;
		if (span < count) {
			throw new IllegalStateException("No room below the ephemeral port range (" + EPHEMERAL_PORT_RANGE
					+ ") for " + count + " ports from " + FIRST_NODE_PORT);
		}
		int offset = ThreadLocalRandom.current().nextInt(span);
		var ports = new ArrayList<Integer>(count);
		for (int i = 0; i < span && ports.size() < count; i++) {
			int port = FIRST_NODE_PORT + (offset + i) % span;
			if (isFree(port, addresses)) {
				ports.add(port);
			}
		}
		if (ports.size() < count) {
			throw new IOException("Only " + ports.size() + " of " + count + " free ports below the ephemeral range");
		}
		return ports;
	}

	/** First port the kernel auto-assigns to outgoing connections. */
	private static int ephemeralPortStart() {
		// Read a line: Files.readString and readAllBytes return only the first byte of this /proc file on JDK 27.
		try (var reader = Files.newBufferedReader(EPHEMERAL_PORT_RANGE)) {
			return Integer.parseInt(reader.readLine().strip().split("\\s+")[0]);
		} catch (NoSuchFileException e) {
			return DEFAULT_EPHEMERAL_PORT_START;
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/** Binds the port on every address the way the nodes and proxies will (with SO_REUSEADDR), then releases it. */
	private static boolean isFree(int port, List<InetAddress> addresses) throws IOException {
		var sockets = new ArrayList<ServerSocket>();
		try {
			for (var address : addresses) {
				var socket = new ServerSocket();
				sockets.add(socket);
				socket.setReuseAddress(true);
				socket.bind(new InetSocketAddress(address, port), 1);
			}
			return true;
		} catch (IOException e) {
			return false;
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
