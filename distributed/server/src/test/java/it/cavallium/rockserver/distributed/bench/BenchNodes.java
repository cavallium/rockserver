package it.cavallium.rockserver.distributed.bench;

import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * The server side of a benchmark: either a replicated cluster of {@code ReplicatedServerMain} JVMs or one
 * single-node Rockserver JVM, all on this machine, talking over loopback directly (no proxies), each with its data
 * under {@code root}. Every JVM gets the same heap and JVM options, so the two targets compare fairly.
 */
final class BenchNodes implements AutoCloseable {

	private static final String REPLICATED_MAIN = "it.cavallium.rockserver.distributed.ReplicatedServerMain";
	private static final String SINGLE_MAIN = "it.cavallium.rockserver.core.Main";
	private static final String LOOPBACK = "127.0.0.1";
	private static final Duration READY_TIMEOUT = Duration.ofMinutes(3);

	/** One server JVM. */
	record Node(String id, HostAndPort api, Process process, Path dir, String peerSpec) {}

	private final List<Node> nodes;
	private final boolean replicated;

	private BenchNodes(List<Node> nodes, boolean replicated) {
		this.nodes = nodes;
		this.replicated = replicated;
	}

	List<Node> nodes() {
		return nodes;
	}

	boolean replicated() {
		return replicated;
	}

	/**
	 * Starts {@code size} replicated nodes and waits until each prints {@code READY}.
	 *
	 * @param jvmOptions options for every node JVM, such as heap size or a JFR recording
	 * @param serverArgs extra {@code run} options, such as {@code --snapshot-every}
	 */
	static BenchNodes startReplicated(Path root, int size, List<String> jvmOptions, List<String> serverArgs)
			throws IOException {
		return startReplicated(root, List.of(), size, jvmOptions, serverArgs);
	}

	/**
	 * Like {@link #startReplicated(Path, int, List, List)}, with node {@code i}'s data under
	 * {@code dataRoots[i % dataRoots.size()]/<root name>/node-<id>} when {@code dataRoots} is not empty: put each
	 * node on its own device to stand for separate machines, whose fsyncs do not queue behind each other.
	 */
	static BenchNodes startReplicated(Path root, List<Path> dataRoots, int size, List<String> jvmOptions,
			List<String> serverArgs) throws IOException {
		var ports = freePorts(size * 2);
		var peers = new ArrayList<String>();
		for (int i = 0; i < size; i++) {
			peers.add((i + 1) + "=" + LOOPBACK + ":" + ports.get(2 * i) + "/" + LOOPBACK + ":" + ports.get(2 * i + 1));
		}
		var nodes = new ArrayList<Node>();
		try {
			for (int i = 0; i < size; i++) {
				var id = String.valueOf(i + 1);
				var dir = root.resolve("node-" + id);
				Files.createDirectories(dir);
				var command = javaCommand(jvmOptions, dir, id);
				command.add(REPLICATED_MAIN);
				command.add("run");
				command.add("--node-id");
				command.add(id);
				for (var peer : peers) {
					command.add("--peer");
					command.add(peer);
				}
				var dataDir = dataRoots.isEmpty() ? dir.resolve("data")
						: dataRoots.get(i % dataRoots.size()).resolve(root.getFileName().toString()).resolve("node-" + id);
				Files.createDirectories(dataDir.getParent());
				command.add("--data-dir");
				command.add(dataDir.toString());
				command.add("--raft-bind-host");
				command.add(LOOPBACK);
				command.add("--api-bind-host");
				command.add(LOOPBACK);
				command.addAll(serverArgs);
				nodes.add(new Node(id, new HostAndPort(LOOPBACK, ports.get(2 * i + 1)), launch(command, dir), dir, peers.get(i)));
			}
			for (var node : nodes) {
				awaitOutput(node, "READY " + node.id());
			}
		} catch (IOException | RuntimeException e) {
			nodes.forEach(node -> node.process().destroyForcibly());
			throw e;
		}
		return new BenchNodes(List.copyOf(nodes), true);
	}

	/** Starts one single-node Rockserver JVM serving gRPC, with the same JVM options. */
	static BenchNodes startSingle(Path root, List<String> jvmOptions, Path rockserverConfig) throws IOException {
		var ports = freePorts(2);
		var dir = root.resolve("single");
		Files.createDirectories(dir);
		var command = javaCommand(jvmOptions, dir, "single");
		command.add(SINGLE_MAIN);
		command.add("--database-url");
		command.add("file://" + dir.resolve("data").toAbsolutePath());
		command.add("--grpc-listen-url");
		command.add("http://" + LOOPBACK + ":" + ports.get(0));
		command.add("--thrift-listen-url");
		command.add("http://" + LOOPBACK + ":" + ports.get(1));
		if (rockserverConfig != null) {
			command.add("--config");
			command.add(rockserverConfig.toString());
		}
		var node = new Node("single", new HostAndPort(LOOPBACK, ports.get(0)), launch(command, dir), dir, null);
		try {
			awaitPort(node);
		} catch (IOException | RuntimeException e) {
			node.process().destroyForcibly();
			throw e;
		}
		return new BenchNodes(List.of(node), false);
	}

	private static List<String> javaCommand(List<String> jvmOptions, Path dir, String id) {
		var command = new ArrayList<String>();
		command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
		command.add("-cp");
		command.add(System.getProperty("java.class.path"));
		command.add("--enable-native-access=ALL-UNNAMED");
		command.add("-Dorg.slf4j.simpleLogger.defaultLogLevel=warn");
		command.add("-Dorg.slf4j.simpleLogger.log.it.cavallium.rockserver.distributed=info");
		for (var option : jvmOptions) {
			command.add(option.replace("{dir}", dir.toString()).replace("{id}", id));
		}
		return command;
	}

	private static Process launch(List<String> command, Path dir) throws IOException {
		return new ProcessBuilder(command)
				.redirectOutput(ProcessBuilder.Redirect.appendTo(dir.resolve("stdout.log").toFile()))
				.redirectError(ProcessBuilder.Redirect.appendTo(dir.resolve("stderr.log").toFile()))
				.start();
	}

	private static void awaitOutput(Node node, String line) throws IOException {
		long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
		var stdout = node.dir().resolve("stdout.log");
		while (System.nanoTime() - deadline < 0) {
			if (!node.process().isAlive()) {
				throw new IOException("Node " + node.id() + " exited with " + node.process().exitValue() + ":\n"
						+ tail(node.dir().resolve("stderr.log")));
			}
			if (Files.exists(stdout)
					&& Files.readString(stdout, StandardCharsets.UTF_8).lines().anyMatch(l -> l.strip().equals(line))) {
				return;
			}
			sleep(100);
		}
		throw new IOException("Node " + node.id() + " not ready in " + READY_TIMEOUT);
	}

	private static void awaitPort(Node node) throws IOException {
		long deadline = System.nanoTime() + READY_TIMEOUT.toNanos();
		while (System.nanoTime() - deadline < 0) {
			if (!node.process().isAlive()) {
				throw new IOException("Node " + node.id() + " exited:\n" + tail(node.dir().resolve("stderr.log")));
			}
			try (var socket = new java.net.Socket()) {
				socket.connect(new InetSocketAddress(node.api().host(), node.api().port()), 200);
				return;
			} catch (IOException notYet) {
				sleep(100);
			}
		}
		throw new IOException("Node " + node.id() + " not listening in " + READY_TIMEOUT);
	}

	static String tail(Path file) {
		try {
			var lines = Files.readAllLines(file, StandardCharsets.UTF_8);
			return String.join("\n", lines.subList(Math.max(0, lines.size() - 40), lines.size()));
		} catch (IOException e) {
			return "(no " + file + ")";
		}
	}

	private static List<Integer> freePorts(int count) throws IOException {
		var sockets = new ArrayList<ServerSocket>();
		try {
			var ports = new ArrayList<Integer>();
			for (int i = 0; i < count; i++) {
				var socket = new ServerSocket(0, 1, InetAddress.getByName(LOOPBACK));
				sockets.add(socket);
				ports.add(socket.getLocalPort());
			}
			return ports;
		} finally {
			for (var socket : sockets) {
				socket.close();
			}
		}
	}

	/** The id of the current Raft leader, from {@code ReplicatedServerMain admin status}; null if none. */
	String leaderId() throws IOException, InterruptedException {
		var command = new ArrayList<String>();
		command.add(Path.of(System.getProperty("java.home"), "bin", "java").toString());
		command.add("-cp");
		command.add(System.getProperty("java.class.path"));
		command.add("--enable-native-access=ALL-UNNAMED");
		command.add("-Dorg.slf4j.simpleLogger.defaultLogLevel=error");
		command.add(REPLICATED_MAIN);
		command.add("admin");
		for (var node : nodes) {
			command.add("--peer");
			command.add(node.peerSpec());
		}
		command.add("status");
		var process = new ProcessBuilder(command).redirectErrorStream(true).start();
		var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
		process.waitFor();
		for (var line : output.lines().toList()) {
			int colon = line.indexOf(": LEADER");
			if (colon > 0) {
				return line.substring(0, colon).strip();
			}
		}
		return null;
	}

	/** Resident set size of each node JVM in bytes, from /proc. */
	Map<String, Long> residentBytes() {
		var result = new LinkedHashMap<String, Long>();
		for (var node : nodes) {
			try {
				for (var line : Files.readAllLines(Path.of("/proc/" + node.process().pid() + "/status"))) {
					if (line.startsWith("VmRSS:")) {
						result.put(node.id(), Long.parseLong(line.replaceAll("\\D+", "")) * 1024);
					}
				}
			} catch (IOException | NumberFormatException e) {
				result.put(node.id(), -1L);
			}
		}
		return result;
	}

	/** CPU time (user + system) of each node JVM in nanoseconds, from /proc. */
	Map<String, Long> cpuNanos() {
		var result = new LinkedHashMap<String, Long>();
		long tick = 100; // USER_HZ on Linux
		for (var node : nodes) {
			try {
				var stat = Files.readString(Path.of("/proc/" + node.process().pid() + "/stat"));
				var fields = stat.substring(stat.lastIndexOf(')') + 2).split(" ");
				long ticks = Long.parseLong(fields[11]) + Long.parseLong(fields[12]);
				result.put(node.id(), ticks * (1_000_000_000L / tick));
			} catch (IOException | RuntimeException e) {
				result.put(node.id(), -1L);
			}
		}
		return result;
	}

	/**
	 * Bytes written by each whole-disk block device, from /proc/diskstats: unlike a process's {@code write_bytes},
	 * which XFS inflates for preallocated files such as the Raft log, this is what reached the disks. It includes
	 * every other process writing to them.
	 */
	static Map<String, Long> deviceWriteBytes() {
		var result = new LinkedHashMap<String, Long>();
		try {
			for (var line : Files.readAllLines(Path.of("/proc/diskstats"))) {
				var fields = line.trim().split("\\s+");
				var name = fields[2];
				if (Files.exists(Path.of("/sys/block", name, "device"))) {
					result.put(name, Long.parseLong(fields[9]) * 512);
				}
			}
		} catch (IOException | RuntimeException e) {
			// Not Linux: no device statistics.
		}
		return result;
	}

	/** Bytes written to disk by each node JVM, from /proc. */
	Map<String, Long> writeBytes() {
		var result = new LinkedHashMap<String, Long>();
		for (var node : nodes) {
			try {
				for (var line : Files.readAllLines(Path.of("/proc/" + node.process().pid() + "/io"))) {
					if (line.startsWith("write_bytes:")) {
						result.put(node.id(), Long.parseLong(line.replaceAll("\\D+", "")));
					}
				}
			} catch (IOException | NumberFormatException e) {
				result.put(node.id(), -1L);
			}
		}
		return result;
	}

	/** Stops every node gracefully (SIGTERM), then forcibly after a grace period. */
	@Override
	public void close() {
		for (var node : nodes) {
			node.process().destroy();
		}
		for (var node : nodes) {
			try {
				if (!node.process().waitFor(60, TimeUnit.SECONDS)) {
					node.process().destroyForcibly().waitFor(10, TimeUnit.SECONDS);
				}
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				node.process().destroyForcibly();
			}
		}
	}

	/** Kills one node at once (SIGKILL), for failover runs. */
	void kill(String id) {
		for (var node : nodes) {
			if (node.id().equals(id)) {
				node.process().destroyForcibly();
				try {
					node.process().waitFor(30, TimeUnit.SECONDS);
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
				}
			}
		}
	}

	private static void sleep(long millis) {
		try {
			Thread.sleep(millis);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new UncheckedIOException(new IOException("Interrupted", e));
		}
	}
}
