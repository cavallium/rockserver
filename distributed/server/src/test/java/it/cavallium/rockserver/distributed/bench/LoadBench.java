package it.cavallium.rockserver.distributed.bench;

import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.distributed.bench.YotsubaWorkload.Kind;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.Semaphore;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.locks.LockSupport;

/**
 * Load benchmark of replicated Rockserver against a single node, with a Yotsuba-shaped workload
 * ({@link YotsubaWorkload}). It starts the servers as separate JVMs ({@link BenchNodes}), preloads data, then runs
 * either open-loop steps at multiples of the production rate, or closed-loop steps with a fixed number of
 * concurrent clients to find the maximum throughput. Results go to stdout and to {@code <root>/results.md}.
 *
 * <pre>
 * java -cp ... LoadBench --target replicated|single --root DIR [--scales 1,2,4] [--closed 64,256]
 *     [--step-seconds 30] [--warmup-seconds 10] [--route leader|all] [--connections 4]
 *     [--chats 20000] [--messages-per-chat 50] [--value-bytes 600] [--skew 0.5] [--max-in-flight 512]
 *     [--only UPDATE,GET] [--jvm "-Xmx4g -XX:+UseZGC"] [--server-arg "--snapshot-every 100000"] [--label text]
 * </pre>
 */
public final class LoadBench {

	private final Map<String, String> options;
	private final Path root;
	private final StringBuilder report = new StringBuilder();

	private LoadBench(Map<String, String> options) {
		this.options = options;
		this.root = Path.of(required("--root"));
	}

	public static void main(String[] args) throws Exception {
		var options = new LinkedHashMap<String, String>();
		for (int i = 0; i < args.length; i += 2) {
			if (!args[i].startsWith("--") || i + 1 >= args.length) {
				throw new IllegalArgumentException("Expected --name value pairs, got " + Arrays.toString(args));
			}
			options.put(args[i], args[i + 1]);
		}
		new LoadBench(options).run();
		System.exit(0);
	}

	private void run() throws Exception {
		Files.createDirectories(root);
		var jvm = List.of(option("--jvm", "-Xms4g -Xmx4g -XX:+UseZGC -XX:+AlwaysPreTouch").split("\\s+"));
		var serverArgs = splitArgs(option("--server-arg", ""));
		boolean replicated = option("--target", "replicated").equals("replicated");
		int nodeCount = Integer.parseInt(option("--nodes", "3"));
		var workload = new YotsubaWorkload(Integer.parseInt(option("--chats", "20000")),
				Integer.parseInt(option("--messages-per-chat", "50")), Integer.parseInt(option("--value-bytes", "600")),
				Double.parseDouble(option("--skew", "0.5")));

		line("# " + option("--label", "load benchmark") + " — " + Instant.now());
		line("target=" + (replicated ? "replicated x" + nodeCount : "single") + " options=" + options);
		try (var nodes = replicated
				? BenchNodes.startReplicated(root, list("--node-dirs", "").stream().map(Path::of).toList(), nodeCount, jvm,
						serverArgs)
				: BenchNodes.startSingle(root, jvm, null)) {
			var connections = connect(nodes, replicated);
			try {
				var setup = connections.getFirst().getAsyncApi(RequestContext.batch(YotsubaWorkload.TIMEOUT));
				workload.createColumns(setup);
				long preloadStart = System.nanoTime();
				workload.preload(setup, 512, 16);
				double preloadSeconds = (System.nanoTime() - preloadStart) / 1e9;
				long preloaded = (long) workload.chats * workload.messagesPerChat;
				line(String.format(Locale.ROOT, "preload: %d messages in %.1f s (%.0f keys/s, %.1f MiB/s)", preloaded,
						preloadSeconds, preloaded / preloadSeconds,
						preloaded * (workload.valueBytes + 12) / preloadSeconds / (1 << 20)));

				var mix = mix();
				int warmup = Integer.parseInt(option("--warmup-seconds", "10"));
				int step = Integer.parseInt(option("--step-seconds", "30"));
				for (var scale : list("--scales", "")) {
					openLoop(nodes, connections, workload, mix, Double.parseDouble(scale), warmup, step);
				}
				for (var clients : list("--closed", "")) {
					closedLoop(nodes, connections, workload, mix, Integer.parseInt(clients), warmup, step);
				}
			} finally {
				connections.forEach(GrpcConnection::close);
			}
			line("node RSS MiB: " + mib(nodes.residentBytes()));
			BenchStats.SAMPLES.forEach((kind, message) -> line("error " + kind + ": " + message));
		}
		Files.writeString(root.resolve("results.md"), report, StandardCharsets.UTF_8, StandardOpenOption.CREATE,
				StandardOpenOption.APPEND);
	}

	/** Connections to the leader only, or spread over every node. */
	private List<GrpcConnection> connect(BenchNodes nodes, boolean replicated) throws Exception {
		int perNode = Integer.parseInt(option("--connections", "4"));
		var targets = new ArrayList<BenchNodes.Node>();
		if (!replicated || option("--route", "leader").equals("all")) {
			targets.addAll(nodes.nodes());
		} else {
			targets.add(leader(nodes));
		}
		var connections = new ArrayList<GrpcConnection>();
		for (var node : targets) {
			for (int i = 0; i < perNode; i++) {
				connections.add(GrpcConnection.forHostAndPort("bench-" + node.id() + "-" + i, node.api()));
			}
		}
		line("connections: " + connections.size() + " to nodes " + targets.stream().map(BenchNodes.Node::id).toList());
		return connections;
	}

	/** The current leader, polled until one is elected. */
	private BenchNodes.Node leader(BenchNodes nodes) throws Exception {
		for (int attempt = 0; attempt < 100; attempt++) {
			var id = nodes.leaderId();
			for (var node : nodes.nodes()) {
				if (node.id().equals(id)) {
					line("leader: node " + id);
					return node;
				}
			}
			Thread.sleep(200);
		}
		throw new IllegalStateException("No leader elected");
	}

	/** Open loop: every kind arrives as a Poisson process at {@code scale} times its production rate. */
	private void openLoop(BenchNodes nodes, List<GrpcConnection> connections, YotsubaWorkload workload,
			Map<Kind, Double> mix, double scale, int warmupSeconds, int stepSeconds) throws Exception {
		var stats = new BenchStats();
		var stop = new AtomicBoolean();
		int maxInFlight = Integer.parseInt(option("--max-in-flight", "512"));
		var writes = new Semaphore(maxInFlight);
		var reads = new Semaphore(maxInFlight);
		var waiting = new AtomicInteger();
		var executor = Executors.newVirtualThreadPerTaskExecutor();
		var apis = apis(connections);
		var counter = new AtomicLong();
		var dispatchers = new ArrayList<Thread>();
		for (var entry : mix.entrySet()) {
			var kind = entry.getKey();
			double rate = entry.getValue() * scale;
			if (rate <= 0) {
				continue;
			}
			var op = stats.op(kind.name());
			var limiter = isWrite(kind) ? writes : reads;
			dispatchers.add(Thread.ofPlatform().daemon().name("dispatch-" + kind).start(() -> {
				long next = System.nanoTime();
				var random = ThreadLocalRandom.current();
				while (!stop.get()) {
					next += (long) (-Math.log(1 - random.nextDouble()) / rate * 1e9);
					long wait = next - System.nanoTime();
					if (wait > 0) {
						LockSupport.parkNanos(wait);
					}
					long intended = next;
					if (waiting.get() > 100_000) {
						stats.dropped.increment();
						continue;
					}
					waiting.incrementAndGet();
					var api = apis.get((int) (counter.getAndIncrement() % apis.size()));
					executor.execute(() -> {
						limiter.acquireUninterruptibly();
						waiting.decrementAndGet();
						try {
							long bytes = workload.run(kind, api.get(kind), op);
							op.success(System.nanoTime() - intended, bytes);
						} catch (Throwable error) {
							op.failure(error);
						} finally {
							limiter.release();
						}
					});
				}
			}));
		}
		measure(nodes, stats, String.format(Locale.ROOT, "open loop x%.2f (%.0f ops/s offered)", scale,
				mix.values().stream().mapToDouble(Double::doubleValue).sum() * scale), warmupSeconds, stepSeconds);
		stop.set(true);
		for (var dispatcher : dispatchers) {
			dispatcher.join();
		}
		executor.shutdown();
		executor.awaitTermination(2, TimeUnit.MINUTES);
		executor.shutdownNow();
	}

	/** Closed loop: {@code clients} workers each run operations back to back, picked by the production mix. */
	private void closedLoop(BenchNodes nodes, List<GrpcConnection> connections, YotsubaWorkload workload,
			Map<Kind, Double> mix, int clients, int warmupSeconds, int stepSeconds) throws Exception {
		var stats = new BenchStats();
		var stop = new AtomicBoolean();
		var apis = apis(connections);
		var kinds = mix.keySet().toArray(Kind[]::new);
		var cumulative = new double[kinds.length];
		double total = 0;
		for (int i = 0; i < kinds.length; i++) {
			total += mix.get(kinds[i]);
			cumulative[i] = total;
		}
		double sum = total;
		var done = new CountDownLatch(clients);
		for (int c = 0; c < clients; c++) {
			var api = apis.get(c % apis.size());
			Thread.ofVirtual().name("client-" + c).start(() -> {
				var random = ThreadLocalRandom.current();
				try {
					while (!stop.get()) {
						double pick = random.nextDouble() * sum;
						int index = 0;
						while (cumulative[index] < pick) {
							index++;
						}
						var kind = kinds[index];
						var op = stats.op(kind.name());
						long start = System.nanoTime();
						try {
							long bytes = workload.run(kind, api.get(kind), op);
							op.success(System.nanoTime() - start, bytes);
						} catch (Throwable error) {
							op.failure(error);
						}
					}
				} finally {
					done.countDown();
				}
			});
		}
		measure(nodes, stats, "closed loop, " + clients + " clients", warmupSeconds, stepSeconds);
		stop.set(true);
		done.await(2, TimeUnit.MINUTES);
	}

	/** Runs the warmup, then the measured interval, and reports it with the servers' CPU and disk usage. */
	private void measure(BenchNodes nodes, BenchStats stats, String title, int warmupSeconds, int stepSeconds)
			throws InterruptedException {
		Thread.sleep(warmupSeconds * 1000L);
		stats.snapshot(warmupSeconds);
		if (options.containsKey("--jfr")) {
			startJfr(nodes, title, stepSeconds);
		}
		var cpuBefore = nodes.cpuNanos();
		var diskBefore = BenchNodes.deviceWriteBytes();
		long start = System.nanoTime();
		Thread.sleep(stepSeconds * 1000L);
		double seconds = (System.nanoTime() - start) / 1e9;
		var rows = stats.snapshot(seconds);
		var cpuAfter = nodes.cpuNanos();
		var diskAfter = BenchNodes.deviceWriteBytes();
		var cpu = new LinkedHashMap<String, String>();
		var disk = new LinkedHashMap<String, String>();
		for (var id : cpuAfter.keySet()) {
			cpu.put(id, String.format(Locale.ROOT, "%.1f", (cpuAfter.get(id) - cpuBefore.get(id)) / 1e9 / seconds));
		}
		for (var device : diskAfter.keySet()) {
			long written = diskAfter.get(device) - diskBefore.getOrDefault(device, diskAfter.get(device));
			if (written > 0) {
				disk.put(device, String.format(Locale.ROOT, "%.1f", written / seconds / (1 << 20)));
			}
		}
		line("");
		line("## " + title);
		line("```");
		line(BenchStats.format(rows).stripTrailing());
		line("dropped (client queue full): " + stats.dropped.sumThenReset());
		line("server cores used: " + cpu + ", device writes MiB/s (all processes): " + disk);
		line("```");
	}

	/** Records the measured interval of every node with JFR, into {@code <root>/node-<id>-<step>.jfr}. */
	private void startJfr(BenchNodes nodes, String title, int seconds) {
		var step = title.replaceAll("[^A-Za-z0-9.]+", "-");
		for (var node : nodes.nodes()) {
			var file = root.resolve("node-" + node.id() + "-" + step + ".jfr");
			try {
				new ProcessBuilder("jcmd", String.valueOf(node.process().pid()), "JFR.start", "settings=profile",
						"duration=" + seconds + "s", "filename=" + file).redirectErrorStream(true)
						.redirectOutput(ProcessBuilder.Redirect.DISCARD).start();
			} catch (java.io.IOException e) {
				line("JFR not started on node " + node.id() + ": " + e);
			}
		}
	}

	/** Per connection, an API for each kind, with the workload profile Yotsuba uses for it. */
	private static List<Map<Kind, RocksDBAsyncAPI>> apis(List<GrpcConnection> connections) {
		var result = new ArrayList<Map<Kind, RocksDBAsyncAPI>>();
		for (var connection : connections) {
			var latency = connection.getAsyncApi(RequestContext.latency(YotsubaWorkload.TIMEOUT));
			var ingest = connection.getAsyncApi(RequestContext.ingest(YotsubaWorkload.TIMEOUT));
			var batch = connection.getAsyncApi(RequestContext.batch(YotsubaWorkload.TIMEOUT));
			var analytical = connection.getAsyncApi(RequestContext.analytical(YotsubaWorkload.TIMEOUT));
			var map = new LinkedHashMap<Kind, RocksDBAsyncAPI>();
			for (var kind : Kind.values()) {
				map.put(kind, switch (kind) {
					case UPDATE, ENSURE, HISTORY_PUT_MULTI, FILE_PUT_MULTI -> ingest;
					case GET, EXISTS_MULTI -> latency;
					case FIRST_LAST, RANGE_PAGE -> analytical;
					case MONITOR_TX -> batch;
				});
			}
			result.add(map);
		}
		return result;
	}

	private static boolean isWrite(Kind kind) {
		return switch (kind) {
			case UPDATE, ENSURE, HISTORY_PUT_MULTI, FILE_PUT_MULTI, MONITOR_TX -> true;
			case GET, EXISTS_MULTI, FIRST_LAST, RANGE_PAGE -> false;
		};
	}

	private Map<Kind, Double> mix() {
		var mix = YotsubaWorkload.productionMix();
		var only = option("--only", "");
		if (!only.isBlank()) {
			var keep = List.of(only.split(","));
			mix.replaceAll((kind, rate) -> keep.contains(kind.name()) ? rate : 0.0);
		}
		return mix;
	}

	private static Map<String, Long> mib(Map<String, Long> bytes) {
		var result = new LinkedHashMap<String, Long>();
		bytes.forEach((id, value) -> result.put(id, value >> 20));
		return result;
	}

	private void line(String text) {
		System.out.println(text);
		System.out.flush();
		report.append(text).append('\n');
	}

	private String option(String name, String fallback) {
		return options.getOrDefault(name, fallback);
	}

	private String required(String name) {
		var value = options.get(name);
		if (value == null) {
			throw new IllegalArgumentException("Missing " + name);
		}
		return value;
	}

	private List<String> list(String name, String fallback) {
		var value = option(name, fallback);
		return value.isBlank() ? List.of() : List.of(value.split(","));
	}

	private static List<String> splitArgs(String value) {
		return value.isBlank() ? List.of() : List.of(value.trim().split("\\s+"));
	}

}
