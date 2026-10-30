package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.distributed.LinearizabilityChecker.Operation;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Queue;
import java.util.Set;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * Network partitions on a three-node cluster of separate JVMs, under a register workload checked for
 * linearizability (design scenarios 5 and 10).
 *
 * <p>Every node gets its own {@link ClusterClient}, connected to that node only, so some clients keep talking to the
 * cut-off side for the whole fault. The test JVM reaches every node's API and Raft port directly; only node-to-node
 * traffic crosses the {@link Network}.</p>
 */
@Timeout(300)
class PartitionTest {

	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(30);
	private static final Duration POLL_INTERVAL = Duration.ofMillis(20);
	/**
	 * Nodes give up on a request after 3 seconds, well within a fault: the test also covers nodes whose Raft client
	 * ran out of retries while cut off.
	 */
	private static final List<String> SERVER_ARGS = List.of("--election-timeout-ms", "400:800",
			"--request-timeout-ms", "3000");
	/** One attempt of a register operation. */
	private static final RequestContext OP_CONTEXT = RequestContext.latency(Duration.ofSeconds(1));
	/** How long a register operation keeps retrying on its node: at most about 2 seconds per operation. */
	private static final Duration OP_RETRY = Duration.ofSeconds(1);
	private static final RequestContext SETUP_CONTEXT = RequestContext.batch(Duration.ofSeconds(5));
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final String COLUMN = "registers";
	private static final String HOT_COLUMN = "hot";
	private static final int KEYS = 3;
	private static final int WORKERS_PER_NODE = 2;
	private static final Duration HEALTHY = Duration.ofSeconds(2);
	/**
	 * How long each fault lasts: many election timeouts, and long enough for every client of the cut-off side to
	 * complete several operations.
	 */
	private static final Duration FAULT = Duration.ofSeconds(8);
	private static final Duration AFTER_HEAL = Duration.ofSeconds(2);

	/** Kept when a test fails, for the node logs. */
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;

	/**
	 * The leader is cut off from both followers. It must acknowledge nothing (no write and no read: a read could
	 * only be stale) while the followers elect a new leader and keep serving; after the heal it rejoins.
	 */
	@Test
	void isolatedLeaderAcknowledgesNothingWhileTheMajorityTakesOver() throws Exception {
		try (var cluster = ProcessCluster.start(root, 3, SERVER_ARGS); var probe = new RaftProbe(cluster)) {
			cluster.waitReady(READY_TIMEOUT);
			long column = createColumn(cluster);
			var oldLeader = probe.awaitLeader(cluster.nodeIds(), 0);
			var hot = hotCounters(cluster);
			var majority = others(cluster, oldLeader.id());

			try (var workload = new Workload(cluster, column)) {
				workload.letRun(HEALTHY);
				cluster.network().isolate(oldLeader.id());
				long isolatedAt = System.nanoTime();
				var newLeader = probe.awaitLeader(majority, oldLeader.term() + 1);
				long electedAt = System.nanoTime();
				assertNotEquals(oldLeader.id(), newLeader.id());
				workload.letRunUntil(isolatedAt + FAULT.toNanos());
				long healAt = System.nanoTime();
				cluster.network().heal();
				// Every node, including those whose Raft client gave up during the fault, serves writes again.
				long servedAt = workload.awaitServedByEveryNode(healAt);
				workload.letRun(AFTER_HEAL);
				workload.stop();
				var ops = workload.sent();
				checkHotCounters(cluster, hot);

				var isolated = during(ops, oldLeader.id()::equals, isolatedAt, healAt);
				assertNothingAcknowledged(isolated, "the isolated old leader " + oldLeader.id(), workload, isolatedAt);

				var majoritySide = during(ops, majority::contains, electedAt, healAt);
				assertTrue(count(majoritySide, Operation.Kind.PUT, OpStatus.OK) > 0,
						"no write acknowledged after electing " + newLeader + ": " + summary(majoritySide));
				assertTrue(count(majoritySide, Operation.Kind.GET, OpStatus.OK) > 0,
						"no read served after electing " + newLeader + ": " + summary(majoritySide));

				assertLinearizable(workload.history());
				long firstWriteAt = firstAcknowledgedWrite(ops, majority, isolatedAt);
				System.out.printf("PartitionTest isolated leader %s (term %d): new leader %s (term %d) after %d ms,"
								+ " first majority write after %d ms, every node served again %d ms after heal;"
								+ " isolated node ops %s; %d ops in total%n",
						oldLeader.id(), oldLeader.term(), newLeader.id(), newLeader.term(),
						millis(electedAt - isolatedAt), millis(firstWriteAt - isolatedAt), millis(servedAt - healAt), summary(isolated), ops.size());
			}
			assertReplicasIdentical(cluster, probe);
		}
	}

	/**
	 * The leader and one follower are cut off from the other follower. The leader keeps its majority, so service
	 * through the majority side must not be interrupted, and the cut-off follower must not disrupt it, then or
	 * after the heal (pre-vote).
	 */
	@Test
	void leaderWithAMajorityKeepsServingThroughAPartition() throws Exception {
		try (var cluster = ProcessCluster.start(root, 3, SERVER_ARGS); var probe = new RaftProbe(cluster)) {
			cluster.waitReady(READY_TIMEOUT);
			long column = createColumn(cluster);
			var leader = probe.awaitLeader(cluster.nodeIds(), 0);
			var hot = hotCounters(cluster);
			var followers = others(cluster, leader.id());
			var majority = Set.of(leader.id(), followers.getFirst());
			var minority = followers.getLast();

			try (var workload = new Workload(cluster, column)) {
				workload.letRun(HEALTHY);
				cluster.network().partition(majority, Set.of(minority));
				long partitionedAt = System.nanoTime();
				workload.letRun(FAULT);
				long healAt = System.nanoTime();
				cluster.network().heal();
				// The cut-off follower's Raft client gave up on its writes during the fault: it must serve them again.
				long servedAt = workload.awaitServedByEveryNode(healAt);
				workload.letRun(AFTER_HEAL);
				workload.stop();
				var ops = workload.sent();
				checkHotCounters(cluster, hot);

				var majoritySide = during(ops, majority::contains, partitionedAt, healAt);
				var unacknowledged = majoritySide.stream().filter(op -> op.status() != OpStatus.OK).toList();
				assertEquals(List.of(), unacknowledged, "the majority side " + majority + " must keep serving");
				assertTrue(count(majoritySide, Operation.Kind.PUT, OpStatus.OK) > 0, summary(majoritySide).toString());
				assertTrue(count(majoritySide, Operation.Kind.GET, OpStatus.OK) > 0, summary(majoritySide).toString());

				var cutOff = during(ops, minority::equals, partitionedAt, healAt);
				assertNothingAcknowledged(cutOff, "the cut-off follower " + minority, workload, partitionedAt);

				var leaderAfter = probe.awaitLeader(cluster.nodeIds(), 0);
				assertEquals(leader, leaderAfter, "the partition and the heal must not depose the leader");

				assertLinearizable(workload.history());
				System.out.printf("PartitionTest partition %s | %s: leader %s kept term %d; every node served again"
								+ " %d ms after heal; cut-off node ops %s; %d ops in total%n",
						majority, minority, leader.id(), leader.term(), millis(servedAt - healAt), summary(cutOff),
						ops.size());
			}
			assertReplicasIdentical(cluster, probe);
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Assertions

	/**
	 * A node cut off from the majority can neither commit a write nor confirm that a read is current: every write
	 * and read sent to it while cut off must fail or stay unknown. Each kind must have been tried at least once.
	 */
	private static void assertNothingAcknowledged(List<Sent> ops, String node, Workload workload, long from) {
		var tried = "operations sent to " + node + " while cut off: " + summary(ops) + "\n" + workload.progress(from);
		assertTrue(count(ops, Operation.Kind.PUT) > 0, tried);
		assertTrue(count(ops, Operation.Kind.GET) > 0, tried);
		var acknowledged = ops.stream().filter(op -> op.status() == OpStatus.OK).toList();
		assertEquals(List.of(), acknowledged, node + " acknowledged operations while cut off from the majority");
	}

	private static void assertLinearizable(History history) {
		var result = LinearizabilityChecker.check(history.operations());
		assertTrue(result.linearizable(), result.description());
	}

	/**
	 * Waits until every replica applied everything committed, stops the nodes and compares their databases: every
	 * user column, with its schema and contents, must be identical byte for byte.
	 *
	 * <p>The nodes stop gracefully: with the default {@code delay-wal-flush-duration}, a killed node loses the
	 * last seconds of its RocksDB WAL, which only a restart restores, by replaying the Raft log.</p>
	 */
	private static void assertReplicasIdentical(ProcessCluster cluster, RaftProbe probe) throws IOException {
		probe.awaitConverged(cluster.nodeIds());
		for (String id : cluster.nodeIds()) {
			cluster.stop(id);
		}
		var contents = new LinkedHashMap<String, Map<String, Map<String, String>>>();
		var schemas = new LinkedHashMap<String, Map<String, ColumnSchema>>();
		for (String id : cluster.nodeIds()) {
			try (var db = new EmbeddedConnection(cluster.dataDir(id).resolve("db"), "check-" + id, null)) {
				var api = db.getSyncApi(SETUP_CONTEXT);
				var definitions = new TreeMap<>(api.getAllColumnDefinitions());
				definitions.remove(NodeStore.META_COLUMN);
				schemas.put(id, definitions);
				var columns = new TreeMap<String, Map<String, String>>();
				for (String name : definitions.keySet()) {
					columns.put(name, columnContents(api, api.getColumnId(name)));
				}
				contents.put(id, columns);
			}
		}
		var first = cluster.nodeIds().getFirst();
		assertTrue(contents.get(first).containsKey(COLUMN), "column " + COLUMN + " missing on node " + first);
		for (String id : cluster.nodeIds()) {
			assertEquals(schemas.get(first), schemas.get(id), "column schemas of node " + id + " vs node " + first);
			assertEquals(contents.get(first), contents.get(id), "contents of node " + id + " vs node " + first);
		}
	}

	/** Every key and value of a column, hex-encoded, in key order. */
	private static Map<String, String> columnContents(RocksDBSyncAPI api, long columnId) {
		var hex = HexFormat.of();
		var result = new TreeMap<String, String>();
		try (Stream<KV> range = api.getRange(0, columnId, null, null, false, RequestType.<KV>allInRange())) {
			range.forEach(kv -> result.put(
					Stream.of(kv.keys().keys()).map(key -> hex.formatHex(key.toByteArray()))
							.collect(Collectors.joining("/")),
					kv.value() == null ? "null" : hex.formatHex(kv.value().toByteArray())));
		}
		return result;
	}

	// ---------------------------------------------------------------------------------------------------------
	// Helpers

	/** Workers updating two counters of a column of their own, through every node, through the whole test. */
	private static HotCounters hotCounters(ProcessCluster cluster) {
		long column;
		try (var client = new ClusterClient(cluster.apiAddresses(), WAIT_TIMEOUT, true)) {
			column = client.createColumn(HOT_COLUMN, SCHEMA, SETUP_CONTEXT).valueOrThrow();
		}
		var hot = new HotCounters(cluster.apiAddresses(), column, 0, 2, 3);
		hot.start();
		return hot;
	}

	private static void checkHotCounters(ProcessCluster cluster, HotCounters hot) throws InterruptedException {
		hot.stop();
		try (var client = new ClusterClient(cluster.apiAddresses(), WAIT_TIMEOUT, true)) {
			long column = client.getColumnId(HOT_COLUMN, SETUP_CONTEXT).valueOrThrow();
			hot.check(key -> client.get(column, HotCounters.key(key), SETUP_CONTEXT).valueOrThrow(), 1);
		}
	}

	private static long createColumn(ProcessCluster cluster) {
		// createColumn returns the existing id for the same schema, so retrying an ambiguous attempt is harmless.
		try (var client = new ClusterClient(cluster.apiAddresses(), WAIT_TIMEOUT, true)) {
			return client.createColumn(COLUMN, SCHEMA, SETUP_CONTEXT).valueOrThrow();
		}
	}

	private static List<String> others(ProcessCluster cluster, String id) {
		return cluster.nodeIds().stream().filter(other -> !other.equals(id)).toList();
	}

	/** Operations sent to the matching nodes, invoked at or after {@code from} and completed before {@code to}. */
	private static List<Sent> during(List<Sent> ops, Predicate<String> nodes, long from, long to) {
		return ops.stream()
				.filter(op -> nodes.test(op.node()) && op.invokeNanos() - from >= 0 && op.completeNanos() - to < 0)
				.toList();
	}

	private static long count(List<Sent> ops, Operation.Kind kind) {
		return ops.stream().filter(op -> op.kind() == kind).count();
	}

	private static long count(List<Sent> ops, Operation.Kind kind, OpStatus status) {
		return ops.stream().filter(op -> op.kind() == kind && op.status() == status).count();
	}

	/** Operation counts by kind and status, e.g. {@code {PUT UNKNOWN=4, GET FAILED=3}}. */
	private static Map<String, Long> summary(List<Sent> ops) {
		return ops.stream().collect(Collectors.groupingBy(op -> op.kind() + " " + op.status(), TreeMap::new,
				Collectors.counting()));
	}

	/** Completion time of the first acknowledged write sent to {@code nodes} at or after {@code from}. */
	private static long firstAcknowledgedWrite(List<Sent> ops, Collection<String> nodes, long from) {
		return ops.stream()
				.filter(op -> nodes.contains(op.node()) && op.kind() == Operation.Kind.PUT
						&& op.status() == OpStatus.OK && op.invokeNanos() - from >= 0)
				.mapToLong(Sent::completeNanos)
				.min()
				.orElseThrow();
	}

	private static long millis(long nanos) {
		return TimeUnit.NANOSECONDS.toMillis(nanos);
	}

	private static Keys key(int register) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(register).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static @Nullable String text(@Nullable Buf value) {
		return value == null ? null : new String(value.toByteArray(), StandardCharsets.UTF_8);
	}

	/** Polls {@code condition} until it holds; fails after {@code timeout}. */
	private static void await(Duration timeout, String what, BooleanSupplier condition) {
		await(timeout, () -> what, condition);
	}

	private static void await(Duration timeout, Supplier<String> what, BooleanSupplier condition) {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() - deadline > 0) {
				throw new AssertionError("Not reached within " + timeout + ": " + what.get());
			}
			pause(POLL_INTERVAL);
		}
	}

	private static void pause(Duration duration) {
		try {
			Thread.sleep(duration);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError("Interrupted", e);
		}
	}

	/**
	 * One operation as seen by its client.
	 *
	 * @param node the only node its client talks to
	 * @param invokeNanos taken before the operation was recorded in the {@link History}
	 * @param completeNanos taken after its completion was recorded there
	 */
	private record Sent(String node, Operation.Kind kind, long invokeNanos, long completeNanos, OpStatus status) {}

	// ---------------------------------------------------------------------------------------------------------
	// Workload

	/**
	 * {@value #WORKERS_PER_NODE} client threads per node doing puts of unique values and gets, alternately, on
	 * {@value #KEYS} registers, back to back. All workers of a node share one {@link ClusterClient} connected to that
	 * node only, which never retries an ambiguous write (a register history cannot tell a duplicate apart).
	 */
	private static final class Workload implements AutoCloseable {

		private static final Duration STOP_TIMEOUT = Duration.ofSeconds(30);

		private final History history = new History();
		private final Queue<Sent> sent = new ConcurrentLinkedQueue<>();
		private final Queue<Throwable> crashes = new ConcurrentLinkedQueue<>();
		/** Invocation time of each worker's current operation. */
		private final Map<String, Long> inFlight = new ConcurrentHashMap<>();
		private final Map<String, ClusterClient> clients = new LinkedHashMap<>();
		private final List<Thread> workers = new ArrayList<>();
		private final long column;
		private volatile boolean running = true;

		Workload(ProcessCluster cluster, long column) {
			this.column = column;
			for (String id : cluster.nodeIds()) {
				clients.put(id, new ClusterClient(Map.of(id, cluster.apiAddress(id)), OP_RETRY, false));
			}
			clients.forEach((id, client) -> {
				for (int i = 0; i < WORKERS_PER_NODE; i++) {
					String worker = "n" + id + "w" + i;
					workers.add(Thread.ofPlatform().name("partition-test-" + worker).start(() -> {
						try {
							work(worker, id, client);
						} catch (Throwable e) {
							crashes.add(e);
						}
					}));
				}
			});
		}

		private void work(String worker, String node, ClusterClient client) {
			var random = ThreadLocalRandom.current();
			long sequence = 0;
			// Puts and gets alternate, so that any two operations in a row try both.
			boolean put = random.nextBoolean();
			while (running) {
				int register = random.nextInt(KEYS);
				String name = "k" + register;
				long invoke = System.nanoTime();
				inFlight.put(worker, invoke);
				Operation.Kind kind;
				OpStatus status;
				put = !put;
				if (put) {
					kind = Operation.Kind.PUT;
					String value = worker + "-" + sequence++;
					var pending = history.invokePut(name, value);
					var result = client.put(column, key(register), text(value), OP_CONTEXT);
					status = result.status();
					pending.complete(status);
				} else {
					kind = Operation.Kind.GET;
					var pending = history.invokeGet(name);
					var result = client.get(column, key(register), OP_CONTEXT);
					status = result.status();
					pending.complete(status, result.acknowledged() ? text(result.value()) : null);
				}
				sent.add(new Sent(node, kind, invoke, System.nanoTime(), status));
				inFlight.remove(worker);
			}
		}

		void letRun(Duration duration) {
			letRunUntil(System.nanoTime() + duration.toNanos());
		}

		/** Lets the workers run until {@code deadlineNanos}: the length of a phase, not a synchronization. */
		void letRunUntil(long deadlineNanos) {
			long remaining = deadlineNanos - System.nanoTime();
			if (remaining > 0) {
				pause(Duration.ofNanos(remaining));
			}
		}

		/**
		 * Waits until the clients of every node got a write and a read acknowledged, both sent at or after
		 * {@code from}; returns when the last of them completed.
		 */
		long awaitServedByEveryNode(long from) {
			await(WAIT_TIMEOUT, () -> "every node acknowledges a write and a read again\n" + progress(from),
					() -> clients.keySet().stream().allMatch(id -> Stream.of(Operation.Kind.PUT, Operation.Kind.GET)
							.allMatch(kind -> firstAcknowledged(id, kind, from).isPresent())));
			return clients.keySet().stream()
					.flatMap(id -> Stream.of(Operation.Kind.PUT, Operation.Kind.GET)
							.map(kind -> firstAcknowledged(id, kind, from)))
					.mapToLong(Optional::orElseThrow)
					.max()
					.orElseThrow();
		}

		/** What each node's clients did since {@code from}, and what they are waiting for now. */
		String progress(long from) {
			var text = new StringBuilder();
			for (String id : clients.keySet()) {
				var ops = sent.stream().filter(op -> op.node().equals(id) && op.invokeNanos() - from >= 0).toList();
				text.append("node ").append(id).append(": ").append(summary(ops)).append(", last:");
				for (var op : ops.subList(Math.max(0, ops.size() - 4), ops.size())) {
					long took = millis(op.completeNanos() - op.invokeNanos());
					text.append(' ').append(op.kind()).append(' ').append(op.status()).append(" in ").append(took)
							.append("ms");
				}
				inFlight.forEach((worker, since) -> {
					if (worker.startsWith("n" + id + "w")) {
						long waiting = millis(System.nanoTime() - since);
						text.append(", ").append(worker).append(" waiting ").append(waiting).append("ms");
					}
				});
				text.append('\n');
			}
			return text.toString();
		}

		private Optional<Long> firstAcknowledged(String node, Operation.Kind kind, long from) {
			return sent.stream()
					.filter(op -> op.node().equals(node) && op.kind() == kind && op.status() == OpStatus.OK
							&& op.invokeNanos() - from >= 0)
					.map(Sent::completeNanos)
					.min(Long::compare);
		}

		/** Stops the workers after their current operation; fails if one of them crashed. */
		void stop() throws InterruptedException {
			running = false;
			for (var worker : workers) {
				if (!worker.join(STOP_TIMEOUT)) {
					throw new AssertionError("Worker " + worker.getName() + " still running after " + STOP_TIMEOUT);
				}
			}
			if (!crashes.isEmpty()) {
				var error = new AssertionError("A workload thread crashed", crashes.peek());
				crashes.stream().skip(1).forEach(error::addSuppressed);
				throw error;
			}
		}

		List<Sent> sent() {
			return List.copyOf(sent);
		}

		History history() {
			return history;
		}

		@Override
		public void close() {
			running = false;
			clients.values().forEach(ClusterClient::close);
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Raft state

	/** Asks nodes for their Raft role and log state on their Raft ports, bypassing the network's link proxies. */
	private static final class RaftProbe implements AutoCloseable {

		private static final RaftGroupId GROUP_ID = RaftGroupId.valueOf(UUID.fromString(NodeConfig.DEFAULT_GROUP_ID));

		private final RaftClient client;

		/**
		 * One node's view.
		 *
		 * @param term the leader's term; -1 for other roles, whose term the reply does not carry
		 */
		record Role(String id, RaftPeerRole role, long term) {}

		RaftProbe(ProcessCluster cluster) {
			var peers = cluster.nodeIds().stream()
					.map(id -> RaftPeer.newBuilder()
							.setId(id)
							.setAddress(cluster.raftAddress(id).host() + ":" + cluster.raftAddress(id).port())
							.build())
					.toList();
			var properties = new RaftProperties();
			RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(1, TimeUnit.SECONDS));
			this.client = RaftClient.newBuilder()
					.setClientId(ClientId.randomId())
					.setRaftGroup(RaftGroup.valueOf(GROUP_ID, peers))
					.setProperties(properties)
					.setRetryPolicy(RetryPolicies.noRetry())
					.build();
		}

		private record Info(Role role, long applied, long committed) {}

		private Optional<Info> info(String id) {
			try {
				var reply = client.getGroupManagementApi(RaftPeerId.valueOf(id)).info(GROUP_ID);
				var roleInfo = reply.getRoleInfoProto();
				long term = roleInfo.hasLeaderInfo() ? roleInfo.getLeaderInfo().getTerm() : -1;
				var log = reply.getLogInfoProto();
				return Optional.of(new Info(new Role(id, roleInfo.getRole(), term), log.getApplied().getIndex(),
						log.getCommitted().getIndex()));
			} catch (IOException | RuntimeException e) {
				return Optional.empty();
			}
		}

		/** Waits until one of {@code candidates} reports to lead with a term of at least {@code minTerm}. */
		Role awaitLeader(Collection<String> candidates, long minTerm) {
			var leader = new Role[1];
			await(WAIT_TIMEOUT, "a leader among " + candidates + " with term >= " + minTerm, () -> {
				for (String id : candidates) {
					var info = info(id);
					if (info.isPresent() && info.get().role().role() == RaftPeerRole.LEADER
							&& info.get().role().term() >= minTerm) {
						leader[0] = info.get().role();
						return true;
					}
				}
				return false;
			});
			return leader[0];
		}

		/** Waits until every node applied the same index, which is also what each of them knows as committed. */
		void awaitConverged(List<String> ids) {
			await(WAIT_TIMEOUT, "nodes " + ids + " applied the same log", () -> {
				var indexes = new ArrayList<Long>();
				for (String id : ids) {
					var info = info(id);
					if (info.isEmpty() || info.get().applied() != info.get().committed()) {
						return false;
					}
					indexes.add(info.get().applied());
				}
				return indexes.stream().distinct().count() == 1;
			});
		}

		@Override
		public void close() throws IOException {
			client.close();
		}
	}
}
