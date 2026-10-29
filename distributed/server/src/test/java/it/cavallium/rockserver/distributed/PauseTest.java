package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.IntStream;
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
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.util.TimeDuration;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * Process pauses (SIGSTOP/SIGCONT) of a three-node cluster under a register workload.
 *
 * <p>Clients (one per node, plus one that fails over across all nodes) keep putting and getting a few registers
 * while the leader is frozen for three maximum election timeouts and then continued, and later while a follower is
 * frozen. The frozen leader wakes up still believing it leads its old term, with requests queued in its sockets:
 * it must neither serve a stale read nor acknowledge a write in that term. The whole history must be
 * linearizable, the follower pause may cost at most a short blip, and the replicas must end up identical.</p>
 */
@Timeout(300)
class PauseTest {

	private static final Duration ELECTION_TIMEOUT_MIN = Duration.ofMillis(400);
	private static final Duration ELECTION_TIMEOUT_MAX = Duration.ofMillis(800);
	private static final Duration PAUSE = ELECTION_TIMEOUT_MAX.multipliedBy(3);
	private static final List<String> SERVER_ARGS = List.of(
			"--election-timeout-ms", ELECTION_TIMEOUT_MIN.toMillis() + ":" + ELECTION_TIMEOUT_MAX.toMillis(),
			"--request-timeout-ms", "3000");
	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration WAIT_TIMEOUT = Duration.ofSeconds(30);
	/** One attempt of a workload operation. */
	private static final Duration ATTEMPT_TIMEOUT = Duration.ofSeconds(1);
	private static final RequestContext ATTEMPT = RequestContext.latency(ATTEMPT_TIMEOUT);
	private static final RequestContext SETUP = RequestContext.batch(Duration.ofSeconds(10));
	/** How long one workload call of the failover client keeps retrying. */
	private static final Duration FAILOVER_RETRY = Duration.ofSeconds(3);
	/**
	 * Longest stretch without an acknowledged operation, during the follower pause, for clients of the nodes that
	 * keep running: about one Raft round trip, plus scheduling noise.
	 */
	private static final Duration RUNNING_NODE_BLIP = Duration.ofMillis(1500);
	/**
	 * The same for the failover client, which may sit on the frozen follower for one attempt per operation until a
	 * read moves it to another node.
	 */
	private static final Duration FAILOVER_BLIP = ATTEMPT_TIMEOUT.multipliedBy(3);
	private static final int REGISTERS = 6;
	/** Operations sent straight to the old leader the moment it is continued, while it still believes it leads. */
	private static final int RESUME_BURST = 16;
	private static final String FAILOVER_CLIENT = "any";
	private static final String COLUMN = "registers";
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

	// Kept on failure: the node logs and data directories are the evidence.
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private ProcessCluster cluster;
	private RaftProbe probe;
	private final List<ClusterClient> clients = new ArrayList<>();
	private final ExecutorService executor = Executors.newVirtualThreadPerTaskExecutor();

	@AfterEach
	void tearDown() {
		executor.shutdownNow();
		clients.forEach(ClusterClient::close);
		if (probe != null) {
			probe.close();
		}
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void pausedLeaderAndFollowerKeepTheHistoryLinearizable() throws Exception {
		long testStart = System.nanoTime();
		cluster = ProcessCluster.start(root, 3, SERVER_ARGS);
		cluster.waitReady(READY_TIMEOUT);
		probe = new RaftProbe(cluster);
		var history = new History();

		var setup = client(cluster.apiAddresses(), WAIT_TIMEOUT, true);
		var created = setup.createColumn(COLUMN, SCHEMA, SETUP);
		assertTrue(created.acknowledged(), () -> "createColumn: " + created.error());
		long columnId = created.value();

		var workers = new LinkedHashMap<String, Worker>();
		for (var id : cluster.nodeIds()) {
			// Retries stay on the node, within about one attempt: the handshake waits at most the retry time.
			var client = client(Map.of(id, cluster.apiAddress(id)), ATTEMPT_TIMEOUT, false);
			workers.put(id, new Worker(id, client, columnId, history));
		}
		workers.put(FAILOVER_CLIENT, new Worker(FAILOVER_CLIENT, client(cluster.apiAddresses(), FAILOVER_RETRY, false),
				columnId, history));
		var running = new ArrayList<Future<?>>();
		workers.values().forEach(worker -> running.add(executor.submit(worker)));
		long warmup = System.nanoTime();
		for (var worker : workers.values()) {
			awaitCondition("first acknowledged operation of client " + worker.name,
					() -> worker.acknowledgedSince(warmup, false));
		}

		// ---- Freeze the leader for three maximum election timeouts, and until the running nodes have overwritten
		// every register: from then on, any read the old leader serves from its own state is detectably stale.
		var firstLeader = probe.awaitLeader(null);
		String oldLeader = firstLeader.id();
		long leaderPausedAt = System.nanoTime();
		cluster.pause(oldLeader);
		long leaderPauseEnd = leaderPausedAt + PAUSE.toNanos();
		var newLeader = probe.awaitLeaderUntil(oldLeader, leaderPauseEnd);
		assertNotNull(newLeader, "no leader elected among the running nodes while node " + oldLeader + " was frozen");
		assertTrue(newLeader.term() > firstLeader.term(), () -> "new leader " + newLeader + " after " + firstLeader);
		long electedAt = System.nanoTime();
		holdUntil(leaderPauseEnd);
		var survivors = workers.values().stream().filter(worker -> !worker.name.equals(oldLeader)).toList();
		long readFailoverAt = firstAcknowledgedAfter(survivors, leaderPausedAt, false);
		long writeFailoverAt = firstAcknowledgedAfter(survivors, leaderPausedAt, true);
		awaitCondition("every register overwritten through the running nodes", () -> IntStream.range(0, REGISTERS)
				.allMatch(register -> survivors.stream().anyMatch(w -> w.putAcknowledgedSince(leaderPausedAt, register))));

		// ---- Continue it, and hit it at once while it may still believe it leads its old term.
		cluster.resume(oldLeader);
		long leaderResumedAt = System.nanoTime();
		var burst = new ArrayList<Future<?>>();
		var oldLeaderWorker = workers.get(oldLeader);
		for (int i = 0; i < RESUME_BURST; i++) {
			boolean put = i % 2 == 0;
			int register = i % REGISTERS;
			burst.add(executor.submit(() -> oldLeaderWorker.operation(put, register, "burst")));
		}
		var stepDown = probe.awaitRole(oldLeader, RaftPeerRole.FOLLOWER, newLeader.term());
		long steppedDownAt = System.nanoTime();
		for (var future : burst) {
			future.get(WAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		}
		awaitCondition("acknowledged operation through the continued old leader",
				() -> oldLeaderWorker.acknowledgedSince(leaderResumedAt, false));
		long oldLeaderServingAt = oldLeaderWorker.firstAcknowledgedSince(leaderResumedAt, false);

		// ---- Freeze a follower: the leader and the other follower still make a majority.
		var leaderBefore = probe.awaitLeader(null);
		// A follower that was never frozen: of three nodes, at most the leader and the old leader are excluded.
		String follower = cluster.nodeIds().stream()
				.filter(id -> !id.equals(leaderBefore.id()) && !id.equals(oldLeader))
				.findFirst()
				.orElseThrow();
		long followerPausedAt = System.nanoTime();
		cluster.pause(follower);
		holdUntil(followerPausedAt + PAUSE.toNanos());
		long followerResumeAt = System.nanoTime();
		cluster.resume(follower);
		var followerWorker = workers.get(follower);
		awaitCondition("acknowledged operation through the continued follower",
				() -> followerWorker.acknowledgedSince(followerResumeAt, false));
		long followerServingAt = followerWorker.firstAcknowledgedSince(followerResumeAt, false);
		var leaderAfter = probe.awaitLeader(null);
		// One more blip bound past the pause, so that a stall right after it shows up too.
		long blipWindowEnd = followerResumeAt + RUNNING_NODE_BLIP.toNanos();
		holdUntil(blipWindowEnd);

		for (var worker : workers.values()) {
			worker.stop();
		}
		for (var future : running) {
			future.get(WAIT_TIMEOUT.toSeconds(), TimeUnit.SECONDS);
		}

		// ---- Final reads, recorded too: the values every replica must hold.
		var finalValues = new TreeMap<Long, String>();
		for (int register = 0; register < REGISTERS; register++) {
			var get = history.invokeGet(registerName(register));
			var read = setup.get(columnId, key(register), SETUP);
			get.complete(read.status(), read.acknowledged() ? text(read.value()) : null);
			assertTrue(read.acknowledged(), () -> "final read: " + read.error());
			finalValues.put((long) register, text(read.value()));
		}

		var operations = history.operations();
		var result = LinearizabilityChecker.check(operations);
		var stats = new StringBuilder();
		workers.values().forEach(worker -> stats.append("\n  ").append(worker.summary()));

		// ---- Every replica must converge to the same data.
		var converged = probe.awaitConverged();
		// A node buffers its RocksDB WAL (delay-wal-flush-duration, 5 s by default) and relies on Raft replay after a
		// crash, so a killed node's database may lack its last applied entries until it restarts: flush the WALs
		// (a local command on each node), then kill. A graceful stop also flushes, but its leadership hand-offs
		// between nodes that are all going down take several seconds.
		long stopStart = System.nanoTime();
		for (var id : cluster.nodeIds()) {
			setup.api(id, SETUP).flushWal();
		}
		cluster.nodeIds().forEach(cluster::kill);
		long stopped = System.nanoTime();
		var replicas = new LinkedHashMap<String, Map<String, Map<String, String>>>();
		for (var id : cluster.nodeIds()) {
			replicas.put(id, readReplica(cluster.dataDir(id).resolve("db"), id));
		}

		System.out.printf("""
				PauseTest measurements (cluster in %s):
				  leader %s (term %d) frozen for %d ms (at least %d); new leader %s (term %d) elected after %d ms
				  first operation (put) sent through a running node after the freeze acknowledged after %d (%d) ms
				  old leader stepped down %d ms after being continued (%s), served again after %d ms
				  follower %s frozen for %d ms; leader before %s, after %s; it served again %d ms after
				  max gap without an acknowledged operation during the follower pause:%s
				  operations: %d (%s), converged at applied index %d, stopped in %d ms, total %d ms
				  clients:%s
				""",
				root, oldLeader, firstLeader.term(), millis(leaderResumedAt - leaderPausedAt), PAUSE.toMillis(),
				newLeader.id(), newLeader.term(),
				millis(electedAt - leaderPausedAt),
				millis(readFailoverAt - leaderPausedAt), millis(writeFailoverAt - leaderPausedAt),
				millis(steppedDownAt - leaderResumedAt), stepDown, millis(oldLeaderServingAt - leaderResumedAt),
				follower, PAUSE.toMillis(), leaderBefore, leaderAfter, millis(followerServingAt - followerResumeAt),
				gaps(workers, followerPausedAt, blipWindowEnd),
				operations.size(), countByStatus(operations), converged, millis(stopped - stopStart),
				millis(System.nanoTime() - testStart),
				stats);

		assertTrue(result.linearizable(), result.description());

		assertEquals(leaderBefore.id(), leaderAfter.id(), "leadership moved while a follower was frozen");
		assertEquals(leaderBefore.term(), leaderAfter.term(), "an election ran while a follower was frozen");
		for (var worker : workers.values()) {
			if (worker.name.equals(follower)) {
				continue;
			}
			var bound = worker.name.equals(FAILOVER_CLIENT) ? FAILOVER_BLIP : RUNNING_NODE_BLIP;
			long gap = worker.maxGapWithoutOk(followerPausedAt, blipWindowEnd);
			assertTrue(gap <= bound.toNanos(), () -> "client " + worker.name + " had no acknowledged operation for "
					+ millis(gap) + " ms while follower " + follower + " was frozen (bound " + bound.toMillis() + " ms)");
		}

		var reference = replicas.get(cluster.nodeIds().getFirst());
		for (var entry : replicas.entrySet()) {
			assertEquals(reference, entry.getValue(), "replica " + entry.getKey() + " differs from replica "
					+ cluster.nodeIds().getFirst());
		}
		var registers = reference.get(COLUMN + " #" + columnId);
		assertNotNull(registers, "column " + COLUMN + " #" + columnId + " missing: " + reference.keySet());
		var expected = new TreeMap<String, String>();
		finalValues.forEach((register, value) -> {
			if (value != null) {
				expected.put(HexFormat.of().formatHex(key(register).keys()[0].toByteArray()),
						HexFormat.of().formatHex(value.getBytes(StandardCharsets.UTF_8)));
			}
		});
		assertEquals(expected, registers, "replica contents differ from the final linearizable reads");
	}

	/**
	 * A client looping over random puts and gets on the shared registers, recording each one into the history.
	 * Every put writes a value never written before, so a read tells which put it observed.
	 */
	private static final class Worker implements Runnable {

		final String name;
		private final ClusterClient client;
		private final long columnId;
		private final History history;
		private final AtomicLong sequence = new AtomicLong();
		private final ConcurrentLinkedQueue<Ack> acknowledged = new ConcurrentLinkedQueue<>();
		private final Map<String, AtomicLong> statusCounts = new ConcurrentHashMap<>();
		private volatile boolean stopped;

		Worker(String name, ClusterClient client, long columnId, History history) {
			this.name = name;
			this.client = client;
			this.columnId = columnId;
			this.history = history;
		}

		@Override
		public void run() {
			var random = ThreadLocalRandom.current();
			while (!stopped && !Thread.currentThread().isInterrupted()) {
				operation(random.nextBoolean(), random.nextInt(REGISTERS), "op");
			}
		}

		void stop() {
			stopped = true;
		}

		void operation(boolean put, int register, String label) {
			String key = registerName(register);
			if (put) {
				String value = name + "-" + label + "-" + sequence.incrementAndGet();
				long invoked = System.nanoTime();
				var pending = history.invokePut(key, value);
				var result = client.put(columnId, key(register), text(value), ATTEMPT);
				pending.complete(result.status());
				record(true, register, invoked, result);
			} else {
				long invoked = System.nanoTime();
				var pending = history.invokeGet(key);
				var result = client.get(columnId, key(register), ATTEMPT);
				pending.complete(result.status(), result.acknowledged() ? text(result.value()) : null);
				record(false, register, invoked, result);
			}
		}

		private void record(boolean put, int register, long invoked, ClusterClient.Result<?> result) {
			if (result.acknowledged()) {
				acknowledged.add(new Ack(put, register, invoked, System.nanoTime()));
			}
			var outcome = (put ? "put " : "get ") + result.status()
					+ (result.error() != null ? " " + result.error().getClass().getSimpleName() : "");
			statusCounts.computeIfAbsent(outcome, _ -> new AtomicLong()).incrementAndGet();
		}

		/** Whether an operation sent at or after {@code sinceNanos} was acknowledged (only puts if asked). */
		boolean acknowledgedSince(long sinceNanos, boolean putsOnly) {
			return sentSince(sinceNanos, putsOnly).findAny().isPresent();
		}

		/** When the first acknowledged operation sent at or after {@code sinceNanos} completed. */
		long firstAcknowledgedSince(long sinceNanos, boolean putsOnly) {
			return sentSince(sinceNanos, putsOnly).mapToLong(Ack::completed).min().orElseThrow();
		}

		/** Whether a put to {@code register} sent at or after {@code sinceNanos} was acknowledged. */
		boolean putAcknowledgedSince(long sinceNanos, int register) {
			return sentSince(sinceNanos, true).anyMatch(ack -> ack.register() == register);
		}

		private Stream<Ack> sentSince(long sinceNanos, boolean putsOnly) {
			return acknowledged.stream().filter(ack -> ack.invoked() - sinceNanos >= 0 && (ack.put() || !putsOnly));
		}

		/** Longest stretch of {@code [from, to]} without an acknowledged operation completing. */
		long maxGapWithoutOk(long from, long to) {
			var times = Stream.concat(Stream.of(from, to), acknowledged.stream()
					.map(Ack::completed)
					.filter(time -> time - from > 0 && time - to < 0)).sorted().toList();
			long max = 0;
			for (int i = 1; i < times.size(); i++) {
				max = Math.max(max, times.get(i) - times.get(i - 1));
			}
			return max;
		}

		String summary() {
			return name + ": " + new TreeMap<>(statusCounts);
		}

		private record Ack(boolean put, int register, long invoked, long completed) {}
	}

	/** When the first operation sent through one of {@code workers} at or after {@code sinceNanos} was acknowledged. */
	private static long firstAcknowledgedAfter(List<Worker> workers, long sinceNanos, boolean putsOnly) {
		awaitCondition("acknowledged " + (putsOnly ? "put" : "operation") + " through a running node",
				() -> workers.stream().anyMatch(worker -> worker.acknowledgedSince(sinceNanos, putsOnly)));
		return workers.stream()
				.filter(worker -> worker.acknowledgedSince(sinceNanos, putsOnly))
				.mapToLong(worker -> worker.firstAcknowledgedSince(sinceNanos, putsOnly))
				.min()
				.orElseThrow();
	}

	/** Role, term and log position of one node, as its Raft server reports them. */
	private record NodeState(String id, RaftPeerRole role, long term, long committed, long applied) {
		@Override
		public String toString() {
			return id + " " + role + " term " + term + " committed " + committed + " applied " + applied;
		}
	}

	/**
	 * Asks each node's Raft server for its state, straight over its Raft port: role and log position through the
	 * Ratis group info, the current term through {@link TermService} (the group info only has the term of the last
	 * log entry, which lags behind a fresh election).
	 */
	private static final class RaftProbe implements AutoCloseable {

		private static final Duration CALL_TIMEOUT = Duration.ofMillis(500);

		private final RaftGroupId groupId = RaftGroupId.valueOf(UUID.fromString(NodeConfig.DEFAULT_GROUP_ID));
		private final List<String> ids;
		private final RaftClient client;
		private final Map<String, ManagedChannel> termChannels = new LinkedHashMap<>();

		RaftProbe(ProcessCluster cluster) {
			this.ids = cluster.nodeIds();
			for (var id : ids) {
				var address = cluster.raftAddress(id);
				termChannels.put(id, NettyChannelBuilder.forAddress(address.host(), address.port()).usePlaintext().build());
			}
			var peers = ids.stream()
					.map(id -> RaftPeer.newBuilder()
							.setId(id)
							.setAddress(cluster.raftAddress(id).host() + ":" + cluster.raftAddress(id).port())
							.build())
					.toList();
			var properties = new RaftProperties();
			RaftClientConfigKeys.Rpc.setRequestTimeout(properties,
					TimeDuration.valueOf(CALL_TIMEOUT.toMillis(), TimeUnit.MILLISECONDS));
			this.client = RaftClient.newBuilder()
					.setClientId(ClientId.randomId())
					.setRaftGroup(RaftGroup.valueOf(groupId, peers))
					.setProperties(properties)
					.setRetryPolicy(RetryPolicies.noRetry())
					.build();
		}

		/** The node's state, null if it does not answer in time. */
		@Nullable NodeState state(String id) {
			try {
				long termBefore = term(id);
				var info = client.getGroupManagementApi(RaftPeerId.valueOf(id)).info(groupId);
				long termAfter = term(id);
				if (termBefore != termAfter) {
					// The role may belong to either term.
					return null;
				}
				var log = info.getLogInfoProto();
				return new NodeState(id, info.getRoleInfoProto().getRole(), termAfter,
						log.getCommitted().getIndex(), log.getApplied().getIndex());
			} catch (IOException | RuntimeException | ExecutionException | TimeoutException e) {
				return null;
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new IllegalStateException(e);
			}
		}

		private long term(String id) throws ExecutionException, TimeoutException, InterruptedException {
			return TermService.currentTerm(termChannels.get(id), groupId, CALL_TIMEOUT)
					.get(CALL_TIMEOUT.toMillis() * 2, TimeUnit.MILLISECONDS);
		}

		/** The leader of the highest term among the nodes other than {@code excluded}, or null. */
		@Nullable NodeState leader(@Nullable String excluded) {
			NodeState best = null;
			for (var id : ids) {
				if (id.equals(excluded)) {
					continue;
				}
				var state = state(id);
				if (state != null && state.role() == RaftPeerRole.LEADER && (best == null || state.term() > best.term())) {
					best = state;
				}
			}
			return best;
		}

		NodeState awaitLeader(@Nullable String excluded) {
			var leader = awaitLeaderUntil(excluded, System.nanoTime() + WAIT_TIMEOUT.toNanos());
			assertNotNull(leader, "no leader within " + WAIT_TIMEOUT);
			return leader;
		}

		@Nullable NodeState awaitLeaderUntil(@Nullable String excluded, long deadlineNanos) {
			return poll(() -> leader(excluded), deadlineNanos);
		}

		/** Waits until the node is in {@code role} in a term of at least {@code minTerm}. */
		NodeState awaitRole(String id, RaftPeerRole role, long minTerm) {
			var state = poll(() -> {
				var current = state(id);
				return current != null && current.role() == role && current.term() >= minTerm ? current : null;
			}, System.nanoTime() + WAIT_TIMEOUT.toNanos());
			assertNotNull(state, "node " + id + " not " + role + " in term >= " + minTerm + ": " + state(id));
			return state;
		}

		/** Waits until every node has applied the leader's last committed entry; returns that index. */
		long awaitConverged() {
			var states = new ArrayList<NodeState>();
			var index = poll(() -> {
				states.clear();
				for (var id : ids) {
					var state = state(id);
					if (state == null) {
						return null;
					}
					states.add(state);
				}
				long committed = states.stream().mapToLong(NodeState::committed).max().orElseThrow();
				return states.stream().allMatch(state -> state.applied() == committed) ? committed : null;
			}, System.nanoTime() + WAIT_TIMEOUT.toNanos());
			assertNotNull(index, "replicas did not converge: " + states);
			return index;
		}

		private static <T> @Nullable T poll(Supplier<@Nullable T> probe, long deadlineNanos) {
			while (true) {
				var value = probe.get();
				if (value != null || System.nanoTime() - deadlineNanos >= 0) {
					return value;
				}
				pause();
			}
		}

		@Override
		public void close() {
			termChannels.values().forEach(ManagedChannel::shutdownNow);
			try {
				client.close();
			} catch (IOException e) {
				throw new IllegalStateException(e);
			}
		}
	}

	/** Every user column of a stopped node's database: "name #id" to (hex key to hex value). */
	private static Map<String, Map<String, String>> readReplica(Path db, String id) throws IOException {
		var columns = new TreeMap<String, Map<String, String>>();
		try (var connection = new EmbeddedConnection(db, "pause-test-replica-" + id, null)) {
			var api = connection.getSyncApi(SETUP);
			for (var name : api.getAllColumnDefinitions().keySet()) {
				if (name.equals(NodeStore.META_COLUMN)) {
					continue;
				}
				var contents = new TreeMap<String, String>();
				long columnId = api.getColumnId(name);
				try (Stream<KV> range = api.getRange(0, columnId, null, null, false, RequestType.<KV>allInRange())) {
					range.forEach(kv -> contents.put(hex(kv.keys()), kv.value() == null ? null
							: HexFormat.of().formatHex(kv.value().toByteArray())));
				}
				// Column ids must match too.
				columns.put(name + " #" + columnId, contents);
			}
		}
		return columns;
	}

	private static String hex(Keys keys) {
		var parts = new ArrayList<String>();
		for (Buf key : keys.keys()) {
			parts.add(HexFormat.of().formatHex(key.toByteArray()));
		}
		return String.join("/", parts);
	}

	private ClusterClient client(Map<String, HostAndPort> addresses,
			Duration retryTimeout,
			boolean retryAmbiguousWrites) {
		var client = new ClusterClient(new LinkedHashMap<>(addresses), retryTimeout, retryAmbiguousWrites);
		clients.add(client);
		return client;
	}

	private static String gaps(Map<String, Worker> workers, long from, long to) {
		var result = new StringBuilder();
		workers.values().forEach(worker -> result.append(' ').append(worker.name).append('=')
				.append(millis(worker.maxGapWithoutOk(from, to))).append("ms"));
		return result.toString();
	}

	private static Map<OpStatus, Long> countByStatus(List<LinearizabilityChecker.Operation> operations) {
		var counts = new TreeMap<OpStatus, Long>();
		operations.forEach(op -> counts.merge(op.status(), 1L, Long::sum));
		return counts;
	}

	private static void awaitCondition(String what, BooleanSupplier condition) {
		long deadline = System.nanoTime() + WAIT_TIMEOUT.toNanos();
		while (!condition.getAsBoolean()) {
			assertFalse(System.nanoTime() - deadline >= 0, "timed out waiting for " + what);
			pause();
		}
	}

	/** Keeps the current fault in place until {@code deadlineNanos}: the scenario's timing, not a sync point. */
	private static void holdUntil(long deadlineNanos) throws InterruptedException {
		long remaining = deadlineNanos - System.nanoTime();
		if (remaining > 0) {
			Thread.sleep(Duration.ofNanos(remaining));
		}
	}

	private static void pause() {
		try {
			Thread.sleep(20);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new IllegalStateException(e);
		}
	}

	private static String registerName(int register) {
		return "r" + register;
	}

	private static Keys key(long register) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(register).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static @Nullable String text(@Nullable Buf value) {
		return value == null ? null : new String(value.toByteArray(), StandardCharsets.UTF_8);
	}

	private static long millis(long nanos) {
		return TimeUnit.NANOSECONDS.toMillis(nanos);
	}

}
