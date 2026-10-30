package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

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
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BooleanSupplier;
import java.util.stream.Stream;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.GroupInfoReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Scenario 2 of the design: the leader is killed (SIGKILL) in the middle of concurrent writes, three times.
 *
 * <p>Writers merge unique tokens into a set of keys and put unique values into others, through a
 * {@link ClusterClient} that knows every node and does not retry ambiguous writes. After the faults, every
 * acknowledged merge must appear exactly once, no token may appear twice, the registers must hold their last
 * acknowledged value (or a value whose write outcome is unknown), and the three replicas must hold identical data
 * on disk. Writes must resume quickly after each kill.</p>
 */
@Timeout(300)
class LeaderKillTest {

	private static final int ROUNDS = 3;
	private static final int MERGE_WRITERS = 4;
	private static final int PUT_WRITERS = 2;
	private static final int MERGE_KEYS = 8;
	private static final int PUT_KEYS_PER_WRITER = 4;
	private static final long PUT_KEY_BASE = 1_000;
	/** Acknowledged writes to wait for before each kill, and after each restart. */
	private static final int WRITES_PER_PHASE = 150;

	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration PHASE_TIMEOUT = Duration.ofSeconds(60);
	/** Bound on the time from a leader kill to the first acknowledged write sent after it. */
	private static final Duration MAX_FAILOVER = Duration.ofSeconds(10);
	private static final Duration CLIENT_RETRY_TIMEOUT = Duration.ofSeconds(20);
	private static final RequestContext WRITE_CONTEXT = RequestContext.latency(Duration.ofSeconds(8));
	private static final RequestContext READ_CONTEXT = RequestContext.latency(Duration.ofSeconds(5));
	private static final RequestContext BATCH_CONTEXT = RequestContext.batch(Duration.ofSeconds(30));
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final String COLUMN = "data";
	private static final String HOT_COLUMN = "hot";
	private static final int HOT_COUNTERS = 2;
	private static final int HOT_WORKERS = 6;
	private static final int MINIMUM_HOT_INCREMENTS = 20;
	private static final RaftGroupId GROUP_ID = RaftGroupId.valueOf(UUID.fromString(NodeConfig.DEFAULT_GROUP_ID));

	@TempDir
	Path root;
	private ProcessCluster cluster;
	private ClusterClient client;

	/** One write sent by a writer, with what the client knows about its outcome. */
	private record Write(boolean merge, long key, String token, OpStatus status, long invokedNanos,
			long completedNanos) {}

	@AfterEach
	void tearDown() {
		if (client != null) {
			client.close();
		}
		if (cluster != null) {
			cluster.close();
		}
	}

	/** What the nodes run with: {@code PipelinedLeaderKillTest} changes it. */
	List<String> serverArgs(Path rockserverConfig) {
		return List.of("--rockserver-config", rockserverConfig.toString(),
				"--election-timeout-ms", "400:800",
				"--request-timeout-ms", "5000");
	}

	@Test
	void acknowledgedWritesSurviveRepeatedLeaderKills() throws Exception {
		var rockserverConfig = root.resolve("rockserver.conf");
		Files.writeString(rockserverConfig, InProcessCluster.MERGE_CONFIG);
		cluster = ProcessCluster.start(root.resolve("cluster"), 3, serverArgs(rockserverConfig));
		cluster.waitReady(READY_TIMEOUT);
		client = new ClusterClient(cluster.apiAddresses(), CLIENT_RETRY_TIMEOUT, false);
		var column = client.createColumn(COLUMN, SCHEMA, BATCH_CONTEXT);
		assertEquals(OpStatus.OK, column.status(), () -> "createColumn: " + column.error());
		long columnId = column.value();

		long hotColumn = client.createColumn(HOT_COLUMN, SCHEMA, BATCH_CONTEXT).valueOrThrow();
		var hot = new HotCounters(cluster.apiAddresses(), hotColumn, 0, HOT_COUNTERS, HOT_WORKERS);
		hot.start();
		var workload = new Workload(cluster.apiAddresses(), columnId);
		workload.start();
		var failovers = new ArrayList<Duration>();
		try {
			awaitAcknowledged(workload, WRITES_PER_PHASE);
			for (int round = 1; round <= ROUNDS; round++) {
				var leader = awaitLeader(PHASE_TIMEOUT);
				long killedAt = System.nanoTime();
				cluster.kill(leader);
				await(PHASE_TIMEOUT, () -> workload.lastAcknowledgedInvocation() > killedAt,
						"no write acknowledged after killing leader " + leader + " in round " + round);
				var failover = Duration.ofNanos(workload.firstAcknowledgedCompletionAfter(killedAt) - killedAt);
				failovers.add(failover);
				System.out.println("Round " + round + ": killed leader " + leader + ", writes resumed after "
						+ failover.toMillis() + " ms");

				awaitAcknowledged(workload, WRITES_PER_PHASE);
				cluster.start(leader);
				cluster.waitReady(leader, READY_TIMEOUT);
				awaitAcknowledged(workload, WRITES_PER_PHASE);
				awaitCaughtUp(leader);
			}
			awaitAcknowledged(workload, WRITES_PER_PHASE);
		} finally {
			workload.stop();
			hot.stop();
		}
		workload.checkWritersSucceeded();

		var writes = workload.writes();
		var summary = summarize(writes);
		System.out.println("Writes: " + summary + ", failovers: " + failovers.stream().map(Duration::toMillis).toList()
				+ " ms");
		for (int round = 0; round < failovers.size(); round++) {
			assertTrue(failovers.get(round).compareTo(MAX_FAILOVER) < 0,
					"round " + (round + 1) + ": writes resumed only after " + failovers.get(round));
		}

		awaitQuiescent();
		var finalValues = readThroughCluster(columnId);
		int unknownMergesApplied = checkMerges(writes, finalValues);
		checkPuts(writes, finalValues);
		System.out.println("Merges with an unknown outcome that were applied: " + unknownMergesApplied);
		var hotValues = new TreeMap<String, String>();
		hot.check(key -> {
			var value = client.get(hotColumn, key(key), READ_CONTEXT).valueOrThrow();
			if (value != null) {
				hotValues.put(hexKey(key), text(value));
			}
			return value;
		}, MINIMUM_HOT_INCREMENTS, cluster.pipelinedUpdates());

		for (var id : cluster.nodeIds()) {
			cluster.stop(id);
		}
		var replicas = new LinkedHashMap<String, Map<String, Map<String, String>>>();
		for (var id : cluster.nodeIds()) {
			replicas.put(id, replicaContents(cluster.dataDir(id).resolve("db"), rockserverConfig));
		}
		var expectedColumn = new TreeMap<String, String>();
		finalValues.forEach((key, value) -> expectedColumn.put(hexKey(key), value));
		var expected = Map.of(COLUMN, expectedColumn, HOT_COLUMN, hotValues);
		for (var replica : replicas.entrySet()) {
			assertEquals(expected, replica.getValue(),
					"replica " + replica.getKey() + " differs from the cluster's reads");
		}
	}

	/**
	 * Concurrent writers that record every write and its outcome. Each writer has its own client, which starts on
	 * a different node: a kill then catches writes in flight both on the leader and on followers, whose own Raft
	 * client retries them on the next leader.
	 */
	private static final class Workload {
		private final Map<String, HostAndPort> addresses;
		private final long columnId;
		private final List<ClusterClient> clients = new ArrayList<>();
		private final AtomicBoolean stopped = new AtomicBoolean();
		private final ConcurrentLinkedQueue<Write> writes = new ConcurrentLinkedQueue<>();
		private final AtomicLong acknowledged = new AtomicLong();
		private final AtomicLong lastAcknowledgedInvocation = new AtomicLong(Long.MIN_VALUE);
		private final ConcurrentLinkedQueue<Throwable> writerFailures = new ConcurrentLinkedQueue<>();
		private final List<Thread> threads = new ArrayList<>();

		Workload(Map<String, HostAndPort> addresses, long columnId) {
			this.addresses = addresses;
			this.columnId = columnId;
		}

		void start() {
			for (int i = 0; i < MERGE_WRITERS; i++) {
				int writer = i;
				var client = newClient();
				threads.add(Thread.ofPlatform().name("merge-writer-" + i)
						.start(() -> run(() -> mergeLoop(client, writer))));
			}
			for (int i = 0; i < PUT_WRITERS; i++) {
				int writer = i;
				var client = newClient();
				threads.add(Thread.ofPlatform().name("put-writer-" + i)
						.start(() -> run(() -> putLoop(client, writer))));
			}
		}

		/** A client whose failover order starts at the next node. */
		private ClusterClient newClient() {
			var ids = new ArrayList<>(addresses.keySet());
			int first = clients.size() % ids.size();
			var ordered = new LinkedHashMap<String, HostAndPort>();
			for (int i = 0; i < ids.size(); i++) {
				var id = ids.get((first + i) % ids.size());
				ordered.put(id, addresses.get(id));
			}
			var client = new ClusterClient(ordered, CLIENT_RETRY_TIMEOUT, false);
			clients.add(client);
			return client;
		}

		private void run(Runnable loop) {
			try {
				loop.run();
			} catch (Throwable e) {
				writerFailures.add(e);
			}
		}

		private void mergeLoop(ClusterClient client, int writer) {
			for (long seq = 0; !stopped.get(); seq++) {
				long key = ThreadLocalRandom.current().nextInt(MERGE_KEYS);
				var token = "m" + writer + "-" + seq;
				long invoked = System.nanoTime();
				var result = client.merge(columnId, key(key), text(token), WRITE_CONTEXT);
				record(new Write(true, key, token, result.status(), invoked, System.nanoTime()));
			}
		}

		/** Each put writer owns its keys, so the writes to one key are sequential. */
		private void putLoop(ClusterClient client, int writer) {
			for (long seq = 0; !stopped.get(); seq++) {
				long key = PUT_KEY_BASE + writer * PUT_KEYS_PER_WRITER + seq % PUT_KEYS_PER_WRITER;
				var token = "p" + writer + "-" + seq;
				long invoked = System.nanoTime();
				var result = client.put(columnId, key(key), text(token), WRITE_CONTEXT);
				record(new Write(false, key, token, result.status(), invoked, System.nanoTime()));
			}
		}

		private void record(Write write) {
			writes.add(write);
			if (write.status() == OpStatus.OK) {
				lastAcknowledgedInvocation.accumulateAndGet(write.invokedNanos(), Math::max);
				acknowledged.incrementAndGet();
			}
		}

		long acknowledged() {
			return acknowledged.get();
		}

		long lastAcknowledgedInvocation() {
			return lastAcknowledgedInvocation.get();
		}

		/** Completion time of the first acknowledged write invoked after {@code nanos}. */
		long firstAcknowledgedCompletionAfter(long nanos) {
			return writes.stream()
					.filter(w -> w.status() == OpStatus.OK && w.invokedNanos() > nanos)
					.mapToLong(Write::completedNanos)
					.min()
					.orElseThrow();
		}

		void stop() throws InterruptedException {
			stopped.set(true);
			for (var thread : threads) {
				// A write retries for at most CLIENT_RETRY_TIMEOUT, each attempt bounded by its request timeout.
				thread.join(CLIENT_RETRY_TIMEOUT.plus(Duration.ofSeconds(30)));
				if (thread.isAlive()) {
					fail("writer " + thread.getName() + " did not stop");
				}
			}
			clients.forEach(ClusterClient::close);
		}

		void checkWritersSucceeded() {
			if (!writerFailures.isEmpty()) {
				var error = new AssertionError("writer threads failed");
				writerFailures.forEach(error::addSuppressed);
				throw error;
			}
		}

		List<Write> writes() {
			return List.copyOf(writes);
		}
	}

	private static String summarize(List<Write> writes) {
		var counts = new TreeMap<String, Integer>();
		for (var write : writes) {
			counts.merge((write.merge() ? "merge " : "put ") + write.status(), 1, Integer::sum);
		}
		return counts.toString();
	}

	/**
	 * Every acknowledged merge exactly once; unknown ones at most once; failed ones never; nothing foreign.
	 *
	 * @return how many merges with an unknown outcome were applied
	 */
	private static int checkMerges(List<Write> writes, Map<Long, String> finalValues) {
		var byToken = new HashMap<String, Write>();
		for (var write : writes) {
			if (write.merge()) {
				byToken.put(write.token(), write);
			}
		}
		var applied = new HashSet<String>();
		for (long key = 0; key < MERGE_KEYS; key++) {
			var value = finalValues.get(key);
			var counts = new HashMap<String, Integer>();
			if (value != null) {
				for (var token : value.split(",")) {
					counts.merge(token, 1, Integer::sum);
				}
			}
			for (var entry : counts.entrySet()) {
				var token = entry.getKey();
				assertEquals(1, entry.getValue(), "token " + token + " applied more than once to key " + key
						+ ": " + byToken.get(token));
				var write = byToken.get(token);
				assertNotNull(write, "key " + key + " holds token " + token + " that no writer sent");
				assertEquals(key, write.key(), "token " + token + " applied to the wrong key");
				assertTrue(write.status() != OpStatus.FAILED, "failed write applied: " + write);
				applied.add(token);
			}
		}
		int unknownApplied = 0;
		for (var write : byToken.values()) {
			if (write.status() == OpStatus.OK) {
				assertTrue(applied.contains(write.token()), "acknowledged merge lost: " + write);
			} else if (write.status() == OpStatus.UNKNOWN && applied.contains(write.token())) {
				unknownApplied++;
			}
		}
		return unknownApplied;
	}

	/**
	 * Each register holds its last acknowledged value, or the value of a write whose outcome is unknown (which may
	 * take effect at any time after it was sent).
	 */
	private static void checkPuts(List<Write> writes, Map<Long, String> finalValues) {
		var byKey = new HashMap<Long, List<Write>>();
		for (var write : writes) {
			if (!write.merge()) {
				byKey.computeIfAbsent(write.key(), k -> new ArrayList<>()).add(write);
			}
		}
		assertEquals(PUT_WRITERS * PUT_KEYS_PER_WRITER, byKey.size(), "put keys written");
		for (var entry : byKey.entrySet()) {
			long key = entry.getKey();
			var keyWrites = entry.getValue();
			var lastAcknowledged = keyWrites.stream().filter(w -> w.status() == OpStatus.OK)
					.reduce((first, second) -> second).orElse(null);
			var value = finalValues.get(key);
			if (lastAcknowledged != null && lastAcknowledged.token().equals(value)) {
				continue;
			}
			var holder = keyWrites.stream().filter(w -> w.token().equals(value)).findFirst().orElse(null);
			assertTrue(holder != null && holder.status() == OpStatus.UNKNOWN, "key " + key + " holds " + value
					+ ", written by " + holder + ", but its last acknowledged write is " + lastAcknowledged);
		}
	}

	/** Linearizable reads of every written key. */
	private Map<Long, String> readThroughCluster(long columnId) {
		var values = new TreeMap<Long, String>();
		var keys = new ArrayList<Long>();
		for (long key = 0; key < MERGE_KEYS; key++) {
			keys.add(key);
		}
		for (long key = PUT_KEY_BASE; key < PUT_KEY_BASE + PUT_WRITERS * PUT_KEYS_PER_WRITER; key++) {
			keys.add(key);
		}
		for (long key : keys) {
			var read = client.get(columnId, key(key), READ_CONTEXT);
			assertEquals(OpStatus.OK, read.status(), () -> "read of key " + key + ": " + read.error());
			if (read.value() != null) {
				values.put(key, text(read.value()));
			}
		}
		return values;
	}

	/** Every user column of a stopped node's database: column name to hex key to value. */
	private static Map<String, Map<String, String>> replicaContents(Path db, Path rockserverConfig)
			throws IOException {
		var contents = new TreeMap<String, Map<String, String>>();
		try (var connection = new EmbeddedConnection(db, "replica-check", rockserverConfig)) {
			var api = connection.getSyncApi(BATCH_CONTEXT);
			for (var name : api.getAllColumnDefinitions().keySet()) {
				if (name.equals(NodeStore.META_COLUMN)) {
					continue;
				}
				var column = new TreeMap<String, String>();
				try (Stream<KV> range = api.getRange(0, api.getColumnId(name), null, null, false,
						RequestType.<KV>allInRange())) {
					range.forEach(kv -> column.put(HexFormat.of().formatHex(kv.keys().keys()[0].toByteArray()),
							kv.value() == null ? null : text(kv.value())));
				}
				contents.put(name, column);
			}
		}
		return contents;
	}

	// ---------------------------------------------------------------------------------------------------------
	// Cluster state, observed through each node's Raft server like ReplicatedServerMain admin status

	/** Waits until exactly one running node reports itself as leader. */
	private String awaitLeader(Duration timeout) {
		String[] leader = new String[1];
		await(timeout, () -> (leader[0] = leaderOrNull()) != null, "no single leader");
		return leader[0];
	}

	private @Nullable String leaderOrNull() {
		String leader = null;
		for (var entry : groupInfos().entrySet()) {
			if (entry.getValue().getRoleInfoProto().getRole() == RaftPeerRole.LEADER) {
				if (leader != null) {
					return null;
				}
				leader = entry.getKey();
			}
		}
		return leader;
	}

	/** Waits until the node applied everything the leader had committed when the wait started. */
	private void awaitCaughtUp(String id) {
		var leader = awaitLeader(PHASE_TIMEOUT);
		var leaderInfo = groupInfos().get(leader);
		assertNotNull(leaderInfo, "leader " + leader + " unreachable");
		long target = leaderInfo.getLogInfoProto().getCommitted().getIndex();
		await(PHASE_TIMEOUT, () -> {
			var info = groupInfos().get(id);
			return info != null && info.getLogInfoProto().getApplied().getIndex() >= target;
		}, "node " + id + " did not catch up to index " + target);
	}

	/**
	 * Waits until no write is in flight anywhere: every node applied the leader's last entry, and the log stopped
	 * growing. The servers may still retry writes that the client gave up on.
	 */
	private void awaitQuiescent() {
		long[] last = {-1};
		int[] stableRounds = {0};
		await(PHASE_TIMEOUT, () -> {
			var infos = groupInfos();
			if (infos.size() != cluster.nodeIds().size()) {
				stableRounds[0] = 0;
				return false;
			}
			long lastEntry = -1;
			for (var info : infos.values()) {
				if (info.getRoleInfoProto().getRole() == RaftPeerRole.LEADER) {
					lastEntry = info.getLogInfoProto().getLastEntry().getIndex();
				}
			}
			long index = lastEntry;
			boolean converged = index >= 0 && infos.values().stream()
					.allMatch(info -> info.getLogInfoProto().getApplied().getIndex() == index);
			stableRounds[0] = converged && index == last[0] ? stableRounds[0] + 1 : 0;
			last[0] = index;
			// Unchanged for about two seconds of polling.
			return stableRounds[0] >= 20;
		}, "the cluster did not become quiescent");
	}

	/** Group info of every running node that answers, by node id. */
	private Map<String, GroupInfoReply> groupInfos() {
		var peers = cluster.nodeIds().stream()
				.map(id -> RaftPeer.newBuilder().setId(id).setAddress(cluster.raftAddress(id).host() + ":"
						+ cluster.raftAddress(id).port()).build())
				.toList();
		var properties = new RaftProperties();
		RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(1, TimeUnit.SECONDS));
		var infos = new LinkedHashMap<String, GroupInfoReply>();
		// A fresh client each time: a cached channel to a killed node may wait out a long reconnect backoff.
		try (var admin = RaftClient.newBuilder()
				.setClientId(ClientId.randomId())
				.setRaftGroup(RaftGroup.valueOf(GROUP_ID, peers))
				.setProperties(properties)
				.setRetryPolicy(RetryPolicies.noRetry())
				.build()) {
			for (var id : cluster.nodeIds()) {
				if (!cluster.isRunning(id)) {
					continue;
				}
				try {
					infos.put(id, admin.getGroupManagementApi(RaftPeerId.valueOf(id)).info(GROUP_ID));
				} catch (IOException unreachable) {
					// Starting, or not answering: not part of this observation.
				}
			}
		} catch (IOException e) {
			throw new IllegalStateException("Cannot close the admin client", e);
		}
		return infos;
	}

	private void awaitAcknowledged(Workload workload, int count) {
		long target = workload.acknowledged() + count;
		await(PHASE_TIMEOUT, () -> workload.acknowledged() >= target, "fewer than " + count + " writes acknowledged");
	}

	private static void await(Duration timeout, BooleanSupplier condition, String failure) {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() - deadline > 0) {
				fail(failure + " within " + timeout);
			}
			try {
				Thread.sleep(100);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AssertionError(e);
			}
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static String hexKey(long value) {
		return HexFormat.of().formatHex(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String text(Buf value) {
		return new String(value.toByteArray(), StandardCharsets.UTF_8);
	}
}
