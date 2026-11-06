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
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
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

/**
 * What the targeted fault scenarios share: a {@link ProcessCluster} observed through each node's Raft server, a
 * workload of writes to unique keys whose outcome the client records, and the checks after the fault: every
 * acknowledged write is readable through the cluster, no write that certainly failed is, and the replicas hold the
 * same data on disk.
 */
final class ScenarioSupport {

	static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	static final Duration PHASE_TIMEOUT = Duration.ofSeconds(60);
	static final Duration CLIENT_RETRY_TIMEOUT = Duration.ofSeconds(20);
	static final RequestContext WRITE_CONTEXT = RequestContext.latency(Duration.ofSeconds(8));
	static final RequestContext READ_CONTEXT = RequestContext.latency(Duration.ofSeconds(5));
	static final RequestContext BATCH_CONTEXT = RequestContext.batch(Duration.ofSeconds(30));
	static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	static final RaftGroupId GROUP_ID = RaftGroupId.valueOf(UUID.fromString(NodeConfig.DEFAULT_GROUP_ID));

	private final ProcessCluster cluster;

	ScenarioSupport(ProcessCluster cluster) {
		this.cluster = cluster;
	}

	// ---------------------------------------------------------------------------------------------------------
	// Workload

	/** One write of a unique key, with what the client knows of its outcome. */
	record Write(long key, String value, OpStatus status) {}

	/**
	 * Writers that each put unique keys ({@code writer << 32 | sequence}) with a value naming them, through their own
	 * client starting at a different node, and record the outcome of every write. A client does not retry a write
	 * whose outcome is unknown.
	 */
	static final class UniqueWrites {
		private final Map<String, HostAndPort> addresses;
		private final long columnId;
		private final int writers;
		private final int valueBytes;
		private final AtomicBoolean stopped = new AtomicBoolean();
		private final ConcurrentLinkedQueue<Write> writes = new ConcurrentLinkedQueue<>();
		private final AtomicLong acknowledged = new AtomicLong();
		private final AtomicLong lastAcknowledgedNanos = new AtomicLong(Long.MIN_VALUE);
		private final ConcurrentLinkedQueue<Throwable> failures = new ConcurrentLinkedQueue<>();
		private final List<Thread> threads = new ArrayList<>();
		private final List<ClusterClient> clients = new ArrayList<>();

		UniqueWrites(Map<String, HostAndPort> addresses, long columnId, int writers, int valueBytes) {
			this.addresses = addresses;
			this.columnId = columnId;
			this.writers = writers;
			this.valueBytes = valueBytes;
		}

		UniqueWrites start() {
			var ids = new ArrayList<>(addresses.keySet());
			for (int w = 0; w < writers; w++) {
				var ordered = new LinkedHashMap<String, HostAndPort>();
				for (int i = 0; i < ids.size(); i++) {
					var id = ids.get((w + i) % ids.size());
					ordered.put(id, addresses.get(id));
				}
				var client = new ClusterClient(ordered, CLIENT_RETRY_TIMEOUT, false);
				clients.add(client);
				int writer = w;
				threads.add(Thread.ofPlatform().name("unique-writer-" + w).start(() -> {
					try {
						for (long seq = 0; !stopped.get(); seq++) {
							long key = ((long) writer << 32) | seq;
							var value = value(key, valueBytes);
							var result = client.put(columnId, key(key), text(value), WRITE_CONTEXT);
							writes.add(new Write(key, value, result.status()));
							if (result.status() == OpStatus.OK) {
								acknowledged.incrementAndGet();
								lastAcknowledgedNanos.accumulateAndGet(System.nanoTime(), Math::max);
							}
						}
					} catch (Throwable e) {
						failures.add(e);
					}
				}));
			}
			return this;
		}

		long acknowledged() {
			return acknowledged.get();
		}

		/** When the latest acknowledgement arrived, in {@link System#nanoTime()} units. */
		long lastAcknowledgedNanos() {
			return lastAcknowledgedNanos.get();
		}

		void awaitMore(long count, Duration timeout, String what) {
			long target = acknowledged() + count;
			await(timeout, () -> acknowledged() >= target, "fewer than " + count + " writes acknowledged " + what);
		}

		void stop() throws InterruptedException {
			stopped.set(true);
			for (var thread : threads) {
				thread.join(CLIENT_RETRY_TIMEOUT.plus(Duration.ofSeconds(30)));
				if (thread.isAlive()) {
					fail("writer " + thread.getName() + " did not stop");
				}
			}
			clients.forEach(ClusterClient::close);
			if (!failures.isEmpty()) {
				var error = new AssertionError("writer threads failed");
				failures.forEach(error::addSuppressed);
				throw error;
			}
		}

		List<Write> writes() {
			return List.copyOf(writes);
		}
	}

	/** A value that names its key, padded to {@code bytes}. */
	static String value(long key, int bytes) {
		var value = new StringBuilder("v").append(Long.toHexString(key)).append('-');
		while (value.length() < bytes) {
			value.append((char) ('a' + value.length() % 26));
		}
		return value.toString();
	}

	/**
	 * Reads every written key through the cluster, linearizably: an acknowledged write must be there, a write that
	 * certainly failed must not, and a write with an unknown outcome may be either. Returns how many unknown writes
	 * took effect.
	 */
	static int checkWrites(ClusterClient client, long columnId, List<Write> writes) {
		int unknownApplied = 0;
		for (var write : writes) {
			var read = client.get(columnId, key(write.key()), READ_CONTEXT);
			assertEquals(OpStatus.OK, read.status(), () -> "read of key " + write.key() + ": " + read.error());
			var value = read.value() == null ? null : text(read.value());
			switch (write.status()) {
				case OK -> assertEquals(write.value(), value, "acknowledged write of key " + Long.toHexString(write.key())
						+ " lost");
				case FAILED -> assertEquals(null, value, "write of key " + Long.toHexString(write.key())
						+ " failed as not applied, but took effect");
				case UNKNOWN -> {
					if (value != null) {
						assertEquals(write.value(), value);
						unknownApplied++;
					}
				}
			}
		}
		return unknownApplied;
	}

	/** Stops every node, then compares the user columns of every replica on disk. */
	void checkReplicasIdentical(Path rockserverConfig) throws IOException {
		for (var id : cluster.nodeIds()) {
			if (cluster.isRunning(id)) {
				cluster.stop(id);
			}
		}
		Map<String, Map<String, String>> first = null;
		String firstId = null;
		for (var id : cluster.nodeIds()) {
			if (!java.nio.file.Files.isDirectory(cluster.dataDir(id).resolve("db"))) {
				continue;
			}
			var contents = replicaContents(cluster.dataDir(id).resolve("db"), rockserverConfig);
			if (first == null) {
				first = contents;
				firstId = id;
			} else {
				assertEquals(first, contents, "replica " + id + " differs from replica " + firstId);
			}
		}
		assertNotNull(first, "no replica on disk");
	}

	/** Every user column of a stopped node's database: column name to hex key to value. */
	static Map<String, Map<String, String>> replicaContents(Path db, Path rockserverConfig) throws IOException {
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
	// Cluster state, through each node's Raft server

	String awaitLeader(Duration timeout) {
		String[] leader = new String[1];
		await(timeout, () -> (leader[0] = leaderOrNull()) != null, "no single leader");
		return leader[0];
	}

	@Nullable String leaderOrNull() {
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

	/**
	 * The term of the last entry in the node's log, or -1 if it does not answer: every leader appends an entry as it
	 * starts its term, so this is the term of the latest leader the node followed.
	 */
	long lastEntryTerm(String id) {
		var info = groupInfos().get(id);
		return info == null ? -1 : info.getLogInfoProto().getLastEntry().getTerm();
	}

	/** Waits until the node applied everything the leader had committed when the wait started. */
	void awaitCaughtUp(String id) {
		var leader = awaitLeader(PHASE_TIMEOUT);
		var leaderInfo = groupInfos().get(leader);
		assertNotNull(leaderInfo, "leader " + leader + " unreachable");
		long target = leaderInfo.getLogInfoProto().getCommitted().getIndex();
		await(PHASE_TIMEOUT, () -> {
			var info = groupInfos().get(id);
			return info != null && info.getLogInfoProto().getApplied().getIndex() >= target;
		}, "node " + id + " did not catch up to index " + target);
	}

	/** Index of the node's applied entry, or -1 if it does not answer. */
	long applied(String id) {
		var info = groupInfos().get(id);
		return info == null ? -1 : info.getLogInfoProto().getApplied().getIndex();
	}

	/** Index of the node's latest snapshot, or -1 if it does not answer. */
	long snapshotIndex(String id) {
		var info = groupInfos().get(id);
		return info == null ? -1 : info.getLogInfoProto().getLastSnapshot().getIndex();
	}

	/** Waits until every running node applied the leader's last entry, and the log stopped growing for about 2 s. */
	void awaitQuiescent() {
		long[] last = {-1};
		int[] stableRounds = {0};
		await(PHASE_TIMEOUT, () -> {
			var infos = groupInfos();
			long running = cluster.nodeIds().stream().filter(cluster::isRunning).count();
			if (infos.size() != running) {
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
			return stableRounds[0] >= 20;
		}, "the cluster did not become quiescent");
	}

	/** Group info of every running node that answers, by node id. */
	Map<String, GroupInfoReply> groupInfos() {
		return groupInfos(cluster.nodeIds());
	}

	/** Group info of the node, or null if it is not running or does not answer within a second. */
	@Nullable GroupInfoReply groupInfo(String id) {
		return groupInfos(List.of(id)).get(id);
	}

	/** Group info of those of {@code ids} that run and answer, by node id. */
	Map<String, GroupInfoReply> groupInfos(Collection<String> ids) {
		var peers = cluster.nodeIds().stream()
				.map(id -> RaftPeer.newBuilder().setId(id).setAddress(cluster.raftAddress(id).host() + ":"
						+ cluster.raftAddress(id).port()).build())
				.toList();
		var properties = new RaftProperties();
		RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(1, TimeUnit.SECONDS));
		var infos = new LinkedHashMap<String, GroupInfoReply>();
		try (var admin = RaftClient.newBuilder()
				.setClientId(ClientId.randomId())
				.setRaftGroup(RaftGroup.valueOf(GROUP_ID, peers))
				.setProperties(properties)
				.setRetryPolicy(RetryPolicies.noRetry())
				.build()) {
			for (var id : ids) {
				if (!cluster.isRunning(id)) {
					continue;
				}
				try {
					infos.put(id, admin.getGroupManagementApi(RaftPeerId.valueOf(id)).info(GROUP_ID));
				} catch (IOException unreachable) {
					// Starting, or not answering.
				}
			}
		} catch (IOException e) {
			throw new IllegalStateException("Cannot close the admin client", e);
		}
		return infos;
	}

	/** Waits until the node halted at a crash point, and checks that it did so there. */
	void awaitCrash(String id, CrashPoints.Point point, Duration timeout) throws InterruptedException {
		int code = cluster.awaitExit(id, timeout);
		assertEquals(CrashPoints.EXIT_CODE, code, () -> "node " + id + " exited otherwise\n" + cluster.logTail(id));
		assertTrue(cluster.output(id).contains("CRASH POINT " + point), () -> cluster.logTail(id));
	}

	static void await(Duration timeout, BooleanSupplier condition, String failure) {
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

	static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	static String text(Buf value) {
		return new String(value.toByteArray(), StandardCharsets.UTF_8);
	}
}
