package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
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
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.SplittableRandom;
import java.util.TreeMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * Membership changes on a multi-process cluster, through the {@code admin} command line: permanent loss and
 * replacement of a node, and clusters seeded from an existing single-node database.
 *
 * <p>A new member catches up by snapshot install only once the leader's log no longer starts at its first entry,
 * so these tests write enough data to fill a Raft log segment and pass the purge gap
 * ({@link #fillUntilLogsArePurged}).</p>
 */
@Timeout(600)
class MembershipTest {

	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration TIMEOUT = Duration.ofSeconds(90);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(10));
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final List<String> SERVER_ARGS = List.of("--snapshot-every", "200",
			"--election-timeout-ms", "400:800",
			"--request-timeout-ms", "5000");
	/** Large values fill a 64 MB Raft log segment in about a thousand entries, the purge gap. */
	private static final int FILLER_VALUE_BYTES = 64 * 1024;
	private static final int MAX_FILLER_KEYS = 4_000;
	private static final int WRITERS = 8;
	private static final Duration RETRY_PAUSE = Duration.ofMillis(100);
	private static final String SNAPSHOT_INSTALLED = "installing snapshot";
	private static final String REPLICA_FAILED = "replica failed";
	private static final String SEEDED_BUT_EMPTY = "The cluster was seeded with an existing database, but this"
			+ " replica's database is empty";

	/** Kept when a test fails, for the node logs. */
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private ProcessCluster cluster;
	private ClusterClient client;

	@AfterEach
	void tearDown() {
		if (client != null) {
			client.close();
		}
		if (cluster != null) {
			cluster.close();
		}
	}

	/**
	 * A member is lost with its disk; a new node with a new id and empty data replaces it through add-peer and
	 * remove-peer, catches up by snapshot install, and the cluster then survives the loss of another original member.
	 */
	@Test
	void lostNodeIsReplacedByANewNodeThatCatchesUpFromASnapshot() throws Exception {
		cluster = ProcessCluster.create(root, 3, 1, serverArgs());
		startAll(List.of("1", "2", "3"));
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		long column = client.createColumn("data", SCHEMA, CONTEXT).valueOrThrow();
		int keys = fillUntilLogsArePurged(column, 0, List.of("1", "2", "3"));

		cluster.kill("1");
		deleteRecursively(cluster.dataDir("1"));
		cluster.setMembers(List.of("2", "3", "4"));
		cluster.start("4");
		cluster.waitReady("4", READY_TIMEOUT);

		long addStart = System.nanoTime();
		assertOk(cluster.admin(List.of("1", "2", "3"), "add-peer", cluster.peer("4").format()));
		var addTime = Duration.ofNanos(System.nanoTime() - addStart);
		assertOk(cluster.admin(List.of("1", "2", "3", "4"), "remove-peer", "1"));
		var membershipTime = Duration.ofNanos(System.nanoTime() - addStart);
		var output4 = cluster.output("4");
		assertTrue(output4.contains(SNAPSHOT_INSTALLED), "node 4 did not catch up by snapshot install");
		assertFalse(output4.contains(REPLICA_FAILED), "node 4 failed:\n" + cluster.logTail("4"));
		System.out.printf("MEASURE replacement: %d keys of %d bytes, add-peer (catch-up) %d ms,"
				+ " add-peer + remove-peer %d ms%n", keys, FILLER_VALUE_BYTES, addTime.toMillis(),
				membershipTime.toMillis());

		for (int i = keys; i < keys + 100; i++) {
			client.put(column, key(i), fillerValue(i), CONTEXT).valueOrThrow();
		}
		keys += 100;

		// Another original member is lost: the new membership {2, 3, 4} keeps a majority.
		cluster.kill("2");
		long failoverStart = System.nanoTime();
		client.put(column, key(keys), fillerValue(keys), CONTEXT).valueOrThrow();
		System.out.printf("MEASURE first write after losing node 2: %d ms%n",
				Duration.ofNanos(System.nanoTime() - failoverStart).toMillis());
		keys++;
		for (int i = 0; i < keys; i += 37) {
			assertArrayEquals(fillerValue(i).toByteArray(), client.get(column, key(i), CONTEXT).valueOrThrow()
					.toByteArray(), "key " + i);
		}

		// Whichever survivor leads, the other one serves through it.
		var survivors = List.of("3", "4");
		awaitAppliedEverywhere(survivors, column, key(keys - 1), fillerValue(keys - 1));
		stopAll(survivors);
		assertReplicasEqual(survivors, Map.of("data", (long) keys));
	}

	/**
	 * A cluster seeded with a copy of an existing database serves its data; a member added later with an empty
	 * database receives the data as a snapshot.
	 */
	@Test
	void seededClusterServesTheSeedAndANewMemberReceivesItAsASnapshot() throws Exception {
		cluster = ProcessCluster.create(root, 3, 1, serverArgs());
		var seed = createSeed();
		for (var id : List.of("1", "2", "3")) {
			copyRecursively(seed.dir, cluster.dataDir(id).resolve("db"));
		}
		startAll(List.of("1", "2", "3"));
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		assertSeedServed(seed, List.of("1", "2", "3"));
		assertTrue(cluster.output(leaderOf(List.of("1", "2", "3"))).contains("(seeded: true)"),
				"the cluster was not initialized as seeded");

		long filler = client.createColumn("filler", SCHEMA, CONTEXT).valueOrThrow();
		int keys = fillUntilLogsArePurged(filler, 0, List.of("1", "2", "3"));

		cluster.start("4");
		cluster.waitReady("4", READY_TIMEOUT);
		long addStart = System.nanoTime();
		assertOk(cluster.admin(List.of("1", "2", "3"), "add-peer", cluster.peer("4").format()));
		System.out.printf("MEASURE seeded join: add-peer (catch-up) %d ms after %d filler keys%n",
				Duration.ofNanos(System.nanoTime() - addStart).toMillis(), keys);
		var output4 = cluster.output("4");
		assertTrue(output4.contains(SNAPSHOT_INSTALLED), "node 4 did not receive a snapshot");
		assertFalse(output4.contains(REPLICA_FAILED), "node 4 failed:\n" + cluster.logTail("4"));

		// The new member serves the seed from its own database once it leads.
		var members = List.of("1", "2", "3", "4");
		assertOk(cluster.admin(members, "transfer-leader", "4"));
		assertSeedServed(seed, List.of("4"));
		awaitAppliedEverywhere(members, filler, key(keys - 1), fillerValue(keys - 1));
		stopAll(members);
		var expected = new TreeMap<String, Long>(seed.keysByColumn);
		expected.put("filler", (long) keys);
		assertReplicasEqual(members, expected);
	}

	/**
	 * A member with an empty database added to a seeded cluster right after it started must get the seed from the
	 * snapshot the leader takes after genesis, never by replaying a log that lacks the seed. (If it ever did replay
	 * the genesis entry, it would stop with a clear error instead of diverging.)
	 */
	@Test
	void emptyMemberJoiningASeededClusterRightAwayGetsTheSeedFromASnapshot() throws Exception {
		cluster = ProcessCluster.create(root, 3, 1, serverArgs());
		var seed = createSeed();
		for (var id : List.of("1", "2", "3")) {
			copyRecursively(seed.dir, cluster.dataDir(id).resolve("db"));
		}
		startAll(List.of("1", "2", "3"));
		client = new ClusterClient(cluster.apiAddresses(), TIMEOUT, true);
		assertSeedServed(seed, List.of("1", "2", "3"));

		cluster.start("4");
		cluster.waitReady("4", READY_TIMEOUT);
		assertOk(cluster.admin(List.of("1", "2", "3"), "add-peer", cluster.peer("4").format()));
		InProcessCluster.await(TIMEOUT, () -> cluster.output("4").contains(SNAPSHOT_INSTALLED));
		assertFalse(cluster.output("4").contains(SEEDED_BUT_EMPTY), "node 4 replayed the seeded genesis entry");

		var column = client.getColumnId("seeded", CONTEXT).valueOrThrow();
		client.put(column, key(1_000_000), text("after"), CONTEXT).valueOrThrow();
		var members = List.of("1", "2", "3", "4");
		awaitAppliedEverywhere(members, column, key(1_000_000), text("after"));
		stopAll(members);
		var expected = new TreeMap<String, Long>(seed.keysByColumn);
		expected.merge("seeded", 1L, Long::sum);
		assertReplicasEqual(members, expected);
	}

	// ---- cluster helpers ----

	private List<String> serverArgs() throws IOException {
		var config = root.resolve("rockserver.conf");
		Files.writeString(config, InProcessCluster.MERGE_CONFIG);
		var args = new ArrayList<>(SERVER_ARGS);
		args.add("--rockserver-config");
		args.add(config.toString());
		return args;
	}

	private Path rockserverConfig() {
		return root.resolve("rockserver.conf");
	}

	private void startAll(List<String> ids) throws IOException {
		for (var id : ids) {
			cluster.start(id);
		}
		cluster.waitReady(READY_TIMEOUT);
	}

	/**
	 * Stops the nodes gracefully, so that their databases hold everything they applied: replicas apply entries
	 * without the RocksDB WAL, so after a kill the files are a consistent but older prefix, which the node completes
	 * from its Raft log only when it starts again.
	 */
	private void stopAll(List<String> ids) {
		for (var id : ids) {
			cluster.stop(id);
		}
	}

	private static void assertOk(String adminOutput) {
		assertTrue(adminOutput.lines().anyMatch(line -> line.strip().equals("OK")), adminOutput);
	}

	/** The leader according to {@code admin status}, asked through {@code members}. */
	private String leaderOf(List<String> members) throws IOException {
		var status = cluster.admin(members, "status");
		return status.lines()
				.filter(line -> line.contains(": LEADER"))
				.map(line -> line.substring(0, line.indexOf(':')))
				.findFirst()
				.orElseThrow(() -> new AssertionError("No leader in status:\n" + status));
	}

	/**
	 * Makes each member lead in turn and reads {@code key} through it: a leader serves a read only after applying
	 * every committed entry, so afterwards every member's database holds all the user writes made so far. Reading
	 * through each other member at the same time checks that followers reach every leader, including members that
	 * joined after them.
	 */
	private void awaitAppliedEverywhere(List<String> members, long column, Keys key, Buf expected)
			throws IOException {
		for (var leader : members) {
			assertOk(cluster.admin(members, "transfer-leader", leader));
			InProcessCluster.await(TIMEOUT, () -> {
				try {
					return leaderOf(members).equals(leader);
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			});
			for (var via : members) {
				var value = readThrough(via, column, key);
				assertNotNull(value, "read through " + via + " with leader " + leader);
				assertArrayEquals(expected.toByteArray(), value.toByteArray(),
						"read through " + via + " with leader " + leader);
			}
			assertEquals(leader, leaderOf(members), "leadership moved during the reads");
		}
	}

	/**
	 * Reads {@code key} through node {@code via} only, retrying retryable failures (a follower that did not learn
	 * of a new leader yet) within the timeout; any other failure fails the test at once.
	 */
	private Buf readThrough(String via, long column, Keys key) {
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (true) {
			try {
				return client.api(via, CONTEXT).get(0, column, key, RequestType.current());
			} catch (RuntimeException e) {
				if (!ClusterClient.isRetryable(e) || System.nanoTime() - deadline > 0) {
					throw new AssertionError("Read through node " + via + " failed", e);
				}
			}
			try {
				// Retry pause only: the loop's outcome does not depend on its length.
				Thread.sleep(RETRY_PAUSE);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				throw new AssertionError(e);
			}
		}
	}

	/**
	 * Writes keys from {@code firstKey} up, in rounds, until the Raft log of every member starts after index 0,
	 * so that a member added from now on needs a snapshot. Returns the next unused key.
	 */
	private int fillUntilLogsArePurged(long column, int firstKey, List<String> members) throws Exception {
		int next = firstKey;
		long start = System.nanoTime();
		try (var executor = Executors.newVirtualThreadPerTaskExecutor()) {
			while (!members.stream().allMatch(this::logPurged)) {
				if (next - firstKey >= MAX_FILLER_KEYS) {
					throw new AssertionError("Raft logs not purged after " + MAX_FILLER_KEYS + " writes: "
							+ members.stream().map(id -> id + "=" + logFiles(id)).toList());
				}
				var round = new ArrayList<Future<?>>();
				for (int w = 0; w < WRITERS; w++) {
					int writer = w;
					int base = next;
					round.add(executor.submit(() -> {
						for (int i = base + writer; i < base + 200; i += WRITERS) {
							client.put(column, key(i), fillerValue(i), CONTEXT).valueOrThrow();
						}
					}));
				}
				for (var future : round) {
					try {
						future.get();
					} catch (ExecutionException e) {
						throw new AssertionError("Filler write failed", e.getCause());
					}
				}
				next += 200;
			}
		}
		System.out.printf("MEASURE filler: %d keys of %d bytes until the logs were purged, %d ms%n", next - firstKey,
				FILLER_VALUE_BYTES, Duration.ofNanos(System.nanoTime() - start).toMillis());
		return next;
	}

	/** True if the node's Raft log no longer holds index 0: some prefix was purged after a snapshot. */
	private boolean logPurged(String id) {
		var files = logFiles(id);
		return !files.isEmpty() && files.stream().noneMatch(name -> name.equals("log_inprogress_0")
				|| name.startsWith("log_0-"));
	}

	private List<String> logFiles(String id) {
		var dir = cluster.dataDir(id).resolve("raft").resolve(NodeConfig.DEFAULT_GROUP_ID).resolve("current");
		try (Stream<Path> files = Files.list(dir)) {
			return files.map(path -> path.getFileName().toString()).filter(name -> name.startsWith("log_")).sorted()
					.toList();
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	// ---- seed ----

	private record Seed(Path dir, long columnA, long columnB, Map<String, Long> keysByColumn) {}

	private static final int SEED_KEYS = 500;

	/** A single-node Rockserver database with two columns, written through the embedded API. */
	private Seed createSeed() throws IOException {
		var dir = root.resolve("seed");
		long a;
		long b;
		try (var connection = new EmbeddedConnection(dir, "seed", rockserverConfig())) {
			var db = connection.getEmbeddedDB();
			a = db.createColumn("seeded", SCHEMA);
			b = db.createColumn("seeded-merges", SCHEMA);
			for (int i = 0; i < SEED_KEYS; i++) {
				db.put(0, a, key(i), text("seed-" + i), RequestType.none());
			}
			for (int i = 0; i < 20; i++) {
				db.merge(0, b, key(i % 5), text("m" + i), RequestType.none());
			}
		}
		return new Seed(dir, a, b, Map.of("seeded", (long) SEED_KEYS, "seeded-merges", 5L));
	}

	/** Every seeded key reads back through each of {@code via}, with the seed's column ids. */
	private void assertSeedServed(Seed seed, List<String> via) {
		for (var id : via) {
			var api = client.api(id, CONTEXT);
			assertEquals(seed.columnA, api.getColumnId("seeded"), "column id through " + id);
			assertEquals(seed.columnB, api.getColumnId("seeded-merges"), "column id through " + id);
			for (int i = 0; i < SEED_KEYS; i += 7) {
				assertEquals("seed-" + i, text(api.get(0, seed.columnA, key(i), RequestType.current())),
						"key " + i + " through " + id);
			}
			assertEquals("m0,m5,m10,m15", text(api.get(0, seed.columnB, key(0), RequestType.current())));
		}
	}

	// ---- replica comparison ----

	private record ColumnDump(long id, long keys, String sha256) {}

	/** Every user column of a stopped node's database: id, key count and a digest of its contents in key order. */
	private Map<String, ColumnDump> dump(Path dbDir) throws IOException {
		var result = new TreeMap<String, ColumnDump>();
		try (var connection = new EmbeddedConnection(dbDir, "dump", rockserverConfig())) {
			var db = connection.getEmbeddedDB();
			for (var name : db.getAllColumnDefinitions().keySet()) {
				if (name.equals(NodeStore.META_COLUMN)) {
					continue;
				}
				long id = db.getColumnId(name);
				var digest = sha256();
				long count = 0;
				try (Stream<KV> range = db.getRange(0, id, null, null, false, RequestType.<KV>allInRange())) {
					for (var kv : (Iterable<KV>) range::iterator) {
						for (var part : kv.keys().keys()) {
							update(digest, part.toByteArray());
						}
						update(digest, kv.value() == null ? new byte[0] : kv.value().toByteArray());
						count++;
					}
				}
				result.put(name, new ColumnDump(id, count, HexFormat.of().formatHex(digest.digest())));
			}
		}
		return result;
	}

	/** Every replica holds identical user data, with the expected number of keys per column. */
	private void assertReplicasEqual(List<String> ids, Map<String, Long> expectedKeys) throws IOException {
		Map<String, ColumnDump> first = null;
		for (var id : ids) {
			var dump = dump(cluster.dataDir(id).resolve("db"));
			var counts = new TreeMap<String, Long>();
			dump.forEach((name, column) -> counts.put(name, column.keys()));
			assertEquals(new TreeMap<>(expectedKeys), counts, "key counts of node " + id);
			if (first == null) {
				first = dump;
			} else {
				assertEquals(first, dump, "node " + id + " differs from node " + ids.getFirst());
			}
		}
	}

	private static MessageDigest sha256() {
		try {
			return MessageDigest.getInstance("SHA-256");
		} catch (NoSuchAlgorithmException e) {
			throw new IllegalStateException(e);
		}
	}

	/** Length-prefixed, so that different splits of the same bytes digest differently. */
	private static void update(MessageDigest digest, byte[] bytes) {
		digest.update(ByteBuffer.allocate(Integer.BYTES).putInt(bytes.length).array());
		digest.update(bytes);
	}

	// ---- data ----

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String text(Buf value) {
		return value == null ? null : new String(value.toByteArray(), StandardCharsets.UTF_8);
	}

	/** Incompressible value of a filler key, reproducible from the key. */
	private static Buf fillerValue(long key) {
		var bytes = new byte[FILLER_VALUE_BYTES];
		new SplittableRandom(key).nextBytes(bytes);
		return Buf.wrap(bytes);
	}

	// ---- files ----

	private static void copyRecursively(Path source, Path target) throws IOException {
		try (Stream<Path> paths = Files.walk(source)) {
			for (var path : (Iterable<Path>) paths::iterator) {
				var copy = target.resolve(source.relativize(path).toString());
				if (Files.isDirectory(path)) {
					Files.createDirectories(copy);
				} else {
					Files.copy(path, copy);
				}
			}
		}
	}

	private static void deleteRecursively(Path path) throws IOException {
		if (!Files.exists(path)) {
			return;
		}
		try (Stream<Path> paths = Files.walk(path)) {
			for (var entry : paths.sorted((a, b) -> b.getNameCount() - a.getNameCount()).toList()) {
				Files.delete(entry);
			}
		}
	}
}
