package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.IntConsumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@code build-seed}: a seed built from chosen columns of a single-node database whose volume and WAL are outside
 * its directory, by linking table files, opens as a node's database with the same contents and seeds a cluster.
 */
@Timeout(300)
@Tag("slow")
class BuildSeedTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema FIXED = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final ColumnSchema BUCKETED = ColumnSchema.of(IntList.of(Integer.BYTES),
			ObjectList.of(ColumnHashType.XXHASH32), true);

	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private InProcessCluster cluster;
	private IntConsumer previousExiter;
	private Path sourceDb;
	private Path sourceConfig;
	private Path sourceVolume;
	/** A directory outside {@link #root}'s file system, when a test made one. */
	private Path otherFileSystem;
	private final Map<String, Set<KV>> expected = new java.util.HashMap<>();

	private static final class Exited extends Error {
		final int code;

		Exited(int code) {
			super("exit " + code, null, false, false);
			this.code = code;
		}
	}

	private record Run(int code, String out, String err) {}

	@BeforeEach
	void setUp() throws Exception {
		previousExiter = ReplicatedServerMain.exiter;
		ReplicatedServerMain.exiter = code -> {
			throw new Exited(code);
		};
		createSource(root.resolve("single/db"), root.resolve("disk-a/volume"), root.resolve("disk-b/wal"));
	}

	@AfterEach
	void tearDown() throws Exception {
		ReplicatedServerMain.exiter = previousExiter;
		if (cluster != null) {
			cluster.close();
		}
		if (otherFileSystem != null) {
			NodeStore.deleteRecursively(otherFileSystem);
		}
	}

	private static synchronized Run run(String... args) {
		var out = new ByteArrayOutputStream();
		var err = new ByteArrayOutputStream();
		var oldOut = System.out;
		var oldErr = System.err;
		System.setOut(new PrintStream(out, true, StandardCharsets.UTF_8));
		System.setErr(new PrintStream(err, true, StandardCharsets.UTF_8));
		int code = 0;
		try {
			ReplicatedServerMain.main(args);
		} catch (Exited exited) {
			code = exited.code;
		} catch (Throwable e) {
			code = 1;
			err.writeBytes(String.valueOf(e).getBytes(StandardCharsets.UTF_8));
		} finally {
			System.setOut(oldOut);
			System.setErr(oldErr);
		}
		var run = new Run(code, out.toString(StandardCharsets.UTF_8), err.toString(StandardCharsets.UTF_8));
		System.out.println("$ " + String.join(" ", args) + "\n-> " + code + "\n" + run.out() + run.err());
		return run;
	}

	private static Buf longBuf(long value) {
		return Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
	}

	private static Buf intBuf(int value) {
		return Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array());
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static Set<KV> contents(RocksDBSyncAPI api, String column) {
		long id = api.getColumnId(column);
		try (var stream = api.getRange(0, id, null, null, false, RequestType.<KV>allInRange())) {
			var result = new HashSet<KV>();
			stream.forEach(kv -> assertTrue(result.add(kv), "duplicate " + kv.keys()));
			return result;
		}
	}

	/** A single-node database with its volume and WAL outside its directory: three columns, files on several levels. */
	private void createSource(Path db, Path volume, Path wal) throws IOException {
		sourceDb = db;
		sourceVolume = volume;
		Files.createDirectories(sourceDb);
		Files.createDirectories(sourceVolume);
		Files.createDirectories(wal);
		sourceConfig = root.resolve(db.getParent().getFileName() + ".conf");
		Files.writeString(sourceConfig, """
				database.global.wal-path: "%s"
				database.global.fallback-column-options.volumes: [
				  { volume-path: "%s", target-size: "10TiB" }
				]
				""".formatted(wal, sourceVolume));
		try (var connection = new EmbeddedConnection(sourceDb, "single", sourceConfig)) {
			var api = connection.getSyncApi(RequestContext.batch(TIMEOUT));
			long fixed = api.createColumn("fixed", FIXED);
			long bucketed = api.createColumn("bucketed", BUCKETED);
			long huge = api.createColumn("huge", FIXED);
			for (int round = 0; round < 3; round++) {
				for (int i = round * 1000; i < (round + 1) * 1000; i++) {
					api.put(0, fixed, new Keys(longBuf(i * 7L)), text("fixed-" + i + "-" + "v".repeat(i % 90)),
							RequestType.none());
					api.put(0, bucketed, new Keys(intBuf(i % 10), text("key-" + i + "-" + "x".repeat(i % 50))),
							text("bucketed-" + i), RequestType.none());
					api.put(0, huge, new Keys(longBuf(i)), text("huge-" + i), RequestType.none());
				}
				// Overwrites and deletes across files, so that the order of the imported files matters.
				for (int i = 0; i < 200; i++) {
					api.put(0, fixed, new Keys(longBuf(i * 7L)), text("round-" + round + "-" + i), RequestType.none());
					api.delete(0, bucketed, new Keys(intBuf(i % 10), text("key-" + (round * 1000 + i) + "-"
							+ "x".repeat((round * 1000 + i) % 50))), RequestType.none());
				}
				if (round == 0) {
					connection.getEmbeddedDB().flush();
					connection.getEmbeddedDB().compact();
				} else if (round == 1) {
					connection.getEmbeddedDB().flush();
				}
				// The last round stays in the memtables until the close.
			}
			for (String column : List.of("fixed", "bucketed", "huge")) {
				expected.put(column, contents(api, column));
			}
			// Table files below level 0 too: the import keeps each file's level.
			var levels = connection.getEmbeddedDB().getDb().get().getLiveFilesMetaData().stream()
					.filter(file -> new String(file.columnFamilyName(), StandardCharsets.UTF_8).equals("fixed"))
					.map(file -> file.level()).collect(java.util.stream.Collectors.toSet());
			assertTrue(levels.size() >= 2 && levels.contains(0), "levels of fixed: " + levels);
		}
		assertEquals(3000, expected.get("fixed").size());
		assertEquals(2400, expected.get("bucketed").size());
		assertEquals(Set.of(), tableInodes(sourceDb), "the source's volume is outside its directory");
		assertFalse(tableInodes(sourceVolume).isEmpty());
	}

	private static Set<Object> tableInodes(Path dir) throws IOException {
		var inodes = new HashSet<>();
		try (Stream<Path> walk = Files.walk(dir)) {
			for (Path file : walk.filter(path -> path.getFileName().toString().endsWith(".sst")).toList()) {
				inodes.add(Files.readAttributes(file, BasicFileAttributes.class).fileKey());
			}
		}
		return inodes;
	}

	private static void copy(Path source, Path target) throws IOException {
		try (Stream<Path> walk = Files.walk(source)) {
			for (Path path : walk.toList()) {
				Path destination = target.resolve(source.relativize(path).toString());
				if (Files.isDirectory(path)) {
					Files.createDirectories(destination);
				} else {
					Files.copy(path, destination);
				}
			}
		}
	}

	private Run buildSeed(Path seed, String... more) {
		var args = new ArrayList<>(List.of("build-seed", "--source-db", sourceDb.toString(), "--source-config",
				sourceConfig.toString(), "--target", seed.toString()));
		args.addAll(List.of(more));
		return run(args.toArray(String[]::new));
	}

	private void assertNothingLeft(Path seed) throws IOException {
		assertFalse(Files.exists(seed.resolveSibling(seed.getFileName() + ".tmp")), "the temporary directory is left");
		assertFalse(Files.exists(sourceVolume.resolveSibling("volume.build-seed-export")), "the export directory is left");
		try (Stream<Path> list = Files.list(sourceDb)) {
			assertEquals(List.of(), list.filter(path -> Files.isSymbolicLink(path)
					|| path.getFileName().toString().startsWith("column-export")).toList(), "left in the source database");
		}
		try (Stream<Path> list = Files.list(seed.getParent())) {
			assertEquals(List.of(), list.filter(path -> path.getFileName().toString().endsWith(".conf")
					&& !path.equals(sourceConfig)).filter(path -> path.getFileName().toString().startsWith("rockserver-"))
					.toList(), "a config file is left");
		}
	}

	@Test
	void theSeedHoldsTheChosenColumnsAsLinksAndOpensAsANodesDatabase() throws Exception {
		Path seed = root.resolve("seed");
		var built = buildSeed(seed, "--exclude-column", "huge");
		assertEquals(0, built.code(), built.toString());
		assertTrue(built.out().strip().endsWith("OK"), built.out());
		assertTrue(built.out().lines().anyMatch(line -> line.startsWith("column fixed: ") && line.endsWith(", linked")),
				built.out());
		assertTrue(built.out().lines().anyMatch(line -> line.startsWith("column bucketed: ") && line.endsWith(", linked")),
				built.out());
		assertFalse(built.out().contains("huge"), built.out());
		assertFalse(built.out().contains("copying"), built.out());
		assertNothingLeft(seed);

		// Volumes inside the seed, every table file of its columns a link to one of the source's.
		assertTrue(Files.isDirectory(seed.resolve("volume")));
		var sourceInodes = tableInodes(sourceVolume);
		var seedInodes = tableInodes(seed.resolve("volume"));
		assertFalse(seedInodes.isEmpty());
		seedInodes.removeAll(tableInodes(seed.resolve("volume")).stream().filter(sourceInodes::contains).toList());
		// What is left is not of the user columns: Rockserver's internal columns of the seed (the schemas).
		long userFiles = tableInodes(seed.resolve("volume")).stream().filter(sourceInodes::contains).count();
		assertTrue(userFiles >= 4, "linked table files: " + userFiles);
		assertTrue(seedInodes.size() <= 2, "table files of the seed that are no links: " + seedInodes.size());

		// No replication metadata, and no excluded column, before a node opens it (read on a copy).
		Path plainCopy = root.resolve("plain-copy");
		copy(seed, plainCopy);
		try (var plain = new EmbeddedConnection(plainCopy, "plain", null)) {
			var definitions = plain.getEmbeddedDB().getAllColumnDefinitions();
			assertEquals(Map.of("fixed", FIXED, "bucketed", BUCKETED), definitions);
		}
		assertNull(NodeStore.readSeedDigest(seed));

		// The source is as it was.
		try (var source = new EmbeddedConnection(sourceDb, "single", sourceConfig)) {
			var api = source.getSyncApi(RequestContext.batch(TIMEOUT));
			for (String column : List.of("fixed", "bucketed", "huge")) {
				assertEquals(expected.get(column), contents(api, column), column);
			}
		}

		// As a node opens it, with the default config; after a compaction; after a reopen.
		try (var store = NodeStore.open(seed, null, "seed")) {
			assertNull(store.clusterId());
			var api = store.connection().getSyncApi(RequestContext.batch(TIMEOUT));
			var definitions = new java.util.TreeMap<>(store.db().getAllColumnDefinitions());
			definitions.remove(NodeStore.META_COLUMN);
			assertEquals(Map.of("fixed", FIXED, "bucketed", BUCKETED), definitions);
			assertEquals(expected.get("fixed"), contents(api, "fixed"));
			assertEquals(expected.get("bucketed"), contents(api, "bucketed"));
			// Point reads, through the bloom filters of the imported files.
			assertEquals(text("round-2-5"), api.get(0, api.getColumnId("fixed"), new Keys(longBuf(35)),
					RequestType.<Buf>current()));
			assertEquals(text("bucketed-2999"), api.get(0, api.getColumnId("bucketed"),
					new Keys(intBuf(9), text("key-2999-" + "x".repeat(2999 % 50))), RequestType.<Buf>current()));
			// New writes go over the imported data.
			api.put(0, api.getColumnId("fixed"), new Keys(longBuf(35)), text("new"), RequestType.none());
			store.db().flush();
			store.db().compact();
			assertEquals(text("new"), api.get(0, api.getColumnId("fixed"), new Keys(longBuf(35)),
					RequestType.<Buf>current()));
			assertEquals(expected.get("bucketed"), contents(api, "bucketed"));
			assertEquals(3000, contents(api, "fixed").size());
		}
		try (var store = NodeStore.open(seed, null, "seed")) {
			var api = store.connection().getSyncApi(RequestContext.batch(TIMEOUT));
			assertEquals(expected.get("bucketed"), contents(api, "bucketed"));
			var fixed = contents(api, "fixed");
			var want = new HashSet<>(expected.get("fixed"));
			want.removeIf(kv -> kv.keys().equals(new Keys(longBuf(35))));
			want.add(new KV(new Keys(longBuf(35)), text("new")));
			assertEquals(want, fixed);
		}
	}

	/** The cluster's config has its WAL outside the database: the seed keeps its own in wal.detached. */
	@Test
	void aSeedForAClusterWithAnOutsideWalOpensWithThatConfig() throws Exception {
		Path clusterConfig = root.resolve("cluster.conf");
		Path clusterWal = root.resolve("node-wal");
		Files.writeString(clusterConfig, "database.global.wal-path: \"" + clusterWal + "\"\n");
		Path seed = root.resolve("seed");
		var built = buildSeed(seed, "--rockserver-config", clusterConfig.toString(), "--column", "bucketed");
		assertEquals(0, built.code(), built.toString());
		assertNothingLeft(seed);
		assertFalse(Files.exists(clusterWal), "the build wrote into the cluster's WAL directory");
		try (var store = NodeStore.open(seed, clusterConfig, "seed")) {
			var api = store.connection().getSyncApi(RequestContext.batch(TIMEOUT));
			var definitions = new java.util.TreeMap<>(store.db().getAllColumnDefinitions());
			definitions.remove(NodeStore.META_COLUMN);
			assertEquals(Map.of("bucketed", BUCKETED), definitions);
			assertEquals(expected.get("bucketed"), contents(api, "bucketed"));
		}
	}

	@Test
	void threeCopiesOfTheSeedStartAClusterWithEqualChecksums() throws Exception {
		Path seed = root.resolve("seed");
		var built = buildSeed(seed, "--exclude-column", "huge");
		assertEquals(0, built.code(), built.toString());
		cluster = new InProcessCluster(Files.createDirectories(root.resolve("cluster")), 3, config -> config);
		for (var id : cluster.ids()) {
			copy(seed, cluster.config(id).dbDir());
		}
		cluster.startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream()
				.allMatch(node -> node.stateMachine().store().clusterId() != null));
		cluster.awaitConverged(TIMEOUT);
		for (var node : cluster.runningNodes()) {
			assertTrue(node.stateMachine().store().seeded());
			assertFalse(node.isFailed());
			assertNotNull(node.stateMachine().store().seedDigest());
		}
		var api = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));
		assertEquals(expected.get("fixed"), contents(api, "fixed"));
		assertEquals(expected.get("bucketed"), contents(api, "bucketed"));
		long fixed = api.createColumn("fixed", FIXED);
		long bucketed = api.createColumn("bucketed", BUCKETED);
		for (int i = 0; i < 100; i++) {
			api.put(0, fixed, new Keys(longBuf(1_000_000L + i)), text("cluster-" + i), RequestType.none());
			api.put(0, bucketed, new Keys(intBuf(3), text("cluster-" + i)), text("cluster-" + i), RequestType.none());
		}
		api.delete(0, bucketed, new Keys(intBuf(9), text("key-2999-" + "x".repeat(2999 % 50))), RequestType.none());
		assertEquals(text("cluster-7"), api.get(0, fixed, new Keys(longBuf(1_000_007L)), RequestType.<Buf>current()));
		assertEquals(text("round-2-5"), api.get(0, fixed, new Keys(longBuf(35)), RequestType.<Buf>current()));
		assertEquals(3100, contents(api, "fixed").size());
		assertEquals(2400 + 100 - 1, contents(api, "bucketed").size());

		var admin = new ArrayList<String>(List.of("admin"));
		for (var peer : leader.config().peers()) {
			admin.add("--peer");
			admin.add(peer.format());
		}
		admin.addAll(List.of("--timeout-ms", "30000", "checksum"));
		var checksum = run(admin.toArray(String[]::new));
		assertEquals(0, checksum.code(), checksum.toString());
		assertTrue(checksum.out().strip().endsWith("OK"), checksum.out());
	}

	/**
	 * As in production: the column's volume is on another file system than the database directory. A seed built on
	 * the volume's file system is linked; one built elsewhere is copied, and the command says so first.
	 */
	@Test
	void aVolumeOnAnotherFileSystemIsLinkedThereAndCopiedElsewhere() throws Exception {
		Path varTmp = Path.of("/var/tmp");
		org.junit.jupiter.api.Assumptions.assumeTrue(Files.isDirectory(varTmp)
				&& !Files.getFileStore(varTmp).equals(Files.getFileStore(root)), "one file system only");
		otherFileSystem = Files.createTempDirectory(varTmp, "build-seed-test");
		expected.clear();
		createSource(root.resolve("other/db"), otherFileSystem.resolve("volume"), root.resolve("other-wal"));

		Path linkedSeed = otherFileSystem.resolve("seed");
		var linked = buildSeed(linkedSeed, "--exclude-column", "huge");
		assertEquals(0, linked.code(), linked.toString());
		assertFalse(linked.out().contains("copying"), linked.out());
		assertEquals(2, linked.out().lines().filter(line -> line.endsWith(", linked")).count(), linked.out());
		assertNothingLeft(linkedSeed);
		var volumeInodes = tableInodes(sourceVolume);
		assertTrue(tableInodes(linkedSeed).stream().filter(volumeInodes::contains).count() >= 4);

		Path copiedSeed = root.resolve("copied-seed");
		var copied = buildSeed(copiedSeed, "--exclude-column", "huge");
		assertEquals(0, copied.code(), copied.toString());
		assertEquals(2, copied.out().lines().filter(line -> line.contains("copying") && line.contains("into the seed"))
				.count(), copied.out());
		assertEquals(2, copied.out().lines().filter(line -> line.endsWith(", copied")).count(), copied.out());
		assertNothingLeft(copiedSeed);

		// The export directory on another file system than the volume: copied twice over, and said so.
		Path exportDir = Files.createDirectories(root.resolve("exports"));
		Path exportedSeed = otherFileSystem.resolve("exported-seed");
		var exported = buildSeed(exportedSeed, "--column", "fixed", "--export-dir", exportDir.toString());
		assertEquals(0, exported.code(), exported.toString());
		assertTrue(exported.out().contains("is on another file system"), exported.out());
		assertTrue(exported.out().lines().anyMatch(line -> line.endsWith(", copied")), exported.out());
		try (Stream<Path> list = Files.list(exportDir)) {
			assertEquals(0, list.count(), "left in --export-dir");
		}

		for (Path seed : List.of(linkedSeed, copiedSeed, exportedSeed)) {
			try (var store = NodeStore.open(seed, null, "seed")) {
				var api = store.connection().getSyncApi(RequestContext.batch(TIMEOUT));
				assertEquals(expected.get("fixed"), contents(api, "fixed"), seed.toString());
				if (!seed.equals(exportedSeed)) {
					assertEquals(expected.get("bucketed"), contents(api, "bucketed"), seed.toString());
				}
			}
		}
		// The source still opens and holds everything.
		try (var source = new EmbeddedConnection(sourceDb, "single", sourceConfig)) {
			var api = source.getSyncApi(RequestContext.batch(TIMEOUT));
			for (String column : List.of("fixed", "bucketed", "huge")) {
				assertEquals(expected.get(column), contents(api, column), column);
			}
		}
	}

	private static byte[] jarOf(Class<?> type) throws IOException {
		var bytes = new ByteArrayOutputStream();
		try (var jar = new java.util.jar.JarOutputStream(bytes)) {
			String entry = type.getName().replace('.', '/') + ".class";
			jar.putNextEntry(new java.util.jar.JarEntry(entry));
			try (var in = type.getClassLoader().getResourceAsStream(entry)) {
				in.transferTo(jar);
			}
			jar.closeEntry();
		}
		return bytes.toByteArray();
	}

	/** A column whose schema names an uploaded merge operator is refused; without it the seed is built. */
	@Test
	void aColumnWithAMergeOperatorInItsSchemaIsRefused() throws Exception {
		Path db = root.resolve("merging/db");
		Files.createDirectories(db);
		var operator = it.cavallium.rockserver.core.impl.MyStringAppendOperator.class;
		var key = new Keys(longBuf(1));
		try (var connection = new EmbeddedConnection(db, "merging", null)) {
			var api = connection.getSyncApi(RequestContext.batch(TIMEOUT));
			long version = api.uploadMergeOperator("append", operator.getName(), jarOf(operator));
			long merged = api.createColumn("merged",
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true, "append", version));
			api.put(0, merged, key, text("a"), RequestType.none());
			api.put(0, api.createColumn("plain", FIXED), key, text("p"), RequestType.none());
			// Rockserver cannot reopen a database whose WAL holds merges of such a column.
			connection.getEmbeddedDB().flush();
		}
		Path seed = root.resolve("merging-seed");
		var refused = run("build-seed", "--source-db", db.toString(), "--target", seed.toString());
		assertEquals(1, refused.code(), refused.toString());
		assertTrue(refused.err().contains("Column merged has a merge operator in its schema (append version"),
				refused.err());
		assertFalse(Files.exists(seed));
		assertFalse(Files.exists(seed.resolveSibling("merging-seed.tmp")));

		var built = run("build-seed", "--source-db", db.toString(), "--target", seed.toString(), "--exclude-column",
				"merged");
		assertEquals(0, built.code(), built.toString());
		try (var store = NodeStore.open(seed, null, "seed")) {
			var api = store.connection().getSyncApi(RequestContext.batch(TIMEOUT));
			assertEquals(text("p"), api.get(0, api.getColumnId("plain"), key, RequestType.<Buf>current()));
			assertFalse(store.db().getAllColumnDefinitions().containsKey("merged"));
		}
	}

	/** A merge operator that only the source's config gives the column: refused unless the cluster's config gives it too. */
	@Test
	void aMergeOperatorOfTheSourceConfigMustBeInTheClusterConfig() throws Exception {
		Path db = root.resolve("configured/db");
		Files.createDirectories(db);
		Path mergeConfig = root.resolve("merge.conf");
		Files.writeString(mergeConfig, InProcessCluster.MERGE_CONFIG);
		var key = new Keys(longBuf(1));
		try (var connection = new EmbeddedConnection(db, "configured", mergeConfig)) {
			var api = connection.getSyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumn("configured", FIXED);
			api.put(0, column, key, text("a"), RequestType.none());
			connection.getEmbeddedDB().flush();
			api.merge(0, column, key, text("b"), RequestType.none());
		}
		Path seed = root.resolve("configured-seed");
		var refused = run("build-seed", "--source-db", db.toString(), "--source-config", mergeConfig.toString(),
				"--target", seed.toString());
		assertEquals(1, refused.code(), refused.toString());
		assertTrue(refused.err().contains("merge-operator-class"), refused.err());
		assertFalse(Files.exists(seed));
		assertFalse(Files.exists(seed.resolveSibling("configured-seed.tmp")));

		var built = run("build-seed", "--source-db", db.toString(), "--source-config", mergeConfig.toString(),
				"--target", seed.toString(), "--rockserver-config", mergeConfig.toString());
		assertEquals(0, built.code(), built.toString());
		try (var store = NodeStore.open(seed, mergeConfig, "seed")) {
			var api = store.connection().getSyncApi(RequestContext.batch(TIMEOUT));
			assertEquals(text("a,b"), api.get(0, api.getColumnId("configured"), key, RequestType.<Buf>current()));
		}
	}

	@Test
	void refusals() throws Exception {
		Path seed = root.resolve("seed");

		var both = buildSeed(seed, "--column", "fixed", "--exclude-column", "huge");
		assertEquals(2, both.code(), both.toString());
		var noTarget = run("build-seed", "--source-db", sourceDb.toString());
		assertEquals(2, noTarget.code(), noTarget.toString());

		var unknown = buildSeed(seed, "--exclude-column", "nope");
		assertEquals(1, unknown.code(), unknown.toString());
		assertTrue(unknown.err().contains("Unknown column nope"), unknown.err());
		assertFalse(Files.exists(seed));
		assertNothingLeft(seed);

		var noDatabase = run("build-seed", "--source-db", root.resolve("missing").toString(), "--target", seed.toString());
		assertEquals(1, noDatabase.code(), noDatabase.toString());
		assertTrue(noDatabase.err().contains("No database in"), noDatabase.err());

		// A cluster config whose volume is outside the database: nothing is written.
		Path outside = root.resolve("outside.conf");
		Files.writeString(outside, """
				database.global.fallback-column-options.volumes: [
				  { volume-path: "/var/somewhere-else", target-size: "10TiB" }
				]
				""");
		var volumeOutside = buildSeed(seed, "--rockserver-config", outside.toString());
		assertEquals(1, volumeOutside.code(), volumeOutside.toString());
		assertTrue(volumeOutside.err().contains("outside the database directory"), volumeOutside.err());
		assertFalse(Files.exists(seed));
		assertNothingLeft(seed);

		try (var running = new EmbeddedConnection(sourceDb, "single", sourceConfig)) {
			var inUse = buildSeed(seed);
			assertEquals(1, inUse.code(), inUse.toString());
			assertTrue(inUse.err().contains("is in use"), inUse.err());
			assertFalse(Files.exists(seed));
			assertNothingLeft(seed);
		}

		Files.createDirectories(seed);
		var exists = buildSeed(seed);
		assertEquals(1, exists.code(), exists.toString());
		assertTrue(exists.err().contains("already exists"), exists.err());
		try (Stream<Path> list = Files.list(seed)) {
			assertEquals(0, list.count(), "the existing target was written to");
		}
	}
}
