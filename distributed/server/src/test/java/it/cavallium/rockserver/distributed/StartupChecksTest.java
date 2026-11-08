package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.RequestType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;

/** Checks a node makes on its own files before and while it starts: see "Disk layout" and "Heap" in the design. */
class StartupChecksTest {

	@TempDir
	Path tempDir;

	@Test
	void theConfigDigestIgnoresLayoutCommentsAndKeyOrder() throws IOException {
		var a = tempDir.resolve("a.conf");
		var b = tempDir.resolve("b.conf");
		var c = tempDir.resolve("c.conf");
		Files.writeString(a, "database.global.block-cache = 1GiB\ndatabase.global.optimistic = true\n");
		Files.writeString(b, "# the same\ndatabase: { global: { optimistic: true,\n  block-cache: \"1GiB\" } }\n");
		Files.writeString(c, "database.global.block-cache = 2GiB\ndatabase.global.optimistic = true\n");
		assertEquals(NodeStore.configDigest(a), NodeStore.configDigest(b));
		assertNotEquals(NodeStore.configDigest(a), NodeStore.configDigest(c));
		assertNotEquals(NodeStore.configDigest(a), NodeStore.configDigest(null));
		assertEquals(NodeStore.configDigest(null), NodeStore.configDigest(null));
	}

	/** DD-35: a volume outside the database directory would make every snapshot fail. */
	@Test
	void aColumnVolumeOutsideTheDatabaseIsRefusedBeforeItIsCreated() throws IOException {
		var outside = tempDir.resolve("outside");
		var config = tempDir.resolve("volumes.conf");
		Files.writeString(config, """
				database.global.fallback-column-options.volumes = [
				  { volume-path: "../outside", target-size: "10TiB" }
				]
				""");
		var error = assertThrows(IOException.class, () -> NodeStore.open(tempDir.resolve("db"), config, "volumes"));
		assertTrue(error.getMessage().contains("outside the database directory"), error.getMessage());
		assertFalse(Files.exists(outside), "the volume was created");

		var perColumn = tempDir.resolve("column-volumes.conf");
		Files.writeString(perColumn, """
				database.global.column-options = [
				  { name: "data", volumes: [ { volume-path: "/var/somewhere-else", target-size: "10TiB" } ] }
				]
				""");
		assertThrows(IOException.class, () -> NodeStore.open(tempDir.resolve("db2"), perColumn, "column-volumes"));

		var inside = tempDir.resolve("inside.conf");
		Files.writeString(inside, """
				database.global.fallback-column-options.volumes = [
				  { volume-path: "./volume-a", target-size: "10TiB" }
				]
				""");
		NodeStore.open(tempDir.resolve("db3"), inside, "inside").close();
	}

	/** DD-18: copies of one seed have one digest; a copy that differs, or a later copy, has another. */
	@Test
	void theSeedDigestTellsCopiesApart() throws IOException {
		var seed = tempDir.resolve("seed");
		try (var store = NodeStore.open(seed, null, "seed")) {
			long column = store.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
			for (int i = 0; i < 100; i++) {
				NodeStoreTest.put(store, column, i, "v" + i);
			}
		}
		var copyA = tempDir.resolve("a/db");
		var copyB = tempDir.resolve("b/db");
		var truncated = tempDir.resolve("c/db");
		copy(seed, copyA);
		copy(seed, copyB);
		copy(seed, truncated);
		var digest = NodeStore.recordSeedDigest(copyA);
		assertEquals(digest, NodeStore.recordSeedDigest(copyB));
		var sst = largestFile(truncated, ".sst");
		Files.write(sst, Arrays.copyOf(Files.readAllBytes(sst), (int) Files.size(sst) - 1));
		assertNotEquals(digest, NodeStore.recordSeedDigest(truncated));

		// Recorded once: the start-up check's open changes the files, not the recorded digest.
		NodeStore.open(copyA, null, "a").close();
		NodeStore.noteSeedOpened(copyA);
		assertNotEquals(digest, NodeStore.databaseFilesDigest(copyA));
		assertEquals(digest, NodeStore.recordSeedDigest(copyA));
		assertEquals(digest, NodeStore.recordSeedDigest(copyA));
		try (var store = NodeStore.open(copyA, null, "a")) {
			assertEquals(digest, store.seedDigest());
		}
		assertNull(NodeStore.recordSeedDigest(tempDir.resolve("empty/db")));
	}

	/**
	 * A first start that failed after recording the digest of a partial copy: the operator copies the seed again, and the
	 * next first start takes the digest of what db/ holds now, not the stale one.
	 */
	@Test
	void aSeedCopiedAgainAfterAFailedFirstStartGetsANewDigest() throws IOException {
		var seed = tempDir.resolve("seed");
		try (var store = NodeStore.open(seed, null, "seed")) {
			long column = store.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
			for (int i = 0; i < 100; i++) {
				NodeStoreTest.put(store, column, i, "v" + i);
			}
		}
		var good = tempDir.resolve("good/db");
		copy(seed, good);
		var digest = NodeStore.recordSeedDigest(good);

		var node = tempDir.resolve("node/db");
		copy(seed, node);
		Files.delete(largestFile(node, ".sst"));
		var partial = NodeStore.recordSeedDigest(node);
		assertNotEquals(digest, partial);
		assertEquals(partial, NodeStore.recordSeedDigest(node), "a retry with the same files changed the digest");

		deleteTree(node);
		copy(seed, node);
		assertEquals(digest, NodeStore.recordSeedDigest(node));
		assertEquals(digest, NodeStore.readSeedDigest(node));
	}

	/** DD-19: a replica's database becomes a seed: same data, no replication metadata, so a new cluster accepts it. */
	@Test
	void anExportedReplicaSeedsANewCluster() throws IOException {
		var replica = tempDir.resolve("node/db");
		long column;
		try (var store = NodeStore.open(replica, null, "replica")) {
			column = store.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
			for (int i = 0; i < 50; i++) {
				NodeStoreTest.put(store, column, i, "v" + i);
			}
			NodeStoreTest.setClusterId(store, "old-cluster");
			NodeStoreTest.setApplied(store, TermIndex.valueOf(4, 77));
		}
		var target = tempDir.resolve("seed");
		var exported = NodeStore.exportSeed(replica, null, target);
		assertEquals(TermIndex.valueOf(4, 77), exported.applied());
		assertEquals("old-cluster", exported.clusterId());
		assertThrows(IOException.class, () -> NodeStore.exportSeed(replica, null, target), "an existing target");

		try (var seed = NodeStore.open(target, null, "seed")) {
			assertNull(seed.clusterId(), "the copy still belongs to the old cluster");
			assertEquals(TermIndex.INITIAL_VALUE, seed.applied());
			assertTrue(seed.hasUserColumns());
			long seedColumn = seed.db().getColumnId(NodeStoreTest.DATA_COLUMN);
			var contents = NodeStoreTest.contents(seed, seedColumn);
			assertEquals(50, contents.size());
			assertEquals("v49", contents.get(49));
		}
		// The replica itself is unchanged.
		try (var store = NodeStore.open(replica, null, "replica")) {
			assertEquals("old-cluster", store.clusterId());
			assertEquals(Map.of(0, "v0"), Map.of(0, NodeStoreTest.contents(store, column).get(0)));
		}
	}

	/**
	 * export-seed with an absolute volume path inside the source database: the copy opens its own volume. A config the
	 * copy cannot use is refused before anything is written.
	 */
	@Test
	void exportSeedHandlesAbsoluteVolumePathsAndLeavesNoTargetWhenRefused() throws IOException {
		var replica = tempDir.resolve("node/db").toAbsolutePath();
		var config = tempDir.resolve("absolute.conf");
		Files.writeString(config, """
				database.global.fallback-column-options.volumes = [
				  { volume-path: "%s", target-size: "10TiB" }
				]
				""".formatted(replica.resolve("volume")));
		long column;
		try (var store = NodeStore.open(replica, config, "replica")) {
			column = store.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
			for (int i = 0; i < 20; i++) {
				NodeStoreTest.put(store, column, i, "v" + i);
			}
			NodeStoreTest.setClusterId(store, "old-cluster");
			store.db().flush();
		}
		var target = tempDir.resolve("seed").toAbsolutePath();
		NodeStore.exportSeed(replica, config, target);
		// The source's volume holds the source's tables; the copy has its own under target/volume.
		var relative = tempDir.resolve("relative.conf");
		Files.writeString(relative, """
				database.global.fallback-column-options.volumes = [
				  { volume-path: "./volume", target-size: "10TiB" }
				]
				""");
		try (var seed = NodeStore.open(target, relative, "seed")) {
			assertNull(seed.clusterId());
			assertEquals(20, NodeStoreTest.contents(seed, seed.db().getColumnId(NodeStoreTest.DATA_COLUMN)).size());
		}
		try (var store = NodeStore.open(replica, config, "replica")) {
			assertEquals("old-cluster", store.clusterId());
		}

		// A volume outside both databases: refused before the checkpoint, so the export can be run again.
		var outside = tempDir.resolve("outside.conf");
		Files.writeString(outside, """
				database.global.column-options = [
				  { name: "other", volumes: [ { volume-path: "%s", target-size: "10TiB" } ] }
				]
				""".formatted(tempDir.resolve("elsewhere").toAbsolutePath()));
		var refused = tempDir.resolve("refused");
		assertThrows(IOException.class, () -> NodeStore.exportSeed(replica, outside, refused));
		assertFalse(Files.exists(refused), "a half-made target");
		try (var leftovers = Files.list(tempDir)) {
			assertTrue(leftovers.noneMatch(path -> path.getFileName().toString().startsWith("rockserver-replica-")));
		}
	}

	/**
	 * DD-6a: without -Djava.io.tmpdir the RocksDB library is extracted into DATA-DIR/tmp, not into the JDK's temporary
	 * directory, which a property set while the JVM runs does not move. One short JVM that loads the library.
	 */
	@Test
	void theRocksDbLibraryIsExtractedIntoTheDataDirectory() throws Exception {
		var jdkTemporary = Files.createDirectories(tempDir.resolve("jdk-tmp"));
		var dataDir = tempDir.resolve("node");
		Files.createDirectories(dataDir.resolve("tmp"));
		var stale = Files.writeString(dataDir.resolve("tmp/librocksdbjni123456.so"), "left by a killed JVM");
		var command = List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
				"-cp", System.getProperty("java.class.path"),
				"-Djava.io.tmpdir=" + jdkTemporary,
				LoadLibrary.class.getName(), dataDir.toString(), jdkTemporary.toString(), "false");
		var process = new ProcessBuilder(command).redirectErrorStream(true).start();
		var output = new String(process.getInputStream().readAllBytes());
		assertTrue(process.waitFor(60, TimeUnit.SECONDS));
		assertEquals(0, process.exitValue(), output);
		assertTrue(output.contains("node: [librocksdbjni"), output);
		assertTrue(output.contains("jdk: []"), output);
		assertFalse(Files.exists(stale), "the copy of an earlier run is still there");
	}

	/**
	 * R27-2: with an explicit -Djava.io.tmpdir the node still extracts the RocksDB library into DATA-DIR/tmp, and deletes
	 * the copies earlier runs left there, so a node killed again and again (halt, ExitOnOutOfMemoryError, SIGKILL) no
	 * longer fills the given directory with 17 MB copies. A given directory inside DATA-DIR is the node's own: the copies
	 * that older versions left there are deleted too. One outside DATA-DIR (possibly shared) is not touched.
	 */
	@Test
	void anExplicitTemporaryDirectoryGetsNoCopyOfTheRocksDbLibrary() throws Exception {
		for (boolean inside : new boolean[] {true, false}) {
			var dataDir = tempDir.resolve(inside ? "explicit-inside" : "explicit-outside");
			var given = Files.createDirectories(inside ? dataDir.resolve("jtmp") : tempDir.resolve("shared-tmp"));
			Files.createDirectories(dataDir.resolve("tmp"));
			var staleInNodeTmp = Files.writeString(dataDir.resolve("tmp/librocksdbjni111.so"), "left by a killed JVM");
			var staleInGiven = Files.writeString(given.resolve("librocksdbjni222.so"), "left by a killed JVM");
			var command = List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
					"-cp", System.getProperty("java.class.path"),
					"-Djava.io.tmpdir=" + given,
					LoadLibrary.class.getName(), dataDir.toString(), given.toString(), "true");
			var process = new ProcessBuilder(command).redirectErrorStream(true).start();
			var output = new String(process.getInputStream().readAllBytes());
			assertTrue(process.waitFor(60, TimeUnit.SECONDS));
			assertEquals(0, process.exitValue(), output);
			assertTrue(output.contains("node: [librocksdbjni"), output);
			assertFalse(Files.exists(staleInNodeTmp), output);
			// The operator's choice stays for everything else.
			assertTrue(output.contains("tmpdir: " + given), output);
			if (inside) {
				assertTrue(output.contains("jdk: []"), output);
			} else {
				assertTrue(output.contains("jdk: [librocksdbjni222.so]"), output);
				assertTrue(Files.exists(staleInGiven), "a copy in a shared directory, maybe in use by another node");
			}
		}
	}

	/**
	 * R27 review 5: a DATA-DIR/tmp, or a given -Djava.io.tmpdir under DATA-DIR, that is a symlink to a shared directory
	 * is not the node's own: the copies of the RocksDB library in it may be another JVM's, between extracting and loading,
	 * and are not deleted. The library is still extracted there (the node's copy is the only new one).
	 */
	@Test
	void aSymlinkedTemporaryDirectoryIsNotCleaned() throws Exception {
		for (boolean explicit : new boolean[] {false, true}) {
			var dataDir = Files.createDirectories(tempDir.resolve(explicit ? "symlink-given" : "symlink-node"));
			var shared = Files.createDirectories(tempDir.resolve(explicit ? "shared-given" : "shared-node"));
			var othersCopy = Files.writeString(shared.resolve("librocksdbjni333.so"), "another JVM's, being loaded");
			Path given;
			if (explicit) {
				given = Files.createSymbolicLink(dataDir.resolve("jtmp"), shared);
			} else {
				Files.createSymbolicLink(dataDir.resolve("tmp"), shared);
				given = Files.createDirectories(tempDir.resolve("jdk-tmp-symlink"));
			}
			var command = List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
					"-cp", System.getProperty("java.class.path"),
					"-Djava.io.tmpdir=" + given,
					LoadLibrary.class.getName(), dataDir.toString(), given.toString(), String.valueOf(explicit));
			var process = new ProcessBuilder(command).redirectErrorStream(true).start();
			var output = new String(process.getInputStream().readAllBytes());
			assertTrue(process.waitFor(60, TimeUnit.SECONDS));
			assertEquals(0, process.exitValue(), output);
			assertTrue(Files.exists(othersCopy), "a copy in a shared directory behind a symlink was deleted: " + output);
			assertTrue(output.contains("node: [librocksdbjni"), output);
		}
	}

	/** Run by {@link #theRocksDbLibraryIsExtractedIntoTheDataDirectory} in its own JVM, where RocksDB is not loaded. */
	public static final class LoadLibrary {
		public static void main(String[] args) throws Exception {
			var dataDir = Path.of(args[0]);
			ReplicatedServerMain.useNodeTemporaryDirectory(dataDir, Boolean.parseBoolean(args[2]));
			System.out.println("tmpdir: " + System.getProperty("java.io.tmpdir"));
			RocksDB.loadLibrary();
			try (var options = new Options()) {
				options.setCreateIfMissing(true);
			}
			System.out.println("node: " + names(dataDir.resolve("tmp")));
			System.out.println("jdk: " + names(Path.of(args[1])));
		}

		private static List<String> names(Path directory) throws IOException {
			try (var files = Files.list(directory)) {
				return files.map(path -> path.getFileName().toString()).filter(name -> name.contains("rocksdbjni"))
						.sorted().toList();
			}
		}
	}

	/**
	 * CR2-6: a database whose cluster version has a capability this version lacks (a downgrade, or a newer cluster's
	 * backup restored here) is refused at the start, with an exception of its own ({@code run} exits with 5).
	 */
	@Test
	void aDatabaseOfANewerClusterVersionIsRefused() throws Exception {
		try (var cluster = new InProcessCluster(Files.createDirectories(tempDir.resolve("cluster")), 1, config -> config).startAll()) {
			cluster.awaitLeader(Duration.ofSeconds(30));
			InProcessCluster.await(Duration.ofSeconds(30),
					() -> cluster.node("n0").stateMachine().ops().clusterVersion() == Capabilities.SUPPORTED);
			cluster.stop("n0");
			var config = cluster.config("n0");
			long newer = Capabilities.SUPPORTED | (1L << 40);
			writeClusterVersion(config, newer);
			var refused = assertThrows(ReplicatedNode.NewerVersionException.class, () -> cluster.start("n0"));
			assertTrue(refused.getMessage().contains("0x10000000000"), refused.getMessage());
			// Nothing was changed by the refusal: the same database starts again once its version is known.
			writeClusterVersion(config, Capabilities.SUPPORTED);
			cluster.start("n0");
			cluster.awaitLeader(Duration.ofSeconds(30));
		}
	}

	private static void writeClusterVersion(NodeConfig config, long version) throws IOException {
		try (var store = NodeStore.open(config.dbDir(), config.rockserverConfig(), "test")) {
			store.db().put(0L, store.metaColumnId(), NodeStore.key(NodeStore.CLUSTER_VERSION),
					Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(version).array()),
					RequestType.none());
			assertEquals(version, store.clusterVersion());
		}
	}

	@Test
	void directMemoryErrorsAreRecognized() {
		assertTrue(DirectMemoryWatch.isDirectMemoryError("io.netty.util.internal.OutOfDirectMemoryError", "failed"));
		assertTrue(DirectMemoryWatch.isDirectMemoryError(
				"org.apache.ratis.thirdparty.io.netty.util.internal.OutOfDirectMemoryError", null));
		assertTrue(DirectMemoryWatch.isDirectMemoryError("java.lang.OutOfMemoryError",
				"Cannot reserve 1024 bytes of direct buffer memory (allocated: 10, limit: 20)"));
		assertFalse(DirectMemoryWatch.isDirectMemoryError("java.lang.OutOfMemoryError", "Java heap space"));
		assertFalse(DirectMemoryWatch.isDirectMemoryError("java.lang.StackOverflowError", null));
	}

	/** DD-27: an error made in Java code, which -XX:+ExitOnOutOfMemoryError ignores, reaches the watch. */
	@Test
	void theWatchSeesADirectMemoryErrorAndNothingElse() throws Exception {
		var seen = new CompletableFuture<String>();
		try (var watch = DirectMemoryWatch.start(seen::complete)) {
			if (watch == null) {
				return;
			}
			// JFR records once the stream runs: make errors until one arrives.
			var unrelated = new StackOverflowError("unrelated");
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
			while (!seen.isDone() && System.nanoTime() - deadline < 0) {
				// On a thread named like an appender of Ratis: only those are fatal (CR2-1, see DirectMemoryWatchTest).
				var thread = Thread.ofPlatform().name("n0@group-1->n1-GrpcLogAppender-LogAppenderDaemon").start(() -> {
					var error = new OutOfMemoryError("Cannot reserve 8 bytes of direct buffer memory (allocated: 1, limit: 2)");
					assertTrue(error.getMessage().contains("direct"), unrelated.getMessage());
				});
				thread.join();
				Thread.sleep(200);
			}
			var description = seen.get(1, TimeUnit.SECONDS);
			assertTrue(description.contains("direct buffer memory"), description);
		}
	}

	/**
	 * DD-26: {@code run} refuses a JVM without -XX:+ExitOnOutOfMemoryError before it starts anything. One short JVM that
	 * exits at once, not a cluster.
	 */
	@Test
	void runRefusesAJvmWithoutExitOnOutOfMemory() throws Exception {
		var command = List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
				"-cp", System.getProperty("java.class.path"),
				"-Djava.io.tmpdir=" + tempDir,
				ReplicatedServerMain.class.getName(), "run", "--node-id", "a",
				"--peer", "a=127.0.0.1:1/127.0.0.1:2", "--data-dir", tempDir.resolve("node").toString());
		var process = new ProcessBuilder(command).redirectErrorStream(true).start();
		var output = new String(process.getInputStream().readAllBytes());
		assertTrue(process.waitFor(60, TimeUnit.SECONDS));
		assertEquals(2, process.exitValue(), output);
		assertTrue(output.contains("-XX:+ExitOnOutOfMemoryError"), output);
		assertFalse(Files.exists(tempDir.resolve("node/db")), "it started anyway");
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

	private static void deleteTree(Path root) throws IOException {
		try (Stream<Path> walk = Files.walk(root)) {
			for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
				Files.delete(path);
			}
		}
	}

	private static Path largestFile(Path root, String suffix) throws IOException {
		try (Stream<Path> walk = Files.walk(root)) {
			return walk.filter(path -> path.toString().endsWith(suffix)).max(Comparator.comparingLong(path -> {
				try {
					return Files.size(path);
				} catch (IOException e) {
					throw new UncheckedIOException(e);
				}
			})).orElseThrow();
		}
	}
}
