package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.stream.Stream;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

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
		Files.write(sst, java.util.Arrays.copyOf(Files.readAllBytes(sst), (int) Files.size(sst) - 1));
		assertNotEquals(digest, NodeStore.recordSeedDigest(truncated));

		// Recorded once: opening the database changes its files, not the recorded digest.
		NodeStore.open(copyA, null, "a").close();
		assertNotEquals(digest, NodeStore.databaseFilesDigest(copyA));
		assertEquals(digest, NodeStore.recordSeedDigest(copyA));
		try (var store = NodeStore.open(copyA, null, "a")) {
			assertEquals(digest, store.seedDigest());
		}
		assertNull(NodeStore.recordSeedDigest(tempDir.resolve("empty/db")));
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
				var error = new OutOfMemoryError("Cannot reserve 8 bytes of direct buffer memory (allocated: 1, limit: 2)");
				assertTrue(error.getMessage().contains("direct"), unrelated.getMessage());
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

	private static Path largestFile(Path root, String suffix) throws IOException {
		try (Stream<Path> walk = Files.walk(root)) {
			return walk.filter(path -> path.toString().endsWith(suffix)).max(Comparator.comparingLong(path -> {
				try {
					return Files.size(path);
				} catch (IOException e) {
					throw new java.io.UncheckedIOException(e);
				}
			})).orElseThrow();
		}
	}
}
