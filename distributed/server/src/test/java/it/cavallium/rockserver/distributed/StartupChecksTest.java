package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.RequestType;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
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
import org.rocksdb.ColumnFamilyDescriptor;
import org.rocksdb.ColumnFamilyHandle;
import org.rocksdb.ConfigOptions;
import org.rocksdb.DBOptions;
import org.rocksdb.DbPath;
import org.rocksdb.FlushOptions;
import org.rocksdb.Options;
import org.rocksdb.OptionsUtil;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

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

	/**
	 * R41-1: the marker a start-up check writes before its open keeps the seed digest only for files that open can have
	 * changed. After a killed check, files the open left keep the digest; a copy made again after it (here a partial one,
	 * then one of another seed) gets the digest of what it holds, as without the marker, so that genesis still finds it.
	 */
	@Test
	void theOpeningMarkerKeepsTheDigestOnlyForTheFilesThatOpenLeft() throws IOException {
		var seed = tempDir.resolve("seed");
		var other = tempDir.resolve("other");
		for (var dir : List.of(seed, other)) {
			try (var store = NodeStore.open(dir, null, "seed")) {
				long column = store.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
				for (int i = 0; i < (dir == seed ? 100 : 101); i++) {
					NodeStoreTest.put(store, column, i, "v" + i);
				}
			}
		}
		var node = tempDir.resolve("node/db");
		copy(seed, node);
		var digest = NodeStore.recordSeedDigest(node);
		NodeStore.noteSeedOpening(node);
		// The check's open, cut before noteSeedOpened: the files changed, the seed's table files are the same ones.
		startupCheckOpen(node);
		assertNotEquals(digest, NodeStore.databaseFilesDigest(node));
		assertEquals(digest, NodeStore.recordSeedDigest(node));
		assertEquals(digest, NodeStore.recordSeedDigest(node), "kept again by a retry");

		// The operator copies the seed again, partially: other files, with another digest.
		NodeStore.noteSeedOpening(node);
		deleteTree(node);
		copy(seed, node);
		Files.delete(largestFile(node, ".sst"));
		var partial = NodeStore.recordSeedDigest(node);
		assertNotEquals(digest, partial);

		// Another seed copied in after a killed check: another digest too.
		NodeStore.noteSeedOpening(node);
		deleteTree(node);
		copy(other, node);
		assertNotEquals(partial, NodeStore.recordSeedDigest(node));
		assertNotEquals(digest, NodeStore.readSeedDigest(node));

		// The same seed copied again matches its digest, marker or not.
		NodeStore.noteSeedOpening(node);
		deleteTree(node);
		copy(seed, node);
		assertEquals(digest, NodeStore.recordSeedDigest(node));
	}

	/**
	 * Review of R41, 1: after a killed start-up check, another database copied over {@code db/} without deleting it
	 * first ({@code cp -r other/. db/}, {@code rsync -a other/ db/} without {@code --delete}). Its table files have other
	 * numbers, so the seed's listed table files stay untouched next to them, and CURRENT, MANIFEST and IDENTITY are the
	 * other database's. It is not the seed: a new digest is recorded, and genesis stops it.
	 */
	@Test
	void anotherDatabaseCopiedOverTheSeedAfterAKilledCheckGetsANewDigest() throws IOException {
		var seed = tempDir.resolve("seed");
		var other = tempDir.resolve("other");
		try (var store = NodeStore.open(seed, null, "seed")) {
			long column = store.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
			for (int i = 0; i < 100; i++) {
				NodeStoreTest.put(store, column, i, "v" + i);
			}
		}
		// A longer history: its files have higher numbers than any of the seed's.
		for (int round = 0; round < 6; round++) {
			NodeStore.open(other, null, "other").close();
		}
		try (var store = NodeStore.open(other, null, "other")) {
			long column = store.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
			for (int i = 0; i < 100; i++) {
				NodeStoreTest.put(store, column, i, "w" + i);
			}
			store.db().flush();
		}
		for (boolean inPlace : List.of(true, false)) {
			var node = tempDir.resolve((inPlace ? "in-place" : "renamed") + "/db");
			copy(seed, node);
			var digest = NodeStore.recordSeedDigest(node);
			NodeStore.noteSeedOpening(node);
			startupCheckOpen(node);
			assertEquals(digest, NodeStore.recordSeedDigest(node), "the killed check alone");
			var seedTables = fileNames(node, ".sst");
			var otherTables = fileNames(other, ".sst");
			assertFalse(otherTables.isEmpty());
			assertTrue(seedTables.stream().noneMatch(otherTables::contains), seedTables + " " + otherTables);

			NodeStore.noteSeedOpening(node);
			copyOver(other, node, inPlace);
			assertTrue(fileNames(node, ".sst").containsAll(seedTables), "the seed's table files are still there");
			assertNotEquals(digest, NodeStore.recordSeedDigest(node), (inPlace ? "cp" : "rsync") + " of another database");
		}
	}

	/**
	 * R43-1: after a killed start-up check, the operator lets the seed's source database run on (here flushes that its
	 * own compaction folds into a new table file, deleting one the marker listed), stops it, and copies it again with
	 * {@code rsync -a --delete}: the quick check (size and modification time) skips IDENTITY and every table file the
	 * source still holds, so they keep their inode and change time; {@code --delete} removes the rest. That is not the
	 * seed: a new digest is recorded. A listed table file gone is enough to tell, because the check's open never deletes
	 * one ({@link #theStartupCheckOpenDeletesNoListedTableFile}).
	 */
	@Test
	void aLaterCopyOfTheSourceByRsyncDeleteAfterAKilledCheckGetsANewDigest() throws Exception {
		var source = tempDir.resolve("source");
		try (var store = NodeStore.open(source, null, "source")) {
			long kept = store.db().createColumn("kept", NodeStoreTest.DATA_SCHEMA);
			long changed = store.db().createColumn("changed", NodeStoreTest.DATA_SCHEMA);
			for (int i = 0; i < 100; i++) {
				NodeStoreTest.put(store, kept, i, "v" + i);
				NodeStoreTest.put(store, changed, i, "v" + i);
			}
		}
		var node = tempDir.resolve("node/db");
		rsyncDelete(source, node);
		var digest = NodeStore.recordSeedDigest(node);
		NodeStore.noteSeedOpening(node);
		startupCheckOpen(node);
		assertEquals(digest, NodeStore.recordSeedDigest(node), "the killed check alone");

		// The source runs on: new values for the same keys of one column, flushed next to the old ones, compacted.
		var seedTables = fileNames(source, ".sst");
		try (var store = NodeStore.open(source, null, "source")) {
			long changed = store.db().createColumn("changed", NodeStoreTest.DATA_SCHEMA);
			// Four L0 files in all reach the compaction trigger of the default config.
			for (int round = 0; round < 3; round++) {
				for (int i = 0; i < 100; i++) {
					NodeStoreTest.put(store, changed, i, "w" + round + "-" + i);
				}
				store.db().flush();
			}
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
			while (fileNames(source, ".sst").containsAll(seedTables)) {
				assertTrue(System.nanoTime() < deadline, "the source compacted none of " + seedTables);
				Thread.sleep(20);
			}
		}
		var left = fileNames(source, ".sst").stream().filter(seedTables::contains).toList();
		assertFalse(left.isEmpty(), "the source kept none of " + seedTables);

		// The marker of the killed check is still there: the retry above kept the digest without writing another.
		rsyncDelete(source, node);
		assertTrue(fileNames(node, ".sst").containsAll(left));
		assertNotEquals(digest, NodeStore.recordSeedDigest(node), "a later copy of the source with rsync --delete");
	}

	/**
	 * Review of R43, 1: two first starts killed in a row inside the start-up check's open. The first was killed during
	 * its WAL replay, after it wrote a recovery table file but before it committed it to the MANIFEST (RocksDB applies
	 * the recovery edits at the end of recovery): here a stray table file numbered above any the MANIFEST knows. The
	 * second open deletes it as obsolete (or would write the same number again). The marker of the second start must
	 * list the seed's files, carried over from the first marker, not that stray file: the third start keeps the digest.
	 */
	@Test
	void twoKilledChecksInARowKeepTheSeedDigestDespiteAnUncommittedTableFile() throws IOException {
		var seed = tempDir.resolve("seed");
		try (var store = NodeStore.open(seed, null, "seed")) {
			long column = store.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
			for (int i = 0; i < 100; i++) {
				NodeStoreTest.put(store, column, i, "v" + i);
			}
		}
		var node = tempDir.resolve("node/db");
		copy(seed, node);
		var digest = NodeStore.recordSeedDigest(node);
		NodeStore.noteSeedOpening(node);
		var marker = Files.readString(node.resolveSibling("db.seed-digest"));

		// The first check, killed during its WAL replay: a recovery table file the MANIFEST does not reference yet.
		var stray = node.resolve("999999.sst");
		Files.copy(largestFile(node, ".sst"), stray);
		assertEquals(digest, NodeStore.recordSeedDigest(node), "the first killed check");

		// The second check: its marker lists the seed's files, then its open deletes the stray file, and it is killed.
		NodeStore.noteSeedOpening(node);
		startupCheckOpen(node);
		assertFalse(Files.exists(stray), "the open deleted the uncommitted table file");
		assertEquals(digest, NodeStore.recordSeedDigest(node), "the second killed check");
		assertEquals(marker, Files.readString(node.resolveSibling("db.seed-digest")),
				"the marker of a kept digest is carried over");

		// A third one, killed again, still keeps it.
		NodeStore.noteSeedOpening(node);
		startupCheckOpen(node);
		assertEquals(digest, NodeStore.recordSeedDigest(node), "the third killed check");
	}

	/**
	 * R45-1: a table file shared by hard links with something outside {@code db/} (a backup copy that restore-backup
	 * linked on the same file system, the source of an export, a seed spread with {@code cp -al}) gets a new change time
	 * whenever any other link to it is created or removed: Linux updates the inode's ctime on {@code link()} and
	 * {@code unlink()}. After a killed check the operator deletes the backup copy (one link fewer per shared file) and
	 * links another copy (one more): neither changes the files the check's open left, so the digest is kept. A table
	 * file rewritten in place with its own size and modification time ({@code cp -p} over it), its link count
	 * unchanged, is still another file.
	 */
	@Test
	void aKilledCheckKeepsTheDigestWhenOutsideHardLinksToTheSeedComeAndGo() throws IOException {
		org.junit.jupiter.api.Assumptions.assumeTrue(birthTimeKnown(tempDir),
				"the file system of " + tempDir + " keeps no birth time");
		var seed = tempDir.resolve("seed");
		try (var store = NodeStore.open(seed, null, "seed")) {
			long column = store.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
			// Three table files in the data column (below the L0 compaction trigger).
			for (int round = 0; round < 3; round++) {
				for (int i = 0; i < 100; i++) {
					NodeStoreTest.put(store, column, i, "v" + round + "-" + i);
				}
				store.db().flush();
			}
		}
		var node = tempDir.resolve("node/db");
		copy(seed, node);
		var untouched = largestFile(node, ".sst");
		var shared = new ArrayList<Path>();
		try (Stream<Path> walk = Files.walk(node)) {
			walk.filter(path -> path.toString().endsWith(".sst") && !path.equals(untouched)).forEach(shared::add);
		}
		shared.add(node.resolve("IDENTITY"));
		assertTrue(shared.size() >= 3, shared.toString());
		// Half of them linked from a backup copy before the first start, the other half not yet.
		var backup = Files.createDirectories(tempDir.resolve("backup"));
		var later = Files.createDirectories(tempDir.resolve("later"));
		for (int i = 0; i < shared.size(); i += 2) {
			Files.createLink(backup.resolve(shared.get(i).getFileName()), shared.get(i));
		}
		var digest = NodeStore.recordSeedDigest(node);
		NodeStore.noteSeedOpening(node);
		startupCheckOpen(node);

		// Before the restart: the backup copy deleted, another copy linked.
		deleteTree(backup);
		for (int i = 1; i < shared.size(); i += 2) {
			Files.createLink(later.resolve(shared.get(i).getFileName()), shared.get(i));
		}
		assertEquals(digest, NodeStore.recordSeedDigest(node), "outside links removed and added after a killed check");
		assertEquals(digest, NodeStore.recordSeedDigest(node), "kept again by a retry");

		// One listed table file written again in place with its own size and modification time, nothing linked.
		var modified = Files.getLastModifiedTime(untouched);
		Files.write(untouched, Files.readAllBytes(untouched));
		Files.setLastModifiedTime(untouched, modified);
		assertNotEquals(digest, NodeStore.recordSeedDigest(node), "a table file written again in place");
	}

	/**
	 * R45-1, the rule itself on {@code <size> <inode>@<ctime>,<links>,<mtime>,<birth time>} values: only a change of
	 * the link count explains a new change time, and only when the inode, size, modification time and birth time are
	 * the same and the birth time is known.
	 */
	@Test
	void aTableFileIsTheSameDespiteANewChangeTimeOnlyWhenOnlyItsLinkCountChanged() {
		String t1 = "2026-11-09T10:00:00.100000000Z";
		String t2 = "2026-11-09T11:00:00.200000000Z";
		String m = "2026-11-01T09:00:00.300000000Z";
		String b = "2026-11-01T08:59:59.400000000Z";
		String b2 = "2026-11-09T10:59:59.500000000Z";
		String before = "4096 77@" + t1 + ",2," + m + "," + b;
		assertTrue(NodeStore.sameTable(before, before));
		assertTrue(NodeStore.sameTable(before, "4096 -"), "unknown identity");
		assertTrue(NodeStore.sameTable(before, "4096 77@" + t2 + ",1," + m + "," + b), "an outside link removed");
		assertTrue(NodeStore.sameTable(before, "4096 77@" + t2 + ",3," + m + "," + b), "an outside link added");
		assertFalse(NodeStore.sameTable(before, "4096 77@" + t2 + ",2," + m + "," + b),
				"written again in place, its modification time set back (cp -p over it)");
		assertFalse(NodeStore.sameTable(before, "4096 77@" + t2 + ",1," + m + "," + b2),
				"a copy made again after rm -r on the reused inode, modification time kept, fewer links");
		assertFalse(NodeStore.sameTable("4096 77@" + t1 + ",2," + m + ",-", "4096 77@" + t2 + ",1," + m + ",-"),
				"no birth time to tell a reused inode");
		assertFalse(NodeStore.sameTable(before, "4096 77@" + t2 + ",1," + t2 + "," + b), "written again");
		assertFalse(NodeStore.sameTable(before, "4096 78@" + t2 + ",1," + m + "," + b), "another inode");
		assertFalse(NodeStore.sameTable(before, "4097 77@" + t2 + ",1," + m + "," + b), "another size");
		assertFalse(NodeStore.sameTable(before, "4097 77@" + t1 + ",2," + m + "," + b), "another size");
		// A marker written before R45-1 lists the inode and change time only.
		assertTrue(NodeStore.sameTable("4096 77@" + t1, before), "an older marker, same file");
		assertFalse(NodeStore.sameTable("4096 77@" + t1, "4096 77@" + t2 + ",1," + m + "," + b),
				"an older marker, nothing to tell a link change");
	}

	/**
	 * Whether files under {@code dir} have a birth time: one whose modification time is set to the epoch still has
	 * another creation time (without one, Java gives the modification time, or the epoch).
	 */
	private static boolean birthTimeKnown(Path dir) throws IOException {
		var probe = Files.createTempFile(dir, "birth", ".probe");
		try {
			Files.setLastModifiedTime(probe, java.nio.file.attribute.FileTime.fromMillis(0));
			return Files.readAttributes(probe, java.nio.file.attribute.BasicFileAttributes.class).creationTime()
					.toMillis() != 0;
		} finally {
			Files.delete(probe);
		}
	}

	/**
	 * Like {@code rsync -a --delete source/ target/}: a file whose size and modification time are those of the source's
	 * is skipped (not touched); any other is copied to a new file, with the source's modification time, and renamed over
	 * the old one; what the source does not hold is deleted.
	 */
	private static void rsyncDelete(Path source, Path target) throws IOException {
		Files.createDirectories(target);
		try (Stream<Path> walk = Files.walk(source)) {
			for (Path path : walk.toList()) {
				Path destination = target.resolve(source.relativize(path).toString());
				if (Files.isDirectory(path)) {
					Files.createDirectories(destination);
				} else if (!Files.isRegularFile(destination) || Files.size(destination) != Files.size(path)
						|| !Files.getLastModifiedTime(destination).equals(Files.getLastModifiedTime(path))) {
					Path temporary = destination.resolveSibling("." + destination.getFileName() + ".tmp");
					Files.copy(path, temporary, java.nio.file.StandardCopyOption.REPLACE_EXISTING,
							java.nio.file.StandardCopyOption.COPY_ATTRIBUTES);
					Files.move(temporary, destination, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
				}
			}
		}
		try (Stream<Path> walk = Files.walk(target)) {
			for (Path path : walk.sorted(Comparator.reverseOrder()).toList()) {
				if (!Files.exists(source.resolve(target.relativize(path).toString()))) {
					Files.delete(path);
				}
			}
		}
	}

	/**
	 * R43-2: the start-up check's open deletes no table file the marker listed, so the keep rule can ask for every one of
	 * them. Its own compaction did: here the data column holds four overlapping L0 files, which the default compaction
	 * trigger (four L0 files) folds into one, deleting them. Compactions stay off whatever the node's config says.
	 */
	@Test
	void theStartupCheckOpenDeletesNoListedTableFile() throws Exception {
		var config = tempDir.resolve("no-compactions.conf");
		Files.writeString(config, "database.global.disable-auto-compactions = true\n");
		var seed = tempDir.resolve("seed");
		try (var store = NodeStore.open(seed, config, "seed")) {
			long column = store.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
			for (int round = 0; round < 4; round++) {
				for (int i = 0; i < 100; i++) {
					NodeStoreTest.put(store, column, i, "v" + round + "-" + i);
				}
				store.db().flush();
			}
		}
		var node = tempDir.resolve("node/db");
		copy(seed, node);
		var digest = NodeStore.recordSeedDigest(node);
		NodeStore.noteSeedOpening(node);
		var seedTables = fileNames(node, ".sst");
		assertTrue(seedTables.size() >= 2, seedTables.toString());
		// Killed while open, after its compaction (if any) had the time to run. The node's config turns compactions on
		// for the column itself, and asks for adaptive compaction I/O, which needs them: the check's open overrides both.
		var nodeConfig = tempDir.resolve("node.conf");
		Files.writeString(nodeConfig, """
				database.global.adaptive-compaction-io = true
				database.global.column-options = [ { name: "%s", disable-auto-compactions: false } ]
				""".formatted(NodeStoreTest.DATA_COLUMN));
		try (var store = NodeStore.openForStartupCheck(node, nodeConfig, "node")) {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
			while (fileNames(node, ".sst").containsAll(seedTables) && System.nanoTime() < deadline) {
				Thread.sleep(20);
			}
		}
		var gone = seedTables.stream().filter(name -> !fileNamesUnchecked(node, ".sst").contains(name)).toList();
		assertTrue(gone.isEmpty(), "the start-up check's open deleted " + gone + " of " + seedTables);
		assertEquals(digest, NodeStore.recordSeedDigest(node));
	}

	/**
	 * R44-1: the start-up check's open deletes no table file of Rockserver's internal columns either. They open with
	 * options of their own ({@code RocksDBLoader.getCompatibilityColumnOptions}), which read no column config: before,
	 * they kept RocksDB's automatic compactions (L0 trigger 4) whatever the check's config said. Here the seed's
	 * {@code _column_schemas_} holds five L0 files (each clean run that creates a column flushes one), which the open
	 * folded into one, deleting the files the marker listed. The OPTIONS file of the check's open shows every column
	 * without automatic compactions.
	 */
	@Test
	void theStartupCheckOpenDeletesNoTableFileOfAnInternalColumn() throws Exception {
		var seed = tempDir.resolve("seed");
		try (var store = NodeStore.open(seed, null, "seed")) {
			long column = store.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
			for (int i = 0; i < 100; i++) {
				NodeStoreTest.put(store, column, i, "v" + i);
			}
		}
		var schemaTables = addL0Files(seed, "_column_schemas_", 4);
		assertTrue(schemaTables.size() >= 4, schemaTables.toString());
		var node = tempDir.resolve("node/db");
		copy(seed, node);
		var digest = NodeStore.recordSeedDigest(node);
		NodeStore.noteSeedOpening(node);
		// Killed while open, after its compaction (if any) had the time to run.
		Path options;
		String optionsText;
		try (var store = startupCheckStore(node)) {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
			while (fileNames(node, ".sst").containsAll(schemaTables) && System.nanoTime() < deadline) {
				Thread.sleep(20);
			}
			options = newestOptionsFile(node);
			optionsText = Files.readString(options);
		}
		var gone = schemaTables.stream().filter(name -> !fileNamesUnchecked(node, ".sst").contains(name)).toList();
		assertTrue(gone.isEmpty(), "the start-up check's open deleted " + gone + " of " + schemaTables);
		for (var section : optionsText.split("\\n(?=\\[)")) {
			if (section.startsWith("[CFOptions ")) {
				assertTrue(section.contains("\n  disable_auto_compactions=true\n"),
						options + ": " + section.lines().findFirst().orElseThrow() + " compacts automatically");
			}
		}
		assertEquals(digest, NodeStore.recordSeedDigest(node));
	}

	/**
	 * Review of R44, 2: the variant R44-1 names. An exported seed has no meta column, so the start-up check's own
	 * {@code createColumn} of it writes a {@code _column_schemas_} row, which the check's close flushes into one more L0
	 * file: a seed with three such files reaches the L0 trigger (4) at that close. Neither that close nor a second check
	 * (the first one was killed after recording the marker, or the node stopped before the cluster formed) may compact
	 * the column and delete the files the marker listed.
	 */
	@Test
	void theStartupCheckOfAnExportedSeedDeletesNoTableFileOfColumnSchemas() throws Exception {
		var replica = tempDir.resolve("replica");
		try (var store = NodeStore.open(replica, null, "replica")) {
			long column = store.db().createColumn(NodeStoreTest.DATA_COLUMN, NodeStoreTest.DATA_SCHEMA);
			for (int i = 0; i < 100; i++) {
				NodeStoreTest.put(store, column, i, "v" + i);
			}
			NodeStoreTest.setApplied(store, TermIndex.valueOf(1, 5));
		}
		var seed = tempDir.resolve("seed");
		NodeStore.exportSeed(replica, null, seed);
		int exported = addL0Files(seed, "_column_schemas_", 0).size();
		assertTrue(exported >= 1 && exported <= 3, "the export left " + exported + " _column_schemas_ files");
		var schemaTables = addL0Files(seed, "_column_schemas_", 3 - exported);
		assertEquals(3, schemaTables.size(), schemaTables.toString());
		var node = tempDir.resolve("node/db");
		copy(seed, node);
		var digest = NodeStore.recordSeedDigest(node);
		NodeStore.noteSeedOpening(node);
		// The first check creates the meta column; its close flushes the schema row into a fourth L0 file.
		startupCheckOpen(node);
		var afterFirst = addL0FilesUnchanged(node);
		assertTrue(afterFirst.containsAll(schemaTables) && afterFirst.size() == 4,
				"after the first check's close: " + afterFirst + ", the seed's: " + schemaTables);
		// A second check, killed while open after its compaction (if any) had the time to run.
		try (var store = startupCheckStore(node)) {
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(3);
			while (fileNames(node, ".sst").containsAll(schemaTables) && System.nanoTime() < deadline) {
				Thread.sleep(20);
			}
		}
		var gone = schemaTables.stream().filter(name -> !fileNamesUnchecked(node, ".sst").contains(name)).toList();
		assertTrue(gone.isEmpty(), "a start-up check deleted " + gone + " of " + schemaTables);
		assertEquals(digest, NodeStore.recordSeedDigest(node));
	}

	/** The names of the {@code _column_schemas_} table files of the closed database {@code dbDir}, nothing written. */
	private static List<String> addL0FilesUnchanged(Path dbDir) throws RocksDBException {
		return addL0Files(dbDir, "_column_schemas_", 0);
	}

	/**
	 * Writes every entry of the column {@code columnName} of the closed replica database {@code dbDir} again and
	 * flushes it, {@code times} times, through RocksDB itself with automatic compactions off: one more L0 file each
	 * time, the entries unchanged. Returns the names of that column's table files.
	 */
	private static List<String> addL0Files(Path dbDir, String columnName, int times) throws RocksDBException {
		RocksDB.loadLibrary();
		// The options the database was written with, its column volume (which the OPTIONS file does not record) and
		// automatic compactions off.
		try (var configOptions = new ConfigOptions().setIgnoreUnknownOptions(true);
				var dbOptions = new DBOptions();
				var flushOptions = new FlushOptions().setWaitForFlush(true)) {
			var descriptors = new ArrayList<ColumnFamilyDescriptor>();
			OptionsUtil.loadLatestOptions(configOptions, dbDir.toString(), dbOptions, descriptors);
			// An exported seed's OPTIONS file names the directory export-seed opened it in (<target>.tmp): the same WAL and
			// info log directories in dbDir.
			if (!dbOptions.walDir().isEmpty()) {
				dbOptions.setWalDir(dbDir.resolve(Path.of(dbOptions.walDir()).getFileName()).toString());
			}
			if (!dbOptions.dbLogDir().isEmpty()) {
				dbOptions.setDbLogDir(dbDir.resolve(Path.of(dbOptions.dbLogDir()).getFileName()).toString());
			}
			for (var descriptor : descriptors) {
				descriptor.getOptions().setDisableAutoCompactions(true)
						.setCfPaths(List.of(new DbPath(dbDir.resolve("volume"), Long.MAX_VALUE)));
			}
			var handles = new ArrayList<ColumnFamilyHandle>();
			try (var db = RocksDB.open(dbOptions, dbDir.toString(), descriptors, handles)) {
				try {
					var handle = handles.get(descriptors.stream()
							.map(descriptor -> new String(descriptor.getName(), StandardCharsets.UTF_8)).toList()
							.indexOf(columnName));
					for (int round = 0; round < times; round++) {
						try (var iterator = db.newIterator(handle)) {
							for (iterator.seekToFirst(); iterator.isValid(); iterator.next()) {
								db.put(handle, iterator.key(), iterator.value());
							}
						}
						db.flush(flushOptions, List.of(handle));
					}
					return db.getLiveFilesMetaData().stream()
							.filter(table -> new String(table.columnFamilyName(), StandardCharsets.UTF_8).equals(columnName))
							.map(table -> Path.of(table.fileName()).getFileName().toString()).toList();
				} finally {
					handles.forEach(ColumnFamilyHandle::close);
				}
			} finally {
				descriptors.forEach(descriptor -> descriptor.getOptions().close());
			}
		}
	}

	/** The OPTIONS file RocksDB wrote last in {@code dbDir}. */
	private static Path newestOptionsFile(Path dbDir) throws IOException {
		try (var files = Files.list(dbDir)) {
			return files.filter(path -> path.getFileName().toString().matches("OPTIONS-[0-9]+"))
					.max(Comparator.comparingLong(path -> Long.parseLong(path.getFileName().toString().substring(8))))
					.orElseThrow();
		}
	}

	/** The start-up check's open and close of {@code dbDir}, as {@code ReplicatedNode} makes it. */
	private static void startupCheckOpen(Path dbDir) throws IOException {
		startupCheckStore(dbDir).close();
	}

	private static NodeStore startupCheckStore(Path dbDir) throws IOException {
		return NodeStore.openForStartupCheck(dbDir, null, "node");
	}

	private static List<String> fileNamesUnchecked(Path root, String suffix) {
		try {
			return fileNames(root, suffix);
		} catch (IOException e) {
			throw new UncheckedIOException(e);
		}
	}

	/** The names of the files under {@code root} that end with {@code suffix}. */
	private static List<String> fileNames(Path root, String suffix) throws IOException {
		try (Stream<Path> walk = Files.walk(root)) {
			return walk.map(path -> path.getFileName().toString()).filter(name -> name.endsWith(suffix)).toList();
		}
	}

	/**
	 * Copies {@code source} over {@code target} without deleting what it holds: in place, as {@code cp} (same inode), or
	 * as {@code rsync} (a new file renamed over the old one).
	 */
	private static void copyOver(Path source, Path target, boolean inPlace) throws IOException {
		try (Stream<Path> walk = Files.walk(source)) {
			for (Path path : walk.toList()) {
				Path destination = target.resolve(source.relativize(path).toString());
				if (Files.isDirectory(path)) {
					Files.createDirectories(destination);
				} else if (inPlace) {
					Files.write(destination, Files.readAllBytes(path));
				} else {
					Path temporary = destination.resolveSibling("." + destination.getFileName() + ".tmp");
					Files.copy(path, temporary, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
					Files.move(temporary, destination, java.nio.file.StandardCopyOption.REPLACE_EXISTING);
				}
			}
		}
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

	/**
	 * R28 review 3: a malformed value of {@code run} (a number that is not one, an {@code --election-timeout-ms} without
	 * MIN:MAX, a value that the node's configuration refuses, a malformed {@code --peer}) is wrong usage, exit 2 with the
	 * usage, like an unknown option, and not a stack trace with 1. Short JVMs that exit at once.
	 */
	@Test
	void runRefusesMalformedValuesWithTwo() throws Exception {
		for (var bad : List.of(List.of("--snapshot-every", "often"), List.of("--election-timeout-ms", "1000"),
				List.of("--parallel-apply-threads", "1"), List.of("--peer", "b=nowhere"),
				// R29-4: a flag given a value turned the flag on, and the value was ignored; so was any stray word.
				List.of("--join", "false"), List.of("--pipelined-updates", "false"), List.of("extra"))) {
			var command = new java.util.ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
					"-cp", System.getProperty("java.class.path"),
					"-Djava.io.tmpdir=" + tempDir, "-XX:+ExitOnOutOfMemoryError",
					ReplicatedServerMain.class.getName(), "run", "--node-id", "a",
					"--peer", "a=127.0.0.1:1/127.0.0.1:2", "--data-dir", tempDir.resolve("node").toString()));
			command.addAll(bad);
			var log = tempDir.resolve("run.log");
			var process = new ProcessBuilder(command).redirectErrorStream(true).redirectOutput(log.toFile()).start();
			// A node that started anyway is stopped.
			if (!process.waitFor(60, TimeUnit.SECONDS)) {
				process.destroyForcibly().waitFor();
				fail(bad + ": still running after 60 s: " + Files.readString(log));
			}
			var output = Files.readString(log);
			assertEquals(2, process.exitValue(), bad + ": " + output);
			assertTrue(output.contains("Usage:"), bad + ": " + output);
			assertFalse(Files.exists(tempDir.resolve("node/db")), "it started anyway");
		}
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
