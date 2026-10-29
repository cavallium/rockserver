package it.cavallium.rockserver.distributed;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.stream.Stream;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.jetbrains.annotations.Nullable;

/**
 * Ratis snapshot storage holding exactly one snapshot: a RocksDB checkpoint.
 *
 * <p>Layout, under the Ratis state machine directory: {@code snapshot/db/} (the checkpoint) and
 * {@code snapshot/snapshot.properties} (term, index; written last and fsynced, so its presence marks a complete
 * snapshot). New snapshots are built in {@code tmp/} and swapped in by rename. Ratis' own snapshot installation
 * writes the leader's files into {@link #getSnapshotDir()}, so an installed snapshot has the same layout.</p>
 *
 * <p>The listed files carry no digest: Ratis' leader computes each file's MD5 while streaming it and the follower
 * verifies it, saving a {@code <file>.md5} next to every installed file. Those side files are not listed, so a
 * follower that later leads does not send them on.</p>
 */
final class CheckpointStorage implements StateMachineStorage {

	private static final String SNAPSHOT_DIR = "snapshot";
	private static final String TMP_DIR = "tmp";
	private static final String DB_DIR = "db";
	private static final String PROPERTIES_FILE = "snapshot.properties";
	/** Where {@link #take} moves the previous snapshot while swapping; Ratis' install uses {@code snapshot.tmp*}. */
	private static final String OLD_SNAPSHOT_DIR = SNAPSHOT_DIR + ".old";

	private Path stateMachineDir;
	private Path snapshotDir;
	private Path tmpDir;
	private volatile @Nullable FileListSnapshotInfo latest;

	@Override
	public synchronized void init(RaftStorage raftStorage) throws IOException {
		stateMachineDir = raftStorage.getStorageDir().getStateMachineDir().toPath().toAbsolutePath();
		snapshotDir = stateMachineDir.resolve(SNAPSHOT_DIR);
		tmpDir = stateMachineDir.resolve(TMP_DIR);
		Files.createDirectories(stateMachineDir);
		// No snapshot is being built or installed before the server starts: leftovers are from a crash.
		NodeStore.deleteRecursively(tmpDir);
		Files.createDirectories(tmpDir);
		recoverInterruptedSwap();
		reload();
	}

	/**
	 * A crash while a snapshot was swapped in (by {@link #take} or by Ratis' install) can leave the previous
	 * snapshot in a {@code snapshot.*} sibling: keep it only if the current snapshot is missing or incomplete.
	 */
	private void recoverInterruptedSwap() throws IOException {
		List<Path> asides;
		try (Stream<Path> siblings = Files.list(stateMachineDir)) {
			asides = siblings
					.filter(path -> path.getFileName().toString().startsWith(SNAPSHOT_DIR + "."))
					.filter(Files::isDirectory)
					.toList();
		}
		if (readTermIndex(snapshotDir) == null) {
			Path best = null;
			TermIndex bestTermIndex = null;
			for (Path aside : asides) {
				TermIndex termIndex = readTermIndex(aside);
				if (termIndex != null && (bestTermIndex == null || termIndex.compareTo(bestTermIndex) > 0)) {
					best = aside;
					bestTermIndex = termIndex;
				}
			}
			if (best != null) {
				NodeStore.deleteRecursively(snapshotDir);
				Files.move(best, snapshotDir);
				NodeStore.syncDirectory(stateMachineDir);
			}
		}
		for (Path aside : asides) {
			NodeStore.deleteRecursively(aside);
		}
	}

	/** The latest complete snapshot, listing every file under the snapshot directory by absolute path. */
	@Override
	public @Nullable FileListSnapshotInfo getLatestSnapshot() {
		return latest;
	}

	/**
	 * Re-reads the snapshot directory, e.g. after Ratis installed a snapshot from the leader. The snapshot's files
	 * are fsynced first, because Ratis does not sync the files it installs.
	 */
	synchronized void reload() throws IOException {
		TermIndex termIndex = readTermIndex(snapshotDir);
		if (termIndex == null) {
			latest = null;
			return;
		}
		NodeStore.syncTree(snapshotDir);
		latest = new FileListSnapshotInfo(listFiles(), termIndex);
	}

	/** Checkpoints {@code store} into a new snapshot that replaces the previous one. */
	synchronized TermIndex take(NodeStore store) throws IOException {
		Path building = tmpDir.resolve("snapshot-" + store.applied().getIndex() + "-" + System.nanoTime());
		TermIndex termIndex;
		try {
			termIndex = store.checkpoint(building.resolve(DB_DIR));
			writeProperties(building.resolve(PROPERTIES_FILE), termIndex);
			NodeStore.syncDirectory(building);
		} catch (IOException | RuntimeException failure) {
			try {
				NodeStore.deleteRecursively(building);
			} catch (IOException cleanupFailure) {
				failure.addSuppressed(cleanupFailure);
			}
			throw failure;
		}

		Path old = stateMachineDir.resolve(OLD_SNAPSHOT_DIR);
		NodeStore.deleteRecursively(old);
		if (Files.exists(snapshotDir)) {
			Files.move(snapshotDir, old);
		}
		Files.move(building, snapshotDir);
		NodeStore.syncDirectory(stateMachineDir);
		NodeStore.syncDirectory(tmpDir);
		latest = new FileListSnapshotInfo(listFiles(), termIndex);
		NodeStore.deleteRecursively(old);
		return termIndex;
	}

	/** Directory of the latest snapshot's checkpoint, for {@link NodeStore#restore}. */
	Path snapshotDbDir() {
		return snapshotDir.resolve(DB_DIR);
	}

	@Override
	public void format() throws IOException {
	}

	@Override
	public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) {
		// Only one snapshot is ever kept.
	}

	@Override
	public File getSnapshotDir() {
		return snapshotDir.toFile();
	}

	@Override
	public File getTmpDir() {
		return tmpDir.toFile();
	}

	/** Every regular file of the snapshot except Ratis' digest side files, with the properties file last. */
	private List<FileInfo> listFiles() throws IOException {
		List<Path> dbFiles;
		try (Stream<Path> walk = Files.walk(snapshotDbDir())) {
			dbFiles = walk
					.filter(Files::isRegularFile)
					.filter(path -> !path.getFileName().toString().endsWith(".md5"))
					.sorted()
					.toList();
		}
		var files = new ArrayList<FileInfo>(dbFiles.size() + 1);
		for (Path path : dbFiles) {
			files.add(new FileInfo(path, null));
		}
		files.add(new FileInfo(snapshotDir.resolve(PROPERTIES_FILE), null));
		return files;
	}

	private static void writeProperties(Path file, TermIndex termIndex) throws IOException {
		String content = "term=" + termIndex.getTerm() + "\nindex=" + termIndex.getIndex() + "\n";
		Files.writeString(file, content, StandardCharsets.UTF_8);
		NodeStore.syncFile(file);
	}

	/** The snapshot position stored in {@code dir}, or null if {@code dir} holds no complete snapshot. */
	private static @Nullable TermIndex readTermIndex(Path dir) throws IOException {
		Path file = dir.resolve(PROPERTIES_FILE);
		if (!Files.isRegularFile(file) || !Files.isDirectory(dir.resolve(DB_DIR))) {
			return null;
		}
		var properties = new Properties();
		properties.load(new StringReader(Files.readString(file, StandardCharsets.UTF_8)));
		String term = properties.getProperty("term");
		String index = properties.getProperty("index");
		if (term == null || index == null) {
			return null;
		}
		try {
			return TermIndex.valueOf(Long.parseLong(term), Long.parseLong(index));
		} catch (NumberFormatException e) {
			return null;
		}
	}
}
