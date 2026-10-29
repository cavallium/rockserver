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
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.statemachine.StateMachineStorage;
import org.apache.ratis.statemachine.impl.FileListSnapshotInfo;
import org.apache.ratis.util.MD5FileUtil;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Ratis snapshot storage whose snapshots are RocksDB checkpoints.
 *
 * <p>Layout, under the Ratis state machine directory: {@code snapshots/} ({@link #getSnapshotDir()}) holds one
 * directory per snapshot, {@code snapshot-<term>-<index>/}, with {@code db/} (the checkpoint) and
 * {@code snapshot.properties} (term, index). New snapshots are built in {@code tmp/} and renamed into
 * {@code snapshots/} once complete and fsynced. The latest snapshot is the usable one with the highest
 * position.</p>
 *
 * <p>A path is never reused for different content: a leader streams a snapshot to a follower from the file list
 * it got when the installation started, opening each file only when it reaches it. {@link #take} also keeps the
 * previous snapshot until the next one is taken, so an installation survives one newer snapshot.</p>
 *
 * <p>Ratis' own installation writes the leader's files into a new directory, at the same paths relative to
 * {@link #getSnapshotDir()}, and swaps it in place of that directory. It saves a {@code <file>.md5} next to every
 * file but syncs nothing, so an installed snapshot is used only once each file matches its saved digest; it is
 * then fsynced and marked as verified. The listed files carry no digest (the leader computes it while streaming),
 * and the side files are not listed, so a follower that later leads does not send them on.</p>
 */
final class CheckpointStorage implements StateMachineStorage {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointStorage.class);
	private static final String SNAPSHOTS_DIR = "snapshots";
	private static final String TMP_DIR = "tmp";
	private static final String SNAPSHOT_PREFIX = "snapshot-";
	private static final String DB_DIR = "db";
	private static final String PROPERTIES_FILE = "snapshot.properties";
	/** Written into an installed snapshot once its files matched their digests and were fsynced. */
	private static final String VERIFIED_FILE = "installed.verified";

	private Path stateMachineDir;
	private Path snapshotsDir;
	private Path tmpDir;
	private @Nullable Path latestDir;
	private volatile @Nullable FileListSnapshotInfo latest;

	@Override
	public synchronized void init(RaftStorage raftStorage) throws IOException {
		stateMachineDir = raftStorage.getStorageDir().getStateMachineDir().toPath().toAbsolutePath();
		snapshotsDir = stateMachineDir.resolve(SNAPSHOTS_DIR);
		tmpDir = stateMachineDir.resolve(TMP_DIR);
		// No snapshot is being built or installed before the server starts: leftovers are from a crash.
		NodeStore.deleteRecursively(tmpDir);
		Files.createDirectories(tmpDir);
		Files.createDirectories(snapshotsDir);
		reload();
		for (Path aside : installAsides()) {
			if (latestDir == null && findLatest(aside) != null) {
				NodeStore.deleteRecursively(snapshotsDir);
				Files.move(aside, snapshotsDir);
				NodeStore.syncDirectory(stateMachineDir);
				reload();
			} else {
				NodeStore.deleteRecursively(aside);
			}
		}
		deleteSnapshotsExcept(latestDir, null);
	}

	/**
	 * Ratis installs a snapshot by moving {@code snapshots/} aside to a {@code snapshots.tmp*} sibling, renaming
	 * the new directory into its place and deleting the old one. After a crash in between, the old directory is
	 * used only if the current one has no usable snapshot.
	 */
	private List<Path> installAsides() throws IOException {
		try (Stream<Path> siblings = Files.list(stateMachineDir)) {
			return siblings
					.filter(path -> path.getFileName().toString().startsWith(SNAPSHOTS_DIR + "."))
					.filter(Files::isDirectory)
					.toList();
		}
	}

	/** The latest usable snapshot, listing every file under its directory by absolute path. */
	@Override
	public @Nullable FileListSnapshotInfo getLatestSnapshot() {
		return latest;
	}

	/**
	 * Re-reads the snapshot directory. The state machine calls it first thing after Ratis installed a snapshot
	 * from the leader: this verifies the installed files against their digests and fsyncs them.
	 */
	synchronized void reload() throws IOException {
		latestDir = findLatest(snapshotsDir);
		latest = latestDir == null ? null : new FileListSnapshotInfo(listFiles(latestDir), readTermIndex(latestDir));
	}

	/**
	 * Checkpoints {@code store} into a new snapshot that becomes the latest one. The previous snapshot is kept
	 * until the next call, older ones are deleted.
	 */
	synchronized TermIndex take(NodeStore store) throws IOException {
		Path building = tmpDir.resolve("building-" + System.nanoTime());
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

		Path target = snapshotsDir.resolve(SNAPSHOT_PREFIX + termIndex.getTerm() + "-" + termIndex.getIndex());
		Path previous = latestDir;
		if (target.equals(previous)) {
			// Nothing was applied since the latest snapshot, which stays as it is.
			NodeStore.deleteRecursively(building);
			return termIndex;
		}
		// Only an unusable leftover can be at this path.
		NodeStore.deleteRecursively(target);
		Files.move(building, target);
		NodeStore.syncDirectory(snapshotsDir);
		NodeStore.syncDirectory(tmpDir);
		latestDir = target;
		latest = new FileListSnapshotInfo(listFiles(target), termIndex);
		deleteSnapshotsExcept(target, previous);
		return termIndex;
	}

	/** Directory of the latest snapshot's checkpoint, for {@link NodeStore#restore}. */
	synchronized Path snapshotDbDir() {
		if (latestDir == null) {
			throw new IllegalStateException("No snapshot in " + snapshotsDir);
		}
		return latestDir.resolve(DB_DIR);
	}

	@Override
	public void format() throws IOException {
	}

	@Override
	public void cleanupOldSnapshots(SnapshotRetentionPolicy snapshotRetentionPolicy) {
		// take() deletes old snapshots itself.
	}

	@Override
	public File getSnapshotDir() {
		return snapshotsDir.toFile();
	}

	@Override
	public File getTmpDir() {
		return tmpDir.toFile();
	}

	private void deleteSnapshotsExcept(@Nullable Path kept, @Nullable Path alsoKept) throws IOException {
		for (Path dir : snapshotDirs(snapshotsDir)) {
			if (!dir.equals(kept) && !dir.equals(alsoKept)) {
				NodeStore.deleteRecursively(dir);
			}
		}
	}

	/** The usable snapshot with the highest position among the snapshot directories in {@code parent}. */
	private static @Nullable Path findLatest(Path parent) throws IOException {
		Path best = null;
		TermIndex bestTermIndex = null;
		for (Path dir : snapshotDirs(parent)) {
			TermIndex termIndex = readTermIndex(dir);
			if (termIndex != null && (bestTermIndex == null || termIndex.compareTo(bestTermIndex) > 0)
					&& isVerified(dir)) {
				best = dir;
				bestTermIndex = termIndex;
			}
		}
		return best;
	}

	private static List<Path> snapshotDirs(Path parent) throws IOException {
		if (!Files.isDirectory(parent)) {
			return List.of();
		}
		try (Stream<Path> children = Files.list(parent)) {
			return children
					.filter(path -> path.getFileName().toString().startsWith(SNAPSHOT_PREFIX))
					.filter(Files::isDirectory)
					.sorted()
					.toList();
		}
	}

	/**
	 * True for a snapshot taken locally (fsynced before it was renamed into place) or an installed one verified
	 * earlier. Otherwise the snapshot was installed by Ratis, and every file must match its saved digest, which
	 * fails if a crash lost data that Ratis never synced; the files are then fsynced and the snapshot marked.
	 */
	private static boolean isVerified(Path dir) throws IOException {
		boolean installed;
		try (Stream<Path> walk = Files.walk(dir)) {
			installed = walk.anyMatch(path -> path.getFileName().toString().endsWith(MD5FileUtil.MD5_SUFFIX));
		}
		if (!installed || Files.exists(dir.resolve(VERIFIED_FILE))) {
			return true;
		}
		for (Path file : snapshotFiles(dir)) {
			try {
				MD5Hash saved = MD5FileUtil.readStoredMd5ForFile(file.toFile());
				if (saved == null || !saved.equals(MD5FileUtil.computeMd5ForFile(file.toFile()))) {
					LOG.warn("Ignoring installed snapshot {}: {} does not match its saved digest", dir, file);
					return false;
				}
			} catch (IOException e) {
				LOG.warn("Ignoring installed snapshot {}: cannot verify {}", dir, file, e);
				return false;
			}
		}
		NodeStore.syncTree(dir);
		Path verified = dir.resolve(VERIFIED_FILE);
		Files.createFile(verified);
		NodeStore.syncFile(verified);
		NodeStore.syncDirectory(dir);
		return true;
	}

	/** The files of a snapshot without Ratis' digest side files and the marker, with the properties file last. */
	private static List<Path> snapshotFiles(Path dir) throws IOException {
		var files = new ArrayList<Path>();
		try (Stream<Path> walk = Files.walk(dir.resolve(DB_DIR))) {
			walk.filter(Files::isRegularFile)
					.filter(path -> !path.getFileName().toString().endsWith(MD5FileUtil.MD5_SUFFIX))
					.sorted()
					.forEach(files::add);
		}
		files.add(dir.resolve(PROPERTIES_FILE));
		return files;
	}

	private static List<FileInfo> listFiles(Path dir) throws IOException {
		return snapshotFiles(dir).stream().map(path -> new FileInfo(path, null)).toList();
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
		try {
			properties.load(new StringReader(Files.readString(file, StandardCharsets.UTF_8)));
			String term = properties.getProperty("term");
			String index = properties.getProperty("index");
			if (term == null || index == null) {
				return null;
			}
			return TermIndex.valueOf(Long.parseLong(term), Long.parseLong(index));
		} catch (IllegalArgumentException e) {
			// A damaged file: malformed escapes or numbers.
			return null;
		}
	}
}
