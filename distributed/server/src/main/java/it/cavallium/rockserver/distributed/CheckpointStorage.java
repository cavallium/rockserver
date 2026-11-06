package it.cavallium.rockserver.distributed;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.stream.Collectors;
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
 * <p><b>Snapshots being sent.</b> A leader streams a snapshot to a follower from the file list it got when the
 * installation started, opening each file only when it reaches it and reading the list again for every chunk. A
 * path is therefore never reused for different content, and a snapshot stays on disk while an installation may
 * still read it: {@link #take} keeps the previous snapshot, and also every older one whose file list was read
 * within the last {@link #DEFAULT_INSTALL_READ_LEASE} (an installation that read nothing for that long has timed
 * out). Other snapshots are deleted. A slow installation of a large database thus survives any number of newer
 * snapshots, at the cost of the disk space of the table files that only its snapshot still links.</p>
 *
 * <p><b>Snapshots being received.</b> Ratis' installation writes the leader's files into
 * {@code tmp/snapshot-<request id>/}, at the same paths relative to {@link #getSnapshotDir()}, and when complete
 * moves {@code snapshots/} aside, renames the new directory into its place and deletes the old one. An
 * installation that fails is restarted under a new request id and leaves its partial copy behind: the copies are
 * deleted when the server starts, when a newer installation starts writing (Ratis reads the latest snapshot
 * for every chunk it receives; installations on one follower never overlap, a new one invalidates the older
 * ones), and once their files did not change for {@link #INSTALL_ABANDONED_AFTER}.</p>
 *
 * <p>Ratis saves a {@code <file>.md5} next to every received file but syncs nothing, so an installed snapshot is
 * used only once each file matches its saved digest; it is then fsynced and marked as verified. A snapshot that
 * fails this check lost data in a crash after Ratis installed it, possibly after the Raft log was already reset to
 * start after it: that is reported as an error rather than ignored, since the replica cannot catch up without it.
 * The listed files carry no digest (the leader computes it while streaming), and the side files are not listed,
 * so a follower that later leads does not send them on.</p>
 */
final class CheckpointStorage implements StateMachineStorage {

	private static final Logger LOG = LoggerFactory.getLogger(CheckpointStorage.class);
	/** How long a snapshot whose file list was read is kept after a newer one is taken. */
	static final Duration DEFAULT_INSTALL_READ_LEASE = Duration.ofMinutes(10);
	/** Minimum interval between two checks for partial installations to delete. */
	static final Duration INSTALL_SCAN_INTERVAL = Duration.ofSeconds(1);
	/**
	 * An installation whose files did not change for this long failed: its partial copy is deleted, without waiting for
	 * a newer installation or a restart. The leader keeps the snapshot of an installation that read nothing for
	 * {@link #DEFAULT_INSTALL_READ_LEASE} no longer either.
	 */
	static final Duration INSTALL_ABANDONED_AFTER = DEFAULT_INSTALL_READ_LEASE;
	/** Minimum interval between two looks at the files of the current installations, see {@link #INSTALL_ABANDONED_AFTER}. */
	static final Duration INSTALL_PROGRESS_INTERVAL = Duration.ofSeconds(10);
	private static final String SNAPSHOTS_DIR = "snapshots";
	private static final String TMP_DIR = "tmp";
	private static final String SNAPSHOT_PREFIX = "snapshot-";
	/** Prefix of the directories Ratis receives snapshots into, under {@link #getTmpDir()}. */
	private static final String INSTALL_PREFIX = "snapshot-";
	private static final String DB_DIR = "db";
	private static final String PROPERTIES_FILE = "snapshot.properties";
	/** Written into an installed snapshot once its files matched their digests and were fsynced. */
	private static final String VERIFIED_FILE = "installed.verified";
	private static final long NEVER = Long.MIN_VALUE;

	private final long installReadLeaseNanos;
	private final LongSupplier nanoClock;
	private Path stateMachineDir;
	private Path snapshotsDir;
	private volatile Path tmpDir;
	private @Nullable Path latestDir;
	private volatile @Nullable Snapshot latest;
	/** The snapshots listed by this instance that are still on disk, by directory. */
	private final Map<Path, Snapshot> listed = new HashMap<>();

	private final Object installsLock = new Object();
	private volatile long lastInstallScan = NEVER;
	/** Every installation directory seen in {@link #tmpDir}, and the ones of the newest installation. */
	private final Set<String> seenInstalls = new HashSet<>();
	private Set<String> currentInstalls = Set.of();
	/** Of each current installation, its files (count and bytes) when last looked at, and since when they are so. */
	private final Map<String, long[]> installProgress = new HashMap<>();
	private long lastProgressCheck = NEVER;

	CheckpointStorage() {
		this(DEFAULT_INSTALL_READ_LEASE, System::nanoTime);
	}

	/** {@code nanoClock} is a monotonic clock in nanoseconds, like {@link System#nanoTime()}. */
	CheckpointStorage(Duration installReadLease, LongSupplier nanoClock) {
		this.installReadLeaseNanos = installReadLease.toNanos();
		this.nanoClock = nanoClock;
	}

	@Override
	public synchronized void init(RaftStorage raftStorage) throws IOException {
		stateMachineDir = raftStorage.getStorageDir().getStateMachineDir().toPath().toAbsolutePath();
		snapshotsDir = stateMachineDir.resolve(SNAPSHOTS_DIR);
		Path tmp = stateMachineDir.resolve(TMP_DIR);
		// No snapshot is being built or installed before the server starts: leftovers are from a crash.
		NodeStore.deleteRecursively(tmp);
		Files.createDirectories(tmp);
		tmpDir = tmp;
		Files.createDirectories(snapshotsDir);
		List<Path> asides = installAsides();
		try {
			reload();
		} catch (DamagedSnapshotException damaged) {
			if (asides.isEmpty()) {
				throw damaged;
			}
			// Ratis was still swapping the directories, so it had not reset the log yet: the install did not happen.
			LOG.warn("Discarding a snapshot installation interrupted by a crash", damaged);
			NodeStore.deleteRecursively(snapshotsDir);
			Files.createDirectories(snapshotsDir);
			reload();
		}
		for (Path aside : asides) {
			if (latestDir == null && usableSnapshot(aside)) {
				NodeStore.deleteRecursively(snapshotsDir);
				Files.move(aside, snapshotsDir);
				NodeStore.syncDirectory(stateMachineDir);
				reload();
			} else {
				NodeStore.deleteRecursively(aside);
			}
		}
		deleteUnusedSnapshots(null);
	}

	/**
	 * Ratis installs a snapshot by moving {@code snapshots/} aside to a {@code snapshots.tmp*} sibling, renaming
	 * the new directory into its place and deleting the old one. After a crash in between, the old directory is
	 * used only if the current one is missing or holds a damaged installation.
	 */
	private List<Path> installAsides() throws IOException {
		try (Stream<Path> siblings = Files.list(stateMachineDir)) {
			return siblings
					.filter(path -> path.getFileName().toString().startsWith(SNAPSHOTS_DIR + "."))
					.filter(Files::isDirectory)
					.toList();
		}
	}

	private static boolean usableSnapshot(Path aside) throws IOException {
		try {
			return findLatest(aside) != null;
		} catch (DamagedSnapshotException damaged) {
			LOG.warn("Ignoring the snapshots moved aside in {}", aside, damaged);
			return false;
		}
	}

	/**
	 * The latest usable snapshot, listing every file under its directory by absolute path. Also deletes the
	 * partial copies of installations that a newer installation replaced.
	 */
	@Override
	public @Nullable FileListSnapshotInfo getLatestSnapshot() {
		discardReplacedInstalls();
		return latest;
	}

	/**
	 * Re-reads the snapshot directory. The state machine calls it first thing after Ratis installed a snapshot
	 * from the leader: this verifies the installed files against their digests and fsyncs them.
	 *
	 * @throws DamagedSnapshotException if an installed snapshot lost data, see the class documentation
	 */
	synchronized void reload() throws IOException {
		listed.clear();
		latestDir = findLatest(snapshotsDir);
		latest = latestDir == null ? null : listSnapshot(latestDir, readTermIndex(latestDir));
	}

	/**
	 * Checkpoints {@code store} into a new snapshot that becomes the latest one. The previous snapshot is kept, and
	 * older ones are deleted unless an installation is still reading them.
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
		latest = listSnapshot(target, termIndex);
		deleteUnusedSnapshots(previous);
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

	private Snapshot listSnapshot(Path dir, TermIndex termIndex) throws IOException {
		var snapshot = new Snapshot(listFiles(dir), termIndex);
		listed.put(dir, snapshot);
		return snapshot;
	}

	/** Deletes every snapshot except the latest, {@code previous} and the ones an installation is reading. */
	private void deleteUnusedSnapshots(@Nullable Path previous) throws IOException {
		long now = nanoClock.getAsLong();
		for (Path dir : snapshotDirs(snapshotsDir)) {
			if (dir.equals(latestDir) || dir.equals(previous)) {
				continue;
			}
			Snapshot snapshot = listed.get(dir);
			if (snapshot != null && snapshot.readSince(now - installReadLeaseNanos)) {
				LOG.info("Keeping snapshot {}: an installation is still reading it", dir);
				continue;
			}
			NodeStore.deleteRecursively(dir);
			listed.remove(dir);
		}
	}

	/**
	 * Deletes the directories of the installations a newer one replaced: every installation directory except the
	 * ones that appeared since the previous check that found a new one. A directory that comes back after being
	 * deleted (a stale request writing one more chunk) is never taken for a new installation.
	 */
	private void discardReplacedInstalls() {
		Path tmp = tmpDir;
		long now = nanoClock.getAsLong();
		long last = lastInstallScan;
		if (tmp == null || (last != NEVER && now - last < INSTALL_SCAN_INTERVAL.toNanos())) {
			return;
		}
		synchronized (installsLock) {
			lastInstallScan = now;
			try {
				Set<String> present;
				try (Stream<Path> children = Files.list(tmp)) {
					present = children
							.filter(path -> path.getFileName().toString().startsWith(INSTALL_PREFIX))
							.filter(Files::isDirectory)
							.map(path -> path.getFileName().toString())
							.collect(Collectors.toSet());
				}
				Set<String> fresh = present.stream()
						.filter(name -> !seenInstalls.contains(name))
						.collect(Collectors.toSet());
				if (!fresh.isEmpty()) {
					seenInstalls.addAll(fresh);
					currentInstalls = fresh;
				}
				for (String name : present) {
					if (!currentInstalls.contains(name)) {
						discard(tmp.resolve(name));
					}
				}
				discardAbandonedInstalls(tmp, present, now);
			} catch (IOException ex) {
				LOG.warn("Cannot delete the partial copies of replaced snapshot installations in {}", tmp, ex);
			}
		}
	}

	/**
	 * Deletes the current installations whose files did not change for {@link #INSTALL_ABANDONED_AFTER}: Ratis gave up
	 * on them (the leader stopped sending, or this follower failed to write), and only a newer installation or a restart
	 * would delete them otherwise. One that is only slow and gets another chunk after this fails, and Ratis starts it
	 * over. Looks at the files at most every {@link #INSTALL_PROGRESS_INTERVAL}.
	 */
	private void discardAbandonedInstalls(Path tmp, Set<String> present, long now) throws IOException {
		installProgress.keySet().retainAll(currentInstalls);
		if (lastProgressCheck != NEVER && now - lastProgressCheck < INSTALL_PROGRESS_INTERVAL.toNanos()) {
			return;
		}
		lastProgressCheck = now;
		var abandoned = new ArrayList<String>();
		for (String name : currentInstalls) {
			if (!present.contains(name)) {
				continue;
			}
			long files = 0;
			long bytes = 0;
			try (Stream<Path> walk = Files.walk(tmp.resolve(name))) {
				for (Path file : (Iterable<Path>) walk.filter(Files::isRegularFile)::iterator) {
					files++;
					bytes += Files.size(file);
				}
			} catch (java.io.UncheckedIOException | java.nio.file.NoSuchFileException ex) {
				// Changing under the walk: it is alive.
				installProgress.remove(name);
				continue;
			}
			long[] known = installProgress.get(name);
			if (known == null || known[0] != files || known[1] != bytes) {
				installProgress.put(name, new long[] {files, bytes, now});
			} else if (now - known[2] >= INSTALL_ABANDONED_AFTER.toNanos()) {
				abandoned.add(name);
			}
		}
		for (String name : abandoned) {
			LOG.warn("The snapshot installation in {} wrote nothing for {}: it failed", tmp.resolve(name),
					INSTALL_ABANDONED_AFTER);
			installProgress.remove(name);
			var rest = new HashSet<>(currentInstalls);
			rest.remove(name);
			currentInstalls = Set.copyOf(rest);
			discard(tmp.resolve(name));
		}
	}

	/** Renames {@code dir} out of the way, then deletes it in the background: it can hold a whole database. */
	private void discard(Path dir) throws IOException {
		Path discarded = dir.resolveSibling("discarded-" + System.nanoTime());
		Files.move(dir, discarded);
		LOG.info("Deleting {}, the partial copy of a replaced snapshot installation", dir);
		Thread.ofVirtual().name("snapshot-install-cleanup").start(() -> {
			try {
				NodeStore.deleteRecursively(discarded);
			} catch (IOException | RuntimeException ex) {
				LOG.warn("Cannot delete {}", discarded, ex);
			}
		});
	}

	/**
	 * The snapshot with the highest position among the snapshot directories in {@code parent}, verifying installed
	 * snapshots first.
	 */
	private static @Nullable Path findLatest(Path parent) throws IOException {
		Path best = null;
		TermIndex bestTermIndex = null;
		for (Path dir : snapshotDirs(parent)) {
			verifyIfInstalled(dir);
			TermIndex termIndex = readTermIndex(dir);
			if (termIndex != null && (bestTermIndex == null || termIndex.compareTo(bestTermIndex) > 0)) {
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
	 * Does nothing for a snapshot taken locally (fsynced before it was renamed into place) or an installed one
	 * verified earlier. Otherwise the snapshot was installed by Ratis, and every file must match its saved digest,
	 * which fails if a crash lost data that Ratis never synced; the files are then fsynced and the snapshot marked.
	 */
	private static void verifyIfInstalled(Path dir) throws IOException {
		boolean installed;
		try (Stream<Path> walk = Files.walk(dir)) {
			installed = walk.anyMatch(path -> path.getFileName().toString().endsWith(MD5FileUtil.MD5_SUFFIX));
		}
		if (!installed || Files.exists(dir.resolve(VERIFIED_FILE))) {
			return;
		}
		if (readTermIndex(dir) == null) {
			throw new DamagedSnapshotException(dir, "its position is missing", null);
		}
		for (Path file : snapshotFiles(dir)) {
			MD5Hash saved;
			MD5Hash actual;
			try {
				saved = MD5FileUtil.readStoredMd5ForFile(file.toFile());
				actual = MD5FileUtil.computeMd5ForFile(file.toFile());
			} catch (IOException ex) {
				throw new DamagedSnapshotException(dir, "cannot verify " + file, ex);
			}
			if (saved == null || !saved.equals(actual)) {
				throw new DamagedSnapshotException(dir, file + " does not match its saved digest", null);
			}
		}
		NodeStore.syncTree(dir);
		Path verified = dir.resolve(VERIFIED_FILE);
		Files.createFile(verified);
		NodeStore.syncFile(verified);
		NodeStore.syncDirectory(dir);
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

	/** A listed snapshot that records when its file list was last read, as an installation does for every chunk. */
	private final class Snapshot extends FileListSnapshotInfo {

		private volatile long lastRead = NEVER;

		Snapshot(List<FileInfo> files, TermIndex termIndex) {
			super(files, termIndex);
		}

		@Override
		public List<FileInfo> getFiles() {
			lastRead = nanoClock.getAsLong();
			return super.getFiles();
		}

		boolean readSince(long since) {
			long read = lastRead;
			return read != NEVER && read - since >= 0;
		}
	}

	/** An installed snapshot lost data in a crash; this replica cannot use it and must be rebuilt. */
	static final class DamagedSnapshotException extends IOException {

		DamagedSnapshotException(Path dir, String reason, @Nullable Throwable cause) {
			super("Installed snapshot " + dir + " is damaged (" + reason + "), probably by a crash before it was"
					+ " synced. The Raft log may already start after it, so this replica cannot catch up by itself:"
					+ " rebuild it from the other members", cause);
		}
	}
}
