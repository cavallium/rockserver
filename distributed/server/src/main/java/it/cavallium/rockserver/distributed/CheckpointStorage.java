package it.cavallium.rockserver.distributed;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Properties;
import java.util.Set;
import java.util.function.LongSupplier;
import java.util.regex.Pattern;
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
 * {@code snapshot.properties} (term, index, and from R51-2 the size of every file of the checkpoint). New snapshots are built in {@code tmp/} and renamed into
 * {@code snapshots/} once complete and fsynced. The latest snapshot is the usable one with the highest
 * position.</p>
 *
 * <p><b>Snapshots being sent.</b> A leader streams a snapshot to a follower from the file list it got when the
 * installation started, opening each file only when it reaches it and reading the list again for every chunk. A
 * path is therefore never reused for different content, and a snapshot stays on disk while an installation may
 * still read it: {@link #take} keeps the previous snapshot, and also every older one whose file list was read
 * within the last {@link #DEFAULT_INSTALL_READ_LEASE} (an installation that read nothing for that long has timed
 * out). Other snapshots are deleted, each renamed into {@code tmp/} first so that a crash during the delete leaves no part
 * of one in {@code snapshots/} (R52-1). A slow installation of a large database thus survives any number of newer
 * snapshots, at the cost of the disk space of the table files that only its snapshot still links.</p>
 *
 * <p><b>Snapshots being received.</b> Ratis' installation writes the leader's files into
 * {@code tmp/snapshot-<request id>/}, at the same paths relative to {@link #getSnapshotDir()}, and when complete
 * moves {@code snapshots/} aside, renames the new directory into its place and deletes the old one. An
 * installation that fails is restarted under a new request id and leaves its partial copy behind: the copies are
 * deleted when the server starts, soon after a newer installation starts writing (installations on one follower
 * never overlap, a new one invalidates the older ones), and once their files did not change for
 * {@link #INSTALL_ABANDONED_AFTER}. The node's install cleanup thread looks for them ({@link #discardFailedInstalls});
 * {@link #getLatestSnapshot()}, which Ratis calls under its locks, never does. After a crash during the swap, the old
 * snapshots moved aside are used only if the new ones are missing or damaged, and only those whose checkpoint is
 * complete: Ratis deletes the aside file by file, so a crash during that delete leaves it partial (R51-2). Of several
 * asides the newest usable snapshot is used. Ratis also swallows a failure of that delete, so an aside can outlive a
 * swap that completed: when the Raft log already starts past the entry after the newest snapshot named in the asides,
 * Ratis had reset it, and a damaged install is reported as with no aside (R58-3, {@link #raftLogStart}).</p>
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
	/**
	 * Prefix of an unused snapshot moved into {@link #getTmpDir()} to be deleted (R52-1); not {@link #INSTALL_PREFIX},
	 * so {@link #discardFailedInstalls} leaves it alone, and {@link #init} deletes it with the rest of {@code tmp/}.
	 */
	private static final String DELETING_PREFIX = "deleting-";
	private static final String DB_DIR = "db";
	private static final String PROPERTIES_FILE = "snapshot.properties";
	/** Prefix of the keys of {@link #PROPERTIES_FILE} that list the checkpoint's files with their sizes (R51-2). */
	private static final String FILE_KEY_PREFIX = "file.";
	/** Written into an installed snapshot once its files matched their digests and were fsynced. */
	private static final String VERIFIED_FILE = "installed.verified";
	private static final long NEVER = Long.MIN_VALUE;
	/** A Raft log segment file of Ratis 3.3.1: {@code log_inprogress_<start>} (group 1) or {@code log_<start>-<end>} (group 2). */
	private static final Pattern LOG_SEGMENT = Pattern.compile("log_inprogress_(\\d+)|log_(\\d+)-\\d+");

	private final long installReadLeaseNanos;
	private final LongSupplier nanoClock;
	private Path stateMachineDir;
	private Path snapshotsDir;
	private volatile Path tmpDir;
	/**
	 * Written under this storage's monitor, read without it by {@link #latestSnapshotDir}: the node monitor reads it
	 * while {@link #take} or {@link #reload} may hold the monitor for a whole checkpoint or install (R25-W).
	 */
	private volatile @Nullable Path latestDir;
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
	public void init(RaftStorage raftStorage) throws IOException {
		init(raftStorage.getStorageDir().getStateMachineDir().toPath());
	}

	/**
	 * Like {@link #init(RaftStorage)}, from the state machine directory ({@code raft/<group>/sm}) of a stopped node:
	 * {@code export-seed} reads the latest snapshot this way, after the same recovery and verification as a start
	 * (R31-2).
	 */
	synchronized void init(Path stateMachineDir) throws IOException {
		this.stateMachineDir = stateMachineDir.toAbsolutePath();
		snapshotsDir = stateMachineDir.resolve(SNAPSHOTS_DIR);
		Path tmp = stateMachineDir.resolve(TMP_DIR);
		// No snapshot is being built or installed before the server starts: leftovers are from a crash.
		NodeStore.deleteRecursively(tmp);
		Files.createDirectories(tmp);
		tmpDir = tmp;
		Files.createDirectories(snapshotsDir);
		List<Path> asides = installAsides();
		DamagedSnapshotException damagedInstall = null;
		try {
			reload();
		} catch (DamagedSnapshotException damaged) {
			if (asides.isEmpty()) {
				throw damaged;
			}
			damagedInstall = damaged;
		}
		if (latestDir == null && !asides.isEmpty()) {
			// The received snapshot is damaged, or snapshots/ was missing: the old snapshots moved aside are used if
			// Ratis was still swapping the directories when it stopped. They may be whole (a crash before the received
			// directory took their place) or partly deleted (a crash during Ratis' deleteFully of them): usableSnapshot
			// keeps only whole checkpoints (R51-2). Of several asides, the newest usable snapshot wins (R58-3).
			//
			// R58-3: Ratis swallows a failure of its deleteFully of the aside, so an aside also outlives a swap that
			// completed, after which Ratis resets the Raft log to start after the received snapshot before the state
			// machine verifies it. Before that reset the log never starts past the entry after the newest snapshot the
			// node had, which is in the aside (Ratis purges only up to a snapshot); a log that starts past it proves the
			// swap completed, so the aside (whose db/ is older than the received snapshot too) would leave a gap before
			// the log. That damaged install is reported as with no aside, and snapshots/ is kept for inspection. The
			// bound is read from the snapshots' names before usableSnapshot deletes the incomplete ones: a partly deleted
			// snapshot still says where the log may start.
			long asideIndex = 0;
			for (Path aside : asides) {
				for (Path dir : snapshotDirs(aside)) {
					TermIndex named = namedTermIndex(dir);
					if (named != null) {
						asideIndex = Math.max(asideIndex, named.getIndex());
					}
				}
			}
			// Known gap (review of R58, 3): right after a completed swap Ratis' purge leaves no segment until the next
			// entry is appended, so logStart is empty and the aside is promoted. By reading Ratis that opens no gap: an
			// empty log restarts after the promoted snapshot and the leader sends what follows.
			OptionalLong logStart = raftLogStart(this.stateMachineDir);
			if (logStart.isPresent() && logStart.getAsLong() > asideIndex + 1) {
				String gap = "the Raft log starts at index " + logStart.getAsLong() + ", past index " + (asideIndex + 1)
						+ " after the newest snapshot moved aside (index " + asideIndex + ", in " + asides + "): Ratis had"
						+ " completed the swap and reset the log, and failed to delete the old snapshots";
				if (damagedInstall != null) {
					throw new DamagedSnapshotException(damagedInstall.dir(), damagedInstall.reason() + "; " + gap,
							damagedInstall);
				}
				throw new IOException("No snapshot in " + snapshotsDir + ", and " + gap + ": this replica cannot catch up"
						+ " by itself, rebuild it from the other members");
			}
			Path chosen = newestUsableAside(asides);
			if (damagedInstall != null) {
				LOG.warn("Discarding a snapshot installation interrupted by a crash", damagedInstall);
				NodeStore.deleteRecursively(snapshotsDir);
				Files.createDirectories(snapshotsDir);
				reload();
			}
			if (chosen != null) {
				NodeStore.deleteRecursively(snapshotsDir);
				Files.move(chosen, snapshotsDir);
				NodeStore.syncDirectory(stateMachineDir);
				reload();
			}
		}
		for (Path aside : asides) {
			if (Files.exists(aside)) {
				NodeStore.deleteRecursively(aside);
			}
		}
		deleteUnusedSnapshots(null);
	}

	/**
	 * Of {@code asides}, the one whose latest usable snapshot is the newest, whatever the order of the list (the order
	 * of a directory listing, R58-3); null if none holds a usable snapshot. Deletes the incomplete snapshots of each
	 * ({@link #usableSnapshot}).
	 */
	static @Nullable Path newestUsableAside(List<Path> asides) throws IOException {
		Path chosen = null;
		TermIndex chosenTermIndex = null;
		for (Path aside : asides) {
			if (usableSnapshot(aside)) {
				TermIndex termIndex = readTermIndex(findLatest(aside));
				if (chosenTermIndex == null || termIndex.compareTo(chosenTermIndex) > 0) {
					chosen = aside;
					chosenTermIndex = termIndex;
				}
			}
		}
		return chosen;
	}

	/**
	 * The first index of the Raft log next to {@code stateMachineDir} ({@code raft/<group>/current}, Ratis' segment
	 * files {@code log_<start>-<end>} and {@code log_inprogress_<start>}), or empty if it holds no segment. An empty
	 * open segment is skipped: Ratis deletes it at the start.
	 */
	static OptionalLong raftLogStart(Path stateMachineDir) throws IOException {
		Path current = stateMachineDir.toAbsolutePath().getParent().resolve("current");
		if (!Files.isDirectory(current)) {
			return OptionalLong.empty();
		}
		long start = Long.MAX_VALUE;
		try (Stream<Path> files = Files.list(current)) {
			for (Path file : files.toList()) {
				var matcher = LOG_SEGMENT.matcher(file.getFileName().toString());
				if (!matcher.matches() || !Files.isRegularFile(file)) {
					continue;
				}
				boolean open = matcher.group(1) != null;
				if (open && Files.size(file) == 0) {
					continue;
				}
				start = Math.min(start, Long.parseLong(open ? matcher.group(1) : matcher.group(2)));
			}
		}
		return start == Long.MAX_VALUE ? OptionalLong.empty() : OptionalLong.of(start);
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

	/**
	 * Whether {@code aside}, old snapshots that Ratis moved aside, holds a snapshot to use. Ratis 3.3.1 deletes the
	 * aside file by file ({@code FileUtils.deleteFully}), so a crash during that delete leaves snapshots with their
	 * {@code snapshot.properties} and part of their {@code db/}: each snapshot whose checkpoint is not complete
	 * ({@link #completeCheckpoint}) is deleted first (R51-2), so that the node never uses, nor streams to a follower, a
	 * checkpoint that cannot open.
	 */
	private static boolean usableSnapshot(Path aside) throws IOException {
		for (Path dir : snapshotDirs(aside)) {
			var missing = completeCheckpoint(dir);
			if (missing != null) {
				LOG.warn("Ignoring the snapshot {} moved aside: its checkpoint is not complete ({})", dir, missing);
				NodeStore.deleteRecursively(dir);
			}
		}
		try {
			return findLatest(aside) != null;
		} catch (DamagedSnapshotException damaged) {
			LOG.warn("Ignoring the snapshots moved aside in {}", aside, damaged);
			return false;
		}
	}

	/**
	 * The latest usable snapshot, listing every file under its directory by absolute path. Reads a field only: Ratis
	 * calls this under its server monitor on every append that carries entries (a follower checking the append
	 * against its snapshot index) and on the leader's appender threads, so it must never touch the disk nor wait for
	 * the install lock, see {@link #discardFailedInstalls}.
	 */
	@Override
	public @Nullable FileListSnapshotInfo getLatestSnapshot() {
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
			writeProperties(building.resolve(PROPERTIES_FILE), termIndex, checkpointFiles(building));
			NodeStore.syncDirectory(building);
			CrashPoints.reach(CrashPoints.Point.SNAPSHOT_BUILT);
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

	/**
	 * Directory of the latest snapshot, null if none: for its age. Reads a field only, without this storage's monitor,
	 * which {@link #take} holds for a whole checkpoint and {@link #reload} for the digest checks and fsyncs of an
	 * install: on a hung disk for good (R25-W).
	 */
	@Nullable Path latestSnapshotDir() {
		return latestDir;
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

	/**
	 * Deletes every snapshot except the latest, {@code previous} and the ones an installation is reading. Each one is
	 * first renamed into {@code tmp/} and the rename fsynced, then its files are deleted there (R52-1): a delete in place
	 * goes in reverse path order, so a crash in the middle would leave in {@code snapshots/} an installed snapshot
	 * without {@code snapshot.properties} or {@code installed.verified} but with Ratis' {@code .md5} files, which looks
	 * like an installation that lost its position ({@link #findLatest} also copes with such a leftover from a version
	 * before R52).
	 *
	 * <p>A snapshot whose name puts it past the latest one stays too (R53-1): it is an installed snapshot that Ratis
	 * swapped in while a {@link #take} ran. Ratis 3.3.1 ends an install with {@code StateMachine.pause()}, then
	 * {@code SnapshotManager.finalizeSnapshot} (the snapshot directory moved aside and the received one renamed into
	 * its place, without this storage's monitor), then the reload, and {@code pause()} does not wait for a take on the
	 * updater thread: the take moves its checkpoint into the installed directory, and deleting the installed snapshot
	 * there left the reload with the replica's own older one while Ratis had purged its log up to the installed index,
	 * a replica that stopped applying without an error. {@link #reload} then finds the installed snapshot, and the
	 * next take after it deletes the checkpoint the racing take left. At the start no usable snapshot is past the
	 * latest ({@link #findLatest} picks the furthest, and fails on a damaged one past it).</p>
	 */
	private void deleteUnusedSnapshots(@Nullable Path previous) throws IOException {
		long now = nanoClock.getAsLong();
		long latestIndex = latest == null ? Long.MAX_VALUE : latest.getIndex();
		for (Path dir : snapshotDirs(snapshotsDir)) {
			if (dir.equals(latestDir) || dir.equals(previous)) {
				continue;
			}
			TermIndex named = namedTermIndex(dir);
			if (named != null && named.getIndex() > latestIndex) {
				LOG.warn("Keeping snapshot {}, past the latest one {}: an install swapped it in while a snapshot was"
						+ " taken, the reload after the install loads it", dir, latestDir);
				continue;
			}
			Snapshot snapshot = listed.get(dir);
			if (snapshot != null && snapshot.readSince(now - installReadLeaseNanos)) {
				LOG.info("Keeping snapshot {}: an installation is still reading it", dir);
				continue;
			}
			Path deleting = tmpDir.resolve(DELETING_PREFIX + dir.getFileName() + "-" + System.nanoTime());
			Files.move(dir, deleting, StandardCopyOption.ATOMIC_MOVE);
			listed.remove(dir);
			NodeStore.syncDirectory(snapshotsDir);
			CrashPoints.reach(CrashPoints.Point.SNAPSHOT_DELETING);
			NodeStore.deleteRecursively(deleting);
		}
	}

	/** Tests only: the lock that {@link #discardFailedInstalls} holds while it lists, walks and renames. */
	Object installsLock() {
		return installsLock;
	}

	/**
	 * Deletes the directories of the installations a newer one replaced: every installation directory except the
	 * ones that appeared since the previous check that found a new one. A directory that comes back after being
	 * deleted (a stale request writing one more chunk) is never taken for a new installation. Also deletes the
	 * installations that stopped writing, see {@link #INSTALL_ABANDONED_AFTER}. Runs at most every
	 * {@link #INSTALL_SCAN_INTERVAL}.
	 *
	 * <p>Lists, walks and renames in the data directory under the install lock, so only the node's install cleanup
	 * thread calls it (and tests): never a Ratis thread, which could hold the server monitor while a hung disk holds
	 * this lock. {@link #init} deletes every installation directory left from before the start.</p>
	 */
	void discardFailedInstalls() {
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
			} catch (UncheckedIOException | NoSuchFileException ex) {
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
	 *
	 * <p>A damaged installed snapshot ({@link #verifyIfInstalled}) is fatal unless its directory name places it before a
	 * usable snapshot (R52-1): then it is not needed (a newer whole snapshot was taken after it was installed, since an
	 * installation replaces the whole snapshot directory) and is most likely the leftover of a delete that a crash
	 * interrupted. It is logged and skipped, and {@link #deleteUnusedSnapshots} deletes it. A damaged snapshot at or
	 * after the latest usable one, or one whose name tells no position, stays fatal: the Raft log may start after it.</p>
	 */
	private static @Nullable Path findLatest(Path parent) throws IOException {
		Path best = null;
		TermIndex bestTermIndex = null;
		var damaged = new ArrayList<DamagedSnapshotException>();
		for (Path dir : snapshotDirs(parent)) {
			try {
				verifyIfInstalled(dir);
			} catch (DamagedSnapshotException damage) {
				damaged.add(damage);
				continue;
			}
			TermIndex termIndex = readTermIndex(dir);
			if (termIndex != null && (bestTermIndex == null || termIndex.compareTo(bestTermIndex) > 0)) {
				best = dir;
				bestTermIndex = termIndex;
			}
		}
		for (var damage : damaged) {
			TermIndex named = namedTermIndex(damage.dir());
			if (bestTermIndex == null || named == null || named.compareTo(bestTermIndex) >= 0) {
				throw damage;
			}
		}
		for (var damage : damaged) {
			LOG.warn("Ignoring the snapshot {}, older than the latest snapshot {}: {} (the leftover of a delete interrupted"
					+ " by a crash, most likely); it is deleted", damage.dir(), best, damage.reason());
		}
		return best;
	}

	/** The position in the name of a snapshot directory, {@code snapshot-<term>-<index>}, or null if it tells none. */
	private static @Nullable TermIndex namedTermIndex(Path dir) {
		String name = dir.getFileName().toString();
		if (!name.startsWith(SNAPSHOT_PREFIX)) {
			return null;
		}
		String[] parts = name.substring(SNAPSHOT_PREFIX.length()).split("-");
		if (parts.length != 2) {
			return null;
		}
		try {
			return TermIndex.valueOf(Long.parseLong(parts[0]), Long.parseLong(parts[1]));
		} catch (NumberFormatException e) {
			return null;
		}
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

	/**
	 * Writes the snapshot's position and, from R51-2, the size of every file of its checkpoint, as
	 * {@code file.<path relative to the snapshot directory>=<bytes>}: {@link #completeCheckpoint} reads them. The file is
	 * listed and sent to a follower with the checkpoint, whose files it describes there as well; a version before
	 * R51-2 ignores the extra keys.
	 */
	private static void writeProperties(Path file, TermIndex termIndex, Map<String, Long> files) throws IOException {
		var content = new StringBuilder("term=" + termIndex.getTerm() + "\nindex=" + termIndex.getIndex() + "\n");
		var listed = new Properties();
		files.forEach((name, bytes) -> listed.setProperty(FILE_KEY_PREFIX + name, Long.toString(bytes)));
		var escaped = new java.io.StringWriter();
		listed.store(escaped, null);
		escaped.toString().lines().filter(line -> !line.startsWith("#")).sorted()
				.forEach(line -> content.append(line).append('\n'));
		Files.writeString(file, content, StandardCharsets.UTF_8);
		NodeStore.syncFile(file);
	}

	/** The files of the checkpoint in {@code dir}, by path relative to {@code dir} (with {@code /}), with their sizes. */
	private static Map<String, Long> checkpointFiles(Path dir) throws IOException {
		var files = new java.util.TreeMap<String, Long>();
		try (Stream<Path> walk = Files.walk(dir.resolve(DB_DIR))) {
			for (Path path : (Iterable<Path>) walk.filter(Files::isRegularFile)
					.filter(path -> !path.getFileName().toString().endsWith(MD5FileUtil.MD5_SUFFIX))::iterator) {
				var relative = new StringBuilder();
				dir.relativize(path).forEach(part -> relative.append(relative.isEmpty() ? "" : "/").append(part));
				files.put(relative.toString(), Files.size(path));
			}
		}
		return files;
	}

	/**
	 * Null if the checkpoint of snapshot {@code dir} is complete, else what is missing (R51-2). With the file list of
	 * {@link #writeProperties}, every listed file must be there at its size. A snapshot of a version before R51-2 has
	 * none: its {@code CURRENT} must name a MANIFEST that is there, which catches most but not every partial delete
	 * (a table file can go first).
	 */
	static @Nullable String completeCheckpoint(Path dir) throws IOException {
		Path file = dir.resolve(PROPERTIES_FILE);
		if (!Files.isRegularFile(file)) {
			return "no " + PROPERTIES_FILE;
		}
		var properties = new Properties();
		try {
			properties.load(new StringReader(Files.readString(file, StandardCharsets.UTF_8)));
		} catch (IllegalArgumentException damaged) {
			return PROPERTIES_FILE + " cannot be read";
		}
		boolean listed = false;
		for (String key : properties.stringPropertyNames()) {
			if (!key.startsWith(FILE_KEY_PREFIX)) {
				continue;
			}
			listed = true;
			Path listedFile = dir.resolve(key.substring(FILE_KEY_PREFIX.length()));
			long bytes;
			try {
				bytes = Long.parseLong(properties.getProperty(key));
			} catch (NumberFormatException damaged) {
				return "the size of " + listedFile + " cannot be read";
			}
			if (!listedFile.normalize().startsWith(dir.resolve(DB_DIR)) || !Files.isRegularFile(listedFile)) {
				return listedFile + " is missing";
			}
			if (Files.size(listedFile) != bytes) {
				return listedFile + " has " + Files.size(listedFile) + " bytes, not " + bytes;
			}
		}
		if (listed) {
			return null;
		}
		Path current = dir.resolve(DB_DIR).resolve("CURRENT");
		if (!Files.isRegularFile(current)) {
			return current + " is missing";
		}
		String manifest = Files.readString(current, StandardCharsets.UTF_8).strip();
		if (!manifest.startsWith("MANIFEST-") || manifest.contains("/")
				|| !Files.isRegularFile(dir.resolve(DB_DIR).resolve(manifest))) {
			return "the MANIFEST that " + current + " names (" + manifest + ") is missing";
		}
		return null;
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

		private final Path dir;
		private final String reason;

		DamagedSnapshotException(Path dir, String reason, @Nullable Throwable cause) {
			super("Installed snapshot " + dir + " is damaged (" + reason + "), probably by a crash before it was"
					+ " synced. The Raft log may already start after it, so this replica cannot catch up by itself:"
					+ " rebuild it from the other members", cause);
			this.dir = dir;
			this.reason = reason;
		}

		/** The damaged snapshot's directory, {@code snapshot-<term>-<index>}. */
		Path dir() {
			return dir;
		}

		/** What is wrong with it, without the advice of the message (which fits a start, not {@code export-seed}). */
		String reason() {
			return reason;
		}

		/**
		 * The snapshot's index, from its directory name ({@code snapshot-<term>-<index>}, as Ratis installs it; its
		 * properties file may be what the crash damaged), or -1 if the name does not tell.
		 */
		long snapshotIndex() {
			TermIndex named = namedTermIndex(dir);
			return named == null ? -1 : named.getIndex();
		}
	}
}
