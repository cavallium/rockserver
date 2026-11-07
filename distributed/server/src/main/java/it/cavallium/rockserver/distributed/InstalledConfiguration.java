package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.NoSuchFileException;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Arrays;
import java.util.Comparator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ratis.proto.RaftProtos;
import org.jetbrains.annotations.Nullable;

/**
 * Upper bounds of the terms of the configurations snapshot chunks gave Ratis (round-13 review, finding 1; round 15,
 * R14-1), kept in the Raft directory as {@link #FILE}. A leader sends its latest configuration with each snapshot
 * chunk without its term ({@code LeaderProtoUtils.toInstallSnapshotRequestProtoBuilder} passes none), and Ratis writes
 * it to {@code raft-meta.conf} and into memory, often past the end of the node's log: {@link MembershipFacts} cannot
 * tell its term. The sender's term bounds it (a leader, deposed or not, holds no entry of a later term than its own),
 * so {@link RaftTraffic} records, before Ratis sees a chunk, the index and halves of its configuration (the key) with
 * the leader's term: the highest term seen for that key. {@link RemovalRule} uses a bound only to tell that such a
 * configuration is older than K*.
 *
 * <p>Ratis 3.3.1 takes the configuration of a chunk it rejects. {@code SnapshotInstallationHandler.installSnapshotImpl}
 * applies it for every reply that {@code checkAndInstallSnapshot} returns (offsets 120-241): NOT_LEADER (the leader's
 * term is below the current one), SNAPSHOT_EXPIRED and ALREADY_INSTALLED as well as SUCCESS. It drops its
 * configurations from the chunk's index on ({@code ServerState.truncate}, {@code ConfigurationManager
 * .removeConfigurations}) and takes the chunk's when it differs from the one left. Only a chunk it fails with an
 * exception (wrong chunk index, group, life cycle, write error) leaves its configuration alone. So every chunk is
 * recorded, whatever its term, and a chunk of another configuration never replaces the record of one (it did until
 * round 15: a deposed leader's chunk with D', then an older one's with D, left D bounded by the older term).</p>
 *
 * <p>The record holds a bound per key, at most {@link #CAP} of them, and a {@code floor}. Invariant: for every chunk
 * forwarded to Ratis since the file was written at this node's start, the record holds its key with a bound not below
 * its leader's term, or does not hold its key and the floor is not below it; and the floor is not below the term of
 * any chunk Ratis handled before that start. A key is added with the greater of its term and the floor, a full record
 * drops the lowest index (then the lowest bound) other than the key just recorded and raises the floor to that bound,
 * updates are serialized per Raft directory, and the file (synced, with its directory) is replaced before the chunk is
 * forwarded. At a start
 * ({@link #initialize}) a missing file, an unreadable one or one of the single record of round 13 (whose bound R14-1
 * may have left too low) is replaced by an empty record whose floor is the term in Ratis' {@code raft-meta}: Ratis
 * persists a term before it acts on a chunk of that term ({@code changeToFollowerAndPersistMetadata}), and a chunk it
 * rejects has a lower one. A key that is not held bounds nothing ({@link RemovalRule#UNKNOWN_TERM}): the node waits.</p>
 *
 * <p>The file: a first line {@code records-v2 <floor>}, which a version of round 13 fails to read as an index (it then
 * bounds nothing), then four lines per key: index, bound, new half, old half (ids separated by commas).</p>
 */
record InstalledConfiguration(long floor, Map<Key, Long> bounds) {

	static final String FILE = "installed-configuration";
	/** At most this many keys; then the lowest index goes, and its bound raises the floor. */
	static final int CAP = 32;
	static final InstalledConfiguration EMPTY = new InstalledConfiguration(0, Map.of());
	private static final String TAG = "records-v2";
	/**
	 * Per Raft directory: serializes its updates (install streams of several leaders may run at once), and keeps the
	 * record last read or written with the attributes of its file.
	 */
	private static final ConcurrentHashMap<Path, Cache> CACHES = new ConcurrentHashMap<>();
	private static final Comparator<Map.Entry<Key, Long>> EVICTION = Comparator
			.comparingLong((Map.Entry<Key, Long> entry) -> entry.getKey().index()).thenComparingLong(Map.Entry::getValue);

	/** A configuration: its index and the ids of its halves. Coarser than Ratis' equality (no addresses). */
	record Key(long index, Set<String> newHalf, Set<String> oldHalf) {
		Key {
			newHalf = Set.copyOf(newHalf);
			oldHalf = Set.copyOf(oldHalf);
		}

		static Key of(long index, List<String> newHalf, List<String> oldHalf) {
			return new Key(index, Set.copyOf(newHalf), Set.copyOf(oldHalf));
		}
	}

	InstalledConfiguration {
		bounds = Map.copyOf(bounds);
	}

	/** The bound of the term of {@code conf}, {@link RemovalRule#UNKNOWN_TERM} if {@code record} does not hold it. */
	static long boundOf(@Nullable InstalledConfiguration record, @Nullable Conf conf) {
		if (record == null || conf == null || conf.oldHalf() == null) {
			return RemovalRule.UNKNOWN_TERM;
		}
		var bound = record.bounds.get(Key.of(conf.index(), conf.newHalf(), conf.oldHalf()));
		return bound == null ? RemovalRule.UNKNOWN_TERM : bound;
	}

	/** {@code record} (null: none) after a chunk of a leader of term {@code leaderTerm} with the given configuration. */
	static InstalledConfiguration with(@Nullable InstalledConfiguration record, long leaderTerm, long index,
			List<String> newHalf, List<String> oldHalf) {
		var current = record == null ? EMPTY : record;
		var key = Key.of(index, newHalf, oldHalf);
		var bound = current.bounds.get(key);
		if (bound != null && bound >= leaderTerm) {
			return current;
		}
		var bounds = new LinkedHashMap<>(current.bounds);
		bounds.put(key, bound != null ? leaderTerm : Math.max(leaderTerm, current.floor));
		long floor = current.floor;
		while (bounds.size() > CAP) {
			// Not the key just recorded: Ratis may hold it now.
			var evicted = bounds.entrySet().stream().filter(entry -> !entry.getKey().equals(key)).min(EVICTION)
					.orElseThrow();
			bounds.remove(evicted.getKey());
			floor = Math.max(floor, evicted.getValue());
		}
		return new InstalledConfiguration(floor, bounds);
	}

	/**
	 * The record in {@code raftDir}: {@link #EMPTY} if there is no file, null if it cannot be read or is of the old
	 * format (then it bounds nothing).
	 */
	static @Nullable InstalledConfiguration read(Path raftDir) {
		try {
			var record = parse(raftDir);
			return record == null ? EMPTY : record;
		} catch (IOException | RuntimeException e) {
			return null;
		}
	}

	/** The record in {@code raftDir}, null if there is no file; throws if it cannot be read or is of the old format. */
	private static @Nullable InstalledConfiguration parse(Path raftDir) throws IOException {
		List<String> lines;
		try {
			lines = Files.readAllLines(raftDir.resolve(FILE), StandardCharsets.UTF_8);
		} catch (NoSuchFileException e) {
			return null;
		}
		if (lines.isEmpty() || !lines.getFirst().startsWith(TAG + " ") || (lines.size() - 1) % 4 != 0) {
			throw new IOException("not a record of this format: " + raftDir.resolve(FILE));
		}
		long floor = Long.parseLong(lines.getFirst().substring(TAG.length() + 1).trim());
		var bounds = new LinkedHashMap<Key, Long>();
		for (int i = 1; i < lines.size(); i += 4) {
			bounds.put(new Key(Long.parseLong(lines.get(i).trim()), ids(lines.get(i + 2)), ids(lines.get(i + 3))),
					Long.parseLong(lines.get(i + 1).trim()));
		}
		return new InstalledConfiguration(floor, bounds);
	}

	/** The lock of a Raft directory, and its record as of the file with {@code version}'s attributes. */
	private static final class Cache {
		@Nullable Object version;
		@Nullable InstalledConfiguration record;

		/**
		 * The record of the file (null if none), read again only if its attributes changed: a chunk that changes
		 * nothing costs no read. The file is replaced by a move, so a new file has a new key.
		 */
		@Nullable InstalledConfiguration current(Path raftDir) throws IOException {
			var version = version(raftDir);
			if (version == null || !version.equals(this.version)) {
				record = parse(raftDir);
				this.version = version;
			}
			return record;
		}

		static @Nullable Object version(Path raftDir) throws IOException {
			try {
				var attributes = Files.readAttributes(raftDir.resolve(FILE), BasicFileAttributes.class);
				return List.of(String.valueOf(attributes.fileKey()), attributes.lastModifiedTime(), attributes.size());
			} catch (NoSuchFileException e) {
				return null;
			}
		}
	}

	private static Cache cache(Path raftDir) {
		return CACHES.computeIfAbsent(raftDir.toAbsolutePath().normalize(), _ -> new Cache());
	}

	/**
	 * At a start, before the Raft server: keeps a file of this format, else writes an empty record with the floor
	 * {@code floor} (the term in Ratis' {@code raft-meta}, 0 for a new group).
	 */
	static void initialize(Path raftDir, long floor) throws IOException {
		var cache = cache(raftDir);
		synchronized (cache) {
			try {
				if (cache.current(raftDir) != null) {
					return;
				}
			} catch (IOException | RuntimeException e) {
				// Unreadable, or the single record of round 13: replaced.
			}
			write(raftDir, cache, new InstalledConfiguration(floor, Map.of()));
		}
	}

	/**
	 * Records a chunk of a leader of term {@code leaderTerm} with the configuration ({@code index}, {@code newHalf},
	 * {@code oldHalf}) in {@code raftDir}: replaces the file (synced) only if the record changes. Throws if the file
	 * cannot be read: merging into an empty record would lose its floor.
	 */
	static void update(Path raftDir, long lostFloor, long leaderTerm, long index, List<String> newHalf,
			List<String> oldHalf) throws IOException {
		var cache = cache(raftDir);
		synchronized (cache) {
			var old = cache.current(raftDir);
			var next = with(old, leaderTerm, index, newHalf, oldHalf);
			if (!next.equals(old)) {
				write(raftDir, cache, next);
			}
		}
	}

	/** Under the lock of {@code raftDir}, {@code cache}. */
	private static void write(Path raftDir, Cache cache, InstalledConfiguration record) throws IOException {
		cache.version = null;
		var text = new StringBuilder(TAG).append(' ').append(record.floor).append('\n');
		record.bounds.entrySet().stream().sorted(EVICTION).forEach(entry -> text.append(entry.getKey().index())
				.append('\n').append(entry.getValue()).append('\n').append(String.join(",",
						new TreeSet<>(entry.getKey().newHalf()))).append('\n').append(String.join(",",
						new TreeSet<>(entry.getKey().oldHalf()))).append('\n'));
		var file = raftDir.resolve(FILE);
		var temporary = raftDir.resolve(FILE + ".tmp");
		Files.writeString(temporary, text, StandardCharsets.UTF_8);
		NodeStore.syncFile(temporary);
		try {
			Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
		} catch (AtomicMoveNotSupportedException e) {
			Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING);
		}
		NodeStore.syncDirectory(raftDir);
		cache.record = record;
		cache.version = Cache.version(raftDir);
	}

	/**
	 * Records the configuration of the snapshot chunk {@code request} with its leader's term, if it carries one:
	 * {@link RaftTraffic}, before Ratis sees it.
	 */
	static void record(Path raftDir, long lostFloor, RaftProtos.InstallSnapshotRequestProto request) throws IOException {
		if (!request.hasLastRaftConfigurationLogEntryProto()) {
			return;
		}
		var entry = request.getLastRaftConfigurationLogEntryProto();
		var conf = entry.getConfigurationEntry();
		update(raftDir, lostFloor, request.getLeaderTerm(), entry.getIndex(), peerIds(conf.getPeersList()),
				peerIds(conf.getOldPeersList()));
	}

	private static List<String> peerIds(List<RaftProtos.RaftPeerProto> peers) {
		return peers.stream().map(peer -> peer.getId().toStringUtf8()).toList();
	}

	private static Set<String> ids(String line) {
		var trimmed = line.trim();
		return trimmed.isEmpty() ? Set.of() : Set.copyOf(Arrays.stream(trimmed.split(",")).map(String::trim).toList());
	}
}
