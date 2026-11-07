package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Arrays;
import java.util.List;
import java.util.Set;
import org.apache.ratis.proto.RaftProtos;
import org.jetbrains.annotations.Nullable;

/**
 * An upper bound of the term of the configuration a snapshot install gave Ratis (round-13 review, finding 1), kept in
 * the Raft directory as {@link #FILE}. A leader sends its latest configuration with each snapshot chunk without its
 * term ({@code LeaderProtoUtils.toInstallSnapshotRequestProtoBuilder} passes none), and Ratis writes it to
 * {@code raft-meta.conf} and into memory, often past the end of the node's log: {@link MembershipFacts} cannot tell
 * its term. The leader's term bounds it (a leader holds no entry of a later term), so {@link RaftTraffic} records,
 * before Ratis sees a chunk, the index and halves of its configuration with the leader's term: the highest term seen
 * for that index and those halves, or the new one in place of a record of another configuration.
 *
 * <p>A record never bounds the configuration Ratis holds too low. It is written before Ratis acts on the chunk; a
 * chunk Ratis rejects leaves a record of another configuration (which then matches none, and bounds nothing) or a
 * higher bound for the same one. Ratis replaces its configuration only with one that differs (index or peers), so a
 * later configuration with the same index and halves keeps the one in memory, whose record then holds the higher
 * term. {@link RemovalRule} uses the bound only to tell that such a configuration is older than K*.</p>
 */
record InstalledConfiguration(long index, long termBound, List<String> newHalf, List<String> oldHalf) {

	static final String FILE = "installed-configuration";

	InstalledConfiguration {
		newHalf = List.copyOf(newHalf);
		oldHalf = List.copyOf(oldHalf);
	}

	/** Whether this record is of the configuration ({@code index}, {@code newHalf}, {@code oldHalf}). */
	private boolean of(long index, List<String> newHalf, @Nullable List<String> oldHalf) {
		return this.index == index && oldHalf != null && Set.copyOf(this.newHalf).equals(Set.copyOf(newHalf))
				&& Set.copyOf(this.oldHalf).equals(Set.copyOf(oldHalf));
	}

	/** The bound of the term of {@code conf}, {@link RemovalRule#UNKNOWN_TERM} if {@code record} is not of it. */
	static long boundOf(@Nullable InstalledConfiguration record, @Nullable Conf conf) {
		return record != null && conf != null && record.of(conf.index(), conf.newHalf(), conf.oldHalf())
				? record.termBound : RemovalRule.UNKNOWN_TERM;
	}

	/** {@code record} after a chunk of a leader of term {@code leaderTerm} with the given configuration. */
	static InstalledConfiguration next(@Nullable InstalledConfiguration record, long leaderTerm, long index,
			List<String> newHalf, List<String> oldHalf) {
		if (record != null && record.of(index, newHalf, oldHalf) && record.termBound >= leaderTerm) {
			return record;
		}
		return new InstalledConfiguration(index, leaderTerm, newHalf, oldHalf);
	}

	/** R15 tests: at most this many configurations are kept. Stub: one record is kept. */
	static final int CAP = 32;

	/** R15 tests: the record after a chunk. Stub: {@link #next}, the old replacement. */
	static InstalledConfiguration with(@Nullable InstalledConfiguration record, long leaderTerm, long index,
			List<String> newHalf, List<String> oldHalf) {
		return next(record, leaderTerm, index, newHalf, oldHalf);
	}

	/** R15 tests: the file at a start, with the floor {@code floor}. Stub: keeps the file as it is. */
	static void initialize(Path raftDir, long floor) {
	}

	/** The record in {@code raftDir}, null if none or unreadable (then it bounds nothing). */
	static @Nullable InstalledConfiguration read(Path raftDir) {
		try {
			var lines = Files.readAllLines(raftDir.resolve(FILE), StandardCharsets.UTF_8);
			if (lines.size() < 4) {
				return null;
			}
			return new InstalledConfiguration(Long.parseLong(lines.get(0).trim()), Long.parseLong(lines.get(1).trim()),
					ids(lines.get(2)), ids(lines.get(3)));
		} catch (IOException | RuntimeException e) {
			return null;
		}
	}

	/**
	 * Records a chunk of a leader of term {@code leaderTerm} with the configuration ({@code index}, {@code newHalf},
	 * {@code oldHalf}) in {@code raftDir}: replaces the file (synced) only if the record changes.
	 */
	static void update(Path raftDir, long leaderTerm, long index, List<String> newHalf, List<String> oldHalf)
			throws IOException {
		var old = read(raftDir);
		var next = next(old, leaderTerm, index, newHalf, oldHalf);
		if (next.equals(old)) {
			return;
		}
		var file = raftDir.resolve(FILE);
		var temporary = raftDir.resolve(FILE + ".tmp");
		Files.writeString(temporary, next.index + "\n" + next.termBound + "\n" + String.join(",", next.newHalf) + "\n"
				+ String.join(",", next.oldHalf) + "\n", StandardCharsets.UTF_8);
		NodeStore.syncFile(temporary);
		try {
			Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
		} catch (AtomicMoveNotSupportedException e) {
			Files.move(temporary, file, StandardCopyOption.REPLACE_EXISTING);
		}
		NodeStore.syncDirectory(raftDir);
	}

	/**
	 * Records the configuration of the snapshot chunk {@code request} with its leader's term, if it carries one:
	 * {@link RaftTraffic}, before Ratis sees it.
	 */
	static void record(Path raftDir, RaftProtos.InstallSnapshotRequestProto request) throws IOException {
		if (!request.hasLastRaftConfigurationLogEntryProto()) {
			return;
		}
		var entry = request.getLastRaftConfigurationLogEntryProto();
		var conf = entry.getConfigurationEntry();
		update(raftDir, request.getLeaderTerm(), entry.getIndex(), peerIds(conf.getPeersList()),
				peerIds(conf.getOldPeersList()));
	}

	private static List<String> peerIds(List<RaftProtos.RaftPeerProto> peers) {
		return peers.stream().map(peer -> peer.getId().toStringUtf8()).toList();
	}

	private static List<String> ids(String line) {
		var trimmed = line.trim();
		return trimmed.isEmpty() ? List.of() : Arrays.stream(trimmed.split(",")).map(String::trim).toList();
	}
}
