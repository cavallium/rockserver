package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import it.cavallium.rockserver.distributed.RemovalRule.Probe;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.util.LifeCycle;
import org.jetbrains.annotations.Nullable;

/**
 * What a node's own Raft log and committed history say about membership (R11): the answer it gives to
 * {@link TermService#CONFIGURATION2} and the facts it decides on itself ({@link RemovalRule.Own}).
 */
final class MembershipFacts {

	private MembershipFacts() {}

	static List<String> ids(Collection<RaftPeer> peers) {
		return peers.stream().map(peer -> peer.getId().toString()).toList();
	}

	/**
	 * The term of the entry at {@code index} of {@code division}'s log; {@link RemovalRule#PURGED} if it is in the
	 * snapshot (at or below the commit index, so the committed entry), {@link RemovalRule#BEYOND} past the end.
	 */
	static long termAt(RaftServer.Division division, long index) {
		var log = division.getRaftLog();
		var termIndex = log.getTermIndex(index);
		if (termIndex != null) {
			return termIndex.getTerm();
		}
		return index >= 0 && index <= log.getSnapshotIndex() ? RemovalRule.PURGED : RemovalRule.BEYOND;
	}

	/** What a node's log holds at the index of a configuration it reports: see {@link #entry}. */
	enum Entry {
		/** That configuration's entry. */
		SAME,
		/** Another entry: another configuration, or an ordinary entry. */
		OTHER,
		/** Nothing: the index is in the snapshot, past the end of the log, or the log could not be read. */
		NONE
	}

	/**
	 * What {@code division}'s log holds at the index of {@code conf}. Ratis' configuration ({@code getRaftConf}) is not
	 * always the entry at its index (R11 review, finding 1): a snapshot install writes the leader's latest
	 * configuration to {@code raft-meta.conf}, committed or not, and a restart loads that file first and then keeps it
	 * against every configuration of the log at a lower index, also when a later leader replaced the entry at its index
	 * with an ordinary one, until the node applies that leader's first configuration and rewrites the file
	 * ({@code ServerState.initialize}, {@code ConfigurationManager.addConfiguration}).
	 */
	static Entry entry(RaftServer.Division division, RaftConfiguration conf) {
		long index = conf.getLogEntryIndex();
		var log = division.getRaftLog();
		if (index < 0 || log.getTermIndex(index) == null) {
			return Entry.NONE;
		}
		RaftProtos.LogEntryProto proto;
		try {
			proto = log.get(index);
		} catch (RaftLogIOException | RuntimeException ex) {
			return Entry.NONE;
		}
		if (proto == null) {
			return Entry.NONE;
		}
		if (!proto.hasConfigurationEntry()) {
			return Entry.OTHER;
		}
		var entry = proto.getConfigurationEntry();
		return sameIds(entry.getPeersList(), conf.getCurrentPeers())
				&& sameIds(entry.getOldPeersList(), conf.getPreviousPeers()) ? Entry.SAME : Entry.OTHER;
	}

	private static boolean sameIds(List<RaftProtos.RaftPeerProto> entry, Collection<RaftPeer> peers) {
		return entry.stream().map(peer -> peer.getId().toStringUtf8()).collect(Collectors.toSet())
				.equals(Set.copyOf(ids(peers)));
	}

	/**
	 * The term of the configuration {@code conf}: from the log if its entry is there, else from the history if that
	 * recorded the same configuration at its index, else unknown. Unknown too when the log holds another entry at its
	 * index: then {@code conf} is a stale configuration of Ratis (see {@link #entry}), and the term of that entry is
	 * not its term.
	 */
	private static long confTerm(RaftServer.Division division, CommittedConfigurations history, RaftConfiguration conf,
			Entry entry) {
		long index = conf.getLogEntryIndex();
		if (entry == Entry.SAME) {
			long term = termAt(division, index);
			if (term > 0) {
				return term;
			}
		}
		if (entry == Entry.OTHER) {
			return RemovalRule.UNKNOWN_TERM;
		}
		var recorded = history.at(index);
		return sameConfiguration(recorded, conf) ? recorded.term() : RemovalRule.UNKNOWN_TERM;
	}

	/** Whether the history's record {@code recorded} is the configuration {@code conf} (both halves). */
	private static boolean sameConfiguration(@Nullable Conf recorded, RaftConfiguration conf) {
		return recorded != null && recorded.oldHalf() != null
				&& Set.copyOf(recorded.newHalf()).equals(Set.copyOf(ids(conf.getCurrentPeers())))
				&& Set.copyOf(recorded.oldHalf()).equals(Set.copyOf(ids(conf.getPreviousPeers())));
	}

	/** {@code conf} with the term {@code term}. */
	static Conf conf(RaftConfiguration conf, long term) {
		return new Conf(term, conf.getLogEntryIndex(), ids(conf.getCurrentPeers()), ids(conf.getPreviousPeers()));
	}

	/** The latest configuration of {@code division}'s log, committed or not, with its term if known. */
	static Conf latest(RaftServer.Division division, CommittedConfigurations history) {
		var conf = division.getRaftConf();
		return conf(conf, conf.getLogEntryIndex() < 0 ? RemovalRule.UNKNOWN_TERM
				: confTerm(division, history, conf, entry(division, conf)));
	}

	/**
	 * The newest committed configuration of this node: the newest it applied ({@code history}), or its latest
	 * configuration if that is at or below its commit index and in its log or snapshot (Ratis' commit index covers only
	 * entries that match the leader's log). A configuration that a snapshot install took from the leader, past the
	 * snapshot and not in the log yet, may not be committed: it does not count. Nor does a configuration whose index
	 * holds another entry in the log (a stale one, see {@link #entry}): the commit index covers that entry, not it. In
	 * the snapshot, where the log no longer tells, it counts only if the history recorded it at its index, or does not
	 * cover that index (a database of an older version): a stale configuration stays in Ratis' memory after the entry
	 * at its index is purged (round-13 review).
	 * Null if none.
	 */
	static @Nullable Conf committed(RaftServer.Division division, CommittedConfigurations history) {
		var applied = history.last();
		var conf = division.getRaftConf();
		long index = conf.getLogEntryIndex();
		var log = division.getRaftLog();
		Conf derived = null;
		if (index >= 0 && index <= log.getLastCommittedIndex()) {
			var entry = entry(division, conf);
			boolean inSnapshot = entry == Entry.NONE && log.getTermIndex(index) == null && index <= log.getSnapshotIndex();
			if (inSnapshot) {
				var recorded = history.at(index);
				inSnapshot = recorded != null ? sameConfiguration(recorded, conf) : index < history.completeFrom();
			}
			if (entry == Entry.SAME || inSnapshot) {
				derived = conf(conf, confTerm(division, history, conf, entry));
			}
		}
		if (derived == null) {
			return applied;
		}
		if (applied == null || derived.index() > applied.index()) {
			return derived;
		}
		if (derived.index() == applied.index()) {
			var merged = RemovalRule.merge(List.of(applied, derived));
			return merged != null ? merged : applied;
		}
		return applied;
	}

	/**
	 * The index below which the configuration of a snapshot chunk from the leader of {@code leaderTerm} shows an install
	 * stream that leader gave up on (R16-2), {@link RaftTraffic#NO_INDEX} if none: see
	 * {@link #installFloor(Conf, Conf, long)}, with this node's committed and latest configurations.
	 */
	static long installFloor(RaftServer.Division division, CommittedConfigurations history, long leaderTerm) {
		return installFloor(committed(division, history), latest(division, history), leaderTerm);
	}

	/**
	 * The index below which the configuration of a snapshot chunk from the leader of {@code leaderTerm} shows an install
	 * stream that leader gave up on (R16-2), {@link RaftTraffic#NO_INDEX} if none. A leader builds each chunk with its
	 * latest configuration ({@code LeaderProtoUtils} reads {@code getRaftConf()}), whose index is at least that of every
	 * committed configuration (Leader Completeness) and of every configuration it appended itself, and its appender
	 * sends no appends while an install runs: so a chunk of a live stream is never below the {@code committed}
	 * configuration, nor below the {@code latest} one if that is of {@code leaderTerm} (only that leader appends
	 * entries of its term). The latest of another term, or of a term not known, gives no floor: a later leader may not
	 * hold it (uncommitted), and an earlier one is answered NOT_LEADER before this. The latest configuration is the
	 * newest in the log, so the newest of {@code leaderTerm} if the node's term is not above it; Ratis' stale
	 * {@code raft-meta.conf} (see {@link #entry}), whose term is unknown, only hides that floor.
	 */
	static long installFloor(@Nullable Conf committed, Conf latest, long leaderTerm) {
		long floor = committed == null ? RaftTraffic.NO_INDEX : committed.index();
		return floor;
	}

	/**
	 * The node's own facts for {@link RemovalRule#decide}: {@code self}, its committed and latest configurations, the
	 * terms of its log, whether its latest configuration is stale ({@link Entry#OTHER}), whether it was a voter once,
	 * and the bound of its latest configuration's term that {@code installed} gives (round-13 review, finding 1).
	 */
	static RemovalRule.Own own(RaftServer.Division division, CommittedConfigurations history, String self,
			boolean seeded, boolean memberFile, @Nullable InstalledConfiguration installed) {
		var conf = division.getRaftConf();
		var latest = latest(division, history);
		return new RemovalRule.Own(self, committed(division, history), latest, index -> termAt(division, index), seeded,
				memberFile, conf.getLogEntryIndex() >= 0 && entry(division, conf) == Entry.OTHER,
				InstalledConfiguration.boundOf(installed, latest));
	}

	/** What {@code division}'s log says about the entry ({@code term}, {@code index}). */
	static Probe probe(RaftServer.Division division, CommittedConfigurations history, long term, long index) {
		if (term <= RemovalRule.UNKNOWN_TERM || index < 0) {
			return Probe.UNKNOWN;
		}
		var log = division.getRaftLog();
		var found = log.getTermIndex(index);
		if (found != null) {
			if (found.getTerm() != term) {
				return Probe.ABSENT;
			}
			// Probes are of configurations: an ordinary entry at (term, index) is the only entry there in any log
			// (Log Matching), so no configuration has that term and index (a stale one of Ratis, see entry()).
			var body = configurationBody(log, index);
			if (body == null) {
				return Probe.UNKNOWN;
			}
			if (!body) {
				return Probe.ABSENT;
			}
			// Applied here, so committed, also while the commit index of a restarted node has not caught up yet.
			var applied = history.at(index);
			return index <= log.getLastCommittedIndex() || (applied != null && applied.term() == term) ? Probe.COMMITTED
					: Probe.PRESENT;
		}
		if (index > log.getSnapshotIndex()) {
			// Past the end of the log (a log is a contiguous run after the snapshot).
			return Probe.ABSENT;
		}
		// In the snapshot: committed here, so the history decides where it recorded a configuration. Probes are of
		// configurations believed committed (K*, or the asker's latest, of which only COMMITTED counts). No record is
		// UNKNOWN, never ABSENT (R12-2): during a snapshot install Ratis moves the snapshot index before the state
		// machine swaps in the snapshot's history, and for a K* that really is committed "complete history, no record"
		// cannot happen otherwise, so ABSENT there would only ever drop a blocker wrongly.
		var recorded = history.at(index);
		return recorded == null ? Probe.UNKNOWN : recorded.term() == term ? Probe.COMMITTED : Probe.ABSENT;
	}

	/** Whether the entry at {@code index} of {@code log} is a configuration entry; null if it cannot tell. */
	private static @Nullable Boolean configurationBody(RaftLog log, long index) {
		try {
			var headers = log.getEntries(index, index + 1);
			if (headers != null && headers.length == 1 && headers[0] != null && headers[0].getIndex() == index) {
				return headers[0].getLogEntryBodyCase() == RaftProtos.LogEntryProto.LogEntryBodyCase.CONFIGURATIONENTRY;
			}
		} catch (RuntimeException ex) {
			return null;
		}
		return null;
	}

	/** The answer to {@link TermService#CONFIGURATION2}, for the probes {@code probes} (at most two). */
	static RemovalRule.Report report(RaftServer.Division division, CommittedConfigurations history,
			List<TermIndex> probes) {
		var info = division.getInfo();
		var addresses = new LinkedHashMap<String, String>();
		for (var peer : division.getRaftConf().getAllPeers()) {
			if (peer.getAddress() != null) {
				addresses.put(peer.getId().toString(), peer.getAddress());
			}
		}
		var own = probes.isEmpty() ? Probe.UNKNOWN : probe(division, history, probes.get(0).getTerm(),
				probes.get(0).getIndex());
		long probedTerm = probes.size() < 2 ? RemovalRule.UNKNOWN_TERM : probes.get(1).getTerm();
		long probedIndex = probes.size() < 2 ? -1 : probes.get(1).getIndex();
		var kStar = probes.size() < 2 ? Probe.UNKNOWN : probe(division, history, probedTerm, probedIndex);
		return new RemovalRule.Report(TermService.CONFIGURATION_VERSION,
				info.getLifeCycleState() == LifeCycle.State.RUNNING, info.getLeaderId() != null, info.getCurrentTerm(),
				division.getRaftLog().getLastCommittedIndex(), committed(division, history), latest(division, history), own,
				probedTerm, probedIndex, kStar, addresses);
	}
}
