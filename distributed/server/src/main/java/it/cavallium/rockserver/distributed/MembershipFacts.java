package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import it.cavallium.rockserver.distributed.RemovalRule.Probe;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
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

	/** The term of the configuration entry at {@code index}: from the log, else from the history, else unknown. */
	private static long confTerm(RaftServer.Division division, CommittedConfigurations history, long index) {
		long term = termAt(division, index);
		if (term > 0) {
			return term;
		}
		var recorded = history.at(index);
		return recorded != null ? recorded.term() : RemovalRule.UNKNOWN_TERM;
	}

	/** {@code conf} with the term {@code term}. */
	static Conf conf(RaftConfiguration conf, long term) {
		return new Conf(term, conf.getLogEntryIndex(), ids(conf.getCurrentPeers()), ids(conf.getPreviousPeers()));
	}

	/** The latest configuration of {@code division}'s log, committed or not, with its term if known. */
	static Conf latest(RaftServer.Division division, CommittedConfigurations history) {
		var conf = division.getRaftConf();
		return conf(conf, conf.getLogEntryIndex() < 0 ? RemovalRule.UNKNOWN_TERM
				: confTerm(division, history, conf.getLogEntryIndex()));
	}

	/**
	 * The newest committed configuration of this node: the newest it applied ({@code history}), or its latest
	 * configuration if that is at or below its commit index and in its log or snapshot (Ratis' commit index covers only
	 * entries that match the leader's log). A configuration that a snapshot install took from the leader, past the
	 * snapshot and not in the log yet, may not be committed: it does not count. Null if none.
	 */
	static @Nullable Conf committed(RaftServer.Division division, CommittedConfigurations history) {
		var applied = history.last();
		var conf = division.getRaftConf();
		long index = conf.getLogEntryIndex();
		var log = division.getRaftLog();
		Conf derived = null;
		if (index >= 0 && index <= log.getLastCommittedIndex()
				&& (log.getTermIndex(index) != null || index <= log.getSnapshotIndex())) {
			derived = conf(conf, confTerm(division, history, index));
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
			// Applied here, so committed, also while the commit index of a restarted node has not caught up yet.
			var applied = history.at(index);
			return index <= log.getLastCommittedIndex() || (applied != null && applied.term() == term) ? Probe.COMMITTED
					: Probe.PRESENT;
		}
		if (index > log.getSnapshotIndex()) {
			// Past the end of the log (a log is a contiguous run after the snapshot).
			return Probe.ABSENT;
		}
		// In the snapshot: committed here, so the history decides, where it is complete. Probes are of configurations.
		var recorded = history.at(index);
		if (recorded != null) {
			return recorded.term() == term ? Probe.COMMITTED : Probe.ABSENT;
		}
		return index >= history.completeFrom() ? Probe.ABSENT : Probe.UNKNOWN;
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
