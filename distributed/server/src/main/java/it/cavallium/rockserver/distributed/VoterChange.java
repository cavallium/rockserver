package it.cavallium.rockserver.distributed;

import java.util.Collection;
import java.util.Collections;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.ratis.protocol.RaftPeer;

/** Skeleton (round 14, test commit): counts nothing yet. */
record VoterChange(SortedSet<String> added, SortedSet<String> removed) {

	static final String MULTI_VOTER_REFUSAL = "refused: changes more than one voter";

	VoterChange {
		added = Collections.unmodifiableSortedSet(new TreeSet<>(added));
		removed = Collections.unmodifiableSortedSet(new TreeSet<>(removed));
	}

	int size() {
		return added.size() + removed.size();
	}

	static VoterChange between(Collection<RaftPeer> currentServers, Collection<RaftPeer> newServers) {
		return new VoterChange(new TreeSet<>(), new TreeSet<>());
	}

	static String refusal(VoterChange change, Collection<RaftPeer> currentServers) {
		return MULTI_VOTER_REFUSAL;
	}
}
