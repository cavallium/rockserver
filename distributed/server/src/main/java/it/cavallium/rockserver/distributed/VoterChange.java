package it.cavallium.rockserver.distributed;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.SortedSet;
import java.util.TreeSet;
import org.apache.ratis.protocol.RaftPeer;

/**
 * Which voters a configuration change adds and removes: the ids of the server lists (role FOLLOWER), compared by id.
 * Listeners neither vote nor count toward a commit, so they are not looked at; a voter that becomes a listener leaves
 * the server list (a removal), a listener that becomes a voter enters it (an addition). An address-only or
 * priority-only change counts nothing.
 *
 * <p>{@code admin} and the leader's {@link MembershipGuard} refuse a change of more than one voter (round 14). With
 * Ratis 3.3.1 a leader elected while a joint configuration is uncommitted writes only the new half in its term-start
 * entry ({@code LeaderStateImpl.StartupLogEntry}) and from then on commits with the new half alone. When a majority of
 * the new half and a majority of the old half can be disjoint, the old majority, which never saw the joint entry, can
 * elect another leader that overwrites committed entries. With one voter added or removed every majority of the new
 * half meets every majority of the old half. See "Replacing a node" in the design document.</p>
 */
record VoterChange(SortedSet<String> added, SortedSet<String> removed) {

	/** How every refusal of a change of more than one voter starts; a compare-and-set mismatch does not. */
	static final String MULTI_VOTER_REFUSAL = "refused: changes more than one voter";

	VoterChange {
		added = Collections.unmodifiableSortedSet(new TreeSet<>(added));
		removed = Collections.unmodifiableSortedSet(new TreeSet<>(removed));
	}

	/** How many voters change. */
	int size() {
		return added.size() + removed.size();
	}

	/** Whether the change may go without {@code --unsafe-multi-voter-change}: at most one voter changes. */
	boolean safe() {
		return size() <= 1;
	}

	static VoterChange between(Collection<RaftPeer> currentServers, Collection<RaftPeer> newServers) {
		var current = ids(currentServers);
		var target = ids(newServers);
		var added = new TreeSet<>(target);
		added.removeAll(current);
		var removed = new TreeSet<>(current);
		removed.removeAll(target);
		return new VoterChange(added, removed);
	}

	static SortedSet<String> ids(Collection<RaftPeer> peers) {
		var ids = new TreeSet<String>();
		peers.forEach(peer -> ids.add(peer.getId().toString()));
		return ids;
	}

	/** The message of a refusal of {@code change} against the voters {@code currentServers}. */
	static String refusal(VoterChange change, Collection<RaftPeer> currentServers) {
		var oldNode = change.removed().size() == 1 ? change.removed().first() : "OLD";
		var newNode = change.added().size() == 1 ? change.added().first() : "NEW";
		return MULTI_VOTER_REFUSAL + ": " + change.size() + " voters would change at once (adds " + change.added()
				+ ", removes " + change.removed() + "; the voters now are " + ids(currentServers) + "). With Ratis 3.3.1 a"
				+ " leader elected during such a change keeps only the new voters in its term-start entry and commits with"
				+ " them alone, so committed entries can be lost if the leader changes. Change one voter at a time: for a"
				+ " node that is down, remove-peer " + oldNode + " with --peer listing the current members, then add-peer "
				+ newNode + "=... with --peer listing the members that stay; for a node that runs, add-peer " + newNode
				+ "=... with --peer listing every member, then remove-peer " + oldNode + ". A change of several voters"
				+ " is done one voter per command. See \"Replacing a node\" in docs/distributed/runbooks.md";
	}

	/**
	 * R59-1: the voters of {@code currentServers} whose id differs from {@code id} only by case ({@code n1} for
	 * {@code N1}), sorted; none if {@code id} is a voter itself. Ratis compares ids case-sensitively, so
	 * {@code remove-peer N1} of the voter {@code n1} removes nothing: {@code admin} refuses it as a likely typo.
	 */
	static SortedSet<String> caseVariants(String id, Collection<RaftPeer> currentServers) {
		var voters = ids(currentServers);
		var variants = new TreeSet<String>();
		if (!voters.contains(id)) {
			voters.stream().filter(voter -> voter.equalsIgnoreCase(id)).forEach(variants::add);
		}
		return variants;
	}

	/**
	 * R51-1: the members of {@code newServers} that are voters in {@code currentServers} too, at another address, each as
	 * {@code id (at OLD in the configuration, NEW in --peer)}, in the order of {@code newServers}. Ratis writes the
	 * address of every server of the new configuration as given, and a running leader keeps its connection to a member
	 * by id, so a mistyped address of a member that stays shows only once the nodes restart and read the configuration
	 * from their log. Addresses are compared as written: another spelling of the same host is a change too.
	 */
	static List<String> addressChanges(Collection<RaftPeer> currentServers, Collection<RaftPeer> newServers) {
		var changes = new ArrayList<String>();
		for (var target : newServers) {
			for (var current : currentServers) {
				if (current.getId().equals(target.getId())
						&& !String.valueOf(current.getAddress()).equals(String.valueOf(target.getAddress()))) {
					changes.add(target.getId() + " (at " + current.getAddress() + " in the configuration, "
							+ target.getAddress() + " in --peer)");
				}
			}
		}
		return changes;
	}
}
