package it.cavallium.rockserver.distributed;

import org.jetbrains.annotations.Nullable;

/**
 * What a node tells about itself on its Raft port, through {@link TermService#nodeState}.
 *
 * @param nextLogIndex the index that the next entry of its Raft log will get: 0 if it never had an entry
 * @param seeded whether its cluster started from a seed database
 * @param hasSnapshot whether it holds a snapshot that a new member can start from
 * @param startedOutsideJoin whether it was started without {@code --join} with a peer list of its own, and has no
 *                           configuration from a log yet
 * @param clusterId the cluster its database belongs to, null if it does not belong to one yet
 */
record NodeState(long nextLogIndex, boolean seeded, boolean hasSnapshot, boolean startedOutsideJoin,
		@Nullable String clusterId) {

	/**
	 * Why {@code add-peer} must not add a node in this state to the cluster {@code clusterId}, or null if it may. Raft
	 * compares log entries by term and index only, and a leader of term 1 of another cluster writes the same term and
	 * indices as a leader of this one. So the entries of a node that elected itself would pass for this cluster's, and
	 * would never be replaced.
	 *
	 * @param id the node's id
	 * @param clusterId the id of the cluster, null if no member knows it yet
	 */
	@Nullable String refusalToJoin(String id, @Nullable String clusterId) {
		if (this.clusterId != null) {
			if (clusterId == null || this.clusterId.equals(clusterId)) {
				return null;
			}
			return "Node " + id + " belongs to cluster " + this.clusterId + ", not to cluster " + clusterId + ": it has a log"
					+ " of its own. Wipe its data directory and start it with --join";
		}
		if (nextLogIndex > 0) {
			return "Node " + id + " has a log of its own, " + nextLogIndex + " entries, and no cluster: it elected itself or"
					+ " started another cluster. Wipe its data directory and start it with --join";
		}
		if (startedOutsideJoin) {
			return "Node " + id + " was started without --join, and that start stays in its data directory. Wipe the"
					+ " directory and start it with --join";
		}
		return null;
	}
}
