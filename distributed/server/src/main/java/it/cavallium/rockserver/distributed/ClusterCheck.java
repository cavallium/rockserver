package it.cavallium.rockserver.distributed;

import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Tells each follower of a leader, once per term, which cluster it follows: a follower of another cluster stops.
 *
 * <p>{@code add-peer} refuses a node that has a log of its own, but the node may get one right after, and the Ratis
 * tools do not ask. Nothing in the log would show it: the group id is the same by default, a leader of term 1 writes the
 * same terms and indices in any cluster, and the node then keeps its own entries as if they were this cluster's. The
 * check needs no entry and looks at no position in the log, so it works during the catch-up of {@code add-peer}, while
 * the node is not a member yet, and the node leaves before the configuration changes.</p>
 *
 * <p>It also compares the digest of the Rockserver config of each follower with its own ({@link NodeStore#configDigest}):
 * every node must run the same config file. A difference is logged on both nodes, once per term, and stops nothing: a
 * rolling change of the config passes through it, and some settings (such as cache sizes) may differ on purpose.</p>
 *
 * <p>Runs on the initialization thread of the node.</p>
 */
final class ClusterCheck {

	private static final Logger LOG = LoggerFactory.getLogger(ClusterCheck.class);
	/** After a follower that did not answer, usually one that is down. */
	private static final Duration RETRY_AFTER = Duration.ofSeconds(5);

	private final String nodeId;
	private final RaftGroupId groupId;
	private final Duration timeout;
	private final @Nullable String configDigest;
	private final Set<String> checked = ConcurrentHashMap.newKeySet();
	private final Map<String, Long> nextTryNanos = new HashMap<>();
	private long term = -1;

	ClusterCheck(String nodeId, RaftGroupId groupId, Duration timeout) {
		this(nodeId, groupId, timeout, null);
	}

	/** @param configDigest the digest of this node's Rockserver config, null to compare none */
	ClusterCheck(String nodeId, RaftGroupId groupId, Duration timeout, @Nullable String configDigest) {
		this.nodeId = nodeId;
		this.groupId = groupId;
		this.timeout = timeout;
		this.configDigest = configDigest;
	}

	/**
	 * @param term the term that this node leads
	 * @param clusterId the cluster of this node
	 * @param followers Raft address by peer id, with the peers that are still catching up to join
	 */
	void check(long term, String clusterId, Map<String, String> followers, long nowNanos) {
		if (term != this.term) {
			this.term = term;
			checked.clear();
			nextTryNanos.clear();
		}
		followers.forEach((id, address) -> {
			var nextTry = nextTryNanos.get(id);
			if (!checked.contains(id) && (nextTry == null || nowNanos - nextTry >= 0)) {
				nextTryNanos.put(id, nowNanos + RETRY_AFTER.toNanos());
				tell(id, address, clusterId);
			}
		});
	}

	private void tell(String id, String address, String clusterId) {
		// A channel of its own, shut down after the one call: a follower is told once per term.
		var channel = PeerChannels.open(address);
		TermService.nodeState(channel, groupId, clusterId, timeout).whenComplete((state, error) -> {
			if (error != null) {
				channel.shutdown();
				LOG.debug("{}: follower {} did not answer the cluster check", nodeId, id, error);
			} else if (state != null && state.clusterId() != null && !state.clusterId().equals(clusterId)) {
				channel.shutdown();
				LOG.error("{}: follower {} belongs to cluster {}, not to cluster {}: it stops", nodeId, id, state.clusterId(),
						clusterId);
			} else {
				checked.add(id);
				compareConfig(id, channel);
			}
		});
	}

	private void compareConfig(String id, ManagedChannel channel) {
		if (configDigest == null) {
			channel.shutdown();
			return;
		}
		TermService.configDigest(channel, configDigest, timeout).whenComplete((digest, error) -> {
			channel.shutdown();
			if (error != null) {
				// UNIMPLEMENTED on a node of an older version: nothing to compare.
				LOG.debug("{}: follower {} did not tell its config digest", nodeId, id, error);
			} else if (!digest.equals(configDigest)) {
				LOG.error("{}: follower {} runs another Rockserver config (digest {}) than this node ({}): every node must run"
						+ " the same config file", nodeId, id, digest, configDigest);
			}
		});
	}
}
