package it.cavallium.rockserver.distributed;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.RaftLog;

/** What the membership tests share: configuration changes, waits, and the message filters of {@link RaftTraffic}. */
final class RemovalScenarios {

	static final Duration TIMEOUT = Duration.ofSeconds(60);

	private RemovalScenarios() {}

	/** The leader, once it committed the first entry of its term: before that Ratis refuses a configuration change. */
	static ReplicatedNode awaitReadyLeader(InProcessCluster cluster) {
		var leader = cluster.awaitLeader(TIMEOUT);
		InProcessCluster.await(TIMEOUT, () -> leader.isLeader()
				&& Objects.requireNonNull(leader.stateMachine().division()).getInfo().isLeaderReady());
		return leader;
	}

	/**
	 * A ready leader among the running nodes, whatever the others think: unlike {@link #awaitReadyLeader}, for a cluster
	 * with a running node that follows no leader (a joiner before its add, a removed node).
	 */
	static ReplicatedNode awaitAnyReadyLeader(InProcessCluster cluster) {
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().anyMatch(node -> node.isLeader()
				&& Objects.requireNonNull(node.stateMachine().division()).getInfo().isLeaderReady()));
		return cluster.runningNodes().stream().filter(ReplicatedNode::isLeader).findFirst().orElseThrow();
	}

	/** Asks {@code leader} to change the configuration to {@code ids} (Ratis' setConfiguration, as add/remove-peer do). */
	static CompletableFuture<RaftClientReply> setConfiguration(InProcessCluster cluster, ReplicatedNode leader,
			List<String> ids) throws Exception {
		var division = Objects.requireNonNull(leader.stateMachine().division());
		return division.getRaftServer().setConfigurationAsync(new SetConfigurationRequest(ClientId.randomId(),
				RaftPeerId.valueOf(leader.config().nodeId()), division.getGroup().getGroupId(), 1, cluster.raftPeers(ids)));
	}

	/** The latest configuration of {@code node}'s log. */
	static org.apache.ratis.server.RaftConfiguration conf(ReplicatedNode node) {
		return Objects.requireNonNull(node.stateMachine().division()).getRaftConf();
	}

	static boolean holds(List<org.apache.ratis.protocol.RaftPeer> peers, String id) {
		return peers.stream().anyMatch(peer -> peer.getId().toString().equals(id));
	}

	/** Whether {@code node}'s configuration is a joint one that holds {@code id} in its old half only. */
	static boolean jointWithoutIn(ReplicatedNode node, String id) {
		var conf = conf(node);
		return holds(List.copyOf(conf.getPreviousPeers()), id) && !holds(List.copyOf(conf.getCurrentPeers()), id);
	}

	/** Whether {@code node}'s configuration is a joint one that holds {@code id} in its new half only. */
	static boolean jointWithIn(ReplicatedNode node, String id) {
		var conf = conf(node);
		return !conf.getPreviousPeers().isEmpty() && !holds(List.copyOf(conf.getPreviousPeers()), id)
				&& holds(List.copyOf(conf.getCurrentPeers()), id);
	}

	/** Whether {@code node}'s configuration is not joint and leaves {@code id} out. */
	static boolean stableWithout(ReplicatedNode node, String id) {
		var conf = conf(node);
		return conf.getLogEntryIndex() >= 0 && conf.getPreviousPeers().isEmpty()
				&& !holds(List.copyOf(conf.getCurrentPeers()), id);
	}

	/** Whether {@code node}'s configuration is not joint and holds {@code id}. */
	static boolean stableWith(ReplicatedNode node, String id) {
		var conf = conf(node);
		return conf.getPreviousPeers().isEmpty() && holds(List.copyOf(conf.getCurrentPeers()), id);
	}

	/**
	 * Waits until {@code node}'s latest configuration is in its log on disk: Ratis takes a configuration from an append
	 * before it writes the entry, and a halt can drop a write still queued.
	 */
	static void awaitConfigurationOnDisk(ReplicatedNode node) {
		var division = Objects.requireNonNull(node.stateMachine().division());
		long index = division.getRaftConf().getLogEntryIndex();
		InProcessCluster.await(TIMEOUT, () -> division.getRaftLog().getFlushIndex() >= index);
	}

	/** The (term, index) of {@code node}'s latest configuration entry. */
	static TermIndex confEntry(ReplicatedNode node) {
		var division = Objects.requireNonNull(node.stateMachine().division());
		long index = division.getRaftConf().getLogEntryIndex();
		RaftLog log = division.getRaftLog();
		return Objects.requireNonNull(log.getTermIndex(index), "configuration entry " + index);
	}

	/** The Raft peer id that sent {@code message}, or null for a message that is not a Raft request. */
	static String sender(Object message) {
		if (message instanceof RaftProtos.AppendEntriesRequestProto append) {
			return append.getServerRequest().getRequestorId().toStringUtf8();
		}
		if (message instanceof RaftProtos.RequestVoteRequestProto vote) {
			return vote.getServerRequest().getRequestorId().toStringUtf8();
		}
		if (message instanceof RaftProtos.InstallSnapshotRequestProto install) {
			return install.getServerRequest().getRequestorId().toStringUtf8();
		}
		return null;
	}

	static boolean isAppend(Object message) {
		return message instanceof RaftProtos.AppendEntriesRequestProto
				|| message instanceof RaftProtos.InstallSnapshotRequestProto;
	}

	static boolean isVote(Object message) {
		return message instanceof RaftProtos.RequestVoteRequestProto;
	}

	/** Whether {@code message} is an append that carries a configuration entry matching {@code test}. */
	static boolean carriesConfiguration(Object message,
			java.util.function.Predicate<RaftProtos.RaftConfigurationProto> test) {
		return message instanceof RaftProtos.AppendEntriesRequestProto append && append.getEntriesList().stream()
				.anyMatch(entry -> entry.hasConfigurationEntry() && test.test(entry.getConfigurationEntry()));
	}

	static boolean holdsId(List<RaftProtos.RaftPeerProto> peers, String id) {
		return peers.stream().anyMatch(peer -> peer.getId().toStringUtf8().equals(id));
	}
}
