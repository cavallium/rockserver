package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ratis.protocol.RaftPeer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * R3-G3-4: the peers the leader asks before it raises the cluster version include Ratis' staging followers (a peer that
 * {@code add-peer} is bringing in, not in the configuration yet), and a staging peer that lacks a capability keeps the
 * leader from raising the version to it.
 */
@Timeout(120)
class PollPeersTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	/** The staging peer's version: checksums and backups only. */
	private static final long OLDER = Capabilities.CHECKSUM | Capabilities.BACKUP;

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		ReplicaOps.SUPPORTED_FOR_TESTING.clear();
		if (cluster != null) {
			cluster.close();
		}
	}

	private static RaftPeer peer(String id, String address) {
		return RaftPeer.newBuilder().setId(id).setAddress(address).build();
	}

	@Test
	void stagingFollowersAreAskedToo() {
		var raft = new StubRaft();
		raft.currentPeers = List.of(peer("a", "h:1"), peer("b", "h:2"));
		raft.previousPeers = List.of(peer("b", "h:2"), peer("c", "h:3"));
		// d is being added; a appears among the followers too; e has no address (nothing to ask).
		raft.stagingFollowers = List.of(peer("d", "h:4"), peer("a", "h:9"), peer("e", ""));

		var peers = ReplicaOps.pollPeers(raft.division());

		var addresses = new TreeMap<String, String>();
		peers.forEach((id, peer) -> addresses.put(id, peer.getAddress()));
		assertEquals(Map.of("a", "h:1", "b", "h:2", "c", "h:3", "d", "h:4"), addresses);
	}

	/**
	 * In process: the leader's check runs against a division (a stub) whose configuration lacks one member, which it
	 * lists as a staging follower instead. That member answers that it lacks capabilities: no raise. The same check with
	 * the member left out of the followers raises the version, which shows that it is the staging peer's answer that
	 * holds the raise back.
	 */
	@Test
	void aStagingPeerThatLacksACapabilityHoldsTheRaiseBack() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var ops = leader.stateMachine().ops();
		InProcessCluster.await(TIMEOUT, () -> ops.clusterVersion() == Capabilities.SUPPORTED);
		var all = leader.stateMachine().division().getRaftConf().getAllPeers();
		var staging = all.stream().filter(p -> !p.getId().toString().equals(leader.config().nodeId())).findFirst()
				.orElseThrow();
		ReplicaOps.SUPPORTED_FOR_TESTING.put(staging.getId().toString(), OLDER);
		// A raise pending on the leader: its in-memory version only, the replicas keep theirs.
		ops.clusterVersion(OLDER);

		var raft = new StubRaft();
		raft.leader = true;
		raft.leaderReady = true;
		raft.currentPeers = all.stream().filter(p -> !p.equals(staging)).toList();
		raft.stagingFollowers = List.of(staging);
		var groupId = leader.stateMachine().groupId();
		long term = leader.stateMachine().division().getInfo().getCurrentTerm() + 1000;
		// Several rounds (each new term starts a check unless one is running), for 3 s: still no raise.
		long end = System.nanoTime() + Duration.ofSeconds(3).toNanos();
		while (System.nanoTime() - end < 0) {
			raft.term = ++term;
			ops.onLeader(raft.division(), groupId);
			Thread.sleep(100);
		}
		assertEquals(OLDER, ops.clusterVersion(), "raised past what the staging peer supports");

		// The control: without the staging peer the same check raises the version (the member that lacks bits then fails
		// to apply it, which this test does not look at).
		raft.stagingFollowers = List.of();
		long[] round = {term};
		InProcessCluster.await(TIMEOUT, () -> {
			raft.term = ++round[0];
			ops.onLeader(raft.division(), groupId);
			return ops.clusterVersion() == Capabilities.SUPPORTED;
		});
	}
}
