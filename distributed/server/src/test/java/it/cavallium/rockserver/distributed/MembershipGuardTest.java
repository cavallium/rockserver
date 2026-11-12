package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.impl.ClientProtoUtils;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.SetConfigurationException;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.ProtoUtils;
import org.apache.ratis.util.TimeDuration;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * Round 14: the leader's {@link MembershipGuard} on Ratis' admin service. A configuration change that changes more than
 * one voter, sent through gRPC as any Ratis client (an old {@code admin} too) sends it, is refused at once with
 * {@link VoterChange#MULTI_VOTER_REFUSAL}, whichever member the client asks first; single changes, address-only and
 * priority-only changes go through; the header of {@code --unsafe-multi-voter-change} lets a multi-voter change through.
 * The guard turns what it lets through into a compare-and-set against the configuration it checked.
 */
@Timeout(180)
@Tag("slow")
class MembershipGuardTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	/** Far below the clients' retry time (60 s): a refusal is not retried. */
	private static final Duration AT_ONCE = Duration.ofSeconds(10);
	private static final List<String> ORIGINALS = List.of("n0", "n1", "n2");

	/** Kept when a test fails, for the node logs. */
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	private RaftGroupId groupId() {
		return RaftGroupId.valueOf(UUID.fromString(cluster.config("n0").groupId()));
	}

	/** A gRPC client of the group {@code ids} (in that order), asking {@code first} first unless null. */
	private RaftClient admin(List<String> ids, @Nullable String first) {
		var builder = RaftClient.newBuilder()
				.setClientId(ClientId.randomId())
				.setRaftGroup(RaftGroup.valueOf(groupId(), cluster.raftPeers(ids)))
				.setProperties(RaftSettings.client(cluster.config("n0")))
				.setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(120,
						TimeDuration.valueOf(500, TimeUnit.MILLISECONDS)));
		if (first != null) {
			builder.setLeaderId(RaftPeerId.valueOf(first));
		}
		return builder.build();
	}

	private static List<String> ids(Collection<RaftPeer> peers) {
		return peers.stream().map(peer -> peer.getId().toString()).sorted().toList();
	}

	private static List<String> protoIds(List<RaftPeerProto> peers) {
		return ids(ProtoUtils.toRaftPeers(peers));
	}

	private static List<String> with(List<String> ids, String... more) {
		var all = new ArrayList<>(ids);
		all.addAll(Arrays.asList(more));
		return all;
	}

	/** The leader ({@link InProcessCluster#awaitLeader} also waits for a --join node to know it, which it does not). */
	private ReplicatedNode leader() {
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().anyMatch(ReplicatedNode::isLeader));
		return cluster.runningNodes().stream().filter(ReplicatedNode::isLeader).findFirst().orElseThrow();
	}

	private void assertVoters(List<String> expected) {
		var conf = leader().stateMachine().division().getRaftConf();
		assertEquals(expected.stream().sorted().toList(), ids(conf.getCurrentPeers()));
		assertTrue(conf.getPreviousPeers().isEmpty(), () -> "a change is under way: " + conf);
	}

	private static void assertRefusedForMoreThanOneVoter(IOException error) {
		for (Throwable cause = error; cause != null; cause = cause.getCause()) {
			if (cause instanceof SetConfigurationException
					&& String.valueOf(cause.getMessage()).contains(VoterChange.MULTI_VOTER_REFUSAL)) {
				return;
			}
		}
		throw new AssertionError("not the guard's refusal", error);
	}

	@Test
	void aChangeOfTwoVotersIsRefusedAtOnce() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		cluster.awaitLeader(TIMEOUT);
		cluster.join("n3");
		cluster.join("n4");
		long start = System.nanoTime();
		try (var client = admin(ORIGINALS, null)) {
			var error = assertThrows(IOException.class,
					() -> client.admin().setConfiguration(cluster.raftPeers(with(ORIGINALS, "n3", "n4"))));
			assertRefusedForMoreThanOneVoter(error);
		}
		var elapsed = Duration.ofNanos(System.nanoTime() - start);
		assertTrue(elapsed.compareTo(AT_ONCE) < 0, () -> "took " + elapsed);
		assertVoters(ORIGINALS);
	}

	@Test
	void aClientThatAsksAFollowerFirstIsRefusedByTheLeader() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower().config().nodeId();
		var order = new ArrayList<String>();
		order.add(follower);
		ORIGINALS.stream().filter(id -> !id.equals(follower)).forEach(order::add);
		cluster.join("n3");
		// One voter removed and one added: as many members as before, still two changes.
		var target = with(ORIGINALS.stream().filter(id -> !id.equals(follower)).toList(), "n3");
		try (var client = admin(order, follower)) {
			var error = assertThrows(IOException.class, () -> client.admin().setConfiguration(cluster.raftPeers(target)));
			assertRefusedForMoreThanOneVoter(error);
		}
		assertVoters(ORIGINALS);
	}

	@Test
	void singleChangesAndAddressOrPriorityChangesGoThrough() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		cluster.awaitLeader(TIMEOUT);
		var joined = cluster.join("n3");
		var withJoined = with(ORIGINALS, "n3");
		try (var client = admin(ORIGINALS, null)) {
			assertTrue(client.admin().setConfiguration(cluster.raftPeers(withJoined)).isSuccess());
			assertVoters(withJoined);
			// The same request again (a re-run after a success) changes nothing and succeeds.
			assertTrue(client.admin().setConfiguration(cluster.raftPeers(withJoined)).isSuccess());

			// Address only: n3 under another name of the same host; the guard lets it through.
			var peers = new ArrayList<>(cluster.raftPeers(withJoined));
			var moved = "localhost:" + joined.config().self().raftPort();
			peers.set(3, RaftPeer.newBuilder(peers.get(3)).setAddress(moved).build());
			assertTrue(client.admin().setConfiguration(peers).isSuccess());
			// Ratis compares peers by id only: an address-only change alone is "no change" to it, and succeeds.

			// Priority only.
			peers.set(1, RaftPeer.newBuilder(peers.get(1)).setPriority(1).build());
			assertTrue(client.admin().setConfiguration(peers).isSuccess());
			assertVoters(withJoined);

			assertTrue(client.admin().setConfiguration(cluster.raftPeers(ORIGINALS)).isSuccess());
		}
		assertVoters(ORIGINALS);
	}

	@Test
	void theUnsafeHeaderLetsAChangeOfTwoVotersThrough() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		cluster.awaitLeader(TIMEOUT);
		cluster.join("n3");
		cluster.join("n4");
		var all = with(ORIGINALS, "n3", "n4");
		var arguments = SetConfigurationRequest.Arguments.newBuilder()
				.setServersInCurrentConf(cluster.raftPeers(ORIGINALS))
				.setServersInNewConf(cluster.raftPeers(all))
				.setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET)
				.build();
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (true) {
			var leader = leader();
			var reply = ReplicatedServerMain.setConfigurationUnsafe(leader.config().self().raftAddress(),
					RaftPeerId.valueOf(leader.config().nodeId()), groupId(), arguments, TIMEOUT);
			if (reply.isSuccess()) {
				break;
			}
			// Leadership moved between the look-up and the call: asked again.
			assertNotNull(reply.getNotLeaderException(), reply::toString);
			assertTrue(System.nanoTime() - deadline < 0, "no leader took the change");
		}
		assertVoters(all);
	}

	private static SetConfigurationRequestProto request(SetConfigurationRequest.Mode mode, List<RaftPeer> servers,
			List<RaftPeer> currentServers) {
		var arguments = SetConfigurationRequest.Arguments.newBuilder().setServersInNewConf(servers).setMode(mode);
		if (currentServers != null) {
			arguments.setServersInCurrentConf(currentServers);
		}
		return ClientProtoUtils.toSetConfigurationRequestProto(new SetConfigurationRequest(ClientId.randomId(),
				RaftPeerId.valueOf("n0"), RaftGroupId.randomId(), 7, arguments.build()));
	}

	private static List<RaftPeer> peers(String... ids) {
		return Arrays.stream(ids).map(id -> RaftPeer.newBuilder().setId(id).setAddress("127.0.0.1:1").build()).toList();
	}

	@Test
	void whatTheGuardLetsThroughIsACompareAndSetAgainstTheConfigurationItChecked() {
		var current = peers("n0", "n1", "n2");
		var listener = peers("l0");

		var added = MembershipGuard.decide(request(SetConfigurationRequest.Mode.SET_UNCONDITIONALLY,
				peers("n0", "n1", "n2", "n3"), List.of()), current, listener, false);
		assertNull(added.refusal());
		assertEquals(SetConfigurationRequestProto.Mode.COMPARE_AND_SET, added.forward().getMode());
		assertEquals(List.of("n0", "n1", "n2"), protoIds(added.forward().getCurrentPeersList()));
		assertEquals(List.of("l0"), protoIds(added.forward().getCurrentListenersList()));
		assertEquals(List.of("n0", "n1", "n2", "n3"), protoIds(added.forward().getPeersList()));
		assertEquals(7, added.forward().getRpcRequest().getCallId(), "the call id, for Ratis' retry cache");

		var two = request(SetConfigurationRequest.Mode.SET_UNCONDITIONALLY, peers("n0", "n1", "n2", "n3", "n4"),
				List.of());
		var refused = MembershipGuard.decide(two, current, List.of(), false);
		assertNull(refused.forward());
		assertInstanceOf(SetConfigurationException.class, refused.refusal());
		assertTrue(refused.refusal().getMessage().startsWith(VoterChange.MULTI_VOTER_REFUSAL),
				refused.refusal().getMessage());
		var unsafe = MembershipGuard.decide(two, current, List.of(), true);
		assertNull(unsafe.refusal());
		assertEquals(SetConfigurationRequestProto.Mode.COMPARE_AND_SET, unsafe.forward().getMode());

		// ADD keeps the current members.
		var add = MembershipGuard.decide(request(SetConfigurationRequest.Mode.ADD, peers("n3"), List.of()), current,
				List.of(), false);
		assertNull(add.refusal());
		assertEquals(List.of("n0", "n1", "n2", "n3"), protoIds(add.forward().getPeersList()));
		assertEquals(SetConfigurationRequestProto.Mode.COMPARE_AND_SET, add.forward().getMode());
		var addTwo = MembershipGuard.decide(request(SetConfigurationRequest.Mode.ADD, peers("n3", "n4"), List.of()),
				current, List.of(), false);
		assertNotNull(addTwo.refusal());

		// A compare-and-set against another configuration is refused as Ratis refuses it, without the marker: the
		// caller reads the configuration again.
		var stale = MembershipGuard.decide(request(SetConfigurationRequest.Mode.COMPARE_AND_SET, peers("n0", "n1"),
				peers("n0", "n1", "n3")), current, List.of(), false);
		assertNull(stale.forward());
		assertInstanceOf(SetConfigurationException.class, stale.refusal());
		assertFalse(stale.refusal().getMessage().contains(VoterChange.MULTI_VOTER_REFUSAL), stale.refusal().getMessage());
	}

	/**
	 * Review of round 14: on a node that is not the leader the guard checks too, against that node's own configuration.
	 * Ratis handles the request only later (at half-close, through the division's executor), and the node may have won
	 * an election meanwhile. What it lets through is a compare-and-set against the configuration it checked, so a
	 * leader with another configuration refuses it; what it refuses goes back as {@link NotLeaderException}, and the
	 * client asks the leader, which checks again.
	 */
	@Test
	void onANonLeaderTheGuardChecksAgainstThatNodesConfiguration() {
		var current = peers("n0", "n1", "n2");
		var notLeader = new NotLeaderException(RaftGroupMemberId.valueOf(RaftPeerId.valueOf("n1"), RaftGroupId.randomId()),
				current.getFirst(), current);

		var two = request(SetConfigurationRequest.Mode.SET_UNCONDITIONALLY, peers("n0", "n1", "n2", "n3", "n4"),
				List.of());
		var refused = MembershipGuard.decide(two, current, List.of(), false, () -> notLeader);
		assertNull(refused.forward());
		assertInstanceOf(NotLeaderException.class, refused.refusal());

		var one = MembershipGuard.decide(request(SetConfigurationRequest.Mode.SET_UNCONDITIONALLY,
				peers("n0", "n1", "n2", "n3"), List.of()), current, List.of(), false, () -> notLeader);
		assertNull(one.refusal());
		assertEquals(SetConfigurationRequestProto.Mode.COMPARE_AND_SET, one.forward().getMode());
		assertEquals(List.of("n0", "n1", "n2"), protoIds(one.forward().getCurrentPeersList()));

		// A compare-and-set against another configuration than this node's: the leader decides.
		var stale = MembershipGuard.decide(request(SetConfigurationRequest.Mode.COMPARE_AND_SET, peers("n0", "n1"),
				peers("n0", "n1", "n3")), current, List.of(), false, () -> notLeader);
		assertInstanceOf(NotLeaderException.class, stale.refusal());

		var unsafe = MembershipGuard.decide(two, current, List.of(), true, () -> notLeader);
		assertNull(unsafe.refusal());
		assertEquals(SetConfigurationRequestProto.Mode.COMPARE_AND_SET, unsafe.forward().getMode());
	}
}
