package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.RemovalScenarios.TIMEOUT;
import static it.cavallium.rockserver.distributed.RemovalScenarios.awaitReadyLeader;
import static it.cavallium.rockserver.distributed.RemovalScenarios.confEntry;
import static it.cavallium.rockserver.distributed.RemovalScenarios.isAppend;
import static it.cavallium.rockserver.distributed.RemovalScenarios.sender;
import static it.cavallium.rockserver.distributed.RemovalScenarios.setConfiguration;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.distributed.RemovalRule.Probe;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * R11: the committed configurations a node applied ({@link CommittedConfigurations}): recorded with their terms, kept
 * across a restart, carried by a snapshot, and answered for purged entries. A snapshot install, whose Ratis
 * notification reports the leader's latest configuration whether committed or not, records nothing.
 */
@Timeout(300)
class CommittedConfigurationsTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));

	/** Kept when a test fails, for the node logs. */
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		RaftTraffic.dropForTesting = null;
		try {
			if (cluster != null) {
				cluster.close();
			}
		} finally {
			System.clearProperty(RaftSettings.PURGE_UP_TO_SNAPSHOT);
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	@Test
	void appliedConfigurationsAreRecordedWithTheirTermsAndSurviveARestart() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		var leader = awaitReadyLeader(cluster);
		var follower = cluster.anyFollower();
		var id = follower.config().nodeId();
		var start = confEntry(leader);
		cluster.awaitConverged(TIMEOUT);
		var history = follower.stateMachine().configurations();
		var recorded = Objects.requireNonNull(history.at(start.getIndex()), "the term-start configuration");
		assertEquals(start.getTerm(), recorded.term());
		assertEquals(3, recorded.newHalf().size());
		assertEquals(0, history.completeFrom(), "a database that applied every entry");

		cluster.halt(id);
		var restarted = cluster.start(id);
		var reloaded = restarted.stateMachine().configurations().at(start.getIndex());
		assertEquals(recorded, reloaded, "read back from the database");
		var division = Objects.requireNonNull(restarted.stateMachine().division());
		assertEquals(Probe.COMMITTED, MembershipFacts.probe(division, restarted.stateMachine().configurations(),
				start.getTerm(), start.getIndex()));
		assertEquals(Probe.ABSENT, MembershipFacts.probe(division, restarted.stateMachine().configurations(),
				start.getTerm() + 1, start.getIndex()));
		assertEquals(Probe.ABSENT, MembershipFacts.probe(division, restarted.stateMachine().configurations(),
				start.getTerm(), 1_000_000));
	}

	/**
	 * Five nodes; F is down while the others write past what the leader keeps in its log. Then the leader appends a
	 * joint configuration J that cannot commit (its appends reach F only) and sends F a snapshot: Ratis notifies F of J
	 * on that install, but F records nothing past the snapshot, and its committed configuration is never J. The others
	 * then elect a leader that drops J.
	 */
	@Test
	void aSnapshotInstallRecordsTheLeadersHistoryAndNotItsUncommittedConfiguration() throws Exception {
		System.setProperty(RaftSettings.PURGE_UP_TO_SNAPSHOT, "true");
		cluster = new InProcessCluster(root, 5, config -> config.withSnapshotEveryEntries(64)
				.withLogSegmentBytes(256 << 10).withElectionTimeouts(Duration.ofSeconds(8), Duration.ofSeconds(12)))
				.startAll();
		var leader = awaitReadyLeader(cluster);
		var l = leader.config().nodeId();
		var followers = cluster.ids().stream().filter(id -> !id.equals(l)).toList();
		var f = followers.get(0);
		var removed = followers.get(1);
		long fApplied = cluster.node(f).appliedIndex();
		cluster.halt(f);
		var api = leader.connection().getSyncApi(CONTEXT);
		long column = api.createColumn("c", SCHEMA);
		for (int i = 0; leader.stateMachine().division().getRaftLog().getStartIndex() <= fApplied + 1; i++) {
			assertTrue(i < 30_000, "the log was never purged past F");
			api.put(0, column, key(i), Buf.wrap(("v" + i).getBytes(StandardCharsets.UTF_8)), RequestType.none());
		}
		var division = Objects.requireNonNull(leader.stateMachine().division());
		var snapshot = division.getRaftServer().snapshotManagementAsync(SnapshotManagementRequest.newCreate(
				ClientId.randomId(), division.getId(), division.getGroup().getGroupId(), 1, 30_000)).get(60, TimeUnit.SECONDS);
		assertTrue(snapshot.isSuccess(), () -> String.valueOf(snapshot.getException()));
		long snapshotIndex = Objects.requireNonNull(leader.stateMachine().getLatestSnapshot()).getIndex();

		RaftTraffic.dropForTesting = (receiver, message) -> isAppend(message) && l.equals(sender(message))
				&& !receiver.equals(f);
		var newHalf = cluster.ids().stream().filter(id -> !id.equals(removed)).toList();
		var reply = setConfiguration(cluster, leader, newHalf);
		InProcessCluster.await(TIMEOUT, () -> !RemovalScenarios.conf(leader).getPreviousPeers().isEmpty());
		long jointIndex = RemovalScenarios.conf(leader).getLogEntryIndex();
		var fNode = cluster.start(f);
		InProcessCluster.await(TIMEOUT, () -> fNode.appliedIndex() >= snapshotIndex);
		// The scenario: L, still leading with J uncommitted, sent the snapshot.
		assertTrue(leader.isLeader() && l.equals(fNode.leaderId()), "L lost its leadership before the install");
		assertTrue(fNode.stateMachine().storeGeneration() >= 2, "F installed a snapshot");
		var fHistory = fNode.stateMachine().configurations();
		assertNotNull(fHistory.at(0), "the leader's history up to its snapshot came with it");
		assertEquals(0, fHistory.completeFrom());
		assertTrue(fHistory.all().stream().allMatch(conf -> conf.inNew(removed)), () -> "J recorded: " + fHistory.all());
		var fDivision = Objects.requireNonNull(fNode.stateMachine().division());
		var committed = Objects.requireNonNull(MembershipFacts.committed(fDivision, fHistory));
		assertNotEquals(jointIndex, committed.index(), "J is not committed");
		assertFalse(reply.isDone() && reply.join().isSuccess(), "J must not commit");

		cluster.halt(l);
		RaftTraffic.dropForTesting = null;
		InProcessCluster.await(TIMEOUT, () -> cluster.runningNodes().stream().anyMatch(ReplicatedNode::isLeader));
		for (var node : cluster.runningNodes()) {
			var nodeDivision = Objects.requireNonNull(node.stateMachine().division());
			var nodeCommitted = MembershipFacts.committed(nodeDivision, node.stateMachine().configurations());
			assertTrue(nodeCommitted == null || nodeCommitted.inNew(removed), () -> node.config().nodeId()
					+ " reports a committed configuration without " + removed + ": " + nodeCommitted);
		}
	}
}
