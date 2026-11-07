package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.RemovalScenarios.TIMEOUT;
import static it.cavallium.rockserver.distributed.RemovalScenarios.awaitReadyLeader;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.grpc.RaftServerProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * R16-3: the paths of {@link RaftTraffic} that answer or fail a snapshot install stream, driven by a real Ratis install
 * in an in-process cluster rather than a fake call ({@link RaftTrafficTest}): a chunk below the follower's term is
 * answered NOT_LEADER and the leader steps down; a chunk whose configuration cannot be recorded fails the stream with
 * UNAVAILABLE, and the leader's appender installs again; in both cases the follower then installs and catches up. Also
 * the start floor of {@link InstalledConfiguration} when Ratis' {@code raft-meta} cannot be read ({@link Long#MAX_VALUE},
 * at a start that then fails, since Ratis cannot read it either) and its lowering at the next start that reads it.
 */
@Timeout(300)
class InstallStreamTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));

	/** Kept when a test fails, for the node logs. */
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		RaftTraffic.termForTesting = null;
		RaftTraffic.recordFailureForTesting = null;
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

	/** The leader, the follower that must install, and the index of the snapshot it must install. */
	private record Lagging(ReplicatedNode leader, String follower, long snapshotIndex) {}

	/**
	 * Three nodes; a follower is halted while the leader writes past what it keeps in its log and takes a snapshot.
	 * The follower is still halted when this returns: its next start needs an install.
	 */
	private Lagging lagging() throws Exception {
		System.setProperty(RaftSettings.PURGE_UP_TO_SNAPSHOT, "true");
		// Elections far apart: no leader change but the one a test causes.
		cluster = new InProcessCluster(root, 3, config -> config.withSnapshotEveryEntries(64)
				.withLogSegmentBytes(256 << 10).withElectionTimeouts(Duration.ofSeconds(3), Duration.ofSeconds(5)))
				.startAll();
		var leader = awaitReadyLeader(cluster);
		var l = leader.config().nodeId();
		var f = cluster.ids().stream().filter(id -> !id.equals(l)).findFirst().orElseThrow();
		long fApplied = cluster.node(f).appliedIndex();
		cluster.halt(f);
		var api = leader.connection().getSyncApi(CONTEXT);
		long column = api.createColumn("c", SCHEMA);
		for (int i = 0; leader.stateMachine().division().getRaftLog().getStartIndex() <= fApplied + 1; i++) {
			assertTrue(i < 30_000, "the log was never purged past the follower");
			api.put(0, column, key(i), Buf.wrap(("v" + i).getBytes(StandardCharsets.UTF_8)), RequestType.none());
		}
		var division = Objects.requireNonNull(leader.stateMachine().division());
		var snapshot = division.getRaftServer().snapshotManagementAsync(SnapshotManagementRequest.newCreate(
				ClientId.randomId(), division.getId(), division.getGroup().getGroupId(), 1, 30_000)).get(60, TimeUnit.SECONDS);
		assertTrue(snapshot.isSuccess(), () -> String.valueOf(snapshot.getException()));
		return new Lagging(leader, f, Objects.requireNonNull(leader.stateMachine().getLatestSnapshot()).getIndex());
	}

	/**
	 * The first chunk the follower gets is seen as below its term (the follower's term is made to look one higher): the
	 * follower answers NOT_LEADER with that term, without Ratis, and the leader steps down on the reply (its term rises
	 * past the one it led). A leader is elected again, which installs the snapshot on the follower.
	 */
	@Test
	void aChunkBelowTheFollowersTermStepsTheLeaderDownAndTheInstallEndsLater() throws Exception {
		var lagging = lagging();
		var leader = lagging.leader();
		long leaderTerm = leader.stateMachine().division().getInfo().getCurrentTerm();
		var answered = new AtomicInteger();
		RaftTraffic.termForTesting = (receiver, chunk) -> receiver.equals(lagging.follower())
				&& chunk.getLeaderTerm() == leaderTerm && answered.getAndIncrement() == 0
				? chunk.getLeaderTerm() + 1 : RaftTraffic.NO_TERM;
		var fNode = cluster.start(lagging.follower());
		InProcessCluster.await(TIMEOUT, () -> answered.get() > 0);
		InProcessCluster.await(TIMEOUT, () -> leader.stateMachine().division().getInfo().getCurrentTerm() > leaderTerm);
		RaftTraffic.termForTesting = null;
		InProcessCluster.await(TIMEOUT, () -> fNode.appliedIndex() >= lagging.snapshotIndex());
		assertTrue(fNode.stateMachine().storeGeneration() >= 2, "the follower installed no snapshot");
		cluster.awaitConverged(TIMEOUT);
		var newLeader = cluster.awaitLeader(TIMEOUT);
		assertTrue(newLeader.stateMachine().division().getInfo().getCurrentTerm() > leaderTerm);
	}

	/**
	 * The record of the first chunk with a configuration fails: the stream fails with UNAVAILABLE and Ratis is told of
	 * the end as a cancel; the leader's appender starts the install again after its wait, on the same term, and the
	 * follower installs and catches up. Nothing is left of the failed stream that stops the next one.
	 */
	@Test
	void aFailedRecordFailsTheStreamAndTheLeaderInstallsAgain() throws Exception {
		var lagging = lagging();
		var leader = lagging.leader();
		long leaderTerm = leader.stateMachine().division().getInfo().getCurrentTerm();
		var failed = new AtomicInteger();
		var later = new AtomicInteger();
		RaftTraffic.recordFailureForTesting = (receiver, chunk) -> {
			if (!receiver.equals(lagging.follower()) || !chunk.hasLastRaftConfigurationLogEntryProto()) {
				return false;
			}
			if (failed.get() == 0) {
				failed.incrementAndGet();
				return true;
			}
			later.incrementAndGet();
			return false;
		};
		var fNode = cluster.start(lagging.follower());
		InProcessCluster.await(TIMEOUT, () -> fNode.appliedIndex() >= lagging.snapshotIndex());
		assertEquals(1, failed.get());
		assertTrue(later.get() > 0, "no chunk came after the failed one");
		assertTrue(fNode.stateMachine().storeGeneration() >= 2, "the follower installed no snapshot");
		assertTrue(leader.isLeader(), "the leader changed");
		assertEquals(leaderTerm, leader.stateMachine().division().getInfo().getCurrentTerm(), "the term changed");
		cluster.awaitConverged(TIMEOUT);
		var record = InstalledConfiguration.read(fNode.config().raftDir());
		assertNotNull(record);
		assertTrue(!record.bounds().isEmpty(), () -> "the install recorded nothing: " + record);
	}

	/**
	 * R16 review, finding 2: how a running node wires the R16-2 check. After a real install, the follower's floor
	 * ({@link ReplicatedNode#installFloor}) for the leader's term is the index of its configuration (the leader's
	 * term-start entry or later), and for a later term its committed configuration; neither is unknown. A chunk the
	 * follower got from that install, replayed through its own {@link RaftTraffic} with a configuration one index older,
	 * fails its stream with FAILED_PRECONDITION and never reaches Ratis (a fake handler stands in for it here). Were the
	 * node's floor always unknown, the replayed chunk would pass. R17 review, finding 4: the refusal is counted in the
	 * node's own {@code install.staleChunks} series, the one {@code rockserver-install-stale-chunks} reads.
	 */
	@Test
	void theRunningNodesFloorFailsAReplayedChunkOfAnOlderConfiguration() throws Exception {
		var lagging = lagging();
		var leader = lagging.leader();
		long leaderTerm = leader.stateMachine().division().getInfo().getCurrentTerm();
		var captured = new AtomicReference<RaftProtos.InstallSnapshotRequestProto>();
		RaftTraffic.termForTesting = (receiver, chunk) -> {
			if (receiver.equals(lagging.follower()) && chunk.hasLastRaftConfigurationLogEntryProto()) {
				captured.compareAndSet(null, chunk);
			}
			return RaftTraffic.NO_TERM;
		};
		var fNode = cluster.start(lagging.follower());
		InProcessCluster.await(TIMEOUT, () -> fNode.appliedIndex() >= lagging.snapshotIndex());
		RaftTraffic.termForTesting = null;
		cluster.awaitConverged(TIMEOUT);
		assertTrue(leader.isLeader(), "the leader changed");
		var chunk = Objects.requireNonNull(captured.get(), "no chunk reached the follower");
		var group = RaftGroupId.valueOf(chunk.getServerRequest().getRaftGroupId().getId());

		long confIndex = fNode.stateMachine().division().getRaftConf().getLogEntryIndex();
		long floor = fNode.installFloor(group, leaderTerm);
		assertNotEquals(RaftTraffic.NO_INDEX, floor, "the running node knows no floor");
		assertEquals(confIndex, floor);
		assertTrue(chunk.getLastRaftConfigurationLogEntryProto().getIndex() <= floor);
		long committedFloor = fNode.installFloor(group, leaderTerm + 1);
		assertNotEquals(RaftTraffic.NO_INDEX, committedFloor, "the running node knows no committed configuration");
		assertTrue(committedFloor <= floor);
		assertEquals(RaftTraffic.NO_INDEX, fNode.installFloor(RaftGroupId.randomId(), leaderTerm), "another group");

		var older = chunk.toBuilder().setLastRaftConfigurationLogEntryProto(
				chunk.getLastRaftConfigurationLogEntryProto().toBuilder().setIndex(floor - 1)).build();
		var call = new RaftTrafficTest.FakeCall<RaftProtos.InstallSnapshotRequestProto,
				RaftProtos.InstallSnapshotReplyProto>(RaftServerProtocolServiceGrpc.getInstallSnapshotMethod());
		var handler = new RaftTrafficTest.FakeHandler<RaftProtos.InstallSnapshotRequestProto,
				RaftProtos.InstallSnapshotReplyProto>();
		// R17 review, finding 4: the node wires the series the alert reads, under its exported name.
		var staleChunks = fNode.metrics().amount("install.staleChunks");
		long staleBefore = staleChunks.count();
		var listener = fNode.raftTraffic().interceptCall(call, new Metadata(), handler);
		listener.onMessage(older);
		assertNotNull(call.closed, () -> "the replayed chunk's stream is not failed: " + call.events);
		assertEquals(Status.Code.FAILED_PRECONDITION, call.closed.getCode());
		assertTrue(handler.messages.isEmpty(), "the replayed chunk reached Ratis");
		assertEquals(staleBefore + 1, staleChunks.count(), "the node's install.staleChunks");
	}

	/**
	 * A follower's {@code raft-meta} is damaged while it is down. Its next start keeps a record it has, and then fails,
	 * since Ratis cannot read the file either. Without a record, the start writes one with the floor
	 * {@link Long#MAX_VALUE} (every configuration a chunk installs is bounded by no term), and fails the same way. Once the file is repaired, the next start lowers the floor to the term in it, and
	 * the node runs.
	 */
	@Test
	void anUnreadableRaftMetaGivesTheFloorMaxAndALaterStartLowersIt() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		var leader = awaitReadyLeader(cluster);
		var f = cluster.ids().stream().filter(id -> !id.equals(leader.config().nodeId())).findFirst().orElseThrow();
		var config = cluster.config(f);
		cluster.stop(f);
		var meta = config.raftDir().resolve(config.groupId()).resolve("current").resolve("raft-meta");
		var good = Files.readString(meta, StandardCharsets.UTF_8);
		var properties = new Properties();
		properties.load(new java.io.StringReader(good));
		long term = Long.parseLong(properties.getProperty("term").trim());
		Files.writeString(meta, "votedFor=" + properties.getProperty("votedFor") + "\n", StandardCharsets.UTF_8);

		var before = InstalledConfiguration.read(config.raftDir());
		assertNotNull(before);
		assertThrows(Exception.class, () -> cluster.start(f), "Ratis started without a term in raft-meta");
		// A record of this format is kept: its floor held for every chunk before, and only comes down at a start.
		assertEquals(before, InstalledConfiguration.read(config.raftDir()));

		// Without a record as well (lost or damaged with it), the floor is no term at all.
		Files.delete(config.raftDir().resolve(InstalledConfiguration.FILE));
		assertThrows(Exception.class, () -> cluster.start(f), "Ratis started without a term in raft-meta");
		var damaged = InstalledConfiguration.read(config.raftDir());
		assertNotNull(damaged);
		assertEquals(Long.MAX_VALUE, damaged.floor());

		Files.writeString(meta, good, StandardCharsets.UTF_8);
		var restarted = cluster.start(f);
		var lowered = InstalledConfiguration.read(config.raftDir());
		assertNotNull(lowered);
		assertEquals(term, lowered.floor());
		var observed = new AtomicLong();
		InProcessCluster.await(TIMEOUT, () -> {
			observed.set(restarted.stateMachine().division().getInfo().getCurrentTerm());
			return restarted.leaderId() != null;
		});
		assertTrue(observed.get() >= term);
		cluster.awaitConverged(TIMEOUT);
	}
}
