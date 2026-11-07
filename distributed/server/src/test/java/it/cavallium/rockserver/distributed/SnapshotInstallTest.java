package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.SnapshotManagementRequest;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * A snapshot install to an existing member that lags past the leader's log, in process: with partitioned apply on every
 * node (R2-PA-2: snapshots taken and installed by nodes that apply in lanes), and a follower read that arrives while the
 * install replaces the database (AR-2.1b, WRo-6: the {@code storeGeneration} path of {@link Router}).
 *
 * <p>The log is purged up to each snapshot ({@link RaftSettings#PURGE_UP_TO_SNAPSHOT}, a test setting), so that the
 * member that was down needs an install: the install itself is the production one.</p>
 */
@Timeout(300)
class SnapshotInstallTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(60);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
	private static final String PADDING = "x".repeat(1000);

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		var gate = RockserverStateMachine.installGateForTesting;
		if (gate != null) {
			gate.countDown();
		}
		RockserverStateMachine.installGateForTesting = null;
		RockserverStateMachine.installReachedForTesting = null;
		try {
			if (cluster != null) {
				cluster.close();
			}
		} finally {
			System.clearProperty(RaftSettings.PURGE_UP_TO_SNAPSHOT);
			System.clearProperty(Router.FOLLOWER_READS_PROPERTY);
		}
	}

	@Test
	void aLaggingMemberWithLanesInstallsASnapshotWhileItsFollowerReadsAreForwarded() throws Exception {
		System.setProperty(RaftSettings.PURGE_UP_TO_SNAPSHOT, "true");
		System.setProperty(Router.FOLLOWER_READS_PROPERTY, "true");
		cluster = new InProcessCluster(temp, 3, config -> config.withSnapshotEveryEntries(64)
				.withLogSegmentBytes(256 << 10).withParallelApplyThreads(3)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		long column = api(leader).createColumn("c", SCHEMA);
		var laggingId = cluster.anyFollower().config().nodeId();
		long laggingApplied = cluster.node(laggingId).appliedIndex();
		cluster.halt(laggingId);

		// Until the leader's log no longer holds what the lagging member needs: only an install can bring it back.
		int written = 0;
		while (leader.stateMachine().division().getRaftLog().getStartIndex() <= laggingApplied + 1) {
			assertTrue(written < 30_000, "the log was never purged past the lagging member");
			for (int i = 0; i < 500; i++, written++) {
				api(leader).put(0, column, key(written), text(written), RequestType.none());
			}
		}
		// A snapshot at the commit index, so that the installed database holds every write and a read index of a quiet
		// cluster is at most its index: the lagging member's apply reaches it as soon as the new database is open.
		var division = leader.stateMachine().division();
		var reply = division.getRaftServer().snapshotManagementAsync(SnapshotManagementRequest.newCreate(ClientId.randomId(),
				division.getId(), division.getGroup().getGroupId(), 1, 30_000)).get(60, TimeUnit.SECONDS);
		assertTrue(reply.isSuccess(), () -> String.valueOf(reply.getException()));
		long commit = division.getRaftLog().getLastCommittedIndex();
		var snapshot = leader.stateMachine().getLatestSnapshot();
		assertNotNull(snapshot);
		assertEquals(commit, snapshot.getIndex(), "nothing committed after the snapshot");

		var reached = new CountDownLatch(1);
		RockserverStateMachine.installReachedForTesting = reached;
		RockserverStateMachine.installGateForTesting = new CountDownLatch(1);
		var lagging = cluster.start(laggingId);
		assertTrue(reached.await(TIMEOUT.toSeconds(), TimeUnit.SECONDS), "the install reached the new database");
		// Held between the two bumps: the new database is open and applied up to the snapshot, but not published.
		assertEquals(1, lagging.stateMachine().storeGeneration() & 1, "the generation is odd during the install");
		assertTrue(lagging.appliedIndex() >= commit, "applied up to the snapshot: " + lagging.appliedIndex());
		var router = ((ReplicatedConnection) lagging.connection()).router;
		long served = router.followerReadsServedForTesting();
		long forwarded = router.followerReadsForwardedForTesting();
		int last = written - 1;
		assertEquals("v" + last, value(api(lagging).get(0, column, key(last), RequestType.current())));
		assertEquals(served, router.followerReadsServedForTesting(), "not served from a database being replaced");
		assertEquals(forwarded + 1, router.followerReadsForwardedForTesting(), "forwarded instead");

		RockserverStateMachine.installGateForTesting.countDown();
		RockserverStateMachine.installGateForTesting = null;
		cluster.awaitConverged(TIMEOUT);
		assertEquals(0, lagging.stateMachine().storeGeneration() & 1);
		assertTrue(lagging.stateMachine().storeGeneration() >= 2, "it installed a snapshot");
		// RaftTraffic recorded the configuration of the chunks with the leader's term (round-13 review, finding 1).
		var raftConf = division.getRaftConf();
		var installedConf = new RemovalRule.Conf(RemovalRule.UNKNOWN_TERM, raftConf.getLogEntryIndex(),
				raftConf.getCurrentPeers().stream().map(peer -> peer.getId().toString()).toList(),
				raftConf.getPreviousPeers().stream().map(peer -> peer.getId().toString()).toList());
		assertEquals(division.getInfo().getCurrentTerm(), InstalledConfiguration.boundOf(
				InstalledConfiguration.read(lagging.config().raftDir()), installedConf),
				"the configuration of the install is recorded with the leader's term");
		for (int i = 0; i < written; i += 97) {
			assertEquals("v" + i, localValue(lagging, column, i));
		}
		assertEquals("v" + last, localValue(lagging, column, last));
		// The installed replica applies new entries in its lanes, and serves its reads again.
		var installedApplier = lagging.stateMachine().applierForTesting();
		long segmentedBefore = installedApplier.segmentedEntries;
		for (int i = written; i < written + 200; i++) {
			api(leader).put(0, column, key(i), text(i), RequestType.none());
		}
		cluster.awaitConverged(TIMEOUT);
		assertTrue(lagging.stateMachine().applierForTesting() == installedApplier, "no other install");
		assertTrue(installedApplier.segmentedEntries > segmentedBefore,
				"the installed database's applier sorted entries into lanes: " + installedApplier.segmentedEntries);
		assertEquals("v" + (written + 199), localValue(lagging, column, written + 199));
		long servedAfter = router.followerReadsServedForTesting();
		for (int i = 0; i < 50 && router.followerReadsServedForTesting() == servedAfter; i++) {
			assertEquals("v" + i, value(api(lagging).get(0, column, key(i), RequestType.current())));
		}
		assertTrue(router.followerReadsServedForTesting() > servedAfter, "the follower serves reads again");
	}

	/**
	 * WRo-6 / AR-2.1b, the other half: a read that found the database published (an even generation), then failed
	 * because an install closed that database under it, is forwarded too; a read that failed with no install around
	 * it keeps its error. The race itself needs a second install on a running member (a partition, which only the
	 * process suites have), so the decision is checked here, where removing either half fails.
	 */
	@Test
	void aLocalReadFailureIsForwardedOnlyWhenAnInstallReplacedTheDatabase() {
		assertTrue(Router.replacedDuringRead(1, 1), "the install was under way: not even tried");
		assertTrue(Router.replacedDuringRead(1, 2));
		assertTrue(Router.replacedDuringRead(2, 3), "the install closed the database it was reading");
		assertTrue(Router.replacedDuringRead(2, 4), "and published the next one before the failure was seen");
		assertFalse(Router.replacedDuringRead(2, 2), "no install: the read's own error");
		assertFalse(Router.replacedDuringRead(0, 0));
	}

	/**
	 * CR2-6: a member rolled back below the cluster version after it answered the leader's poll, so that its own
	 * database is still older (here: it lacks CHECKSUM_SCHEMAS, and so does the version it runs), installs the leader's
	 * snapshot, whose version has the capability. The install is refused and the replica stops, with a
	 * NewerVersionException in its failure (exit 5), instead of applying entries it cannot read.
	 */
	@Test
	void anInstalledSnapshotOfANewerClusterVersionStopsTheReplica() throws Exception {
		System.setProperty(RaftSettings.PURGE_UP_TO_SNAPSHOT, "true");
		cluster = new InProcessCluster(temp, 3, config -> config.withSnapshotEveryEntries(64)
				.withLogSegmentBytes(256 << 10)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		long column = api(leader).createColumn("c", SCHEMA);
		var laggingId = cluster.anyFollower().config().nodeId();
		long laggingApplied = cluster.node(laggingId).appliedIndex();
		cluster.stop(laggingId);
		long older = Capabilities.SUPPORTED & ~Capabilities.CHECKSUM_SCHEMAS;
		var config = cluster.config(laggingId);
		try (var store = NodeStore.open(config.dbDir(), config.rockserverConfig(), "test")) {
			store.db().put(0L, store.metaColumnId(), NodeStore.key(NodeStore.CLUSTER_VERSION),
					Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(older).array()), RequestType.none());
		}
		ReplicaOps.SUPPORTED_FOR_TESTING.put(laggingId, older);
		try {
			int written = 0;
			while (leader.stateMachine().division().getRaftLog().getStartIndex() <= laggingApplied + 1) {
				assertTrue(written < 30_000, "the log was never purged past the lagging member");
				for (int i = 0; i < 500; i++, written++) {
					api(leader).put(0, column, key(written), text(written), RequestType.none());
				}
			}
			var lagging = cluster.start(laggingId);
			var failure = lagging.stopped().get(TIMEOUT.toSeconds(), TimeUnit.SECONDS);
			assertTrue(lagging.isFailed(), "the replica failed");
			assertTrue(ReplicatedNode.NewerVersionException.in(failure), String.valueOf(failure));
		} finally {
			ReplicaOps.SUPPORTED_FOR_TESTING.remove(laggingId);
		}
	}

	private static RocksDBSyncAPI api(ReplicatedNode node) {
		return node.connection().getSyncApi(CONTEXT);
	}

	private static String localValue(ReplicatedNode node, long column, int index) {
		return value(node.stateMachine().store().db().get(0, column, key(index), RequestType.current()));
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(int index) {
		return Buf.wrap(("v" + index + "|" + PADDING).getBytes(StandardCharsets.UTF_8));
	}

	private static String value(Buf buf) {
		if (buf == null) {
			return null;
		}
		var text = new String(buf.toByteArray(), StandardCharsets.UTF_8);
		int bar = text.indexOf('|');
		return bar < 0 ? text : text.substring(0, bar);
	}
}
