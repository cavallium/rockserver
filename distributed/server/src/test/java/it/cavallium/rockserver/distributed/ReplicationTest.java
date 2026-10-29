package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

/** Replication through real Ratis servers in one JVM: writes, reads, transactions, restarts, catch-up. */
class ReplicationTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String text(Buf value) {
		return value == null ? null : new String(value.toByteArray(), StandardCharsets.UTF_8);
	}

	private static RocksDBSyncAPI api(ReplicatedNode node) {
		return node.connection().getSyncApi(CONTEXT);
	}

	/** Reads a key from a node's own database, bypassing routing, to compare replicas. */
	private static String localValue(ReplicatedNode node, long columnId, long key) {
		return text(node.stateMachine().store().db().get(0, columnId, key(key), RequestType.current()));
	}

	@Test
	void writesThroughAnyNodeReachEveryReplica() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		long column = api(follower).createColumn("users", SCHEMA);
		for (ReplicatedNode node : cluster.runningNodes()) {
			api(node).put(0, column, key(node.config().nodeId().hashCode()), text("from " + node.config().nodeId()),
					RequestType.none());
		}
		assertEquals("from n1", text(api(follower).get(0, column, key("n1".hashCode()), RequestType.current())));
		cluster.awaitConverged(TIMEOUT);
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals(column, node.stateMachine().store().db().getColumnId("users"), "column id differs");
			for (String id : cluster.ids()) {
				assertEquals("from " + id, localValue(node, column, id.hashCode()));
			}
		}
		// The internal metadata column stays hidden.
		assertEquals(List.of("users"), List.copyOf(api(follower).getAllColumnDefinitions().keySet()));
	}

	@Test
	void writeResultsAreComputedByApply() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		assertNull(api.put(0, column, key(1), text("a"), RequestType.previous()));
		assertEquals("a", text(api.put(0, column, key(1), text("b"), RequestType.previous())));
		assertTrue(api.put(0, column, key(1), text("c"), RequestType.changed()));
		assertFalse(api.put(0, column, key(1), text("c"), RequestType.changed()));
		assertEquals("c", text(api.delete(0, column, key(1), RequestType.previous())));
		assertEquals("x", text(api.merge(0, column, key(2), text("x"), RequestType.merged())));
		assertEquals("x,y", text(api.merge(0, column, key(2), text("y"), RequestType.merged())));
	}

	@Test
	void writesContinueAfterLeaderLossAndTheOldLeaderCatchesUp() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		long column = api(leader).createColumn("c", SCHEMA);
		for (int i = 0; i < 50; i++) {
			api(leader).merge(0, column, key(1), text(Integer.toString(i)), RequestType.none());
		}
		String oldLeader = leader.config().nodeId();
		cluster.halt(oldLeader);
		var survivor = cluster.runningNodes().getFirst();
		for (int i = 50; i < 100; i++) {
			api(survivor).merge(0, column, key(1), text(Integer.toString(i)), RequestType.none());
		}
		var expected = String.join(",", java.util.stream.IntStream.range(0, 100).mapToObj(Integer::toString).toList());
		assertEquals(expected, text(api(survivor).get(0, column, key(1), RequestType.current())));

		cluster.start(oldLeader);
		cluster.awaitConverged(TIMEOUT);
		// Each merge was applied exactly once on every replica, including across the restart.
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals(expected, localValue(node, column, 1));
		}
	}

	/**
	 * A write that gives up because no majority answered for its whole request timeout must not break later
	 * writes through the same node (found by {@link RandomFaultTest}: after an isolation, every write proposed by
	 * the formerly isolated node kept failing until it was restarted).
	 */
	@Test
	void writesThroughANodeRecoverAfterAProposalTimedOut() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withRequestTimeout(Duration.ofSeconds(1))).startAll();
		cluster.awaitLeader(TIMEOUT);
		var node = cluster.anyFollower();
		var api = api(node);
		long column = api.createColumn("c", SCHEMA);
		var others = cluster.ids().stream().filter(id -> !id.equals(node.config().nodeId())).toList();
		for (String id : others) {
			cluster.halt(id);
		}
		assertThrows(RocksDBException.class, () -> api.put(0, column, key(1), text("lost"), RequestType.none()));

		for (String id : others) {
			cluster.start(id);
		}
		cluster.awaitLeader(TIMEOUT);
		api.put(0, column, key(2), text("after"), RequestType.none());
		assertEquals("after", text(api.get(0, column, key(2), RequestType.current())));
	}

	@Test
	void laggingFollowerCatchesUpFromASnapshot() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withSnapshotEveryEntries(64)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		long column = api(leader).createColumn("c", SCHEMA);
		var lagging = cluster.anyFollower().config().nodeId();
		cluster.halt(lagging);
		var api = api(cluster.awaitLeader(TIMEOUT));
		for (int i = 0; i < 3000; i++) {
			api.put(0, column, key(i), text("v" + i), RequestType.none());
		}
		cluster.start(lagging);
		cluster.awaitConverged(Duration.ofSeconds(60));
		var node = cluster.node(lagging);
		for (int i = 0; i < 3000; i += 97) {
			assertEquals("v" + i, localValue(node, column, i));
		}
	}

	/**
	 * A follower whose missing entries are gone from the leader's log can only catch up by installing a snapshot
	 * (found by {@link RandomFaultTest}: the install stopped the follower's Raft server for good, because Ratis
	 * reloads a state machine only once it is paused).
	 */
	@Test
	void followerBehindThePurgedLogInstallsASnapshot() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withSnapshotEveryEntries(64)).startAll();
		long column = api(cluster.awaitLeader(TIMEOUT)).createColumn("c", SCHEMA);
		var lagging = cluster.anyFollower();
		var laggingId = lagging.config().nodeId();
		long laggingApplied = lagging.appliedIndex();
		cluster.halt(laggingId);
		int written = 0;
		// A new term starts a new log segment, and only closed segments are purged: change leaders a few times.
		for (int term = 0; term < 3; term++) {
			var leader = cluster.awaitLeader(TIMEOUT);
			for (int i = 0; i < 600; i++, written++) {
				api(leader).put(0, column, key(written), text("v" + written), RequestType.none());
			}
			var leaderId = leader.config().nodeId();
			cluster.stop(leaderId);
			cluster.start(leaderId);
		}
		var leader = cluster.awaitLeader(TIMEOUT);
		api(leader).put(0, column, key(written), text("v" + written), RequestType.none());
		written++;
		long logStart = leader.stateMachine().division().getRaftLog().getStartIndex();
		assertTrue(logStart > laggingApplied + 1, "log not purged past the lagging follower: starts at " + logStart
				+ ", follower applied " + laggingApplied);

		cluster.start(laggingId);
		cluster.awaitConverged(Duration.ofSeconds(60));
		var node = cluster.node(laggingId);
		assertFalse(node.stateMachine().isFailed());
		for (int i = 0; i < written; i += 97) {
			assertEquals("v" + i, localValue(node, column, i));
		}
		assertEquals("v" + (written - 1), localValue(node, column, written - 1));
		// The installed replica keeps applying new entries.
		api(cluster.awaitLeader(TIMEOUT)).put(0, column, key(written), text("after"), RequestType.none());
		cluster.awaitConverged(TIMEOUT);
		assertEquals("after", localValue(node, column, written));
	}

	@Test
	void batchWritesAreReplicated() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		var batches = new ArrayList<KVBatch>();
		for (int b = 0; b < 10; b++) {
			var keys = new ArrayList<Keys>();
			var values = new ArrayList<Buf>();
			for (int i = 0; i < 100; i++) {
				keys.add(key(b * 100L + i));
				values.add(text("b" + b + "i" + i));
			}
			batches.add(new KVBatch.KVBatchRef(keys, values));
		}
		api.putBatch(column, Flux.fromIterable(batches), PutBatchMode.WRITE_BATCH);
		cluster.awaitConverged(TIMEOUT);
		for (ReplicatedNode node : cluster.runningNodes()) {
			assertEquals("b9i99", localValue(node, column, 999));
		}
	}

	@Test
	void transactionsCommitAtomicallyAndDetectConflicts() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		api.put(0, column, key(1), text("initial"), RequestType.none());

		long tx = api.openTransaction(Duration.ofSeconds(30));
		var update = api.get(tx, column, key(1), RequestType.forUpdate());
		assertEquals("initial", text(update.previous()));
		api.put(tx, column, key(1), text("tx"), RequestType.none());
		api.put(tx, column, key(2), text("tx2"), RequestType.none());
		// Not visible outside the transaction before commit.
		assertNull(api.get(0, column, key(2), RequestType.current()));
		assertTrue(api.closeTransaction(tx, true));
		assertEquals("tx", text(api.get(0, column, key(1), RequestType.current())));
		assertEquals("tx2", text(api.get(0, column, key(2), RequestType.current())));

		long conflicting = api.openTransaction(Duration.ofSeconds(30));
		api.get(conflicting, column, key(1), RequestType.forUpdate());
		api.put(0, column, key(1), text("outside"), RequestType.none());
		api.put(conflicting, column, key(1), text("lost update"), RequestType.none());
		assertFalse(api.closeTransaction(conflicting, true), "a concurrent write must make the commit fail");
		assertTrue(api.closeTransaction(conflicting, false));
		assertEquals("outside", text(api.get(0, column, key(1), RequestType.current())));
	}

	@Test
	void updatesDetectConflicts() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		var first = api.get(0, column, key(1), RequestType.forUpdate());
		api.put(first.updateId(), column, key(1), text("one"), RequestType.none());
		assertEquals("one", text(api.get(0, column, key(1), RequestType.current())));

		var stale = api.get(0, column, key(1), RequestType.forUpdate());
		api.put(0, column, key(1), text("two"), RequestType.none());
		assertThrows(RocksDBRetryException.class,
				() -> api.put(stale.updateId(), column, key(1), text("lost"), RequestType.none()));
		assertEquals("two", text(api.get(0, column, key(1), RequestType.current())));
	}

	@Test
	void transactionsDoNotSurviveALeaderChange() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		var api = api(follower);
		long column = api.createColumn("c", SCHEMA);
		long tx = api.openTransaction(Duration.ofSeconds(30));
		api.put(tx, column, key(1), text("tx"), RequestType.none());
		cluster.stop(leader.config().nodeId());
		cluster.awaitLeader(TIMEOUT);
		assertThrows(RocksDBRetryException.class, () -> api.closeTransaction(tx, true));
		assertNull(api.get(0, column, key(1), RequestType.current()));
	}

	@Test
	void restartOfTheWholeClusterKeepsEveryAcknowledgedWrite() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var api = api(cluster.anyFollower());
		long column = api.createColumn("c", SCHEMA);
		for (int i = 0; i < 200; i++) {
			api.merge(0, column, key(i % 10), text("m" + i), RequestType.none());
		}
		for (String id : cluster.ids()) {
			cluster.halt(id);
		}
		cluster.startAll();
		cluster.awaitLeader(TIMEOUT);
		var after = api(cluster.anyFollower());
		for (int k = 0; k < 10; k++) {
			var expected = new ArrayList<String>();
			for (int i = k; i < 200; i += 10) {
				expected.add("m" + i);
			}
			assertEquals(String.join(",", expected), text(after.get(0, column, key(k), RequestType.current())));
		}
	}
}
