package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.TimeUnit;
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Leaders with skewed wall clocks ({@code RockserverStateMachine.TestHooks#clockSkewMillis}): the replicated clock
 * that session expiry reads never goes back, every replica holds the same one, and session writes keep applying
 * exactly once while leadership moves between nodes whose clocks disagree by hours.
 */
@Timeout(180)
class ClockSkewTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final long SKEW_MILLIS = TimeUnit.HOURS.toMillis(2);

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	/**
	 * A leader two hours behind takes over: the clock stays where the previous leader left it, and a session
	 * write still applies once. Then every clock is back on time, and the next session write moves the replicated
	 * clock past the wall clock, whichever node leads.
	 */
	@Test
	void aLeaderBehindNeverMovesTheReplicatedClockBack() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var leaderId = leader.config().nodeId();
		try (var connection = GrpcConnection.forHostsAndPorts("skew-client", addresses())) {
			assertTrue(connection.getCapabilities().clientSessions());
			var api = connection.getAsyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumnAsync("c", SCHEMA).join();
			assertEquals("a", merge(api, column, "a"));
			cluster.awaitConverged(TIMEOUT);
			long before = sameClockEverywhere();
			assertTrue(before > 0, "a session write stamped the clock");

			for (var node : cluster.runningNodes()) {
				if (node != leader) {
					node.stateMachine().testHooks().clockSkewMillis = -SKEW_MILLIS;
				}
			}
			cluster.stop(leaderId);
			cluster.awaitLeader(TIMEOUT);
			assertEquals("a,b", merge(api, column, "b"));
			cluster.awaitConverged(TIMEOUT);
			long frozen = sameClockEverywhere();
			// Every stamp of the new leader is older than the clock: it stays put, it never goes back.
			assertEquals(before, frozen, "the clock moved under a leader behind it");

			// The node with the right time comes back and every clock is right again: the clock moves forward. The
			// restarted node need not lead, so the check is against the wall clock: with no skew left, the leader
			// stamps "c" with a time at least the one read here, and `after >= frozen` alone would hold even if the
			// write were stamped two hours behind, or not stamped at all.
			cluster.start(leaderId);
			cluster.awaitConverged(TIMEOUT);
			for (var node : cluster.runningNodes()) {
				node.stateMachine().testHooks().clockSkewMillis = 0;
			}
			// LeaderClock reads the wall clock only at the start of a term and then moves with the monotonic clock, so a
			// leader whose skew ended within its term keeps stamping behind. A new term is needed: unless the restarted
			// node, never skewed, leads already, stop the skewed leader and bring it back once another one leads.
			var current = cluster.awaitLeader(TIMEOUT);
			if (current != cluster.node(leaderId)) {
				var currentId = current.config().nodeId();
				cluster.stop(currentId);
				cluster.awaitLeader(TIMEOUT);
				cluster.start(currentId);
				cluster.awaitConverged(TIMEOUT);
			}
			long wallBeforeMerge = System.currentTimeMillis();
			assertTrue(wallBeforeMerge > frozen, "the wall clock is behind the replicated clock");
			assertEquals("a,b,c", merge(api, column, "c"));
			cluster.awaitConverged(TIMEOUT);
			long after = sameClockEverywhere();
			assertTrue(after >= wallBeforeMerge,
					"the clock did not move forward to the wall clock " + wallBeforeMerge + ": " + frozen + " -> " + after);
			assertEquals("a,b,c", localValue(cluster.node(leaderId), column));
		}
	}

	/**
	 * A leader two hours ahead takes over: the clock jumps forward with it (by at most the skew), and stays there
	 * when a node on time leads again. A session write sent across both changes takes effect at most once, and the
	 * client sees either its result or an error, never a second application.
	 */
	@Test
	void aLeaderAheadMovesTheClockForwardOnlyAndWritesStayExactlyOnce() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var leaderId = leader.config().nodeId();
		try (var connection = GrpcConnection.forHostsAndPorts("skew-client", addresses())) {
			var api = connection.getAsyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumnAsync("c", SCHEMA).join();
			assertEquals("a", merge(api, column, "a"));
			cluster.awaitConverged(TIMEOUT);
			long before = sameClockEverywhere();

			for (var node : cluster.runningNodes()) {
				if (node != leader) {
					node.stateMachine().testHooks().clockSkewMillis = SKEW_MILLIS;
				}
			}
			cluster.stop(leaderId);
			cluster.awaitLeader(TIMEOUT);
			var expected = new StringBuilder("a");
			mergeAtMostOnce(api, column, "b", expected);
			cluster.awaitConverged(TIMEOUT);
			long ahead = sameClockEverywhere();
			assertTrue(ahead >= before, "the clock went back from " + before + " to " + ahead);
			assertTrue(ahead <= System.currentTimeMillis() + SKEW_MILLIS, "the clock passed the skewed leader's time");

			// Back to a leader on time: the clock does not follow it back.
			cluster.start(leaderId);
			cluster.awaitConverged(TIMEOUT);
			var skewed = cluster.awaitLeader(TIMEOUT);
			for (var node : cluster.runningNodes()) {
				node.stateMachine().testHooks().clockSkewMillis = 0;
			}
			cluster.stop(skewed.config().nodeId());
			cluster.awaitLeader(TIMEOUT);
			mergeAtMostOnce(api, column, "c", expected);
			cluster.awaitConverged(TIMEOUT);
			long after = sameClockEverywhere();
			assertTrue(after >= ahead, "the clock went back from " + ahead + " to " + after);
			for (var node : cluster.runningNodes()) {
				assertEquals(expected.toString(), localValue(node, column), "on " + node.config().nodeId());
			}
		}
	}

	/**
	 * Merges {@code value}: on success the result must be the expected list plus it, exactly once; on failure (a
	 * session that expired at the new clock may be refused) the value must not be there at all.
	 */
	private void mergeAtMostOnce(RocksDBAsyncAPI api, long column, String value,
			StringBuilder expected) throws Exception {
		String result = null;
		Throwable failure = null;
		for (int attempt = 0; attempt < 3 && result == null; attempt++) {
			try {
				result = merge(api, column, value);
			} catch (RuntimeException e) {
				failure = e;
				System.out.println("Merge of " + value + " failed: " + e);
				cluster.awaitConverged(TIMEOUT);
				var current = localValue(cluster.awaitLeader(TIMEOUT), column);
				if (current != null && current.equals(expected + "," + value)) {
					// Applied although the answer was an error: count it, do not send it again.
					expected.append(',').append(value);
					return;
				}
				assertEquals(expected.toString(), current, "a failed merge left a trace");
			}
		}
		assertTrue(result != null, () -> "merge of " + value + " never succeeded");
		expected.append(',').append(value);
		assertEquals(expected.toString(), result, failure == null ? "" : "after " + failure);
	}

	private static String merge(RocksDBAsyncAPI api, long column, String value) {
		return api.mergeAsync(0, column, key(), text(value), RequestType.merged()).join()
				.toString(StandardCharsets.UTF_8);
	}

	/** The persisted replicated clock, the same on every running node. */
	private long sameClockEverywhere() {
		Long clock = null;
		for (var node : cluster.runningNodes()) {
			var store = node.stateMachine().store();
			Buf value = store.db().get(0, store.metaColumnId(), NodeStore.key(NodeStore.CLOCK), RequestType.current());
			long nodeClock = value == null ? 0 : ByteBuffer.wrap(value.toByteArray()).getLong();
			if (clock == null) {
				clock = nodeClock;
			} else {
				assertEquals(clock, nodeClock, "clock of node " + node.config().nodeId());
			}
		}
		return clock;
	}

	private ArrayList<HostAndPort> addresses() {
		var addresses = new ArrayList<HostAndPort>();
		for (var node : cluster.runningNodes()) {
			var self = node.config().self();
			addresses.add(new HostAndPort(self.apiHost(), self.apiPort()));
		}
		return addresses;
	}

	private static @Nullable String localValue(ReplicatedNode node, long column) {
		Buf value = node.stateMachine().store().db().get(0, column, key(), RequestType.current());
		return value == null ? null : value.toString(StandardCharsets.UTF_8);
	}

	private static Keys key() {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(1).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}
}
