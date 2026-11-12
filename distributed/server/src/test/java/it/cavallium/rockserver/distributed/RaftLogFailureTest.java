package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.server.raftlog.segmented.LogWorkerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * R62-2: a node whose Raft log failed stops as a failed node, leader or follower. The log of one node of three gets a
 * failed write ({@link LogWorkerGate#fail}: the next task of its writer throws an {@code IOException}, as a write or an
 * fsync does on a full or failing disk), under writes through another node. Ratis 3.3.1 then writes nothing more to
 * that log and leaves the division alive: before the fix a follower stayed up for good, failing every request with
 * entries, and a leader stayed up with a closed log while the Raft watch logged "failed to check Raft, retrying".
 * Expected: the node stops within {@link #STOP_SECONDS} s with a failure that names its Raft log (exit code 3 in a
 * process), and the two others go on acknowledging writes.
 */
@Timeout(180)
@Tag("slow")
class RaftLogFailureTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
	private static final int STOP_SECONDS = 30;

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	@ParameterizedTest(name = "the {0}")
	@ValueSource(strings = {"leader", "follower"})
	void aNodeWhoseRaftLogFailedStops(String role) throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(Duration.ofSeconds(30));
		var followers = cluster.runningNodes().stream().filter(node -> node != leader).toList();
		var victim = role.equals("leader") ? leader : followers.getFirst();
		// Not the victim: its API goes away with it.
		var through = followers.getLast();
		long column = through.connection().getSyncApi(CONTEXT).createColumn("c", SCHEMA);
		var api = through.connection().getAsyncApi(CONTEXT);
		api.putAsync(0, column, key(0), Buf.wrap(new byte[] {0}), RequestType.none()).get(20, TimeUnit.SECONDS);
		cluster.awaitConverged(Duration.ofSeconds(30));

		LogWorkerGate.fail(victim.stateMachine().division().getRaftLog(),
				new IOException("No space left on device (test)"));
		long failedAt = System.nanoTime();
		long acknowledged = 0;
		long key = 1;
		// Entries for the failed log to refuse; a write may fail while a leader's log is the failed one.
		while (!victim.stopped().isDone() && System.nanoTime() - failedAt < TimeUnit.SECONDS.toNanos(STOP_SECONDS)) {
			try {
				api.putAsync(0, column, key(key), Buf.wrap(new byte[] {1}), RequestType.none()).get(5, TimeUnit.SECONDS);
				acknowledged = key;
			} catch (Exception notWritten) {
				// Goes on.
			}
			key++;
			Thread.sleep(50);
		}
		assertTrue(victim.stopped().isDone(), "node " + victim.config().nodeId() + " (" + role
				+ ") is still running " + STOP_SECONDS + " s after a write to its Raft log failed");
		var failure = victim.stopped().join();
		assertNotNull(failure, "the node stopped, but not as a failed node");
		assertTrue(failure.getMessage().contains("The Raft log of node " + victim.config().nodeId() + " failed"),
				failure.toString());
		assertEquals(3, ReplicatedServerMain.failedExitStatus(victim));
		System.out.println("Node " + victim.config().nodeId() + " (" + role + ") stopped "
				+ Duration.ofNanos(System.nanoTime() - failedAt).toMillis() + " ms after its log failed: "
				+ failure.getMessage());

		// The two others serve on.
		cluster.halt(victim.config().nodeId());
		cluster.awaitLeader(Duration.ofSeconds(30));
		api.putAsync(0, column, key(key), Buf.wrap(new byte[] {2}), RequestType.none()).get(20, TimeUnit.SECONDS);
		var read = through.connection().getSyncApi(CONTEXT);
		assertArrayEquals(new byte[] {2}, read.get(0, column, key(key), RequestType.current()).toByteArray());
		if (acknowledged > 0) {
			assertArrayEquals(new byte[] {1}, read.get(0, column, key(acknowledged), RequestType.current()).toByteArray());
		}
	}

	/**
	 * Review of R62, 1: a follower whose log failed is never counted for an entry that its disk does not hold. The
	 * question was whether the entries of the request whose write failed, which stay in the follower's log in memory
	 * (its answers name a next index past them), are answered as stored when the leader sends them again. In Ratis
	 * 3.3.1 they are not: a follower appends the entries of its requests one request after the other, each chained to
	 * the one before ({@code ServerImplUtils.NavigableIndices.append}: {@code thenComposeAsync} on the previous
	 * request's future), so after one failed write every later request that carries entries fails without reaching
	 * the log, a resend of the same entries included. The leader's match index for that follower therefore stays at
	 * what the follower flushed, for as long as it runs ({@link LogWriterWatch} then stops it).
	 *
	 * <p>Two loads: entries on their way when the log fails, then silence (the leader sends the same entries again and
	 * again), and a steady trickle of new entries. A Ratis that answers such a resend as stored fails here, and
	 * then a node with a failed log counts in majorities for entries it does not hold until it stops: see "Long pauses"
	 * in design.md.</p>
	 */
	@ParameterizedTest(name = "{0}")
	@ValueSource(strings = {"a burst then silence", "a trickle"})
	void aFollowerWhoseLogFailedIsNeverCountedForAnEntryItDidNotStore(String load) throws Exception {
		boolean burst = load.startsWith("a burst");
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(Duration.ofSeconds(30));
		var followers = cluster.runningNodes().stream().filter(node -> node != leader).toList();
		var victim = followers.getFirst();
		var through = followers.getLast();
		long column = through.connection().getSyncApi(CONTEXT).createColumn("c", SCHEMA);
		var api = through.connection().getAsyncApi(CONTEXT);
		api.putAsync(0, column, key(0), Buf.wrap(new byte[] {0}), RequestType.none()).get(20, TimeUnit.SECONDS);
		cluster.awaitConverged(Duration.ofSeconds(30));

		var victimLog = victim.stateMachine().division().getRaftLog();
		var leaderDivision = leader.stateMachine().division();
		var leaderInfo = leaderDivision.getInfo();
		long term = leaderInfo.getCurrentTerm();
		var pending = new ArrayList<CompletableFuture<Void>>();
		long key = 1;
		if (burst) {
			for (; key <= 300; key++) {
				pending.add(api.putAsync(0, column, key(key), Buf.wrap(new byte[] {1}), RequestType.none()));
				if (key == 150) {
					LogWorkerGate.fail(victimLog, new IOException("No space left on device (test)"));
				}
			}
		} else {
			LogWorkerGate.fail(victimLog, new IOException("No space left on device (test)"));
		}
		long failedAt = System.nanoTime();
		long samples = 0;
		long notStored = 0;
		while (!victim.stopped().isDone() && System.nanoTime() - failedAt < TimeUnit.SECONDS.toNanos(STOP_SECONDS)) {
			if (!burst) {
				pending.add(api.putAsync(0, column, key(key++), Buf.wrap(new byte[] {1}), RequestType.none()));
			}
			for (int i = 0; i < 10 && !victim.stopped().isDone(); i++) {
				// The flush index after the match indices: it only grows, so a match index above it was above it.
				long counted = Long.MAX_VALUE;
				for (long match : leaderInfo.getFollowerMatchIndices()) {
					// The other follower holds everything: the smaller of the two is this follower's, or beyond it.
					counted = Math.min(counted, match);
				}
				long last = leaderDivision.getRaftLog().getNextIndex() - 1;
				long flushed;
				try {
					flushed = victimLog.getFlushIndex();
				} catch (RuntimeException closed) {
					break;
				}
				if (counted != Long.MAX_VALUE) {
					samples++;
					notStored = Math.max(notStored, last - flushed);
					assertTrue(counted <= flushed, "the leader counts the follower whose log failed up to entry " + counted
							+ ", and that follower's disk holds its log up to " + flushed + " only, "
							+ Duration.ofNanos(System.nanoTime() - failedAt).toMillis() + " ms after the failure");
				}
				Thread.sleep(5);
			}
		}
		assertTrue(victim.stopped().isDone(), "the follower is still running");
		assertEquals(term, leaderInfo.getCurrentTerm(), "the leader changed: the match indices read were not one leader's");
		System.out.println(load + ": " + samples + " samples in "
				+ Duration.ofNanos(System.nanoTime() - failedAt).toMillis() + " ms until the follower stopped, the leader's"
				+ " log up to " + notStored + " entries past that follower's flush index, never counted for one of them");
		assertTrue(samples > 100, "only " + samples + " samples");
		assertTrue(notStored > 0, "the leader's log never passed the follower's flush index: nothing was at stake");
		int acknowledged = 0;
		for (var write : pending) {
			try {
				write.get(20, TimeUnit.SECONDS);
				acknowledged++;
			} catch (Exception notWritten) {
				// Not this test's subject.
			}
		}
		// The two others are a majority: the writes went on.
		assertTrue(acknowledged > 0, "no write was acknowledged");
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
