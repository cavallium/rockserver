package it.cavallium.rockserver.distributed;

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

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
