package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * R62-3: a leader goes on accepting writes after more conflicting transactions than Ratis' pending-request limit
 * holds. Every transaction whose commit conflicts is refused in {@code preAppendTransaction}, and Ratis 3.3.1 kept the
 * permit of each such request until the leader stopped leading: after {@link RaftSettings#PENDING_REQUESTS_LIMIT}
 * conflicts the leader refused every write ("Failed to acquire a pending write request"), which the proposers resent
 * until their callers' deadlines. That is what stopped {@code RandomFaultTest} and {@code PipelinedRandomFaultTest},
 * whose workloads conflict thousands of times in a run.
 */
@Timeout(600)
class RefusedPermitsTest {

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));
	private static final int CONFLICTS = RaftSettings.PENDING_REQUESTS_LIMIT + 200;

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void aLeaderAcceptsWritesAfterMoreConflictsThanItsPendingRequestLimit() throws Exception {
		cluster = new InProcessCluster(temp, 1, c -> c).startAll();
		var leader = cluster.awaitLeader(Duration.ofSeconds(30));
		var api = leader.connection().getSyncApi(CONTEXT);
		long column = api.createColumn("c", SCHEMA);
		api.put(0, column, key(1), value(0), RequestType.none());
		long term = leader.stateMachine().division().getInfo().getCurrentTerm();
		long start = System.nanoTime();
		for (int i = 1; i <= CONFLICTS; i++) {
			long transaction = api.openTransaction(Duration.ofSeconds(30));
			api.get(transaction, column, key(1), RequestType.forUpdate());
			try {
				api.put(0, column, key(1), value(i), RequestType.none());
			} catch (RuntimeException refused) {
				throw new AssertionError("the write after " + (i - 1) + " conflicts was not accepted", refused);
			}
			api.put(transaction, column, key(1), value(-i), RequestType.none());
			assertFalse(api.closeTransaction(transaction, true), "the commit over a concurrent write succeeded");
			assertTrue(api.closeTransaction(transaction, false));
		}
		System.out.println(CONFLICTS + " conflicts in " + Duration.ofNanos(System.nanoTime() - start).toMillis() + " ms");
		assertEquals(term, leader.stateMachine().division().getInfo().getCurrentTerm(),
				"the leader changed: a new term has new limits, and the test shows nothing");
		assertEquals(CONFLICTS, leader.stateMachine().refusedPermits().released());
		api.put(0, column, key(2), value(7), RequestType.none());
		assertArrayEquals(value(7).toByteArray(), api.get(0, column, key(2), RequestType.current()).toByteArray());
		assertArrayEquals(value(CONFLICTS).toByteArray(), api.get(0, column, key(1), RequestType.current()).toByteArray());
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf value(long value) {
		return Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
	}
}
