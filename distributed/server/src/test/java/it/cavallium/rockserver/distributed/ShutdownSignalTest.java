package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
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
import org.junit.jupiter.api.io.TempDir;

/**
 * DD-7h: a SIGTERM that races a replica failure. The shutdown hook's stop ({@link ReplicatedServerMain#closeOnSignal})
 * returns only once the failed node stopped, and then gives the failure's exit status, 3. The process version (a real
 * SIGTERM) is still to run.
 */
class ShutdownSignalTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

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
	void aSignalDuringAReplicaFailureWaitsForTheStopAndExitsWithThree() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var failing = cluster.anyFollower();
		var api = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));
		long column = api.createColumn("c", SCHEMA);
		cluster.awaitConverged(TIMEOUT);
		failing.stateMachine().testHooks().applyFailure = new IllegalStateException("Injected apply failure");
		api.put(0, column, new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(1).array())),
				Buf.wrap(new byte[] {1}), RequestType.none());
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (!failing.isFailed()) {
			assertTrue(System.nanoTime() < deadline, "the replica did not fail");
			Thread.onSpinWait();
		}
		// The failure's own stop is running, or about to: the signal's stop must wait for it either way.
		assertEquals(3, ReplicatedServerMain.closeOnSignal(failing));
		assertTrue(failing.stopped().isDone(), "the hook halts only once the node stopped");
	}
}
