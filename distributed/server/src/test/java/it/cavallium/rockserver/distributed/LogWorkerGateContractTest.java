package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.server.raftlog.segmented.LogWorkerGate;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link LogWorkerGate}, which the hung-disk and slow-disk tests use, reaches into Ratis-private code (the log worker
 * field of {@code SegmentedRaftLog} and its {@code addIOTask}). This pins that it still works against the Ratis
 * version in use: the members exist, and on a real single-node cluster a blocked worker holds a write back until it is
 * released, and a pause delays one by about its length.
 */
@Tag("slow")
class LogWorkerGateContractTest {

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

	@Test
	void theRatisMembersTheGateUsesExist() throws Exception {
		LogWorkerGate.checkContract();
	}

	@Test
	void aBlockedWorkerHoldsWritesBackAndAPauseDelaysThem() throws Exception {
		cluster = new InProcessCluster(temp, 1, c -> c).startAll();
		var node = cluster.awaitLeader(Duration.ofSeconds(30));
		var api = node.connection().getAsyncApi(CONTEXT);
		long column = node.connection().getSyncApi(CONTEXT).createColumn("c", SCHEMA);
		api.putAsync(0, column, key(0), Buf.wrap(new byte[] {0}), RequestType.none()).get(10, TimeUnit.SECONDS);

		var log = node.stateMachine().division().getRaftLog();
		var release = new CountDownLatch(1);
		LogWorkerGate.block(log, release);
		var held = api.putAsync(0, column, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
		Thread.sleep(1000);
		assertFalse(held.isDone(), "a write was acknowledged while the log worker was blocked");
		release.countDown();
		held.get(10, TimeUnit.SECONDS);

		var paused = LogWorkerGate.pause(log, 500);
		long start = System.nanoTime();
		api.putAsync(0, column, key(2), Buf.wrap(new byte[] {2}), RequestType.none()).get(10, TimeUnit.SECONDS);
		long tookMillis = (System.nanoTime() - start) / 1_000_000;
		assertTrue(paused.isDone());
		assertTrue(tookMillis >= 300, "the write took " + tookMillis + " ms behind a 500 ms pause");
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
