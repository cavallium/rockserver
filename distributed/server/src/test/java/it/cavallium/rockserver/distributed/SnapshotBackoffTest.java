package it.cavallium.rockserver.distributed;

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
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** DD-7e: a snapshot that fails is not tried again at every applied entry. */
class SnapshotBackoffTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

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
	void theDelayDoublesUpToAMinuteAndEndsWithASuccess() {
		var backoff = new RockserverStateMachine.SnapshotBackoff();
		long now = 1_000;
		assertFalse(backoff.waiting(now));
		assertEquals(TimeUnit.SECONDS.toNanos(1), backoff.failed(now));
		assertTrue(backoff.waiting(now + TimeUnit.MILLISECONDS.toNanos(999)));
		assertFalse(backoff.waiting(now + TimeUnit.SECONDS.toNanos(1)));
		assertEquals(TimeUnit.SECONDS.toNanos(2), backoff.failed(now));
		assertEquals(TimeUnit.SECONDS.toNanos(4), backoff.failed(now));
		for (int i = 0; i < 40; i++) {
			backoff.failed(now);
		}
		assertEquals(TimeUnit.MINUTES.toNanos(1), backoff.failed(now));
		backoff.succeeded();
		assertFalse(backoff.waiting(now));
		assertEquals(TimeUnit.SECONDS.toNanos(1), backoff.failed(now));
	}

	@Test
	void aFailingSnapshotIsTriedAgainOnlyAfterItsDelay() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c.withSnapshotEveryEntries(4)).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var machine = leader.stateMachine();
		machine.snapshotFailureForTests = new IOException("injected");
		var api = leader.connection().getSyncApi(RequestContext.batch(Duration.ofSeconds(20)));
		long column = api.createColumn("c", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		long attemptsBefore = machine.snapshotAttempts.get();
		long start = System.nanoTime();
		// Many entries past the snapshot interval within the first delay: one try, maybe two if the first came earlier.
		for (long i = 0; i < 60; i++) {
			api.put(0, column, new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(i).array())),
					Buf.wrap(new byte[8]), RequestType.none());
		}
		long elapsedMillis = (System.nanoTime() - start) / 1_000_000;
		long attempts = machine.snapshotAttempts.get() - attemptsBefore;
		// After delays of 1 s, 2 s, 4 s...: the first try, one per doubling within the elapsed time, and a margin.
		long allowed = 2 + (64 - Long.numberOfLeadingZeros(elapsedMillis / 1000 + 1));
		assertTrue(attempts >= 1 && attempts <= allowed, () -> attempts + " tries in " + elapsedMillis + " ms");
		machine.snapshotFailureForTests = null;
		InProcessCluster.await(TIMEOUT, () -> {
			try {
				api.put(0, column, new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(1000).array())),
						Buf.wrap(new byte[8]), RequestType.none());
			} catch (RuntimeException ex) {
				return false;
			}
			return machine.getLatestSnapshot() != null;
		});
	}
}
