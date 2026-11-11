package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
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
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.server.raftlog.segmented.LogWorkerGate;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * {@link LogWriterWatch} (R62-2): the rule, and what it reads of Ratis (the name of the thread that writes the log and
 * where that thread waits for work), against the Ratis version in use.
 */
class LogWriterWatchTest {

	private static final long SECOND = TimeUnit.SECONDS.toNanos(1);

	@Test
	void aLogWithNothingToFlushNeverFails() {
		var watch = new LogWriterWatch(Duration.ofSeconds(3));
		for (int i = 0; i < 100; i++) {
			assertFalse(watch.failed(7, 7, i * SECOND, () -> true));
		}
	}

	@Test
	void aLogThatFlushesNeverFails() {
		var watch = new LogWriterWatch(Duration.ofSeconds(3));
		for (int i = 0; i < 100; i++) {
			assertFalse(watch.failed(i, i + 5, i * SECOND, () -> true));
		}
	}

	@Test
	void entriesWaitingBehindAnIdleWriterForTheWholeTimeAreAFailedLog() {
		var watch = new LogWriterWatch(Duration.ofSeconds(3));
		assertFalse(watch.failed(7, 9, 0, () -> true));
		assertFalse(watch.failed(7, 9, SECOND, () -> true));
		assertFalse(watch.failed(7, 12, 2 * SECOND, () -> true));
		assertTrue(watch.failed(7, 12, 3 * SECOND, () -> true));
	}

	@Test
	void aBusyWriterStartsTheTimeOver() {
		// A hung or slow disk: the writer is inside the write or the fsync, not waiting for work.
		var watch = new LogWriterWatch(Duration.ofSeconds(3));
		assertFalse(watch.failed(7, 9, 0, () -> true));
		assertFalse(watch.failed(7, 9, 2 * SECOND, () -> false));
		assertFalse(watch.failed(7, 9, 4 * SECOND, () -> true));
		assertTrue(watch.failed(7, 9, 5 * SECOND, () -> true));
		for (int i = 0; i < 100; i++) {
			assertFalse(new LogWriterWatch(Duration.ofSeconds(3)).failed(7, 9, i * SECOND, () -> false));
		}
	}

	@Test
	void theWriterIsAskedOnlyWhileEntriesWaitBehindAStandingFlushIndex() {
		var watch = new LogWriterWatch(Duration.ofSeconds(3));
		var asked = new AtomicInteger();
		watch.failed(7, 7, 0, () -> asked.incrementAndGet() > 0);
		watch.failed(7, 9, SECOND, () -> asked.incrementAndGet() > 0);
		watch.failed(8, 9, 2 * SECOND, () -> asked.incrementAndGet() > 0);
		assertTrue(asked.get() == 0, "asked " + asked.get() + " times");
		watch.failed(8, 9, 3 * SECOND, () -> asked.incrementAndGet() > 0);
		assertTrue(asked.get() == 1, "asked " + asked.get() + " times");
	}

	/** Ratis contract: the writer thread is found by its name, waits where the rule expects, and not while it writes. */
	@Test
	void theWriterThreadOfARealLogIsFoundIdleWhenIdleAndBusyWhileItsTaskRuns(@TempDir Path temp) throws Exception {
		try (var cluster = new InProcessCluster(temp, 1, c -> c).startAll()) {
			var node = cluster.awaitLeader(Duration.ofSeconds(30));
			var context = RequestContext.batch(Duration.ofSeconds(20));
			long column = node.connection().getSyncApi(context)
					.createColumn("c", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			var division = node.stateMachine().division();
			var name = division.getMemberId() + LogWriterWatch.WRITER_THREAD_SUFFIX;
			var writer = LogWriterWatch.findThread(name);
			assertNotNull(writer, "no thread named " + name);
			assertNull(LogWriterWatch.findThread(name + "-none"));
			long deadline = System.nanoTime() + 10 * SECOND;
			while (!LogWriterWatch.idle(writer)) {
				assertTrue(System.nanoTime() < deadline, "the writer of an idle log does not wait in "
						+ LogWriterWatch.IDLE_CLASS + "." + LogWriterWatch.IDLE_METHOD);
				Thread.sleep(20);
			}
			var release = new CountDownLatch(1);
			LogWorkerGate.block(division.getRaftLog(), release);
			var held = node.connection().getAsyncApi(context).putAsync(0, column,
					new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(1).array())), Buf.wrap(new byte[] {1}),
					RequestType.none());
			try {
				for (int i = 0; i < 20; i++) {
					Thread.sleep(50);
					assertFalse(LogWriterWatch.idle(writer), "the writer counts as idle while a task of its log runs");
				}
			} finally {
				release.countDown();
			}
			held.get(20, TimeUnit.SECONDS);
		}
	}
}
