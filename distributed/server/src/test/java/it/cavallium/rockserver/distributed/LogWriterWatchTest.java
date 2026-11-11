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
	/** Between two checks of one row: under {@link LogWriterWatch#MAX_GAP}. */
	private static final long HALF = SECOND / 2;

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
		for (int i = 0; i < 6; i++) {
			assertFalse(watch.failed(7, i < 3 ? 9 : 12, i * HALF, () -> true), "check " + i);
		}
		assertTrue(watch.failed(7, 12, 6 * HALF, () -> true));
	}

	@Test
	void aBusyWriterStartsTheTimeOver() {
		// A hung or slow disk: the writer is inside the write or the fsync, not waiting for work.
		var watch = new LogWriterWatch(Duration.ofSeconds(3));
		for (int i = 0; i < 5; i++) {
			assertFalse(watch.failed(7, 9, i * HALF, () -> true));
		}
		assertFalse(watch.failed(7, 9, 5 * HALF, () -> false));
		for (int i = 6; i < 11; i++) {
			assertFalse(watch.failed(7, 9, i * HALF, () -> true), "check " + i);
		}
		assertTrue(watch.failed(7, 9, 11 * HALF, () -> true));
		var busy = new LogWriterWatch(Duration.ofSeconds(3));
		for (int i = 0; i < 100; i++) {
			assertFalse(busy.failed(7, 9, i * HALF, () -> false));
		}
	}

	/**
	 * Review of R62: a process stopped for longer than the whole time (SIGSTOP, a frozen machine) has one check before
	 * the pause and one right after it, when the writer thread may not have run yet. The pause starts the time over,
	 * and the writer is not even asked at the check after it.
	 */
	@Test
	void aPauseOfTheProcessBetweenTwoChecksStartsTheTimeOver() {
		var watch = new LogWriterWatch(Duration.ofSeconds(3));
		assertFalse(watch.failed(7, 9, 0, () -> true));
		assertFalse(watch.failed(7, 9, HALF, () -> true));
		var asked = new AtomicInteger();
		// 60 s without a check, then the first one: the same flush index, and the writer still where it was.
		long after = HALF + 60 * SECOND;
		assertFalse(watch.failed(7, 9, after, () -> asked.incrementAndGet() > 0));
		assertTrue(asked.get() == 0, "the writer was asked right after a pause");
		// A burst of checks right after the pause does not make up for the time either.
		for (int i = 1; i <= 20; i++) {
			assertFalse(watch.failed(7, 9, after + i, () -> true), "check " + i + " of the burst");
		}
		// It takes the whole time again, from the first check after the pause.
		for (int i = 1; i < 6; i++) {
			assertFalse(watch.failed(7, 9, after + i * HALF, () -> true), "check " + i + " after the pause");
		}
		assertTrue(watch.failed(7, 9, after + 6 * HALF, () -> true));
	}

	/** Every gap longer than {@link LogWriterWatch#MAX_GAP} starts over, so a row of pauses never adds up. */
	@Test
	void checksFurtherApartThanTheGapNeverAddUp() {
		var watch = new LogWriterWatch(Duration.ofSeconds(3));
		long gap = LogWriterWatch.MAX_GAP.toNanos() + 1;
		for (int i = 0; i < 100; i++) {
			assertFalse(watch.failed(7, 9, i * gap, () -> true), "check " + i);
		}
		// At exactly the gap they are one row.
		var atTheGap = new LogWriterWatch(Duration.ofSeconds(3));
		boolean failed = false;
		for (int i = 0; i < 6 && !failed; i++) {
			failed = atTheGap.failed(7, 9, i * LogWriterWatch.MAX_GAP.toNanos(), () -> true);
		}
		assertTrue(failed);
	}

	/** The time alone is not enough: no fewer than {@link LogWriterWatch#MIN_CHECKS} checks in a row. */
	@Test
	void fewerChecksThanTheLeastNumberAreNotAFailedLogWhateverTheTime() {
		var watch = new LogWriterWatch(Duration.ofSeconds(3), Duration.ofSeconds(10), 5);
		assertFalse(watch.failed(7, 9, 0, () -> true));
		assertFalse(watch.failed(7, 9, 4 * SECOND, () -> true));
		assertFalse(watch.failed(7, 9, 8 * SECOND, () -> true));
		assertFalse(watch.failed(7, 9, 12 * SECOND, () -> true));
		assertTrue(watch.failed(7, 9, 16 * SECOND, () -> true));
	}

	@Test
	void theWriterIsAskedOnlyWhileEntriesWaitBehindAStandingFlushIndex() {
		var watch = new LogWriterWatch(Duration.ofSeconds(3));
		var asked = new AtomicInteger();
		watch.failed(7, 7, 0, () -> asked.incrementAndGet() > 0);
		watch.failed(7, 9, HALF, () -> asked.incrementAndGet() > 0);
		watch.failed(8, 9, 2 * HALF, () -> asked.incrementAndGet() > 0);
		assertTrue(asked.get() == 0, "asked " + asked.get() + " times");
		watch.failed(8, 9, 3 * HALF, () -> asked.incrementAndGet() > 0);
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
