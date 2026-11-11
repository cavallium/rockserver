package org.apache.ratis.server.raftlog.segmented;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.server.raftlog.RaftLogBase;

/** Stops the log worker of a Raft log, as a hung disk does. In this package for {@link SegmentedRaftLog.Task}. */
public final class LogWorkerGate {

	private LogWorkerGate() {}

	/**
	 * Bytes of log entries the log keeps in memory (the entry caches of its closed segments and of its open one), and
	 * the most closed segments Ratis keeps cached before it evicts, as {@code {bytes, maxCachedSegments}}.
	 */
	public static long[] cache(RaftLog log) throws ReflectiveOperationException {
		var cache = ((SegmentedRaftLog) log).getRaftLogCache();
		var closedField = SegmentedRaftLogCache.class.getDeclaredField("closedSegments");
		closedField.setAccessible(true);
		var closed = (SegmentedRaftLogCache.LogSegmentList) closedField.get(cache);
		var open = cache.getOpenSegment();
		var cacheInfo = SegmentedRaftLogCache.LogSegmentList.class.getDeclaredMethod("getCacheInfo");
		cacheInfo.setAccessible(true);
		var info = cacheInfo.invoke(closed);
		var size = info.getClass().getDeclaredMethod("getSize");
		size.setAccessible(true);
		long bytes = (long) size.invoke(info) + (open == null ? 0 : open.getTotalCacheSize());
		return new long[] {bytes, cache.getMaxCachedSegments()};
	}

	/** The worker writes nothing more once it reaches this, until {@code release} opens. */
	public static void block(RaftLog log, CountDownLatch release) throws ReflectiveOperationException {
		run(log, release::await);
	}

	/**
	 * The worker sleeps {@code millis} once it reaches this, as a disk that stalls for that long, then goes on. The
	 * result completes after the sleep: a slow (not stopped) disk is a loop of short pauses.
	 */
	public static CompletableFuture<Void> pause(RaftLog log, long millis)
			throws ReflectiveOperationException {
		var slept = new CompletableFuture<Void>();
		run(log, () -> {
			try {
				Thread.sleep(millis);
			} finally {
				slept.complete(null);
			}
		});
		return slept;
	}

	/**
	 * The worker's next task fails with {@code error}, as a write or an fsync does on a full or failing disk. Ratis
	 * 3.3.1 then fails every later task of this log with "Log already failed", without touching the disk.
	 */
	public static void fail(RaftLog log, IOException error) throws ReflectiveOperationException {
		add(log, new SegmentedRaftLog.Task() {
			@Override
			void execute() throws IOException {
				throw error;
			}

			@Override
			long getEndIndex() {
				// Not below what the worker wrote last: such a failure is ignored, as that of an entry a snapshot replaced.
				return Long.MAX_VALUE;
			}
		});
	}

	/**
	 * Holds the write lock of {@code log} on a thread of its own until {@code release} opens, as Ratis does while an
	 * append waits for room in the queue of a log worker that stopped writing (a hung disk): every read of the log that
	 * takes its read lock ({@code getNextIndex}, {@code getTermIndex}, {@code getLastEntryTermIndex}, ...) waits
	 * meanwhile. Returns once the lock is held; join the returned thread after opening {@code release}.
	 */
	public static Thread holdWriteLock(RaftLog log, CountDownLatch release) throws InterruptedException {
		var held = new CountDownLatch(1);
		var holder = Thread.ofPlatform().daemon().name("log-write-lock-holder").start(() -> {
			try (var _ = ((RaftLogBase) log).writeLock()) {
				held.countDown();
				release.await();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		held.await();
		return holder;
	}

	/**
	 * Fails unless the Ratis internals this gate uses are still there: a Ratis upgrade that renames them must fail
	 * here, by name, rather than as an odd timing in the tests that use the gate.
	 */
	public static void checkContract() throws ReflectiveOperationException {
		var workerField = SegmentedRaftLog.class.getDeclaredField("fileLogWorker");
		if (workerField.getType() != SegmentedRaftLogWorker.class) {
			throw new NoSuchFieldException("SegmentedRaftLog.fileLogWorker is a " + workerField.getType());
		}
		SegmentedRaftLogWorker.class.getDeclaredMethod("addIOTask", SegmentedRaftLog.Task.class);
		var closed = SegmentedRaftLogCache.class.getDeclaredField("closedSegments");
		if (closed.getType() != SegmentedRaftLogCache.LogSegmentList.class) {
			throw new NoSuchFieldException("SegmentedRaftLogCache.closedSegments is a " + closed.getType());
		}
		SegmentedRaftLog.Task.class.getDeclaredMethod("execute");
		SegmentedRaftLog.Task.class.getDeclaredMethod("getEndIndex");
		RaftLogBase.class.getDeclaredMethod("writeLock");
	}

	@FunctionalInterface
	private interface Wait {
		void run() throws InterruptedException;
	}

	private static void add(RaftLog log, SegmentedRaftLog.Task task) throws ReflectiveOperationException {
		var workerField = SegmentedRaftLog.class.getDeclaredField("fileLogWorker");
		workerField.setAccessible(true);
		var addTask = SegmentedRaftLogWorker.class.getDeclaredMethod("addIOTask", SegmentedRaftLog.Task.class);
		addTask.setAccessible(true);
		addTask.invoke(workerField.get(log), task);
	}

	private static void run(RaftLog log, Wait wait) throws ReflectiveOperationException {
		add(log, new SegmentedRaftLog.Task() {
			@Override
			void execute() throws IOException {
				try {
					wait.run();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					throw new InterruptedIOException();
				}
			}

			@Override
			long getEndIndex() {
				return 0;
			}
		});
	}
}
