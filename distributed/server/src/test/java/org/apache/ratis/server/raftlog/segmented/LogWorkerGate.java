package org.apache.ratis.server.raftlog.segmented;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.concurrent.CountDownLatch;
import org.apache.ratis.server.raftlog.RaftLog;

/** Stops the log worker of a Raft log, as a hung disk does. In this package for {@link SegmentedRaftLog.Task}. */
public final class LogWorkerGate {

	private LogWorkerGate() {}

	/** The worker writes nothing more once it reaches this, until {@code release} opens. */
	public static void block(RaftLog log, CountDownLatch release) throws ReflectiveOperationException {
		run(log, release::await);
	}

	/**
	 * The worker sleeps {@code millis} once it reaches this, as a disk that stalls for that long, then goes on. The
	 * result completes after the sleep: a slow (not stopped) disk is a loop of short pauses.
	 */
	public static java.util.concurrent.CompletableFuture<Void> pause(RaftLog log, long millis)
			throws ReflectiveOperationException {
		var slept = new java.util.concurrent.CompletableFuture<Void>();
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
	 * Fails unless the Ratis internals this gate uses are still there: a Ratis upgrade that renames them must fail
	 * here, by name, rather than as an odd timing in the tests that use the gate.
	 */
	public static void checkContract() throws ReflectiveOperationException {
		var workerField = SegmentedRaftLog.class.getDeclaredField("fileLogWorker");
		if (workerField.getType() != SegmentedRaftLogWorker.class) {
			throw new NoSuchFieldException("SegmentedRaftLog.fileLogWorker is a " + workerField.getType());
		}
		SegmentedRaftLogWorker.class.getDeclaredMethod("addIOTask", SegmentedRaftLog.Task.class);
		SegmentedRaftLog.Task.class.getDeclaredMethod("execute");
		SegmentedRaftLog.Task.class.getDeclaredMethod("getEndIndex");
	}

	@FunctionalInterface
	private interface Wait {
		void run() throws InterruptedException;
	}

	private static void run(RaftLog log, Wait wait) throws ReflectiveOperationException {
		var workerField = SegmentedRaftLog.class.getDeclaredField("fileLogWorker");
		workerField.setAccessible(true);
		var addTask = SegmentedRaftLogWorker.class.getDeclaredMethod("addIOTask", SegmentedRaftLog.Task.class);
		addTask.setAccessible(true);
		addTask.invoke(workerField.get(log), new SegmentedRaftLog.Task() {
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
