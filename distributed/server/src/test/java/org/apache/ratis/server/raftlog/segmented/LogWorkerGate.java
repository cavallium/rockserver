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
