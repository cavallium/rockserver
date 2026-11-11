package it.cavallium.rockserver.distributed;

import java.time.Duration;
import java.util.function.BooleanSupplier;
import org.jetbrains.annotations.Nullable;

/**
 * Tells when this node's Raft log ended after a write to it failed (R62-2).
 *
 * <p>In Ratis 3.3.1 the thread that writes the log ({@code SegmentedRaftLogWorker}) does not close anything when a
 * write or an fsync fails with an {@code IOException} (a full disk, an I/O error): it fails that task, remembers the
 * error, and from then on fails every task it takes, without touching the disk ("Log already failed at index"). On a
 * leader the next append then closes the log, which {@code ReplicatedNode} sees ({@code RaftLog.isOpened}). On a
 * follower nothing else happens: the division stays alive, every request with entries fails, and the node is a member
 * that can store nothing until it is restarted. Ratis tells nobody: the state machine is only told of a failed read.
 * The entries of the failed request also stay in the log's memory, and a follower's answer names its log's next
 * index, not its flush index, so the same entries sent again may be answered as stored (read in the bytecode, not
 * reproduced).</p>
 *
 * <p>What shows it from outside: the log holds entries past its flush index, the flush index stands still, and the
 * writer thread waits for work instead of writing. A healthy writer with entries to flush is writing or syncing; a
 * writer on a hung or slow disk is inside the write or the fsync, not waiting for work. So when every check for
 * {@link #AFTER} found the same flush index below the last entry and the writer idle, the log has failed, and the
 * node stops as a failed node (exit code 3), to be restarted on a disk that works. A check that finds the writer
 * busy, or does not find its thread, starts the time over.</p>
 */
final class LogWriterWatch {

	/** Many checks of the Raft watch, and well under the time a client waits for one write. */
	static final Duration AFTER = Duration.ofSeconds(3);
	/** What Ratis appends to the division's member id to name the thread that writes the log. */
	static final String WRITER_THREAD_SUFFIX = "-SegmentedRaftLogWorker";
	/** Where that thread waits for its next task. */
	static final String IDLE_CLASS = "org.apache.ratis.util.DataBlockingQueue";
	static final String IDLE_METHOD = "poll";

	private final long afterNanos;
	private boolean waiting;
	private long flushIndex;
	private long sinceNanos;

	LogWriterWatch(Duration after) {
		this.afterNanos = after.toNanos();
	}

	/**
	 * Takes the current observation and tells whether the log has failed.
	 *
	 * @param flushIndex the last entry of the log that reached the disk
	 * @param lastIndex the last entry of the log
	 * @param writerIdle whether the log's writer waits for work now; asked only while the flush index stands still below
	 *     the last entry
	 */
	boolean failed(long flushIndex, long lastIndex, long nowNanos, BooleanSupplier writerIdle) {
		boolean waiting = flushIndex < lastIndex;
		if (!waiting || !this.waiting || flushIndex != this.flushIndex || !writerIdle.getAsBoolean()) {
			this.waiting = waiting;
			this.flushIndex = flushIndex;
			sinceNanos = nowNanos;
			return false;
		}
		return nowNanos - sinceNanos >= afterNanos;
	}

	/** The live thread named {@code name}, or null. */
	static @Nullable Thread findThread(String name) {
		var group = Thread.currentThread().getThreadGroup();
		while (group.getParent() != null) {
			group = group.getParent();
		}
		var threads = new Thread[Math.max(64, group.activeCount() * 2)];
		int count = group.enumerate(threads, true);
		for (int i = 0; i < count; i++) {
			if (threads[i].getName().equals(name)) {
				return threads[i];
			}
		}
		return null;
	}

	/** Whether {@code writer}, the thread that writes a Raft log, waits for its next task. */
	static boolean idle(Thread writer) {
		for (var frame : writer.getStackTrace()) {
			if (frame.getClassName().equals(IDLE_CLASS) && frame.getMethodName().equals(IDLE_METHOD)) {
				return true;
			}
		}
		return false;
	}
}
