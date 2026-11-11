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
 * The entries of the failed request stay in the log's memory, and a follower's answers name a next index past them;
 * but it never answers them, or any later entry, as stored (review of R62, 1): a follower appends the entries of its
 * requests one request after the other, each chained to the future of the one before
 * ({@code ServerImplUtils.NavigableIndices.append}), so after one failed write every later request that carries
 * entries fails without reaching the log, the same entries sent again included. The leader's match index for such a
 * follower stays at what it flushed ({@code RaftLogFailureTest}).</p>
 *
 * <p>What shows it from outside: the log holds entries past its flush index, the flush index stands still, and the
 * writer thread waits for work instead of writing. A healthy writer with entries to flush is writing or syncing; a
 * writer on a hung or slow disk is inside the write or the fsync, not waiting for work. So when every check for
 * {@link #AFTER} found the same flush index below the last entry and the writer idle, the log has failed, and the
 * node stops as a failed node (exit code 3), to be restarted on a disk that works. A check that finds the writer
 * busy, or does not find its thread, starts the time over.</p>
 *
 * <p>The time alone is not enough (review of R62): a process that was stopped for longer than {@link #AFTER} (SIGSTOP,
 * a frozen virtual machine, a long safepoint) has one check before the pause and one right after it, {@link #AFTER}
 * apart, and the second may run before the writer thread, which was handed its task before the pause, has run at
 * all. So the checks must also be an unbroken row: a check that comes later than {@link #MAX_GAP} after the one
 * before it starts the time over, and no fewer than {@link #MIN_CHECKS} checks in a row must have found the same.
 * Between two checks of such a row the process ran, and a writer that runs takes its task.</p>
 */
final class LogWriterWatch {

	/** Many checks of the Raft watch, and well under the time a client waits for one write. */
	static final Duration AFTER = Duration.ofSeconds(3);
	/** What Ratis appends to the division's member id to name the thread that writes the log. */
	static final String WRITER_THREAD_SUFFIX = "-SegmentedRaftLogWorker";
	/** Where that thread waits for its next task. */
	static final String IDLE_CLASS = "org.apache.ratis.util.DataBlockingQueue";
	static final String IDLE_METHOD = "poll";

	/**
	 * Two checks further apart than this are not one row: the process, or the thread that checks, stood still between
	 * them. Several times the interval of the Raft watch, and well under {@link #AFTER}.
	 */
	static final Duration MAX_GAP = Duration.ofSeconds(1);
	/** The least number of checks in a row that must have found the same, whatever the time between them. */
	static final int MIN_CHECKS = 5;

	private final long afterNanos;
	private final long maxGapNanos;
	private final int minChecks;
	private boolean waiting;
	private long flushIndex;
	private long sinceNanos;
	private long lastNanos;
	private int checks;

	LogWriterWatch(Duration after) {
		this(after, MAX_GAP, MIN_CHECKS);
	}

	LogWriterWatch(Duration after, Duration maxGap, int minChecks) {
		this.afterNanos = after.toNanos();
		this.maxGapNanos = maxGap.toNanos();
		this.minChecks = minChecks;
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
		boolean gap = nowNanos - lastNanos > maxGapNanos;
		lastNanos = nowNanos;
		// The writer is not asked after a gap: what it does right after a pause says nothing.
		if (!waiting || !this.waiting || flushIndex != this.flushIndex || gap || !writerIdle.getAsBoolean()) {
			this.waiting = waiting;
			this.flushIndex = flushIndex;
			sinceNanos = nowNanos;
			checks = 1;
			return false;
		}
		checks++;
		return nowNanos - sinceNanos >= afterNanos && checks >= minChecks;
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
