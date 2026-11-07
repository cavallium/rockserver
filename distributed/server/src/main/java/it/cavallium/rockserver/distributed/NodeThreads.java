package it.cavallium.rockserver.distributed;

import java.io.Closeable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * The executors of one node, besides those of Ratis and of the database. Closed by the node, after everything that
 * uses them.
 *
 * <p>Virtual threads run what waits for I/O. RocksDB calls pin the carrier of the virtual thread that makes them
 * (JEP 491 removed pinning by {@code synchronized}, not by native frames), so slow disk reads can hold every carrier
 * at once. The control plane must not wait behind them: the proposer's reply handling, resends and expiries, and the
 * read barrier's rounds run on {@link #control()}, a few platform threads that never run database calls. Their
 * callers' own continuations are handed back to virtual threads (see {@link Proposer}), or hop to {@link #local()}
 * before touching the database (see {@link LocalCommands}).</p>
 *
 * <p>The control plane never calls Ratis' submit either: on the leader its append can wait for room in a full log
 * queue during a disk stall, for as long as the stall lasts. The proposer encodes and submits its batches on
 * {@link #submit()} instead.</p>
 */
final class NodeThreads implements Closeable {

	/**
	 * Platform threads of {@link #control()}: their work is short, and never waits for the disk, the database, a remote
	 * call or Ratis' submit (see {@link #submit()}).
	 */
	static final int CONTROL_THREADS = Math.max(2, Math.min(4, Runtime.getRuntime().availableProcessors() / 4));

	private final ScheduledThreadPoolExecutor control;
	private final ExecutorService local;
	private final ThreadPoolExecutor submit;

	NodeThreads(String nodeId) {
		this.control = new ScheduledThreadPoolExecutor(CONTROL_THREADS,
				Thread.ofPlatform().daemon().name("rockserver-" + nodeId + "-control-", 0).factory());
		// Expiry timers are cancelled once their request is answered: drop them at once instead of at their deadline.
		control.setRemoveOnCancelPolicy(true);
		control.setExecuteExistingDelayedTasksAfterShutdownPolicy(false);
		this.local = Executors.newThreadPerTaskExecutor(
				Thread.ofVirtual().name("rockserver-" + nodeId + "-local-", 0).factory());
		this.submit = new ThreadPoolExecutor(0, Integer.MAX_VALUE, 60, TimeUnit.SECONDS, new SynchronousQueue<>(),
				Thread.ofPlatform().daemon().name("rockserver-" + nodeId + "-submit-", 0).factory());
	}

	/** Platform threads of the control plane: never block them on the database or on a remote call. */
	ScheduledExecutorService control() {
		return control;
	}

	/**
	 * The leader's short local commands (point reads, single-key staged writes, transaction opens), each on a virtual
	 * thread of its own. See performance.md, item 11, for why these are virtual threads and not a platform pool.
	 */
	ExecutorService local() {
		return local;
	}

	/**
	 * The proposer's submit threads: encode its batches and submit them to this node's Raft server (or hand them to
	 * the sender of remote attempts). A submission may block inside Ratis while a disk stall fills its log queue, so
	 * this is never the control plane. Platform threads, not virtual ones: most batches are sent from reply handlers
	 * as slots free up, and on virtual threads they would wait behind carriers pinned by database reads, as the
	 * control plane would. A cached pool (direct handoff, idle threads stop after 60 s): a thread blocks only for a
	 * batch that holds one of the proposer's slots, so at most twice {@link Proposer#MAX_BATCHES_IN_FLIGHT} block at
	 * once (both lanes), plus those taking a batch.
	 */
	ExecutorService submit() {
		return submit;
	}

	/**
	 * Drops the control plane's pending timers, and lets local commands and submissions already running finish.
	 */
	@Override
	public void close() {
		control.shutdownNow();
		submit.shutdown();
		local.shutdown();
	}
}
