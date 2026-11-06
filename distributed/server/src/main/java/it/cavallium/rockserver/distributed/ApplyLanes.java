package it.cavallium.rockserver.distributed;

import java.util.concurrent.LinkedBlockingQueue;

/**
 * The worker threads of the partitioned apply (see {@link Applier}, "Partitioned apply"): {@link #count} lanes, each
 * one platform thread that runs the work given to it in the order it was given. Platform threads, not virtual ones:
 * the work is RocksDB JNI calls. Owned by the state machine, shared by the appliers it creates, closed with it.
 */
final class ApplyLanes implements AutoCloseable {

	private final Thread[] threads;
	private final LinkedBlockingQueue<Runnable>[] queues;
	private volatile boolean closed;

	@SuppressWarnings("unchecked")
	ApplyLanes(String name, int count) {
		if (count < 2) {
			throw new IllegalArgumentException("A partitioned apply needs at least 2 lanes: " + count);
		}
		threads = new Thread[count];
		queues = new LinkedBlockingQueue[count];
		for (int i = 0; i < count; i++) {
			var queue = new LinkedBlockingQueue<Runnable>();
			queues[i] = queue;
			threads[i] = Thread.ofPlatform().name(name + "-apply-lane-" + i).daemon().start(() -> run(queue));
		}
	}

	int count() {
		return threads.length;
	}

	/** Runs {@code work} on lane {@code lane}, after the work given to that lane before. */
	void submit(int lane, Runnable work) {
		if (closed) {
			throw new IllegalStateException("The apply lanes are closed");
		}
		queues[lane].add(work);
	}

	private void run(LinkedBlockingQueue<Runnable> queue) {
		while (true) {
			Runnable work;
			try {
				work = queue.take();
			} catch (InterruptedException ex) {
				return;
			}
			if (work == STOP) {
				return;
			}
			// The work reports its own failures (see Applier); nothing that escapes may end the lane, or the apply
			// thread would wait for it forever.
			try {
				work.run();
			} catch (Throwable ignored) {
				// Reported by the work itself.
			}
		}
	}

	private static final Runnable STOP = () -> {};

	/** Stops the lanes once they ran what they were given, and waits for them. */
	@Override
	public void close() {
		closed = true;
		for (var queue : queues) {
			queue.add(STOP);
		}
		for (var thread : threads) {
			try {
				thread.join();
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				return;
			}
		}
	}
}
