package it.cavallium.rockserver.core.impl;

import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE;

import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.util.concurrent.Executor;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public record RWScheduler(Scheduler read, Scheduler write, Executor readExecutor, Executor writeExecutor) {

	private static final int EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 10;

	public RWScheduler(Scheduler read, Scheduler write) {
		this(read, write, read::schedule, write::schedule);
	}

	public RWScheduler(int readCap, int writeCap, String name) {
		this(createExecutor(readCap, name + "-read"), createExecutor(writeCap, name + "-write"), name);
	}

	private RWScheduler(java.util.concurrent.ExecutorService readExecutor, java.util.concurrent.ExecutorService writeExecutor, String name) {
		this(Schedulers.fromExecutorService(readExecutor, name + "-read"), Schedulers.fromExecutorService(writeExecutor, name + "-write"), readExecutor, writeExecutor);
	}

	private static java.util.concurrent.ExecutorService createExecutor(int cap, String name) {
		var threadFactory = new ThreadFactoryBuilder()
				.setDaemon(false)
				.setNameFormat(name + "-%d")
				.build();
		var executor = new ThreadPoolExecutor(cap,
				cap,
				0L,
				TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<>(DEFAULT_BOUNDED_ELASTIC_QUEUESIZE),
				threadFactory,
				new ThreadPoolExecutor.AbortPolicy()
		);
		executor.allowCoreThreadTimeOut(false);
		return executor;
	}

	public Mono<Void> disposeGracefully() {
		return Mono.whenDelayError(read.disposeGracefully(), write.disposeGracefully());
	}

	public void dispose() {
		var interruption = new CloseInterruption();
		try {
			RuntimeException failure = cleanup(null, read::dispose);
			failure = cleanup(failure, write::dispose);
			failure = cleanup(failure, () -> shutdownExecutor(readExecutor, "read", interruption));
			failure = cleanup(failure, () -> shutdownExecutor(writeExecutor, "write", interruption));
			if (failure != null) {
				throw failure;
			}
		} finally {
			interruption.restore();
		}
	}

	private static RuntimeException cleanup(RuntimeException failure, Runnable cleanup) {
		try {
			cleanup.run();
		} catch (RuntimeException e) {
			if (failure == null) {
				return e;
			}
			if (failure != e) {
				failure.addSuppressed(e);
			}
		}
		return failure;
	}

	private static void shutdownExecutor(Executor executor, String name, CloseInterruption interruption) {
		if (executor instanceof java.util.concurrent.ExecutorService es) {
			es.shutdown();
			if (executorTerminationTimedOut(es, interruption)) {
				es.shutdownNow();
				if (executorTerminationTimedOut(es, interruption)) {
					throw new IllegalStateException(name + " executor did not terminate after forced shutdown");
				}
			}
		}
	}

	private static boolean executorTerminationTimedOut(java.util.concurrent.ExecutorService executor,
	                                                   CloseInterruption interruption) {
		var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS);
		while (true) {
			try {
				var remainingNanos = deadlineNanos - System.nanoTime();
				if (remainingNanos <= 0) {
					return true;
				}
				return !executor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
			} catch (InterruptedException _) {
				interruption.record();
				executor.shutdownNow();
			}
		}
	}

	private static final class CloseInterruption {

		private boolean interrupted;

		void record() {
			interrupted = true;
		}

		void restore() {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}
}
