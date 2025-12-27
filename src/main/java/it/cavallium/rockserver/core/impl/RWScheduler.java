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
		return new ThreadPoolExecutor(cap,
				cap,
				0L,
				TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<>(DEFAULT_BOUNDED_ELASTIC_QUEUESIZE),
				threadFactory,
				new ThreadPoolExecutor.AbortPolicy()
		);
	}

	public Mono<Void> disposeGracefully() {
		return Mono.whenDelayError(read.disposeGracefully(), write.disposeGracefully());
	}

	public void dispose() {
		read.dispose();
		write.dispose();
		shutdownExecutor(readExecutor);
		shutdownExecutor(writeExecutor);
	}

	private void shutdownExecutor(Executor executor) {
		if (executor instanceof java.util.concurrent.ExecutorService es) {
			es.shutdown();
			try {
				if (!es.awaitTermination(10, TimeUnit.SECONDS)) {
					es.shutdownNow();
				}
			} catch (InterruptedException e) {
				es.shutdownNow();
				Thread.currentThread().interrupt();
			}
		}
	}
}
