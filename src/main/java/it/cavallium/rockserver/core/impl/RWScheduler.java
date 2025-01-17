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
		this(createScheduler(readCap, name + "-read"), createScheduler(writeCap, name + "-write"));
	}

	private static Scheduler createScheduler(int cap, String name) {
		var threadFactory = new ThreadFactoryBuilder()
				.setDaemon(false)
				.setNameFormat(name + "-%d")
				.build();
		var readPool = new ThreadPoolExecutor(cap,
				cap,
				0L,
				TimeUnit.MILLISECONDS,
				new LinkedBlockingQueue<>(DEFAULT_BOUNDED_ELASTIC_QUEUESIZE),
				threadFactory,
				new ThreadPoolExecutor.AbortPolicy()
		);
		return Schedulers.fromExecutorService(readPool, name);
	}

	public Mono<Void> disposeGracefully() {
		return Mono.whenDelayError(read.disposeGracefully(), write.disposeGracefully());
	}

	public void dispose() {
		read.dispose();
		write.dispose();
	}
}
