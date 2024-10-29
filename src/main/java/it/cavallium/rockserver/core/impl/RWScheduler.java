package it.cavallium.rockserver.core.impl;

import static reactor.core.scheduler.Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE;

import java.util.concurrent.Executor;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public record RWScheduler(Scheduler read, Scheduler write, Executor readExecutor, Executor writeExecutor) {

	public RWScheduler(Scheduler read, Scheduler write) {
		this(read, write, read::schedule, write::schedule);
	}

	public RWScheduler(int readCap, int writeCap, String name) {
		this(
				Schedulers.newBoundedElastic(readCap, DEFAULT_BOUNDED_ELASTIC_QUEUESIZE, name + "-read"),
				Schedulers.newBoundedElastic(writeCap, DEFAULT_BOUNDED_ELASTIC_QUEUESIZE, name + "-write")
		);
	}

	public Mono<Void> disposeGracefully() {
		return Mono.whenDelayError(read.disposeGracefully(), write.disposeGracefully());
	}

	public void dispose() {
		read.dispose();
		write.dispose();
	}
}
