package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.AdaptiveBatcher;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;
import reactor.util.context.Context;

public class AdaptiveBatcherTest {

    @Test
    @DisplayName("The batching boundary should preserve downstream Reactor context")
    public void testDownstreamContextIsVisibleToSource() {
        Flux<String> contextualSource = Flux.deferContextual(context ->
            Flux.just(context.<String>get("tenant")));

        StepVerifier.create(
                AdaptiveBatcher.buffer(contextualSource, 1, 4, Duration.ofSeconds(1))
                    .contextWrite(Context.of("tenant", "rockserver"))
            )
            .expectNext(List.of("rockserver"))
            .verifyComplete();
    }

    @Test
    @Timeout(10)
    @DisplayName("Cancellation while scheduling a timeout should dispose the late task")
    public void testCancelWhileTimeoutSchedulingIsInFlight() throws Exception {
        CountDownLatch schedulingStarted = new CountDownLatch(1);
        CountDownLatch releaseScheduling = new CountDownLatch(1);
        AtomicReference<Disposable> returnedTask = new AtomicReference<>();
        Scheduler blockingScheduler = new Scheduler() {
            @Override
            public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
                schedulingStarted.countDown();
                try {
                    if (!releaseScheduling.await(1, TimeUnit.SECONDS)) {
                        throw new AssertionError("timeout scheduling was not released");
                    }
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new AssertionError(exception);
                }
                Disposable disposable = Disposables.single();
                returnedTask.set(disposable);
                return disposable;
            }

            @Override
            public Disposable schedule(Runnable task) {
                task.run();
                return Disposables.disposed();
            }

            @Override
            public Worker createWorker() {
                return Schedulers.immediate().createWorker();
            }
        };
        TestPublisher<Integer> publisher = TestPublisher.create();
        AtomicInteger receivedBatches = new AtomicInteger();
        BaseSubscriber<List<Integer>> subscriber = new BaseSubscriber<>() {
            @Override
            protected void hookOnSubscribe(Subscription subscription) {
                request(1);
            }

            @Override
            protected void hookOnNext(List<Integer> value) {
                receivedBatches.incrementAndGet();
            }
        };

        AdaptiveBatcher.buffer(publisher.flux(), 2, 8, Duration.ofSeconds(1), blockingScheduler)
            .subscribe(subscriber);
        Thread producer = Thread.startVirtualThread(() -> publisher.next(1));
        Assertions.assertTrue(schedulingStarted.await(1, TimeUnit.SECONDS));

        subscriber.cancel();
        releaseScheduling.countDown();
        producer.join(1_000);

        Assertions.assertFalse(producer.isAlive());
        publisher.assertCancelled();
        Assertions.assertEquals(0, receivedBatches.get());
        Assertions.assertNotNull(returnedTask.get());
        Assertions.assertTrue(returnedTask.get().isDisposed(),
            "a timer returned after cancellation must be disposed");
    }

    @Test
    @Timeout(10)
    @DisplayName("Scheduler rejection should cancel upstream and fail even without downstream demand")
    public void testSchedulerRejectionIsTerminalWithoutDemand() {
        RejectedExecutionException rejection = new RejectedExecutionException("synthetic rejection");
        Scheduler rejectingScheduler = new Scheduler() {
            @Override
            public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
                throw rejection;
            }

            @Override
            public Disposable schedule(Runnable task) {
                task.run();
                return Disposables.disposed();
            }

            @Override
            public Worker createWorker() {
                return Schedulers.immediate().createWorker();
            }
        };
        TestPublisher<Integer> publisher = TestPublisher.create();

        StepVerifier.create(
                AdaptiveBatcher.buffer(
                    publisher.flux(), 2, 8, Duration.ofSeconds(1), rejectingScheduler),
                0
            )
            .then(() -> publisher.next(1))
            .expectErrorSatisfies(error -> Assertions.assertSame(rejection, error))
            .verify();

        publisher.assertCancelled();
    }

    @Test
    @Timeout(10)
    @DisplayName("A full batch paused by backpressure should cancel its obsolete timer")
    public void testFullBatchWaitingForDemandCancelsTimer() {
        AtomicReference<Disposable> timer = new AtomicReference<>();
        Scheduler recordingScheduler = new Scheduler() {
            @Override
            public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
                Disposable disposable = Disposables.single();
                timer.set(disposable);
                return disposable;
            }

            @Override
            public Disposable schedule(Runnable task) {
                task.run();
                return Disposables.disposed();
            }

            @Override
            public Worker createWorker() {
                return Schedulers.immediate().createWorker();
            }
        };
        TestPublisher<Integer> publisher = TestPublisher.create();

        StepVerifier.create(
                AdaptiveBatcher.buffer(
                    publisher.flux(), 2, 8, Duration.ofMinutes(1), recordingScheduler),
                0
            )
            .then(() -> publisher.next(1))
            .then(() -> Assertions.assertNotNull(timer.get()))
            .then(() -> publisher.next(2))
            .then(() -> Assertions.assertTrue(timer.get().isDisposed(),
                "a full batch no longer needs its partial-batch timeout"))
            .thenRequest(1)
            .expectNext(List.of(1, 2))
            .thenCancel()
            .verify();
    }

    @Test
    @DisplayName("Invalid batching bounds and timeouts should fail at assembly")
    public void testConfigurationValidation() {
        Flux<Integer> source = Flux.never();

        Assertions.assertThrows(IllegalArgumentException.class,
            () -> AdaptiveBatcher.buffer(source, 0, 8, Duration.ofSeconds(1)));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> AdaptiveBatcher.buffer(source, 8, 4, Duration.ofSeconds(1)));
        Assertions.assertThrows(IllegalArgumentException.class,
            () -> AdaptiveBatcher.buffer(source, 4, 8, Duration.ZERO));
    }

    @Timeout(10)
    @Test
    @DisplayName("A paused downstream should not turn the first minimum batch into one maximum-sized batch")
    public void testInitialUpstreamRequestIsLimitedToFirstBatch() {
        AtomicLong requested = new AtomicLong();
        Flux<Integer> source = Flux.from(subscriber -> subscriber.onSubscribe(new Subscription() {
            @Override
            public void request(long n) {
                requested.addAndGet(n);
            }

            @Override
            public void cancel() {
            }
        }));

        StepVerifier.create(AdaptiveBatcher.buffer(source, 4, 32, Duration.ofMinutes(1)), 0)
            .expectSubscription()
            .then(() -> Assertions.assertEquals(4, requested.get()))
            .thenCancel()
            .verify();
    }

    @Timeout(10)
    @Test
    @DisplayName("A partial timeout should reuse outstanding upstream demand")
    public void testTimeoutDoesNotOverRequestNextBatch() {
        AtomicInteger requestCalls = new AtomicInteger();
        AtomicInteger nextValue = new AtomicInteger(1);
        AtomicLong outstanding = new AtomicLong();
        Flux<Integer> source = Flux.create(sink -> sink.onRequest(n -> {
            outstanding.addAndGet(n);
            if (requestCalls.incrementAndGet() == 1) {
                sink.next(nextValue.getAndIncrement());
                outstanding.decrementAndGet();
            } else {
                long toEmit = outstanding.getAndSet(0);
                for (long i = 0; i < toEmit; i++) {
                    sink.next(nextValue.getAndIncrement());
                }
            }
        }));

        StepVerifier.withVirtualTime(() ->
                AdaptiveBatcher.buffer(source, 4, 8, Duration.ofSeconds(1))
            )
            .expectSubscription()
            .thenRequest(1)
            .thenAwait(Duration.ofSeconds(1))
            .expectNext(List.of(1))
            .thenRequest(1)
            .expectNext(List.of(2, 3, 4, 5))
            .thenCancel()
            .verify();
    }

    @Timeout(10)
    @Test
    @DisplayName("Delayed demand from a larger batch should be split at the shrunken limit")
    public void testOutstandingDemandIsSplitAfterShrink() {
        AtomicInteger nextValue = new AtomicInteger(1);
        AtomicLong outstanding = new AtomicLong();
        AtomicReference<FluxSink<Integer>> sourceSink = new AtomicReference<>();
        Flux<Integer> source = Flux.create(sink -> {
            sourceSink.set(sink);
            sink.onRequest(outstanding::addAndGet);
        });

        StepVerifier.withVirtualTime(() ->
                AdaptiveBatcher.buffer(source, 4, 8, Duration.ofSeconds(1)),
                2
            )
            .expectSubscription()
            .then(() -> emitRequested(sourceSink.get(), outstanding, nextValue, 4))
            .expectNext(List.of(1, 2, 3, 4))
            .then(() -> emitRequested(sourceSink.get(), outstanding, nextValue, 1))
            .thenAwait(Duration.ofSeconds(1))
            .expectNext(List.of(5))
            .then(() -> emitRequested(sourceSink.get(), outstanding, nextValue, 7))
            .thenRequest(1)
            .expectNext(List.of(6, 7, 8, 9))
            .thenCancel()
            .verify();
    }

    @Timeout(10)
    @Test
    @DisplayName("A canceled timeout should not flush a newer batch")
    public void testStaleTimeoutCannotFlushNextBatch() {
        List<Runnable> scheduledTasks = new ArrayList<>();
        Scheduler manualScheduler = new Scheduler() {
            @Override
            public Disposable schedule(Runnable task, long delay, java.util.concurrent.TimeUnit unit) {
                scheduledTasks.add(task);
                return Disposables.single();
            }

            @Override
            public Disposable schedule(Runnable task) {
                task.run();
                return Disposables.disposed();
            }

            @Override
            public Worker createWorker() {
                return Schedulers.immediate().createWorker();
            }
        };
        TestPublisher<Integer> publisher = TestPublisher.create();

        StepVerifier.create(AdaptiveBatcher.buffer(
                publisher.flux(), 4, 8, Duration.ofSeconds(1), manualScheduler), 2)
            .then(() -> publisher.next(1, 2, 3, 4))
            .expectNext(List.of(1, 2, 3, 4))
            .then(() -> publisher.next(5))
            .then(() -> {
                Assertions.assertEquals(2, scheduledTasks.size());
                scheduledTasks.getFirst().run();
            })
            .then(() -> publisher.next(6, 7, 8, 9, 10, 11, 12))
            .expectNext(List.of(5, 6, 7, 8, 9, 10, 11, 12))
            .thenCancel()
            .verify();
    }

    @Timeout(10)
    @Test
    @DisplayName("Delayed demand should flush a fired timeout before synchronous upstream prefetch")
    public void testFiredTimeoutWinsOverLateSynchronousItems() {
        List<Runnable> scheduledTasks = new ArrayList<>();
        Scheduler manualScheduler = new Scheduler() {
            @Override
            public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
                scheduledTasks.add(task);
                return Disposables.single();
            }

            @Override
            public Disposable schedule(Runnable task) {
                task.run();
                return Disposables.disposed();
            }

            @Override
            public Worker createWorker() {
                return Schedulers.immediate().createWorker();
            }
        };
        AtomicInteger nextValue = new AtomicInteger(1);
        AtomicLong outstanding = new AtomicLong();
        AtomicBoolean emitOneOnRequest = new AtomicBoolean();
        AtomicReference<FluxSink<Integer>> sourceSink = new AtomicReference<>();
        Flux<Integer> source = Flux.create(sink -> {
            sourceSink.set(sink);
            sink.onRequest(n -> {
                outstanding.addAndGet(n);
                if (emitOneOnRequest.getAndSet(false)) {
                    Assertions.assertTrue(outstanding.getAndDecrement() > 0);
                    sink.next(nextValue.getAndIncrement());
                }
            });
        });

        StepVerifier.create(AdaptiveBatcher.buffer(
                source, 2, 8, Duration.ofSeconds(1), manualScheduler), 1)
            .then(() -> emitRequested(sourceSink.get(), outstanding, nextValue, 2))
            .expectNext(List.of(1, 2))
            .then(() -> emitRequested(sourceSink.get(), outstanding, nextValue, 3))
            .then(() -> {
                Assertions.assertEquals(2, scheduledTasks.size());
                scheduledTasks.get(1).run();
                emitOneOnRequest.set(true);
            })
            .thenRequest(1)
            .expectNext(List.of(3, 4, 5))
            .thenCancel()
            .verify();
    }

    @Timeout(10)
    @Test
    @DisplayName("Upstream demand should be replenished before delivering a full batch")
    public void testDemandIsReplenishedBeforeDownstreamDelivery() {
        AtomicLong requested = new AtomicLong();
        AtomicLong outstanding = new AtomicLong();
        AtomicReference<FluxSink<Integer>> sourceSink = new AtomicReference<>();
        Flux<Integer> source = Flux.create(sink -> {
            sourceSink.set(sink);
            sink.onRequest(n -> {
                requested.addAndGet(n);
                outstanding.addAndGet(n);
            });
        });

        StepVerifier.create(AdaptiveBatcher.buffer(source, 4, 8, Duration.ofMinutes(1)), 1)
            .then(() -> emitRequested(sourceSink.get(), outstanding, new AtomicInteger(1), 4))
            .assertNext(batch -> {
                Assertions.assertEquals(List.of(1, 2, 3, 4), batch);
                Assertions.assertEquals(12, requested.get(),
                    "the next bounded request must be visible before downstream processing starts");
            })
            .thenCancel()
            .verify();
    }

    @Test
    @Timeout(10)
    @DisplayName("An upstream error racing a timeout flush must not turn into completion")
    public void testConcurrentErrorWinsOverTimeoutCompletionPath() throws Exception {
        List<Runnable> scheduledTasks = new ArrayList<>();
        Scheduler manualScheduler = new Scheduler() {
            @Override
            public Disposable schedule(Runnable task, long delay, TimeUnit unit) {
                scheduledTasks.add(task);
                return Disposables.single();
            }

            @Override
            public Disposable schedule(Runnable task) {
                task.run();
                return Disposables.disposed();
            }

            @Override
            public Worker createWorker() {
                return Schedulers.immediate().createWorker();
            }
        };
        TestPublisher<Integer> publisher = TestPublisher.create();
        RuntimeException failure = new RuntimeException("boom");
        CountDownLatch deliveryStarted = new CountDownLatch(1);
        CountDownLatch releaseDelivery = new CountDownLatch(1);
        CountDownLatch terminated = new CountDownLatch(1);
        AtomicReference<Throwable> terminalError = new AtomicReference<>();
        AtomicBoolean completed = new AtomicBoolean();

        AdaptiveBatcher.buffer(publisher.flux(), 4, 8, Duration.ofSeconds(1), manualScheduler)
            .subscribe(new BaseSubscriber<>() {
                @Override
                protected void hookOnSubscribe(Subscription subscription) {
                    request(1);
                }

                @Override
                protected void hookOnNext(List<Integer> value) {
                    deliveryStarted.countDown();
                    try {
                        releaseDelivery.await();
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                        throw new AssertionError(exception);
                    }
                }

                @Override
                protected void hookOnError(Throwable throwable) {
                    terminalError.set(throwable);
                    terminated.countDown();
                }

                @Override
                protected void hookOnComplete() {
                    completed.set(true);
                    terminated.countDown();
                }
            });

        publisher.next(1);
        Assertions.assertEquals(1, scheduledTasks.size());
        Thread timeoutThread = Thread.startVirtualThread(scheduledTasks.getFirst());
        Assertions.assertTrue(deliveryStarted.await(1, TimeUnit.SECONDS));

        publisher.error(failure);
        releaseDelivery.countDown();

        Assertions.assertTrue(terminated.await(1, TimeUnit.SECONDS));
        timeoutThread.join(1_000);
        Assertions.assertFalse(timeoutThread.isAlive());
        Assertions.assertSame(failure, terminalError.get());
        Assertions.assertFalse(completed.get());
    }

    @Test
    @Timeout(10)
    @DisplayName("Completion should preserve a shrunken limit for delayed in-flight items")
    public void testCompletionSplitsOutstandingDemandAfterShrink() {
        AtomicInteger nextValue = new AtomicInteger(1);
        AtomicLong outstanding = new AtomicLong();
        AtomicReference<FluxSink<Integer>> sourceSink = new AtomicReference<>();
        Flux<Integer> source = Flux.create(sink -> {
            sourceSink.set(sink);
            sink.onRequest(outstanding::addAndGet);
        });

        StepVerifier.withVirtualTime(() ->
                AdaptiveBatcher.buffer(source, 4, 8, Duration.ofSeconds(1)),
                2
            )
            .then(() -> emitRequested(sourceSink.get(), outstanding, nextValue, 4))
            .expectNext(List.of(1, 2, 3, 4))
            .then(() -> emitRequested(sourceSink.get(), outstanding, nextValue, 1))
            .thenAwait(Duration.ofSeconds(1))
            .expectNext(List.of(5))
            .then(() -> emitRequested(sourceSink.get(), outstanding, nextValue, 7))
            .then(() -> sourceSink.get().complete())
            .thenRequest(1)
            .expectNext(List.of(6, 7, 8, 9))
            .thenRequest(1)
            .expectNext(List.of(10, 11, 12))
            .verifyComplete();
    }

    @Timeout(10)
    @Test
    @DisplayName("An empty source should complete without downstream demand")
    public void testEmptyCompletionIsNotBackpressured() {
        StepVerifier.create(
                AdaptiveBatcher.buffer(Flux.empty(), 4, 8, Duration.ofSeconds(1)),
                0
            )
            .expectSubscription()
            .verifyComplete();
    }

    @Timeout(10)
    @Test
    @DisplayName("An upstream error should terminate without downstream demand")
    public void testErrorIsNotBackpressured() {
        RuntimeException failure = new RuntimeException("boom");

        StepVerifier.create(
                AdaptiveBatcher.buffer(Flux.<Integer>error(failure), 4, 8, Duration.ofSeconds(1)),
                0
            )
            .expectSubscription()
            .expectErrorMatches(throwable -> throwable == failure)
            .verify();
    }

    @Timeout(10)
    @Test
    @DisplayName("Batch size should grow adaptively with a fast producer")
    public void testAdaptiveGrowth() {
        Flux<Integer> source = Flux.range(1, 100);

        StepVerifier.create(AdaptiveBatcher.buffer(source, 4, 32, Duration.ofMinutes(1)))
            .assertNext(batch -> Assertions.assertEquals(4, batch.size()))  // Start at 4. Next limit -> 8
            .assertNext(batch -> Assertions.assertEquals(8, batch.size()))  // Next limit -> 16
            .assertNext(batch -> Assertions.assertEquals(16, batch.size())) // Next limit -> 32
            .assertNext(batch -> Assertions.assertEquals(32, batch.size())) // At max. Next limit -> 32
            .assertNext(batch -> Assertions.assertEquals(32, batch.size())) // Stays at max
            .assertNext(batch -> Assertions.assertEquals(8, batch.size()))  // Remainder
            .verifyComplete();
    }

    @Timeout(10)
    @Test
    @DisplayName("Batch size should shrink after a timeout")
    public void testTimeoutAndShrink() {
        TestPublisher<Integer> publisher = TestPublisher.create();

        StepVerifier.withVirtualTime(() ->
                AdaptiveBatcher.buffer(publisher.flux(), 4, 32, Duration.ofSeconds(1))
            )
            .expectSubscription()
            .thenRequest(Long.MAX_VALUE)
            // 1. Fill a batch to grow the limit
            .then(() -> publisher.next(1, 2, 3, 4)) // Batch of 4. Limit -> 8
            .expectNext(List.of(1, 2, 3, 4))
            .then(() -> publisher.next(5, 6, 7)) // Emit 3 items, less than new limit of 8
            .thenAwait(Duration.ofSeconds(1))    // Trigger timeout
            .assertNext(batch -> {
                // The partial batch is flushed. Next limit should shrink from 8 -> 4
                Assertions.assertEquals(List.of(5, 6, 7), batch);
            })
            .then(() -> publisher.next(8, 9, 10, 11)) // Emit 4 more items
            .expectNext(List.of(8, 9, 10, 11)) // New batch is emitted at the shrunken size
            .then(publisher::complete)
            .verifyComplete();
    }

    @Timeout(10)
    @Test
    @DisplayName("Downstream backpressure should be respected")
    public void testBackpressure() {
        Flux<Integer> source = Flux.range(1, 100);

        StepVerifier.create(AdaptiveBatcher.buffer(source, 10, 100, Duration.ofMinutes(1)), 1)
            .assertNext(batch -> Assertions.assertEquals(10, batch.size())) // First batch, size 10. Next limit -> 20
            .thenRequest(1)
            .assertNext(batch -> Assertions.assertEquals(20, batch.size())) // Second batch, size 20. Next limit -> 40
            .thenCancel()
            .verify();
    }

    @Timeout(10)
    @Test
    @DisplayName("Cancellation should dispose the timeout timer")
    public void testCancellationCleansUpTimer() {
        AtomicReference<Disposable> timeoutTaskRef = new AtomicReference<>();
        TestPublisher<Integer> publisher = TestPublisher.create();

        // Custom scheduler to capture the disposable
        Scheduler testScheduler = Schedulers.newParallel("test");
        // A bit of a hack to observe the internal state, for testing purposes.
        Scheduler spyScheduler = new Scheduler() {
            @Override
            public Disposable schedule(Runnable task, long delay, java.util.concurrent.TimeUnit unit) {
                Disposable d = testScheduler.schedule(task, delay, unit);
                timeoutTaskRef.set(d);
                return d;
            }
            @Override public Disposable schedule(Runnable task) { return testScheduler.schedule(task); }
            @Override public Worker createWorker() { return testScheduler.createWorker(); }
        };

        StepVerifier.create(AdaptiveBatcher.buffer(publisher.flux(), 10, 20, Duration.ofMinutes(1), spyScheduler))
            .thenRequest(1)
            .then(() -> publisher.next(1)) // Emit one item to schedule a timer
            .then(() -> Assertions.assertNotNull(timeoutTaskRef.get()))
            .thenCancel()
            .verify();

        // After cancellation, the disposable for the timer should be disposed.
        Assertions.assertTrue(timeoutTaskRef.get().isDisposed());
        testScheduler.dispose();
    }

    @Timeout(10)
    @Test
    @DisplayName("Empty source should complete without emitting any batches")
    public void testEmptySource() {
        StepVerifier.create(AdaptiveBatcher.buffer(Flux.empty(), 5, 10, Duration.ofSeconds(1)))
            .verifyComplete();
    }

    @Timeout(10)
    @Test
    @DisplayName("Source with fewer items than minBatchSize emits one batch on completion")
    public void testSourceSmallerThanMinSizeOnComplete() {
        StepVerifier.create(AdaptiveBatcher.buffer(Flux.range(1, 3), 5, 10, Duration.ofSeconds(1)))
            .assertNext(batch -> Assertions.assertEquals(3, batch.size()))
            .verifyComplete();
    }

    @Timeout(10)
    @Test
    @DisplayName("Error from source should be propagated downstream immediately")
    public void testErrorPropagation() {
        RuntimeException testException = new RuntimeException("boom");
        Flux<Integer> source = Flux.concat(Flux.just(1, 2, 3), Flux.error(testException));

        StepVerifier.create(AdaptiveBatcher.buffer(source, 5, 10, Duration.ofSeconds(1)))
            .expectErrorMessage("boom")
            .verify();
    }

    @Timeout(10)
    @Test
    @DisplayName("Should work correctly with an asynchronous, concurrent producer (No Deadlock Test)")
    public void testConcurrentProducer() {
        // This test, which previously hung, should now pass reliably.
        Flux<Integer> source = Flux.range(1, 1000)
            .publishOn(Schedulers.parallel());

        StepVerifier.create(AdaptiveBatcher.buffer(source, 10, 100, Duration.ofMillis(50)))
            .thenRequest(Long.MAX_VALUE)
            .expectNextCount(7) // 10+20+40+80+100+100+...
            .thenConsumeWhile(batch -> !batch.isEmpty())
            .verifyComplete();
    }

    private static void emitRequested(FluxSink<Integer> sink,
            AtomicLong outstanding,
            AtomicInteger nextValue,
            int count) {
        for (int i = 0; i < count; i++) {
            Assertions.assertTrue(outstanding.get() > 0, "Source emitted without demand");
            sink.next(nextValue.getAndIncrement());
            outstanding.decrementAndGet();
        }
    }
}
