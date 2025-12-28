package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.AdaptiveBatcher;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.test.publisher.TestPublisher;

public class AdaptiveBatcherTest {

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
}