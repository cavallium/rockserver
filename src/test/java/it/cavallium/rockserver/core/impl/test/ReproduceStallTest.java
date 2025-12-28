package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.AdaptiveBatcher;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;
import reactor.core.scheduler.Schedulers;

public class ReproduceStallTest {

    @Timeout(30)
    @Test
    public void testStallWithBufferTimeout() throws InterruptedException {
        // Simulate structure: Flux.interval -> bufferTimeout -> AdaptiveBatcher
        // Source produces 100k items.
        // bufferTimeout batches them into chunks of 10.
        // AdaptiveBatcher batches those chunks.
        
        int totalItems = 100_000;
        Flux<List<Integer>> source = Flux.interval(Duration.ofNanos(100_000)) // 10k items/sec
                .take(totalItems)
                .map(Long::intValue)
                .bufferTimeout(10, Duration.ofMillis(50));

        AtomicInteger receivedItemsCount = new AtomicInteger();
        CountDownLatch done = new CountDownLatch(1);

        AdaptiveBatcher.buffer(source, 5, 20, Duration.ofMillis(10), Schedulers.parallel())
                .subscribe(new reactor.core.publisher.BaseSubscriber<List<List<Integer>>>() {
                    @Override
                    protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) {
                        request(1);
                    }

                    @Override
                    protected void hookOnNext(List<List<Integer>> batchOfBatches) {
                        for (List<Integer> batch : batchOfBatches) {
                            receivedItemsCount.addAndGet(batch.size());
                        }
                        // Simulate slight processing delay
                        try {
                            Thread.sleep(0, 100); // 100ns
                        } catch (InterruptedException e) {
                        }
                        request(1);
                    }

                    @Override
                    protected void hookOnComplete() {
                        done.countDown();
                    }

                    @Override
                    protected void hookOnError(Throwable throwable) {
                        throwable.printStackTrace();
                        done.countDown();
                    }
                });

        boolean finished = done.await(20, TimeUnit.SECONDS);
        Assertions.assertTrue(finished, "Stream stalled! Received so far: " + receivedItemsCount.get());
        Assertions.assertEquals(totalItems, receivedItemsCount.get());
    }
}
