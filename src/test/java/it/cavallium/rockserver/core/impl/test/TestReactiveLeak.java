package it.cavallium.rockserver.core.impl.test;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.SignalType;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

public class TestReactiveLeak {
    @Test
    public void test() throws Exception {
        AtomicInteger createdCount = new AtomicInteger();
        AtomicInteger cancelledCount = new AtomicInteger();
        CountDownLatch cancelled = new CountDownLatch(10);
        Scheduler scheduler = Schedulers.newBoundedElastic(1, 100, "reactive-leak-test");

        try {
            Flux<Integer> source = Flux.range(1, 100)
                    .publishOn(scheduler)
                    .doFinally(signal -> {
                        if (signal == SignalType.CANCEL) {
                            cancelledCount.incrementAndGet();
                            cancelled.countDown();
                        }
                    });
        
            for (int i = 0; i < 10; i++) {
                source.subscribe(new Subscriber<>() {
                    private Subscription s;
                    @Override
                    public void onSubscribe(Subscription s) {
                        createdCount.incrementAndGet();
                        this.s = s;
                        s.request(1);
                        s.cancel();
                    }
                    @Override
                    public void onNext(Integer integer) {
                        // doing nothing
                    }
                    @Override
                    public void onError(Throwable t) {
                    }
                    @Override
                    public void onComplete() {
                    }
                });
            }
        
            assertTrue(cancelled.await(5, TimeUnit.SECONDS), "All cancelled subscriptions should run doFinally cleanup");
            assertEquals(createdCount.get(), cancelledCount.get());
        } finally {
            scheduler.dispose();
        }
    }
}
