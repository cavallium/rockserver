package it.cavallium.rockserver.core.impl.test;

import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import java.util.ArrayList;
import java.util.List;
import reactor.core.scheduler.Schedulers;
import org.reactivestreams.Subscriber;
import org.reactivestreams.Subscription;
import java.util.concurrent.atomic.AtomicInteger;
import static org.junit.jupiter.api.Assertions.*;

public class TestReactiveLeak {
    @Test
    public void test() throws Exception {
        AtomicInteger closedCount = new AtomicInteger();
        AtomicInteger createdCount = new AtomicInteger();

        Flux<Integer> source = Flux.range(1, 100).publishOn(Schedulers.boundedElastic());
        
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
                    closedCount.incrementAndGet();
                }
                @Override
                public void onComplete() {
                    closedCount.incrementAndGet();
                }
            });
        }
        
        Thread.sleep(1000);
        System.out.println("Created: " + createdCount.get() + ", Closed: " + closedCount.get());
        assertEquals(createdCount.get(), closedCount.get());
    }
}
