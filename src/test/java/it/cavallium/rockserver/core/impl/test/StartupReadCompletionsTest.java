package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.StartupReadCompletions;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class StartupReadCompletionsTest {
    @Test void sharesTheEpochAndSeparatesCohortsAndResetGenerations() {
        var counters = new StartupReadCompletions();
        counters.recordPoint(40, false); counters.recordPoint(80, true);
        counters.recordBulk(250, 12, false);
        var first = counters.snapshot(1);
        assertEquals(new StartupReadCompletions.Cohort(2, 120, 80, 2, 1), first.point());
        assertEquals(new StartupReadCompletions.Cohort(1, 250, 250, 12, 0), first.bulk());
        counters.recordPoint(100, false);
        assertSame(first, counters.snapshot(1));
        var second = counters.snapshot(2);
        assertEquals(3, second.pointCompleted());
        assertEquals(new StartupReadCompletions.Cohort(1, 100, 100, 1, 0), second.point());
        assertEquals(0, counters.snapshot(3).point().tailNanos());
        assertThrows(IllegalArgumentException.class, () -> counters.snapshot(2));
        counters.reset();
        var reset = counters.snapshot(4);
        assertEquals(1, reset.generation()); assertEquals(0, reset.pointCompleted());
    }

    @Test void concurrentPublishersAndSamplerConserveCompleteCohorts() throws Exception {
        var counters = new StartupReadCompletions();
        int workers = 8, calls = 10_000;
        var start = new CountDownLatch(1); var finished = new CountDownLatch(workers);
        try (var executor = Executors.newFixedThreadPool(workers)) {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < workers; i++) futures.add(executor.submit(() -> {
                try {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    for (int j = 0; j < calls; j++) {
                        counters.recordPoint(7, j % 2 == 0);
                        counters.recordBulk(11, 3, j % 2 == 0);
                    }
                } catch (InterruptedException e) { throw new AssertionError(e); }
                finally { finished.countDown(); }
            }));
            start.countDown();
            long point = 0, bulk = 0, pointFailures = 0, bulkFailures = 0, epoch = 0;
            do {
                var snapshot = counters.snapshot(epoch++);
                assertTrue(snapshot.valid());
                var p = snapshot.point(); var b = snapshot.bulk();
                assertEquals(p.count() * 7, p.nanos()); assertEquals(b.count() * 11, b.nanos());
                assertEquals(b.count() * 3, b.keys());
                assertEquals(p.count() == 0 ? 0 : 7, p.tailNanos());
                assertEquals(b.count() == 0 ? 0 : 11, b.tailNanos());
                point += p.count(); bulk += b.count();
                pointFailures += p.failures(); bulkFailures += b.failures();
            } while (finished.getCount() != 0);
            for (var future : futures) future.get(5, TimeUnit.SECONDS);
            var last = counters.snapshot(epoch);
            point += last.point().count(); bulk += last.bulk().count();
            assertEquals((long) workers * calls, point); assertEquals(point, bulk);
            assertEquals(point, last.pointCompleted()); assertEquals(bulk, last.bulkCompleted());
            pointFailures += last.point().failures(); bulkFailures += last.bulk().failures();
            assertEquals(point / 2, pointFailures); assertEquals(bulk / 2, bulkFailures);
        }
    }

    @Test void overflowInvalidatesUntilExplicitReset() {
        var counters = new StartupReadCompletions();
        counters.recordPoint(Long.MAX_VALUE, false); counters.recordPoint(1, false);
        assertFalse(counters.snapshot(1).valid()); assertFalse(counters.snapshot(2).valid());
        counters.reset(); assertTrue(counters.snapshot(3).valid());
    }
}
