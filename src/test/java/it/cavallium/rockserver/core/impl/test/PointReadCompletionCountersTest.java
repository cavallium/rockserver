package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.EmbeddedDB;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import static org.junit.jupiter.api.Assertions.*;

@Timeout(20)
class PointReadCompletionCountersTest {
    private record Snapshot(long count, long nanos, long tailNanos) {}

    // The test module uses reflection rather than introducing a public telemetry API.
    private static final class Counters {
        final Object monitor;
        final java.lang.reflect.Method record, sample, count, nanos, tail;
        Counters() throws Exception {
            var type = Class.forName(EmbeddedDB.class.getName() + "$PointReadCompletionCounters");
            var constructor = type.getDeclaredConstructor(); constructor.setAccessible(true);
            monitor = constructor.newInstance();
            record = type.getDeclaredMethod("record", long.class); record.setAccessible(true);
            sample = type.getDeclaredMethod("sample"); sample.setAccessible(true);
            var snapshot = sample.getReturnType();
            count = snapshot.getDeclaredMethod("count"); count.setAccessible(true);
            nanos = snapshot.getDeclaredMethod("nanos"); nanos.setAccessible(true);
            tail = snapshot.getDeclaredMethod("tailNanos"); tail.setAccessible(true);
        }
        Object stripeFor(Thread thread) throws Exception {
            var field = monitor.getClass().getDeclaredField("stripes"); field.setAccessible(true);
            var stripes = (Object[]) field.get(monitor);
            return stripes[(int) thread.threadId() & (stripes.length - 1)];
        }
        void record(long elapsed) {
            try { record.invoke(monitor, elapsed); }
            catch (ReflectiveOperationException failure) { throw new AssertionError(failure); }
        }
        Snapshot sample() throws Exception {
            var result = sample.invoke(monitor);
            return new Snapshot((long) count.invoke(result), (long) nanos.invoke(result), (long) tail.invoke(result));
        }
    }

    @Test void splitPublicationCanExposeUncountedTimeEvenWithCountBookends() throws Exception {
        // Source-equivalent old POINT ordering; barriers force a publisher paused before count++.
        var count = new AtomicLong(32);
        var nanos = new AtomicLong(32_000_000);
        var elapsedPublished = new CountDownLatch(1);
        var finishPublication = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            var publisher = executor.submit(() -> {
                nanos.addAndGet(1_000_000_000);
                elapsedPublished.countDown();
                try { assertTrue(finishPublication.await(5, TimeUnit.SECONDS)); }
                catch (InterruptedException failure) { throw new AssertionError(failure); }
                count.incrementAndGet();
            });
            try {
                assertTrue(elapsedPublished.await(5, TimeUnit.SECONDS));
                long before = count.get(), elapsed = nanos.get(), after = count.get();
                assertEquals(before, after, "count bookends do not detect an in-progress publisher");
                assertEquals(32, before);
                assertEquals(1_032_000_000, elapsed);
                assertTrue(elapsed / before > 30_000_000,
                        "uncounted time inflates the mean of the 32 completed 1ms calls");
            } finally { finishPublication.countDown(); }
            publisher.get(5, TimeUnit.SECONDS);
            assertEquals(33, count.get());
            assertEquals(1_032_000_000, nanos.get());
        }
    }

    @Test void blockedPublicationRemainsEntirelyInTheNextTailCohort() throws Exception {
        var counters = new Counters();
        counters.record(700);
        var attempting = new CountDownLatch(1);
        try (var executor = Executors.newSingleThreadExecutor()) {
            java.util.concurrent.Future<?> publisher;
            var publisherThread = executor.submit(Thread::currentThread).get(5, TimeUnit.SECONDS);
            synchronized (counters.stripeFor(publisherThread)) {
                publisher = executor.submit(() -> { attempting.countDown(); counters.record(1700); });
                assertTrue(attempting.await(5, TimeUnit.SECONDS));
                assertEquals(new Snapshot(1, 700, 700), counters.sample());
                assertFalse(publisher.isDone());
                assertEquals(new Snapshot(1, 700, 0), counters.sample());
            }
            publisher.get(5, TimeUnit.SECONDS);
            assertEquals(new Snapshot(2, 2400, 1700), counters.sample());
            assertEquals(new Snapshot(2, 2400, 0), counters.sample());
        }
    }

    @Test void anotherStripePublishesWhileOnePublisherWaitsAndBothTailsDrainOnce() throws Exception {
        var counters = new Counters();
        var attempting = new CountDownLatch(1);
        var first = new Thread(() -> { attempting.countDown(); counters.record(1700); });
        Thread second;
        do { second = new Thread(() -> counters.record(2900)); }
        while (counters.stripeFor(first) == counters.stripeFor(second));
        synchronized (counters.stripeFor(first)) {
            first.start();
            assertTrue(attempting.await(5, TimeUnit.SECONDS));
            second.start(); second.join(5000);
            assertFalse(second.isAlive(), "a different stripe must publish without the blocked stripe");
            assertEquals(new Snapshot(1, 2900, 2900), counters.sample());
            assertEquals(new Snapshot(1, 2900, 0), counters.sample());
        }
        first.join(5000); assertFalse(first.isAlive());
        assertEquals(new Snapshot(2, 4600, 1700), counters.sample());
        assertEquals(new Snapshot(2, 4600, 0), counters.sample());
    }

    @Test void concurrentPublishersAndRepeatedSamplesRetainCompletedCohorts() throws Exception {
        var counters = new Counters();
        int publishers = 4, calls = 100_000;
        long elapsed = 1234;
        var start = new CountDownLatch(1);
        var finished = new CountDownLatch(publishers);
        try (var executor = Executors.newFixedThreadPool(publishers)) {
            var futures = new java.util.ArrayList<java.util.concurrent.Future<?>>();
            for (int i = 0; i < publishers; i++) futures.add(executor.submit(() -> {
                try {
                    assertTrue(start.await(5, TimeUnit.SECONDS));
                    for (int j = 0; j < calls; j++) counters.record(elapsed);
                } catch (InterruptedException failure) { throw new AssertionError(failure); }
                finally { finished.countDown(); }
            }));
            long previous = 0;
            start.countDown();
            do {
                var sample = counters.sample();
                assertEquals(sample.count() * elapsed, sample.nanos());
                assertTrue(sample.count() >= previous);
                assertEquals(sample.count() == previous ? 0 : elapsed, sample.tailNanos());
                previous = sample.count();
            } while (finished.getCount() != 0);
            for (var future : futures) future.get(5, TimeUnit.SECONDS);
            var last = counters.sample();
            assertEquals((long) publishers * calls, last.count());
            assertEquals(last.count() * elapsed, last.nanos());
            assertEquals(last.count() == previous ? 0 : elapsed, last.tailNanos());
            assertEquals(0, counters.sample().tailNanos());
        }
    }

    @Test void zeroDurationCompletionsStillCountAndQuietSamplesOnlyDrainTail() throws Exception {
        var counters = new Counters();
        counters.record(0);
        counters.record(900);
        counters.record(300);
        assertEquals(new Snapshot(3, 1200, 900), counters.sample());
        assertEquals(new Snapshot(3, 1200, 0), counters.sample());
    }
}
