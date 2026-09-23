package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.mock;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.common.ColumnTableProperties;
import java.lang.reflect.Method;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Supplier;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.core.publisher.Sinks;

class TablePropertiesMetricsTest {
    private static final String VALUES = "rocksdb.table.properties";

    @Test
    void cachesScrapesPreventsOverlapRemovesDeletedColumnsAndSignalsFailure() throws Exception {
        var registry = new SimpleMeterRegistry();
        try (AutoCloseable registryClose = registry::close) {
            var source = new AtomicReference<Sinks.One<Map<String, ColumnTableProperties>>>(Sinks.one());
            var calls = new AtomicInteger();
            try (var metrics = create(registry, "test", () -> { calls.incrementAndGet(); return source.get().asMono(); })) {
                refresh(metrics);
                refresh(metrics);
                assertEquals(1, calls.get(), "one collection at a time");
                var empty = mock(ColumnTableProperties.class);
                source.get().tryEmitValue(Map.of("column", empty));
                assertEquals(1, registry.get(VALUES + ".collection.success").gauge().value());
                assertEquals(0, registry.get(VALUES).tag("property_name", "num.entries").gauge().value());
                registry.getMeters().forEach(m -> m.measure().forEach(_ -> {}));
                refresh(metrics);
                assertEquals(1, calls.get(), "scrapes and early refreshes must not collect");
                source.set(Sinks.one());
                makeDue(metrics);
                refresh(metrics);
                source.get().tryEmitValue(Map.of());
                assertTrue(registry.find(VALUES).gauges().isEmpty(), "deleted columns disappear");
                source.set(Sinks.one());
                makeDue(metrics);
                refresh(metrics);
                source.get().tryEmitError(new IllegalStateException("fixture"));
                assertEquals(0, registry.get(VALUES + ".collection.success").gauge().value());
                assertTrue(registry.get(VALUES + ".last.success.time").gauge().value() > 0);
            }
        }
    }

    @Test
    void closeCancelsPendingCollectionAndPreventsLaterPublication() throws Exception {
        var registry = new SimpleMeterRegistry();
        try (AutoCloseable registryClose = registry::close) {
            var cancelled = new AtomicInteger();
            var source = Sinks.<Map<String, ColumnTableProperties>>one();
            var metrics = create(registry, "test", () -> source.asMono().doOnCancel(cancelled::incrementAndGet));
            refresh(metrics);
            metrics.close();
            metrics.close();
            assertEquals(1, cancelled.get());
            source.tryEmitValue(Map.of("late", mock(ColumnTableProperties.class)));
            refresh(metrics);
            assertTrue(registry.getMeters().isEmpty(), "close removes values and health gauges");
        }
    }

    @Test
    @org.junit.jupiter.api.Timeout(10)
    void closeDoesNotWaitForSubscriptionCallbacksAndCancelsLateSubscription() throws Exception {
        var registry = new SimpleMeterRegistry();
        var entered = new java.util.concurrent.CountDownLatch(1);
        var release = new java.util.concurrent.CountDownLatch(1);
        var cancelled = new AtomicInteger();
        try (AutoCloseable registryClose = registry::close;
             var executor = java.util.concurrent.Executors.newFixedThreadPool(2);
             var metrics = create(registry, "test", () -> {
                 entered.countDown();
                 try {
                     if (!release.await(5, java.util.concurrent.TimeUnit.SECONDS)) throw new AssertionError("subscription not released");
                 } catch (InterruptedException e) { throw new AssertionError(e); }
                 return Mono.<Map<String, ColumnTableProperties>>never().doOnCancel(cancelled::incrementAndGet);
             })) {
            var subscribing = executor.submit(() -> { refresh(metrics); return null; });
            try {
                assertTrue(entered.await(5, java.util.concurrent.TimeUnit.SECONDS));
                executor.submit(() -> { metrics.close(); return null; }).get(1, java.util.concurrent.TimeUnit.SECONDS);
            } finally { release.countDown(); }
            subscribing.get(5, java.util.concurrent.TimeUnit.SECONDS);
            assertEquals(1, cancelled.get(), "late subscription must be cancelled by its disposed slot");
            assertTrue(registry.getMeters().isEmpty(), "close removes values and health gauges");
        }
    }

    @Test
    void closeRemovesOnlyItsOwnMetersAndIsIdempotent() throws Exception {
        var registry = new SimpleMeterRegistry();
        try (AutoCloseable registryClose = registry::close;
             var other = create(registry, "other", () -> Mono.just(Map.of("column", mock(ColumnTableProperties.class))))) {
            var unrelated = registry.counter("unrelated");
            refresh(other);
            var remaining = java.util.List.copyOf(registry.getMeters());
            try (var metrics = create(registry, "test", () -> Mono.just(Map.of("column", mock(ColumnTableProperties.class))))) {
                refresh(metrics);
                assertEquals(remaining.size() + 20, registry.getMeters().size());
                registry.config().onMeterRemoved(meter -> assertFalse(Thread.holdsLock(metrics),
                        "registry callbacks must run outside the collector monitor"));
                metrics.close();
                metrics.close();
                assertEquals(java.util.Set.copyOf(remaining), java.util.Set.copyOf(registry.getMeters()));
                assertSame(unrelated, registry.get("unrelated").counter());
                assertEquals(1, registry.get(VALUES + ".collection.success").tag("database", "other").gauge().value());
            }
        }
    }

    private static AutoCloseable create(MeterRegistry registry, String database, Supplier<Mono<Map<String, ColumnTableProperties>>> source) throws Exception {
        var c = Class.forName("it.cavallium.rockserver.core.impl.TablePropertiesMetrics")
                .getDeclaredConstructor(String.class, MeterRegistry.class, long.class, Supplier.class);
        c.setAccessible(true);
        return (AutoCloseable) c.newInstance(database, registry, 60L, source);
    }
    private static void refresh(AutoCloseable metrics) throws Exception {
        Method m = metrics.getClass().getDeclaredMethod("refreshIfDue");
        m.setAccessible(true);
        m.invoke(metrics);
    }
    private static void makeDue(AutoCloseable metrics) throws Exception {
        var field = metrics.getClass().getDeclaredField("lastAttemptNanos");
        field.setAccessible(true);
        field.setLong(metrics, System.nanoTime() - java.time.Duration.ofMinutes(2).toNanos());
    }
}
