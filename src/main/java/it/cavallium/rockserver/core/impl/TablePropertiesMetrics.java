package it.cavallium.rockserver.core.impl;

import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.MultiGauge;
import io.micrometer.core.instrument.Tags;
import it.cavallium.rockserver.core.common.ColumnTableProperties;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.function.Supplier;
import reactor.core.Disposable;
import reactor.core.Disposables;
import reactor.core.publisher.Mono;

/** One non-overlapping background collection; scrapes only read the last completed observation. */
final class TablePropertiesMetrics implements AutoCloseable {
    private final Supplier<Mono<Map<String, ColumnTableProperties>>> collector;
    private final MultiGauge values;
    private final long intervalNanos;
    private Disposable.Swap subscription;
    private boolean running;
    private boolean closed;
    private boolean attempted;
    private long lastAttemptNanos;
    private volatile double success = Double.NaN;
    private volatile double lastSuccessSeconds = Double.NaN;

    TablePropertiesMetrics(String database, MeterRegistry registry, long intervalSeconds,
            Supplier<Mono<Map<String, ColumnTableProperties>>> collector) {
        if (intervalSeconds < 60) throw new IllegalArgumentException("table-properties-interval-seconds must be at least 60");
        this.intervalNanos = Duration.ofSeconds(intervalSeconds).toNanos();
        this.collector = collector;
        values = MultiGauge.builder("rocksdb.table.properties").tag("database", database).register(registry);
        Gauge.builder("rocksdb.table.properties.collection.success", () -> success)
                .tag("database", database).register(registry);
        Gauge.builder("rocksdb.table.properties.last.success.time", () -> lastSuccessSeconds)
                .baseUnit("seconds").tag("database", database).register(registry);
    }

    void refreshIfDue() {
        final Disposable.Swap pending;
        synchronized (this) {
            long now = System.nanoTime();
            if (closed || running || (attempted && now - lastAttemptNanos < intervalNanos)) return;
            running = true;
            attempted = true;
            lastAttemptNanos = now;
            pending = Disposables.swap();
            subscription = pending;
        }
        // A separate slot per attempt handles close-before-subscribe and synchronous completion.
        // Subscription and cancellation can invoke arbitrary callbacks; neither owns our monitor.
        pending.update(Mono.defer(() -> pending.isDisposed() ? Mono.empty() : collector.get())
                .subscribe(this::publish, this::failed));
    }

    private synchronized void publish(Map<String, ColumnTableProperties> snapshot) {
        if (closed) return;
        List<MultiGauge.Row<?>> rows = new ArrayList<>();
        snapshot.forEach((column, p) -> {
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "table.count"), p.tableCount()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "data.size"), p.dataSize()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "index.size"), p.indexSize()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "index.partitions"), p.indexPartitions()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "top.level.index.size"), p.topLevelIndexSize()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "filter.size"), p.filterSize()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "raw.key.size"), p.rawKeySize()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "raw.value.size"), p.rawValueSize()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "num.data.blocks"), p.numDataBlocks()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "num.entries"), p.numEntries()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "num.deletions"), p.numDeletions()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "num.merge.operands"), p.numMergeOperands()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "num.range.deletions"), p.numRangeDeletions()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "slow.compression.estimated.data.size"), p.slowCompressionEstimatedDataSize()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "fast.compression.estimated.data.size"), p.fastCompressionEstimatedDataSize()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "oldest.creation.time"), p.oldestCreationTime()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "newest.creation.time"), p.newestCreationTime()));
            rows.add(MultiGauge.Row.of(Tags.of("column_family", column, "property_name", "oldest.key.time"), p.oldestKeyTime()));
        });
        values.register(rows, true); // Removes deleted column-family series, including an empty snapshot.
        lastSuccessSeconds = System.currentTimeMillis() / 1000.0;
        success = 1;
        running = false;
    }

    private synchronized void failed(Throwable error) {
        if (closed) return;
        // A failure must not masquerade as an empty database or a fresh previous value.
        values.register(List.of(), true);
        success = 0;
        running = false;
        org.slf4j.LoggerFactory.getLogger(TablePropertiesMetrics.class)
                .warn("Failed to collect SST table properties", error);
    }

    @Override
    public void close() {
        final Disposable.Swap pending;
        synchronized (this) {
            if (closed) return;
            closed = true;
            pending = subscription;
            values.register(List.of(), true);
        }
        if (pending != null) pending.dispose();
    }
}
