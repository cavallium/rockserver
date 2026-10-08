package it.cavallium.rockserver.core.impl.rocksdb;

import io.micrometer.core.instrument.MeterRegistry;
import io.micrometer.core.instrument.Timer;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import org.rocksdb.AbstractEventListener;
import org.rocksdb.CompactionJobInfo;
import org.rocksdb.RocksDB;

/** Aggregate completed work only; never exports filenames or column names. */
final class CompactionMetrics extends AbstractEventListener {
    private final MeterRegistry registry;
    private final String database;
    private final ConcurrentHashMap<String, Timer> timers = new ConcurrentHashMap<>();
    CompactionMetrics(MeterRegistry registry, String database) {
        super(EnabledEventCallback.ON_COMPACTION_COMPLETED);
        this.registry = registry;
        this.database = database;
    }
    @Override public void onCompactionCompleted(RocksDB db, CompactionJobInfo info) {
        try {
            String reason = info.compactionReason().name();
            int level = info.outputLevel();
            var timer = timers.computeIfAbsent(reason + ':' + level, ignored ->
                    Timer.builder("rockserver.compaction.completed").tag("db", database)
                            .tag("reason", reason).tag("level", Integer.toString(level)).register(registry));
            try (var stats = info.stats()) { timer.record(stats.elapsedMicros(), TimeUnit.MICROSECONDS); }
        } catch (RuntimeException ignored) {
            // Telemetry must never fail RocksDB's background callback.
        }
    }
    @Override public void close() {
        super.close();
        timers.values().forEach(registry::remove);
    }
}
