package it.cavallium.rockserver.core.impl;

import jdk.jfr.Category;
import jdk.jfr.Enabled;
import jdk.jfr.Description;
import jdk.jfr.Event;
import jdk.jfr.EventType;
import jdk.jfr.Label;
import jdk.jfr.Name;
import jdk.jfr.StackTrace;

/** Optional diagnostic recordings; durations exclude public queue/future residence. No request payloads. */
final class StartupIoJfr {
    private StartupIoJfr() {}
    @Name("rockserver.StartupIoPolicy") @Label("Startup I/O workload epoch")
    @Category("Rockserver") @Enabled(false) @StackTrace(false)
    static final class Policy extends Event {
        public String database, workload, reason;
        public long epoch, inputEpoch, sampleAgeNanos;
        public boolean inputPresent;
        public boolean telemetryValid, complete, fresh, reset, structurallyValid, conservationChecked, conserved;
        public boolean readsPresent, writesPresent, readValid, writeValid, windowEvaluated, windowValid, pointSlow, bulkSlow, writeSlow;
        public int workers, active, queued, activeWrites;
        public long accepted, terminal;
        public boolean writeOutstanding, urgentRecovery, recovering;
        public int recoveryWindows, pressureMask;
        public boolean capturePressure, qualifiedHealthy, idleNeutral;
        public long pointCount, pointNanos, pointMaxNanos, pointFailures, bulkCount, bulkNanos, bulkMaxNanos, bulkFailures, writeCount, writeNanos, writeMaxNanos, writeFailures;
        public long readGeneration, writeGeneration, pointCompleted, bulkCompleted, writeCompleted;
        public boolean pointReady, bulkReady, writeReady;
        public int pointWindows, bulkWindows, writeWindows;
        public double pointMeanNanos, pointBaselineMaxNanos, bulkMeanNanos, bulkBaselineMaxNanos, writeMeanNanos, writeBaselineMaxNanos;
    }
    @Name("rockserver.StartupIoStorageFeedback") @Label("Published startup storage feedback")
    @Category("Rockserver") @Enabled(false) @StackTrace(false)
    static final class Storage extends Event {
        public String database, storage;
        public long epoch;
        @Description("This controller mask after successful feedback publication; the shared physical aggregate may differ")
        public int mask;
        public boolean fallback;
    }
    @Name("rockserver.ServicePut") @Label("Synchronous service Put execution")
    @Category("Rockserver") @Enabled(false) @StackTrace(false)
    static final class ServicePut extends Event { public boolean failed; }
    @Name("rockserver.NativePut") @Label("Native RocksDB Put attempt")
    @Category("Rockserver") @Enabled(false) @StackTrace(false)
    static final class NativePut extends Event { public boolean failed; }
    private static final EventType POLICY = EventType.getEventType(Policy.class);
    private static final EventType STORAGE = EventType.getEventType(Storage.class);
    private static final EventType SERVICE_PUT = EventType.getEventType(ServicePut.class);
    private static final EventType NATIVE_PUT = EventType.getEventType(NativePut.class);
    static Policy policy() { return POLICY.isEnabled() ? new Policy() : null; }
    static ServicePut servicePut() {
        if (!SERVICE_PUT.isEnabled()) return null;
        var event = new ServicePut(); event.begin(); return event;
    }
    static NativePut nativePut() {
        if (!NATIVE_PUT.isEnabled()) return null;
        return new NativePut();
    }
    static void storage(String database, String storage, long epoch, int mask, boolean fallback) {
        if (!STORAGE.isEnabled()) return;
        var event = new Storage(); event.database = database; event.storage = storage;
        event.epoch = epoch; event.mask = mask; event.fallback = fallback; event.commit();
    }
}
