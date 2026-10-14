package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.StartupIoPolicy;
import it.cavallium.rockserver.core.impl.StartupReadCompletions;
import it.cavallium.rockserver.core.impl.StartupWriteCompletions;

import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeStartupBridge;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import jdk.jfr.Recording;
import jdk.jfr.consumer.RecordedEvent;
import jdk.jfr.consumer.RecordingFile;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.condition.EnabledIfSystemProperty;
import org.junit.jupiter.api.io.TempDir;

@EnabledIfSystemProperty(named = "rockserver.startup.native.tests", matches = "true")
class StartupIoJfrTest {
    @TempDir Path directory;
    private List<RecordedEvent> events(Recording recording, String name) throws Exception {
        recording.stop();
        Path file = directory.resolve(name + ".jfr"); recording.dump(file);
        return RecordingFile.readAllEvents(file).stream().filter(e -> e.getEventType().getName().startsWith("rockserver.")).toList();
    }
    private void puts(Path db) throws Exception {
        try (var connection = new EmbeddedConnection(db, "jfr-put", null)) {
            var api = connection.getSyncApi(RequestContext.batch());
            long column = api.createColumn("column", ColumnSchema.of(IntList.of(1), ObjectList.of(), true));
            var key = new Keys(Buf.wrap(new byte[]{1})); var value = Buf.wrap(new byte[]{7});
            api.put(0, column, key, value, RequestType.none());
            assertEquals(value, api.get(0, column, key, RequestType.current()));
            assertThrows(RocksDBException.class, () -> api.put(0, -1, key, value, RequestType.none()));
        }
    }
    @Test void putEventsAreDisabledByDefaultAndCoverFeatureOffServiceAndNativeExecution() throws Exception {
        puts(directory.resolve("no-recording-db"));
        try (var recording = new Recording()) {
            recording.start(); puts(directory.resolve("disabled-db"));
            assertTrue(events(recording, "disabled").isEmpty());
        }
        try (var recording = new Recording()) {
            recording.enable("rockserver.ServicePut").withThreshold(Duration.ZERO);
            recording.enable("rockserver.NativePut").withThreshold(Duration.ZERO);
            recording.start(); puts(directory.resolve("enabled-db"));
            var events = events(recording, "enabled");
            var service = events.stream().filter(e -> e.getEventType().getName().equals("rockserver.ServicePut")).toList();
            var nativeCalls = events.stream().filter(e -> e.getEventType().getName().equals("rockserver.NativePut")).toList();
            assertEquals(2, service.size());
            assertEquals(1, service.stream().filter(e -> e.getBoolean("failed")).count());
            assertEquals(2, nativeCalls.size(), "metadata and foreground Put attempts");
            assertTrue(nativeCalls.stream().noneMatch(e -> e.getBoolean("failed")));
            var successfulService = service.stream().filter(e -> !e.getBoolean("failed")).findFirst().orElseThrow();
            assertTrue(nativeCalls.stream().anyMatch(e -> !e.getStartTime().isBefore(successfulService.getStartTime())
                    && !e.getEndTime().isAfter(successfulService.getEndTime())));
        }
    }
    @Test void actualControllerPublishesOneDrainedWindowAndStorageMaskWithoutChangingDecision() throws Exception {
        var reads = new StartupReadCompletions(); var writes = new StartupWriteCompletions();
        reads.recordPoint(100, false); writes.record(200, false);
        var sampled = new CountDownLatch(1);
        var registry = new SimpleMeterRegistry();
        try (var recording = new Recording();
             var budget = NativeStartupBridge.createBudget(new NativeStartupBridge.BudgetConfig("jfr-storage", 1, 100, 2000))) {
            recording.enable("rockserver.StartupIoPolicy"); recording.enable("rockserver.StartupIoStorageFeedback"); recording.start();
            var type = Class.forName("it.cavallium.rockserver.core.impl.StartupIoController");
            var constructor = type.getDeclaredConstructors()[0]; constructor.setAccessible(true);
            java.util.function.LongFunction<Map<String, StartupIoPolicy.InputSample>> sampler = epoch -> {
                        var input = new StartupIoPolicy.InputSample(epoch, System.nanoTime(), true, true,
                                1, 0, 0, 1, 1, false, false, reads.snapshot(epoch), writes.snapshot(epoch));
                        sampled.countDown(); return Map.of("workload", input);
                    };
            try (var controller = (AutoCloseable) constructor.newInstance("jfr-controller", Map.of("storage", budget),
                    Map.of("storage", Set.of("workload")), Map.of(), Map.of(), Map.of(), sampler, registry)) {
                assertTrue(sampled.await(5, TimeUnit.SECONDS));
            }
            var events = events(recording, "controller");
            var policy = events.stream().filter(e -> e.getEventType().getName().equals("rockserver.StartupIoPolicy")).findFirst().orElseThrow();
            assertEquals(1, policy.getLong("pointCount")); assertEquals(100, policy.getLong("pointNanos"));
            assertEquals(1, policy.getLong("writeCount")); assertEquals(200, policy.getLong("writeNanos"));
            assertEquals("calibrating", policy.getString("reason")); assertTrue(policy.getBoolean("conserved"));
            assertEquals(1, policy.getInt("pointWindows")); assertEquals(100, policy.getDouble("pointMeanNanos"));
            assertFalse(policy.getBoolean("writeReady"));
            var published = events.stream().filter(e -> e.getEventType().getName().equals("rockserver.StartupIoStorageFeedback")).findFirst().orElseThrow();
            assertEquals(policy.getLong("epoch"), published.getLong("epoch"));
            assertEquals(policy.getInt("pressureMask"), published.getInt("mask")); assertFalse(published.getBoolean("fallback"));
            assertEquals(1, reads.snapshot(policy.getLong("epoch")).point().count(), "recording must not drain counters twice");
            assertEquals(1, writes.snapshot(policy.getLong("epoch")).calls().count());
        } finally {
            registry.close();
        }
    }
    @Test void recordedPolicyPreservesDecisionsAndReportsActualSlowResetAndStaleBranches() throws Exception {
        var recorded = new StartupIoPolicy(); var ordinary = new StartupIoPolicy();
        var method = StartupIoPolicy.class.getDeclaredMethod("sampleRecorded", String.class, String.class, long.class,
                StartupIoPolicy.InputSample.class, long.class); method.setAccessible(true);
        var reads = new StartupReadCompletions(); var writes = new StartupWriteCompletions();
        try (var recording = new Recording()) {
            // Register before enabling by name, without requiring any running recording.
            Class.forName("it.cavallium.rockserver.core.impl.StartupIoJfr");
            recording.enable("rockserver.StartupIoPolicy"); recording.start();
            for (long epoch = 1; epoch <= 6; epoch++) {
                if (epoch == 5) writes.reset();
                reads.recordPoint(100, false); writes.record(epoch == 4 ? 1000 : 100, false);
                long sampled = epoch * 100_000_000;
                var input = new StartupIoPolicy.InputSample(epoch, sampled, true, true, 1, 0, 0, 1, 1,
                        false, false, reads.snapshot(epoch), writes.snapshot(epoch));
                long now = sampled + (epoch == 6 ? StartupIoPolicy.MAX_SAMPLE_AGE_NANOS + 1 : 0);
                assertEquals(ordinary.sample(input, now), method.invoke(recorded, "policy-test", "workload", epoch, input, now));
            }
            assertEquals(ordinary.sample(null, 700_000_000),
                    method.invoke(recorded, "policy-test", "workload", 7L, null, 700_000_000L));
            var events = events(recording, "policy-branches"); assertEquals(7, events.size());
            var missing = events.stream().filter(e -> e.getLong("epoch") == 7).findFirst().orElseThrow();
            assertFalse(missing.getBoolean("inputPresent")); assertEquals("missing_input", missing.getString("reason"));
            var slow = events.stream().filter(e -> e.getLong("epoch") == 4).findFirst().orElseThrow();
            assertEquals("failure_queue_or_slow_cohort", slow.getString("reason"));
            assertTrue(slow.getBoolean("writeSlow")); assertFalse(slow.getBoolean("pointSlow"));
            assertEquals(100, slow.getDouble("writeMeanNanos")); assertEquals(100, slow.getDouble("writeBaselineMaxNanos"));
            assertTrue(events.stream().filter(e -> e.getLong("epoch") == 5).findFirst().orElseThrow().getBoolean("reset"));
            var stale = events.stream().filter(e -> e.getLong("epoch") == 6).findFirst().orElseThrow();
            assertFalse(stale.getBoolean("fresh")); assertFalse(stale.getBoolean("conservationChecked"));
            assertEquals("invalid_window", stale.getString("reason"));
        }
    }
}
