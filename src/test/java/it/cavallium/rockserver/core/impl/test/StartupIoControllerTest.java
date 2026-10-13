package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.*;

import it.cavallium.rockserver.core.impl.StartupIoPolicy;
import it.cavallium.rockserver.core.impl.StartupReadCompletions;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Pure Java aggregation boundary: no experimental native ABI or scheduler-thread timing. */
class StartupIoControllerTest {
    private static final class Owner {
        final StartupIoPolicy policy = new StartupIoPolicy();
        final StartupReadCompletions reads = new StartupReadCompletions();
        long epoch;
        StartupIoPolicy.InputSample input(int active, boolean complete, boolean urgent) {
            epoch++;
            return new StartupIoPolicy.InputSample(epoch, epoch * 100_000_000, true, complete,
                    2, active, 0, 100, active == 0 ? 100 : 99, urgent, reads.snapshot(epoch));
        }
        StartupIoPolicy.Decision sample(int active, boolean complete, boolean urgent) {
            var sample = input(active, complete, urgent);
            return policy.sample(sample, sample.sampledNanos());
        }
        StartupIoPolicy.Decision idle() { return sample(0, true, false); }
        StartupIoPolicy.Decision healthy() {
            StartupIoPolicy.Decision result = null;
            for (int i = 0; i < StartupIoPolicy.CALIBRATION_WINDOWS; i++) {
                reads.recordPoint(100, false);
                result = sample(0, true, false);
            }
            assertTrue(result.qualifiedHealthy());
            assertFalse(result.idleNeutral());
            return result;
        }
    }
    private int aggregate(Set<String> owners, Map<String, StartupIoPolicy.Decision> decisions) throws Exception {
        var type = Class.forName("it.cavallium.rockserver.core.impl.StartupIoController");
        var method = type.getDeclaredMethod("storagePressureMask", Set.class, Map.class);
        method.setAccessible(true);
        return (Integer) method.invoke(null, owners, decisions);
    }
    @Test void aHealthyOwnerCanGrowWithGenuineIdleOwnersAndUnrelatedUnusedGroups() throws Exception {
        var decisions = Map.of("hot", new Owner().healthy(), "idle-owner", new Owner().idle(),
                ":shared", new Owner().idle(), "unused", new Owner().sample(1, true, false));
        assertEquals(0, aggregate(Set.of("hot", "idle-owner", ":shared"), decisions));
    }
    @Test void idleOnlyPublishesNeutralWithoutManufacturingHealthyGrowth() throws Exception {
        var idle = new Owner().idle();
        assertTrue(idle.idleNeutral());
        assertFalse(idle.qualifiedHealthy());
        assertEquals(StartupIoPolicy.IDLE_NEUTRAL, aggregate(Set.of("idle-owner", ":shared"),
                Map.of("idle-owner", idle, ":shared", new Owner().idle())));
        assertEquals(StartupIoPolicy.IDLE_NEUTRAL, aggregate(Set.of(":shared"), Map.of(":shared", idle)));
        assertEquals(StartupIoPolicy.UNKNOWN | StartupIoPolicy.FOREGROUND, aggregate(Set.of(), Map.of()));
    }
    @Test void unknownCompletedBusyStaleIncompleteRecoveringAndUrgentOwnersStillVeto() throws Exception {
        var vetoes = new HashMap<String, StartupIoPolicy.Decision>();
        var uncalibrated = new Owner(); uncalibrated.reads.recordPoint(100, false);
        vetoes.put("completed uncalibrated", uncalibrated.idle());
        vetoes.put("busy", new Owner().sample(1, true, false));
        vetoes.put("incomplete", new Owner().sample(0, false, false));
        vetoes.put("urgent", new Owner().sample(0, true, true));
        var stale = new Owner(); var old = stale.input(0, true, false);
        vetoes.put("stale", stale.policy.sample(old, old.sampledNanos() + StartupIoPolicy.MAX_SAMPLE_AGE_NANOS + 1));
        var recovering = new Owner(); recovering.reads.recordPoint(100, true); recovering.idle();
        vetoes.put("recovering", recovering.idle());
        var healthy = new Owner().healthy();
        for (var entry : vetoes.entrySet()) {
            assertFalse(entry.getValue().idleNeutral(), entry.getKey());
            int mask = aggregate(Set.of("hot", "other", ":shared"),
                    Map.of("hot", healthy, "other", entry.getValue(), ":shared", new Owner().idle()));
            assertNotEquals(0, mask, entry.getKey());
            assertNotEquals(StartupIoPolicy.IDLE_NEUTRAL, mask, entry.getKey());
        }
        assertEquals(StartupIoPolicy.UNKNOWN | StartupIoPolicy.FOREGROUND,
                aggregate(Set.of("hot", "missing"), Map.of("hot", healthy)));
    }
    @Test void aRecoveringIdleOwnerIsNeutralOnlyAfterFreshRecoveryWindows() throws Exception {
        var recovering = new Owner(); recovering.reads.recordPoint(100, true); recovering.idle();
        var healthy = new Owner().healthy();
        for (int i = 0; i < StartupIoPolicy.RECOVERY_WINDOWS; i++) {
            var idle = recovering.idle();
            int mask = aggregate(Set.of("hot", "idle-owner"), Map.of("hot", healthy, "idle-owner", idle));
            assertEquals(i == StartupIoPolicy.RECOVERY_WINDOWS - 1, idle.idleNeutral());
            assertEquals(i == StartupIoPolicy.RECOVERY_WINDOWS - 1, mask == 0);
        }
    }
    @Test void sharedCdcCannotQualifyGrowthAndItsPressureStillVetoesActualOwners() throws Exception {
        assertEquals(StartupIoPolicy.UNKNOWN, aggregate(Set.of("idle-owner", ":shared"),
                Map.of("idle-owner", new Owner().idle(), ":shared", new Owner().healthy())));
        assertEquals(StartupIoPolicy.URGENT, aggregate(Set.of("hot", ":shared"),
                Map.of("hot", new Owner().healthy(), ":shared", new Owner().sample(0, true, true))));
        assertEquals(StartupIoPolicy.URGENT, aggregate(Set.of(":shared"),
                Map.of(":shared", new Owner().sample(0, true, true))));
    }
}
