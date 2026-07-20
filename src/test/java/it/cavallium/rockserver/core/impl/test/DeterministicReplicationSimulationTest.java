package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.junit.jupiter.api.Test;

class DeterministicReplicationSimulationTest {

    @Test
    void deterministicScenarioCoversPartitionsNetworkDiskRestartDuplicatesAndDelayedSnapshots() {
        var simulation = new Simulation("n1", "n2", "n3");

        assertFalse(simulation.write("p1", "req-1", "account:1", "v1").duplicate());
        simulation.isolate("n3");

        var committed = simulation.write("p1", "req-2", "account:1", "v2");
        var duplicate = simulation.write("p1", "req-2", "account:1", "v2");

        assertFalse(committed.duplicate());
        assertTrue(duplicate.duplicate());
        assertEquals(committed.index(), duplicate.index());
        assertEquals(2, simulation.logSize());

        simulation.failDisk("n2");
        assertThrows(IllegalStateException.class,
                () -> simulation.write("p2", "req-3", "account:2", "blocked"));
        simulation.recoverDisk("n2");

        simulation.write("p2", "req-4", "account:2", "v4");
        simulation.restart("n2");
        assertEquals("v4", simulation.value("n2", "p2", "account:2"));

        simulation.heal("n3");
        simulation.delaySnapshot("n3");
        simulation.catchUp("n3");

        assertTrue(simulation.needsSnapshot("n3"));
        assertEquals("v1", simulation.value("n3", "p1", "account:1"));

        simulation.releaseSnapshot("n3");
        simulation.catchUp("n3");

        assertFalse(simulation.needsSnapshot("n3"));
        simulation.assertConverged("p1", "account:1", "v2");
        simulation.assertConverged("p2", "account:2", "v4");
    }

    private record WriteResult(long index, boolean duplicate) {
    }

    private record Entry(long index, String partitionId, String requestId, String key, String value) {
    }

    private static final class Simulation {

        private static final int SNAPSHOT_THRESHOLD = 1;

        private final Map<String, Replica> replicas = new HashMap<>();
        private final Set<String> isolated = new HashSet<>();
        private final List<Entry> log = new ArrayList<>();
        private final Map<String, Map<String, String>> committedState = new HashMap<>();
        private final Map<String, Long> committedDedupe = new HashMap<>();
        private final String leaderId;

        private Simulation(String leaderId, String... replicaIds) {
            this.leaderId = leaderId;
            replicas.put(leaderId, new Replica());
            for (String replicaId : replicaIds) {
                replicas.put(replicaId, new Replica());
            }
        }

        private WriteResult write(String partitionId, String requestId, String key, String value) {
            if (committedDedupe.containsKey(requestId)) {
                return new WriteResult(committedDedupe.get(requestId), true);
            }
            var writableReplicas = writableReplicas();
            if (writableReplicas.size() < quorumSize()) {
                throw new IllegalStateException("write cannot reach quorum");
            }

            var entry = new Entry(log.size() + 1L, partitionId, requestId, key, value);
            log.add(entry);
            committedDedupe.put(requestId, entry.index());
            committedState.computeIfAbsent(partitionId, ignored -> new HashMap<>()).put(key, value);
            for (String replicaId : writableReplicas) {
                replicas.get(replicaId).apply(entry);
            }
            return new WriteResult(entry.index(), false);
        }

        private void isolate(String replicaId) {
            isolated.add(replicaId);
        }

        private void heal(String replicaId) {
            isolated.remove(replicaId);
        }

        private void failDisk(String replicaId) {
            replicas.get(replicaId).diskWritable = false;
        }

        private void recoverDisk(String replicaId) {
            replicas.get(replicaId).diskWritable = true;
        }

        private void restart(String replicaId) {
            var replica = replicas.get(replicaId);
            replica.running = false;
            replica.running = true;
        }

        private void delaySnapshot(String replicaId) {
            replicas.get(replicaId).delaySnapshot = true;
        }

        private void releaseSnapshot(String replicaId) {
            replicas.get(replicaId).delaySnapshot = false;
        }

        private void catchUp(String replicaId) {
            if (isolated.contains(replicaId)) {
                throw new IllegalStateException("isolated replica cannot catch up");
            }
            var replica = replicas.get(replicaId);
            var lag = log.size() - replica.lastApplied;
            if (lag > SNAPSHOT_THRESHOLD) {
                replica.needsSnapshot = true;
                if (replica.delaySnapshot) {
                    return;
                }
                replica.installSnapshot(committedState, committedDedupe, log.size());
                return;
            }
            for (int i = (int) replica.lastApplied; i < log.size(); i++) {
                replica.apply(log.get(i));
            }
        }

        private boolean needsSnapshot(String replicaId) {
            return replicas.get(replicaId).needsSnapshot;
        }

        private String value(String replicaId, String partitionId, String key) {
            return replicas.get(replicaId).state.getOrDefault(partitionId, Map.of()).get(key);
        }

        private int logSize() {
            return log.size();
        }

        private void assertConverged(String partitionId, String key, String value) {
            for (String replicaId : replicas.keySet()) {
                assertEquals(value, value(replicaId, partitionId, key), replicaId);
            }
        }

        private List<String> writableReplicas() {
            var writableReplicas = new ArrayList<String>();
            for (var entry : replicas.entrySet()) {
                if (entry.getKey().equals(leaderId) || !isolated.contains(entry.getKey())) {
                    var replica = entry.getValue();
                    if (replica.running && replica.diskWritable) {
                        writableReplicas.add(entry.getKey());
                    }
                }
            }
            return writableReplicas;
        }

        private int quorumSize() {
            return replicas.size() / 2 + 1;
        }
    }

    private static final class Replica {

        private final Map<String, Map<String, String>> state = new HashMap<>();
        private final Map<String, Long> dedupe = new HashMap<>();
        private boolean running = true;
        private boolean diskWritable = true;
        private boolean delaySnapshot;
        private boolean needsSnapshot;
        private long lastApplied;

        private void apply(Entry entry) {
            state.computeIfAbsent(entry.partitionId(), ignored -> new HashMap<>()).put(entry.key(), entry.value());
            dedupe.put(entry.requestId(), entry.index());
            lastApplied = entry.index();
        }

        private void installSnapshot(Map<String, Map<String, String>> committedState,
                                     Map<String, Long> committedDedupe,
                                     long lastApplied) {
            state.clear();
            for (var partition : committedState.entrySet()) {
                state.put(partition.getKey(), new HashMap<>(partition.getValue()));
            }
            dedupe.clear();
            dedupe.putAll(committedDedupe);
            this.lastApplied = lastApplied;
            needsSnapshot = false;
        }
    }
}
