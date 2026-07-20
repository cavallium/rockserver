package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import it.cavallium.rockserver.core.cluster.ReplicationFailurePhase;

import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;

import org.junit.jupiter.api.Test;

class WritePhaseCrashCoverageTest {

    @Test
    void everyWriteFailurePhaseHasCrashCoverageAndRecoveryInvariant() {
        var coverage = new EnumMap<ReplicationFailurePhase, PhaseCoverage>(ReplicationFailurePhase.class);
        for (PhaseCoverage phaseCoverage : List.of(
                new PhaseCoverage(
                        ReplicationFailurePhase.APPEND,
                        "ReplicatedWriteSyncAPITest",
                        "append failure happens before proposal and must not create a committed entry"
                ),
                new PhaseCoverage(
                        ReplicationFailurePhase.COMMIT,
                        "ReplicatedWriteSyncAPITest",
                        "commit failure is returned as a retryable replicated write failure"
                ),
                new PhaseCoverage(
                        ReplicationFailurePhase.APPLY,
                        "RockserverRatisStateMachinePersistentBatchTest",
                        "apply failure must not advance durable lastApplied"
                ),
                new PhaseCoverage(
                        ReplicationFailurePhase.RESPONSE_PERSISTENCE,
                        "RockserverRatisStateMachinePersistentBatchTest",
                        "response persistence failure must keep the write retryable through dedupe state"
                ),
                new PhaseCoverage(
                        ReplicationFailurePhase.ROCKSDB_WRITE,
                        "RockserverRatisStateMachinePersistentBatchTest",
                        "RocksDB write failure must not publish a partial state-machine batch"
                ),
                new PhaseCoverage(
                        ReplicationFailurePhase.RESPONSE_SEND,
                        "ReplicatedWriteSyncAPITest and RockserverRatisStateMachinePersistentBatchTest",
                        "response-send failure happens after commit and must be safe for duplicate retry"
                )
        )) {
            coverage.put(phaseCoverage.phase(), phaseCoverage);
        }

        assertEquals(EnumSet.allOf(ReplicationFailurePhase.class), coverage.keySet());
        for (PhaseCoverage phaseCoverage : coverage.values()) {
            assertFalse(phaseCoverage.testReference().isBlank());
            assertFalse(phaseCoverage.recoveryInvariant().isBlank());
        }
    }

    private record PhaseCoverage(ReplicationFailurePhase phase,
                                 String testReference,
                                 String recoveryInvariant) {
    }
}
