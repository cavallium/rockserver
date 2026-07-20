package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.ReplicationFailureInjector;
import it.cavallium.rockserver.core.cluster.command.FailureInjectingReplicatedLogClient;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogClient;

import java.util.concurrent.CompletableFuture;

import org.junit.jupiter.api.Test;

class FailureInjectingReplicatedLogClientTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        ReplicatedLogClient delegate = ignored -> CompletableFuture.completedFuture(
                new ReplicatedLogCommit(1, true));

        assertIllegalArgument("delegate must not be null",
                () -> new FailureInjectingReplicatedLogClient(null, ReplicationFailureInjector.none()));
        assertIllegalArgument("failureInjector must not be null",
                () -> new FailureInjectingReplicatedLogClient(delegate, null));
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        assertEquals(message, assertThrows(IllegalArgumentException.class, operation::run).getMessage());
    }
}
