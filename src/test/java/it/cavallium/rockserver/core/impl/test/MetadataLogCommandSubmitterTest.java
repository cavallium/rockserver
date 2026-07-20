package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogClient;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandApplier;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandCodec;
import it.cavallium.rockserver.core.cluster.metadata.MetadataLogCommandSubmitter;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.IOException;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

class MetadataLogCommandSubmitterTest {

    @Test
    void encodesMetadataCommandProposesToLogAndReturnsCommittedSnapshot() {
        var metadata = new ClusterMetadata[] {
                new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of())
        };
        var logClient = new RecordingLogClient(payload -> {
            metadata[0] = MetadataCommandApplier.apply(metadata[0], MetadataCommandCodec.decode(payload));
            return new ReplicatedLogCommit(11, true);
        });
        var submitter = new MetadataLogCommandSubmitter(logClient, () -> metadata[0]);
        var command = new MetadataCommand.CreateColumn(
                7,
                new ClusterColumnMetadata(42, "accounts", schema(), 0)
        );

        var committed = submitter.submit(command);

        assertEquals(command, MetadataCommandCodec.decode(logClient.payload));
        assertEquals(8, committed.epoch());
        assertEquals("accounts", committed.requireColumn(42).name());
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of());
        var logClient = new RecordingLogClient(_ -> new ReplicatedLogCommit(11, true));
        var submitter = new MetadataLogCommandSubmitter(logClient, () -> metadata);

        assertIllegalArgument("metadataLogClient must not be null",
                () -> new MetadataLogCommandSubmitter(null, () -> metadata));
        assertIllegalArgument("committedMetadataReader must not be null",
                () -> new MetadataLogCommandSubmitter(logClient, null));
        assertIllegalArgument("command must not be null", () -> submitter.submit(null));
    }

    @Test
    void rejectsUncommittedMetadataProposal() {
        var submitter = new MetadataLogCommandSubmitter(
                new RecordingLogClient(_ -> new ReplicatedLogCommit(11, false)),
                () -> new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of())
        );

        assertThrows(IllegalStateException.class, () -> submitter.submit(
                new MetadataCommand.DeleteColumn(7, 42, 0)
        ));
    }

    @Test
    void rejectsNullMetadataProposalFuture() {
        var submitter = new MetadataLogCommandSubmitter(
                _ -> null,
                () -> new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of())
        );

        var failure = assertThrows(IllegalStateException.class, () -> submitter.submit(
                new MetadataCommand.DeleteColumn(7, 42, 0)
        ));

        assertEquals("metadata command proposal future must not be null", failure.getMessage());
    }

    @Test
    void rejectsNullMetadataProposalCommit() {
        var submitter = new MetadataLogCommandSubmitter(
                _ -> CompletableFuture.completedFuture(null),
                () -> new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of())
        );

        var failure = assertThrows(IllegalStateException.class, () -> submitter.submit(
                new MetadataCommand.DeleteColumn(7, 42, 0)
        ));

        assertEquals("metadata command commit must not be null", failure.getMessage());
    }

    @Test
    void rejectsNullCommittedMetadataSnapshot() {
        var submitter = new MetadataLogCommandSubmitter(
                new RecordingLogClient(_ -> new ReplicatedLogCommit(11, true)),
                () -> null
        );

        var failure = assertThrows(IllegalStateException.class, () -> submitter.submit(
                new MetadataCommand.DeleteColumn(7, 42, 0)
        ));

        assertEquals("committed metadata must not be null", failure.getMessage());
    }

    @Test
    void propagatesFatalMetadataProposalFailureCause() {
        var failure = new AssertionError("proposal failed");
        var submitter = new MetadataLogCommandSubmitter(
                _ -> CompletableFuture.failedFuture(failure),
                () -> new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of())
        );

        assertSame(failure, assertThrows(AssertionError.class, () -> submitter.submit(
                new MetadataCommand.DeleteColumn(7, 42, 0)
        )));
    }

    @Test
    void propagatesNestedFatalMetadataProposalFailureCause() {
        var failure = new AssertionError("proposal failed");
        var submitter = new MetadataLogCommandSubmitter(
                _ -> CompletableFuture.failedFuture(new CompletionException(new CompletionException(failure))),
                () -> new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of())
        );

        assertSame(failure, assertThrows(AssertionError.class, () -> submitter.submit(
                new MetadataCommand.DeleteColumn(7, 42, 0)
        )));
    }

    @Test
    void flattensNestedCheckedMetadataProposalFailureCause() {
        var failure = new IOException("proposal failed");
        var submitter = new MetadataLogCommandSubmitter(
                _ -> CompletableFuture.failedFuture(new CompletionException(new CompletionException(failure))),
                () -> new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of())
        );

        var thrown = assertThrows(CompletionException.class, () -> submitter.submit(
                new MetadataCommand.DeleteColumn(7, 42, 0)
        ));

        assertEquals("metadata command proposal failed", thrown.getMessage());
        assertSame(failure, thrown.getCause());
    }

    @Test
    void interruptionCancelsMetadataProposalAndRestoresInterruptFlag() {
        var response = new CompletableFuture<ReplicatedLogCommit>();
        var submitter = new MetadataLogCommandSubmitter(
                _ -> response,
                () -> new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of())
        );

        Thread.currentThread().interrupt();
        try {
            var failure = assertThrows(CompletionException.class, () -> submitter.submit(
                    new MetadataCommand.DeleteColumn(7, 42, 0)
            ));

            assertInstanceOf(InterruptedException.class, failure.getCause());
            assertEquals("metadata command proposal interrupted", failure.getMessage());
            assertTrue(response.isCancelled());
            assertTrue(Thread.interrupted());
        } finally {
            clearInterruptFlag();
        }
    }

    @SuppressWarnings("ResultOfMethodCallIgnored")
    private static void clearInterruptFlag() {
        Thread.interrupted();
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }

    private static final class RecordingLogClient implements ReplicatedLogClient {

        private final ProposalHandler proposalHandler;
        private byte[] payload;

        private RecordingLogClient(ProposalHandler proposalHandler) {
            this.proposalHandler = proposalHandler;
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            this.payload = payload.clone();
            return CompletableFuture.completedFuture(proposalHandler.propose(payload));
        }
    }

    @FunctionalInterface
    private interface ProposalHandler {

        ReplicatedLogCommit propose(byte[] payload);
    }
}
