package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandCodec;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandValidator;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.Map;
import java.util.UUID;

import org.junit.jupiter.api.Test;

class MetadataCommandValidatorTest {

    @Test
    void validProposalDecodesAndProjectsNextMetadataEpoch() {
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of());
        var command = new MetadataCommand.CreateColumn(
                metadata.epoch(),
                new ClusterColumnMetadata(42, "accounts", schema(), 0)
        );

        var decoded = MetadataCommandValidator.decodeForProposal(MetadataCommandCodec.encode(command));
        var projected = MetadataCommandValidator.projectForProposal(metadata, decoded);

        assertEquals(command, decoded);
        assertEquals(8, projected.epoch());
        assertEquals(command.column(), projected.requireColumn(42));
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of());
        var command = new MetadataCommand.DeleteColumn(7, 42, 0);

        assertIllegalArgument("payload must not be null", () -> MetadataCommandValidator.decodeForProposal(null));
        assertIllegalArgument("payload must not be null", () -> MetadataCommandValidator.decodeCommitted(null));
        assertIllegalArgument("metadata must not be null",
                () -> MetadataCommandValidator.projectForProposal(null, command));
        assertIllegalArgument("command must not be null",
                () -> MetadataCommandValidator.projectForProposal(metadata, null));
    }

    @Test
    void proposalPayloadCapLeavesHeadroomBelowRatisLogBuffer() {
        var oversized = new byte[MetadataCommandValidator.MAX_PROPOSAL_PAYLOAD_BYTES + 1];

        var thrown = assertThrows(IllegalArgumentException.class,
                () -> MetadataCommandValidator.decodeForProposal(oversized));

        assertEquals("Metadata proposal payload exceeds "
                + MetadataCommandValidator.MAX_PROPOSAL_PAYLOAD_BYTES + " bytes: " + oversized.length,
                thrown.getMessage());
    }

    @Test
    void projectedMetadataRejectsConcurrentCommandWithStaleEpoch() {
        var metadata = new ClusterMetadata(new ClusterId(new UUID(0, 1)), 7, Map.of());
        var command = new MetadataCommand.CreateColumn(
                metadata.epoch(),
                new ClusterColumnMetadata(42, "accounts", schema(), 0)
        );
        var projected = MetadataCommandValidator.projectForProposal(metadata, command);

        assertThrows(IllegalArgumentException.class,
                () -> MetadataCommandValidator.projectForProposal(projected, command));
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true);
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }
}
