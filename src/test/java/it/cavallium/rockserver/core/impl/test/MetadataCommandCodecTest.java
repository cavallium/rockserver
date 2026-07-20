package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.cluster.metadata.BackupManifest;
import it.cavallium.rockserver.core.cluster.metadata.CdcTokenTranslation;
import it.cavallium.rockserver.core.cluster.metadata.MergeOperatorActivation;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandCodec;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeKind;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;

import java.io.ByteArrayOutputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.zip.CRC32C;

import org.junit.jupiter.api.Test;

class MetadataCommandCodecTest {

    @Test
    void modeledMetadataCommandsRoundTripThroughChecksummedPayloads() {
        var n1 = NodeId.of("n1");
        var n2 = NodeId.of("n2");
        var partition = partition(new ShardId(7), n1, null, buf(9), 3);
        var left = partition(new ShardId(8), n1, null, buf(5), 4);
        var right = partition(new ShardId(9), n1, buf(5), buf(9), 4);
        var merged = partition(new ShardId(10), n1, null, buf(9), 5);
        var replacementPeer = new ReplicaMember(
                n2,
                URI.create("grpc://127.0.0.1:19002"),
                ReplicaRole.VOTER,
                11,
                10,
                true
        );
        var commands = List.<MetadataCommand>of(
                new MetadataCommand.RegisterNode(
                        1,
                        n1,
                        URI.create("http://127.0.0.1:8080"),
                        URI.create("grpc://127.0.0.1:19001"),
                        "rack-a"
                ),
                new MetadataCommand.CreateColumn(
                        2,
                        new ClusterColumnMetadata(42, "accounts", schema(), 0)
                ),
                new MetadataCommand.DeleteColumn(3, 42, 0),
                new MetadataCommand.CreatePartition(4, partition),
                new MetadataCommand.SplitPartition(5, partition.partitionId(), partition.epoch(), left, right),
                new MetadataCommand.ChangePartitionPeers(
                        6,
                        partition.partitionId(),
                        partition.epoch(),
                        List.of(replacementPeer),
                        n2
                ),
                new MetadataCommand.MergePartitions(7, left.partitionId(), right.partitionId(), merged),
                new MetadataCommand.UpdateColumnSchema(8, 42, 0, 1, schema()),
                new MetadataCommand.PrepareSchemaChange(
                        9,
                        100,
                        SchemaChangeKind.CREATE_COLUMN,
                        43,
                        1,
                        new ClusterColumnMetadata(43, "orders", schema(), 1),
                        null
                ),
                new MetadataCommand.ActivateSchemaChange(10, 100, 43, 1),
                new MetadataCommand.FenceSchemaChange(11, 100, 43, 1),
                new MetadataCommand.PrepareSchemaChange(
                        12,
                        101,
                        SchemaChangeKind.MERGE_OPERATOR_ACTIVATION,
                        42,
                        2,
                        null,
                        new MergeOperatorActivation("counter", 5, Buf.wrap((byte) 1, (byte) 2))
                ),
                new MetadataCommand.RecordBackupManifest(13, backupManifest()),
                new MetadataCommand.RecordCdcTokenTranslation(14, new CdcTokenTranslation.Split(
                        14,
                        token(13, partition.partitionId(), partition.epoch(), 20),
                        token(14, left.partitionId(), left.epoch(), 0),
                        token(14, right.partitionId(), right.epoch(), 0)
                ))
        );

        for (var command : commands) {
            assertEquals(command, MetadataCommandCodec.decode(MetadataCommandCodec.encode(command)));
        }
    }

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("command must not be null", () -> MetadataCommandCodec.encode(null));
        assertIllegalArgument("payload must not be null", () -> MetadataCommandCodec.decode(null));
    }

    @Test
    void checksumMismatchIsRejected() {
        var payload = MetadataCommandCodec.encode(new MetadataCommand.DeleteColumn(3, 42, 0));
        payload[payload.length - 1] ^= 1;

        assertThrows(IllegalArgumentException.class, () -> MetadataCommandCodec.decode(payload));
    }

    @Test
    void oversizedDecodedStringLengthIsRejectedBeforeAllocation() {
        var payload = payloadWithBody(bodyWithMaxDeclaredKindLength());

        var thrown = assertThrows(IllegalArgumentException.class, () -> MetadataCommandCodec.decode(payload));

        assertEquals("Invalid metadata command string length: " + Integer.MAX_VALUE, thrown.getMessage());
    }

    @Test
    void withinLimitDecodedStringLengthBeyondRemainingPayloadIsRejectedBeforeAllocation() {
        var payload = payloadWithBody(bodyWithDeclaredKindLength(64 * 1024));

        var thrown = assertThrows(IllegalArgumentException.class, () -> MetadataCommandCodec.decode(payload));

        assertEquals("Unexpected end of metadata command payload", thrown.getMessage());
    }

    @Test
    void decodedCollectionCountMustFitRemainingPayloadBeforePreallocation() {
        var payload = payloadWithBody(backupManifestBodyWithDeclaredPartitionCount(1_000_000));

        var thrown = assertThrows(IllegalArgumentException.class, () -> MetadataCommandCodec.decode(payload));

        assertEquals("Invalid metadata command backup partition cut count for remaining payload: 1000000",
                thrown.getMessage());
    }

    @Test
    void oversizedEncodedStringLengthIsRejected() {
        var failureDomain = "x".repeat(70_000);
        var command = new MetadataCommand.RegisterNode(
                1,
                NodeId.of("n1"),
                URI.create("http://127.0.0.1:8080"),
                URI.create("grpc://127.0.0.1:19001"),
                failureDomain
        );

        var thrown = assertThrows(IllegalArgumentException.class, () -> MetadataCommandCodec.encode(command));

        assertEquals("Invalid metadata command string length: 70000", thrown.getMessage());
    }

    @Test
    void duplicateStringMapKeysAreRejectedBeforeMetadataConstruction() {
        var payload = payloadWithBody(createPartitionBodyWithDuplicatePlacementKeys());

        var thrown = assertThrows(IllegalArgumentException.class, () -> MetadataCommandCodec.decode(payload));

        assertEquals("Duplicate metadata command map key: rack", thrown.getMessage());
    }

    @Test
    void invalidPlacementConstraintsAreRejectedBeforeMetadataConstruction() {
        var invalidKey = payloadWithBody(createPartitionBodyWithPlacementEntries(
                "rack\ninjected=true",
                "rack-a"
        ));
        var keyThrown = assertThrows(IllegalArgumentException.class, () -> MetadataCommandCodec.decode(invalidKey));
        assertEquals("placement constraint key must be a single line", keyThrown.getMessage());

        var invalidValue = payloadWithBody(createPartitionBodyWithPlacementEntries(
                "rack",
                "rack-a\ninjected=true"
        ));
        var valueThrown = assertThrows(IllegalArgumentException.class, () -> MetadataCommandCodec.decode(invalidValue));
        assertEquals("placement constraint value must be a single line", valueThrown.getMessage());
    }

    @Test
    void invalidColumnNameIsRejectedBeforeMetadataConstruction() {
        var payload = payloadWithBody(createColumnBodyWithInvalidName());

        var thrown = assertThrows(IllegalArgumentException.class, () -> MetadataCommandCodec.decode(payload));

        assertEquals("column name must be a single line", thrown.getMessage());
    }

    @Test
    void malformedUtf8StringsAreRejectedBeforeMetadataConstruction() {
        var payload = payloadWithBody(registerNodeBodyWithMalformedFailureDomain());

        var thrown = assertThrows(IllegalArgumentException.class, () -> MetadataCommandCodec.decode(payload));

        assertEquals("Malformed metadata command UTF-8 string", thrown.getMessage());
    }

    @Test
    void invalidSchemaStringsAreRejectedBeforeMetadataConstruction() {
        var invalidName = payloadWithBody(createColumnBodyWithInvalidMergeOperatorName());
        var nameThrown = assertThrows(IllegalArgumentException.class,
                () -> MetadataCommandCodec.decode(invalidName));
        assertEquals("merge operator name must be a single line", nameThrown.getMessage());

        var invalidClass = payloadWithBody(createColumnBodyWithInvalidMergeOperatorClass());
        var classThrown = assertThrows(IllegalArgumentException.class,
                () -> MetadataCommandCodec.decode(invalidClass));
        assertEquals("merge operator class must be a single line", classThrown.getMessage());
    }

    private static PartitionDescriptor partition(ShardId partitionId,
                                                 NodeId leader,
                                                 Buf startInclusive,
                                                 Buf endExclusive,
                                                 long epoch) {
        return new PartitionDescriptor(
                partitionId,
                startInclusive,
                endExclusive,
                epoch,
                "group-" + partitionId,
                List.of(new ReplicaMember(
                        leader,
                        URI.create("grpc://127.0.0.1:19001"),
                        ReplicaRole.VOTER,
                        10,
                        10,
                        true
                )),
                leader,
                Map.of("rack", "rack-a"),
                PartitionSplitState.ACTIVE
        );
    }

    private static ColumnSchema schema() {
        return ColumnSchema.of(
                IntList.of(4),
                ObjectList.of(ColumnHashType.XXHASH8),
                true,
                "merge",
                2L
        );
    }

    private static Buf buf(int value) {
        return Buf.wrap((byte) value);
    }

    private static BackupManifest backupManifest() {
        return new BackupManifest(
                "backup-13",
                13,
                13,
                List.of(new BackupManifest.PartitionCut(new ShardId(7), 3, 5, 99, 13))
        );
    }

    private static ReplicatedCdcToken token(long metadataEpoch,
                                            ShardId partitionId,
                                            long partitionEpoch,
                                            long appliedIndex) {
        return new ReplicatedCdcToken(metadataEpoch, partitionId, partitionEpoch, appliedIndex, 0);
    }

    private static byte[] bodyWithMaxDeclaredKindLength() {
        return bodyWithDeclaredKindLength(Integer.MAX_VALUE);
    }

    private static byte[] bodyWithDeclaredKindLength(int length) {
        var encoded = MetadataCommandCodec.encode(new MetadataCommand.DeleteColumn(3, 42, 0));
        var magicAndVersion = Arrays.copyOfRange(encoded, Integer.BYTES, Integer.BYTES + 2 * Integer.BYTES);
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.write(magicAndVersion);
            out.writeInt(length);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return bytes.toByteArray();
    }

    private static byte[] backupManifestBodyWithDeclaredPartitionCount(int partitionCount) {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(0x52534D43);
            out.writeInt(1);
            writeTestString(out, "RECORD_BACKUP_MANIFEST");
            out.writeLong(4);
            writeTestString(out, "backup-4");
            out.writeLong(4);
            out.writeLong(4);
            out.writeInt(partitionCount);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return bytes.toByteArray();
    }

    private static byte[] createPartitionBodyWithDuplicatePlacementKeys() {
        return createPartitionBodyWithPlacementEntries(
                "rack",
                "rack-a",
                "rack",
                "rack-b"
        );
    }

    private static byte[] createPartitionBodyWithPlacementEntries(String... placementKeyValues) {
        if (placementKeyValues.length % 2 != 0) {
            throw new IllegalArgumentException("placementKeyValues must contain key/value pairs");
        }
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(0x52534D43);
            out.writeInt(1);
            writeTestString(out, "CREATE_PARTITION");
            out.writeLong(4);
            out.writeLong(7);
            out.writeBoolean(false);
            out.writeBoolean(false);
            out.writeLong(3);
            writeTestString(out, "group-7");
            out.writeInt(1);
            writeTestString(out, "n1");
            writeTestString(out, "grpc://127.0.0.1:19001");
            writeTestString(out, ReplicaRole.VOTER.name());
            out.writeLong(10);
            out.writeLong(10);
            out.writeBoolean(true);
            out.writeBoolean(true);
            writeTestString(out, "n1");
            out.writeInt(placementKeyValues.length / 2);
            for (var placementKeyValue : placementKeyValues) {
                writeTestString(out, placementKeyValue);
            }
            writeTestString(out, PartitionSplitState.ACTIVE.name());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return bytes.toByteArray();
    }

    private static byte[] createColumnBodyWithInvalidName() {
        return createColumnBody("accounts\ninjected=true", null, null);
    }

    private static byte[] createColumnBodyWithInvalidMergeOperatorName() {
        return createColumnBody("accounts", "merge\ninjected=true", null);
    }

    private static byte[] createColumnBodyWithInvalidMergeOperatorClass() {
        return createColumnBody("accounts", "merge", "it.example.Merge\ninjected=true");
    }

    private static byte[] registerNodeBodyWithMalformedFailureDomain() {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(0x52534D43);
            out.writeInt(1);
            writeTestString(out, "REGISTER_NODE");
            out.writeLong(4);
            writeTestString(out, "n1");
            writeTestString(out, "http://127.0.0.1:8080");
            writeTestString(out, "grpc://127.0.0.1:19001");
            out.writeInt(2);
            out.write(new byte[]{(byte) 0xC3, 0x28});
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return bytes.toByteArray();
    }

    private static byte[] createColumnBody(String columnName,
                                           String mergeOperatorName,
                                           String mergeOperatorClass) {
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(0x52534D43);
            out.writeInt(1);
            writeTestString(out, "CREATE_COLUMN");
            out.writeLong(4);
            out.writeLong(42);
            writeTestString(out, columnName);
            out.writeBoolean(true);
            out.writeInt(1);
            writeTestString(out, "4");
            out.writeInt(0);
            writeNullableTestString(out, mergeOperatorName);
            out.writeBoolean(mergeOperatorName != null);
            if (mergeOperatorName != null) {
                out.writeLong(3);
            }
            writeNullableTestString(out, mergeOperatorClass);
            out.writeLong(0);
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return bytes.toByteArray();
    }

    private static void writeTestString(DataOutputStream out, String value) throws IOException {
        var bytes = value.getBytes(StandardCharsets.UTF_8);
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    private static void writeNullableTestString(DataOutputStream out, String value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            writeTestString(out, value);
        }
    }

    private static byte[] payloadWithBody(byte[] body) {
        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        var bytes = new ByteArrayOutputStream();
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(body.length);
            out.write(body);
            out.writeLong(crc.getValue());
        } catch (IOException e) {
            throw new AssertionError(e);
        }
        return bytes.toByteArray();
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }
}
