package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.ReplicatedCdcToken;
import it.cavallium.rockserver.core.cluster.internal.BoundedByteArrayOutputStream;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.ByteArrayInputStream;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.zip.CRC32C;

import org.jetbrains.annotations.Nullable;

public final class MetadataCommandCodec {

    private static final int MAGIC = 0x52534D43;
    private static final int VERSION = 1;
    private static final int MAX_LIST_SIZE = 1_000_000;
    private static final int MAX_METADATA_COMMAND_BODY_BYTES = 64 * 1024 * 1024;
    private static final int MAX_METADATA_COMMAND_BINARY_BYTES = 8 * 1024 * 1024;
    private static final int MAX_METADATA_COMMAND_STRING_BYTES = 64 * 1024;

    private MetadataCommandCodec() {
    }

    public static byte[] encode(MetadataCommand command) {
        requireArgument(command, "command");
        var body = serializeBody(command);
        if (body.length > MAX_METADATA_COMMAND_BODY_BYTES) {
            throw new IllegalArgumentException("Invalid metadata command body length: " + body.length);
        }
        var payload = ByteBuffer.allocate(Integer.BYTES + body.length + Long.BYTES);
        payload.putInt(body.length);
        payload.put(body);
        payload.putLong(checksum(body));
        return payload.array();
    }

    public static MetadataCommand decode(byte[] payload) {
        requireArgument(payload, "payload");
        if (payload.length < Integer.BYTES + Long.BYTES) {
            throw new IllegalArgumentException("Metadata command payload is too short");
        }

        var buffer = ByteBuffer.wrap(payload);
        int bodyLength = buffer.getInt();
        if (bodyLength < 0
                || bodyLength > MAX_METADATA_COMMAND_BODY_BYTES
                || bodyLength > buffer.remaining() - Long.BYTES) {
            throw new IllegalArgumentException("Invalid metadata command body length: " + bodyLength);
        }

        byte[] body = new byte[bodyLength];
        buffer.get(body);
        long expectedCrc = buffer.getLong();
        if (buffer.hasRemaining()) {
            throw new IllegalArgumentException("Trailing bytes after metadata command checksum");
        }

        if (checksum(body) != expectedCrc) {
            throw new IllegalArgumentException("Metadata command checksum mismatch");
        }

        return deserializeBody(body);
    }

    private static long checksum(byte[] body) {
        var crc = new CRC32C();
        crc.update(body, 0, body.length);
        return crc.getValue();
    }

    private static void requireArgument(Object value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
    }

    private static byte[] serializeBody(MetadataCommand command) {
        var bytes = new BoundedByteArrayOutputStream(
                MAX_METADATA_COMMAND_BODY_BYTES,
                "metadata command body"
        );
        try (var out = new DataOutputStream(bytes)) {
            out.writeInt(MAGIC);
            out.writeInt(VERSION);
            writeString(out, command.kind().name());
            out.writeLong(command.expectedMetadataEpoch());
            switch (command) {
                case MetadataCommand.RegisterNode registerNode -> writeRegisterNode(out, registerNode);
                case MetadataCommand.CreateColumn createColumn -> writeColumn(out, createColumn.column());
                case MetadataCommand.DeleteColumn deleteColumn -> {
                    out.writeLong(deleteColumn.columnId());
                    out.writeLong(deleteColumn.expectedSchemaEpoch());
                }
                case MetadataCommand.CreatePartition createPartition ->
                        writePartition(out, createPartition.partition());
                case MetadataCommand.ChangePartitionPeers changePeers -> writeChangePartitionPeers(out, changePeers);
                case MetadataCommand.UpdateColumnSchema updateColumnSchema -> writeUpdateColumnSchema(out,
                        updateColumnSchema);
                case MetadataCommand.SplitPartition splitPartition -> writeSplitPartition(out, splitPartition);
                case MetadataCommand.MergePartitions mergePartitions -> writeMergePartitions(out, mergePartitions);
                case MetadataCommand.PrepareSchemaChange prepareSchemaChange -> writePrepareSchemaChange(out,
                        prepareSchemaChange);
                case MetadataCommand.ActivateSchemaChange activateSchemaChange -> writeActivateSchemaChange(out,
                        activateSchemaChange);
                case MetadataCommand.FenceSchemaChange fenceSchemaChange -> writeFenceSchemaChange(out,
                        fenceSchemaChange);
                case MetadataCommand.RecordBackupManifest recordBackupManifest -> writeBackupManifest(out,
                        recordBackupManifest.manifest());
                case MetadataCommand.RecordCdcTokenTranslation recordCdcTokenTranslation ->
                        writeCdcTokenTranslation(out, recordCdcTokenTranslation.translation());
            }
        } catch (IOException e) {
            throw new UncheckedIOException("Failed to encode metadata command", e);
        }
        return bytes.toByteArray();
    }

    private static MetadataCommand deserializeBody(byte[] body) {
        try (var in = new DataInputStream(new ByteArrayInputStream(body))) {
            int magic = in.readInt();
            if (magic != MAGIC) {
                throw new IllegalArgumentException("Invalid metadata command magic");
            }
            int version = in.readInt();
            if (version != VERSION) {
                throw new IllegalArgumentException("Unsupported metadata command version: " + version);
            }
            var kind = MetadataCommandKind.valueOf(readString(in));
            long expectedMetadataEpoch = in.readLong();
            var command = switch (kind) {
                case REGISTER_NODE -> readRegisterNode(in, expectedMetadataEpoch);
                case CREATE_COLUMN -> new MetadataCommand.CreateColumn(expectedMetadataEpoch, readColumn(in));
                case DELETE_COLUMN -> new MetadataCommand.DeleteColumn(
                        expectedMetadataEpoch,
                        in.readLong(),
                        in.readLong()
                );
                case CREATE_PARTITION -> new MetadataCommand.CreatePartition(expectedMetadataEpoch, readPartition(in));
                case CHANGE_PARTITION_PEERS -> readChangePartitionPeers(in, expectedMetadataEpoch);
                case UPDATE_COLUMN_SCHEMA -> readUpdateColumnSchema(in, expectedMetadataEpoch);
                case SPLIT_PARTITION -> readSplitPartition(in, expectedMetadataEpoch);
                case MERGE_PARTITIONS -> readMergePartitions(in, expectedMetadataEpoch);
                case PREPARE_SCHEMA_CHANGE -> readPrepareSchemaChange(in, expectedMetadataEpoch);
                case ACTIVATE_SCHEMA_CHANGE -> readActivateSchemaChange(in, expectedMetadataEpoch);
                case FENCE_SCHEMA_CHANGE -> readFenceSchemaChange(in, expectedMetadataEpoch);
                case RECORD_BACKUP_MANIFEST -> new MetadataCommand.RecordBackupManifest(
                        expectedMetadataEpoch,
                        readBackupManifest(in)
                );
                case RECORD_CDC_TOKEN_TRANSLATION ->
                        new MetadataCommand.RecordCdcTokenTranslation(expectedMetadataEpoch,
                                readCdcTokenTranslation(in));
            };
            if (in.available() != 0) {
                throw new IllegalArgumentException("Trailing bytes in metadata command body");
            }
            return command;
        } catch (IOException e) {
            throw new IllegalArgumentException("Malformed metadata command payload", e);
        }
    }

    private static void writeRegisterNode(DataOutputStream out, MetadataCommand.RegisterNode command)
            throws IOException {
        writeNodeId(out, command.nodeId());
        writeString(out, command.publicEndpoint().toString());
        writeString(out, command.ratisEndpoint().toString());
        writeString(out, command.failureDomain());
    }

    private static MetadataCommand.RegisterNode readRegisterNode(DataInputStream in, long expectedMetadataEpoch)
            throws IOException {
        return new MetadataCommand.RegisterNode(
                expectedMetadataEpoch,
                readNodeId(in),
                URI.create(readString(in)),
                URI.create(readString(in)),
                readString(in)
        );
    }

    private static void writeSplitPartition(DataOutputStream out,
                                            MetadataCommand.SplitPartition command) throws IOException {
        writeShardId(out, command.sourcePartitionId());
        out.writeLong(command.expectedSourceEpoch());
        writePartition(out, command.left());
        writePartition(out, command.right());
    }

    private static MetadataCommand.SplitPartition readSplitPartition(DataInputStream in,
                                                                     long expectedMetadataEpoch)
            throws IOException {
        return new MetadataCommand.SplitPartition(
                expectedMetadataEpoch,
                readShardId(in),
                in.readLong(),
                readPartition(in),
                readPartition(in)
        );
    }

    private static void writeMergePartitions(DataOutputStream out,
                                             MetadataCommand.MergePartitions command) throws IOException {
        writeShardId(out, command.leftPartitionId());
        writeShardId(out, command.rightPartitionId());
        writePartition(out, command.merged());
    }

    private static MetadataCommand.MergePartitions readMergePartitions(DataInputStream in,
                                                                       long expectedMetadataEpoch)
            throws IOException {
        return new MetadataCommand.MergePartitions(
                expectedMetadataEpoch,
                readShardId(in),
                readShardId(in),
                readPartition(in)
        );
    }

    private static void writeCdcTokenTranslation(DataOutputStream out, CdcTokenTranslation translation)
            throws IOException {
        writeString(out, translation.kind().name());
        out.writeLong(translation.metadataEpoch());
        switch (translation) {
            case CdcTokenTranslation.Split split -> {
                writeCdcToken(out, split.sourceEndToken());
                writeCdcToken(out, split.leftStartToken());
                writeCdcToken(out, split.rightStartToken());
            }
            case CdcTokenTranslation.Merge merge -> {
                writeCdcToken(out, merge.leftEndToken());
                writeCdcToken(out, merge.rightEndToken());
                writeCdcToken(out, merge.mergedStartToken());
            }
        }
    }

    private static CdcTokenTranslation readCdcTokenTranslation(DataInputStream in) throws IOException {
        var kind = CdcTokenTranslation.TranslationKind.valueOf(readString(in));
        long metadataEpoch = in.readLong();
        return switch (kind) {
            case SPLIT -> new CdcTokenTranslation.Split(
                    metadataEpoch,
                    readCdcToken(in),
                    readCdcToken(in),
                    readCdcToken(in)
            );
            case MERGE -> new CdcTokenTranslation.Merge(
                    metadataEpoch,
                    readCdcToken(in),
                    readCdcToken(in),
                    readCdcToken(in)
            );
        };
    }

    private static void writeCdcToken(DataOutputStream out, ReplicatedCdcToken token) throws IOException {
        out.writeLong(token.metadataEpoch());
        writeShardId(out, token.partitionId());
        out.writeLong(token.partitionEpoch());
        out.writeLong(token.appliedIndex());
        out.writeInt(token.eventOrdinal());
    }

    private static ReplicatedCdcToken readCdcToken(DataInputStream in) throws IOException {
        return new ReplicatedCdcToken(
                in.readLong(),
                readShardId(in),
                in.readLong(),
                in.readLong(),
                in.readInt()
        );
    }

    private static void writeChangePartitionPeers(DataOutputStream out,
                                                  MetadataCommand.ChangePartitionPeers command) throws IOException {
        writeShardId(out, command.partitionId());
        out.writeLong(command.expectedPartitionEpoch());
        writeReplicaMembers(out, command.peers());
        writeNullableNodeId(out, command.leaderHint());
    }

    private static MetadataCommand.ChangePartitionPeers readChangePartitionPeers(DataInputStream in,
                                                                                 long expectedMetadataEpoch)
            throws IOException {
        return new MetadataCommand.ChangePartitionPeers(
                expectedMetadataEpoch,
                readShardId(in),
                in.readLong(),
                readReplicaMembers(in),
                readNullableNodeId(in)
        );
    }

    private static void writeUpdateColumnSchema(DataOutputStream out,
                                                MetadataCommand.UpdateColumnSchema command) throws IOException {
        out.writeLong(command.columnId());
        out.writeLong(command.expectedSchemaEpoch());
        out.writeLong(command.newSchemaEpoch());
        writeSchema(out, command.schema());
    }

    private static MetadataCommand.UpdateColumnSchema readUpdateColumnSchema(DataInputStream in,
                                                                             long expectedMetadataEpoch)
            throws IOException {
        return new MetadataCommand.UpdateColumnSchema(
                expectedMetadataEpoch,
                in.readLong(),
                in.readLong(),
                in.readLong(),
                readSchema(in)
        );
    }

    private static void writePrepareSchemaChange(DataOutputStream out,
                                                 MetadataCommand.PrepareSchemaChange command) throws IOException {
        out.writeLong(command.schemaChangeId());
        writeString(out, command.changeKind().name());
        out.writeLong(command.columnId());
        out.writeLong(command.targetSchemaEpoch());
        writeNullableColumn(out, command.column());
        writeNullableMergeOperatorActivation(out, command.mergeOperatorActivation());
    }

    private static MetadataCommand.PrepareSchemaChange readPrepareSchemaChange(DataInputStream in,
                                                                               long expectedMetadataEpoch)
            throws IOException {
        return new MetadataCommand.PrepareSchemaChange(
                expectedMetadataEpoch,
                in.readLong(),
                SchemaChangeKind.valueOf(readString(in)),
                in.readLong(),
                in.readLong(),
                readNullableColumn(in),
                readNullableMergeOperatorActivation(in)
        );
    }

    private static void writeActivateSchemaChange(DataOutputStream out,
                                                  MetadataCommand.ActivateSchemaChange command) throws IOException {
        out.writeLong(command.schemaChangeId());
        out.writeLong(command.columnId());
        out.writeLong(command.targetSchemaEpoch());
    }

    private static MetadataCommand.ActivateSchemaChange readActivateSchemaChange(DataInputStream in,
                                                                                 long expectedMetadataEpoch)
            throws IOException {
        return new MetadataCommand.ActivateSchemaChange(
                expectedMetadataEpoch,
                in.readLong(),
                in.readLong(),
                in.readLong()
        );
    }

    private static void writeFenceSchemaChange(DataOutputStream out,
                                               MetadataCommand.FenceSchemaChange command) throws IOException {
        out.writeLong(command.schemaChangeId());
        out.writeLong(command.columnId());
        out.writeLong(command.fencedSchemaEpoch());
    }

    private static MetadataCommand.FenceSchemaChange readFenceSchemaChange(DataInputStream in,
                                                                           long expectedMetadataEpoch)
            throws IOException {
        return new MetadataCommand.FenceSchemaChange(
                expectedMetadataEpoch,
                in.readLong(),
                in.readLong(),
                in.readLong()
        );
    }

    private static void writeBackupManifest(DataOutputStream out, BackupManifest manifest) throws IOException {
        writeString(out, manifest.backupId());
        out.writeLong(manifest.metadataBackupEpoch());
        out.writeLong(manifest.metadataEpoch());
        writeCount(out, manifest.partitionCuts().size());
        for (var cut : manifest.partitionCuts()) {
            writeShardId(out, cut.partitionId());
            out.writeLong(cut.partitionEpoch());
            out.writeLong(cut.ratisTerm());
            out.writeLong(cut.ratisIndex());
            out.writeLong(cut.observedMetadataBackupEpoch());
        }
    }

    private static BackupManifest readBackupManifest(DataInputStream in) throws IOException {
        var backupId = readString(in);
        var metadataBackupEpoch = in.readLong();
        var metadataEpoch = in.readLong();
        var count = readCount(in, 5 * Long.BYTES, "backup partition cut");
        var cuts = new java.util.ArrayList<BackupManifest.PartitionCut>(count);
        for (int index = 0; index < count; index++) {
            cuts.add(new BackupManifest.PartitionCut(
                    readShardId(in),
                    in.readLong(),
                    in.readLong(),
                    in.readLong(),
                    in.readLong()
            ));
        }
        return new BackupManifest(backupId, metadataBackupEpoch, metadataEpoch, cuts);
    }

    private static void writeColumn(DataOutputStream out, ClusterColumnMetadata column) throws IOException {
        out.writeLong(column.columnId());
        writeString(out, column.name());
        writeSchema(out, column.schema());
        out.writeLong(column.schemaEpoch());
    }

    private static ClusterColumnMetadata readColumn(DataInputStream in) throws IOException {
        return new ClusterColumnMetadata(in.readLong(), readString(in), readSchema(in), in.readLong());
    }

    private static void writeNullableColumn(DataOutputStream out,
                                            @Nullable ClusterColumnMetadata column) throws IOException {
        out.writeBoolean(column != null);
        if (column != null) {
            writeColumn(out, column);
        }
    }

    private static @Nullable ClusterColumnMetadata readNullableColumn(DataInputStream in) throws IOException {
        return in.readBoolean() ? readColumn(in) : null;
    }

    private static void writeNullableMergeOperatorActivation(DataOutputStream out,
                                                             @Nullable MergeOperatorActivation activation)
            throws IOException {
        out.writeBoolean(activation != null);
        if (activation != null) {
            writeString(out, activation.name());
            out.writeLong(activation.version());
            writeBytes(out, activation.artifactHash().toByteArray());
        }
    }

    private static @Nullable MergeOperatorActivation readNullableMergeOperatorActivation(DataInputStream in)
            throws IOException {
        if (!in.readBoolean()) {
            return null;
        }
        return new MergeOperatorActivation(
                readString(in),
                in.readLong(),
                Buf.wrap(readBytes(in)).freeze()
        );
    }

    private static void writeSchema(DataOutputStream out, ColumnSchema schema) throws IOException {
        out.writeBoolean(schema.hasValue());
        var fixedKeys = new java.util.ArrayList<String>();
        for (int index = 0; index < schema.fixedLengthKeysCount(); index++) {
            fixedKeys.add(Integer.toString(schema.key(index)));
        }
        writeStringList(out, fixedKeys);
        var variableTailKeys = new ObjectArrayList<String>();
        for (int index = 0; index < schema.variableLengthKeysCount(); index++) {
            variableTailKeys.add(schema.variableTailKey(schema.fixedLengthKeysCount() + index).name());
        }
        writeStringList(out, variableTailKeys);
        writeNullableString(out, schema.mergeOperatorName());
        writeNullableLong(out, schema.mergeOperatorVersion());
        writeNullableString(out, schema.mergeOperatorClass());
    }

    private static ColumnSchema readSchema(DataInputStream in) throws IOException {
        boolean hasValue = in.readBoolean();
        var fixedKeys = new IntArrayList();
        for (var value : readStringList(in)) {
            fixedKeys.add(Integer.parseInt(value));
        }
        var variableTailKeys = new ObjectArrayList<ColumnHashType>();
        for (var value : readStringList(in)) {
            variableTailKeys.add(ColumnHashType.valueOf(value));
        }
        return ColumnSchema.of(
                fixedKeys,
                variableTailKeys,
                hasValue,
                readNullableString(in),
                readNullableLong(in),
                readNullableString(in)
        );
    }

    private static void writePartition(DataOutputStream out, PartitionDescriptor partition) throws IOException {
        writeShardId(out, partition.partitionId());
        writeNullableBytes(out, partition.startKeyInclusive());
        writeNullableBytes(out, partition.endKeyExclusive());
        out.writeLong(partition.epoch());
        writeString(out, partition.ratisGroupId());
        writeReplicaMembers(out, partition.peers());
        writeNullableNodeId(out, partition.leaderHint());
        writeStringMap(out, partition.placementConstraints());
        writeString(out, partition.splitState().name());
    }

    private static PartitionDescriptor readPartition(DataInputStream in) throws IOException {
        return new PartitionDescriptor(
                readShardId(in),
                readNullableBytes(in),
                readNullableBytes(in),
                in.readLong(),
                readString(in),
                readReplicaMembers(in),
                readNullableNodeId(in),
                readStringMap(in),
                PartitionSplitState.valueOf(readString(in))
        );
    }

    private static void writeReplicaMembers(DataOutputStream out, List<ReplicaMember> peers) throws IOException {
        writeCount(out, peers.size());
        for (var peer : peers) {
            writeNodeId(out, peer.nodeId());
            writeString(out, peer.endpoint().toString());
            writeString(out, peer.role().name());
            out.writeLong(peer.matchIndex());
            out.writeLong(peer.appliedIndex());
            out.writeBoolean(peer.servingReads());
        }
    }

    private static List<ReplicaMember> readReplicaMembers(DataInputStream in) throws IOException {
        var count = readCount(in, 3 * Integer.BYTES + 2 * Long.BYTES + Byte.BYTES, "replica member");
        var peers = new java.util.ArrayList<ReplicaMember>(count);
        for (int index = 0; index < count; index++) {
            peers.add(new ReplicaMember(
                    readNodeId(in),
                    URI.create(readString(in)),
                    ReplicaRole.valueOf(readString(in)),
                    in.readLong(),
                    in.readLong(),
                    in.readBoolean()
            ));
        }
        return List.copyOf(peers);
    }

    private static void writeStringMap(DataOutputStream out, Map<String, String> map) throws IOException {
        var sorted = new TreeMap<>(map);
        writeCount(out, sorted.size());
        for (var entry : sorted.entrySet()) {
            writeString(out, entry.getKey());
            writeString(out, entry.getValue());
        }
    }

    private static Map<String, String> readStringMap(DataInputStream in) throws IOException {
        var count = readCount(in, 2 * Integer.BYTES, "map entry");
        var map = new LinkedHashMap<String, String>();
        for (int index = 0; index < count; index++) {
            var key = readString(in);
            var value = readString(in);
            if (map.put(key, value) != null) {
                throw new IllegalArgumentException("Duplicate metadata command map key: " + key);
            }
        }
        return Map.copyOf(map);
    }

    private static void writeStringList(DataOutputStream out, List<String> values) throws IOException {
        writeCount(out, values.size());
        for (var value : values) {
            writeString(out, value);
        }
    }

    private static List<String> readStringList(DataInputStream in) throws IOException {
        var count = readCount(in, Integer.BYTES, "string");
        var values = new java.util.ArrayList<String>(count);
        for (int index = 0; index < count; index++) {
            values.add(readString(in));
        }
        return List.copyOf(values);
    }

    private static void writeShardId(DataOutputStream out, ShardId shardId) throws IOException {
        out.writeLong(shardId.value());
    }

    private static ShardId readShardId(DataInputStream in) throws IOException {
        return new ShardId(in.readLong());
    }

    private static void writeNodeId(DataOutputStream out, NodeId nodeId) throws IOException {
        writeString(out, nodeId.value());
    }

    private static NodeId readNodeId(DataInputStream in) throws IOException {
        return NodeId.of(readString(in));
    }

    private static void writeNullableNodeId(DataOutputStream out, @Nullable NodeId nodeId) throws IOException {
        out.writeBoolean(nodeId != null);
        if (nodeId != null) {
            writeNodeId(out, nodeId);
        }
    }

    private static @Nullable NodeId readNullableNodeId(DataInputStream in) throws IOException {
        return in.readBoolean() ? readNodeId(in) : null;
    }

    private static void writeNullableBytes(DataOutputStream out, @Nullable Buf value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            writeBytes(out, value.toByteArray());
        }
    }

    private static @Nullable Buf readNullableBytes(DataInputStream in) throws IOException {
        return in.readBoolean() ? Buf.wrap(readBytes(in)).freeze() : null;
    }

    private static void writeNullableLong(DataOutputStream out, @Nullable Long value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            out.writeLong(value);
        }
    }

    private static @Nullable Long readNullableLong(DataInputStream in) throws IOException {
        return in.readBoolean() ? in.readLong() : null;
    }

    private static void writeNullableString(DataOutputStream out, @Nullable String value) throws IOException {
        out.writeBoolean(value != null);
        if (value != null) {
            writeString(out, value);
        }
    }

    private static @Nullable String readNullableString(DataInputStream in) throws IOException {
        return in.readBoolean() ? readString(in) : null;
    }

    private static void writeString(DataOutputStream out, String value) throws IOException {
        if (value.length() > MAX_METADATA_COMMAND_STRING_BYTES) {
            throw new IllegalArgumentException("Invalid metadata command string length: " + value.length());
        }
        writeLengthPrefixedBytes(
                out,
                value.getBytes(StandardCharsets.UTF_8),
                MAX_METADATA_COMMAND_STRING_BYTES,
                "metadata command string"
        );
    }

    private static String readString(DataInputStream in) throws IOException {
        var bytes = readLengthPrefixedBytes(
                in,
                MAX_METADATA_COMMAND_STRING_BYTES,
                "metadata command string"
        );
        try {
            return StandardCharsets.UTF_8.newDecoder()
                    .onMalformedInput(CodingErrorAction.REPORT)
                    .onUnmappableCharacter(CodingErrorAction.REPORT)
                    .decode(ByteBuffer.wrap(bytes))
                    .toString();
        } catch (CharacterCodingException e) {
            throw new IllegalArgumentException("Malformed metadata command UTF-8 string", e);
        }
    }

    private static void writeBytes(DataOutputStream out, byte[] value) throws IOException {
        writeLengthPrefixedBytes(out, value, MAX_METADATA_COMMAND_BINARY_BYTES, "metadata command byte array");
    }

    private static void writeLengthPrefixedBytes(DataOutputStream out,
                                                 byte[] value,
                                                 int maxLength,
                                                 String description) throws IOException {
        if (value.length > maxLength) {
            throw new IllegalArgumentException("Invalid " + description + " length: " + value.length);
        }
        out.writeInt(value.length);
        out.write(value);
    }

    private static byte[] readBytes(DataInputStream in) throws IOException {
        return readLengthPrefixedBytes(in, MAX_METADATA_COMMAND_BINARY_BYTES, "metadata command byte array");
    }

    private static byte[] readLengthPrefixedBytes(DataInputStream in,
                                                  int maxLength,
                                                  String description) throws IOException {
        int length = in.readInt();
        if (length < 0 || length > maxLength) {
            throw new IllegalArgumentException("Invalid " + description + " length: " + length);
        }
        if (length > in.available()) {
            throw new IllegalArgumentException("Unexpected end of metadata command payload");
        }
        byte[] value = in.readNBytes(length);
        if (value.length != length) {
            throw new IllegalArgumentException("Unexpected end of metadata command payload");
        }
        return value;
    }

    private static void writeCount(DataOutputStream out, int count) throws IOException {
        if (count < 0 || count > MAX_LIST_SIZE) {
            throw new IllegalArgumentException("Invalid metadata command list size: " + count);
        }
        out.writeInt(count);
    }

    private static int readCount(DataInputStream in, int minimumElementBytes, String description) throws IOException {
        var count = in.readInt();
        if (count < 0 || count > MAX_LIST_SIZE) {
            throw new IllegalArgumentException("Invalid metadata command list size: " + count);
        }
        if (count > in.available() / minimumElementBytes) {
            throw new IllegalArgumentException("Invalid metadata command " + description
                    + " count for remaining payload: " + count);
        }
        return count;
    }

}
