package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;

import org.jetbrains.annotations.Nullable;

public record SchemaChangeMetadata(long schemaChangeId,
                                   SchemaChangeKind changeKind,
                                   long columnId,
                                   long targetSchemaEpoch,
                                   @Nullable ClusterColumnMetadata column,
                                   @Nullable MergeOperatorActivation mergeOperatorActivation,
                                   SchemaChangeStatus status) {

    public SchemaChangeMetadata {
        requireNonNegative(schemaChangeId, "schemaChangeId");
        requireArgument(changeKind, "changeKind");
        requireNonNegative(columnId, "columnId");
        requireNonNegative(targetSchemaEpoch, "targetSchemaEpoch");
        requireArgument(status, "status");
        validatePayload(changeKind, columnId, targetSchemaEpoch, column, mergeOperatorActivation);
    }

    public static SchemaChangeMetadata prepared(MetadataCommand.PrepareSchemaChange command) {
        return new SchemaChangeMetadata(
                command.schemaChangeId(),
                command.changeKind(),
                command.columnId(),
                command.targetSchemaEpoch(),
                command.column(),
                command.mergeOperatorActivation(),
                SchemaChangeStatus.PREPARED
        );
    }

    public SchemaChangeMetadata withStatus(SchemaChangeStatus nextStatus) {
        return new SchemaChangeMetadata(
                schemaChangeId,
                changeKind,
                columnId,
                targetSchemaEpoch,
                column,
                mergeOperatorActivation,
                nextStatus
        );
    }

    private static void validatePayload(SchemaChangeKind changeKind,
                                        long columnId,
                                        long targetSchemaEpoch,
                                        @Nullable ClusterColumnMetadata column,
                                        @Nullable MergeOperatorActivation mergeOperatorActivation) {
        switch (changeKind) {
            case CREATE_COLUMN -> {
                requireArgument(column, "column");
                if (column.columnId() != columnId) {
                    throw new IllegalArgumentException("prepared column id must match columnId");
                }
                if (column.schemaEpoch() != targetSchemaEpoch) {
                    throw new IllegalArgumentException("prepared column schema epoch must match targetSchemaEpoch");
                }
                if (column.schema().mergeOperatorClass() != null) {
                    throw new IllegalArgumentException("local mergeOperatorClass is not allowed in replicated metadata");
                }
                if (mergeOperatorActivation != null) {
                    throw new IllegalArgumentException("create-column schema change must not include merge activation");
                }
            }
            case DELETE_COLUMN -> {
                if (column != null || mergeOperatorActivation != null) {
                    throw new IllegalArgumentException("delete-column schema change must not include payload metadata");
                }
            }
            case MERGE_OPERATOR_ACTIVATION -> {
                if (column != null) {
                    throw new IllegalArgumentException("merge-operator activation must not include column metadata");
                }
                requireArgument(mergeOperatorActivation, "mergeOperatorActivation");
            }
        }
    }

    private static void requireNonNegative(long value, String name) {
        if (value < 0) {
            throw new IllegalArgumentException(name + " must be >= 0");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
