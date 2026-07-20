package it.cavallium.rockserver.core.cluster;

import it.cavallium.rockserver.core.common.ColumnSchema;

public record ClusterColumnMetadata(long columnId,
                                    String name,
                                    ColumnSchema schema,
                                    long schemaEpoch) {

    public ClusterColumnMetadata {
        if (columnId < 0) {
            throw new IllegalArgumentException("columnId must be >= 0");
        }
        SingleLineStrings.requireNonBlank(name, "column name");
        if (schema == null) {
            throw new IllegalArgumentException("schema must not be null");
        }
        if (schemaEpoch < 0) {
            throw new IllegalArgumentException("schemaEpoch must be >= 0");
        }
    }
}
