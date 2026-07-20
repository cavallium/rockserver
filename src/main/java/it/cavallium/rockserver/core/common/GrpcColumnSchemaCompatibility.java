package it.cavallium.rockserver.core.common;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public final class GrpcColumnSchemaCompatibility {

    private GrpcColumnSchemaCompatibility() {
    }

    public static void setMergeOperatorFields(
            it.cavallium.rockserver.core.common.api.proto.ColumnSchema.Builder builder,
            @NotNull ColumnSchema schema
    ) {
        if (schema.mergeOperatorName() != null) {
            builder.setMergeOperatorName(schema.mergeOperatorName());
        }
        if (schema.mergeOperatorVersion() != null) {
            builder.setMergeOperatorVersion(schema.mergeOperatorVersion());
        }
        if (schema.mergeOperatorClass() != null) {
            invokeMergeOperatorClass(builder, schema.mergeOperatorClass());
        }
    }

    @Nullable
    public static String getMergeOperatorClass(Object schema) {
        try {
            var hasMethod = schema.getClass().getMethod("hasMergeOperatorClass");
            Boolean has = (Boolean) hasMethod.invoke(schema);
            if (Boolean.TRUE.equals(has)) {
                var getter = schema.getClass().getMethod("getMergeOperatorClass");
                return (String) getter.invoke(schema);
            }
        } catch (NoSuchMethodException e) {
            return null;
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to read generated ColumnSchema mergeOperatorClass", e);
        }
        return null;
    }

    private static void invokeMergeOperatorClass(Object builder, String mergeOperatorClass) {
        try {
            var setter = builder.getClass().getMethod("setMergeOperatorClass", String.class);
            setter.invoke(builder, mergeOperatorClass);
        } catch (NoSuchMethodException ignored) {
            // Older generated protos do not expose mergeOperatorClass; ignore to stay backward compatible.
        } catch (ReflectiveOperationException e) {
            throw new IllegalStateException("Failed to write generated ColumnSchema mergeOperatorClass", e);
        }
    }
}
