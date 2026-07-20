package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.lang.reflect.InvocationTargetException;

import org.junit.jupiter.api.Test;

class GrpcColumnSchemaCompatibilityTest {

    private static final Class<it.cavallium.rockserver.core.common.api.proto.ColumnSchema> PROTO_SCHEMA =
            it.cavallium.rockserver.core.common.api.proto.ColumnSchema.class;

    @Test
    void clientSchemaMappingPreservesMergeOperatorClass() {
        var source = localSchema();

        var proto = invokeStatic(GrpcConnection.class, "mapColumnSchema", ColumnSchema.class, source, PROTO_SCHEMA);
        var roundTrip = invokeStatic(GrpcConnection.class, "unmapColumnSchema", PROTO_SCHEMA, proto, ColumnSchema.class);

        assertTrue(proto.hasMergeOperatorClass());
        assertEquals("it.example.MergeOperator", proto.getMergeOperatorClass());
        assertEquals(source, roundTrip);
    }

    @Test
    void serverSchemaMappingPreservesMergeOperatorClass() throws ClassNotFoundException {
        var serverImpl = Class.forName("it.cavallium.rockserver.core.server.GrpcServer$GrpcServerImpl");
        var source = protoSchema();

        var local = invokeStatic(serverImpl, "mapColumnSchema", PROTO_SCHEMA, source, ColumnSchema.class);
        var roundTrip = invokeStatic(serverImpl, "unmapColumnSchema", ColumnSchema.class, local, PROTO_SCHEMA);

        assertEquals("it.example.MergeOperator", local.mergeOperatorClass());
        assertTrue(roundTrip.hasMergeOperatorClass());
        assertEquals(source, roundTrip);
    }

    private static ColumnSchema localSchema() {
        return ColumnSchema.of(
                IntArrayList.of(4, 8),
                ObjectArrayList.of(),
                true,
                "merge",
                7L,
                "it.example.MergeOperator");
    }

    private static it.cavallium.rockserver.core.common.api.proto.ColumnSchema protoSchema() {
        return it.cavallium.rockserver.core.common.api.proto.ColumnSchema.newBuilder()
                .addFixedKeys(4)
                .addFixedKeys(8)
                .setHasValue(true)
                .setMergeOperatorName("merge")
                .setMergeOperatorVersion(7L)
                .setMergeOperatorClass("it.example.MergeOperator")
                .build();
    }

    private static <T> T invokeStatic(Class<?> owner,
                                      String methodName,
                                      Class<?> parameterType,
                                      Object argument,
                                      Class<T> resultType) {
        try {
            var method = owner.getDeclaredMethod(methodName, parameterType);
            method.setAccessible(true);
            return resultType.cast(method.invoke(null, argument));
        } catch (NoSuchMethodException | IllegalAccessException e) {
            throw new AssertionError("Unable to invoke " + owner.getName() + "." + methodName, e);
        } catch (InvocationTargetException e) {
            var cause = e.getCause();
            if (cause instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (cause instanceof Error error) {
                throw error;
            }
            throw new AssertionError("Unexpected checked failure from " + owner.getName() + "." + methodName, cause);
        }
    }
}
