package it.cavallium.rockserver.core.client;

import com.google.protobuf.ByteString;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.TransactionOpenScope;
import java.util.*;

/** The current gRPC and Thrift transaction declaration codecs. */
public final class TransactionScopeWire {
    private TransactionScopeWire() {}
    public static it.cavallium.rockserver.core.common.api.proto.TransactionOpenScope grpc(TransactionOpenScope scope) {
        var result = it.cavallium.rockserver.core.common.api.proto.TransactionOpenScope.newBuilder();
        for (var span : scope.spans()) {
            var value = it.cavallium.rockserver.core.common.api.proto.TransactionScopeSpan.newBuilder().setColumnId(span.columnId()).setPoint(span.point());
            if (span.lowerInclusive() != null) value.setLowerInclusive(grpcKeys(span.lowerInclusive()));
            if (span.upperExclusive() != null) value.setUpperExclusive(grpcKeys(span.upperExclusive()));
            result.addSpans(value);
        }
        return result.build();
    }
    private static it.cavallium.rockserver.core.common.api.proto.TransactionScopeKeys grpcKeys(Keys keys) {
        var value = it.cavallium.rockserver.core.common.api.proto.TransactionScopeKeys.newBuilder();
        for (var key : keys.keys()) value.addKeys(ByteString.copyFrom(key.toByteArray()));
        return value.build();
    }
    public static TransactionOpenScope grpc(it.cavallium.rockserver.core.common.api.proto.TransactionOpenScope scope) {
        if (scope.getSpansCount() > TransactionOpenScope.MAX_SPANS) throw new IllegalArgumentException("Too many transaction scope spans");
        long bytes = 0;
        for (var span : scope.getSpansList()) for (var keys : List.of(span.getLowerInclusive(), span.getUpperExclusive())) {
            if (keys.getKeysCount() > TransactionOpenScope.MAX_KEY_COMPONENTS) throw new IllegalArgumentException("Too many scope key components");
            for (var key : keys.getKeysList()) { bytes += key.size(); if (bytes > TransactionOpenScope.MAX_BOUND_BYTES) throw new IllegalArgumentException("Scope byte budget exceeded"); }
        }
        return new TransactionOpenScope(scope.getSpansList().stream().map(span -> new TransactionOpenScope.Span(span.getColumnId(), span.getPoint(),
                span.hasLowerInclusive() ? new Keys(span.getLowerInclusive().getKeysList().stream().map(key -> Buf.wrap(key.toByteArray())).toArray(Buf[]::new)) : null,
                span.hasUpperExclusive() ? new Keys(span.getUpperExclusive().getKeysList().stream().map(key -> Buf.wrap(key.toByteArray())).toArray(Buf[]::new)) : null)).toList());
    }
    public static it.cavallium.rockserver.core.common.api.TransactionOpenScope thrift(TransactionOpenScope scope) {
        if (scope == null) return null;
        return new it.cavallium.rockserver.core.common.api.TransactionOpenScope(scope.spans().stream().map(span -> {
            var value = new it.cavallium.rockserver.core.common.api.TransactionScopeSpan(span.columnId(), span.point());
            if (span.lowerInclusive() != null) value.setLowerInclusive(thriftKeys(span.lowerInclusive()));
            if (span.upperExclusive() != null) value.setUpperExclusive(thriftKeys(span.upperExclusive()));
            return value;
        }).toList());
    }
    private static it.cavallium.rockserver.core.common.api.TransactionScopeKeys thriftKeys(Keys keys) {
        return new it.cavallium.rockserver.core.common.api.TransactionScopeKeys(Arrays.stream(keys.keys()).map(key -> java.nio.ByteBuffer.wrap(key.toByteArray())).toList());
    }
    public static TransactionOpenScope thrift(it.cavallium.rockserver.core.common.api.TransactionOpenScope scope) {
        if (scope == null) return null;
        if (scope.getSpans() == null || scope.getSpansSize() > TransactionOpenScope.MAX_SPANS) throw new IllegalArgumentException("Invalid scope span count");
        long bytes = 0;
        for (var span : scope.getSpans()) {
            if (span == null || !span.isSetColumnId() || !span.isSetPoint()) throw new IllegalArgumentException("Missing scope span field");
            for (var keys : Arrays.asList(span.getLowerInclusive(), span.getUpperExclusive())) if (keys != null) {
                if (keys.getKeys() == null || keys.getKeysSize() > TransactionOpenScope.MAX_KEY_COMPONENTS) throw new IllegalArgumentException("Invalid scope key components");
                for (var key : keys.getKeys()) { bytes += Objects.requireNonNull(key).remaining(); if (bytes > TransactionOpenScope.MAX_BOUND_BYTES) throw new IllegalArgumentException("Scope byte budget exceeded"); }
            }
        }
        return new TransactionOpenScope(scope.getSpans().stream().map(span -> new TransactionOpenScope.Span(span.getColumnId(), span.isPoint(), thriftKeys(span.getLowerInclusive()), thriftKeys(span.getUpperExclusive()))).toList());
    }
    private static Keys thriftKeys(it.cavallium.rockserver.core.common.api.TransactionScopeKeys keys) {
        if (keys == null) return null;
        return new Keys(keys.getKeys().stream().map(key -> { var copy = key.duplicate(); byte[] bytes = new byte[copy.remaining()]; copy.get(bytes); return Buf.wrap(bytes); }).toArray(Buf[]::new));
    }
}
