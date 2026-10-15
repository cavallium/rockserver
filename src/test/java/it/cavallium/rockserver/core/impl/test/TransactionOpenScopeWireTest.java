package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.TransactionScopeWire;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.TransactionOpenScope;
import it.cavallium.rockserver.core.common.api.proto.OpenTransactionRequest;
import java.util.*;
import org.apache.thrift.TDeserializer;
import org.apache.thrift.TSerializer;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class TransactionOpenScopeWireTest {
    @Test void codecsDistinguishAbsentScopeFromExplicitScopeAndAbsentFromEmptyBounds() throws Exception {
        var scope=new TransactionOpenScope(List.of(TransactionOpenScope.Span.point(71,new Keys(Buf.wrap(new byte[]{1}))),
                TransactionOpenScope.Span.range(72,new Keys(),null)));
        var absent=OpenTransactionRequest.parseFrom(OpenTransactionRequest.newBuilder().setTransactionLeaseTtlNanos(123).build().toByteArray());
        assertFalse(absent.hasScope());
        var explicit=OpenTransactionRequest.parseFrom(absent.toBuilder().setScope(TransactionScopeWire.grpc(scope)).build().toByteArray());
        assertTrue(explicit.hasScope());assertEquals(scope,TransactionScopeWire.grpc(explicit.getScope()));
        var noScope=new it.cavallium.rockserver.core.common.api.RocksDB.openTransaction_args().setTransactionLeaseTtlNanos(123)
                .setContext(new it.cavallium.rockserver.core.common.api.RequestContext(it.cavallium.rockserver.core.common.api.WorkloadProfile.BATCH,123));
        var decoded=new it.cavallium.rockserver.core.common.api.RocksDB.openTransaction_args();
        var serializer=new TSerializer();var deserializer=new TDeserializer();
        deserializer.deserialize(decoded,serializer.serialize(noScope));assertFalse(decoded.isSetScope());
        noScope.setScope(TransactionScopeWire.thrift(scope));deserializer.deserialize(decoded,serializer.serialize(noScope));
        assertTrue(decoded.isSetScope());assertEquals(scope,TransactionScopeWire.thrift(decoded.getScope()));
        assertNotNull(TransactionScopeWire.thrift(decoded.getScope()).spans().get(1).lowerInclusive());
        assertNull(TransactionScopeWire.thrift(decoded.getScope()).spans().get(1).upperExclusive());
        assertThrows(IllegalArgumentException.class,()->TransactionScopeWire.grpc(it.cavallium.rockserver.core.common.api.proto.TransactionOpenScope.getDefaultInstance()));
    }
    @Test void declarationOwnsAllKeysAndRejectsAggregateAndCountOverflow() {
        byte[] raw={1};var keys=new Keys(Buf.wrap(raw));
        var point=TransactionOpenScope.Span.point(71,keys);var scope=new TransactionOpenScope(List.of(point));
        raw[0]=2;keys.keys()[0]=Buf.wrap(new byte[]{3});scope.spans().getFirst().lowerInclusive().keys()[0]=Buf.wrap(new byte[]{4});
        assertEquals(new Keys(Buf.wrap(new byte[]{1})),scope.spans().getFirst().lowerInclusive());
        assertThrows(IllegalArgumentException.class,()->new TransactionOpenScope(Collections.nCopies(TransactionOpenScope.MAX_SPANS+1,point)));
        var large=TransactionOpenScope.Span.point(71,new Keys(Buf.wrap(new byte[TransactionOpenScope.MAX_BOUND_BYTES/2+1])));
        assertThrows(IllegalArgumentException.class,()->new TransactionOpenScope(List.of(large,large)));
        assertThrows(IllegalArgumentException.class,()->TransactionOpenScope.Span.point(71,null));
    }
}
