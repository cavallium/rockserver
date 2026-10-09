package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class SnapshotFenceTest {
    final UUID a=new UUID(0,10),b=new UUID(-1,20),column=new UUID(0,30);
    OperationId operation(int n) { return new OperationId(new UUID(0,40),1,n); }
    SnapshotFence.Participant participant(UUID group,int n,TransactionScope scope) { return new SnapshotFence.Participant(group,group,1,new byte[32],operation(n),operation(n+1),scope); }
    TransactionScope scope() { return new TransactionScope(List.of(TransactionScope.Span.range(column,new byte[]{1},new byte[]{8}),TransactionScope.Span.point(column,new byte[]{3}))); }
    SnapshotFence fence() { return new SnapshotFence(new UUID(0,1),new UUID(0,2),new UUID(0,3),new UUID(0,4),new UUID(0,5),1,new OperationId(new UUID(0,4),1,0),2,3,new byte[32],List.of(participant(b,2,scope()),participant(a,0,scope()))); }
    @Test void canonicalDefinitionOwnsSchemaAndScopesAndFreezesEveryIdentity() {
        byte[] lower={1},upper={8},schema=new byte[32],participantSchema=new byte[32];
        var scope=new TransactionScope(List.of(TransactionScope.Span.range(column,lower,upper),TransactionScope.Span.point(column,new byte[]{3})));
        var participant=new SnapshotFence.Participant(a,a,1,participantSchema,operation(0),operation(1),scope);
        var reference=fence(); var fence=new SnapshotFence(reference.clusterId(),reference.databaseId(),reference.domainId(),reference.fenceId(),reference.homeGroup(),1,reference.operation(),2,3,schema,List.of(participant,participant(b,2,scope())));
        lower[0]=4; upper[0]=5; schema[0]=1; participantSchema[0]=2;
        assertEquals(reference,fence); assertEquals(a,fence.participants().getFirst().groupId()); assertEquals(participant,fence.participant(a));
        assertEquals(0,fence.schemaDigest()[0]); fence.schemaDigest()[0]=3; participant.schemaDigest()[0]=4;
        participant.scope().spans().getFirst().lowerInclusive()[0]=5; assertEquals(reference,fence);
        byte[] bytes=fence.encode(); var decoded=SnapshotFence.decode(bytes); Arrays.fill(bytes,(byte)0);
        assertEquals(fence,decoded); assertEquals(fence.hashCode(),decoded.hashCode()); assertEquals(participant.hashCode(),decoded.participant(a).hashCode());
        var reordered=new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{3}),TransactionScope.Span.range(column,new byte[]{1},new byte[]{8})));
        assertEquals(participant,participant(a,0,reordered));
        assertNotEquals(fence,new SnapshotFence(fence.clusterId(),fence.databaseId(),fence.domainId(),fence.fenceId(),fence.homeGroup(),1,fence.operation(),3,3,fence.schemaDigest(),fence.participants()));
        var changed=participant(a,0,new TransactionScope(List.of(TransactionScope.Span.range(column,new byte[]{1},new byte[]{9}))));
        assertNotEquals(fence,new SnapshotFence(fence.clusterId(),fence.databaseId(),fence.domainId(),fence.fenceId(),fence.homeGroup(),1,fence.operation(),2,3,fence.schemaDigest(),List.of(changed,fence.participant(b))));
    }
    @Test void participantControlAndAggregateBoundsAreEnforcedBeforeEncoding() {
        var fence=fence();
        assertThrows(IllegalArgumentException.class,() -> new SnapshotFence(fence.clusterId(),fence.databaseId(),fence.domainId(),fence.fenceId(),fence.homeGroup(),1,fence.operation(),2,3,new byte[32],List.of(fence.participant(a),fence.participant(a))));
        var duplicate=new SnapshotFence.Participant(b,b,1,new byte[32],operation(0),operation(3),scope());
        assertThrows(IllegalArgumentException.class,() -> new SnapshotFence(fence.clusterId(),fence.databaseId(),fence.domainId(),fence.fenceId(),fence.homeGroup(),1,fence.operation(),2,3,new byte[32],List.of(fence.participant(a),duplicate)));
        var huge=participant(a,0,new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[SnapshotFence.MAX_BYTES]))));
        assertThrows(IllegalArgumentException.class,() -> new SnapshotFence(fence.clusterId(),fence.databaseId(),fence.domainId(),fence.fenceId(),fence.homeGroup(),1,fence.operation(),2,3,new byte[32],List.of(huge)));
        assertThrows(IllegalArgumentException.class,() -> fence.participant(UUID.randomUUID()));
    }
    @Test void definitionEqualityBindsEveryIdentityAndPhysicalScopeField() {
        var reference=fence(); byte[] canonical=reference.encode();
        int[] fields={0,16,32,48,64,87,88,111,119,127,135,136,172,188,211,212,244,267,275,276,299,307,338,355,360};
        for (int offset:fields) {
            byte[] bytes=canonical.clone(); bytes[offset]^=(byte)(offset==355 ? 1 : 16);
            var changed=SnapshotFence.decode(bytes);
            assertNotEquals(reference,changed,"Changed identity or scope byte at "+offset);
            var restored=SnapshotFence.decode(changed.encode());
            assertEquals(changed,restored); assertEquals(changed.hashCode(),restored.hashCode());
        }
        byte[] range=canonical.clone(); range[339]=2;
        assertNotEquals(reference,SnapshotFence.decode(range));
    }
    @Test void malformedAndNoncanonicalDefinitionFramesAreRejected() {
        byte[] bytes=fence().encode();
        for (int n=0;n<bytes.length;n++) { byte[] truncated=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> SnapshotFence.decode(truncated)); }
        assertThrows(IllegalArgumentException.class,() -> SnapshotFence.decode(Arrays.copyOf(bytes,bytes.length+1)));
        byte[] count=bytes.clone(); ByteBuffer.wrap(count).putInt(SnapshotFence.HEADER_BYTES-4,65); assertThrows(IllegalArgumentException.class,() -> SnapshotFence.decode(count));
        byte[] kind=bytes.clone(); kind[SnapshotFence.HEADER_BYTES+140]=3; assertThrows(IllegalArgumentException.class,() -> SnapshotFence.decode(kind));
        byte[] spanCount=bytes.clone(); ByteBuffer.wrap(spanCount).putInt(SnapshotFence.HEADER_BYTES+136,-1); assertThrows(IllegalArgumentException.class,() -> SnapshotFence.decode(spanCount));
        byte[] bound=bytes.clone(); ByteBuffer.wrap(bound).putInt(SnapshotFence.HEADER_BYTES+157,Integer.MAX_VALUE); assertThrows(IllegalArgumentException.class,() -> SnapshotFence.decode(bound));
    }
}
