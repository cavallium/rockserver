package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisCdcSubscriptionBatchTest {
    static UUID id(int n) { return new UUID(0,n); }
    final UUID selected=id(1),other=id(2),group=id(3);
    final DistributedCdcCheckpoint.Lineage lineage=new DistributedCdcCheckpoint.Lineage(id(4),group,1);
    DistributedCdcSubscription state(DistributedCdcCheckpoint.Cursor from) {
        var definition=new DistributedCdcSubscription.Definition(id(5),id(6),id(7),id(8),id(9),new byte[32],List.of(selected),new DistributedCdcCheckpoint(Map.of(lineage,from)));
        return DistributedCdcSubscription.initial(definition);
    }
    RatisCdcPollRpc.Result result(DistributedCdcSubscription state,DistributedCdcRecord record) {
        var d=state.definition(); var request=new RatisCdcPollRpc.Request(new RatisCdcPollRpc.Binding(d.clusterId(),d.databaseId(),d.domainId(),lineage),state.checkpoint().cursors().get(lineage),1,4096);
        return new RatisCdcPollRpc.Result(request,RatisCdcPollRpc.Outcome.DATA,new ApplyPosition(1,1),DistributedCdcCheckpoint.Cursor.beginning(),record.end(),true,0,List.of(record));
    }
    @Test void selectedProjectionKeepsOriginalOrdinalsAndOperandsWithoutPredicatesOrUnselectedValues() {
        var state=state(new DistributedCdcCheckpoint.Cursor(new ApplyPosition(1,1),0));
        var command=new DistributedCommand(group,1,new byte[32],new OperationId(id(10),1,0),List.of(
                new Mutation(Mutation.Kind.PUT,selected,new byte[]{1},new byte[]{2}),new Mutation(Mutation.Kind.PUT,other,new byte[]{3},new byte[]{99}),
                new Mutation(Mutation.Kind.MERGE,selected,new byte[]{4},new byte[]{5})),List.of(new KeyPredicate(other,new byte[]{6},new byte[]{98})));
        var record=new DistributedCdcRecord(lineage,new ApplyPosition(1,1),command,null); var reply=result(state,record); var batch=new RatisCdcSubscriptionBatch(id(11),state,reply);
        assertEquals(1,batch.envelopes().size()); var envelope=batch.envelopes().getFirst(); assertEquals(1,envelope.events().size());
        assertEquals(2,envelope.events().getFirst().cursor().mutationOrdinal()); assertEquals(Mutation.Kind.MERGE,envelope.events().getFirst().mutation().kind());
        assertArrayEquals(new byte[]{5},envelope.events().getFirst().mutation().value()); assertEquals(record.end(),batch.next()); assertEquals(command.operationId(),envelope.operation());
        assertThrows(UnsupportedOperationException.class,()->envelope.events().clear());
        byte[] digest=envelope.schemaDigest(); digest[0]=1; assertEquals(0,envelope.schemaDigest()[0]);
    }
    @Test void batchesCannotBindClosedOrDifferentAcknowledgedState() {
        var state=state(DistributedCdcCheckpoint.Cursor.beginning());
        var command=new DistributedCommand(group,1,new byte[32],new OperationId(id(10),1,0),List.of(new Mutation(Mutation.Kind.PUT,selected,new byte[]{1},new byte[]{2})));
        var record=new DistributedCdcRecord(lineage,new ApplyPosition(1,1),command,null); var reply=result(state,record);
        assertThrows(IllegalArgumentException.class,()->new RatisCdcSubscriptionBatch(id(11),state.close(),reply));
        var later=state.deliver(new DistributedCdcCheckpoint(Map.of(lineage,record.end()))).acknowledge(new DistributedCdcCheckpoint(Map.of(lineage,record.end())));
        assertThrows(IllegalArgumentException.class,()->new RatisCdcSubscriptionBatch(id(11),later,reply));
    }
}
