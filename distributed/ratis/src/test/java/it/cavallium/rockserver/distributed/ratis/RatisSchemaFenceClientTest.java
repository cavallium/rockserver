package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisSchemaFenceClientTest {
    final UUID cluster=new UUID(1,1),database=new UUID(1,2),domain=new UUID(1,3),column=new UUID(1,4),group=new UUID(1,5);
    final ReplicaDescriptor replica=new ReplicaDescriptor(new UUID(2,1),new UUID(2,2),new UUID(2,3),"original","127.0.0.1:1",ReplicaDescriptor.Role.VOTER);
    DistributedSchema schema(long epoch,List<DistributedSchema.Column> columns) { return new DistributedSchema(cluster,database,domain,epoch,0,columns); }
    DistributedSchema.Column column(long id,UUID uuid,String name) { return new DistributedSchema.Column(id,uuid,name,ColumnSchema.of(IntArrayList.of(8),ObjectArrayList.of(),true),null); }
    SchemaChange.Definition definition() {
        var original=schema(1,List.of(column(1,column,"original"))); var target=schema(2,List.of(original.column(1),column(2,new UUID(1,6),"added")));
        var descriptor=new RangeDescriptor(new UUID(3,1),group,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var directory=new RangeDirectory(cluster,database,domain,1,original.digest(),0,original.columnIds(),0,List.of(descriptor));
        return new SchemaChange.Definition(new UUID(3,2),new OperationId(new UUID(3,3),1,0),directory,original,target);
    }
    class Transport implements RatisRoutedClient.Transport {
        final SchemaChange.Definition definition=definition(); final AtomicInteger prepares=new AtomicInteger(),writes=new AtomicInteger();
        byte[] reply; boolean badEcho;
        public byte[] read(RangeDescriptor descriptor,UUID column,byte[] key) { throw new AssertionError("No DATA read"); }
        public CommandResult command(RangeDescriptor descriptor,DistributedCommand command) { throw new AssertionError("No DATA command"); }
        public byte[] transaction(RangeDescriptor descriptor,byte[] request,boolean fresh) {
            assertEquals(definition.sourceDirectory().ranges().getFirst(),descriptor); assertTrue(fresh);
            var record=RatisSchemaFenceRpc.request(request); prepares.incrementAndGet();
            int kind=ScopedTransactionRpc.kind(request,ScopedTransactionRpc.MAX_REQUEST_BYTES);
            if (kind==ScopedTransactionRpc.SCHEMA_FENCE_STATE) return RatisSchemaFenceRpc.reply(kind,
                    new RatisSchemaFenceRpc.State(record,new NativeSchemaFence(record.header(),group,new ApplyPosition(1,5)),0,0,0,true));
            if (badEcho) record=new RatisSchemaFenceRecord(new SchemaChange.Definition(new UUID(3,99),definition.beginOperation(),definition.sourceDirectory(),definition.sourceSchema(),definition.targetSchema()).header(),group);
            return RatisSchemaFenceRpc.preparedReply(kind,record);
        }
        public byte[] participant(RangeDescriptor descriptor,byte[] record) {
            assertEquals(definition.sourceDirectory().ranges().getFirst(),descriptor); writes.incrementAndGet();
            var decoded=RatisSchemaFenceRecord.decode(record);
            return reply==null ? new NativeSchemaFence(decoded.header(),group,new ApplyPosition(1,5)).encode() : reply;
        }
    }
    @Test void originalControlRouteSurvivesRouterRefreshAndRepliesRemainQualified() throws Exception {
        var transport=new Transport(); var definition=transport.definition;
        try (var client=new RatisRoutedClient(definition.sourceDirectory(),transport)) {
            var target=client.schemaTarget(definition,group);
            var changed=new RangeDescriptor(new UUID(3,1),group,2,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
            client.refresh(new RangeDirectory(cluster,database,domain,1,definition.sourceSchema().digest(),1,definition.sourceSchema().columnIds(),0,List.of(changed)));
            var proof=client.fenceSchema(target); proof.requireHeader(definition.header());
            assertEquals(new ApplyPosition(1,5),proof.position()); assertTrue(client.schemaFenceState(target).ready());
            client.cancelSchemaPreparation(target); assertEquals(1,transport.writes.get());
        }
    }
    @Test void malformedPrepareAndIndependentProofIdentityNeverBecomeSuccessfulFence() throws Exception {
        var transport=new Transport(); var definition=transport.definition;
        try (var client=new RatisRoutedClient(definition.sourceDirectory(),transport)) {
            var target=client.schemaTarget(definition,group); transport.badEcho=true;
            assertThrows(IOException.class,() -> client.fenceSchema(target)); assertEquals(0,transport.writes.get());
            transport.badEcho=false; var corrupt=new NativeSchemaFence(definition.header(),group,new ApplyPosition(1,5)).encode(); corrupt[75]^=1;
            transport.reply=corrupt; assertThrows(IOException.class,() -> client.fenceSchema(target));
            assertEquals(1,transport.writes.get());
        }
    }
}
