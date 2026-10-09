package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Pure current-format fixtures; proof positions are controlled and establish no quorum or native installation. */
class RatisSchemaFenceRpcTest {
    private static UUID id(int i) { return new UUID(0,i); }
    private static SchemaChange.Definition definition(int change) {
        var source=new DistributedSchema(id(1),id(2),id(3),1,0,List.of());
        var column=new DistributedSchema.Column(1,id(4),"added",ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true),null);
        var target=new DistributedSchema(id(1),id(2),id(3),2,0,List.of(column));
        var replica=new ReplicaDescriptor(id(5),id(6),id(7),"peer","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(id(8),id(9),1,2,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var directory=new RangeDirectory(id(1),id(2),id(3),1,source.digest(),3,Set.of(),0,List.of(range));
        return new SchemaChange.Definition(id(change),new OperationId(id(10),1,1),directory,source,target);
    }
    @Test void fenceRecordAndPrepareCancelEchoExactIdentity() {
        var record=new RatisSchemaFenceRecord(definition(11).header(),id(9));
        assertEquals(record,RatisSchemaFenceRecord.decode(record.encode())); assertTrue(RatisSchemaFenceRecord.isFence(record.encode()));
        for (int kind:new int[]{ScopedTransactionRpc.SCHEMA_FENCE_PREPARE,ScopedTransactionRpc.SCHEMA_FENCE_CANCEL}) {
            byte[] request=kind==ScopedTransactionRpc.SCHEMA_FENCE_PREPARE ? RatisSchemaFenceRpc.prepareRequest(record) : RatisSchemaFenceRpc.cancelRequest(record);
            assertEquals(record,RatisSchemaFenceRpc.request(request));
            byte[] reply=RatisSchemaFenceRpc.preparedReply(kind,record);
            assertEquals(record,RatisSchemaFenceRpc.decodePreparedReply(reply,kind,record));
            assertThrows(IllegalArgumentException.class,() -> RatisSchemaFenceRpc.decodePreparedReply(reply,kind,new RatisSchemaFenceRecord(definition(12).header(),id(9))));
        }
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaFenceRecord(record.header(),id(99)));
        byte[] trailing=Arrays.copyOf(record.encode(),record.encode().length+1);
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaFenceRecord.decode(trailing));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaFenceRecord.decode(new byte[RatisSchemaFenceRecord.MAX_BYTES+1]));
        byte[] malformed=record.encode(); ByteBuffer.wrap(malformed).putInt(20,Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaFenceRecord.decode(malformed));
    }
    @Test void stateSeparatesRetainedFenceAndActualReadiness() {
        var record=new RatisSchemaFenceRecord(definition(11).header(),id(9));
        var proof=new NativeSchemaFence(record.header(),id(9),new ApplyPosition(3,8));
        for (var state:List.of(new RatisSchemaFenceRpc.State(record,null,0,0,0,false),
                new RatisSchemaFenceRpc.State(record,proof,1,2,3,false),new RatisSchemaFenceRpc.State(record,proof,0,0,0,true))) {
            byte[] reply=RatisSchemaFenceRpc.reply(ScopedTransactionRpc.SCHEMA_FENCE_STATE,state);
            assertEquals(state,RatisSchemaFenceRpc.stateReply(reply,ScopedTransactionRpc.SCHEMA_FENCE_STATE,record));
            assertThrows(IllegalArgumentException.class,() -> RatisSchemaFenceRpc.stateReply(reply,ScopedTransactionRpc.SCHEMA_FENCE_PREPARE,record));
            byte[] flag=reply.clone(); flag[flag.length-1]=2;
            assertThrows(IllegalArgumentException.class,() -> RatisSchemaFenceRpc.stateReply(flag,ScopedTransactionRpc.SCHEMA_FENCE_STATE,record));
            assertThrows(IllegalArgumentException.class,() -> RatisSchemaFenceRpc.stateReply(Arrays.copyOf(reply,reply.length-1),ScopedTransactionRpc.SCHEMA_FENCE_STATE,record));
        }
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaFenceRpc.State(record,null,0,0,0,true));
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaFenceRpc.State(record,proof,1,0,0,true));
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaFenceRpc.State(record,proof,-1,0,0,false));
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaFenceRpc.State(record,new NativeSchemaFence(definition(12).header(),id(9),new ApplyPosition(3,8)),0,0,0,true));
    }
    @Test void authorityEchoesIdentityAndCutWithOptionalDirectoryAndHead() {
        var definition=definition(11); var request=new RatisSchemaAuthority.Request(id(1),id(2),id(3));
        assertEquals(request,RatisSchemaAuthority.request(RatisSchemaAuthority.request(request)));
        var status=SchemaChange.Status.begin(definition.header());
        var fencing=status.beginFencing();
        var fenced=fencing.fenced(id(9),new ApplyPosition(3,8)); var installed=fenced.installed(id(9),new ApplyPosition(3,9));
        for (var phase:List.of(status,fencing,fenced,installed,installed.publish(),installed.publish().activated(id(9),new ApplyPosition(3,10)).retire(),status.abort())) {
            var head=new RatisSchemaAdministration.Head(phase,1);
            for (RangeDirectory directory:new RangeDirectory[]{null,definition.sourceDirectory(),definition.targetDirectory()}) {
                var view=new RatisSchemaAuthority.View(request,id(20),new ApplyPosition(3,12),directory,head);
                var decoded=RatisSchemaAuthority.decodeReply(RatisSchemaAuthority.reply(view),request,id(20));
                assertEquals(view.position(),decoded.position()); assertArrayEquals(head.encode(),decoded.head().encode());
                if (directory==null) assertNull(decoded.directory());
                else assertArrayEquals(RangeDirectoryCodec.encode(directory),RangeDirectoryCodec.encode(decoded.directory()));
            }
        }
        var absent=new RatisSchemaAuthority.View(request,id(20),new ApplyPosition(0,0),null,null); byte[] reply=RatisSchemaAuthority.reply(absent);
        assertEquals(absent,RatisSchemaAuthority.decodeReply(reply,request,id(20)));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaAuthority.decodeReply(reply,request,id(21)));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaAuthority.decodeReply(reply,new RatisSchemaAuthority.Request(id(1),id(2),id(99)),id(20)));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaAuthority.decodeReply(new byte[RatisSchemaAuthority.MAX_BYTES+1],request,id(20)));
        byte[] negative=reply.clone(); ByteBuffer.wrap(negative).putLong(ScopedTransactionRpc.HEADER_BYTES+64,-1);
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaAuthority.decodeReply(negative,request,id(20)));
        byte[] blob=reply.clone(); ByteBuffer.wrap(blob).putInt(ScopedTransactionRpc.HEADER_BYTES+80,Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaAuthority.decodeReply(blob,request,id(20)));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaAuthority.decodeReply(ScopedTransactionRpc.frame(ScopedTransactionRpc.SCHEMA_FENCE_STATE,Arrays.copyOfRange(reply,12,reply.length)),request,id(20)));
    }
}
