package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

/** Pure protocol fixtures; no native installation or quorum claim. */
class RatisSchemaInstallRpcTest {
    static UUID id(int i) { return new UUID(0,i); }
    static SchemaChange.Definition definition(int change,boolean create) {
        var column=new DistributedSchema.Column(1,id(4),"added",ColumnSchema.of(IntArrayList.of(4),ObjectArrayList.of(),true),null);
        var source=new DistributedSchema(id(1),id(2),id(3),1,0,create ? List.of() : List.of(column));
        var target=new DistributedSchema(id(1),id(2),id(3),2,0,create ? List.of(column) : List.of());
        var replica=new ReplicaDescriptor(id(5),id(6),id(7),"peer","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(id(8),id(9),1,2,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var directory=new RangeDirectory(id(1),id(2),id(3),1,source.digest(),3,source.columnIds(),0,List.of(range));
        return new SchemaChange.Definition(id(change),new OperationId(id(10),1,1),directory,source,target);
    }
    static RatisSchemaInstallRecord record(int change,boolean create) {
        return new RatisSchemaInstallRecord(definition(change,create),id(9),new ApplyPosition(3,8),create ? "physical-added" : null,create);
    }
    @Test void canonicalFullDefinitionAndPreparationEcho() {
        for (boolean create:new boolean[]{true,false}) {
            var record=record(11,create); assertEquals(record,RatisSchemaInstallRecord.decode(record.encode()));
            assertTrue(RatisSchemaInstallRecord.isInstall(record.encode()));
            assertArrayEquals(record.definition().encode(),RatisSchemaInstallRecord.decode(record.encode()).definition().encode());
            for (int kind:new int[]{ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE,ScopedTransactionRpc.SCHEMA_INSTALL_CANCEL}) {
                byte[] request=kind==ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE ? RatisSchemaInstallRpc.prepareRequest(record) : RatisSchemaInstallRpc.cancelRequest(record);
                assertEquals(record,RatisSchemaInstallRpc.request(request));
                byte[] reply=RatisSchemaInstallRpc.preparedReply(kind,record);
                assertEquals(record,RatisSchemaInstallRpc.decodePreparedReply(reply,kind,record));
                assertThrows(IllegalArgumentException.class,() -> RatisSchemaInstallRpc.decodePreparedReply(reply,kind,record(12,create)));
                int crossed=kind==ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE ? ScopedTransactionRpc.SCHEMA_INSTALL_CANCEL : ScopedTransactionRpc.SCHEMA_INSTALL_PREPARE;
                assertThrows(IllegalArgumentException.class,() -> RatisSchemaInstallRpc.decodePreparedReply(reply,crossed,record));
            }
        }
        var definition=definition(11,true);
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaInstallRecord(definition,id(99),new ApplyPosition(3,8),"name",false));
        for (String name:new String[]{"","é","zero\0name","x".repeat(4097)}) assertThrows(IllegalArgumentException.class,() -> new RatisSchemaInstallRecord(definition,id(9),new ApplyPosition(3,8),name,false));
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaInstallRecord(definition(11,false),id(9),new ApplyPosition(3,8),"name",false));
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaInstallRecord(definition(11,false),id(9),new ApplyPosition(3,8),null,true));
    }
    @Test void installationStateContainsOnlyPortableNativeProofAndOriginalFence() {
        var record=record(11,true); var proof=new NativeSchemaInstallProof(record.header(),record.groupId(),record.expectedFencePosition(),new ApplyPosition(3,9));
        for (var state:List.of(new RatisSchemaInstallRpc.State(record,null,false),new RatisSchemaInstallRpc.State(record,proof,false),new RatisSchemaInstallRpc.State(record,proof,true))) {
            byte[] reply=RatisSchemaInstallRpc.reply(ScopedTransactionRpc.SCHEMA_INSTALL_STATE,state);
            var decoded=RatisSchemaInstallRpc.stateReply(reply,ScopedTransactionRpc.SCHEMA_INSTALL_STATE,record);
            assertEquals(state.record(),decoded.record()); assertEquals(state.ready(),decoded.ready());
            if (proof==state.installation()) assertArrayEquals(proof.encode(),decoded.installation().encode()); else assertNull(decoded.installation());
            assertEquals(ScopedTransactionRpc.HEADER_BYTES+9+record.encode().length+(state.installation()==null ? 0 : NativeSchemaInstallProof.BYTES),reply.length);
            assertThrows(IllegalArgumentException.class,() -> RatisSchemaInstallRpc.stateReply(reply,ScopedTransactionRpc.SCHEMA_INSTALL_STATE,record(12,true)));
            byte[] flag=reply.clone(); flag[flag.length-1]=2;
            assertThrows(IllegalArgumentException.class,() -> RatisSchemaInstallRpc.stateReply(flag,ScopedTransactionRpc.SCHEMA_INSTALL_STATE,record));
        }
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaInstallRpc.State(record,null,true));
        var crossedFence=new NativeSchemaInstallProof(record.header(),record.groupId(),new ApplyPosition(3,7),new ApplyPosition(3,9));
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaInstallRpc.State(record,crossedFence,true));
        var wrongDigest=record.header().targetSchemaDigest(); wrongDigest[0]^=1;
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaInstallRpc.State(record,new NativeSchemaInstallProof(proof.fence(),wrongDigest,proof.position()),true));
    }
    @Test void rejectsTruncationNoncanonicalFlagsAndBounds() {
        var record=record(11,true); byte[] bytes=record.encode();
        for (int length:new int[]{0,4,44,bytes.length-1}) assertThrows(IllegalArgumentException.class,() -> RatisSchemaInstallRecord.decode(Arrays.copyOf(bytes,length)));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaInstallRecord.decode(Arrays.copyOf(bytes,bytes.length+1)));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaInstallRecord.decode(new byte[RatisSchemaInstallRecord.MAX_BYTES+1]));
        byte[] blob=bytes.clone(); ByteBuffer.wrap(blob).putInt(36,Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaInstallRecord.decode(blob));
        byte[] flag=bytes.clone(); flag[flag.length-1]=2;
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaInstallRecord.decode(flag));
        byte[] frame=RatisSchemaInstallRpc.prepareRequest(record);
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaInstallRpc.request(Arrays.copyOf(frame,frame.length-1)));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaInstallRpc.request(new byte[RatisSchemaInstallRpc.MAX_BYTES+1]));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaInstallRpc.request(RatisSchemaFenceRpc.prepareRequest(new RatisSchemaFenceRecord(record.header(),record.groupId()))));
    }
}
