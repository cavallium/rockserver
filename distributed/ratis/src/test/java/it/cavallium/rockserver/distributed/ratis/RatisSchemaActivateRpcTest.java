package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static it.cavallium.rockserver.distributed.ratis.RatisSchemaInstallRpcTest.*;
import static org.junit.jupiter.api.Assertions.*;

class RatisSchemaActivateRpcTest {
    private static RatisSchemaActivateRpc.Request request(int change) { return new RatisSchemaActivateRpc.Request(definition(change,true).header(),id(9),new ApplyPosition(3,9)); }
    private static RatisSchemaActivateRecord frozen(RatisSchemaActivateRpc.Request input) { return new RatisSchemaActivateRecord(input.header(),input.groupId(),input.expectedInstallPosition(),id(20),new ApplyPosition(2,4)); }
    @Test void preparationReturnsFrozenPublicationCutAndCancellationOnlyEchoesRequest() {
        var request=request(11); var record=frozen(request);
        assertEquals(record,RatisSchemaActivateRecord.decode(record.encode())); assertTrue(RatisSchemaActivateRecord.isActivate(record.encode()));
        assertEquals(request,RatisSchemaActivateRpc.Request.decode(request.encode()));
        for (int kind:new int[]{ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,ScopedTransactionRpc.SCHEMA_ACTIVATE_CANCEL}) {
            byte[] query=kind==ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE ? RatisSchemaActivateRpc.prepareRequest(request) : RatisSchemaActivateRpc.cancelRequest(request);
            assertEquals(request,RatisSchemaActivateRpc.request(query));
            var expected=kind==ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE ? record : null;
            byte[] reply=RatisSchemaActivateRpc.preparedReply(kind,request,expected);
            assertEquals(expected,RatisSchemaActivateRpc.decodePreparedReply(reply,kind,request));
            assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRpc.decodePreparedReply(reply,kind,request(12)));
        }
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRpc.preparedReply(ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,request,null));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRpc.preparedReply(ScopedTransactionRpc.SCHEMA_ACTIVATE_CANCEL,request,record));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRpc.preparedReply(ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,request,frozen(request(12))));
        var crossed=new RatisSchemaActivateRecord(request.header(),request.groupId(),new ApplyPosition(3,10),id(20),new ApplyPosition(2,4));
        assertThrows(IllegalArgumentException.class,() -> request.requireRecord(crossed));
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaActivateRecord(request.header(),id(99),request.expectedInstallPosition(),id(20),new ApplyPosition(2,4)));
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaActivateRecord(request.header(),request.groupId(),request.expectedInstallPosition(),request.groupId(),new ApplyPosition(2,4)));
    }
    @Test void frozenPreparationRetainsTheExactOriginalCatalogueCut() {
        var original=request(11); var record=frozen(original);
        var frozen=new RatisSchemaActivateRpc.Request(original.header(),original.groupId(),original.expectedInstallPosition(),record);
        assertEquals(frozen,RatisSchemaActivateRpc.request(RatisSchemaActivateRpc.prepareRequest(frozen)));
        assertEquals(record,RatisSchemaActivateRpc.decodePreparedReply(RatisSchemaActivateRpc.preparedReply(ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,frozen,record),ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,frozen));
        var rebased=new RatisSchemaActivateRecord(original.header(),original.groupId(),original.expectedInstallPosition(),record.catalogueGroup(),new ApplyPosition(2,5));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRpc.preparedReply(ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,frozen,rebased));
        var install=new NativeSchemaInstallProof(original.header(),original.groupId(),new ApplyPosition(3,8),original.expectedInstallPosition());
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaActivateRpc.State(frozen,new NativeSchemaActivationProof(install,record.catalogueGroup(),rebased.cataloguePosition(),new ApplyPosition(3,10)),true));
    }
    @Test void activationStateBindsNativeInstallationAndSeparatesReadiness() {
        var request=request(11); var install=new NativeSchemaInstallProof(request.header(),request.groupId(),new ApplyPosition(3,8),request.expectedInstallPosition());
        var proof=new NativeSchemaActivationProof(install,id(20),new ApplyPosition(2,4),new ApplyPosition(3,10));
        for (var state:List.of(new RatisSchemaActivateRpc.State(request,null,false),new RatisSchemaActivateRpc.State(request,proof,false),new RatisSchemaActivateRpc.State(request,proof,true))) {
            byte[] reply=RatisSchemaActivateRpc.reply(ScopedTransactionRpc.SCHEMA_ACTIVATE_STATE,state);
            var decoded=RatisSchemaActivateRpc.stateReply(reply,ScopedTransactionRpc.SCHEMA_ACTIVATE_STATE,request);
            assertEquals(state.request(),decoded.request()); assertEquals(state.ready(),decoded.ready());
            if (state.activation()==null) assertNull(decoded.activation()); else assertArrayEquals(proof.encode(),decoded.activation().encode());
            assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRpc.stateReply(reply,ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,request));
            assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRpc.stateReply(reply,ScopedTransactionRpc.SCHEMA_ACTIVATE_STATE,request(12)));
            byte[] flag=reply.clone(); flag[flag.length-1]=2;
            assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRpc.stateReply(flag,ScopedTransactionRpc.SCHEMA_ACTIVATE_STATE,request));
        }
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaActivateRpc.State(request,null,true));
        var otherInstall=new NativeSchemaInstallProof(request.header(),request.groupId(),new ApplyPosition(3,8),new ApplyPosition(3,10));
        assertThrows(IllegalArgumentException.class,() -> new RatisSchemaActivateRpc.State(request,new NativeSchemaActivationProof(otherInstall,id(20),new ApplyPosition(2,4),new ApplyPosition(3,11)),true));
    }
    @Test void rejectsInvalidFramesCutsAndLengths() {
        var request=request(11); byte[] bytes=frozen(request).encode();
        for (int length:new int[]{0,4,71,bytes.length-1}) assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRecord.decode(Arrays.copyOf(bytes,length)));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRecord.decode(Arrays.copyOf(bytes,bytes.length+1)));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRecord.decode(new byte[RatisSchemaActivateRecord.MAX_BYTES+1]));
        byte[] negative=bytes.clone(); ByteBuffer.wrap(negative).putLong(20,-1);
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRecord.decode(negative));
        byte[] blob=bytes.clone(); ByteBuffer.wrap(blob).putInt(68,Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRecord.decode(blob));
        byte[] reply=RatisSchemaActivateRpc.preparedReply(ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,request,frozen(request));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRpc.decodePreparedReply(Arrays.copyOf(reply,reply.length-1),ScopedTransactionRpc.SCHEMA_ACTIVATE_PREPARE,request));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRpc.request(new byte[RatisSchemaActivateRpc.MAX_BYTES+1]));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaActivateRpc.request(RatisSchemaInstallRpc.prepareRequest(record(11,true))));
    }
}
