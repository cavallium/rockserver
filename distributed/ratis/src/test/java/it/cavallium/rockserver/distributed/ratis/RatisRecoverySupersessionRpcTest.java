package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisRecoverySupersessionRpcTest {
    static UUID id(int n){return new UUID(0,n);}
    final ReplicaDescriptor receiver=new ReplicaDescriptor(id(1),id(2),id(3),"receiver","127.0.0.1:7301",ReplicaDescriptor.Role.VOTER);
    final DistributedSchema schema=new DistributedSchema(id(4),id(5),id(6),1,0,List.of());
    final RangeDescriptor parent=range(7,8,null,null);
    final RangeDirectory directory=directory(0,List.of(parent));
    final TopologyChange.Definition split=new TopologyChange.Definition(id(9),directory,directory(1,List.of(range(10,11,null,new byte[]{(byte)128}),range(12,13,new byte[]{(byte)128},null))));
    RangeDescriptor range(int range,int group,byte[] lower,byte[] upper){return new RangeDescriptor(id(range),id(group),1,0,lower,upper,RangeDescriptor.TopologyState.ACTIVE,List.of(receiver));}
    RangeDirectory directory(long generation,List<RangeDescriptor> ranges){return new RangeDirectory(schema.clusterId(),schema.databaseId(),schema.domainId(),1,schema.digest(),generation,Set.of(),0,ranges);}
    NativeRecoverySupersession value(int marker){
        var scope=new NativeRecoverySupersession.Scope(id(14),new ApplyPosition(1,20),split.header(),directory,parent.groupId(),receiver,new byte[32],new byte[32],new byte[32]);
        return new NativeRecoverySupersession(scope,new NativeRecoverySupersession.CapturedState(null,null,null,null,null,marker==0?null:new ApplyPosition(1,marker),null));
    }
    @Test void fixedCanonicalRequestAndAckBindExactNativeFenceAndActualOwner(){
        var value=value(3);var request=RatisRecoverySupersessionRpc.Request.of(value);var bytes=RatisRecoverySupersessionRpc.request(request);
        assertEquals(RatisRecoverySupersessionRpc.REQUEST_BYTES,bytes.length);assertEquals(request,RatisRecoverySupersessionRpc.decode(bytes));assertArrayEquals(bytes,RatisRecoverySupersessionRpc.request(RatisRecoverySupersessionRpc.decode(bytes)));
        request.requireBinding(value,receiver);var ack=RatisRecoverySupersessionRpc.reply(request);assertEquals(RatisRecoverySupersessionRpc.MAX_REPLY_BYTES,ack.length);RatisRecoverySupersessionRpc.reply(ack,request);
        assertThrows(IllegalArgumentException.class,()->request.requireBinding(value(4),receiver));
        var other=new ReplicaDescriptor(receiver.nodeId(),id(100),receiver.replicaId(),receiver.peerId(),receiver.address(),receiver.role());
        assertThrows(IllegalArgumentException.class,()->request.requireBinding(value,other));
        var listener=new ReplicaDescriptor(receiver.nodeId(),receiver.nodeIncarnation(),receiver.replicaId(),receiver.peerId(),receiver.address(),ReplicaDescriptor.Role.LISTENER);
        assertThrows(IllegalArgumentException.class,()->request.requireBinding(value,listener));
        assertThrows(IllegalArgumentException.class,()->RatisRecoverySupersessionRpc.reply(ack,RatisRecoverySupersessionRpc.Request.of(value(4))));
    }
    @Test void crossedScopeAndOwnerFieldsCannotQualifyTheSameFence(){
        var value=value(3);var original=RatisRecoverySupersessionRpc.request(RatisRecoverySupersessionRpc.Request.of(value));
        for(int offset:new int[]{0,16,32,48,64,80,96,104,121,153,185}){
            var changed=original.clone();changed[ScopedTransactionRpc.HEADER_BYTES+offset]^=1;
            var request=RatisRecoverySupersessionRpc.decode(changed);
            assertThrows(IllegalArgumentException.class,()->request.requireBinding(value,receiver),"Changed binding offset "+offset);
        }
    }
    @Test void malformedTruncatedTrailingKindsRolesAndDigestRepliesAreRejected(){
        var request=RatisRecoverySupersessionRpc.Request.of(value(3));var bytes=RatisRecoverySupersessionRpc.request(request);var ack=RatisRecoverySupersessionRpc.reply(request);
        assertThrows(IllegalArgumentException.class,()->RatisRecoverySupersessionRpc.decode(Arrays.copyOf(bytes,bytes.length-1)));
        assertThrows(IllegalArgumentException.class,()->RatisRecoverySupersessionRpc.decode(Arrays.copyOf(bytes,bytes.length+1)));
        var kind=bytes.clone();ByteBuffer.wrap(kind).putInt(4,RatisRecoveryImageRpc.KIND);assertThrows(IllegalArgumentException.class,()->RatisRecoverySupersessionRpc.decode(kind));
        var role=bytes.clone();role[ScopedTransactionRpc.HEADER_BYTES+120]=(byte)255;assertThrows(IllegalArgumentException.class,()->RatisRecoverySupersessionRpc.decode(role));
        var negative=bytes.clone();ByteBuffer.wrap(negative).putLong(ScopedTransactionRpc.HEADER_BYTES+96,-1);assertThrows(IllegalArgumentException.class,()->RatisRecoverySupersessionRpc.decode(negative));
        assertThrows(IllegalArgumentException.class,()->RatisRecoverySupersessionRpc.reply(Arrays.copyOf(ack,ack.length-1),request));
        assertThrows(IllegalArgumentException.class,()->RatisRecoverySupersessionRpc.reply(Arrays.copyOf(ack,ack.length+1),request));
        var foreign=ack.clone();foreign[foreign.length-1]^=1;assertThrows(IllegalArgumentException.class,()->RatisRecoverySupersessionRpc.reply(foreign,request));
        var digest=request.scopeDigest();digest[0]^=1;assertArrayEquals(value(3).scopeDigest(),request.scopeDigest());
        assertThrows(IllegalArgumentException.class,()->new RatisRecoverySupersessionRpc.Request(request.cluster(),request.database(),request.catalogue(),request.domain(),request.range(),request.group(),request.epoch(),request.receiver(),new byte[31],request.fenceDigest()));
    }
}
