package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisSchemaReplicaRpcTest {
    static UUID id(int n) { return new UUID(12,n); }
    record Fixture(RatisSchemaReplicaRpc.Binding binding,OperatorUpload upload,byte[] bytes) {}
    static Fixture fixture(int incarnation) {
        byte[] bytes=new byte[OperatorUpload.CHUNK_BYTES+7]; new Random(4).nextBytes(bytes);
        var operator=new DistributedSchema.Operator("sum",1,"example.Sum",OperatorUpload.sha256(bytes),1,1,"sum",1);
        var source=new DistributedSchema(id(1),id(2),id(3),1,0,List.of());
        var target=new DistributedSchema(id(1),id(2),id(3),2,0,List.of(new DistributedSchema.Column(1,id(4),"added",new ColumnSchema(IntArrayList.of(4),ObjectArrayList.of(),true,operator.name(),operator.version(),operator.className()),operator)));
        var voter=new ReplicaDescriptor(id(5),id(incarnation),id(7),"voter","127.0.0.1:9001",ReplicaDescriptor.Role.VOTER);
        var listener=new ReplicaDescriptor(id(8),id(incarnation+1),id(10),"listener","127.0.0.1:9002",ReplicaDescriptor.Role.LISTENER);
        var range=new RangeDescriptor(id(11),id(12),1,2,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(voter,listener));
        var directory=new RangeDirectory(id(1),id(2),id(3),1,source.digest(),0,Set.of(),0,List.of(range));
        var definition=new SchemaChange.Definition(id(13),new OperationId(id(14),0,0),directory,source,target);
        return new Fixture(new RatisSchemaReplicaRpc.Binding(definition,id(12),"physical-added",true),OperatorUpload.create(id(15),id(1),id(2),operator,bytes),bytes);
    }
    @Test void exactTargetAndAllOperationsRoundTripIncludingListenerAndBoundedChunk() {
        var f=fixture(6);
        for (var target:RatisSchemaReplicaClient.targets(f.binding())) {
            assertEquals(target,RatisSchemaReplicaRpc.Target.decode(target.encode()));
            for (int kind=65;kind<=71;kind++) {
                var request=kind==66 ? RatisSchemaReplicaClient.chunk(target,f.upload(),0,Arrays.copyOf(f.bytes(),OperatorUpload.CHUNK_BYTES)) : kind<=68 ? RatisSchemaReplicaClient.artifact(kind,target,f.upload()) : RatisSchemaReplicaClient.capacity(kind,target);
                assertEquals(request,RatisSchemaReplicaRpc.request(request.encode())); assertTrue(request.encode().length<RatisSchemaReplicaRpc.MAX_BYTES);
                var chunks=new BitSet(); if (kind<=68) chunks.set(0,2);
                byte[] response=RatisSchemaReplicaRpc.reply(request,chunks,kind<=68,null,RatisSchemaReplicaRpc.CapacityState.NONE,false);
                var reply=RatisSchemaReplicaRpc.decodeReply(response,request); assertEquals(target,reply.target()); assertArrayEquals(request.digest(),reply.requestDigest());
            }
        }
        var target=RatisSchemaReplicaClient.targets(f.binding()).getFirst();
        byte[] changed=Arrays.copyOf(f.bytes(),OperatorUpload.CHUNK_BYTES); changed[0]^=1;
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaReplicaClient.chunk(target,f.upload(),0,changed));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaReplicaClient.chunk(target,f.upload(),0,new byte[OperatorUpload.CHUNK_BYTES+1]));
    }
    @Test void proofIdentityAndForcedActiveReadinessAreSeparate() {
        var f=fixture(6); var target=RatisSchemaReplicaClient.targets(f.binding()).getFirst(); var request=RatisSchemaReplicaClient.capacity(69,target);
        var proof=new NativeSchemaCapacityReservation(f.binding().header(),f.binding().groupId(),new byte[32],100);
        for (var state:RatisSchemaReplicaRpc.CapacityState.values()) if (state!=RatisSchemaReplicaRpc.CapacityState.NONE) for (boolean forced:new boolean[]{false,true}) {
            var reply=RatisSchemaReplicaRpc.decodeReply(RatisSchemaReplicaRpc.reply(request,new BitSet(),false,proof,state,forced),request);
            assertEquals(state==RatisSchemaReplicaRpc.CapacityState.ACTIVE && forced,reply.capacityActive()); assertEquals(proof,reply.capacity());
        }
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaReplicaRpc.reply(request,new BitSet(),false,null,RatisSchemaReplicaRpc.CapacityState.ACTIVE,true));
        var other=fixture(7); var foreignProof=new NativeSchemaCapacityReservation(other.binding().header(),other.binding().groupId(),new byte[32],100);
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaReplicaRpc.reply(request,new BitSet(),false,foreignProof,RatisSchemaReplicaRpc.CapacityState.ACTIVE,true));
    }
    @Test void rejectsCrossedReplicaManifestChunksAndMalformedFrames() {
        var f=fixture(6); var targets=RatisSchemaReplicaClient.targets(f.binding()); var request=RatisSchemaReplicaClient.artifact(65,targets.getFirst(),f.upload());
        var bits=new BitSet(); bits.set(0,2); byte[] reply=RatisSchemaReplicaRpc.reply(request,bits,true,null,RatisSchemaReplicaRpc.CapacityState.NONE,false);
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaReplicaRpc.decodeReply(reply,RatisSchemaReplicaClient.artifact(65,targets.get(1),f.upload())));
        var newOwner=RatisSchemaReplicaClient.targets(fixture(7).binding()).getFirst();
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaReplicaRpc.decodeReply(reply,RatisSchemaReplicaClient.artifact(65,newOwner,f.upload())));
        var differentUpload=OperatorUpload.create(id(16),id(1),id(2),f.upload().operator(),f.bytes());
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaReplicaRpc.decodeReply(reply,RatisSchemaReplicaClient.artifact(65,targets.getFirst(),differentUpload)));
        for (int n:new int[]{0,12,request.encode().length-1}) assertThrows(IllegalArgumentException.class,() -> RatisSchemaReplicaRpc.request(Arrays.copyOf(request.encode(),n)));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaReplicaRpc.request(new byte[RatisSchemaReplicaRpc.MAX_BYTES+1]));
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaReplicaRpc.Target.decode(Arrays.copyOf(targets.getFirst().encode(),targets.getFirst().encode().length+1)));
        var absent=new BitSet(); byte[] falseReady=RatisSchemaReplicaRpc.reply(request,absent,true,null,RatisSchemaReplicaRpc.CapacityState.NONE,false);
        assertThrows(IllegalArgumentException.class,() -> RatisSchemaReplicaRpc.decodeReply(falseReady,request));
    }
}
