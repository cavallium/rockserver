package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class RatisRecoveryObsoleteRpcTest {
    final RatisRecoverySupersessionRpcTest helper=new RatisRecoverySupersessionRpcTest();
    RatisTopologySourceSnapshotRpc.Request original(){return new RatisTopologySourceSnapshotRpc.Request(new UUID(13110,1),null,new UUID(13110,2),helper.receiver,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,helper.parent.groupId(),helper.directory);}
    NativeRecoverySupersession.Scope scope(RatisTopologySourceSnapshotRpc.Request original){byte[] digest=OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,original));return new NativeRecoverySupersession.Scope(original.catalogue(),new ApplyPosition(1,20),helper.split.header(),helper.directory,helper.parent.groupId(),helper.receiver,new byte[32],List.of(digest));}
    @Test void canonicalRoundTripAndReceiverEvidenceDoNotAssertInstalledImage(){
        var old=original();var scope=scope(old);var request=new RatisRecoveryObsoleteRpc.Request(old,scope,new byte[32]);var bytes=RatisRecoveryObsoleteRpc.request(request);
        assertEquals(bytes.length,request.encodedSize());assertEquals(request,RatisRecoveryObsoleteRpc.decode(bytes));assertArrayEquals(bytes,RatisRecoveryObsoleteRpc.request(RatisRecoveryObsoleteRpc.decode(bytes)));
        var value=new NativeRecoverySupersession(scope,new NativeRecoverySupersession.CapturedState(null,null,null,null,null,null,null));
        request=new RatisRecoveryObsoleteRpc.Request(old,scope,value.digest());request.evidence().requireBinding(value,helper.receiver);
        var ack=RatisRecoveryObsoleteRpc.reply(request);RatisRecoveryObsoleteRpc.reply(ack,request);var changed=new RatisRecoveryObsoleteRpc.Request(old,scope,new byte[32]);assertThrows(IllegalArgumentException.class,()->RatisRecoveryObsoleteRpc.reply(ack,changed));
        byte[] input=new byte[32];var defensive=new RatisRecoveryObsoleteRpc.Request(old,scope,input);input[0]=1;defensive.fenceDigest()[0]=2;assertArrayEquals(new byte[32],defensive.fenceDigest());
    }
    @Test void requestMustBeExactOriginalOrEffectiveCurrentAndBindReceiverAuthority(){
        var old=original();var scope=scope(old);
        assertThrows(IllegalArgumentException.class,()->new RatisRecoveryObsoleteRpc.Request(old.successor(UUID.randomUUID()),scope,new byte[32]));
        var foreign=new RatisTopologySourceSnapshotRpc.Request(old.transferId(),null,UUID.randomUUID(),old.receiver(),old.purpose(),old.sourceGroup(),old.sourceDirectory());
        var hash=OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,foreign));
        var rebound=new NativeRecoverySupersession.Scope(scope.catalogue(),scope.catalogueCut(),scope.header(),scope.sourceDirectory(),scope.groupId(),scope.receiver(),scope.selectedMetadataRequestDigest(),List.of(hash));
        assertThrows(IllegalArgumentException.class,()->new RatisRecoveryObsoleteRpc.Request(foreign,rebound,new byte[32]));
        var meta=new RatisTopologySourceSnapshotRpc.Request(old.transferId(),helper.split.header(),old.catalogue(),old.receiver(),RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,old.sourceGroup(),old.sourceDirectory());
        assertThrows(IllegalArgumentException.class,()->new RatisRecoveryObsoleteRpc.Request(meta,scope,new byte[32]));
    }
    @Test void oneColdFenceAuthorizesTheEntireAbandonedCurrentChainAndNoUnlistedSuccessor(){
        var a=original();var b=a.successor(new UUID(13110,20));var c=a.successor(new UUID(13110,21));var base=scope(a);
        var requests=List.of(a,b,c);var hashes=requests.stream().map(request->OperatorUpload.sha256(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,request))).toList();
        var scope=new NativeRecoverySupersession.Scope(base.catalogue(),base.catalogueCut(),base.header(),base.sourceDirectory(),base.groupId(),base.receiver(),base.selectedMetadataRequestDigest(),hashes);
        var fence=new NativeRecoverySupersession(scope,new NativeRecoverySupersession.CapturedState(null,null,null,null,null,null,null));var cold=NativeRecoverySupersession.decode(fence.encode());
        for(var alias:requests){var obsolete=new RatisRecoveryObsoleteRpc.Request(alias,cold.scope(),cold.digest());var wire=RatisRecoveryObsoleteRpc.request(obsolete);assertEquals(obsolete,RatisRecoveryObsoleteRpc.decode(wire));obsolete.evidence().requireBinding(cold,helper.receiver);RatisRecoveryObsoleteRpc.reply(RatisRecoveryObsoleteRpc.reply(obsolete),obsolete);}
        assertThrows(IllegalArgumentException.class,()->new RatisRecoveryObsoleteRpc.Request(a.successor(new UUID(13110,22)),cold.scope(),cold.digest()));
        assertArrayEquals(fence.digest(),cold.digest());assertEquals(3,cold.scope().currentRequestDigests().size());
    }
    @Test void malformedFramesCannotBecomeCleanupAuthority(){
        var old=original();var request=new RatisRecoveryObsoleteRpc.Request(old,scope(old),new byte[32]);var wire=RatisRecoveryObsoleteRpc.request(request);var ack=RatisRecoveryObsoleteRpc.reply(request);
        assertThrows(IllegalArgumentException.class,()->RatisRecoveryObsoleteRpc.decode(Arrays.copyOf(wire,wire.length-1)));assertThrows(IllegalArgumentException.class,()->RatisRecoveryObsoleteRpc.decode(Arrays.copyOf(wire,wire.length+1)));
        var changed=wire.clone();java.nio.ByteBuffer.wrap(changed).putInt(4,RatisRecoverySupersessionRpc.KIND);assertThrows(IllegalArgumentException.class,()->RatisRecoveryObsoleteRpc.decode(changed));
        var corrupted=wire.clone();corrupted[ScopedTransactionRpc.HEADER_BYTES+4]^=1;assertThrows(IllegalArgumentException.class,()->RatisRecoveryObsoleteRpc.decode(corrupted));
        var badAck=ack.clone();badAck[badAck.length-1]^=1;assertThrows(IllegalArgumentException.class,()->RatisRecoveryObsoleteRpc.reply(badAck,request));assertThrows(IllegalArgumentException.class,()->RatisRecoveryObsoleteRpc.reply(Arrays.copyOf(ack,ack.length+1),request));
    }
}
