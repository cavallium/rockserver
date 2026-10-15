package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologyFreezeRpcTest.*;

class RatisRecoveryLineageRpcTest {
    @Test void canonicalOriginalSplitAndMergeBirthsRetainAllParents() {
        var split=split();var merge=new TopologyChange.Definition(id(70),split.targetDirectory(),directory(3,List.of(range(71,72,null,null))));
        for(var birth:List.of(split,merge))for(var target:birth.targets()) {
            var request=request(birth,target);var encodedRequest=RatisRecoveryLineageRpc.request(request);assertEquals(RatisRecoveryLineageRpc.REQUEST_BYTES,encodedRequest.length);assertEquals(request,RatisRecoveryLineageRpc.request(encodedRequest));
            var reply=new RatisRecoveryLineageRpc.Reply(request,new ApplyPosition(7,100),birth);var encoded=RatisRecoveryLineageRpc.reply(reply);var decoded=RatisRecoveryLineageRpc.reply(encoded,request);
            assertArrayEquals(encoded,RatisRecoveryLineageRpc.reply(decoded));assertArrayEquals(birth.digest(),decoded.birth().digest());assertEquals(target,decoded.target());assertEquals(birth.parents(),decoded.birth().parents());assertEquals(birth.kind()==TopologyChange.Kind.MERGE?2:1,decoded.birth().parents().size());
            var changedDigest=encoded.clone();changedDigest[ScopedTransactionRpc.HEADER_BYTES+156]^=1;assertThrows(IllegalArgumentException.class,()->RatisRecoveryLineageRpc.reply(changedDigest,request));
            var oversized=encoded.clone();ByteBuffer.wrap(oversized).putInt(ScopedTransactionRpc.HEADER_BYTES+152,TopologyChange.MAX_DEFINITION_BYTES+1);assertThrows(IllegalArgumentException.class,()->RatisRecoveryLineageRpc.reply(oversized,request));
            var body=ScopedTransactionRpc.parse(encoded,RatisRecoveryLineageRpc.MAX_REPLY_BYTES).body();var trailing=ScopedTransactionRpc.frame(RatisRecoveryLineageRpc.KIND,Arrays.copyOf(body,body.length+1));assertThrows(IllegalArgumentException.class,()->RatisRecoveryLineageRpc.reply(trailing,request));
            assertThrows(IllegalArgumentException.class,()->new RatisRecoveryLineageRpc.Reply(request,new ApplyPosition(7,89),birth));
            var unrelated=new RatisRecoveryLineageRpc.Request(request.cluster(),request.database(),request.catalogue(),request.column(),request.domain(),id(99),request.group(),request.epoch(),request.minimumCatalogueCut());assertThrows(IllegalArgumentException.class,()->new RatisRecoveryLineageRpc.Reply(unrelated,new ApplyPosition(7,100),birth));
        }
        var source=split.sourceDirectory();var parent=source.ranges().getFirst();var listener=parent.replicas().stream().filter(owner->owner.role()==ReplicaDescriptor.Role.LISTENER).findFirst().orElseThrow();var promoted=new ReplicaDescriptor(listener.nodeId(),listener.nodeIncarnation(),listener.replicaId(),listener.peerId(),listener.address(),ReplicaDescriptor.Role.VOTER);var owners=new ArrayList<>(parent.replicas());owners.remove(listener);owners.add(promoted);
        var updated=new RangeDescriptor(parent.rangeId(),parent.groupId(),parent.rangeEpoch(),parent.configurationVersion()+1,parent.lowerInclusive(),parent.upperExclusive(),parent.topologyState(),owners);var relocation=new TopologyChange.Definition(id(73),source,directory(source.generation()+1,List.of(updated)));assertEquals(TopologyChange.Kind.RELOCATE,relocation.kind());assertThrows(IllegalArgumentException.class,()->new RatisRecoveryLineageRpc.Reply(request(relocation,updated),new ApplyPosition(7,100),relocation));
    }
    @Test void absentBirthIsBoundToEveryExactScopeFieldAndDoesNotAuthorizeBootstrap()throws Exception {
        var birth=split();var range=birth.parents().getFirst();var request=request(birth,range);var cut=new ApplyPosition(7,100);
        var bytes=RatisRecoveryLineageRpc.reply(new RatisRecoveryLineageRpc.Reply(request,cut,null));var absent=RatisRecoveryLineageRpc.reply(bytes,request);assertNull(absent.birth());assertNull(absent.target());assertEquals(cut,absent.catalogueCut());
        var ids=List.of(request.cluster(),request.database(),request.catalogue(),request.column(),request.domain(),request.range(),request.group());
        for(int i=0;i<ids.size();i++){var changed=new ArrayList<>(ids);changed.set(i,id(100+i));var expected=new RatisRecoveryLineageRpc.Request(changed.get(0),changed.get(1),changed.get(2),changed.get(3),changed.get(4),changed.get(5),changed.get(6),request.epoch(),request.minimumCatalogueCut());assertThrows(IllegalArgumentException.class,()->RatisRecoveryLineageRpc.reply(bytes,expected));}
        assertThrows(IllegalArgumentException.class,()->RatisRecoveryLineageRpc.reply(bytes,new RatisRecoveryLineageRpc.Request(request.cluster(),request.database(),request.catalogue(),request.column(),request.domain(),request.range(),request.group(),request.epoch()+1,request.minimumCatalogueCut())));
        assertThrows(IllegalArgumentException.class,()->RatisRecoveryLineageRpc.reply(bytes,new RatisRecoveryLineageRpc.Request(request.cluster(),request.database(),request.catalogue(),request.column(),request.domain(),request.range(),request.group(),request.epoch(),new ApplyPosition(7,91))));
        var resolver=new RatisRecoveryLineage(request.cluster(),request.database(),()->cut,List::of,header->{throw new AssertionError("Absent birth reads no definition");});var lineage=new RatisRecoveryLineage.Request(request.domain(),request.range(),request.group(),request.epoch());
        var lookup=resolver.lookup(lineage,request.minimumCatalogueCut());assertNull(lookup.birth());assertEquals(cut,lookup.catalogueCut());assertThrows(IOException.class,()->resolver.resolve(lineage,request.minimumCatalogueCut(),null));assertInstanceOf(RatisRecoveryLineage.Bootstrap.class,resolver.resolve(lineage,request.minimumCatalogueCut(),birth.sourceDirectory()));
        var badRequest=Arrays.copyOf(RatisRecoveryLineageRpc.request(request),RatisRecoveryLineageRpc.REQUEST_BYTES+1);assertThrows(IllegalArgumentException.class,()->RatisRecoveryLineageRpc.request(badRequest));
    }
    private static RatisRecoveryLineageRpc.Request request(TopologyChange.Definition definition,RangeDescriptor target){var source=definition.sourceDirectory();return new RatisRecoveryLineageRpc.Request(source.clusterId(),source.databaseId(),id(80),id(81),source.domainId(),target.rangeId(),target.groupId(),target.rangeEpoch(),new ApplyPosition(7,90));}
}
