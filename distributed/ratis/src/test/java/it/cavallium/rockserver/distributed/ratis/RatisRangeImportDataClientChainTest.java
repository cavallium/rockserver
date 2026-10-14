package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstImport;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

/** Real data-client qualification and reply decoding with controlled RPCs; no server or quorum. */
class RatisRangeImportDataClientChainTest {
    private static UUID id(long value) { return new UUID(751,value); }
    private static RangeImportDefinition definition(UUID importId, OperationId operation, RangeDirectory source, RangeDescriptor range) {
        var manifest=new NativeSstImportManifest(importId,range.groupId(),range.rangeEpoch(),source.domainId(),source.schemaDigest(),
                range.lowerInclusive(),range.upperExclusive(),List.of(new NativeSstImportManifest.Column(id(21),"imported",false,List.of())),
                RatisRangeImportRecord.DISTRIBUTED_LIMITS);
        return new RangeImportDefinition(importId,operation,source,List.of(manifest));
    }

    @Test void successiveFrozenImportsInstallActivateAndReadExactPublicationWhileRejectingWrongChains() throws Exception {
        var owners=List.of(RatisRangeImportCatalogueTest.owner(1,ReplicaDescriptor.Role.VOTER),RatisRangeImportCatalogueTest.owner(2,ReplicaDescriptor.Role.LISTENER));
        var left=new RangeDescriptor(id(10),id(11),2,3,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.FENCED,owners);
        var right=new RangeDescriptor(id(12),id(13),2,3,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.FENCED,owners);
        var initial=new RangeDirectory(id(1),id(2),id(20),1,new byte[32],7,Set.of(id(21)),1,List.of(left,right));
        var first=definition(id(22),new OperationId(id(30),0,1),initial,left);
        var second=definition(id(23),new OperationId(id(30),0,2),first.targetDirectory(),right);
        assertFalse(Arrays.equals(initial.payloadDigest(),second.sourceDirectory().payloadDigest()));
        var binding=new RatisNodePlan.Catalogue(id(3),1,new byte[32],id(4),id(5),owners);
        var domain=new RatisNodePlan.Domain(initial,Map.of(id(21),"imported"),Set.of());
        var plan=new RatisNodePlan(owners.getFirst().nodeId(),owners.getFirst().nodeIncarnation(),List.of(domain),binding,List.of(),
                List.of(second,first),Map.of(),3,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(binding,List.of()));
        assertEquals(List.of(first,second),plan.imports());
        var definitions=Map.of(first.importId(),first,second.importId(),second);
        var publications=new HashMap<UUID,RatisRangeImportApply.PublicationView>();
        for(var definition:plan.imports()) {
            var publication=new RatisRangeImportRecord.Publication(binding.groupId(),new ApplyPosition(2,20+definition.sourceDirectory().generation()),
                    RatisRangeImportCatalogue.Head.prepared(definition).publish(new byte[32]));
            publications.put(definition.importId(),new RatisRangeImportApply.PublicationView(publication,definition.targetDirectory()));
        }
        var wrongAuthority=new AtomicBoolean();var authorityReads=new AtomicInteger();
        var catalogue=mock(RaftClient.class);var catalogueIo=mock(BlockingApi.class);
        when(catalogue.getGroupId()).thenReturn(RaftGroupId.valueOf(binding.groupId()));when(catalogue.io()).thenReturn(catalogueIo);
        when(catalogueIo.sendReadOnly(any(Message.class))).thenAnswer(call->{
            var request=RatisRangeImportAuthority.decodeRequest(((Message)call.getArgument(0)).getContent().toByteArray());
            var definition=definitions.get(request.importId());assertNotNull(definition);assertArrayEquals(definition.digest(),request.definitionDigest());
            authorityReads.incrementAndGet();var view=publications.get(wrongAuthority.get()?first.importId():request.importId());
            return reply(catalogue,RatisRangeImportAuthority.reply(view));
        });
        var opened=new AtomicInteger();var writes=new AtomicInteger();
        RatisRangeImportDataClient.Connections connections=(range,clientId)->{
            assertEquals(plan.groupDescriptor(range.groupId()),range);opened.incrementAndGet();
            var control=mock(RaftClient.class);var io=mock(BlockingApi.class);when(control.getId()).thenReturn(clientId);
            when(control.getGroupId()).thenReturn(RaftGroupId.valueOf(range.groupId()));when(control.io()).thenReturn(io);
            when(io.sendReadOnly(any(Message.class))).thenAnswer(call->reply(control,((Message)call.getArgument(0)).getContent().toByteArray()));
            when(io.send(any(Message.class))).thenAnswer(call->{
                var record=RatisRangeImportRecord.decode(((Message)call.getArgument(0)).getContent().toByteArray());writes.incrementAndGet();
                if(record instanceof RatisRangeImportRecord.Install install) {
                    var definition=definitions.get(install.importId());install.requireDefinition(definition);
                    return reply(control,new NativeSstImportManifest.Receipt(install.importId(),range.groupId(),range.rangeEpoch(),
                            definition.manifest(range.groupId()).digest(),new ApplyPosition(1,3)).encode());
                }
                var activate=(RatisRangeImportRecord.Activate)record;activate.requireDefinition(definitions.get(activate.receipt().importId()));
                return reply(control,new NativeRangeSstImport.Activation(activate.receipt(),binding.groupId(),activate.publication().completedCatalogueCut(),
                        activate.publication().digest(),new ApplyPosition(1,4)).encode());
            });
            return control;
        };
        var ownerClient=new RatisRangeImportOwnerClient(initial.clusterId(),initial.databaseId(),binding.groupId(),(range,owner)->{throw new AssertionError("Unused owner RPC");});
        try(var client=new RatisRangeImportDataClient(plan,catalogue,connections,(control,primary)->{throw new AssertionError("Unused stream");},ownerClient)) {
            for(var definition:plan.imports()) {
                var install=install(definition);var receipt=client.install(definition,install);
                var view=client.publication(definition);assertArrayEquals(definition.targetDirectory().payloadDigest(),view.directory().payloadDigest());
                var activated=client.activate(definition,new RatisRangeImportRecord.Activate(receipt,view.publication()));
                assertArrayEquals(receipt.encode(),activated.receipt().encode());
            }
            assertArrayEquals(client.install(second,install(second)).encode(),client.install(second,install(second)).encode());
            assertEquals(2,opened.get());assertEquals(6,writes.get());assertEquals(2,authorityReads.get());
            var staleSource=definition(second.importId(),second.operation(),initial,right);
            assertThrows(IOException.class,()->client.install(staleSource,install(staleSource)));
            var unplannedOperation=definition(second.importId(),new OperationId(id(30),0,3),second.sourceDirectory(),right);
            assertThrows(IOException.class,()->client.install(unplannedOperation,install(unplannedOperation)));
            assertThrows(IOException.class,()->client.install(first,install(second)));
            var unplannedRequest=new RatisReferencedFile.Request(id(99),id(98),9,1,new byte[32]);
            assertThrows(IOException.class,()->client.transfer(first,unplannedRequest,Path.of("absent-import-file")));
            var original=install(second);
            var staleEpoch=new RatisRangeImportRecord.Install(original.importId(),original.groupId(),original.rangeEpoch()+1,original.definitionDigest(),original.manifestReference(),Map.of());
            assertThrows(IllegalArgumentException.class,()->client.install(second,staleEpoch));
            assertEquals(2,opened.get());assertEquals(6,writes.get());assertEquals(2,authorityReads.get());
            wrongAuthority.set(true);assertThrows(IOException.class,()->client.publication(second));assertEquals(3,authorityReads.get());
        }
    }

    private static RatisRangeImportRecord.Install install(RangeImportDefinition definition) {
        var manifest=definition.manifests().getFirst();
        var reference=new RatisReferencedFile(manifest.groupId(),1,1,id(40),9,manifest.encode().length,manifest.digest());
        return new RatisRangeImportRecord.Install(definition.importId(),manifest.groupId(),manifest.rangeEpoch(),definition.digest(),reference,Map.of());
    }
    private static RaftClientReply reply(RaftClient client,byte[] bytes) {
        return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("owner1"))
                .setGroupId(client.getGroupId()).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(bytes))).build();
    }
}
