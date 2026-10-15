package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Actual native FREEZE and CAT rows, with socket-free reload; no leader, quorum or SEED claim. */
class RatisNodeFrozenCurrentBootstrapTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @Test void unpublishedFrozenParentRestartsAsCurrentWithoutReopeningWritableAdmission()throws Exception {
        var helper=new RatisTopologySeedRuntimeTest();helper.root=root;
        try(var source=helper.new Fixture(false)) {
            var directory=source.definition.sourceDirectory();var parent=source.definition.parents().getFirst();var id=RaftGroupId.valueOf(parent.groupId());
            assertEquals(TopologyChange.Phase.FENCING,source.catalogueState.phase());
            var original=source.parentStore();var freeze=original.topologyFreeze().encode();var cut=original.applied();
            assertFalse(original.topologyRetired());assertEquals(cut.index(),source.server.machines.get(id).takeSnapshot());
            for(var entry:source.server.storages.entrySet())((org.apache.ratis.server.storage.RaftStorageImpl)entry.getValue()).writeBootstrapConfiguration(source.server.division(entry.getKey()).getRaftConf());
            source.node.close();
            var views=new AtomicInteger();var births=new AtomicInteger();var catalogue=nativeCatalogue(source,views,births);var binding=source.plan.catalogue();
            var epochs=new RatisSessionEpochs(catalogue,directory.clusterId(),directory.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());
            try(var node=RatisNodeRuntime.open(source.plan,source.env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(directory.domainId(),source.routed),source.env.scheduler,source.env.timer,()->20L,source.env.settings(),RatisNodeRuntime.Startup.RECOVER,(plan,logs,machines)->{
                machines.get(id).deferRecoveryDiscovery();return RatisLocalMetadataDiscoveryTest.noSocketServer(plan,logs,machines);
            },List.of(RatisTopologySeedRuntimeTest.schema()))) {
                node.relocationTransport(range->{assertEquals(binding.groupId(),range.groupId());return catalogue;},(range,owner)->{throw new AssertionError("Local frozen CURRENT bootstrap needs no source transfer");});
                node.start();var server=field(node,"server",RaftServer.class);var machine=(RatisRangeStateMachine)server.getDivision(id).getStateMachine();var store=field(machine,"store",ReplicatedRangeStore.class);
                assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertFalse(store.admissionOpen());assertEquals(cut,store.applied());assertArrayEquals(freeze,store.topologyFreeze().encode());
                assertThrows(IllegalStateException.class,()->store.readCurrentPoint(RatisTopologySeedRuntimeTest.COLUMN,new byte[]{1}));
                node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);
                var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot();var request=graph.activeRequests().get(parent.groupId());
                assertNotNull(request);assertEquals(RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,request.purpose());assertNull(request.header());
                assertArrayEquals(directory.payloadDigest(),request.sourceDirectory().payloadDigest());assertEquals(source.env.catalogue.store.applied(),graph.plan().catalogueCut());
                assertTrue(views.get()>=2);assertTrue(births.get()>0);assertFalse(machine.recoveryEligibilityGate().discoveryPending());
                assertEquals(cut,store.applied());assertEquals(cut.index(),server.getDivision(id).getRaftLog().getSnapshotIndex());assertEquals(cut.index(),machine.getLastAppliedTermIndex().getIndex());
                assertArrayEquals(new byte[]{42},store.readCurrentPoint(RatisTopologySeedRuntimeTest.COLUMN,new byte[]{1}));assertArrayEquals(freeze,store.topologyFreeze().encode());assertFalse(store.topologyRetired());
                var fresh=new DistributedCommand(parent.groupId(),parent.rangeEpoch(),directory.schemaDigest(),new OperationId(new UUID(12010,1),1,0),List.of(new Mutation(Mutation.Kind.PUT,RatisTopologySeedRuntimeTest.COLUMN,new byte[]{1},new byte[]{99})));
                var rejected=assertThrows(IllegalStateException.class,()->store.apply(new ApplyPosition(cut.term(),cut.index()+1),fresh));assertTrue(rejected.getMessage().contains("FREEZE"));
                assertEquals(cut,store.applied());assertArrayEquals(new byte[]{42},store.readCurrentPoint(RatisTopologySeedRuntimeTest.COLUMN,new byte[]{1}));assertEquals(0,store.activeSnapshotPins());assertNull(node.nativePool(RatisPoolFixtures.ID).topologySeed(source.definition.intentId()));
                node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);assertArrayEquals(freeze,store.topologyFreeze().encode());
            }
        }
    }

    /** Read-only codec transport backed by the fixture's committed native CAT, not synthetic authority. */
    private static RaftClient nativeCatalogue(RatisTopologySeedRuntimeTest.Fixture source,AtomicInteger views,AtomicInteger births)throws Exception {
        var binding=source.plan.catalogue();var store=source.env.catalogue.store;store.enableCatalogueScopeCompaction();
        var resolver=new RatisRecoveryLineage(source.plan.clusterId(),source.plan.databaseId(),binding.groupId(),store,binding.directoryColumn());
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            assertEquals("sendReadOnly",method.getName());byte[] query=((Message)args[0]).getContent().toByteArray();int kind=ScopedTransactionRpc.kind(query,ScopedTransactionRpc.MAX_REQUEST_BYTES);byte[] wire;
            if(kind==RatisCatalogueViewRpc.KIND) {
                views.incrementAndGet();var request=RatisCatalogueViewRpc.request(query);assertEquals(binding.groupId(),request.catalogue());assertEquals(binding.directoryColumn(),request.column());
                var key=ByteBuffer.allocate(16).putLong(request.domain().getMostSignificantBits()).putLong(request.domain().getLeastSignificantBits()).array();
                var read=store.readSchemaAuthorityPoints(List.of(new ReplicatedRangeStore.PhysicalPoint(binding.directoryColumn(),key)),new int[]{RatisCatalogue.MAX_DIRECTORY_BYTES});
                wire=RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(request,read.position(),RangeDirectoryCodec.decode(read.values().getFirst())));
            } else if(kind==RatisRecoveryLineageRpc.KIND) {
                births.incrementAndGet();var request=RatisRecoveryLineageRpc.request(query);assertEquals(binding.groupId(),request.catalogue());assertEquals(binding.directoryColumn(),request.column());
                var lookup=resolver.lookup(new RatisRecoveryLineage.Request(request.domain(),request.range(),request.group(),request.epoch()),request.minimumCatalogueCut());
                wire=RatisRecoveryLineageRpc.reply(new RatisRecoveryLineageRpc.Reply(request,lookup.catalogueCut(),lookup.birth()==null?null:lookup.birth().definition()));
            } else return source.env.catalogue.client.io().sendReadOnly((Message)args[0]);
            return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf("native-catalogue")).setGroupId(RaftGroupId.valueOf(binding.groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(wire))).build();
        });
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){
            case "getGroupId"->RaftGroupId.valueOf(binding.groupId());case "io"->io;case "close"->null;default->throw new AssertionError(method);
        });
    }
}
