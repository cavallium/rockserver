package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class RatisTopologyAuthorityTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisRangeImportCatalogueTest outer=new RatisRangeImportCatalogueTest();
        final RatisRangeImportCatalogueTest.Fixture nativeOwner;
        final RatisTopologyCatalogue topology;
        final TopologyChange.Definition definition;
        final RaftStorage storage;
        final RatisRangeStateMachine machine;
        final RatisProofQueryService authority=RatisProofQueryService.authorityLane(RatisProofFixtures.admission());
        final AtomicLong term=new AtomicLong(1);
        final AtomicLong writes=new AtomicLong(30);
        volatile Runnable beforeAuthority=()->{};
        Fixture() throws Exception {
            outer.root=root; nativeOwner=outer.new Fixture(); var helper=new RatisTopologyCatalogueTest();
            var source=helper.source(nativeOwner); definition=helper.split(source,1,128); topology=helper.catalogue(nativeOwner);
            var peer=RaftPeer.newBuilder().setId("topology-catalogue").build(); var group=RaftGroup.valueOf(RaftGroupId.valueOf(nativeOwner.outer.catalogue),peer);
            storage=RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build(); storage.initialize();
            machine=new RatisRangeStateMachine(nativeOwner.nativeCatalogue.store,group,new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,2),nativeOwner.pure,command->{},()->new RatisRangeStateMachine.Leadership(term.get(),true));
            machine.role(RatisNodePlan.Role.CATALOGUE); machine.schemaCatalogue(nativeOwner.outer.cluster,nativeOwner.outer.database,nativeOwner.outer.directoryColumn);
            machine.schemaAuthorityQueries((groupId,input,weight,coalesce,action)->authority.submit(groupId,input,weight,coalesce,owned->{ beforeAuthority.run(); return action.run(owned); }));
            var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)->{
                if(method.getName().equals("getId"))return peer.getId(); if(method.getName().equals("toString"))return "topology-authority-fixture"; throw new AssertionError(method);
            });
            machine.initialize(identity,group.getGroupId(),storage); nativeOwner.seed(source);
            assertEquals(CommandResult.Status.SUCCESS,topology.execute(topology.prepareDefinition(nativeOwner.operation(1),definition,null)).status());
            assertEquals(CommandResult.Status.SUCCESS,topology.execute(topology.prepareBegin(nativeOwner.operation(2),definition,null,null,null)).status());
            machine.notifyTermIndexUpdated(1,nativeOwner.nativeCatalogue.store.applied().index());
        }
        void fencing() throws Exception { var prepared=topology.readHead(definition.header().domainId()); assertEquals(CommandResult.Status.SUCCESS,topology.execute(topology.prepareFencing(nativeOwner.operation(3),definition,prepared,null,null)).status()); machine.notifyTermIndexUpdated(1,nativeOwner.nativeCatalogue.store.applied().index()); }
        void nativeAuthority(TopologyChange.State state,RangeDirectory directory) throws Exception {
            byte[] domain=java.nio.ByteBuffer.allocate(16).putLong(directory.domainId().getMostSignificantBits()).putLong(directory.domainId().getLeastSignificantBits()).array();
            // Deliberate native fixture writes test read authority, not implemented catalogue progress/publication.
            nativeOwner.nativeCatalogue.store.apply(new ApplyPosition(1,nativeOwner.nativeCatalogue.next.getAndIncrement()),new DistributedCommand(nativeOwner.outer.catalogue,1,new byte[32],nativeOwner.operation(writes.getAndIncrement()),List.of(
                    new Mutation(Mutation.Kind.PUT,nativeOwner.outer.directoryColumn,RatisTopologyCatalogue.headKey(state.header().domainId()),state.encode()),
                    new Mutation(Mutation.Kind.PUT,nativeOwner.outer.directoryColumn,RatisTopologyCatalogue.stateKey(state.header().intentId()),state.encode()),
                    new Mutation(Mutation.Kind.PUT,nativeOwner.outer.directoryColumn,domain,RangeDirectoryCodec.encode(directory)))));
        }
        RatisTopologyAuthority.View nativeRead(TopologyChange.Stage stage) throws Exception {
            return RatisTopologyAuthority.readNative(nativeOwner.nativeCatalogue.store,nativeOwner.outer.directoryColumn,nativeOwner.outer.cluster,nativeOwner.outer.database,RatisTopologyAuthority.Request.of(definition.header(),stage));
        }
        Message request() { return Message.valueOf(ByteString.copyFrom(RatisTopologyAuthority.request(RatisTopologyAuthority.Request.of(definition.header())))); }
        @Override public void close() throws Exception { authority.stop().get(5,TimeUnit.SECONDS); machine.close(); machine.awaitNativeReaders(Duration.ofSeconds(5)); storage.close(); nativeOwner.close(); }
    }
    static void await(CountDownLatch latch) { try { if(!latch.await(5,TimeUnit.SECONDS)) throw new AssertionError("Authority fixture timed out"); } catch(InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); } }
    static Map<TopologyChange.Phase,TopologyChange.State> progression(TopologyChange.Definition definition) {
        var states=new EnumMap<TopologyChange.Phase,TopologyChange.State>(TopologyChange.Phase.class); var header=definition.header();
        var state=TopologyChange.State.begin(header); states.put(state.phase(),state); states.put(TopologyChange.Phase.ABORTED,state.abort());
        state=state.beginFencing(); states.put(state.phase(),state); byte[] artifact=new byte[32]; artifact[0]=1;
        for(var parent:header.parents()) for(var owner:parent.owners()) state=state.frozen(TopologyChange.Proof.of(TopologyChange.Stage.FREEZE,header,parent.groupId(),owner.replicaId(),new ApplyPosition(1,10),artifact,header.sourceDirectoryDigest()));
        states.put(state.phase(),state); byte[] freeze=state.freezeDigest(); artifact=new byte[32]; artifact[0]=2;
        for(var target:header.targets()) for(var owner:target.owners()) state=state.seeded(TopologyChange.Proof.of(TopologyChange.Stage.SEED,header,target.groupId(),owner.replicaId(),new ApplyPosition(0,0),artifact,freeze));
        states.put(state.phase(),state); state=state.publish(); states.put(state.phase(),state); artifact=new byte[32]; artifact[0]=3;
        for(var target:header.targets()) for(var owner:target.owners()) {
            var seed=state.seeded().get(new TopologyChange.Key(target.groupId(),owner.replicaId()));
            state=state.activated(TopologyChange.Proof.of(TopologyChange.Stage.ACTIVATE,header,target.groupId(),owner.replicaId(),new ApplyPosition(1,1),artifact,seed.digest()));
        }
        states.put(state.phase(),state); state=state.retire(); states.put(state.phase(),state); return states;
    }
    @Test void currentRequestCarriesExplicitPurposeAndRejectsUnknownStageAndOldOrTrailingBodies() throws Exception {
        try(var f=new Fixture()) {
            for(var stage:TopologyChange.Stage.values()) {
                var request=RatisTopologyAuthority.Request.of(f.definition.header(),stage); byte[] encoded=RatisTopologyAuthority.request(request);
                assertEquals(ScopedTransactionRpc.HEADER_BYTES+97,encoded.length); var decoded=RatisTopologyAuthority.decodeRequest(encoded);
                assertEquals(stage,decoded.stage()); assertArrayEquals(encoded,RatisTopologyAuthority.request(decoded));
                byte[] body=ScopedTransactionRpc.parse(encoded,RatisTopologyAuthority.REQUEST_BYTES).body(); body[96]=(byte)255;
                assertThrows(IllegalArgumentException.class,()->RatisTopologyAuthority.decodeRequest(ScopedTransactionRpc.frame(RatisTopologyAuthority.KIND,body)));
                assertThrows(IllegalArgumentException.class,()->RatisTopologyAuthority.decodeRequest(ScopedTransactionRpc.frame(RatisTopologyAuthority.KIND,Arrays.copyOf(body,96))));
                assertThrows(IllegalArgumentException.class,()->RatisTopologyAuthority.decodeRequest(ScopedTransactionRpc.frame(RatisTopologyAuthority.KIND,Arrays.copyOf(body,98))));
            }
            assertEquals(TopologyChange.Stage.FREEZE,RatisTopologyAuthority.Request.of(f.definition.header()).stage());
        }
    }
    @Test void purposesRequireLegitimatePhaseAndExactSourceOrTargetAtOneNativeCut() throws Exception {
        try(var f=new Fixture()) {
            var states=progression(f.definition); byte[] admission=new RatisTopologyAuthority.View(f.nativeOwner.outer.catalogue,new ApplyPosition(1,1),f.definition,states.get(TopologyChange.Phase.FENCING)).admissionDigest();
            for(var phase:TopologyChange.Phase.values()) {
                var state=states.get(phase); assertNotNull(state);
                boolean target=phase==TopologyChange.Phase.PUBLISHED || phase==TopologyChange.Phase.ACTIVE || phase==TopologyChange.Phase.RETIRED;
                f.nativeAuthority(state,target ? f.definition.targetDirectory() : f.definition.sourceDirectory());
                for(var stage:TopologyChange.Stage.values()) {
                    boolean allowed=switch(stage) {
                        case FREEZE -> phase==TopologyChange.Phase.FENCING || phase==TopologyChange.Phase.FENCED || phase==TopologyChange.Phase.SEEDED;
                        case SEED -> phase==TopologyChange.Phase.FENCED || phase==TopologyChange.Phase.SEEDED || target;
                        case ACTIVATE -> target;
                    };
                    if(!allowed) { assertThrows(java.io.IOException.class,()->f.nativeRead(stage),phase+" cannot authorize "+stage); continue; }
                    var view=f.nativeRead(stage); assertEquals(phase,view.state().phase()); assertEquals(f.nativeOwner.nativeCatalogue.store.applied(),view.cut()); assertArrayEquals(admission,view.admissionDigest());
                    var request=RatisTopologyAuthority.Request.of(f.definition.header(),stage);
                    assertArrayEquals(state.encode(),RatisTopologyAuthority.decodeReply(RatisTopologyAuthority.reply(view),request,view.catalogue()).state().encode());
                }
                if(phase!=TopologyChange.Phase.PREPARED && phase!=TopologyChange.Phase.ABORTED) {
                    f.nativeAuthority(state,target ? f.definition.sourceDirectory() : f.definition.targetDirectory());
                    assertThrows(java.io.IOException.class,()->f.nativeRead(target ? TopologyChange.Stage.ACTIVATE : TopologyChange.Stage.FREEZE));
                }
            }
            var fencing=new RatisTopologyAuthority.View(f.nativeOwner.outer.catalogue,new ApplyPosition(1,1),f.definition,states.get(TopologyChange.Phase.FENCING));
            assertThrows(IllegalArgumentException.class,()->RatisTopologyAuthority.decodeReply(RatisTopologyAuthority.reply(fencing),RatisTopologyAuthority.Request.of(f.definition.header(),TopologyChange.Stage.SEED),fencing.catalogue()));
            var published=new RatisTopologyAuthority.View(f.nativeOwner.outer.catalogue,new ApplyPosition(1,1),f.definition,states.get(TopologyChange.Phase.PUBLISHED));
            assertThrows(IllegalArgumentException.class,()->RatisTopologyAuthority.decodeReply(RatisTopologyAuthority.reply(published),RatisTopologyAuthority.Request.of(f.definition.header()),published.catalogue()));
        }
    }
    @Test void leadershipLossWhileAuthorityIsQueuedRejectsItsOldBarrier() throws Exception {
        try(var f=new Fixture()) {
            f.fencing(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); f.beforeAuthority=()->{ entered.countDown(); await(release); };
            var result=f.machine.queryLinearizable(f.request()); await(entered);
            try { f.term.set(2); } finally { release.countDown(); }
            assertThrows(ExecutionException.class,()->result.get(5,TimeUnit.SECONDS));
        }
    }
    @Test void observerCancellationRetainsAuthorityChargeUntilActualReadReturns() throws Exception {
        try(var f=new Fixture()) {
            f.fencing(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1); f.beforeAuthority=()->{ entered.countDown(); await(release); };
            var result=f.machine.queryLinearizable(f.request()); await(entered);
            try { assertTrue(result.cancel(false)); assertEquals(1,f.authority.stats().jobs()); assertEquals(RatisProofQueryService.MAX_BYTES,f.authority.stats().bytes()); }
            finally { release.countDown(); }
            f.authority.stop().get(5,TimeUnit.SECONDS); assertEquals(0,f.authority.stats().jobs()); assertEquals(0,f.authority.stats().bytes());
        }
    }
    @Test void freshFencingAuthorityReturnsOneExactCutAndRejectsOtherRequestIdentities() throws Exception {
        try(var f=new Fixture()) {
            assertThrows(ExecutionException.class,()->f.machine.query(f.request()).get(5,TimeUnit.SECONDS));
            assertThrows(ExecutionException.class,()->f.machine.queryLinearizable(f.request()).get(5,TimeUnit.SECONDS));
            f.fencing(); byte[] reply=f.machine.queryLinearizable(f.request()).get(5,TimeUnit.SECONDS).getContent().toByteArray();
            var expected=RatisTopologyAuthority.Request.of(f.definition.header()); var view=RatisTopologyAuthority.decodeReply(reply,expected,f.nativeOwner.outer.catalogue);
            assertEquals(f.nativeOwner.nativeCatalogue.store.applied(),view.cut()); assertEquals(TopologyChange.Phase.FENCING,view.state().phase()); assertArrayEquals(f.definition.encode(),view.definition().encode());
            assertArrayEquals(view.admissionDigest(),new RatisTopologyAuthority.View(view.catalogue(),new ApplyPosition(2,view.cut().index()+1),view.definition(),view.state()).admissionDigest());
            var wrong=new RatisTopologyAuthority.Request(expected.cluster(),expected.database(),expected.domain(),UUID.randomUUID(),expected.digest(),expected.stage());
            assertThrows(IllegalArgumentException.class,()->RatisTopologyAuthority.decodeReply(reply,wrong,f.nativeOwner.outer.catalogue));
            assertThrows(IllegalArgumentException.class,()->RatisTopologyAuthority.decodeReply(reply,expected,UUID.randomUUID()));
            assertThrows(IllegalArgumentException.class,()->RatisTopologyAuthority.decodeReply(Arrays.copyOf(reply,reply.length-1),expected,f.nativeOwner.outer.catalogue));
            f.term.set(2); assertThrows(ExecutionException.class,()->f.machine.queryLinearizable(f.request()).get(5,TimeUnit.SECONDS));
        }
    }
    @Test void authorityRejectsChangedSourceAndInconsistentHeadState() throws Exception {
        try(var f=new Fixture()) {
            f.fencing(); var store=f.nativeOwner.nativeCatalogue.store; var source=f.definition.sourceDirectory();
            var changed=new RangeDirectory(source.clusterId(),source.databaseId(),source.domainId(),source.schemaEpoch(),source.schemaDigest(),source.generation()+1,source.columns(),source.routingPrefixBytes(),source.ranges());
            byte[] key=java.nio.ByteBuffer.allocate(16).putLong(source.domainId().getMostSignificantBits()).putLong(source.domainId().getLeastSignificantBits()).array();
            // Deliberate native corruption bypasses catalogue validation to exercise the authority boundary.
            store.apply(new ApplyPosition(1,f.nativeOwner.nativeCatalogue.next.getAndIncrement()),new DistributedCommand(f.nativeOwner.outer.catalogue,1,new byte[32],f.nativeOwner.operation(20),List.of(new Mutation(Mutation.Kind.PUT,f.nativeOwner.outer.directoryColumn,key,RangeDirectoryCodec.encode(changed)))));
            var request=RatisTopologyAuthority.Request.of(f.definition.header()); assertThrows(java.io.IOException.class,()->RatisTopologyAuthority.readNative(store,f.nativeOwner.outer.directoryColumn,source.clusterId(),source.databaseId(),request));
            store.apply(new ApplyPosition(1,f.nativeOwner.nativeCatalogue.next.getAndIncrement()),new DistributedCommand(f.nativeOwner.outer.catalogue,1,new byte[32],f.nativeOwner.operation(21),List.of(new Mutation(Mutation.Kind.PUT,f.nativeOwner.outer.directoryColumn,key,RangeDirectoryCodec.encode(source)),new Mutation(Mutation.Kind.PUT,f.nativeOwner.outer.directoryColumn,RatisTopologyCatalogue.stateKey(f.definition.intentId()),TopologyChange.State.begin(f.definition.header()).encode()))));
            assertThrows(java.io.IOException.class,()->RatisTopologyAuthority.readNative(store,f.nativeOwner.outer.directoryColumn,source.clusterId(),source.databaseId(),request));
            store.apply(new ApplyPosition(1,f.nativeOwner.nativeCatalogue.next.getAndIncrement()),new DistributedCommand(f.nativeOwner.outer.catalogue,1,new byte[32],f.nativeOwner.operation(22),List.of(new Mutation(Mutation.Kind.PUT,f.nativeOwner.outer.directoryColumn,RatisTopologyCatalogue.definitionKey(request.digest()),new byte[TopologyChange.MAX_DEFINITION_BYTES+1]))));
            var oversized=assertThrows(java.io.IOException.class,()->RatisTopologyAuthority.readNative(store,f.nativeOwner.outer.directoryColumn,source.clusterId(),source.databaseId(),request));
            assertInstanceOf(IllegalStateException.class,oversized.getCause());
        }
    }
}
