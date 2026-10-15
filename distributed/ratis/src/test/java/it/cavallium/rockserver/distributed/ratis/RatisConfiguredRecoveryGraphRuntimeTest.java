package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.lang.reflect.InvocationTargetException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Actual CAT lineage and Node-owned forced graph; controlled client calls and division lifecycle only. */
class RatisConfiguredRecoveryGraphRuntimeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisTwoPoolWorkflowFixture source;
        final TopologyChange.Definition split;
        final RatisNodeRuntimeTest.Env env;
        final RatisNodePlan plan;
        final RatisRoutedClient route;
        final RatisSessionEpochs epochs;
        final AtomicInteger calls=new AtomicInteger(),clientCloses=new AtomicInteger(),hostCalls=new AtomicInteger();
        final AtomicBoolean block=new AtomicBoolean();
        final CountDownLatch entered=new CountDownLatch(1),release=new CountDownLatch(1);
        Fixture(String name)throws Exception {
            source=new RatisTwoPoolWorkflowFixture(root.resolve(name+"-source"),false);split=source.split();env=new RatisNodeRuntimeTest.Env(root.resolve(name+"-receiver"),false,source.first.digest());var pool=RatisPoolFixtures.ID;var catalogue=source.plan.catalogue();var domain=new RatisNodePlan.Domain(split.sourceDirectory(),Map.of(source.first.columnIds().iterator().next(),"data"),Set.of());
            plan=new RatisNodePlan(source.plan.nodeId(),source.plan.nodeIncarnation(),List.of(domain),catalogue,List.of(),List.of(),Map.of(),4,List.of(RatisPoolFixtures.spec(pool,false)),Map.of(source.first.domainId(),pool),Map.of(catalogue.groupId(),pool));
            route=new RatisRoutedClient(split.sourceDirectory(),new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError("Discovery grants no USER read");}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError("Discovery grants no USER write");}});epochs=new RatisSessionEpochs(source.catalogue,source.first.clusterId(),source.first.databaseId(),catalogue.groupId(),catalogue.epoch(),catalogue.schemaDigest(),catalogue.sessionsColumn());
        }
        RatisNodeRuntime open(RatisNodeRuntime.Startup startup,RatisNodeRuntimeTest.FakeServer server)throws Exception {
            int before=calls.get();var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(source.first.domainId(),route),env.scheduler,env.timer,()->20L,env.settings(),startup,(configuration,logs,machines)->{assertEquals(before,calls.get(),"Startup graph validation needs no remote call before ServerFactory");if(startup==RatisNodeRuntime.Startup.RECOVER){assertTrue(Files.isRegularFile(env.nodeRoot.resolve("configured-recovery-graph/graph")));try(var lock=java.nio.channels.FileChannel.open(env.nodeRoot.resolve("configured-recovery-graph/graph.lock"),StandardOpenOption.WRITE)){assertThrows(java.nio.channels.OverlappingFileLockException.class,lock::tryLock);}}return server.create(configuration,logs,machines);},List.of(source.first));
            node.relocationTransport(this::client,(range,owner)->client(range));return node;
        }
        private RaftClient client(RangeDescriptor range) {
            if(!range.groupId().equals(plan.catalogue().groupId())){hostCalls.incrementAndGet();throw new AssertionError("Discovery attempted a DATA host");}
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                if(!method.getName().equals("sendReadOnly")){hostCalls.incrementAndGet();throw new AssertionError(method);}
                var request=(Message)args[0];int kind=ScopedTransactionRpc.parse(request.getContent().toByteArray(),RatisRecoveryLineageRpc.REQUEST_BYTES).kind();if(kind!=RatisCatalogueViewRpc.KIND && kind!=RatisRecoveryLineageRpc.KIND){hostCalls.incrementAndGet();throw new AssertionError("Unexpected discovery action "+kind);}
                calls.incrementAndGet();if(block.compareAndSet(true,false)){entered.countDown();if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Blocked CAT read was not released");}
                // This fixture places CAT remotely, in its actual standalone catalogue state machine.
                var result=source.catalogueMachine.queryLinearizable(request).get(10,TimeUnit.SECONDS);return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(range.replicas().getFirst().peerId())).setGroupId(RaftGroupId.valueOf(range.groupId())).setCallId(1).setSuccess(true).setMessage(result).build();
            });
            return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "getGroupId"->RaftGroupId.valueOf(range.groupId());case "close"->{clientCloses.incrementAndGet();yield null;}default->throw new AssertionError(method);});
        }
        void assertNoDispatch(RatisNodeRuntime node,RatisNodeRuntimeTest.FakeServer server)throws Exception {
            assertEquals(0,hostCalls.get());assertEquals(0,field(node,"configuredRecoveries",RatisConfiguredRecoveries.class).size());assertFalse(field(node,"configuredRecoveryServiceStarted",Boolean.class));assertEquals(1,node.localGroups().size());assertEquals(1,server.machines.size());assertTrue(node.nativePool(RatisPoolFixtures.ID).metadataRecoveries().isEmpty());assertTrue(node.nativePool(RatisPoolFixtures.ID).currentRangeRecoveries().isEmpty());assertTrue(node.nativePool(RatisPoolFixtures.ID).topologySeeds().isEmpty());
            var sources=field(field(source.node,"pools",Map.class).get(source.fast),"sources",RatisTopologySourceSnapshotService.class);assertTrue(field(sources,"entries",Map.class).isEmpty());for(var range:split.parents())assertEquals(0,source.store(range.groupId()).activeSnapshotPins());for(var range:split.targets())assertEquals(0,source.store(range.groupId()).activeSnapshotPins());
        }
        public void close()throws Exception{release.countDown();route.close();env.close();source.close();}
    }
    private static Map<UUID,byte[]> frozen(RatisConfiguredRecoveryGraph.Snapshot snapshot){var values=new HashMap<UUID,byte[]>();for(var request:snapshot.frozenRequests())values.put(request.transferId(),RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,request));return values;}
    private static void requireFrozen(Map<UUID,byte[]> expected,RatisConfiguredRecoveryGraph.Snapshot snapshot){var actual=frozen(snapshot);assertEquals(expected.keySet(),actual.keySet());for(var entry:expected.entrySet())assertArrayEquals(entry.getValue(),actual.get(entry.getKey()));}
    private static void shape(RatisConfiguredRecoveryGraph.Snapshot snapshot,TopologyChange.Definition split){assertNotNull(snapshot);assertEquals(2,snapshot.plan().roots().size());assertEquals(3,snapshot.plan().localObligations().size());assertEquals(1,snapshot.plan().definitions().size());assertEquals(1,snapshot.retainedDefinitions().size());assertArrayEquals(split.digest(),snapshot.retainedDefinitions().getFirst().digest());assertEquals(3,snapshot.frozenRequests().size());assertEquals(1,snapshot.frozenRequests().stream().filter(RatisTopologySourceSnapshotRpc.Request::metadataOnly).count());}
    @Test void freshCatGraphForcesOnceReusesFrozenIdentitiesAndColdLoadsWithoutDispatch()throws Exception {
        try(var fixture=new Fixture("graph")) {
            Map<UUID,byte[]> identities;long retained;ApplyPosition cut,pendingCut;Path graphFile=fixture.env.nodeRoot.resolve("configured-recovery-graph/graph");byte[] forced;
            var server=new RatisNodeRuntimeTest.FakeServer();
            try(var node=fixture.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,server)) {
                node.start();var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class);assertNull(graph.snapshot());node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS);assertTrue(Files.isRegularFile(graphFile));assertFalse(Files.exists(graphFile.resolveSibling("graph.pending")));shape(graph.snapshot(),fixture.split);fixture.assertNoDispatch(node,server);identities=frozen(graph.snapshot());cut=graph.snapshot().plan().catalogueCut();retained=field(node,"recoveryGraphCredit",RatisNodeResourceBudget.Lease.class).bytes();assertEquals(4096L+16L*graph.retainedEncodedBytes(),retained);
                var ledger=fixture.epochs.read();assertEquals(CommandResult.Status.SUCCESS,fixture.epochs.execute(fixture.epochs.prepare(new OperationId(new UUID(1240,1),0,0),ledger,ledger.advanceTime(ledger.logicalTimeMillis()+1))).status());node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS);assertTrue(graph.snapshot().plan().catalogueCut().index()>cut.index());shape(graph.snapshot(),fixture.split);requireFrozen(identities,graph.snapshot());assertEquals(retained,field(node,"recoveryGraphCredit",RatisNodeResourceBudget.Lease.class).bytes(),"Clock-only CAT progress cannot accumulate retained graph credit");
                // Lose only the acknowledgement: the Node's real protected journal write succeeds first.
                var files=field(graph,"files",RatisJournalAccess.class);var credit=field(node,"recoveryGraphCredit",RatisNodeResourceBudget.Lease.class);graph.close();var lose=new AtomicBoolean(true);var reads=new AtomicInteger();var writes=new AtomicInteger();
                var uncertain=(RatisJournalAccess)Proxy.newProxyInstance(RatisJournalAccess.class.getClassLoader(),new Class<?>[]{RatisJournalAccess.class},(proxy,method,args)->{
                    if(method.getName().equals("read"))reads.incrementAndGet();
                    final Object result;try{result=method.invoke(files,args);}catch(InvocationTargetException failed){throw failed.getCause();}
                    if(method.getName().equals("write")){writes.incrementAndGet();if(lose.getAndSet(false))throw new IOException("Lost actual protected graph force acknowledgement");}return result;
                });
                graph=new RatisConfiguredRecoveryGraph(graphFile.getParent(),fixture.plan,uncertain,(kept,incoming)->credit.growTo(Math.max(credit.bytes(),Math.addExact(4096L,Math.multiplyExact(16L,Math.addExact(kept,incoming))))));var graphField=RatisNodeRuntime.class.getDeclaredField("recoveryGraph");graphField.setAccessible(true);graphField.set(node,graph);credit.shrinkTo(4096L+16L*graph.retainedEncodedBytes(),1);var previous=graph.snapshot();
                advanceClock(fixture,1);var failure=assertThrows(ExecutionException.class,()->node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS));assertInstanceOf(IOException.class,failure.getCause());var candidate=field(graph,"dirty",RatisConfiguredRecoveryGraph.Snapshot.class);assertNotNull(candidate);shape(candidate,fixture.split);requireFrozen(identities,candidate);pendingCut=candidate.plan().catalogueCut();assertTrue(pendingCut.index()>previous.plan().catalogueCut().index());assertEquals(previous.plan().catalogueCut(),graph.snapshot().plan().catalogueCut());assertEquals(1,writes.get());byte[] pendingBytes=Files.readAllBytes(graphFile);assertEquals(4096L+16L*graph.retainedEncodedBytes(),credit.bytes());assertTrue(credit.bytes()>retained,"Uncertain candidate retains both graph generations and its encoded retry body");
                advanceClock(fixture,2);int pendingCalls=fixture.calls.get(),pendingReads=reads.get();node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS);assertEquals(pendingCalls,fixture.calls.get(),"Pending force settles before any later CAT discovery");assertEquals(pendingReads,reads.get(),"Warm pending force requires no journal reread");assertEquals(2,writes.get());assertNull(field(graph,"dirty",RatisConfiguredRecoveryGraph.Snapshot.class));assertEquals(pendingCut,graph.snapshot().plan().catalogueCut(),"Retry must not adopt the now newer CAT cut");requireFrozen(identities,graph.snapshot());assertArrayEquals(pendingBytes,Files.readAllBytes(graphFile));assertEquals(retained,credit.bytes(),"Settled retry refunds only its old graph and encoded candidate");assertEquals(4096L+16L*graph.retainedEncodedBytes(),credit.bytes());
                int calls=fixture.calls.get();field(node,"configuredRecoveries",RatisConfiguredRecoveries.class).step().get(10,TimeUnit.SECONDS);assertEquals(calls,fixture.calls.get());fixture.assertNoDispatch(node,server);forced=Files.readAllBytes(graphFile);
            }
            int calls=fixture.calls.get();server=new RatisNodeRuntimeTest.FakeServer();
            try(var node=fixture.open(RatisNodeRuntime.Startup.RECOVER,server)) {
                var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class);shape(graph.snapshot(),fixture.split);requireFrozen(identities,graph.snapshot());assertEquals(pendingCut,graph.snapshot().plan().catalogueCut());assertEquals(retained,field(node,"recoveryGraphCredit",RatisNodeResourceBudget.Lease.class).bytes());assertEquals(calls,fixture.calls.get());assertArrayEquals(forced,Files.readAllBytes(graphFile));node.start();fixture.assertNoDispatch(node,server);assertEquals(calls,fixture.calls.get());
            }
        }
    }
    private void advanceClock(Fixture fixture,long sequence)throws Exception {
        var ledger=fixture.epochs.read();assertEquals(CommandResult.Status.SUCCESS,fixture.epochs.execute(fixture.epochs.prepare(new OperationId(new UUID(1240,1),0,sequence),ledger,ledger.advanceTime(ledger.logicalTimeMillis()+1))).status());
    }
    @Test void localMetadataBootstrapSettlesUncertainGraphThenStillReadsFreshCat()throws Exception {
        try(var fixture=new Fixture("bootstrap-pending")) {
            try(var node=fixture.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,new RatisNodeRuntimeTest.FakeServer())) {
                node.start();node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS);
                var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class);var files=field(graph,"files",RatisJournalAccess.class);var credit=field(node,"recoveryGraphCredit",RatisNodeResourceBudget.Lease.class);var fail=new AtomicBoolean(true);graph.close();
                var uncertain=(RatisJournalAccess)Proxy.newProxyInstance(RatisJournalAccess.class.getClassLoader(),new Class<?>[]{RatisJournalAccess.class},(proxy,method,args)->{
                    final Object result;try{result=method.invoke(files,args);}catch(InvocationTargetException failed){throw failed.getCause();}
                    if(method.getName().equals("write") && fail.getAndSet(false))throw new IOException("Lost graph force acknowledgement");return result;
                });
                graph=new RatisConfiguredRecoveryGraph(fixture.env.nodeRoot.resolve("configured-recovery-graph"),fixture.plan,uncertain,(kept,incoming)->credit.growTo(Math.max(credit.bytes(),4096L+16L*Math.addExact(kept,incoming))));
                RatisTopologySeedRuntimeTest.field(node,"recoveryGraph",graph);advanceClock(fixture,1);
                assertThrows(ExecutionException.class,()->node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS));
                var pending=field(graph,"dirty",RatisConfiguredRecoveryGraph.Snapshot.class);assertNotNull(pending);int calls=fixture.calls.get();advanceClock(fixture,2);
                node.bootstrapDeferredLocalMetadata().get(15,TimeUnit.SECONDS);
                assertTrue(fixture.calls.get()>calls,"Settling an old force cannot substitute for fresh role authority");assertNull(field(graph,"dirty",RatisConfiguredRecoveryGraph.Snapshot.class));assertTrue(graph.snapshot().plan().catalogueCut().index()>pending.plan().catalogueCut().index());assertEquals(4096L+16L*graph.retainedEncodedBytes(),credit.bytes());
                assertFalse(field(node,"configuredRecoveryServiceStarted",Boolean.class));assertEquals(0,fixture.hostCalls.get());
            }
        }
    }
    @Test void rediscoveryUsesDurableActiveAttemptInsteadOfFirstMatchingHistoricalRequest()throws Exception {
        try(var fixture=new Fixture("active")) {
            UUID group,renewedId=new UUID(Long.MAX_VALUE,Long.MAX_VALUE);Map<UUID,byte[]> history;
            var server=new RatisNodeRuntimeTest.FakeServer();
            try(var node=fixture.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,server)) {
                node.start();node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS);var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class);var snapshot=graph.snapshot();var previous=snapshot.activeRequests().values().iterator().next();group=previous.sourceGroup();
                var active=new ArrayList<>(snapshot.activeRequests().values());active.removeIf(request->request.sourceGroup().equals(group));active.add(previous.successor(renewedId));
                field(node,"configuredRecoveries",RatisConfiguredRecoveries.class).discover(()->{graph.force(snapshot.plan(),active);return null;}).get(15,TimeUnit.SECONDS);
                assertEquals(renewedId,graph.snapshot().activeRequests().get(group).transferId());assertNotEquals(renewedId,graph.snapshot().frozenRequests().stream().filter(request->request.sourceGroup().equals(group)).findFirst().orElseThrow().transferId());history=frozen(graph.snapshot());
                node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS);assertEquals(renewedId,graph.snapshot().activeRequests().get(group).transferId());requireFrozen(history,graph.snapshot());assertEquals(4096L+16L*graph.retainedEncodedBytes(),field(node,"recoveryGraphCredit",RatisNodeResourceBudget.Lease.class).bytes());fixture.assertNoDispatch(node,server);
            }
            try(var node=fixture.open(RatisNodeRuntime.Startup.RECOVER,new RatisNodeRuntimeTest.FakeServer())) {
                var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class);assertEquals(renewedId,graph.snapshot().activeRequests().get(group).transferId());requireFrozen(history,graph.snapshot());node.start();node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS);assertEquals(renewedId,graph.snapshot().activeRequests().get(group).transferId());requireFrozen(history,graph.snapshot());
            }
        }
    }
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void cancelledDiscoveryAndNodeCloseRetainActualCatGraphNativeAndTransportOwnership(boolean bootstrap)throws Exception {
        try(var fixture=new Fixture("blocked")) {
            var server=new RatisNodeRuntimeTest.FakeServer();var node=fixture.open(RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,server);CompletableFuture<Void> closing=null;
            try {
                node.start();var resources=field(node,"resources",RatisNodeResourceBudget.class);var credit=field(node,"recoveryGraphCredit",RatisNodeResourceBudget.Lease.class);var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class);fixture.block.set(true);var observer=bootstrap?node.bootstrapDeferredLocalMetadata():node.discoverConfiguredRecovery();assertTrue(fixture.entered.await(10,TimeUnit.SECONDS));assertTrue(observer.cancel(false));assertTrue(field(node,"installing",Boolean.class));assertNull(graph.snapshot());assertFalse(credit.released());assertTrue(resources.stats().protectedLane().bytes()>=credit.bytes()+4096);
                closing=CompletableFuture.runAsync(()->{try{node.close();}catch(IOException failed){throw new CompletionException(failed);}});long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(2);while(node.state()!=RatisNodeRuntime.State.STOPPING && System.nanoTime()<deadline)Thread.sleep(1);assertEquals(RatisNodeRuntime.State.STOPPING,node.state());assertFalse(closing.isDone());assertFalse(credit.released());assertEquals(0,server.closes);assertEquals(0,fixture.clientCloses.get());assertEquals(1,node.nativePool(RatisPoolFixtures.ID).retainedMetadataOwners().size());var store=field(server.machines.get(RaftGroupId.valueOf(fixture.split.parents().getFirst().groupId())),"store",ReplicatedRangeStore.class);assertNotNull(store.configuration());
                fixture.release.countDown();closing.get(15,TimeUnit.SECONDS);assertEquals(RatisNodeRuntime.State.CLOSED,node.state());assertTrue(credit.released());assertEquals(1,server.closes);assertTrue(fixture.clientCloses.get()>0);assertEquals(new RatisNodeResourceBudget.Usage(0,0),resources.stats().ordinary());assertEquals(new RatisNodeResourceBudget.Usage(0,0),resources.stats().protectedLane());assertTrue(Files.isRegularFile(fixture.env.nodeRoot.resolve("configured-recovery-graph/graph")));assertEquals(0,fixture.hostCalls.get());
            }finally{fixture.release.countDown();if(closing!=null)closing.get(15,TimeUnit.SECONDS);node.close();}
        }
    }
}
