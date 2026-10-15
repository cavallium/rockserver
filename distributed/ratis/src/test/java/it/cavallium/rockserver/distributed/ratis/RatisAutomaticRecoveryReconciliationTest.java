package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.*;
import java.nio.charset.StandardCharsets;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Real native capture, CAT state and receiver Raft dispatch with controlled source transport. */
class RatisAutomaticRecoveryReconciliationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;

    @ParameterizedTest @ValueSource(booleans={false,true})
    void lostCurrentAcknowledgementIsAutomaticallyFencedAndCleanedBeforeMetaPrepareAcrossColdRetry(boolean registrationForceLoss)throws Exception {
        var local=new ReplicaDescriptor(new UUID(13210,1),new UUID(13210,2),new UUID(13210,3),"receiver13210","127.0.0.1:4598",ReplicaDescriptor.Role.LISTENER);
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false,List.of(local))) {
            var directory=source.firstDirectory;var parent=directory.ranges().getFirst();var group=parent.groupId();var binding=source.plan.catalogue();
            // A CURRENT registration requires an absent DATA owner, so the force-loss case starts
            // with only a genuine local HOME and the original remote DATA geometry as its witness.
            var bootstrapRange=registrationForceLoss?new RangeDescriptor(parent.rangeId(),group,parent.rangeEpoch(),parent.configurationVersion(),parent.lowerInclusive(),parent.upperExclusive(),parent.topologyState(),List.of(source.local)):parent;
            var bootstrapDirectory=registrationForceLoss?new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),directory.generation(),directory.columns(),directory.routingPrefixBytes(),List.of(bootstrapRange)):directory;
            var domain=new RatisNodePlan.Domain(bootstrapDirectory,Map.of(source.first.columnIds().iterator().next(),"data"),Set.of());
            var homes=new ArrayList<RatisNodePlan.Home>();
            if(registrationForceLoss){
                var homeId=new UUID(13210,10);var homeColumn=new UUID(13210,11);
                var homeReplica=new ReplicaDescriptor(local.nodeId(),local.nodeIncarnation(),new UUID(13210,12),local.peerId(),local.address(),ReplicaDescriptor.Role.VOTER);
                var homeRange=new RangeDescriptor(homeId,homeId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(homeReplica));
                var homeDirectory=new RangeDirectory(directory.clusterId(),directory.databaseId(),homeId,1,new byte[32],0,Set.of(homeColumn),0,List.of(homeRange));
                homes.add(new RatisNodePlan.Home(RatisNodePlan.Role.TRANSACTION_HOME,homeDirectory,homeColumn,"reconciliation-home"));
            }
            var plan=new RatisNodePlan(local.nodeId(),local.nodeIncarnation(),List.of(domain),binding,homes,List.of(),Map.of(),8,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(binding,homes));
            var receiverRoot=Files.createDirectory(root.resolve("receiver"));SessionJournalFiles.force(receiverRoot);SessionJournalFiles.force(root);
            var epochs=new RatisSessionEpochs(source.catalogue,directory.clusterId(),directory.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());
            var server=new AtomicReference<RaftServer>();var lost=new AtomicBoolean(registrationForceLoss);var currentPrepares=new AtomicInteger();var metaPrepares=new AtomicInteger();var obsoleteCalls=new AtomicInteger();
            var sourceOwner=field(source.node,"pools",Map.class).get(source.fast);var artifacts=field(sourceOwner,"artifacts",RatisTopologySourceSnapshotArtifacts.class);var sources=field(sourceOwner,"sources",RatisTopologySourceSnapshotService.class);
            // The source validates OBSOLETE against the actual receiver's persisted fence via query120.
            var originalTransport=field(source.node,"relocationClients",RatisRelocationDataClient.class);
            try(var sourceTransport=new RatisRelocationDataClient(directory.clusterId(),directory.databaseId(),binding.groupId(),range->RatisPendingCurrentMetadataRegistrationTest.client(source,range),(range,owner)->{
                assertEquals(local.nodeId(),owner.nodeId());return receiverClient(server.get(),range);
            })) {
            field(source.node,"relocationClients",sourceTransport);
            try(var route=new RatisRoutedClient(bootstrapDirectory,new RatisRoutedClient.Transport(){
                public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError("No USER read during reconciliation");}
                public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError("No USER command during reconciliation");}
            })) {
                RatisNodeRuntime node=null;
                try {
                    node=open(source,plan,receiverRoot,epochs,route,server,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,lost,currentPrepares,metaPrepares,obsoleteCalls);
                    node.discoverConfiguredRecovery().get(20,TimeUnit.SECONDS);
                    var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot();var original=graph.activeRequests().get(group);
                    assertNotNull(original);assertFalse(original.metadataOnly());assertEquals(local,original.receiver());
                    node.recoverConfigured(original).get(20,TimeUnit.SECONDS);var manager=field(node,"configuredRecoveries",RatisConfiguredRecoveries.class);
                    var initialManager=manager;NativeCurrentRangeRecovery forcedRegistration=null;
                    if(registrationForceLoss){
                        manager.step().get(20,TimeUnit.SECONDS);assertEquals(RatisConfiguredRecovery.Phase.REGISTER,manager.phase(group));
                        var once=new AtomicBoolean(true);var pool=node.nativePool(RatisPoolFixtures.ID);
                        pool.currentRecoveryForceObserver(after->{if(after && once.getAndSet(false))throw new org.rocksdb.RocksDBException("Lost actual CURRENT registration force acknowledgement");});
                        var failure=assertThrows(ExecutionException.class,()->initialManager.step().get(20,TimeUnit.SECONDS));
                        assertTrue(failureChain(failure).contains("Lost actual CURRENT registration force acknowledgement"),()->failureChain(failure));assertFalse(once.get());
                        var attempt=field(node,"recoveryAttempt",Object.class);assertNotNull(attempt);forcedRegistration=field(attempt,"current",NativeCurrentRangeRecovery.class);
                        assertArrayEquals(forcedRegistration.encode(),rawState(pool,group,0x72),"The actual registration WriteBatch must have persisted before its ACK is lost");
                        assertNull(rawState(pool,group,0x73));assertNull(store(node,group));assertEquals(RatisConfiguredRecovery.Phase.REGISTER,manager.phase(group));
                        pool.currentRecoveryForceObserver(after->{});
                    }else{
                        var prepareFailure=assertThrows(ExecutionException.class,()->initialManager.step().get(20,TimeUnit.SECONDS));
                        assertTrue(hasLostPrepareAcknowledgement(prepareFailure),()->"Expected injected lost PREPARE ACK; actual failure: "+failureChain(prepareFailure));
                        assertTrue(lost.get());assertEquals(RatisConfiguredRecovery.Phase.PREPARE,manager.phase(group));
                    }
                    assertEquals(1,currentPrepares.get());
                    var nativePool=node.nativePool(RatisPoolFixtures.ID);byte[] factualMarker=rawState(nativePool,group,null),factualConfiguration=rawState(nativePool,group,1);
                    assertEquals(1,sources.stats().leases());assertEquals(0,source.store(group).activeSnapshotPins(),"Forced READY files release the native capture pin before PREPARE ACK");assertTrue(sources.stats().reservedImageBytes()>0);assertNotNull(artifacts.lookup(original));assertEquals(0,metaPrepares.get());
                    var consuming=source.publishSplit();assertTrue(source.store(group).topologyRetired());
                    node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);
                    // Resolve the journal from the manager's actual owned slot.
                    var slot=field(manager,"groups",Map.class).get(group);var journal=field(slot,"directory",Path.class).resolve("recovery");
                    var before=RatisSchemaClientJournal.read(journal);assertEquals("FENCE_RECEIVER",ascii(before.get("phase")));assertArrayEquals(consuming.header().encode(),NativeRecoverySupersession.Scope.decode(before.get("supersessionScope")).header().encode());
                    byte[] frozenScope=before.get("supersessionScope");var selected=RatisTopologySourceSnapshotRpc.request(before.get("selected"));assertTrue(selected.metadataOnly());
                    assertArrayEquals(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,original),before.get("initial"));assertEquals(registrationForceLoss,before.containsKey("prepared"));if(registrationForceLoss)assertNotNull(field(node,"recoveryAttempt",Object.class));
                    node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);assertArrayEquals(frozenScope,RatisSchemaClientJournal.read(journal).get("supersessionScope"));assertEquals(0,metaPrepares.get());
                    stepUntil(manager,journal,"CLEANUP_OLD");
                    var nativeOwner=store(node,group);assertNotNull(nativeOwner.acknowledgedRecoverySupersession());assertFalse(nativeOwner.admissionOpen());
                    assertArrayEquals(factualMarker,rawState(nativePool,group,null));assertArrayEquals(factualConfiguration,rawState(nativePool,group,1));
                    if(registrationForceLoss){assertNull(field(node,"recoveryAttempt",Object.class));assertEquals(forcedRegistration,nativeOwner.currentRangeRecoveryRegistration());assertArrayEquals(forcedRegistration.digest(),nativeOwner.acknowledgedRecoverySupersession().captured().currentRegistrationDigest());assertFalse(nativeOwner.currentRangeRecoveryImageInstalled());}
                    assertArrayEquals(frozenScope,nativeOwner.acknowledgedRecoverySupersession().scope().encode());assertEquals(0,metaPrepares.get());assertEquals(1,sources.stats().leases());
                    node.close();node=null;
                    node=open(source,plan,receiverRoot,epochs,route,server,RatisNodeRuntime.Startup.RECOVER,lost,currentPrepares,metaPrepares,obsoleteCalls);
                    node.bootstrapDeferredRecovery().get(20,TimeUnit.SECONDS);assertArrayEquals(frozenScope,RatisSchemaClientJournal.read(journal).get("supersessionScope"));
                    assertArrayEquals(factualMarker,rawState(node.nativePool(RatisPoolFixtures.ID),group,null));assertArrayEquals(factualConfiguration,rawState(node.nativePool(RatisPoolFixtures.ID),group,1));
                    manager=field(node,"configuredRecoveries",RatisConfiguredRecoveries.class);stepUntil(manager,journal,"PREPARE");
                    var reset=RatisSchemaClientJournal.read(journal);assertArrayEquals(before.get("selected"),reset.get("initial"));assertFalse(reset.containsKey("supersessionScope"));
                    assertTrue(obsoleteCalls.get()>0);assertEquals(0,metaPrepares.get());assertEquals(0,sources.stats().leases());assertEquals(0,source.store(group).activeSnapshotPins());
                    var fence=store(node,group).acknowledgedRecoverySupersession();assertTrue(artifacts.isObsolete(new RatisRecoveryObsoleteRpc.Request(original,fence.scope(),fence.digest())));
                    stepUntil(manager,journal,"REGISTER");assertEquals(1,metaPrepares.get());
                    var prepared=RatisTopologySourceSnapshotRpc.reply(RatisSchemaClientJournal.read(journal).get("prepared"),RatisTopologySourceSnapshotRpc.PREPARE,selected);
                    assertNotNull(prepared.metadataFreeze());assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(artifacts.lookup(selected).image(),NativeRangeImage.IoAdmission.UNLIMITED));
                    assertNull(store(node,group).metadataRecoveryRegistration(),"META PREPARE must precede native registration");
                    assertFalse(store(node,group).admissionOpen());
                }finally{if(node!=null)node.close();}
            }finally{field(source.node,"relocationClients",originalTransport);}
            }
        }
    }

    private static RatisNodeRuntime open(RatisTwoPoolWorkflowFixture source,RatisNodePlan plan,Path root,RatisSessionEpochs epochs,RatisRoutedClient route,AtomicReference<RaftServer> server,RatisNodeRuntime.Startup startup,AtomicBoolean lost,AtomicInteger current,AtomicInteger meta,AtomicInteger obsolete)throws Exception {
        var borrowed=source.service.restoreResources();
        var node=RatisNodeRuntime.open(plan,root,ConfigParser.parseDefault(),epochs,Map.of(source.firstDirectory.domainId(),route),borrowed.scheduler,borrowed.timer,()->20L,source.manifest.settings(),startup,(p,logs,machines)->{
            machines.forEach((id,machine)->{if(p.role(id.getUuid())==RatisNodePlan.Role.DATA && !machine.recoveryEligibilityGate().discoveryPending())machine.deferRecoveryDiscovery();});
            var actual=RatisLocalMetadataDiscoveryTest.noSocketServer(p,logs,machines);server.set(actual);return actual;
        },List.of(source.first));
        try {
            node.relocationTransport(range->sourceClient(source,range,lost,current,meta,obsolete),(range,owner)->sourceClient(source,range,lost,current,meta,obsolete));node.start();
            // Both nodes borrow this scheduler; the manual timer does not run the new node's initial pressure sample.
            field(node,"poolPressure",RatisPoolPressureService.class).refresh().get(10,TimeUnit.SECONDS);return node;
        }catch(Throwable failure){node.close();throw failure;}
    }
    private static RaftClient sourceClient(RatisTwoPoolWorkflowFixture source,RangeDescriptor range,AtomicBoolean lost,AtomicInteger current,AtomicInteger meta,AtomicInteger obsolete) {
        var delegate=RatisPendingCurrentMetadataRegistrationTest.client(source,range);
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            byte[] query=((Message)args[0]).getContent().toByteArray();int kind=ScopedTransactionRpc.kind(query,ScopedTransactionRpc.MAX_REQUEST_BYTES);
            RatisTopologySourceSnapshotRpc.Request request=null;
            if(kind==RatisTopologySourceSnapshotRpc.PREPARE){request=RatisTopologySourceSnapshotRpc.request(query);if(request.metadataOnly())meta.incrementAndGet();else current.incrementAndGet();}
            if(kind==RatisRecoveryObsoleteRpc.KIND)obsolete.incrementAndGet();
            final Object reply;try{reply=method.invoke(delegate.io(),args);}catch(InvocationTargetException failure){throw failure.getCause();}
            if(request!=null && !request.metadataOnly() && lost.compareAndSet(false,true))throw new IOException("Lost actual source PREPARE acknowledgement");
            return reply;
        });
        return wrapper(range.groupId(),io);
    }
    private static RaftClient receiverClient(RaftServer server,RangeDescriptor range) {
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            assertEquals("sendStaleRead",method.getName());assertEquals(0L,args[1]);assertEquals(server.getId(),args[2]);
            return RatisRecoverySupersessionQueryTest.stale(server,server.getDivision(RaftGroupId.valueOf(range.groupId())).getGroup(),((Message)args[0]).getContent().toByteArray(),0);
        });return wrapper(range.groupId(),io);
    }
    private static RaftClient wrapper(UUID group,BlockingApi io) {
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(group);case "io"->io;case "close"->null;default->throw new AssertionError(method);});
    }
    private static byte[] rawState(DistributedStoragePool pool,UUID group,Integer tag)throws Exception {
        var loaded=field(pool,"loaded",RocksDBLoader.LoadedDb.class);var state=(org.rocksdb.ColumnFamilyHandle)field(pool,"handles",Map.class).get(DistributedStoragePool.STATE_COLUMN);
        var key=ByteBuffer.allocate(tag==null?16:17).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits());if(tag!=null)key.put(tag.byteValue());
        return loaded.db().get().get(state,key.array());
    }
    private static ReplicatedRangeStore store(RatisNodeRuntime node,UUID group)throws Exception{return (ReplicatedRangeStore)field(node,"stores",Map.class).get(group);}
    private static boolean hasLostPrepareAcknowledgement(Throwable failure){
        for(var cause=failure;cause!=null;cause=cause.getCause())if(cause instanceof IOException && "Lost actual source PREPARE acknowledgement".equals(cause.getMessage()))return true;
        return false;
    }
    private static String failureChain(Throwable failure){
        var text=new StringBuilder();for(var cause=failure;cause!=null;cause=cause.getCause()){if(!text.isEmpty())text.append(" <- ");text.append(cause);}
        return text.toString();
    }
    private static String ascii(byte[] bytes){return new String(bytes,StandardCharsets.US_ASCII);}
    private static void stepUntil(RatisConfiguredRecoveries manager,Path journal,String phase)throws Exception {
        for(int step=0;step<32;step++) {
            if(phase.equals(ascii(RatisSchemaClientJournal.read(journal).get("phase"))))return;
            try{manager.step().get(20,TimeUnit.SECONDS);}catch(ExecutionException otherGroupNotReady){
                // Published but not activated children may be selected between parent cleanup retries.
                if(!phase.equals("CLEANUP_OLD") && !phase.equals("PREPARE") && !phase.equals("REGISTER"))throw otherGroupNotReady;
            }
        }
        fail("Parent journal did not reach "+phase+"; actual phase="+ascii(RatisSchemaClientJournal.read(journal).get("phase"))+", last failure="+manager.failure());
    }
}
