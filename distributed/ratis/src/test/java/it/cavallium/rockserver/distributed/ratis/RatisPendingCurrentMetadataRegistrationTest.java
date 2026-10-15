package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.*;
import java.lang.reflect.*;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Genuine incomplete native CURRENT and actual socket-free META snapshot installation. */
class RatisPendingCurrentMetadataRegistrationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.CsvSource({"false,false","true,false","false,true","true,true"})
    void interruptedCurrentIsSupersededOnlyByExactGraphMetaAndRealImage(boolean coldRetry,boolean importedConfiguration)throws Exception {
        exercise(coldRetry,importedConfiguration,false,false);
    }
    @org.junit.jupiter.api.Test void unknownCurrentForceCanSettleAfterSourceSealsWithoutOldReceiveAuthority()throws Exception {
        exercise(false,false,true,false);
    }
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.CsvSource({"false,false","true,false","false,true","true,true"})
    void interruptedOwnCheckpointBeyondOriginalCfgUsesActualPublishedFloor(boolean coldRetry,boolean importedConfiguration)throws Exception {
        exercise(coldRetry,importedConfiguration,false,true);
    }
    private void exercise(boolean coldRetry,boolean importedConfiguration,boolean lostCurrentForce,boolean laterOwnCheckpoint)throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false);
            var env=new RatisNodeRuntimeTest.Env(root.resolve("receiver"),false,source.first.digest())) {
            var directory=source.firstDirectory;var parent=directory.ranges().getFirst();var id=RaftGroupId.valueOf(parent.groupId());
            var donor=source.server.machines.get(id);var donorStore=source.store(parent.groupId());
            long cfgIndex=donorStore.applied().index()+1;
            donor.notifyConfigurationChanged(1,cfgIndex,RaftConfigurationProto.parseFrom(donorStore.configuration().data()));
            donor.notifyTermIndexUpdated(1,cfgIndex);
            long cfgDeadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
            while((donorStore.configuration().index()!=cfgIndex || donorStore.applied().index()<cfgIndex) && System.nanoTime()<cfgDeadline)Thread.sleep(2);
            assertEquals(cfgIndex,donorStore.configuration().index());assertTrue(cfgIndex>0);
            var oldRequest=new RatisTopologySourceSnapshotRpc.Request(new UUID(12510,1),null,source.plan.catalogue().groupId(),source.local,
                    RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,parent.groupId(),directory);
            var old=RatisTopologySourceSnapshotRpc.reply(source.server.machines.get(id).queryLinearizable(message(RatisTopologySourceSnapshotRpc.request(RatisTopologySourceSnapshotRpc.PREPARE,oldRequest))).get(15,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologySourceSnapshotRpc.PREPARE,oldRequest);
            var sourceOwner=field(source.node,"pools",Map.class).get(source.fast);var artifacts=field(sourceOwner,"artifacts",RatisTopologySourceSnapshotArtifacts.class);
            var oldImage=artifacts.lookup(oldRequest).image();var originalCut=new RatisCatalogue(source.catalogue,directory.clusterId(),directory.databaseId(),oldRequest.catalogue(),source.plan.catalogue().epoch(),source.plan.catalogue().schemaDigest(),source.plan.catalogue().directoryColumn()).readView(directory.domainId()).cut();
            var predecessor=new NativeCurrentRangeRecovery(oldRequest.transferId(),oldRequest.catalogue(),originalCut,directory,old.schema(),parent.groupId(),source.local,old.sourceOwner(),old.image().position(),old.image().sha256(),old.sourceCfg().term(),old.sourceCfg().index(),old.sourceCfg().data(),old.image().topologyOrigin(),Map.of(source.first.columnIds().iterator().next(),"data"),old.image().deterministicMergeColumns(),old.schemaRows());

            // This receiver initially owns no DATA placement. Its retained CURRENT is seeded before exposure;
            // creating the split children first would legitimately overlap the old parent's interval.
            var remoteParent=new RangeDescriptor(parent.rangeId(),parent.groupId(),parent.rangeEpoch(),parent.configurationVersion(),parent.lowerInclusive(),parent.upperExclusive(),parent.topologyState(),List.of(source.plan.catalogue().replicas().getFirst()));
            var remoteDirectory=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),directory.generation(),directory.columns(),directory.routingPrefixBytes(),List.of(remoteParent));
            var domain=new RatisNodePlan.Domain(remoteDirectory,Map.of(source.first.columnIds().iterator().next(),"data"),Set.of());var pool=RatisPoolFixtures.ID;var c=source.plan.catalogue();
            // A dedicated local control owner supplies the node endpoint without owning any DATA interval.
            var homeGroup=new UUID(12510,4);var homeColumn=new UUID(12510,5);
            var homeReplica=new ReplicaDescriptor(source.local.nodeId(),source.local.nodeIncarnation(),new UUID(12510,6),source.local.peerId(),source.local.address(),ReplicaDescriptor.Role.VOTER);
            var homeRange=new RangeDescriptor(homeGroup,homeGroup,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(homeReplica));
            var homeDirectory=new RangeDirectory(directory.clusterId(),directory.databaseId(),homeGroup,1,new byte[32],0,Set.of(homeColumn),0,List.of(homeRange));
            var home=new RatisNodePlan.Home(RatisNodePlan.Role.TRANSACTION_HOME,homeDirectory,homeColumn,"pending-current-home");
            var plan=new RatisNodePlan(source.plan.nodeId(),source.plan.nodeIncarnation(),List.of(domain),c,List.of(home),List.of(),Map.of(),4,List.of(RatisPoolFixtures.spec(pool,false)),Map.of(directory.domainId(),pool),Map.of(c.groupId(),pool,homeGroup,pool));
            var settings=new RatisNodeRuntime.Settings(source.manifest.settings().images(),env.settings().retention(),env.settings().clockPeriod(),1,4,Duration.ofSeconds(10),env.settings().exports(),env.settings().resources());
            var epochs=new RatisSessionEpochs(source.catalogue,directory.clusterId(),directory.databaseId(),c.groupId(),c.epoch(),c.schemaDigest(),c.sessionsColumn());
            try(var route=new RatisRoutedClient(remoteDirectory,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID column,byte[] key){throw new AssertionError("No USER route");}public CommandResult command(RangeDescriptor r,DistributedCommand command){throw new AssertionError("No USER route");}})) {
                var receiver=new AtomicReference<RaftServer>();var machines=new HashMap<RaftGroupId,RatisRangeStateMachine>();
                RatisNodeRuntime node;
                ApplyPosition restoreTarget=predecessor.imageCut();long restoreConfiguration=predecessor.configurationIndex(),retainedFloor=-1;
                if(!lostCurrentForce) {
                var initial=new RatisNodeRuntimeTest.FakeServer();initial.skipFirstNoop=true;
                try(var initialNode=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(directory.domainId(),route),env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,initial::create,List.of(source.first))) {
                    initialNode.relocationTransport(range->client(source,range),(range,owner)->client(source,range));
                    initialNode.start();var pending=laterOwnCheckpoint?initialNode.registerRecovery(old):initialNode.nativePool(pool).openCurrentRangeRecovery(predecessor);
                    Path restoreImage=oldImage;
                    if(laterOwnCheckpoint) {
                        // Establish the original receipt, then publish this receiver's later GROUP
                        // through the actual SM snapshot writer, not an injected floor or cursor.
                        pending.installSnapshot(oldImage,settings.images());
                        pending.completeCurrentRangeRecovery(predecessor,old.image().position(),pending.configuration());
                        var localMachine=field(initialNode,"machines",Map.class).get(id);
                        var adopted=(RatisRangeStateMachine)localMachine;
                        long ownIndex=pending.applied().index()+1;
                        var ownCfg=RaftConfigurationProto.parseFrom(pending.configuration().data());
                        adopted.notifyConfigurationChanged(1,ownIndex,ownCfg);adopted.notifyTermIndexUpdated(1,ownIndex);
                        long ownDeadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
                        while((pending.applied().index()<ownIndex || adopted.getLastAppliedTermIndex().getIndex()<ownIndex || pending.configuration().index()!=ownIndex) && System.nanoTime()<ownDeadline)Thread.sleep(2);
                        assertEquals(new ApplyPosition(1,ownIndex),pending.applied());assertEquals(ownIndex,pending.configuration().index());
                        assertTrue(ownIndex>predecessor.imageCut().index());assertTrue(ownIndex>predecessor.configurationIndex());
                        // FakeServer delivers the real CFG callback but has no Raft apply loop
                        // to persist its committed configuration before this cold restart.
                        var raftStorage=(org.apache.ratis.server.storage.RaftStorageImpl)initial.storages.get(id);
                        raftStorage.writeRaftConfiguration(LogEntryProto.newBuilder().setTerm(1).setIndex(ownIndex).setConfigurationEntry(ownCfg).build());
                        var persistedCfg=raftStorage.readRaftConfiguration();assertNotNull(persistedCfg);
                        assertEquals(ownIndex,persistedCfg.getLogEntryIndex());
                        assertEquals(ownCfg,org.apache.ratis.server.raftlog.LogProtoUtils.toRaftConfigurationProtoBuilder(persistedCfg).build());
                        assertEquals(ownIndex,adopted.takeSnapshot());var published=adopted.getLatestSnapshot();assertNotNull(published);
                        restoreImage=published.getFiles().getFirst().getPath();restoreTarget=new ApplyPosition(published.getTerm(),published.getIndex());
                        restoreConfiguration=ownIndex;retainedFloor=ownIndex;
                        assertEquals(NativeRangeImage.Kind.GROUP,NativeRangeImage.kind(restoreImage,NativeRangeImage.IoAdmission.UNLIMITED));
                        assertArrayEquals(NativeRangeImage.inspect(restoreImage,settings.images()).sha256(),pending.validateCurrentRangeRecoveryImage(restoreImage,settings.images()).sha256());
                        // The genuine sealed donor must advance at least as far as the receiver's
                        // checkpoint before the consuming topology operation captures its META.
                        donor.notifyConfigurationChanged(1,ownIndex,ownCfg);donor.notifyTermIndexUpdated(1,ownIndex);
                        long donorDeadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
                        while((donorStore.applied().index()<ownIndex || donorStore.configuration().index()!=ownIndex) && System.nanoTime()<donorDeadline)Thread.sleep(2);
                        assertEquals(ownIndex,donorStore.applied().index());assertEquals(ownIndex,donorStore.configuration().index());
                        pending.fence();
                    }
                    final Path installingImage=restoreImage;
                    var interrupted=new AtomicBoolean();var cleared=new AtomicBoolean();
                    // CURRENT deliberately preserves CFG/adoption rows during clear. For an own
                    // checkpoint, observe one real USER row instead; this bypasses no production
                    // permission and performs no mutation in the install fault hook.
                    var raw=laterOwnCheckpoint?field(pending,"db",org.rocksdb.RocksDB.class):null;
                    var userColumn=laterOwnCheckpoint?(org.rocksdb.ColumnFamilyHandle)field(pending,"columns",Map.class).get(source.first.columnIds().iterator().next()):null;
                    byte[] witnessKey=new byte[]{7};var beforeUser=laterOwnCheckpoint?raw.get(userColumn,witnessKey):null;
                    if(laterOwnCheckpoint)assertNotNull(beforeUser,"Own checkpoint must start with a real retained USER row");
                    assertThrows(IOException.class,()->pending.installSnapshot(installingImage,settings.images(),batch->{
                        try {
                            boolean absent=laterOwnCheckpoint?raw.get(userColumn,witnessKey)==null:pending.configuration()==null;
                            if(absent)cleared.set(true);
                            if(cleared.get() && (!importedConfiguration || !absent)) {
                                if(laterOwnCheckpoint && importedConfiguration)assertArrayEquals(beforeUser,raw.get(userColumn,witnessKey));
                                interrupted.set(true);throw new IOException("Interrupted real CURRENT image before final marker");
                            }
                        } catch(org.rocksdb.RocksDBException failed){throw new IOException(failed);}
                    }));
                    assertTrue(cleared.get(),"Actual clear batch must precede the selected interruption");
                    assertTrue(interrupted.get(),"Chosen partial native image boundary must be reached");
                    assertTrue(pending.pendingInstallation());assertEquals(restoreTarget,pending.pendingInstallationCut());assertNull(pending.applied());
                    if(importedConfiguration || laterOwnCheckpoint){assertNotNull(pending.configuration());assertEquals(restoreConfiguration,pending.configuration().index());}
                    else assertNull(pending.configuration());
                }
                    source.split();
                    node=open(env,source,plan,route,epochs,settings,machines,receiver,RatisNodeRuntime.Startup.RECOVER);
                }else {
                    node=open(env,source,plan,route,epochs,settings,machines,receiver,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT);
                    var currentLost=new AtomicBoolean(true);node.nativePool(pool).currentRecoveryForceObserver(after->{if(after && currentLost.getAndSet(false))throw new org.rocksdb.RocksDBException("Lost original CURRENT force acknowledgement");});
                    var firstCurrent=node;assertThrows(IOException.class,()->firstCurrent.registerRecovery(old));assertFalse(currentLost.get());
                    assertNotNull(field(node,"recoveryAttempt",Object.class));assertFalse(machines.containsKey(id));assertTrue(node.nativePool(pool).currentRangeRecoveries().isEmpty());
                    source.split();
                    assertThrows(IOException.class,()->firstCurrent.registerRecovery(old),"Sealed CURRENT can no longer requalify its obsolete source");
                    assertNotNull(field(node,"recoveryAttempt",Object.class));
                }
                var prior=field(sourceOwner,"sources",RatisTopologySourceSnapshotService.class);var host=field(prior,"host",RatisTopologySourceSnapshotService.Host.class);
                var delivery=(RatisTopologySourceSnapshotService.Host)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RatisTopologySourceSnapshotService.Host.class},(proxy,method,args)->{
                    if(method.getName().equals("send")) {
                        var chunk=(InstallSnapshotRequestProto)args[1];var reply=receiver.get().installSnapshot(chunk);
                        try{RatisTopologySourceSnapshotService.requireReply(chunk,reply);}catch(IOException invalid){
                            var rpc=chunk.getServerRequest();var answer=reply.getServerReply();
                            throw new IOException("Actual receiver reply: result="+reply.getResult()+", success="+answer.getSuccess()+", term="+reply.getTerm()+", requestedTerm="+chunk.getLeaderTerm()
                                    +", requestIndex="+chunk.getSnapshotChunk().getRequestIndex()+", replyIndex="+(reply.hasRequestIndex()?reply.getRequestIndex():"absent")
                                    +", requestorMatches="+rpc.getRequestorId().equals(answer.getRequestorId())+", receiverMatches="+rpc.getReplyId().equals(answer.getReplyId())+", groupMatches="+rpc.getRaftGroupId().equals(answer.getRaftGroupId()),invalid);
                        }
                        return reply;
                    }
                    if(method.getName().equals("requireReceiverInstalled")){var prepared=((RatisTopologySourceSnapshotService.Lease)args[0]).prepared();var probe=new RatisRecoveryImageRpc.Request(prepared.request(),prepared.image().position(),prepared.image().sha256());var evidence=machines.get(id).recoveryImageEvidence(probe);evidence.require(probe);if(!evidence.imageInstalled())throw new IOException("Actual receiver image has not completed");return null;}
                    try{return method.invoke(host,args);}catch(InvocationTargetException failed){throw failed.getCause();}
                });
                var sourceMachine=source.server.machines.get(id);var sourceField=RatisRangeStateMachine.class.getDeclaredField("sourceSnapshots");sourceField.setAccessible(true);var oldService=sourceField.get(sourceMachine);
                try(var transport=new RatisTopologySourceSnapshotService(delivery,field(prior,"limits",RatisTopologySourceSnapshotService.Limits.class),source.scheduler(),field(prior,"admission",RatisTopologySourceSnapshotService.ResourceAdmission.class))) {
                    sourceField.set(sourceMachine,transport);
                    try {
                        node.discoverConfiguredRecovery().get(15,TimeUnit.SECONDS);
                        var request=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot().activeRequests().get(parent.groupId());assertTrue(request.metadataOnly());
                        var prepared=transport.prepare(request);assertTrue(NativeMetadataRecoveryRegistration.covers(prepared.image().position(),restoreTarget));
                        RatisRangeStateMachine machine;ReplicatedRangeStore pending;
                        if(!lostCurrentForce) {
                        machine=machines.get(id);pending=field(machine,"store",ReplicatedRangeStore.class);assertTrue(pending.pendingInstallation());assertTrue(machine.recoveryEligibilityGate().discoveryPending());
                        assertNull(pending.applied(),"Destructive install has no completed native A");
                        if(importedConfiguration || laterOwnCheckpoint) {
                            assertNotNull(pending.configuration());assertEquals(restoreConfiguration,pending.configuration().index());
                            assertTrue(pending.configuration().index()>0);
                            var actualCfg=receiver.get().getDivision(id).getRaftConf();
                            if(laterOwnCheckpoint){
                                assertEquals(restoreConfiguration,actualCfg.getLogEntryIndex());
                                assertEquals(RaftConfigurationProto.parseFrom(pending.configuration().data()),org.apache.ratis.server.raftlog.LogProtoUtils.toRaftConfigurationProtoBuilder(actualCfg).build());
                            }else assertTrue(actualCfg.getLogEntryIndex()<=0,"Actual receiver core still has bootstrap CFG");
                        }else assertNull(pending.configuration(),"First real clear batch interrupted before importing native CFG");
                        assertEquals(predecessor,pending.currentRangeRecoveryRegistration());
                        assertEquals(restoreTarget,pending.pendingInstallationCut());
                        assertEquals(retainedFloor,receiver.get().getDivision(id).getRaftLog().getSnapshotIndex());
                        if(laterOwnCheckpoint){assertNotNull(machine.getLatestSnapshot());assertEquals(retainedFloor,machine.getLatestSnapshot().getIndex());}
                        var once=new AtomicBoolean(true);node.nativePool(pool).metadataRecoveryForceObserver(after->{if(after && once.getAndSet(false))throw new org.rocksdb.RocksDBException("Lost real META force acknowledgement");});
                        var firstAttempt=node;var registrationFailure=assertThrows(IOException.class,()->firstAttempt.registerRecovery(prepared));
                        if(once.get())throw new AssertionError("Registration failed before the real lost META force acknowledgement",registrationFailure);
                        assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertFalse(pending.admissionOpen());
                        node.nativePool(pool).metadataRecoveryForceObserver(after->{});
                        if(coldRetry){
                            node.close();machines.clear();node=open(env,source,plan,route,epochs,settings,machines,receiver,RatisNodeRuntime.Startup.RECOVER);machine=machines.get(id);pending=field(machine,"store",ReplicatedRangeStore.class);
                            assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertFalse(pending.pendingInstallation());assertNull(pending.pendingInstallationCut());
                            assertNull(pending.applied());assertNull(pending.currentRangeRecoveryRegistration());assertFalse(pending.metadataRecoveryImageInstalled());
                            assertTrue(pending.metadataRecoveryRegistrationForced(pending.metadataRecoveryRegistration()));
                            assertEquals(NativeMetadataRecoveryRegistration.CurrentPredecessor.of(predecessor),pending.metadataRecoveryRegistration().currentPredecessor());
                            if(importedConfiguration || laterOwnCheckpoint){assertNotNull(pending.configuration());assertEquals(restoreConfiguration,pending.configuration().index());}
                            assertEquals(retainedFloor,receiver.get().getDivision(id).getRaftLog().getSnapshotIndex());
                            if(laterOwnCheckpoint){assertNotNull(machine.getLatestSnapshot());assertEquals(retainedFloor,machine.getLatestSnapshot().getIndex());}else assertNull(machine.getLatestSnapshot());assertFalse(pending.admissionOpen());
                        }
                        assertSame(pending,node.registerRecovery(prepared));
                        }else {
                            var frozen=field(node,"recoveryAttempt",Object.class);var controlLost=new AtomicBoolean(true);
                            var registrationOwner=field(node,"pools",Map.class).get(pool);
                            var retainedClaim=field(registrationOwner,"recoveryMetadata",RatisNodeResourceBudget.Lease.class);
                            long retainedBytes=retainedClaim.bytes(),encodedBytes=field(registrationOwner,"recoveryEncodedBytes",Long.class),exposureBytes=field(registrationOwner,"recoveryExposureBytes",Long.class);
                            node.nativePool(pool).currentRecoveryForceObserver(after->{if(after && controlLost.getAndSet(false))throw new org.rocksdb.RocksDBException("Lost exact CURRENT settlement acknowledgement");});
                            var settling=node;assertThrows(IOException.class,()->settling.registerRecovery(prepared));assertFalse(controlLost.get());
                            assertSame(frozen,field(node,"recoveryAttempt",Object.class));assertFalse(machines.containsKey(id));
                            assertSame(retainedClaim,field(registrationOwner,"recoveryMetadata",RatisNodeResourceBudget.Lease.class));
                            assertEquals(retainedBytes,retainedClaim.bytes());
                            assertEquals(encodedBytes,field(registrationOwner,"recoveryEncodedBytes",Long.class).longValue());
                            assertEquals(exposureBytes,field(registrationOwner,"recoveryExposureBytes",Long.class).longValue());
                            assertTrue(node.nativePool(pool).currentRangeRecoveries().isEmpty());assertTrue(node.nativePool(pool).metadataRecoveries().isEmpty());
                            node.nativePool(pool).currentRecoveryForceObserver(after->{});
                            pending=node.registerRecovery(prepared);
                            machine=(RatisRangeStateMachine)receiver.get().getDivision(id).getStateMachine();
                            assertNotNull(machine);assertSame(pending,field(machine,"store",ReplicatedRangeStore.class));
                            machines.put(id,machine); // The Node dynamically added this actual division after factory construction.
                            assertNull(field(node,"recoveryAttempt",Object.class));assertTrue(node.nativePool(pool).currentRangeRecoveries().isEmpty());
                        }
                        assertFalse(pending.pendingInstallation());assertFalse(machine.recoveryEligibilityGate().discoveryPending());
                        assertEquals(org.apache.ratis.server.impl.RecoveryEligibilityStateMachine.Mode.MISSING,machine.recoveryEligibilityGate().snapshot().mode());
                        assertFalse(pending.metadataRecoveryImageInstalled());assertFalse(pending.admissionOpen());assertNull(pending.applied());
                        assertEquals(NativeMetadataRecoveryRegistration.CurrentPredecessor.of(predecessor),pending.metadataRecoveryRegistration().currentPredecessor());
                        assertThrows(IllegalStateException.class,machine.recoveryEligibilityGate()::snapshotPermit);
                        var exactNode=node;var actualRegistration=pending.metadataRecoveryRegistration();
                        assertThrows(IOException.class,()->exactNode.registerRecovery(new RatisTopologySourceSnapshotRpc.Prepared(request.successor(new UUID(12510,3)),prepared.image(),prepared.schema(),prepared.sourceOwner(),prepared.sourceCfg(),prepared.schemaRows(),prepared.metadataFreeze())));
                        assertEquals(actualRegistration,pending.metadataRecoveryRegistration());
                        transport.transfer(request).get(20,TimeUnit.SECONDS);
                        assertTrue(pending.metadataRecoveryImageInstalled());assertTrue(pending.metadataRecoveryPending());assertFalse(pending.admissionOpen());
                        assertEquals(prepared.image().position(),pending.applied());assertEquals(prepared.image().position().index(),receiver.get().getDivision(id).getRaftLog().getSnapshotIndex());
                        assertEquals(NativeRangeImage.Kind.METADATA,NativeRangeImage.kind(machine.getLatestSnapshot().getFiles().getFirst().getPath(),NativeRangeImage.IoAdmission.UNLIMITED));
                        var head=transport.head(new RatisSourceHeadRpc.Request(request,prepared.image().position()));assertTrue(node.completeRecovery(prepared,head));
                        assertFalse(pending.metadataRecoveryPending());assertFalse(pending.admissionOpen());assertTrue(pending.userHandlesDetached());
                    } finally {sourceField.set(sourceMachine,oldService);node.close();}
                }
            }
        }
    }
    static RatisNodeRuntime open(RatisNodeRuntimeTest.Env env,RatisTwoPoolWorkflowFixture source,RatisNodePlan plan,RatisRoutedClient route,RatisSessionEpochs epochs,RatisNodeRuntime.Settings settings,Map<RaftGroupId,RatisRangeStateMachine> machines,AtomicReference<RaftServer> server,RatisNodeRuntime.Startup startup)throws Exception {
        var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(route.directory().domainId(),route),env.scheduler,env.timer,()->20L,settings,startup,(p,logs,created)->{machines.putAll(created);var inventory=created.get(RaftGroupId.valueOf(source.firstDirectory.ranges().getFirst().groupId()));if(inventory!=null && !inventory.recoveryEligibilityGate().discoveryPending())inventory.deferRecoveryDiscovery();var actual=RatisLocalMetadataDiscoveryTest.noSocketServer(p,logs,created);server.set(actual);return actual;},List.of(source.first));
        try {
            node.relocationTransport(range->client(source,range),(range,owner)->client(source,range));node.start();return node;
        }catch(Exception|Error failed){
            try{failed.addSuppressed(new IOException(startupMembership(node,server.get())));}catch(Exception|Error diagnostics){failed.addSuppressed(diagnostics);}
            try{node.close();}catch(Exception|Error cleanup){failed.addSuppressed(cleanup);}throw failed;
        }
    }
    static String startupMembership(RatisNodeRuntime node,RaftServer server)throws Exception {
        var result=new StringBuilder("Actual startup membership (registered groups, bounded):");
        int count=0;
        for(var group:field(node,"groups",RatisRuntimeGroups.class).raftGroups()) {
            if(++count>4){result.append(" group-limit");break;}
            result.append(" group=").append(group.getGroupId()).append(" expected=").append(peerSummary(group.getPeers()));
            try {
                var division=server.getDivision(group.getGroupId());var cfg=division.getRaftConf();
                result.append(" cfgIndex=").append(cfg.getLogEntryIndex());
                for(var role:List.of(RaftPeerRole.FOLLOWER,RaftPeerRole.LISTENER))result.append(' ').append(role).append('=').append(peerSummary(cfg.getCurrentPeers(role))).append(" previous=").append(peerSummary(cfg.getPreviousPeers(role)));
                var persisted=((org.apache.ratis.server.storage.RaftStorageImpl)division.getRaftStorage()).readRaftConfiguration();
                result.append(" persistedCfg=").append(persisted==null?"absent":persisted.getLogEntryIndex());
                if(persisted!=null)for(var role:List.of(RaftPeerRole.FOLLOWER,RaftPeerRole.LISTENER))result.append(" persisted").append(role).append('=').append(peerSummary(persisted.getCurrentPeers(role))).append(" previous=").append(peerSummary(persisted.getPreviousPeers(role)));
                var machine=(RatisRangeStateMachine)division.getStateMachine();var snapshot=machine.getLatestSnapshot();var store=field(machine,"store",ReplicatedRangeStore.class);
                result.append(" nativeA=").append(store.applied()).append(" nativeCfg=").append(store.configuration()==null?"absent":store.configuration().term()+"/"+store.configuration().index());
                result.append(" snapshot=").append(snapshot==null?"absent":snapshot.getTermIndex()+":"+snapshot.getFiles().getFirst().getPath());
                var current=store.currentRangeRecoveryRegistration();if(current!=null)result.append(" originalCurrent=").append(current.transferId()).append(" image=").append(current.imageCut()).append(" cfg=").append(current.configurationTerm()).append('/').append(current.configurationIndex());
            }catch(Exception|Error unavailable){result.append(" unavailable=").append(unavailable.getClass().getSimpleName()).append(':').append(unavailable.getMessage());}
        }
        return result.toString();
    }
    static List<String> peerSummary(Collection<RaftPeer> peers){return peers.stream().limit(8).map(peer->peer.getId()+"@"+peer.getAddress()+"|stream="+Objects.toString(peer.getDataStreamAddress(),"")+"|role="+peer.getStartupRole()).toList();}
    static Message message(byte[] value){return Message.valueOf(ByteString.copyFrom(value));}
    static RaftClient client(RatisTwoPoolWorkflowFixture source,RangeDescriptor range){
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{if(range.groupId().equals(source.plan.catalogue().groupId()))try{return method.invoke(source.catalogue.io(),args);}catch(InvocationTargetException failed){throw failed.getCause();}var machine=source.server.machines.get(RaftGroupId.valueOf(range.groupId()));var query=(Message)args[0];var result=switch(method.getName()){case "sendReadOnly"->machine.queryLinearizable(query);case "sendStaleRead"->machine.queryStale(query,(long)args[1]);default->throw new AssertionError(method);};return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(source.plan.peerId())).setGroupId(RaftGroupId.valueOf(range.groupId())).setCallId(1).setSuccess(true).setMessage(result.get(15,TimeUnit.SECONDS)).build();});
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "getGroupId"->RaftGroupId.valueOf(range.groupId());case "close"->null;default->throw new AssertionError(method);});
    }
}
