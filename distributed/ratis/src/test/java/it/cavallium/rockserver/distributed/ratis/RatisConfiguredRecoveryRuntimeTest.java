package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.*;
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.MD5FileUtil;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** Actual source/receiver Runtime ports and native image chunks; only Raft wire delivery/log floor are controlled. */
class RatisConfiguredRecoveryRuntimeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void sourceAbandonmentPortForcesExactGraphSuccessorBeforeReturningReceipt()throws Exception {
        try(var env=new RatisNodeRuntimeTest.Env(root.resolve("source-successor"),false)) {
            var receiver=new RatisNodeRuntimeTest.FakeServer();
            try(var node=RatisNodeRuntime.open(env.plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(env.routed.directory().domainId(),env.routed),env.scheduler,env.timer,()->20L,env.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,receiver::create,List.of())) {
                var directory=env.routed.directory();var range=directory.ranges().getFirst();
                var replica=range.replicas().stream().filter(r->r.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow();
                var request=new RatisTopologySourceSnapshotRpc.Request(new UUID(994,80),null,env.plan.catalogue().groupId(),replica,RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,range.groupId(),directory);
                var cut=new ApplyPosition(1,10);var roots=new ArrayList<RatisConfiguredRecoveryDiscovery.Root>();var obligations=new ArrayList<RatisConfiguredRecoveryDiscovery.Obligation>();var requests=new ArrayList<RatisTopologySourceSnapshotRpc.Request>();
                for(var configured:directory.ranges()) {
                    roots.add(new RatisConfiguredRecoveryDiscovery.Root(directory,configured));var local=configured.replicas().stream().filter(r->r.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow();
                    obligations.add(new RatisConfiguredRecoveryDiscovery.Obligation(request.purpose(),directory,configured,local,null));
                    requests.add(configured.groupId().equals(range.groupId())?request:new RatisTopologySourceSnapshotRpc.Request(new UUID(994,82),null,env.plan.catalogue().groupId(),local,request.purpose(),configured.groupId(),directory));
                }
                var plan=new RatisConfiguredRecoveryDiscovery.Plan(cut,roots,List.of(),List.of(),obligations);
                var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class);
                var source=range.replicas().stream().filter(r->r.role()==ReplicaDescriptor.Role.VOTER).findFirst().orElseThrow();
                var receipt=new RatisTopologySourceSnapshotRpc.Abandoned(request,new UUID(994,81),new ApplyPosition(1,2),source,new byte[32]);
                var client=mock(RaftClient.class);var io=mock(BlockingApi.class);when(client.getGroupId()).thenReturn(RaftGroupId.valueOf(range.groupId()));when(client.io()).thenReturn(io);
                when(io.sendReadOnly(any(Message.class))).thenAnswer(call->{
                    var actual=RatisTopologySourceSnapshotRpc.request(((Message)call.getArgument(0)).getContent().toByteArray());RatisTopologySourceSnapshotRpc.requireRequest(request,actual);
                    return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(source.peerId())).setGroupId(RaftGroupId.valueOf(range.groupId())).setCallId(1).setSuccess(true).setMessage(Message.valueOf(ByteString.copyFrom(RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,receipt)))).build();
                });
                node.relocationTransport(ignored->client,(ignored,owner)->client);
                node.start();graph.force(plan,requests);
                var host=node.configuredRecoveryHost(bytes->{});
                for(int retry=0;retry<2;retry++) {
                    var failed=assertThrows(ExecutionException.class,()->host.prepare(request,null).get(10,TimeUnit.SECONDS));
                    var observed=assertInstanceOf(RatisTopologySourceSnapshotRpc.AbandonedException.class,failed.getCause()).receipt();
                    assertArrayEquals(RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,receipt),RatisTopologySourceSnapshotRpc.reply(RatisTopologySourceSnapshotRpc.PREPARE,observed));
                    RatisTopologySourceSnapshotRpc.requireRequest(receipt.successorRequest(),graph.snapshot().activeRequests().get(range.groupId()));
                    assertEquals(requests.size()+1,graph.snapshot().frozenRequests().size());assertEquals(cut,graph.snapshot().plan().catalogueCut());
                }
            }
        }
    }
    @Test void manualBornCurrentBeforeFirstGraphWaitsForActualAncestorMetadataBeforeSourceIo()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("manual-current-source"),false);var env=new RatisNodeRuntimeTest.Env(root.resolve("manual-current-receiver"),false,source.first.digest())) {
            var split=source.split();var catalogue=source.plan.catalogue();var pool=RatisPoolFixtures.ID;var domain=new RatisNodePlan.Domain(split.sourceDirectory(),Map.of(source.first.columnIds().iterator().next(),"receiver-data"),Set.of());
            var plan=new RatisNodePlan(source.plan.nodeId(),source.plan.nodeIncarnation(),List.of(domain),catalogue,List.of(),List.of(),Map.of(),4,List.of(RatisPoolFixtures.spec(pool,false)),Map.of(source.first.domainId(),pool),Map.of(catalogue.groupId(),pool));
            var epochs=new RatisSessionEpochs(source.catalogue,source.first.clusterId(),source.first.databaseId(),catalogue.groupId(),catalogue.epoch(),catalogue.schemaDigest(),catalogue.sessionsColumn());
            var receiver=new RatisNodeRuntimeTest.FakeServer();receiver.skipFirstNoop=true;var dataCalls=new java.util.concurrent.atomic.AtomicInteger();
            try(var route=new RatisRoutedClient(split.sourceDirectory(),new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError("No USER read");}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError("No USER write");}});
                var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(source.first.domainId(),route),env.scheduler,env.timer,()->20L,env.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,receiver::create,List.of(source.first))) {
                node.relocationTransport(range->{if(!range.groupId().equals(catalogue.groupId())){dataCalls.incrementAndGet();throw new AssertionError("CURRENT contacted a source before ancestor META readiness");}return client(source,range);},(range,owner)->{dataCalls.incrementAndGet();throw new AssertionError("No owner RPC before ancestor META readiness");});node.start();
                var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class);assertNull(graph.snapshot());var child=split.targets().getFirst();
                var request=new RatisTopologySourceSnapshotRpc.Request(new UUID(994,160),null,catalogue.groupId(),child.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,child.groupId(),split.targetDirectory());
                node.recoverConfigured(request).get(15,TimeUnit.SECONDS);var manager=field(node,"configuredRecoveries",RatisConfiguredRecoveries.class);
                var failure=assertThrows(ExecutionException.class,()->manager.step().get(15,TimeUnit.SECONDS));assertInstanceOf(IOException.class,failure.getCause());assertTrue(failure.getCause().getMessage().contains("Ancestor META"),failure::toString);
                assertEquals(0,dataCalls.get());assertNotNull(graph.snapshot());RatisTopologySourceSnapshotRpc.requireRequest(request,graph.snapshot().activeRequests().get(child.groupId()));
                assertEquals(RatisConfiguredRecovery.Phase.PREPARE,manager.phase(child.groupId()));assertFalse(receiver.machines.containsKey(RaftGroupId.valueOf(child.groupId())));assertTrue(node.nativePool(pool).currentRangeRecoveries().isEmpty());assertTrue(node.nativePool(pool).metadataRecoveries().isEmpty());
            }
        }
    }
    private static void awaitDone(RatisNodeRuntime node,RatisTransactionRecoveryService control,RatisConfiguredRecoveries manager,UUID group)throws Exception {
        Throwable last=null;
        for(int steps=0;steps<64 && node.configuredRecoveryPhase(group)!=RatisConfiguredRecovery.Phase.DONE;steps++) {
            control.tick().get(15,TimeUnit.SECONDS);
            try{manager.step().get(15,TimeUnit.SECONDS);}catch(ExecutionException waiting){last=waiting.getCause();}
        }
        if(node.configuredRecoveryPhase(group)!=RatisConfiguredRecovery.Phase.DONE){
            var store=(ReplicatedRangeStore)field(node,"stores",Map.class).get(group);var configuration=store==null?null:store.configuration();
            String nativeState=store==null?"absent":"cut="+store.applied()+", CURRENT-ready="+store.currentRangeRecoveryReady()+", META-pending="+store.metadataRecoveryPending()+", CFG="+(configuration==null?"absent":configuration.term()+"/"+configuration.index()+"/"+HexFormat.of().formatHex(OperatorUpload.sha256(configuration.data())));
            throw new AssertionError("Recovery did not settle for "+group+": phase="+node.configuredRecoveryPhase(group)+", native "+nativeState,last);
        }
    }

    @Test void nodeOwnedRecoveryRecoversMetadataAndCurrentChildrenAndColdJournals()throws Exception {
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("source"),false);var env=new RatisNodeRuntimeTest.Env(root.resolve("receiver"),false,source.first.digest())) {
            var split=source.split();var catalogue=source.plan.catalogue();var pool=RatisPoolFixtures.ID;
            var domain=new RatisNodePlan.Domain(split.sourceDirectory(),Map.of(source.first.columnIds().iterator().next(),"receiver-data"),Set.of());
            var plan=new RatisNodePlan(source.plan.nodeId(),source.plan.nodeIncarnation(),List.of(domain),catalogue,List.of(),List.of(),Map.of(),4,List.of(RatisPoolFixtures.spec(pool,false)),Map.of(source.first.domainId(),pool),Map.of(catalogue.groupId(),pool));
            var route=new RatisRoutedClient(split.sourceDirectory(),new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor range,UUID column,byte[] key){throw new AssertionError("No external USER fixture transport");}public CommandResult command(RangeDescriptor range,DistributedCommand command){throw new AssertionError("No external USER fixture transport");}});
            var epochs=new RatisSessionEpochs(source.catalogue,source.first.clusterId(),source.first.databaseId(),catalogue.groupId(),catalogue.epoch(),catalogue.schemaDigest(),catalogue.sessionsColumn());
            try {
                for(int round=0;round<2;round++) {
                    var receiver=new RatisNodeRuntimeTest.FakeServer();receiver.skipFirstNoop=true;
                    try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(source.first.domainId(),route),env.scheduler,env.timer,()->20L,env.settings(),round==0?RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT:RatisNodeRuntime.Startup.RECOVER,receiver::create,List.of(source.first))) {
                        node.relocationTransport(range->client(source,range),(range,owner)->client(source,range));node.start();
                        source.recoveryOwner=target->{try{var machine=receiver.machines.get(RaftGroupId.valueOf(target.key().groupId()));var response=machine.queryStale(Message.valueOf(ByteString.copyFrom(RatisRecoveryObligationRpc.request(target))),0).get(10,TimeUnit.SECONDS);return RatisRecoveryObligationRpc.reply(response.getContent().toByteArray(),target);}catch(Exception failed){throw new IOException("Actual receiver completion proof failed",failed);}};
                        var owners=field(source.node,"pools",Map.class);var owner=owners.get(source.fast);var prior=field(owner,"sources",RatisTopologySourceSnapshotService.class);var host=field(prior,"host",RatisTopologySourceSnapshotService.Host.class);var bytes=new ByteArrayOutputStream();var preparedSources=new java.util.concurrent.ConcurrentHashMap<UUID,RatisTopologySourceSnapshotRpc.Prepared>();
                        var delivery=(RatisTopologySourceSnapshotService.Host)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RatisTopologySourceSnapshotService.Host.class},(proxy,method,args)->{
                            if(method.getName().equals("send")){
                                var scope=(RatisTopologySourceSnapshotRpc.Request)args[0];var chunk=(InstallSnapshotRequestProto)args[1];assertEquals(scope.transferId().toString(),chunk.getSnapshotChunk().getRequestId());if(chunk.getSnapshotChunk().getRequestIndex()==0)bytes.reset();for(var file:chunk.getSnapshotChunk().getFileChunksList())file.getData().writeTo(bytes);
                                if(chunk.getSnapshotChunk().getDone()){var machine=receiver.machines.get(RaftGroupId.valueOf(scope.sourceGroup()));var cut=chunk.getSnapshotChunk().getTermIndex();install(machine,bytes.toByteArray(),cut.getTerm(),cut.getIndex());when(receiver.division(machine.getGroupId()).getRaftLog().getSnapshotIndex()).thenReturn(cut.getIndex());}
                                var rpc=chunk.getServerRequest();return InstallSnapshotReplyProto.newBuilder().setServerReply(RaftRpcReplyProto.newBuilder().setRequestorId(rpc.getRequestorId()).setReplyId(rpc.getReplyId()).setRaftGroupId(rpc.getRaftGroupId()).setSuccess(true)).setTerm(chunk.getLeaderTerm()).setRequestIndex(chunk.getSnapshotChunk().getRequestIndex()).setResult(InstallSnapshotResult.SUCCESS).build();
                            }
                            if(method.getName().equals("requireReceiverInstalled")){var lease=(RatisTopologySourceSnapshotService.Lease)args[0];var prepared=lease.prepared();var request=new RatisRecoveryImageRpc.Request(prepared.request(),prepared.image().position(),prepared.image().sha256());var evidence=receiver.machines.get(RaftGroupId.valueOf(prepared.request().sourceGroup())).recoveryImageEvidence(request);evidence.require(request);assertTrue(evidence.imageInstalled());return null;}
                            try{var result=method.invoke(host,args);if(result instanceof RatisTopologySourceSnapshotService.Lease lease)preparedSources.put(lease.prepared().request().sourceGroup(),lease.prepared());return result;}catch(InvocationTargetException failed){throw failed.getCause();}
                        });
                        var limits=field(prior,"limits",RatisTopologySourceSnapshotService.Limits.class);var admission=field(prior,"admission",RatisTopologySourceSnapshotService.ResourceAdmission.class);
                        try(var transport=new RatisTopologySourceSnapshotService(delivery,limits,source.scheduler(),admission)) {
                            var sourceField=RatisRangeStateMachine.class.getDeclaredField("sourceSnapshots");sourceField.setAccessible(true);var originals=new HashMap<RatisRangeStateMachine,Object>();for(var range:java.util.stream.Stream.concat(split.parents().stream(),split.targets().stream()).toList()){var machine=source.server.machines.get(RaftGroupId.valueOf(range.groupId()));originals.put(machine,sourceField.get(machine));sourceField.set(machine,transport);}
                            try {
                                var requests=new ArrayList<RatisTopologySourceSnapshotRpc.Request>();var parent=split.parents().getFirst();requests.add(new RatisTopologySourceSnapshotRpc.Request(new UUID(994,1),split.header(),catalogue.groupId(),parent.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA,parent.groupId(),split.sourceDirectory()));int number=1;for(var child:split.targets())requests.add(new RatisTopologySourceSnapshotRpc.Request(new UUID(994,++number),null,catalogue.groupId(),child.replicas().getFirst(),RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,child.groupId(),split.targetDirectory()));
                                var manager=field(node,"configuredRecoveries",RatisConfiguredRecoveries.class);var control=field(node,"configuredRecoveryService",RatisTransactionRecoveryService.class);
                                for(var proposed:requests){
                                    var discovered=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot();
                                    final var request=discovered==null?proposed:discovered.activeRequests().getOrDefault(proposed.sourceGroup(),proposed);
                                    if(round==0){node.recoverConfigured(request).get(15,TimeUnit.SECONDS);assertTrue(Files.isRegularFile(env.nodeRoot.resolve("configured-recovery").resolve(pool.toString()).resolve(request.sourceGroup().toString()).resolve("recovery")));assertEquals(RatisConfiguredRecovery.Phase.PREPARE,node.configuredRecoveryPhase(request.sourceGroup()));if(request.metadataOnly())assertThrows(ExecutionException.class,()->node.recoverConfigured(request.successor(new UUID(994,99))).get(15,TimeUnit.SECONDS),"Unfinished metadata capture keeps its original request identity");}
                                    awaitDone(node,control,manager,request.sourceGroup());
                                    assertEquals(RatisConfiguredRecovery.Phase.DONE,node.configuredRecoveryPhase(request.sourceGroup()));assertNull(node.configuredRecoveryFailure());
                                    if(round==0 && request.metadataOnly()){
                                        var receiverOwner=field(node,"pools",Map.class).get(pool);var graph=field(receiverOwner,"recoveryMetadata",RatisNodeResourceBudget.Lease.class);long retained=graph.bytes();var metaMachine=receiver.machines.get(RaftGroupId.valueOf(request.sourceGroup()));var donor=source.server.machines.get(RaftGroupId.valueOf(request.sourceGroup()));long own=store(receiver,request.sourceGroup()).applied().index()+1;
                                        donor.notifyTermIndexUpdated(1,own);metaMachine.notifyTermIndexUpdated(1,own);assertEquals(own,metaMachine.takeSnapshot());var permit=metaMachine.recoveryEligibilityGate().snapshotPermit();assertSame(store(receiver,request.sourceGroup()),node.registerRecovery(preparedSources.get(request.sourceGroup())));permit.requireCurrent();assertEquals(org.apache.ratis.util.LifeCycle.State.RUNNING,metaMachine.getLifeCycleState());donor.notifyTermIndexUpdated(1,own+1);
                                        var successor=request.successor(new UUID(994,4));node.recoverConfigured(successor).get(15,TimeUnit.SECONDS);assertEquals(RatisConfiguredRecovery.Phase.PREPARE,node.configuredRecoveryPhase(request.sourceGroup()));manager.step().get(15,TimeUnit.SECONDS);assertEquals(RatisConfiguredRecovery.Phase.REGISTER,node.configuredRecoveryPhase(request.sourceGroup()));
                                        var once=new java.util.concurrent.atomic.AtomicBoolean(true);node.nativePool(pool).metadataRecoveryForceObserver(after->{if(after && once.getAndSet(false))throw new org.rocksdb.RocksDBException("Lost forced META successor acknowledgement");});assertThrows(ExecutionException.class,()->manager.step().get(15,TimeUnit.SECONDS));var frozen=store(receiver,request.sourceGroup()).metadataRecoveryRegistration().catalogueCut();assertTrue(store(receiver,request.sourceGroup()).metadataRecoveryPending());assertFalse(store(receiver,request.sourceGroup()).admissionOpen());
                                        var ledger=epochs.read();assertEquals(CommandResult.Status.SUCCESS,epochs.execute(epochs.prepare(new OperationId(new UUID(994,70),0,0),ledger,ledger.advanceTime(ledger.logicalTimeMillis()+1))).status());node.nativePool(pool).metadataRecoveryForceObserver(after->{});
                                        awaitDone(node,control,manager,request.sourceGroup());
                                        assertEquals(RatisConfiguredRecovery.Phase.DONE,node.configuredRecoveryPhase(request.sourceGroup()));var recovered=store(receiver,request.sourceGroup());assertEquals(successor.transferId(),recovered.metadataRecoveryRegistration().transferId());assertEquals(frozen,recovered.metadataRecoveryRegistration().catalogueCut(),"Unknown successor force retains its original CAT cut");assertFalse(recovered.metadataRecoveryPending());assertFalse(recovered.admissionOpen());assertEquals(own+1,recovered.applied().index());assertEquals(retained,graph.bytes(),"A settled successor replaces its graph credit instead of accumulating history");assertEquals(224L,field(receiverOwner,"recoveryExposureBytes",Long.class));
                                    }
                                }
                            }finally{for(var entry:originals.entrySet())sourceField.set(entry.getKey(),entry.getValue());}
                        }
                        assertEquals(3,node.localGroups().size());assertTrue(node.nativePool(pool).topologySeeds().isEmpty());var ancestor=store(receiver,split.parents().getFirst().groupId());assertFalse(ancestor.metadataRecoveryPending());assertTrue(ancestor.userHandlesDetached());assertNull(ancestor.topologyBirth());
                        for(var child:split.targets()){var recovered=store(receiver,child.groupId());assertTrue(recovered.currentRangeRecoveryReady());assertTrue(recovered.admissionOpen());assertArrayEquals(source.store(child.groupId()).topologyBirth().encode(),recovered.topologyBirth().encode());assertArrayEquals(source.store(child.groupId()).topologyActivation().encode(),recovered.topologyActivation().encode());}
                        assertArrayEquals(new byte[]{11},store(receiver,split.targets().getFirst().groupId()).readCurrentPoint(source.first.columnIds().iterator().next(),new byte[]{7}));assertEquals(0,source.store(split.parents().getFirst().groupId()).activeSnapshotPins());
                        assertArrayEquals(new byte[]{22},source.store(source.secondDirectory.ranges().getFirst().groupId()).readCurrentPoint(source.second.columnIds().iterator().next(),new byte[]{7}));
                        var topology=epochs.topologyCatalogue(catalogue.directoryColumn());
                        if(round==0){source.loseRecoveryAck=true;assertThrows(ExecutionException.class,()->node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS));assertFalse(source.loseRecoveryAck,"The actual catalogue ACK committed before its reply was lost");assertFalse(topology.readRecovery(split.intentId()).completions().isEmpty());}
                        else {node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);var acknowledged=topology.readRecovery(split.intentId());assertEquals(3,acknowledged.completions().size());for(var obligation:field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot().plan().localObligations()){var key=new TopologyChange.Key(obligation.range().groupId(),obligation.localReplica().replicaId());assertEquals(RatisRecoveryObligationRpc.local(store(receiver,obligation.range().groupId())).completion(),acknowledged.completions().get(key));}assertEquals(TopologyChange.Phase.RETIRED,topology.readIntent(split.intentId()).phase());}

                    }
                }
            }finally{route.close();}
        }
    }

    @Test void absentConfiguredOwnerRecoversRetiredSplitMergeDagAndNonIdempotentReceiptsAutomatically()throws Exception {
        var absent=new ReplicaDescriptor(new UUID(995,1),new UUID(995,2),new UUID(995,3),"receiver995","127.0.0.1:4597",ReplicaDescriptor.Role.LISTENER);
        try(var source=new RatisTwoPoolWorkflowFixture(root.resolve("dag-source"),false,List.of(absent));var env=new RatisNodeRuntimeTest.Env(root.resolve("dag-receiver"),false,source.first.digest())){
            var initial=source.firstDirectory;var originalRange=initial.ranges().getFirst();var binding=source.plan.catalogue();var pool=RatisPoolFixtures.ID;
            // This node has a genuine local HOME but no local DATA image at initial startup.
            // The remote-only bootstrap geometry is the same incoming-node witness used by
            // the existing registration/reconciliation workflows; fresh CAT supplies membership.
            var remoteRange=new RangeDescriptor(originalRange.rangeId(),originalRange.groupId(),originalRange.rangeEpoch(),originalRange.configurationVersion(),originalRange.lowerInclusive(),originalRange.upperExclusive(),originalRange.topologyState(),List.of(source.local));
            var bootstrap=new RangeDirectory(initial.clusterId(),initial.databaseId(),initial.domainId(),initial.schemaEpoch(),initial.schemaDigest(),0,initial.columns(),initial.routingPrefixBytes(),List.of(remoteRange));
            var domain=new RatisNodePlan.Domain(bootstrap,Map.of(source.first.columnIds().iterator().next(),"receiver-data"),Set.of());
            var homeId=new UUID(995,10);var homeColumn=new UUID(995,11);var homeReplica=new ReplicaDescriptor(absent.nodeId(),absent.nodeIncarnation(),new UUID(995,12),absent.peerId(),absent.address(),ReplicaDescriptor.Role.VOTER);
            var homeRange=new RangeDescriptor(homeId,homeId,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(homeReplica));var homeDirectory=new RangeDirectory(initial.clusterId(),initial.databaseId(),homeId,1,new byte[32],0,Set.of(homeColumn),0,List.of(homeRange));var home=new RatisNodePlan.Home(RatisNodePlan.Role.TRANSACTION_HOME,homeDirectory,homeColumn,"recovery-home");
            var plan=new RatisNodePlan(absent.nodeId(),absent.nodeIncarnation(),List.of(domain),binding,List.of(home),List.of(),Map.of(),8,RatisPoolFixtures.pools(),RatisPoolFixtures.domains(List.of(domain)),RatisPoolFixtures.groups(binding,List.of(home)));
            var epochs=new RatisSessionEpochs(source.catalogue,initial.clusterId(),initial.databaseId(),binding.groupId(),binding.epoch(),binding.schemaDigest(),binding.sessionsColumn());
            var receiver=new java.util.concurrent.atomic.AtomicReference<RatisNodeRuntimeTest.FakeServer>();var requests=new java.util.concurrent.CopyOnWriteArrayList<RatisTopologySourceSnapshotRpc.Request>();
            var owner=field(source.node,"pools",Map.class).get(source.fast);var prior=field(owner,"sources",RatisTopologySourceSnapshotService.class);var host=field(prior,"host",RatisTopologySourceSnapshotService.Host.class);var bytes=new ByteArrayOutputStream();
            var delivery=(RatisTopologySourceSnapshotService.Host)Proxy.newProxyInstance(getClass().getClassLoader(),new Class<?>[]{RatisTopologySourceSnapshotService.Host.class},(proxy,method,args)->{
                if(method.getName().equals("send")){var scope=(RatisTopologySourceSnapshotRpc.Request)args[0];var chunk=(InstallSnapshotRequestProto)args[1];if(chunk.getSnapshotChunk().getRequestIndex()==0)bytes.reset();for(var file:chunk.getSnapshotChunk().getFileChunksList())file.getData().writeTo(bytes);if(chunk.getSnapshotChunk().getDone()){var machine=receiver.get().machines.get(RaftGroupId.valueOf(scope.sourceGroup()));var cut=chunk.getSnapshotChunk().getTermIndex();install(machine,bytes.toByteArray(),cut.getTerm(),cut.getIndex());when(receiver.get().division(machine.getGroupId()).getRaftLog().getSnapshotIndex()).thenReturn(cut.getIndex());}var rpc=chunk.getServerRequest();return InstallSnapshotReplyProto.newBuilder().setServerReply(RaftRpcReplyProto.newBuilder().setRequestorId(rpc.getRequestorId()).setReplyId(rpc.getReplyId()).setRaftGroupId(rpc.getRaftGroupId()).setSuccess(true)).setTerm(chunk.getLeaderTerm()).setRequestIndex(chunk.getSnapshotChunk().getRequestIndex()).setResult(InstallSnapshotResult.SUCCESS).build();}
                if(method.getName().equals("requireReceiverInstalled")){var prepared=((RatisTopologySourceSnapshotService.Lease)args[0]).prepared();var request=new RatisRecoveryImageRpc.Request(prepared.request(),prepared.image().position(),prepared.image().sha256());var evidence=receiver.get().machines.get(RaftGroupId.valueOf(prepared.request().sourceGroup())).recoveryImageEvidence(request);evidence.require(request);assertTrue(evidence.imageInstalled());return null;}
                try{return method.invoke(host,args);}catch(InvocationTargetException failed){throw failed.getCause();}
            });
            var sourceField=RatisRangeStateMachine.class.getDeclaredField("sourceSnapshots");sourceField.setAccessible(true);var originals=new HashMap<RatisRangeStateMachine,Object>();
            var limits=field(prior,"limits",RatisTopologySourceSnapshotService.Limits.class);var admission=field(prior,"admission",RatisTopologySourceSnapshotService.ResourceAdmission.class);
            var originalSourceTransport=field(source.node,"relocationClients",RatisRelocationDataClient.class);
            try(var sourceTransport=new RatisRelocationDataClient(initial.clusterId(),initial.databaseId(),binding.groupId(),r->client(source,r),(r,o)->o.nodeId().equals(absent.nodeId())?receiverClient(receiver.get(),r):client(source,r));var route=new RatisRoutedClient(bootstrap,new RatisRoutedClient.Transport(){public byte[] read(RangeDescriptor r,UUID c,byte[] k){throw new AssertionError("No external USER transport");}public CommandResult command(RangeDescriptor r,DistributedCommand c){throw new AssertionError("No external USER transport");}});var transport=new RatisTopologySourceSnapshotService(delivery,limits,source.scheduler(),admission)){
                field(source.node,"relocationClients",sourceTransport);
                source.recoveryOwner=target->{try{var result=receiver.get().machines.get(RaftGroupId.valueOf(target.key().groupId())).queryStale(Message.valueOf(ByteString.copyFrom(RatisRecoveryObligationRpc.request(target))),0).get(10,TimeUnit.SECONDS);return RatisRecoveryObligationRpc.reply(result.getContent().toByteArray(),target);}catch(Exception failure){throw new IOException(failure);}};
                var policy=new RatisNodeRuntime.SchemaReplicaConfig(RatisSchemaReplicaService.Limits.defaults(),operator->{if(!source.appendOperator.equals(operator))throw new IOException("Unexpected operator tuple");return new it.cavallium.rockserver.core.impl.MergeOperatorRegistry.DeterministicContract("append-contract",1,true);});
                RatisTwoPoolWorkflowFixture.SchemaMerge provisioned;TopologyChange.Definition split,merge;DistributedCommand childWrite;byte[] originalReceipt,childReceipt;
                try{
                    for(var machine:source.server.machines.values()){originals.put(machine,sourceField.get(machine));sourceField.set(machine,transport);}
                    var available=new RatisNodeRuntimeTest.FakeServer();available.skipFirstNoop=true;receiver.set(available);
                    // Provision while this exact configured owner is online. All schema ARTIFACT,
                    // CAPACITY and follower apply paths use its actual native adapter.
                    try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(initial.domainId(),route),env.scheduler,env.timer,()->20L,source.manifest.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,available::create,List.of(source.first),null,null,RatisRoutedClient::connect,policy)){
                        node.relocationTransport(r->recordingClient(source,r,requests),(r,o)->recordingClient(source,r,requests));node.start();field(node,"poolPressure",RatisPoolPressureService.class).refresh().get(10,TimeUnit.SECONDS);
                        node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);var manager=field(node,"configuredRecoveries",RatisConfiguredRecoveries.class);var control=field(node,"configuredRecoveryService",RatisTransactionRecoveryService.class);
                        assertFalse(available.machines.containsKey(RaftGroupId.valueOf(originalRange.groupId())),"An empty incoming DATA owner has no initialized voting division");assertNotNull(node.configuredRecoveryPhase(originalRange.groupId()),"Fresh CAT must dispatch recovery for its genuinely absent native group");
                        awaitDone(node,control,manager,originalRange.groupId());assertTrue(store(available,originalRange.groupId()).currentRangeRecoveryReady());
                        source.schemaPeer=available;try{provisioned=source.createOperatorAndMerge(initial.domainId());}finally{source.schemaPeer=null;}
                        assertArrayEquals(new byte[]{11,33},store(available,originalRange.groupId()).readCurrentPoint(provisioned.column(),new byte[]{9}));
                        originalReceipt=source.store(originalRange.groupId()).operationResult(provisioned.last().operationId(),provisioned.last().receiptDigest());assertNotNull(originalReceipt);
                    }
                    requests.clear();split=source.split();var topology=epochs.topologyCatalogue(binding.directoryColumn());
                    assertEquals(TopologyChange.Phase.RETIRED,topology.readIntent(split.intentId()).phase());assertTrue(topology.readIntent(split.intentId()).activated().keySet().stream().noneMatch(k->k.replicaId().equals(absent.replicaId())));
                    assertTrue(topology.readRecovery(split.intentId()).owners().contains(new TopologyChange.Key(split.targets().getFirst().groupId(),absent.replicaId())));
                    var child=split.targets().getFirst();var childRoute=source.routes.get(initial.domainId());field(source.node,"poolPressure",RatisPoolPressureService.class).refresh().get(10,TimeUnit.SECONDS);
                    // A fresh born group owns a new replicated session authority. Bind through
                    // the normal issued-grant protocol before its first non-idempotent write.
                    try(var session=RatisBackendSessions.window(source.root.resolve("child-window"),childRoute,epochs,60000,new RatisSessionClient.Limits(4,1L<<20,1L<<20),List.of(new RatisBackendSessions.Target(childRoute,child.groupId(),split.targetDirectory())),RatisJournalAccess.standalone())){
                        var pending=session.prepare(List.of(new Mutation(Mutation.Kind.MERGE,provisioned.column(),new byte[]{9},new byte[]{55})));childWrite=pending.command();childReceipt=session.execute(pending).encode();assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(childReceipt).status());
                    }
                    assertArrayEquals(new byte[]{11,33,55},source.store(child.groupId()).readCurrentPoint(provisioned.column(),new byte[]{9}));
                    assertArrayEquals(childReceipt,source.apply(child.groupId(),DistributedCommandCodec.encode(childWrite)).getContent().toByteArray());assertArrayEquals(new byte[]{11,33,55},source.store(child.groupId()).readCurrentPoint(provisioned.column(),new byte[]{9}));
                    merge=source.merge(split);assertEquals(TopologyChange.Phase.RETIRED,topology.readIntent(merge.intentId()).phase());
                    for(var machine:source.server.machines.values())if(!originals.containsKey(machine)){originals.put(machine,sourceField.get(machine));sourceField.set(machine,transport);}
                    var leaf=merge.targets().getFirst();Map<TopologyChange.Key,RatisTopologyRecoveryObligations.Completion> lostReceipts=null;
                    for(int round=0;round<2;round++){
                        var restarted=new RatisNodeRuntimeTest.FakeServer();restarted.skipFirstNoop=true;receiver.set(restarted);
                        try(var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),epochs,Map.of(initial.domainId(),route),env.scheduler,env.timer,()->20L,source.manifest.settings(),RatisNodeRuntime.Startup.RECOVER,restarted::create,List.of(source.first),null,null,RatisRoutedClient::connect,policy)){
                            node.relocationTransport(r->recordingClient(source,r,requests),(r,o)->recordingClient(source,r,requests));node.start();field(node,"poolPressure",RatisPoolPressureService.class).refresh().get(10,TimeUnit.SECONDS);
                            node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);var graph=field(node,"recoveryGraph",RatisConfiguredRecoveryGraph.class).snapshot();assertEquals(4,graph.plan().localObligations().size());assertEquals(2,graph.plan().definitions().size());assertEquals(3,graph.plan().localObligations().stream().filter(o->o.purpose()==RatisTopologySourceSnapshotRpc.Purpose.ANCESTOR_METADATA).count());
                            var manager=field(node,"configuredRecoveries",RatisConfiguredRecoveries.class);var control=field(node,"configuredRecoveryService",RatisTransactionRecoveryService.class);
                            for(var obligation:graph.plan().localObligations())awaitAutomatic(node,control,manager,obligation.range().groupId());
                            assertTrue(store(restarted,leaf.groupId()).currentRangeRecoveryReady());assertArrayEquals(new byte[]{11,33,55},store(restarted,leaf.groupId()).readCurrentPoint(provisioned.column(),new byte[]{9}));
                            for(var definition:List.of(split,merge))for(var ancestor:definition.parents()){
                                var recovered=store(restarted,ancestor.groupId());assertTrue(recovered.userHandlesDetached());assertFalse(recovered.admissionOpen());assertFalse(field(restarted.machines.get(RaftGroupId.valueOf(ancestor.groupId())),"currentDataVoter",Boolean.class));
                                var lineage=new DistributedCdcCheckpoint.Lineage(ancestor.rangeId(),ancestor.groupId(),ancestor.rangeEpoch());var expected=source.store(ancestor.groupId()).pollRetiredCdc(lineage,DistributedCdcCheckpoint.Cursor.beginning(),256,1<<20);var actual=recovered.pollRetiredCdc(lineage,DistributedCdcCheckpoint.Cursor.beginning(),256,1<<20);assertEquals(expected.cut(),actual.cut());assertEquals(expected.records().size(),actual.records().size());for(int i=0;i<actual.records().size();i++)assertArrayEquals(expected.records().get(i).encode(),actual.records().get(i).encode());
                            }
                            assertArrayEquals(originalReceipt,store(restarted,originalRange.groupId()).operationResult(provisioned.last().operationId(),provisioned.last().receiptDigest()));assertArrayEquals(childReceipt,store(restarted,childWrite.groupId()).operationResult(childWrite.operationId(),childWrite.receiptDigest()));
                            assertTrue(requests.stream().allMatch(r->r.metadataOnly() || r.sourceGroup().equals(leaf.groupId())),"Historical parents never request USER replay after absence");
                            if(round==0){source.loseRecoveryAck=true;assertThrows(ExecutionException.class,()->node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS));assertFalse(source.loseRecoveryAck);lostReceipts=topology.readRecovery(split.intentId()).completions();assertFalse(lostReceipts.isEmpty());}
                            else{node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);for(var entry:lostReceipts.entrySet())assertEquals(entry.getValue(),topology.readRecovery(split.intentId()).completions().get(entry.getKey()));assertEquals(merge.intentId(),topology.readHead(initial.domainId()).header().intentId());}
                        }
                    }
                }finally{source.schemaPeer=null;field(source.node,"relocationClients",originalSourceTransport);for(var entry:originals.entrySet())sourceField.set(entry.getKey(),entry.getValue());}
            }
        }
    }
    private static RaftClient receiverClient(RatisNodeRuntimeTest.FakeServer receiver,RangeDescriptor range){
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            var machine=receiver.machines.get(RaftGroupId.valueOf(range.groupId()));var request=(Message)args[0];var result=switch(method.getName()){case "sendReadOnly"->machine.queryLinearizable(request);case "sendStaleRead"->machine.queryStale(request,(long)args[1]);default->throw new AssertionError(method);};return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(receiver.plan.peerId())).setGroupId(RaftGroupId.valueOf(range.groupId())).setCallId(1).setSuccess(true).setMessage(result.get(15,TimeUnit.SECONDS)).build();
        });return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "getGroupId"->RaftGroupId.valueOf(range.groupId());case "close"->null;default->throw new AssertionError(method);});
    }
    private static RaftClient recordingClient(RatisTwoPoolWorkflowFixture source,RangeDescriptor range,List<RatisTopologySourceSnapshotRpc.Request> requests){
        var delegate=client(source,range);var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            if(args!=null && args.length>0 && args[0] instanceof Message message){byte[] encoded=message.getContent().toByteArray();if(ScopedTransactionRpc.kind(encoded,ScopedTransactionRpc.MAX_REQUEST_BYTES)==RatisTopologySourceSnapshotRpc.PREPARE)requests.add(RatisTopologySourceSnapshotRpc.request(encoded));}
            try{return method.invoke(delegate.io(),args);}catch(InvocationTargetException failure){throw failure.getCause();}
        });return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "getGroupId"->delegate.getGroupId();case "close"->null;default->throw new AssertionError(method);});
    }
    private static void awaitAutomatic(RatisNodeRuntime node,RatisTransactionRecoveryService control,RatisConfiguredRecoveries manager,UUID group)throws Exception {
        Throwable last=null;for(int steps=0;steps<128;steps++){
            var phase=node.configuredRecoveryPhase(group);if(phase==RatisConfiguredRecovery.Phase.DONE || phase==RatisConfiguredRecovery.Phase.LOCAL_DONE)return;
            try{manager.step().get(15,TimeUnit.SECONDS);}catch(ExecutionException waiting){last=waiting.getCause();}
        }throw new AssertionError("Automatic DAG recovery did not settle for "+group,last);
    }

    @Test void initialZeroDelayControlTickCannotDisplaceTheFirstAcceptedRequest()throws Exception {
        try(var env=new RatisNodeRuntimeTest.Env(root.resolve("initial-tick"),false)) {
            var armed=new java.util.concurrent.atomic.AtomicBoolean();var reference=new java.util.concurrent.atomic.AtomicReference<RatisNodeRuntime>();
            var immediate=new reactor.core.scheduler.Scheduler(){
                public reactor.core.Disposable schedulePeriodically(Runnable task,long initial,long period,TimeUnit unit){var scheduled=env.timer.schedulePeriodically(task,initial,period,unit);if(armed.get()){task.run();try{field(reference.get(),"configuredRecoveryService",RatisTransactionRecoveryService.class).tick().get(10,TimeUnit.SECONDS);}catch(Exception failed){throw new java.util.concurrent.CompletionException(failed);}}return scheduled;}
                public reactor.core.Disposable schedule(Runnable task){return env.timer.schedule(task);}public Worker createWorker(){return env.timer.createWorker();}public void dispose(){env.timer.dispose();}public boolean isDisposed(){return env.timer.isDisposed();}
            };
            var receiver=new RatisNodeRuntimeTest.FakeServer();
            try(var node=RatisNodeRuntime.open(env.plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(env.routed.directory().domainId(),env.routed),env.scheduler,immediate,()->20L,env.settings(),RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,receiver::create,List.of())) {
                reference.set(node);node.relocationTransport(range->{throw new AssertionError("No source action is dispatched by initial admission");},(range,owner)->{throw new AssertionError("No owner RPC in initial admission");});node.start();
                var manager=field(node,"configuredRecoveries",RatisConfiguredRecoveries.class);var io=field(manager,"io",ThreadPoolExecutor.class);var entered=new CountDownLatch(1);var release=new CountDownLatch(1);io.execute(()->{entered.countDown();try{release.await();}catch(InterruptedException interrupted){Thread.currentThread().interrupt();}});assertTrue(entered.await(10,TimeUnit.SECONDS));armed.set(true);
                var directory=env.routed.directory();var range=directory.ranges().getFirst();var request=new RatisTopologySourceSnapshotRpc.Request(new UUID(994,8),null,env.plan.catalogue().groupId(),range.replicas().stream().filter(replica->replica.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow(),RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,range.groupId(),directory);CompletableFuture<Void> accepted;
                try{accepted=node.recoverConfigured(request);if(accepted.isDone())accepted.get(10,TimeUnit.SECONDS);assertFalse(accepted.isDone(),"The forced enqueue remains behind actual IO; a zero-delay tick must coalesce with it");}finally{release.countDown();}
                accepted.get(10,TimeUnit.SECONDS);assertEquals(RatisConfiguredRecovery.Phase.PREPARE,node.configuredRecoveryPhase(range.groupId()));assertNull(node.configuredRecoveryFailure());
            }
        }
    }
    @Test void nodeCloseKeepsNativeAndTransportOwnersUntilActualSourceCallReturns()throws Exception {
        try(var env=new RatisNodeRuntimeTest.Env(root.resolve("close-source"),false)) {
            var base=env.settings();var settings=new RatisNodeRuntime.Settings(base.images(),base.retention(),base.clockPeriod(),base.propagationVisits(),base.maxPropagationRanges(),java.time.Duration.ofSeconds(5),base.exports(),base.resources());var receiver=new RatisNodeRuntimeTest.FakeServer();var entered=new CountDownLatch(1);var release=new CountDownLatch(1);
            try(var node=RatisNodeRuntime.open(env.plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(env.routed.directory().domainId(),env.routed),env.scheduler,env.timer,()->20L,settings,RatisNodeRuntime.Startup.AUTHORIZED_INITIAL_PLACEMENT,receiver::create,List.of())) {
                node.relocationTransport(range->{entered.countDown();try{if(!release.await(10,TimeUnit.SECONDS))throw new IOException("Fixture source release timed out");}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}throw new IOException("Source connection outcome is unresolved");},(range,owner)->{throw new AssertionError("No owner RPC before PREPARE");});node.start();
                var directory=env.routed.directory();var range=directory.ranges().getFirst();var request=new RatisTopologySourceSnapshotRpc.Request(new UUID(994,9),null,env.plan.catalogue().groupId(),range.replicas().stream().filter(replica->replica.nodeId().equals(env.plan.nodeId())).findFirst().orElseThrow(),RatisTopologySourceSnapshotRpc.Purpose.CURRENT_LEAF,range.groupId(),directory);node.recoverConfigured(request).get(10,TimeUnit.SECONDS);var manager=field(node,"configuredRecoveries",RatisConfiguredRecoveries.class);var call=manager.step();assertTrue(entered.await(10,TimeUnit.SECONDS));
                var closing=CompletableFuture.runAsync(()->{try{node.close();}catch(IOException failed){throw new java.util.concurrent.CompletionException(failed);}});
                try{for(int tries=0;tries<100 && node.state()!=RatisNodeRuntime.State.STOPPING;tries++)Thread.sleep(10);assertEquals(RatisNodeRuntime.State.STOPPING,node.state());assertFalse(closing.isDone());assertFalse(call.isDone());assertEquals(0,receiver.closes);assertFalse(node.nativePool(node.poolId(range.groupId())).persistedRangeGroups(16).isEmpty());}finally{release.countDown();}
                closing.get(10,TimeUnit.SECONDS);assertEquals(RatisNodeRuntime.State.CLOSED,node.state());assertEquals(1,receiver.closes);assertEquals(0,manager.size());assertThrows(ExecutionException.class,()->call.get(10,TimeUnit.SECONDS));
            }finally{release.countDown();}
        }
    }
    private static ReplicatedRangeStore store(RatisNodeRuntimeTest.FakeServer server,UUID group)throws Exception{return field(server.machines.get(RaftGroupId.valueOf(group)),"store",ReplicatedRangeStore.class);}
    private static void install(RatisRangeStateMachine machine,byte[] image,long term,long index)throws Exception {
        machine.prepareRecoveryReplacement();machine.pause();Path file=machine.getStateMachineStorage().getSnapshotFile(term,index).toPath();Files.createDirectories(file.getParent());Files.write(file,image);MD5FileUtil.saveMD5File(file.toFile(),MD5FileUtil.computeMd5ForFile(file.toFile()));SessionJournalFiles.force(file);SessionJournalFiles.force(MD5FileUtil.getDigestFileForFile(file.toFile()).toPath());SessionJournalFiles.force(file.getParent());machine.syncReceivedSnapshot(term,index);machine.reinitialize();
    }
    private static RaftClient client(RatisTwoPoolWorkflowFixture source,RangeDescriptor range){
        if(range.groupId().equals(source.plan.catalogue().groupId()))return source.catalogue;
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{var machine=source.server.machines.get(RaftGroupId.valueOf(range.groupId()));var request=(Message)args[0];var result=switch(method.getName()){case "sendReadOnly"->machine.queryLinearizable(request);case "sendStaleRead"->machine.queryStale(request,(long)args[1]);default->throw new AssertionError(method);};return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(source.plan.peerId())).setGroupId(range==null?null:RaftGroupId.valueOf(range.groupId())).setCallId(1).setSuccess(true).setMessage(result.get(15,TimeUnit.SECONDS)).build();});
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "io"->io;case "getGroupId"->RaftGroupId.valueOf(range.groupId());case "close"->null;default->throw new AssertionError(method);});
    }
}
