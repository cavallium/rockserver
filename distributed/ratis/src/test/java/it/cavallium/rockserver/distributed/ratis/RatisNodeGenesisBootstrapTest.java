package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.lang.reflect.*;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.impl.RecoveryEligibilityStateMachine;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ProtoUtils;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;
import static it.cavallium.rockserver.distributed.ratis.RatisTopologySeedRuntimeTest.field;

/** JNI DATA/CAT and a real local updater; no sockets, elections or quorum acceptance. */
class RatisNodeGenesisBootstrapTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static Message message(byte[] bytes){return Message.valueOf(ByteString.copyFrom(bytes));}
    static byte[] uuid(UUID value){return ByteBuffer.allocate(16).putLong(value.getMostSignificantBits()).putLong(value.getLeastSignificantBits()).array();}
    static RatisNodeRuntime.Settings settings(RatisNodeRuntimeTest.Env env){var s=env.settings();return new RatisNodeRuntime.Settings(s.images(),s.retention(),s.clockPeriod(),s.propagationVisits(),s.maxPropagationRanges(),Duration.ofSeconds(10),s.exports(),s.resources());}
    @org.junit.jupiter.params.ParameterizedTest @org.junit.jupiter.params.provider.ValueSource(booleans={false,true})
    void actualOriginalMetadataProgressAndStagedCatIssuanceTransitionLiveWithoutSnapshot(boolean newerTerm)throws Exception {
        try(var env=new RatisNodeRuntimeTest.Env(root)) {
            var plan=originalPlan(env);var domain=plan.domains().getFirst();var directory=domain.directory();var range=directory.ranges().getFirst();
            try(var route=route(directory)) {
                var machines=new HashMap<RaftGroupId,RatisRangeStateMachine>();var calls=new AtomicInteger();
                var catalogue=catClient(env,plan,calls);
                var node=open(env,plan,route,machines,catalogue,RatisNodeRuntime.Startup.AUTHORIZED_GENESIS);
                var resources=field(node,"resources",RatisNodeResourceBudget.class);
                var committed=field(node,"committedDispatcher",RatisCommittedApplyDispatcher.class);
                try(node) {
                    node.start();var machine=machines.get(RaftGroupId.valueOf(range.groupId()));assertNotNull(machine);
                    var store=field(machine,"store",ReplicatedRangeStore.class);var division=machine.getServer().join().getDivision(machine.getGroupId());
                    field(machine,"leadershipSource",(RatisRangeStateMachine.LeadershipSource)()->new RatisRangeStateMachine.Leadership(1,true));
                    assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertNull(store.applied());assertFalse(store.admissionOpen());
                    assertNull(machine.getLatestSnapshot());assertEquals(-1,division.getRaftLog().getSnapshotIndex());
                    var state=message(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]));
                    var point=message(ScopedTransactionRpc.pointRequest(env.outer.data,new byte[]{1}));
                    assertThrows(ExecutionException.class,()->machine.queryLinearizable(point).get(5,TimeUnit.SECONDS));
                    node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);
                    assertEquals(RecoveryEligibilityStateMachine.Mode.GENESIS,machine.recoveryEligibilityGate().snapshot().mode());
                    assertTrue(machine.genesisActive());assertFalse(store.admissionOpen());assertNull(store.applied());
                    assertThrows(IOException.class,()->machine.startTransaction(null));
                    assertThrows(ExecutionException.class,()->machine.applyTransaction(null).get(5,TimeUnit.SECONDS));
                    assertThrows(IOException.class,machine::reinitialize);assertEquals(-1,machine.takeSnapshot());
                    assertNotNull(machine.queryLinearizable(state).get(5,TimeUnit.SECONDS));
                    assertThrows(ExecutionException.class,()->machine.query(state).get(5,TimeUnit.SECONDS));
                    assertThrows(ExecutionException.class,()->machine.queryLinearizable(point).get(5,TimeUnit.SECONDS));
                    assertThrows(IllegalStateException.class,machine.recoveryEligibilityGate()::snapshotPermit);
                    var cfg=org.apache.ratis.server.raftlog.LogProtoUtils.toRaftConfigurationProtoBuilder(division.getRaftConf()).build();
                    var remote=range.replicas().stream().filter(r->!r.nodeId().equals(plan.nodeId())).findFirst().orElseThrow();
                    appendOriginalMetadata(plan,range,machine,store);
                    assertEquals(0,store.configuration().index());assertArrayEquals(cfg.toByteArray(),store.configuration().data());
                    assertFalse(store.admissionOpen());assertNotNull(machine.queryLinearizable(state).get(5,TimeUnit.SECONDS));
                    assertThrows(ExecutionException.class,()->machine.queryLinearizable(point).get(5,TimeUnit.SECONDS));
                    assertNull(machine.getLatestSnapshot());assertEquals(-1,division.getRaftLog().getSnapshotIndex());
                    if(newerTerm){
                        var heartbeat=AppendEntriesRequestProto.newBuilder().setServerRequest(rpc(plan,range,remote,3)).setLeaderTerm(2).setLeaderCommit(1).setPreviousLog(TermIndex.valueOf(1,1).toProto()).build();
                        assertEquals(AppendEntriesReplyProto.AppendResult.SUCCESS,machine.getServer().join().appendEntries(heartbeat).getResult());
                        assertEquals(2,division.getInfo().getCurrentTerm());assertEquals(new ApplyPosition(1,1),store.applied());
                        field(machine,"leadershipSource",(RatisRangeStateMachine.LeadershipSource)()->new RatisRangeStateMachine.Leadership(2,true));
                    }
                    // A legitimate multi-domain bootstrap first advertises both identities while publishing its other domain.
                    var otherId=new UUID(12070,1);var otherColumn=new UUID(12070,2);var otherGroup=new UUID(12070,3);
                    var otherRange=new RangeDescriptor(new UUID(12070,4),otherGroup,1,0,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(remote));
                    var otherDirectory=new RangeDirectory(plan.clusterId(),plan.databaseId(),otherId,1,new byte[32],0,Set.of(otherColumn),0,List.of(otherRange));
                    var publisherDomains=List.of(domain,new RatisNodePlan.Domain(otherDirectory,Map.of(otherColumn,"other"),Set.of()));
                    var pools=new HashMap<>(plan.domainPools());pools.put(otherId,RatisPoolFixtures.ID);
                    var publisherPlan=new RatisNodePlan(plan.nodeId(),plan.nodeIncarnation(),publisherDomains,plan.catalogue(),List.of(),List.of(),Map.of(),2,plan.pools().values(),pools,plan.groupPools());
                    var roster=DatabaseDomainRoster.initial(plan.clusterId(),plan.databaseId(),List.of(directory.domainId(),otherId));
                    var ledger=env.catalogue.epochs.read();
                    try(var publishOther=new RatisCatalogueGenesis(root.resolve("other-genesis"),env.catalogue.client,publisherPlan,otherId,10,ledger,null,roster)) {
                        assertEquals(CommandResult.Status.SUCCESS,publishOther.execute().status());
                    }
                    try{node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);}catch(ExecutionException incomplete){assertInstanceOf(IOException.class,incomplete.getCause());}
                    assertEquals(RecoveryEligibilityStateMachine.Mode.GENESIS_SUSPENDED,machine.recoveryEligibilityGate().snapshot().mode());
                    assertFalse(store.admissionOpen());assertTrue(machine.genesisActive());
                    assertThrows(IllegalStateException.class,machine.recoveryEligibilityGate()::snapshotPermit);
                    var vote=RequestVoteRequestProto.newBuilder().setServerRequest(rpc(plan,range,remote,2)).setCandidateTerm(2).setCandidateLastEntry(TermIndex.valueOf(1,1).toProto()).setPreVote(true).build();
                    assertFalse(machine.getServer().join().requestVote(vote).getServerReply().getSuccess());
                    try(var publishOriginal=new RatisCatalogueGenesis(root.resolve("original-genesis"),env.catalogue.client,plan,directory.domainId(),10,ledger,roster,roster)) {
                        assertEquals(CommandResult.Status.SUCCESS,publishOriginal.execute().status());
                    }
                    var before=store.applied();node.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);
                    assertEquals(RecoveryEligibilityStateMachine.Mode.NORMAL,machine.recoveryEligibilityGate().snapshot().mode());
                    assertFalse(machine.genesisActive());assertFalse(machine.recoveryEligibilityGate().discoveryPending());assertTrue(store.admissionOpen());
                    assertEquals(before,store.applied());assertSame(division,machine.getServer().join().getDivision(machine.getGroupId()));
                    assertNull(machine.getLatestSnapshot());assertEquals(-1,division.getRaftLog().getSnapshotIndex());
                    if(newerTerm){
                        var status=node.readiness().groups().stream().filter(value->value.group().equals(range.groupId())).findFirst().orElseThrow();
                        assertTrue(status.issues().stream().anyMatch(issue->issue.reason()==RatisNodeReadiness.Reason.CURRENT_TERM_NOT_APPLIED));assertFalse(status.servingReady());
                        var termStart=List.of(LogEntryProto.newBuilder().setTerm(2).setIndex(2).setConfigurationEntry(cfg).build(),LogEntryProto.newBuilder().setTerm(2).setIndex(3).setMetadataEntry(MetadataProto.newBuilder()).build());
                        var append=AppendEntriesRequestProto.newBuilder().setServerRequest(rpc(plan,range,remote,4)).setLeaderTerm(2).setLeaderCommit(3).setPreviousLog(TermIndex.valueOf(1,1).toProto()).addAllEntries(termStart).build();
                        assertEquals(AppendEntriesReplyProto.AppendResult.SUCCESS,machine.getServer().join().appendEntries(append).getResult());
                        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);while((store.applied().index()<3 || actualUpdaterIndex(division)<3) && System.nanoTime()<deadline)Thread.sleep(2);
                        assertEquals(new ApplyPosition(2,3),store.applied());
                    }
                    assertNotNull(machine.queryLinearizable(point).get(5,TimeUnit.SECONDS));assertEquals(0,store.activeSnapshotPins());
                    assertTrue(calls.get()>0);committed.awaitDrained(Duration.ofSeconds(5));
                }
                committed.awaitDrained(Duration.ofSeconds(5));assertEquals(0,committed.stats().reads());assertEquals(0,committed.stats().ordinary());assertEquals(0,committed.stats().protectedOwners());
                assertEquals(0,resources.stats().ordinary().bytes());assertEquals(0,resources.stats().protectedLane().bytes());
            }
        }
    }
    @Test void coldUnissuedGenesisQualifiesOriginalMetadataWithoutRegressingNativeAOrManufacturingSnapshot()throws Exception {
        try(var env=new RatisNodeRuntimeTest.Env(root)) {
            var plan=originalPlan(env);var directory=plan.domains().getFirst().directory();var range=directory.ranges().getFirst();
            var calls=new AtomicInteger();var catalogue=catClient(env,plan,calls);
            try(var route=route(directory)) {
                var firstMachines=new HashMap<RaftGroupId,RatisRangeStateMachine>();
                var first=open(env,plan,route,firstMachines,catalogue,RatisNodeRuntime.Startup.AUTHORIZED_GENESIS);
                byte[] identity;
                try(first) {
                    first.start();var machine=firstMachines.get(RaftGroupId.valueOf(range.groupId()));var store=field(machine,"store",ReplicatedRangeStore.class);
                    first.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);
                    assertEquals(RecoveryEligibilityStateMachine.Mode.GENESIS,machine.recoveryEligibilityGate().snapshot().mode());
                    appendOriginalMetadata(plan,range,machine,store);
                    assertEquals(new ApplyPosition(1,1),store.applied());assertNull(machine.getLatestSnapshot());
                    assertEquals(-1,machine.getServer().join().getDivision(machine.getGroupId()).getRaftLog().getSnapshotIndex());
                    assertFalse(store.admissionOpen());identity=Files.readAllBytes(env.nodeRoot.resolve("node.identity"));
                }
                var recoveredMachines=new HashMap<RaftGroupId,RatisRangeStateMachine>();
                try(var recovered=open(env,plan,route,recoveredMachines,catalogue,RatisNodeRuntime.Startup.RECOVER)) {
                    recovered.start();var machine=recoveredMachines.get(RaftGroupId.valueOf(range.groupId()));var store=field(machine,"store",ReplicatedRangeStore.class);
                    var division=machine.getServer().join().getDivision(machine.getGroupId());
                    field(machine,"leadershipSource",(RatisRangeStateMachine.LeadershipSource)()->new RatisRangeStateMachine.Leadership(1,true));
                    assertArrayEquals(identity,Files.readAllBytes(env.nodeRoot.resolve("node.identity")));
                    assertTrue(machine.recoveryEligibilityGate().discoveryPending());assertFalse(machine.genesisActive());
                    assertEquals(new ApplyPosition(1,1),store.applied());assertEquals(TermIndex.valueOf(1,1),machine.getLastAppliedTermIndex());
                    assertNull(machine.getLatestSnapshot());assertEquals(-1,division.getRaftLog().getSnapshotIndex());assertEquals(-1,actualUpdaterIndex(division));
                    assertFalse(store.admissionOpen());var point=message(ScopedTransactionRpc.pointRequest(env.outer.data,new byte[]{1}));
                    assertThrows(ExecutionException.class,()->machine.queryLinearizable(point).get(5,TimeUnit.SECONDS));
                    assertThrows(IllegalStateException.class,machine.recoveryEligibilityGate()::snapshotPermit);
                    int before=calls.get();recovered.bootstrapDeferredRecovery().get(15,TimeUnit.SECONDS);assertTrue(calls.get()>before);
                    assertEquals(RecoveryEligibilityStateMachine.Mode.GENESIS,machine.recoveryEligibilityGate().snapshot().mode());
                    // Native completion may be ahead of the follower's persisted Raft commit metadata.
                    assertEquals(0,division.getRaftLog().getLastCommittedIndex());
                    long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
                    while(actualUpdaterIndex(division)<0 && System.nanoTime()<deadline)Thread.sleep(2);
                    assertEquals(0,actualUpdaterIndex(division));assertEquals(new ApplyPosition(1,1),store.applied());
                    var remote=range.replicas().stream().filter(value->!value.nodeId().equals(plan.nodeId())).findFirst().orElseThrow();
                    var heartbeat=AppendEntriesRequestProto.newBuilder().setServerRequest(rpc(plan,range,remote,5)).setLeaderTerm(1).setLeaderCommit(1).setPreviousLog(TermIndex.valueOf(1,1).toProto()).build();
                    assertEquals(AppendEntriesReplyProto.AppendResult.SUCCESS,machine.getServer().join().appendEntries(heartbeat).getResult());
                    while(actualUpdaterIndex(division)<1 && System.nanoTime()<deadline)Thread.sleep(2);
                    assertEquals(1,actualUpdaterIndex(division));assertEquals(new ApplyPosition(1,1),store.applied());
                    assertEquals(TermIndex.valueOf(1,1),machine.getLastAppliedTermIndex());assertEquals(0,store.configuration().index());
                    assertFalse(store.admissionOpen());assertTrue(machine.genesisActive());assertFalse(machine.recoveryEligibilityGate().discoveryPending());
                    assertNotNull(machine.queryLinearizable(message(ScopedTransactionRpc.frame(ScopedTransactionRpc.STATE,new byte[0]))).get(5,TimeUnit.SECONDS));
                    assertThrows(ExecutionException.class,()->machine.queryLinearizable(point).get(5,TimeUnit.SECONDS));
                    assertThrows(IOException.class,()->machine.startTransaction(null));assertThrows(IOException.class,machine::reinitialize);
                    assertEquals(-1,machine.takeSnapshot());assertNull(machine.getLatestSnapshot());assertEquals(-1,division.getRaftLog().getSnapshotIndex());
                    assertEquals(0,store.activeSnapshotPins());assertArrayEquals(identity,Files.readAllBytes(env.nodeRoot.resolve("node.identity")));
                    field(recovered,"committedDispatcher",RatisCommittedApplyDispatcher.class).awaitDrained(Duration.ofSeconds(5));
                }
            }
        }
    }
    static long actualUpdaterIndex(RaftServer.Division division)throws Exception {
        var state=field(division,"state",Object.class);var supplier=field(state,"stateMachineUpdater",java.util.function.Supplier.class);
        var updater=supplier.get();var read=updater.getClass().getDeclaredMethod("getLastAppliedIndex");read.setAccessible(true);return (long)read.invoke(updater);
    }
    static void appendOriginalMetadata(RatisNodePlan plan,RangeDescriptor range,RatisRangeStateMachine machine,ReplicatedRangeStore store)throws Exception {
        var division=machine.getServer().join().getDivision(machine.getGroupId());
        var cfg=org.apache.ratis.server.raftlog.LogProtoUtils.toRaftConfigurationProtoBuilder(division.getRaftConf()).build();
        var entries=List.of(LogEntryProto.newBuilder().setTerm(1).setIndex(0).setConfigurationEntry(cfg).build(),
                LogEntryProto.newBuilder().setTerm(1).setIndex(1).setMetadataEntry(MetadataProto.newBuilder()).build());
        var remote=range.replicas().stream().filter(r->!r.nodeId().equals(plan.nodeId())).findFirst().orElseThrow();
        var request=AppendEntriesRequestProto.newBuilder().setServerRequest(rpc(plan,range,remote,1)).setLeaderTerm(1).setLeaderCommit(1).setInitializing(false).addAllEntries(entries).build();
        assertEquals(AppendEntriesReplyProto.AppendResult.SUCCESS,machine.getServer().join().appendEntries(request).getResult());
        long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(10);
        while((store.applied()==null || store.applied().index()<1 || actualUpdaterIndex(division)<1) && System.nanoTime()<deadline)Thread.sleep(2);
        assertEquals(new ApplyPosition(1,1),store.applied());assertEquals(TermIndex.valueOf(1,1),machine.getLastAppliedTermIndex());assertEquals(1,actualUpdaterIndex(division));
    }
    static RatisNodePlan originalPlan(RatisNodeRuntimeTest.Env env) {
        var original=env.plan.domains().getFirst();var old=original.directory();var first=old.ranges().getFirst();
        var range=new RangeDescriptor(first.rangeId(),first.groupId(),first.rangeEpoch(),0,null,null,RangeDescriptor.TopologyState.ACTIVE,first.replicas());
        var directory=new RangeDirectory(old.clusterId(),old.databaseId(),old.domainId(),old.schemaEpoch(),old.schemaDigest(),0,old.columns(),old.routingPrefixBytes(),List.of(range));
        var domain=new RatisNodePlan.Domain(directory,original.columns(),original.merges());
        return new RatisNodePlan(env.plan.nodeId(),env.plan.nodeIncarnation(),List.of(domain),env.plan.catalogue(),List.of(),List.of(),Map.of(),2,
                env.plan.pools().values(),env.plan.domainPools(),env.plan.groupPools());
    }
    static RatisRoutedClient route(RangeDirectory directory){return new RatisRoutedClient(directory,new RatisRoutedClient.Transport(){
        public byte[] read(RangeDescriptor r,UUID c,byte[] k){throw new AssertionError("No routed USER read in bootstrap");}
        public CommandResult command(RangeDescriptor r,DistributedCommand c){throw new AssertionError("No routed USER mutation in bootstrap");}
    });}
    static RatisNodeRuntime open(RatisNodeRuntimeTest.Env env,RatisNodePlan plan,RatisRoutedClient route,Map<RaftGroupId,RatisRangeStateMachine> machines,
            RaftClient catalogue,RatisNodeRuntime.Startup startup)throws IOException {
        var node=RatisNodeRuntime.open(plan,env.nodeRoot,ConfigParser.parseDefault(),env.catalogue.epochs,Map.of(route.directory().domainId(),route),env.scheduler,env.timer,()->20L,settings(env),startup,
                (configuration,logs,created)->{machines.putAll(created);return RatisLocalMetadataDiscoveryTest.noSocketServer(configuration,logs,created);});
        node.relocationTransport(r->{assertEquals(plan.catalogue().groupId(),r.groupId());return catalogue;},(r,owner)->{throw new AssertionError("No DATA recovery capture during original creation");});return node;
    }
    static RaftRpcRequestProto rpc(RatisNodePlan plan,RangeDescriptor range,ReplicaDescriptor remote,long call){return RaftRpcRequestProto.newBuilder().setRequestorId(ByteString.copyFromUtf8(remote.peerId())).setReplyId(ByteString.copyFromUtf8(plan.peerId())).setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(RaftGroupId.valueOf(range.groupId()))).setCallId(call).build();}
    static RaftClient catClient(RatisNodeRuntimeTest.Env env,RatisNodePlan plan,AtomicInteger calls){
        env.catalogue.store.enableCatalogueScopeCompaction();var binding=plan.catalogue();var resolver=new RatisRecoveryLineage(plan.clusterId(),plan.databaseId(),binding.groupId(),env.catalogue.store,binding.directoryColumn());
        var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
            if(method.getName().equals("sendReadOnly")) {
                byte[] query=((Message)args[0]).getContent().toByteArray();int kind=ScopedTransactionRpc.kind(query,ScopedTransactionRpc.MAX_REQUEST_BYTES);byte[] wire=null;
                if(kind==RatisCatalogueViewRpc.KIND){calls.incrementAndGet();var request=RatisCatalogueViewRpc.request(query);var read=env.catalogue.store.readSchemaAuthorityPoints(List.of(new ReplicatedRangeStore.PhysicalPoint(binding.directoryColumn(),uuid(request.domain())),new ReplicatedRangeStore.PhysicalPoint(binding.directoryColumn(),RatisDatabaseDomainRoster.key(request.database()))),new int[]{RatisCatalogue.MAX_DIRECTORY_BYTES,DatabaseDomainRoster.MAX_BYTES});var d=read.values().getFirst();var r=read.values().get(1);wire=RatisCatalogueViewRpc.reply(new RatisCatalogueViewRpc.View(request,read.position(),d==null?null:RangeDirectoryCodec.decode(d),r==null?null:DatabaseDomainRoster.decode(r)));}
                else if(kind==RatisRecoveryLineageRpc.KIND){var request=RatisRecoveryLineageRpc.request(query);var found=resolver.lookup(new RatisRecoveryLineage.Request(request.domain(),request.range(),request.group(),request.epoch()),request.minimumCatalogueCut());wire=RatisRecoveryLineageRpc.reply(new RatisRecoveryLineageRpc.Reply(request,found.catalogueCut(),found.birth()==null?null:found.birth().definition()));}
                if(wire!=null)return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(RaftPeerId.valueOf(binding.replicas().getFirst().peerId())).setGroupId(RaftGroupId.valueOf(binding.groupId())).setCallId(1).setSuccess(true).setMessage(message(wire)).build();
            }
            try{return method.invoke(env.catalogue.client.io(),args);}catch(InvocationTargetException failure){throw failure.getCause();}
        });
        return (RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(binding.groupId());case "io"->io;case "close"->null;default->throw new AssertionError(method);});
    }
}
