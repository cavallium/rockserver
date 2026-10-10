package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.*;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real native birth/activation and adapter ownership; controlled division interfaces, no sockets/quorum. */
class RatisTopologyActivationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    static void await(CountDownLatch latch) { RatisTopologyFreezeTest.await(latch); }
    static final class Fixture implements AutoCloseable {
        final RatisTopologyFreezeTest.Fixture parent;
        final NativeTopologySeed seed;
        final ReplicatedRangeStore child;
        final RangeDescriptor target;
        final RaftGroup group;
        final RatisTopologyAuthority.View view;
        final RatisTopologyActivationRecord record;
        final RatisRangeStateMachine machine;
        final RaftStorage storage;
        final AtomicLong term=new AtomicLong(1);
        final AtomicInteger reads=new AtomicInteger(),observations=new AtomicInteger();
        final AtomicReference<RaftConfiguration> configuration=new AtomicReference<>();
        final SessionEpochLedger ledger;
        final RatisSessionEpochs epochs;
        final AtomicReference<LifeCycle.State> divisionState=new AtomicReference<>(LifeCycle.State.RUNNING);
        volatile Runnable beforeRead=()->{};
        volatile boolean unavailable;
        Fixture(Path path)throws Exception { this(path,false); }
        Fixture(Path path,boolean sessions)throws Exception {
            parent=new RatisTopologyFreezeTest.Fixture(path.resolve("parent")); parent.apply(parent.propose(DistributedCommandCodec.encode(parent.put(0,9))),1).get(5,TimeUnit.SECONDS);parent.prepare();var freeze=parent.freeze(2);parent.machine.topologyCleanup().get(5,TimeUnit.SECONDS);
            var authority=TopologyChange.State.begin(parent.definition.header()).beginFencing().frozen(freeze.proof(parent.local.replicaId())); seed=parent.pool.seedTopology(parent.definition,authority,List.of(parent.store),parent.local);
            var published=authority;for(var range:parent.definition.targets())published=published.seeded(seed.proof(range.groupId()));published=published.publish();
            view=new RatisTopologyAuthority.View(parent.view.catalogue(),new ApplyPosition(1,5),parent.definition,published);target=parent.definition.targets().getFirst();child=parent.pool.reopenTopologyRange(seed.birth(target.groupId()),parent.schema);group=RaftGroup.valueOf(RaftGroupId.valueOf(target.groupId()),parent.peer);record=RatisTopologyActivationRecord.of(view,target.groupId());
            storage=RaftStorage.newBuilder().setDirectory(path.resolve("child-raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();storage.initialize();
            ledger=SessionEpochLedger.initial(parent.schema.clusterId(),parent.schema.databaseId(),0).issue(RatisTopologyFreezeTest.id(333),1000);
            var io=(org.apache.ratis.client.api.BlockingApi)Proxy.newProxyInstance(org.apache.ratis.client.api.BlockingApi.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.api.BlockingApi.class},(proxy,method,args)->{
                if(!method.getName().equals("sendReadOnly"))throw new AssertionError(method);byte[] value=ledger.encode();byte[] payload=ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,java.nio.ByteBuffer.allocate(4+value.length).putInt(value.length).put(value).array());return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(parent.peer.getId()).setGroupId(RaftGroupId.valueOf(parent.view.catalogue())).setCallId(1).setSuccess(true).setMessage(message(payload)).build();
            });
            var client=(org.apache.ratis.client.RaftClient)Proxy.newProxyInstance(org.apache.ratis.client.RaftClient.class.getClassLoader(),new Class<?>[]{org.apache.ratis.client.RaftClient.class},(proxy,method,args)->switch(method.getName()){case "getGroupId"->RaftGroupId.valueOf(parent.view.catalogue());case "io"->io;default->throw new AssertionError(method);});
            epochs=new RatisSessionEpochs(client,parent.schema.clusterId(),parent.schema.databaseId(),parent.view.catalogue(),1,new byte[32],RatisTopologyFreezeTest.id(334));
            machine=sessions ? RatisRangeStateMachine.withSessions(child,group,new NativeRangeImage.Limits(4<<20,16<<20,2),command->{},epochs,parent.definition.targetDirectory(),RatisRangeStateMachine.RetentionLimits.defaults(),()->new RatisRangeStateMachine.Leadership(term.get(),true)) : new RatisRangeStateMachine(child,group,new NativeRangeImage.Limits(4<<20,16<<20,1),command->{},command->{},()->new RatisRangeStateMachine.Leadership(term.get(),true));machine.role(RatisNodePlan.Role.DATA);machine.proofQueries(parent.queries);
            machine.topologyAuthority((header,stage)->{reads.incrementAndGet();beforeRead.run();if(unavailable)throw new IOException("Controlled unavailable authority");assertEquals(TopologyChange.Stage.ACTIVATE,stage);return view;},parent.definition.targetDirectory(),parent.local,parent.pool);machine.topologyActivated((definition,receipt)->observations.incrementAndGet());
            configuration.set(configuration(parent.peer));
            var info=(DivisionInfo)Proxy.newProxyInstance(DivisionInfo.class.getClassLoader(),new Class<?>[]{DivisionInfo.class},(proxy,method,args)->{if(method.getName().equals("getCurrentTerm"))return term.get();if(method.getName().equals("getLifeCycleState"))return divisionState.get();if(method.getName().equals("isAlive"))return divisionState.get()==LifeCycle.State.RUNNING;throw new AssertionError(method);});
            var division=(RaftServer.Division)Proxy.newProxyInstance(RaftServer.Division.class.getClassLoader(),new Class<?>[]{RaftServer.Division.class},(proxy,method,args)->switch(method.getName()){case "getInfo"->info;case "getRaftConf"->configuration.get();case "getStateMachine"->machine;case "toString"->"controlled-child-division";default->throw new AssertionError(method);});
            var server=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)->switch(method.getName()){case "getId"->parent.peer.getId();case "getDivision"->division;case "toString"->"controlled-child-server";default->throw new AssertionError(method);});
            machine.initialize(server,group.getGroupId(),storage);machine.notifyTermIndexUpdated(1,0);
        }
        static RaftConfiguration configuration(RaftPeer peer) { return (RaftConfiguration)Proxy.newProxyInstance(RaftConfiguration.class.getClassLoader(),new Class<?>[]{RaftConfiguration.class},(proxy,method,args)->switch(method.getName()){case "getCurrentPeers","getAllPeers"->args==null || args.length==0 || args[0]==RaftPeerRole.FOLLOWER ? List.of(peer):List.of();case "getPreviousPeers"->List.of();case "getLogEntryIndex"->-1L;case "toString"->"controlled-stable-configuration";default->throw new AssertionError(method);}); }
        CompletableFuture<Message> preparing() { return machine.queryLinearizable(message(RatisTopologyActivationRpc.prepareRequest(record))); }
        RatisTopologyActivationRpc.State prepare()throws Exception { return RatisTopologyActivationRpc.stateReply(preparing().get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologyActivationRpc.PREPARE,record); }
        void cancel()throws Exception { RatisTopologyActivationRpc.decodeCancelledReply(machine.query(message(RatisTopologyActivationRpc.cancelRequest(record))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),record); }
        TransactionContext propose(byte[] bytes)throws IOException { return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(parent.peer.getId()).setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(message(bytes)).build()); }
        NativeTopologyActivation activate(long index)throws Exception { var context=propose(record.encode());assertNull(context.getException());machine.preAppendTransaction(context);context.initLogEntry(term.get(),index);return NativeTopologyActivation.decode(machine.applyTransaction(context).get(5,TimeUnit.SECONDS).getContent().toByteArray()); }
        NativeTopologyActivation replay(long index)throws Exception { var context=TransactionContext.newBuilder().setStateMachine(machine).setServerRole(RaftPeerRole.FOLLOWER).setLogEntry(LogEntryProto.newBuilder().setTerm(term.get()).setIndex(index).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(record.encode()))).build()).build();return NativeTopologyActivation.decode(machine.applyTransaction(context).get(5,TimeUnit.SECONDS).getContent().toByteArray()); }
        RatisTopologyOwnerRpc.Evidence evidence(TopologyChange.Stage stage)throws Exception { var target=new RatisTopologyOwnerRpc.Target(parent.definition,stage,parent.view.catalogue(),this.target.groupId(),parent.local.replicaId());var response=machine.queryStale(message(RatisTopologyOwnerRpc.request(target)),0).get(5,TimeUnit.SECONDS);return RatisTopologyOwnerRpc.decodeReply(response.getContent().toByteArray(),target); }
        @Override public void close()throws Exception { machine.close();machine.topologyCleanup().get(5,TimeUnit.SECONDS);machine.awaitNativeReaders(Duration.ofSeconds(5));storage.close();parent.close(); }
    }
    @Test void freshPreparationOriginalRetryAndChildBusinessAfterNativeActivation()throws Exception {
        try(var f=new Fixture(root)) {
            assertNotNull(f.propose(f.record.encode()).getException());assertEquals(0,f.reads.get());assertTrue(f.prepare().ready());assertTrue(f.prepare().ready());assertEquals(1,f.reads.get());var receipt=f.activate(1);assertEquals(1,f.observations.get());assertFalse(f.child.topologyBirthPending());assertEquals(1,f.machine.takeSnapshot());assertEquals(new ApplyPosition(1,1),receipt.position());
            f.unavailable=true;assertArrayEquals(receipt.encode(),f.activate(2).encode());assertEquals(1,f.reads.get());assertEquals(1,f.observations.get());assertArrayEquals(new byte[]{9},f.child.readCurrentPoint(f.parent.column,new byte[]{1}));
            var command=new DistributedCommand(f.target.groupId(),1,f.parent.schema.digest(),new OperationId(RatisTopologyFreezeTest.id(333),1,0),List.of(new Mutation(Mutation.Kind.PUT,f.parent.column,new byte[]{1},new byte[]{8})));var write=f.propose(DistributedCommandCodec.encode(command));assertNull(write.getException());f.machine.preAppendTransaction(write);write.initLogEntry(1,3);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.machine.applyTransaction(write).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());assertArrayEquals(new byte[]{8},f.child.readCurrentPoint(f.parent.column,new byte[]{1}));
            assertNotNull(NativeRangeImage.write(root.resolve("child-image"),f.child,new NativeRangeImage.Limits(4<<20,16<<20,1)).topologyOrigin());
        }
    }
    @Test void followerUsesOwnDurableBirthAndOwnerProofRequiresActualCurrentDivision()throws Exception {
        try(var f=new Fixture(root)) {
            f.unavailable=true;var receipt=f.replay(1);assertEquals(0,f.reads.get());assertEquals(1,f.observations.get());assertArrayEquals(f.seed.birth(f.target.groupId()).proof(f.seed).digest(),receipt.proof().predecessorDigest());
            var evidence=f.evidence(TopologyChange.Stage.ACTIVATE);assertArrayEquals(receipt.encode(),evidence.artifact());assertEquals(0,evidence.configurationTerm());assertEquals(-1,evidence.configurationIndex());assertEquals(new ApplyPosition(1,1),evidence.completedCut());assertEquals(f.parent.local.replicaId(),evidence.proof().owner().replicaId());
            assertArrayEquals(f.seed.encode(),f.evidence(TopologyChange.Stage.SEED).artifact());
            for(var state:List.of(LifeCycle.State.STARTING,LifeCycle.State.CLOSING)) { f.divisionState.set(state);assertThrows(ExecutionException.class,()->f.evidence(TopologyChange.Stage.ACTIVATE)); } f.divisionState.set(LifeCycle.State.RUNNING);
            f.term.set(2);assertThrows(ExecutionException.class,()->f.evidence(TopologyChange.Stage.ACTIVATE));f.term.set(1);
            f.configuration.set(Fixture.configuration(RaftPeer.newBuilder().setId("wrong").setAddress("127.0.0.1:9999").build()));assertThrows(ExecutionException.class,()->f.evidence(TopologyChange.Stage.ACTIVATE));
        }
    }
    @Test void cancelledAuthorityLookupRetainsGateUntilActualReturnAndLeaderChangeInvalidatesAppend()throws Exception {
        try(var f=new Fixture(root)) {
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);f.beforeRead=()->{entered.countDown();await(release);};var preparing=f.preparing();await(entered);
            try { f.cancel();assertEquals(1,f.machine.topologyActivationStats().holds());assertEquals(1,f.machine.topologyActivationStats().reading());assertEquals(1,f.machine.proposalGateStats().gates()); }
            finally { release.countDown(); }
            assertThrows(ExecutionException.class,()->preparing.get(5,TimeUnit.SECONDS));f.machine.topologyCleanup().get(5,TimeUnit.SECONDS);assertEquals(0,f.machine.topologyActivationStats().holds());assertEquals(0,f.machine.proposalGateStats().gates());
            f.beforeRead=()->{};assertTrue(f.prepare().ready());var proposal=f.propose(f.record.encode());assertNull(proposal.getException());f.machine.notifyNotLeader(List.of(proposal));assertThrows(IOException.class,()->f.machine.preAppendTransaction(proposal));assertTrue(f.child.topologyBirthPending());
        }
    }
    @Test void possiblyAppendedActivationRetainsChargeThroughLeaderLossAndActualContextRelease()throws Exception {
        try(var f=new Fixture(root)) {
            f.prepare();var proposal=f.propose(f.record.encode());assertNull(proposal.getException());f.machine.preAppendTransaction(proposal);proposal.initLogEntry(1,1);
            f.machine.notifyNotLeader(List.of());assertEquals(1,f.machine.topologyActivationStats().holds());f.term.set(2);assertThrows(ExecutionException.class,()->f.preparing().get(5,TimeUnit.SECONDS));assertEquals(1,f.machine.topologyActivationStats().holds());
            // A genuine newer committed NOOP excludes the old suffix, but the retained context
            // still owns its encoded input until cancellation actually releases that reference.
            f.machine.notifyTermIndexUpdated(2,1);assertThrows(ExecutionException.class,()->f.preparing().get(5,TimeUnit.SECONDS));assertEquals(1,f.machine.topologyActivationStats().holds());assertTrue(f.machine.topologyActivationStats().bytes()>0);
            f.machine.cancelTransaction(proposal);assertEquals(0,f.machine.topologyActivationStats().holds());assertTrue(f.prepare().ready());var receipt=f.activate(2);assertEquals(new ApplyPosition(2,2),receipt.position());assertEquals(0,f.machine.topologyActivationStats().holds());
        }
    }

    @Test void originalTopologyRetryAfterActualSchemaActivationPreservesCurrentSessionAdmission()throws Exception {
        try(var f=new Fixture(root,true)) {
            f.prepare();var original=f.activate(1);var issuer=RatisTopologyFreezeTest.id(335);var binding=DistributedCommand.sessionControl(f.target.groupId(),1,f.parent.schema.digest(),new OperationId(issuer,1,0),RangeSessionControl.bind(f.ledger,f.ledger.grants().getFirst(),4),-1);
            var accepted=f.propose(DistributedCommandCodec.encode(binding));assertNull(accepted.getException());f.machine.preAppendTransaction(accepted);accepted.initLogEntry(1,2);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.machine.applyTransaction(accepted).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());
            var sibling=f.parent.pool.reopenTopologyRange(f.seed.birth(f.parent.definition.targets().getLast().groupId()),f.parent.schema);sibling.initializeConfiguration(f.child.configuration().data());sibling.applyNoop(new ApplyPosition(1,0));sibling.applyTopologyActivation(new ApplyPosition(1,1),f.record.definition(),f.record.freezeDigest(),f.record.catalogueGroup(),f.record.catalogueCut(),f.record.publicationCommitment());
            var columns=new ArrayList<>(f.parent.schema.columns());var extra=RatisTopologyFreezeTest.id(336);columns.add(new DistributedSchema.Column(2,extra,"added",columns.getFirst().schema(),null));var schema=new DistributedSchema(f.parent.schema.clusterId(),f.parent.schema.databaseId(),f.parent.schema.domainId(),f.parent.schema.schemaEpoch()+1,0,columns);
            var change=new SchemaChange.Definition(RatisTopologyFreezeTest.id(337),new OperationId(RatisTopologyFreezeTest.id(338),1,0),f.parent.definition.targetDirectory(),f.parent.schema,schema);var names=Map.of(f.parent.column,"data",extra,"added");
            f.child.applySchemaFence(new ApplyPosition(1,3),change.header());var install=f.parent.pool.installSchema(f.child,new ApplyPosition(1,4),change,names,Set.of());sibling.applySchemaFence(new ApplyPosition(1,2),change.header());var siblingInstall=f.parent.pool.installSchema(sibling,new ApplyPosition(1,3),change,names,Set.of());sibling.applySchemaActivation(new ApplyPosition(1,4),siblingInstall.proof(),f.record.catalogueGroup(),f.record.catalogueCut());
            f.machine.notifyTermIndexUpdated(1,4);var activation=new RatisSchemaActivateRecord(change.header(),f.target.groupId(),install.position(),f.record.catalogueGroup(),f.record.catalogueCut());var replay=TransactionContext.newBuilder().setStateMachine(f.machine).setServerRole(RaftPeerRole.FOLLOWER).setLogEntry(LogEntryProto.newBuilder().setTerm(1).setIndex(5).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(activation.encode()))).build()).build();f.machine.applyTransaction(replay).get(5,TimeUnit.SECONDS);
            assertArrayEquals(original.encode(),f.replay(6).encode());assertEquals(1,f.reads.get());
            var advance=DistributedCommand.sessionControl(f.target.groupId(),1,schema.digest(),new OperationId(issuer,1,1),RangeSessionControl.advance(f.ledger),0);var control=f.propose(DistributedCommandCodec.encode(advance));assertNull(control.getException());f.machine.preAppendTransaction(control);control.initLogEntry(1,7);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.machine.applyTransaction(control).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());
            var command=new DistributedCommand(f.target.groupId(),1,schema.digest(),new OperationId(f.ledger.grants().getFirst().sessionId(),1,0),List.of(new Mutation(Mutation.Kind.PUT,extra,new byte[]{1},new byte[]{8})));var data=f.propose(DistributedCommandCodec.encode(command));assertNull(data.getException());f.machine.preAppendTransaction(data);data.initLogEntry(1,8);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.machine.applyTransaction(data).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());assertArrayEquals(new byte[]{8},f.child.readCurrentPoint(extra,new byte[]{1}));
        }
    }

}
