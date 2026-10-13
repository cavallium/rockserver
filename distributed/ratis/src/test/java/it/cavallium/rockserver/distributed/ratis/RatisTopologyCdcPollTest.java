package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.api.BlockingApi;
import org.apache.ratis.protocol.*;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Real native parent terminal/retirement metadata behind controlled Ratis identity, without a server or sockets. */
class RatisTopologyCdcPollTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static UUID id(long n) { return new UUID(0,n); }
    static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    static final class Fixture implements AutoCloseable {
        final UUID cluster,database,domain;
        final UUID column=id(4),groupId=id(9),rangeId=id(8),catalogue=id(20),session=id(30),admin=id(31);
        final ReplicaDescriptor local=new ReplicaDescriptor(id(5),id(6),id(7),"parent","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        final DistributedSchema schema;
        final RangeDescriptor parent=new RangeDescriptor(rangeId,groupId,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(local));
        final RangeDirectory directory,target;
        final TopologyChange.Definition definition;
        final RaftPeer peer=RaftPeer.newBuilder().setId(local.peerId()).setAddress(local.address()).build();
        final RaftGroup group=RaftGroup.valueOf(RaftGroupId.valueOf(groupId),peer);
        final AtomicReference<RatisRangeStateMachine.Leadership> leader=new AtomicReference<>(new RatisRangeStateMachine.Leadership(1,true));
        final AtomicReference<SessionEpochLedger> ledger;
        final DistributedStoragePool pool; final ReplicatedRangeStore store; final RaftStorage storage; final RatisRangeStateMachine machine;
        final RatisProofQueryService queries=new RatisProofQueryService(RatisProofFixtures.admission());
        final RatisProofQueryService sourceQueries=RatisProofQueryService.sourceOutcomeLane(RatisProofFixtures.admission());
        NativeTopologyFreeze freeze; NativeTopologySeed seed; ReplicatedRangeStore child;
        long index;
        Fixture(Path path) throws Exception { this(path,id(1),id(2),id(3)); }
        Fixture(Path path,RatisRangeStateMachine.RetentionLimits retention) throws Exception { this(path,id(1),id(2),id(3),retention); }
        Fixture(Path path,UUID cluster,UUID database,UUID domain) throws Exception {
            this(path,cluster,database,domain,RatisRangeStateMachine.RetentionLimits.defaults());
        }
        Fixture(Path path,UUID cluster,UUID database,UUID domain,RatisRangeStateMachine.RetentionLimits retention) throws Exception {
            this.cluster=cluster;this.database=database;this.domain=domain;
            schema=new DistributedSchema(cluster,database,domain,1,0,List.of(new DistributedSchema.Column(1,column,"data",ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(),true),null)));
            directory=new RangeDirectory(cluster,database,domain,1,schema.digest(),1,Set.of(column),0,List.of(parent));
            target=new RangeDirectory(cluster,database,domain,1,schema.digest(),2,Set.of(column),0,List.of(
                    new RangeDescriptor(id(41),id(51),1,1,null,new byte[]{(byte)128},RangeDescriptor.TopologyState.ACTIVE,List.of(local)),
                    new RangeDescriptor(id(42),id(52),1,1,new byte[]{(byte)128},null,RangeDescriptor.TopologyState.ACTIVE,List.of(local))));
            definition=new TopologyChange.Definition(id(40),directory,target);
            ledger=new AtomicReference<>(SessionEpochLedger.initial(cluster,database,10).issue(session,SessionEpochLedger.MAX_LIFETIME_MILLIS+10));
            pool=DistributedStoragePool.open(path.resolve("native"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            store=pool.openRange(new RangeOwnership.Owner(groupId,1),new RangeOwnership.Interval(domain,Set.of(column),null,null),schema.digest(),Map.of(column,"data"),Set.of()); store.enableLogicalSchema(schema);
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)-> {
                if(!method.getName().equals("sendReadOnly"))throw new AssertionError(method);
                byte[] bytes=ledger.get().encode(); return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(RaftGroupId.valueOf(catalogue)).setCallId(1).setSuccess(true).setMessage(message(ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,ByteBuffer.allocate(4+bytes.length).putInt(bytes.length).put(bytes).array()))).build();
            });
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)-> { if(method.getName().equals("getGroupId"))return RaftGroupId.valueOf(catalogue);if(method.getName().equals("io"))return io;throw new AssertionError(method); });
            var epochs=new RatisSessionEpochs(client,cluster,database,catalogue,1,new byte[32],id(21));
            machine=RatisRangeStateMachine.withSessions(store,group,new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,2),command->{},epochs,directory,retention,leader::get);
            machine.cdcBinding(cluster,database,rangeId); machine.proofQueries(queries);machine.sourceOutcomeQueries(sourceQueries);
            storage=RaftStorage.newBuilder().setDirectory(path.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build(); storage.initialize();
            var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)-> { if(method.getName().equals("getId"))return peer.getId();if(method.getName().equals("toString"))return "topology-cdc-fixture";throw new AssertionError(method); });
            machine.initialize(identity,group.getGroupId(),storage); machine.notifyTermIndexUpdated(1,0);
            var grant=ledger.get().grants().getFirst(); apply(DistributedCommand.sessionControl(groupId,1,schema.digest(),new OperationId(admin,1,0),RangeSessionControl.bind(ledger.get(),grant,16),-1));
            apply(new DistributedCommand(groupId,1,schema.digest(),new OperationId(session,grant.epoch(),0),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{5},new byte[]{9}))));
        }
        TransactionContext propose(DistributedCommand command) throws IOException { return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(message(DistributedCommandCodec.encode(command))).build()); }
        byte[] apply(DistributedCommand command) throws Exception { var tx=propose(command); assertNull(tx.getException());machine.preAppendTransaction(tx);tx.initLogEntry(leader.get().term(),++index);return machine.applyTransaction(tx).get(5,TimeUnit.SECONDS).getContent().toByteArray(); }
        void seal(boolean retire) throws Exception {
            byte[] initial=TopologyChange.State.begin(definition.header()).beginFencing().encode(); byte[] admission=OperatorUpload.sha256(ByteBuffer.allocate(16+initial.length).putLong(catalogue.getMostSignificantBits()).putLong(catalogue.getLeastSignificantBits()).put(initial).array());
            freeze=store.applyTopologyFreeze(new ApplyPosition(1,++index),definition,admission); machine.notifyTermIndexUpdated(1,index);
            if(retire) { var authority=TopologyChange.State.begin(definition.header()).beginFencing().frozen(freeze.proof(local.replicaId())); seed=pool.seedTopology(definition,authority,List.of(store),local); child=pool.reopenTopologyRange(seed.birth(target.ranges().getFirst().groupId()),schema); }
        }
        DistributedCommand clock(long sequence,long time) { ledger.set(ledger.get().advanceTime(time)); return DistributedCommand.sessionControl(groupId,1,schema.digest(),new OperationId(admin,Math.max(1,ledger.get().minimumEpoch()),sequence),RangeSessionControl.advance(ledger.get()),-1); }
        RatisCdcPollRpc.Request request(DistributedCdcCheckpoint.Cursor from,int bytes) { return new RatisCdcPollRpc.Request(new RatisCdcPollRpc.Binding(cluster,database,domain,new DistributedCdcCheckpoint.Lineage(rangeId,groupId,1)),from,16,bytes); }
        RatisCdcPollRpc.Result poll(RatisCdcPollRpc.Request request) throws Exception { return RatisCdcPollRpc.decodeReply(machine.queryLinearizable(message(RatisCdcPollRpc.request(request))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),request); }
        @Override public void close() throws Exception { sourceQueries.stop().get(5,TimeUnit.SECONDS);queries.stop().get(5,TimeUnit.SECONDS);machine.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));storage.close();pool.close(); }
    }
    @Test void frozenAndRetiredParentPollReturnsRealBarrierAndClockAgesOnlyMetadata() throws Exception {
        for(boolean retired:new boolean[]{false,true})try(var f=new Fixture(root.resolve("parent-"+retired))) {
            f.seal(retired);
            if(retired) {
                f.child.initializeConfiguration(f.store.configuration().data()); f.child.applyNoop(new ApplyPosition(1,0));
                var common=f.seed.authority().freezeDigest();f.child.applyTopologyActivation(new ApplyPosition(1,1),f.definition,common,f.catalogue,new ApplyPosition(1,1),NativeTopologyActivation.publicationCommitment(f.definition,common,f.catalogue));
                try(var pin=f.child.capture()){assertArrayEquals(new byte[]{9},pin.get(f.column,new byte[]{5}));}
            }
            assertEquals(0,f.store.activeSnapshotPins()); var page=f.poll(f.request(DistributedCdcCheckpoint.Cursor.beginning(),4096));
            assertEquals(2,page.records().size()); var barrier=page.records().getLast(); assertEquals(DistributedCdcRecord.Kind.TERMINAL_BARRIER,barrier.kind());assertNull(barrier.command());assertTrue(barrier.events().isEmpty());assertEquals(0,barrier.mutationCount()); assertEquals(f.freeze.position(),barrier.position());assertEquals(barrier.end(),page.next());
            var stale=f.request(barrier.end(),4096);assertThrows(ExecutionException.class,()->f.machine.query(message(RatisCdcPollRpc.request(stale))).get(5,TimeUnit.SECONDS));assertThrows(ExecutionException.class,()->f.machine.queryStale(message(RatisCdcPollRpc.request(stale)),0).get(5,TimeUnit.SECONDS));
            var foreign=new RatisCdcPollRpc.Request(new RatisCdcPollRpc.Binding(f.cluster,f.database,f.domain,new DistributedCdcCheckpoint.Lineage(f.rangeId,UUID.randomUUID(),1)),barrier.end(),1,4096);assertThrows(ExecutionException.class,()->f.poll(foreign));
            var before=f.poll(f.request(barrier.end(),4096));assertTrue(before.fromVerified());assertTrue(before.records().isEmpty());assertEquals(barrier.end(),before.next());
            var clock=f.clock(1,20); byte[] originalClock=f.apply(clock);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(originalClock).status());assertArrayEquals(originalClock,f.apply(clock));
            var after=f.poll(f.request(barrier.end(),4096)); assertEquals(f.store.applied(),after.cut()); assertEquals(barrier.end(),after.next());assertTrue(after.fromVerified());assertFalse(after.cut().equals(barrier.position()));
            assertThrows(ExecutionException.class,()->f.poll(f.request(DistributedCdcCheckpoint.Cursor.after(after.cut()),4096)));
            var grant=f.ledger.get().grants().getFirst(); var bind=DistributedCommand.sessionControl(f.groupId,1,f.schema.digest(),new OperationId(f.admin,1,9),RangeSessionControl.bind(f.ledger.get(),grant,16),-1); assertNotNull(f.propose(bind).getException());
            assertNotNull(f.propose(new DistributedCommand(f.groupId,1,f.schema.digest(),new OperationId(f.session,1,1),List.of(new Mutation(Mutation.Kind.PUT,f.column,new byte[]{5},new byte[]{8})))).getException());
            assertEquals(0,f.store.activeSnapshotPins());
            if(retired) try(var pin=f.child.capture()){assertArrayEquals(new byte[]{9},pin.get(f.column,new byte[]{5}));assertEquals(new ApplyPosition(1,1),f.child.applied());}
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(f.apply(f.clock(2,20+SessionEpochLedger.MAX_LIFETIME_MILLIS))).status());
            var gap=f.poll(f.request(DistributedCdcCheckpoint.Cursor.beginning(),4096)); assertEquals(RatisCdcPollRpc.Outcome.GAP,gap.outcome());assertEquals(barrier.end(),gap.floor()); assertEquals(DistributedCdcCheckpoint.Cursor.beginning(),gap.next());
            if(retired) try(var pin=f.child.capture()){assertArrayEquals(new byte[]{9},pin.get(f.column,new byte[]{5}));}
        }
    }
    @Test void barrierBudgetAndConfigurationGenerationLifecycleRemainQualified() throws Exception {
        try(var f=new Fixture(root)) {
            f.seal(true); var from=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,2));
            int exact=RatisCdcPollRpc.REPLY_BASE_BYTES+4+DistributedCdcRecord.terminalBarrier(f.freeze).encodedSize();
            var large=f.poll(f.request(from,exact-1));assertEquals(RatisCdcPollRpc.Outcome.TOO_LARGE,large.outcome());assertEquals(exact,large.requiredBytes());assertEquals(from,large.next());
            assertEquals(exact,RatisCdcPollRpc.reply(f.poll(f.request(from,exact))).length);
            var pending=f.propose(f.clock(1,20));assertNull(pending.getException());
            f.machine.notifyConfigurationChanged(1,f.index+1,RaftConfigurationProto.newBuilder().addPeers(f.peer.getRaftPeerProto()).build());
            assertThrows(org.apache.ratis.protocol.exceptions.StateMachineException.class,()->f.machine.preAppendTransaction(pending));assertThrows(ExecutionException.class,()->f.poll(f.request(from,4096)));
            f.machine.notifyTermIndexUpdated(1,++f.index); f.machine.notifyNotLeader(List.of());f.leader.set(new RatisRangeStateMachine.Leadership(2,false));assertThrows(ExecutionException.class,()->f.poll(f.request(from,4096)));
            f.machine.pause(); assertThrows(ExecutionException.class,()->f.poll(f.request(from,4096)));
        }
    }
}
