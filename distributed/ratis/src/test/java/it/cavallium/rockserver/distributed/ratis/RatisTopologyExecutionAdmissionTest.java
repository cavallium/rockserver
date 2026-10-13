package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Catalogue native CAS/receipt admission with controlled configured proof reader; no owner transport claim. */
class RatisTopologyExecutionAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisRangeImportCatalogueTest outer=new RatisRangeImportCatalogueTest();
        final RatisRangeImportCatalogueTest.Fixture nativeOwner;
        final RatisTopologyCatalogue catalogue;
        final TopologyChange.Definition definition;
        final TopologyChange.Proof proof;
        final DistributedCommand command;
        final RatisRangeStateMachine machine;
        final RaftStorage storage;
        final RaftPeer peer=RaftPeer.newBuilder().setId("topology-catalogue").build();
        final RaftGroup group;
        final AtomicLong term=new AtomicLong(1);
        final AtomicInteger reads=new AtomicInteger();
        final RatisProofQueryService queries=RatisProofQueryService.importPublicationLane(RatisProofFixtures.admission());
        volatile Runnable beforeRead=()->{};
        volatile boolean unavailable;
        Fixture(boolean admission)throws Exception {
            outer.root=root;nativeOwner=outer.new Fixture();group=RaftGroup.valueOf(RaftGroupId.valueOf(nativeOwner.outer.catalogue),peer);
            storage=RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();storage.initialize();
            machine=new RatisRangeStateMachine(nativeOwner.nativeCatalogue.store,group,new NativeRangeImage.Limits(4<<20,32<<20,2),nativeOwner.pure,ignored->{},()->new RatisRangeStateMachine.Leadership(term.get(),true));machine.role(RatisNodePlan.Role.CATALOGUE);machine.importPublicationQueries(queries);
            if(admission)machine.topologyExecutionAdmission(this::admit);
            var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)->switch(method.getName()){case "getId"->peer.getId();case "toString"->"controlled-topology-catalogue";default->throw new AssertionError(method);});machine.initialize(identity,group.getGroupId(),storage);
            var helper=new RatisTopologyCatalogueTest();var source=helper.source(nativeOwner);definition=helper.split(source,1,128);catalogue=helper.catalogue(nativeOwner);nativeOwner.seed(source);catalogue.execute(catalogue.prepareDefinition(nativeOwner.operation(1),definition,null));catalogue.execute(catalogue.prepareBegin(nativeOwner.operation(2),definition,null,null,null));catalogue.execute(catalogue.prepareFencing(nativeOwner.operation(3),definition,catalogue.readHead(source.domainId()),null,null));
            machine.notifyTermIndexUpdated(1,nativeOwner.nativeCatalogue.store.applied().index());var participant=definition.header().parents().getFirst();proof=TopologyChange.Proof.of(TopologyChange.Stage.FREEZE,definition.header(),participant.groupId(),participant.owners().getFirst().replicaId(),new ApplyPosition(1,5),new byte[32],definition.header().sourceDirectoryDigest());command=catalogue.prepareProof(nativeOwner.operation(4),definition,catalogue.readHead(source.domainId()),proof);
        }
        void admit(DistributedCommand command)throws IOException { RatisTopologyCatalogue.admitExecution(command,nativeOwner.outer.cluster,nativeOwner.outer.database,nativeOwner.outer.catalogue,1,new byte[32],nativeOwner.outer.directoryColumn,nativeOwner.immutable::read,(definition,expected)->{reads.incrementAndGet();beforeRead.run();if(unavailable)throw new IOException("Controlled absent owner evidence");assertEquals(proof,expected);return new RatisTopologyCatalogue.QualifiedOwner(proof,new ApplyPosition(1,5));}); }
        CompletableFuture<Message> preparing() { return machine.queryLinearizable(RatisTopologyActivationTest.message(RatisTopologyExecutionRpc.prepareRequest(command))); }
        void prepare()throws Exception { RatisTopologyExecutionRpc.requireEcho(preparing().get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologyExecutionRpc.PREPARE,command); }
        void cancel()throws Exception { RatisTopologyExecutionRpc.requireEcho(machine.queryLinearizable(RatisTopologyActivationTest.message(RatisTopologyExecutionRpc.cancelRequest(command))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologyExecutionRpc.CANCEL,command); }
        TransactionContext propose()throws IOException { return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(RatisTopologyActivationTest.message(DistributedCommandCodec.encode(command))).build()); }
        void apply(TransactionContext context)throws Exception { machine.preAppendTransaction(context);context.initLogEntry(term.get(),nativeOwner.nativeCatalogue.next.getAndIncrement());assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(machine.applyTransaction(context).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status()); }
        void replay()throws Exception { var context=TransactionContext.newBuilder().setStateMachine(machine).setServerRole(RaftPeerRole.FOLLOWER).setLogEntry(LogEntryProto.newBuilder().setTerm(term.get()).setIndex(nativeOwner.nativeCatalogue.next.getAndIncrement()).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build()).build();assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(machine.applyTransaction(context).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status()); }
        @Override public void close()throws Exception { queries.stop().get(5,TimeUnit.SECONDS);machine.close();machine.awaitNativeReaders(Duration.ofSeconds(5));storage.close();nativeOwner.close(); }
    }
    @Test void newExecutionRequiresPreparedOwnerButSavedOutcomeAndReplayDoNotLookUpOwners()throws Exception {
        try(var f=new Fixture(true)) {
            assertTrue(RatisTopologyCatalogue.requiresOwnerAdmission(f.command));assertNotNull(f.propose().getException());assertEquals(0,f.reads.get());f.prepare();f.prepare();assertEquals(1,f.reads.get());var proposal=f.propose();assertNull(proposal.getException());f.apply(proposal);assertEquals(0,f.machine.importPublicationStats().holds());assertEquals(0,f.machine.proposalGateStats().gates());
            f.unavailable=true;f.prepare();assertEquals(1,f.reads.get());f.apply(f.propose());f.replay();assertEquals(1,f.reads.get());assertTrue(f.catalogue.readHead(f.definition.header().domainId()).frozen().containsValue(f.proof));
        }
    }
    @Test void followerCommittedExecutionNeverDoesOwnerLookupAndUnconfiguredLeaderFailsClosed()throws Exception {
        try(var f=new Fixture(false)) { assertNotNull(f.propose().getException());assertThrows(ExecutionException.class,()->f.preparing().get(5,TimeUnit.SECONDS));f.replay();assertEquals(0,f.reads.get()); }
    }
    @Test void cancellationRetainsActualLookupChargeAndTermChangeRejectsPreparedAppend()throws Exception {
        try(var f=new Fixture(true)) {
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);f.beforeRead=()->{entered.countDown();RatisTopologyActivationTest.await(release);};var preparing=f.preparing();RatisTopologyActivationTest.await(entered);
            try { f.cancel();assertEquals(1,f.machine.importPublicationStats().holds());assertEquals(1,f.machine.importPublicationStats().reading());assertTrue(f.machine.importPublicationStats().bytes()>0);assertEquals(1,f.machine.proposalGateStats().gates()); }
            finally { release.countDown(); }
            assertThrows(ExecutionException.class,()->preparing.get(5,TimeUnit.SECONDS));assertEquals(0,f.machine.importPublicationStats().holds());assertEquals(0,f.machine.proposalGateStats().gates());f.beforeRead=()->{};f.prepare();var proposal=f.propose();assertNull(proposal.getException());f.term.set(2);assertThrows(IOException.class,()->f.machine.preAppendTransaction(proposal));
        }
    }
    @Test void unrelatedSameTermNoopDuringActualOwnerLookupPreservesPreparedKeyGate()throws Exception {
        try(var f=new Fixture(true)) {
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);f.beforeRead=()->{entered.countDown();RatisTopologyActivationTest.await(release);};var preparing=f.preparing();RatisTopologyActivationTest.await(entered);
            var before=f.nativeOwner.nativeCatalogue.store.applied();
            try { f.machine.notifyTermIndexUpdated(1,f.nativeOwner.nativeCatalogue.next.getAndIncrement());assertTrue(f.nativeOwner.nativeCatalogue.store.applied().index()>before.index());assertEquals(1,f.machine.importPublicationStats().reading());assertEquals(1,f.machine.proposalGateStats().gates()); }
            finally { release.countDown(); }
            RatisTopologyExecutionRpc.requireEcho(preparing.get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisTopologyExecutionRpc.PREPARE,f.command);assertEquals(1,f.reads.get());var proposal=f.propose();assertNull(proposal.getException());f.apply(proposal);assertTrue(f.catalogue.readHead(f.definition.header().domainId()).frozen().containsValue(f.proof));assertEquals(0,f.machine.importPublicationStats().holds());assertEquals(0,f.machine.proposalGateStats().gates());
        }
    }

}
