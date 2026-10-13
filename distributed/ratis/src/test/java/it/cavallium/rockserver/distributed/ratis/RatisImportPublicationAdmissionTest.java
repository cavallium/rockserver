package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.proto.RaftProtos.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native admission and exact saved-result replay; no server, socket or quorum. */
class RatisImportPublicationAdmissionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final class Fixture implements AutoCloseable {
        final RatisRangeImportCatalogueTest test=new RatisRangeImportCatalogueTest();
        final RatisRangeImportCatalogueTest.Fixture catalogue;
        final RaftStorage storage;
        final RatisRangeStateMachine machine;
        final RaftPeer peer=RaftPeer.newBuilder().setId("catalogue-fixture").build();
        final RaftGroup group;
        final DistributedCommand publication;
        final RatisProofQueryService queries=RatisProofQueryService.importPublicationLane(RatisProofFixtures.admission());
        final AtomicLong term=new AtomicLong(1);
        final RatisProofQueryService authority=RatisProofQueryService.authorityLane(RatisProofFixtures.admission());
        Fixture(RatisRangeStateMachine.ImportPublicationAdmission admission) throws Exception {
            this(admission,machine->{});
        }
        Fixture(RatisRangeStateMachine.ImportPublicationAdmission admission,java.util.function.Consumer<RatisRangeStateMachine> configure) throws Exception {
            test.root=root; catalogue=test.new Fixture();
            group=RaftGroup.valueOf(RaftGroupId.valueOf(catalogue.outer.catalogue),peer);
            storage=RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT)
                    .setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build(); storage.initialize();
            machine=new RatisRangeStateMachine(catalogue.nativeCatalogue.store,group,new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,2),
                    catalogue.pure,command -> {},() -> new RatisRangeStateMachine.Leadership(term.get(),true));
            machine.role(RatisNodePlan.Role.CATALOGUE);machine.schemaCatalogue(catalogue.outer.cluster,catalogue.outer.database,catalogue.outer.directoryColumn);machine.schemaAuthorityQueries(authority);machine.importPublicationQueries(queries); if(admission!=null) machine.importPublicationAdmission(admission);
            configure.accept(machine);
            var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args) -> {
                if(method.getName().equals("getId")) return peer.getId(); if(method.getName().equals("toString")) return "import-admission-fixture"; throw new AssertionError(method);
            });
            machine.initialize(identity,group.getGroupId(),storage);
            var definition=RatisRangeImportCatalogueTest.definition(catalogue.outer.cluster,catalogue.outer.database,catalogue.operation(20));
            var owners=RatisRangeImportCatalogueTest.admission(definition,5); catalogue.seed(definition.sourceDirectory()); catalogue.stage(definition,owners);
            machine.notifyTermIndexUpdated(1,catalogue.nativeCatalogue.store.applied().index());
            publication=catalogue.catalogue.preparePublication(definition,catalogue.catalogue.readHead(definition.sourceDirectory().domainId()),owners,null);
        }
        CompletableFuture<Message> prepare(){return machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisImportPublicationRpc.prepareRequest(publication))));}
        void cancel()throws Exception{var reply=machine.queryLinearizable(Message.valueOf(ByteString.copyFrom(RatisImportPublicationRpc.cancelRequest(publication)))).get(5,TimeUnit.SECONDS);RatisImportPublicationRpc.decodePreparedReply(reply.getContent().toByteArray(),ScopedTransactionRpc.IMPORT_PUBLICATION_CANCEL,publication);}
        void replay()throws Exception{var context=TransactionContext.newBuilder().setStateMachine(machine).setServerRole(RaftPeerRole.FOLLOWER).setLogEntry(LogEntryProto.newBuilder().setTerm(term.get()).setIndex(catalogue.nativeCatalogue.next.getAndIncrement()).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(DistributedCommandCodec.encode(publication)))).build()).build();assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(machine.applyTransaction(context).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status());}
        TransactionContext propose() throws IOException {
            return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId())
                    .setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType())
                    .setMessage(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(publication)))).build());
        }
        void apply(TransactionContext context) throws Exception { machine.preAppendTransaction(context); context.initLogEntry(1,catalogue.nativeCatalogue.next.getAndIncrement());
            assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(machine.applyTransaction(context).get(5,TimeUnit.SECONDS).getContent().toByteArray()).status()); }
        public void close() throws Exception { authority.stop().get(5,TimeUnit.SECONDS);queries.stop().get(5,TimeUnit.SECONDS);machine.close(); machine.awaitNativeReaders(Duration.ofSeconds(5)); storage.close(); catalogue.close(); }
    }
    @Test void newPublicationFailsClosedWithoutTrustedAdmission() throws Exception {
        try(var f=new Fixture(null)) { assertNotNull(f.propose().getException()); }
    }
    @Test void lostSuccessfulPrepareReplyCanRetryAndStaleUnsubmittedCutIsRequalified() throws Exception {
        var checks=new AtomicInteger();
        try(var f=new Fixture(command->checks.incrementAndGet())) {
            f.prepare().get(5,TimeUnit.SECONDS); // The transport loses this successful reply.
            var retry=f.prepare().get(5,TimeUnit.SECONDS);
            RatisImportPublicationRpc.decodePreparedReply(retry.getContent().toByteArray(),ScopedTransactionRpc.IMPORT_PUBLICATION_PREPARE,f.publication);
            assertEquals(1,checks.get());assertEquals(1,f.machine.importPublicationStats().holds());assertEquals(1,f.machine.proposalGateStats().gates());
            f.machine.notifyTermIndexUpdated(1,f.catalogue.nativeCatalogue.next.getAndIncrement());
            f.prepare().get(5,TimeUnit.SECONDS);assertEquals(2,checks.get());assertEquals(1,f.machine.importPublicationStats().holds());assertEquals(1,f.machine.proposalGateStats().gates());
            var proposal=f.propose();assertNull(proposal.getException());f.apply(proposal);
            assertEquals(0,f.machine.importPublicationStats().holds());assertEquals(0,f.machine.proposalGateStats().gates());
            f.prepare().get(5,TimeUnit.SECONDS);assertEquals(2,checks.get());
        }
    }
    @Test void freshChecksPrecedeAppendButSavedExactOutcomeReplaysWithoutThem() throws Exception {
        var checks=new AtomicInteger(); var unavailable=new java.util.concurrent.atomic.AtomicBoolean();
        try(var f=new Fixture(command -> { checks.incrementAndGet(); if(unavailable.get()) throw new IOException("Owner unavailable"); })) {
            unavailable.set(true);assertNotNull(f.propose().getException());assertEquals(0,checks.get(),"startTransaction must never perform owner RPCs");assertThrows(ExecutionException.class,()->f.prepare().get(5,TimeUnit.SECONDS));assertEquals(1,checks.get());
            unavailable.set(false);var reply=f.prepare().get(5,TimeUnit.SECONDS);RatisImportPublicationRpc.decodePreparedReply(reply.getContent().toByteArray(),ScopedTransactionRpc.IMPORT_PUBLICATION_PREPARE,f.publication);var accepted=f.propose(); assertNull(accepted.getException()); assertEquals(2,checks.get()); f.apply(accepted);assertEquals(0,f.machine.importPublicationStats().holds());
            unavailable.set(true); var retry=f.propose(); assertNull(retry.getException()); assertEquals(2,checks.get()); f.apply(retry);
            f.replay();
            assertEquals(2,checks.get(),"Committed apply and saved outcome must not perform fresh owner reads");
        }
    }
    @Test void blockedOwnerLookupKeepsGateAndBytesUntilCancelledActualLookupExits()throws Exception{
        var entered=new CountDownLatch(1);var resume=new CountDownLatch(1);var checks=new AtomicInteger();
        try(var f=new Fixture(command->{checks.incrementAndGet();entered.countDown();try{if(!resume.await(5,TimeUnit.SECONDS))throw new IOException("Lookup timed out");}catch(InterruptedException e){throw new IOException(e);}})){
            try{var preparing=f.prepare();assertTrue(entered.await(5,TimeUnit.SECONDS));assertEquals(RatisProofQueryService.IMPORT_PUBLICATION_BYTES,f.queries.stats().bytes());assertEquals(1,f.machine.importPublicationStats().reading());assertNotNull(f.propose().getException());assertThrows(ExecutionException.class,()->f.prepare().get(5,TimeUnit.SECONDS));
                assertTrue(preparing.cancel(false));f.cancel();assertEquals(1,f.machine.importPublicationStats().holds());assertEquals(1,f.machine.proposalGateStats().gates());assertEquals(RatisProofQueryService.IMPORT_PUBLICATION_BYTES,f.queries.stats().bytes());resume.countDown();f.queries.stop().get(5,TimeUnit.SECONDS);assertEquals(0,f.machine.importPublicationStats().holds());assertEquals(0,f.machine.proposalGateStats().gates());assertEquals(0,f.queries.stats().bytes());assertEquals(1,checks.get());
            }finally{resume.countDown();}
        }
    }
    @Test void queuedConfigurationInvalidatesPreparedTicketBeforeAppend()throws Exception{
        try(var f=new Fixture(command->{})){f.prepare().get(5,TimeUnit.SECONDS);var proposed=f.propose();assertNull(proposed.getException());long index=f.catalogue.nativeCatalogue.next.get();f.machine.notifyConfigurationChanged(1,index,RaftConfigurationProto.newBuilder().addPeers(f.peer.getRaftPeerProto()).build());assertThrows(org.apache.ratis.protocol.exceptions.StateMachineException.class,()->f.machine.preAppendTransaction(proposed));assertEquals(0,f.machine.importPublicationStats().holds());assertNotNull(f.propose().getException());}
    }
    @Test void submittedUnknownSuffixRetainsHoldUntilNewNativeTermExcludesIt()throws Exception{
        try(var f=new Fixture(command->{})){f.prepare().get(5,TimeUnit.SECONDS);var proposed=f.propose();assertNull(proposed.getException());proposed.initLogEntry(1,f.catalogue.nativeCatalogue.next.get());f.cancel();assertEquals(1,f.machine.importPublicationStats().holds());f.machine.notifyNotLeader(List.of(proposed));assertEquals(1,f.machine.importPublicationStats().holds());assertEquals(1,f.machine.proposalGateStats().gates());f.term.set(2);f.machine.notifyTermIndexUpdated(2,f.catalogue.nativeCatalogue.next.getAndIncrement());assertNotNull(f.propose().getException());assertEquals(0,f.machine.importPublicationStats().holds());assertEquals(0,f.machine.proposalGateStats().gates());f.prepare().get(5,TimeUnit.SECONDS);var fresh=f.propose();assertNull(fresh.getException());f.machine.cancelTransaction(fresh);assertEquals(0,f.machine.importPublicationStats().holds());}
    }
    @Test void freshBarrierAndExactEnvelopeAreRequired()throws Exception{
        try(var f=new Fixture(command->{})){byte[] request=RatisImportPublicationRpc.prepareRequest(f.publication);assertThrows(ExecutionException.class,()->f.machine.query(Message.valueOf(ByteString.copyFrom(request))).get(5,TimeUnit.SECONDS));assertThrows(ExecutionException.class,()->f.machine.query(Message.valueOf(ByteString.copyFrom(RatisImportPublicationRpc.cancelRequest(f.publication)))).get(5,TimeUnit.SECONDS));assertThrows(IllegalArgumentException.class,()->RatisImportPublicationRpc.decodePreparedReply(RatisImportPublicationRpc.cancelRequest(f.publication),ScopedTransactionRpc.IMPORT_PUBLICATION_PREPARE,f.publication));byte[] changed=Arrays.copyOf(request,request.length+1);assertThrows(IllegalArgumentException.class,()->RatisImportPublicationRpc.request(changed));assertEquals(0,f.machine.importPublicationStats().holds());}
    }

    @Test void importAuthorityRequiresFreshBarrierAndReadsPublishedHeadAndDirectoryAtOneCut()throws Exception{
        try(var f=new Fixture(command->{})){
            var definition=RatisRangeImportCatalogueTest.definition(f.catalogue.outer.cluster,f.catalogue.outer.database,f.catalogue.operation(20));var request=RatisRangeImportAuthority.Request.of(definition);var message=Message.valueOf(ByteString.copyFrom(RatisRangeImportAuthority.request(request)));
            assertThrows(ExecutionException.class,()->f.machine.query(message).get(5,TimeUnit.SECONDS));assertThrows(ExecutionException.class,()->f.machine.queryLinearizable(message).get(5,TimeUnit.SECONDS));
            f.prepare().get(5,TimeUnit.SECONDS);f.apply(f.propose());var reply=f.machine.queryLinearizable(message).get(5,TimeUnit.SECONDS);var view=RatisRangeImportAuthority.decodeReply(reply.getContent().toByteArray(),request,f.catalogue.outer.catalogue);assertEquals(f.catalogue.nativeCatalogue.store.applied(),view.publication().completedCatalogueCut());assertArrayEquals(definition.targetDirectory().payloadDigest(),view.directory().payloadDigest());assertEquals(0,f.queries.stats().jobs());
            f.machine.notifyConfigurationChanged(1,f.catalogue.nativeCatalogue.next.get(),RaftConfigurationProto.newBuilder().addPeers(f.peer.getRaftPeerProto()).build());assertThrows(ExecutionException.class,()->f.machine.queryLinearizable(message).get(5,TimeUnit.SECONDS));
        }
    }

}
