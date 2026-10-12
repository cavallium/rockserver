package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
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
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Real sealed source ABSENT and durable HOME reservation; no server/network or accepted-plan authority claim. */
class RatisHomeReservationTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static UUID id(long n){return new UUID(0,n);}
    static Message message(byte[] bytes){return RatisTopologyCdcPollTest.message(bytes);}
    final class Fixture implements AutoCloseable {
        final RatisTopologyCdcPollTest.Fixture source;
        final UUID groupId=id(80),column=id(81),domain=id(82),controlSession=id(88);
        final RaftPeer peer=RaftPeer.newBuilder().setId("home").setAddress("127.0.0.1:9997").build();
        final RaftGroup group=RaftGroup.valueOf(RaftGroupId.valueOf(groupId),peer);
        final DistributedStoragePool pool;final ReplicatedRangeStore store;final RaftStorage storage;final RatisRangeStateMachine machine;
        final RatisProofQueryService queries=RatisProofQueryService.operationRerouteLane();final AtomicReference<RatisRangeStateMachine.Leadership> leader=new AtomicReference<>(new RatisRangeStateMachine.Leadership(1,true));
        final AtomicInteger sourceReads=new AtomicInteger();volatile Runnable beforeSource=()->{};
        final OperationRerouteOrigin origin;final TransactionStatus reserved;final DistributedCommand reservation;long index;
        Fixture(Path path,boolean configured)throws Exception{
            source=new RatisTopologyCdcPollTest.Fixture(path.resolve("source"));source.seal(true);
            source.ledger.set(source.ledger.get().issue(controlSession,SessionEpochLedger.MAX_LIFETIME_MILLIS+10));
            pool=DistributedStoragePool.open(path.resolve("home-native"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
            store=pool.openRange(new RangeOwnership.Owner(groupId,1),new RangeOwnership.Interval(domain,Set.of(column),null,null),new byte[32],Map.of(column,"home"),Set.of());
            var replica=new ReplicaDescriptor(id(84),id(85),id(86),peer.getId().toString(),peer.getAddress(),ReplicaDescriptor.Role.VOTER);
            var directory=new RangeDirectory(source.cluster,source.database,domain,1,new byte[32],1,Set.of(column),0,List.of(new RangeDescriptor(id(83),groupId,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica))));
            var io=(BlockingApi)Proxy.newProxyInstance(BlockingApi.class.getClassLoader(),new Class<?>[]{BlockingApi.class},(proxy,method,args)->{
                if(!method.getName().equals("sendReadOnly"))throw new AssertionError(method);byte[] bytes=source.ledger.get().encode();return new RaftClientReply.Builder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(RaftGroupId.valueOf(source.catalogue)).setCallId(1).setSuccess(true).setMessage(message(ScopedTransactionRpc.frame(ScopedTransactionRpc.POINT,ByteBuffer.allocate(4+bytes.length).putInt(bytes.length).put(bytes).array()))).build();
            });
            var client=(RaftClient)Proxy.newProxyInstance(RaftClient.class.getClassLoader(),new Class<?>[]{RaftClient.class},(proxy,method,args)->{if(method.getName().equals("getGroupId"))return RaftGroupId.valueOf(source.catalogue);if(method.getName().equals("io"))return io;throw new AssertionError(method);});
            var epochs=new RatisSessionEpochs(client,source.cluster,source.database,source.catalogue,1,new byte[32],id(21));
            machine=RatisRangeStateMachine.withSessions(store,group,new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,2),RatisTransactionStatuses.validator(source.cluster,source.database,groupId,1,new byte[32],column),epochs,directory,RatisRangeStateMachine.RetentionLimits.defaults(),leader::get);
            machine.role(RatisNodePlan.Role.TRANSACTION_HOME,(key,value)->RatisTransactionStatuses.validateRecord(source.cluster,source.database,key,value));machine.homeRecoveryReservations(source.cluster,source.database,column);machine.proofQueries(queries);machine.operationRerouteQueries(queries);
            if(configured)machine.operationRerouteAdmission(command->RatisTransactionStatuses.admitReservation(command,entry->{sourceReads.incrementAndGet();beforeSource.run();var request=new RatisTopologyOperationOutcomeRpc.Request(entry.clusterId(),entry.databaseId(),entry.domainId(),entry.source(),entry.operation(),entry.originalReceiptDigest());try{return RatisTopologyOperationOutcomeRpc.decodeReply(source.machine.queryLinearizable(message(RatisTopologyOperationOutcomeRpc.request(request))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),request);}catch(InterruptedException interrupted){Thread.currentThread().interrupt();throw new IOException(interrupted);}catch(ExecutionException|TimeoutException failure){throw new IOException(failure);}}),null);
            storage=RaftStorage.newBuilder().setDirectory(path.resolve("home-raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();storage.initialize();
            var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)->{if(method.getName().equals("getId"))return peer.getId();if(method.getName().equals("toString"))return "home-reservation-fixture";throw new AssertionError(method);});
            machine.initialize(identity,group.getGroupId(),storage);machine.notifyTermIndexUpdated(1,0);
            for(var grant:source.ledger.get().grants())apply(DistributedCommand.sessionControl(groupId,1,new byte[32],new OperationId(controlSession,2,400+grant.epoch()),RangeSessionControl.bind(source.ledger.get(),grant,4096),-1));
            var original=new DistributedCommand(source.groupId,1,source.schema.digest(),new OperationId(source.session,1,1),List.of(new Mutation(Mutation.Kind.PUT,source.column,new byte[]{10},new byte[]{7}),new Mutation(Mutation.Kind.PUT,source.column,new byte[]{(byte)200},new byte[]{8})));
            origin=new OperationRerouteOrigin(source.cluster,source.database,source.domain,new DistributedCdcCheckpoint.Lineage(source.rangeId,source.groupId,1),original.operationId(),original.receiptDigest(),OperatorUpload.sha256(source.freeze.encode()),source.store.applied(),source.target.payloadDigest(),OperatorUpload.sha256(DistributedCommandCodec.encode(original)),2,new OperationId(controlSession,2,0));
            reserved=TransactionStatus.reserve(origin,source.schema.schemaEpoch(),source.schema.digest());reservation=command(origin.firstControl(),null,reserved);
        }
        DistributedCommand command(OperationId control,TransactionStatus old,TransactionStatus next){byte[] key=ByteBuffer.allocate(16).putLong(next.transactionId().getMostSignificantBits()).putLong(next.transactionId().getLeastSignificantBits()).array();return new DistributedCommand(groupId,1,new byte[32],control,List.of(new Mutation(Mutation.Kind.PUT,column,key,next.encode())),List.of(new KeyPredicate(column,key,old==null?null:old.encode())));}
        TransactionContext propose(DistributedCommand command)throws IOException{return machine.startTransaction(RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId()).setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(message(DistributedCommandCodec.encode(command))).build());}
        byte[] apply(DistributedCommand command)throws Exception{var tx=propose(command);assertNull(tx.getException());machine.preAppendTransaction(tx);tx.initLogEntry(leader.get().term(),++index);return machine.applyTransaction(tx).get(5,TimeUnit.SECONDS).getContent().toByteArray();}
        CompletableFuture<Message> prepare(DistributedCommand command){return machine.queryLinearizable(message(RatisOperationRerouteAdmissionRpc.prepareRequest(command)));}
        TransactionStatus read()throws Exception{return TransactionStatus.decode(store.readCurrentPoint(column,reservation.mutations().getFirst().key()));}
        @Override public void close()throws Exception{queries.stop().get(5,TimeUnit.SECONDS);machine.close();machine.awaitNativeReaders(java.time.Duration.ofSeconds(5));storage.close();pool.close();source.close();}
    }
    @Test void reservationNeedsFreshAbsentTicketAndOriginalReceiptRetrySkipsSource()throws Exception{
        try(var f=new Fixture(root,true)){
            assertNotNull(f.propose(f.reservation).getException());assertThrows(ExecutionException.class,()->f.machine.query(message(RatisOperationRerouteAdmissionRpc.prepareRequest(f.reservation))).get(5,TimeUnit.SECONDS));
            RatisOperationRerouteAdmissionRpc.requireEcho(f.prepare(f.reservation).get(5,TimeUnit.SECONDS).getContent().toByteArray(),RatisOperationRerouteAdmissionRpc.PREPARE,f.reservation);
            byte[] result=f.apply(f.reservation);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(result).status());assertTrue(f.read().reserved());assertEquals(f.origin,f.read().origin());assertEquals(1,f.sourceReads.get());
            f.beforeSource=()->{throw new AssertionError("Saved reservation must not query source again");};f.prepare(f.reservation).get(5,TimeUnit.SECONDS);assertArrayEquals(result,f.apply(f.reservation));assertEquals(1,f.sourceReads.get());
            assertTrue(f.source.child.topologyBirthPending());assertEquals(0,f.source.store.activeSnapshotPins());
        }
    }
    @Test void unconfiguredReservationAndHashOnlyPromotionStayClosed()throws Exception{
        try(var f=new Fixture(root.resolve("closed"),false)){assertThrows(ExecutionException.class,()->f.prepare(f.reservation).get(5,TimeUnit.SECONDS));assertNotNull(f.propose(f.reservation).getException());}
        try(var f=new Fixture(root.resolve("promotion"),true)){
            f.prepare(f.reservation).get(5,TimeUnit.SECONDS);f.apply(f.reservation);var participants=new ArrayList<TransactionStatus.Participant>();
            for(var target:f.source.definition.targets())participants.add(new TransactionStatus.Participant(target.rangeId(),target.groupId(),target.rangeEpoch(),f.source.schema.digest(),new byte[32],null,false));
            var accepted=TransactionStatus.begin(f.origin.clusterId(),f.origin.databaseId(),f.origin.domainId(),f.origin.transactionId(),f.origin.operation(),f.reserved.schemaEpoch(),f.reserved.schemaDigest(),new byte[32],participants);
            var promotion=f.command(new OperationId(f.origin.firstControl().sessionId(),f.origin.firstControl().epoch(),128),f.reserved,f.reserved.promote(accepted));
            assertThrows(ExecutionException.class,()->f.prepare(promotion).get(5,TimeUnit.SECONDS));assertNotNull(f.propose(promotion).getException());assertEquals(f.reserved,f.read());
        }
    }
    @Test void queuedCancelledAdmissionAndConfigurationLossRetainActualOwnershipUntilReturn()throws Exception{
        try(var f=new Fixture(root,true)){
            var entered=new CountDownLatch(1);var release=new CountDownLatch(1);f.beforeSource=()->{entered.countDown();try{if(!release.await(5,TimeUnit.SECONDS))throw new AssertionError("Fixture timeout");}catch(InterruptedException interrupted){throw new AssertionError(interrupted);}};
            try{var future=f.prepare(f.reservation);assertTrue(entered.await(5,TimeUnit.SECONDS));assertTrue(future.cancel(false));assertEquals(1,f.queries.stats().jobs());
                f.machine.queryLinearizable(message(RatisOperationRerouteAdmissionRpc.cancelRequest(f.reservation))).get(5,TimeUnit.SECONDS);assertNotNull(f.propose(f.reservation).getException());assertEquals(1,f.queries.stats().jobs());release.countDown();f.queries.stop().get(5,TimeUnit.SECONDS);assertEquals(0,f.queries.stats().jobs());
            }finally{release.countDown();}
        }
        try(var f=new Fixture(root.resolve("configuration"),true)){
            f.prepare(f.reservation).get(5,TimeUnit.SECONDS);var tx=f.propose(f.reservation);assertNull(tx.getException());f.machine.notifyConfigurationChanged(1,f.index+1,RaftConfigurationProto.newBuilder().addPeers(f.peer.getRaftPeerProto()).build());
            assertThrows(org.apache.ratis.protocol.exceptions.StateMachineException.class,()->f.machine.preAppendTransaction(tx));assertNotNull(f.propose(f.reservation).getException());
        }
    }
}
