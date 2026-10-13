package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import java.lang.reflect.Proxy;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual original native outcomes behind qualified metadata queries; no server, sockets or quorum. */
class RatisTopologyOperationOutcomeTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static Message message(byte[] bytes){return RatisTopologyCdcPollTest.message(bytes);}
    DistributedCommand spanning(RatisTopologyCdcPollTest.Fixture f) {
        return new DistributedCommand(f.groupId,1,f.schema.digest(),new OperationId(f.session,1,1),List.of(
                new Mutation(Mutation.Kind.PUT,f.column,new byte[]{10},new byte[]{7},Mutation.ReturnMode.CURRENT),new Mutation(Mutation.Kind.PUT,f.column,new byte[]{(byte)200},new byte[]{8},Mutation.ReturnMode.CURRENT)));
    }
    RatisTopologyOperationOutcomeRpc.Request request(RatisTopologyCdcPollTest.Fixture f,DistributedCommand command) {
        return new RatisTopologyOperationOutcomeRpc.Request(f.cluster,f.database,f.domain,new DistributedCdcCheckpoint.Lineage(f.rangeId,f.groupId,1),command.operationId(),command.receiptDigest());
    }
    RatisTopologyOperationOutcomeRpc.Reply query(RatisTopologyCdcPollTest.Fixture f,RatisTopologyOperationOutcomeRpc.Request request)throws Exception {
        byte[] bytes=f.machine.queryLinearizable(message(RatisTopologyOperationOutcomeRpc.request(request))).get(5,TimeUnit.SECONDS).getContent().toByteArray();return RatisTopologyOperationOutcomeRpc.decodeReply(bytes,request);
    }
    @Test void wholeSpanningOriginalOutcomeSurvivesFreezeSeedAndNeverWritesChildren()throws Exception {
        try(var f=new RatisTopologyCdcPollTest.Fixture(root)) {
            var original=spanning(f);byte[] before=DistributedCommandCodec.encode(original),hash=original.receiptDigest(),result=f.apply(original);var decoded=CommandResult.decode(result);assertEquals(2,decoded.valueCount());
            var request=request(f,original);assertThrows(ExecutionException.class,()->query(f,request));f.seal(true);
            long childIndex=f.child.applied()==null ? -1 : f.child.applied().index();var reply=query(f,request);
            assertEquals(RatisTopologyOperationOutcomeRpc.Outcome.FOUND,reply.outcome());assertArrayEquals(result,reply.result());assertArrayEquals(f.freeze.encode(),reply.freeze().encode());assertEquals(f.store.applied(),reply.cut());assertEquals(f.store.configuration().index(),reply.configurationIndex());
            assertArrayEquals(before,DistributedCommandCodec.encode(original));assertArrayEquals(hash,original.receiptDigest());assertEquals(childIndex,f.child.applied()==null ? -1 : f.child.applied().index());assertEquals(0,f.store.activeSnapshotPins());
            assertThrows(ExecutionException.class,()->f.machine.query(message(RatisTopologyOperationOutcomeRpc.request(request))).get(5,TimeUnit.SECONDS));assertThrows(ExecutionException.class,()->f.machine.queryStale(message(RatisTopologyOperationOutcomeRpc.request(request)),0).get(5,TimeUnit.SECONDS));
            byte[] wrong=hash.clone();wrong[0]^=1;var mismatched=new RatisTopologyOperationOutcomeRpc.Request(f.cluster,f.database,f.domain,request.lineage(),request.operation(),wrong);assertThrows(ExecutionException.class,()->query(f,mismatched));
            var absent=new RatisTopologyOperationOutcomeRpc.Request(f.cluster,f.database,f.domain,request.lineage(),new OperationId(f.session,1,99),new byte[32]);var missing=query(f,absent);assertEquals(RatisTopologyOperationOutcomeRpc.Outcome.ABSENT,missing.outcome());assertNull(missing.result());
        }
    }
    @Test void acknowledgementAndEpochFencesAreExpiredRatherThanAuthoritativeAbsence()throws Exception {
        for(boolean ack:new boolean[]{false,true})try(var f=new RatisTopologyCdcPollTest.Fixture(root.resolve("expired-"+ack))) {
            var original=spanning(f);f.apply(original);
            if(ack)f.apply(new DistributedCommand(f.groupId,1,f.schema.digest(),new OperationId(f.session,1,2),List.of(),List.of(),1));
            f.seal(true);if(!ack)f.apply(f.clock(1,20+SessionEpochLedger.MAX_LIFETIME_MILLIS));
            var reply=query(f,request(f,original));assertEquals(RatisTopologyOperationOutcomeRpc.Outcome.EXPIRED,reply.outcome());assertNull(reply.result());assertEquals(0,f.store.activeSnapshotPins());
        }
    }
    @Test void retiredMetadataAdmissionAppliesExactOriginalAckAndDrainsItsReceipts()throws Exception {
        try(var f=new RatisTopologyCdcPollTest.Fixture(root)) {
            var original=spanning(f);f.apply(original);f.seal(true);var childPosition=f.child.applied();
            var ack=new DistributedCommand(f.groupId,1,f.schema.digest(),new OperationId(f.session,1,2),List.of(),List.of(),1);
            assertTrue(f.store.metadataTopologyControl(ack));byte[] result=f.apply(ack);assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(result).status());
            assertEquals(RatisTopologyOperationOutcomeRpc.Outcome.EXPIRED,query(f,request(f,original)).outcome());
            assertEquals(RatisTopologyOperationOutcomeRpc.Outcome.FOUND,query(f,request(f,ack)).outcome());assertArrayEquals(result,f.apply(ack));
            assertEquals(childPosition,f.child.applied());assertEquals(0,f.store.activeSnapshotPins());assertEquals(0,f.child.activeSnapshotPins());
            var effects=new DistributedCommand(f.groupId,1,f.schema.digest(),new OperationId(f.session,1,3),original.mutations(),List.of(),2);assertFalse(f.store.metadataTopologyControl(effects));
        }
    }
    @Test void foreignNamespaceLineageConfigurationTermAndLifecycleStayUnavailable()throws Exception {
        try(var f=new RatisTopologyCdcPollTest.Fixture(root)) {
            var original=spanning(f);f.apply(original);f.seal(true);var request=request(f,original);
            for(var foreign:List.of(
                    new RatisTopologyOperationOutcomeRpc.Request(f.cluster,UUID.randomUUID(),f.domain,request.lineage(),request.operation(),request.receiptDigest()),
                    new RatisTopologyOperationOutcomeRpc.Request(f.cluster,f.database,f.domain,new DistributedCdcCheckpoint.Lineage(UUID.randomUUID(),f.groupId,1),request.operation(),request.receiptDigest()),
                    new RatisTopologyOperationOutcomeRpc.Request(f.cluster,f.database,f.domain,new DistributedCdcCheckpoint.Lineage(f.rangeId,f.child.ownership().owner().groupId(),1),request.operation(),request.receiptDigest())))assertThrows(ExecutionException.class,()->query(f,foreign));
            assertThrows(IllegalStateException.class,()->f.child.readTopologyOperationOutcome(f.cluster,f.database,f.domain,new DistributedCdcCheckpoint.Lineage(f.target.ranges().getFirst().rangeId(),f.child.ownership().owner().groupId(),1),request.operation(),request.receiptDigest()));
            var childGroup=RaftGroup.valueOf(RaftGroupId.valueOf(f.child.ownership().owner().groupId()),f.peer);var childQueries=new RatisProofQueryService(RatisProofFixtures.admission());
            var childStorage=RaftStorage.newBuilder().setDirectory(root.resolve("child-raft").toFile()).setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB).build();childStorage.initialize();
            var childMachine=new RatisRangeStateMachine(f.child,childGroup,new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,2),command->{},command->{},f.leader::get);childMachine.proofQueries(childQueries);
            var identity=(RaftServer)Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},(proxy,method,args)->{if(method.getName().equals("getId"))return f.peer.getId();if(method.getName().equals("toString"))return "pending-child-outcome-fixture";throw new AssertionError(method);});
            try {
                childMachine.initialize(identity,childGroup.getGroupId(),childStorage);
                var childRequest=new RatisTopologyOperationOutcomeRpc.Request(f.cluster,f.database,f.domain,new DistributedCdcCheckpoint.Lineage(f.target.ranges().getFirst().rangeId(),childGroup.getGroupId().getUuid(),1),request.operation(),request.receiptDigest());
                assertThrows(ExecutionException.class,()->childMachine.queryLinearizable(message(RatisTopologyOperationOutcomeRpc.request(childRequest))).get(5,TimeUnit.SECONDS));
            } finally {childQueries.stop().get(5,TimeUnit.SECONDS);childMachine.close();childMachine.awaitNativeReaders(java.time.Duration.ofSeconds(5));childStorage.close();}

            f.machine.notifyConfigurationChanged(1,f.index+1,RaftConfigurationProto.newBuilder().addPeers(f.peer.getRaftPeerProto()).build());assertThrows(ExecutionException.class,()->query(f,request));
            f.machine.notifyTermIndexUpdated(1,++f.index);f.leader.set(new RatisRangeStateMachine.Leadership(2,true));assertThrows(ExecutionException.class,()->query(f,request));
            f.machine.pause();assertThrows(ExecutionException.class,()->query(f,request));
        }
    }
    @Test void catalogueRoleAndQueuedOldGenerationCannotProduceAbsence()throws Exception {
        try(var catalogue=new RatisRetentionAdmissionTest.Fixture(root.resolve("catalogue"),RatisRangeStateMachine.RetentionLimits.defaults(),command->{},System::nanoTime,null,machine->machine.role(RatisNodePlan.Role.CATALOGUE))) {
            var request=new RatisTopologyOperationOutcomeRpc.Request(new UUID(0,1),new UUID(0,2),catalogue.groupId,new DistributedCdcCheckpoint.Lineage(new UUID(0,8),catalogue.groupId,1),new OperationId(catalogue.admin,1,0),new byte[32]);
            assertThrows(ExecutionException.class,()->catalogue.machine.queryLinearizable(message(RatisTopologyOperationOutcomeRpc.request(request))).get(5,TimeUnit.SECONDS));
        }
        try(var f=new RatisTopologyCdcPollTest.Fixture(root.resolve("queued"))) {
            var original=spanning(f);f.apply(original);f.seal(true);var entered=new CountDownLatch(1);var release=new CountDownLatch(1);var blockers=new ArrayList<CompletableFuture<Message>>();
            try {
                for(int i=0;i<1;i++)blockers.add(f.sourceQueries.submit(f.groupId,new byte[]{(byte)i},1,false,owned->{entered.countDown();if(!release.await(5,TimeUnit.SECONDS))throw new java.io.IOException("Fixture timeout");return Message.EMPTY;}));
                assertTrue(entered.await(5,TimeUnit.SECONDS));var future=f.machine.queryLinearizable(message(RatisTopologyOperationOutcomeRpc.request(request(f,original))));assertEquals(2,f.sourceQueries.stats().jobs());
                assertTrue(future.cancel(false));assertEquals(2,f.sourceQueries.stats().jobs());f.machine.notifyNotLeader(List.of());f.leader.set(new RatisRangeStateMachine.Leadership(2,false));release.countDown();
                for(var blocker:blockers)blocker.get(5,TimeUnit.SECONDS);f.sourceQueries.stop().get(5,TimeUnit.SECONDS);assertEquals(0,f.sourceQueries.stats().jobs());assertEquals(0,f.sourceQueries.stats().bytes());assertEquals(0,f.store.activeSnapshotPins());
            } finally {release.countDown();}
        }
    }
    @Test void boundedCodecRejectsCrossedRequestsAndMixedOutcomes()throws Exception {
        try(var f=new RatisTopologyCdcPollTest.Fixture(root)) {
            var original=spanning(f);byte[] result=f.apply(original);f.seal(false);var request=request(f,original);var nativeOutcome=f.store.readTopologyOperationOutcome(f.cluster,f.database,f.domain,request.lineage(),request.operation(),request.receiptDigest());
            byte[] bytes=RatisTopologyOperationOutcomeRpc.reply(request,nativeOutcome);assertTrue(bytes.length<=RatisTopologyOperationOutcomeRpc.MAX_REPLY_BYTES);assertArrayEquals(result,RatisTopologyOperationOutcomeRpc.decodeReply(bytes,request).result());
            assertEquals(RatisTopologyOperationOutcomeRpc.REQUEST_BYTES,RatisTopologyOperationOutcomeRpc.request(request).length);assertArrayEquals(RatisTopologyOperationOutcomeRpc.request(request),RatisTopologyOperationOutcomeRpc.request(RatisTopologyOperationOutcomeRpc.decodeRequest(RatisTopologyOperationOutcomeRpc.request(request))));
            var other=new RatisTopologyOperationOutcomeRpc.Request(f.cluster,f.database,f.domain,request.lineage(),new OperationId(f.session,1,2),request.receiptDigest());assertThrows(IllegalArgumentException.class,()->RatisTopologyOperationOutcomeRpc.decodeReply(bytes,other));
            byte[] mixed=bytes.clone();mixed[ScopedTransactionRpc.HEADER_BYTES+32]=(byte)RatisTopologyOperationOutcomeRpc.Outcome.ABSENT.ordinal();assertThrows(IllegalArgumentException.class,()->RatisTopologyOperationOutcomeRpc.decodeReply(mixed,request));
            assertThrows(IllegalArgumentException.class,()->RatisTopologyOperationOutcomeRpc.decodeReply(Arrays.copyOf(bytes,bytes.length-1),request));assertThrows(IllegalArgumentException.class,()->RatisTopologyOperationOutcomeRpc.decodeReply(Arrays.copyOf(bytes,bytes.length+1),request));
            byte[] malformed=RatisTopologyOperationOutcomeRpc.request(request);ByteBuffer.wrap(malformed).putLong(ScopedTransactionRpc.HEADER_BYTES+104,-1);assertThrows(IllegalArgumentException.class,()->RatisTopologyOperationOutcomeRpc.decodeRequest(malformed));
            var reply=query(f,request);
            byte[] maximumResult=new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[CommandResult.MAX_BYTES-12])).encode();
            var maximum=new RatisTopologyOperationOutcomeRpc.Reply(request,RatisTopologyOperationOutcomeRpc.Outcome.FOUND,reply.cut(),reply.freeze(),reply.configurationTerm(),reply.configurationIndex(),reply.configuration(),reply.credit(),maximumResult);
            byte[] maximumWire=RatisTopologyOperationOutcomeRpc.reply(maximum);assertTrue(maximumWire.length<=RatisTopologyOperationOutcomeRpc.MAX_REPLY_BYTES);assertArrayEquals(maximumResult,RatisTopologyOperationOutcomeRpc.decodeReply(maximumWire,request).result());
            assertThrows(IllegalArgumentException.class,()->RatisTopologyOperationOutcomeRpc.decodeReply(new byte[RatisTopologyOperationOutcomeRpc.MAX_REPLY_BYTES+1],request));
            assertThrows(IllegalArgumentException.class,()->new RatisTopologyOperationOutcomeRpc.Reply(request,RatisTopologyOperationOutcomeRpc.Outcome.ABSENT,reply.cut(),reply.freeze(),reply.configurationTerm(),reply.configurationIndex(),reply.configuration(),reply.credit(),result));
            assertThrows(IllegalArgumentException.class,()->new RatisTopologyOperationOutcomeRpc.Reply(request,RatisTopologyOperationOutcomeRpc.Outcome.FOUND,reply.cut(),reply.freeze(),reply.configurationTerm(),reply.configurationIndex(),reply.configuration(),reply.credit(),new CommandResult(CommandResult.Status.SESSION_EXPIRED,List.of()).encode()));
        }
    }
}
