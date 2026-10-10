package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Native/Ratis-interface fixtures only; no server or network execution. */
class RatisCdcPollTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final UUID CLUSTER=new UUID(1,1),DATABASE=new UUID(1,2),DOMAIN=new UUID(1,3),RANGE=new UUID(1,4),COLUMN=new UUID(1,5);
    static Message message(byte[] bytes) { return Message.valueOf(ByteString.copyFrom(bytes)); }
    RatisRetentionAdmissionTest.Fixture fixture(String name) throws Exception {
        return new RatisRetentionAdmissionTest.Fixture(root.resolve(name),RatisRangeStateMachine.RetentionLimits.defaults(),command->{},System::nanoTime,
                new RangeOwnership.Interval(DOMAIN,Set.of(COLUMN),null,null),machine->machine.cdcBinding(CLUSTER,DATABASE,RANGE));
    }
    RatisCdcPollRpc.Request request(UUID group,int bytes) {
        return new RatisCdcPollRpc.Request(new RatisCdcPollRpc.Binding(CLUSTER,DATABASE,DOMAIN,new DistributedCdcCheckpoint.Lineage(RANGE,group,1)),DistributedCdcCheckpoint.Cursor.beginning(),256,bytes);
    }

    @Test void freshPollReturnsExactBoundedNativeEnvelopeAndRejectsStaleOrForeignReads() throws Exception {
        try(var f=fixture("fresh")) {
            var command=f.put(0); f.apply(f.propose(command),1);
            var record=new DistributedCdcRecord(request(f.groupId,4096).binding().lineage(),new ApplyPosition(1,1),command,null);
            int exactBytes=RatisCdcPollRpc.REPLY_BASE_BYTES+4+record.encodedSize(); var request=request(f.groupId,exactBytes); var wire=message(RatisCdcPollRpc.request(request));
            assertThrows(ExecutionException.class,()->f.machine.query(wire).get(5,TimeUnit.SECONDS));
            assertThrows(ExecutionException.class,()->f.machine.queryStale(wire,0).get(5,TimeUnit.SECONDS));
            byte[] encoded=f.machine.queryLinearizable(wire).get(5,TimeUnit.SECONDS).getContent().toByteArray();
            assertEquals(exactBytes,encoded.length); var reply=RatisCdcPollRpc.decodeReply(encoded,request);
            assertEquals(new ApplyPosition(1,1),reply.cut()); assertEquals(1,reply.records().size()); assertEquals(command.mutations(),reply.records().getFirst().command().mutations());
            var tiny=request(f.groupId,exactBytes-1);
            var large=RatisCdcPollRpc.decodeReply(f.machine.queryLinearizable(message(RatisCdcPollRpc.request(tiny))).get(5,TimeUnit.SECONDS).getContent().toByteArray(),tiny);
            assertEquals(RatisCdcPollRpc.Outcome.TOO_LARGE,large.outcome()); assertEquals(exactBytes,large.requiredBytes()); assertEquals(tiny.from(),large.next());
            var foreign=new RatisCdcPollRpc.Request(new RatisCdcPollRpc.Binding(CLUSTER,UUID.randomUUID(),DOMAIN,request.binding().lineage()),request.from(),request.maxRecords(),request.maxBytes());
            assertThrows(ExecutionException.class,()->f.machine.queryLinearizable(message(RatisCdcPollRpc.request(foreign))).get(5,TimeUnit.SECONDS));
            assertEquals(new ApplyPosition(1,1),f.store.applied()); assertEquals(0,f.proofQueries.stats().jobs());
        }
    }

    @Test void queuedPollCannotBorrowNewLeadershipAndCancellationRetainsActualAdmission() throws Exception {
        for(boolean cancel:new boolean[]{false,true}) try(var f=fixture("queued-"+cancel)) {
            var entered=new CountDownLatch(4); var release=new CountDownLatch(1); var blockers=new ArrayList<CompletableFuture<Message>>();
            try {
                for(int i=0;i<4;i++) blockers.add(f.proofQueries.submit(f.groupId,new byte[]{(byte)i},64,false,owned->{
                    entered.countDown(); if(!release.await(10,TimeUnit.SECONDS)) throw new IOException("CDC fixture timed out"); return message(new byte[0]);
                }));
                assertTrue(entered.await(5,TimeUnit.SECONDS));
                var future=f.machine.queryLinearizable(message(RatisCdcPollRpc.request(request(f.groupId,4096)))); assertEquals(5,f.proofQueries.stats().jobs());
                if(cancel) { assertTrue(future.cancel(true)); assertEquals(5,f.proofQueries.stats().jobs()); }
                f.leadership.set(new RatisRangeStateMachine.Leadership(2,true)); f.machine.notifyTermIndexUpdated(2,1); release.countDown();
                if(!cancel) assertThrows(ExecutionException.class,()->future.get(5,TimeUnit.SECONDS));
                for(var blocker:blockers) blocker.get(5,TimeUnit.SECONDS);
                f.proofQueries.stop().get(5,TimeUnit.SECONDS); assertEquals(0,f.proofQueries.stats().jobs()); assertEquals(0,f.proofQueries.stats().bytes());
            } finally { release.countDown(); }
        }
    }

    @Test void nativeRetentionGapReturnsARequestBoundOutcomeWithoutAdvancingTheCaller() throws Exception {
        try(var f=fixture("gap")) {
            for(int i=0;i<4096;i++) f.apply(f.propose(f.put(i)),i+1L);
            f.apply(f.propose(f.ack(10000,4095)),4097);
            f.apply(f.propose(f.put(4096)),4098);
            var request=request(f.groupId,4096);
            byte[] wire=f.machine.queryLinearizable(message(RatisCdcPollRpc.request(request))).get(5,TimeUnit.SECONDS).getContent().toByteArray();
            var result=RatisCdcPollRpc.decodeReply(wire,request);
            assertEquals(RatisCdcPollRpc.Outcome.GAP,result.outcome()); assertTrue(result.records().isEmpty());
            assertEquals(DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,1)),result.floor());
            assertEquals(request.from(),result.next()); assertEquals(new ApplyPosition(1,4098),result.cut());
            assertEquals(RatisCdcPollRpc.REPLY_BASE_BYTES,wire.length); assertEquals(0,f.proofQueries.stats().jobs());
        }
    }

    @Test void wireBindingPreservesPartialOrdinalAndRejectsCrossRequestAndMalformedReplies() {
        UUID group=new UUID(2,1); var binding=request(group,4096).binding();
        var command=new DistributedCommand(group,1,new byte[32],new OperationId(new UUID(2,2),1,0),List.of(
                new Mutation(Mutation.Kind.MERGE,COLUMN,new byte[]{1},new byte[]{2}),new Mutation(Mutation.Kind.DELETE_RANGE,COLUMN,new byte[]{3},new byte[]{5})));
        var record=new DistributedCdcRecord(binding.lineage(),new ApplyPosition(1,1),command,null);
        var partial=new RatisCdcPollRpc.Request(binding,new DistributedCdcCheckpoint.Cursor(new ApplyPosition(1,1),0),2,4096);
        assertEquals(partial,RatisCdcPollRpc.decodeRequest(RatisCdcPollRpc.request(partial)));
        var result=new RatisCdcPollRpc.Result(partial,RatisCdcPollRpc.Outcome.DATA,new ApplyPosition(2,3),DistributedCdcCheckpoint.Cursor.beginning(),DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(2,3)),0,List.of(record));
        byte[] encoded=RatisCdcPollRpc.reply(result); var decoded=RatisCdcPollRpc.decodeReply(encoded,partial);
        assertEquals(command.mutations(),decoded.records().getFirst().command().mutations()); assertEquals(partial.from(),decoded.request().from());
        var different=new RatisCdcPollRpc.Request(binding,DistributedCdcCheckpoint.Cursor.beginning(),2,4096);
        assertThrows(IllegalArgumentException.class,()->RatisCdcPollRpc.decodeReply(encoded,different));
        byte[] count=encoded.clone(); ByteBuffer.wrap(count).putInt(ScopedTransactionRpc.HEADER_BYTES+93,Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class,()->RatisCdcPollRpc.decodeReply(count,partial));
        byte[] size=encoded.clone(); ByteBuffer.wrap(size).putInt(RatisCdcPollRpc.REPLY_BASE_BYTES,Integer.MAX_VALUE);
        assertThrows(IllegalArgumentException.class,()->RatisCdcPollRpc.decodeReply(size,partial));
        assertThrows(IllegalArgumentException.class,()->RatisCdcPollRpc.decodeReply(Arrays.copyOf(encoded,encoded.length-1),partial));
        assertThrows(IllegalArgumentException.class,()->new RatisCdcPollRpc.Result(partial,RatisCdcPollRpc.Outcome.DATA,new ApplyPosition(2,0),result.floor(),result.next(),0,List.of(record)));
        var gap=new RatisCdcPollRpc.Result(different,RatisCdcPollRpc.Outcome.GAP,new ApplyPosition(2,3),DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,1)),different.from(),0,List.of());
        var decodedGap=RatisCdcPollRpc.decodeReply(RatisCdcPollRpc.reply(gap),different);
        assertEquals(RatisCdcPollRpc.Outcome.GAP,decodedGap.outcome()); assertEquals(gap.floor(),decodedGap.floor()); assertEquals(different.from(),decodedGap.next());
        assertThrows(IllegalArgumentException.class,()->new RatisCdcPollRpc.Result(different,RatisCdcPollRpc.Outcome.GAP,gap.cut(),DistributedCdcCheckpoint.Cursor.beginning(),gap.next(),0,List.of()));
    }

    @Test void routedPollUsesFreshFrozenCursorAndDoesNotRebaseForeignLineage() throws Exception {
        UUID group=new UUID(2,1); var lineage=request(group,4096).binding().lineage();
        var replica=new ReplicaDescriptor(new UUID(3,1),new UUID(3,2),new UUID(3,3),"local","127.0.0.1:9999",ReplicaDescriptor.Role.VOTER);
        var descriptor=new RangeDescriptor(RANGE,group,1,1,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(replica));
        var directory=new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,new byte[32],1,Set.of(COLUMN),0,List.of(descriptor));
        var requests=new ArrayList<byte[]>(); var lost=new java.util.concurrent.atomic.AtomicBoolean(true);
        var transport=new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor range,UUID column,byte[] key) { throw new AssertionError(); }
            public CommandResult command(RangeDescriptor range,DistributedCommand command) { throw new AssertionError(); }
            public byte[] transaction(RangeDescriptor range,byte[] encoded,boolean fresh) throws IOException {
                assertTrue(fresh); assertEquals(descriptor,range); requests.add(encoded.clone());
                if(lost.getAndSet(false)) throw new IOException("Lost read reply");
                var request=RatisCdcPollRpc.decodeRequest(encoded);
                return RatisCdcPollRpc.reply(new RatisCdcPollRpc.Result(request,RatisCdcPollRpc.Outcome.DATA,new ApplyPosition(1,0),DistributedCdcCheckpoint.Cursor.beginning(),DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(1,0)),0,List.of()));
            }
        };
        try(var client=new RatisRoutedClient(directory,transport)) {
            assertThrows(IOException.class,()->client.pollCdc(lineage,DistributedCdcCheckpoint.Cursor.beginning(),1,4096));
            var result=client.pollCdc(lineage,DistributedCdcCheckpoint.Cursor.beginning(),1,4096); assertTrue(result.records().isEmpty());
            assertArrayEquals(requests.getFirst(),requests.getLast());
            assertThrows(IllegalArgumentException.class,()->client.pollCdc(new DistributedCdcCheckpoint.Lineage(RANGE,group,2),DistributedCdcCheckpoint.Cursor.beginning(),1,4096));
            assertEquals(2,requests.size());
        }
    }
}
