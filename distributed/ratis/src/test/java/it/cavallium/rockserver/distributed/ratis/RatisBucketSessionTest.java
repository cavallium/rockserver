package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import static org.junit.jupiter.api.Assertions.*;

/** Logical collision operations through Ratis interfaces and native stores; no network/quorum acceptance. */
class RatisBucketSessionTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    DistributedSchema schema(boolean value) {
        var o=new RatisSessionEpochsTest(); return new DistributedSchema(o.cluster,o.database,new UUID(6,1),1,0,List.of(
                new DistributedSchema.Column(71,o.data,"bucket",ColumnSchema.of(IntArrayList.of(1),ObjectArrayList.of(ColumnHashType.ALLSAME8),value),null)));
    }
    Keys key(int member) { return new Keys(Buf.wrap(new byte[]{7}),Buf.wrap(new byte[]{(byte)member})); }
    @Test void durableV6LostReplyRetryAfterLaterCollidingUpdateKeepsOriginalMemberReturns() throws Exception {
        var schema=schema(true);
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest(),schema)) {
            Path path=Files.createDirectory(root.resolve("session")); byte[] frozen;
            try (var session=RatisDurableLogicalSession.create(path,schema,f.catalogue.epochs,f.routed,1000,RatisSessionClient.Limits.defaults())) {
                session.advance(); var pending=session.prepare(List.of(
                        RatisLogicalClient.Change.put(71,key(1),Buf.wrap(new byte[]{1}),Mutation.ReturnMode.PREVIOUS),
                        RatisLogicalClient.Change.put(71,key(2),Buf.wrap(new byte[]{2}),Mutation.ReturnMode.CURRENT)),
                        List.of(new RatisLogicalClient.Condition(71,key(1),null)));
                assertTrue(pending.command().hasBucketData()); assertTrue(pending.command().predicates().getFirst().bucket());
                frozen=DistributedCommandCodec.encode(pending.command()); session.ensureBinding(pending);
                f.lost.add(pending.descriptor().groupId()); assertThrows(IOException.class,() -> session.execute(pending));
            }
            long catalogueCommands=f.catalogue.next.get();
            try (var session=RatisDurableLogicalSession.reopen(path,schema,f.catalogue.epochs,f.routed)) {
                var original=session.pending().getFirst(); assertArrayEquals(frozen,DistributedCommandCodec.encode(original.command()));
                var later=session.preparePut(71,key(1),Buf.wrap(new byte[]{9}),Mutation.ReturnMode.PREVIOUS);
                assertArrayEquals(new byte[]{1},session.execute(later).value(0)); session.acknowledge(later);
                int reads=f.catalogue.reads.get(); var result=session.execute(original); assertNull(result.value(0)); assertArrayEquals(new byte[]{2},result.value(1)); session.acknowledge(original);
                assertArrayEquals(new byte[]{9},session.get(71,key(1)).toByteArray()); assertArrayEquals(new byte[]{2},session.get(71,key(2)).toByteArray()); assertNull(session.get(71,key(3)));
                assertEquals(reads,f.catalogue.reads.get()); assertEquals(catalogueCommands,f.catalogue.next.get());
                var deletion=session.prepareDelete(71,key(2),Mutation.ReturnMode.PREVIOUS); assertEquals(1,deletion.command().acknowledgedThrough());
                assertArrayEquals(new byte[]{2},session.execute(deletion).value(0)); session.acknowledge(deletion); assertNull(session.get(71,key(2)));
                for (var store:f.stores.values()) assertEquals(0,store.activeSnapshotPins());
            }
        }
    }
    @Test void scopedRpcStagingPreservesCollisionMembersAndTheAcceptedLogicalResult() throws Exception {
        var schema=schema(true); var column=schema.column(71).columnId();
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest(),schema)) {
            var first=schema.encodeLogicalKey(column,key(1)); var second=schema.encodeLogicalKey(column,key(2));
            var mutations=List.of(Mutation.bucketPut(column,first.physicalKey(),first.variableKeys(),new byte[]{1},Mutation.ReturnMode.PREVIOUS),
                    Mutation.bucketPut(column,second.physicalKey(),second.variableKeys(),new byte[]{2},Mutation.ReturnMode.CURRENT));
            var scope=new TransactionScope(List.of(TransactionScope.Span.point(column,first.physicalKey())));
            var client=new RatisSessionClient(f.routed,f.ledger,f.ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());
            try (var tx=f.routed.openTransaction(scope,java.time.Duration.ofSeconds(1))) {
                var preview=tx.stage(mutations); assertNull(preview.value(0)); assertArrayEquals(new byte[]{2},preview.value(1));
                var bucket=schema.decodeBucket(column,first.physicalKey(),tx.get(column,first.physicalKey()));
                assertArrayEquals(new byte[]{1},bucket.get(first.variableKeys())); assertArrayEquals(new byte[]{2},bucket.get(second.variableKeys()));
                var intent=client.prepareTransactionIntent(mutations);
                f.routed.execute(client.prepareBinding(f.catalogue.epochs,new OperationId(f.outer.maintenance,1,100),intent,-1));
                var accepted=tx.plan(intent.command()); assertArrayEquals(preview.encode(),accepted.acceptance().result());
                client.acceptTransaction(intent,accepted); assertArrayEquals(preview.encode(),client.execute(intent).encode()); client.acknowledge(intent);
            }
            for (var store:f.stores.values()) assertEquals(0,store.activeSnapshotPins());
        }
    }
    @Test void keyOnlyLogicalMembersDistinguishAbsenceAndEmptyPresence() throws Exception {
        var schema=schema(false);
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest(),schema)) {
            var client=new RatisLogicalClient(schema,f.routed,f.ledger,f.ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());
            var pending=client.preparePut(71,key(1),null,Mutation.ReturnMode.CURRENT); assertTrue(pending.command().hasBucketData());
            f.routed.execute(client.prepareBinding(f.catalogue.epochs,new OperationId(f.outer.maintenance,1,100),pending,-1));
            assertArrayEquals(new byte[0],client.execute(pending).value(0)); client.acknowledge(pending);
            assertTrue(client.get(71,key(1)).isEmpty()); assertNull(client.get(71,key(2)));
            var remove=client.prepareDelete(71,key(1),Mutation.ReturnMode.PREVIOUS); assertArrayEquals(new byte[0],client.execute(remove).value(0)); client.acknowledge(remove);
            assertNull(client.get(71,key(1))); assertThrows(IllegalArgumentException.class,() -> client.preparePut(71,key(2),Buf.wrap(new byte[]{1}),Mutation.ReturnMode.NONE));
        }
    }
    @Test void logicalMemberBytesCountTowardCommandBudgetBeforeRpcOrSequenceAllocation() throws Exception {
        var schema=schema(true);
        try (var f=new RatisSessionPropagationTest.Fixture(root,true,schema.digest(),schema)) {
            var client=new RatisLogicalClient(schema,f.routed,f.ledger,f.ledger.grants().getFirst(),RatisSessionClient.Limits.defaults());
            var keys=new Keys(Buf.wrap(new byte[]{7}),Buf.wrap(new byte[65535])); var changes=new ArrayList<RatisLogicalClient.Change>();
            for (int i=0;i<33;i++) changes.add(RatisLogicalClient.Change.put(71,keys,Buf.wrap(new byte[0]),Mutation.ReturnMode.NONE));
            assertThrows(IllegalArgumentException.class,() -> client.prepare(changes,List.of())); assertEquals(0,client.outstanding()); assertEquals(0,f.submissions);
            assertEquals(0,client.preparePut(71,key(1),Buf.wrap(new byte[]{1}),Mutation.ReturnMode.NONE).operationId().sequence());
        }
    }

}
