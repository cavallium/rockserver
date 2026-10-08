package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Durable phase ordering with controlled transport; no native/Ratis transaction acceptance claim. */
class RatisAcceptedSessionClientTest {
    private static final UUID CLUSTER=new UUID(0,1), DATABASE=new UUID(0,2), DOMAIN=new UUID(0,3), COLUMN=new UUID(0,4), GROUP=new UUID(0,5), SESSION=new UUID(0,6);
    private static final SessionEpochLedger ISSUANCE=SessionEpochLedger.initial(CLUSTER,DATABASE,10).issue(SESSION,100);
    private static final SessionEpochLedger.Grant GRANT=ISSUANCE.grants().getFirst();
    private static final RatisSessionClient.Limits LIMITS=RatisSessionClient.Limits.defaults();
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    private static RangeDirectory directory(long generation,String address) {
        var peer=new ReplicaDescriptor(new UUID(1,1),new UUID(1,2),new UUID(1,3),"peer",address,ReplicaDescriptor.Role.VOTER);
        var range=new RangeDescriptor(new UUID(1,4),GROUP,1,generation,null,null,RangeDescriptor.TopologyState.ACTIVE,List.of(peer));
        return new RangeDirectory(CLUSTER,DATABASE,DOMAIN,1,new byte[32],generation,Set.of(COLUMN),0,List.of(range));
    }
    private static Mutation put(int key) { return new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {(byte)key},new byte[] {(byte)(key+1)}); }
    private static DistributedCommand accepted(DistributedCommand base) {
        var values=new ArrayList<byte[]>(); for (var mutation:base.mutations()) if (mutation.returnMode()!=Mutation.ReturnMode.NONE) values.add(new byte[] {42});
        var result=new CommandResult(CommandResult.Status.SUCCESS,values).encode();
        return DistributedCommand.acceptedTransaction(base,new TransactionAcceptance(new ApplyPosition(1,7),3,new UUID(2,1),
                new TransactionScope(List.of(TransactionScope.Span.range(COLUMN,null,null))),result));
    }
    private static final class Controlled implements RatisRoutedClient.Transport {
        final AtomicInteger calls=new AtomicInteger(), closes=new AtomicInteger(); final Map<OperationId,byte[]> received=new HashMap<>();
        Path journal; boolean lose=true;
        public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
        public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException {
            calls.incrementAndGet(); byte[] wire=DistributedCommandCodec.encode(command);
            if (command.acceptance()!=null) {
                byte[] body=SessionJournalFiles.read(journal,SessionWindowJournal.MAX_BYTES); var input=ByteBuffer.wrap(body);
                assertEquals(2,input.getInt(4)); int size=input.getInt(121); assertArrayEquals(wire,Arrays.copyOfRange(body,125,125+size),"Accepted bytes must be checkpointed before transport execution");
            }
            byte[] first=received.putIfAbsent(command.operationId(),wire); if (first!=null) assertArrayEquals(first,wire);
            if (lose && command.acceptance()!=null) { lose=false; throw new IOException("Lost applied reply"); }
            return command.acceptance()==null ? new CommandResult(CommandResult.Status.SUCCESS,List.of()) : CommandResult.decode(command.acceptance().result());
        }
        public void close() { closes.incrementAndGet(); }
    }

    @Test void acceptedBytesAreDurableBeforeRpcAndLostReplyReopensExactV7AndOriginalRoute() throws Exception {
        Path path=root.resolve("accepted.bin"); var transport=new Controlled(); transport.journal=path; byte[] frozen;
        try (var routed=new RatisRoutedClient(directory(0,"127.0.0.1:9001"),transport)) {
            try (var client=RatisDurableSessionClient.create(path,routed,ISSUANCE,GRANT,LIMITS)) {
                var intent=client.prepareTransactionIntent(List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {1},new byte[] {2},Mutation.ReturnMode.PREVIOUS)));
                assertTrue(intent.awaitingAcceptance()); assertThrows(IllegalStateException.class,() -> client.execute(intent)); assertThrows(IllegalStateException.class,() -> client.acknowledge(intent)); assertEquals(0,transport.calls.get());
                routed.refresh(directory(1,"127.0.0.1:9011")); var accepted=accepted(intent.command()); frozen=DistributedCommandCodec.encode(accepted);
                client.acceptTransaction(intent,accepted); assertFalse(intent.awaitingAcceptance()); assertEquals("127.0.0.1:9001",intent.descriptor().replicas().getFirst().address());
                assertThrows(IllegalStateException.class,() -> client.acceptTransaction(intent,accepted)); assertThrows(IOException.class,() -> client.execute(intent)); assertNull(intent.savedResult());
            }
            try (var client=RatisDurableSessionClient.reopen(path,routed)) {
                var pending=client.pending().getFirst(); assertFalse(pending.awaitingAcceptance()); assertArrayEquals(frozen,DistributedCommandCodec.encode(pending.command()));
                assertEquals("127.0.0.1:9001",pending.descriptor().replicas().getFirst().address()); assertArrayEquals(pending.command().acceptance().result(),client.execute(pending).encode());
                client.acknowledge(pending); assertEquals(0,client.acknowledgedThrough());
                var next=client.prepare(List.of(put(2))); assertEquals(1,next.command().operationId().sequence()); assertEquals(0,next.command().acknowledgedThrough());
            }
            assertEquals(2,transport.calls.get()); assertEquals(0,transport.closes.get());
        }
    }

    @Test void recoveredIntentCannotExecuteOrConsumeWhileOrdinaryWindowEntriesRemainOrdinary() throws Exception {
        Path path=root.resolve("intent.bin"); var transport=new Controlled(); transport.journal=path;
        try (var routed=new RatisRoutedClient(directory(0,"127.0.0.1:9001"),transport)) {
            byte[] original;
            try (var client=RatisDurableSessionClient.create(path,routed,ISSUANCE,GRANT,LIMITS)) {
                var intent=client.prepareTransactionIntent(List.of(put(1))); original=DistributedCommandCodec.encode(intent.command());
                var ordinary=client.prepare(List.of(put(2))); client.execute(ordinary); client.acknowledge(ordinary); assertEquals(-1,client.acknowledgedThrough());
            }
            try (var client=RatisDurableSessionClient.reopen(path,routed)) {
                var intent=client.pending().getFirst(); assertTrue(intent.awaitingAcceptance()); assertArrayEquals(original,DistributedCommandCodec.encode(intent.command()));
                int calls=transport.calls.get(); assertThrows(IllegalStateException.class,() -> client.execute(intent)); assertThrows(IllegalStateException.class,() -> client.acknowledge(intent)); assertEquals(calls,transport.calls.get());
                assertThrows(IllegalArgumentException.class,() -> client.acceptTransaction(intent,intent.command()));
                var ordinary=client.pending().getLast(); assertFalse(ordinary.awaitingAcceptance()); assertTrue(ordinary.consumed()); assertNotNull(ordinary.savedResult());
            }
        }
    }

    @Test void forgedIdentityOrReorderedOperandsCannotReplaceIntentAndQuotaFailureRollsBack() throws Exception {
        Path path=root.resolve("forged.bin"); var transport=new Controlled(); transport.journal=path;
        try (var routed=new RatisRoutedClient(directory(0,"127.0.0.1:9001"),transport);
                var client=RatisDurableSessionClient.create(path,routed,ISSUANCE,GRANT,LIMITS)) {
            var intent=client.prepareTransactionIntent(List.of(put(1),put(2)),List.of(new KeyPredicate(COLUMN,new byte[] {3},null)));
            var base=intent.command(); byte[] original=Files.readAllBytes(path);
            var foreign=new DistributedCommand(base.groupId(),base.rangeEpoch(),base.schemaDigest(),new OperationId(new UUID(9,9),GRANT.epoch(),base.operationId().sequence()),base.mutations(),base.predicates());
            var reordered=new DistributedCommand(base.groupId(),base.rangeEpoch(),base.schemaDigest(),base.operationId(),List.of(put(2),put(1)),base.predicates());
            for (var forged:List.of(accepted(foreign),accepted(reordered))) assertThrows(IllegalArgumentException.class,() -> client.acceptTransaction(intent,forged));
            assertTrue(intent.awaitingAcceptance()); assertArrayEquals(original,Files.readAllBytes(path)); assertEquals(0,transport.calls.get());
        }
        Path limited=root.resolve("limited.bin"); var transport2=new Controlled(); transport2.journal=limited;
        try (var routed=new RatisRoutedClient(directory(0,"127.0.0.1:9001"),transport2);
                var client=RatisDurableSessionClient.create(limited,routed,ISSUANCE,GRANT,new RatisSessionClient.Limits(2,300,16))) {
            var intent=client.prepareTransactionIntent(List.of(put(1))); client.prepare(List.of(put(2))); byte[] before=Files.readAllBytes(limited);
            assertThrows(IllegalStateException.class,() -> client.acceptTransaction(intent,accepted(intent.command())));
            assertTrue(intent.awaitingAcceptance()); assertNull(intent.command().acceptance()); assertArrayEquals(before,Files.readAllBytes(limited)); assertEquals(0,transport2.calls.get());
        }
    }

    @Test void failedAcceptanceCheckpointFencesAndReopenRetainsOnlyTheLastDurablePhase() throws Exception {
        Path path=root.resolve("failure.bin"); var transport=new Controlled(); transport.journal=path;
        try (var routed=new RatisRoutedClient(directory(0,"127.0.0.1:9001"),transport)) {
            byte[] durableIntent;
            try (var client=RatisDurableSessionClient.create(path,routed,ISSUANCE,GRANT,LIMITS)) {
                var intent=client.prepareTransactionIntent(List.of(put(1))); durableIntent=Files.readAllBytes(path);
                Files.delete(path); Files.createDirectory(path); Files.writeString(path.resolve("block"),"fixture");
                assertThrows(IOException.class,() -> client.acceptTransaction(intent,accepted(intent.command())));
                assertTrue(intent.awaitingAcceptance()); assertNull(intent.command().acceptance()); assertThrows(IOException.class,() -> client.execute(intent)); assertEquals(0,transport.calls.get());
            }
            Files.delete(path.resolve("block")); Files.delete(path); Files.write(path,durableIntent);
            try (var client=RatisDurableSessionClient.reopen(path,routed)) {
                var intent=client.pending().getFirst(); assertTrue(intent.awaitingAcceptance()); assertThrows(IllegalStateException.class,() -> client.execute(intent));
            }
        }
    }

    @Test void definitivePlanningFailureCanBeAbandonedConsumedAndFollowedByANewIntent() throws Exception {
        Path path=root.resolve("abandoned.bin"); var transport=new Controlled(); transport.journal=path;
        try (var routed=new RatisRoutedClient(directory(0,"127.0.0.1:9001"),transport)) {
            try (var client=RatisDurableSessionClient.create(path,routed,ISSUANCE,GRANT,LIMITS)) {
                var intent=client.prepareTransactionIntent(List.of(put(1)));
                var result=client.abandonTransactionIntent(intent); assertEquals(CommandResult.Status.CONFLICT,result.status()); assertEquals(0,result.valueCount());
                assertFalse(intent.awaitingAcceptance()); assertTrue(intent.abandoned()); assertFalse(intent.consumed()); assertEquals(-1,client.acknowledgedThrough());
                assertArrayEquals(result.encode(),intent.savedResult().encode()); assertArrayEquals(result.encode(),client.execute(intent).encode()); assertEquals(0,transport.calls.get());
                assertThrows(IllegalStateException.class,() -> client.acceptTransaction(intent,accepted(intent.command())));
            }
            try (var client=RatisDurableSessionClient.reopen(path,routed)) {
                var abandoned=client.pending().getFirst(); assertTrue(abandoned.abandoned()); assertFalse(abandoned.awaitingAcceptance());
                assertEquals(CommandResult.Status.CONFLICT,client.execute(abandoned).status()); assertEquals(0,transport.calls.get());
                client.acknowledge(abandoned); assertEquals(0,client.acknowledgedThrough());
                var next=client.prepareTransactionIntent(List.of(put(2))); assertEquals(1,next.command().operationId().sequence()); assertEquals(0,next.command().acknowledgedThrough());
            }
            try (var client=RatisDurableSessionClient.reopen(path,routed)) {
                var next=client.pending().getFirst(); assertTrue(next.awaitingAcceptance()); assertFalse(next.abandoned()); assertEquals(0,client.acknowledgedThrough());
                client.acceptTransaction(next,accepted(next.command())); assertThrows(IllegalStateException.class,() -> client.abandonTransactionIntent(next));
                var ordinary=client.prepare(List.of(put(3))); assertThrows(IllegalStateException.class,() -> client.abandonTransactionIntent(ordinary)); assertEquals(0,transport.calls.get());
            }
        }
    }

    @Test void abandonedConsumedHolesRetainPhaseUntilTheContiguousPrefixDrains() throws Exception {
        Path path=root.resolve("abandoned-hole.bin"); var transport=new Controlled(); transport.journal=path;
        try (var routed=new RatisRoutedClient(directory(0,"127.0.0.1:9001"),transport)) {
            try (var client=RatisDurableSessionClient.create(path,routed,ISSUANCE,GRANT,LIMITS)) {
                client.prepareTransactionIntent(List.of(put(1))); var second=client.prepareTransactionIntent(List.of(put(2)));
                client.abandonTransactionIntent(second); client.acknowledge(second); assertEquals(-1,client.acknowledgedThrough());
            }
            try (var client=RatisDurableSessionClient.reopen(path,routed)) {
                var first=client.pending().getFirst(); var second=client.pending().getLast();
                assertTrue(first.awaitingAcceptance()); assertTrue(second.abandoned()); assertTrue(second.consumed());
                assertEquals(CommandResult.Status.CONFLICT,client.execute(second).status()); client.abandonTransactionIntent(first); client.acknowledge(first);
                assertEquals(1,client.acknowledgedThrough()); assertTrue(client.pending().isEmpty()); assertEquals(0,transport.calls.get());
            }
        }
    }

    @Test void abandonedCoreIntentIsCachedAndOwnerSealedAndCannotBecomeAccepted() throws Exception {
        var transport=new Controlled();
        try (var routed=new RatisRoutedClient(directory(0,"127.0.0.1:9001"),transport)) {
            var core=new RatisSessionClient(routed,ISSUANCE,GRANT,LIMITS); var intent=core.prepareTransactionIntent(List.of(put(1)));
            core.abandonTransactionIntent(intent); assertTrue(intent.abandoned()); assertEquals(CommandResult.Status.CONFLICT,core.execute(intent).status());
            assertThrows(IllegalStateException.class,() -> core.acceptTransaction(intent,accepted(intent.command())));
            var snapshot=core.snapshot(); var entry=snapshot.entries().getFirst();
            var invalid=new RatisSessionClient.WindowEntry(entry.prepared(),new CommandResult(CommandResult.Status.SUCCESS,List.of()),false,false,true);
            var wrongState=new RatisSessionClient.WindowState(snapshot.nextSequence(),snapshot.acknowledgedThrough(),snapshot.exhausted(),List.of(invalid));
            assertThrows(IllegalArgumentException.class,() -> RatisSessionClient.restore(routed,GRANT,LIMITS,wrongState));
            var restored=RatisSessionClient.restore(routed,GRANT,LIMITS,snapshot); var terminal=restored.retained().getFirst();
            assertTrue(terminal.abandoned()); assertEquals(CommandResult.Status.CONFLICT,restored.execute(terminal).status());
            assertThrows(IllegalArgumentException.class,() -> restored.abandonTransactionIntent(intent)); restored.acknowledge(terminal); assertEquals(0,restored.acknowledgedThrough());
            assertEquals(0,transport.calls.get());
        }
    }

    @Test void failedAbandonCheckpointFencesBeforePublishingItsLocalConflict() throws Exception {
        Path path=root.resolve("abort-failure.bin"); var transport=new Controlled(); transport.journal=path;
        try (var routed=new RatisRoutedClient(directory(0,"127.0.0.1:9001"),transport)) {
            byte[] original;
            try (var client=RatisDurableSessionClient.create(path,routed,ISSUANCE,GRANT,LIMITS)) {
                var intent=client.prepareTransactionIntent(List.of(put(1))); original=Files.readAllBytes(path);
                Files.delete(path); Files.createDirectory(path); Files.writeString(path.resolve("block"),"fixture");
                assertThrows(IOException.class,() -> client.abandonTransactionIntent(intent)); assertTrue(intent.awaitingAcceptance()); assertFalse(intent.abandoned()); assertNull(intent.savedResult());
                assertThrows(IOException.class,() -> client.execute(intent)); assertThrows(IOException.class,() -> client.acknowledge(intent)); assertEquals(0,transport.calls.get());
            }
            Files.delete(path.resolve("block")); Files.delete(path); Files.write(path,original);
            try (var client=RatisDurableSessionClient.reopen(path,routed)) {
                var intent=client.pending().getFirst(); assertTrue(intent.awaitingAcceptance()); assertFalse(intent.abandoned());
                assertEquals(CommandResult.Status.CONFLICT,client.abandonTransactionIntent(intent).status()); assertEquals(-1,client.acknowledgedThrough()); assertEquals(0,transport.calls.get());
            }
        }
    }

    @Test void coreRecoveredIntentRemainsBlockedAndFailedResultReservationDoesNotAllocateIdentity() throws Exception {
        var transport=new Controlled();
        try (var routed=new RatisRoutedClient(directory(0,"127.0.0.1:9001"),transport)) {
            var core=new RatisSessionClient(routed,ISSUANCE,GRANT,LIMITS); var intent=core.prepareTransactionIntent(List.of(put(1)));
            assertTrue(intent.awaitingAcceptance()); assertThrows(IllegalStateException.class,() -> core.execute(intent)); assertThrows(IllegalStateException.class,() -> core.acknowledge(intent));
            var recovered=RatisSessionClient.restore(routed,GRANT,LIMITS,core.snapshot()); var pending=recovered.retained().getFirst();
            assertTrue(pending.awaitingAcceptance()); assertThrows(IllegalStateException.class,() -> recovered.execute(pending)); assertEquals(0,transport.calls.get());
            var bounded=new RatisSessionClient(routed,ISSUANCE,GRANT,new RatisSessionClient.Limits(1,1024,8));
            assertThrows(IllegalStateException.class,() -> bounded.prepareTransactionIntent(List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {1},new byte[] {2},Mutation.ReturnMode.PREVIOUS))));
            assertEquals(0,bounded.outstanding()); assertEquals(0,bounded.prepare(List.of(put(2))).command().operationId().sequence());
        }
    }

    @Test void legacyV1WindowReadsCanonicallyAndNextCheckpointUpgradesToV2() throws Exception {
        Path path=root.resolve("legacy.bin"); var transport=new Controlled(); transport.journal=path;
        try (var routed=new RatisRoutedClient(directory(0,"127.0.0.1:9001"),transport)) {
            try (var client=RatisDurableSessionClient.create(path,routed,ISSUANCE,GRANT,LIMITS)) { client.prepare(List.of(put(1))); }
            byte[] current=SessionJournalFiles.read(path,SessionWindowJournal.MAX_BYTES), old=Arrays.copyOf(current,current.length-1);
            assertEquals(0,current[current.length-1]); ByteBuffer.wrap(old).putInt(4,1); SessionJournalFiles.write(path,old,SessionWindowJournal.MAX_BYTES);
            try (var client=RatisDurableSessionClient.reopen(path,routed)) {
                var ordinary=client.pending().getFirst(); assertFalse(ordinary.awaitingAcceptance()); assertNull(ordinary.command().acceptance());
                client.execute(ordinary); assertEquals(2,ByteBuffer.wrap(SessionJournalFiles.read(path,SessionWindowJournal.MAX_BYTES)).getInt(4));
            }
        }
    }

    @Test void inconsistentIntentFlagsAndAcceptedSuccessPayloadsAreRejectedOnRecovery() throws Exception {
        Path path=root.resolve("flag.bin"); var transport=new Controlled(); transport.journal=path; transport.lose=false;
        try (var routed=new RatisRoutedClient(directory(0,"127.0.0.1:9001"),transport)) {
            try (var client=RatisDurableSessionClient.create(path,routed,ISSUANCE,GRANT,LIMITS)) {
                var intent=client.prepareTransactionIntent(List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[] {1},new byte[] {2},Mutation.ReturnMode.PREVIOUS)));
                client.acceptTransaction(intent,accepted(intent.command()));
            }
            byte[] valid=SessionJournalFiles.read(path,SessionWindowJournal.MAX_BYTES);
            for (int phase:new int[] {1,2,3}) {
                byte[] body=valid.clone(); body[body.length-1]=(byte)phase; SessionJournalFiles.write(path,body,SessionWindowJournal.MAX_BYTES);
                assertThrows(IOException.class,() -> RatisDurableSessionClient.reopen(path,routed));
            }
            SessionJournalFiles.write(path,valid,SessionWindowJournal.MAX_BYTES);
            try (var journal=SessionWindowJournal.open(path,routed,false)) {
                var saved=journal.read(); var entry=saved.state().entries().getFirst();
                var wrong=new RatisSessionClient.WindowEntry(entry.prepared(),new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[] {99})),false,false);
                var state=new RatisSessionClient.WindowState(saved.state().nextSequence(),saved.state().acknowledgedThrough(),saved.state().exhausted(),List.of(wrong));
                assertThrows(IOException.class,() -> journal.write(saved.grant(),saved.limits(),state));
                assertArrayEquals(valid,SessionJournalFiles.read(path,SessionWindowJournal.MAX_BYTES));
            }
        }
    }
}
