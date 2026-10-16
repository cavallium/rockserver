package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Original server capture metadata through controlled native dispatch; journal reopen performs no capture RPC. */
class RatisRemoteTransactionJournalTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisDurableTransactionTest owner=new RatisDurableTransactionTest();
    final RatisParticipantPrepareTest helper=owner.helper;
    final AtomicLong clock=new AtomicLong(100);
    final AtomicInteger opens=new AtomicInteger(),queries=new AtomicInteger();
    OperationId business() { return new OperationId(new UUID(0,46000),1,0); }
    Map<UUID,OperationId> prepares(UUID group) { return Map.of(group,helper.control(0)); }
    Map<UUID,OperationId> finalizes(UUID group) { return Map.of(group,new OperationId(new UUID(0,46001),3,0)); }
    final class Fixture implements AutoCloseable {
        final RatisParticipantPrepareTest.Node home,data;
        final RatisRoutedClient routed;
        final RatisTransactionPlan plan;
        final RatisRoutedClient.ScopedTransaction capture;
        final DistributedCommand accepted;
        final RatisTransactionPlan.Accepted frozen;
        Fixture(String name) throws Exception {
            helper.root=root; home=helper.new Node(name+"-home",true); data=helper.new Node(name+"-data",false,null,null,clock::get);
            routed=new RatisRoutedClient(data.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return data.wire.read(route,column,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return data.wire.command(route,command); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { return data.wire.participant(route,bytes); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException {
                    queries.incrementAndGet(); if (ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()==ScopedTransactionRpc.OPEN) opens.incrementAndGet();
                    return data.wire.transaction(route,bytes,fresh);
                }
            });
            plan=RatisTransactionPlan.create(List.of(data.directory),business(),List.of(new Mutation(Mutation.Kind.PUT,data.f.column,new byte[]{2},new byte[]{7},Mutation.ReturnMode.CURRENT)),List.of());
            var part=plan.partitions().getFirst(); capture=routed.openTransaction(part.scope(),Duration.ofSeconds(1));
            accepted=capture.participantIntent(part.command(),capture.stage(part.command().mutations(),part.command().predicates()));
            frozen=plan.accept(new UUID(0,46002),Map.of(data.f.groupId,accepted),prepares(data.f.groupId),finalizes(data.f.groupId),Map.of(data.f.groupId,capture.captureId()));
        }
        RatisDurableTransaction create(Path path,RatisTransactionPlan.Accepted accepted) throws IOException {
            return RatisDurableTransaction.create(path,home.statuses,RatisTransactionBindings.of(routed),accepted.initial(),home.statuses.prepare(owner.homeOperation(0),null,accepted.initial()),accepted.intents());
        }
        public void close() throws Exception { try { capture.close(); } finally { try { routed.close(); } finally { try { data.close(); } finally { home.close(); } } } }
    }

    @Test void currentJournalRoundTripsOriginalRemoteCaptureAndLegitimateLocalNullWithExactByteCharge() throws Exception {
        try (var f=new Fixture("roundtrip")) {
            var remotePath=root.resolve("remote.journal"); var localPath=root.resolve("local.journal"); var group=f.data.f.groupId;
            var local=f.plan.accept(f.frozen.initial().transactionId(),Map.of(group,f.accepted),prepares(group),finalizes(group));
            try (var remote=f.create(remotePath,f.frozen); var journal=f.create(localPath,local)) {
                assertEquals(f.capture.captureId(),remote.snapshot().participant(group).intent().captureId()); assertNull(journal.snapshot().participant(group).intent().captureId());
            }
            assertEquals(16,Files.size(remotePath)-Files.size(localPath)); int before=queries.get();
            for (var path:List.of(remotePath,localPath)) try (var reopened=RatisDurableTransaction.reopen(path,f.home.statuses,RatisTransactionBindings.of(f.routed))) {
                var intent=reopened.snapshot().participant(group).intent(); assertEquals(path.equals(remotePath) ? f.capture.captureId() : null,intent.captureId());
                assertArrayEquals(DistributedCommandCodec.encode(f.accepted),DistributedCommandCodec.encode(intent.acceptedBusiness())); assertEquals(f.plan.partitions().getFirst().route(),intent.route());
                assertEquals(f.accepted.acceptance(),intent.acceptedBusiness().acceptance()); assertNull(reopened.snapshot().observedHome());
            }
            assertEquals(before,queries.get()); assertEquals(1,opens.get()); assertEquals(0,f.home.wire.index); assertEquals(0,f.data.wire.index);
        }
    }

    @Test void reopenRestoresOnlyTheOriginalHandleAndNeverExtendsItsServerDeadline() throws Exception {
        try (var f=new Fixture("expired")) {
            var path=root.resolve("expired.journal"); try (var journal=f.create(path,f.frozen)) {}
            var original=f.capture.captureId(); var cut=f.capture.snapshot(); clock.addAndGet(Duration.ofSeconds(1).toNanos()); f.data.f.machine.expireTransactions();
            assertEquals(0,f.data.f.store.activeTrackedSnapshotPins()); int before=queries.get();
            try (var journal=RatisDurableTransaction.reopen(path,f.home.statuses,RatisTransactionBindings.of(f.routed))) {
                var intent=journal.snapshot().participant(f.data.f.groupId).intent();
                try (var restored=f.routed.restoreTransaction(intent)) {
                    assertEquals(original,restored.captureId()); assertEquals(cut,restored.snapshot()); assertEquals(before,queries.get());
                    assertThrows(IOException.class,() -> restored.get(f.data.f.column,new byte[]{2}));
                }
                assertEquals(original,intent.captureId()); assertEquals(cut,intent.acceptedBusiness().acceptance().snapshot());
            }
            assertEquals(1,opens.get()); assertEquals(0,f.data.f.store.activeTrackedSnapshotPins()); assertEquals(0,f.data.wire.index);
        }
    }

    @Test void sameCaptureCannotChangeAcceptedOriginProofUnderTheFrozenHomeDigest() throws Exception {
        try (var f=new Fixture("changed-proof")) {
            var group=f.data.f.groupId; var audit=f.accepted.acceptance();
            var changedAudit=new TransactionAcceptance(new ApplyPosition(audit.snapshot().term(),audit.snapshot().index()+1),audit.leaderGeneration(),audit.ownerIncarnation(),audit.scope(),audit.result());
            var changed=DistributedCommand.acceptedTransaction(f.plan.partitions().getFirst().command(),changedAudit);
            var other=f.plan.accept(f.frozen.initial().transactionId(),Map.of(group,changed),prepares(group),finalizes(group),Map.of(group,f.capture.captureId()));
            assertEquals(f.capture.captureId(),other.intents().getFirst().captureId()); assertFalse(Arrays.equals(f.frozen.initial().planDigest(),other.initial().planDigest()));
            var path=root.resolve("changed-proof.journal");
            assertThrows(IllegalArgumentException.class,() -> RatisDurableTransaction.create(path,f.home.statuses,RatisTransactionBindings.of(f.routed),f.frozen.initial(),f.home.statuses.prepare(owner.homeOperation(0),null,f.frozen.initial()),other.intents()));
            assertFalse(Files.exists(path)); assertEquals(1,opens.get()); assertEquals(0,f.home.wire.index); assertEquals(0,f.data.wire.index);
            assertThrows(IllegalArgumentException.class,() -> new RatisDurableTransaction.Intent(f.plan.partitions().getFirst().route(),f.plan.partitions().getFirst().command(),helper.control(0),finalizes(group).get(group),List.of(0),f.capture.captureId()));
        }
    }

    @Test void checksummedCaptureSubstitutionStillFailsFrozenDefinitionValidation() throws Exception {
        try (var f=new Fixture("changed-capture")) {
            var path=root.resolve("changed-capture.journal"); try (var journal=f.create(path,f.frozen)) {}
            byte[] body=SessionJournalFiles.read(path,RatisDurableTransaction.MAX_BYTES); var id=f.capture.captureId();
            byte[] original=ByteBuffer.allocate(16).putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()).array(); int offset=-1,count=0;
            for (int n=0;n<=body.length-original.length;n++) if (Arrays.equals(body,n,n+original.length,original,0,original.length)) { offset=n; count++; }
            assertEquals(1,count); ByteBuffer.wrap(body).putLong(offset,id.getMostSignificantBits()^1); SessionJournalFiles.write(path,body,RatisDurableTransaction.MAX_BYTES);
            assertThrows(IOException.class,() -> RatisDurableTransaction.reopen(path,f.home.statuses,RatisTransactionBindings.of(f.routed)));
            assertEquals(1,opens.get()); assertEquals(0,f.home.wire.index); assertEquals(0,f.data.wire.index);
        }
    }
}
