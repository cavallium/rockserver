package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Certified original captures and 2PC over controlled native groups; no OPEN, servers or network. */
class RatisCapturedTransactionPlanTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    static final Duration TTL=Duration.ofSeconds(30);
    final class Captured implements AutoCloseable {
        final RatisAtomicClientTest.Fixture f;
        final Path snapshotPath;
        final UUID column,left,right;
        final RatisDurableSnapshot.Snapshot cuts;
        final Map<UUID,RatisRoutedClient.ScopedTransaction> handles=new HashMap<>();
        Captured(String name) throws Exception {
            f=new RatisAtomicClientTest.Fixture(root.resolve(name),true); column=f.schema.column(71).columnId(); left=f.groups.getFirst(); right=f.groups.getLast();
            write(2,0,1); write(200,1,2);
            var participants=new ArrayList<SnapshotFence.Participant>();
            for (int n=0;n<f.directory.ranges().size();n++) {
                var route=f.directory.ranges().get(n); byte[] low={(byte)(n==0 ? 1 : 200)},high={(byte)(n==0 ? 5 : 205)};
                var scope=new TransactionScope(List.of(TransactionScope.Span.range(column,low,high)));
                participants.add(new SnapshotFence.Participant(f.directory.domainId(), route.rangeId(), route.groupId(), route.rangeEpoch(), f.schema.digest(), operation(20+n*2), operation(21+n*2), scope));
            }
            var fence=new SnapshotFence(f.directory.clusterId(), f.directory.databaseId(), new UUID(180,1), f.snapshotGroup, 1, operation(10), List.of(new DomainBinding(f.directory.domainId(), f.directory.generation(), f.directory.payloadDigest(), f.directory.schemaEpoch(), f.schema.digest())), participants);
            var initial=SnapshotFenceStatus.begin(fence); snapshotPath=f.root.resolve("writable-snapshot.journal");
            RatisDurableSnapshot.Snapshot captured;
            try (var journal=RatisDurableSnapshot.create(snapshotPath,f.snapshots,f.data,initial,f.snapshots.prepare(operation(30),null,initial),f.directory.ranges(),TTL,RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED);
                    var coordinator=new RatisSnapshotCoordinator(journal,f.snapshots,f.data,last -> operation(last.sequence()+1),UUID::randomUUID,TTL)) {
                boolean complete=false; for (int n=0;n<40 && !complete;n++) { int before=f.writes(); complete=coordinator.step().complete(); assertTrue(f.writes()-before<=1); }
                assertTrue(complete); captured=journal.snapshot(); assertEquals(SnapshotFenceStatus.Phase.COMPLETE,captured.observedHome().phase()); assertTrue(captured.observedHome().allReady());
                assertFalse(captured.observedHome().consumed()); assertEquals(RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED,captured.captureMode());
                for (var progress:captured.participants()) { assertNotNull(progress.certifiedHandle()); assertEquals(progress.captureToken().position(),progress.certifiedHandle().position()); assertEquals(progress.releaseReceipt(),progress.nativeGate().released()); }
            }
            int opens=f.opens.get(),requests=f.captureRequests.get();
            try (var journal=RatisDurableSnapshot.reopen(snapshotPath,f.snapshots,f.data)) {
                cuts=journal.snapshot();
                for (var progress:cuts.participants()) {
                    assertEquals(captured.participant(progress.route().groupId()).certifiedHandle(),progress.certifiedHandle());
                    handles.put(progress.route().groupId(),f.data.capturedTransaction(progress.capture(),new RatisSnapshotCaptureRequest.Result(progress.captureToken(),progress.certifiedHandle())));
                }
            }
            assertEquals(opens,f.opens.get()); assertEquals(requests,f.captureRequests.get()); assertEquals(0,f.opens.get());
            assertArrayEquals(new byte[]{0},handles.get(left).get(column,new byte[]{2})); assertArrayEquals(new byte[]{1},handles.get(right).get(column,new byte[]{(byte)200})); assertNull(handles.get(right).get(column,new byte[]{(byte)203}));
        }
        OperationId operation(long n) { return new OperationId(f.session,1,n); }
        void write(int key,int value,long sequence) throws IOException {
            assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.prepare(operation(sequence),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{(byte)key},new byte[]{(byte)value})))).status());
        }
        RatisTransactionPlan plan() { return RatisTransactionPlan.captured(f.directory,operation(100),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{2},new byte[]{7},Mutation.ReturnMode.PREVIOUS)),List.of(),cuts); }
        Map<UUID,OperationId> controls(boolean finalize) { return Map.of(left,operation(finalize ? 130 : 110),right,operation(finalize ? 131 : 111)); }
        Map<UUID,DistributedCommand> accept(RatisTransactionPlan plan) throws Exception {
            var accepted=new HashMap<UUID,DistributedCommand>();
            for (var part:plan.partitions()) {
                var handle=handles.get(part.route().groupId()); var command=part.command().readCertification() ? handle.readCertification(plan.operation())
                        : handle.participantIntent(part.command(),handle.stage(part.command().mutations(),part.command().predicates()));
                if (part.command().readCertification()) assertArrayEquals(DistributedCommandCodec.encode(part.command()),DistributedCommandCodec.encode(command));
                assertArrayEquals(ScopedTransactionRpc.scopeBytes(part.scope()),ScopedTransactionRpc.scopeBytes(command.acceptance().scope())); accepted.put(part.route().groupId(),command);
            }
            return accepted;
        }
        RatisTransactionCoordinator driver(RatisDurableTransaction journal) throws IOException {
            return new RatisTransactionCoordinator(journal,f.homes,f.data,last -> operation(last.sequence()+1),(transaction,progress) -> {
                var operation=handles.get(progress.intent().route().groupId()).authorizeParticipant(progress.intent().acceptedBusiness(),progress.intent().prepareControl(),transaction);
                return new RatisTransactionCoordinator.Authorization(operation.preparation(),operation.negative(),() -> {});
            });
        }
        RatisDurableTransaction journal(Path path,RatisTransactionPlan.Accepted accepted) throws IOException {
            return RatisDurableTransaction.create(path,f.homes,f.data,accepted.initial(),f.homes.prepare(operation(200),null,accepted.initial()),accepted.intents());
        }
        void drain(RatisDurableTransaction journal) throws Exception {
            try (var driver=driver(journal)) { boolean complete=false; for (int n=0;n<40 && !complete;n++) { int before=f.writes(); complete=driver.step().complete(); assertTrue(f.writes()-before<=1); } assertTrue(complete); }
        }
        public void close() throws Exception {
            try { for (var handle:handles.values()) handle.close(); } finally { f.close(); }
        }
    }

    @Test void originalWritableCaptureIncludesAndLocksTheEntireReadOnlyParticipantThroughReopenAndCommit() throws Exception {
        try (var x=new Captured("captured-commit")) {
            var plan=x.plan(); assertEquals(2,plan.partitions().size());
            var read=plan.partitions().stream().filter(part -> part.route().groupId().equals(x.right)).findFirst().orElseThrow();
            assertTrue(read.command().readCertification()); assertTrue(read.command().mutations().isEmpty()); assertTrue(read.resultPositions().isEmpty()); assertTrue(read.scope().containsRange(x.column,new byte[]{(byte)200},new byte[]{(byte)205}));
            var accepted=plan.acceptCaptured(new UUID(180,2),x.accept(plan),x.controls(false),x.controls(true)); var path=x.f.root.resolve("captured-transaction.journal");
            try (var journal=x.journal(path,accepted); var coordinator=x.driver(journal)) {
                boolean locked=false;
                for (int n=0;n<20 && !locked;n++) { int before=x.f.writes(); coordinator.step(); assertTrue(x.f.writes()-before<=1); var progress=journal.snapshot().participant(x.right); locked=progress.vote()!=null && progress.vote().prepared() && progress.outcome()==null; }
                assertTrue(locked); int writes=x.f.writes(); assertThrows(IOException.class,() -> x.write(203,9,350)); assertEquals(writes,x.f.writes());
                assertTrue(journal.snapshot().participant(x.right).intent().acceptedBusiness().readCertification());
            }
            int requests=x.f.captureRequests.get();
            try (var journal=RatisDurableTransaction.reopen(path,x.f.homes,x.f.data)) {
                for (var progress:journal.snapshot().participants()) {
                    var original=x.cuts.participant(progress.intent().route().groupId()).certifiedHandle(); assertEquals(original.id(),progress.intent().captureId()); assertEquals(original.position(),progress.intent().acceptedBusiness().acceptance().snapshot());
                }
                x.drain(journal); assertEquals(TransactionStatus.Phase.COMMIT,journal.snapshot().observedHome().phase()); assertTrue(journal.snapshot().observedHome().fullyFinalized()); assertFalse(journal.snapshot().observedHome().consumed());
                var result=journal.result(); assertEquals(CommandResult.Status.SUCCESS,result.status()); assertEquals(1,result.valueCount()); assertArrayEquals(new byte[]{0},result.value(0));
                assertEquals(0,CommandResult.decode(journal.snapshot().participant(x.right).outcome().result()).valueCount());
            }
            assertEquals(0,x.f.opens.get()); assertEquals(requests,x.f.captureRequests.get()); assertArrayEquals(new byte[]{7},x.f.get(71,RatisAtomicClientTest.key(2))); assertArrayEquals(new byte[]{1},x.f.get(71,RatisAtomicClientTest.key(200))); assertNull(x.f.get(71,RatisAtomicClientTest.key(203)));
            var capture=x.cuts.participant(x.left); assertThrows(IOException.class,() -> x.f.data.snapshotGet(capture.capture().target(),capture.captureToken(),x.column,new byte[]{2}));
            assertEquals(0,x.f.stores.get(x.left).activeTrackedSnapshotPins()); assertEquals(0,x.f.stores.get(x.right).activeTrackedSnapshotPins());
        }
    }

    @Test void changesInTheReadOnlyProjectionPreventWriteSkewPhantomsAndAbaCommit() throws Exception {
        for (String kind:List.of("write-skew","phantom","aba")) try (var x=new Captured("captured-"+kind)) {
            var plan=x.plan(); var accepted=plan.acceptCaptured(new UUID(181,kind.hashCode()),x.accept(plan),x.controls(false),x.controls(true));
            if (kind.equals("phantom")) x.write(203,2,300); else { x.write(200,2,300); if (kind.equals("aba")) x.write(200,1,301); }
            try (var journal=x.journal(x.f.root.resolve("conflict.journal"),accepted)) {
                x.drain(journal); assertEquals(TransactionStatus.Phase.ABORT,journal.snapshot().observedHome().phase()); assertEquals(CommandResult.Status.CONFLICT,journal.result().status()); assertTrue(journal.snapshot().observedHome().fullyFinalized()); assertFalse(journal.snapshot().observedHome().consumed());
                assertEquals(x.cuts.participant(x.right).certifiedHandle().id(),journal.snapshot().participant(x.right).intent().captureId());
            }
            assertArrayEquals(new byte[]{0},x.f.get(71,RatisAtomicClientTest.key(2))); assertEquals(0,x.f.opens.get());
        }
    }

    @Test void capturedPlanRejectsMissingReadParticipantsOutsideWritesAndChangedProofOrHandleIds() throws Exception {
        try (var x=new Captured("captured-validation")) {
            var plan=x.plan(); var accepted=x.accept(plan); var transaction=new UUID(180,3); int writes=x.f.writes(),opens=x.f.opens.get(),captures=x.f.captureRequests.get();
            var missing=new HashMap<>(accepted); missing.remove(x.right); assertThrows(IllegalArgumentException.class,() -> plan.acceptCaptured(transaction,missing,x.controls(false),x.controls(true)));
            var missingCuts=new RatisDurableSnapshot.Snapshot(x.cuts.initial(),x.cuts.observedHome(),x.cuts.pendingHome(),x.cuts.homeResult(),x.cuts.homeSuperseded(),x.cuts.lastHome(),List.of(x.cuts.participant(x.left)),x.cuts.homeRetirementProof(),x.cuts.releaseRequested(),x.cuts.requestedTtl(),x.cuts.captureMode());
            assertThrows(IllegalArgumentException.class,() -> RatisTransactionPlan.captured(x.f.directory,x.operation(100),List.of(),List.of(),missingCuts));
            assertThrows(IllegalArgumentException.class,() -> RatisTransactionPlan.captured(x.f.directory,x.operation(100),List.of(new Mutation(Mutation.Kind.PUT,x.column,new byte[]{(byte)206},new byte[]{9})),List.of(),x.cuts));
            assertThrows(IllegalArgumentException.class,() -> RatisTransactionPlan.captured(x.f.directory,x.operation(100),List.of(new Mutation(Mutation.Kind.PUT,x.f.schema.column(72).columnId(),new byte[]{2},new byte[]{9})),List.of(),x.cuts));
            var ids=new HashMap<UUID,UUID>(); x.cuts.participants().forEach(progress -> ids.put(progress.route().groupId(),progress.certifiedHandle().id())); ids.put(x.right,new UUID(182,1));
            assertThrows(IllegalArgumentException.class,() -> plan.accept(transaction,accepted,x.controls(false),x.controls(true),ids));
            var audit=accepted.get(x.left).acceptance(); var forged=new TransactionAcceptance(audit.snapshot(),audit.leaderGeneration(),new UUID(182,2),audit.scope(),audit.result());
            var changed=new HashMap<>(accepted); var write=plan.partitions().stream().filter(part -> part.route().groupId().equals(x.left)).findFirst().orElseThrow().command(); changed.put(x.left,DistributedCommand.acceptedTransaction(write,forged));
            assertThrows(IllegalArgumentException.class,() -> plan.acceptCaptured(transaction,changed,x.controls(false),x.controls(true)));
            assertEquals(writes,x.f.writes()); assertEquals(opens,x.f.opens.get()); assertEquals(captures,x.f.captureRequests.get()); assertNull(x.f.homes.read(transaction));
        }
    }
}
