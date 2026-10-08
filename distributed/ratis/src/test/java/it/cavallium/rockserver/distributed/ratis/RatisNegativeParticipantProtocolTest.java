package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Frozen protocol/transport/journal invariants. Controlled replies do not prove native rejection authority. */
class RatisNegativeParticipantProtocolTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisDurableTransactionTest helper=new RatisDurableTransactionTest();
    static final class Wire implements RatisRoutedClient.Transport {
        final RatisParticipantPrepareTest.Wire nativeWire;
        final List<byte[]> requests=new ArrayList<>();
        boolean lose,positive;
        PreparedParticipant prepared;
        ParticipantOutcome outcome;
        Wire(RatisParticipantPrepareTest.Wire nativeWire) { this.nativeWire=nativeWire; }
        public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return nativeWire.read(route,column,key); }
        public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return nativeWire.command(route,command); }
        public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException {
            requests.add(bytes.clone()); RatisParticipantRecord.decode(bytes);
            if (lose) { lose=false; throw new IOException("Controlled lost reply"); }
            return RatisParticipantRecord.result(positive ? ParticipantVote.prepared(new ApplyPosition(1,1)) : ParticipantVote.negative());
        }
        public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) {
            assertTrue(fresh); int kind=ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind();
            byte[] value=switch (kind) { case ScopedTransactionRpc.PARTICIPANT_STATE -> prepared==null ? null : prepared.encode(); case ScopedTransactionRpc.PARTICIPANT_OUTCOME -> outcome==null ? null : outcome.encode(); default -> throw new AssertionError("Unexpected query"); };
            var body=ByteBuffer.allocate(4+(value==null ? 0 : value.length)).putInt(value==null ? -1 : value.length); if (value!=null) body.put(value);
            return ScopedTransactionRpc.frame(kind,body.array());
        }
    }
    PreparedParticipant.Preparation preparation(RatisParticipantPrepareTest.Node home,RatisDurableTransactionTest.Plan plan) {
        return new PreparedParticipant.Preparation(home.f.groupId,1,plan.intent().prepareControl(),plan.initial(),plan.intent().acceptedBusiness());
    }
    @Test void explicitNegativeOpcodeBindsItsDigestAndRejectsMalformedKindsAndPositiveReplies() throws Exception {
        helper.helper.root=root;
        try (var home=helper.helper.new Node("codec-home",true); var data=helper.helper.new Node("codec-data",false); var pin=data.f.store.captureTracked(helper.helper.scope(data,1,4))) {
            var plan=helper.plan(home,data,pin); var preparation=preparation(home,plan); byte[] positive=RatisParticipantRecord.encode(preparation),negative=RatisParticipantRecord.encode(preparation,true);
            assertEquals(1,ByteBuffer.wrap(positive).getInt(4)); assertEquals(3,ByteBuffer.wrap(negative).getInt(4)); assertTrue(RatisParticipantRecord.isNegative(negative)); assertFalse(RatisParticipantRecord.isNegative(positive)); assertFalse(RatisParticipantRecord.isFinalization(negative));
            assertEquals(preparation,RatisParticipantRecord.decode(negative)); assertEquals(preparation,RatisParticipantRecord.decode(positive));
            assertFalse(Arrays.equals(RatisParticipantRecord.digest(preparation),RatisParticipantRecord.digest(preparation,true)));
            assertArrayEquals(Arrays.copyOfRange(positive,8,positive.length),Arrays.copyOfRange(negative,8,negative.length));
            byte[] invalid=negative.clone(); ByteBuffer.wrap(invalid).putInt(4,4); assertThrows(IllegalArgumentException.class,() -> RatisParticipantRecord.decode(invalid));
            assertThrows(IllegalArgumentException.class,() -> RatisParticipantRecord.decode(Arrays.copyOf(negative,negative.length+1)));
            for (int n=0;n<negative.length;n++) { byte[] truncated=Arrays.copyOf(negative,n); assertThrows(IllegalArgumentException.class,() -> RatisParticipantRecord.decode(truncated)); }
            var wire=new Wire(data.wire);
            try (var routed=new RatisRoutedClient(data.directory,wire)) {
                var operation=routed.prepareParticipant(preparation,true); assertTrue(operation.negative()); assertArrayEquals(negative,operation.bytes());
                byte[] returned=operation.bytes(); Arrays.fill(returned,(byte)0); assertArrayEquals(negative,operation.bytes());
                var restored=routed.restoreParticipant(preparation,operation.descriptor(),true); assertArrayEquals(negative,restored.bytes());
                wire.positive=true; assertThrows(IOException.class,() -> routed.executeParticipant(operation));
                wire.positive=false; assertEquals(ParticipantVote.negative(),routed.executeParticipant(operation));
            }
        }
    }
    @Test void lostNegativeReplyReopensWithExactKindRouteAndBodyAndCannotBecomePositive() throws Exception {
        helper.helper.root=root;
        try (var home=helper.helper.new Node("journal-home",true); var data=helper.helper.new Node("journal-data",false); var pin=data.f.store.captureTracked(helper.helper.scope(data,1,4))) {
            var plan=helper.plan(home,data,pin); var preparation=preparation(home,plan); var wire=new Wire(data.wire);
            try (var routed=new RatisRoutedClient(data.directory,wire)) {
                Path path=root.resolve("negative.bin"); var journal=RatisDurableTransaction.create(path,home.statuses,routed,plan.initial(),plan.registration(),List.of(plan.intent()));
                try {
                    journal.executeHome(); var operation=routed.prepareParticipant(preparation,true); journal.freezePreparation(operation); wire.lose=true;
                    var owned=journal; assertThrows(IOException.class,() -> owned.executePreparation(data.f.groupId)); assertNull(journal.snapshot().participant(data.f.groupId).vote());
                    journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,routed);
                    var saved=journal.snapshot().participant(data.f.groupId).preparation(); assertTrue(saved.negative()); assertArrayEquals(operation.bytes(),saved.bytes()); assertEquals(operation.descriptor(),saved.descriptor());
                    var reopened=journal; assertThrows(IllegalArgumentException.class,() -> reopened.freezePreparation(routed.prepareParticipant(preparation,false)));
                    assertEquals(ParticipantVote.negative(),journal.executePreparation(data.f.groupId)); assertEquals(2,wire.requests.size()); assertArrayEquals(wire.requests.getFirst(),wire.requests.getLast());
                    journal.close(); journal=RatisDurableTransaction.reopen(path,home.statuses,routed); assertEquals(ParticipantVote.negative(),journal.snapshot().participant(data.f.groupId).vote()); assertTrue(journal.snapshot().participant(data.f.groupId).preparation().negative());
                    // The inverse kind change is also forbidden, even before a positive RPC is sent.
                    Path positivePath=root.resolve("positive.bin");
                    try (var positiveJournal=RatisDurableTransaction.create(positivePath,home.statuses,routed,plan.initial(),plan.registration(),List.of(plan.intent()))) {
                        positiveJournal.refreshHome(); positiveJournal.freezePreparation(routed.prepareParticipant(preparation,false));
                        assertThrows(IllegalArgumentException.class,() -> positiveJournal.freezePreparation(operation));
                    }
                } finally { journal.close(); }
            }
        }
    }
    @Test void frozenNegativeRejectsFreshPositivePreparedAndTerminalProofsWithoutSavingThem() throws Exception {
        helper.helper.root=root;
        try (var home=helper.helper.new Node("proof-home",true); var data=helper.helper.new Node("proof-data",false); var pin=data.f.store.captureTracked(helper.helper.scope(data,1,4))) {
            var plan=helper.plan(home,data,pin); var preparation=preparation(home,plan); var wire=new Wire(data.wire);
            try (var routed=new RatisRoutedClient(data.directory,wire); var journal=RatisDurableTransaction.create(root.resolve("proof.bin"),home.statuses,routed,plan.initial(),plan.registration(),List.of(plan.intent()))) {
                journal.executeHome(); journal.freezePreparation(routed.prepareParticipant(preparation,true));
                wire.positive=true; assertThrows(IOException.class,() -> journal.executePreparation(data.f.groupId)); assertNull(journal.snapshot().participant(data.f.groupId).vote());
                wire.prepared=new PreparedParticipant(preparation,new ApplyPosition(1,1));
                assertThrows(IOException.class,() -> journal.resolveParticipant(data.f.groupId)); assertNull(journal.snapshot().participant(data.f.groupId).vote());
                wire.prepared=null; var abort=plan.initial().decide(TransactionStatus.Phase.ABORT); journal.queueRecovery(abort); journal.executeHome();
                var finalization=new ParticipantFinalization(home.f.groupId,1,data.f.groupId,plan.intent().finalizeControl(),abort);
                wire.outcome=new ParticipantOutcome(finalization,new ApplyPosition(1,2),ParticipantVote.prepared(new ApplyPosition(1,1)),null);
                assertThrows(IOException.class,() -> journal.resolveParticipant(data.f.groupId)); assertNull(journal.snapshot().participant(data.f.groupId).outcome()); assertNull(journal.snapshot().participant(data.f.groupId).vote());
            }
        }
    }
    @Test void checksummedCurrentJournalRejectsMalformedKindAndNegativeKindWithPositiveSavedProof() throws Exception {
        helper.helper.root=root;
        try (var home=helper.helper.new Node("decode-home",true); var data=helper.helper.new Node("decode-data",false); var pin=data.f.store.captureTracked(helper.helper.scope(data,1,4))) {
            var plan=helper.plan(home,data,pin); var preparation=preparation(home,plan); var wire=new Wire(data.wire); wire.positive=true;
            try (var routed=new RatisRoutedClient(data.directory,wire)) {
                Path path=root.resolve("decode.bin");
                try (var journal=RatisDurableTransaction.create(path,home.statuses,routed,plan.initial(),plan.registration(),List.of(plan.intent()))) {
                    journal.executeHome(); journal.freezePreparation(routed.prepareParticipant(preparation,false)); assertTrue(journal.executePreparation(data.f.groupId).prepared());
                }
                byte[] body=SessionJournalFiles.read(path,RatisDurableTransaction.MAX_BYTES); int offset=preparationKind(body);
                assertEquals(1,body[offset]); byte[] negative=body.clone(); negative[offset]=2; SessionJournalFiles.write(path,negative,RatisDurableTransaction.MAX_BYTES);
                assertThrows(IOException.class,() -> RatisDurableTransaction.reopen(path,home.statuses,routed));
                byte[] malformed=body.clone(); malformed[offset]=3; SessionJournalFiles.write(path,malformed,RatisDurableTransaction.MAX_BYTES);
                assertThrows(IOException.class,() -> RatisDurableTransaction.reopen(path,home.statuses,routed));
                SessionJournalFiles.write(path,body,RatisDurableTransaction.MAX_BYTES);
                try (var journal=RatisDurableTransaction.reopen(path,home.statuses,routed)) { assertFalse(journal.snapshot().participant(data.f.groupId).preparation().negative()); assertTrue(journal.snapshot().participant(data.f.groupId).vote().prepared()); }
            }
        }
    }
    // Locate the single participant's documented current state-kind byte without relying on random body matches.
    private int preparationKind(byte[] body) throws IOException {
        var b=ByteBuffer.wrap(body); b.position(68); skipBlob(b); SessionWindowJournal.route(b); b.position(b.position()+32+24); skipBlob(b);
        assertEquals(1,b.get()); skipBlob(b); SessionWindowJournal.route(b); assertEquals(0,b.get()); skipBlob(b); b.get(); assertEquals(1,b.getInt());
        SessionWindowJournal.route(b); skipBlob(b); b.position(b.position()+64); int slots=b.getInt(); b.position(b.position()+4*slots); skipBlob(b); return b.position();
    }
    private void skipBlob(ByteBuffer b) { int size=b.getInt(); if (size>=0) b.position(b.position()+size); }
}
