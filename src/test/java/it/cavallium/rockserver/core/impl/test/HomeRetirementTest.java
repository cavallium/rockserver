package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class HomeRetirementTest {
    final UUID home=new UUID(0,1),column=new UUID(0,2),left=new UUID(0,3),right=new UUID(0,4),transaction=new UUID(0,5);
    TransactionStatus initial(long sequence) {
        return TransactionStatus.begin(new UUID(0,6), new UUID(0,7), transaction, new OperationId(new UUID(0,9),1,sequence), List.of(new DomainBinding(new UUID(0,8), 0, new byte[32], 1, new byte[32])), new byte[32], List.of(new TransactionStatus.Participant(new UUID(0,8), right, right, 1, new byte[32], new byte[32], null, false),
                        new TransactionStatus.Participant(new UUID(0,8), left, left, 1, new byte[32], new byte[32], null, false)));
    }
    TransactionStatus consumed() {
        return initial(0).prepared(left,new byte[32],new ApplyPosition(1,3)).prepared(right,new byte[32],new ApplyPosition(2,4))
                .decide(TransactionStatus.Phase.COMMIT).finalized(left).finalized(right).consume();
    }
    HomeIdentity identity(TransactionStatus status) { return new HomeIdentity(home,1,new byte[32],column,status); }
    ParticipantFenceProof dataFence(UUID group,long term,long index) { return new ParticipantFenceProof(new ParticipantIdentity(home,1,group,initial(0)),2,new ApplyPosition(term,index)); }
    List<ParticipantFenceProof> fences() { return List.of(dataFence(right,3,9),dataFence(left,3,8)); }

    @Test void consumedModeStoresOneStatusAndFortyBytesPerCanonicalDataGroup() {
        var consumed=consumed(); var retirement=new HomeRetirement(identity(consumed),consumed,fences());
        assertEquals(initial(0),retirement.identity().definition());
        assertEquals(List.of(left,right),retirement.participants().stream().map(p -> p.identity().participantGroup()).toList());
        assertEquals(81+consumed.encode().length+40*2,retirement.encode().length);
        var copy=HomeRetirement.decode(retirement.encode()); assertEquals(retirement,copy); assertArrayEquals(retirement.encode(),copy.encode()); assertArrayEquals(retirement.digest(),copy.digest());
        var input=new ArrayList<>(fences()); var owned=new HomeRetirement(identity(consumed),consumed,input); input.clear(); assertEquals(2,owned.participants().size());
        assertThrows(UnsupportedOperationException.class,() -> retirement.participants().clear());
    }

    @Test void orphanModeHasOnlyTheInitialDefinitionAndNoConsumedOrDataProof() {
        var identity=identity(consumed()); var retirement=new HomeRetirement(identity,null,List.of()); var bytes=retirement.encode();
        assertEquals(81+initial(0).encode().length,bytes.length); assertEquals(0,bytes[72]);
        var copy=HomeRetirement.decode(bytes); assertEquals(retirement,copy); assertNull(copy.consumed()); assertTrue(copy.participants().isEmpty());
        assertEquals(initial(0),copy.identity().definition());
        assertThrows(IllegalArgumentException.class,() -> new HomeRetirement(identity,null,fences()));
        var wrongMode=new HomeRetirement(identity,consumed(),fences()).encode(); wrongMode[72]=0;
        assertThrows(IllegalArgumentException.class,() -> HomeRetirement.decode(wrongMode));
    }

    @Test void consumedModeRequiresEveryExactFenceAfterAnyRecordedPositiveVote() {
        var consumed=consumed(); var identity=identity(consumed);
        assertThrows(IllegalArgumentException.class,() -> new HomeRetirement(identity,consumed,List.of()));
        assertThrows(IllegalArgumentException.class,() -> new HomeRetirement(identity,consumed,List.of(fences().getFirst())));
        assertThrows(IllegalArgumentException.class,() -> new HomeRetirement(identity,consumed,List.of(dataFence(left,3,8),dataFence(left,3,9))));
        var crossedHome=new ParticipantFenceProof(new ParticipantIdentity(home,2,right,initial(0)),2,new ApplyPosition(3,9));
        assertThrows(IllegalArgumentException.class,() -> new HomeRetirement(identity,consumed,List.of(dataFence(left,3,8),crossedHome)));
        var crossedBusiness=new ParticipantFenceProof(new ParticipantIdentity(home,1,right,initial(1)),2,new ApplyPosition(3,9));
        assertThrows(IllegalArgumentException.class,() -> new HomeRetirement(identity,consumed,List.of(dataFence(left,3,8),crossedBusiness)));
        assertThrows(IllegalArgumentException.class,() -> new HomeRetirement(identity,consumed,List.of(dataFence(left,1,3),dataFence(right,3,9))));
        assertThrows(IllegalArgumentException.class,() -> new HomeRetirement(identity,consumed,List.of(dataFence(left,3,8),dataFence(right,1,9))));
        assertThrows(IllegalArgumentException.class,() -> new HomeRetirement(identity,consumed.terminalDecision(),fences()));
        assertThrows(IllegalArgumentException.class,() -> new HomeRetirement(identity(initial(1)),consumed,fences()));
    }

    @Test void retirementCodecRejectsNoncanonicalGroupOrderAndMalformedBodies() {
        var retirement=new HomeRetirement(identity(consumed()),consumed(),fences()); byte[] bytes=retirement.encode(); int proofs=81+consumed().encode().length;
        byte[] reversed=bytes.clone(); System.arraycopy(bytes,proofs+40,reversed,proofs,40); System.arraycopy(bytes,proofs,reversed,proofs+40,40);
        assertThrows(IllegalArgumentException.class,() -> HomeRetirement.decode(reversed));
        byte[] duplicate=bytes.clone(); System.arraycopy(bytes,proofs,duplicate,proofs+40,40);
        assertThrows(IllegalArgumentException.class,() -> HomeRetirement.decode(duplicate));
        byte[] badMode=bytes.clone(); badMode[72]=2; assertThrows(IllegalArgumentException.class,() -> HomeRetirement.decode(badMode));
        byte[] badCount=bytes.clone(); ByteBuffer.wrap(badCount).putInt(proofs-4,1); assertThrows(IllegalArgumentException.class,() -> HomeRetirement.decode(badCount));
        assertThrows(IllegalArgumentException.class,() -> HomeRetirement.decode(Arrays.copyOf(bytes,bytes.length+1)));
        for (int n=0;n<bytes.length;n++) { byte[] truncated=Arrays.copyOf(bytes,n); assertThrows(IllegalArgumentException.class,() -> HomeRetirement.decode(truncated)); }
    }

    @Test void homeFenceCoveragePreservesTheExactPlacementAndOriginalBusinessAnchor() {
        var identity=identity(consumed()); var proof=new HomeFenceProof(identity,2,new ApplyPosition(2,7));
        assertEquals(identity(initial(0)),identity); assertEquals(identity,HomeIdentity.decode(identity.encode()));
        assertEquals(proof,HomeFenceProof.decode(proof.encode())); assertTrue(proof.covers(proof));
        var advanced=new HomeFenceProof(identity,3,new ApplyPosition(3,8)); assertTrue(advanced.covers(proof)); assertFalse(proof.covers(advanced));
        assertFalse(new HomeFenceProof(identity,2,new ApplyPosition(3,8)).covers(advanced));
        assertFalse(new HomeFenceProof(identity,3,new ApplyPosition(3,7)).covers(proof));
        assertFalse(new HomeFenceProof(identity,3,new ApplyPosition(3,6)).covers(proof));
        assertFalse(new HomeFenceProof(identity,3,new ApplyPosition(1,8)).covers(proof));
        var otherPlacement=new HomeIdentity(home,2,new byte[32],column,initial(0));
        assertFalse(new HomeFenceProof(otherPlacement,3,new ApplyPosition(3,8)).covers(proof));
        assertFalse(new HomeFenceProof(identity(initial(1)),3,new ApplyPosition(3,8)).covers(proof)); assertFalse(proof.covers(null));
        assertThrows(IllegalArgumentException.class,() -> new HomeFenceProof(identity,1,new ApplyPosition(2,7)));
        assertThrows(IllegalArgumentException.class,() -> new HomeFenceProof(identity,2,new ApplyPosition(0,7)));
    }

    @Test void retiredRecoveryResultCarriesOnlyAbsenceProofAndNeverAnOriginalReceipt() {
        var proof=new HomeFenceProof(identity(initial(0)),2,new ApplyPosition(1,0));
        var retired=HomeRecoveryResult.decode(HomeRecoveryResult.retired(proof).encode());
        assertTrue(retired.retired()); assertEquals(proof,retired.retirement()); assertNull(retired.receipt()); assertEquals(0,retired.retirement().position().index());
        var before=initial(0); var abort=before.decide(TransactionStatus.Phase.ABORT);
        byte[] key=ByteBuffer.allocate(16).putLong(transaction.getMostSignificantBits()).putLong(transaction.getLeastSignificantBits()).array();
        var command=new DistributedCommand(home,1,new byte[32],new OperationId(new UUID(0,10),0,0),List.of(new Mutation(Mutation.Kind.PUT,column,key,abort.encode())),List.of(new KeyPredicate(column,key,before.encode())));
        var receipt=new HomeRecoveryReceipt(new HomeRecovery(HomeRecovery.Action.ABORT,command,null),new ApplyPosition(2,7),new CommandResult(CommandResult.Status.SUCCESS,List.of()));
        var applied=HomeRecoveryResult.decode(HomeRecoveryResult.applied(receipt).encode());
        assertFalse(applied.retired()); assertEquals(receipt,applied.receipt()); assertNull(applied.retirement());
        assertThrows(IllegalArgumentException.class,() -> new HomeRecoveryResult(receipt,proof)); assertThrows(IllegalArgumentException.class,() -> new HomeRecoveryResult(null,null));
        byte[] badKind=retired.encode(); badKind[0]=2; assertThrows(IllegalArgumentException.class,() -> HomeRecoveryResult.decode(badKind));
        assertThrows(IllegalArgumentException.class,() -> HomeRecoveryResult.decode(new byte[0]));
    }
}
