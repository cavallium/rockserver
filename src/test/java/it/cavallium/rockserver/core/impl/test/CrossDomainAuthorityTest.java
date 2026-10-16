package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;

class CrossDomainAuthorityTest {
    static UUID id(long n){return new UUID(0,n);}
    static byte[] digest(int n){var value=new byte[32];value[0]=(byte)n;return value;}
    final UUID cluster=id(1),database=id(2),a=id(3),b=id(4),ga=id(5),gb=id(6),column=id(7);
    final DomainBinding alpha=new DomainBinding(a,7,digest(1),2,digest(2)),beta=new DomainBinding(b,8,digest(3),3,digest(4));
    OperationId control(long n){return new OperationId(id(50),1,n);}
    TransactionStatus.Participant participant(DomainBinding binding,UUID range,UUID group){return new TransactionStatus.Participant(binding.domainId(),range,group,1,binding.schemaDigest(),digest(9),null,false);}
    SnapshotFence.Participant snapshot(DomainBinding binding,UUID range,UUID group,long n){return new SnapshotFence.Participant(binding.domainId(),range,group,1,binding.schemaDigest(),control(n),control(n+1),new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{1}))));}
    TransactionStatus status(List<DomainBinding> bindings,List<TransactionStatus.Participant> participants){return TransactionStatus.begin(cluster,database,id(8),control(0),bindings,digest(8),participants);}

    @Test void canonicalVectorAndParticipantQualificationSurviveOneGlobalDecision() {
        var value=status(List.of(beta,alpha),List.of(participant(beta,gb,gb),participant(alpha,ga,ga)));
        assertEquals(List.of(alpha,beta),value.domains());assertEquals(alpha,value.participantDomain(ga));assertEquals(beta,value.participantDomain(gb));assertEquals(value,TransactionStatus.decode(value.encode()));
        var complete=value.prepared(ga,digest(9),new ApplyPosition(1,1)).prepared(gb,digest(9),new ApplyPosition(2,2)).decide(TransactionStatus.Phase.COMMIT);
        for(var group:List.of(ga,gb)){var finalization=new ParticipantFinalization(id(60),1,group,control(group.equals(ga)?30:31),complete);assertEquals(complete.terminalDecision(),ParticipantFinalization.decode(finalization.encode()).decision());assertEquals(2,finalization.decision().domains().size());}
        assertFalse(complete.fullyFinalized());var first=complete.finalized(ga);assertThrows(IllegalArgumentException.class,first::consume);assertTrue(first.finalized(gb).consume().gcEligible());
        var corrupt=value.encode();int start=TransactionStatus.HEADER_BYTES;var firstBinding=Arrays.copyOfRange(corrupt,start,start+DomainBinding.BYTES);System.arraycopy(corrupt,start+DomainBinding.BYTES,corrupt,start,DomainBinding.BYTES);System.arraycopy(firstBinding,0,corrupt,start+DomainBinding.BYTES,DomainBinding.BYTES);assertThrows(IllegalArgumentException.class,()->TransactionStatus.decode(corrupt));
    }
    @Test void oneFenceRequiresAllDomainParticipantsBeforeCompleteAndRelease() {
        var fence=new SnapshotFence(cluster,database,id(10),id(60),1,control(0),List.of(beta,alpha),List.of(snapshot(beta,gb,gb,3),snapshot(alpha,ga,ga,1)));
        assertEquals(List.of(alpha,beta),fence.domains());assertEquals(fence,SnapshotFence.decode(fence.encode()));
        var preparing=SnapshotFenceStatus.begin(fence).ready(ga,new ApplyPosition(1,1));assertFalse(preparing.allReady());assertThrows(IllegalArgumentException.class,preparing::complete);
        var ready=preparing.ready(gb,new ApplyPosition(2,1));assertTrue(ready.allReady());
        var captured=ready.capture(ga,new SnapshotFenceStatus.Capture(id(11),id(12),new ApplyPosition(1,1)));assertThrows(IllegalArgumentException.class,captured::complete);
        var complete=captured.capture(gb,new SnapshotFenceStatus.Capture(id(13),id(14),new ApplyPosition(2,1))).complete();assertEquals(fence,SnapshotFenceStatus.decode(complete.encode()).fence());
        var bytes=fence.encode();int start=SnapshotFence.HEADER_BYTES;var first=Arrays.copyOfRange(bytes,start,start+DomainBinding.BYTES);System.arraycopy(bytes,start+DomainBinding.BYTES,bytes,start,DomainBinding.BYTES);System.arraycopy(first,0,bytes,start+DomainBinding.BYTES,DomainBinding.BYTES);assertThrows(IllegalArgumentException.class,()->SnapshotFence.decode(bytes));
    }
    @Test void domainsCannotHideGlobalIdentityCollisionsOrUnregisteredAndUnusedBindings() {
        var pa=participant(alpha,ga,ga);var pb=participant(beta,gb,gb);
        assertThrows(IllegalArgumentException.class,()->status(List.of(alpha,beta),List.of(pa,participant(beta,ga,gb))));
        assertThrows(IllegalArgumentException.class,()->status(List.of(alpha,beta),List.of(pa,participant(beta,gb,ga))));
        assertThrows(IllegalArgumentException.class,()->status(List.of(alpha),List.of(pa,pb)));
        assertThrows(IllegalArgumentException.class,()->status(List.of(alpha,beta),List.of(pa)));
        assertThrows(IllegalArgumentException.class,()->status(List.of(alpha,alpha),List.of(pa)));
        assertThrows(IllegalArgumentException.class,()->status(List.of(alpha,beta),List.of(pa,new TransactionStatus.Participant(b,gb,gb,1,alpha.schemaDigest(),digest(9),null,false))));
        // Range and group are separate namespaces: typed cross-use remains valid.
        assertDoesNotThrow(()->status(List.of(alpha,beta),List.of(participant(alpha,ga,gb),participant(beta,gb,ga))));
        assertThrows(IllegalArgumentException.class,()->new SnapshotFence(cluster,database,id(10),id(60),1,control(0),List.of(alpha,beta),List.of(snapshot(alpha,ga,ga,1),snapshot(beta,ga,gb,3))));
        var bindings=new ArrayList<DomainBinding>();var participants=new ArrayList<TransactionStatus.Participant>();
        for(int n=0;n<64;n++){var binding=new DomainBinding(id(100+n),n,digest(n),1,digest(0));bindings.add(binding);participants.add(participant(binding,id(200+n),id(300+n)));}
        assertEquals(64,status(bindings,participants).participants().size());bindings.add(new DomainBinding(id(999),0,digest(0),1,digest(0)));participants.add(participant(bindings.getLast(),id(1000),id(1001)));assertThrows(IllegalArgumentException.class,()->status(bindings,participants));
    }
    @Test void reservedOriginAndBothNoEffectAbortsKeepTheExactSingletonTargetBinding() {
        var origin=OperationRerouteOriginTest.origin(2);var target=new DomainBinding(origin.domainId(),7,origin.targetDirectoryDigest(),1,new byte[32]);var reserved=TransactionStatus.reserve(origin,target);
        var other=new DomainBinding(id(999),8,digest(7),1,new byte[32]);var participants=List.of(participant(target,ga,ga),participant(other,gb,gb));
        var attempted=TransactionStatus.begin(origin.clusterId(),origin.databaseId(),origin.transactionId(),origin.operation(),List.of(target,other),digest(8),participants);
        assertThrows(IllegalArgumentException.class,()->reserved.promote(attempted));
        var changed=new DomainBinding(target.domainId(),target.directoryGeneration()+1,target.directoryDigest(),target.schemaEpoch(),target.schemaDigest());
        var changedAccepted=TransactionStatus.begin(origin.clusterId(),origin.databaseId(),origin.transactionId(),origin.operation(),List.of(changed),digest(8),List.of(participant(changed,ga,ga),participant(changed,gb,gb)));assertThrows(IllegalArgumentException.class,()->reserved.promote(changedAccepted));
        assertThrows(IllegalArgumentException.class,()->TransactionStatus.reserve(origin,new DomainBinding(target.domainId(),7,digest(99),1,new byte[32])));
        for(var outcome:List.of(TransactionStatus.NoEffectOutcome.CONFLICT,TransactionStatus.NoEffectOutcome.SESSION_EXPIRED)){var aborted=reserved.abortReservation(outcome);var consumed=aborted.consume();assertEquals(List.of(target),consumed.domains());assertEquals(reserved,consumed.asInitial());assertEquals(consumed,TransactionStatus.decode(consumed.encode()));assertEquals(outcome,consumed.noEffectOutcome());}
        var accepted=TransactionStatus.begin(origin.clusterId(),origin.databaseId(),origin.transactionId(),origin.operation(),List.of(target),digest(8),List.of(participant(target,ga,ga),participant(target,gb,gb)));assertNotEquals(Arrays.toString(origin.planDigest()),Arrays.toString(accepted.planDigest()));assertArrayEquals(accepted.planDigest(),reserved.promote(accepted).planDigest());
    }
}
