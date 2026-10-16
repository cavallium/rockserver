package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.ByteBuffer;
import java.io.IOException;
import java.util.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.*;
import static org.mockito.Mockito.*;

class RatisProtectedLogEntryTest {
    private static final UUID GROUP=new UUID(0,1),COLUMN=new UUID(0,2),OTHER=new UUID(0,3);
    private static final OperationId OP=new OperationId(new UUID(0,4),1,0);
    private static final OperationId ACK=new OperationId(OP.sessionId(),OP.epoch(),1);
    private static final byte[] DIGEST=new byte[32];
    private record Fixture(RatisRangeStateMachine machine,ReplicatedRangeStore store,RatisLogResourceBudget budget) {}
    private static Fixture fixture() {
        var ownership=new RangeOwnership().register(new RangeOwnership.Owner(GROUP,0),new RangeOwnership.Interval(OTHER,Set.of(COLUMN),null,null),RangeOwnership.State.ACTIVE);
        var store=mock(ReplicatedRangeStore.class);when(store.ownership()).thenReturn(ownership);
        var machine=new RatisRangeStateMachine(store,RaftGroup.valueOf(RaftGroupId.valueOf(GROUP),List.of()),new NativeRangeImage.Limits(1<<20,2<<20,1));
        var limits=RatisLogResourceBudget.Limits.defaults();
        var budget=new RatisLogResourceBudget(GROUP,new RatisLogResourceBudget.Shared(limits),new RatisLogResourceBudget.Shared(limits),limits,(bytes,protectedProgress)->{throw new AssertionError("Classification must not reserve disk");});
        machine.logResources(budget);clearInvocations(store);return new Fixture(machine,store,budget);
    }
    private static LogEntryProto entry(byte[] bytes) {
        return LogEntryProto.newBuilder().setTerm(1).setIndex(1).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(bytes))).build();
    }
    private static LogEntryProto entry(DistributedCommand command){return entry(DistributedCommandCodec.encode(command));}
    @Test void ackAndAuthorityAdvanceAreProtectedWithoutNativeReadsButDataAndGrantBirthAreOrdinary() throws Exception {
        var f=fixture();var ack=new DistributedCommand(GROUP,0,DIGEST,ACK,List.of(),List.of(),0);
        assertTrue(f.machine.isProtectedLogEntry(entry(ack)));
        assertTrue(f.machine.isProtectedLogEntry(entry(DistributedCommand.advanceSessionEpochFloor(GROUP,0,DIGEST,OP,1))));
        var ledger=SessionEpochLedger.initial(OTHER,COLUMN,0).issue(OP.sessionId(),1000);
        assertTrue(f.machine.isProtectedLogEntry(entry(DistributedCommand.sessionControl(GROUP,0,DIGEST,OP,RangeSessionControl.advance(ledger),-1))));
        assertFalse(f.machine.isProtectedLogEntry(entry(DistributedCommand.sessionControl(GROUP,0,DIGEST,OP,RangeSessionControl.bind(ledger,ledger.grants().getFirst(),1),-1))));
        assertFalse(f.machine.isProtectedLogEntry(entry(new DistributedCommand(GROUP,0,DIGEST,ACK,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,new byte[]{1},new byte[]{2})),List.of(),0))));
        assertFalse(f.machine.isProtectedLogEntry(entry(new DistributedCommand(OTHER,0,DIGEST,ACK,List.of(),List.of(),0))));
        verifyNoInteractions(f.store);
    }
    @Test void snapshotHomeSuccessorUsesTheCurrentHeaderBeforeProtectedClassification() throws Exception {
        var f=fixture();
        for(var setting:Map.of("role",RatisNodePlan.Role.SNAPSHOT_HOME,"snapshotHomeConfigured",true,"homeColumn",COLUMN,"homeCluster",OTHER,"homeDatabase",COLUMN).entrySet()) {
            var field=RatisRangeStateMachine.class.getDeclaredField(setting.getKey());field.setAccessible(true);field.set(f.machine,setting.getValue());
        }
        var fence=new SnapshotFence(OTHER, COLUMN, new UUID(0,10), GROUP, 0, OP, List.of(new DomainBinding(OTHER, 0, new byte[32], 0, DIGEST)), List.of(new SnapshotFence.Participant(OTHER, COLUMN, OTHER, 0, DIGEST, new OperationId(OP.sessionId(),1,2), new OperationId(OP.sessionId(),1,3), new TransactionScope(List.of(TransactionScope.Span.range(COLUMN,null,null))))));
        var before=SnapshotFenceStatus.begin(fence);var after=before.abort();
        byte[] key=ByteBuffer.allocate(16).putLong(fence.fenceId().getMostSignificantBits()).putLong(fence.fenceId().getLeastSignificantBits()).array();
        var command=new DistributedCommand(GROUP,0,DIGEST,ACK,List.of(new Mutation(Mutation.Kind.PUT,COLUMN,key,after.encode())),List.of(new KeyPredicate(COLUMN,key,before.encode())),-1);
        assertTrue(f.machine.isProtectedLogEntry(entry(command)));
        verifyNoInteractions(f.store);
    }
    @Test void canonicalCommitAndAbortFinalizationsRequireTheExactParticipantGroup() throws Exception {
        var f=fixture();var initial=TransactionStatus.begin(OTHER, COLUMN, new UUID(0,5), OP, List.of(new DomainBinding(OTHER, 0, new byte[32], 0, DIGEST)), DIGEST, List.of(new TransactionStatus.Participant(OTHER, COLUMN, GROUP, 0, DIGEST, DIGEST, null, false)));
        var recovery=new OperationId(OP.sessionId(),1,1);
        for(var decision:List.of(initial.decide(TransactionStatus.Phase.ABORT),initial.prepared(GROUP,DIGEST,new ApplyPosition(1,2)).decide(TransactionStatus.Phase.COMMIT))) {
            var finalization=new ParticipantFinalization(OTHER,0,GROUP,recovery,decision);
            assertTrue(f.machine.isProtectedLogEntry(entry(RatisParticipantRecord.encode(finalization))));
            byte[] malformed=RatisParticipantRecord.encode(finalization);malformed=Arrays.copyOf(malformed,malformed.length-1);
            assertFalse(f.machine.isProtectedLogEntry(entry(malformed)));
        }
        var foreign=TransactionStatus.begin(OTHER, COLUMN, new UUID(0,6), OP, List.of(new DomainBinding(OTHER, 0, new byte[32], 0, DIGEST)), DIGEST, List.of(new TransactionStatus.Participant(OTHER, COLUMN, OTHER, 0, DIGEST, DIGEST, null, false))).decide(TransactionStatus.Phase.ABORT);
        assertFalse(f.machine.isProtectedLogEntry(entry(RatisParticipantRecord.encode(new ParticipantFinalization(COLUMN,0,OTHER,recovery,foreign)))));
        verifyNoInteractions(f.store);
    }
    @Test void malformedUnknownAndExternalPayloadsCannotBorrowProtectedCredit() throws Exception {
        var f=fixture();
        for(int tag:new int[]{0x52535054,0x52534743,0x52534843,RatisTopologyActivationRecord.MAGIC,RatisTopologyRelocationRecord.MAGIC,RatisSchemaActivateRecord.MAGIC,0x53524f55,0x12345678}) {
            assertFalse(f.machine.isProtectedLogEntry(entry(ByteBuffer.allocate(8).putInt(tag).putInt(2).array())));
        }
        var ack=entry(new DistributedCommand(GROUP,0,DIGEST,ACK,List.of(),List.of(),0));
        var external=ack.toBuilder().setStateMachineLogEntry(ack.getStateMachineLogEntry().toBuilder().setStateMachineEntry(StateMachineEntryProto.newBuilder().setStateMachineData(ByteString.copyFrom(new byte[]{1})))).build();
        assertFalse(f.machine.isProtectedLogEntry(external));assertFalse(f.machine.isProtectedLogEntry(LogEntryProto.getDefaultInstance()));
        verifyNoInteractions(f.store);
    }
    @Test void scratchIsChargedBeforeTheFullCopyAndRefusalDoesNotReinterpretTheControl() throws Exception {
        var f=fixture();var ack=new DistributedCommand(GROUP,0,DIGEST,ACK,List.of(),List.of(),0);
        var wire=spy(ByteString.copyFrom(DistributedCommandCodec.encode(ack)));
        doAnswer(invocation->{assertEquals(1,f.budget.snapshot().classificationOwners());assertEquals(65536+16L*DistributedCommandCodec.HEADER_BYTES,f.budget.snapshot().decodeBytes());return invocation.callRealMethod();}).when(wire).toByteArray();
        var entry=LogEntryProto.newBuilder().setTerm(1).setIndex(1).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(wire)).build();
        assertTrue(f.machine.isProtectedLogEntry(entry));verify(wire).toByteArray();assertEquals(RatisLogResourceBudget.Usage.ZERO,f.budget.snapshot());
        clearInvocations(wire);
        try(var occupied=f.budget.reserveClassification(RatisLogResourceBudget.Limits.defaults().decodeBytes())) {
            assertThrows(IOException.class,()->f.machine.isProtectedLogEntry(entry));verify(wire,never()).toByteArray();assertEquals(1,f.budget.snapshot().classificationOwners());
        }
        assertEquals(RatisLogResourceBudget.Usage.ZERO,f.budget.snapshot());verifyNoInteractions(f.store);
    }
    @Test void largeUnknownAndFreshSourceCreditReturnFromTheBoundedPrefixWithoutFullCopy() throws Exception {
        var f=fixture();
        for(int tag:new int[]{0x12345678,0x53524f55}) {
            var bytes=new byte[DistributedCommandCodec.MAX_ENCODED_BYTES];ByteBuffer.wrap(bytes).putInt(tag).putInt(RatisSourceOutcomeRecord.Kind.CREDIT.ordinal());
            var wire=spy(ByteString.copyFrom(bytes));
            var entry=LogEntryProto.newBuilder().setTerm(1).setIndex(1).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(wire)).build();
            assertFalse(f.machine.isProtectedLogEntry(entry));verify(wire,never()).toByteArray();assertEquals(RatisLogResourceBudget.Usage.ZERO,f.budget.snapshot());
        }
        verifyNoInteractions(f.store);
    }
}
