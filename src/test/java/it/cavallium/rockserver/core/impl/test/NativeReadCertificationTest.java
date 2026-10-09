package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NativeReadCertificationTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;
    final NativePreparedParticipantTest helper=new NativePreparedParticipantTest();
    final NativeBucketApplyTest outer=helper.outer;
    final UUID incarnation=new UUID(0,99);
    @Test void originalAbsenceAndRangeReadScopesRejectInsertionAndPhantomsWithoutFabricatedPredicates() throws Exception {
        for (boolean range:new boolean[]{false,true}) try (var f=outer.new Fixture(root.resolve("conflict-"+range),true)) {
            helper.enable(f); var scope=range ? new TransactionScope(List.of(TransactionScope.Span.range(outer.fixed,helper.key(1),helper.key(4)))) : helper.point(outer.fixed,2);
            try (var pin=f.store.captureTracked(scope)) {
                assertNull(pin.get(outer.fixed,helper.key(2))); var accepted=f.store.planReadCertification(pin,new OperationId(outer.session,1,10),0,incarnation); assertTrue(accepted.readCertification()); assertTrue(accepted.mutations().isEmpty()); assertTrue(accepted.predicates().isEmpty()); assertEquals(0,CommandResult.decode(accepted.acceptance().result()).valueCount()); assertEquals(pin.position(),accepted.acceptance().snapshot());
                f.apply(1,helper.put(f,0,2,8)); assertThrows(IllegalStateException.class,() -> f.store.planReadCertification(pin,new OperationId(outer.session,1,10),0,incarnation)); assertEquals(RangeMutationHistory.Status.CONFLICT,pin.status());
            }
            assertEquals(0,f.store.activeTrackedSnapshotPins());
        }
    }
    @Test void noEffectsParticipantRetainsReadLockAndCommitsAfterReopenWithoutChangingUserData() throws Exception {
        var source=root.resolve("prepared-read"); PreparedParticipant.Preparation preparation; ApplyPosition vote;
        try (var f=outer.new Fixture(source,true)) {
            helper.enable(f); f.apply(1,helper.put(f,0,1,7)); DistributedCommand accepted;
            try (var pin=f.store.captureTracked(helper.point(outer.fixed,1))) { accepted=f.store.planReadCertification(pin,new OperationId(outer.session,1,10),3,incarnation); }
            var registration=TransactionStatus.begin(outer.cluster,outer.database,outer.domain,new UUID(0,501),accepted.operationId(),1,f.schema.digest(),new byte[32],List.of(new TransactionStatus.Participant(outer.group,outer.group,1,f.schema.digest(),accepted.payloadDigest(),null,false)));
            preparation=new PreparedParticipant.Preparation(new UUID(0,100),1,new OperationId(new UUID(0,101),1,0),registration,accepted);
            assertNull(f.store.estimatePreparation(preparation).existing());
            vote=f.store.applyPrepared(new ApplyPosition(1,2),preparation).position(); assertEquals(1,f.store.preparedCapacity().count()); assertTrue(f.store.preparedCapacity().scopeBytes()>0);
            assertThrows(IllegalStateException.class,() -> f.apply(3,helper.put(f,1,1,9))); assertArrayEquals(helper.key(7),f.raw(outer.fixed,helper.key(1)));
        }
        try (var f=outer.new Fixture(source,true)) {
            assertEquals(preparation,f.store.preparedParticipant(preparation.transactionId()).preparation());
            var decision=preparation.registration().prepared(outer.group,preparation.business().payloadDigest(),vote).decide(TransactionStatus.Phase.COMMIT); var finalization=new ParticipantFinalization(preparation.homeGroup(),preparation.homeEpoch(),outer.group,new OperationId(new UUID(0,102),1,0),decision);
            var applied=f.store.applyFinalization(new ApplyPosition(1,3),finalization); assertEquals(new ApplyPosition(1,3),applied.position()); assertEquals(applied,f.store.applyFinalization(new ApplyPosition(1,4),finalization)); assertArrayEquals(helper.key(7),f.raw(outer.fixed,helper.key(1)));
            var outcome=f.store.finalizedParticipant(preparation.transactionId()); assertEquals(CommandResult.Status.SUCCESS,CommandResult.decode(outcome.result()).status()); assertEquals(0,CommandResult.decode(outcome.result()).valueCount());
        }
    }
    @Test void canonicalCodecAllowsOnlyZeroResultAckFreeReadCertificationAndKeepsPlainEmptyDataInvalid() throws Exception {
        try (var f=outer.new Fixture(root.resolve("codec"),true)) {
            helper.enable(f); try (var pin=f.store.captureTracked(helper.point(outer.fixed,1))) {
                var accepted=f.store.planReadCertification(pin,new OperationId(outer.session,1,10),0,incarnation); var encoded=DistributedCommandCodec.encode(accepted); var decoded=DistributedCommandCodec.decode(encoded); assertTrue(decoded.readCertification()); assertArrayEquals(encoded,DistributedCommandCodec.encode(decoded));
                var acknowledged=encoded.clone(); java.nio.ByteBuffer.wrap(acknowledged).putLong(93,0); assertThrows(IllegalArgumentException.class,() -> DistributedCommandCodec.decode(acknowledged));
                var ackOnly=new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,12),List.of(),List.of(),0); assertFalse(ackOnly.readCertification()); assertArrayEquals(DistributedCommandCodec.encode(ackOnly),DistributedCommandCodec.encode(DistributedCommandCodec.decode(DistributedCommandCodec.encode(ackOnly))));
                assertThrows(IllegalArgumentException.class,() -> new DistributedCommand(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,11),List.of()));
                var wrong=new TransactionAcceptance(pin.position(),0,incarnation,pin.scope(),new CommandResult(CommandResult.Status.SUCCESS,List.of(new byte[]{1})).encode()); assertThrows(IllegalArgumentException.class,() -> DistributedCommand.readCertification(outer.group,1,f.schema.digest(),new OperationId(outer.session,1,11),wrong));
            }
        }
    }
}
