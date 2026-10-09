package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class NativeSchemaTerminalProofTest {
    @TempDir(factory=DiskTestDirectory.class) Path root;

    @Test void finalizedPrepareAndDecisionRetryAfterColumnDeletionAndReopen() throws Exception {
        for (var phase:List.of(TransactionStatus.Phase.COMMIT,TransactionStatus.Phase.ABORT)) {
            var helper=new NativeSchemaInstallationTest(); var source=helper.source(); var path=root.resolve(phase.name());
            var target=new DistributedSchema(source.clusterId(),source.databaseId(),source.domainId(),1,0,List.of());
            var change=helper.change(source,target,List.of(helper.descriptor(helper.group,null,null)));
            var limits=new NativeRangeImage.Limits(4*1024*1024,32*1024*1024,256);
            var image=root.resolve(phase.name()+".image");
            PreparedParticipant.Preparation preparation; ParticipantFinalization finalization; ParticipantVote vote; ParticipantOutcome outcome;
            try (var pool=helper.open(path)) {
                var store=helper.store(pool,helper.group,source,null,null); store.enableLogicalSchema(source);
                store.initializeConfiguration(new byte[]{1}); store.enableScopedHistory(RangeMutationHistory.Limits.defaults());
                store.applyNoop(new ApplyPosition(1,0));
                var business=new DistributedCommand(helper.group,1,source.digest(),new OperationId(UUID.randomUUID(),1,0),
                        List.of(new Mutation(Mutation.Kind.PUT,helper.column,new byte[]{1},new byte[]{7})));
                var scope=new TransactionScope(List.of(TransactionScope.Span.point(helper.column,new byte[]{1})));
                DistributedCommand accepted;
                try (var pin=store.captureTracked(scope)) { accepted=store.planScopedCommit(pin,business,0,UUID.randomUUID()); }
                var participant=new TransactionStatus.Participant(helper.group,helper.group,1,source.digest(),PreparedParticipant.Preparation.planDigest(accepted),null,false);
                var initial=TransactionStatus.begin(source.clusterId(),source.databaseId(),source.domainId(),UUID.randomUUID(),business.operationId(),
                        source.schemaEpoch(),source.digest(),change.sourceDirectory().payloadDigest(),List.of(participant));
                preparation=new PreparedParticipant.Preparation(UUID.randomUUID(),1,new OperationId(UUID.randomUUID(),1,0),initial,accepted);
                vote=store.applyPrepared(new ApplyPosition(1,1),preparation);
                var decision=initial.prepared(helper.group,participant.planDigest(),vote.position()).decide(phase);
                finalization=new ParticipantFinalization(preparation.homeGroup(),1,helper.group,new OperationId(UUID.randomUUID(),1,0),decision);
                store.applyFinalization(new ApplyPosition(1,2),finalization); outcome=store.finalizedParticipant(initial.transactionId());
                store.applySchemaFence(new ApplyPosition(1,3),change.header());
                pool.installSchema(store,new ApplyPosition(1,4),change,Map.of(),Set.of());
                assertEquals(vote,store.applyPrepared(new ApplyPosition(1,5),preparation));
                store.applyFinalization(new ApplyPosition(1,6),finalization); assertEquals(outcome,store.finalizedParticipant(initial.transactionId()));
                NativeRangeImage.write(image,store,limits); NativeRangeImage.inspect(image,limits);
            }
            try (var pool=helper.open(path)) {
                var recovered=helper.store(pool,helper.group,source,null,null);
                assertEquals(Set.of(),recovered.ownership().interval().columns());
                assertEquals(vote,recovered.applyPrepared(new ApplyPosition(1,7),preparation));
                recovered.applyFinalization(new ApplyPosition(1,8),finalization);
                assertEquals(outcome,recovered.finalizedParticipant(preparation.transactionId()));
                assertThrows(IllegalStateException.class,recovered::capture);
            }
            try (var pool=helper.open(root.resolve(phase.name()+".import"))) {
                var imported=pool.openRange(new RangeOwnership.Owner(helper.group,1),
                        new RangeOwnership.Interval(source.domainId(),Set.of(),null,null),target.digest(),Map.of(),Set.of());
                imported.enableLogicalSchema(target); imported.fence(); imported.installSnapshot(image,limits); imported.activate();
                assertEquals(vote,imported.applyPrepared(new ApplyPosition(1,7),preparation));
                imported.applyFinalization(new ApplyPosition(1,8),finalization);
                assertEquals(outcome,imported.finalizedParticipant(preparation.transactionId()));
                assertThrows(IllegalStateException.class,imported::capture);
            }
        }
    }
}
