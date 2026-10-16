package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Public plan routing and native predicate-only 2PC; no process/network/quorum fixture. */
class RatisTransactionPlanTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotCoordinatorTest helper=new RatisSnapshotCoordinatorTest();
    RatisSnapshotCoordinatorTest.Fixture fixture(String name) throws Exception { helper.root=root; return helper.new Fixture(name); }
    OperationId operation(long n) { return new OperationId(new UUID(0,10003),1,n); }
    @Test void fromOriginalPreservesAckAndReturnPositionsAndRejectsAcceptedRecapture()throws Exception {
        try(var f=fixture("original-ack")) {
            var original=new DistributedCommand(new UUID(0,12000),1,f.directory.schemaDigest(),operation(2048),List.of(new Mutation(Mutation.Kind.PUT,helper.column,new byte[]{(byte)130},new byte[]{8},Mutation.ReturnMode.CURRENT),new Mutation(Mutation.Kind.PUT,helper.column,new byte[]{1},new byte[]{7},Mutation.ReturnMode.PREVIOUS)),List.of(),1024);
            var plan=RatisTransactionPlan.fromOriginal(f.directory,original);assertEquals(2,plan.partitions().size());
            for(var part:plan.partitions()){assertEquals(1024,part.command().acknowledgedThrough());assertEquals(original.operationId(),part.command().operationId());assertEquals(part.route().groupId().equals(f.left.f.groupId) ? List.of(1) : List.of(0),part.resultPositions());}
            assertArrayEquals(RatisAtomicClient.rawPlanDigest(plan),RatisOperationRerouteAdmissionRpc.normalizedDigest(original,f.directory));
            var unacked=new DistributedCommand(original.groupId(),1,original.schemaDigest(),original.operationId(),original.mutations(),original.predicates());assertFalse(Arrays.equals(RatisAtomicClient.rawPlanDigest(plan),RatisOperationRerouteAdmissionRpc.normalizedDigest(unacked,f.directory)));
            var accepted=DistributedCommand.acceptedTransaction(original,new TransactionAcceptance(new ApplyPosition(1,0),0,new UUID(0,12001),new TransactionScope(List.of(TransactionScope.Span.point(helper.column,new byte[]{1}),TransactionScope.Span.point(helper.column,new byte[]{(byte)130}))),new CommandResult(CommandResult.Status.SUCCESS,Arrays.asList(new byte[]{8},null)).encode()));assertThrows(IllegalArgumentException.class,()->RatisTransactionPlan.fromOriginal(f.directory,accepted));assertEquals(0,f.writes());
        }
    }
    @Test void partitionsPreserveReturnOrderAndClipTombstonesAtShardBoundsBeforeAnyRpc() throws Exception {
        try (var f=fixture("partition")) {
            var plan=f.data.transactionPlan(operation(0),List.of(new Mutation(Mutation.Kind.PUT,helper.column,new byte[]{(byte)130},new byte[]{8},Mutation.ReturnMode.CURRENT),new Mutation(Mutation.Kind.PUT,helper.column,new byte[]{1},new byte[]{7},Mutation.ReturnMode.PREVIOUS),new Mutation(Mutation.Kind.PUT,helper.column,new byte[]{(byte)131},new byte[0],Mutation.ReturnMode.CURRENT),new Mutation(Mutation.Kind.DELETE_RANGE,helper.column,new byte[]{0},new byte[]{(byte)255})),List.of(new KeyPredicate(helper.column,new byte[]{2},null)));
            assertEquals(0,f.writes()); assertEquals(2,plan.partitions().size());
            for (var part:plan.partitions()) {
                boolean left=part.route().groupId().equals(f.left.f.groupId); assertEquals(left ? List.of(1) : List.of(0,2),part.resultPositions());
                var deletion=part.command().mutations().getLast(); assertEquals(Mutation.Kind.DELETE_RANGE,deletion.kind());
                assertArrayEquals(new byte[]{(byte)(left ? 0 : 128)},deletion.key()); assertArrayEquals(new byte[]{(byte)(left ? 128 : 255)},deletion.value());
                assertTrue(part.scope().containsRange(helper.column,deletion.key(),deletion.value()));
            }
            assertThrows(IllegalArgumentException.class,() -> RatisTransactionPlan.create(List.of(f.directory),operation(0),List.of(),List.of()));
        }
    }
    @Test void remoteAcceptanceFreezesACompleteCaptureSetAndLocalAuthorityRemainsExplicit() throws Exception {
        try (var f=fixture("capture-map")) {
            var plan=f.data.transactionPlan(operation(0),List.of(new Mutation(Mutation.Kind.PUT,helper.column,new byte[]{1},new byte[]{7},Mutation.ReturnMode.CURRENT),
                    new Mutation(Mutation.Kind.PUT,helper.column,new byte[]{(byte)130},new byte[]{8},Mutation.ReturnMode.CURRENT)),List.of());
            var accepted=new HashMap<UUID,DistributedCommand>(); var prepares=new HashMap<UUID,OperationId>(); var finalizes=new HashMap<UUID,OperationId>(); var ids=new HashMap<UUID,UUID>();
            int index=0;
            for (var part:plan.partitions()) try (var pin=f.node(part.route().groupId()).f.store.captureTracked(part.scope())) {
                var group=part.route().groupId(); accepted.put(group,f.node(group).f.machine.participantIntent(pin,part.command()));
                prepares.put(group,helper.control(100+index)); finalizes.put(group,helper.control(110+index)); ids.put(group,new UUID(0,10100+index++));
            }
            var transaction=new UUID(0,10110); var frozen=plan.accept(transaction,accepted,prepares,finalizes,ids);
            for (var intent:frozen.intents()) assertEquals(ids.get(intent.route().groupId()),intent.captureId());
            var local=plan.accept(transaction,accepted,prepares,finalizes); assertTrue(local.intents().stream().allMatch(intent -> intent.captureId()==null));
            assertFalse(Arrays.equals(local.initial().planDigest(),frozen.initial().planDigest()));
            assertThrows(IllegalArgumentException.class,() -> plan.accept(transaction,accepted,prepares,finalizes,Map.of()));
            var missing=new HashMap<>(ids); missing.remove(plan.partitions().getFirst().route().groupId());
            assertThrows(IllegalArgumentException.class,() -> plan.accept(transaction,accepted,prepares,finalizes,missing));
            var extra=new HashMap<>(ids); extra.put(new UUID(0,10111),new UUID(0,10112));
            assertThrows(IllegalArgumentException.class,() -> plan.accept(transaction,accepted,prepares,finalizes,extra));
            var nullId=new HashMap<>(ids); nullId.put(plan.partitions().getFirst().route().groupId(),null);
            assertThrows(NullPointerException.class,() -> plan.accept(transaction,accepted,prepares,finalizes,nullId));
            UUID group=plan.partitions().getFirst().route().groupId(); UUID original=ids.put(group,new UUID(0,10113));
            assertEquals(original,frozen.intents().stream().filter(intent -> intent.route().groupId().equals(group)).findFirst().orElseThrow().captureId());
            assertFalse(Arrays.equals(frozen.initial().planDigest(),plan.accept(transaction,accepted,prepares,finalizes,ids).initial().planDigest())); assertEquals(0,f.writes());
        }
    }
    @Test void predicateOnlyParticipantLocksItsReadAndContributesNoSpuriousResult() throws Exception {
        try (var f=fixture("read-only")) {
            var seed=new Mutation(Mutation.Kind.PUT,helper.column,new byte[]{1},new byte[]{2}); f.data.execute(f.data.prepare(operation(0),List.of(seed)));
            var plan=f.data.transactionPlan(operation(1),List.of(new Mutation(Mutation.Kind.PUT,helper.column,new byte[]{(byte)130},new byte[]{8},Mutation.ReturnMode.CURRENT)),List.of(new KeyPredicate(helper.column,new byte[]{1},new byte[]{2})));
            var pins=new HashMap<UUID,ReplicatedRangeStore.ScopedHistoryPin>();
            try {
                var accepted=new HashMap<UUID,DistributedCommand>(); var prepares=new HashMap<UUID,OperationId>(); var finalizes=new HashMap<UUID,OperationId>(); int index=0;
                for (var part:plan.partitions()) {
                    var group=part.route().groupId(); var pin=f.node(group).f.store.captureTracked(part.scope()); pins.put(group,pin);
                    accepted.put(group,f.node(group).f.machine.participantIntent(pin,part.command())); prepares.put(group,helper.control(60+index)); finalizes.put(group,helper.control(70+index++));
                }
                var frozen=plan.accept(new UUID(0,10004),accepted,prepares,finalizes);
                Path file=root.resolve("read-only.journal");
                try (var journal=RatisDurableTransaction.create(file,f.transactions.statuses,RatisTransactionBindings.of(f.data),frozen.initial(),f.transactions.statuses.prepare(helper.control(80),null,frozen.initial()),frozen.intents());
                        var coordinator=new RatisTransactionCoordinator(journal,f.transactions.statuses,RatisTransactionBindings.of(f.data),last -> helper.control(last.sequence()+1),(tx,progress) -> {
                            try {
                                var part=plan.partitions().stream().filter(p -> p.route().groupId().equals(progress.intent().route().groupId())).findFirst().orElseThrow();
                                var token=f.node(part.route().groupId()).f.machine.prepareParticipant(pins.get(part.route().groupId()),part.command(),progress.intent().prepareControl(),f.transactions.statuses,tx);
                                return new RatisTransactionCoordinator.Authorization(token.preparation(),token.negative(),token);
                            } catch (Exception failure) { throw new IOException(failure); }
                        })) {
                    assertThrows(IllegalStateException.class,journal::result); boolean complete=false,locked=false;
                    for (int n=0;n<32 && !complete;n++) {
                        complete=coordinator.step().complete();
                        var progress=journal.snapshot().participant(f.left.f.groupId);
                        if (!locked && progress.vote()!=null && progress.vote().prepared() && progress.outcome()==null) {
                            assertThrows(IOException.class,() -> f.data.read(helper.column,new byte[]{1})); locked=true;
                            assertEquals(0,progress.intent().acceptedBusiness().mutations().size()); assertTrue(progress.intent().resultPositions().isEmpty());
                        }
                    }
                    assertTrue(complete); assertTrue(locked); var result=journal.result(); assertEquals(CommandResult.Status.SUCCESS,result.status()); assertEquals(1,result.valueCount()); assertArrayEquals(new byte[]{8},result.value(0));
                    assertFalse(journal.snapshot().observedHome().consumed()); assertArrayEquals(new byte[]{2},f.data.read(helper.column,new byte[]{1}));
                }
                try (var reopened=RatisDurableTransaction.reopen(file,f.transactions.statuses,RatisTransactionBindings.of(f.data))) { assertArrayEquals(new byte[]{8},reopened.result().value(0)); assertFalse(reopened.snapshot().observedHome().consumed()); }
            } finally { for (var pin:pins.values()) pin.close(); }
        }
    }
}
