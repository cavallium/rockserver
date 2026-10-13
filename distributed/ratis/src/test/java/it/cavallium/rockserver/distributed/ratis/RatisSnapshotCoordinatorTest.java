package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Two native groups with controlled dispatch; no network, quorum or process acceptance. */
class RatisSnapshotCoordinatorTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotAdmissionTest helper=new RatisSnapshotAdmissionTest();
    final UUID column=new UUID(0,9010),domain=new UUID(0,9011),tx=new UUID(0,9012);
    OperationId control(long n) { return new OperationId(new UUID(0,9013),3,n); }
    final class Fixture implements AutoCloseable {
        final RatisSnapshotAdmissionTest.Home home;
        final RatisParticipantPrepareTest.Node transactions,left,right;
        final RangeDirectory directory;
        final RatisRoutedClient data;
        final SnapshotFence fence;
        final AtomicLong clock=new AtomicLong();
        final AtomicInteger captures=new AtomicInteger(),helped=new AtomicInteger();
        boolean loseCapture;
        RatisParticipantRecoveryRegistry registry;
        RatisSnapshotReadinessHelper.Recovery recovery;
        Fixture(String name) throws Exception {
            helper.root=root; helper.helper.root=root; home=helper.new Home(name+"-snap-home"); transactions=helper.helper.new Node(name+"-tx-home",true);
            helper.helper.snapshotRecovery=(group,prepared) -> recovery.step(group,prepared);
            byte[] split={(byte)128};
            left=helper.helper.new Node(name+"-left",false,new RangeOwnership.Interval(domain,Set.of(column),null,split),home.homes,clock::get);
            right=helper.helper.new Node(name+"-right",false,new RangeOwnership.Interval(domain,Set.of(column),split,null),home.homes,clock::get);
            var l=route(left,null,split); var r=route(right,split,null);
            directory=new RangeDirectory(helper.helper.cluster,helper.helper.database,domain,1,new byte[32],0,Set.of(column),0,List.of(l,r));
            fence=new SnapshotFence(directory.clusterId(),directory.databaseId(),domain,new UUID(0,9014),home.f.groupId,1,new OperationId(new UUID(0,9014),1,0),0,1,new byte[32],List.of(
                    new SnapshotFence.Participant(l.rangeId(),l.groupId(),1,new byte[32],control(0),control(1),scope(1)),
                    new SnapshotFence.Participant(r.rangeId(),r.groupId(),1,new byte[32],control(2),control(3),new TransactionScope(List.of(TransactionScope.Span.range(column,new byte[]{(byte)130},new byte[]{(byte)134}))))));
            data=new RatisRoutedClient(directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID col,byte[] key) throws IOException { return node(route.groupId()).wire.read(route,col,key); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return node(route.groupId()).wire.command(route,command); }
                public byte[] participant(RangeDescriptor route,byte[] bytes) throws IOException { return node(route.groupId()).wire.participant(route,bytes); }
                public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException { return node(route.groupId()).wire.transaction(route,bytes,fresh); }
                public java.util.concurrent.CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] bytes) throws IOException {
                    var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);
                    if(packet.kind()==ScopedTransactionRpc.SNAPSHOT_CAPTURE){var state=home.homes.read(fence.fenceId());assertTrue(state.allReady());if(!state.terminal())assertFalse(left.f.store.snapshotGate(fence.fenceId()).released()!=null || right.f.store.snapshotGate(fence.fenceId()).released()!=null);captures.incrementAndGet();}
                    if(owner!=null)assertEquals(route.replicas().getFirst(),owner);var machine=node(route.groupId()).f.machine;var message=Message.valueOf(ByteString.copyFrom(bytes));return (owner==null?machine.queryLinearizable(message):machine.queryStale(message,0)).thenApply(reply->{if(loseCapture&&packet.kind()==ScopedTransactionRpc.SNAPSHOT_CAPTURE){loseCapture=false;throw new java.util.concurrent.CompletionException(new IOException("Lost published multi-group capture reply"));}return reply.getContent().toByteArray();});
                }
            });
            registry=new RatisParticipantRecoveryRegistry(java.nio.file.Files.createDirectory(root.resolve(name+"-recoveries")), transactions.statuses, Map.of(data.directory().domainId(), data), group -> data.directory(), false, Set.of(left.f.groupId,right.f.groupId), RatisParticipantRecoveryRegistry.Limits.defaults());
            recovery=(group,prepared) -> { assertEquals(0,captures.get()); boolean progress=registry.step(group,prepared); if (progress) helped.incrementAndGet(); return progress; };
        }
        RangeDescriptor route(RatisParticipantPrepareTest.Node node,byte[] lower,byte[] upper) {
            var original=node.directory.ranges().getFirst(); return new RangeDescriptor(original.rangeId(),original.groupId(),1,0,lower,upper,RangeDescriptor.TopologyState.ACTIVE,original.replicas());
        }
        RatisParticipantPrepareTest.Node node(UUID group) { if (group.equals(left.f.groupId)) return left; if (group.equals(right.f.groupId)) return right; throw new AssertionError("Unknown group"); }
        TransactionScope scope(int key) { return new TransactionScope(List.of(TransactionScope.Span.point(column,new byte[]{(byte)key}))); }
        DistributedCommand put(RatisParticipantPrepareTest.Node node,int key,int value,long sequence) {
            return new DistributedCommand(node.f.groupId,1,new byte[32],new OperationId(new UUID(0,9015),1,sequence),List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{(byte)key},new byte[]{(byte)value})));
        }
        void committedTransactionWithRightStillPrepared() throws Exception {
            var lb=put(left,1,7,0); var rb=put(right,130,8,0);
            try (var lp=left.f.store.captureTracked(scope(1)); var rp=right.f.store.captureTracked(scope(130))) {
                var la=left.f.machine.participantIntent(lp,lb); var ra=right.f.machine.participantIntent(rp,rb);
                var initial=TransactionStatus.begin(directory.clusterId(),directory.databaseId(),domain,tx,lb.operationId(),1,new byte[32],new byte[32],List.of(
                        new TransactionStatus.Participant(left.f.groupId,left.f.groupId,1,new byte[32],la.payloadDigest(),null,false),
                        new TransactionStatus.Participant(right.f.groupId,right.f.groupId,1,new byte[32],ra.payloadDigest(),null,false)));
                txHome(null,initial,10); TransactionStatus voted;
                try (var l=left.f.machine.prepareParticipant(lp,lb,control(4),transactions.statuses,tx); var r=right.f.machine.prepareParticipant(rp,rb,control(5),transactions.statuses,tx)) {
                    var lv=data.executeParticipant(data.restoreParticipant(l.preparation(),directory.ranges().getFirst()));
                    var rv=data.executeParticipant(data.restoreParticipant(r.preparation(),directory.ranges().getLast()));
                    voted=initial.prepared(left.f.groupId,la.payloadDigest(),lv.position()); txHome(initial,voted,11);
                    var both=voted.prepared(right.f.groupId,ra.payloadDigest(),rv.position()); txHome(voted,both,12); voted=both;
                }
                txHome(voted,voted.decide(TransactionStatus.Phase.COMMIT),13); finalize(left,control(6));
            }
            assertNotNull(left.f.store.committedParticipant(tx));
        }
        UUID anotherPreparedRight() throws Exception {
            UUID other=new UUID(0,9016); var business=put(right,131,6,3);
            try (var pin=right.f.store.captureTracked(scope(131))) {
                var accepted=right.f.machine.participantIntent(pin,business);
                var initial=TransactionStatus.begin(directory.clusterId(),directory.databaseId(),domain,other,business.operationId(),1,new byte[32],new byte[32],List.of(
                        new TransactionStatus.Participant(right.f.groupId,right.f.groupId,1,new byte[32],accepted.payloadDigest(),null,false)));
                txHome(null,initial,30);
                try (var proposal=right.f.machine.prepareParticipant(pin,business,control(31),transactions.statuses,other)) {
                    data.executeParticipant(data.restoreParticipant(proposal.preparation(),directory.ranges().getLast()));
                }
            }
            return other;
        }
        void txHome(TransactionStatus before,TransactionStatus after,long n) throws Exception { assertEquals(CommandResult.Status.SUCCESS,transactions.statuses.execute(transactions.statuses.prepare(control(n),before,after)).status()); }
        void finalize(RatisParticipantPrepareTest.Node node,OperationId operation) throws Exception {
            try (var proposal=node.f.machine.prepareFinalization(transactions.statuses,tx,operation)) {
                data.executeFinalization(data.restoreFinalization(proposal.finalization(),directory.ranges().stream().filter(r -> r.groupId().equals(node.f.groupId)).findFirst().orElseThrow()));
            }
        }
        RatisSnapshotCoordinator driver(RatisDurableSnapshot journal) throws Exception {
            return new RatisSnapshotCoordinator(journal,home.homes,data,last -> new OperationId(last.sessionId(),last.epoch(),last.sequence()+1),UUID::randomUUID,Duration.ofSeconds(10));
        }
        long writes() { return home.wire.index+transactions.wire.index+left.wire.index+right.wire.index; }
        @Override public void close() throws Exception { registry.close(); data.close(); right.close(); left.close(); transactions.close(); home.close(); }
    }
    RatisSnapshotReadinessHelper readiness(SnapshotFence fence,RatisRoutedClient data,UUID group,RatisSnapshotReadinessHelper.Recovery recovery) {
        return new RatisSnapshotReadinessHelper(fence,group,(after,limit) -> data.snapshotBlockers(fence,group,after,limit),transaction -> data.preparedParticipant(group,transaction),recovery);
    }
    @Test void compactBlockerCodecOwnsDigestsAndRejectsMalformedCutsOrderingAndCursors() throws Exception {
        try (var f=new Fixture("codec")) {
            var binding=ScopedTransactionRpc.SnapshotBinding.of(f.fence); var group=f.right.f.groupId; byte[] digest=new byte[32];
            var reference=new RatisSnapshotBlockers.Reference(tx,digest,new ApplyPosition(1,1)); Arrays.fill(digest,(byte)1); assertArrayEquals(new byte[32],reference.digest());
            var page=new RatisSnapshotBlockers(binding,group,new ApplyPosition(1,2),new ApplyPosition(1,3),List.of(reference),true);
            assertEquals(page,RatisSnapshotBlockers.decode(page.encode()));
            for (int n=0;n<page.encode().length;n++) { byte[] truncated=Arrays.copyOf(page.encode(),n); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotBlockers.decode(truncated)); }
            for (UUID cursor:Arrays.asList(null,tx)) {
                var request=new RatisSnapshotBlockers.Request(f.fence,cursor,1); assertEquals(request,RatisSnapshotBlockers.Request.decode(request.encode()));
                for (int n=0;n<request.encode().length;n++) { byte[] truncated=Arrays.copyOf(request.encode(),n); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotBlockers.Request.decode(truncated)); }
            }
            assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotBlockers(binding,group,new ApplyPosition(2,3),new ApplyPosition(1,3),List.of(),false));
            assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotBlockers(binding,group,new ApplyPosition(1,2),new ApplyPosition(1,3),List.of(reference,reference),false));
            assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotBlockers.Request(f.fence,null,17));
            for (var cut:List.of(new ApplyPosition(1,2),new ApplyPosition(1,3),new ApplyPosition(2,1))) {
                var impossible=new RatisSnapshotBlockers.Reference(tx,new byte[32],cut);
                assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotBlockers(binding,group,new ApplyPosition(1,2),new ApplyPosition(2,3),List.of(impossible),false));
            }
        }
    }
    @Test void nativeBlockerEnumerationIsFreshBoundedAndUnavailableAfterRelease() throws Exception {
        try (var f=new Fixture("enumeration")) {
            f.committedTransactionWithRightStillPrepared(); f.home.register(f.fence);
            try (var install=f.right.f.machine.prepareSnapshotInstall(f.home.homes,f.fence)) { helper.append(f.right,install); }
            var page=f.data.snapshotBlockers(f.fence,f.right.f.groupId,null,1);
            assertEquals(1,page.records().size()); assertEquals(tx,page.records().getFirst().transaction()); assertFalse(page.more());
            assertTrue(f.data.snapshotBlockers(f.fence,f.right.f.groupId,tx,1).records().isEmpty());
            assertFalse(f.data.snapshotReadiness(f.fence,f.right.f.groupId).ready());
            byte[] request=ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_BLOCKERS,new RatisSnapshotBlockers.Request(f.fence,null,1).encode());
            assertThrows(ExecutionException.class,() -> f.right.f.machine.queryStale(Message.valueOf(ByteString.copyFrom(request)),0).get(5,TimeUnit.SECONDS));
            f.finalize(f.right,control(7)); assertTrue(f.data.snapshotBlockers(f.fence,f.right.f.groupId,null,1).records().isEmpty());
            assertFalse(readiness(f.fence,f.data,f.right.f.groupId,(group,participant) -> { throw new AssertionError("Empty enumeration performs no recovery"); }).help());
            var home=f.home.homes.read(f.fence.fenceId()); f.home.transition(home,home.abort());
            try (var release=f.right.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { helper.append(f.right,release); }
            assertThrows(IOException.class,() -> f.data.snapshotBlockers(f.fence,f.right.f.groupId,null,1));
        }
    }
    @Test void helperRotatesPastUnavailableBlockersAndWrapsWithoutDeclaringReady() throws Exception {
        try (var f=new Fixture("rotation")) {
            f.committedTransactionWithRightStillPrepared(); UUID other=f.anotherPreparedRight(); f.home.register(f.fence);
            try (var install=f.right.f.machine.prepareSnapshotInstall(f.home.homes,f.fence)) { helper.append(f.right,install); }
            var first=f.data.snapshotBlockers(f.fence,f.right.f.groupId,null,1); assertTrue(first.more()); assertEquals(tx,first.records().getFirst().transaction());
            var visited=new ArrayList<UUID>();
            var recovery=readiness(f.fence,f.data,f.right.f.groupId,(group,participant) -> {
                UUID selected=participant.preparation().transactionId(); visited.add(selected); if (selected.equals(tx)) throw new IOException("Unavailable recovery authority"); return false;
            });
            for (int n=0;n<2;n++) { assertThrows(IOException.class,() -> recovery.help()); assertFalse(recovery.help()); }
            assertEquals(List.of(tx,other,tx,other),visited); assertFalse(f.data.snapshotReadiness(f.fence,f.right.f.groupId).ready()); assertEquals(0,f.captures.get());
        }
    }
    @Test void helperRejectsCrossedGroupOrChangedPlanBeforeCallingRecovery() throws Exception {
        try (var f=new Fixture("crossed-proof")) {
            f.committedTransactionWithRightStillPrepared(); f.home.register(f.fence);
            try (var install=f.right.f.machine.prepareSnapshotInstall(f.home.homes,f.fence)) { helper.append(f.right,install); }
            for (boolean crossedGroup:new boolean[]{false,true}) {
                var transport=new RatisRoutedClient.Transport() {
                    public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                    public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                    public byte[] transaction(RangeDescriptor route,byte[] bytes,boolean fresh) throws IOException {
                        byte[] actual=f.node(route.groupId()).wire.transaction(route,bytes,fresh); var packet=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REQUEST_BYTES);
                        if (packet.kind()!=ScopedTransactionRpc.SNAPSHOT_BLOCKERS) return actual;
                        var framed=ScopedTransactionRpc.parse(actual,ScopedTransactionRpc.MAX_REPLY_BYTES); var page=RatisSnapshotBlockers.decode(framed.body()); var old=page.records().getFirst(); byte[] digest=old.digest(); digest[0]^=1;
                        var changed=new RatisSnapshotBlockers(page.binding(),crossedGroup ? f.left.f.groupId : page.group(),page.installed(),page.position(),List.of(new RatisSnapshotBlockers.Reference(old.transaction(),crossedGroup ? old.digest() : digest,old.prepared())),false);
                        return ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_BLOCKERS,changed.encode());
                    }
                };
                try (var client=new RatisRoutedClient(f.directory,transport)) {
                    var readiness=readiness(f.fence,client,f.right.f.groupId,(group,participant) -> { throw new AssertionError("Invalid proof must not reach recovery"); });
                    assertThrows(IOException.class,() -> readiness.help());
                }
            }
        }
    }
    @Test void heldBarriersSurviveReopenAndCannotCompleteOrReleaseBeforeExplicitAdvance()throws Exception {
        try(var f=new Fixture("held-barriers")) {
            Path path=root.resolve("held-barriers.journal");var initial=SnapshotFenceStatus.begin(f.fence);
            try(var journal=RatisDurableSnapshot.create(path,f.home.homes,f.data,initial,f.home.homes.prepare(control(20),null,initial),f.directory.ranges());var driver=f.driver(journal)) {
                boolean ready=false;for(int n=0;n<32 && !ready;n++)ready=driver.step(RatisSnapshotCoordinator.StopAt.GATES_READY).action()==RatisSnapshotCoordinator.Action.GATES_READY;
                assertTrue(ready);assertEquals(0,f.captures.get());assertFalse(journal.snapshot().observedHome().terminal());long writes=f.writes();driver.step(RatisSnapshotCoordinator.StopAt.GATES_READY);assertEquals(writes,f.writes());
                assertNull(f.left.f.store.snapshotGate(f.fence.fenceId()).released());assertNull(f.right.f.store.snapshotGate(f.fence.fenceId()).released());
                var mutation=f.put(f.left,1,9,100);assertThrows(IOException.class,()->f.data.execute(f.data.prepare(mutation.operationId(),mutation.mutations())));
            }
            try(var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.data);var driver=f.driver(journal)) {
                boolean captured=false;for(int n=0;n<32 && !captured;n++){var step=driver.step(RatisSnapshotCoordinator.StopAt.CAPTURES_REPORTED);captured=step.action()==RatisSnapshotCoordinator.Action.CAPTURE_REPORTED && step.participant()==null;assertFalse(step.complete());}
                assertTrue(captured);assertEquals(1,f.left.f.store.activeSnapshotPins());assertEquals(1,f.right.f.store.activeSnapshotPins());assertEquals(2,journal.snapshot().participants().stream().map(p->p.captureToken().snapshotId()).distinct().count());assertFalse(journal.snapshot().observedHome().terminal());for(var group:List.of(f.left,f.right))assertNull(group.f.store.snapshotGate(f.fence.fenceId()).released());
            }
            try(var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.data);var driver=f.driver(journal)) {
                long writes=f.writes();var held=driver.step(RatisSnapshotCoordinator.StopAt.CAPTURES_REPORTED);assertEquals(RatisSnapshotCoordinator.Action.CAPTURE_REPORTED,held.action());assertEquals(writes,f.writes());assertFalse(journal.snapshot().observedHome().terminal());
                boolean complete=false;for(int n=0;n<32 && !complete;n++)complete=driver.step().complete();assertTrue(complete);assertThrows(IOException.class,()->driver.step(RatisSnapshotCoordinator.StopAt.CAPTURES_REPORTED));
                var mutation=f.put(f.left,1,9,101);assertEquals(CommandResult.Status.SUCCESS,f.data.execute(f.data.prepare(mutation.operationId(),mutation.mutations())).status());
            }
        }
    }
    @Test void twoGroupsHelpCommittedPreparationBeforeCaptureAndRecoverLostCaptureReply() throws Exception {
        try (var f=new Fixture("two-groups")) {
            f.committedTransactionWithRightStillPrepared(); assertNull(f.right.f.store.committedParticipant(tx));
            Path path=root.resolve("two-groups.journal"); var initial=SnapshotFenceStatus.begin(f.fence);
            try (var journal=RatisDurableSnapshot.create(path,f.home.homes,f.data,initial,f.home.homes.prepare(control(20),null,initial),f.directory.ranges()); var driver=f.driver(journal)) {
                f.loseCapture=true; boolean lost=false;
                for (int n=0;n<32 && !lost;n++) { long before=f.writes(); try { assertFalse(driver.step().complete()); } catch (IOException failure) { assertEquals("Lost published multi-group capture reply",failure.getMessage()); lost=true; } assertTrue(f.writes()-before<=1); }
                assertTrue(lost); assertEquals(1,f.helped.get()); assertNotNull(f.right.f.store.committedParticipant(tx));
            }
            try (var journal=RatisDurableSnapshot.reopen(path,f.home.homes,f.data); var driver=f.driver(journal)) {
                boolean complete=false;
                for (int n=0;n<32 && !complete;n++) { long before=f.writes(); complete=driver.step().complete(); assertTrue(f.writes()-before<=1); }
                assertTrue(complete); assertEquals(SnapshotFenceStatus.Phase.COMPLETE,journal.snapshot().observedHome().phase());
                f.data.execute(f.data.prepare(f.put(f.left,1,9,1).operationId(),f.put(f.left,1,9,1).mutations()));
                f.data.execute(f.data.prepare(f.put(f.right,130,9,2).operationId(),f.put(f.right,130,9,2).mutations()));
                for (var progress:journal.snapshot().participants()) {
                    boolean left=progress.route().groupId().equals(f.left.f.groupId); byte[] key={(byte)(left ? 1 : 130)};
                    assertArrayEquals(new byte[]{9},f.data.read(column,key)); assertArrayEquals(new byte[]{(byte)(left ? 7 : 8)},f.data.snapshotGet(progress.capture().target(),progress.captureToken(),column,key));
                }
            }
        }
    }
}
