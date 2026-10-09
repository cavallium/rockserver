package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.*;
import java.util.function.Consumer;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Home CAS through controlled dispatch/native apply. Participant gate and capture provenance are separate contracts. */
class RatisSnapshotFencesTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final UUID a=new UUID(0,20),b=new UUID(0,21),dataColumn=new UUID(0,22);
    OperationId operation(long n) { return new OperationId(new UUID(0,40),1,n); }
    OperationId control(long n) { return new OperationId(new UUID(0,80),1,n); }
    SnapshotFenceStatus initial(RangeDirectory directory) {
        var scope=new TransactionScope(List.of(TransactionScope.Span.range(dataColumn,new byte[]{1},new byte[]{8})));
        byte[] dataSchema=new byte[32]; dataSchema[0]=7;
        var participants=List.of(new SnapshotFence.Participant(a,a,1,dataSchema,control(0),control(1),scope),new SnapshotFence.Participant(b,b,1,dataSchema,control(2),control(3),scope));
        return SnapshotFenceStatus.begin(new SnapshotFence(directory.clusterId(),directory.databaseId(),new UUID(0,30),new UUID(0,31),directory.ranges().getFirst().groupId(),1,new OperationId(new UUID(0,31),1,0),2,3,dataSchema,participants));
    }
    SnapshotFenceStatus withHome(SnapshotFenceStatus initial,UUID home,long epoch) {
        var f=initial.fence(); return SnapshotFenceStatus.begin(new SnapshotFence(f.clusterId(),f.databaseId(),f.domainId(),f.fenceId(),home,epoch,f.operation(),f.directoryGeneration(),f.schemaEpoch(),f.schemaDigest(),f.participants()));
    }
    SnapshotFenceStatus.Capture capture(long id,ApplyPosition position) { return new SnapshotFenceStatus.Capture(new UUID(0,id),new UUID(0,id+1),position); }
    RatisRoutedClient.Transport transport(RatisTransactionRpcTest.Controlled wire) {
        return new RatisRoutedClient.Transport() {
            public byte[] read(RangeDescriptor route,UUID column,byte[] key) throws IOException { return ScopedTransactionRpc.pointValue(wire.transaction(route,ScopedTransactionRpc.pointRequest(column,key),true),SnapshotFenceStatus.MAX_BYTES); }
            public CommandResult command(RangeDescriptor route,DistributedCommand command) throws IOException { return wire.command(route,command); }
        };
    }
    @Test void lostExactRepliesStaleCasAndOrderedCaptureDecisionRemainDurable() throws Exception {
        var helper=new RatisTransactionRpcTest(); helper.root=root; var validation=new AtomicReference<Consumer<DistributedCommand>>(command -> {});
        try (var f=new RatisRetentionAdmissionTest.Fixture(root.resolve("snapshot-home"),RatisRangeStateMachine.RetentionLimits.defaults(),command -> validation.get().accept(command))) {
            var directory=helper.directory(f); validation.set(RatisSnapshotFences.validator(directory.clusterId(),directory.databaseId(),f.groupId,1,new byte[32],f.column));
            var wire=helper.new Controlled(f);
            try (var routed=new RatisRoutedClient(directory,transport(wire))) {
                var homes=new RatisSnapshotFences(routed,f.column); var initial=initial(directory); UUID id=initial.fence().fenceId(); assertNull(homes.read(id));
                var begin=homes.prepare(operation(0),null,initial); wire.lose=true; assertThrows(IOException.class,() -> homes.execute(begin));
                assertEquals(initial,homes.read(id)); assertEquals(CommandResult.Status.SUCCESS,homes.execute(begin).status());
                var first=initial.ready(a,new ApplyPosition(1,1)); var other=initial.ready(b,new ApplyPosition(1,2));
                assertEquals(CommandResult.Status.SUCCESS,homes.execute(homes.prepare(operation(1),initial,first)).status());
                var stale=homes.prepare(operation(2),initial,other); assertEquals(CommandResult.Status.CONFLICT,homes.execute(stale).status()); assertEquals(CommandResult.Status.CONFLICT,homes.execute(stale).status());
                var ready=first.ready(b,new ApplyPosition(1,2)); homes.execute(homes.prepare(operation(3),first,ready));
                var capturedA=ready.capture(a,capture(90,new ApplyPosition(1,3))); homes.execute(homes.prepare(operation(4),ready,capturedA));
                var captured=capturedA.capture(b,capture(92,new ApplyPosition(1,4))); homes.execute(homes.prepare(operation(5),capturedA,captured));
                var complete=captured.complete(); homes.execute(homes.prepare(operation(6),captured,complete));
                var lateAbort=homes.prepare(operation(7),ready,ready.abort()); assertEquals(CommandResult.Status.CONFLICT,homes.execute(lateAbort).status());
                var read=homes.readRegistration(id); assertEquals(complete,read.status()); assertEquals(directory.ranges().getFirst(),read.descriptor());
                assertNotEquals(directory.domainId(),complete.fence().domainId()); assertFalse(Arrays.equals(directory.schemaDigest(),complete.fence().schemaDigest()),"Home schema is independent of frozen data schema");
                f.machine.takeSnapshot(); f.machine.pause(); f.machine.reinitialize(); assertEquals(complete,homes.read(id));
                assertEquals(CommandResult.Status.SUCCESS,homes.execute(begin).status()); assertEquals(complete,homes.read(id));
                var ack=routed.prepareAcknowledgement(operation(8),f.column,begin.command().mutations().getFirst().key(),7); assertEquals(CommandResult.Status.SUCCESS,homes.execute(ack).status());
                assertEquals(CommandResult.Status.SESSION_EXPIRED,homes.execute(begin).status()); assertEquals(complete,homes.read(id)); f.assertReleased();
            }
        }
    }
    @Test void validatorRejectsRawBypassesReusedControlsWrongAuthorityAndIllegalTransitions() throws Exception {
        var helper=new RatisTransactionRpcTest(); helper.root=root;
        try (var f=helper.fixture("snapshot-envelopes"); var routed=new RatisRoutedClient(helper.directory(f),helper.new Controlled(f))) {
            var directory=helper.directory(f); var homes=new RatisSnapshotFences(routed,f.column); var initial=initial(directory);
            var validator=RatisSnapshotFences.validator(directory.clusterId(),directory.databaseId(),f.groupId,1,new byte[32],f.column);
            var base=homes.prepare(operation(0),null,initial).command(); validator.accept(base); byte[] key=base.mutations().getFirst().key();
            var scope=new TransactionScope(List.of(TransactionScope.Span.point(f.column,key)));
            var accepted=DistributedCommand.acceptedTransaction(base,new TransactionAcceptance(new ApplyPosition(1,0),0,new UUID(0,9),scope,new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()));
            assertThrows(IllegalArgumentException.class,() -> validator.accept(accepted));
            var delete=new DistributedCommand(f.groupId,1,new byte[32],operation(1),List.of(new Mutation(Mutation.Kind.DELETE,f.column,key,null)),base.predicates()); assertThrows(IllegalArgumentException.class,() -> validator.accept(delete));
            var extra=new DistributedCommand(f.groupId,1,new byte[32],operation(2),List.of(base.mutations().getFirst(),base.mutations().getFirst()),base.predicates()); assertThrows(IllegalArgumentException.class,() -> validator.accept(extra));
            byte[] wrongSchema=new byte[32]; wrongSchema[0]=1;
            assertThrows(IllegalArgumentException.class,() -> validator.accept(new DistributedCommand(f.groupId,1,wrongSchema,operation(3),base.mutations(),base.predicates())));
            assertThrows(IllegalArgumentException.class,() -> homes.prepare(control(0),null,initial)); assertThrows(IllegalArgumentException.class,() -> homes.prepare(control(3),null,initial));
            assertThrows(IllegalArgumentException.class,() -> homes.prepare(operation(4),null,withHome(initial,new UUID(0,99),1)));
            assertThrows(IllegalArgumentException.class,() -> homes.prepare(operation(4),null,withHome(initial,f.groupId,2)));
            var ready=initial.ready(a,new ApplyPosition(1,1)).ready(b,new ApplyPosition(1,2));
            assertThrows(IllegalArgumentException.class,() -> homes.prepare(operation(4),initial,ready)); assertThrows(IllegalArgumentException.class,() -> homes.prepare(operation(4),null,initial.abort()));
            var abort=initial.abort(); assertThrows(IllegalArgumentException.class,() -> homes.prepare(operation(4),abort,abort));
            var fence=initial.fence(); var changed=SnapshotFenceStatus.begin(new SnapshotFence(fence.clusterId(),fence.databaseId(),fence.domainId(),fence.fenceId(),fence.homeGroup(),1,fence.operation(),3,3,fence.schemaDigest(),fence.participants())).ready(a,new ApplyPosition(1,1));
            assertThrows(IllegalArgumentException.class,() -> homes.prepare(operation(4),initial,changed));
        }
    }
    @Test void readBindsHomeAuthorityToCapturedRouteRatherThanRefreshedDirectory() throws Exception {
        var helper=new RatisTransactionRpcTest(); helper.root=root;
        try (var f=helper.fixture("snapshot-route")) {
            var directory=helper.directory(f); var old=directory.ranges().getFirst();
            var next=new RangeDescriptor(old.rangeId(),new UUID(0,99),2,1,null,null,RangeDescriptor.TopologyState.ACTIVE,old.replicas());
            var refreshed=new RangeDirectory(directory.clusterId(),directory.databaseId(),directory.domainId(),directory.schemaEpoch(),directory.schemaDigest(),1,directory.columns(),0,List.of(next));
            var initial=initial(directory); var value=new AtomicReference<>(initial.encode()); var client=new AtomicReference<RatisRoutedClient>();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { client.get().refresh(refreshed); return value.get(); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError("Read-only fixture"); }
            };
            try (var routed=new RatisRoutedClient(directory,transport)) {
                client.set(routed); var homes=new RatisSnapshotFences(routed,f.column); var read=homes.readRegistration(initial.fence().fenceId());
                assertEquals(initial,read.status()); assertEquals(old,read.descriptor()); assertEquals(next,routed.directory().ranges().getFirst());
                assertThrows(IOException.class,() -> homes.read(initial.fence().fenceId()));
                value.set(withHome(initial,next.groupId(),next.rangeEpoch()).encode()); assertEquals(next,homes.readRegistration(initial.fence().fenceId()).descriptor());
                assertThrows(IOException.class,() -> homes.read(new UUID(0,100)));
            }
        }
    }
    @Test void registrationReservesLargestFutureExpectedAndNextStatusBeforeFirstRpc() throws Exception {
        var helper=new RatisTransactionRpcTest(); helper.root=root;
        try (var f=helper.fixture("snapshot-capacity")) {
            var directory=helper.directory(f); var base=initial(directory).fence();
            var scope=new TransactionScope(List.of(TransactionScope.Span.point(dataColumn,new byte[DistributedCommandCodec.MAX_ENCODED_BYTES/2])));
            var participant=new SnapshotFence.Participant(a,a,1,new byte[32],control(0),control(1),scope);
            var oversized=SnapshotFenceStatus.begin(new SnapshotFence(base.clusterId(),base.databaseId(),base.domainId(),base.fenceId(),base.homeGroup(),1,base.operation(),2,3,base.schemaDigest(),List.of(participant)));
            var calls=new AtomicInteger(); var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { return null; }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { calls.incrementAndGet(); throw new AssertionError("Must reject before RPC"); }
            };
            try (var routed=new RatisRoutedClient(directory,transport)) {
                var homes=new RatisSnapshotFences(routed,f.column); byte[] key=java.nio.ByteBuffer.allocate(16).putLong(base.fenceId().getMostSignificantBits()).putLong(base.fenceId().getLeastSignificantBits()).array();
                var raw=routed.prepare(operation(0),List.of(new Mutation(Mutation.Kind.PUT,f.column,key,oversized.encode())),List.of(new KeyPredicate(f.column,key,null)));
                assertTrue(DistributedCommandCodec.encodedSize(raw.command())<DistributedCommandCodec.MAX_ENCODED_BYTES,"Initial body fits while future full-image CAS cannot");
                assertThrows(IllegalArgumentException.class,() -> homes.prepare(operation(0),null,oversized));
                assertThrows(IllegalArgumentException.class,() -> homes.execute(raw)); assertEquals(0,calls.get()); assertNull(homes.read(base.fenceId()));
            }
        }
    }
}
