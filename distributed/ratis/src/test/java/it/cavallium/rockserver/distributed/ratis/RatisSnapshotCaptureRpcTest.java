package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
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

/** Qualified remote capture through controlled owner dispatch; no network or server processes. */
class RatisSnapshotCaptureRpcTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisSnapshotAdmissionTest helper=new RatisSnapshotAdmissionTest();
    final class Fixture implements AutoCloseable {
        final RatisSnapshotAdmissionTest.Home home;
        final RatisParticipantPrepareTest.Node data;
        final RatisRoutedClient client;
        final SnapshotFence fence;
        final SnapshotFenceStatus initial;
        final AtomicLong clock=new AtomicLong(); final AtomicInteger homes=new AtomicInteger(),owners=new AtomicInteger();
        boolean lose,loseGateProof;
        Fixture(String name,boolean configured,boolean ready) throws Exception {
            helper.root=root; helper.helper.root=root; home=helper.new Home(name+"-home");
            data=helper.helper.new Node(name+"-data",false,null,configured ? home.homes : null,clock::get);
            data.wire.command(data.directory.ranges().getFirst(),data.f.put(2)); fence=helper.fence(home,data,1,helper.scope(data,1,4)); initial=home.register(fence); ApplyPosition installed;
            try (var install=data.f.machine.prepareSnapshotInstall(home.homes,fence)) { installed=helper.append(data,install).position(); }
            if (ready) home.transition(initial,initial.ready(data.f.groupId,installed));
            home.wire.afterRead=homes::incrementAndGet;
            client=new RatisRoutedClient(data.directory,new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError("No latest fallback"); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError("No write fallback"); }
                public byte[] participant(RangeDescriptor route,byte[] record) throws IOException { return data.wire.participant(route,record); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) throws IOException { assertTrue(fresh); owners.incrementAndGet(); byte[] reply=data.wire.transaction(route,request,true);
                    if (loseGateProof && ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES).kind()==ScopedTransactionRpc.SNAPSHOT_GATE_STATE) { loseGateProof=false; throw new IOException("Lost native gate proof reply"); } return reply; }
                public byte[] snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request) throws IOException {
                    assertEquals(data.directory.ranges().getFirst(),route); assertEquals(route.replicas().getFirst(),owner);
                    try {
                        byte[] response=data.f.machine.queryStale(Message.valueOf(ByteString.copyFrom(request)),0).get(5,TimeUnit.SECONDS).getContent().toByteArray();
                        if (lose) { lose=false; throw new IOException("Lost capture reply after pin publication"); } return response;
                    } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException(failure); }
                }
            });
        }
        RatisRoutedClient.PreparedSnapshotCapture operation() throws Exception { return client.prepareSnapshotCapture(client.snapshotTarget(fence,data.f.groupId),UUID.randomUUID(),Duration.ofSeconds(10)); }
        @Override public void close() throws Exception { client.close(); data.close(); home.close(); }
    }
    @Test void readinessRequiresFreshLeaderAndRetainsTheExactInstalledCut() throws Exception {
        try (var f=new Fixture("readiness",true,false)) {
            var retry=f.data.wire.propose(RatisSnapshotRecord.install(f.fence,f.data.f.groupId).encode()); assertNull(retry.getException());
            try (var borrowed=f.data.f.machine.prepareSnapshotInstall(f.home.homes,f.fence)) { assertTrue(borrowed.valid()); assertTrue(borrowed.drained()); }
            finally { f.data.f.machine.cancelTransaction(retry); }
            var proof=f.client.snapshotReadiness(f.fence,f.data.f.groupId);
            assertTrue(proof.ready()); assertEquals(f.data.f.store.snapshotGate(f.fence.fenceId()).installed(),proof.installed());
            assertEquals(ScopedTransactionRpc.SnapshotBinding.of(f.fence),proof.binding());
            assertEquals(proof,RatisSnapshotReadiness.decode(proof.encode()));
            byte[] request=ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_READY,f.fence.encode());
            assertThrows(ExecutionException.class,() -> f.data.f.machine.queryStale(Message.valueOf(ByteString.copyFrom(request)),0).get(5,TimeUnit.SECONDS));
            f.home.transition(f.initial,f.initial.abort());
            try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { helper.append(f.data,release); }
            var released=f.client.snapshotReadiness(f.fence,f.data.f.groupId);
            assertFalse(released.ready()); assertEquals(proof.installed(),released.installed());
            f.data.f.machine.notifyNotLeader(List.of()); f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,false));
            assertThrows(IOException.class,() -> f.client.snapshotReadiness(f.fence,f.data.f.groupId));
        }
    }
    @Test void readinessCodecRejectsTruncationInvalidFlagsAndImpossiblePositions() {
        var binding=new ScopedTransactionRpc.SnapshotBinding(UUID.randomUUID(),new byte[32]); var group=UUID.randomUUID();
        for (var proof:List.of(new RatisSnapshotReadiness(binding,group,null,false),new RatisSnapshotReadiness(binding,group,new ApplyPosition(1,2),false),new RatisSnapshotReadiness(binding,group,new ApplyPosition(1,2),true))) {
            assertEquals(proof,RatisSnapshotReadiness.decode(proof.encode()));
            for (int n=0;n<proof.encode().length;n++) { byte[] truncated=Arrays.copyOf(proof.encode(),n); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotReadiness.decode(truncated)); }
            byte[] changed=proof.encode(); changed[64]=4; assertThrows(IllegalArgumentException.class,() -> RatisSnapshotReadiness.decode(changed));
        }
        assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotReadiness(binding,group,null,true));
        assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotReadiness(binding,group,new ApplyPosition(0,2),true));
    }
    @Test void lostReplyReopenAndLeaderLossRepeatTheOriginalPinWithoutAnotherHomeRead() throws Exception {
        try (var f=new Fixture("retry",true,true)) {
            var operation=f.operation(); byte[] frozen=operation.bytes(); f.lose=true; assertThrows(IOException.class,() -> f.client.executeSnapshotCapture(operation));
            assertEquals(1,f.data.f.machine.snapshotPinStats().pins()); assertEquals(1,f.homes.get());
            var target=f.client.restoreSnapshotTarget(f.fence,operation.target().descriptor(),operation.target().replica(),operation.target().incarnation());
            var restored=f.client.restoreSnapshotCapture(target,RatisSnapshotCaptureRequest.decode(operation.request().encode())); assertArrayEquals(frozen,restored.bytes());
            var token=f.client.executeSnapshotCapture(restored); var ready=f.home.homes.read(f.fence.fenceId());
            var captured=ready.capture(f.data.f.groupId,token); f.home.transition(ready,captured); f.home.transition(captured,captured.complete());
            try (var release=f.data.f.machine.prepareSnapshotRelease(f.home.homes,f.fence)) { helper.append(f.data,release); }
            f.data.wire.command(f.data.directory.ranges().getFirst(),new DistributedCommand(f.data.f.groupId,1,new byte[32],new OperationId(new UUID(0,8000),1,0),List.of(new Mutation(Mutation.Kind.PUT,f.data.f.column,new byte[]{2},new byte[]{9}))));
            int reads=f.homes.get(); f.data.f.machine.notifyNotLeader(List.of()); f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,false));
            assertEquals(token,f.client.executeSnapshotCapture(restored)); assertEquals(reads,f.homes.get()); assertEquals(1,f.owners.get());
            assertArrayEquals(new byte[]{7},f.client.snapshotGet(target,token,f.data.f.column,new byte[]{2}));
            f.clock.set(Duration.ofSeconds(10).toNanos()); f.data.f.machine.expireTransactions();
            assertThrows(RatisSnapshotLostException.class,() -> f.client.executeSnapshotCapture(restored)); assertEquals(reads,f.homes.get()); assertEquals(0,f.data.f.store.activeSnapshotPins());
        }
    }
    @Test void unknownCaptureRequiresCurrentLeaderAllReadyAndConfiguredHomeAuthority() throws Exception {
        try (var f=new Fixture("authority",true,false)) {
            var operation=f.operation(); assertThrows(IOException.class,() -> f.client.executeSnapshotCapture(operation)); assertEquals(0,f.data.f.machine.snapshotPinStats().entries());
            var gate=f.data.f.store.snapshotGate(f.fence.fenceId()); f.home.transition(f.initial,f.initial.ready(f.data.f.groupId,gate.installed()));
            f.data.f.machine.notifyNotLeader(List.of()); f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(2,false));
            assertThrows(IOException.class,() -> f.client.executeSnapshotCapture(operation)); assertEquals(0,f.data.f.machine.snapshotPinStats().pins());
            f.data.f.leadership.set(new RatisRangeStateMachine.Leadership(1,true)); assertNotNull(f.client.executeSnapshotCapture(operation));
            assertThrows(IllegalStateException.class,() -> f.data.f.machine.snapshotHomes(f.home.homes));
        }
        try (var f=new Fixture("unconfigured",false,true)) {
            var operation=f.operation(); assertThrows(IOException.class,() -> f.client.executeSnapshotCapture(operation)); assertEquals(0,f.data.f.machine.snapshotPinStats().entries()); assertEquals(0,f.homes.get());
            var oldOwner=new RatisSnapshotCaptureRequest(f.fence,UUID.randomUUID(),UUID.randomUUID(),Duration.ofSeconds(10));
            byte[] raw=f.data.f.machine.queryStale(Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_CAPTURE,oldOwner.encode()))),0).get(5,TimeUnit.SECONDS).getContent().toByteArray();
            assertThrows(RatisSnapshotLostException.class,() -> oldOwner.result(ScopedTransactionRpc.parse(raw,ScopedTransactionRpc.MAX_REPLY_BYTES).body())); assertEquals(0,f.homes.get());
        }
    }
    @Test void expiryWhileHomeReadRunsRetainsChargesAndCannotPublishOrRecreateAPin() throws Exception {
        try (var f=new Fixture("busy",true,true); var executor=Executors.newVirtualThreadPerTaskExecutor()) {
            var operation=f.operation(); var entered=new CountDownLatch(1); var release=new CountDownLatch(1);
            f.home.wire.afterRead=() -> { f.homes.incrementAndGet(); entered.countDown(); try { if (!release.await(5,TimeUnit.SECONDS)) throw new AssertionError("Fixture timeout"); } catch (InterruptedException failure) { throw new AssertionError(failure); } };
            var pending=executor.submit(() -> f.client.executeSnapshotCapture(operation));
            try {
                assertTrue(entered.await(5,TimeUnit.SECONDS)); long bytes=f.data.f.machine.snapshotPinStats().bytes();
                assertThrows(IOException.class,() -> f.client.executeSnapshotCapture(operation)); assertEquals(1,f.homes.get());
                f.clock.set(Duration.ofSeconds(10).toNanos()); f.data.f.machine.expireTransactions(); assertFalse(pending.isDone()); assertEquals(bytes,f.data.f.machine.snapshotPinStats().bytes());
            } finally { release.countDown(); }
            var failure=assertThrows(ExecutionException.class,() -> pending.get(5,TimeUnit.SECONDS)); assertInstanceOf(RatisSnapshotLostException.class,failure.getCause());
            assertEquals(0,f.data.f.machine.snapshotPinStats().pins()); assertEquals(RatisSnapshotPins.TOMBSTONE_BYTES,f.data.f.machine.snapshotPinStats().bytes()); assertEquals(0,f.data.f.store.activeSnapshotPins());
            assertThrows(RatisSnapshotLostException.class,() -> f.client.executeSnapshotCapture(operation)); assertEquals(1,f.homes.get());
        }
    }
    @Test void currentCaptureCodecOwnsRequestAndValidatesIdentityPositionTtlAndLoss() throws Exception {
        try (var f=new Fixture("codec",true,true)) {
            var operation=f.operation(); var request=operation.request(); byte[] bytes=request.encode(); var decoded=RatisSnapshotCaptureRequest.decode(bytes); assertEquals(request,decoded); Arrays.fill(bytes,(byte)0); assertEquals(request,decoded);
            for (int n=0;n<request.encode().length;n++) { byte[] truncated=Arrays.copyOf(request.encode(),n); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotCaptureRequest.decode(truncated)); }
            assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotCaptureRequest(f.fence,request.owner(),request.snapshotId(),Duration.ofNanos(1)));
            assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotCaptureRequest(f.fence,request.owner(),request.snapshotId(),Duration.ofSeconds(Long.MAX_VALUE)));
            var token=new SnapshotFenceStatus.Capture(request.owner(),request.snapshotId(),new ApplyPosition(1,1)); assertEquals(token,request.result(request.reply(token)));
            assertThrows(RatisSnapshotLostException.class,() -> request.result(request.reply(null)));
            byte[] changed=request.reply(token); changed[48]^=1; assertThrows(IllegalArgumentException.class,() -> request.result(changed));
            var different=new RatisSnapshotCaptureRequest(f.fence,UUID.randomUUID(),request.snapshotId(),request.ttl()); assertThrows(IllegalArgumentException.class,() -> f.client.restoreSnapshotCapture(operation.target(),different));
            byte[] position=request.reply(token); ByteBuffer.wrap(position).putLong(89,0); assertThrows(IllegalArgumentException.class,() -> request.result(position));
        }
    }
}
