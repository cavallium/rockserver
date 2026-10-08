package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Committed-record dispatch and verified retries only; new leader gate admission is a separate contract. */
class RatisSnapshotRecordTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    final RatisParticipantPrepareTest helper=new RatisParticipantPrepareTest();
    SnapshotFence fence(RatisParticipantPrepareTest.Node data) {
        return new SnapshotFence(helper.cluster,helper.database,data.directory.domainId(),new UUID(0,400),new UUID(0,401),1,0,1,new byte[32],
                List.of(new SnapshotFence.Participant(data.f.groupId,data.f.groupId,1,new byte[32],new OperationId(new UUID(0,402),1,0),new OperationId(new UUID(0,402),1,1),helper.scope(data,1,4))));
    }
    static TransactionContext committed(RatisRangeStateMachine machine,long term,long index,RatisSnapshotRecord request) {
        return TransactionContext.newBuilder().setStateMachine(machine).setServerRole(RaftPeerRole.FOLLOWER)
                .setLogEntry(LogEntryProto.newBuilder().setTerm(term).setIndex(index).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(request.encode()))).build()).build();
    }
    RatisSnapshotRecord.Result apply(RatisParticipantPrepareTest.Node data,long index,RatisSnapshotRecord request) throws Exception {
        byte[] bytes=data.f.machine.applyTransaction(committed(data.f.machine,1,index,request)).get(5,TimeUnit.SECONDS).getContent().toByteArray();
        data.wire.index=index; return RatisSnapshotRecord.Result.decode(bytes);
    }
    @Test void currentFramesOwnValuesAndRejectMalformedKindsBodiesAndReplies() throws Exception {
        helper.root=root;
        try (var data=helper.new Node("codec",false)) {
            var fence=fence(data); var install=RatisSnapshotRecord.install(fence,data.f.groupId); var release=RatisSnapshotRecord.release(SnapshotFenceStatus.begin(fence).abort(),data.f.groupId);
            for (var request:List.of(install,release)) {
                byte[] bytes=request.encode(); assertTrue(RatisSnapshotRecord.isSnapshot(bytes)); var decoded=RatisSnapshotRecord.decode(bytes);
                assertEquals(request,decoded); assertArrayEquals(request.digest(),decoded.digest()); Arrays.fill(bytes,(byte)0); assertEquals(request,decoded);
                for (int n=0;n<request.encode().length;n++) { byte[] truncated=Arrays.copyOf(request.encode(),n); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotRecord.decode(truncated)); }
                assertThrows(IllegalArgumentException.class,() -> RatisSnapshotRecord.decode(Arrays.copyOf(request.encode(),request.encode().length+1)));
                byte[] kind=request.encode(); ByteBuffer.wrap(kind).putInt(4,3); assertThrows(IllegalArgumentException.class,() -> RatisSnapshotRecord.decode(kind));
            }
            assertNotEquals(install.operation(),release.operation());
            assertThrows(IllegalArgumentException.class,() -> RatisSnapshotRecord.release(SnapshotFenceStatus.begin(fence),data.f.groupId));
            assertThrows(IllegalArgumentException.class,() -> RatisSnapshotRecord.install(fence,new UUID(0,999)));
            for (var result:List.of(new RatisSnapshotRecord.Result(null),new RatisSnapshotRecord.Result(new ApplyPosition(1,1)))) assertEquals(result,RatisSnapshotRecord.Result.decode(result.encode()));
            assertThrows(IllegalArgumentException.class,() -> RatisSnapshotRecord.Result.decode(new CommandResult(CommandResult.Status.SUCCESS,List.of()).encode()));
            assertThrows(IllegalArgumentException.class,() -> new RatisSnapshotRecord.Result(new ApplyPosition(1,0)));
        }
    }
    @Test void committedGatesDispatchAndExactRetriesNeedNoHomeAuthorityOrAppend() throws Exception {
        helper.root=root;
        try (var data=helper.new Node("committed",false)) {
            var fence=fence(data); var install=RatisSnapshotRecord.install(fence,data.f.groupId); var operation=data.routed.prepareSnapshotGate(install);
            assertNotNull(data.wire.propose(operation.bytes()).getException(),"An encoding alone cannot authorize a new gate");
            assertEquals(new ApplyPosition(1,1),apply(data,1,install).position());
            long index=data.wire.index; assertEquals(new ApplyPosition(1,1),data.routed.executeSnapshotGate(operation).position()); assertEquals(index,data.wire.index,"Retained proof must not append again");
            assertEquals(new ApplyPosition(1,1),data.routed.executeSnapshotGate(operation).position());
            assertTrue(data.routed.snapshotGate(fence,data.f.groupId).active()); assertNotNull(data.f.propose(data.f.put(2)).getException());
            var body=ByteBuffer.allocate(16).putLong(fence.fenceId().getMostSignificantBits()).putLong(fence.fenceId().getLeastSignificantBits()).array();
            var query=ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_GATE_STATE,body);
            assertThrows(IOException.class,() -> data.wire.transaction(operation.descriptor(),query,false));
            assertThrows(ExecutionException.class,() -> data.f.machine.queryStale(Message.valueOf(ByteString.copyFrom(query)),0).get(5,TimeUnit.SECONDS));
            SnapshotFenceStatus complete;
            try (var pin=data.f.store.captureGate(fence)) {
                complete=SnapshotFenceStatus.begin(fence).ready(data.f.groupId,new ApplyPosition(1,1)).capture(data.f.groupId,new SnapshotFenceStatus.Capture(new UUID(0,403),new UUID(0,404),pin.position())).complete();
            }
            var release=RatisSnapshotRecord.release(complete,data.f.groupId); var releaseOperation=data.routed.prepareSnapshotGate(release);
            assertNotNull(data.wire.propose(releaseOperation.bytes()).getException(),"A new release still requires fresh home authority");
            assertEquals(new ApplyPosition(1,2),apply(data,2,release).position());
            assertEquals(new ApplyPosition(1,2),data.routed.executeSnapshotGate(releaseOperation).position());
            assertFalse(data.routed.snapshotGate(fence,data.f.groupId).active());
            var floor=DistributedCommand.advanceSessionEpochFloor(data.f.groupId,1,new byte[32],new OperationId(data.f.admin,2,0),2);
            data.wire.command(operation.descriptor(),floor);
            assertEquals(new ApplyPosition(1,1),data.routed.executeSnapshotGate(operation).position());
            assertEquals(new ApplyPosition(1,2),data.routed.snapshotGate(fence,data.f.groupId).released()); data.f.assertReleased();
        }
    }
    @Test void releaseFirstSuppressionAndOriginalRoutesRemainStableAcrossRefresh() throws Exception {
        helper.root=root;
        try (var data=helper.new Node("release-first",false)) {
            var fence=fence(data); var install=RatisSnapshotRecord.install(fence,data.f.groupId);
            var operation=data.routed.prepareSnapshotGate(install); var abort=SnapshotFenceStatus.begin(fence).abort();
            apply(data,1,RatisSnapshotRecord.release(abort,data.f.groupId));
            assertTrue(data.routed.executeSnapshotGate(operation).suppressed()); assertNull(data.routed.snapshotGate(fence,data.f.groupId).installed());
            var old=operation.descriptor(); var refreshed=new RangeDescriptor(old.rangeId(),old.groupId(),old.rangeEpoch(),1,old.lowerInclusive(),old.upperExclusive(),old.topologyState(),old.replicas());
            data.routed.refresh(new RangeDirectory(helper.cluster,helper.database,data.directory.domainId(),1,new byte[32],1,data.directory.columns(),0,List.of(refreshed)));
            var restored=data.routed.restoreSnapshotGate(install,old); assertArrayEquals(operation.bytes(),restored.bytes()); assertEquals(0,restored.descriptor().configurationVersion());
            assertTrue(data.routed.executeSnapshotGate(restored).suppressed()); assertFalse(data.routed.snapshotGate(fence,data.f.groupId).active());
            var changed=SnapshotFenceStatus.begin(fence).ready(data.f.groupId,new ApplyPosition(1,1)).abort();
            assertNotNull(data.wire.propose(RatisSnapshotRecord.release(changed,data.f.groupId).encode()).getException());
            data.f.assertReleased();
        }
    }
    @Test void nativeSnapshotCoverageVerifiesDeferredInstallReleaseAndSuppressedResults() throws Exception {
        helper.root=root;
        for (boolean releaseFirst:new boolean[]{false,true}) try (var data=helper.new Node("deferred-"+releaseFirst,false)) {
            var fence=fence(data); var install=RatisSnapshotRecord.install(fence,data.f.groupId); var release=RatisSnapshotRecord.release(SnapshotFenceStatus.begin(fence).abort(),data.f.groupId);
            var first=releaseFirst ? release : install; var second=releaseFirst ? install : release;
            var firstResult=apply(data,1,first); var secondResult=apply(data,2,second); data.f.machine.takeSnapshot(); data.f.machine.pause();
            var firstPending=data.f.machine.applyTransaction(committed(data.f.machine,1,1,first));
            var secondPending=data.f.machine.applyTransaction(committed(data.f.machine,1,2,second));
            assertFalse(firstPending.isDone()); assertFalse(secondPending.isDone()); data.f.machine.reinitialize();
            assertEquals(firstResult,RatisSnapshotRecord.Result.decode(firstPending.get(5,TimeUnit.SECONDS).getContent().toByteArray()));
            assertEquals(secondResult,RatisSnapshotRecord.Result.decode(secondPending.get(5,TimeUnit.SECONDS).getContent().toByteArray()));
            assertFalse(data.routed.snapshotGate(fence,data.f.groupId).active()); data.f.assertReleased();
        }
    }
    @Test void routedRepliesCannotChangeFenceBindingOrReturnSuppressedRelease() throws Exception {
        helper.root=root;
        try (var data=helper.new Node("reply",false)) {
            var fence=fence(data); var terminal=SnapshotFenceStatus.begin(fence).abort();
            var transport=new RatisRoutedClient.Transport() {
                public byte[] read(RangeDescriptor route,UUID column,byte[] key) { throw new AssertionError(); }
                public CommandResult command(RangeDescriptor route,DistributedCommand command) { throw new AssertionError(); }
                public byte[] participant(RangeDescriptor route,byte[] record) { return new RatisSnapshotRecord.Result(null).encode(); }
                public byte[] transaction(RangeDescriptor route,byte[] request,boolean fresh) {
                    var changed=new SnapshotFence(fence.clusterId(),fence.databaseId(),fence.domainId(),fence.fenceId(),fence.homeGroup(),fence.homeEpoch(),1,fence.schemaEpoch(),fence.schemaDigest(),fence.participants());
                    var record=SnapshotGate.install(changed,data.f.groupId,new ApplyPosition(1,1)).encode();
                    return ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_GATE_STATE,ByteBuffer.allocate(4+record.length).putInt(record.length).put(record).array());
                }
            };
            try (var routed=new RatisRoutedClient(data.directory,transport)) {
                assertThrows(IOException.class,() -> routed.snapshotGate(fence,data.f.groupId));
                var release=routed.prepareSnapshotGate(RatisSnapshotRecord.release(terminal,data.f.groupId));
                assertThrows(IOException.class,() -> routed.executeSnapshotGate(release));
            }
        }
    }
}
