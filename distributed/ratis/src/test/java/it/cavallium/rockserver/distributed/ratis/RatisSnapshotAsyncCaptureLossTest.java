package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.UUID;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Malformed capture reply cannot leave a durable async controller endlessly retrying capture. */
class RatisSnapshotAsyncCaptureLossTest {
    @TempDir(factory=RatisDiskTestDirectory.class) Path root;
    @Test void changedCaptureBindingForcesLossAndAbortThenCleansOriginalCapture()throws Exception{malformedCaptureForcesLoss(false);}
    @Test void changedCaptureOpcodeForcesLossAndAbortThenCleansOriginalCapture()throws Exception{malformedCaptureForcesLoss(true);}
    private void malformedCaptureForcesLoss(boolean opcode)throws Exception{
        var helper=new RatisSnapshotCoordinatorTest();helper.root=root;
        try(var f=helper.new Fixture("async-capture-loss")){
            var changed=new AtomicBoolean();
            var transport=new RatisRoutedClient.Transport(){
                public byte[] read(RangeDescriptor route,UUID column,byte[] key)throws IOException{return f.node(route.groupId()).wire.read(route,column,key);}
                public CommandResult command(RangeDescriptor route,DistributedCommand command)throws IOException{return f.node(route.groupId()).wire.command(route,command);}
                public CompletableFuture<byte[]> readAsync(RangeDescriptor route,UUID column,byte[] key)throws IOException{return f.node(route.groupId()).wire.readAsync(route,column,key);}
                public CompletableFuture<CommandResult> commandAsync(RangeDescriptor route,DistributedCommand command)throws IOException{return f.node(route.groupId()).wire.commandAsync(route,command);}
                public CompletableFuture<byte[]> transactionAsync(RangeDescriptor route,byte[] request,boolean fresh)throws IOException{return f.node(route.groupId()).wire.transactionAsync(route,request,fresh);}
                public CompletableFuture<byte[]> participantAsync(RangeDescriptor route,byte[] record)throws IOException{return f.node(route.groupId()).wire.participantAsync(route,record);}
                public CompletableFuture<byte[]> snapshot(RangeDescriptor route,ReplicaDescriptor owner,byte[] request)throws IOException{
                    var packet=ScopedTransactionRpc.parse(request,ScopedTransactionRpc.MAX_REQUEST_BYTES);var machine=f.node(route.groupId()).f.machine;
                    var message=Message.valueOf(ByteString.copyFrom(request));
                    return (owner==null?machine.queryLinearizable(message):machine.queryStale(message,0)).thenApply(reply->{
                        byte[] bytes=reply.getContent().toByteArray();
                        if(packet.kind()==ScopedTransactionRpc.SNAPSHOT_CAPTURE&&changed.compareAndSet(false,true)){
                            var response=ScopedTransactionRpc.parse(bytes,ScopedTransactionRpc.MAX_REPLY_BYTES);var body=response.body();
                            if(opcode)return ScopedTransactionRpc.frame(ScopedTransactionRpc.SNAPSHOT_GATE_STATE,body);
                            body[48]^=1;return ScopedTransactionRpc.frame(response.kind(),body);
                        }
                        return bytes;
                    });
                }
            };
            try(var data=new RatisRoutedClient(f.directory,transport)){
                var initial=SnapshotFenceStatus.begin(f.fence);var path=root.resolve("changed-capture.journal");
                try(var journal=RatisDurableSnapshot.create(path,f.home.homes,data,initial,f.home.homes.prepare(helper.control(20),null,initial),f.directory.ranges());
                    var coordinator=new RatisSnapshotCoordinator(journal,f.home.homes,data,last->new OperationId(last.sessionId(),last.epoch(),last.sequence()+1),UUID::randomUUID,Duration.ofSeconds(10))){
                    boolean ready=false;for(int n=0;n<32&&!ready;n++)ready=coordinator.stepAsync(RatisSnapshotCoordinator.StopAt.GATES_READY).get(10,TimeUnit.SECONDS).action()==RatisSnapshotCoordinator.Action.GATES_READY;assertTrue(ready);
                    var aborted=coordinator.stepAsync(RatisSnapshotCoordinator.StopAt.CAPTURES_REPORTED).get(10,TimeUnit.SECONDS);
                    assertTrue(changed.get());assertEquals(RatisSnapshotCoordinator.Action.ABORTED,aborted.action());assertEquals(SnapshotFenceStatus.Phase.ABORT,journal.snapshot().observedHome().phase());
                    assertTrue(journal.snapshot().participant(aborted.participant()).captureLost());assertNotNull(journal.snapshot().participant(aborted.participant()).capture());assertNull(journal.snapshot().participant(aborted.participant()).captureToken());
                }
                try(var journal=RatisDurableSnapshot.reopen(path,f.home.homes,data);
                    var coordinator=new RatisSnapshotCoordinator(journal,f.home.homes,data,last->new OperationId(last.sessionId(),last.epoch(),last.sequence()+1),UUID::randomUUID,Duration.ofSeconds(10))){
                    assertEquals(1,journal.snapshot().participants().stream().filter(RatisDurableSnapshot.ParticipantProgress::captureLost).count());
                    boolean released=false;for(int n=0;n<32&&!released;n++)released=coordinator.stepAsync(RatisSnapshotCoordinator.StopAt.RELEASED).get(10,TimeUnit.SECONDS).complete();assertTrue(released);
                    var saved=journal.snapshot();journal.consume(new OperationId(saved.lastHome().sessionId(),saved.lastHome().epoch(),saved.lastHome().sequence()+1));assertEquals(CommandResult.Status.SUCCESS,journal.executeHomeAsync().get(10,TimeUnit.SECONDS).status());
                    for(var participant:journal.snapshot().participants())if(participant.capture()!=null){var result=journal.releaseCaptureAsync(participant.route().groupId()).get(10,TimeUnit.SECONDS);if(result==RatisDurableSnapshot.CaptureReleaseProgress.RECOVERED)assertEquals(RatisDurableSnapshot.CaptureReleaseProgress.RELEASED,journal.releaseCaptureAsync(participant.route().groupId()).get(10,TimeUnit.SECONDS));}
                    assertEquals(0,f.left.f.store.activeSnapshotPins());assertEquals(0,f.right.f.store.activeSnapshotPins());
                }
            }
        }
    }
}
