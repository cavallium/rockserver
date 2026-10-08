package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;

import java.lang.reflect.Proxy;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import static org.junit.jupiter.api.Assertions.*;

/** Real native apply/image/reload composition; no server, sockets, transport or quorum assertion. */
class RatisSessionAcknowledgementTest {
    @TempDir(factory = RatisDiskTestDirectory.class) Path root;

    @Test void snapshotFloorResolvesCollectedDeferredReceiptAndResumedTailAppliesOnce() throws Exception {
        UUID groupId = UUID.randomUUID(), column = UUID.randomUUID(), session = UUID.randomUUID(), admin = UUID.randomUUID();
        var peer = RaftPeer.newBuilder().setId("fixture").build();
        var group = RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peer);
        var identity = (RaftServer) Proxy.newProxyInstance(RaftServer.class.getClassLoader(),new Class<?>[]{RaftServer.class},
                (proxy,method,args) -> {
                    if (method.getName().equals("getId")) return peer.getId();
                    if (method.getName().equals("toString")) return "identity-only-fixture";
                    throw new AssertionError("Unexpected server operation: " + method);
                });
        try (var pool = DistributedStoragePool.open(root.resolve("native"),ConfigParser.parseDefault(),LoggerFactory.getLogger(getClass()));
                var storage = RaftStorage.newBuilder().setDirectory(root.resolve("raft").toFile())
                        .setOption(RaftStorage.StartupOption.FORMAT).setStorageFreeSpaceMin(org.apache.ratis.util.SizeInBytes.ONE_MB)
                        .setLogCorruptionPolicy(org.apache.ratis.server.RaftServerConfigKeys.Log.CorruptionPolicy.EXCEPTION).build()) {
            storage.initialize();
            var store = pool.openRange(new RangeOwnership.Owner(groupId,1),
                    new RangeOwnership.Interval(groupId,Set.of(column),null,null),new byte[32],Map.of(column,"data"),Set.of());
            var machine = new RatisRangeStateMachine(store,group,new NativeRangeImage.Limits(2*1024*1024,16*1024*1024,1));
            try {
                machine.initialize(identity,group.getGroupId(),storage);
                var fence = DistributedCommand.advanceSessionEpochFloor(groupId,1,new byte[32],new OperationId(admin,4,0),2);
                assertInstanceOf(IllegalStateException.class,machine.startTransaction(request(group,peer,fence)).getException());
                var original = new DistributedCommand(groupId,1,new byte[32],new OperationId(session,1,0),
                        List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{7})));
                var ack = new DistributedCommand(groupId,1,new byte[32],new OperationId(session,1,1),List.of(),List.of(),0);
                assertEquals(CommandResult.Status.SUCCESS,result(machine.applyTransaction(transaction(machine,0,original)).get(5,TimeUnit.SECONDS)));
                assertEquals(CommandResult.Status.SUCCESS,result(machine.applyTransaction(transaction(machine,1,ack)).get(5,TimeUnit.SECONDS)));
                assertEquals(1,machine.takeSnapshot());
                machine.pause();
                var covered = machine.applyTransaction(transaction(machine,0,original));
                var tail = new DistributedCommand(groupId,1,new byte[32],new OperationId(session,1,2),
                        List.of(new Mutation(Mutation.Kind.PUT,column,new byte[]{1},new byte[]{8})));
                var deferredTail = machine.applyTransaction(transaction(machine,2,tail));
                assertFalse(covered.isDone()); assertFalse(deferredTail.isDone());
                machine.reinitialize();
                assertEquals(CommandResult.Status.SESSION_EXPIRED,result(covered.get(5,TimeUnit.SECONDS)));
                assertFalse(deferredTail.isDone());
                assertEquals(1,machine.getLastAppliedTermIndex().getIndex());
                try (var pin = store.capture()) { assertArrayEquals(new byte[]{7},pin.get(column,new byte[]{1})); }
                assertEquals(CommandResult.Status.SUCCESS,result(machine.applyTransaction(transaction(machine,2,tail)).get(5,TimeUnit.SECONDS)));
                assertEquals(CommandResult.Status.SUCCESS,result(deferredTail.get(5,TimeUnit.SECONDS)));
                assertEquals(2,machine.getLastAppliedTermIndex().getIndex());
                assertEquals(CommandResult.Status.SESSION_EXPIRED,result(machine.applyTransaction(transaction(machine,3,original)).get(5,TimeUnit.SECONDS)));
                try (var pin = store.capture()) { assertArrayEquals(new byte[]{8},pin.get(column,new byte[]{1})); }
                // A committed follower entry does not repeat proposer lifetime authority.
                assertEquals(CommandResult.Status.SUCCESS,result(machine.applyTransaction(transaction(machine,4,fence)).get(5,TimeUnit.SECONDS)));
                assertEquals(4,machine.takeSnapshot());
            } finally { machine.close(); machine.awaitNativeReaders(Duration.ofSeconds(5)); }
            var allowed = new java.util.concurrent.atomic.AtomicBoolean(true);
            var checks = new java.util.concurrent.atomic.AtomicInteger();
            var authorized = new RatisRangeStateMachine(store,group,new NativeRangeImage.Limits(2*1024*1024,16*1024*1024,1),command -> {},command -> {
                checks.incrementAndGet();
                if (!allowed.get()) throw new IllegalStateException("lifetime authority changed");
            });
            try {
                authorized.initialize(identity,group.getGroupId(),storage);
                var advance = DistributedCommand.advanceSessionEpochFloor(groupId,1,new byte[32],new OperationId(admin,4,1),3);
                var accepted = authorized.startTransaction(request(group,peer,advance));
                assertNull(accepted.getException()); accepted.initLogEntry(1,5);
                allowed.set(false);
                assertEquals(CommandResult.Status.SUCCESS,result(authorized.applyTransaction(accepted).get(5,TimeUnit.SECONDS)));
                assertEquals(1,checks.get());
                assertEquals(CommandResult.Status.SUCCESS,result(authorized.applyTransaction(transaction(authorized,5,advance)).get(5,TimeUnit.SECONDS)));
                assertEquals(1,checks.get(),"Replay never consults changed admission authority");
                var duplicate = authorized.startTransaction(request(group,peer,advance));
                assertNull(duplicate.getException()); duplicate.initLogEntry(1,6);
                assertEquals(CommandResult.Status.SUCCESS,result(authorized.applyTransaction(duplicate).get(5,TimeUnit.SECONDS)));
                assertEquals(1,checks.get(),"A known original outcome does not need new authority");
                var next = DistributedCommand.advanceSessionEpochFloor(groupId,1,new byte[32],new OperationId(admin,4,2),4);
                assertInstanceOf(IllegalStateException.class,authorized.startTransaction(request(group,peer,next)).getException());
            } finally { authorized.close(); authorized.awaitNativeReaders(Duration.ofSeconds(5)); }
        }
    }
    private static RaftClientRequest request(RaftGroup group,RaftPeer peer,DistributedCommand command) {
        return RaftClientRequest.newBuilder().setClientId(ClientId.randomId()).setServerId(peer.getId()).setGroupId(group.getGroupId())
                .setCallId(1).setType(RaftClientRequest.writeRequestType()).setMessage(Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build();
    }
    private static TransactionContext transaction(RatisRangeStateMachine machine,long index,DistributedCommand command) {
        return TransactionContext.newBuilder().setStateMachine(machine).setServerRole(RaftPeerRole.FOLLOWER)
                .setLogEntry(LogEntryProto.newBuilder().setTerm(1).setIndex(index).setStateMachineLogEntry(
                        StateMachineLogEntryProto.newBuilder().setLogData(ByteString.copyFrom(DistributedCommandCodec.encode(command)))).build()).build();
    }
    private static CommandResult.Status result(Message reply) { return CommandResult.decode(reply.getContent().toByteArray()).status(); }
}
