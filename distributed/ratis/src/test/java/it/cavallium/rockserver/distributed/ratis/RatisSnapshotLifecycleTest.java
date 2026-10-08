package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.net.ServerSocket;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

/** Real initialized server/native storage; manual callback injection controls the reload gap.
 * This does not simulate the network snapshot installation scheduler. */
class RatisSnapshotLifecycleTest {
    @TempDir(factory = RatisDiskTestDirectory.class, cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS) Path root;

    @Test void pausedCallbacksRemainDeferredUntilInstalledCutAndActualTailReplay() { exerciseLifecycle(false); }

    @Test void failedPauseSettlesCallbacksBeforeBoundedServerShutdownAndRetainsNativePin() { exerciseLifecycle(true); }

    private void exerciseLifecycle(boolean heldReader) {
        assertTimeoutPreemptively(Duration.ofSeconds(45), () -> {
            RocksDB.loadLibrary();
            var groupId = UUID.randomUUID();
            var column = UUID.randomUUID();
            int port;
            try (var socket = new ServerSocket(0)) { port = socket.getLocalPort(); }
            var peer = RaftPeer.newBuilder().setId("single").setAddress("127.0.0.1:" + port).build();
            var group = RaftGroup.valueOf(RaftGroupId.valueOf(groupId), peer);
            var handles = new ArrayList<ColumnFamilyHandle>();
            try (var options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
                    var plain = new ColumnFamilyOptions();
                    var merged = new ColumnFamilyOptions().setMergeOperator(new StringAppendOperator(","))) {
                var descriptors = List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, plain),
                        new ColumnFamilyDescriptor(bytes("data"), merged), new ColumnFamilyDescriptor(bytes("state"), plain),
                        new ColumnFamilyDescriptor(bytes("receipts"), plain));
                try (var db = RocksDB.open(options, root.resolve("pool").toString(), descriptors, handles)) {
                    var ownership = new RangeOwnership();
                    var owner = new RangeOwnership.Owner(groupId, 1);
                    ownership.register(owner, new RangeOwnership.Interval(groupId, Set.of(column), null, null), RangeOwnership.State.ACTIVE);
                    var store = new ReplicatedRangeStore(db, ownership, owner, new byte[32], Map.of(column, handles.get(1)),
                            handles.get(2), handles.get(3), Set.of(column));
                    var machine = new RatisRangeStateMachine(store, group, new NativeRangeImage.Limits(1024 * 1024, 16L * 1024 * 1024, 1));
                    RaftProperties properties = RatisDurability.properties(root.resolve("raft"));
                    org.apache.ratis.RaftConfigKeys.Rpc.setType(properties, org.apache.ratis.rpc.SupportedRpcType.GRPC);
                    GrpcConfigKeys.Server.setPort(properties, port);
                    var server = RaftServer.newBuilder().setServerId(peer.getId()).setGroup(group).setProperties(properties)
                            .setOption(RaftStorage.StartupOption.RECOVER)
                            .setParameters(RatisSnapshotAckInterceptor.parameters((id, term, index) -> machine.syncReceivedSnapshot(term, index)))
                            .setStateMachine(machine).build();
                    try {
                        server.start();
                        var clientProperties = new RaftProperties();
                        org.apache.ratis.RaftConfigKeys.Rpc.setType(clientProperties, org.apache.ratis.rpc.SupportedRpcType.GRPC);
                        org.apache.ratis.client.RaftClientConfigKeys.Rpc.setRequestTimeout(clientProperties,
                                org.apache.ratis.util.TimeDuration.valueOf(3, TimeUnit.SECONDS));
                        try (var client = RaftClient.newBuilder().setRaftGroup(group).setProperties(clientProperties)
                                .setRetryPolicy(org.apache.ratis.retry.RetryPolicies.retryUpToMaximumCountWithFixedSleep(100,
                                        org.apache.ratis.util.TimeDuration.valueOf(100, TimeUnit.MILLISECONDS))).build()) {
                            var session = UUID.randomUUID();
                            var command = command(groupId, column, session, 1, "seed");
                            var reply = client.io().send(message(command));
                            assertTrue(reply.isSuccess());
                            byte[] original = reply.getMessage().getContent().toByteArray();
                            var pointRead = Message.valueOf(ByteString.copyFrom(ScopedTransactionRpc.pointRequest(column,bytes("key"))));
                            assertTrue(client.io().sendStaleRead(pointRead, reply.getLogIndex(), peer.getId()).isSuccess());
                            assertTrue(client.getSnapshotManagementApi().create(true, 10_000).isSuccess());
                            var d = store.applied();
                            var next = server.getDivision(group.getGroupId()).getRaftLog().getNextIndex();
                            var tail = command(groupId, column, session, 2, "tail");
                            if (heldReader) {
                                try (var pin = store.capture(); var executor = java.util.concurrent.Executors.newSingleThreadExecutor()) {
                                    var pausing = executor.submit(machine::pause);
                                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                                    while (machine.getLifeCycleState() != LifeCycle.State.PAUSED && System.nanoTime() < deadline) Thread.sleep(1);
                                    assertEquals(LifeCycle.State.PAUSED, machine.getLifeCycleState());
                                    var pending = machine.applyTransaction(transaction(machine, d.term(), next, tail));
                                    assertFalse(pending.isDone());
                                    assertThrows(java.util.concurrent.ExecutionException.class, () -> pausing.get(15, TimeUnit.SECONDS));
                                    assertTrue(pending.isCompletedExceptionally(), "Failed pause must unblock Ratis apply futures");
                                    assertEquals(1, store.activeSnapshotPins());
                                    assertThrows(java.io.IOException.class, () -> machine.awaitNativeReaders(Duration.ofMillis(20)));
                                    machine.beginShutdown();
                                    assertTimeoutPreemptively(Duration.ofSeconds(5), server::close);
                                    assertEquals(1, store.activeSnapshotPins());
                                    assertArrayEquals(bytes("seed"), pin.get(column, bytes("key")));
                                }
                                machine.awaitNativeReaders(Duration.ofSeconds(1));
                                assertEquals(0, store.activeSnapshotPins());
                                return;
                            }
                            machine.pause();
                            assertEquals(LifeCycle.State.PAUSED, machine.getLifeCycleState());
                            var covered = machine.applyTransaction(transaction(machine, d.term(), reply.getLogIndex(), command));
                            var uncovered = machine.applyTransaction(transaction(machine, d.term(), next, tail));
                            machine.notifyTermIndexUpdated(d.term(), next + 1);
                            machine.notifyConfigurationChanged(0, next + 10, RaftConfigurationProto.newBuilder().addPeers(peer.getRaftPeerProto()).build());
                            assertFalse(covered.isDone());
                            assertFalse(uncovered.isDone());
                            assertEquals(d, store.applied());
                            assertTrue(machine.query(pointRead).isCompletedExceptionally());
                            assertTrue(machine.queryStale(pointRead, d.index() + 1).isCompletedExceptionally());
                            machine.reinitialize();
                            assertEquals(LifeCycle.State.RUNNING, machine.getLifeCycleState());
                            assertArrayEquals(original, covered.get(5, TimeUnit.SECONDS).getContent().toByteArray());
                            assertFalse(uncovered.isDone());
                            assertEquals(d, store.applied());
                            var replayed = client.io().send(message(tail));
                            assertTrue(replayed.isSuccess());
                            assertEquals(next, replayed.getLogIndex());
                            assertArrayEquals(replayed.getMessage().getContent().toByteArray(), uncovered.get(5, TimeUnit.SECONDS).getContent().toByteArray());
                            try (var pin = store.capture()) { assertArrayEquals(bytes("seed,tail"), pin.get(column, bytes("key"))); }
                            // Manually replay the deferred void callback at the callback boundary.
                            machine.notifyTermIndexUpdated(d.term(), next + 1);
                            assertEquals(next + 1, store.applied().index());
                            assertTrue(store.configuration().index() <= d.index());
                            assertTrue(machine.queryStale(pointRead, next + 2).isCompletedExceptionally());
                        }
                    } finally {
                        machine.beginShutdown();
                        server.close();
                        machine.awaitNativeReaders(Duration.ofSeconds(10));
                        for (var handle : handles) handle.close();
                    }
                }
            }
        });
    }

    private static DistributedCommand command(UUID group, UUID column, UUID session, long sequence, String value) {
        return new DistributedCommand(group, 1, new byte[32], new OperationId(session, 1, sequence),
                List.of(new Mutation(Mutation.Kind.MERGE, column, bytes("key"), bytes(value), Mutation.ReturnMode.CURRENT)));
    }
    private static TransactionContext transaction(RatisRangeStateMachine machine, long term, long index, DistributedCommand command) {
        return TransactionContext.newBuilder().setStateMachine(machine).setServerRole(RaftPeerRole.FOLLOWER)
                .setLogEntry(LogEntryProto.newBuilder().setTerm(term).setIndex(index)
                        .setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder().setLogData(message(command).getContent())).build()).build();
    }
    private static Message message(DistributedCommand command) { return Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command))); }
    private static byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }
}
