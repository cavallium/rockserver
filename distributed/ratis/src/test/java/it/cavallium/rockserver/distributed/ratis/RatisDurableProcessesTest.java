package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.*;
import static org.junit.jupiter.api.Assertions.*;

/** Real JVMs and disk logs on one host: protocol smoke coverage, not independent failure domains. */
public class RatisDurableProcessesTest {
    private static final UUID GROUP = UUID.fromString("b8800000-0000-0000-0000-000000000001");
    private static final UUID A = UUID.fromString("b8800000-0000-0000-0000-000000000002");
    private static final UUID B = UUID.fromString("b8800000-0000-0000-0000-000000000003");
    private static final UUID SESSION = UUID.fromString("b8800000-0000-0000-0000-000000000004");
    @TempDir Path root;

    @Test void durableReceiptSurvivesLeaderKillAndNodeRestart() {
        assertTimeoutPreemptively(Duration.ofSeconds(90), () -> {
            var processes = new Process[3];
            var ports = new int[] {freePort(), freePort(), freePort()};
            var group = group(ports);
            try {
                for (int i = 0; i < 3; i++) processes[i] = start(i, ports);
                try (var client = client(group)) {
                    var seed = command(0, new Mutation(Mutation.Kind.PUT, A, bytes("key"), bytes("seed")));
                    assertTrue(client.io().send(message(seed)).isSuccess());
                    var update = command(1, new Mutation(Mutation.Kind.MERGE, A, bytes("key"), bytes("first"), Mutation.ReturnMode.CURRENT),
                            new Mutation(Mutation.Kind.PUT, B, bytes("key"), bytes("paired"), Mutation.ReturnMode.PREVIOUS));
                    // Discard the application result; a fresh transport must resolve the same operation.
                    assertTrue(client.io().send(message(update)).isSuccess());
                    int leader = Integer.parseInt(client.getLeaderId().toString().substring(1));
                    processes[leader].destroyForcibly();
                    assertTrue(processes[leader].waitFor(10, TimeUnit.SECONDS));
                    try (var retry = client(group)) {
                        var resolved = retry.io().send(message(update));
                        assertTrue(resolved.isSuccess());
                        byte[] originalOutcome = resolved.getMessage().getContent().toByteArray();
                        var original = CommandResult.decode(originalOutcome);
                        assertEquals(CommandResult.Status.SUCCESS, original.status());
                        assertArrayEquals(bytes("seed,first"), original.value(0));
                        assertNull(original.value(1));
                        assertArrayEquals(bytes("seed,first"), read(retry, A, "key"));
                        assertArrayEquals(bytes("paired"), read(retry, B, "key"));
                        assertTrue(retry.io().send(message(command(2,
                                new Mutation(Mutation.Kind.MERGE, A, bytes("key"), bytes("later"))))).isSuccess());
                        processes[leader] = start(leader, ports);
                        assertArrayEquals(bytes("seed,first,later"), read(retry, A, "key"));
                        var delayed = retry.io().send(message(update));
                        assertTrue(delayed.isSuccess());
                        assertArrayEquals(originalOutcome, delayed.getMessage().getContent().toByteArray());
                        assertThrows(org.apache.ratis.protocol.exceptions.StateMachineException.class,
                                () -> retry.io().send(message(command(1,
                                        new Mutation(Mutation.Kind.PUT, B, bytes("key"), bytes("wrong-payload"))))));
                        assertThrows(org.apache.ratis.protocol.exceptions.StateMachineException.class,
                                () -> retry.io().send(Message.valueOf(ByteString.copyFrom(new byte[] {1, 2}))));
                        assertArrayEquals(bytes("seed,first,later"), read(retry, A, "key"));
                        // Stop remaining original voters; the restarted one must join a durable quorum.
                        int other = (leader + 1) % 3;
                        processes[other].destroyForcibly();
                        assertTrue(processes[other].waitFor(10, TimeUnit.SECONDS));
                        assertTrue(retry.io().send(message(command(3,
                                new Mutation(Mutation.Kind.PUT, B, bytes("restart"), bytes("accepted"))))).isSuccess());
                        assertArrayEquals(bytes("accepted"), read(retry, B, "restart"));
                    }
                }
            } finally {
                for (var process : processes) if (process != null) process.destroyForcibly();
                for (var process : processes) if (process != null) process.waitFor(10, TimeUnit.SECONDS);
            }
        });
    }

    private Process start(int node, int[] ports) throws Exception {
        Path directory = root.resolve("node" + node);
        Files.createDirectories(directory);
        return new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
                RatisDurableProcessesTest.class.getName(), directory.toString(), Integer.toString(node),
                Integer.toString(ports[0]), Integer.toString(ports[1]), Integer.toString(ports[2]))
                .redirectErrorStream(true).redirectOutput(directory.resolve("process.log").toFile()).start();
    }

    private static RaftClient client(RaftGroup group) {
        var properties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
        RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(3, TimeUnit.SECONDS));
        return RaftClient.newBuilder().setRaftGroup(group).setProperties(properties)
                .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(100,
                        TimeDuration.valueOf(100, TimeUnit.MILLISECONDS))).build();
    }

    private static RaftGroup group(int[] ports) {
        var peers = new ArrayList<RaftPeer>();
        for (int i = 0; i < ports.length; i++) peers.add(RaftPeer.newBuilder().setId("n" + i)
                .setAddress("127.0.0.1:" + ports[i]).build());
        return RaftGroup.valueOf(RaftGroupId.valueOf(GROUP), peers);
    }
    private static byte[] read(RaftClient client, UUID column, String key) throws Exception {
        byte[] keyBytes = bytes(key);
        var request = ByteBuffer.allocate(16 + keyBytes.length).putLong(column.getMostSignificantBits())
                .putLong(column.getLeastSignificantBits()).put(keyBytes).array();
        var reply = client.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(request)));
        assertTrue(reply.isSuccess(), () -> String.valueOf(reply.getException()));
        var value = ByteBuffer.wrap(reply.getMessage().getContent().toByteArray());
        int size = value.getInt();
        if (size == -1) return null;
        var result = new byte[size];
        value.get(result);
        assertFalse(value.hasRemaining());
        return result;
    }
    private static Message message(DistributedCommand command) {
        return Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command)));
    }
    private static DistributedCommand command(long sequence, Mutation... mutations) {
        return new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, sequence), List.of(mutations));
    }
    private static byte[] bytes(String text) { return text.getBytes(StandardCharsets.UTF_8); }
    private static int freePort() throws Exception { try (var socket = new ServerSocket(0)) { return socket.getLocalPort(); } }

    public static void main(String[] args) throws Exception {
        RocksDB.loadLibrary();
        Path directory = Path.of(args[0]);
        int node = Integer.parseInt(args[1]);
        int[] ports = {Integer.parseInt(args[2]), Integer.parseInt(args[3]), Integer.parseInt(args[4])};
        var handles = new ArrayList<ColumnFamilyHandle>();
        try (var options = new DBOptions().setCreateIfMissing(true).setCreateMissingColumnFamilies(true);
                var plain = new ColumnFamilyOptions();
                var merged = new ColumnFamilyOptions().setMergeOperator(new StringAppendOperator(","))) {
            var descriptors = List.of(new ColumnFamilyDescriptor(RocksDB.DEFAULT_COLUMN_FAMILY, plain),
                    new ColumnFamilyDescriptor(bytes("a"), merged), new ColumnFamilyDescriptor(bytes("b"), plain),
                    new ColumnFamilyDescriptor(bytes("state"), plain), new ColumnFamilyDescriptor(bytes("receipts"), plain));
            try (var db = RocksDB.open(options, directory.resolve("pool").toString(), descriptors, handles)) {
                var store = new ReplicatedRangeStore(db, GROUP, 1, new byte[32],
                        Map.of(A, handles.get(1), B, handles.get(2)), handles.get(3), handles.get(4));
                var properties = RatisDurability.properties(directory.resolve("raft"));
                RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
                GrpcConfigKeys.Server.setPort(properties, ports[node]);
                try (var server = RaftServer.newBuilder().setServerId(RaftPeerId.valueOf("n" + node)).setGroup(group(ports))
                        .setProperties(properties).setOption(RaftStorage.StartupOption.RECOVER)
                        .setStateMachine(new RatisRangeStateMachine(store)).build()) {
                    server.start();
                    System.in.read();
                } finally {
                    for (var handle : handles) handle.close();
                }
            }
        }
    }
}
