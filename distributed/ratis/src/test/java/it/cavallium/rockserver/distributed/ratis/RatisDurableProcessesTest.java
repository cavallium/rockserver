package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import it.cavallium.rockserver.core.impl.rocksdb.DistributedStoragePool;
import it.cavallium.rockserver.core.config.ConfigParser;
import org.slf4j.LoggerFactory;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeImage;
import it.cavallium.rockserver.core.impl.rocksdb.RangeOwnership;
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
import java.util.Set;
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
    @TempDir(factory = RatisDiskTestDirectory.class, cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS) Path root;

    @Test void durableReceiptSurvivesLeaderKillAndNodeRestart() {
        assertTimeoutPreemptively(Duration.ofSeconds(90), () -> {
            var processes = new Process[3];
            var ports = new int[] {freePort(), freePort(), freePort()};
            var group = group(ports);
            try {
                for (int i = 0; i < 3; i++) processes[i] = start(i, ports);
                try (var client = client(group)) {
                    awaitInitialVoters(client, group);
                    var seed = command(0, new Mutation(Mutation.Kind.PUT, A, bytes("key"), bytes("seed")));
                    assertTrue(client.io().send(message(seed)).isSuccess());
                    var update = new DistributedCommand(GROUP, 1, new byte[32], new OperationId(SESSION, 1, 1),
                            List.of(new Mutation(Mutation.Kind.MERGE, A, bytes("key"), bytes("first"), Mutation.ReturnMode.CURRENT),
                                    new Mutation(Mutation.Kind.PUT, B, bytes("key"), bytes("paired"), Mutation.ReturnMode.PREVIOUS)),
                            List.of(new KeyPredicate(A, bytes("key"), bytes("seed")), new KeyPredicate(B, bytes("key"), null)));
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
                        assertThrows(org.apache.ratis.protocol.exceptions.StateMachineException.class,
                                () -> retry.io().send(message(command(4,
                                        new Mutation(Mutation.Kind.MERGE, B, bytes("key"), bytes("not-supported"), Mutation.ReturnMode.CURRENT)))));
                        assertThrows(org.apache.ratis.protocol.exceptions.StateMachineException.class,
                                () -> retry.io().send(message(command(5,
                                        new Mutation(Mutation.Kind.MERGE, B, bytes("key"), bytes("not-supported"))))));
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

    @Test void portableSnapshotRestoresEmptyNativePoolsWithRetainedRaft() {
        assertTimeoutPreemptively(Duration.ofSeconds(90), () -> {
            var processes = new Process[3];
            var ports = new int[] {freePort(), freePort(), freePort()};
            var group = group(ports);
            var initial = command(10, new Mutation(Mutation.Kind.PUT, A, bytes("image"), bytes("seed")));
            var merge = command(11, new Mutation(Mutation.Kind.MERGE, A, bytes("image"), bytes("once"), Mutation.ReturnMode.CURRENT),
                    new Mutation(Mutation.Kind.PUT, B, bytes("image"), bytes("paired")));
            byte[] outcome;
            try {
                for (int i = 0; i < 3; i++) processes[i] = start(i, ports);
                try (var client = client(group)) {
                    awaitInitialVoters(client, group);
                    assertTrue(client.io().send(message(initial)).isSuccess());
                    var applied = client.io().send(message(merge));
                    assertTrue(applied.isSuccess());
                    outcome = applied.getMessage().getContent().toByteArray();
                    for (var peer : group.getPeers()) {
                        awaitPeerValue(client, peer.getId(), applied.getLogIndex(), A, "image", bytes("seed,once"));
                        var snapshot = client.getSnapshotManagementApi(peer.getId()).create(true, 15_000);
                        assertTrue(snapshot.isSuccess(), () -> String.valueOf(snapshot.getException()));
                        assertTrue(snapshot.getLogIndex() >= applied.getLogIndex());
                    }
                }
                for (var process : processes) process.destroyForcibly();
                for (var process : processes) assertTrue(process.waitFor(10, TimeUnit.SECONDS));
                Path abandonedPartial;
                try (var files = Files.walk(root.resolve("node0").resolve("raft"))) {
                    Path image = files.filter(path -> path.getParent() != null && path.getParent().getFileName().toString().equals("sm")
                            && org.apache.ratis.statemachine.impl.SimpleStateMachineStorage.SNAPSHOT_REGEX.matcher(path.getFileName().toString()).matches())
                            .findFirst().orElseThrow();
                    Path published = image.getParent();
                    Path temporary = published.getParent().resolve("tmp");
                    Files.createDirectories(temporary);
                    Files.move(published, temporary.resolve("snapshot-completed-orphan"));
                    Path partial = temporary.resolve("snapshot-incomplete-orphan");
                    Files.createDirectories(partial);
                    abandonedPartial = partial.resolve("snapshot.99_999");
                    Files.write(abandonedPartial, new byte[] {1, 2, 3});
                }
                for (int i = 0; i < 3; i++) {
                    Path pool = root.resolve("node" + i).resolve("pool");
                    Files.move(pool, root.resolve("node" + i).resolve("saved-pool"));
                    processes[i] = start(i, ports);
                }
                try (var client = client(group)) {
                    awaitPeerValue(client, RaftPeerId.valueOf("n0"), 0, A, "image", bytes("seed,once"));
                    assertTrue(Files.exists(abandonedPartial), "Incomplete staging must remain inert");
                    assertArrayEquals(bytes("seed,once"), read(client, A, "image"));
                    assertArrayEquals(bytes("paired"), read(client, B, "image"));
                    assertArrayEquals(outcome, client.io().send(message(merge)).getMessage().getContent().toByteArray());
                    assertTrue(client.io().send(message(command(12, new Mutation(Mutation.Kind.MERGE, A, bytes("image"), bytes("tail"))))).isSuccess());
                    assertArrayEquals(bytes("seed,once,tail"), read(client, A, "image"));
                    assertArrayEquals(outcome, client.io().send(message(merge)).getMessage().getContent().toByteArray());
                }
            } finally {
                for (var process : processes) if (process != null) process.destroyForcibly();
                for (var process : processes) if (process != null) process.waitFor(10, TimeUnit.SECONDS);
            }
        });
    }

    @Test void emptyListenerReceivesDurableSnapshotAndTailBeforePromotion() {
        assertTimeoutPreemptively(Duration.ofSeconds(120), () -> {
            var processes = new Process[4];
            var ports = new int[] {freePort(), freePort(), freePort(), freePort()};
            var group = group(ports);
            var original = command(20, new Mutation(Mutation.Kind.MERGE, A, bytes("remote"), bytes("snapshot"), Mutation.ReturnMode.CURRENT));
            try {
                for (int i = 0; i < 3; i++) processes[i] = start(i, ports, true);
                try (var client = client(group)) {
                    awaitInitialVoters(client, group);
                    var first = client.io().send(message(original));
                    assertTrue(first.isSuccess());
                    byte[] outcome = first.getMessage().getContent().toByteArray();
                    var padding = client.io().send(message(command(23, new Mutation(Mutation.Kind.PUT, B, bytes("padding"), new byte[16 * 1024]))));
                    assertTrue(padding.isSuccess());
                    // The oversized entry is allowed in the current segment; the next entry seals it before purge.
                    var sealed = client.io().send(message(command(24, new Mutation(Mutation.Kind.PUT, B, bytes("seal"), bytes("closed")))));
                    assertTrue(sealed.isSuccess());
                    for (int i = 0; i < 3; i++) {
                        var peer = RaftPeerId.valueOf("n" + i);
                        awaitPeerValue(client, peer, sealed.getLogIndex(), A, "remote", bytes("snapshot"));
                        var snapshot = client.getSnapshotManagementApi(peer).create(true, 15_000);
                        assertTrue(snapshot.isSuccess());
                        assertTrue(snapshot.getLogIndex() >= sealed.getLogIndex());
                    }
                    // All initial voters can purge D; learner must receive the portable image.
                    var tail = client.io().send(message(command(21, new Mutation(Mutation.Kind.MERGE, A, bytes("remote"), bytes("tail")))));
                    assertTrue(tail.isSuccess());
                    processes[3] = start(3, ports, true);
                    awaitPeerValue(client, RaftPeerId.valueOf("n3"), tail.getLogIndex(), A, "remote", bytes("snapshot,tail"));
                    Path ack = root.resolve("node3").resolve("snapshot-ack");
                    assertTrue(Files.exists(ack), "Learner must use final snapshot RPC durability barrier");
                    long installed = Long.parseLong(Files.readString(ack));
                    assertTrue(installed >= first.getLogIndex());
                    var voters = group.getPeers().stream().map(peer -> RaftPeer.newBuilder(peer)
                            .setStartupRole(org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER).build()).toList();
                    assertTrue(client.admin().setConfiguration(voters).isSuccess());
                    processes[0].destroyForcibly();
                    assertTrue(processes[0].waitFor(10, TimeUnit.SECONDS));
                    // The promoted replica now participates in the remaining 3/4 quorum.
                    var promoted = client.io().send(message(command(22, new Mutation(Mutation.Kind.MERGE, A, bytes("remote"), bytes("promoted")))));
                    assertTrue(promoted.isSuccess());
                    awaitPeerValue(client, RaftPeerId.valueOf("n3"), promoted.getLogIndex(), A, "remote", bytes("snapshot,tail,promoted"));
                    assertArrayEquals(outcome, client.io().send(message(original)).getMessage().getContent().toByteArray());
                }
            } finally {
                for (var process : processes) if (process != null) process.destroyForcibly();
                for (var process : processes) if (process != null) process.waitFor(10, TimeUnit.SECONDS);
            }
        });
    }

    private static void awaitInitialVoters(RaftClient client, RaftGroup group) throws Exception {
        // Server.start alone permits an empty voter log. Released Ratis excludes that
        // peer's reply during election, so prove initial CFG apply before range activation.
        for (var peer : group.getPeers()) {
            if (peer.getStartupRole() != org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER) {
                awaitPeerValue(client, peer.getId(), 0, A, "bootstrap-readiness", null);
            }
        }
    }

    private static void awaitPeerValue(RaftClient client, RaftPeerId peer, long minimum, UUID column, String key, byte[] expected) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(40);
        Throwable last = null;
        do {
            try {
                var keyBytes = bytes(key);
                var request = ByteBuffer.allocate(16 + keyBytes.length).putLong(column.getMostSignificantBits())
                        .putLong(column.getLeastSignificantBits()).put(keyBytes).array();
                var reply = client.io().sendStaleRead(Message.valueOf(ByteString.copyFrom(request)), minimum, peer);
                if (reply.isSuccess()) {
                    var buffer = ByteBuffer.wrap(reply.getMessage().getContent().toByteArray());
                    int size = buffer.getInt();
                    if (size == -1 && expected == null && !buffer.hasRemaining()) return;
                    if (expected != null && size == expected.length) {
                        var actual = new byte[size]; buffer.get(actual);
                        if (java.util.Arrays.equals(expected, actual)) return;
                    }
                }
            } catch (java.io.IOException failure) { last = failure; }
            Thread.sleep(50);
        } while (System.nanoTime() < deadline);
        throw new AssertionError("Peer did not install snapshot/apply tail: " + peer, last);
    }

    private Process start(int node, int[] ports) throws Exception { return start(node, ports, false); }

    private Process start(int node, int[] ports, boolean purge) throws Exception {
        Path directory = root.resolve("node" + node);
        Files.createDirectories(directory);
        var arguments = new ArrayList<String>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--enable-native-access=ALL-UNNAMED", "-cp", System.getProperty("java.class.path"),
                RatisDurableProcessesTest.class.getName(), directory.toString(), Integer.toString(node)));
        for (int port : ports) arguments.add(Integer.toString(port));
        if (purge) arguments.add("purge");
        Path ready = directory.resolve("ready");
        Files.deleteIfExists(ready);
        var process = new ProcessBuilder(arguments).redirectErrorStream(true)
                .redirectOutput(directory.resolve("process.log").toFile()).start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (process.isAlive() && !Files.exists(ready) && System.nanoTime() < deadline) Thread.sleep(10);
        if (process.isAlive() && Files.exists(ready)) return process;
        process.destroyForcibly();
        process.waitFor(10, TimeUnit.SECONDS);
        Path log = directory.resolve("process.log");
        String tail = "";
        if (Files.exists(log)) {
            try (var channel = java.nio.channels.FileChannel.open(log)) {
                long size = channel.size();
                channel.position(Math.max(0, size - 16 * 1024));
                var buffer = ByteBuffer.allocate((int) Math.min(size, 16 * 1024));
                while (buffer.hasRemaining() && channel.read(buffer) >= 0) {}
                tail = new String(buffer.array(), 0, buffer.position(), StandardCharsets.UTF_8);
            }
        }
        throw new AssertionError("Node " + node + " did not start. Preserved log: " + log + "\n" + tail);
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
                .setAddress("127.0.0.1:" + ports[i])
                .setStartupRole(i >= 3 ? org.apache.ratis.proto.RaftProtos.RaftPeerRole.LISTENER : org.apache.ratis.proto.RaftProtos.RaftPeerRole.FOLLOWER).build());
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
        boolean purge = args[args.length - 1].equals("purge");
        int[] ports = java.util.stream.IntStream.range(2, args.length - (purge ? 1 : 0)).map(i -> Integer.parseInt(args[i])).toArray();
        Path configuration = directory.resolve("pool.conf");
        String defaults;
        try (var input = RatisDurableProcessesTest.class.getClassLoader()
                .getResourceAsStream("it/cavallium/rockserver/core/resources/default.conf")) {
            if (input == null) throw new java.io.IOException("Missing default pool configuration");
            defaults = new String(input.readAllBytes(), StandardCharsets.UTF_8);
        }
        Files.writeString(configuration, defaults + "\n" + """
                database.global.column-options: [
                  ${database.global.fallback-column-options} {
                    name: "a"
                    merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
                  },
                  ${database.global.fallback-column-options} { name: "b" }
                ]
                """);
        var pool = DistributedStoragePool.open(directory.resolve("pool"), ConfigParser.parse(configuration),
                LoggerFactory.getLogger(RatisDurableProcessesTest.class));
        try {
                var owner = new RangeOwnership.Owner(GROUP, 1);
                var store = pool.openRange(owner, new RangeOwnership.Interval(GROUP, Set.of(A, B), null, null),
                        new byte[32], Map.of(A, "a", B, "b"), Set.of(A));
                var properties = RatisDurability.properties(directory.resolve("raft"));
                RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
                GrpcConfigKeys.Server.setPort(properties, ports[node]);
                if (purge) {
                    org.apache.ratis.server.RaftServerConfigKeys.Log.setSegmentSizeMax(properties, org.apache.ratis.util.SizeInBytes.valueOf("1KB"));
                    org.apache.ratis.server.RaftServerConfigKeys.Log.setPurgeGap(properties, 1);
                    org.apache.ratis.server.RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);
                    org.apache.ratis.server.RaftServerConfigKeys.Log.setPurgePreservationLogNum(properties, 0);
                }
                var machine = new RatisRangeStateMachine(store, group(ports), new NativeRangeImage.Limits(8 * 1024 * 1024, 64L * 1024 * 1024, 2));
                var server = RaftServer.newBuilder().setServerId(RaftPeerId.valueOf("n" + node)).setGroup(group(ports))
                        .setProperties(properties).setOption(RaftStorage.StartupOption.RECOVER)
                        .setParameters(RatisSnapshotAckInterceptor.parameters((id, term, index) -> {
                            if (!id.equals(group(ports).getGroupId())) throw new java.io.IOException("Unknown snapshot group");
                            machine.syncReceivedSnapshot(term, index);
                            Files.writeString(directory.resolve("snapshot-ack"), Long.toString(index));
                        })).setStateMachine(machine).build();
                try {
                    server.start();
                    Files.writeString(directory.resolve("ready"), "started");
                    System.in.read();
                } finally {
                    machine.beginShutdown();
                    server.close();
                    machine.awaitNativeReaders(Duration.ofSeconds(10));
                }
        } finally {
            if (!pool.close(Duration.ofSeconds(10))) throw new java.io.IOException("Native pool drain incomplete");
        }
    }
}
