package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.config.ConfigParser;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.*;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.*;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.slf4j.LoggerFactory;
import static org.junit.jupiter.api.Assertions.*;

/** Three JVMs, independent catalogue quorum, two data groups in one native pool per JVM. */
class RatisCatalogueProcessesTest {
    static final UUID CLUSTER = id(1), DATABASE = id(2), DOMAIN = id(3), CATALOGUE = id(4), META = id(5),
            LEFT = id(6), RIGHT = id(7), DATA = id(8), SESSION = id(9);
    @TempDir(factory = RatisDiskTestDirectory.class, cleanup = org.junit.jupiter.api.io.CleanupMode.ON_SUCCESS) Path root;

    @Test void catalogueCasAndCachedDataRoutingSurviveCatalogueQuorumLossAndDataLeaderKill() {
        assertTimeoutPreemptively(Duration.ofSeconds(150), () -> {
            int[] ports = {port(), port(), port()};
            Process[] nodes = new Process[3];
            try {
                for (int n = 0; n < 3; n++) nodes[n] = start(n, ports);
                try (var catalogueClient = client(group(CATALOGUE, ports), 100)) {
                    for (UUID gid : List.of(LEFT, RIGHT)) {
                        for (var peer : group(gid, ports).getPeers()) {
                            assertTrue(catalogueClient.getGroupManagementApi(peer.getId()).add(group(gid, ports)).isSuccess());
                        }
                    }
                    for (UUID gid : List.of(CATALOGUE, LEFT, RIGHT)) bootstrap(gid, ports);
                    var catalogue = new RatisCatalogue(catalogueClient, CLUSTER, DATABASE, CATALOGUE, 1, new byte[32], META);
                    var initial = directory(ports, 0);
                    var create = catalogue.prepare(op(1), null, initial);
                    assertEquals(CommandResult.Status.SUCCESS, catalogue.execute(create).status());
                    assertEquals(CommandResult.Status.SUCCESS, catalogue.execute(create).status());
                    assertArrayEquals(initial.payloadDigest(), catalogue.read(DOMAIN).payloadDigest());
                    var updated = directory(ports, 1);
                    var winner = catalogue.prepare(op(2), initial, updated);
                    var loser = catalogue.prepare(op(3), initial, updated);
                    try (var competing = java.util.concurrent.Executors.newFixedThreadPool(2)) {
                        var first = competing.submit(() -> catalogue.execute(winner).status());
                        var second = competing.submit(() -> catalogue.execute(loser).status());
                        var firstStatus = first.get(15, TimeUnit.SECONDS);
                        var secondStatus = second.get(15, TimeUnit.SECONDS);
                        assertEquals(Set.of(CommandResult.Status.SUCCESS, CommandResult.Status.CONFLICT), Set.of(firstStatus, secondStatus));
                        assertEquals(firstStatus, catalogue.execute(winner).status());
                        assertEquals(secondStatus, catalogue.execute(loser).status());
                    }
                    assertArrayEquals(updated.payloadDigest(), catalogue.read(DOMAIN).payloadDigest());
                    // Malformed direct writes are rejected by the catalogue replica, before log/native admission.
                    var bypass = new DistributedCommand(CATALOGUE, 1, new byte[32], op(4),
                            List.of(new Mutation(Mutation.Kind.PUT, META, bytes("bad"), bytes("unvalidated"))));
                    assertThrows(IOException.class, () -> catalogueClient.io().send(message(bypass)));
                    try (var routed = RatisRoutedClient.connect(catalogue.read(DOMAIN))) {
                        var left = routed.prepare(op(10), List.of(new Mutation(Mutation.Kind.PUT, DATA, bytes("a"), bytes("left"), Mutation.ReturnMode.CURRENT)));
                        var right = routed.prepare(op(11), List.of(new Mutation(Mutation.Kind.PUT, DATA, bytes("z"), bytes("right"), Mutation.ReturnMode.CURRENT)));
                        assertArrayEquals(bytes("left"), executeEventually(routed, left).value(0));
                        assertArrayEquals(bytes("right"), executeEventually(routed, right).value(0));
                        assertArrayEquals(bytes("left"), readEventually(routed, "a"));
                        assertArrayEquals(bytes("right"), readEventually(routed, "z"));
                        int catalogueLeader = leader(catalogueClient, CATALOGUE);
                        for (int n = 0; n < 3; n++) if (n != catalogueLeader) {
                            assertTrue(catalogueClient.getGroupManagementApi(RaftPeerId.valueOf("n" + n))
                                    .remove(RaftGroupId.valueOf(CATALOGUE), false, false).isSuccess());
                        }
                        try (var noRetry = client(group(CATALOGUE, ports), 1, RaftPeerId.valueOf("n" + catalogueLeader))) {
                            var unavailable = new RatisCatalogue(noRetry, CLUSTER, DATABASE, CATALOGUE, 1, new byte[32], META);
                            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
                            boolean quorumUnavailable = false;
                            do {
                                try { unavailable.read(DOMAIN); Thread.sleep(100); }
                                catch (IOException expected) { quorumUnavailable = true; }
                            } while (!quorumUnavailable && System.nanoTime() < deadline);
                            assertTrue(quorumUnavailable, "Catalogue quorum loss did not stop linearizable reads");
                        }
                        var afterLoss = routed.prepare(op(12), List.of(new Mutation(Mutation.Kind.PUT, DATA, bytes("b"), bytes("cached"), Mutation.ReturnMode.CURRENT)));
                        assertArrayEquals(bytes("cached"), executeEventually(routed, afterLoss).value(0));
                        int dataLeader;
                        try (var leftClient = client(group(LEFT, ports), 100)) { dataLeader = leader(leftClient, LEFT); }
                        nodes[dataLeader].destroyForcibly();
                        assertTrue(nodes[dataLeader].waitFor(10, TimeUnit.SECONDS));
                        // Same immutable command returns the original receipt from the surviving quorum.
                        assertArrayEquals(bytes("cached"), executeEventually(routed, afterLoss).value(0));
                        var afterKill = routed.prepare(op(13), List.of(new Mutation(Mutation.Kind.PUT, DATA, bytes("c"), bytes("survived"))));
                        assertEquals(CommandResult.Status.SUCCESS, executeEventually(routed, afterKill).status());
                        assertArrayEquals(bytes("survived"), readEventually(routed, "c"));
                        assertArrayEquals(bytes("right"), readEventually(routed, "z"));
                    }
                }
            } finally {
                for (var node : nodes) if (node != null) node.destroyForcibly();
                for (var node : nodes) if (node != null) node.waitFor(10, TimeUnit.SECONDS);
            }
        });
    }

    private static CommandResult executeEventually(RatisRoutedClient client, RatisRoutedClient.PreparedCommand command) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(35);
        IOException last = null;
        do { try { return client.execute(command); } catch (IOException failure) { last = failure; Thread.sleep(100); } }
        while (System.nanoTime() < deadline);
        throw new AssertionError("Data command did not complete with original identity", last);
    }
    private static byte[] readEventually(RatisRoutedClient client, String key) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(35);
        IOException last = null;
        do { try { return client.read(DATA, bytes(key)); } catch (IOException failure) { last = failure; Thread.sleep(100); } }
        while (System.nanoTime() < deadline);
        throw new AssertionError("Data read did not complete", last);
    }
    private static int leader(RaftClient client, UUID group) throws Exception {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(35);
        do {
            for (int n = 0; n < 3; n++) {
                var reply = client.getGroupManagementApi(RaftPeerId.valueOf("n" + n)).info(RaftGroupId.valueOf(group));
                if (reply.isSuccess() && reply.getRoleInfoProto().getRole() == RaftPeerRole.LEADER) return n;
            }
            Thread.sleep(100);
        } while (System.nanoTime() < deadline);
        throw new AssertionError("No leader");
    }
    private static void bootstrap(UUID gid, int[] ports) throws Exception {
        try (var client = client(group(gid, ports), 100)) {
            UUID col = gid.equals(CATALOGUE) ? META : DATA;
            var query = Message.valueOf(ByteString.copyFrom(ByteBuffer.allocate(17).putLong(col.getMostSignificantBits())
                    .putLong(col.getLeastSignificantBits()).put((byte)(gid.equals(RIGHT) ? 'z' : 'a')).array()));
            for (var peer : group(gid, ports).getPeers()) {
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
                boolean ready = false;
                do {
                    try { ready = client.io().sendStaleRead(query, 0, peer.getId()).isSuccess(); }
                    catch (IOException ignored) {}
                    if (!ready) Thread.sleep(50);
                } while (!ready && System.nanoTime() < deadline);
                assertTrue(ready, "Initial voter not applied: " + gid + "/" + peer.getId());
            }
        }
    }
    private Process start(int node, int[] ports) throws Exception {
        Path dir = root.resolve("node" + node); Files.createDirectories(dir);
        var args = new ArrayList<>(List.of(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
                "--enable-native-access=ALL-UNNAMED", "-Dorg.slf4j.simpleLogger.defaultLogLevel=warn", "-cp", System.getProperty("java.class.path"),
                getClass().getName(), dir.toString(), Integer.toString(node)));
        for (int port : ports) args.add(Integer.toString(port));
        var process = new ProcessBuilder(args).redirectErrorStream(true).redirectOutput(dir.resolve("process.log").toFile()).start();
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(15);
        while (process.isAlive() && !Files.exists(dir.resolve("ready")) && System.nanoTime() < deadline) Thread.sleep(20);
        if (process.isAlive() && Files.exists(dir.resolve("ready"))) return process;
        process.destroyForcibly(); process.waitFor(10, TimeUnit.SECONDS);
        throw new AssertionError("Child startup failed; log " + dir.resolve("process.log") + "\n" + Files.readString(dir.resolve("process.log")));
    }
    private static RangeDirectory directory(int[] ports, long generation) {
        var replicas = new ArrayList<ReplicaDescriptor>();
        for (int n = 0; n < 3; n++) replicas.add(new ReplicaDescriptor(id(20+n), id(30+n), id(40+n), "n"+n,
                "127.0.0.1:"+ports[n], ReplicaDescriptor.Role.VOTER));
        return new RangeDirectory(CLUSTER, DATABASE, DOMAIN, 1, new byte[32], generation, Set.of(DATA), 0, List.of(
                new RangeDescriptor(id(50), LEFT, 1, 0, null, bytes("m"), RangeDescriptor.TopologyState.ACTIVE, replicas),
                new RangeDescriptor(id(51), RIGHT, 1, 0, bytes("m"), null, RangeDescriptor.TopologyState.ACTIVE, replicas)));
    }
    private static RaftGroup group(UUID gid, int[] ports) {
        var peers = new ArrayList<RaftPeer>();
        for (int n = 0; n < 3; n++) peers.add(RaftPeer.newBuilder().setId("n"+n).setAddress("127.0.0.1:"+ports[n]).build());
        return RaftGroup.valueOf(RaftGroupId.valueOf(gid), peers);
    }
    private static RaftClient client(RaftGroup group, int attempts) {
        return client(group, attempts, null);
    }
    private static RaftClient client(RaftGroup group, int attempts, RaftPeerId leader) {
        var properties = new RaftProperties(); org.apache.ratis.RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
        RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(3, TimeUnit.SECONDS));
        return RaftClient.newBuilder().setRaftGroup(group).setLeaderId(leader).setProperties(properties)
                .setRetryPolicy(RetryPolicies.retryUpToMaximumCountWithFixedSleep(attempts, TimeDuration.valueOf(100, TimeUnit.MILLISECONDS))).build();
    }
    private static OperationId op(long n) { return new OperationId(SESSION, 1, n); }
    private static UUID id(long n) { return new UUID(0, n); }
    private static byte[] bytes(String s) { return s.getBytes(StandardCharsets.UTF_8); }
    private static Message message(DistributedCommand command) { return Message.valueOf(ByteString.copyFrom(DistributedCommandCodec.encode(command))); }
    private static int port() throws Exception { try (var s = new ServerSocket(0)) { return s.getLocalPort(); } }

    public static void main(String[] args) throws Exception {
        Path dir = Path.of(args[0]); int node = Integer.parseInt(args[1]);
        int[] ports = Arrays.stream(args).skip(2).mapToInt(Integer::parseInt).toArray();
        String defaults;
        try (var input = RatisCatalogueProcessesTest.class.getClassLoader().getResourceAsStream("it/cavallium/rockserver/core/resources/default.conf")) {
            defaults = new String(Objects.requireNonNull(input).readAllBytes(), StandardCharsets.UTF_8);
        }
        Path config = dir.resolve("pool.conf");
        Files.writeString(config, defaults + "\ndatabase.global.column-options: [ ${database.global.fallback-column-options} { name: \"data\" } ]\n");
        var log = LoggerFactory.getLogger(RatisCatalogueProcessesTest.class);
        var dataPool = DistributedStoragePool.open(dir.resolve("data-pool"), ConfigParser.parse(config), log);
        var cataloguePool = DistributedStoragePool.open(dir.resolve("catalogue-pool"), ConfigParser.parse(config), log);
        var machines = new HashMap<RaftGroupId, RatisRangeStateMachine>();
        try {
            for (UUID gid : List.of(CATALOGUE, LEFT, RIGHT)) {
                boolean meta = gid.equals(CATALOGUE);
                UUID col = meta ? META : DATA;
                var interval = new RangeOwnership.Interval(meta ? CATALOGUE : DOMAIN, Set.of(col),
                        gid.equals(RIGHT) ? bytes("m") : null, gid.equals(LEFT) ? bytes("m") : null);
                var store = (meta ? cataloguePool : dataPool).openRange(new RangeOwnership.Owner(gid, 1), interval,
                        new byte[32], Map.of(col, "data"), Set.of());
                var limits = new NativeRangeImage.Limits(2 * 1024 * 1024, 32L * 1024 * 1024, 1);
                var machine = meta ? new RatisRangeStateMachine(store, group(gid, ports), limits,
                        RatisCatalogue.validator(CLUSTER, DATABASE, CATALOGUE, 1, new byte[32], META))
                        : new RatisRangeStateMachine(store, group(gid, ports), limits);
                machines.put(RaftGroupId.valueOf(gid), machine);
            }
            var properties = RatisDurability.properties(dir.resolve("raft"));
            org.apache.ratis.RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
            GrpcConfigKeys.Server.setPort(properties, ports[node]);
            var server = RaftServer.newBuilder().setServerId(RaftPeerId.valueOf("n"+node)).setGroup(group(CATALOGUE, ports))
                    .setProperties(properties).setOption(RaftStorage.StartupOption.RECOVER)
                    .setStateMachineRegistry(machines::get)
                    .setParameters(RatisSnapshotAckInterceptor.parameters((id, term, index) -> machines.get(id).syncReceivedSnapshot(term, index))).build();
            try { server.start(); Files.writeString(dir.resolve("ready"), "started"); System.in.read(); }
            finally {
                machines.values().forEach(RatisRangeStateMachine::beginShutdown); server.close();
                for (var machine : machines.values()) machine.awaitNativeReaders(Duration.ofSeconds(10));
            }
        } finally {
            if (!dataPool.close(Duration.ofSeconds(10))) throw new IOException("Data native drain incomplete");
            if (!cataloguePool.close(Duration.ofSeconds(10))) throw new IOException("Catalogue native drain incomplete");
        }
    }
}
