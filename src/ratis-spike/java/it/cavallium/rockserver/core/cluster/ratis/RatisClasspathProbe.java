package it.cavallium.rockserver.core.cluster.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionBoundaryRules;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.client.ReplicatedConnection;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadResponseCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandSeal;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseMode;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseCodec;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutation;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandCodec;
import it.cavallium.rockserver.core.cluster.metadata.MetadataGroupStartupPolicy;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.replication.ratis.RatisAdapterFactory;
import it.cavallium.rockserver.core.replication.ratis.RatisMetadataCommandSubmitter;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterConfig;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterDistributionManifest;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterLaunchPlan;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterMetadataConfig;
import it.cavallium.rockserver.core.replication.ratis.RatisServerLifecycle;
import it.cavallium.rockserver.ratis.AdapterMain;
import it.cavallium.rockserver.ratis.AdapterResourceScopeProbe;
import it.cavallium.rockserver.ratis.RatisClasspathAdapterConfig;
import it.cavallium.rockserver.ratis.RatisClasspathAdapterRuntime;
import it.cavallium.rockserver.ratis.RatisClasspathAdapterRuntimeProbe;
import it.cavallium.rockserver.ratis.RatisClasspathPartitionClient;
import it.cavallium.rockserver.ratis.RatisClasspathPartitionClientProbe;
import it.cavallium.rockserver.ratis.RatisClasspathPartitionServer;
import it.cavallium.rockserver.ratis.RockserverMetadataClasspathStateMachine;
import it.cavallium.rockserver.ratis.RockserverRatisClasspathSnapshotArchive;
import it.cavallium.rockserver.ratis.RockserverRatisClasspathStateMachineFactory;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.DataOutputStream;
import java.io.IOException;
import java.lang.reflect.Proxy;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.io.MD5Hash;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.SetConfigurationRequest;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.proto.RaftProtos.ReplicationLevel;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.TransactionContext;
import org.apache.ratis.statemachine.impl.SingleFileSnapshotInfo;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.ExitUtils;
import org.apache.ratis.util.MD5FileUtil;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;
import org.rocksdb.Checkpoint;
import org.rocksdb.Options;
import org.rocksdb.RocksDB;
import org.rocksdb.RocksDBException;

/**
 * Classpath-only Ratis probe.
 *
 * <p>Ratis 3.2.2 server artifacts are not JPMS-clean with Rockserver because
 * {@code ratis-server} and {@code ratis-server-api} contain split packages.
 * Keep this probe outside the main module path until the production adapter
 * boundary is selected.
 */
public final class RatisClasspathProbe {

    public static void main(String[] args) throws Exception {
        var options = ProbeOptions.parse(args);
        if (options.externalProcessOnly()) {
            runExternalProcessAdapterRehearsal(options);
            return;
        }

        AdapterResourceScopeProbe.run();
        RatisClasspathAdapterRuntimeProbe.run();
        RatisClasspathPartitionClientProbe.run();
        runAdapterConfigProbe();
        runRockserverSnapshotArchiveRejectionProbe();
        runDirectStateMachineProbe();
        runTransportChoiceProbe();
        runDurabilitySettingsProbe();
        runPartitionServerFailedStartRetryProbe();
        runAdapterRuntimeReloadCleanupProbe();
        runThreePeerGroupProbe();
        runRockserverSinglePartitionProbe();
        runMetadataGroupProbe();
        runMetadataGroupUnsafeSeedRejectionProbe();
        runExternalProcessAdapterRehearsal(options);
        runMembershipChangeProbe();
        runRocksDbSnapshotProbe();
        runRocksDbSnapshotTransferProbe();
    }

    private record ProbeOptions(boolean externalProcessOnly, Path adapterHome, Path externalProcessStorageRoot) {

        private static ProbeOptions parse(String[] args) {
            var externalProcessOnly = false;
            var adapterHome = Path.of("target", "ratis-adapter-distribution");
            Path externalProcessStorageRoot = null;
            for (var arg : args) {
                if (arg.equals("--external-process-only")) {
                    externalProcessOnly = true;
                } else if (arg.startsWith("--external-process-adapter-home=")) {
                    adapterHome = Path.of(value(arg, "--external-process-adapter-home="));
                } else if (arg.startsWith("--external-process-storage-root=")) {
                    externalProcessStorageRoot = Path.of(value(arg, "--external-process-storage-root="));
                } else {
                    throw new IllegalArgumentException("Unsupported Ratis classpath probe argument: " + arg);
                }
            }
            if (externalProcessStorageRoot == null) {
                externalProcessStorageRoot = Path.of("target", "ratis-spike-process-adapter-rehearsal");
            }
            return new ProbeOptions(
                    externalProcessOnly,
                    adapterHome.toAbsolutePath().normalize(),
                    externalProcessStorageRoot.toAbsolutePath().normalize()
            );
        }

        private static String value(String arg, String prefix) {
            var value = arg.substring(prefix.length());
            if (value.isBlank()) {
                throw new IllegalArgumentException("Ratis classpath probe argument must not be blank: " + prefix);
            }
            return value;
        }
    }

    private static void runAdapterConfigProbe() throws IOException {
        var properties = new Properties();
        properties.setProperty("metadataEpoch", "7");
        properties.setProperty("metadataSha256", "0".repeat(64));
        properties.setProperty("partition.0.partitionId", "partition-a");
        properties.setProperty("partition.0.raftGroupId", UUID.randomUUID().toString());
        properties.setProperty("partition.0.localPeerId", "node-a");
        properties.setProperty("partition.0.dataPath", "target/ratis-adapter-data/partition-partition-a");
        properties.setProperty("partition.0.peer.0.peerId", "node-a");
        properties.setProperty("partition.0.peer.0.endpoint", "grpc://127.0.0.1:19001");
        properties.setProperty("partition.0.peer.0.failureDomain", "rack-a");

        var config = RatisClasspathAdapterConfig.fromProperties(properties);

        require(config.partitions().size() == 1, "Adapter config must parse one partition");
        require(config.partitions().getFirst().voters().size() == 1, "Adapter config must parse one peer");
        config.requireMetadataEpoch(7);
        config.requireMetadataSha256("0".repeat(64));
        config.requireLocalPeerId("node-a");
        config.requireDataRoot(Path.of("target", "ratis-adapter-data"));
        config.requireMetadataGroupTopologySha256(config.metadataGroupTopologySha256());
        require(rejectsMetadataEpoch(config, 8),
                "Adapter config must reject a mismatched metadata epoch");
        require(rejectsMetadataSha256(config, "1".repeat(64)),
                "Adapter config must reject a mismatched metadata SHA-256");
        require(rejectsLocalPeerId(config, "node-b"),
                "Adapter config must reject a mismatched process node id");
        require(rejectsDataRoot(config, Path.of("target", "wrong-ratis-adapter-data")),
                "Adapter config must reject a mismatched process data root");
        require(rejectsDataRoot(
                        properties,
                        "partition-a/../../outside",
                        "target/outside",
                        Path.of("target", "ratis-adapter-data")),
                "Adapter config must reject path-segment traversal in partition ids");
        var symlinkDataRoot = Path.of("target", "ratis-adapter-symlink-data");
        var redirectedData = Path.of("target", "ratis-adapter-symlink-redirect");
        deleteRecursively(symlinkDataRoot);
        deleteRecursively(redirectedData);
        Files.createDirectories(symlinkDataRoot);
        Files.createDirectories(redirectedData);
        var partitionDataLink = symlinkDataRoot.resolve("partition-partition-a");
        Files.createSymbolicLink(partitionDataLink, redirectedData.toAbsolutePath());
        try {
            var symlinkProperties = new Properties();
            symlinkProperties.putAll(properties);
            symlinkProperties.setProperty(
                    "partition.0.dataPath",
                    symlinkDataRoot.resolve("partition-partition-a").toString()
            );
            require(rejectsDataRoot(RatisClasspathAdapterConfig.fromProperties(symlinkProperties), symlinkDataRoot),
                    "Adapter config must reject partition data symlinks outside the trusted root");
            Files.delete(partitionDataLink);
            Files.createSymbolicLink(partitionDataLink, symlinkDataRoot.resolve("missing-partition-target"));
            require(rejectsDataRoot(RatisClasspathAdapterConfig.fromProperties(symlinkProperties), symlinkDataRoot),
                    "Adapter config must reject broken partition data symlinks");
        } finally {
            deleteRecursively(symlinkDataRoot);
            deleteRecursively(redirectedData);
        }
        require(rejectsMetadataGroupTopologySha256(config, "0".repeat(64)),
                "Adapter config must reject a mismatched metadata-group topology SHA-256");
        var metadataGroupProperties = new Properties();
        metadataGroupProperties.putAll(properties);
        metadataGroupProperties.setProperty("partition.0.partitionId",
                RatisProcessAdapterConfig.METADATA_GROUP_PARTITION_ID);
        metadataGroupProperties.setProperty("partition.0.raftGroupId", "metadata-group");
        metadataGroupProperties.setProperty("partition.0.dataPath", "target/ratis-adapter-data/metadata-group");
        var metadataGroupConfig = RatisClasspathAdapterConfig.fromProperties(metadataGroupProperties);
        var metadataGroupTopologySha256 = metadataGroupConfig.metadataGroupTopologySha256();
        metadataGroupConfig.requireMetadataGroupTopologySha256(metadataGroupTopologySha256);
        metadataGroupProperties.setProperty("partition.0.raftGroupId", "tampered-metadata-group");
        require(rejectsMetadataGroupTopologySha256(
                        RatisClasspathAdapterConfig.fromProperties(metadataGroupProperties),
                        metadataGroupTopologySha256),
                "Adapter config must reject a tampered metadata-group topology");
        require(rejects(properties, "partition.2.partitionId", "partition-c"),
                "Adapter config must reject partition index gaps");
        require(rejects(properties, "partition.0.peer.2.peerId", "node-c"),
                "Adapter config must reject peer index gaps");
        require(rejects(properties, "partition.00.partitionId", "partition-alias"),
                "Adapter config must reject non-canonical partition indexes");
        require(rejects(properties, "partition.0.peer.00.peerId", "node-alias"),
                "Adapter config must reject non-canonical peer indexes");
        require(rejects(properties, "modulePath.0", "mods"),
                "Adapter config must reject unknown keys");
        require(rejectsAdditionalPartition(properties, "partition-a", UUID.randomUUID().toString(),
                        "target/ratis-adapter-data/partition-partition-b"),
                "Adapter config must reject duplicate partition ids");
        require(rejectsAdditionalPartition(properties, "partition-b", properties.getProperty("partition.0.raftGroupId"),
                        "target/ratis-adapter-data/partition-partition-b"),
                "Adapter config must reject duplicate Ratis group ids");
        require(rejectsAdditionalPartition(properties, "partition-b", UUID.randomUUID().toString(),
                        "target/ratis-adapter-data/partition-partition-a"),
                "Adapter config must reject duplicate data paths");
        require(rejectsAdditionalPeer(properties, "node-a", "grpc://127.0.0.1:19002"),
                "Adapter config must reject duplicate peer ids");
        require(rejectsAdditionalPeer(properties, "node-b", "grpc://127.0.0.1:19001"),
                "Adapter config must reject duplicate peer endpoints");
        require(rejectsAdapterArguments("--print-health", "--unknown-option"),
                "Adapter entrypoint must reject unknown arguments");
        require(rejectsAdapterArguments("--print-health", "--health-port=01"),
                "Adapter entrypoint must reject non-canonical health ports");
        require(rejectsAdapterArguments("--print-health", "--rpc-port=+1"),
                "Adapter entrypoint must reject non-canonical RPC ports");
        require(rejectsAdapterArguments("--print-health", "--health-port=65536"),
                "Adapter entrypoint must reject out-of-range health ports");
        System.out.println("Ratis adapter config probe OK: partitions=" + config.partitions().size());
    }

    private static void runDirectStateMachineProbe() throws Exception {
        var peer = RaftPeer.newBuilder()
                .setId("probe-node-1")
                .setAddress("127.0.0.1:19000")
                .build();
        var group = RaftGroup.valueOf(RaftGroupId.randomId(), peer);
        var stateMachine = new EchoStateMachine();
        var command = "probe-command";
        var logEntry = RaftProtos.LogEntryProto.newBuilder()
                .setTerm(1)
                .setIndex(1)
                .setStateMachineLogEntry(RaftProtos.StateMachineLogEntryProto.newBuilder()
                        .setLogData(ByteString.copyFrom(command, StandardCharsets.UTF_8))
                        .build())
                .build();
        var transaction = TransactionContext.newBuilder()
                .setStateMachine(stateMachine)
                .setLogEntry(logEntry)
                .build();
        var applied = stateMachine.applyTransaction(transaction)
                .get(5, TimeUnit.SECONDS)
                .getContent()
                .toString(StandardCharsets.UTF_8);
        var queried = stateMachine.query(Message.valueOf("probe-query"))
                .get(5, TimeUnit.SECONDS)
                .getContent()
                .toString(StandardCharsets.UTF_8);

        if (!"applied:probe-command".equals(applied)) {
            throw new IllegalStateException("Unexpected apply response: " + applied);
        }
        if (!"query:probe-query:last=probe-command".equals(queried)) {
            throw new IllegalStateException("Unexpected query response: " + queried);
        }

        System.out.printf("Ratis classpath probe OK: group=%s peer=%s%n", group.getGroupId(), peer.getId());
    }

    private static void runTransportChoiceProbe() {
        var properties = baseProperties();
        var rpcType = RaftConfigKeys.Rpc.type(properties, null);
        require(SupportedRpcType.GRPC.name().equals(rpcType.name()),
                "Ratis spike must use the ratis-grpc transport");
        System.out.printf("Ratis transport probe OK: rpc=%s artifact=ratis-grpc%n", rpcType.name());
    }

    private static void runDurabilitySettingsProbe() throws Exception {
        var storageRoot = Path.of("target", "ratis-spike-durability-settings");
        deleteRecursively(storageRoot);
        try {
            var peer = newPeer("durability-node-1", allocatePort());
            var properties = serverProperties(peer, storageRoot.resolve(peer.getId().toString()));
            verifyDurabilityServerProperties(properties);
            var writeRequestType = RaftClientRequest.writeRequestType();
            require(writeRequestType.getWrite().getReplication() == ReplicationLevel.MAJORITY,
                    "Default Ratis write request type must require majority replication");
            require(RaftClientRequest.watchRequestType(1, ReplicationLevel.MAJORITY)
                            .getWatch()
                            .getReplication() == ReplicationLevel.MAJORITY,
                    "Ratis watch API must expose MAJORITY replication");
            require(RaftClientRequest.watchRequestType(1, ReplicationLevel.MAJORITY_COMMITTED)
                            .getWatch()
                            .getReplication() == ReplicationLevel.MAJORITY_COMMITTED,
                    "Ratis watch API must expose MAJORITY_COMMITTED quorum commit");
            System.out.printf(
                    "Ratis durability settings probe OK: forceSyncNum=%s unsafeFlush=%s asyncFlush=%s writeReplication=%s%n",
                    RaftServerConfigKeys.Log.forceSyncNum(properties),
                    RaftServerConfigKeys.Log.unsafeFlushEnabled(properties),
                    RaftServerConfigKeys.Log.asyncFlushEnabled(properties),
                    writeRequestType.getWrite().getReplication());
        } finally {
            deleteRecursively(storageRoot);
        }
    }

    private static void runPartitionServerFailedStartRetryProbe() throws Exception {
        var storageRoot = Path.of("target", "ratis-spike-partition-server-failed-start");
        deleteRecursively(storageRoot);
        ExitUtils.disableSystemExit();
        ExitUtils.clear();
        try (var occupied = new ServerSocket()) {
            occupied.bind(new InetSocketAddress("127.0.0.1", 0));
            var peer = new RatisPeerDescriptor(
                    "failed-start-node",
                    "grpc://127.0.0.1:" + occupied.getLocalPort(),
                    "ratis-spike"
            );
            var descriptor = new RatisPartitionDescriptor(
                    "failed-start-partition",
                    UUID.randomUUID().toString(),
                    peer.peerId(),
                    List.of(peer),
                    storageRoot.resolve("partition")
            );
            try (var server = new RatisClasspathPartitionServer(descriptor, new EchoStateMachine())) {
                require(rejectsPartitionServerStart(server),
                        "Ratis classpath partition server must reject an occupied listener port");
                require(ExitUtils.clear(), "Ratis bind failure must record a terminated state");
                occupied.close();
                server.start();
            }
            verifyPartitionServerCloseRetry(descriptor);
            System.out.println("Ratis partition-server failed-start retry probe OK");
        } finally {
            deleteRecursively(storageRoot);
        }
    }

    private static boolean rejectsPartitionServerStart(RatisClasspathPartitionServer server) {
        try {
            server.start();
            return false;
        } catch (IOException | RuntimeException expected) {
            return true;
        }
    }

    private static void verifyPartitionServerCloseRetry(RatisPartitionDescriptor descriptor) throws Exception {
        var closeFailure = new IOException("injected partition server close failure");
        var closeAttempts = new AtomicInteger();
        var raftServer = (RaftServer) Proxy.newProxyInstance(
                RaftServer.class.getClassLoader(),
                new Class<?>[]{RaftServer.class},
                (_, method, _) -> {
                    if (!method.getName().equals("close")) {
                        throw new UnsupportedOperationException("Unexpected proxy call: " + method.getName());
                    }
                    if (closeAttempts.incrementAndGet() == 1) {
                        throw closeFailure;
                    }
                    return null;
                }
        );
        var server = new RatisClasspathPartitionServer(descriptor, new EchoStateMachine());
        var serverField = RatisClasspathPartitionServer.class.getDeclaredField("server");
        serverField.setAccessible(true);
        serverField.set(server, raftServer);

        IOException reportedFailure;
        try {
            server.close();
            throw new IllegalStateException("Partition server close probe must inject a failure");
        } catch (IOException e) {
            reportedFailure = e;
        }
        require(reportedFailure == closeFailure,
                "Partition server close retry must preserve the original close failure");
        server.close();
        server.close();
        require(closeAttempts.get() == 2,
                "Partition server close retry must retain ownership until cleanup succeeds");
    }

    private static void runAdapterRuntimeReloadCleanupProbe() throws Exception {
        var storageRoot = Path.of("target", "ratis-spike-adapter-runtime-reload-cleanup");
        deleteRecursively(storageRoot);
        try {
            var removedDescriptor = runtimeProbeDescriptor(
                    "runtime-remove",
                    storageRoot.resolve("remove"),
                    allocatePort()
            );
            try (var runtime = new RatisClasspathAdapterRuntime(
                    List.of(removedDescriptor),
                    _ -> new EchoStateMachine()
            )) {
                runtime.start();
                var retiredClient = runtime.borrowedClient(removedDescriptor.partitionId());
                runtime.reload(List.of(), _ -> new EchoStateMachine());
                require(runtime.isStarted(), "Successful runtime partition removal must keep runtime started");
                require(runtime.clients().isEmpty(), "Successful runtime partition removal must unpublish clients");
                require(rejectsRetiredRuntimeClient(retiredClient),
                        "Removed runtime partition client handles must reject operations after unpublication");
            }

            var retryCleanupDescriptor = runtimeProbeDescriptor(
                    "runtime-retry-client-cleanup",
                    storageRoot.resolve("retry-client-cleanup"),
                    allocatePort()
            );
            try (var runtime = new RatisClasspathAdapterRuntime(
                    List.of(retryCleanupDescriptor),
                    _ -> new EchoStateMachine()
            )) {
                runtime.start();
                var retiredClient = runtime.borrowedClient(retryCleanupDescriptor.partitionId());
                var closeAttempts = new AtomicInteger();
                injectPartitionClientCloseFailures(
                        runtime.client(retryCleanupDescriptor.partitionId()),
                        closeAttempts,
                        1
                );

                var rejected = false;
                try {
                    runtime.reload(List.of(), _ -> new EchoStateMachine());
                } catch (RuntimeException expected) {
                    rejected = true;
                }
                require(rejected, "Runtime removal probe must report its injected client close failure");
                require(closeAttempts.get() == 2,
                        "Runtime removal cleanup must retry privately retained client close obligations");
                require(!runtime.isStarted(), "Failed runtime partition removal must stop runtime");
                require(runtime.clients().isEmpty(), "Failed runtime partition removal must keep clients unpublished");
                require(rejectsRetiredRuntimeClient(retiredClient),
                        "Failed runtime partition removal must retire stale client handles");
                require(!runtime.health().isReady(), "Failed runtime partition removal must degrade runtime health");
            }

            var restartCleanupDescriptor = runtimeProbeDescriptor(
                    "runtime-restart-client-cleanup",
                    storageRoot.resolve("restart-client-cleanup"),
                    allocatePort()
            );
            try (var runtime = new RatisClasspathAdapterRuntime(
                    List.of(restartCleanupDescriptor),
                    _ -> new EchoStateMachine()
            )) {
                runtime.start();
                var closeAttempts = new AtomicInteger();
                injectPartitionClientCloseFailures(
                        runtime.client(restartCleanupDescriptor.partitionId()),
                        closeAttempts,
                        2
                );

                var rejected = false;
                try {
                    runtime.close();
                } catch (RuntimeException expected) {
                    rejected = true;
                }
                require(rejected, "Runtime close probe must report its injected client close failure");
                require(closeAttempts.get() == 1,
                        "Runtime close must retain its failed client cleanup obligation");
                require(!runtime.isStarted(), "Failed runtime close must stop runtime");
                require(runtime.clients().isEmpty(), "Failed runtime close must unpublish clients");

                rejected = false;
                try {
                    runtime.start();
                } catch (RuntimeException expected) {
                    rejected = true;
                }
                require(rejected, "Runtime startup cleanup preflight must report retained cleanup failures");
                require(closeAttempts.get() == 2,
                        "Runtime startup cleanup preflight must retry retained client cleanup");
                require(!runtime.isStarted(), "Failed startup cleanup preflight must leave runtime stopped");
                require(runtime.clients().isEmpty(),
                        "Failed startup cleanup preflight must not publish fresh clients");

                runtime.start();
                require(closeAttempts.get() == 3,
                        "Runtime startup cleanup preflight must retry retained cleanup before restart");
                require(runtime.isStarted(), "Runtime must restart after retained cleanup succeeds");
                require(runtime.clients().size() == 1,
                        "Runtime restart must publish a fresh client only after retained cleanup succeeds");
            }

            var failedDescriptor = runtimeProbeDescriptor(
                    "runtime-failed-reload",
                    storageRoot.resolve("failed-reload"),
                    allocatePort()
            );
            try (var runtime = new RatisClasspathAdapterRuntime(
                    List.of(failedDescriptor),
                    _ -> new EchoStateMachine()
            )) {
                runtime.start();
                var rejected = false;
                try {
                    runtime.reload(
                            List.of(failedDescriptor),
                            new RatisClasspathAdapterRuntime.StateMachineFactory() {
                                @Override
                                public StateMachine create(RatisPartitionDescriptor descriptor) {
                                    return new EchoStateMachine();
                                }

                                @Override
                                public void reload(StateMachine stateMachine, RatisPartitionDescriptor descriptor)
                                        throws IOException {
                                    throw new IOException("injected runtime reload failure");
                                }
                            }
                    );
                } catch (IOException expected) {
                    rejected = true;
                }
                require(rejected, "Runtime reload probe must inject a state-machine reload failure");
                require(!runtime.isStarted(), "Failed live state-machine reload must stop runtime");
                require(runtime.clients().isEmpty(), "Failed live state-machine reload must unpublish clients");
                require(!runtime.health().isReady(), "Failed live state-machine reload must degrade runtime health");
            }

            var fatalStartupFirstPort = allocatePort();
            var fatalStartupFirst = runtimeProbeDescriptor(
                    "runtime-fatal-startup-first",
                    storageRoot.resolve("fatal-startup-first"),
                    fatalStartupFirstPort
            );
            var fatalStartupSecond = runtimeProbeDescriptor(
                    "runtime-fatal-startup-second",
                    storageRoot.resolve("fatal-startup-second"),
                    allocatePort()
            );
            var fatalStartupFailure = new AssertionError("injected runtime startup failure");
            try (var runtime = new RatisClasspathAdapterRuntime(
                    List.of(fatalStartupFirst, fatalStartupSecond),
                    descriptor -> {
                        if (descriptor.partitionId().equals(fatalStartupSecond.partitionId())) {
                            throw fatalStartupFailure;
                        }
                        return new EchoStateMachine();
                    }
            )) {
                Error reportedFailure;
                try {
                    runtime.start();
                    throw new IllegalStateException("Runtime startup probe must inject a fatal failure");
                } catch (Error e) {
                    reportedFailure = e;
                }
                require(reportedFailure == fatalStartupFailure,
                        "Runtime startup rollback must preserve the original fatal failure");
                require(!runtime.isStarted(), "Fatal runtime startup failure must leave runtime stopped");
                require(runtime.clients().isEmpty(), "Fatal runtime startup failure must leave clients unpublished");
                require(!runtime.health().isReady(), "Fatal runtime startup failure must degrade runtime health");
            }
            try (var available = new ServerSocket()) {
                available.bind(new InetSocketAddress("127.0.0.1", fatalStartupFirstPort));
            }

            var fatalReloadDescriptor = runtimeProbeDescriptor(
                    "runtime-fatal-reload",
                    storageRoot.resolve("fatal-reload"),
                    allocatePort()
            );
            try (var runtime = new RatisClasspathAdapterRuntime(
                    List.of(fatalReloadDescriptor),
                    _ -> new EchoStateMachine()
            )) {
                runtime.start();
                var retiredClient = runtime.borrowedClient(fatalReloadDescriptor.partitionId());
                var fatalReloadFailure = new AssertionError("injected runtime reload failure");
                Error reportedFailure;
                try {
                    runtime.reload(
                            List.of(fatalReloadDescriptor),
                            new RatisClasspathAdapterRuntime.StateMachineFactory() {
                                @Override
                                public StateMachine create(RatisPartitionDescriptor descriptor) {
                                    return new EchoStateMachine();
                                }

                                @Override
                                public void reload(StateMachine stateMachine, RatisPartitionDescriptor descriptor) {
                                    throw fatalReloadFailure;
                                }
                            }
                    );
                    throw new IllegalStateException("Runtime reload probe must inject a fatal failure");
                } catch (Error e) {
                    reportedFailure = e;
                }
                require(reportedFailure == fatalReloadFailure,
                        "Runtime reload rollback must preserve the original fatal failure");
                require(!runtime.isStarted(), "Fatal live state-machine reload must stop runtime");
                require(runtime.clients().isEmpty(), "Fatal live state-machine reload must unpublish clients");
                require(rejectsRetiredRuntimeClient(retiredClient),
                        "Fatal live state-machine reload must retire stale client handles");
                require(!runtime.health().isReady(), "Fatal live state-machine reload must degrade runtime health");
            }
            System.out.println("Ratis adapter-runtime reload cleanup probe OK");
        } finally {
            deleteRecursively(storageRoot);
        }
    }

    private static boolean rejectsRetiredRuntimeClient(RatisPartitionClient client) {
        try {
            client.queryLocal(new byte[0]).join();
            return false;
        } catch (CompletionException e) {
            return e.getCause() instanceof IllegalStateException
                    && e.getCause().getMessage().contains("partition is retired");
        }
    }

    private static void injectPartitionClientCloseFailures(RatisClasspathPartitionClient client,
                                                           AtomicInteger closeAttempts,
                                                           int failures) throws Exception {
        var clientField = RatisClasspathPartitionClient.class.getDeclaredField("client");
        clientField.setAccessible(true);
        var delegate = (RaftClient) clientField.get(client);
        var closeFailure = new IOException("injected partition client close failure");
        var failingClient = (RaftClient) Proxy.newProxyInstance(
                RaftClient.class.getClassLoader(),
                new Class<?>[]{RaftClient.class},
                (_, method, _) -> {
                    if (!method.getName().equals("close")) {
                        throw new UnsupportedOperationException("Unexpected proxy call: " + method.getName());
                    }
                    if (closeAttempts.incrementAndGet() <= failures) {
                        throw closeFailure;
                    }
                    delegate.close();
                    return null;
                }
        );
        clientField.set(client, failingClient);
    }

    private static RatisPartitionDescriptor runtimeProbeDescriptor(String id, Path dataPath, int port) {
        var peer = new RatisPeerDescriptor(
                id + "-node",
                "grpc://127.0.0.1:" + port,
                "ratis-spike"
        );
        return new RatisPartitionDescriptor(
                id + "-partition",
                UUID.randomUUID().toString(),
                peer.peerId(),
                List.of(peer),
                dataPath
        );
    }

    private static void runThreePeerGroupProbe() throws Exception {
        var storageRoot = Path.of("target", "ratis-spike-three-peer");
        deleteRecursively(storageRoot);
        Files.createDirectories(storageRoot);

        var peers = List.of(
                newPeer("probe-node-1", allocatePort()),
                newPeer("probe-node-2", allocatePort()),
                newPeer("probe-node-3", allocatePort())
        );
        var group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
        var servers = new ArrayList<RaftServer>();

        try {
            for (var peer : peers) {
                var properties = serverProperties(peer, storageRoot.resolve(peer.getId().toString()));
                var server = RaftServer.newBuilder()
                        .setServerId(peer.getId())
                        .setGroup(group)
                        .setStateMachine(new EchoStateMachine())
                        .setProperties(properties)
                        .build();
                servers.add(server);
                server.start();
            }

            try (var client = RaftClient.newBuilder()
                    .setRaftGroup(group)
                    .setProperties(clientProperties())
                    .build()) {
                var writeReply = sendWithRetry(client, "three-peer-command");
                if (!"applied:three-peer-command".equals(content(writeReply.getMessage()))) {
                    throw new IllegalStateException("Unexpected three-peer write response: " + writeReply);
                }

                var readReply = sendReadWithRetry(client, "three-peer-query");
                var readContent = content(readReply.getMessage());
                if (!"query:three-peer-query:last=three-peer-command".equals(readContent)) {
                    throw new IllegalStateException("Unexpected three-peer query response: " + readContent);
                }
                verifyReadPaths(client, peers, writeReply.getLogIndex());
                verifyDurabilityWatchPaths(client, writeReply.getLogIndex());

                System.out.printf("Ratis three-peer probe OK: group=%s logIndex=%s%n",
                        group.getGroupId(),
                        writeReply.getLogIndex());
            }
        } finally {
            for (int i = servers.size() - 1; i >= 0; i--) {
                servers.get(i).close();
            }
            deleteRecursively(storageRoot);
        }
    }

    private static void runRockserverSinglePartitionProbe() throws Exception {
        var storageRoot = Path.of("target", "ratis-spike-rockserver-single-partition");
        deleteRecursively(storageRoot);
        Files.createDirectories(storageRoot);

        var peers = List.of(
                newPeer("rockserver-node-1", allocatePort()),
                newPeer("rockserver-node-2", allocatePort()),
                newPeer("rockserver-node-3", allocatePort())
        );
        var group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
        var metadata = rockserverMetadata(peers, group.getGroupId().getUuid().toString());
        var partition = metadata.requirePartition(ROCKSERVER_PARTITION);
        var stateMachineFactory = new RockserverRatisClasspathStateMachineFactory(metadata);
        require(rejectsPartitionDescriptorAlias(stateMachineFactory, partition, group, peers, storageRoot),
                "Rockserver state-machine factory must reject non-canonical partition descriptor ids");
        require(rejectsPartitionDescriptorTopologyMismatch(stateMachineFactory, partition, group, peers, storageRoot),
                "Rockserver state-machine factory must reject partition topology mismatches");
        var runtimes = new ArrayList<RatisClasspathAdapterRuntime>();
        var runtimeDescriptors = new ArrayList<List<RatisPartitionDescriptor>>();

        try {
            for (var peer : peers) {
                var descriptor = ratisPartitionDescriptor(partition, group, peers, storageRoot, peer.getId());
                var runtime = new RatisClasspathAdapterRuntime(
                        List.of(descriptor),
                        stateMachineFactory
                );
                runtimeDescriptors.add(List.of(descriptor));
                runtimes.add(runtime);
                runtime.start();
            }

            var client = runtimes.getFirst().client(partition.partitionId().toString());
            var firstValue = bytes("rockserver-ratis-value-1");
            var putReply = proposeWithRetry(client, rockserverWrite(1, ReplicatedMutation.put(
                    ROCKSERVER_COLUMN_ID,
                    ROCKSERVER_KEY,
                    Buf.wrap(firstValue).freeze()
            )), "Rockserver Ratis put");
            assertPointRead(queryLinearizableWithRetry(client, rockserverRead(), "Rockserver Ratis read after put"),
                    firstValue,
                    "Rockserver Ratis read after put");
            require(rejectsLinearizableQuery(client, rockserverRead(ROCKSERVER_SCHEMA_EPOCH, ROCKSERVER_OUTSIDE_KEY)),
                    "Rockserver Ratis state machine must reject a point read outside its local partition");
            var rejectedValue = bytes("rockserver-ratis-value-unsealed-must-not-commit");
            require(rejectsProposal(client, legacyRockserverWrite(100, ReplicatedMutation.put(
                            ROCKSERVER_COLUMN_ID,
                            ROCKSERVER_KEY,
                            Buf.wrap(rejectedValue).freeze()
                    ))),
                    "Rockserver Ratis state machine must reject an unsealed proposal before append");
            assertPointRead(queryLinearizableWithRetry(client, rockserverRead(),
                            "Rockserver Ratis read after rejected unsealed proposal"),
                    firstValue,
                    "Rockserver Ratis read after rejected unsealed proposal");

            var deleteReply = proposeWithRetry(client, rockserverWrite(2, ReplicatedMutation.delete(
                    ROCKSERVER_COLUMN_ID,
                    ROCKSERVER_KEY
            )), "Rockserver Ratis delete");
            assertPointRead(queryLinearizableWithRetry(client, rockserverRead(), "Rockserver Ratis read after delete"),
                    null,
                    "Rockserver Ratis read after delete");

            var secondValue = bytes("rockserver-ratis-value-2");
            proposeWithRetry(client, rockserverWrite(3, ReplicatedMutation.put(
                    ROCKSERVER_COLUMN_ID,
                    ROCKSERVER_KEY,
                    Buf.wrap(secondValue).freeze()
            )), "Rockserver Ratis second put");
            assertPointRead(queryLinearizableWithRetry(client, rockserverRead(), "Rockserver Ratis read after second put"),
                    secondValue,
                    "Rockserver Ratis read after second put");
            var rangeValue = bytes("rockserver-ratis-range-value");
            proposeWithRetry(client, rockserverWrite(4, ReplicatedMutation.put(
                    ROCKSERVER_COLUMN_ID,
                    ROCKSERVER_RANGE_KEY,
                    Buf.wrap(rangeValue).freeze()
            )), "Rockserver Ratis range put");
            assertRangeRead(queryLinearizableWithRetry(client,
                            rockserverRangeRead(ROCKSERVER_KEY, ROCKSERVER_OUTSIDE_KEY, false),
                            "Rockserver Ratis range read"),
                    List.of(
                            new KV(ROCKSERVER_KEY, Buf.wrap(secondValue).freeze()),
                            new KV(ROCKSERVER_RANGE_KEY, Buf.wrap(rangeValue).freeze())
                    ),
                    "Rockserver Ratis range read");

            var laggingRuntimeIndex = runtimes.size() - 1;
            runtimes.get(laggingRuntimeIndex).close();
            var snapshotValue = bytes("rockserver-ratis-value-snapshot");
            var snapshotCommit = proposeWithRetry(client, rockserverWrite(5, ReplicatedMutation.put(
                    ROCKSERVER_COLUMN_ID,
                    ROCKSERVER_KEY,
                    Buf.wrap(snapshotValue).freeze()
            )), "Rockserver Ratis snapshot put");
            long snapshotIndex;
            try (var snapshotClient = newClient(group, null)) {
                var snapshotReply = snapshotClient.getSnapshotManagementApi()
                        .create(true, TimeUnit.SECONDS.toMillis(15));
                if (!snapshotReply.isSuccess()) {
                    throw new IllegalStateException("Unexpected Rockserver Ratis snapshot response: " + snapshotReply);
                }
                if (snapshotReply.getLogIndex() < snapshotCommit.index()) {
                    throw new IllegalStateException("Rockserver Ratis snapshot index " + snapshotReply.getLogIndex()
                            + " is before write index " + snapshotCommit.index());
                }
                snapshotIndex = snapshotReply.getLogIndex();
            }

            var postSnapshotValue = bytes("rockserver-ratis-value-post-snapshot");
            proposeWithRetry(client, rockserverWrite(6, ReplicatedMutation.put(
                    ROCKSERVER_COLUMN_ID,
                    ROCKSERVER_KEY,
                    Buf.wrap(postSnapshotValue).freeze()
            )), "Rockserver Ratis post-snapshot put");

            var reloadedMetadata = rockserverMetadata(
                    peers,
                    group.getGroupId().getUuid().toString(),
                    ROCKSERVER_SCHEMA_EPOCH + 1
            );
            var reloadedFactory = new RockserverRatisClasspathStateMachineFactory(reloadedMetadata);
            for (int i = 0; i < runtimes.size(); i++) {
                if (i != laggingRuntimeIndex) {
                    runtimes.get(i).reload(runtimeDescriptors.get(i), reloadedFactory);
                }
            }
            var reloadedValue = bytes("rockserver-ratis-value-reloaded");
            var reloadedCommit = proposeWithRetry(client, rockserverWrite(7, ROCKSERVER_SCHEMA_EPOCH + 1,
                    ReplicatedMutation.put(
                            ROCKSERVER_COLUMN_ID,
                            ROCKSERVER_KEY,
                            Buf.wrap(reloadedValue).freeze()
                    )), "Rockserver Ratis reloaded put");
            assertPointRead(queryLinearizableWithRetry(client,
                            rockserverRead(ROCKSERVER_SCHEMA_EPOCH + 1),
                            "Rockserver Ratis read after runtime metadata reload"),
                    reloadedValue,
                    "Rockserver Ratis read after runtime metadata reload");

            var laggingDescriptors = runtimeDescriptors.get(laggingRuntimeIndex);
            deleteRecursively(laggingDescriptors.getFirst().dataPath());
            var restartedLaggingRuntime = new RatisClasspathAdapterRuntime(laggingDescriptors, reloadedFactory);
            runtimes.set(laggingRuntimeIndex, restartedLaggingRuntime);
            restartedLaggingRuntime.start();
            var restartedLaggingClient = restartedLaggingRuntime.client(partition.partitionId().toString());
            assertPointRead(queryStaleWithRetry(restartedLaggingClient,
                            rockserverRead(ROCKSERVER_SCHEMA_EPOCH + 1),
                            reloadedCommit.index(),
                            "Rockserver Ratis schema-evolution snapshot-transfer follower read"),
                    reloadedValue,
                    "Rockserver Ratis schema-evolution snapshot-transfer follower read");

            var postRecoveryValue = bytes("rockserver-ratis-value-post-recovery");
            var postRecoveryCommit = proposeWithRetry(client, rockserverWrite(8, ROCKSERVER_SCHEMA_EPOCH + 1,
                    ReplicatedMutation.put(
                            ROCKSERVER_COLUMN_ID,
                            ROCKSERVER_KEY,
                            Buf.wrap(postRecoveryValue).freeze()
                    )), "Rockserver Ratis post-recovery put");
            assertPointRead(queryStaleWithRetry(restartedLaggingClient,
                            rockserverRead(ROCKSERVER_SCHEMA_EPOCH + 1),
                            postRecoveryCommit.index(),
                            "Rockserver Ratis post-recovery follower read"),
                    postRecoveryValue,
                    "Rockserver Ratis post-recovery follower read");

            System.out.printf(
                    "Ratis Rockserver single-partition adapter-runtime probe OK: group=%s putIndex=%s deleteIndex=%s reloadSchemaEpoch=%s snapshotIndex=%s%n",
                    group.getGroupId(),
                    putReply.index(),
                    deleteReply.index(),
                    ROCKSERVER_SCHEMA_EPOCH + 1,
                    snapshotIndex);
        } finally {
            for (int i = runtimes.size() - 1; i >= 0; i--) {
                runtimes.get(i).close();
            }
            deleteRecursively(storageRoot);
        }
    }

    private static void runRockserverSnapshotArchiveRejectionProbe() throws IOException {
        var storageRoot = Path.of("target", "ratis-spike-rockserver-snapshot-archive-rejection");
        deleteRecursively(storageRoot);
        var targetDirectory = storageRoot.resolve("rocksdb");
        Files.createDirectories(targetDirectory);
        Files.writeString(targetDirectory.resolve("retained"), "retained", StandardCharsets.UTF_8);
        try {
            var traversalArchive = storageRoot.resolve("traversal.snapshot");
            try (var output = new DataOutputStream(Files.newOutputStream(traversalArchive))) {
                writeSnapshotArchiveHeader(output, 1, 0);
                writeSnapshotArchivePath(output, "../escape");
                output.writeLong(0);
                output.writeLong(0);
            }
            require(rejectsSnapshotArchive(traversalArchive, targetDirectory),
                    "Rockserver Ratis snapshot archive must reject traversal paths");
            require(!Files.exists(storageRoot.resolve("escape")),
                    "Rejected Rockserver Ratis snapshot archive must not write outside its staging directory");

            var trailingArchive = storageRoot.resolve("trailing.snapshot");
            try (var output = new DataOutputStream(Files.newOutputStream(trailingArchive))) {
                writeSnapshotArchiveHeader(output, 0, 0);
                output.writeByte(1);
            }
            require(rejectsSnapshotArchive(trailingArchive, targetDirectory),
                    "Rockserver Ratis snapshot archive must reject trailing bytes");

            var checksumArchive = storageRoot.resolve("checksum.snapshot");
            try (var output = new DataOutputStream(Files.newOutputStream(checksumArchive))) {
                writeSnapshotArchiveHeader(output, 1, 1);
                writeSnapshotArchivePath(output, "CURRENT");
                output.writeLong(1);
                output.writeByte(1);
                output.writeLong(0);
            }
            require(rejectsSnapshotArchive(checksumArchive, targetDirectory),
                    "Rockserver Ratis snapshot archive must reject checksum mismatches");

            System.out.println("Ratis Rockserver snapshot-archive rejection probe OK");
        } finally {
            deleteRecursively(storageRoot);
        }
    }

    private static void writeSnapshotArchiveHeader(DataOutputStream output, int fileCount, long totalSizeBytes)
            throws IOException {
        output.writeInt(0x5253_4350);
        output.writeInt(1);
        output.writeInt(fileCount);
        output.writeLong(totalSizeBytes);
    }

    private static void writeSnapshotArchivePath(DataOutputStream output, String relativePath) throws IOException {
        var bytes = relativePath.getBytes(StandardCharsets.UTF_8);
        output.writeInt(bytes.length);
        output.write(bytes);
    }

    private static boolean rejectsSnapshotArchive(Path archive, Path targetDirectory) throws IOException {
        try {
            RockserverRatisClasspathSnapshotArchive.install(archive, targetDirectory);
            return false;
        } catch (IOException _) {
            return Files.readString(targetDirectory.resolve("retained"), StandardCharsets.UTF_8).equals("retained");
        }
    }

    private static void runMetadataGroupProbe() throws Exception {
        var storageRoot = Path.of("target", "ratis-spike-metadata-group");
        deleteRecursively(storageRoot);
        Files.createDirectories(storageRoot);

        var peers = List.of(
                newPeer("metadata-node-1", allocatePort()),
                newPeer("metadata-node-2", allocatePort()),
                newPeer("metadata-node-3", allocatePort())
        );
        var group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
        var initialMetadata = metadataGroupInitialMetadata(peers);
        var servers = new ArrayList<RatisClasspathPartitionServer>();
        var descriptors = new ArrayList<RatisPartitionDescriptor>();
        var stateMachines = new LinkedHashMap<RaftPeerId, RockserverMetadataClasspathStateMachine>();

        try {
            for (var peer : peers) {
                var descriptor = metadataRatisDescriptor(group, peers, storageRoot, peer.getId());
                descriptors.add(descriptor);
                var stateMachine = new RockserverMetadataClasspathStateMachine(initialMetadata);
                stateMachines.put(peer.getId(), stateMachine);
                var server = new RatisClasspathPartitionServer(
                        descriptor,
                        stateMachine
                );
                servers.add(server);
                server.start();
            }

            try (var client = new RatisClasspathPartitionClient(descriptors.getFirst())) {
                metadataSnapshotWithRetry(client, "metadata group initial read");
                var submitter = new RatisMetadataCommandSubmitter(client);
                long columnId;
                ClusterMetadata withColumn;
                NodeId registeredNodeId;
                URI registeredPublicEndpoint;
                URI registeredRatisEndpoint;
                PartitionDescriptor partition;
                ClusterMetadata withPartition;
                try (var metadataConnection = metadataProbeConnection(initialMetadata, submitter)) {
                    columnId = metadataConnection.getSyncApi().createColumn(
                            "metadata_accounts",
                            ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true)
                    );
                    withColumn = metadataSnapshotWithRetry(client, "metadata group create-column");
                    registeredNodeId = NodeId.of("metadata-node-4");
                    registeredPublicEndpoint = URI.create("http://127.0.0.1:" + allocatePort());
                    registeredRatisEndpoint = URI.create("grpc://127.0.0.1:" + allocatePort());
                    metadataConnection.registerNode(
                            registeredNodeId,
                            registeredPublicEndpoint,
                            registeredRatisEndpoint,
                            "ratis-spike-extra"
                    );
                    var afterRegisterNode = metadataSnapshotWithRetry(client, "metadata group register-node");
                    require(afterRegisterNode.requireNode(registeredNodeId).ratisEndpoint()
                                    .equals(registeredRatisEndpoint),
                            "metadata group register-node must be visible in committed snapshot");
                    partition = metadataConnection.createPartition(
                            new ShardId(8),
                            null,
                            null,
                            1,
                            "metadata-created-data-group",
                            List.of(NodeId.of(peers.getFirst().getId().toString()), registeredNodeId),
                            NodeId.of(peers.getFirst().getId().toString()),
                            Map.of("probe", "metadata-group")
                    );
                    withPartition = metadataSnapshotWithRetry(client, "metadata group create-partition");
                }
                require(withColumn.epoch() == initialMetadata.epoch() + 1,
                        "metadata group create-column must advance metadata epoch");
                require(withColumn.requireColumn(columnId).name().equals("metadata_accounts"),
                        "metadata group create-column must be visible in committed snapshot");
                require(withColumn.shards().equals(initialMetadata.shards()),
                        "metadata group snapshot must preserve replica groups");
                require(withPartition.requireNode(registeredNodeId).publicEndpoint().equals(registeredPublicEndpoint),
                        "metadata group register-node must preserve public endpoint");
                require(withPartition.requirePartition(partition.partitionId()).equals(partition),
                        "metadata group create-partition must be visible in committed snapshot");
                require(withPartition.requireGroup(partition.partitionId()).members().equals(partition.peers()),
                        "metadata group create-partition must publish a route group");
                require(rejectsProposal(client, new byte[]{1}),
                        "metadata group must reject malformed proposals before append");
                var concurrentNodeA = metadataProbeRegisterNode(withPartition.epoch(), "metadata-concurrent-node-a");
                var concurrentNodeB = metadataProbeRegisterNode(withPartition.epoch(), "metadata-concurrent-node-b");
                var concurrentProposalA = client.propose(MetadataCommandCodec.encode(concurrentNodeA));
                var concurrentProposalB = client.propose(MetadataCommandCodec.encode(concurrentNodeB));
                var concurrentCommitA = committedProposal(concurrentProposalA);
                var concurrentCommitB = committedProposal(concurrentProposalB);
                require(concurrentCommitA != concurrentCommitB,
                        "metadata group must commit exactly one concurrent proposal for an expected epoch");
                var withConcurrentProposal = metadataSnapshotWithRetry(client,
                        "metadata group concurrent proposal validation");
                require(withConcurrentProposal.epoch() == withPartition.epoch() + 1,
                        "metadata group concurrent proposal validation must advance exactly one epoch");
                require(withConcurrentProposal.findNode(concurrentNodeA.nodeId()).isPresent() == concurrentCommitA,
                        "metadata group committed concurrent node A must match proposal result");
                require(withConcurrentProposal.findNode(concurrentNodeB.nodeId()).isPresent() == concurrentCommitB,
                        "metadata group committed concurrent node B must match proposal result");
                var staleCommand = metadataProbeRegisterNode(withPartition.epoch(), "metadata-stale-node");
                require(rejectsProposal(client, MetadataCommandCodec.encode(staleCommand)),
                        "metadata group must reject stale proposals before append");
                require(metadataSnapshotWithRetry(client, "metadata group rejected proposal state")
                                .equals(withConcurrentProposal),
                        "metadata group rejected proposals must not change committed metadata");

                long snapshotIndex;
                try (var snapshotClient = RaftClient.newBuilder()
                        .setRaftGroup(group)
                        .setProperties(clientProperties())
                        .build()) {
                    var snapshotReply = snapshotClient.getSnapshotManagementApi()
                            .create(true, TimeUnit.SECONDS.toMillis(15));
                    require(snapshotReply.isSuccess(),
                            "metadata group snapshot request must succeed: " + snapshotReply);
                    snapshotIndex = snapshotReply.getLogIndex();
                    require(snapshotIndex >= withConcurrentProposal.epoch(),
                            "metadata group snapshot index must be nonzero after metadata commands");
                }

                var snapshotPeerId = stateMachines.entrySet().stream()
                        .filter(entry -> entry.getValue().getLatestSnapshot() != null)
                        .filter(entry -> entry.getValue().getLatestSnapshot().getTermIndex().getIndex()
                                == snapshotIndex)
                        .map(Map.Entry::getKey)
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException(
                                "No metadata state machine recorded snapshot index " + snapshotIndex
                        ));
                var snapshotDescriptor = descriptors.stream()
                        .filter(descriptor -> descriptor.localPeerId().equals(snapshotPeerId.toString()))
                        .findFirst()
                        .orElseThrow(() -> new IllegalStateException(
                                "No metadata partition descriptor matched snapshot peer " + snapshotPeerId
                        ));
                closeRatisClasspathPartitionServers(servers);
                stateMachines.clear();

                var restartedServer = new RatisClasspathPartitionServer(
                        snapshotDescriptor,
                        new RockserverMetadataClasspathStateMachine(metadataSnapshotPoison(initialMetadata))
                );
                servers.add(restartedServer);
                restartedServer.start();
                try (var restartedClient = new RatisClasspathPartitionClient(snapshotDescriptor)) {
                    var recoveredPayload = queryStaleWithRetry(
                            restartedClient,
                            RatisMetadataCommandSubmitter.snapshotQueryPayload(),
                            snapshotIndex,
                            "metadata group snapshot recovery"
                    );
                    var recoveredMetadata = RatisMetadataCommandSubmitter.decodeSnapshot(recoveredPayload);
                    require(recoveredMetadata.epoch() == withConcurrentProposal.epoch(),
                            "metadata group snapshot recovery must preserve metadata epoch");
                    require(recoveredMetadata.requireColumn(columnId).name().equals("metadata_accounts"),
                            "metadata group snapshot recovery must preserve created column");
                    require(recoveredMetadata.requirePartition(partition.partitionId()).equals(partition),
                            "metadata group snapshot recovery must preserve created partition");
                }

                System.out.printf(
                        "Ratis metadata group probe OK: group=%s column=%s partition=%s metadataEpoch=%s snapshotIndex=%s snapshotPeer=%s%n",
                        group.getGroupId(),
                        columnId,
                        partition.partitionId(),
                        withConcurrentProposal.epoch(),
                        snapshotIndex,
                        snapshotPeerId);
            }
        } finally {
            closeRatisClasspathPartitionServers(servers);
            deleteRecursively(storageRoot);
        }
    }

    private static void runMetadataGroupUnsafeSeedRejectionProbe() throws Exception {
        var storageRoot = Path.of("target", "ratis-spike-metadata-unsafe-seed");
        deleteRecursively(storageRoot);
        Files.createDirectories(storageRoot);
        var peers = List.of(newPeer("metadata-unsafe-seed-node-1", allocatePort()));
        var group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
        var stateMachine = new RockserverMetadataClasspathStateMachine(metadataGroupGeneratedLiveMetadata(peers));
        try {
            try (var storage = RaftStorage.newBuilder()
                    .setDirectory(storageRoot.resolve("ratis").toFile())
                    .setOption(RaftStorage.StartupOption.FORMAT)
                    .setStorageFreeSpaceMin(SizeInBytes.ZERO)
                    .build()) {
                storage.initialize();
                stateMachine.initialize(metadataProbeRaftServer(peers.getFirst(), group), group.getGroupId(), storage);
            }
            throw new IllegalStateException("metadata group accepted generated live metadata without snapshot");
        } catch (IOException e) {
            require(failureChainContains(e, MetadataGroupStartupPolicy.GENERATED_LIVE_METADATA_REQUIRES_SNAPSHOT),
                    "metadata group unsafe seed rejection should explain the missing snapshot");
        } finally {
            deleteRecursively(storageRoot);
        }
        System.out.println("Ratis metadata group unsafe seed rejection probe OK");
    }

    private static RaftServer metadataProbeRaftServer(RaftPeer peer, RaftGroup group) {
        return (RaftServer) Proxy.newProxyInstance(
                RaftServer.class.getClassLoader(),
                new Class<?>[]{RaftServer.class},
                (proxy, method, args) -> switch (method.getName()) {
                    case "getId" -> peer.getId();
                    case "getPeer" -> peer;
                    case "getGroupIds" -> List.of(group.getGroupId());
                    case "getGroups" -> List.of(group);
                    case "getProperties" -> baseProperties();
                    case "close" -> null;
                    case "toString" -> "metadata-probe-raft-server:" + peer.getId();
                    case "hashCode" -> System.identityHashCode(proxy);
                    case "equals" -> args != null && args.length == 1 && proxy == args[0];
                    default -> throw new UnsupportedOperationException(
                            "metadata unsafe-seed probe RaftServer proxy does not implement " + method
                    );
                }
        );
    }

    private static void closeRatisClasspathPartitionServers(List<RatisClasspathPartitionServer> servers)
            throws IOException {
        IOException failure = null;
        for (int i = servers.size() - 1; i >= 0; i--) {
            try {
                servers.get(i).close();
            } catch (IOException e) {
                if (failure == null) {
                    failure = e;
                } else {
                    failure.addSuppressed(e);
                }
            }
        }
        servers.clear();
        if (failure != null) {
            throw failure;
        }
    }

    private static ReplicatedConnection metadataProbeConnection(ClusterMetadata initialMetadata,
                                                                RatisMetadataCommandSubmitter submitter) {
        return new ReplicatedConnection(
                "metadata-group-probe",
                initialMetadata,
                target -> {
                    throw new UnsupportedOperationException("metadata group probe does not route data writes");
                },
                target -> {
                    throw new UnsupportedOperationException("metadata group probe does not route point reads");
                },
                target -> {
                    throw new UnsupportedOperationException("metadata group probe does not route range reads");
                },
                null,
                ignored -> {
                },
                submitter
        );
    }

    private static void runExternalProcessAdapterRehearsal(ProbeOptions options) throws Exception {
        var storageRoot = options.externalProcessStorageRoot();
        deleteRecursively(storageRoot);
        Files.createDirectories(storageRoot);

        var adapterHome = options.adapterHome();
        var manifestPath = adapterHome.resolve("ratis-adapter.properties");
        require(Files.exists(manifestPath),
                "Ratis adapter distribution manifest must exist before the process rehearsal: " + manifestPath);
        var distribution = RatisProcessAdapterDistributionManifest.read(
                manifestPath,
                adapterHome,
                javaExecutable()
        );
        var peers = List.of(
                newPeer("process-node-1", allocatePort()),
                newPeer("process-node-2", allocatePort()),
                newPeer("process-node-3", allocatePort())
        );
        var metadata = rockserverMetadata(peers);
        var partition = metadata.requirePartition(ROCKSERVER_PARTITION);
        var raftGroup = rockserverRaftGroup(peers);
        var plans = new ArrayList<RatisProcessAdapterLaunchPlan>();
        var lifecycles = new ArrayList<RatisServerLifecycle>();

        try {
            for (int i = 0; i < peers.size(); i++) {
                var peer = peers.get(i);
                var nodeId = NodeId.of(peer.getId().toString());
                var nodeRoot = storageRoot.resolve(nodeId.value());
                var plan = RatisProcessAdapterLaunchPlan.fromMetadata(
                        distribution,
                        metadata,
                        nodeId,
                        new RatisProcessAdapterLaunchPlan.Options(
                                nodeRoot.resolve("adapter-work"),
                                nodeRoot.resolve("adapter-data"),
                                nodeRoot.resolve("conf").resolve("ratis-adapter.properties"),
                                new InetSocketAddress("127.0.0.1", allocatePort()),
                                new InetSocketAddress("127.0.0.1", allocatePort()),
                                List.of("-Dlogging.level=ERROR"),
                                Map.of()
                        )
                );
                plans.add(plan);
                var lifecycle = plan.lifecycle(
                        "apache-ratis-process-probe",
                        Duration.ofSeconds(10),
                        Duration.ofSeconds(20)
                );
                lifecycles.add(lifecycle);
                lifecycle.start();
            }

            byte[] firstValue = bytes("process-ratis-value-1");
            try (var node1Client = openProcessPartitionClient(plans.getFirst())) {
                var firstCommit = proposeWithRetry(node1Client, rockserverWrite(10, ReplicatedMutation.put(
                        ROCKSERVER_COLUMN_ID,
                        ROCKSERVER_KEY,
                        Buf.wrap(firstValue).freeze()
                )), "process adapter initial put");
                assertPointRead(queryLinearizableWithRetry(node1Client, rockserverRead(),
                                "process adapter initial read"),
                        firstValue,
                        "process adapter initial read");
                runExternalProcessMetadataDigestMismatchProbe(plans.getFirst(), firstValue);

                try (var directClient = newClient(raftGroup, null)) {
                    assertSuccess(directClient.admin().transferLeadership(
                                    peers.getFirst().getId(),
                                    TimeUnit.SECONDS.toMillis(15)
                            ),
                            "process adapter leader transfer");
                }

                System.out.printf("Ratis process adapter rehearsal leader transfer OK: leader=%s index=%s%n",
                        peers.getFirst().getId(),
                        firstCommit.index());
            }

            lifecycles.getFirst().close();
            var restartedLifecycle = plans.getFirst().lifecycle(
                    "apache-ratis-process-probe",
                    Duration.ofSeconds(10),
                    Duration.ofSeconds(20)
            );
            lifecycles.set(0, restartedLifecycle);
            restartedLifecycle.start();

            byte[] secondValue = bytes("process-ratis-value-after-restart");
            ReplicatedLogCommit secondCommit;
            try (var node2Client = openProcessPartitionClient(plans.get(1))) {
                secondCommit = proposeWithRetry(node2Client, rockserverWrite(11, ReplicatedMutation.put(
                        ROCKSERVER_COLUMN_ID,
                        ROCKSERVER_KEY,
                        Buf.wrap(secondValue).freeze()
                )), "process adapter put after leader restart");
                assertPointRead(queryLinearizableWithRetry(node2Client, rockserverRead(),
                                "process adapter read after leader restart"),
                        secondValue,
                        "process adapter read after leader restart");
            }

            try (var restartedNode1Client = openProcessPartitionClient(plans.getFirst())) {
                assertPointRead(queryStaleWithRetry(restartedNode1Client, rockserverRead(), secondCommit.index(),
                                "process adapter restarted local read"),
                        secondValue,
                        "process adapter restarted local read");
            }

            System.out.printf(
                    "Ratis external-process Rockserver adapter rehearsal OK: group=%s partition=%s restartedPeer=%s commitIndex=%s%n",
                    raftGroup.getGroupId(),
                    partition.partitionId(),
                    peers.getFirst().getId(),
                    secondCommit.index());
        } finally {
            for (int i = lifecycles.size() - 1; i >= 0; i--) {
                lifecycles.get(i).close();
            }
            deleteRecursively(storageRoot);
        }
    }

    private static void runMembershipChangeProbe() throws Exception {
        var storageRoot = Path.of("target", "ratis-spike-membership");
        deleteRecursively(storageRoot);
        Files.createDirectories(storageRoot);

        var peer1 = newPeer("membership-node-1", allocatePort());
        var peer2 = newPeer("membership-node-2", allocatePort());
        var peer3 = newPeer("membership-node-3", allocatePort());
        var peer4 = newPeer("membership-node-4", allocatePort());
        var peer5 = newPeer("membership-node-5", allocatePort());
        var initialPeers = List.of(peer1, peer2, peer3);
        var addedPeers = List.of(peer1, peer2, peer3, peer4);
        var removedPeers = List.of(peer1, peer3, peer4);
        var replacementPeers = List.of(peer1, peer4, peer5);
        var groupId = RaftGroupId.randomId();
        var initialGroup = RaftGroup.valueOf(groupId, initialPeers);
        var servers = new LinkedHashMap<RaftPeerId, RaftServer>();

        try {
            for (var peer : initialPeers) {
                servers.put(peer.getId(), startEchoServer(peer, initialGroup, storageRoot));
            }

            try (var client = newClient(initialGroup, null)) {
                assertReply(sendWithRetry(client, "membership-initial"),
                        "applied:membership-initial",
                        "membership initial write");
                servers.put(peer4.getId(), startEchoServer(peer4, RaftGroup.valueOf(groupId, addedPeers),
                        storageRoot));
                assertSuccess(client.admin().setConfiguration(SetConfigurationRequest.Arguments.newBuilder()
                                .setMode(SetConfigurationRequest.Mode.ADD)
                                .setServersInNewConf(List.of(peer4))
                                .build()),
                        "add peer");
            }

            var addedGroup = RaftGroup.valueOf(groupId, addedPeers);
            try (var client = newClient(addedGroup, null)) {
                assertReply(sendWithRetry(client, "membership-after-add"),
                        "applied:membership-after-add",
                        "membership write after add");
                assertSuccess(client.admin().setConfiguration(removedPeers), "remove peer");
            }
            closeServer(servers.remove(peer2.getId()));

            var removedGroup = RaftGroup.valueOf(groupId, removedPeers);
            try (var client = newClient(removedGroup, null)) {
                assertReply(sendWithRetry(client, "membership-after-remove"),
                        "applied:membership-after-remove",
                        "membership write after remove");
                assertSuccess(client.admin().transferLeadership(peer4.getId(), TimeUnit.SECONDS.toMillis(15)),
                        "transfer leadership");
            }

            try (var client = newClient(removedGroup, peer4.getId())) {
                assertReply(sendWithRetry(client, "membership-after-transfer"),
                        "applied:membership-after-transfer",
                        "membership write after leader transfer");
                servers.put(peer5.getId(), startEchoServer(peer5, RaftGroup.valueOf(groupId, replacementPeers),
                        storageRoot));
                assertSuccess(client.admin().setConfiguration(SetConfigurationRequest.Arguments.newBuilder()
                                .setMode(SetConfigurationRequest.Mode.COMPARE_AND_SET)
                                .setServersInCurrentConf(removedPeers)
                                .setServersInNewConf(replacementPeers)
                                .build()),
                        "replace peer");
            }
            closeServer(servers.remove(peer3.getId()));

            try (var client = newClient(RaftGroup.valueOf(groupId, replacementPeers), peer4.getId())) {
                assertReply(sendWithRetry(client, "membership-after-replacement"),
                        "applied:membership-after-replacement",
                        "membership write after replacement");
            }

            System.out.printf("Ratis membership probe OK: group=%s finalPeers=%s leader=%s%n",
                    groupId,
                    replacementPeers.stream().map(peer -> peer.getId().toString()).toList(),
                    peer4.getId());
        } finally {
            for (var server : servers.values()) {
                closeServer(server);
            }
            deleteRecursively(storageRoot);
        }
    }

    private static void runRocksDbSnapshotProbe() throws Exception {
        RocksDB.loadLibrary();
        var storageRoot = Path.of("target", "ratis-spike-rocksdb-snapshot");
        deleteRecursively(storageRoot);
        Files.createDirectories(storageRoot);

        var peer = newPeer("snapshot-node-1", allocatePort());
        var group = RaftGroup.valueOf(RaftGroupId.randomId(), peer);
        var snapshotIndex = -1L;

        try {
            try (var server = newSnapshotServer(peer, group, storageRoot, RaftStorage.StartupOption.FORMAT)) {
                server.start();
                try (var client = RaftClient.newBuilder()
                        .setRaftGroup(group)
                        .setProperties(clientProperties())
                        .build()) {
                    var writeReply = sendWithRetry(client, "snapshot-value");
                    assertReply(sendReadVariantWithRetry(
                                    () -> client.io().sendReadOnly(Message.valueOf("snapshot-query")),
                                    "snapshot"),
                            "rocksdb:snapshot-value",
                            "snapshot read before restart");

                    var snapshotReply = client.getSnapshotManagementApi().create(true, TimeUnit.SECONDS.toMillis(15));
                    if (!snapshotReply.isSuccess()) {
                        throw new IllegalStateException("Unexpected snapshot response: " + snapshotReply);
                    }
                    if (snapshotReply.getLogIndex() < writeReply.getLogIndex()) {
                        throw new IllegalStateException("Snapshot index " + snapshotReply.getLogIndex()
                                + " is before write index " + writeReply.getLogIndex());
                    }
                    snapshotIndex = snapshotReply.getLogIndex();
                }
            }

            try (var restarted = newSnapshotServer(peer, group, storageRoot, RaftStorage.StartupOption.RECOVER)) {
                restarted.start();
                try (var client = RaftClient.newBuilder()
                        .setRaftGroup(group)
                        .setProperties(clientProperties())
                        .build()) {
                    assertReply(sendReadVariantWithRetry(
                                    () -> client.io().sendReadOnly(Message.valueOf("snapshot-query")),
                                    "snapshot restart"),
                            "rocksdb:snapshot-value",
                            "snapshot read after restart");
                }
            }

            System.out.printf("Ratis RocksDB snapshot probe OK: group=%s snapshotIndex=%s%n",
                    group.getGroupId(),
                    snapshotIndex);
        } finally {
            deleteRecursively(storageRoot);
        }
    }

    private static void runRocksDbSnapshotTransferProbe() throws Exception {
        RocksDB.loadLibrary();
        var storageRoot = Path.of("target", "ratis-spike-rocksdb-snapshot-transfer");
        deleteRecursively(storageRoot);
        Files.createDirectories(storageRoot);

        var peers = List.of(
                newPeer("snapshot-transfer-node-1", allocatePort()),
                newPeer("snapshot-transfer-node-2", allocatePort()),
                newPeer("snapshot-transfer-node-3", allocatePort())
        );
        var laggingPeer = peers.get(2);
        var group = RaftGroup.valueOf(RaftGroupId.randomId(), peers);
        var servers = new LinkedHashMap<RaftPeerId, RaftServer>();
        long snapshotIndex;

        try {
            for (var peer : peers.subList(0, 2)) {
                var server = newSnapshotServer(peer, group, storageRoot, RaftStorage.StartupOption.FORMAT);
                servers.put(peer.getId(), server);
                server.start();
            }
            try (var client = newClient(group, null)) {
                assertReply(sendWithRetry(client, "snapshot-transfer-value"),
                        "rocksdb-applied:snapshot-transfer-value",
                        "snapshot transfer initial write");
                var snapshotReply = client.getSnapshotManagementApi().create(true, TimeUnit.SECONDS.toMillis(15));
                if (!snapshotReply.isSuccess()) {
                    throw new IllegalStateException("Unexpected snapshot-transfer snapshot response: " + snapshotReply);
                }
                snapshotIndex = snapshotReply.getLogIndex();
            }

            var follower = newSnapshotServer(laggingPeer, group, storageRoot, RaftStorage.StartupOption.FORMAT);
            servers.put(laggingPeer.getId(), follower);
            follower.start();
            try (var client = newClient(group, null)) {
                assertReply(sendStaleReadWithRetry(client, "snapshot-transfer-query", snapshotIndex,
                                laggingPeer.getId()),
                        "rocksdb:snapshot-transfer-value",
                        "snapshot transfer stale read");
                var postAddWrite = sendWithRetry(client, "snapshot-transfer-after-add");
                assertReply(postAddWrite,
                        "rocksdb-applied:snapshot-transfer-after-add",
                        "snapshot transfer post-add write");
                assertReply(sendStaleReadWithRetry(client, "snapshot-transfer-post-add-query",
                                postAddWrite.getLogIndex(), laggingPeer.getId()),
                        "rocksdb:snapshot-transfer-after-add",
                        "snapshot transfer post-add stale read");
            }

            System.out.printf("Ratis RocksDB snapshot transfer probe OK: group=%s follower=%s snapshotIndex=%s%n",
                    group.getGroupId(),
                    laggingPeer.getId(),
                    snapshotIndex);
        } finally {
            for (var server : servers.values()) {
                closeServer(server);
            }
            deleteRecursively(storageRoot);
        }
    }

    private static RaftServer newSnapshotServer(RaftPeer peer, RaftGroup group, Path storageRoot,
                                                RaftStorage.StartupOption option) throws IOException {
        var properties = serverProperties(peer, storageRoot.resolve(peer.getId().toString()));
        RaftServerConfigKeys.Snapshot.setAutoTriggerEnabled(properties, false);
        RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);
        return RaftServer.newBuilder()
                .setServerId(peer.getId())
                .setGroup(group)
                .setOption(option)
                .setStateMachine(new RocksDbCheckpointStateMachine())
                .setProperties(properties)
                .build();
    }

    private static RaftServer startEchoServer(RaftPeer peer, RaftGroup group, Path storageRoot) throws IOException {
        var server = RaftServer.newBuilder()
                .setServerId(peer.getId())
                .setGroup(group)
                .setStateMachine(new EchoStateMachine())
                .setProperties(serverProperties(peer, storageRoot.resolve(peer.getId().toString())))
                .build();
        server.start();
        return server;
    }

    private static RaftClient newClient(RaftGroup group, RaftPeerId leaderId) {
        var builder = RaftClient.newBuilder()
                .setRaftGroup(group)
                .setProperties(clientProperties());
        if (leaderId != null) {
            builder.setLeaderId(leaderId);
        }
        return builder.build();
    }

    private static void closeServer(RaftServer server) throws IOException {
        if (server != null) {
            server.close();
        }
    }

    private static RatisPartitionClient openProcessPartitionClient(RatisProcessAdapterLaunchPlan plan) {
        var adapterFactory = plan.adapterFactory("apache-ratis-process-probe", Duration.ofSeconds(20));
        try {
            return new OwnedProcessPartitionClient(
                    adapterFactory.openPartition(plan.adapterConfig().partitions().getFirst()),
                    adapterFactory
            );
        } catch (RuntimeException | Error e) {
            try {
                adapterFactory.close();
            } catch (RuntimeException | Error closeFailure) {
                suppress(e, closeFailure);
            }
            throw e;
        }
    }

    private static void runExternalProcessMetadataDigestMismatchProbe(RatisProcessAdapterLaunchPlan plan,
                                                                      byte[] expectedValue)
            throws Exception {
        var metadataConfig = RatisProcessAdapterMetadataConfig.read(plan.metadataPath());
        var tamperedProperties = metadataConfig.toProperties();
        tamperedProperties.setProperty("column.0.name", "tampered-process-column");
        var tamperedMetadataConfig = RatisProcessAdapterMetadataConfig.fromProperties(tamperedProperties);
        try (var adapterFactory = plan.adapterFactory("apache-ratis-process-probe", Duration.ofSeconds(20))) {
            tamperedMetadataConfig.write(plan.metadataPath());
            try {
                require(rejectsReloadMetadata(adapterFactory),
                        "Process adapter reload must reject metadata whose semantic digest does not match config");
                try (var partitionClient = adapterFactory.openPartition(plan.adapterConfig().partitions().getFirst())) {
                    assertPointRead(queryLinearizableWithRetry(
                                    partitionClient,
                                    rockserverRead(),
                                    "process adapter read after rejected metadata digest mismatch"),
                            expectedValue,
                            "process adapter read after rejected metadata digest mismatch");
                }
            } finally {
                metadataConfig.write(plan.metadataPath());
            }
            adapterFactory.reloadMetadata().join();
        }
        System.out.println("Ratis external-process metadata digest mismatch rejection probe OK");
    }

    private static boolean rejectsReloadMetadata(RatisAdapterFactory adapterFactory) {
        try {
            adapterFactory.reloadMetadata().join();
            return false;
        } catch (CompletionException expected) {
            return true;
        }
    }

    private static void suppress(Throwable failure, Throwable addition) {
        if (failure != addition) {
            failure.addSuppressed(addition);
        }
    }

    private record OwnedProcessPartitionClient(RatisPartitionClient partitionClient,
                                               RatisAdapterFactory adapterFactory) implements RatisPartitionClient {

        private OwnedProcessPartitionClient {
            Objects.requireNonNull(partitionClient, "partitionClient");
            Objects.requireNonNull(adapterFactory, "adapterFactory");
        }

        @Override
        public RatisPartitionDescriptor descriptor() {
            return partitionClient.descriptor();
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
            return partitionClient.propose(payload);
        }

        @Override
        public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
            return partitionClient.waitForCommit(index);
        }

        @Override
        public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
            return partitionClient.queryLinearizable(request);
        }

        @Override
        public CompletableFuture<byte[]> queryStale(byte[] request, long minAppliedIndex) {
            return partitionClient.queryStale(request, minAppliedIndex);
        }

        @Override
        public CompletableFuture<byte[]> queryLocal(byte[] request) {
            return partitionClient.queryLocal(request);
        }

        @Override
        public void close() {
            Throwable failure = null;
            try {
                partitionClient.close();
            } catch (RuntimeException | Error e) {
                failure = e;
            }
            try {
                adapterFactory.close();
            } catch (RuntimeException | Error e) {
                if (failure != null) {
                    suppress(failure, e);
                } else {
                    throw e;
                }
            }
            if (failure instanceof RuntimeException runtimeException) {
                throw runtimeException;
            }
            if (failure instanceof Error error) {
                throw error;
            }
        }
    }

    private static RaftPeer newPeer(String id, int port) {
        return RaftPeer.newBuilder()
                .setId(RaftPeerId.valueOf(id))
                .setAddress("127.0.0.1:" + port)
                .build();
    }

    private static RatisPartitionDescriptor ratisPartitionDescriptor(
            PartitionDescriptor partition,
            RaftGroup group,
            List<RaftPeer> peers,
            Path storageRoot,
            RaftPeerId localPeerId) {
        return new RatisPartitionDescriptor(
                partition.partitionId().toString(),
                group.getGroupId().getUuid().toString(),
                localPeerId.toString(),
                peers.stream()
                        .map(peer -> new RatisPeerDescriptor(
                                peer.getId().toString(),
                                "grpc://" + peer.getAddress(),
                                "ratis-spike"
                        ))
                        .toList(),
                storageRoot.resolve("adapter-" + localPeerId + "-" + partition.partitionId())
        );
    }

    private static RatisPartitionDescriptor metadataRatisDescriptor(RaftGroup group,
                                                                    List<RaftPeer> peers,
                                                                    Path storageRoot,
                                                                    RaftPeerId localPeerId) {
        return new RatisPartitionDescriptor(
                "metadata-group",
                group.getGroupId().getUuid().toString(),
                localPeerId.toString(),
                peers.stream()
                        .map(peer -> new RatisPeerDescriptor(
                                peer.getId().toString(),
                                "grpc://" + peer.getAddress(),
                                "ratis-spike"
                        ))
                        .toList(),
                storageRoot.resolve("adapter-" + localPeerId + "-metadata")
        );
    }

    private static RaftProperties serverProperties(RaftPeer peer, Path storageDir) throws IOException {
        Files.createDirectories(storageDir);
        var properties = baseProperties();
        GrpcConfigKeys.Server.setPort(properties, port(peer));
        RaftServerConfigKeys.setStorageDir(properties, List.of(storageDir.toFile()));
        RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
        RaftServerConfigKeys.setStagingTimeout(properties, TimeDuration.valueOf(20, TimeUnit.SECONDS));
        configureDurabilityServerProperties(properties);
        return properties;
    }

    private static RaftProperties clientProperties() {
        return baseProperties();
    }

    private static RaftProperties baseProperties() {
        var properties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
        return properties;
    }

    private static void configureDurabilityServerProperties(RaftProperties properties) {
        RaftServerConfigKeys.Log.setUseMemory(properties, false);
        RaftServerConfigKeys.Log.setForceSyncNum(properties, 1);
        RaftServerConfigKeys.Log.setUnsafeFlushEnabled(properties, false);
        RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, false);
        RaftServerConfigKeys.Log.setLogMetadataEnabled(properties, true);
        RaftServerConfigKeys.Log.StateMachineData.setSync(properties, true);
    }

    private static void verifyDurabilityServerProperties(RaftProperties properties) {
        require(!RaftServerConfigKeys.Log.useMemory(properties), "Ratis durability probe must use segmented disk log");
        require(RaftServerConfigKeys.Log.forceSyncNum(properties) == 1,
                "Ratis durability probe must force-sync every log entry");
        require(!RaftServerConfigKeys.Log.unsafeFlushEnabled(properties),
                "Ratis unsafe flush must be disabled for replicated durability");
        require(!RaftServerConfigKeys.Log.asyncFlushEnabled(properties),
                "Ratis async flush must be disabled for replicated durability");
        require(RaftServerConfigKeys.Log.logMetadataEnabled(properties),
                "Ratis log metadata must stay enabled for commit-index recovery");
        require(RaftServerConfigKeys.Log.StateMachineData.sync(properties),
                "Ratis state-machine data sync must stay enabled");
    }

    private static int port(RaftPeer peer) {
        var address = peer.getAddress();
        return Integer.parseInt(address.substring(address.lastIndexOf(':') + 1));
    }

    private static org.apache.ratis.protocol.RaftClientReply sendWithRetry(RaftClient client, String command)
            throws Exception {
        var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        Exception lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                var reply = client.io().send(Message.valueOf(command));
                if (reply.isSuccess()) {
                    return reply;
                }
                lastFailure = new IllegalStateException("Ratis write failed: " + reply);
            } catch (IOException e) {
                lastFailure = e;
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Timed out waiting for three-peer Ratis write", lastFailure);
    }

    private static org.apache.ratis.protocol.RaftClientReply sendWithRetry(RaftClient client,
                                                                           byte[] payload,
                                                                           String operation) throws Exception {
        var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        Exception lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                var reply = client.io().send(Message.valueOf(ByteString.copyFrom(payload)));
                if (reply.isSuccess()) {
                    return reply;
                }
                lastFailure = new IllegalStateException("Ratis " + operation + " failed: " + reply);
            } catch (IOException e) {
                lastFailure = e;
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Timed out waiting for Ratis " + operation, lastFailure);
    }

    private static org.apache.ratis.protocol.RaftClientReply sendReadWithRetry(RaftClient client, String query)
            throws Exception {
        var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        Exception lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                var reply = client.io().sendReadOnly(Message.valueOf(query));
                if (reply.isSuccess()) {
                    return reply;
                }
                lastFailure = new IllegalStateException("Ratis read failed: " + reply);
            } catch (IOException e) {
                lastFailure = e;
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Timed out waiting for three-peer Ratis read", lastFailure);
    }

    private static byte[] sendReadWithRetry(RaftClient client, byte[] query, String operation)
            throws Exception {
        var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        Exception lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                var reply = client.io().sendReadOnly(Message.valueOf(ByteString.copyFrom(query)));
                if (reply.isSuccess()) {
                    return reply.getMessage().getContent().toByteArray();
                }
                lastFailure = new IllegalStateException("Ratis " + operation + " failed: " + reply);
            } catch (IOException e) {
                lastFailure = e;
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Timed out waiting for Ratis " + operation, lastFailure);
    }

    private static ReplicatedLogCommit proposeWithRetry(RatisPartitionClient client, byte[] payload, String operation)
            throws Exception {
        var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        Exception lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                var commit = client.propose(payload).get(20, TimeUnit.SECONDS);
                if (commit.committed()) {
                    return commit;
                }
                lastFailure = new IllegalStateException("Ratis process adapter " + operation
                        + " was not committed: " + commit);
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Timed out waiting for Ratis process adapter " + operation, lastFailure);
    }

    private static boolean rejectsProposal(RatisPartitionClient client, byte[] payload) throws Exception {
        try {
            return !client.propose(payload).get(20, TimeUnit.SECONDS).committed();
        } catch (Exception e) {
            return true;
        }
    }

    private static boolean rejectsLinearizableQuery(RatisPartitionClient client, byte[] payload) throws Exception {
        try {
            client.queryLinearizable(payload).get(20, TimeUnit.SECONDS);
            return false;
        } catch (Exception e) {
            return true;
        }
    }

    private static boolean committedProposal(CompletableFuture<ReplicatedLogCommit> proposal) {
        try {
            return proposal.get(20, TimeUnit.SECONDS).committed();
        } catch (Exception e) {
            return false;
        }
    }

    private static MetadataCommand.RegisterNode metadataProbeRegisterNode(long expectedMetadataEpoch, String nodeId)
            throws IOException {
        return new MetadataCommand.RegisterNode(
                expectedMetadataEpoch,
                NodeId.of(nodeId),
                URI.create("http://127.0.0.1:" + allocatePort()),
                URI.create("grpc://127.0.0.1:" + allocatePort()),
                "ratis-spike-concurrent"
        );
    }

    private static byte[] queryLinearizableWithRetry(RatisPartitionClient client, byte[] payload, String operation)
            throws Exception {
        return queryProcessAdapterWithRetry(() -> client.queryLinearizable(payload), operation);
    }

    private static byte[] queryStaleWithRetry(RatisPartitionClient client,
                                              byte[] payload,
                                              long minAppliedIndex,
                                              String operation) throws Exception {
        return queryProcessAdapterWithRetry(() -> client.queryStale(payload, minAppliedIndex), operation);
    }

    private static ClusterMetadata metadataSnapshotWithRetry(RatisPartitionClient client, String operation)
            throws Exception {
        var payload = queryProcessAdapterWithRetry(
                () -> client.queryLinearizable(RatisMetadataCommandSubmitter.snapshotQueryPayload()),
                operation
        );
        return RatisMetadataCommandSubmitter.decodeSnapshot(payload);
    }

    private static byte[] queryProcessAdapterWithRetry(ProcessAdapterQuery query, String operation) throws Exception {
        var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(30);
        Exception lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                return query.send().get(20, TimeUnit.SECONDS);
            } catch (Exception e) {
                lastFailure = e;
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Timed out waiting for Ratis process adapter " + operation, lastFailure);
    }

    private static void verifyReadPaths(RaftClient client, List<RaftPeer> peers, long minAppliedIndex)
            throws Exception {
        assertReply(sendReadWithRetry(client, "linearizable-query"),
                "query:linearizable-query:last=three-peer-command",
                "linearizable read");
        assertReply(sendReadAfterWriteWithRetry(client, "read-after-write-query"),
                "query:read-after-write-query:last=three-peer-command",
                "read-after-write read");
        assertReply(sendNonLinearizableReadWithRetry(client, "nonlinearizable-query"),
                "query:nonlinearizable-query:last=three-peer-command",
                "non-linearizable read");

        var staleTarget = staleReadTarget(client, peers);
        assertReply(sendStaleReadWithRetry(client, "stale-query", minAppliedIndex, staleTarget),
                "query:stale-query:last=three-peer-command",
                "stale read");
        expectStaleReadFailure(client, staleTarget, minAppliedIndex + 1000);

        System.out.printf("Ratis read-path probe OK: leader=%s staleTarget=%s minIndex=%s%n",
                client.getLeaderId(),
                staleTarget,
                minAppliedIndex);
    }

    private static void verifyDurabilityWatchPaths(RaftClient client, long logIndex) throws IOException {
        assertWatch(client, logIndex, ReplicationLevel.MAJORITY, "majority replication");
        assertWatch(client, logIndex, ReplicationLevel.MAJORITY_COMMITTED, "majority commit");
        System.out.printf("Ratis durability watch probe OK: logIndex=%s replication=%s commit=%s%n",
                logIndex,
                ReplicationLevel.MAJORITY,
                ReplicationLevel.MAJORITY_COMMITTED);
    }

    private static void assertWatch(RaftClient client, long logIndex, ReplicationLevel level, String name)
            throws IOException {
        var reply = client.io().watch(logIndex, level);
        if (!reply.isSuccess()) {
            throw new IllegalStateException("Unexpected " + name + " watch response: " + reply);
        }
    }

    private static org.apache.ratis.protocol.RaftClientReply sendReadAfterWriteWithRetry(RaftClient client,
                                                                                         String query) throws Exception {
        return sendReadVariantWithRetry(
                () -> client.io().sendReadAfterWrite(Message.valueOf(query)),
                "read-after-write");
    }

    private static org.apache.ratis.protocol.RaftClientReply sendNonLinearizableReadWithRetry(RaftClient client,
                                                                                              String query) throws Exception {
        return sendReadVariantWithRetry(
                () -> client.io().sendReadOnlyNonLinearizable(Message.valueOf(query)),
                "non-linearizable");
    }

    private static org.apache.ratis.protocol.RaftClientReply sendStaleReadWithRetry(RaftClient client, String query,
                                                                                    long minIndex, RaftPeerId server) throws Exception {
        return sendReadVariantWithRetry(
                () -> client.io().sendStaleRead(Message.valueOf(query), minIndex, server),
                "stale");
    }

    private static org.apache.ratis.protocol.RaftClientReply sendReadVariantWithRetry(ReadOperation operation,
                                                                                      String readKind) throws Exception {
        var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(20);
        Exception lastFailure = null;
        while (System.nanoTime() < deadlineNanos) {
            try {
                var reply = operation.send();
                if (reply.isSuccess()) {
                    return reply;
                }
                lastFailure = new IllegalStateException("Ratis " + readKind + " read failed: " + reply);
            } catch (IOException e) {
                lastFailure = e;
            }
            Thread.sleep(250);
        }
        throw new IllegalStateException("Timed out waiting for Ratis " + readKind + " read", lastFailure);
    }

    private static RaftPeerId staleReadTarget(RaftClient client, List<RaftPeer> peers) {
        var leader = client.getLeaderId();
        return peers.stream()
                .map(RaftPeer::getId)
                .filter(peerId -> !peerId.equals(leader))
                .findFirst()
                .orElseThrow(() -> new IllegalStateException("Expected at least one non-leader peer"));
    }

    private static void expectStaleReadFailure(RaftClient client, RaftPeerId target, long unavailableMinIndex)
            throws IOException {
        try {
            client.io().sendStaleRead(Message.valueOf("stale-too-new-query"), unavailableMinIndex, target);
        } catch (IOException expected) {
            return;
        }
        throw new IllegalStateException("Expected stale read failure for unavailable minIndex " + unavailableMinIndex);
    }

    private static void assertReply(org.apache.ratis.protocol.RaftClientReply reply, String expected, String readKind) {
        var actual = content(reply.getMessage());
        if (!expected.equals(actual)) {
            throw new IllegalStateException("Unexpected " + readKind + " response: " + actual);
        }
    }

    private static void assertSuccess(org.apache.ratis.protocol.RaftClientReply reply, String operation) {
        if (!reply.isSuccess()) {
            throw new IllegalStateException("Unexpected " + operation + " response: " + reply);
        }
    }

    private static void assertBytes(byte[] actual, byte[] expected, String operation) {
        if (!Arrays.equals(expected, actual)) {
            throw new IllegalStateException(operation + " returned "
                    + new String(actual, StandardCharsets.UTF_8)
                    + " instead of "
                    + new String(expected, StandardCharsets.UTF_8));
        }
    }

    private static void assertPointRead(byte[] encodedResponse, byte[] expected, String operation) {
        var responses = PartitionWriteResponseCodec.decodeResponses(encodedResponse);
        if (responses.size() != 1) {
            throw new IllegalStateException(operation + " returned " + responses.size() + " point-read values");
        }
        var response = responses.getFirst();
        if (expected == null) {
            if (response != null) {
                throw new IllegalStateException(operation + " returned a value after delete");
            }
            return;
        }
        if (!(response instanceof Buf value)) {
            throw new IllegalStateException(operation + " returned null instead of "
                    + new String(expected, StandardCharsets.UTF_8));
        }
        assertBytes(value.toByteArray(), expected, operation);
    }

    private static void assertRangeRead(byte[] encodedResponse, List<KV> expected, String operation) {
        var entries = PartitionRangeReadResponseCodec.decodeEntries(encodedResponse);
        if (!expected.equals(entries)) {
            throw new IllegalStateException(operation + " returned " + entries + " instead of " + expected);
        }
    }

    private static String content(Message message) {
        return message.getContent().toString(StandardCharsets.UTF_8);
    }

    private static byte[] rockserverWrite(long requestSequence, ReplicatedMutation mutation) {
        return rockserverWrite(requestSequence, ROCKSERVER_SCHEMA_EPOCH, mutation);
    }

    private static byte[] rockserverWrite(long requestSequence, long schemaEpoch, ReplicatedMutation mutation) {
        var command = new PartitionWriteCommand(
                new ClientRequestId(ROCKSERVER_CLIENT_ID, requestSequence),
                schemaEpoch,
                List.of(mutation.withCalculatedKey(PartitionBoundaryRules.routingKey(ROCKSERVER_SCHEMA,
                        mutation.keys()))),
                PartitionWriteResponseMode.NONE,
                new PartitionWriteCommandSeal(schemaEpoch, ROCKSERVER_PARTITION, schemaEpoch, buf(0x10), buf(0x20))
        );
        return PartitionWriteCommandCodec.encode(command);
    }

    private static byte[] legacyRockserverWrite(long requestSequence, ReplicatedMutation mutation) {
        return PartitionWriteCommandCodec.encode(PartitionWriteCommand.single(
                new ClientRequestId(ROCKSERVER_CLIENT_ID, requestSequence),
                ROCKSERVER_SCHEMA_EPOCH,
                mutation
        ));
    }

    private static byte[] rockserverRead() {
        return rockserverRead(ROCKSERVER_SCHEMA_EPOCH);
    }

    private static byte[] rockserverRead(long schemaEpoch) {
        return rockserverRead(schemaEpoch, ROCKSERVER_KEY);
    }

    private static byte[] rockserverRead(long schemaEpoch, Keys keys) {
        return PartitionPointReadCommandCodec.encode(new PartitionPointReadCommand(
                schemaEpoch,
                ROCKSERVER_COLUMN_ID,
                keys
        ));
    }

    private static byte[] rockserverRangeRead(Keys startKeysInclusive, Keys endKeysExclusive, boolean reverse) {
        return PartitionRangeReadCommandCodec.encode(new PartitionRangeReadCommand(
                ROCKSERVER_SCHEMA_EPOCH,
                ROCKSERVER_COLUMN_ID,
                startKeysInclusive,
                endKeysExclusive,
                reverse,
                128
        ));
    }

    private static ClusterMetadata rockserverMetadata(List<RaftPeer> peers) {
        return rockserverMetadata(peers, ROCKSERVER_RAFT_GROUP_ID, ROCKSERVER_SCHEMA_EPOCH);
    }

    private static ClusterMetadata rockserverMetadata(List<RaftPeer> peers, long schemaEpoch) {
        return rockserverMetadata(peers, ROCKSERVER_RAFT_GROUP_ID, schemaEpoch);
    }

    private static ClusterMetadata rockserverMetadata(List<RaftPeer> peers, String raftGroupId) {
        return rockserverMetadata(peers, raftGroupId, ROCKSERVER_SCHEMA_EPOCH);
    }

    private static ClusterMetadata rockserverMetadata(List<RaftPeer> peers, String raftGroupId, long schemaEpoch) {
        var replicaMembers = peers.stream()
                .map(peer -> new ReplicaMember(
                        NodeId.of(peer.getId().toString()),
                        URI.create("grpc://" + peer.getAddress()),
                        ReplicaRole.VOTER,
                        0,
                        0,
                        true
                ))
                .toList();
        var nodes = new LinkedHashMap<NodeId, ClusterNodeMetadata>();
        for (var peer : peers) {
            var nodeId = NodeId.of(peer.getId().toString());
            nodes.put(nodeId, new ClusterNodeMetadata(
                    nodeId,
                    URI.create("http://" + peer.getAddress()),
                    URI.create("grpc://" + peer.getAddress()),
                    "ratis-spike"
            ));
        }
        var partition = new PartitionDescriptor(
                ROCKSERVER_PARTITION,
                buf(0x10),
                buf(0x20),
                schemaEpoch,
                raftGroupId,
                replicaMembers,
                NodeId.of(peers.getFirst().getId().toString()),
                Map.of("probe", "single-partition"),
                PartitionSplitState.ACTIVE
        );
        var column = new ClusterColumnMetadata(
                ROCKSERVER_COLUMN_ID,
                "accounts",
                ROCKSERVER_SCHEMA,
                schemaEpoch
        );
        var group = new ReplicaGroupMetadata(
                ROCKSERVER_PARTITION,
                partition.epoch(),
                partition.leaderHint(),
                replicaMembers,
                schemaEpoch
        );
        return new ClusterMetadata(
                ROCKSERVER_CLUSTER_ID,
                schemaEpoch,
                Map.of(ROCKSERVER_PARTITION, group),
                Map.of(ROCKSERVER_COLUMN_ID, column),
                Map.of(ROCKSERVER_PARTITION, partition),
                nodes
        );
    }

    private static ClusterMetadata metadataGroupInitialMetadata(List<RaftPeer> peers) {
        var nodes = new LinkedHashMap<NodeId, ClusterNodeMetadata>();
        for (var peer : peers) {
            var nodeId = NodeId.of(peer.getId().toString());
            nodes.put(nodeId, new ClusterNodeMetadata(
                    nodeId,
                    URI.create("http://" + peer.getAddress()),
                    URI.create("grpc://" + peer.getAddress()),
                    "ratis-spike"
            ));
        }
        var replicaMembers = metadataProbeMembers(peers);
        var group = new ReplicaGroupMetadata(
                new ShardId(99),
                0,
                NodeId.of(peers.getFirst().getId().toString()),
                replicaMembers,
                0
        );
        return new ClusterMetadata(
                ROCKSERVER_CLUSTER_ID,
                0,
                Map.of(group.shardId(), group),
                Map.of(),
                Map.of(),
                nodes
        );
    }

    private static ClusterMetadata metadataSnapshotPoison(ClusterMetadata metadata) {
        return new ClusterMetadata(
                metadata.clusterId(),
                999,
                Map.of(),
                Map.of(),
                Map.of(),
                metadata.nodesById()
        );
    }

    private static ClusterMetadata metadataGroupGeneratedLiveMetadata(List<RaftPeer> peers) {
        var initialMetadata = metadataGroupInitialMetadata(peers);
        var column = new ClusterColumnMetadata(
                45,
                "unsafe_seed_column",
                ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true),
                0
        );
        return new ClusterMetadata(
                initialMetadata.clusterId(),
                initialMetadata.epoch() + 1,
                initialMetadata.shards(),
                Map.of(column.columnId(), column),
                initialMetadata.partitionsById(),
                initialMetadata.nodesById()
        );
    }

    private static List<ReplicaMember> metadataProbeMembers(List<RaftPeer> peers) {
        return peers.stream()
                .map(peer -> new ReplicaMember(
                        NodeId.of(peer.getId().toString()),
                        URI.create("grpc://" + peer.getAddress()),
                        ReplicaRole.VOTER,
                        0,
                        0,
                        true
                ))
                .toList();
    }

    private static RaftGroup rockserverRaftGroup(List<RaftPeer> peers) {
        return RaftGroup.valueOf(
                RaftGroupId.valueOf(UUID.nameUUIDFromBytes(ROCKSERVER_RAFT_GROUP_ID.getBytes(StandardCharsets.UTF_8))),
                peers
        );
    }

    private static Path javaExecutable() {
        var java = Path.of(System.getProperty("java.home"), "bin", "java");
        require(Files.exists(java), "Java executable must exist for process adapter rehearsal: " + java);
        return java;
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static Buf buf(int... values) {
        var bytes = new byte[values.length];
        for (int i = 0; i < values.length; i++) {
            bytes[i] = (byte) values[i];
        }
        return Buf.wrap(bytes).freeze();
    }

    private static void require(boolean condition, String message) {
        if (!condition) {
            throw new IllegalStateException(message);
        }
    }

    private static boolean failureChainContains(Throwable failure, String message) {
        if (failure == null) {
            return false;
        }
        if (failure.getMessage() != null && failure.getMessage().contains(message)) {
            return true;
        }
        if (failureChainContains(failure.getCause(), message)) {
            return true;
        }
        for (var suppressed : failure.getSuppressed()) {
            if (failureChainContains(suppressed, message)) {
                return true;
            }
        }
        return false;
    }

    private static boolean rejects(Properties source, String key, String value) {
        var copy = new Properties();
        copy.putAll(source);
        copy.setProperty(key, value);
        try {
            RatisClasspathAdapterConfig.fromProperties(copy);
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    private static boolean rejectsMetadataEpoch(RatisClasspathAdapterConfig config, long metadataEpoch) {
        try {
            config.requireMetadataEpoch(metadataEpoch);
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    private static boolean rejectsMetadataSha256(RatisClasspathAdapterConfig config, String metadataSha256) {
        try {
            config.requireMetadataSha256(metadataSha256);
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    private static boolean rejectsLocalPeerId(RatisClasspathAdapterConfig config, String localPeerId) {
        try {
            config.requireLocalPeerId(localPeerId);
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    private static boolean rejectsDataRoot(RatisClasspathAdapterConfig config, Path dataRoot) {
        try {
            config.requireDataRoot(dataRoot);
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    private static boolean rejectsDataRoot(Properties source, String partitionId, String dataPath, Path dataRoot) {
        var copy = new Properties();
        copy.putAll(source);
        copy.setProperty("partition.0.partitionId", partitionId);
        copy.setProperty("partition.0.dataPath", dataPath);
        try {
            RatisClasspathAdapterConfig.fromProperties(copy).requireDataRoot(dataRoot);
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    private static boolean rejectsMetadataGroupTopologySha256(RatisClasspathAdapterConfig config, String sha256) {
        try {
            config.requireMetadataGroupTopologySha256(sha256);
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    private static boolean rejectsAdditionalPartition(Properties source,
                                                      String partitionId,
                                                      String raftGroupId,
                                                      String dataPath) {
        var copy = new Properties();
        copy.putAll(source);
        copy.setProperty("partition.1.partitionId", partitionId);
        copy.setProperty("partition.1.raftGroupId", raftGroupId);
        copy.setProperty("partition.1.localPeerId", "node-b");
        copy.setProperty("partition.1.dataPath", dataPath);
        copy.setProperty("partition.1.peer.0.peerId", "node-b");
        copy.setProperty("partition.1.peer.0.endpoint", "grpc://127.0.0.1:19002");
        copy.setProperty("partition.1.peer.0.failureDomain", "rack-b");
        try {
            RatisClasspathAdapterConfig.fromProperties(copy);
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    private static boolean rejectsAdditionalPeer(Properties source, String peerId, String endpoint) {
        var copy = new Properties();
        copy.putAll(source);
        copy.setProperty("partition.0.peer.1.peerId", peerId);
        copy.setProperty("partition.0.peer.1.endpoint", endpoint);
        copy.setProperty("partition.0.peer.1.failureDomain", "rack-b");
        try {
            RatisClasspathAdapterConfig.fromProperties(copy);
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    private static boolean rejectsPartitionDescriptorAlias(RockserverRatisClasspathStateMachineFactory factory,
                                                           PartitionDescriptor partition,
                                                           RaftGroup group,
                                                           List<RaftPeer> peers,
                                                           Path storageRoot) {
        var descriptor = ratisPartitionDescriptor(partition, group, peers, storageRoot, peers.getFirst().getId());
        var alias = new RatisPartitionDescriptor(
                "0" + descriptor.partitionId(),
                descriptor.raftGroupId(),
                descriptor.localPeerId(),
                descriptor.voters(),
                descriptor.dataPath()
        );
        return rejectsPartitionDescriptor(factory, alias);
    }

    private static boolean rejectsPartitionDescriptorTopologyMismatch(
            RockserverRatisClasspathStateMachineFactory factory,
            PartitionDescriptor partition,
            RaftGroup group,
            List<RaftPeer> peers,
            Path storageRoot) {
        var descriptor = ratisPartitionDescriptor(partition, group, peers, storageRoot, peers.getFirst().getId());
        var wrongGroup = new RatisPartitionDescriptor(
                descriptor.partitionId(),
                descriptor.raftGroupId() + "-wrong",
                descriptor.localPeerId(),
                descriptor.voters(),
                descriptor.dataPath()
        );
        var firstVoter = descriptor.voters().getFirst();
        var wrongVoters = new ArrayList<>(descriptor.voters());
        wrongVoters.set(0, new RatisPeerDescriptor(
                firstVoter.peerId(),
                firstVoter.endpoint(),
                firstVoter.failureDomain() + "-wrong"
        ));
        var wrongVoter = new RatisPartitionDescriptor(
                descriptor.partitionId(),
                descriptor.raftGroupId(),
                descriptor.localPeerId(),
                wrongVoters,
                descriptor.dataPath()
        );
        return rejectsPartitionDescriptor(factory, wrongGroup) && rejectsPartitionDescriptor(factory, wrongVoter);
    }

    private static boolean rejectsPartitionDescriptor(RockserverRatisClasspathStateMachineFactory factory,
                                                      RatisPartitionDescriptor descriptor) {
        try {
            factory.validateReload(List.of(descriptor));
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        }
    }

    private static boolean rejectsAdapterArguments(String... args) {
        try {
            AdapterMain.main(args);
            return false;
        } catch (IllegalArgumentException expected) {
            return true;
        } catch (IOException e) {
            throw new IllegalStateException("Adapter entrypoint argument probe failed", e);
        }
    }

    private static final ClusterId ROCKSERVER_CLUSTER_ID = new ClusterId(new UUID(0, 1));
    private static final ClientId ROCKSERVER_CLIENT_ID = new ClientId(new UUID(0, 2));
    private static final ShardId ROCKSERVER_PARTITION = new ShardId(7);
    private static final String ROCKSERVER_RAFT_GROUP_ID = "rockserver-ratis-group";
    private static final long ROCKSERVER_COLUMN_ID = 4;
    private static final long ROCKSERVER_SCHEMA_EPOCH = 3;
    private static final ColumnSchema ROCKSERVER_SCHEMA = ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true);
    private static final Keys ROCKSERVER_KEY = new Keys(buf(0x18));
    private static final Keys ROCKSERVER_RANGE_KEY = new Keys(buf(0x19));
    private static final Keys ROCKSERVER_OUTSIDE_KEY = new Keys(buf(0x20));
    private static final Set<Integer> ALLOCATED_PORTS = new HashSet<>();

    private static synchronized int allocatePort() throws IOException {
        while (true) {
            try (var socket = new ServerSocket(0)) {
                socket.setReuseAddress(true);
                var port = socket.getLocalPort();
                if (ALLOCATED_PORTS.add(port)) {
                    return port;
                }
            }
        }
    }

    private static void deleteRecursively(Path root) throws IOException {
        if (!Files.exists(root)) {
            return;
        }
        try (var paths = Files.walk(root)) {
            var sorted = paths.sorted(Comparator.reverseOrder()).toList();
            for (var path : sorted) {
                Files.deleteIfExists(path);
            }
        }
    }

    private static void zipDirectory(Path sourceDir, Path targetZip) throws IOException {
        try (var output = new ZipOutputStream(Files.newOutputStream(targetZip))) {
            try (var paths = Files.walk(sourceDir)) {
                var files = paths.filter(Files::isRegularFile).sorted().toList();
                for (var file : files) {
                    var entryName = sourceDir.relativize(file).toString().replace('\\', '/');
                    output.putNextEntry(new ZipEntry(entryName));
                    Files.copy(file, output);
                    output.closeEntry();
                }
            }
        }
    }

    private static void unzipDirectory(Path zipFile, Path targetDir) throws IOException {
        Files.createDirectories(targetDir);
        try (var input = new ZipInputStream(Files.newInputStream(zipFile))) {
            ZipEntry entry;
            while ((entry = input.getNextEntry()) != null) {
                var target = targetDir.resolve(entry.getName()).normalize();
                if (!target.startsWith(targetDir)) {
                    throw new IOException("Snapshot entry escapes target directory: " + entry.getName());
                }
                if (entry.isDirectory()) {
                    Files.createDirectories(target);
                } else {
                    Files.createDirectories(target.getParent());
                    Files.copy(input, target, StandardCopyOption.REPLACE_EXISTING);
                }
                input.closeEntry();
            }
        }
    }

    private static final class RocksDbCheckpointStateMachine extends BaseStateMachine {

        private static final byte[] SNAPSHOT_KEY = "snapshot-key".getBytes(StandardCharsets.UTF_8);

        private final SimpleStateMachineStorage snapshotStorage = new SimpleStateMachineStorage();
        private Path stateMachineDir;
        private Path dbDir;
        private Options options;
        private RocksDB db;

        @Override
        public synchronized void initialize(RaftServer raftServer, RaftGroupId raftGroupId, RaftStorage storage)
                throws IOException {
            super.initialize(raftServer, raftGroupId, storage);
            snapshotStorage.init(storage);
            stateMachineDir = storage.getStorageDir().getStateMachineDir().toPath();
            dbDir = stateMachineDir.resolve("rocksdb-current");
            openFromLatestSnapshot();
        }

        @Override
        public synchronized CompletableFuture<Message> applyTransaction(TransactionContext trx) {
            var entry = trx.getLogEntry();
            var payload = entry.getStateMachineLogEntry().getLogData().toString(StandardCharsets.UTF_8);
            try {
                db.put(SNAPSHOT_KEY, payload.getBytes(StandardCharsets.UTF_8));
                updateLastAppliedTermIndex(entry.getTerm(), entry.getIndex());
                return CompletableFuture.completedFuture(Message.valueOf("rocksdb-applied:" + payload));
            } catch (RocksDBException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public synchronized CompletableFuture<Message> query(Message request) {
            try {
                var value = db.get(SNAPSHOT_KEY);
                var text = value == null ? "null" : new String(value, StandardCharsets.UTF_8);
                return CompletableFuture.completedFuture(Message.valueOf("rocksdb:" + text));
            } catch (RocksDBException e) {
                return CompletableFuture.failedFuture(e);
            }
        }

        @Override
        public synchronized long takeSnapshot() throws IOException {
            var termIndex = getLastAppliedTermIndex();
            if (termIndex.getIndex() <= 0) {
                return termIndex.getIndex();
            }

            var checkpointDir = stateMachineDir.resolve("checkpoint-tmp-" + termIndex.getTerm() + "-"
                    + termIndex.getIndex());
            var snapshotFile = snapshotStorage.getSnapshotFile(termIndex.getTerm(), termIndex.getIndex()).toPath();
            var tmpSnapshotFile = Path.of(snapshotFile + ".tmp");
            deleteRecursively(checkpointDir);
            Files.deleteIfExists(tmpSnapshotFile);

            try (var checkpoint = Checkpoint.create(db)) {
                checkpoint.createCheckpoint(checkpointDir.toString());
            } catch (RocksDBException e) {
                throw new IOException("Failed to create RocksDB checkpoint", e);
            }

            zipDirectory(checkpointDir, tmpSnapshotFile);
            Files.move(tmpSnapshotFile, snapshotFile, StandardCopyOption.REPLACE_EXISTING,
                    StandardCopyOption.ATOMIC_MOVE);
            MD5Hash md5 = MD5FileUtil.computeAndSaveMd5ForFile(snapshotFile.toFile());
            var snapshotInfo = new SingleFileSnapshotInfo(new FileInfo(snapshotFile, md5), termIndex);
            snapshotStorage.updateLatestSnapshot(snapshotInfo);
            deleteRecursively(checkpointDir);
            return termIndex.getIndex();
        }

        @Override
        public synchronized void pause() {
            closeRocksDb();
        }

        @Override
        public synchronized void reinitialize() throws IOException {
            closeRocksDb();
            openFromLatestSnapshot();
        }

        @Override
        public SimpleStateMachineStorage getStateMachineStorage() {
            return snapshotStorage;
        }

        @Override
        public SingleFileSnapshotInfo getLatestSnapshot() {
            return snapshotStorage.getLatestSnapshot();
        }

        @Override
        public synchronized void close() throws IOException {
            closeRocksDb();
            super.close();
        }

        private void openFromLatestSnapshot() throws IOException {
            var snapshot = snapshotStorage.getLatestSnapshot();
            if (snapshot != null) {
                deleteRecursively(dbDir);
                unzipDirectory(snapshot.getFile().getPath(), dbDir);
                setLastAppliedTermIndex(snapshot.getTermIndex());
            }
            Files.createDirectories(dbDir);
            options = new Options().setCreateIfMissing(true);
            try {
                db = RocksDB.open(options, dbDir.toString());
            } catch (RocksDBException e) {
                throw new IOException("Failed to open RocksDB snapshot probe database", e);
            }
        }

        private void closeRocksDb() {
            if (db != null) {
                db.close();
                db = null;
            }
            if (options != null) {
                options.close();
                options = null;
            }
        }
    }

    private static final class EchoStateMachine extends BaseStateMachine {

        private final AtomicReference<String> lastApplied = new AtomicReference<>("");

        @Override
        public CompletableFuture<Message> applyTransaction(TransactionContext trx) {
            var payload = trx.getStateMachineLogEntry().getLogData().toString(StandardCharsets.UTF_8);
            lastApplied.set(payload);
            return CompletableFuture.completedFuture(Message.valueOf("applied:" + payload));
        }

        @Override
        public CompletableFuture<Message> query(Message request) {
            return CompletableFuture.completedFuture(Message.valueOf(
                    "query:" + request.getContent().toString(StandardCharsets.UTF_8) + ":last=" + lastApplied.get()));
        }
    }

    @FunctionalInterface
    private interface ReadOperation {
        org.apache.ratis.protocol.RaftClientReply send() throws IOException;
    }

    @FunctionalInterface
    private interface ProcessAdapterQuery {
        CompletableFuture<byte[]> send();
    }
}
