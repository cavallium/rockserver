package it.cavallium.rockserver.ratis;

import it.cavallium.rockserver.core.cluster.internal.DurableAtomicFileWriter;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;

import java.io.IOException;
import java.net.URI;
import java.nio.file.Files;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;

import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.util.SizeInBytes;
import org.apache.ratis.util.TimeDuration;

public final class RatisClasspathPartitionServer implements RatisClasspathAdapterRuntime.PartitionServer {

    private final RatisPartitionDescriptor descriptor;
    private final StateMachine stateMachine;
    private RaftServer server;

    public RatisClasspathPartitionServer(RatisPartitionDescriptor descriptor, StateMachine stateMachine) {
        this.descriptor = Objects.requireNonNull(descriptor, "descriptor");
        this.stateMachine = Objects.requireNonNull(stateMachine, "stateMachine");
    }

    public synchronized void start() throws IOException {
        if (server != null) {
            throw new IllegalStateException("Ratis partition server is already started");
        }
        var localPeer = localPeer(descriptor);
        var storageDir = descriptor.dataPath().resolve("ratis");
        DurableAtomicFileWriter.createDirectoriesDurably(storageDir);
        var starting = RaftServer.newBuilder()
                .setServerId(RaftPeerId.valueOf(descriptor.localPeerId()))
                .setGroup(RatisClasspathPartitionClient.raftGroup(descriptor))
                .setOption(startupOption(storageDir))
                .setStateMachine(stateMachine)
                .setProperties(serverProperties(localPeer, storageDir.toFile()))
                .build();
        try {
            starting.start();
            server = starting;
        } catch (IOException | RuntimeException | Error e) {
            try {
                starting.close();
            } catch (IOException | RuntimeException | Error closeFailure) {
                server = starting;
                if (e != closeFailure) {
                    e.addSuppressed(closeFailure);
                }
            }
            throw e;
        }
    }

    public synchronized void reloadStateMachine(RatisClasspathAdapterRuntime.StateMachineFactory stateMachineFactory)
            throws IOException {
        Objects.requireNonNull(stateMachineFactory, "stateMachineFactory");
        stateMachineFactory.reload(stateMachine, descriptor);
    }

    @Override
    public synchronized void close() throws IOException {
        var running = server;
        if (running != null) {
            running.close();
            server = null;
        }
    }

    private static RatisPeerDescriptor localPeer(RatisPartitionDescriptor descriptor) {
        return descriptor.voters().stream()
                .filter(peer -> peer.peerId().equals(descriptor.localPeerId()))
                .findFirst()
                .orElseThrow(() -> new IllegalArgumentException(
                        "localPeerId must be present in partition voters: " + descriptor.localPeerId()
                ));
    }

    private static RaftProperties serverProperties(RatisPeerDescriptor localPeer, java.io.File storageDir) {
        var properties = new RaftProperties();
        RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
        GrpcConfigKeys.Server.setPort(properties, ratisPort(localPeer.endpoint()));
        RaftServerConfigKeys.setStorageDir(properties, List.of(storageDir));
        RaftServerConfigKeys.Read.setOption(properties, RaftServerConfigKeys.Read.Option.LINEARIZABLE);
        RaftServerConfigKeys.setStagingTimeout(properties, TimeDuration.valueOf(20, TimeUnit.SECONDS));
        RaftServerConfigKeys.Log.setPurgeUptoSnapshotIndex(properties, true);
        RaftServerConfigKeys.Log.Appender.setBufferByteLimit(properties, SizeInBytes.valueOf("4MB"));
        configureDurabilityServerProperties(properties);
        return properties;
    }

    private static RaftStorage.StartupOption startupOption(java.nio.file.Path storageDir) throws IOException {
        try (var children = Files.list(storageDir)) {
            return children.findAny().isPresent()
                    ? RaftStorage.StartupOption.RECOVER
                    : RaftStorage.StartupOption.FORMAT;
        }
    }

    private static void configureDurabilityServerProperties(RaftProperties properties) {
        RaftServerConfigKeys.Log.setUseMemory(properties, false);
        RaftServerConfigKeys.Log.setForceSyncNum(properties, 1);
        RaftServerConfigKeys.Log.setUnsafeFlushEnabled(properties, false);
        RaftServerConfigKeys.Log.setAsyncFlushEnabled(properties, false);
        RaftServerConfigKeys.Log.setLogMetadataEnabled(properties, true);
        RaftServerConfigKeys.Log.StateMachineData.setSync(properties, true);
    }

    private static int ratisPort(String endpoint) {
        if (endpoint.contains("://")) {
            var uri = URI.create(endpoint);
            if (uri.getPort() < 0) {
                throw new IllegalArgumentException("Ratis endpoint must include port: " + endpoint);
            }
            return uri.getPort();
        }
        var separator = endpoint.lastIndexOf(':');
        if (separator < 0) {
            throw new IllegalArgumentException("Ratis endpoint must include port: " + endpoint);
        }
        return Integer.parseInt(endpoint.substring(separator + 1));
    }
}
