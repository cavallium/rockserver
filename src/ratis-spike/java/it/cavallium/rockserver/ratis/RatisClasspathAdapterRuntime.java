package it.cavallium.rockserver.ratis;

import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterHealth;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcHandler;
import it.cavallium.rockserver.core.replication.ratis.RatisFeatureUnavailableException;

import java.io.IOException;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.function.Function;

import org.apache.ratis.statemachine.StateMachine;

public final class RatisClasspathAdapterRuntime implements AutoCloseable {

    private LinkedHashMap<String, RatisPartitionDescriptor> partitions;
    private StateMachineFactory stateMachineFactory;
    private final PartitionServerFactory partitionServerFactory;
    private final LinkedHashMap<String, PartitionServer> servers = new LinkedHashMap<>();
    private final LinkedHashMap<String, PartitionClientSlot> clients = new LinkedHashMap<>();
    private final ArrayList<PartitionClientSlot> pendingClientCleanup = new ArrayList<>();
    private final ArrayList<PartitionServer> pendingServerCleanup = new ArrayList<>();
    private boolean started;

    public RatisClasspathAdapterRuntime(List<RatisPartitionDescriptor> partitions,
                                        StateMachineFactory stateMachineFactory) {
        this(partitions, stateMachineFactory, RatisClasspathPartitionServer::new);
    }

    RatisClasspathAdapterRuntime(List<RatisPartitionDescriptor> partitions,
                                 StateMachineFactory stateMachineFactory,
                                 PartitionServerFactory partitionServerFactory) {
        this.partitions = partitionMap(partitions);
        this.stateMachineFactory = Objects.requireNonNull(stateMachineFactory, "stateMachineFactory");
        this.partitionServerFactory = Objects.requireNonNull(partitionServerFactory, "partitionServerFactory");
    }

    public synchronized void start() throws IOException {
        if (started) {
            throw new IllegalStateException("Ratis classpath adapter runtime is already started");
        }
        throwFailure(closePendingCleanup(null));
        try {
            for (var partition : partitions.values()) {
                var server = createServer(partition, stateMachineFactory);
                servers.put(partition.partitionId(), server);
                server.start();
            }
            for (var partition : partitions.values()) {
                clients.put(partition.partitionId(), new PartitionClientSlot(partition));
            }
            started = true;
        } catch (IOException | RuntimeException | Error e) {
            closeAfterFailedStart(e);
            throw e;
        }
    }

    public synchronized boolean isStarted() {
        return started;
    }

    public synchronized RatisProcessAdapterHealth health() {
        return started
                ? RatisProcessAdapterHealth.ready()
                : RatisProcessAdapterHealth.degraded("Ratis classpath adapter runtime is not started");
    }

    public synchronized void reload(List<RatisPartitionDescriptor> reloadedPartitions,
                                    StateMachineFactory reloadedStateMachineFactory) throws IOException {
        var nextPartitions = partitionMap(reloadedPartitions);
        Objects.requireNonNull(reloadedStateMachineFactory, "reloadedStateMachineFactory")
                .validateReload(List.copyOf(nextPartitions.values()));
        if (!started) {
            partitions = nextPartitions;
            stateMachineFactory = reloadedStateMachineFactory;
            return;
        }
        var changedPartitions = changedPartitions(nextPartitions);
        if (!changedPartitions.isEmpty()) {
            throw new RatisFeatureUnavailableException("running Ratis partition descriptor changes require "
                    + "the partition membership workflow before adapter runtime reload: " + changedPartitions);
        }

        var addedServers = new LinkedHashMap<String, PartitionServer>();
        var addedClients = new LinkedHashMap<String, PartitionClientSlot>();
        var existingReloadStarted = false;
        try {
            for (var partition : nextPartitions.values()) {
                if (!partitions.containsKey(partition.partitionId())) {
                    var server = createServer(partition, reloadedStateMachineFactory);
                    addedServers.put(partition.partitionId(), server);
                    server.start();
                    addedClients.put(partition.partitionId(), new PartitionClientSlot(partition));
                }
            }
            for (var partitionId : nextPartitions.keySet()) {
                if (partitions.containsKey(partitionId)) {
                    existingReloadStarted = true;
                    servers.get(partitionId).reloadStateMachine(reloadedStateMachineFactory);
                }
            }
        } catch (IOException | RuntimeException | Error e) {
            var rollbackCleanupFailed = closeAddedAfterFailedReload(addedClients, addedServers, e);
            if (existingReloadStarted || rollbackCleanupFailed) {
                closeAfterFailure(e);
            }
            throw e;
        }

        var closeFailure = closeRemovedPartitions(nextPartitions);
        if (closeFailure != null) {
            closeAddedAfterFailedReload(addedClients, addedServers, closeFailure);
            closeAfterFailure(closeFailure);
            throwFailure(closeFailure);
        }
        clients.putAll(addedClients);
        servers.putAll(addedServers);
        partitions = nextPartitions;
        stateMachineFactory = reloadedStateMachineFactory;
    }

    public synchronized RatisClasspathPartitionClient client(String partitionId) {
        return clientSlot(partitionId).underlyingClient();
    }

    public synchronized RatisPartitionClient borrowedClient(String partitionId) {
        return clientSlot(partitionId).borrowedClient();
    }

    private PartitionClientSlot clientSlot(String partitionId) {
        var client = clients.get(partitionId);
        if (client == null) {
            throw new IllegalArgumentException("Unknown Ratis adapter runtime partition: " + partitionId);
        }
        return client;
    }

    public synchronized Map<String, RatisClasspathPartitionClient> clients() {
        var snapshot = new LinkedHashMap<String, RatisClasspathPartitionClient>();
        clients.forEach((partitionId, client) -> snapshot.put(partitionId, client.underlyingClient()));
        return Map.copyOf(snapshot);
    }

    public RatisProcessAdapterRpcHandler rpcHandler() {
        return new RatisProcessAdapterRpcHandler(this::borrowedClient);
    }

    @Override
    public synchronized void close() throws IOException {
        started = false;
        Throwable failure = closePendingCleanup(null);
        for (var client : clients.values()) {
            failure = closeClient(client, failure);
        }
        clients.clear();

        var serverList = new ArrayList<>(servers.values());
        for (int i = serverList.size() - 1; i >= 0; i--) {
            failure = closeServer(serverList.get(i), failure);
        }
        servers.clear();
        throwFailure(failure);
    }

    private PartitionServer createServer(RatisPartitionDescriptor partition,
                                         StateMachineFactory stateMachineFactory)
            throws IOException {
        return partitionServerFactory.create(
                partition,
                stateMachineFactory.create(partition)
        );
    }

    private List<String> changedPartitions(Map<String, RatisPartitionDescriptor> nextPartitions) {
        var changed = new ArrayList<String>();
        for (var entry : nextPartitions.entrySet()) {
            var current = partitions.get(entry.getKey());
            if (current != null && !current.equals(entry.getValue())) {
                changed.add(entry.getKey());
            }
        }
        return List.copyOf(changed);
    }

    private Throwable closeRemovedPartitions(Map<String, RatisPartitionDescriptor> nextPartitions) {
        Throwable failure = closePendingCleanup(null);
        for (var partitionId : new ArrayList<>(partitions.keySet())) {
            if (nextPartitions.containsKey(partitionId)) {
                continue;
            }
            var client = clients.remove(partitionId);
            if (client != null) {
                failure = closeClient(client, failure);
            }
            var server = servers.remove(partitionId);
            if (server != null) {
                failure = closeServer(server, failure);
            }
        }
        return failure;
    }

    private boolean closeAddedAfterFailedReload(Map<String, PartitionClientSlot> addedClients,
                                                Map<String, PartitionServer> addedServers,
                                                Throwable reloadFailure) {
        var cleanupFailed = false;
        for (var client : addedClients.values()) {
            closeClient(client, reloadFailure);
            cleanupFailed |= pendingClientCleanup.contains(client);
        }
        var serverList = new ArrayList<>(addedServers.values());
        for (int i = serverList.size() - 1; i >= 0; i--) {
            var server = serverList.get(i);
            closeServer(server, reloadFailure);
            cleanupFailed |= pendingServerCleanup.contains(server);
        }
        return cleanupFailed;
    }

    private Throwable closePendingCleanup(Throwable failure) {
        for (var client : new ArrayList<>(pendingClientCleanup)) {
            failure = closeClient(client, failure);
        }
        var serverList = new ArrayList<>(pendingServerCleanup);
        for (int i = serverList.size() - 1; i >= 0; i--) {
            failure = closeServer(serverList.get(i), failure);
        }
        return failure;
    }

    private Throwable closeClient(PartitionClientSlot client, Throwable failure) {
        try {
            client.retireAndClose();
            pendingClientCleanup.remove(client);
        } catch (RuntimeException | Error e) {
            if (!pendingClientCleanup.contains(client)) {
                pendingClientCleanup.add(client);
            }
            failure = merge(failure, e);
        }
        return failure;
    }

    private Throwable closeServer(PartitionServer server, Throwable failure) {
        try {
            server.close();
            pendingServerCleanup.remove(server);
        } catch (IOException | RuntimeException | Error e) {
            if (!pendingServerCleanup.contains(server)) {
                pendingServerCleanup.add(server);
            }
            failure = merge(failure, e);
        }
        return failure;
    }

    private static LinkedHashMap<String, RatisPartitionDescriptor> partitionMap(
            List<RatisPartitionDescriptor> partitions) {
        Objects.requireNonNull(partitions, "partitions");
        var partitionMap = new LinkedHashMap<String, RatisPartitionDescriptor>();
        for (var partition : partitions) {
            if (partitionMap.put(partition.partitionId(), partition) != null) {
                throw new IllegalArgumentException("Duplicate adapter runtime partition id: "
                        + partition.partitionId());
            }
        }
        return partitionMap;
    }

    private void closeAfterFailedStart(Throwable startupFailure) {
        closeAfterFailure(startupFailure);
    }

    private void closeAfterFailure(Throwable failure) {
        try {
            close();
        } catch (IOException | RuntimeException | Error e) {
            suppress(failure, e);
        }
    }

    private static Throwable merge(Throwable failure, Throwable addition) {
        if (failure == null) {
            return addition;
        }
        suppress(failure, addition);
        return failure;
    }

    private static void suppress(Throwable failure, Throwable addition) {
        if (failure != addition) {
            failure.addSuppressed(addition);
        }
    }

    private static void throwFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException ioException) {
            throw ioException;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        if (failure != null) {
            throw new IOException("Unexpected Ratis classpath adapter runtime cleanup failure", failure);
        }
    }

    private static final class PartitionClientSlot {

        private final String partitionId;
        private final RatisClasspathPartitionClient client;
        private final RatisPartitionClient borrowedClient = new BorrowedPartitionClient();
        private int activeOperations;
        private boolean retired;
        private boolean closed;

        private PartitionClientSlot(RatisPartitionDescriptor descriptor) {
            this.partitionId = descriptor.partitionId();
            this.client = new RatisClasspathPartitionClient(descriptor);
        }

        private RatisPartitionClient borrowedClient() {
            return borrowedClient;
        }

        private RatisClasspathPartitionClient underlyingClient() {
            return client;
        }

        private void retireAndClose() {
            var interrupted = false;
            synchronized (this) {
                retired = true;
                while (activeOperations > 0) {
                    try {
                        wait();
                    } catch (InterruptedException e) {
                        interrupted = true;
                    }
                }
                if (closed) {
                    if (interrupted) {
                        Thread.currentThread().interrupt();
                    }
                    return;
                }
            }
            try {
                client.close();
                synchronized (this) {
                    closed = true;
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }

        private synchronized boolean acquire() {
            if (retired) {
                return false;
            }
            activeOperations++;
            return true;
        }

        private synchronized void release() {
            activeOperations--;
            if (activeOperations == 0) {
                notifyAll();
            }
        }

        private <T> CompletableFuture<T> invoke(
                Function<RatisClasspathPartitionClient, CompletableFuture<T>> operation) {
            if (!acquire()) {
                return CompletableFuture.failedFuture(new IllegalStateException(
                        "Ratis adapter runtime partition is retired: " + partitionId
                ));
            }
            try {
                return Objects.requireNonNull(operation.apply(client), "partition client operation future")
                        .whenComplete((_, _) -> release());
            } catch (RuntimeException | Error e) {
                release();
                throw e;
            }
        }

        private final class BorrowedPartitionClient implements RatisPartitionClient {

            @Override
            public RatisPartitionDescriptor descriptor() {
                return client.descriptor();
            }

            @Override
            public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
                return invoke(client -> client.propose(payload));
            }

            @Override
            public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
                return invoke(client -> client.waitForCommit(index));
            }

            @Override
            public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
                return invoke(client -> client.queryLinearizable(request));
            }

            @Override
            public CompletableFuture<byte[]> queryStale(byte[] request, long minAppliedIndex) {
                return invoke(client -> client.queryStale(request, minAppliedIndex));
            }

            @Override
            public CompletableFuture<byte[]> queryLocal(byte[] request) {
                return invoke(client -> client.queryLocal(request));
            }

            @Override
            public void close() {
                // The runtime owns the underlying client lifecycle.
            }
        }
    }

    interface PartitionServer extends AutoCloseable {

        void start() throws IOException;

        void reloadStateMachine(StateMachineFactory stateMachineFactory) throws IOException;

        @Override
        void close() throws IOException;
    }

    @FunctionalInterface
    interface PartitionServerFactory {

        PartitionServer create(RatisPartitionDescriptor descriptor, StateMachine stateMachine);
    }

    @FunctionalInterface
    public interface StateMachineFactory {

        StateMachine create(RatisPartitionDescriptor descriptor) throws IOException;

        default void validateReload(List<RatisPartitionDescriptor> descriptors) throws IOException {
            Objects.requireNonNull(descriptors, "descriptors");
        }

        default void reload(StateMachine stateMachine, RatisPartitionDescriptor descriptor) throws IOException {
            Objects.requireNonNull(stateMachine, "stateMachine");
            Objects.requireNonNull(descriptor, "descriptor");
        }
    }
}
