package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.client.PartitionPointReadSubmitter;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeReadSubmitter;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteSubmitter;
import it.cavallium.rockserver.core.cluster.client.RouteTarget;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class RatisAdapterClientResolver implements PartitionWriteSubmitter.LogClientResolver,
        PartitionPointReadSubmitter.RatisClientResolver,
        PartitionRangeReadSubmitter.RatisClientResolver,
        AutoCloseable {

    private static final Logger LOG = LoggerFactory.getLogger(RatisAdapterClientResolver.class);

    private final RatisAdapterFactory adapterFactory;
    private Map<String, RatisPartitionClient> clientsByPartitionId = Map.of();
    private List<RatisPartitionClient> clientsPendingClose = List.of();
    private boolean closed;
    private boolean factoryClosed;

    public RatisAdapterClientResolver(RatisAdapterFactory adapterFactory,
                                      List<RatisPartitionDescriptor> partitions) {
        this.adapterFactory = requireNonNull(adapterFactory, "adapterFactory");
        try {
            updatePartitions(partitions);
        } catch (RuntimeException | Error e) {
            closeSuppressing(e);
            throw e;
        }
    }

    public static RatisAdapterClientResolver fromLaunchPlan(RatisAdapterFactory adapterFactory,
                                                            RatisProcessAdapterLaunchPlan launchPlan) {
        requireNonNull(launchPlan, "launchPlan");
        return new RatisAdapterClientResolver(adapterFactory, launchPlan.adapterConfig().partitions());
    }

    public synchronized RatisPartitionClient client(String partitionId) {
        requireOpen();
        var client = clientsByPartitionId.get(RatisAdapterBoundary.requireNonBlank(partitionId, "partitionId"));
        if (client == null) {
            throw new IllegalArgumentException("no Ratis adapter client for partition " + partitionId);
        }
        return client;
    }

    @Override
    public synchronized RatisPartitionClient resolve(RouteTarget target) {
        requireNonNull(target, "target");
        requireOpen();
        var client = clientsByPartitionId.get(target.shardId().toString());
        if (client == null) {
            throw new IllegalArgumentException("no Ratis adapter client for partition " + target.shardId());
        }
        return client;
    }

    public synchronized void updatePartitions(List<RatisPartitionDescriptor> partitions) {
        requireOpen();
        var descriptors = descriptorsByPartitionId(partitions);
        var refreshedClients = new LinkedHashMap<String, RatisPartitionClient>();
        var openedClients = new LinkedHashMap<String, RatisPartitionClient>();
        try {
            for (var entry : descriptors.entrySet()) {
                var partitionId = entry.getKey();
                var descriptor = entry.getValue();
                var existingClient = clientsByPartitionId.get(partitionId);
                if (existingClient != null && existingClient.descriptor().equals(descriptor)) {
                    refreshedClients.put(partitionId, existingClient);
                } else {
                    var openedClient = requireOpenedClient(
                            adapterFactory.openPartition(descriptor),
                            partitionId
                    );
                    openedClients.put(partitionId, openedClient);
                    refreshedClients.put(partitionId, openedClient);
                }
            }
        } catch (RuntimeException | Error e) {
            retainForCleanup(openedClients.values());
            var closeFailure = closePendingClients();
            if (closeFailure != null) {
                suppress(e, closeFailure);
            }
            throw e;
        }

        var staleClients = staleClientsFor(refreshedClients);
        clientsByPartitionId = Map.copyOf(refreshedClients);
        closeStaleClients(staleClients);
    }

    @Override
    public synchronized void close() {
        closed = true;
        retainForCleanup(clientsByPartitionId.values());
        clientsByPartitionId = Map.of();
        var failure = closePendingClients();
        if (!factoryClosed) {
            try {
                adapterFactory.close();
                factoryClosed = true;
            } catch (RuntimeException | Error e) {
                failure = merge(failure, e);
            }
        }
        if (failure != null) {
            throwUnchecked(failure);
        }
    }

    private Map<String, RatisPartitionClient> staleClientsFor(Map<String, RatisPartitionClient> refreshedClients) {
        var staleClients = new LinkedHashMap<String, RatisPartitionClient>();
        for (var entry : clientsByPartitionId.entrySet()) {
            if (refreshedClients.get(entry.getKey()) != entry.getValue()) {
                staleClients.put(entry.getKey(), entry.getValue());
            }
        }
        return staleClients;
    }

    private static Map<String, RatisPartitionDescriptor> descriptorsByPartitionId(
            List<RatisPartitionDescriptor> partitions) {
        requireNonNull(partitions, "partitions");
        var descriptors = new LinkedHashMap<String, RatisPartitionDescriptor>();
        for (var partition : partitions) {
            var descriptor = requireNonNull(partition, "partitions entry");
            if (descriptors.putIfAbsent(descriptor.partitionId(), descriptor) != null) {
                throw new IllegalArgumentException("duplicate Ratis adapter partition id: "
                        + descriptor.partitionId());
            }
        }
        return descriptors;
    }

    private static RatisPartitionClient requireOpenedClient(RatisPartitionClient client, String partitionId) {
        if (client == null) {
            throw new IllegalStateException("adapterFactory.openPartition returned null for partition " + partitionId);
        }
        return client;
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private void requireOpen() {
        if (closed) {
            throw new IllegalStateException("Ratis adapter client resolver is closed");
        }
    }

    private void retainForCleanup(Collection<RatisPartitionClient> clients) {
        var retainedClients = new ArrayList<>(clientsPendingClose);
        for (var client : clients) {
            if (!containsIdentity(retainedClients, client)) {
                retainedClients.add(client);
            }
        }
        clientsPendingClose = List.copyOf(retainedClients);
    }

    private Throwable closePendingClients() {
        var retainedClients = new ArrayList<RatisPartitionClient>();
        Throwable failure = null;
        for (var client : clientsPendingClose) {
            try {
                client.close();
            } catch (RuntimeException | Error e) {
                retainedClients.add(client);
                failure = merge(failure, e);
            }
        }
        clientsPendingClose = List.copyOf(retainedClients);
        return failure;
    }

    private void closeStaleClients(Map<String, RatisPartitionClient> clients) {
        retainForCleanup(clients.values());
        var failure = closePendingClients();
        if (failure != null) {
            LOG.warn("Failed to close stale Ratis adapter clients after publishing refreshed routes", failure);
        }
    }

    private void closeSuppressing(Throwable failure) {
        try {
            close();
        } catch (RuntimeException | Error e) {
            suppress(failure, e);
        }
    }

    private static boolean containsIdentity(List<RatisPartitionClient> clients, RatisPartitionClient expected) {
        for (var client : clients) {
            if (client == expected) {
                return true;
            }
        }
        return false;
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

    private static void throwUnchecked(Throwable failure) {
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IllegalStateException("Unexpected checked adapter cleanup failure", failure);
    }
}
