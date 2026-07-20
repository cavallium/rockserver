package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandSealer;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.LongSupplier;

public final class PartitionWriteAdmissionController {

    private final PartitionRouteCache routeCache;
    private final PartitionWriteRouter writeRouter;
    private final int capacityPerPartition;
    private final int maxBatchSize;
    private final LongSupplier currentTimeMillis;
    private final Map<ShardId, PartitionWriteAdmissionQueue> queues = new HashMap<>();

    public PartitionWriteAdmissionController(PartitionRouteCache routeCache,
                                             int capacityPerPartition,
                                             int maxBatchSize,
                                             LongSupplier currentTimeMillis) {
        this.routeCache = requireArgument(routeCache, "routeCache");
        this.writeRouter = new PartitionWriteRouter(this.routeCache);
        if (capacityPerPartition <= 0) {
            throw new IllegalArgumentException("capacityPerPartition must be > 0");
        }
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be > 0");
        }
        this.capacityPerPartition = capacityPerPartition;
        this.maxBatchSize = maxBatchSize;
        this.currentTimeMillis = requireArgument(currentTimeMillis, "currentTimeMillis");
    }

    public PartitionWriteAdmissionQueue.AdmittedPartitionWrite admit(PartitionWriteCommand command,
                                                                     long expectedMetadataEpoch,
                                                                     long deadlineEpochMillis) {
        requireArgument(command, "command");
        if (deadlineEpochMillis < 0) {
            throw new IllegalArgumentException("deadlineEpochMillis must be >= 0");
        }
        requireBeforeDeadline(deadlineEpochMillis);
        requireCurrentEpoch(expectedMetadataEpoch);

        var sealedCommand = sealForAdmission(command);
        var target = writeRouter.routeWriteAtEpoch(expectedMetadataEpoch, sealedCommand);
        requireBeforeDeadline(deadlineEpochMillis);
        return queueFor(target.shardId()).admit(sealedCommand, deadlineEpochMillis);
    }

    public synchronized List<PartitionWriteAdmissionQueue.AdmittedPartitionWrite> drainBatch(ShardId partitionId) {
        requireArgument(partitionId, "partitionId");
        var queue = queues.get(partitionId);
        if (queue == null) {
            return List.of();
        }
        var batch = queue.drainBatch();
        if (queue.queued() == 0) {
            queues.remove(partitionId);
        }
        return batch;
    }

    public synchronized int queued(ShardId partitionId) {
        requireArgument(partitionId, "partitionId");
        var queue = queues.get(partitionId);
        if (queue == null) {
            return 0;
        }
        var queued = queue.queued();
        if (queued == 0) {
            queues.remove(partitionId);
        }
        return queued;
    }

    public synchronized Set<ShardId> queuedPartitions() {
        queues.entrySet().removeIf(entry -> entry.getValue().queued() == 0);
        return Set.copyOf(queues.keySet());
    }

    private void requireCurrentEpoch(long expectedMetadataEpoch) {
        var currentEpoch = routeCache.metadataEpoch();
        if (expectedMetadataEpoch != currentEpoch) {
            var direction = expectedMetadataEpoch > currentEpoch ? "future" : "stale";
            throw new PartitionWriteRouteException(
                    PartitionWriteFailureKind.STALE_ROUTE,
                    direction + " route metadata epoch " + expectedMetadataEpoch + "; current epoch is "
                            + currentEpoch
            );
        }
    }

    private PartitionWriteCommand sealForAdmission(PartitionWriteCommand command) {
        try {
            return PartitionWriteCommandSealer.seal(routeCache.metadata(), command);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw new PartitionWriteRouteException(
                    PartitionWriteFailureKind.INVALID_REQUEST,
                    "invalid replicated partition write: " + e.getMessage(),
                    e
            );
        }
    }

    private synchronized PartitionWriteAdmissionQueue queueFor(ShardId partitionId) {
        return queues.computeIfAbsent(partitionId, id -> new PartitionWriteAdmissionQueue(
                id,
                capacityPerPartition,
                maxBatchSize,
                currentTimeMillis
        ));
    }

    private void requireBeforeDeadline(long deadlineEpochMillis) {
        if (deadlineExpired(deadlineEpochMillis)) {
            throw new PartitionWriteRouteException(
                    PartitionWriteFailureKind.DEADLINE_EXPIRED,
                    "partition write deadline expired before admission"
            );
        }
    }

    private boolean deadlineExpired(long deadlineEpochMillis) {
        return deadlineEpochMillis > 0 && currentTimeMillis.getAsLong() >= deadlineEpochMillis;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
