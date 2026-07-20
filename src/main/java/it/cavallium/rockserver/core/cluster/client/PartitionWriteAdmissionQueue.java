package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandCodec;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Queue;
import java.util.function.LongSupplier;

public final class PartitionWriteAdmissionQueue {

    private final ShardId partitionId;
    private final int capacity;
    private final int maxBatchSize;
    private final LongSupplier currentTimeMillis;
    private final Queue<AdmittedPartitionWrite> queue = new ArrayDeque<>();

    public PartitionWriteAdmissionQueue(ShardId partitionId,
                                        int capacity,
                                        int maxBatchSize,
                                        LongSupplier currentTimeMillis) {
        this.partitionId = requireArgument(partitionId, "partitionId");
        if (capacity <= 0) {
            throw new IllegalArgumentException("capacity must be > 0");
        }
        if (maxBatchSize <= 0) {
            throw new IllegalArgumentException("maxBatchSize must be > 0");
        }
        this.capacity = capacity;
        this.maxBatchSize = maxBatchSize;
        this.currentTimeMillis = requireArgument(currentTimeMillis, "currentTimeMillis");
    }

    public ShardId partitionId() {
        return partitionId;
    }

    public synchronized AdmittedPartitionWrite admit(PartitionWriteCommand command,
                                                     long deadlineEpochMillis) {
        requireSealedCommand(command);
        if (deadlineEpochMillis < 0) {
            throw new IllegalArgumentException("deadlineEpochMillis must be >= 0");
        }
        if (deadlineExpired(deadlineEpochMillis)) {
            throw new PartitionWriteRouteException(
                    PartitionWriteFailureKind.DEADLINE_EXPIRED,
                    "partition write deadline expired before admission"
            );
        }
        pruneExpired();
        if (queue.size() >= capacity) {
            throw new PartitionWriteRouteException(
                    PartitionWriteFailureKind.BACKPRESSURE,
                    "partition " + partitionId + " admission queue is full"
            );
        }
        var admitted = new AdmittedPartitionWrite(partitionId, command, deadlineEpochMillis);
        queue.add(admitted);
        return admitted;
    }

    public synchronized List<AdmittedPartitionWrite> drainBatch() {
        pruneExpired();
        var batchSize = Math.min(maxBatchSize, queue.size());
        var batch = new ArrayList<AdmittedPartitionWrite>(batchSize);
        for (int i = 0; i < batchSize; i++) {
            batch.add(queue.remove());
        }
        return List.copyOf(batch);
    }

    public synchronized int queued() {
        pruneExpired();
        return queue.size();
    }

    private void pruneExpired() {
        queue.removeIf(admitted -> deadlineExpired(admitted.deadlineEpochMillis()));
    }

    private boolean deadlineExpired(long deadlineEpochMillis) {
        return deadlineEpochMillis > 0 && currentTimeMillis.getAsLong() >= deadlineEpochMillis;
    }

    public static final class AdmittedPartitionWrite {

        private final ShardId partitionId;
        private final PartitionWriteCommand command;
        private final byte[] payload;
        private final long deadlineEpochMillis;

        public AdmittedPartitionWrite(ShardId partitionId,
                                      PartitionWriteCommand command,
                                      long deadlineEpochMillis) {
            this.partitionId = requireArgument(partitionId, "partitionId");
            this.command = requireSealedCommand(command);
            if (deadlineEpochMillis < 0) {
                throw new IllegalArgumentException("deadlineEpochMillis must be >= 0");
            }
            this.deadlineEpochMillis = deadlineEpochMillis;
            this.payload = PartitionWriteCommandCodec.encode(command);
        }

        public ShardId partitionId() {
            return partitionId;
        }

        public PartitionWriteCommand command() {
            return command;
        }

        public byte[] payload() {
            return Arrays.copyOf(payload, payload.length);
        }

        public long deadlineEpochMillis() {
            return deadlineEpochMillis;
        }
    }

    private static PartitionWriteCommand requireSealedCommand(PartitionWriteCommand command) {
        requireArgument(command, "command");
        if (command.seal() == null) {
            throw new IllegalArgumentException("partition write admission queue requires a sealed command");
        }
        return command;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
