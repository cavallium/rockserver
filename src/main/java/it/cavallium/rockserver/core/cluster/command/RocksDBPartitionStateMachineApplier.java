package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.ReplicationFailureInjector;
import it.cavallium.rockserver.core.cluster.ReplicationFailurePhase;
import it.cavallium.rockserver.core.cluster.RockserverStorageKeyspace;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.RocksDBException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.jetbrains.annotations.Nullable;
import org.rocksdb.RocksDB;

public final class RocksDBPartitionStateMachineApplier implements PartitionWriteApplier {

    private final ClusterMetadata metadata;
    private final RocksDBPartitionStateWriteBatchWriter writer;
    private final RocksDB db;
    private final PartitionMergeResolver mergeResolver;
    private final ReplicationFailureInjector failureInjector;
    private final PartitionDescriptor cdcPartition;

    public RocksDBPartitionStateMachineApplier(ClusterMetadata metadata,
                                               RocksDBPartitionStateWriteBatchWriter writer) {
        this(metadata, writer, ReplicationFailureInjector.none());
    }

    public RocksDBPartitionStateMachineApplier(ClusterMetadata metadata,
                                               RocksDBPartitionStateWriteBatchWriter writer,
                                               ReplicationFailureInjector failureInjector) {
        this(metadata, writer, null, null, failureInjector, null);
    }

    public RocksDBPartitionStateMachineApplier(ClusterMetadata metadata, RocksDB db) {
        this(metadata, db, null, ReplicationFailureInjector.none());
    }

    public RocksDBPartitionStateMachineApplier(ClusterMetadata metadata,
                                               RocksDB db,
                                               ReplicationFailureInjector failureInjector) {
        this(metadata, db, null, failureInjector);
    }

    public RocksDBPartitionStateMachineApplier(ClusterMetadata metadata,
                                               RocksDB db,
                                               @Nullable PartitionMergeResolver mergeResolver,
                                               ReplicationFailureInjector failureInjector) {
        this(metadata, new RocksDBPartitionStateWriteBatchWriter(db, failureInjector), db, mergeResolver,
                failureInjector, null);
    }

    public RocksDBPartitionStateMachineApplier(ClusterMetadata metadata,
                                               PartitionDescriptor cdcPartition,
                                               RocksDB db) {
        this(metadata, requireArgument(cdcPartition, "cdcPartition"), db, null, ReplicationFailureInjector.none());
    }

    public RocksDBPartitionStateMachineApplier(ClusterMetadata metadata,
                                               PartitionDescriptor cdcPartition,
                                               RocksDB db,
                                               @Nullable PartitionMergeResolver mergeResolver,
                                               ReplicationFailureInjector failureInjector) {
        this(metadata, new RocksDBPartitionStateWriteBatchWriter(db, failureInjector), db, mergeResolver,
                failureInjector, requireArgument(cdcPartition, "cdcPartition"));
    }

    private RocksDBPartitionStateMachineApplier(ClusterMetadata metadata,
                                                RocksDBPartitionStateWriteBatchWriter writer,
                                                RocksDB db,
                                                @Nullable PartitionMergeResolver mergeResolver,
                                                ReplicationFailureInjector failureInjector,
                                                @Nullable PartitionDescriptor cdcPartition) {
        this.metadata = requireArgument(metadata, "metadata");
        this.writer = requireArgument(writer, "writer");
        this.db = db;
        this.mergeResolver = mergeResolver;
        this.failureInjector = requireArgument(failureInjector, "failureInjector");
        this.cdcPartition = cdcPartition;
    }

    @Override
    public List<Object> apply(long logIndex, PartitionWriteCommand command) {
        var requiredCommand = requireArgument(command, "command");
        var duplicateResponses = readDuplicateResponsesOrThrow(logIndex, requiredCommand);
        if (duplicateResponses != null) {
            return duplicateResponses;
        }
        var preparedApply = prepareApply(requiredCommand);
        validateResponseCount(requiredCommand, preparedApply.responses());
        var encodedResponses = PartitionWriteResponseCodec.encodeResponses(preparedApply.responses());
        if (requiredCommand.responseMode().producesResponse()) {
            failureInjector.inject(ReplicationFailurePhase.RESPONSE_PERSISTENCE);
        }
        var batch = new PartitionStateWriteBatch(
                preparedApply.userWrites(),
                RockserverStorageKeyspace.dedupeResponse(requiredCommand.requestId()),
                encodedResponses,
                RockserverStorageKeyspace.dedupeClientState(requiredCommand.requestId().clientId()),
                ClientDedupeStateCodec.encode(ClientDedupeState.acknowledged(requiredCommand.requestId())),
                RockserverStorageKeyspace.lastAppliedIndex(),
                PartitionStateMetadataCodec.encodeLastAppliedIndex(logIndex),
                logIndex,
                replicatedCdcEvents(logIndex, requiredCommand)
        );
        writer.write(batch);
        return preparedApply.responses();
    }

    @Nullable
    private List<Object> readDuplicateResponsesOrThrow(long logIndex, PartitionWriteCommand command) {
        if (db == null) {
            return null;
        }
        try {
            var requestId = command.requestId();
            var dedupeResponseKey = RockserverStorageKeyspace.dedupeResponse(requestId);
            var dedupeResponseValue = db.get(dedupeResponseKey.toByteArray());
            if (dedupeResponseValue != null) {
                var duplicateResponses = PartitionWriteResponseCodec.decodeResponses(
                        dedupeResponseValue,
                        expectedResponseCount(command)
                );
                var clientState = readPersistedClientDedupeState(command);
                if (clientState == null) {
                    clientState = ClientDedupeState.acknowledged(requestId);
                }
                var batch = new PartitionStateWriteBatch(
                        List.of(),
                        dedupeResponseKey,
                        dedupeResponseValue,
                        RockserverStorageKeyspace.dedupeClientState(requestId.clientId()),
                        ClientDedupeStateCodec.encode(clientState),
                        RockserverStorageKeyspace.lastAppliedIndex(),
                        PartitionStateMetadataCodec.encodeLastAppliedIndex(logIndex),
                        logIndex
                );
                writer.write(batch);
                return duplicateResponses;
            }

            var clientState = readPersistedClientDedupeState(command);
            if (clientState == null) {
                return null;
            }
            if (requestId.sequence() <= clientState.acknowledgedSequence()) {
                throw RocksDBException.of(
                        RocksDBException.RocksDBErrorType.DEDUPE_EXPIRED_UNKNOWN_OUTCOME,
                        "Dedupe response for " + requestId + " was expired after acknowledgement watermark "
                                + clientState.acknowledgedWatermark()
                );
            }
            return null;
        } catch (org.rocksdb.RocksDBException e) {
            throw new IllegalStateException("Failed to read partition dedupe state", e);
        }
    }

    @Nullable
    private ClientDedupeState readPersistedClientDedupeState(PartitionWriteCommand command)
            throws org.rocksdb.RocksDBException {
        var requestId = command.requestId();
        var clientStateValue = db.get(RockserverStorageKeyspace.dedupeClientState(requestId.clientId()).toByteArray());
        if (clientStateValue == null) {
            return null;
        }
        return ClientDedupeStateCodec.decode(clientStateValue);
    }

    private static int expectedResponseCount(PartitionWriteCommand command) {
        return command.responseMode().producesResponse() ? command.mutations().size() : 0;
    }

    private static void validateResponseCount(PartitionWriteCommand command, List<Object> responses) {
        var expectedCount = expectedResponseCount(command);
        if (responses.size() != expectedCount) {
            throw new IllegalStateException("partition write response count " + responses.size()
                    + " does not match expected count " + expectedCount);
        }
    }

    private PreparedApply prepareApply(PartitionWriteCommand command) {
        var responses = command.responseMode().producesResponse()
                ? new ArrayList<>(command.mutations().size())
                : new ArrayList<>(0);
        var userWrites = new ArrayList<PartitionStateWrite>(command.mutations().size());
        for (var mutation : command.mutations()) {
            var storageKey = storageKey(command, mutation);
            Buf previousValue = command.responseMode().producesResponse() ? previousValue(storageKey) : null;
            switch (mutation.operation()) {
                case PUT -> {
                    addPutResponse(command.responseMode(), mutation, previousValue, responses);
                    userWrites.add(PartitionStateWrite.put(storageKey, mutation.requiredValue()));
                }
                case DELETE -> {
                    addDeleteResponse(command.responseMode(), mutation, previousValue, responses);
                    userWrites.add(PartitionStateWrite.delete(storageKey));
                }
                case MERGE -> {
                    if (command.responseMode() == PartitionWriteResponseMode.MERGED_VALUE) {
                        var mergedValue = mergeValue(storageKey, previousValue, mutation.requiredValue());
                        responses.add(mergedValue);
                        userWrites.add(PartitionStateWrite.put(storageKey, mergedValue));
                    } else if (command.responseMode() == PartitionWriteResponseMode.NONE) {
                        userWrites.add(PartitionStateWrite.merge(storageKey, mutation.requiredValue()));
                    } else {
                        throw incompatibleResponseMode(command.responseMode(), mutation);
                    }
                }
            }
        }
        return new PreparedApply(
                List.copyOf(userWrites),
                Collections.unmodifiableList(responses)
        );
    }

    private List<ReplicatedCdcEvent> replicatedCdcEvents(long logIndex, PartitionWriteCommand command) {
        if (cdcPartition == null) {
            return List.of();
        }
        var metadataEpoch = PartitionWriteCommandReplayContext.metadataEpoch(metadata, command);
        var partition = PartitionWriteCommandReplayContext.partition(cdcPartition, command);
        var events = new ArrayList<ReplicatedCdcEvent>(command.mutations().size());
        for (int i = 0; i < command.mutations().size(); i++) {
            var mutation = command.mutations().get(i);
            var calculatedKey = PartitionWriteCommandReplayContext.calculatedKey(metadata, command, mutation);
            events.add(new ReplicatedCdcEvent(
                    new ReplicatedCdcToken(
                            metadataEpoch,
                            partition.partitionId(),
                            partition.partitionEpoch(),
                            logIndex,
                            i
                    ),
                    command.schemaEpoch(),
                    mutation.columnId(),
                    calculatedKey,
                    mutation.value(),
                    mutation.operation()
            ));
        }
        return events;
    }

    private void addPutResponse(PartitionWriteResponseMode responseMode,
                                ReplicatedMutation mutation,
                                @Nullable Buf previousValue,
                                List<Object> responses) {
        switch (responseMode) {
            case NONE -> {
            }
            case PREVIOUS_VALUE -> responses.add(previousValue);
            case PREVIOUS_PRESENCE -> responses.add(previousValue != null);
            case DELTA -> responses.add(new Delta<>(previousValue, mutation.requiredValue()));
            case CHANGED -> responses.add(previousValue == null
                    || !Arrays.equals(previousValue.toByteArray(), mutation.requiredValue().toByteArray()));
            case MERGED_VALUE -> throw incompatibleResponseMode(responseMode, mutation);
        }
    }

    private void addDeleteResponse(PartitionWriteResponseMode responseMode,
                                   ReplicatedMutation mutation,
                                   @Nullable Buf previousValue,
                                   List<Object> responses) {
        switch (responseMode) {
            case NONE -> {
            }
            case PREVIOUS_VALUE -> responses.add(previousValue);
            case PREVIOUS_PRESENCE -> responses.add(previousValue != null);
            case DELTA, CHANGED, MERGED_VALUE -> throw incompatibleResponseMode(responseMode, mutation);
        }
    }

    private Buf previousValue(Buf storageKey) {
        if (db == null) {
            throw new IllegalArgumentException("persistent partition state applier requires RocksDB reads for "
                    + "response-producing commands");
        }
        try {
            var value = db.get(storageKey.toByteArray());
            return value == null ? null : Buf.wrap(value).freeze();
        } catch (org.rocksdb.RocksDBException e) {
            throw new IllegalStateException("Failed to read previous partition value", e);
        }
    }

    private Buf mergeValue(Buf storageKey, @Nullable Buf previousValue, Buf operand) {
        if (previousValue == null) {
            return operand;
        }
        if (mergeResolver == null) {
            throw new IllegalArgumentException("persistent partition state applier requires a deterministic "
                    + "merge resolver for merge response production");
        }
        return requireMergedValue(mergeResolver.merge(storageKey, previousValue, operand));
    }

    private Buf storageKey(PartitionWriteCommand command, ReplicatedMutation mutation) {
        var calculatedKey = PartitionWriteCommandReplayContext.calculatedKey(metadata, command, mutation);
        return RockserverStorageKeyspace.userData(mutation.columnId(), calculatedKey);
    }

    private static IllegalArgumentException incompatibleResponseMode(PartitionWriteResponseMode responseMode,
                                                                     ReplicatedMutation mutation) {
        return new IllegalArgumentException("partition write response mode " + responseMode
                + " is not compatible with " + mutation.operation());
    }

    private record PreparedApply(List<PartitionStateWrite> userWrites, List<Object> responses) {
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static Buf requireMergedValue(Buf value) {
        if (value == null) {
            throw new IllegalStateException("mergedValue must not be null");
        }
        return value;
    }
}
