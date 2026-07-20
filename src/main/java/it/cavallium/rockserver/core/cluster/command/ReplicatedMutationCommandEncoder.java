package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.rockserver.core.cluster.ReplicatedCommandClassifier;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBException;

import java.util.ArrayList;
import java.util.List;

import org.reactivestreams.Publisher;
import reactor.core.Exceptions;
import reactor.core.publisher.Flux;

public final class ReplicatedMutationCommandEncoder {

    private ReplicatedMutationCommandEncoder() {
    }

    public record EncodedPartitionWrite(List<ReplicatedMutation> mutations,
                                        PartitionWriteResponseMode responseMode) {

        public EncodedPartitionWrite {
            var requiredMutations = requireArgument(mutations, "mutations");
            var requiredResponseMode = requireArgument(responseMode, "responseMode");
            for (var mutation : requiredMutations) {
                requireArgument(mutation, "mutation");
            }
            mutations = List.copyOf(requiredMutations);
            requireWithinMutationLimit(mutations.size());
            requiredResponseMode.validate(mutations);
        }

        public boolean isNoop() {
            return mutations.isEmpty();
        }
    }

    public static ReplicatedMutation encode(RocksDBAPICommand<?, ?, ?> command) {
        var encoded = encodePartitionWrite(command);
        requireNoResult(encoded.responseMode());
        if (encoded.isNoop()) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    "replicated command produced no mutations"
            );
        }
        var mutations = encoded.mutations();
        if (mutations.size() != 1) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
                    "replicated command expands to " + mutations.size() + " log entries"
            );
        }
        return mutations.getFirst();
    }

    public static List<ReplicatedMutation> encodeAll(RocksDBAPICommand<?, ?, ?> command) {
        var encoded = encodePartitionWrite(command);
        requireNoResult(encoded.responseMode());
        return encoded.mutations();
    }

    public static EncodedPartitionWrite encodePartitionWrite(RocksDBAPICommand<?, ?, ?> command) {
        requireArgument(command, "command");
        ReplicatedCommandClassifier.requireSafeForReplicatedLog(command);
        return switch (command) {
            case RocksDBAPICommand.RocksDBAPICommandSingle.Put<?> put -> encodePut(put);
            case RocksDBAPICommand.RocksDBAPICommandSingle.Delete<?> delete -> encodeDelete(delete);
            case RocksDBAPICommand.RocksDBAPICommandSingle.Merge<?> merge -> encodeMerge(merge);
            case RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?> putMulti -> encodePutMulti(putMulti);
            case RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?> deleteMulti -> encodeDeleteMulti(deleteMulti);
            case RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?> mergeMulti -> encodeMergeMulti(mergeMulti);
            case RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch putBatch -> new EncodedPartitionWrite(
                    encodePutBatch(putBatch),
                    PartitionWriteResponseMode.NONE
            );
            case RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch mergeBatch -> new EncodedPartitionWrite(
                    encodeMergeBatch(mergeBatch),
                    PartitionWriteResponseMode.NONE
            );
            default -> throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
                    "replicated command encoding does not support " + command.getClass().getName()
            );
        };
    }

    private static EncodedPartitionWrite encodePut(RocksDBAPICommand.RocksDBAPICommandSingle.Put<?> put) {
        requireDirectWrite(put.transactionOrUpdateId());
        var columnId = requireColumnId(put.columnId());
        var keys = requireKeys(put.keys(), "put keys");
        var value = requireWriteField(put.value(), "put value");
        var requestType = requireWriteField(put.requestType(), "put requestType");
        return new EncodedPartitionWrite(
                List.of(ReplicatedMutation.put(columnId, keys, value)),
                putResponseMode(requestType)
        );
    }

    private static EncodedPartitionWrite encodeDelete(RocksDBAPICommand.RocksDBAPICommandSingle.Delete<?> delete) {
        requireDirectWrite(delete.transactionOrUpdateId());
        var columnId = requireColumnId(delete.columnId());
        var keys = requireKeys(delete.keys(), "delete keys");
        var requestType = requireWriteField(delete.requestType(), "delete requestType");
        return new EncodedPartitionWrite(
                List.of(ReplicatedMutation.delete(columnId, keys)),
                deleteResponseMode(requestType)
        );
    }

    private static EncodedPartitionWrite encodeMerge(RocksDBAPICommand.RocksDBAPICommandSingle.Merge<?> merge) {
        requireDirectWrite(merge.transactionOrUpdateId());
        var columnId = requireColumnId(merge.columnId());
        var keys = requireKeys(merge.keys(), "merge keys");
        var value = requireWriteField(merge.value(), "merge value");
        var requestType = requireWriteField(merge.requestType(), "merge requestType");
        return new EncodedPartitionWrite(
                List.of(ReplicatedMutation.merge(columnId, keys, value)),
                mergeResponseMode(requestType)
        );
    }

    private static EncodedPartitionWrite encodePutMulti(
            RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?> putMulti) {
        requireDirectWrite(putMulti.transactionOrUpdateId());
        var columnId = requireColumnId(putMulti.columnId());
        var keys = requireWriteField(putMulti.keys(), "putMulti keys");
        var values = requireWriteField(putMulti.values(), "putMulti values");
        var requestType = requireWriteField(putMulti.requestType(), "putMulti requestType");
        requireMatchingKeysAndValues(keys.size(), values.size());
        requireWithinMutationLimit(keys.size());

        var mutations = new ArrayList<ReplicatedMutation>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            mutations.add(ReplicatedMutation.put(
                    columnId,
                    requireKeys(keys.get(i), "putMulti key " + i),
                    requireWriteField(values.get(i), "putMulti value " + i)
            ));
        }
        return new EncodedPartitionWrite(mutations, putResponseMode(requestType));
    }

    private static EncodedPartitionWrite encodeDeleteMulti(
            RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?> deleteMulti) {
        requireDirectWrite(deleteMulti.transactionOrUpdateId());
        var columnId = requireColumnId(deleteMulti.columnId());
        var keysList = requireWriteField(deleteMulti.keys(), "deleteMulti keys");
        var requestType = requireWriteField(deleteMulti.requestType(), "deleteMulti requestType");
        requireWithinMutationLimit(keysList.size());

        var mutations = new ArrayList<ReplicatedMutation>(keysList.size());
        for (int i = 0; i < keysList.size(); i++) {
            mutations.add(ReplicatedMutation.delete(columnId, requireKeys(keysList.get(i), "deleteMulti key " + i)));
        }
        return new EncodedPartitionWrite(mutations, deleteResponseMode(requestType));
    }

    private static EncodedPartitionWrite encodeMergeMulti(
            RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?> mergeMulti) {
        requireDirectWrite(mergeMulti.transactionOrUpdateId());
        var columnId = requireColumnId(mergeMulti.columnId());
        var keys = requireWriteField(mergeMulti.keys(), "mergeMulti keys");
        var values = requireWriteField(mergeMulti.values(), "mergeMulti values");
        var requestType = requireWriteField(mergeMulti.requestType(), "mergeMulti requestType");
        requireMatchingKeysAndValues(keys.size(), values.size());
        requireWithinMutationLimit(keys.size());

        var mutations = new ArrayList<ReplicatedMutation>(keys.size());
        for (int i = 0; i < keys.size(); i++) {
            mutations.add(ReplicatedMutation.merge(
                    columnId,
                    requireKeys(keys.get(i), "mergeMulti key " + i),
                    requireWriteField(values.get(i), "mergeMulti value " + i)
            ));
        }
        return new EncodedPartitionWrite(mutations, mergeResponseMode(requestType));
    }

    private static List<ReplicatedMutation> encodePutBatch(RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch putBatch) {
        var mode = requireWriteField(putBatch.mode(), "putBatch mode");
        if (mode != PutBatchMode.WRITE_BATCH) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
                    "unsafe local putBatch mode cannot bypass consensus in replicated mode: " + mode
            );
        }
        return collectBatchMutations(requireColumnId(putBatch.columnId()),
                putBatch.batchPublisher(), ReplicatedMutationOp.PUT);
    }

    private static List<ReplicatedMutation> encodeMergeBatch(
            RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch mergeBatch) {
        var mode = requireWriteField(mergeBatch.mode(), "mergeBatch mode");
        if (mode != MergeBatchMode.MERGE_WRITE_BATCH) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
                    "unsafe local mergeBatch mode cannot bypass consensus in replicated mode: " + mode
            );
        }
        return collectBatchMutations(requireColumnId(mergeBatch.columnId()),
                mergeBatch.batchPublisher(), ReplicatedMutationOp.MERGE);
    }

    private static List<ReplicatedMutation> collectBatchMutations(long columnId,
                                                                  Publisher<KVBatch> batchPublisher,
                                                                  ReplicatedMutationOp operation) {
        var requiredBatchPublisher = requireWriteField(batchPublisher, "replicated write batch publisher");
        ArrayList<ReplicatedMutation> mutations;
        try {
            mutations = Flux.from(requiredBatchPublisher)
                    .collect(ArrayList<ReplicatedMutation>::new,
                            (accumulator, batch) -> appendBatchMutations(accumulator, columnId, batch, operation))
                    .block();
        } catch (RocksDBException e) {
            throw e;
        } catch (RuntimeException e) {
            throw batchMaterializationFailure(e);
        }
        if (mutations == null || mutations.isEmpty()) {
            return List.of();
        }
        return List.copyOf(mutations);
    }

    private static void appendBatchMutations(ArrayList<ReplicatedMutation> mutations,
                                             long columnId,
                                             KVBatch batch,
                                             ReplicatedMutationOp operation) {
        var requiredBatch = requireWriteField(batch, "replicated write batch");
        var keys = requireWriteField(requiredBatch.keys(), "replicated write batch keys");
        var values = requireWriteField(requiredBatch.values(), "replicated write batch values");
        requireMatchingKeysAndValues(keys.size(), values.size());
        for (int i = 0; i < keys.size(); i++) {
            requireWithinMutationLimit(mutations.size() + 1);
            var key = requireKeys(keys.get(i), "replicated write batch key " + i);
            var value = requireWriteField(values.get(i), "replicated write batch value " + i);
            mutations.add(operation == ReplicatedMutationOp.PUT
                    ? ReplicatedMutation.put(columnId, key, value)
                    : ReplicatedMutation.merge(columnId, key, value));
        }
    }

    private static RocksDBException batchMaterializationFailure(RuntimeException failure) {
        var unwrapped = Exceptions.unwrap(failure);
        if (unwrapped instanceof RocksDBException rocksDBException) {
            return rocksDBException;
        }
        if (unwrapped instanceof Error error) {
            throw error;
        }
        return RocksDBException.of(
                RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                "replicated write batch publisher failed: " + failureDescription(unwrapped),
                unwrapped
        );
    }

    private static String failureDescription(Throwable failure) {
        var message = failure.getMessage();
        return message == null || message.isBlank() ? failure.getClass().getName() : message;
    }

    private static long requireColumnId(long columnId) {
        if (columnId < 0) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    "columnId must be >= 0"
            );
        }
        return columnId;
    }

    private static Keys requireKeys(Keys keys, String description) {
        var requiredKeys = requireWriteField(keys, description);
        var keyParts = requireWriteField(requiredKeys.keys(), description + " parts");
        for (int i = 0; i < keyParts.length; i++) {
            requireWriteField(keyParts[i], description + " part " + i);
        }
        return requiredKeys;
    }

    private static <T> T requireWriteField(T value, String description) {
        if (value == null) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    description + " must not be null");
        }
        return value;
    }

    private static void requireWithinMutationLimit(int mutationCount) {
        if (mutationCount > PartitionWriteCommand.MAX_MUTATIONS) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    "replicated writes are limited to "
                            + PartitionWriteCommand.MAX_MUTATIONS
                            + " mutations per command"
            );
        }
    }

    private static void requireDirectWrite(long transactionOrUpdateId) {
        if (transactionOrUpdateId != 0) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
                    "replicated transaction/update write contexts are not supported in replicated mode"
            );
        }
    }

    private static void requireNoResult(PartitionWriteResponseMode responseMode) {
        if (responseMode != PartitionWriteResponseMode.NONE) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED,
                    "encodeAll only accepts no-result replicated writes; use encodePartitionWrite for response-producing writes"
            );
        }
    }

    private static void requireMatchingKeysAndValues(int keyCount, int valueCount) {
        if (keyCount != valueCount) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    "keys and values must have the same size: keys=" + keyCount + ", values=" + valueCount
            );
        }
    }

    private static PartitionWriteResponseMode putResponseMode(RequestType.RequestPut<?, ?> requestType) {
        var requiredRequestType = requireWriteField(requestType, "put requestType");
        return switch (requiredRequestType) {
            case RequestType.RequestNothing<?> ignored -> PartitionWriteResponseMode.NONE;
            case RequestType.RequestPrevious<?> ignored -> PartitionWriteResponseMode.PREVIOUS_VALUE;
            case RequestType.RequestPreviousPresence<?> ignored -> PartitionWriteResponseMode.PREVIOUS_PRESENCE;
            case RequestType.RequestDelta<?> ignored -> PartitionWriteResponseMode.DELTA;
            case RequestType.RequestChanged<?> ignored -> PartitionWriteResponseMode.CHANGED;
        };
    }

    private static PartitionWriteResponseMode deleteResponseMode(RequestType.RequestDelete<?, ?> requestType) {
        var requiredRequestType = requireWriteField(requestType, "delete requestType");
        return switch (requiredRequestType) {
            case RequestType.RequestNothing<?> ignored -> PartitionWriteResponseMode.NONE;
            case RequestType.RequestPrevious<?> ignored -> PartitionWriteResponseMode.PREVIOUS_VALUE;
            case RequestType.RequestPreviousPresence<?> ignored -> PartitionWriteResponseMode.PREVIOUS_PRESENCE;
        };
    }

    private static PartitionWriteResponseMode mergeResponseMode(RequestType.RequestMerge<?, ?> requestType) {
        var requiredRequestType = requireWriteField(requestType, "merge requestType");
        return switch (requiredRequestType) {
            case RequestType.RequestNothing<?> ignored -> PartitionWriteResponseMode.NONE;
            case RequestType.RequestMerged<?> ignored -> PartitionWriteResponseMode.MERGED_VALUE;
        };
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
