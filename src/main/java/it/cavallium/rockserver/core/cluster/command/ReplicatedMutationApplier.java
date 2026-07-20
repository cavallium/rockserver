package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public final class ReplicatedMutationApplier {

    private final RocksDBSyncAPI api;

    public ReplicatedMutationApplier(RocksDBSyncAPI api) {
        this.api = requireArgument(api, "api");
    }

    public void apply(List<ReplicatedMutation> mutations) {
        apply(mutations, PartitionWriteResponseMode.NONE);
    }

    public List<Object> apply(PartitionWriteCommand command) {
        requireArgument(command, "command");
        return apply(command.mutations(), command.responseMode());
    }

    public List<Object> apply(List<ReplicatedMutation> mutations, PartitionWriteResponseMode responseMode) {
        requireArgument(mutations, "mutations");
        requireArgument(responseMode, "responseMode");
        if (mutations.isEmpty()) {
            return List.of();
        }
        responseMode.validate(mutations);
        if (responseMode == PartitionWriteResponseMode.NONE) {
            applyWithoutResponse(mutations);
            return List.of();
        }

        if (mutations.size() > 1) {
            var batchResponses = applyHomogeneousBatchWithResponse(mutations, responseMode);
            if (batchResponses != null) {
                return batchResponses;
            }
        }

        var responses = new ArrayList<>(mutations.size());
        for (var mutation : mutations) {
            responses.add(copyResponse(apply(mutation, responseMode)));
        }
        return copyResponses(responses);
    }

    public void apply(ReplicatedMutation mutation) {
        apply(mutation, PartitionWriteResponseMode.NONE);
    }

    private void applyWithoutResponse(List<ReplicatedMutation> mutations) {
        if (mutations.size() > 1 && applyHomogeneousBatch(mutations)) {
            return;
        }
        for (var mutation : mutations) {
            apply(mutation);
        }
    }

    private Object apply(ReplicatedMutation mutation, PartitionWriteResponseMode responseMode) {
        requireArgument(mutation, "mutation");
        requireArgument(responseMode, "responseMode");
        if (!responseMode.isCompatibleWith(mutation.operation())) {
            throw new IllegalArgumentException("partition write response mode " + responseMode
                    + " is not compatible with " + mutation.operation());
        }
        return switch (mutation.operation()) {
            case PUT -> applyPut(mutation, responseMode);
            case DELETE -> applyDelete(mutation, responseMode);
            case MERGE -> applyMerge(mutation, responseMode);
        };
    }

    private Object applyPut(ReplicatedMutation mutation, PartitionWriteResponseMode responseMode) {
        return switch (responseMode) {
            case NONE -> api.put(0, mutation.columnId(), mutation.keys(), mutation.requiredValue(),
                    RequestType.none());
            case PREVIOUS_VALUE -> api.put(0, mutation.columnId(), mutation.keys(), mutation.requiredValue(),
                    RequestType.previous());
            case PREVIOUS_PRESENCE -> api.put(0, mutation.columnId(), mutation.keys(), mutation.requiredValue(),
                    RequestType.previousPresence());
            case DELTA -> api.put(0, mutation.columnId(), mutation.keys(), mutation.requiredValue(),
                    RequestType.delta());
            case CHANGED -> api.put(0, mutation.columnId(), mutation.keys(), mutation.requiredValue(),
                    RequestType.changed());
            case MERGED_VALUE -> throw incompatibleResponseMode(mutation, responseMode);
        };
    }

    private Object applyDelete(ReplicatedMutation mutation, PartitionWriteResponseMode responseMode) {
        return switch (responseMode) {
            case NONE -> api.delete(0, mutation.columnId(), mutation.keys(), RequestType.none());
            case PREVIOUS_VALUE -> api.delete(0, mutation.columnId(), mutation.keys(), RequestType.previous());
            case PREVIOUS_PRESENCE -> api.delete(0, mutation.columnId(), mutation.keys(),
                    RequestType.previousPresence());
            case DELTA, CHANGED, MERGED_VALUE -> throw incompatibleResponseMode(mutation, responseMode);
        };
    }

    private Object applyMerge(ReplicatedMutation mutation, PartitionWriteResponseMode responseMode) {
        return switch (responseMode) {
            case NONE -> api.merge(0, mutation.columnId(), mutation.keys(), mutation.requiredValue(),
                    RequestType.none());
            case MERGED_VALUE -> api.merge(0, mutation.columnId(), mutation.keys(), mutation.requiredValue(),
                    RequestType.merged());
            case PREVIOUS_VALUE, PREVIOUS_PRESENCE, DELTA, CHANGED -> throw incompatibleResponseMode(mutation,
                    responseMode);
        };
    }

    private boolean applyHomogeneousBatch(List<ReplicatedMutation> mutations) {
        if (hasMixedBatchShape(mutations)) {
            return false;
        }

        var first = mutations.getFirst();
        var keys = mutationKeys(mutations);
        RequestType.RequestNothing<Buf> noResult = RequestType.none();
        switch (first.operation()) {
            case PUT -> {
                var values = mutationValues(mutations);
                api.putMulti(0, first.columnId(), keys, values, noResult);
            }
            case DELETE -> api.deleteMulti(0, first.columnId(), keys, noResult);
            case MERGE -> {
                var values = mutationValues(mutations);
                api.mergeMulti(0, first.columnId(), keys, values, noResult);
            }
        }
        return true;
    }

    private List<Object> applyHomogeneousBatchWithResponse(List<ReplicatedMutation> mutations,
                                                           PartitionWriteResponseMode responseMode) {
        if (hasMixedBatchShape(mutations)) {
            return null;
        }

        var first = mutations.getFirst();
        var keys = mutationKeys(mutations);
        return switch (first.operation()) {
            case PUT -> {
                var values = mutationValues(mutations);
                yield switch (responseMode) {
                    case PREVIOUS_VALUE -> copyBatchResponses(api.putMulti(0, first.columnId(), keys, values,
                            RequestType.previous()), mutations.size());
                    case PREVIOUS_PRESENCE -> copyBatchResponses(api.putMulti(0, first.columnId(), keys, values,
                            RequestType.previousPresence()), mutations.size());
                    case DELTA -> copyBatchResponses(api.putMulti(0, first.columnId(), keys, values,
                            RequestType.delta()), mutations.size());
                    case CHANGED -> copyBatchResponses(api.putMulti(0, first.columnId(), keys, values,
                            RequestType.changed()), mutations.size());
                    case NONE, MERGED_VALUE -> throw incompatibleResponseMode(first, responseMode);
                };
            }
            case DELETE -> switch (responseMode) {
                case PREVIOUS_VALUE -> copyBatchResponses(api.deleteMulti(0, first.columnId(), keys,
                        RequestType.previous()), mutations.size());
                case PREVIOUS_PRESENCE -> copyBatchResponses(api.deleteMulti(0, first.columnId(), keys,
                        RequestType.previousPresence()), mutations.size());
                case NONE, DELTA, CHANGED, MERGED_VALUE -> throw incompatibleResponseMode(first, responseMode);
            };
            case MERGE -> {
                var values = mutationValues(mutations);
                yield switch (responseMode) {
                    case MERGED_VALUE -> copyBatchResponses(api.mergeMulti(0, first.columnId(), keys, values,
                            RequestType.merged()), mutations.size());
                    case NONE, PREVIOUS_VALUE, PREVIOUS_PRESENCE, DELTA, CHANGED ->
                            throw incompatibleResponseMode(first, responseMode);
                };
            }
        };
    }

    private static boolean hasMixedBatchShape(List<ReplicatedMutation> mutations) {
        var first = mutations.getFirst();
        for (var mutation : mutations) {
            if (mutation.operation() != first.operation() || mutation.columnId() != first.columnId()) {
                return true;
            }
        }
        return false;
    }

    private static ArrayList<Keys> mutationKeys(List<ReplicatedMutation> mutations) {
        var keys = new ArrayList<Keys>(mutations.size());
        for (var mutation : mutations) {
            keys.add(mutation.keys());
        }
        return keys;
    }

    private static ArrayList<Buf> mutationValues(List<ReplicatedMutation> mutations) {
        var values = new ArrayList<Buf>(mutations.size());
        for (var mutation : mutations) {
            values.add(mutation.requiredValue());
        }
        return values;
    }

    private static IllegalArgumentException incompatibleResponseMode(ReplicatedMutation mutation,
                                                                     PartitionWriteResponseMode responseMode) {
        return new IllegalArgumentException("partition write response mode " + responseMode
                + " is not compatible with " + mutation.operation());
    }

    private static Object copyResponse(Object response) {
        if (response instanceof Buf buf) {
            return Buf.wrap(buf.toByteArray()).freeze();
        }
        if (response instanceof Delta<?> delta) {
            return new Delta<>(copyResponse(delta.previous()), copyResponse(delta.current()));
        }
        return response;
    }

    private static List<Object> copyResponses(List<?> responses) {
        requireState(responses, "responses");
        var copied = new ArrayList<>(responses.size());
        for (var response : responses) {
            copied.add(copyResponse(response));
        }
        return Collections.unmodifiableList(copied);
    }

    private static List<Object> copyBatchResponses(List<?> responses, int expectedCount) {
        requireState(responses, "responses");
        if (responses.size() != expectedCount) {
            throw new IllegalStateException("partition write batch response count " + responses.size()
                    + " does not match mutation count " + expectedCount);
        }
        return copyResponses(responses);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static <T> T requireState(T value, String name) {
        if (value == null) {
            throw new IllegalStateException(name + " must not be null");
        }
        return value;
    }
}
