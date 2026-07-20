package it.cavallium.rockserver.core.replication.ratis;

import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteApplier;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationApplier;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.cluster.ReplicationFailureInjector;
import it.cavallium.rockserver.core.cluster.ReplicationFailurePhase;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.function.Consumer;

public final class RockserverRatisStateMachine {

    private final PartitionWriteApplier applier;
    private final Consumer<PartitionWriteCommand> validator;
    private final ReplicationFailureInjector failureInjector;
    private long lastAppliedIndex;

    public RockserverRatisStateMachine(RocksDBSyncAPI api) {
        this(new ReplicatedMutationApplier(requireNonNull(api, "api")));
    }

    public RockserverRatisStateMachine(ReplicatedMutationApplier applier) {
        this(replicatedMutationApplier(applier), _ -> {
        });
    }

    public RockserverRatisStateMachine(ReplicatedMutationApplier applier,
                                       Consumer<PartitionWriteCommand> validator) {
        this(replicatedMutationApplier(applier), validator);
    }

    public RockserverRatisStateMachine(PartitionWriteApplier applier,
                                       Consumer<PartitionWriteCommand> validator) {
        this(applier, validator, ReplicationFailureInjector.none());
    }

    public RockserverRatisStateMachine(PartitionWriteApplier applier,
                                       Consumer<PartitionWriteCommand> validator,
                                       long initialLastAppliedIndex) {
        this(applier, validator, ReplicationFailureInjector.none(), initialLastAppliedIndex);
    }

    public RockserverRatisStateMachine(PartitionWriteApplier applier,
                                       Consumer<PartitionWriteCommand> validator,
                                       ReplicationFailureInjector failureInjector) {
        this(applier, validator, failureInjector, 0);
    }

    public RockserverRatisStateMachine(PartitionWriteApplier applier,
                                       Consumer<PartitionWriteCommand> validator,
                                       ReplicationFailureInjector failureInjector,
                                       long initialLastAppliedIndex) {
        this.applier = requireNonNull(applier, "applier");
        this.validator = requireNonNull(validator, "validator");
        this.failureInjector = requireNonNull(failureInjector, "failureInjector");
        if (initialLastAppliedIndex < 0) {
            throw new IllegalArgumentException("initialLastAppliedIndex must be >= 0");
        }
        this.lastAppliedIndex = initialLastAppliedIndex;
    }

    public synchronized ApplyResult applyCommitted(long logIndex, byte[] payload) {
        if (logIndex < 0) {
            throw new IllegalArgumentException("logIndex must be >= 0");
        }
        requireNonNull(payload, "payload");
        if (logIndex <= lastAppliedIndex) {
            return new ApplyResult(logIndex, 0, false);
        }

        var command = PartitionWriteCommandCodec.decode(payload);
        validator.accept(command);
        failureInjector.inject(ReplicationFailurePhase.APPLY);
        var result = new ApplyResult(logIndex, command.mutations().size(), true, applier.apply(logIndex, command));
        lastAppliedIndex = logIndex;
        return result;
    }

    public synchronized long lastAppliedIndex() {
        return lastAppliedIndex;
    }

    public record ApplyResult(long logIndex, int mutationsApplied, boolean applied, List<Object> responses) {

        public ApplyResult(long logIndex, int mutationsApplied, boolean applied) {
            this(logIndex, mutationsApplied, applied, List.of());
        }

        public ApplyResult {
            responses = copyResponses(responses);
            if (logIndex < 0) {
                throw new IllegalArgumentException("logIndex must be >= 0");
            }
            if (mutationsApplied < 0) {
                throw new IllegalArgumentException("mutationsApplied must be >= 0");
            }
            if (!applied && mutationsApplied != 0) {
                throw new IllegalArgumentException("skipped apply results must not report mutations");
            }
            if (!applied && !responses.isEmpty()) {
                throw new IllegalArgumentException("skipped apply results must not report responses");
            }
        }
    }

    private static PartitionWriteApplier replicatedMutationApplier(ReplicatedMutationApplier applier) {
        var checkedApplier = requireNonNull(applier, "applier");
        return (_, command) -> checkedApplier.apply(command);
    }

    private static List<Object> copyResponses(List<Object> responses) {
        requireNonNull(responses, "responses");
        var copy = new ArrayList<>(responses.size());
        copy.addAll(responses);
        return Collections.unmodifiableList(copy);
    }

    private static <T> T requireNonNull(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
