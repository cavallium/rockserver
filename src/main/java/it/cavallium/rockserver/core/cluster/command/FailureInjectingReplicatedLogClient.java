package it.cavallium.rockserver.core.cluster.command;

import it.cavallium.rockserver.core.cluster.ReplicationFailureInjector;
import it.cavallium.rockserver.core.cluster.ReplicationFailurePhase;

import java.util.concurrent.CompletableFuture;

import static it.cavallium.rockserver.core.common.FutureUtils.mapCancellation;

public final class FailureInjectingReplicatedLogClient implements ReplicatedLogClient {

    private final ReplicatedLogClient delegate;
    private final ReplicationFailureInjector failureInjector;

    public FailureInjectingReplicatedLogClient(ReplicatedLogClient delegate,
                                               ReplicationFailureInjector failureInjector) {
        this.delegate = requireArgument(delegate, "delegate");
        this.failureInjector = requireArgument(failureInjector, "failureInjector");
    }

    @Override
    public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
        failureInjector.inject(ReplicationFailurePhase.APPEND);
        return mapCancellation(
                delegate.propose(payload),
                commit -> {
                    failureInjector.inject(ReplicationFailurePhase.COMMIT);
                    failureInjector.inject(ReplicationFailurePhase.RESPONSE_SEND);
                    return commit;
                }
        );
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
