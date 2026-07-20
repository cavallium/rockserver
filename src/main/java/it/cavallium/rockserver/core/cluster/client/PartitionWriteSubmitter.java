package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommandSealer;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogClient;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcException;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

import static it.cavallium.rockserver.core.common.FutureUtils.cancelWithSuppressed;

public final class PartitionWriteSubmitter {

    private final PartitionRouteCache routeCache;
    private final PartitionWriteRouter writeRouter;
    private final LogClientResolver logClientResolver;
    private final int maxAttempts;
    private final LongSupplier currentTimeMillis;

    public PartitionWriteSubmitter(PartitionRouteCache routeCache,
                                   LogClientResolver logClientResolver,
                                   int maxAttempts,
                                   LongSupplier currentTimeMillis) {
        this.routeCache = requireArgument(routeCache, "routeCache");
        this.writeRouter = new PartitionWriteRouter(routeCache);
        this.logClientResolver = requireArgument(logClientResolver, "logClientResolver");
        if (maxAttempts <= 0) {
            throw new IllegalArgumentException("maxAttempts must be > 0");
        }
        this.maxAttempts = maxAttempts;
        this.currentTimeMillis = requireArgument(currentTimeMillis, "currentTimeMillis");
    }

    public ReplicatedLogCommit submit(PartitionWriteCommand command,
                                      long expectedMetadataEpoch,
                                      long deadlineEpochMillis) {
        requireArgument(command, "command");
        if (deadlineEpochMillis < 0) {
            throw new IllegalArgumentException("deadlineEpochMillis must be >= 0");
        }

        requireBeforeDeadline(deadlineEpochMillis);
        PartitionWriteCommand stableCommand = null;
        byte[] stablePayload = null;
        long attemptEpoch = expectedMetadataEpoch;
        PartitionWriteRouteException lastRetryableFailure = null;
        for (int attempt = 1; attempt <= maxAttempts; attempt++) {
            requireBeforeDeadline(deadlineEpochMillis);
            var currentMetadataEpoch = routeCache.metadataEpoch();
            if (attemptEpoch != currentMetadataEpoch) {
                if (attemptEpoch > currentMetadataEpoch) {
                    throw new PartitionWriteRouteException(
                            PartitionWriteFailureKind.STALE_ROUTE,
                            "future route metadata epoch " + attemptEpoch + "; current epoch is "
                                    + currentMetadataEpoch
                    );
                }
                lastRetryableFailure = new PartitionWriteRouteException(
                        PartitionWriteFailureKind.STALE_ROUTE,
                        "stale route metadata epoch " + attemptEpoch + "; current epoch is "
                                + currentMetadataEpoch
                );
                attemptEpoch = currentMetadataEpoch;
                continue;
            }

            CompletableFuture<ReplicatedLogCommit> proposal = null;
            try {
                if (stablePayload == null) {
                    stableCommand = admitCommand(command);
                    stablePayload = encodeStablePayload(stableCommand);
                }
                var target = writeRouter.routeWriteAtEpoch(attemptEpoch, stableCommand);
                var logClient = resolveLogClient(target);
                proposal = logClient.propose(Arrays.copyOf(stablePayload, stablePayload.length));
                if (proposal == null) {
                    throw proposalFailure("replicated write proposal returned null future");
                }
                var commit = proposal.get(remainingMillis(deadlineEpochMillis), TimeUnit.MILLISECONDS);
                if (commit == null) {
                    throw proposalFailure("replicated write proposal returned null commit");
                }
                if (!commit.committed()) {
                    throw new PartitionWriteRouteException(
                            PartitionWriteFailureKind.RETRY_EXHAUSTED,
                            "replicated write did not commit at log index " + commit.index()
                    );
                }
                return commit;
            } catch (TimeoutException e) {
                cancelWithSuppressed(proposal, e);
                lastRetryableFailure = new PartitionWriteRouteException(
                        PartitionWriteFailureKind.TIMEOUT,
                        "replicated write proposal timed out",
                        e
                );
            } catch (InterruptedException e) {
                cancelWithSuppressed(proposal, e);
                Thread.currentThread().interrupt();
                throw new PartitionWriteRouteException(
                        PartitionWriteFailureKind.TIMEOUT,
                        "replicated write proposal interrupted",
                        e
                );
            } catch (PartitionWriteRouteException e) {
                if (!e.retryable()) {
                    throw e;
                }
                lastRetryableFailure = e;
                attemptEpoch = nextAttemptEpochAfterRetryableFailure(e, attemptEpoch);
            } catch (RatisProcessAdapterRpcException e) {
                throw adapterRpcProposalFailure(e);
            } catch (RuntimeException e) {
                var proposalFailure = proposalFailureFrom(e);
                if (!proposalFailure.retryable()) {
                    throw proposalFailure;
                }
                lastRetryableFailure = proposalFailure;
                attemptEpoch = nextAttemptEpochAfterRetryableFailure(proposalFailure, attemptEpoch);
            } catch (ExecutionException e) {
                var proposalFailure = proposalFailureFrom(e.getCause());
                if (!proposalFailure.retryable()) {
                    throw proposalFailure;
                }
                lastRetryableFailure = proposalFailure;
                attemptEpoch = nextAttemptEpochAfterRetryableFailure(proposalFailure, attemptEpoch);
            }
        }

        if (lastRetryableFailure != null) {
            if (deadlineExpired(deadlineEpochMillis)) {
                throw new PartitionWriteRouteException(
                        PartitionWriteFailureKind.DEADLINE_EXPIRED,
                        "replicated write deadline expired after retries",
                        lastRetryableFailure
                );
            }
            throw lastRetryableFailure;
        }
        throw new PartitionWriteRouteException(
                PartitionWriteFailureKind.RETRY_EXHAUSTED,
                "replicated write retry budget was exhausted"
        );
    }

    private void updateMetadataFromNotLeader(PartitionWriteRouteException routeException) {
        try {
            routeCache.updateMetadata(routeException.refreshedMetadata());
        } catch (RuntimeException e) {
            throw new PartitionWriteRouteException(
                    PartitionWriteFailureKind.RETRY_EXHAUSTED,
                    "not-leader refreshed metadata was rejected: " + e.getMessage(),
                    e
            );
        }
    }

    private ReplicatedLogClient resolveLogClient(RouteTarget target) {
        var logClient = logClientResolver.resolve(target);
        if (logClient == null) {
            throw proposalFailure("replicated write resolver returned null log client");
        }
        return logClient;
    }

    private void requireBeforeDeadline(long deadlineEpochMillis) {
        if (deadlineExpired(deadlineEpochMillis)) {
            throw new PartitionWriteRouteException(
                    PartitionWriteFailureKind.DEADLINE_EXPIRED,
                    "replicated write deadline expired before proposal"
            );
        }
    }

    private PartitionWriteCommand admitCommand(PartitionWriteCommand command) {
        try {
            return PartitionWriteCommandSealer.seal(routeCache.metadata(), command);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw invalidRequest(e);
        }
    }

    private static byte[] encodeStablePayload(PartitionWriteCommand stableCommand) {
        try {
            return PartitionWriteCommandCodec.encode(stableCommand);
        } catch (IllegalArgumentException | IllegalStateException e) {
            throw invalidRequest(e);
        }
    }

    private static PartitionWriteRouteException invalidRequest(RuntimeException cause) {
        return new PartitionWriteRouteException(
                PartitionWriteFailureKind.INVALID_REQUEST,
                "invalid replicated partition write: " + cause.getMessage(),
                cause
        );
    }

    private static PartitionWriteRouteException proposalFailure(String message) {
        return new PartitionWriteRouteException(PartitionWriteFailureKind.RETRY_EXHAUSTED, message);
    }

    private static PartitionWriteRouteException proposalFailure(Throwable cause) {
        return new PartitionWriteRouteException(
                PartitionWriteFailureKind.RETRY_EXHAUSTED,
                "replicated write proposal failed",
                cause
        );
    }

    private static PartitionWriteRouteException adapterRpcProposalFailure(RatisProcessAdapterRpcException cause) {
        return new PartitionWriteRouteException(
                PartitionWriteFailureKind.RETRY_EXHAUSTED,
                "replicated write proposal failed with adapter error " + cause.errorType() + ": "
                        + cause.getMessage(),
                cause
        );
    }

    private long nextAttemptEpochAfterRetryableFailure(PartitionWriteRouteException routeException,
                                                       long currentAttemptEpoch) {
        if (routeException.kind() == PartitionWriteFailureKind.NOT_LEADER
                && routeException.refreshedMetadata() != null) {
            updateMetadataFromNotLeader(routeException);
            return routeCache.metadataEpoch();
        }
        return currentAttemptEpoch;
    }

    private boolean deadlineExpired(long deadlineEpochMillis) {
        return deadlineEpochMillis > 0 && currentTimeMillis.getAsLong() >= deadlineEpochMillis;
    }

    private long remainingMillis(long deadlineEpochMillis) {
        if (deadlineEpochMillis == 0) {
            return Long.MAX_VALUE;
        }
        return Math.max(1, deadlineEpochMillis - currentTimeMillis.getAsLong());
    }

    private static PartitionWriteRouteException proposalFailureFrom(Throwable failure) {
        var cause = unwrap(failure);
        return switch (cause) {
            case PartitionWriteRouteException routeException -> routeException;
            case RatisProcessAdapterRpcException adapterRpcException -> adapterRpcProposalFailure(adapterRpcException);
            case RuntimeException runtimeException -> proposalFailure(runtimeException);
            case Error error -> throw error;
            case null, default -> new PartitionWriteRouteException(
                    PartitionWriteFailureKind.RETRY_EXHAUSTED,
                    "replicated write proposal failed",
                    cause
            );
        };
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException completionException && completionException.getCause() != null) {
            return unwrap(completionException.getCause());
        }
        return failure;
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    @FunctionalInterface
    public interface LogClientResolver {

        ReplicatedLogClient resolve(RouteTarget target);
    }
}
