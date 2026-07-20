package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommandCodec;
import it.cavallium.rockserver.core.replication.ratis.RatisFeatureUnavailableException;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcException;

import java.util.Arrays;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

import static it.cavallium.rockserver.core.common.FutureUtils.cancelWithSuppressed;

public final class PartitionPointReadSubmitter {

    private final PartitionRouteCache routeCache;
    private final PartitionPointReadRouter readRouter;
    private final RatisClientResolver ratisClientResolver;
    private final LongSupplier currentTimeMillis;

    public PartitionPointReadSubmitter(PartitionRouteCache routeCache,
                                       RatisClientResolver ratisClientResolver,
                                       LongSupplier currentTimeMillis) {
        this.routeCache = requireArgument(routeCache, "routeCache");
        this.readRouter = new PartitionPointReadRouter(routeCache);
        this.ratisClientResolver = requireArgument(ratisClientResolver, "ratisClientResolver");
        this.currentTimeMillis = requireArgument(currentTimeMillis, "currentTimeMillis");
    }

    public byte[] readLinearizable(PartitionPointReadCommand command,
                                   long expectedMetadataEpoch,
                                   long deadlineEpochMillis) {
        return read(command, expectedMetadataEpoch, RoutingPolicy.linearizable(), deadlineEpochMillis);
    }

    public byte[] read(PartitionPointReadCommand command,
                       long expectedMetadataEpoch,
                       RoutingPolicy policy,
                       long deadlineEpochMillis) {
        requireArgument(command, "command");
        requireArgument(policy, "policy");
        if (deadlineEpochMillis < 0) {
            throw new IllegalArgumentException("deadlineEpochMillis must be >= 0");
        }
        var operation = operationName(policy);
        requireBeforeDeadline(deadlineEpochMillis, operation);
        var currentMetadataEpoch = routeCache.metadataEpoch();
        if (expectedMetadataEpoch != currentMetadataEpoch) {
            var direction = expectedMetadataEpoch > currentMetadataEpoch ? "future" : "stale";
            throw new PartitionReadRouteException(
                    PartitionReadFailureKind.STALE_ROUTE,
                    direction + " read route metadata epoch " + expectedMetadataEpoch + "; current epoch is "
                            + currentMetadataEpoch
            );
        }

        var target = readRouter.routeReadAtEpoch(expectedMetadataEpoch, command, policy);
        var payload = PartitionPointReadCommandCodec.encode(command);
        var stablePayload = Arrays.copyOf(payload, payload.length);
        CompletableFuture<byte[]> query = null;
        try {
            var client = resolveClient(target, operation);
            try {
                query = query(client, policy, stablePayload);
            } catch (RatisFeatureUnavailableException e) {
                throw new PartitionReadRouteException(
                        PartitionReadFailureKind.QUERY_FAILED,
                        operation + " is unsupported",
                        e
                );
            } catch (RatisProcessAdapterRpcException e) {
                throw adapterRpcFailure(operation, e);
            } catch (RuntimeException e) {
                throw queryFailure(operation, e);
            }
            if (query == null) {
                throw queryFailed(operation + " returned null query future");
            }
            var result = query.get(remainingMillis(deadlineEpochMillis), TimeUnit.MILLISECONDS);
            return result == null ? null : Arrays.copyOf(result, result.length);
        } catch (RatisFeatureUnavailableException e) {
            throw new PartitionReadRouteException(
                    PartitionReadFailureKind.QUERY_FAILED,
                    operation + " is unsupported",
                    e
            );
        } catch (RatisProcessAdapterRpcException e) {
            throw adapterRpcFailure(operation, e);
        } catch (PartitionReadRouteException e) {
            throw e;
        } catch (RuntimeException e) {
            throw queryFailure(operation, e);
        } catch (TimeoutException e) {
            cancelQueryWithSuppressed(query, e);
            throw new PartitionReadRouteException(
                    PartitionReadFailureKind.TIMEOUT,
                    operation + " timed out",
                    e
            );
        } catch (InterruptedException e) {
            cancelQueryWithSuppressed(query, e);
            Thread.currentThread().interrupt();
            throw new PartitionReadRouteException(
                    PartitionReadFailureKind.TIMEOUT,
                    operation + " interrupted",
                    e
            );
        } catch (ExecutionException e) {
            throw queryFailure(operation, e.getCause());
        }
    }

    private RatisPartitionClient resolveClient(RouteTarget target, String operation) {
        var client = ratisClientResolver.resolve(target);
        if (client == null) {
            throw queryFailed(operation + " resolver returned null Ratis client");
        }
        return client;
    }

    private CompletableFuture<byte[]> query(RatisPartitionClient client,
                                            RoutingPolicy policy,
                                            byte[] stablePayload) {
        var request = Arrays.copyOf(stablePayload, stablePayload.length);
        return switch (policy.consistency()) {
            case LINEARIZABLE -> client.queryLinearizable(request);
            case BOUNDED_STALENESS, FOLLOWER -> client.queryStale(request, policy.minAppliedIndex());
            case LOCAL -> client.queryLocal(request);
        };
    }

    private static void cancelQueryWithSuppressed(CompletableFuture<?> query, Throwable failure) {
        if (query != null) {
            cancelWithSuppressed(query, failure);
        }
    }

    private static PartitionReadRouteException queryFailed(String message) {
        return new PartitionReadRouteException(PartitionReadFailureKind.QUERY_FAILED, message);
    }

    private static PartitionReadRouteException queryFailed(String message, Throwable cause) {
        return new PartitionReadRouteException(PartitionReadFailureKind.QUERY_FAILED, message, cause);
    }

    private static PartitionReadRouteException queryFailure(String operation, Throwable failure) {
        var cause = unwrap(failure);
        if (cause instanceof RatisFeatureUnavailableException unavailableException) {
            return new PartitionReadRouteException(
                    PartitionReadFailureKind.QUERY_FAILED,
                    operation + " is unsupported",
                    unavailableException
            );
        }
        if (cause instanceof RatisProcessAdapterRpcException adapterRpcException) {
            return adapterRpcFailure(operation, adapterRpcException);
        }
        if (cause instanceof RuntimeException runtimeException) {
            return queryFailed(operation + " failed", runtimeException);
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new PartitionReadRouteException(
                PartitionReadFailureKind.QUERY_FAILED,
                operation + " failed",
                cause
        );
    }

    private static PartitionReadRouteException adapterRpcFailure(String operation,
                                                                 RatisProcessAdapterRpcException cause) {
        return new PartitionReadRouteException(
                PartitionReadFailureKind.QUERY_FAILED,
                operation + " failed with adapter error " + cause.errorType() + ": " + cause.getMessage(),
                cause
        );
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException completionException && completionException.getCause() != null) {
            return unwrap(completionException.getCause());
        }
        return failure;
    }

    private static String operationName(RoutingPolicy policy) {
        return switch (policy.consistency()) {
            case LINEARIZABLE -> "linearizable point read";
            case BOUNDED_STALENESS -> "bounded-staleness point read";
            case FOLLOWER -> "follower point read";
            case LOCAL -> "local point read";
        };
    }

    private void requireBeforeDeadline(long deadlineEpochMillis, String operation) {
        if (deadlineExpired(deadlineEpochMillis)) {
            throw new PartitionReadRouteException(
                    PartitionReadFailureKind.DEADLINE_EXPIRED,
                    operation + " deadline expired before routing"
            );
        }
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

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    @FunctionalInterface
    public interface RatisClientResolver {

        RatisPartitionClient resolve(RouteTarget target);
    }
}
