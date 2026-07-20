package it.cavallium.rockserver.core.cluster.client;

import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommandCodec;
import it.cavallium.rockserver.core.replication.ratis.RatisFeatureUnavailableException;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.LongSupplier;

import static it.cavallium.rockserver.core.common.FutureUtils.cancelWithSuppressed;

import org.jetbrains.annotations.Nullable;

public final class PartitionRangeReadSubmitter {

    private static final PartitionRangeScanLimits DEFAULT_SCAN_LIMITS = new PartitionRangeScanLimits(16, 16 * 1024 * 1024);

    private final PartitionRouteCache routeCache;
    private final PartitionRangeReadRouter readRouter;
    private final RatisClientResolver ratisClientResolver;
    private final LongSupplier currentTimeMillis;
    private final PartitionRangeScanLimits scanLimits;

    public PartitionRangeReadSubmitter(PartitionRouteCache routeCache,
                                       RatisClientResolver ratisClientResolver,
                                       LongSupplier currentTimeMillis) {
        this(routeCache, ratisClientResolver, currentTimeMillis, DEFAULT_SCAN_LIMITS);
    }

    public PartitionRangeReadSubmitter(PartitionRouteCache routeCache,
                                       RatisClientResolver ratisClientResolver,
                                       LongSupplier currentTimeMillis,
                                       PartitionRangeScanLimits scanLimits) {
        this.routeCache = requireArgument(routeCache, "routeCache");
        this.readRouter = new PartitionRangeReadRouter(routeCache);
        this.ratisClientResolver = requireArgument(ratisClientResolver, "ratisClientResolver");
        this.currentTimeMillis = requireArgument(currentTimeMillis, "currentTimeMillis");
        this.scanLimits = requireArgument(scanLimits, "scanLimits");
    }

    public byte[] scanLinearizable(PartitionRangeReadCommand command,
                                   long expectedMetadataEpoch,
                                   long deadlineEpochMillis) {
        validateRequest(command, deadlineEpochMillis);
        requireCurrentEpoch(expectedMetadataEpoch);

        var target = readRouter.routeLinearizableScanAtEpoch(expectedMetadataEpoch, command);
        return queryLinearizable(target, command, deadlineEpochMillis);
    }

    public List<PartitionRangeScanResult> scanLinearizableAcrossPartitions(PartitionRangeReadCommand command,
                                                                           long expectedMetadataEpoch,
                                                                           long deadlineEpochMillis) {
        validateRequest(command, deadlineEpochMillis);
        requireCurrentEpoch(expectedMetadataEpoch);

        var routes = readRouter.routeLinearizableScanSegmentsAtEpoch(expectedMetadataEpoch, command);
        requireWithinFanoutLimit(routes.size());
        var results = new ArrayList<PartitionRangeScanResult>(routes.size());
        for (var route : routes) {
            requireBeforeDeadline(deadlineEpochMillis, "before segment dispatch");
            requireCurrentEpoch(expectedMetadataEpoch);
            results.add(new PartitionRangeScanResult(
                    route.target(),
                    route.command(),
                    queryLinearizable(route.target(), route.command(), deadlineEpochMillis)
            ));
        }
        return List.copyOf(results);
    }

    public PartitionRangeScanCursor openCursor(PartitionRangeReadCommand command, long expectedMetadataEpoch) {
        requireArgument(command, "command");
        requireCurrentEpoch(expectedMetadataEpoch);
        readRouter.routeLinearizableScanSegmentsAtEpoch(expectedMetadataEpoch, command);
        return new PartitionRangeScanCursor(expectedMetadataEpoch, command, 0);
    }

    public PartitionRangeScanPage scanLinearizablePage(PartitionRangeScanCursor cursor,
                                                       int maxSegments,
                                                       long deadlineEpochMillis) {
        requireArgument(cursor, "cursor");
        if (maxSegments <= 0) {
            throw new IllegalArgumentException("maxSegments must be > 0");
        }
        validateRequest(cursor.command(), deadlineEpochMillis);
        requireCurrentEpoch(cursor.metadataEpoch());

        var routes = readRouter.routeLinearizableScanSegmentsAtEpoch(cursor.metadataEpoch(), cursor.command());
        if (cursor.nextSegmentIndex() > routes.size()) {
            throw new IllegalArgumentException("cursor next segment index " + cursor.nextSegmentIndex()
                    + " is past planned segment count " + routes.size());
        }
        var allowedSegments = Math.min(maxSegments, scanLimits.maxSegmentsPerPage());
        var endExclusive = Math.min(routes.size(), cursor.nextSegmentIndex() + allowedSegments);
        var results = new ArrayList<PartitionRangeScanResult>(endExclusive - cursor.nextSegmentIndex());
        for (int i = cursor.nextSegmentIndex(); i < endExclusive; i++) {
            requireBeforeDeadline(deadlineEpochMillis, "before segment dispatch");
            requireCurrentEpoch(cursor.metadataEpoch());
            var route = routes.get(i);
            results.add(new PartitionRangeScanResult(
                    route.target(),
                    route.command(),
                    queryLinearizable(route.target(), route.command(), deadlineEpochMillis)
            ));
        }
        var nextCursor = endExclusive < routes.size()
                ? new PartitionRangeScanCursor(cursor.metadataEpoch(), cursor.command(), endExclusive)
                : null;
        return new PartitionRangeScanPage(results, nextCursor);
    }

    private void validateRequest(PartitionRangeReadCommand command, long deadlineEpochMillis) {
        requireArgument(command, "command");
        if (deadlineEpochMillis < 0) {
            throw new IllegalArgumentException("deadlineEpochMillis must be >= 0");
        }
        requireBeforeDeadline(deadlineEpochMillis, "before routing");
    }

    private void requireCurrentEpoch(long expectedMetadataEpoch) {
        var currentMetadataEpoch = routeCache.metadataEpoch();
        if (expectedMetadataEpoch != currentMetadataEpoch) {
            var direction = expectedMetadataEpoch > currentMetadataEpoch ? "future" : "stale";
            throw new PartitionReadRouteException(
                    PartitionReadFailureKind.STALE_ROUTE,
                    direction + " range read route metadata epoch " + expectedMetadataEpoch + "; current epoch is "
                            + currentMetadataEpoch
            );
        }
    }

    private void requireWithinFanoutLimit(int segmentCount) {
        if (segmentCount > scanLimits.maxSegmentsPerPage()) {
            throw new PartitionReadRouteException(
                    PartitionReadFailureKind.RESOURCE_LIMIT_EXCEEDED,
                    "range scan touches " + segmentCount + " partitions but the configured page fanout limit is "
                            + scanLimits.maxSegmentsPerPage()
            );
        }
    }

    @SuppressWarnings("resource")
    private byte[] queryLinearizable(RouteTarget target,
                                     PartitionRangeReadCommand command,
                                     long deadlineEpochMillis) {
        var payload = PartitionRangeReadCommandCodec.encode(command);
        var stablePayload = Arrays.copyOf(payload, payload.length);
        CompletableFuture<byte[]> query = null;
        try {
            var client = resolveClient(target);
            try {
                query = client.queryLinearizable(Arrays.copyOf(stablePayload, stablePayload.length));
            } catch (RatisFeatureUnavailableException e) {
                throw unsupportedRangeScan(e);
            } catch (RatisProcessAdapterRpcException e) {
                throw adapterRpcFailure(e);
            } catch (RuntimeException e) {
                throw queryFailure(e);
            }
            if (query == null) {
                throw queryFailed("linearizable range scan returned null query future");
            }
            var result = query.get(remainingMillis(deadlineEpochMillis), TimeUnit.MILLISECONDS);
            if (result != null && result.length > scanLimits.maxResponseBytesPerSegment()) {
                throw new PartitionReadRouteException(
                        PartitionReadFailureKind.RESOURCE_LIMIT_EXCEEDED,
                        "range scan segment response has " + result.length
                                + " bytes but the configured limit is "
                                + scanLimits.maxResponseBytesPerSegment()
                );
            }
            return result == null ? null : Arrays.copyOf(result, result.length);
        } catch (TimeoutException e) {
            cancelWithSuppressed(query, e);
            throw new PartitionReadRouteException(
                    PartitionReadFailureKind.TIMEOUT,
                    "linearizable range scan timed out",
                    e
            );
        } catch (InterruptedException e) {
            cancelWithSuppressed(query, e);
            Thread.currentThread().interrupt();
            throw new PartitionReadRouteException(
                    PartitionReadFailureKind.TIMEOUT,
                    "linearizable range scan interrupted",
                    e
            );
        } catch (PartitionReadRouteException e) {
            throw e;
        } catch (RatisFeatureUnavailableException e) {
            throw unsupportedRangeScan(e);
        } catch (RatisProcessAdapterRpcException e) {
            throw adapterRpcFailure(e);
        } catch (RuntimeException e) {
            throw queryFailure(e);
        } catch (ExecutionException e) {
            throw queryFailure(e.getCause());
        }
    }

    private RatisPartitionClient resolveClient(RouteTarget target) {
        var client = ratisClientResolver.resolve(target);
        if (client == null) {
            throw queryFailed("linearizable range scan resolver returned null Ratis client");
        }
        return client;
    }

    private static PartitionReadRouteException unsupportedRangeScan(RatisFeatureUnavailableException cause) {
        return new PartitionReadRouteException(
                PartitionReadFailureKind.QUERY_FAILED,
                "linearizable range scan is unsupported",
                cause
        );
    }

    private static PartitionReadRouteException queryFailed(String message) {
        return new PartitionReadRouteException(PartitionReadFailureKind.QUERY_FAILED, message);
    }

    private static PartitionReadRouteException queryFailed(Throwable cause) {
        return new PartitionReadRouteException(
                PartitionReadFailureKind.QUERY_FAILED,
                "linearizable range scan failed",
                cause
        );
    }

    private static PartitionReadRouteException queryFailure(Throwable failure) {
        var cause = unwrap(failure);
        if (cause instanceof RatisFeatureUnavailableException unavailableException) {
            return unsupportedRangeScan(unavailableException);
        }
        if (cause instanceof RatisProcessAdapterRpcException adapterRpcException) {
            return adapterRpcFailure(adapterRpcException);
        }
        if (cause instanceof RuntimeException runtimeException) {
            return queryFailed(runtimeException);
        }
        if (cause instanceof Error error) {
            throw error;
        }
        return new PartitionReadRouteException(
                PartitionReadFailureKind.QUERY_FAILED,
                "linearizable range scan failed",
                cause
        );
    }

    private static PartitionReadRouteException adapterRpcFailure(RatisProcessAdapterRpcException cause) {
        return new PartitionReadRouteException(
                PartitionReadFailureKind.QUERY_FAILED,
                "linearizable range scan failed with adapter error " + cause.errorType() + ": " + cause.getMessage(),
                cause
        );
    }

    private static Throwable unwrap(Throwable failure) {
        if (failure instanceof CompletionException completionException && completionException.getCause() != null) {
            return unwrap(completionException.getCause());
        }
        return failure;
    }

    private void requireBeforeDeadline(long deadlineEpochMillis, String phase) {
        if (deadlineExpired(deadlineEpochMillis)) {
            throw new PartitionReadRouteException(
                    PartitionReadFailureKind.DEADLINE_EXPIRED,
                    "linearizable range scan deadline expired " + phase
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

    private static List<PartitionRangeScanResult> copyResults(List<PartitionRangeScanResult> results) {
        requireArgument(results, "results");
        for (int i = 0; i < results.size(); i++) {
            if (results.get(i) == null) {
                throw new IllegalArgumentException("results[" + i + "] must not be null");
            }
        }
        return List.copyOf(results);
    }

    @FunctionalInterface
    public interface RatisClientResolver {

        RatisPartitionClient resolve(RouteTarget target);
    }

    public record PartitionRangeScanLimits(int maxSegmentsPerPage,
                                           int maxResponseBytesPerSegment) {

        public PartitionRangeScanLimits {
            if (maxSegmentsPerPage <= 0) {
                throw new IllegalArgumentException("maxSegmentsPerPage must be > 0");
            }
            if (maxResponseBytesPerSegment <= 0) {
                throw new IllegalArgumentException("maxResponseBytesPerSegment must be > 0");
            }
        }
    }

    public record PartitionRangeScanResult(RouteTarget target,
                                           PartitionRangeReadCommand command,
                                           byte[] response) {

        public PartitionRangeScanResult {
            requireArgument(target, "target");
            requireArgument(command, "command");
            response = response == null ? null : Arrays.copyOf(response, response.length);
        }

        @Override
        public byte[] response() {
            return response == null ? null : Arrays.copyOf(response, response.length);
        }
    }

    public record PartitionRangeScanPage(List<PartitionRangeScanResult> results,
                                         @Nullable PartitionRangeScanCursor nextCursor) {

        public PartitionRangeScanPage {
            results = copyResults(results);
        }
    }
}
