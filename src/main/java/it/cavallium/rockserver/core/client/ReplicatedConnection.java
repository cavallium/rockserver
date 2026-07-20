package it.cavallium.rockserver.core.client;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClientId;
import it.cavallium.rockserver.core.cluster.ClientRequestId;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.ClusterNodeMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandClassifier;
import it.cavallium.rockserver.core.cluster.ReplicatedCommandKind;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionRangeReadResponseCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseCodec;
import it.cavallium.rockserver.core.cluster.command.PartitionWriteResponseMode;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationCommandEncoder;
import it.cavallium.rockserver.core.cluster.command.ReplicatedMutationCommandEncoder.EncodedPartitionWrite;
import it.cavallium.rockserver.core.cluster.client.PartitionPointReadSubmitter;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeReadSubmitter;
import it.cavallium.rockserver.core.cluster.client.PartitionRangeScanCursor;
import it.cavallium.rockserver.core.cluster.client.PartitionReadRouteException;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteFailureKind;
import it.cavallium.rockserver.core.cluster.client.PartitionRouteCache;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteRouteException;
import it.cavallium.rockserver.core.cluster.client.PartitionWriteSubmitter;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommand;
import it.cavallium.rockserver.core.cluster.metadata.MetadataCommandSubmitter;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeMetadata;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeKind;
import it.cavallium.rockserver.core.cluster.metadata.SchemaChangeStatus;
import it.cavallium.rockserver.core.cluster.workflow.PartitionMembershipChangePlan;
import it.cavallium.rockserver.core.cluster.workflow.PartitionCreationWorkflowPlanner;
import it.cavallium.rockserver.core.cluster.workflow.PartitionMergePlan;
import it.cavallium.rockserver.core.cluster.workflow.PartitionSplitPlan;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.FirstAndLast;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;

import java.io.IOException;
import java.net.URI;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.SynchronousQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.LongSupplier;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import static it.cavallium.rockserver.core.common.FutureUtils.submitCancellationAware;
import static it.cavallium.rockserver.core.common.FutureUtils.unwrapCompletionException;

import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public final class ReplicatedConnection extends BaseConnection {

    private static final int DEFAULT_MAX_WRITE_ATTEMPTS = 3;
    private static final int DEFAULT_MAX_CONCURRENT_ASYNC_REQUESTS = 64;
    private static final int DEFAULT_MAX_RANGE_READ_ENTRIES_PER_SEGMENT = 16_384;
    private static final int DEFAULT_MAX_RANGE_READ_SEGMENTS_PER_STREAM_PAGE = 1;
    private static final int ASYNC_EXECUTOR_KEEP_ALIVE_SECONDS = 30;
    private static final int ASYNC_EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 5;
    private static final LongSupplier SYSTEM_MILLIS = System::currentTimeMillis;

    private final String name;
    private final PartitionRouteCache routeCache;
    private final PartitionWriteSubmitter writeSubmitter;
    private final PartitionPointReadSubmitter pointReadSubmitter;
    private final PartitionRangeReadSubmitter rangeReadSubmitter;
    private final ClientId clientId;
    private final LongSupplier currentTimeMillis;
    private final AtomicLong requestSequence = new AtomicLong();
    private final AutoCloseable closeable;
    private final Consumer<ClusterMetadata> metadataUpdateListener;
    private final MetadataCommandSubmitter metadataSubmitter;
    private final ThreadPoolExecutor asyncExecutor = asyncExecutor();
    private final AtomicBoolean closed = new AtomicBoolean();
    private boolean asyncExecutorShutdown;
    private boolean ownedResourceClosed;
    private boolean asyncExecutorTerminated;
    private final PartitionCreationWorkflowPlanner partitionCreationPlanner = new PartitionCreationWorkflowPlanner();
    private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
        @Override
        public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
            return executeSync(req);
        }
    };
    private final RocksDBAsyncAPI asyncApi = new RocksDBAsyncAPI() {
        @SuppressWarnings("unchecked")
        @Override
        public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> req) {
            if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle<?>) {
                return (RA) executeAsync(req);
            }
            if (req instanceof RocksDBAPICommand.RocksDBAPICommandStream<?>) {
                return (RA) executeAsyncStream(req);
            }
            return RocksDBAsyncAPI.super.requestAsync(req);
        }
    };

    public ReplicatedConnection(String name,
                                ClusterMetadata metadata,
                                PartitionWriteSubmitter.LogClientResolver writeResolver,
                                PartitionPointReadSubmitter.RatisClientResolver pointReadResolver,
                                PartitionRangeReadSubmitter.RatisClientResolver rangeReadResolver) {
        this(name, metadata, writeResolver, pointReadResolver, rangeReadResolver, null);
    }

    public ReplicatedConnection(String name,
                                ClusterMetadata metadata,
                                PartitionWriteSubmitter.LogClientResolver writeResolver,
                                PartitionPointReadSubmitter.RatisClientResolver pointReadResolver,
                                PartitionRangeReadSubmitter.RatisClientResolver rangeReadResolver,
                                AutoCloseable closeable) {
        this(name, metadata, writeResolver, pointReadResolver, rangeReadResolver, closeable, ignored -> {
        });
    }

    public ReplicatedConnection(String name,
                                ClusterMetadata metadata,
                                PartitionWriteSubmitter.LogClientResolver writeResolver,
                                PartitionPointReadSubmitter.RatisClientResolver pointReadResolver,
                                PartitionRangeReadSubmitter.RatisClientResolver rangeReadResolver,
                                AutoCloseable closeable,
                                Consumer<ClusterMetadata> metadataUpdateListener) {
        this(name,
                metadata,
                writeResolver,
                pointReadResolver,
                rangeReadResolver,
                closeable,
                ClientId.random(),
                DEFAULT_MAX_WRITE_ATTEMPTS,
                SYSTEM_MILLIS,
                metadataUpdateListener,
                null);
    }

    public ReplicatedConnection(String name,
                                ClusterMetadata metadata,
                                PartitionWriteSubmitter.LogClientResolver writeResolver,
                                PartitionPointReadSubmitter.RatisClientResolver pointReadResolver,
                                PartitionRangeReadSubmitter.RatisClientResolver rangeReadResolver,
                                AutoCloseable closeable,
                                Consumer<ClusterMetadata> metadataUpdateListener,
                                MetadataCommandSubmitter metadataSubmitter) {
        this(name,
                metadata,
                writeResolver,
                pointReadResolver,
                rangeReadResolver,
                closeable,
                ClientId.random(),
                DEFAULT_MAX_WRITE_ATTEMPTS,
                SYSTEM_MILLIS,
                metadataUpdateListener,
                metadataSubmitter);
    }

    public ReplicatedConnection(String name,
                                ClusterMetadata metadata,
                                PartitionWriteSubmitter.LogClientResolver writeResolver,
                                PartitionPointReadSubmitter.RatisClientResolver pointReadResolver,
                                PartitionRangeReadSubmitter.RatisClientResolver rangeReadResolver,
                                AutoCloseable closeable,
                                ClientId clientId,
                                int maxWriteAttempts,
                                LongSupplier currentTimeMillis) {
        this(name, metadata, writeResolver, pointReadResolver, rangeReadResolver, closeable, clientId,
                maxWriteAttempts, currentTimeMillis, ignored -> {
                }, null);
    }

    public ReplicatedConnection(String name,
                                ClusterMetadata metadata,
                                PartitionWriteSubmitter.LogClientResolver writeResolver,
                                PartitionPointReadSubmitter.RatisClientResolver pointReadResolver,
                                PartitionRangeReadSubmitter.RatisClientResolver rangeReadResolver,
                                AutoCloseable closeable,
                                ClientId clientId,
                                int maxWriteAttempts,
                                LongSupplier currentTimeMillis,
                                Consumer<ClusterMetadata> metadataUpdateListener) {
        this(name, metadata, writeResolver, pointReadResolver, rangeReadResolver, closeable, clientId,
                maxWriteAttempts, currentTimeMillis, metadataUpdateListener, null);
    }

    public ReplicatedConnection(String name,
                                ClusterMetadata metadata,
                                PartitionWriteSubmitter.LogClientResolver writeResolver,
                                PartitionPointReadSubmitter.RatisClientResolver pointReadResolver,
                                PartitionRangeReadSubmitter.RatisClientResolver rangeReadResolver,
                                AutoCloseable closeable,
                                ClientId clientId,
                                int maxWriteAttempts,
                                LongSupplier currentTimeMillis,
                                Consumer<ClusterMetadata> metadataUpdateListener,
                                MetadataCommandSubmitter metadataSubmitter) {
        super(name);
        this.name = name == null ? "replicated" : name;
        this.routeCache = new PartitionRouteCache(requireArgument(metadata, "metadata"));
        this.clientId = requireArgument(clientId, "clientId");
        this.currentTimeMillis = requireArgument(currentTimeMillis, "currentTimeMillis");
        this.writeSubmitter = new PartitionWriteSubmitter(
                routeCache,
                requireArgument(writeResolver, "writeResolver"),
                maxWriteAttempts,
                this.currentTimeMillis
        );
        this.pointReadSubmitter = new PartitionPointReadSubmitter(
                routeCache,
                requireArgument(pointReadResolver, "pointReadResolver"),
                this.currentTimeMillis
        );
        this.rangeReadSubmitter = new PartitionRangeReadSubmitter(
                routeCache,
                requireArgument(rangeReadResolver, "rangeReadResolver"),
                this.currentTimeMillis
        );
        this.closeable = closeable;
        this.metadataUpdateListener = requireArgument(metadataUpdateListener, "metadataUpdateListener");
        this.metadataSubmitter = metadataSubmitter;
    }

    public synchronized void updateMetadata(ClusterMetadata metadata) {
        requireOpen();
        routeCache.updateMetadata(metadata, metadataUpdateListener);
    }

    public void registerNode(NodeId nodeId,
                             URI publicEndpoint,
                             URI ratisEndpoint,
                             String failureDomain) {
        requireOpen();
        requireMetadataSubmitter(MetadataCommand.RegisterNode.class);
        var metadata = routeCache.metadata();
        submitMetadataCommand(new MetadataCommand.RegisterNode(
                metadata.epoch(),
                nodeId,
                publicEndpoint,
                ratisEndpoint,
                failureDomain
        ));
    }

    public PartitionDescriptor createPartition(ShardId partitionId,
                                               Buf startKeyInclusive,
                                               Buf endKeyExclusive,
                                               long partitionEpoch,
                                               String ratisGroupId,
                                               List<NodeId> voterNodeIds,
                                               NodeId leaderHint,
                                               Map<String, String> placementConstraints) {
        requireOpen();
        requireMetadataSubmitter(MetadataCommand.CreatePartition.class);
        var metadata = routeCache.metadata();
        var plan = partitionCreationPlanner.planCreatePartition(
                metadata,
                metadata.epoch(),
                partitionId,
                startKeyInclusive,
                endKeyExclusive,
                partitionEpoch,
                ratisGroupId,
                voterNodeIds,
                leaderHint,
                placementConstraints
        );
        submitMetadataCommand(plan.command());
        return plan.command().partition();
    }

    public PartitionDescriptor applyPartitionMembershipChange(PartitionMembershipChangePlan plan) {
        requireOpen();
        requireArgument(plan, "plan");
        requireMetadataSubmitter(MetadataCommand.ChangePartitionPeers.class);
        submitMetadataCommand(plan.command());
        return routeCache.metadata().requirePartition(plan.command().partitionId());
    }

    @SuppressWarnings("UnusedReturnValue")
    public PartitionSplitPlan applyPartitionSplit(PartitionSplitPlan plan) {
        requireOpen();
        requireArgument(plan, "plan");
        requireMetadataSubmitter(MetadataCommand.SplitPartition.class);
        submitTopologyCommandWithCdcTranslation(plan.splitCommand(), plan.cdcTokenTranslationCommand());
        return plan;
    }

    @SuppressWarnings("UnusedReturnValue")
    public PartitionMergePlan applyPartitionMerge(PartitionMergePlan plan) {
        requireOpen();
        requireArgument(plan, "plan");
        requireMetadataSubmitter(MetadataCommand.MergePartitions.class);
        submitTopologyCommandWithCdcTranslation(plan.mergeCommand(), plan.cdcTokenTranslationCommand());
        return plan;
    }

    @Override
    public URI getUrl() {
        return URI.create("rockserver-replicated:///" + name);
    }

    @Override
    public RocksDBSyncAPI getSyncApi() {
        return syncApi;
    }

    @Override
    public RocksDBAsyncAPI getAsyncApi() {
        return asyncApi;
    }

    @Override
    public synchronized void close() throws IOException {
        closed.set(true);
        Throwable failure = null;
        if (!asyncExecutorShutdown) {
            try {
                asyncExecutor.shutdownNow();
                asyncExecutorShutdown = true;
            } catch (RuntimeException | Error e) {
                failure = e;
            }
        }
        if (!ownedResourceClosed) {
            failure = merge(failure, closeOwnedResource());
        }
        if (asyncExecutorShutdown && !asyncExecutorTerminated) {
            var interrupted = false;
            try {
                var deadlineNanos = System.nanoTime()
                        + TimeUnit.SECONDS.toNanos(ASYNC_EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS);
                while (true) {
                    try {
                        var remainingNanos = deadlineNanos - System.nanoTime();
                        if (remainingNanos <= 0) {
                            failure = merge(failure, asyncRequestsDidNotStopFailure());
                            break;
                        }
                        if (asyncExecutor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                            asyncExecutorTerminated = true;
                            break;
                        }
                        failure = merge(failure, asyncRequestsDidNotStopFailure());
                        break;
                    } catch (InterruptedException _) {
                        interrupted = true;
                    }
                }
            } finally {
                if (interrupted) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        if (failure != null) {
            throwFailure(failure);
        }
    }

    private static IOException asyncRequestsDidNotStopFailure() {
        return new IOException("Replicated connection async requests did not stop within "
                + ASYNC_EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS + " seconds");
    }

    private <R, RS, RA> CompletableFuture<RS> executeAsync(RocksDBAPICommand<R, RS, RA> req) {
        var request = requireArgument(req, "req");
        if (closed.get()) {
            return CompletableFuture.failedFuture(closedException());
        }
        var kind = ReplicatedCommandClassifier.classify(request);
        var unsupportedFailure = unsupportedPublicRoutingFailure(request, kind);
        if (unsupportedFailure != null) {
            return CompletableFuture.failedFuture(unsupportedFailure);
        }
        var invalidRequest = invalidAsyncAdmissionRequest(request);
        if (invalidRequest != null) {
            return CompletableFuture.failedFuture(invalidRequest);
        }
        var immediateResult = executeImmediateAsync(request);
        if (immediateResult != null) {
            return immediateResult;
        }
        try {
            return submitCancellationAware(asyncExecutor, () -> executeSync(request));
        } catch (RejectedExecutionException e) {
            if (closed.get()) {
                return CompletableFuture.failedFuture(closedException());
            }
            return CompletableFuture.failedFuture(new IllegalStateException(
                    "Replicated connection async request executor is saturated", e));
        }
    }

    @SuppressWarnings("unchecked")
    private <R, RS, RA> CompletableFuture<RS> executeImmediateAsync(RocksDBAPICommand<R, RS, RA> req) {
        try {
            if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.GetColumnId(String columnName)) {
                return CompletableFuture.completedFuture((RS) routeCache.metadata()
                        .findColumn(columnName)
                        .map(ClusterColumnMetadata::columnId)
                        .orElseThrow(() -> RocksDBException.of(
                                RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND,
                                "unknown column: " + columnName
                        )));
            }
            if (req instanceof RocksDBAPICommand.GetAllColumnDefinitions) {
                return CompletableFuture.completedFuture((RS) columnDefinitions());
            }
            if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CheckMergeOperator checkMergeOperator) {
                return CompletableFuture.completedFuture((RS) executeCheckMergeOperator(checkMergeOperator));
            }
            if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Get<?> get
                    && get.requestType() instanceof RequestType.RequestNothing<?>) {
                return CompletableFuture.completedFuture(null);
            }
        } catch (Throwable failure) {
            return CompletableFuture.failedFuture(failure);
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private <R, RS, RA> Publisher<R> executeAsyncStream(RocksDBAPICommand<R, RS, RA> req) {
        var request = requireArgument(req, "req");
        if (closed.get()) {
            return Flux.error(closedException());
        }
        var kind = ReplicatedCommandClassifier.classify(request);
        var unsupportedFailure = unsupportedPublicRoutingFailure(request, kind);
        if (unsupportedFailure != null) {
            return Flux.error(unsupportedFailure);
        }
        var invalidRequest = invalidAsyncAdmissionRequest(request);
        if (invalidRequest != null) {
            return Flux.error(invalidRequest);
        }
        return Flux.create(sink -> {
            if (closed.get()) {
                sink.error(closedException());
                return;
            }
            Future<?> invocation;
            try {
                invocation = asyncExecutor.submit(() -> {
                    try (var stream = (Stream<R>) executeSync(request)) {
                        var iterator = stream.iterator();
                        while (!sink.isCancelled() && iterator.hasNext()) {
                            sink.next(iterator.next());
                        }
                        if (!sink.isCancelled()) {
                            sink.complete();
                        }
                    } catch (Throwable e) {
                        if (!sink.isCancelled()) {
                            sink.error(e);
                        }
                    }
                });
            } catch (RejectedExecutionException e) {
                if (closed.get()) {
                    sink.error(closedException());
                } else {
                    sink.error(new IllegalStateException(
                            "Replicated connection async request executor is saturated", e));
                }
                return;
            } catch (RuntimeException | Error e) {
                sink.error(e);
                return;
            }
            sink.onCancel(() -> invocation.cancel(true));
        });
    }

    private static RuntimeException invalidAsyncAdmissionRequest(RocksDBAPICommand<?, ?, ?> req) {
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?> reduceRange
                && reduceRange.timeoutMs() < 0) {
            return new IllegalArgumentException("timeoutMs must be >= 0");
        }
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> getRange
                && getRange.timeoutMs() < 0) {
            return new IllegalArgumentException("timeoutMs must be >= 0");
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private <R, RS, RA> RS executeSync(RocksDBAPICommand<R, RS, RA> req) {
        requireOpen();
        requireArgument(req, "req");
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.UploadMergeOperator) {
            throw unsupportedMergeOperatorUpload();
        }
        var kind = ReplicatedCommandClassifier.classify(req);
        var unsupportedFailure = unsupportedPublicRoutingFailure(req, kind);
        if (unsupportedFailure != null) {
            throw unsupportedFailure;
        }
        return switch (kind) {
            case REPLICATED_WRITE -> (RS) executeWrite(req);
            case ROUTED_READ -> (RS) executeRead(req);
            case REPLICATED_METADATA -> (RS) executeMetadata(req);
            case LOCAL_SESSION, LOCAL_ADMIN, CDC, UNSUPPORTED ->
                    throw new IllegalStateException("Unsupported replicated command kind reached sync execution: " + kind);
        };
    }

    private Object executeMetadata(RocksDBAPICommand<?, ?, ?> req) {
        requireMetadataSubmitter(req.getClass());
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn createColumn) {
            return executeCreateColumn(createColumn);
        }
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumn deleteColumn) {
            executeDeleteColumn(deleteColumn);
            return null;
        }
        throw notImplemented("replicated metadata command is not supported by the current metadata workflow: "
                + req.getClass().getName());
    }

    private static RocksDBException unsupportedMergeOperatorUpload() {
        return notImplemented(
                "replicated merge-operator artifact upload requires replicated artifact distribution "
                        + "and activation preflight");
    }

    private static RocksDBException unsupportedPublicRoutingFailure(RocksDBAPICommand<?, ?, ?> req,
                                                                    ReplicatedCommandKind kind) {
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.UploadMergeOperator) {
            return unsupportedMergeOperatorUpload();
        }
        return switch (kind) {
            case LOCAL_SESSION, LOCAL_ADMIN, CDC, UNSUPPORTED -> notImplemented(
                    "command is not supported by replicated public API routing: " + req.getClass().getName());
            case REPLICATED_WRITE, ROUTED_READ, REPLICATED_METADATA -> null;
        };
    }

    private long executeCreateColumn(RocksDBAPICommand.RocksDBAPICommandSingle.CreateColumn createColumn) {
        var name = requireCreateColumnName(createColumn.name());
        var schema = requireCreateColumnSchema(createColumn.schema());
        var metadata = routeCache.metadata();
        if (metadata.findColumn(name).isPresent()) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.COLUMN_EXISTS,
                    "column already exists: " + name
            );
        }
        var columnId = nextColumnId(metadata);
        MetadataCommand.CreateColumn command;
        try {
            command = new MetadataCommand.CreateColumn(
                    metadata.epoch(),
                    new ClusterColumnMetadata(columnId, name, schema, 0)
            );
        } catch (IllegalArgumentException e) {
            throw columnCreateFail(e.getMessage(), e);
        }
        submitMetadataCommand(command);
        return columnId;
    }

    private static String requireCreateColumnName(String name) {
        if (name == null) {
            throw columnCreateFail("column name must not be null");
        }
        if (name.isBlank()) {
            throw columnCreateFail("column name must not be blank");
        }
        if (name.indexOf('\r') >= 0 || name.indexOf('\n') >= 0) {
            throw columnCreateFail("column name must be a single line");
        }
        return name;
    }

    private static ColumnSchema requireCreateColumnSchema(ColumnSchema schema) {
        if (schema == null) {
            throw columnCreateFail("column schema must not be null");
        }
        return schema;
    }

    private static RocksDBException columnCreateFail(String message) {
        return RocksDBException.of(RocksDBException.RocksDBErrorType.COLUMN_CREATE_FAIL, message);
    }

    private static RocksDBException columnCreateFail(String message, RuntimeException cause) {
        return RocksDBException.of(RocksDBException.RocksDBErrorType.COLUMN_CREATE_FAIL, message, cause);
    }

    private void executeDeleteColumn(RocksDBAPICommand.RocksDBAPICommandSingle.DeleteColumn deleteColumn) {
        var metadata = routeCache.metadata();
        var column = metadata.findColumn(deleteColumn.columnId())
                .orElseThrow(() -> RocksDBException.of(
                        RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND,
                        "unknown column: " + deleteColumn.columnId()
                ));
        submitMetadataCommand(new MetadataCommand.DeleteColumn(
                metadata.epoch(),
                deleteColumn.columnId(),
                column.schemaEpoch()
        ));
    }

    private void submitMetadataCommand(MetadataCommand command) {
        refreshCommittedMetadata(submitMetadataCommandToLog(command));
    }

    private void submitTopologyCommandWithCdcTranslation(MetadataCommand topologyCommand,
                                                         MetadataCommand.RecordCdcTokenTranslation translationCommand) {
        requireInternalContract(topologyCommand, "topology metadata command");
        requireInternalContract(translationCommand, "CDC translation metadata command");
        submitMetadataCommandToLog(topologyCommand);
        var committedMetadata = submitMetadataCommandToLog(translationCommand);
        refreshCommittedMetadata(committedMetadata);
    }

    private ClusterMetadata submitMetadataCommandToLog(MetadataCommand command) {
        var metadataCommand = requireInternalContract(command, "metadata command");
        var submitter = requireMetadataSubmitter(metadataCommand.getClass());
        try {
            var committedMetadata = requireInternalContract(
                    submitter.submit(metadataCommand),
                    "metadata submitter committed metadata"
            );
            verifyCommittedMetadataCommand(metadataCommand, committedMetadata);
            return committedMetadata;
        } catch (RocksDBException e) {
            throw e;
        } catch (RuntimeException e) {
            throw mapMetadataCommandFailure(metadataCommand, e);
        }
    }

    private void refreshCommittedMetadata(ClusterMetadata committedMetadata) {
        try {
            updateMetadata(committedMetadata);
        } catch (RocksDBException e) {
            throw e;
        } catch (RuntimeException e) {
            throw commitFailed(e);
        }
    }

    private static void verifyCommittedMetadataCommand(MetadataCommand command,
                                                       ClusterMetadata committedMetadata) {
        requireCommittedEpoch(command, committedMetadata);
        switch (command) {
            case MetadataCommand.RegisterNode registerNode -> requireCommittedNode(registerNode, committedMetadata);
            case MetadataCommand.CreateColumn createColumn -> requireCommittedColumn(createColumn, committedMetadata);
            case MetadataCommand.DeleteColumn deleteColumn -> requireDeletedColumn(deleteColumn, committedMetadata);
            case MetadataCommand.CreatePartition createPartition -> requireCommittedPartition(
                    createPartition.kind().name(),
                    createPartition.partition(),
                    committedMetadata
            );
            case MetadataCommand.ChangePartitionPeers changePeers -> requireCommittedPeerChange(
                    changePeers,
                    committedMetadata
            );
            case MetadataCommand.UpdateColumnSchema updateColumnSchema -> requireCommittedSchemaUpdate(
                    updateColumnSchema,
                    committedMetadata
            );
            case MetadataCommand.SplitPartition splitPartition ->
                    requireCommittedSplit(splitPartition, committedMetadata);
            case MetadataCommand.MergePartitions mergePartitions -> requireCommittedMerge(mergePartitions,
                    committedMetadata);
            case MetadataCommand.PrepareSchemaChange prepareSchemaChange -> requireCommittedSchemaChange(
                    prepareSchemaChange.schemaChangeId(),
                    SchemaChangeMetadata.prepared(prepareSchemaChange),
                    committedMetadata
            );
            case MetadataCommand.ActivateSchemaChange activateSchemaChange -> requireCommittedSchemaChangeStatus(
                    activateSchemaChange.schemaChangeId(),
                    SchemaChangeStatus.ACTIVATED,
                    committedMetadata
            );
            case MetadataCommand.FenceSchemaChange fenceSchemaChange -> requireCommittedSchemaChangeStatus(
                    fenceSchemaChange.schemaChangeId(),
                    SchemaChangeStatus.FENCED,
                    committedMetadata
            );
            case MetadataCommand.RecordBackupManifest recordBackupManifest -> {
                var committed = committedMetadata.findBackupManifest(recordBackupManifest.manifest().backupId());
                if (committed.isEmpty() || !committed.get().equals(recordBackupManifest.manifest())) {
                    throw metadataCommitMismatch(command, "backup manifest was not recorded");
                }
            }
            case MetadataCommand.RecordCdcTokenTranslation recordCdcTokenTranslation -> {
                var translation = recordCdcTokenTranslation.translation();
                var committed = committedMetadata.findCdcTokenTranslation(translation.metadataEpoch());
                if (committed.isEmpty() || !committed.get().equals(translation)) {
                    throw metadataCommitMismatch(command, "CDC token translation was not recorded");
                }
            }
        }
    }

    private static void requireCommittedEpoch(MetadataCommand command,
                                              ClusterMetadata committedMetadata) {
        var minimumEpoch = command instanceof MetadataCommand.RecordCdcTokenTranslation
                ? command.expectedMetadataEpoch()
                : command.expectedMetadataEpoch() + 1;
        if (committedMetadata.epoch() < minimumEpoch) {
            throw metadataCommitMismatch(command,
                    "committed metadata epoch " + committedMetadata.epoch()
                            + " is before expected effect epoch " + minimumEpoch);
        }
    }

    private static void requireCommittedNode(MetadataCommand.RegisterNode command,
                                             ClusterMetadata committedMetadata) {
        var expected = new ClusterNodeMetadata(
                command.nodeId(),
                command.publicEndpoint(),
                command.ratisEndpoint(),
                command.failureDomain()
        );
        var committed = committedMetadata.findNode(command.nodeId());
        if (committed.isEmpty() || !committed.get().equals(expected)) {
            throw metadataCommitMismatch(command, "registered node was not committed");
        }
    }

    private static void requireCommittedColumn(MetadataCommand.CreateColumn command,
                                               ClusterMetadata committedMetadata) {
        var committed = committedMetadata.findColumn(command.column().columnId());
        if (committed.isEmpty() || !committed.get().equals(command.column())) {
            throw metadataCommitMismatch(command, "created column was not committed");
        }
    }

    private static void requireDeletedColumn(MetadataCommand.DeleteColumn command,
                                             ClusterMetadata committedMetadata) {
        if (committedMetadata.findColumn(command.columnId()).isPresent()) {
            throw metadataCommitMismatch(command, "deleted column is still present");
        }
    }

    private static void requireCommittedPartition(String commandKind,
                                                  PartitionDescriptor partition,
                                                  ClusterMetadata committedMetadata) {
        var committedPartition = committedMetadata.findPartition(partition.partitionId());
        if (committedPartition.isEmpty() || !committedPartition.get().equals(partition)) {
            throw metadataCommitMismatch(commandKind, "partition was not committed");
        }
        requireCommittedRouteGroup(commandKind, partition, committedMetadata);
    }

    private static void requireCommittedPeerChange(MetadataCommand.ChangePartitionPeers command,
                                                   ClusterMetadata committedMetadata) {
        var committedPartition = committedMetadata.findPartition(command.partitionId())
                .orElseThrow(() -> metadataCommitMismatch(command, "partition peer change is missing"));
        if (committedPartition.epoch() != command.expectedPartitionEpoch() + 1
                || !committedPartition.peers().equals(command.peers())
                || !Objects.equals(committedPartition.leaderHint(), command.leaderHint())) {
            throw metadataCommitMismatch(command, "partition peer change was not committed");
        }
        var committedGroup = committedMetadata.findGroup(command.partitionId())
                .orElseThrow(() -> metadataCommitMismatch(command, "route group peer change is missing"));
        if (!committedGroup.members().equals(command.peers())
                || !Objects.equals(committedGroup.leader(), command.leaderHint())) {
            throw metadataCommitMismatch(command, "route group peer change was not committed");
        }
    }

    private static void requireCommittedSchemaUpdate(MetadataCommand.UpdateColumnSchema command,
                                                     ClusterMetadata committedMetadata) {
        var committed = committedMetadata.findColumn(command.columnId())
                .orElseThrow(() -> metadataCommitMismatch(command, "updated column is missing"));
        if (committed.schemaEpoch() != command.newSchemaEpoch() || !committed.schema().equals(command.schema())) {
            throw metadataCommitMismatch(command, "column schema update was not committed");
        }
    }

    private static void requireCommittedSplit(MetadataCommand.SplitPartition command,
                                              ClusterMetadata committedMetadata) {
        if (committedMetadata.findPartition(command.sourcePartitionId()).isPresent()
                || committedMetadata.findGroup(command.sourcePartitionId()).isPresent()) {
            throw metadataCommitMismatch(command, "split source partition is still present");
        }
        requireCommittedPartition(command.kind().name(), command.left(), committedMetadata);
        requireCommittedPartition(command.kind().name(), command.right(), committedMetadata);
    }

    private static void requireCommittedMerge(MetadataCommand.MergePartitions command,
                                              ClusterMetadata committedMetadata) {
        if (committedMetadata.findPartition(command.leftPartitionId()).isPresent()
                || committedMetadata.findGroup(command.leftPartitionId()).isPresent()
                || committedMetadata.findPartition(command.rightPartitionId()).isPresent()
                || committedMetadata.findGroup(command.rightPartitionId()).isPresent()) {
            throw metadataCommitMismatch(command, "merge source partition is still present");
        }
        requireCommittedPartition(command.kind().name(), command.merged(), committedMetadata);
    }

    private static void requireCommittedSchemaChange(long schemaChangeId,
                                                     SchemaChangeMetadata expected,
                                                     ClusterMetadata committedMetadata) {
        var committed = committedMetadata.findSchemaChange(schemaChangeId);
        if (committed.isEmpty() || !committed.get().equals(expected)) {
            throw metadataCommitMismatch("PREPARE_SCHEMA_CHANGE", "schema change was not prepared");
        }
    }

    private static void requireCommittedSchemaChangeStatus(long schemaChangeId,
                                                           SchemaChangeStatus expectedStatus,
                                                           ClusterMetadata committedMetadata) {
        var committed = committedMetadata.findSchemaChange(schemaChangeId);
        if (committed.isEmpty() || committed.get().status() != expectedStatus) {
            throw metadataCommitMismatch(expectedStatus.name(), "schema change status was not committed");
        }
    }

    private static void requireCommittedRouteGroup(String commandKind,
                                                   PartitionDescriptor partition,
                                                   ClusterMetadata committedMetadata) {
        var expected = routeGroup(partition);
        var committed = committedMetadata.findGroup(partition.partitionId());
        if (committed.isEmpty() || !committed.get().equals(expected)) {
            throw metadataCommitMismatch(commandKind, "partition route group was not committed");
        }
    }

    private static ReplicaGroupMetadata routeGroup(PartitionDescriptor partition) {
        return new ReplicaGroupMetadata(
                partition.partitionId(),
                partition.epoch(),
                partition.leaderHint(),
                partition.peers(),
                partition.epoch()
        );
    }

    private static IllegalStateException metadataCommitMismatch(MetadataCommand command, String reason) {
        return metadataCommitMismatch(command.kind().name(), reason);
    }

    private static IllegalStateException metadataCommitMismatch(String commandKind, String reason) {
        return new IllegalStateException("committed metadata does not contain "
                + commandKind + " effect: " + reason);
    }

    private MetadataCommandSubmitter requireMetadataSubmitter(Class<?> commandType) {
        var requiredCommandType = requireInternalContract(commandType, "metadata command type");
        var submitter = metadataSubmitter;
        if (submitter == null) {
            throw notImplemented("replicated metadata commands require a configured metadata-group submitter: "
                    + requiredCommandType.getName());
        }
        return submitter;
    }

    private static RocksDBException mapMetadataCommandFailure(MetadataCommand command, RuntimeException failure) {
        var runtimeFailure = runtimeMetadataFailure(failure);
        if (runtimeFailure instanceof IllegalArgumentException) {
            var message = runtimeFailure.getMessage();
            if (command instanceof MetadataCommand.CreateColumn createColumn && isDuplicateColumnName(message)) {
                return RocksDBException.of(
                        RocksDBException.RocksDBErrorType.COLUMN_EXISTS,
                        "column already exists: " + createColumn.column().name(),
                        runtimeFailure
                );
            }
            if (command instanceof MetadataCommand.DeleteColumn deleteColumn && isUnknownColumn(message)) {
                return RocksDBException.of(
                        RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND,
                        "unknown column: " + deleteColumn.columnId(),
                        runtimeFailure
                );
            }
        }
        return commitFailed(runtimeFailure);
    }

    private static RocksDBException commitFailed(RuntimeException failure) {
        var runtimeFailure = runtimeMetadataFailure(failure);
        return RocksDBException.of(
                RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN,
                runtimeFailure.getMessage(),
                runtimeFailure
        );
    }

    private static RuntimeException runtimeMetadataFailure(RuntimeException failure) {
        if (failure instanceof CompletionException completionException) {
            return unwrapCompletionException(completionException, "replicated metadata command failed");
        }
        return failure;
    }

    private static boolean isDuplicateColumnName(String message) {
        return message != null && message.startsWith("duplicate column name in cluster metadata:");
    }

    private static boolean isUnknownColumn(String message) {
        return message != null && message.startsWith("unknown column:");
    }

    private Object executeWrite(RocksDBAPICommand<?, ?, ?> req) {
        var encoded = ReplicatedMutationCommandEncoder.encodePartitionWrite(req);
        var metadata = routeCache.metadata();
        var column = requireColumn(metadata, writeColumnId(req));
        if (encoded.isNoop()) {
            return successfulWriteResult(req);
        }
        var command = new PartitionWriteCommand(
                nextRequestId(),
                column.schemaEpoch(),
                encoded.mutations(),
                encoded.responseMode()
        );
        try {
            var commit = writeSubmitter.submit(command, metadata.epoch(), 0);
            return writeResult(req, encoded, commit);
        } catch (PartitionWriteRouteException e) {
            throw writeRouteFailure(e);
        }
    }

    private static long writeColumnId(RocksDBAPICommand<?, ?, ?> req) {
        return switch (req) {
            case RocksDBAPICommand.RocksDBAPICommandSingle.Put<?> put -> put.columnId();
            case RocksDBAPICommand.RocksDBAPICommandSingle.Delete<?> delete -> delete.columnId();
            case RocksDBAPICommand.RocksDBAPICommandSingle.Merge<?> merge -> merge.columnId();
            case RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?> putMulti -> putMulti.columnId();
            case RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?> deleteMulti -> deleteMulti.columnId();
            case RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?> mergeMulti -> mergeMulti.columnId();
            case RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch putBatch -> putBatch.columnId();
            case RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch mergeBatch -> mergeBatch.columnId();
            default -> throw new IllegalStateException(
                    "replicated write command has no column id: " + req.getClass().getName());
        };
    }

    private static RocksDBException writeRouteFailure(PartitionWriteRouteException failure) {
        if (failure.kind() == PartitionWriteFailureKind.INVALID_REQUEST) {
            return notImplemented("replicated partition write is rejected by routing constraints: "
                    + failure.getMessage());
        }
        return RocksDBException.of(
                RocksDBException.RocksDBErrorType.COMMIT_FAILED_TRY_AGAIN,
                failure.getMessage(),
                failure
        );
    }

    private static Object writeResult(RocksDBAPICommand<?, ?, ?> req,
                                      EncodedPartitionWrite encoded,
                                      ReplicatedLogCommit commit) {
        if (encoded.responseMode() == PartitionWriteResponseMode.NONE) {
            return successfulWriteResult(req);
        }
        var responses = decodeCommittedWriteResponses(encoded, commit);
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Put<?>
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Delete<?>
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Merge<?>) {
            return responses.getFirst();
        }
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?>
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?>
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?>) {
            return responses;
        }
        throw RocksDBException.of(
                RocksDBException.RocksDBErrorType.COMMIT_FAILED,
                "replicated write committed with response payload for unsupported command: " + req.getClass().getName()
        );
    }

    private static List<Object> decodeCommittedWriteResponses(EncodedPartitionWrite encoded,
                                                              ReplicatedLogCommit commit) {
        if (!commit.hasResponsePayload()) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.COMMIT_FAILED,
                    "replicated write committed without persisted response payload"
            );
        }
        try {
            var responses = PartitionWriteResponseCodec.decodeResponses(
                    commit.responsePayload(),
                    encoded.mutations().size()
            );
            if (responses.size() != encoded.mutations().size()) {
                throw new IllegalArgumentException("partition write response count "
                        + responses.size()
                        + " does not match mutation count "
                        + encoded.mutations().size());
            }
            return responses;
        } catch (IllegalArgumentException e) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.COMMIT_FAILED,
                    "replicated write committed with invalid response payload: " + e.getMessage(),
                    e
            );
        }
    }

    private Object executeRead(RocksDBAPICommand<?, ?, ?> req) {
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.Get<?> get) {
            return executeGet(get);
        }
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.GetColumnId(String columnName)) {
            return routeCache.metadata()
                    .findColumn(columnName)
                    .map(ClusterColumnMetadata::columnId)
                    .orElseThrow(() -> RocksDBException.of(
                            RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND,
                            "unknown column: " + columnName
                    ));
        }
        if (req instanceof RocksDBAPICommand.GetAllColumnDefinitions) {
            return columnDefinitions();
        }
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CheckMergeOperator checkMergeOperator) {
            return executeCheckMergeOperator(checkMergeOperator);
        }
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?> reduceRange) {
            return executeReduceRange(reduceRange);
        }
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> getRange) {
            return executeGetRange(getRange);
        }
        throw notImplemented("replicated routed read is not supported by the current read workflow: "
                + req.getClass().getName());
    }

    private Map<String, ColumnSchema> columnDefinitions() {
        var definitions = new LinkedHashMap<String, ColumnSchema>();
        for (var column : routeCache.metadata().columnDefinitions()) {
            definitions.put(column.name(), column.schema());
        }
        return Map.copyOf(definitions);
    }

    private Long executeCheckMergeOperator(RocksDBAPICommand.RocksDBAPICommandSingle.CheckMergeOperator check) {
        var name = requirePublicRequestField(check.name(), "merge operator name");
        var hash = requirePublicRequestField(check.hash(), "merge operator hash").clone();
        var metadata = routeCache.metadata();
        Long matchedVersion = null;
        for (var change : metadata.schemaChanges()) {
            if (change.changeKind() != SchemaChangeKind.MERGE_OPERATOR_ACTIVATION
                    || (change.status() != SchemaChangeStatus.ACTIVATED
                    && change.status() != SchemaChangeStatus.FENCED)) {
                continue;
            }
            var activation = change.mergeOperatorActivation();
            if (activation == null) {
                throw RocksDBException.of(
                        RocksDBException.RocksDBErrorType.GET_1,
                        "merge-operator schema change is missing activation metadata: " + change.schemaChangeId()
                );
            }
            if (!name.equals(activation.name()) || !Arrays.equals(activation.artifactHash().toByteArray(), hash)) {
                continue;
            }
            var column = metadata.findColumn(change.columnId()).orElse(null);
            if (column == null || change.targetSchemaEpoch() != column.schemaEpoch()) {
                continue;
            }
            var schema = column.schema();
            if (!name.equals(schema.mergeOperatorName())
                    || schema.mergeOperatorVersion() == null
                    || schema.mergeOperatorVersion() != activation.version()) {
                continue;
            }
            var version = activation.version();
            if (matchedVersion != null && matchedVersion != version) {
                throw RocksDBException.of(
                        RocksDBException.RocksDBErrorType.GET_1,
                        "conflicting active merge operator versions for " + name
                );
            }
            matchedVersion = version;
        }
        return matchedVersion;
    }

    private static <T> T requirePublicRequestField(T value, String description) {
        if (value == null) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    description + " must not be null"
            );
        }
        return value;
    }

    private Object executeGet(RocksDBAPICommand.RocksDBAPICommandSingle.Get<?> get) {
        if (ReplicatedCommandClassifier.usesLocalHandle(get)) {
            throw notImplemented("replicated point reads do not support local transaction/update handles");
        }
        var requestType = requireReadField(get.requestType(), "point read requestType");
        if (requestType instanceof RequestType.RequestNothing<?>) {
            return null;
        }
        if (!(requestType instanceof RequestType.RequestCurrent<?>)
                && !(requestType instanceof RequestType.RequestExists<?>)) {
            throw notImplemented("replicated point reads currently support only RequestType.current() and RequestType.exists()");
        }
        var columnId = requireReadColumnId(get.columnId());
        var keys = requireReadKeys(get.keys(), "point read keys");
        var metadata = routeCache.metadata();
        var column = requireColumn(metadata, columnId);
        var command = new PartitionPointReadCommand(column.schemaEpoch(), columnId, keys);
        try {
            var value = decodePointReadValue(pointReadSubmitter.readLinearizable(command, metadata.epoch(), 0));
            if (requestType instanceof RequestType.RequestExists<?>) {
                return value != null;
            }
            return value;
        } catch (PartitionReadRouteException | IllegalArgumentException | IllegalStateException e) {
            throw RocksDBException.of(
                    RocksDBException.RocksDBErrorType.GET_1,
                    e.getMessage(),
                    e
            );
        }
    }

    private static Buf decodePointReadValue(byte[] encodedResponse) {
        if (encodedResponse == null) {
            throw new IllegalArgumentException("point read response payload is missing");
        }
        var responses = PartitionWriteResponseCodec.decodeResponses(encodedResponse, 1);
        if (responses.size() != 1) {
            throw new IllegalArgumentException("point read response must contain exactly one value");
        }
        var response = responses.getFirst();
        if (response == null) {
            return null;
        }
        if (response instanceof Buf value) {
            return value;
        }
        throw new IllegalArgumentException("point read response must be null or Buf, got "
                + response.getClass().getName());
    }

    private Object executeReduceRange(RocksDBAPICommand.RocksDBAPICommandSingle.ReduceRange<?> reduceRange) {
        if (ReplicatedCommandClassifier.usesLocalHandle(reduceRange)) {
            throw notImplemented("replicated range reads do not support local transaction handles");
        }
        var requestType = requireReadField(reduceRange.requestType(), "reduceRange requestType");
        if (!(requestType instanceof RequestType.RequestEntriesCount<?>)
                && !(requestType instanceof RequestType.RequestGetFirstAndLast<?>)) {
            throw notImplemented("replicated reduceRange currently supports only entriesCount() and firstAndLast()");
        }
        try (var entries = executeRangeReadEntries(reduceRange.columnId(),
                reduceRange.startKeysInclusive(),
                reduceRange.endKeysExclusive(),
                reduceRange.reverse(),
                reduceRange.timeoutMs())) {
            if (requestType instanceof RequestType.RequestEntriesCount<?>) {
                return entries.count();
            }
            var iterator = entries.iterator();
            if (!iterator.hasNext()) {
                return new FirstAndLast<KV>(null, null);
            }
            var first = iterator.next();
            var last = first;
            while (iterator.hasNext()) {
                last = iterator.next();
            }
            return new FirstAndLast<>(first, last);
        }
    }

    private Stream<?> executeGetRange(RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> getRange) {
        if (ReplicatedCommandClassifier.usesLocalHandle(getRange)) {
            throw notImplemented("replicated range reads do not support local transaction handles");
        }
        var requestType = requireReadField(getRange.requestType(), "getRange requestType");
        if (!(requestType instanceof RequestType.RequestGetAllInRange<?>)) {
            throw notImplemented("replicated getRange currently supports only allInRange()");
        }
        return executeRangeReadEntries(getRange.columnId(),
                getRange.startKeysInclusive(),
                getRange.endKeysExclusive(),
                getRange.reverse(),
                getRange.timeoutMs());
    }

    private Stream<KV> executeRangeReadEntries(long columnId,
                                               Keys startKeysInclusive,
                                               Keys endKeysExclusive,
                                               boolean reverse,
                                               long timeoutMs) {
        var readColumnId = requireReadColumnId(columnId);
        var readStartKeysInclusive = requireNullableReadKeys(startKeysInclusive, "range read start keys");
        var readEndKeysExclusive = requireNullableReadKeys(endKeysExclusive, "range read end keys");
        var metadata = routeCache.metadata();
        var column = requireColumn(metadata, readColumnId);
        var command = new PartitionRangeReadCommand(column.schemaEpoch(),
                readColumnId,
                readStartKeysInclusive,
                readEndKeysExclusive,
                reverse,
                DEFAULT_MAX_RANGE_READ_ENTRIES_PER_SEGMENT);
        try {
            var cursor = rangeReadSubmitter.openCursor(command, metadata.epoch());
            var iterator = new RangeReadEntryIterator(cursor, deadlineEpochMillis(timeoutMs));
            return StreamSupport.stream(
                    Spliterators.spliteratorUnknownSize(iterator, Spliterator.ORDERED | Spliterator.NONNULL),
                    false
            );
        } catch (PartitionReadRouteException | IllegalArgumentException | IllegalStateException e) {
            throw rangeReadFailure(e);
        }
    }

    private long deadlineEpochMillis(long timeoutMs) {
        if (timeoutMs < 0) {
            throw new IllegalArgumentException("timeoutMs must be >= 0");
        }
        if (timeoutMs == 0) {
            return 0;
        }
        var now = currentTimeMillis.getAsLong();
        if (Long.MAX_VALUE - timeoutMs < now) {
            return Long.MAX_VALUE;
        }
        return now + timeoutMs;
    }

    private final class RangeReadEntryIterator implements Iterator<KV> {

        private Iterator<KV> currentEntries = Collections.emptyIterator();
        private PartitionRangeScanCursor nextCursor;
        private final long deadlineEpochMillis;
        private boolean exhausted;

        private RangeReadEntryIterator(PartitionRangeScanCursor initialCursor, long deadlineEpochMillis) {
            this.nextCursor = requireInternalContract(initialCursor, "range read initial cursor");
            this.deadlineEpochMillis = deadlineEpochMillis;
        }

        @Override
        public boolean hasNext() {
            while (!currentEntries.hasNext() && !exhausted) {
                fetchNextPage();
            }
            return currentEntries.hasNext();
        }

        @Override
        public KV next() {
            if (!hasNext()) {
                throw new NoSuchElementException("Range read stream is exhausted");
            }
            return currentEntries.next();
        }

        private void fetchNextPage() {
            if (nextCursor == null) {
                exhausted = true;
                currentEntries = Collections.emptyIterator();
                return;
            }
            requireOpen();
            try {
                var currentPage = rangeReadSubmitter.scanLinearizablePage(
                        nextCursor,
                        DEFAULT_MAX_RANGE_READ_SEGMENTS_PER_STREAM_PAGE,
                        deadlineEpochMillis
                );
                nextCursor = currentPage.nextCursor();
                var entries = currentPage.results().stream()
                        .flatMap(segment -> decodeRangeSegmentEntries(segment).stream())
                        .toList();
                currentEntries = entries.iterator();
                if (nextCursor == null && !currentEntries.hasNext()) {
                    exhausted = true;
                }
            } catch (PartitionReadRouteException | IllegalArgumentException | IllegalStateException e) {
                throw rangeReadFailure(e);
            }
        }
    }

    private static RocksDBException rangeReadFailure(RuntimeException failure) {
        return RocksDBException.of(
                RocksDBException.RocksDBErrorType.GET_1,
                failure.getMessage(),
                failure
        );
    }

    private static long requireReadColumnId(long columnId) {
        if (columnId < 0) {
            throw invalidReadRequest("columnId must be >= 0");
        }
        return columnId;
    }

    private static Keys requireNullableReadKeys(Keys keys, String description) {
        if (keys == null) {
            return null;
        }
        return requireReadKeys(keys, description);
    }

    private static Keys requireReadKeys(Keys keys, String description) {
        var requiredKeys = requireReadField(keys, description);
        var keyParts = requireReadField(requiredKeys.keys(), description + " parts");
        for (int i = 0; i < keyParts.length; i++) {
            requireReadField(keyParts[i], description + " part " + i);
        }
        return requiredKeys;
    }

    private static <T> T requireReadField(T value, String description) {
        if (value == null) {
            throw invalidReadRequest(description + " must not be null");
        }
        return value;
    }

    private static RocksDBException invalidReadRequest(String message) {
        return RocksDBException.of(RocksDBException.RocksDBErrorType.GET_1, message);
    }

    private static ClusterColumnMetadata requireColumn(ClusterMetadata metadata, long columnId) {
        return metadata.findColumn(columnId)
                .orElseThrow(() -> RocksDBException.of(
                        RocksDBException.RocksDBErrorType.COLUMN_NOT_FOUND,
                        "unknown column: " + columnId
                ));
    }

    private static List<KV> decodeRangeSegmentEntries(PartitionRangeReadSubmitter.PartitionRangeScanResult segment) {
        var response = segment.response();
        if (response == null) {
            throw new IllegalArgumentException("range read segment response payload is missing");
        }
        var maxEntries = segment.command().maxEntries();
        var entries = PartitionRangeReadResponseCodec.decodeEntries(response, maxEntries);
        if (entries.size() > maxEntries) {
            throw new IllegalArgumentException("range read segment returned "
                    + entries.size()
                    + " entries above requested limit "
                    + maxEntries);
        }
        return entries;
    }

    private ClientRequestId nextRequestId() {
        return new ClientRequestId(clientId, requestSequence.getAndIncrement());
    }

    private Throwable closeOwnedResource() {
        if (closeable == null) {
            ownedResourceClosed = true;
            return null;
        }
        try {
            closeable.close();
            ownedResourceClosed = true;
            return null;
        } catch (IOException | Error e) {
            return e;
        } catch (Exception e) {
            return new IOException("Failed to close replicated connection", e);
        }
    }

    private void requireOpen() {
        if (closed.get()) {
            throw closedException();
        }
    }

    private static IllegalStateException closedException() {
        return new IllegalStateException("Replicated connection is closed");
    }

    private static Throwable merge(Throwable failure, Throwable addition) {
        if (addition == null) {
            return failure;
        }
        if (failure == null) {
            return addition;
        }
        if (failure != addition) {
            failure.addSuppressed(addition);
        }
        return failure;
    }

    private static void throwFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException ioException) {
            throw ioException;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IOException("Failed to close replicated connection", failure);
    }

    private static ThreadPoolExecutor asyncExecutor() {
        return new ThreadPoolExecutor(
                0,
                DEFAULT_MAX_CONCURRENT_ASYNC_REQUESTS,
                ASYNC_EXECUTOR_KEEP_ALIVE_SECONDS,
                TimeUnit.SECONDS,
                new SynchronousQueue<>(),
                ReplicatedConnection::asyncThread
        );
    }

    private static Thread asyncThread(Runnable task) {
        var thread = new Thread(task, "rockserver-replicated-async");
        thread.setDaemon(true);
        return thread;
    }

    private static long nextColumnId(ClusterMetadata metadata) {
        var nextColumnId = 0L;
        for (var column : metadata.columnDefinitions()) {
            if (column.columnId() == Long.MAX_VALUE) {
                throw RocksDBException.of(
                        RocksDBException.RocksDBErrorType.COLUMN_CREATE_FAIL,
                        "column id space is exhausted"
                );
            }
            nextColumnId = Math.max(nextColumnId, column.columnId() + 1);
        }
        return nextColumnId;
    }

    private static Object successfulWriteResult(RocksDBAPICommand<?, ?, ?> req) {
        if (req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.PutMulti<?>
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.DeleteMulti<?>
                || req instanceof RocksDBAPICommand.RocksDBAPICommandSingle.MergeMulti<?>) {
            return List.of();
        }
        return null;
    }

    private static RocksDBException notImplemented(String message) {
        return RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, message);
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static <T> T requireInternalContract(T value, String description) {
        if (value == null) {
            throw new IllegalStateException(description + " must not be null");
        }
        return value;
    }

}
