package it.cavallium.rockserver.core.client;

import static it.cavallium.rockserver.core.common.FutureUtils.submitCancellationAware;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RequestType.RequestChanged;
import it.cavallium.rockserver.core.common.RequestType.RequestCurrent;
import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import it.cavallium.rockserver.core.common.RequestType.RequestDelta;
import it.cavallium.rockserver.core.common.RequestType.RequestEntriesCount;
import it.cavallium.rockserver.core.common.RequestType.RequestExists;
import it.cavallium.rockserver.core.common.RequestType.RequestForUpdate;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestGetAllInRange;
import it.cavallium.rockserver.core.common.RequestType.RequestGetFirstAndLast;
import it.cavallium.rockserver.core.common.RequestType.RequestGetRange;
import it.cavallium.rockserver.core.common.RequestType.RequestIterate;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestMulti;
import it.cavallium.rockserver.core.common.RequestType.RequestNothing;
import it.cavallium.rockserver.core.common.RequestType.RequestPrevious;
import it.cavallium.rockserver.core.common.RequestType.RequestPreviousPresence;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.common.RequestType.RequestReduceRange;
import it.cavallium.rockserver.core.common.RocksDBAPI;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.SerializedKVBatch;
import it.cavallium.rockserver.core.common.TransportRequestLimits;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.OptionalBinary;
import it.cavallium.rockserver.core.common.api.RocksDB;
import it.cavallium.rockserver.core.common.api.RocksDBThriftException;
import it.cavallium.rockserver.core.common.api.UpdateBegin;

import java.io.IOException;
import java.net.URI;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.thrift.TException;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.apache.thrift.transport.TSocket;
import org.apache.thrift.transport.TTransport;
import org.apache.thrift.transport.layered.TFramedTransport;
import org.jetbrains.annotations.NotNull;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class ThriftConnection extends BaseConnection implements RocksDBAPI {

    private static final int MAX_PENDING_ASYNC_REQUESTS = 1024;
    private static final int ASYNC_EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final URI uri;
    private final TTransport transport;
    private final RocksDB.Client client;
    private final ExecutorService executor;
    private final AtomicBoolean closed = new AtomicBoolean();

    public ThriftConnection(String name, String host, int port) throws TException {
        super(name);
        this.uri = URI.create("thrift://" + host + ":" + port);
        this.transport = new TFramedTransport(new TSocket(host, port));
        this.transport.open();
        this.client = new RocksDB.Client(new TBinaryProtocol(this.transport));
        this.executor = new ThreadPoolExecutor(
                1,
                1,
                0L,
                TimeUnit.MILLISECONDS,
                new ArrayBlockingQueue<>(MAX_PENDING_ASYNC_REQUESTS),
                task -> Thread.ofPlatform().daemon().name("rockserver-thrift-client-" + name).unstarted(task),
                new ThreadPoolExecutor.AbortPolicy()
        );
    }

    @SuppressWarnings("unused")
    private ThriftConnection(String name,
                             URI uri,
                             TTransport transport,
                             ExecutorService executor) {
        this(name, uri, transport, null, executor);
    }

    private ThriftConnection(String name,
                             URI uri,
                             TTransport transport,
                             RocksDB.Client client,
                             ExecutorService executor) {
        super(name);
        this.uri = uri;
        this.transport = transport;
        this.client = client;
        this.executor = executor;
    }

    @Override
    public URI getUrl() {
        return uri;
    }

    @Override
    public RocksDBSyncAPI getSyncApi() {
        return this;
    }

    @Override
    public RocksDBAsyncAPI getAsyncApi() {
        return this;
    }

    @Override
    public void close() throws IOException {
        if (!closed.compareAndSet(false, true)) {
            return;
        }
        Throwable failure = null;
        try {
            transport.close();
        } catch (RuntimeException | Error e) {
            failure = e;
        }
        try {
            executor.shutdownNow();
        } catch (RuntimeException | Error e) {
            failure = addCloseFailure(failure, e);
        }
        var interrupted = false;
        try {
            var deadlineNanos = System.nanoTime()
                    + TimeUnit.SECONDS.toNanos(ASYNC_EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS);
            while (true) {
                try {
                    var remainingNanos = deadlineNanos - System.nanoTime();
                    if (remainingNanos <= 0) {
                        failure = addCloseFailure(failure, asyncRequestsDidNotStopFailure());
                        break;
                    }
                    if (executor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS)) {
                        break;
                    }
                    failure = addCloseFailure(failure, asyncRequestsDidNotStopFailure());
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
        try {
            super.close();
        } catch (IOException | RuntimeException | Error e) {
            failure = addCloseFailure(failure, e);
        }
        if (failure != null) {
            throwCloseFailure(failure);
        }
    }

    private static IOException asyncRequestsDidNotStopFailure() {
        return new IOException("Thrift client async requests did not stop within "
                + ASYNC_EXECUTOR_SHUTDOWN_TIMEOUT_SECONDS + " seconds");
    }

    private static Throwable addCloseFailure(Throwable failure, Throwable addition) {
        if (failure == null) {
            return addition;
        }
        if (failure != addition) {
            failure.addSuppressed(addition);
        }
        return failure;
    }

    private static void throwCloseFailure(Throwable failure) throws IOException {
        if (failure instanceof IOException ioException) {
            throw ioException;
        }
        if (failure instanceof RuntimeException runtimeException) {
            throw runtimeException;
        }
        if (failure instanceof Error error) {
            throw error;
        }
        throw new IOException("Unexpected Thrift client close failure", failure);
    }

    // --- Sync API ---

    @Override
    public Stream<SerializedKVBatch> scanRaw(long columnId, int shardIndex, int shardCount) {
        requireOpen();
        throw RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED, "scanRaw is not exposed by the legacy Thrift transport");
    }

    @Override
    public Publisher<SerializedKVBatch> scanRawAsync(long columnId, int shardIndex, int shardCount) {
        requireOpen();
        throw RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED, "scanRawAsync is not exposed by the legacy Thrift transport");
    }

    @Override
    public synchronized long openTransaction(long timeoutMs) {
        try {
            return client().openTransaction(timeoutMs);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized boolean closeTransaction(long transactionId, boolean commit) {
        try {
            return client().closeTransaction(transactionId, commit);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized void closeFailedUpdate(long updateId) {
        try {
            client().closeFailedUpdate(updateId);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized long createColumn(String name, @NotNull ColumnSchema schema) {
        try {
            return client().createColumn(name, mapSchema(schema));
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized long uploadMergeOperator(String name, String className, byte[] jarData) {
        try {
            return client().uploadMergeOperator(name, className, ByteBuffer.wrap(jarData));
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public Long checkMergeOperator(String name, byte[] hash) {
        requireOpen();
        throw unsupportedThriftOperation("checkMergeOperator");
    }

    @Override
    public synchronized void deleteColumn(long columnId) {
        try {
            client().deleteColumn(columnId);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized long getColumnId(@NotNull String name) {
        try {
            return client().getColumnId(name);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @SuppressWarnings({"unchecked", "IfCanBeSwitch"})
    @Override
    public synchronized <T> T put(long transactionOrUpdateId,
                                  long columnId,
                                  Keys keys,
                                  @NotNull Buf value,
                                  RequestPut<? super Buf, T> requestType) {
        try {
            var requiredRequestType = requireRequestType(requestType);
            if (requiredRequestType instanceof RequestNothing) {
                client().put(transactionOrUpdateId, columnId,
                        requiredMapKeys("Thrift client put keys", keys),
                        requiredMapBuf("Thrift client put value", value));
                return null;
            } else if (requiredRequestType instanceof RequestPrevious) {
                return (T) mapOptionalBinary(client().putGetPrevious(transactionOrUpdateId, columnId,
                        requiredMapKeys("Thrift client put keys", keys),
                        requiredMapBuf("Thrift client put value", value)));
            } else if (requiredRequestType instanceof RequestDelta) {
                return (T) mapDelta(client().putGetDelta(transactionOrUpdateId, columnId,
                        requiredMapKeys("Thrift client put keys", keys),
                        requiredMapBuf("Thrift client put value", value)));
            } else if (requiredRequestType instanceof RequestChanged) {
                return (T) (Boolean) client().putGetChanged(transactionOrUpdateId, columnId,
                        requiredMapKeys("Thrift client put keys", keys),
                        requiredMapBuf("Thrift client put value", value));
            } else if (requiredRequestType instanceof RequestPreviousPresence) {
                return (T) (Boolean) client().putGetPreviousPresence(transactionOrUpdateId, columnId,
                        requiredMapKeys("Thrift client put keys", keys),
                        requiredMapBuf("Thrift client put value", value));
            }
            throw unsupportedThriftRequestType("put", requiredRequestType);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public <T> T delete(long transactionOrUpdateId, long columnId, Keys keys, RequestDelete<? super Buf, T> requestType) {
        requireOpen();
        throw unsupportedThriftOperation("delete");
    }

    @SuppressWarnings({"unchecked", "IfCanBeSwitch"})
    @Override
    public synchronized <T> T merge(long transactionOrUpdateId,
                                    long columnId,
                                    Keys keys,
                                    @NotNull Buf value,
                                    RequestMerge<? super Buf, T> requestType) {
        try {
            var requiredRequestType = requireRequestType(requestType);
            if (requiredRequestType instanceof RequestNothing) {
                client().merge(transactionOrUpdateId, columnId,
                        requiredMapKeys("Thrift client merge keys", keys),
                        requiredMapBuf("Thrift client merge value", value));
                return null;
            } else if (requiredRequestType instanceof RequestType.RequestMerged) {
                return (T) mapOptionalBinary(client().mergeGetMerged(transactionOrUpdateId, columnId,
                        requiredMapKeys("Thrift client merge keys", keys),
                        requiredMapBuf("Thrift client merge value", value)));
            }
            throw unsupportedThriftRequestType("merge", requiredRequestType);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @SuppressWarnings({"unchecked", "IfCanBeSwitch"})
    @Override
    public synchronized <T> List<T> putMulti(long transactionOrUpdateId,
                                             long columnId,
                                             @NotNull List<@NotNull Keys> keys,
                                             @NotNull List<@NotNull Buf> values,
                                             RequestPut<? super Buf, T> requestType) {
        try {
            var requiredRequestType = requireRequestType(requestType);
            var thriftKeys = mapKeysList(keys);
            var thriftValues = mapBufList(values);
            requireMatchingBatchSize("Thrift client multi-write", thriftKeys.size(), thriftValues.size());
            if (requiredRequestType instanceof RequestNothing) {
                client().putMulti(transactionOrUpdateId, columnId, thriftKeys, thriftValues);
                return Collections.nCopies(thriftKeys.size(), null);
            } else if (requiredRequestType instanceof RequestPrevious) {
                return (List<T>) mapOptionalBinaryList(client().putMultiGetPrevious(transactionOrUpdateId, columnId, thriftKeys, thriftValues));
            } else if (requiredRequestType instanceof RequestDelta) {
                return (List<T>) client().putMultiGetDelta(transactionOrUpdateId, columnId, thriftKeys, thriftValues)
                        .stream().map(ThriftConnection::mapDelta).collect(Collectors.toList());
            } else if (requiredRequestType instanceof RequestChanged) {
                return (List<T>) client().putMultiGetChanged(transactionOrUpdateId, columnId, thriftKeys, thriftValues);
            } else if (requiredRequestType instanceof RequestPreviousPresence) {
                return (List<T>) client().putMultiGetPreviousPresence(transactionOrUpdateId, columnId, thriftKeys, thriftValues);
            }
            throw unsupportedThriftRequestType("putMulti", requiredRequestType);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public <T> List<T> deleteMulti(long transactionOrUpdateId,
                                   long columnId,
                                   @NotNull List<@NotNull Keys> keys,
                                   RequestDelete<? super Buf, T> requestType) {
        requireOpen();
        throw unsupportedThriftOperation("deleteMulti");
    }

    @SuppressWarnings({"unchecked", "IfCanBeSwitch"})
    @Override
    public synchronized <T> List<T> mergeMulti(long transactionOrUpdateId,
                                               long columnId,
                                               @NotNull List<@NotNull Keys> keys,
                                               @NotNull List<@NotNull Buf> values,
                                               RequestMerge<? super Buf, T> requestType) {
        try {
            var requiredRequestType = requireRequestType(requestType);
            var thriftKeys = mapKeysList(keys);
            var thriftValues = mapBufList(values);
            requireMatchingBatchSize("Thrift client multi-merge", thriftKeys.size(), thriftValues.size());
            if (requiredRequestType instanceof RequestNothing) {
                client().mergeMulti(transactionOrUpdateId, columnId, thriftKeys, thriftValues);
                return Collections.nCopies(thriftKeys.size(), null);
            } else if (requiredRequestType instanceof RequestType.RequestMerged) {
                return (List<T>) mapOptionalBinaryList(client().mergeMultiGetMerged(transactionOrUpdateId, columnId, thriftKeys, thriftValues));
            }
            throw unsupportedThriftRequestType("mergeMulti", requiredRequestType);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized void putBatch(long columnId,
                                      @NotNull Publisher<@NotNull KVBatch> batchPublisher,
                                      @NotNull PutBatchMode mode) {
        try {
            client().putBatch(columnId, mapBatchData(batchPublisher), mapPutBatchMode(mode));
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized void mergeBatch(long columnId,
                                        @NotNull Publisher<@NotNull KVBatch> batchPublisher,
                                        @NotNull MergeBatchMode mode) {
        try {
            client().mergeBatch(columnId, mapBatchData(batchPublisher), mapMergeBatchMode(mode));
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @SuppressWarnings({"unchecked", "IfCanBeSwitch"})
    @Override
    public synchronized <T> T get(long transactionOrUpdateId, long columnId, Keys keys, RequestGet<? super Buf, T> requestType) {
        try {
            var requiredRequestType = requireRequestType(requestType);
            if (requiredRequestType instanceof RequestCurrent) {
                return (T) mapOptionalBinary(client().get(transactionOrUpdateId, columnId,
                        requiredMapKeys("Thrift client get keys", keys)));
            } else if (requiredRequestType instanceof RequestForUpdate) {
                UpdateBegin ub = client().getForUpdate(transactionOrUpdateId, columnId,
                        requiredMapKeys("Thrift client get keys", keys));
                return (T) new it.cavallium.rockserver.core.common.UpdateContext<>(
                        ub.isSetPrevious() ? Utils.fromByteBuffer(ub.previous) : null,
                        ub.getUpdateId());
            } else if (requiredRequestType instanceof RequestExists) {
                return (T) (Boolean) client().exists(transactionOrUpdateId, columnId,
                        requiredMapKeys("Thrift client get keys", keys));
            }
            throw unsupportedThriftRequestType("get", requiredRequestType);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized long openIterator(long transactionId, long columnId, Keys startKeysInclusive, Keys endKeysExclusive, boolean reverse, long timeoutMs) {
        try {
            return client().openIterator(transactionId, columnId, mapKeys(startKeysInclusive), mapKeys(endKeysExclusive), reverse, timeoutMs);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized void closeIterator(long iteratorId) {
        try {
            client().closeIterator(iteratorId);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized void seekTo(long iterationId, Keys keys) {
        try {
            client().seekTo(iterationId, requiredMapKeys("Thrift client iterator seek keys", keys));
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @SuppressWarnings({"unchecked", "IfCanBeSwitch"})
    @Override
    public synchronized <T> T subsequent(long iterationId,
                                         long skipCount,
                                         long takeCount,
                                         @NotNull RequestIterate<? super Buf, T> requestType) {
        try {
            var requiredRequestType = requireRequestType(requestType);
            if (requiredRequestType instanceof RequestNothing) {
                client().subsequent(iterationId, skipCount, takeCount);
                return null;
            } else if (requiredRequestType instanceof RequestExists) {
                return (T) (Boolean) client().subsequentExists(iterationId, skipCount, takeCount);
            } else if (requiredRequestType instanceof RequestMulti) {
                return (T) mapOptionalBinaryList(client().subsequentMultiGet(iterationId, skipCount, takeCount));
            }
            throw unsupportedThriftRequestType("subsequent", requiredRequestType);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized <T> T reduceRange(long transactionId,
                                          long columnId,
                                          Keys startKeysInclusive,
                                          Keys endKeysExclusive,
                                          boolean reverse,
                                          @NotNull RequestReduceRange<? super KV, T> requestType,
                                          long timeoutMs) {
        try {
            var requiredRequestType = requireRequestType(requestType);
            if (requiredRequestType instanceof RequestEntriesCount) {
                return (T) Long.valueOf(client().reduceRangeEntriesCount(transactionId, columnId, mapKeys(startKeysInclusive), mapKeys(endKeysExclusive), reverse, timeoutMs));
            } else if (requiredRequestType instanceof RequestGetFirstAndLast) {
                it.cavallium.rockserver.core.common.api.FirstAndLast fl = client().reduceRangeFirstAndLast(transactionId, columnId, mapKeys(startKeysInclusive), mapKeys(endKeysExclusive), reverse, timeoutMs);
                return (T) new it.cavallium.rockserver.core.common.FirstAndLast<>(
                        fl.isSetFirst() ? mapKV(fl.getFirst()) : null,
                        fl.isSetLast() ? mapKV(fl.getLast()) : null
                );
            }
            throw unsupportedThriftRequestType("reduceRange", requiredRequestType);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public synchronized <T> Stream<T> getRange(long transactionId,
                                               long columnId,
                                               Keys startKeysInclusive,
                                               Keys endKeysExclusive,
                                               boolean reverse,
                                               @NotNull RequestGetRange<? super KV, T> requestType,
                                               long timeoutMs) {
        try {
            var requiredRequestType = requireRequestType(requestType);
            if (requiredRequestType instanceof RequestGetAllInRange) {
                List<it.cavallium.rockserver.core.common.api.KV> list = client().getAllInRange(transactionId, columnId, mapKeys(startKeysInclusive), mapKeys(endKeysExclusive), reverse, timeoutMs);
                return list.stream().map(ThriftConnection::mapKV).map(k -> (T) k);
            }
            throw unsupportedThriftRequestType("getRange", requiredRequestType);
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized void flush() {
        try {
            client().flush();
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized void compact() {
        try {
            client().compact();
        } catch (TException e) {
            throw wrap(e);
        }
    }

    @Override
    public synchronized Map<String, ColumnSchema> getAllColumnDefinitions() {
        try {
            return client().getAllColumnDefinitions().stream()
                    .collect(Collectors.toMap(
                            it.cavallium.rockserver.core.common.api.Column::getName,
                            c -> mapSchemaFromThrift(c.getSchema())
                    ));
        } catch (TException e) {
            throw wrap(e);
        }
    }

    // --- Async API ---

    @Override
    public CompletableFuture<Long> openTransactionAsync(long timeoutMs) {
        return submitAsync(() -> openTransaction(timeoutMs));
    }

    @Override
    public CompletableFuture<Boolean> closeTransactionAsync(long transactionId, boolean commit) {
        return submitAsync(() -> closeTransaction(transactionId, commit));
    }

    @Override
    public CompletableFuture<Void> closeFailedUpdateAsync(long updateId) {
        return submitAsync(() -> closeFailedUpdate(updateId));
    }

    @Override
    public CompletableFuture<Long> createColumnAsync(String name, @NotNull ColumnSchema schema) {
        return submitAsync(() -> createColumn(name, schema));
    }

    @Override
    public CompletableFuture<Long> uploadMergeOperatorAsync(String name, String className, byte[] jarData) {
        return submitAsync(() -> uploadMergeOperator(name, className, jarData));
    }

    @Override
    public CompletableFuture<Long> checkMergeOperatorAsync(String name, byte[] hash) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(closedException());
        }
        return CompletableFuture.failedFuture(unsupportedThriftOperation("checkMergeOperator"));
    }

    @Override
    public CompletableFuture<Void> deleteColumnAsync(long columnId) {
        return submitAsync(() -> deleteColumn(columnId));
    }

    @Override
    public CompletableFuture<Long> getColumnIdAsync(@NotNull String name) {
        return submitAsync(() -> getColumnId(name));
    }

    @Override
    public <T> CompletableFuture<T> putAsync(long transactionOrUpdateId,
                                             long columnId,
                                             @NotNull Keys keys,
                                             @NotNull Buf value,
                                             RequestPut<? super Buf, T> requestType) {
        return submitAsync(() -> put(transactionOrUpdateId, columnId, keys, value, requestType));
    }

    @Override
    public <T> CompletableFuture<T> deleteAsync(long transactionOrUpdateId,
                                                long columnId,
                                                @NotNull Keys keys,
                                                RequestDelete<? super Buf, T> requestType) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(closedException());
        }
        return CompletableFuture.failedFuture(unsupportedThriftOperation("delete"));
    }

    @Override
    public <T> CompletableFuture<T> mergeAsync(long transactionOrUpdateId,
                                               long columnId,
                                               @NotNull Keys keys,
                                               @NotNull Buf value,
                                               RequestMerge<? super Buf, T> requestType) {
        return submitAsync(() -> merge(transactionOrUpdateId, columnId, keys, value, requestType));
    }

    @Override
    public <T> CompletableFuture<List<T>> putMultiAsync(long transactionOrUpdateId,
                                                        long columnId,
                                                        @NotNull List<@NotNull Keys> keys,
                                                        @NotNull List<@NotNull Buf> values,
                                                        RequestPut<? super Buf, T> requestType) {
        return submitAsync(() -> putMulti(transactionOrUpdateId, columnId, keys, values, requestType));
    }

    @Override
    public <T> CompletableFuture<List<T>> deleteMultiAsync(long transactionOrUpdateId,
                                                           long columnId,
                                                           @NotNull List<@NotNull Keys> keys,
                                                           RequestDelete<? super Buf, T> requestType) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(closedException());
        }
        return CompletableFuture.failedFuture(unsupportedThriftOperation("deleteMulti"));
    }

    @Override
    public <T> CompletableFuture<List<T>> mergeMultiAsync(long transactionOrUpdateId,
                                                          long columnId,
                                                          @NotNull List<@NotNull Keys> keys,
                                                          @NotNull List<@NotNull Buf> values,
                                                          RequestMerge<? super Buf, T> requestType) {
        return submitAsync(() -> mergeMulti(transactionOrUpdateId, columnId, keys, values, requestType));
    }

    @Override
    public CompletableFuture<Void> putBatchAsync(long columnId,
                                                 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
                                                 @NotNull PutBatchMode mode) {
        return submitAsync(() -> putBatch(columnId, batchPublisher, mode));
    }

    @Override
    public CompletableFuture<Void> mergeBatchAsync(long columnId,
                                                   @NotNull Publisher<@NotNull KVBatch> batchPublisher,
                                                   @NotNull MergeBatchMode mode) {
        return submitAsync(() -> mergeBatch(columnId, batchPublisher, mode));
    }

    @Override
    public <T> CompletableFuture<T> getAsync(long transactionOrUpdateId,
                                             long columnId,
                                             @NotNull Keys keys,
                                             RequestGet<? super Buf, T> requestType) {
        return submitAsync(() -> get(transactionOrUpdateId, columnId, keys, requestType));
    }

    @Override
    public CompletableFuture<Long> openIteratorAsync(long transactionId,
                                                     long columnId,
                                                     @NotNull Keys startKeysInclusive,
                                                     Keys endKeysExclusive,
                                                     boolean reverse,
                                                     long timeoutMs) {
        return submitAsync(() -> openIterator(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs));
    }

    @Override
    public CompletableFuture<Void> closeIteratorAsync(long iteratorId) {
        return submitAsync(() -> closeIterator(iteratorId));
    }

    @Override
    public CompletableFuture<Void> seekToAsync(long iterationId, @NotNull Keys keys) {
        return submitAsync(() -> seekTo(iterationId, keys));
    }

    @Override
    public <T> CompletableFuture<T> subsequentAsync(long iterationId,
                                                    long skipCount,
                                                    long takeCount,
                                                    @NotNull RequestIterate<? super Buf, T> requestType) {
        return submitAsync(() -> subsequent(iterationId, skipCount, takeCount, requestType));
    }

    @Override
    public <T> CompletableFuture<T> reduceRangeAsync(long transactionId, long columnId, Keys startKeysInclusive, Keys endKeysExclusive, boolean reverse, RequestReduceRange<? super KV, T> requestType, long timeoutMs) {
        return submitAsync(() -> reduceRange(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs));
    }

    @Override
    public <T> Publisher<T> getRangeAsync(long transactionId, long columnId, Keys startKeysInclusive, Keys endKeysExclusive, boolean reverse, RequestGetRange<? super KV, T> requestType, long timeoutMs) {
        if (closed.get()) {
            return Flux.error(closedException());
        }
        return Flux.create(sink -> {
            Future<?> invocation;
            try {
                invocation = executor.submit(() -> {
                    try (Stream<T> stream = getRange(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs)) {
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
                    sink.error(asyncExecutorSaturatedException(e));
                }
                return;
            } catch (RuntimeException | Error e) {
                sink.error(e);
                return;
            }
            sink.onCancel(() -> invocation.cancel(true));
        });
    }

    @Override
    public CompletableFuture<Void> flushAsync() {
        return submitAsync(this::flush);
    }

    @Override
    public CompletableFuture<Void> compactAsync() {
        return submitAsync(this::compact);
    }

    @Override
    public CompletableFuture<Map<String, ColumnSchema>> getAllColumnDefinitionsAsync() {
        return submitAsync(this::getAllColumnDefinitions);
    }

    @Override
    public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
        requireOpen();
        return req.handleSync(this);
    }

    @Override
    public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> req) {
        return req.handleAsync(this);
    }

    // --- Helpers ---

    private <T> CompletableFuture<T> submitAsync(Supplier<? extends T> supplier) {
        if (closed.get()) {
            return CompletableFuture.failedFuture(closedException());
        }
        try {
            return submitCancellationAware(executor, () -> {
                requireOpen();
                return supplier.get();
            });
        } catch (RejectedExecutionException e) {
            if (closed.get()) {
                return CompletableFuture.failedFuture(closedException());
            }
            return CompletableFuture.failedFuture(asyncExecutorSaturatedException(e));
        }
    }

    private CompletableFuture<Void> submitAsync(Runnable runnable) {
        return submitAsync(() -> {
            runnable.run();
            return null;
        });
    }

    private RocksDB.Client client() {
        requireOpen();
        return client;
    }

    private void requireOpen() {
        if (closed.get()) {
            throw closedException();
        }
    }

    private static IllegalStateException closedException() {
        return new IllegalStateException("Thrift connection is closed");
    }

    private static IllegalStateException asyncExecutorSaturatedException(RejectedExecutionException e) {
        return new IllegalStateException("Thrift client async request executor is saturated", e);
    }

    private RocksDBException wrap(TException e) {
        if (e instanceof RocksDBThriftException re) {
            var type = RocksDBErrorType.valueOf(re.getErrorType().name());
            if (type == RocksDBErrorType.UPDATE_RETRY) {
                return new RocksDBRetryException();
            }
            return RocksDBException.of(
                    type,
                    re.getMessage()
            );
        }
        return RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, e);
    }

    private static RocksDBException unsupportedThriftOperation(String operation) {
        return RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,
                operation + " is not exposed by the legacy Thrift transport");
    }

    private static RocksDBException unsupportedThriftRequestType(String operation, Object requestType) {
        return RocksDBException.of(
                RocksDBErrorType.NOT_IMPLEMENTED,
                operation + " request type " + requestType
                        + " is not exposed by the legacy Thrift transport"
        );
    }

    private static List<ByteBuffer> mapKeys(Keys keys) {
        if (keys == null) return null;
        return mapKeys("Thrift client optional key boundary", keys);
    }

    private static List<ByteBuffer> requiredMapKeys(String description, Keys keys) {
        return mapKeys(description, requireClientField(keys, description));
    }

    private static List<ByteBuffer> mapKeys(String description, Keys keys) {
        var keyParts = requireClientField(keys.keys(), description + " parts");
        TransportRequestLimits.requireKeyParts("Thrift client key parts", keyParts.length);
        List<ByteBuffer> list = new ArrayList<>(keyParts.length);
        for (int i = 0; i < keyParts.length; i++) {
            list.add(requiredMapBuf(description + " part " + i, keyParts[i]));
        }
        return list;
    }

    private static List<List<ByteBuffer>> mapKeysList(List<Keys> keysList) {
        var requiredKeysList = requireClientField(keysList, "Thrift client multi-key entries");
        TransportRequestLimits.requireBatchEntries("Thrift client multi-key entries", requiredKeysList.size());
        List<List<ByteBuffer>> result = new ArrayList<>(requiredKeysList.size());
        for (int i = 0; i < requiredKeysList.size(); i++) {
            result.add(requiredMapKeys("Thrift client multi-key entry " + i, requiredKeysList.get(i)));
        }
        return result;
    }

    private static ByteBuffer requiredMapBuf(String description, Buf buf) {
        return Utils.asByteBuffer(requireClientField(buf, description));
    }

    private static List<it.cavallium.rockserver.core.common.api.KV> mapBatchData(Publisher<@NotNull KVBatch> batchPublisher) {
        List<it.cavallium.rockserver.core.common.api.KV> data = new ArrayList<>();
        Flux.from(requireClientField(batchPublisher, "Thrift client batch publisher")).toIterable().forEach(batch -> {
            var requiredBatch = requireClientField(batch, "Thrift client batch");
            List<Keys> keys = requireClientField(requiredBatch.keys(), "Thrift client batch keys");
            List<Buf> values = requireClientField(requiredBatch.values(), "Thrift client batch values");
            requireMatchingBatchSize("Thrift client batch", keys.size(), values.size());
            TransportRequestLimits.requireAdditionalBatchEntries("Thrift client batch entries",
                    data.size(), keys.size());
            for (int i = 0; i < keys.size(); i++) {
                data.add(new it.cavallium.rockserver.core.common.api.KV(
                        requiredMapKeys("Thrift client batch entry " + i + " keys", keys.get(i)),
                        requiredMapBuf("Thrift client batch entry " + i + " value", values.get(i))));
            }
        });
        return data;
    }

    private static List<ByteBuffer> mapBufList(List<Buf> bufList) {
        var requiredBufList = requireClientField(bufList, "Thrift client value entries");
        TransportRequestLimits.requireBatchEntries("Thrift client value entries", requiredBufList.size());
        List<ByteBuffer> result = new ArrayList<>(requiredBufList.size());
        for (int i = 0; i < requiredBufList.size(); i++) {
            result.add(requiredMapBuf("Thrift client value entry " + i, requiredBufList.get(i)));
        }
        return result;
    }

    private static Buf mapOptionalBinary(OptionalBinary opt) {
        if (opt == null || !opt.isSetValue()) return null;
        return Utils.fromByteBuffer(opt.value);
    }

    private static it.cavallium.rockserver.core.common.Delta<Buf> mapDelta(it.cavallium.rockserver.core.common.api.Delta delta) {
        var requiredDelta = requireClientField(delta, "Thrift peer delta response");
        return new it.cavallium.rockserver.core.common.Delta<>(
                requiredDelta.isSetPrevious() ? Utils.fromByteBuffer(requiredDelta.previous) : null,
                requiredDelta.isSetCurrent() ? Utils.fromByteBuffer(requiredDelta.current) : null
        );
    }

    private static List<Buf> mapOptionalBinaryList(List<OptionalBinary> list) {
        var requiredList = requireClientField(list, "Thrift peer response entries");
        TransportRequestLimits.requireBatchEntries("Thrift peer response entries", requiredList.size());
        List<Buf> result = new ArrayList<>(requiredList.size());
        for (OptionalBinary b : requiredList) {
            result.add(mapOptionalBinary(b));
        }
        return result;
    }

    private static KV mapKV(it.cavallium.rockserver.core.common.api.KV kv) {
        var requiredKv = requireClientField(kv, "Thrift peer KV");
        var thriftKeys = requireClientField(requiredKv.getKeys(), "Thrift peer KV keys");
        TransportRequestLimits.requireKeyParts("Thrift peer key parts", thriftKeys.size());
        Buf[] keys = new Buf[thriftKeys.size()];
        for (int i = 0; i < thriftKeys.size(); i++) {
            keys[i] = Utils.fromByteBuffer(requireClientField(thriftKeys.get(i),
                    "Thrift peer KV key part " + i));
        }
        return new KV(new Keys(keys),
                requiredKv.isSetValue() ? Utils.fromByteBuffer(requiredKv.bufferForValue()) : null);
    }

    private static void requireMatchingBatchSize(String context, int keyCount, int valueCount) {
        if (keyCount != valueCount) {
            throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
                    context + " keys count " + keyCount + " does not match values count " + valueCount);
        }
    }

    private static <T> T requireClientField(T value, String description) {
        if (value == null) {
            throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
                    description + " must not be null");
        }
        return value;
    }

    private static <T> T requireRequestType(T requestType) {
        if (requestType == null) {
            throw RocksDBException.of(RocksDBErrorType.NULL_ARGUMENT, "Request type cannot be null");
        }
        return requestType;
    }

    private static it.cavallium.rockserver.core.common.api.PutBatchMode mapPutBatchMode(PutBatchMode mode) {
        return switch (requireClientField(mode, "Thrift client put batch mode")) {
            case WRITE_BATCH -> it.cavallium.rockserver.core.common.api.PutBatchMode.WRITE_BATCH;
            case WRITE_BATCH_NO_WAL -> it.cavallium.rockserver.core.common.api.PutBatchMode.WRITE_BATCH_NO_WAL;
            case SST_INGESTION -> it.cavallium.rockserver.core.common.api.PutBatchMode.SST_INGESTION;
            case SST_INGEST_BEHIND -> it.cavallium.rockserver.core.common.api.PutBatchMode.SST_INGEST_BEHIND;
        };
    }

    private static it.cavallium.rockserver.core.common.api.MergeBatchMode mapMergeBatchMode(MergeBatchMode mode) {
        return switch (requireClientField(mode, "Thrift client merge batch mode")) {
            case MERGE_WRITE_BATCH -> it.cavallium.rockserver.core.common.api.MergeBatchMode.MERGE_WRITE_BATCH;
            case MERGE_WRITE_BATCH_NO_WAL ->
                    it.cavallium.rockserver.core.common.api.MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL;
            case MERGE_SST_INGESTION -> it.cavallium.rockserver.core.common.api.MergeBatchMode.MERGE_SST_INGESTION;
            case MERGE_SST_INGEST_BEHIND ->
                    it.cavallium.rockserver.core.common.api.MergeBatchMode.MERGE_SST_INGEST_BEHIND;
        };
    }

    private static it.cavallium.rockserver.core.common.api.ColumnSchema mapSchema(ColumnSchema schema) {
        var requiredSchema = requireClientField(schema, "Thrift client column schema");
        it.cavallium.rockserver.core.common.api.ColumnSchema s = new it.cavallium.rockserver.core.common.api.ColumnSchema();
        s.setFixedKeys(new ArrayList<>(requiredSchema.keys()));
        s.setVariableTailKeys(requiredSchema.variableTailKeys().stream()
                .map(t -> it.cavallium.rockserver.core.common.api.ColumnHashType.valueOf(t.name()))
                .collect(Collectors.toList()));
        s.setHasValue(requiredSchema.hasValue());
        if (requiredSchema.mergeOperatorName() != null) {
            s.setMergeOperatorName(requiredSchema.mergeOperatorName());
        }
        if (requiredSchema.mergeOperatorVersion() != null) {
            s.setMergeOperatorVersion(requiredSchema.mergeOperatorVersion());
        }
        return s;
    }

    private static ColumnSchema mapSchemaFromThrift(it.cavallium.rockserver.core.common.api.ColumnSchema schema) {
        var requiredSchema = requireClientField(schema, "Thrift peer column schema");
        return ColumnSchema.of(
                it.unimi.dsi.fastutil.ints.IntArrayList.wrap(requireClientField(requiredSchema.getFixedKeys(),
                        "Thrift peer column schema fixedKeys").stream().mapToInt(i -> i).toArray()),
                it.unimi.dsi.fastutil.objects.ObjectArrayList.wrap(requireClientField(requiredSchema.getVariableTailKeys(),
                        "Thrift peer column schema variableTailKeys").stream()
                        .map(t -> it.cavallium.rockserver.core.common.ColumnHashType.valueOf(t.name()))
                        .toArray(it.cavallium.rockserver.core.common.ColumnHashType[]::new)),
                requiredSchema.isHasValue(),
                requiredSchema.isSetMergeOperatorName() ? requiredSchema.getMergeOperatorName() : null,
                requiredSchema.isSetMergeOperatorVersion() ? requiredSchema.getMergeOperatorVersion() : null
        );
    }
}
