package it.cavallium.rockserver.core.server;

import static it.cavallium.rockserver.core.common.Utils.asByteBuffer;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.TransportRequestLimits;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.Column;
import it.cavallium.rockserver.core.common.api.ColumnHashType;
import it.cavallium.rockserver.core.common.api.ColumnSchema;
import it.cavallium.rockserver.core.common.api.Delta;
import it.cavallium.rockserver.core.common.api.FirstAndLast;
import it.cavallium.rockserver.core.common.api.KV;
import it.cavallium.rockserver.core.common.api.MergeBatchMode;
import it.cavallium.rockserver.core.common.api.OptionalBinary;
import it.cavallium.rockserver.core.common.api.PutBatchMode;
import it.cavallium.rockserver.core.common.api.RocksDB.Iface;
import it.cavallium.rockserver.core.common.api.RocksDB.Processor;
import it.cavallium.rockserver.core.common.api.RocksDBThriftException;
import it.cavallium.rockserver.core.common.api.UpdateBegin;
import it.cavallium.rockserver.core.replication.ratis.RatisServerLifecycle;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.io.IOException;

import it.cavallium.buffer.Buf;

import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class ThriftServer extends Server {

    private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class.getName());
    private static final int THRIFT_WORKER_THREADS = 5;
    private static final int THRIFT_WORKER_SHUTDOWN_TIMEOUT_SECONDS = 30;
    private static final int THRIFT_SERVER_SHUTDOWN_TIMEOUT_SECONDS = THRIFT_WORKER_SHUTDOWN_TIMEOUT_SECONDS + 5;
    private static final int THRIFT_WORKER_FORCE_SHUTDOWN_TIMEOUT_SECONDS = 5;

    private final Thread thriftThread;
    private final ExecutorService thriftWorkerExecutor;
    private final TThreadedSelectorServer server;
    private boolean closed;

    public ThriftServer(RocksDBConnection client, String http2Host, int http2Port) throws IOException {
        this(client, http2Host, http2Port, null);
    }

    public ThriftServer(RocksDBConnection client,
                        String http2Host,
                        int http2Port,
                        @Nullable RatisServerLifecycle ratisLifecycle) throws IOException {
        super(client, ratisLifecycle);
        var handler = new ThriftHandler(this.getClient());

        try {
            var serverTransport = new TNonblockingServerSocket(new InetSocketAddress(http2Host, http2Port));
            ExecutorService workerExecutor = null;
            try {
                workerExecutor = Executors.newFixedThreadPool(
                        THRIFT_WORKER_THREADS,
                        Thread.ofPlatform().name("Thrift worker-", 0).factory()
                );
                this.server = new TThreadedSelectorServer(new TThreadedSelectorServer.Args(serverTransport)
                        .processor(new Processor<>(handler))
                        .executorService(workerExecutor)
                        .stopTimeoutVal(THRIFT_WORKER_SHUTDOWN_TIMEOUT_SECONDS)
                        .stopTimeoutUnit(TimeUnit.SECONDS)
                );
                this.thriftThread = Thread.ofPlatform().name("Thrift server thread").unstarted(server::serve);
                this.thriftWorkerExecutor = workerExecutor;
            } catch (RuntimeException | Error e) {
                if (workerExecutor != null) {
                    suppressConstructorCleanupFailure(e, workerExecutor::shutdownNow);
                }
                suppressConstructorCleanupFailure(e, serverTransport::close);
                throw e;
            }
            LOG.info("Thrift RocksDB server is listening at {}:{}", http2Host, http2Port);
        } catch (TTransportException e) {
            throw new IOException("Can't open server socket", e);
        }
    }

    public synchronized void start() throws IOException {
        if (closed) {
            throw new IllegalStateException("Thrift server is closed");
        }
        startRatisThenPublicEndpoint(thriftThread::start, this::closePublicEndpoint);
    }

    private static void suppressConstructorCleanupFailure(Throwable failure, Runnable cleanup) {
        try {
            cleanup.run();
        } catch (RuntimeException | Error cleanupFailure) {
            if (failure != cleanupFailure) {
                failure.addSuppressed(cleanupFailure);
            }
        }
    }

    private static @NotNull List<@NotNull Keys> keysToRecords(@Nullable List<@Nullable List<@NotNull ByteBuffer>> keysMulti) {
        var requiredKeysMulti = requireThriftField(keysMulti, "Thrift multi-key entries");
        TransportRequestLimits.requireBatchEntries("Thrift multi-key entries", requiredKeysMulti.size());
        var result = new ArrayList<Keys>(requiredKeysMulti.size());
        for (int i = 0; i < requiredKeysMulti.size(); i++) {
            result.add(requiredKeysToRecord("Thrift multi-key entry " + i, requiredKeysMulti.get(i)));
        }
        return List.copyOf(result);
    }

    private static @Nullable Keys keysToRecord(@Nullable List<@NotNull ByteBuffer> keys) {
        if (keys == null) {
            return null;
        }
        return keysToRecord("Thrift optional key boundary", keys);
    }

    private static @NotNull Keys requiredKeysToRecord(String description, @Nullable List<@NotNull ByteBuffer> keys) {
        return keysToRecord(description, requireThriftField(keys, description));
    }

    private static @NotNull Keys keysToRecord(String description, List<@NotNull ByteBuffer> keys) {
        TransportRequestLimits.requireKeyParts("Thrift key parts", keys.size());
        var result = new Buf[keys.size()];
        int i = 0;
        for (ByteBuffer key : keys) {
            result[i] = keyToRecord(description + " part " + i, key);
            i++;
        }
        return new Keys(result);
    }

    private static @NotNull List<@NotNull Buf> keyToRecords(@Nullable List<@Nullable ByteBuffer> keyMulti) {
        var requiredKeyMulti = requireThriftField(keyMulti, "Thrift value entries");
        TransportRequestLimits.requireBatchEntries("Thrift value entries", requiredKeyMulti.size());
        var result = new ArrayList<Buf>(requiredKeyMulti.size());
        for (int i = 0; i < requiredKeyMulti.size(); i++) {
            result.add(keyToRecord("Thrift value entry " + i, requiredKeyMulti.get(i)));
        }
        return List.copyOf(result);
    }

    private static @NotNull Buf keyToRecord(@Nullable ByteBuffer key) {
        return keyToRecord("Thrift binary value", key);
    }

    private static @NotNull Buf keyToRecord(String description, @Nullable ByteBuffer key) {
        return Utils.fromByteBuffer(requireThriftField(key, description));
    }

    private static <T> @NotNull T requireThriftField(@Nullable T value, String description) {
        if (value == null) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    description + " must not be null");
        }
        return value;
    }

    private static it.cavallium.rockserver.core.common.ColumnSchema columnSchemaToRecord(@Nullable ColumnSchema schema) {
        var requiredSchema = requireThriftField(schema, "Thrift column schema");
        return it.cavallium.rockserver.core.common.ColumnSchema.of(
                new IntArrayList(requireThriftField(requiredSchema.getFixedKeys(),
                        "Thrift column schema fixedKeys")),
                hashTypesToRecord(requiredSchema.getVariableTailKeys()),
                requiredSchema.isHasValue(),
                requiredSchema.isSetMergeOperatorName() ? requiredSchema.getMergeOperatorName() : null,
                requiredSchema.isSetMergeOperatorVersion() ? requiredSchema.getMergeOperatorVersion() : null
        );
    }

    private static ColumnSchema mapSchemaToThrift(it.cavallium.rockserver.core.common.ColumnSchema schema) {
        ColumnSchema s = new ColumnSchema();
        s.setFixedKeys(new ArrayList<>(schema.keys()));
        s.setVariableTailKeys(schema.variableTailKeys().stream()
                .map(t -> ColumnHashType.valueOf(t.name()))
                .collect(Collectors.toList()));
        s.setHasValue(schema.hasValue());
        if (schema.mergeOperatorName() != null) {
            s.setMergeOperatorName(schema.mergeOperatorName());
        }
        if (schema.mergeOperatorVersion() != null) {
            s.setMergeOperatorVersion(schema.mergeOperatorVersion());
        }
        return s;
    }

    private static ObjectArrayList<it.cavallium.rockserver.core.common.ColumnHashType> hashTypesToRecord(
            @Nullable List<@Nullable ColumnHashType> variableTailKeys) {
        var requiredVariableTailKeys = requireThriftField(variableTailKeys,
                "Thrift column schema variableTailKeys");
        var result = new ObjectArrayList<it.cavallium.rockserver.core.common.ColumnHashType>();
        for (int i = 0; i < requiredVariableTailKeys.size(); i++) {
            result.add(hashTypeToRecord("Thrift column schema variableTailKeys entry " + i,
                    requiredVariableTailKeys.get(i)));
        }
        return result;
    }

    private static it.cavallium.rockserver.core.common.ColumnHashType hashTypeToRecord(String description,
                                                                                       @Nullable ColumnHashType variableTailKey) {
        var requiredVariableTailKey = requireThriftField(variableTailKey, description);
        try {
            return it.cavallium.rockserver.core.common.ColumnHashType.valueOf(requiredVariableTailKey.name());
        } catch (IllegalArgumentException e) {
            throw RocksDBException.of(RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
                    description + " is unsupported: " + requiredVariableTailKey,
                    e);
        }
    }

    private static OptionalBinary mapResult(Buf buf) {
        var result = new OptionalBinary();
        return buf != null ? result.setValue(asByteBuffer(buf)) : result;
    }

    private static UpdateBegin mapResult(UpdateContext<Buf> context) {
        return new UpdateBegin()
                .setUpdateId(context.updateId())
                .setPrevious(context.previous() != null ? asByteBuffer(context.previous()) : null);
    }

    private static Delta mapResult(it.cavallium.rockserver.core.common.Delta<Buf> delta) {
        return new Delta()
                .setPrevious(delta.previous() != null ? asByteBuffer(delta.previous()) : null)
                .setCurrent(delta.current() != null ? asByteBuffer(delta.current()) : null);
    }

    private static List<OptionalBinary> mapResult(List<Buf> multi) {
        return multi.stream().map(ThriftServer::mapResult).toList();
    }

    private static KV mapKV(it.cavallium.rockserver.core.common.KV kv) {
        List<ByteBuffer> keys = new ArrayList<>();
        for (Buf b : kv.keys().keys()) {
            keys.add(asByteBuffer(b));
        }
        var result = new KV().setKeys(keys);
        if (kv.value() != null) {
            result.setValue(asByteBuffer(kv.value()));
        }
        return result;
    }

    private static FirstAndLast mapFirstAndLast(it.cavallium.rockserver.core.common.FirstAndLast<it.cavallium.rockserver.core.common.KV> fl) {
        return new FirstAndLast()
                .setFirst(fl.first() != null ? mapKV(fl.first()) : null)
                .setLast(fl.last() != null ? mapKV(fl.last()) : null);
    }

    private record ThriftHandler(RocksDBConnection client) implements Iface {

        private RocksDBThriftException mapException(it.cavallium.rockserver.core.common.RocksDBException e) {
            return new RocksDBThriftException(
                    it.cavallium.rockserver.core.common.api.RocksDBErrorType.valueOf(e.getErrorUniqueId().name()),
                    e.getMessage()
            );
        }

        @Override
        public long openTransaction(long timeoutMs) throws RocksDBThriftException {
            try {
                return client.getSyncApi().openTransaction(timeoutMs);
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public boolean closeTransaction(long transactionId, boolean commit) throws RocksDBThriftException {
            try {
                return client.getSyncApi().closeTransaction(transactionId, commit);
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void closeFailedUpdate(long updateId) throws RocksDBThriftException {
            try {
                client.getSyncApi().closeFailedUpdate(updateId);
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public long createColumn(String name, ColumnSchema schema) throws RocksDBThriftException {
            try {
                return client.getSyncApi().createColumn(name, columnSchemaToRecord(schema));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void deleteColumn(long columnId) throws RocksDBThriftException {
            try {
                client.getSyncApi().deleteColumn(columnId);
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public long getColumnId(String name) throws RocksDBThriftException {
            try {
                return client.getSyncApi().getColumnId(name);
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void putFast(long transactionOrUpdateId,
                            long columnId,
                            List<ByteBuffer> keys,
                            ByteBuffer value) {
            try {
                this.put(transactionOrUpdateId, columnId, keys, value);
            } catch (Exception e) {
                // Oneway cannot throw exception
                LOG.error("Error in putFast", e);
            }
        }

        @Override
        public void put(long transactionOrUpdateId,
                        long columnId,
                        List<ByteBuffer> keys,
                        ByteBuffer value) throws RocksDBThriftException {
            try {
                client.getSyncApi().put(transactionOrUpdateId, columnId, requiredKeysToRecord("Thrift put keys", keys), keyToRecord(value), RequestType.none());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void putMulti(long transactionOrUpdateId,
                             long columnId,
                             List<List<ByteBuffer>> keysMulti,
                             List<ByteBuffer> valueMulti) throws RocksDBThriftException {
            try {
                client.getSyncApi().putMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti), keyToRecords(valueMulti), RequestType.none());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public OptionalBinary putGetPrevious(long transactionOrUpdateId,
                                             long columnId,
                                             List<ByteBuffer> keys,
                                             ByteBuffer value) throws RocksDBThriftException {
            try {
                return ThriftServer.mapResult(client.getSyncApi().put(transactionOrUpdateId, columnId, requiredKeysToRecord("Thrift put keys", keys), keyToRecord(value), RequestType.previous()));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public Delta putGetDelta(long transactionOrUpdateId,
                                 long columnId,
                                 List<ByteBuffer> keys,
                                 ByteBuffer value) throws RocksDBThriftException {
            try {
                return ThriftServer.mapResult(client.getSyncApi().put(transactionOrUpdateId, columnId, requiredKeysToRecord("Thrift put keys", keys), keyToRecord(value), RequestType.delta()));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public boolean putGetChanged(long transactionOrUpdateId,
                                     long columnId,
                                     List<ByteBuffer> keys,
                                     ByteBuffer value) throws RocksDBThriftException {
            try {
                return client.getSyncApi().put(transactionOrUpdateId, columnId, requiredKeysToRecord("Thrift put keys", keys), keyToRecord(value), RequestType.changed());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public boolean putGetPreviousPresence(long transactionOrUpdateId,
                                              long columnId,
                                              List<ByteBuffer> keys,
                                              ByteBuffer value) throws RocksDBThriftException {
            try {
                return client.getSyncApi().put(transactionOrUpdateId, columnId, requiredKeysToRecord("Thrift put keys", keys), keyToRecord(value), RequestType.previousPresence());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public OptionalBinary get(long transactionOrUpdateId,
                                  long columnId,
                                  List<ByteBuffer> keys) throws RocksDBThriftException {
            try {
                return ThriftServer.mapResult(client.getSyncApi().get(transactionOrUpdateId, columnId, requiredKeysToRecord("Thrift get keys", keys), RequestType.current()));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public UpdateBegin getForUpdate(long transactionOrUpdateId,
                                        long columnId,
                                        List<ByteBuffer> keys) throws RocksDBThriftException {
            try {
                return mapResult(client.getSyncApi().get(transactionOrUpdateId, columnId, requiredKeysToRecord("Thrift get keys", keys), RequestType.forUpdate()));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public boolean exists(long transactionOrUpdateId,
                              long columnId,
                              List<ByteBuffer> keys) throws RocksDBThriftException {
            try {
                return client.getSyncApi().get(transactionOrUpdateId, columnId, requiredKeysToRecord("Thrift get keys", keys), RequestType.exists());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public long openIterator(long transactionId,
                                 long columnId,
                                 List<ByteBuffer> startKeysInclusive,
                                 List<ByteBuffer> endKeysExclusive,
                                 boolean reverse,
                                 long timeoutMs) throws RocksDBThriftException {
            try {
                return client.getSyncApi().openIterator(transactionId, columnId, keysToRecord(startKeysInclusive), keysToRecord(endKeysExclusive), reverse, timeoutMs);
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void closeIterator(long iteratorId) throws RocksDBThriftException {
            try {
                client.getSyncApi().closeIterator(iteratorId);
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void seekTo(long iterationId, List<ByteBuffer> keys) throws RocksDBThriftException {
            try {
                client.getSyncApi().seekTo(iterationId, requiredKeysToRecord("Thrift iterator seek keys", keys));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void subsequent(long iterationId, long skipCount, long takeCount) throws RocksDBThriftException {
            try {
                client.getSyncApi().subsequent(iterationId, skipCount, takeCount, RequestType.none());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public boolean subsequentExists(long iterationId,
                                        long skipCount,
                                        long takeCount) throws RocksDBThriftException {
            try {
                return client.getSyncApi().subsequent(iterationId, skipCount, takeCount, RequestType.exists());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public List<OptionalBinary> subsequentMultiGet(long iterationId,
                                                       long skipCount,
                                                       long takeCount) throws RocksDBThriftException {
            try {
                return mapResult(client.getSyncApi().subsequent(iterationId, skipCount, takeCount, RequestType.multi()));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void putBatch(long columnId, List<KV> data, PutBatchMode mode) throws RocksDBThriftException {
            try {
                client.getSyncApi().putBatch(columnId, kvToBatch(data), mapPutBatchMode(mode));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void merge(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys, ByteBuffer value) throws RocksDBThriftException {
            try {
                client.getSyncApi().merge(transactionOrUpdateId, columnId, requiredKeysToRecord("Thrift merge keys", keys), keyToRecord(value), RequestType.none());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void mergeMulti(long transactionOrUpdateId, long columnId, List<List<ByteBuffer>> keysMulti, List<ByteBuffer> valueMulti) throws RocksDBThriftException {
            try {
                client.getSyncApi().mergeMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti), keyToRecords(valueMulti), RequestType.none());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void mergeBatch(long columnId, List<KV> data, MergeBatchMode mode) throws RocksDBThriftException {
            try {
                client.getSyncApi().mergeBatch(columnId, kvToBatch(data), mapMergeBatchMode(mode));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public OptionalBinary mergeGetMerged(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys, ByteBuffer value) throws RocksDBThriftException {
            try {
                return ThriftServer.mapResult(client.getSyncApi().merge(transactionOrUpdateId, columnId, requiredKeysToRecord("Thrift merge keys", keys), keyToRecord(value), RequestType.merged()));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public List<OptionalBinary> mergeMultiGetMerged(long transactionOrUpdateId, long columnId, List<List<ByteBuffer>> keysMulti, List<ByteBuffer> valueMulti) throws RocksDBThriftException {
            try {
                return ThriftServer.mapResult(client.getSyncApi().mergeMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti), keyToRecords(valueMulti), RequestType.merged()));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public long uploadMergeOperator(String operatorName, String className, ByteBuffer jarPayload) throws RocksDBThriftException {
            try {
                return client.getSyncApi().uploadMergeOperator(operatorName,
                        className,
                        Utils.toByteArray(keyToRecord("Thrift merge-operator jar payload", jarPayload)));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public FirstAndLast reduceRangeFirstAndLast(long transactionId, long columnId, List<ByteBuffer> startKeysInclusive, List<ByteBuffer> endKeysExclusive, boolean reverse, long timeoutMs) throws RocksDBThriftException {
            try {
                return mapFirstAndLast(client.getSyncApi().reduceRange(transactionId, columnId, keysToRecord(startKeysInclusive), keysToRecord(endKeysExclusive), reverse, RequestType.firstAndLast(), timeoutMs));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public long reduceRangeEntriesCount(long transactionId, long columnId, List<ByteBuffer> startKeysInclusive, List<ByteBuffer> endKeysExclusive, boolean reverse, long timeoutMs) throws RocksDBThriftException {
            try {
                return client.getSyncApi().reduceRange(transactionId, columnId, keysToRecord(startKeysInclusive), keysToRecord(endKeysExclusive), reverse, RequestType.entriesCount(), timeoutMs);
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public List<KV> getAllInRange(long transactionId, long columnId, List<ByteBuffer> startKeysInclusive, List<ByteBuffer> endKeysExclusive, boolean reverse, long timeoutMs) throws RocksDBThriftException {
            try {
                return client.getSyncApi().getRange(transactionId, columnId, keysToRecord(startKeysInclusive), keysToRecord(endKeysExclusive), reverse, RequestType.allInRange(), timeoutMs)
                        .map(ThriftServer::mapKV)
                        .collect(Collectors.toList());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public List<OptionalBinary> putMultiGetPrevious(long transactionOrUpdateId, long columnId, List<List<ByteBuffer>> keysMulti, List<ByteBuffer> valueMulti) throws RocksDBThriftException {
            try {
                return mapResult(client.getSyncApi().putMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti), keyToRecords(valueMulti), RequestType.previous()));
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public List<Delta> putMultiGetDelta(long transactionOrUpdateId, long columnId, List<List<ByteBuffer>> keysMulti, List<ByteBuffer> valueMulti) throws RocksDBThriftException {
            try {
                return client.getSyncApi().putMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti), keyToRecords(valueMulti), RequestType.delta())
                        .stream().map(ThriftServer::mapResult).collect(Collectors.toList());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public List<Boolean> putMultiGetChanged(long transactionOrUpdateId, long columnId, List<List<ByteBuffer>> keysMulti, List<ByteBuffer> valueMulti) throws RocksDBThriftException {
            try {
                return client.getSyncApi().putMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti), keyToRecords(valueMulti), RequestType.changed());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public List<Boolean> putMultiGetPreviousPresence(long transactionOrUpdateId, long columnId, List<List<ByteBuffer>> keysMulti, List<ByteBuffer> valueMulti) throws RocksDBThriftException {
            try {
                return client.getSyncApi().putMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti), keyToRecords(valueMulti), RequestType.previousPresence());
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void flush() throws RocksDBThriftException {
            try {
                client.getSyncApi().flush();
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public void compact() throws RocksDBThriftException {
            try {
                client.getSyncApi().compact();
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }

        @Override
        public List<Column> getAllColumnDefinitions() throws RocksDBThriftException {
            try {
                var map = client.getSyncApi().getAllColumnDefinitions();
                List<Column> columns = new ArrayList<>();
                map.forEach((name, schema) -> columns.add(new Column(name, mapSchemaToThrift(schema))));
                return columns;
            } catch (it.cavallium.rockserver.core.common.RocksDBException e) {
                throw mapException(e);
            }
        }
    }

    private static Flux<it.cavallium.rockserver.core.common.KVBatch> kvToBatch(List<KV> data) {
        if (data == null || data.isEmpty()) return Flux.empty();
        TransportRequestLimits.requireBatchEntries("Thrift batch entries", data.size());
        List<Keys> keysList = new ArrayList<>(data.size());
        List<Buf> valuesList = new ArrayList<>(data.size());
        int index = 0;
        for (KV kv : data) {
            var requiredKv = requireThriftField(kv, "Thrift batch entry " + index);
            keysList.add(requiredKeysToRecord("Thrift batch entry " + index + " keys", requiredKv.getKeys()));
            valuesList.add(keyToRecord("Thrift batch entry " + index + " value", requiredKv.bufferForValue()));
            index++;
        }
        return Flux.just(new KVBatchRef(keysList, valuesList));
    }

    private static it.cavallium.rockserver.core.common.MergeBatchMode mapMergeBatchMode(MergeBatchMode mode) {
        return switch (requireThriftField(mode, "Thrift merge batch mode")) {
            case MERGE_WRITE_BATCH -> it.cavallium.rockserver.core.common.MergeBatchMode.MERGE_WRITE_BATCH;
            case MERGE_WRITE_BATCH_NO_WAL ->
                    it.cavallium.rockserver.core.common.MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL;
            case MERGE_SST_INGESTION -> it.cavallium.rockserver.core.common.MergeBatchMode.MERGE_SST_INGESTION;
            case MERGE_SST_INGEST_BEHIND -> it.cavallium.rockserver.core.common.MergeBatchMode.MERGE_SST_INGEST_BEHIND;
        };
    }

    private static it.cavallium.rockserver.core.common.PutBatchMode mapPutBatchMode(PutBatchMode mode) {
        return switch (requireThriftField(mode, "Thrift put batch mode")) {
            case WRITE_BATCH -> it.cavallium.rockserver.core.common.PutBatchMode.WRITE_BATCH;
            case WRITE_BATCH_NO_WAL -> it.cavallium.rockserver.core.common.PutBatchMode.WRITE_BATCH_NO_WAL;
            case SST_INGESTION -> it.cavallium.rockserver.core.common.PutBatchMode.SST_INGESTION;
            case SST_INGEST_BEHIND -> it.cavallium.rockserver.core.common.PutBatchMode.SST_INGEST_BEHIND;
        };
    }

    @Override
    public void close() throws IOException {
        synchronized (this) {
            closed = true;
        }
        closePublicEndpointThenRatis(this::closePublicEndpoint);
    }

    private void closePublicEndpoint() throws IOException {
        LOG.info("Thrift server is shutting down...");
        closeEndpointResources(this::stopThriftServer,
                this::awaitThriftThreadTermination,
                this::closeThriftWorkerExecutor);
    }

    private void stopThriftServer() {
        server.stop();
    }

    private void awaitThriftThreadTermination() {
        awaitThreadTermination(thriftThread,
                TimeUnit.SECONDS.toMillis(THRIFT_SERVER_SHUTDOWN_TIMEOUT_SECONDS));
    }

    static void awaitThreadTermination(Thread thread, long timeoutMillis) {
        if (timeoutMillis <= 0) {
            throw new IllegalArgumentException("timeoutMillis must be positive");
        }
        var interruption = new CloseInterruption();
        var deadlineNanos = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        try {
            while (thread.isAlive()) {
                var remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    break;
                }
                try {
                    TimeUnit.NANOSECONDS.timedJoin(thread, remainingNanos);
                } catch (InterruptedException e) {
                    LOG.error("Thrift server shutdown interrupted", e);
                    interruption.record();
                }
            }
        } finally {
            interruption.restore();
        }
        if (thread.isAlive()) {
            throw new IllegalStateException("Thrift server thread did not terminate after shutdown");
        }
    }

    private void closeThriftWorkerExecutor() {
        closeThriftWorkerExecutor(thriftWorkerExecutor);
    }

    private static void closeThriftWorkerExecutor(ExecutorService thriftWorkerExecutor) {
        var interruption = new CloseInterruption();
        thriftWorkerExecutor.shutdown();
        try {
            if (workerExecutorTerminationTimedOut(thriftWorkerExecutor, interruption)) {
                thriftWorkerExecutor.shutdownNow();
                if (workerExecutorTerminationTimedOut(thriftWorkerExecutor, interruption)) {
                    throw new IllegalStateException("Thrift worker executor did not terminate after forced shutdown");
                }
            }
        } finally {
            interruption.restore();
        }
    }

    private static boolean workerExecutorTerminationTimedOut(ExecutorService thriftWorkerExecutor,
                                                            CloseInterruption interruption) {
        var deadlineNanos = System.nanoTime() + TimeUnit.SECONDS.toNanos(THRIFT_WORKER_FORCE_SHUTDOWN_TIMEOUT_SECONDS);
        while (true) {
            try {
                var remainingNanos = deadlineNanos - System.nanoTime();
                if (remainingNanos <= 0) {
                    return true;
                }
                return !thriftWorkerExecutor.awaitTermination(remainingNanos, TimeUnit.NANOSECONDS);
            } catch (InterruptedException e) {
                LOG.error("Thrift worker executor shutdown interrupted", e);
                interruption.record();
                thriftWorkerExecutor.shutdownNow();
            }
        }
    }

    private static final class CloseInterruption {

        private boolean interrupted;

        void record() {
            interrupted = true;
        }

        void restore() {
            if (interrupted) {
                Thread.currentThread().interrupt();
            }
        }
    }
}
