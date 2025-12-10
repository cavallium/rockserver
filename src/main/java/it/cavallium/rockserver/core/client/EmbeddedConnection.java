package it.cavallium.rockserver.core.client;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.RequestType.RequestGet;
import it.cavallium.rockserver.core.common.RequestType.RequestMerge;
import it.cavallium.rockserver.core.common.RequestType.RequestPut;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;
import java.io.IOException;
import java.net.URI;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import java.util.concurrent.CompletableFuture;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

public class EmbeddedConnection extends BaseConnection implements RocksDBAPI, InternalConnection {

	private final EmbeddedDB db;
	public static final URI PRIVATE_MEMORY_URL = URI.create("memory://private");

	public EmbeddedConnection(@Nullable Path path, String name, @Nullable Path embeddedConfig) throws IOException {
		super(name);
		this.db = new EmbeddedDB(path, name, embeddedConfig);
	}

	@Override
	public void close() throws IOException {
		db.close();
		super.close();
	}

	@Override
	public URI getUrl() {
		return Optional.ofNullable(db.getPath()).map(Path::toUri).orElse(PRIVATE_MEMORY_URL);
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
	public long openTransaction(long timeoutMs) {
		return db.openTransaction(timeoutMs);
	}

	@Override
	public boolean closeTransaction(long transactionId, boolean commit) {
		return db.closeTransaction(transactionId, commit);
	}

	@Override
	public void closeFailedUpdate(long updateId) throws RocksDBException {
		db.closeFailedUpdate(updateId);
	}

	@Override
	public long createColumn(String name, @NotNull ColumnSchema schema) {
		return db.createColumn(name, schema);
	}


	@Override
	public long uploadMergeOperator(String name, String className, byte[] jarData) throws RocksDBException {
		return db.uploadMergeOperator(name, className, jarData);
	}

	@Override
	public void deleteColumn(long columnId) throws RocksDBException {
		db.deleteColumn(columnId);
	}

	@Override
	public long getColumnId(@NotNull String name) {
		return db.getColumnId(name);
	}

	@Override
	public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> req) {
		return req.handleSync(this);
	}

	@SuppressWarnings("unchecked")
    @Override
    public <R, RS, RA> RA requestAsync(RocksDBAPICommand<R, RS, RA> req) {
        return (RA) switch (req) {
            case RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch putBatch -> this.putBatchAsync(putBatch.columnId(), putBatch.batchPublisher(), putBatch.mode());
            case RocksDBAPICommand.RocksDBAPICommandStream.GetRange<?> getRange -> this.getRangeAsync(getRange.transactionId(), getRange.columnId(), getRange.startKeysInclusive(), getRange.endKeysExclusive(), getRange.reverse(), getRange.requestType(), getRange.timeoutMs());
            case RocksDBAPICommand.RocksDBAPICommandStream.CdcPoll cdcPoll -> this.cdcPollAsync(cdcPoll.id(), cdcPoll.fromSeq(), cdcPoll.maxEvents());
            case RocksDBAPICommand.RocksDBAPICommandSingle<?> _ -> CompletableFuture.supplyAsync(() -> req.handleSync(this),
                    (req.isReadOnly() ? db.getScheduler().readExecutor() : db.getScheduler().writeExecutor()));
            case RocksDBAPICommand.RocksDBAPICommandStream<?> _ -> throw RocksDBException.of(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, "The request of type " + req.getClass().getName() + " is not implemented in class " + this.getClass().getName());
        };
    }

	@Override
	public <T> T put(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return db.put(transactionOrUpdateId, columnId, keys, value, requestType);
	}

	@Override
	public <T> T merge(long transactionOrUpdateId,
			long columnId,
			@NotNull Keys keys,
			@NotNull Buf value,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return db.merge(transactionOrUpdateId, columnId, keys, value, requestType);
	}

	@Override
	public <T> List<T> putMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestPut<? super Buf, T> requestType) throws RocksDBException {
		return db.putMulti(transactionOrUpdateId, columnId, keys, values, requestType);
	}

	@Override
	public <T> List<T> mergeMulti(long transactionOrUpdateId,
			long columnId,
			@NotNull List<Keys> keys,
			@NotNull List<@NotNull Buf> values,
			RequestMerge<? super Buf, T> requestType) throws RocksDBException {
		return db.mergeMulti(transactionOrUpdateId, columnId, keys, values, requestType);
	}

	@Override
	public CompletableFuture<Void> putBatchAsync(long columnId,
											 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
											 @NotNull PutBatchMode mode) throws RocksDBException {
		return db.putBatchInternal(columnId, batchPublisher, mode);
	}

	@Override
	public CompletableFuture<Void> mergeBatchAsync(long columnId,
											@NotNull Publisher<@NotNull KVBatch> batchPublisher,
											@NotNull MergeBatchMode mode) throws RocksDBException {
		return db.mergeBatchInternal(columnId, batchPublisher, mode);
	}

	@Override
	public void putBatch(long columnId,
					 @NotNull Publisher<@NotNull KVBatch> batchPublisher,
					 @NotNull PutBatchMode mode) throws RocksDBException {
		db.putBatch(columnId, batchPublisher, mode);
	}

	@Override
	public void mergeBatch(long columnId,
				   @NotNull Publisher<@NotNull KVBatch> batchPublisher,
				   @NotNull MergeBatchMode mode) throws RocksDBException {
		db.mergeBatch(columnId, batchPublisher, mode);
	}

	@Override
	public <T> T get(long transactionOrUpdateId,
			long columnId,
			Keys keys,
			RequestGet<? super Buf, T> requestType) throws RocksDBException {
		return db.get(transactionOrUpdateId, columnId, keys, requestType);
	}

	@Override
	public long openIterator(long transactionId,
			long columnId,
			@NotNull Keys startKeysInclusive,
			@Nullable Keys endKeysExclusive,
			boolean reverse,
			long timeoutMs) throws RocksDBException {
		return db.openIterator(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, timeoutMs);
	}

	@Override
	public void closeIterator(long iteratorId) throws RocksDBException {
		db.closeIterator(iteratorId);
	}

	@Override
	public void seekTo(long iterationId, Keys keys) throws RocksDBException {
		db.seekTo(iterationId, keys);
	}

	@Override
	public <T> T subsequent(long iterationId,
			long skipCount,
			long takeCount,
			@NotNull RequestType.RequestIterate<? super Buf, T> requestType) throws RocksDBException {
		return db.subsequent(iterationId, skipCount, takeCount, requestType);
	}

	@Override
	public <T> T reduceRange(long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.@NotNull RequestReduceRange<? super KV, T> requestType, long timeoutMs) throws RocksDBException {
		return db.reduceRange(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs);
	}

	@Override
	public <T> Stream<T> getRange(long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.@NotNull RequestGetRange<? super KV, T> requestType, long timeoutMs) throws RocksDBException {
		return db.getRange(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs);
	}

	@Override
	public <T> Publisher<T> getRangeAsync(long transactionId, long columnId, @Nullable Keys startKeysInclusive, @Nullable Keys endKeysExclusive, boolean reverse, RequestType.RequestGetRange<? super KV, T> requestType, long timeoutMs) throws RocksDBException {
		return db.getRangeAsyncInternal(transactionId, columnId, startKeysInclusive, endKeysExclusive, reverse, requestType, timeoutMs);
	}

	@Override
	public void flush() {
		db.flush();
	}

	@Override
	public void compact() {
		db.compact();
	}

	@Override
 public Map<String, ColumnSchema> getAllColumnDefinitions() throws RocksDBException {
        return db.getAllColumnDefinitions();
    }

    @Override
    public RWScheduler getScheduler() {
        return db.getScheduler();
    }

    @org.jetbrains.annotations.VisibleForTesting
    public EmbeddedDB getInternalDB() {
        return db;
    }

    // CDC API implementation stubs delegating to EmbeddedDB
    public long cdcCreate(@NotNull String id, @Nullable Long fromSeq, @Nullable List<Long> columnIds) throws RocksDBException {
        return db.cdcCreate(id, fromSeq, columnIds);
    }

    public void cdcDelete(@NotNull String id) throws RocksDBException {
        db.cdcDelete(id);
    }

    public void cdcCommit(@NotNull String id, long seq) throws RocksDBException {
        db.cdcCommit(id, seq);
    }

    public @NotNull java.util.stream.Stream<CDCEvent> cdcPoll(@NotNull String id, @Nullable Long fromSeq, long maxEvents) throws RocksDBException {
        return db.cdcPoll(id, fromSeq, maxEvents);
    }

    public @NotNull Publisher<CDCEvent> cdcPollAsync(@NotNull String id, @Nullable Long fromSeq, long maxEvents) throws RocksDBException {
        // Default: defer to DB implementation; fallback to blocking stream if async is not supported
        return db.cdcPollAsyncInternal(id, fromSeq, maxEvents);
    }
}
