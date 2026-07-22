package it.cavallium.rockserver.core.server;

import static it.cavallium.rockserver.core.common.Utils.asByteBuffer;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.ThriftTransportLimits;
import it.cavallium.rockserver.core.common.UpdateContext;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.WriteClass;
import it.cavallium.rockserver.core.common.api.Column;
import it.cavallium.rockserver.core.common.api.ColumnHashType;
import it.cavallium.rockserver.core.common.api.ColumnSchema;
import it.cavallium.rockserver.core.common.api.Delta;
import it.cavallium.rockserver.core.common.api.FirstAndLast;
import it.cavallium.rockserver.core.common.api.KV;
import it.cavallium.rockserver.core.common.api.MergeBatchMode;
import it.cavallium.rockserver.core.common.api.OptionalBinary;
import it.cavallium.rockserver.core.common.api.OptionalLongValue;
import it.cavallium.rockserver.core.common.api.PutBatchMode;
import it.cavallium.rockserver.core.common.api.RocksDBWriteClass.Iface;
import it.cavallium.rockserver.core.common.api.RocksDBWriteClass.Processor;
import it.cavallium.rockserver.core.common.api.RocksDBThriftException;
import it.cavallium.rockserver.core.common.api.UpdateBegin;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import java.io.IOException;
import it.cavallium.buffer.Buf;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CancellationException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.thrift.TConfiguration;
import org.reactivestreams.Publisher;
import org.apache.thrift.server.TThreadedSelectorServer;
import org.apache.thrift.transport.TNonblockingSocket;
import org.apache.thrift.transport.TNonblockingServerSocket;
import org.apache.thrift.transport.TTransportException;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.jetbrains.annotations.VisibleForTesting;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;

public class ThriftServer extends Server {

	private static final Logger LOG = LoggerFactory.getLogger(ThriftServer.class.getName());
	private static final int LEGACY_MAX_CDC_RESPONSE_SIZE = ThriftTransportLimits.safeCdcResponseSize(
			TConfiguration.DEFAULT_MAX_FRAME_SIZE);

	private final Thread thriftThread;
	private final TThreadedSelectorServer server;

	public ThriftServer(RocksDBConnection client, String http2Host, int http2Port) throws IOException {
		this(client, http2Host, http2Port, ThriftTransportLimits.configuredServerMaxFrameSize());
	}

	public ThriftServer(RocksDBConnection client,
			String http2Host,
			int http2Port,
			int maxFrameSize) throws IOException {
		super(client);
		int validatedMaxFrameSize = ThriftTransportLimits.validateServerMaxFrameSize(maxFrameSize);
		var handler = new ThriftHandler(this.getClient(),
				ThriftTransportLimits.safeCdcResponseSize(validatedMaxFrameSize));

		try {
			var serverTransport = new ConfiguredNonblockingServerSocket(
					new InetSocketAddress(http2Host, http2Port),
					validatedMaxFrameSize);
			this.server = new TThreadedSelectorServer(new TThreadedSelectorServer.Args(serverTransport)
					.processor(new Processor<>(handler))
			);

			this.thriftThread = Thread.ofPlatform().name("Thrift server thread").unstarted(server::serve);
			LOG.info("Thrift RocksDB server is listening at " + http2Host + ":" + http2Port);
		} catch (TTransportException e) {
			throw new IOException("Can't open server socket", e);
		}
	}

	private static final class ConfiguredNonblockingServerSocket extends TNonblockingServerSocket {

		private final int maxFrameSize;

		private ConfiguredNonblockingServerSocket(InetSocketAddress address, int maxFrameSize)
				throws TTransportException {
			super(address, 0, maxFrameSize);
			this.maxFrameSize = maxFrameSize;
		}

		@Override
		public TNonblockingSocket accept() throws TTransportException {
			var socket = super.accept();
			if (socket != null) {
				socket.getConfiguration().setMaxFrameSize(maxFrameSize);
				socket.getConfiguration().setMaxMessageSize(maxFrameSize);
			}
			return socket;
		}
	}

	public void start() {
		thriftThread.start();
	}

	private static @NotNull List<@NotNull Keys> keysToRecords(@NotNull List<@NotNull List< @NotNull ByteBuffer>> keysMulti) {
		return keysMulti.stream().map(ThriftServer::keysToRecord).toList();
	}

	private static Keys keysToRecord(List<@NotNull ByteBuffer> keys) {
		if (keys == null) {
			return null;
		}
		var result = new Buf[keys.size()];
		int i = 0;
		for (ByteBuffer key : keys) {
			result[i] = keyToRecord(key);
			i++;
		}
		return new Keys(result);
	}

	private static @NotNull List<@NotNull Buf> keyToRecords(@NotNull List<@NotNull ByteBuffer> keyMulti) {
		return keyMulti.stream().map(ThriftServer::keyToRecord).toList();
	}

	private static @NotNull Buf keyToRecord(@NotNull ByteBuffer key) {
		return Utils.fromByteBuffer(key);
	}

	private static it.cavallium.rockserver.core.common.ColumnSchema columnSchemaToRecord(ColumnSchema schema) {
		return it.cavallium.rockserver.core.common.ColumnSchema.of(new IntArrayList(schema.getFixedKeys()),
				hashTypesToRecord(schema.getVariableTailKeys()),
				schema.isHasValue(),
				schema.isSetMergeOperatorName() ? schema.getMergeOperatorName() : null,
				schema.isSetMergeOperatorVersion() ? schema.getMergeOperatorVersion() : null,
				schema.isSetMergeOperatorClass() ? schema.getMergeOperatorClass() : null
		);
	}

	private static ColumnSchema mapSchemaToThrift(it.cavallium.rockserver.core.common.ColumnSchema schema) {
		ColumnSchema s = new ColumnSchema();
		var fixedKeys = new ArrayList<Integer>(schema.fixedLengthKeysCount());
		for (int i = 0; i < schema.fixedLengthKeysCount(); i++) {
			fixedKeys.add(schema.key(i));
		}
		s.setFixedKeys(fixedKeys);
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
		if (schema.mergeOperatorClass() != null) {
			s.setMergeOperatorClass(schema.mergeOperatorClass());
		}
		return s;
	}

	private static ObjectArrayList<it.cavallium.rockserver.core.common.ColumnHashType> hashTypesToRecord(List<ColumnHashType> variableTailKeys) {
		var result = new ObjectArrayList<it.cavallium.rockserver.core.common.ColumnHashType>();
		for (ColumnHashType variableTailKey : variableTailKeys) {
			result.add(hashTypeToRecord(variableTailKey));
		}
		return result;
	}

	private static it.cavallium.rockserver.core.common.ColumnHashType hashTypeToRecord(ColumnHashType variableTailKey) {
		return it.cavallium.rockserver.core.common.ColumnHashType.valueOf(variableTailKey.name());
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
		return new KV(keys, asByteBuffer(kv.value()));
	}

	private static FirstAndLast mapFirstAndLast(it.cavallium.rockserver.core.common.FirstAndLast<it.cavallium.rockserver.core.common.KV> fl) {
		return new FirstAndLast()
				.setFirst(fl.first() != null ? mapKV(fl.first()) : null)
				.setLast(fl.last() != null ? mapKV(fl.last()) : null);
	}

	@VisibleForTesting
	public static RocksDBSyncAPI createDispatchingSyncApiForTesting(RocksDBConnection client) {
		return ThriftHandler.dispatchingSyncApi(client);
	}

	private static class ThriftHandler implements Iface {

		private final RocksDBSyncAPI api;
		private final RocksDBAsyncAPI asyncApi;
		private final int maxCdcResponseSize;

		public ThriftHandler(RocksDBConnection client, int maxCdcResponseSize) {
			this.api = dispatchingSyncApi(client);
			this.asyncApi = client.getAsyncApi();
			this.maxCdcResponseSize = maxCdcResponseSize;
		}

		private static RocksDBSyncAPI dispatchingSyncApi(RocksDBConnection client) {
			var syncApi = client.getSyncApi();
			var asyncApi = client.getAsyncApi();
			return new RocksDBSyncAPI() {
				@Override
				@SuppressWarnings("unchecked")
				public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
					if (!shouldDispatchAsync(request)) {
						// Preserve the original zero-hop fast path for ordinary point reads/writes.
						// Only operations that need bounded paging or a dedicated scheduler lane use
						// the async bridge below.
						return syncApi.requestSync(request);
					}
					Object asyncResult = asyncApi.requestAsync(request);
					if (asyncResult instanceof CompletableFuture<?> future) {
						try {
							return (RS) future.get();
						} catch (InterruptedException interrupted) {
							handleInterruptedFuture(request, asyncApi, future);
							Thread.currentThread().interrupt();
							throw it.cavallium.rockserver.core.common.RocksDBException.of(
									it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
									"Thrift request interrupted",
									interrupted);
						} catch (ExecutionException error) {
							throw propagateAsyncFailure(request, error.getCause());
						}
					}
					if (asyncResult instanceof Publisher<?> publisher) {
						return (RS) Flux.from(publisher).toStream();
					}
					throw new IllegalStateException("Unsupported async result for " + request.getClass().getName());
				}
			};
		}

		private static boolean shouldDispatchAsync(RocksDBAPICommand<?, ?, ?> request) {
			if (request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.PutBatch
					|| request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.MergeBatch
					|| request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator
					|| request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate
					|| request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction
					|| request instanceof RocksDBAPICommand.CdcCommit
					|| (request instanceof RocksDBAPICommand.CdcCreate create
					&& create.fromSeq() != null
					&& create.fromSeq() == 0L)
					|| request instanceof RocksDBAPICommand.Flush
					|| request instanceof RocksDBAPICommand.Compact) {
				return true;
			}
			return request.isReadOnly()
					&& request.readWorkClass() == RocksDBAPICommand.ReadWorkClass.COMPOSITE;
		}

		private static void handleInterruptedFuture(RocksDBAPICommand<?, ?, ?> request,
				RocksDBAsyncAPI asyncApi,
				CompletableFuture<?> future) {
			var cleanup = lateSuccessCleanup(request, asyncApi);
			if (cleanup != null) {
				// CompletableFuture.cancel() may report success even after its supplier has
				// entered a native call, while discarding the eventual resource id. Keep the
				// result observable and close an iterator/transaction that finishes after the
				// interrupted Thrift call has already returned.
				future.whenComplete((value, failure) -> {
					if (failure != null) {
						logLateInterruptedFailure(request, failure);
						return;
					}
					try {
						var cleanupFuture = cleanup.apply(value);
						if (cleanupFuture == null) {
							LOG.warn("Late Thrift resource cleanup returned no future: operation={}",
									operationName(request));
							return;
						}
						cleanupFuture.whenComplete((_, cleanupFailure) -> {
							if (cleanupFailure != null) {
								LOG.warn("Late Thrift resource cleanup failed: operation={}",
										operationName(request),
										unwrapCompletionFailure(cleanupFailure));
							}
						});
					} catch (Throwable cleanupFailure) {
						LOG.warn("Late Thrift resource cleanup could not be started: operation={}",
								operationName(request),
								cleanupFailure);
					}
				});
				return;
			}
			if (mustCompleteAfterInterrupt(request)) {
				// Suppressing a queued close would retain precisely the iterator/transaction
				// the caller asked us to release. The Thrift worker can return immediately,
				// but the release command itself must remain live.
				future.whenComplete((_, failure) -> {
					if (failure != null) {
						logLateInterruptedFailure(request, failure);
					}
				});
				return;
			}

			boolean cancelled = future.cancel(true);
			if (!cancelled) {
				future.whenComplete((_, failure) -> {
					if (failure != null) {
						logLateInterruptedFailure(request, failure);
					}
				});
			}
		}

		private static boolean mustCompleteAfterInterrupt(RocksDBAPICommand<?, ?, ?> request) {
			return request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseIterator
					|| request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseTransaction
					|| request instanceof RocksDBAPICommand.RocksDBAPICommandSingle.CloseFailedUpdate
					|| request instanceof RocksDBAPICommand.CdcCommit;
		}

		private static @Nullable Function<Object, CompletableFuture<?>> lateSuccessCleanup(
				RocksDBAPICommand<?, ?, ?> request,
				RocksDBAsyncAPI asyncApi) {
			return switch (request) {
				case RocksDBAPICommand.RocksDBAPICommandSingle.OpenIterator _ ->
						value -> asyncApi.closeIteratorAsync((Long) value);
				case RocksDBAPICommand.RocksDBAPICommandSingle.OpenTransaction _ ->
						value -> asyncApi.closeTransactionAsync((Long) value, false);
				case RocksDBAPICommand.RocksDBAPICommandSingle.Get<?> get
						when get.transactionOrUpdateId() == 0L
						&& get.requestType() instanceof RequestType.RequestForUpdate<?> ->
						value -> asyncApi.closeFailedUpdateAsync(((UpdateContext<?>) value).updateId());
				default -> null;
			};
		}

		private static RuntimeException propagateAsyncFailure(RocksDBAPICommand<?, ?, ?> request,
				@Nullable Throwable failure) {
			var unwrapped = unwrapCompletionFailure(failure);
			int depth = 0;
			for (var current = unwrapped; current != null && depth < 32;
					current = current.getCause(), depth++) {
				if (current instanceof it.cavallium.rockserver.core.common.RocksDBException rocksError) {
					return rocksError;
				}
				if (current.getCause() == current) {
					break;
				}
			}
			if (unwrapped instanceof RuntimeException runtimeError) {
				return runtimeError;
			}
			var cause = unwrapped != null
					? unwrapped
					: new IllegalStateException("Async request failed without a cause");
			return it.cavallium.rockserver.core.common.RocksDBException.of(
					it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
					"Thrift async request failed: " + operationName(request),
					cause);
		}

		private static @Nullable Throwable unwrapCompletionFailure(@Nullable Throwable failure) {
			var current = failure;
			while ((current instanceof CompletionException || current instanceof ExecutionException)
					&& current.getCause() != null
					&& current.getCause() != current) {
				current = current.getCause();
			}
			return current;
		}

		private static void logLateInterruptedFailure(RocksDBAPICommand<?, ?, ?> request, Throwable failure) {
			var unwrapped = unwrapCompletionFailure(failure);
			if (unwrapped instanceof CancellationException) {
				return;
			}
			LOG.debug("Thrift async request failed after its worker was interrupted: operation={}",
					operationName(request),
					unwrapped);
		}

		private static String operationName(RocksDBAPICommand<?, ?, ?> request) {
			return request.getClass().getSimpleName();
		}

		private RocksDBThriftException mapException(it.cavallium.rockserver.core.common.RocksDBException e) {
			return new RocksDBThriftException(
					it.cavallium.rockserver.core.common.api.RocksDBErrorType.valueOf(e.getErrorUniqueId().name()),
					e.getMessage()
			);
		}

		private WriteClass mapWriteClass(int wireWriteClass) {
			return switch (wireWriteClass) {
				case 0 -> WriteClass.FOREGROUND;
				case 1 -> WriteClass.MAINTENANCE;
				default -> throw it.cavallium.rockserver.core.common.RocksDBException.of(
						it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST,
						"Unknown write class: " + wireWriteClass);
			};
		}

		// The inherited RocksDB service is the legacy wire surface. Its missing
		// write-class field is defined to mean foreground.
		@Override
		public boolean closeTransaction(long transactionId, boolean commit) throws RocksDBThriftException {
			return closeTransactionWithWriteClass(transactionId, commit, 0);
		}

		@Override
		public long createColumn(String name, ColumnSchema schema) throws RocksDBThriftException {
			return createColumnWithWriteClass(name, schema, 0);
		}

		@Override
		public void deleteColumn(long columnId) throws RocksDBThriftException {
			deleteColumnWithWriteClass(columnId, 0);
		}

		@Override
		public boolean deleteColumnIfExists(String name) throws RocksDBThriftException {
			return deleteColumnIfExistsWithWriteClass(name, 0);
		}

		@Override
		public void putFast(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys, ByteBuffer value) {
			putFastWithWriteClass(transactionOrUpdateId, columnId, keys, value, 0);
		}

		@Override
		public void put(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys, ByteBuffer value)
				throws RocksDBThriftException {
			putWithWriteClass(transactionOrUpdateId, columnId, keys, value, 0);
		}

		@Override
		public void putMulti(long transactionOrUpdateId, long columnId, List<List<ByteBuffer>> keysMulti,
				List<ByteBuffer> valueMulti) throws RocksDBThriftException {
			putMultiWithWriteClass(transactionOrUpdateId, columnId, keysMulti, valueMulti, 0);
		}

		@Override
		public OptionalBinary putGetPrevious(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys,
				ByteBuffer value) throws RocksDBThriftException {
			return putGetPreviousWithWriteClass(transactionOrUpdateId, columnId, keys, value, 0);
		}

		@Override
		public Delta putGetDelta(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys,
				ByteBuffer value) throws RocksDBThriftException {
			return putGetDeltaWithWriteClass(transactionOrUpdateId, columnId, keys, value, 0);
		}

		@Override
		public boolean putGetChanged(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys,
				ByteBuffer value) throws RocksDBThriftException {
			return putGetChangedWithWriteClass(transactionOrUpdateId, columnId, keys, value, 0);
		}

		@Override
		public boolean putGetPreviousPresence(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys,
				ByteBuffer value) throws RocksDBThriftException {
			return putGetPreviousPresenceWithWriteClass(transactionOrUpdateId, columnId, keys, value, 0);
		}

		@Override
		public void delete(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys)
				throws RocksDBThriftException {
			deleteWithWriteClass(transactionOrUpdateId, columnId, keys, 0);
		}

		@Override
		public OptionalBinary deleteGetPrevious(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys)
				throws RocksDBThriftException {
			return deleteGetPreviousWithWriteClass(transactionOrUpdateId, columnId, keys, 0);
		}

		@Override
		public boolean deleteGetPreviousPresence(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys)
				throws RocksDBThriftException {
			return deleteGetPreviousPresenceWithWriteClass(transactionOrUpdateId, columnId, keys, 0);
		}

		@Override
		public void deleteMulti(long transactionOrUpdateId, long columnId, List<List<ByteBuffer>> keysMulti)
				throws RocksDBThriftException {
			deleteMultiWithWriteClass(transactionOrUpdateId, columnId, keysMulti, 0);
		}

		@Override
		public List<OptionalBinary> deleteMultiGetPrevious(long transactionOrUpdateId, long columnId,
				List<List<ByteBuffer>> keysMulti) throws RocksDBThriftException {
			return deleteMultiGetPreviousWithWriteClass(transactionOrUpdateId, columnId, keysMulti, 0);
		}

		@Override
		public List<Boolean> deleteMultiGetPreviousPresence(long transactionOrUpdateId, long columnId,
				List<List<ByteBuffer>> keysMulti) throws RocksDBThriftException {
			return deleteMultiGetPreviousPresenceWithWriteClass(transactionOrUpdateId, columnId, keysMulti, 0);
		}

		@Override
		public void putBatch(long columnId, List<KV> data, PutBatchMode mode) throws RocksDBThriftException {
			putBatchWithWriteClass(columnId, data, mode, 0);
		}

		@Override
		public void merge(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys, ByteBuffer value)
				throws RocksDBThriftException {
			mergeWithWriteClass(transactionOrUpdateId, columnId, keys, value, 0);
		}

		@Override
		public void mergeMulti(long transactionOrUpdateId, long columnId, List<List<ByteBuffer>> keysMulti,
				List<ByteBuffer> valueMulti) throws RocksDBThriftException {
			mergeMultiWithWriteClass(transactionOrUpdateId, columnId, keysMulti, valueMulti, 0);
		}

		@Override
		public void mergeBatch(long columnId, List<KV> data, MergeBatchMode mode) throws RocksDBThriftException {
			mergeBatchWithWriteClass(columnId, data, mode, 0);
		}

		@Override
		public void deleteRange(long columnId, List<ByteBuffer> startKeysInclusive,
				List<ByteBuffer> endKeysExclusive) throws RocksDBThriftException {
			deleteRangeWithWriteClass(columnId, startKeysInclusive, endKeysExclusive, 0);
		}

		@Override
		public OptionalBinary mergeGetMerged(long transactionOrUpdateId, long columnId, List<ByteBuffer> keys,
				ByteBuffer value) throws RocksDBThriftException {
			return mergeGetMergedWithWriteClass(transactionOrUpdateId, columnId, keys, value, 0);
		}

		@Override
		public List<OptionalBinary> mergeMultiGetMerged(long transactionOrUpdateId, long columnId,
				List<List<ByteBuffer>> keysMulti, List<ByteBuffer> valueMulti) throws RocksDBThriftException {
			return mergeMultiGetMergedWithWriteClass(transactionOrUpdateId, columnId, keysMulti, valueMulti, 0);
		}

		@Override
		public List<OptionalBinary> putMultiGetPrevious(long transactionOrUpdateId, long columnId,
				List<List<ByteBuffer>> keysMulti, List<ByteBuffer> valueMulti) throws RocksDBThriftException {
			return putMultiGetPreviousWithWriteClass(transactionOrUpdateId, columnId, keysMulti, valueMulti, 0);
		}

		@Override
		public List<Delta> putMultiGetDelta(long transactionOrUpdateId, long columnId,
				List<List<ByteBuffer>> keysMulti, List<ByteBuffer> valueMulti) throws RocksDBThriftException {
			return putMultiGetDeltaWithWriteClass(transactionOrUpdateId, columnId, keysMulti, valueMulti, 0);
		}

		@Override
		public List<Boolean> putMultiGetChanged(long transactionOrUpdateId, long columnId,
				List<List<ByteBuffer>> keysMulti, List<ByteBuffer> valueMulti) throws RocksDBThriftException {
			return putMultiGetChangedWithWriteClass(transactionOrUpdateId, columnId, keysMulti, valueMulti, 0);
		}

		@Override
		public List<Boolean> putMultiGetPreviousPresence(long transactionOrUpdateId, long columnId,
				List<List<ByteBuffer>> keysMulti, List<ByteBuffer> valueMulti) throws RocksDBThriftException {
			return putMultiGetPreviousPresenceWithWriteClass(transactionOrUpdateId, columnId, keysMulti, valueMulti, 0);
		}

		@Override
		public long openTransaction(long timeoutMs) throws RocksDBThriftException {
			try {
				return api.openTransaction(timeoutMs);
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public boolean closeTransactionWithWriteClass(long transactionId, boolean commit, int writeClass) throws RocksDBThriftException {
			try {
				return api.closeTransaction(transactionId, commit, mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void closeFailedUpdate(long updateId) throws RocksDBThriftException {
			try {
				api.closeFailedUpdate(updateId);
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public long createColumnWithWriteClass(String name, ColumnSchema schema, int writeClass) throws RocksDBThriftException {
			try {
				return api.createColumn(name, columnSchemaToRecord(schema), mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void deleteColumnWithWriteClass(long columnId, int writeClass) throws RocksDBThriftException {
			try {
				api.deleteColumn(columnId, mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public boolean deleteColumnIfExistsWithWriteClass(String name, int writeClass) throws RocksDBThriftException {
			try {
				return api.deleteColumnIfExists(name, mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void deleteRangeWithWriteClass(long columnId,
				List<ByteBuffer> startKeysInclusive,
				List<ByteBuffer> endKeysExclusive,
				int writeClass)
				throws RocksDBThriftException {
			try {
				api.deleteRange(columnId,
						keysToRecord(startKeysInclusive),
						keysToRecord(endKeysExclusive),
						mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public long getColumnId(String name) throws RocksDBThriftException {
			try {
				return api.getColumnId(name);
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public long estimateNumKeys(long columnId) throws RocksDBThriftException {
			try {
				return api.estimateNumKeys(columnId);
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void putFastWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value,
				int writeClass) {
			try {
				this.putWithWriteClass(transactionOrUpdateId, columnId, keys, value, writeClass);
			} catch (Exception e) {
				// Oneway cannot throw exception
				LOG.error("Error in putFast", e);
			}
		}

		@Override
		public void putWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value,
				int writeClass) throws RocksDBThriftException {
			try {
				api.put(transactionOrUpdateId,
						columnId,
						keysToRecord(keys),
						keyToRecord(value),
						RequestType.none(),
						mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void putMultiWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				List<ByteBuffer> valueMulti,
				int writeClass) throws RocksDBThriftException {
			try {
				api.putMulti(transactionOrUpdateId,
						columnId,
						keysToRecords(keysMulti),
						keyToRecords(valueMulti),
						RequestType.none(),
						mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public OptionalBinary putGetPreviousWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value,
				int writeClass) throws RocksDBThriftException {
			try {
				return ThriftServer.mapResult(api.put(transactionOrUpdateId, columnId, keysToRecord(keys),
						keyToRecord(value), RequestType.previous(), mapWriteClass(writeClass)));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public Delta putGetDeltaWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value,
				int writeClass) throws RocksDBThriftException {
			try {
				return ThriftServer.mapResult(api.put(transactionOrUpdateId, columnId, keysToRecord(keys),
						keyToRecord(value), RequestType.delta(), mapWriteClass(writeClass)));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public boolean putGetChangedWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value,
				int writeClass) throws RocksDBThriftException {
			try {
				return api.put(transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value),
						RequestType.changed(), mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public boolean putGetPreviousPresenceWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value,
				int writeClass) throws RocksDBThriftException {
			try {
				return api.put(transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value),
						RequestType.previousPresence(), mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void deleteWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				int writeClass) throws RocksDBThriftException {
			try {
				api.delete(
						transactionOrUpdateId, columnId, keysToRecord(keys), RequestType.none(), mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public OptionalBinary deleteGetPreviousWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				int writeClass) throws RocksDBThriftException {
			try {
				return ThriftServer.mapResult(api.delete(
						transactionOrUpdateId, columnId, keysToRecord(keys), RequestType.previous(), mapWriteClass(writeClass)));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public boolean deleteGetPreviousPresenceWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				int writeClass) throws RocksDBThriftException {
			try {
				return api.delete(
						transactionOrUpdateId, columnId, keysToRecord(keys), RequestType.previousPresence(),
						mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void deleteMultiWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				int writeClass) throws RocksDBThriftException {
			try {
				api.deleteMulti(
						transactionOrUpdateId, columnId, keysToRecords(keysMulti), RequestType.none(),
						mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public List<OptionalBinary> deleteMultiGetPreviousWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				int writeClass) throws RocksDBThriftException {
			try {
				return ThriftServer.mapResult(api.deleteMulti(
						transactionOrUpdateId, columnId, keysToRecords(keysMulti), RequestType.previous(),
						mapWriteClass(writeClass)));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public List<Boolean> deleteMultiGetPreviousPresenceWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				int writeClass) throws RocksDBThriftException {
			try {
				return api.deleteMulti(
						transactionOrUpdateId, columnId, keysToRecords(keysMulti), RequestType.previousPresence(),
						mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public OptionalBinary get(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys) throws RocksDBThriftException {
			try {
				return ThriftServer.mapResult(api.get(transactionOrUpdateId, columnId, keysToRecord(keys), RequestType.current()));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public UpdateBegin getForUpdate(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys) throws RocksDBThriftException {
			try {
				return mapResult(api.get(transactionOrUpdateId, columnId, keysToRecord(keys), RequestType.forUpdate()));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public boolean exists(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys) throws RocksDBThriftException {
			try {
				return api.get(transactionOrUpdateId, columnId, keysToRecord(keys), RequestType.exists());
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public List<Boolean> existsMulti(long transactionId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				long timeoutMs) throws RocksDBThriftException {
			try {
				return api.existsMulti(transactionId,
						columnId,
						keysToRecords(keysMulti),
						timeoutMs);
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
				return api.openIterator(transactionId, columnId, keysToRecord(startKeysInclusive), keysToRecord(endKeysExclusive), reverse, timeoutMs);
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void closeIterator(long iteratorId) throws RocksDBThriftException {
			try {
				api.closeIterator(iteratorId);
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void seekTo(long iterationId, List<ByteBuffer> keys) throws RocksDBThriftException {
			try {
				api.seekTo(iterationId, keysToRecord(keys));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void subsequent(long iterationId, long skipCount, long takeCount) throws RocksDBThriftException {
			try {
				api.subsequent(iterationId, skipCount, takeCount, RequestType.none());
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public boolean subsequentExists(long iterationId,
				long skipCount,
				long takeCount) throws RocksDBThriftException {
			try {
				return api.subsequent(iterationId, skipCount, takeCount, RequestType.exists());
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public List<OptionalBinary> subsequentMultiGet(long iterationId,
				long skipCount,
				long takeCount) throws RocksDBThriftException {
			try {
				return mapResult(api.subsequent(iterationId, skipCount, takeCount, RequestType.multi()));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void putBatchWithWriteClass(long columnId, List<KV> data, PutBatchMode mode, int writeClass)
				throws RocksDBThriftException {
			try {
				api.putBatch(columnId, kvToBatch(data), mapPutBatchMode(mode), mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void mergeWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value,
				int writeClass) throws RocksDBThriftException {
			try {
				api.merge(transactionOrUpdateId, columnId, keysToRecord(keys), keyToRecord(value),
						RequestType.none(), mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void mergeMultiWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				List<ByteBuffer> valueMulti,
				int writeClass) throws RocksDBThriftException {
			try {
				api.mergeMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti), keyToRecords(valueMulti),
						RequestType.none(), mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void mergeBatchWithWriteClass(long columnId, List<KV> data, MergeBatchMode mode, int writeClass)
				throws RocksDBThriftException {
			try {
				api.mergeBatch(columnId, kvToBatch(data), mapMergeBatchMode(mode), mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public OptionalBinary mergeGetMergedWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<ByteBuffer> keys,
				ByteBuffer value,
				int writeClass) throws RocksDBThriftException {
			try {
				return ThriftServer.mapResult(api.merge(transactionOrUpdateId, columnId, keysToRecord(keys),
						keyToRecord(value), RequestType.merged(), mapWriteClass(writeClass)));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public List<OptionalBinary> mergeMultiGetMergedWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				List<ByteBuffer> valueMulti,
				int writeClass) throws RocksDBThriftException {
			try {
				return ThriftServer.mapResult(api.mergeMulti(transactionOrUpdateId, columnId,
						keysToRecords(keysMulti), keyToRecords(valueMulti), RequestType.merged(),
						mapWriteClass(writeClass)));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public long uploadMergeOperator(String operatorName, String className, ByteBuffer jarPayload) throws RocksDBThriftException {
			try {
				return api.uploadMergeOperator(operatorName, className, Utils.toByteArray(Utils.fromByteBuffer(jarPayload)));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public FirstAndLast reduceRangeFirstAndLast(long transactionId, long columnId, List<ByteBuffer> startKeysInclusive, List<ByteBuffer> endKeysExclusive, boolean reverse, long timeoutMs) throws RocksDBThriftException {
			try {
				return mapFirstAndLast(api.reduceRange(transactionId, columnId, keysToRecord(startKeysInclusive), keysToRecord(endKeysExclusive), reverse, RequestType.firstAndLast(), timeoutMs));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public long reduceRangeEntriesCount(long transactionId, long columnId, List<ByteBuffer> startKeysInclusive, List<ByteBuffer> endKeysExclusive, boolean reverse, long timeoutMs) throws RocksDBThriftException {
			try {
				return api.reduceRange(transactionId, columnId, keysToRecord(startKeysInclusive), keysToRecord(endKeysExclusive), reverse, RequestType.entriesCount(), timeoutMs);
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public List<KV> getAllInRange(long transactionId, long columnId, List<ByteBuffer> startKeysInclusive, List<ByteBuffer> endKeysExclusive, boolean reverse, long timeoutMs) throws RocksDBThriftException {
			try {
				return api.getRange(transactionId, columnId, keysToRecord(startKeysInclusive), keysToRecord(endKeysExclusive), reverse, RequestType.allInRange(), timeoutMs)
						.map(ThriftServer::mapKV)
						.collect(Collectors.toList());
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public List<KV> getAllInRangeNoCache(long transactionId, long columnId, List<ByteBuffer> startKeysInclusive, List<ByteBuffer> endKeysExclusive, boolean reverse, long timeoutMs) throws RocksDBThriftException {
			try {
				return api.getRange(transactionId, columnId, keysToRecord(startKeysInclusive), keysToRecord(endKeysExclusive), reverse, RequestType.allInRangeNoCache(), timeoutMs)
						.map(ThriftServer::mapKV)
						.collect(Collectors.toList());
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public List<OptionalBinary> putMultiGetPreviousWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				List<ByteBuffer> valueMulti,
				int writeClass) throws RocksDBThriftException {
			try {
				return mapResult(api.putMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti),
						keyToRecords(valueMulti), RequestType.previous(), mapWriteClass(writeClass)));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public List<Delta> putMultiGetDeltaWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				List<ByteBuffer> valueMulti,
				int writeClass) throws RocksDBThriftException {
			try {
				return api.putMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti),
						keyToRecords(valueMulti), RequestType.delta(), mapWriteClass(writeClass))
						.stream().map(ThriftServer::mapResult).collect(Collectors.toList());
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public List<Boolean> putMultiGetChangedWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				List<ByteBuffer> valueMulti,
				int writeClass) throws RocksDBThriftException {
			try {
				return api.putMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti),
						keyToRecords(valueMulti), RequestType.changed(), mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public List<Boolean> putMultiGetPreviousPresenceWithWriteClass(long transactionOrUpdateId,
				long columnId,
				List<List<ByteBuffer>> keysMulti,
				List<ByteBuffer> valueMulti,
				int writeClass) throws RocksDBThriftException {
			try {
				return api.putMulti(transactionOrUpdateId, columnId, keysToRecords(keysMulti),
						keyToRecords(valueMulti), RequestType.previousPresence(), mapWriteClass(writeClass));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void flush() throws RocksDBThriftException {
			try {
				api.flush();
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void compact() throws RocksDBThriftException {
			try {
				api.compact();
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public List<Column> getAllColumnDefinitions() throws RocksDBThriftException {
			try {
				var map = api.getAllColumnDefinitions();
				List<Column> columns = new ArrayList<>();
				map.forEach((name, schema) -> columns.add(new Column(name, mapSchemaToThrift(schema))));
				return columns;
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public long cdcCreate(it.cavallium.rockserver.core.common.api.CdcCreateRequest request)
				throws RocksDBThriftException {
			try {
				if (request.isSetExpectAbsent() && request.isSetExpectedLastCommittedSeq()) {
					throw it.cavallium.rockserver.core.common.RocksDBException.of(
							it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.NULL_ARGUMENT,
							"CDC create request cannot set both expectAbsent and expectedLastCommittedSeq");
				}
				if (request.isSetExpectAbsent() && !request.isExpectAbsent()) {
					throw it.cavallium.rockserver.core.common.RocksDBException.of(
							it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.NULL_ARGUMENT,
							"CDC create expectAbsent must be true when set");
				}
				OptionalLong expectedLastCommitted = request.isSetExpectAbsent()
						? OptionalLong.empty()
						: request.isSetExpectedLastCommittedSeq()
								? OptionalLong.of(request.getExpectedLastCommittedSeq())
								: null;
				return api.cdcCreate(request.getId(),
						request.isSetFromSeq() ? request.getFromSeq() : null,
						request.isSetColumnIds() ? request.getColumnIds() : null,
						request.isSetEmitLatestValues() ? request.isEmitLatestValues() : null,
						expectedLastCommitted);
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void cdcDelete(String id) throws RocksDBThriftException {
			try {
				api.cdcDelete(id);
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public long cdcGetEarliestAvailableSequence() throws RocksDBThriftException {
			try {
				return api.cdcGetEarliestAvailableSequence();
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public OptionalLongValue cdcGetLastCommittedSequence(String id) throws RocksDBThriftException {
			try {
				var sequence = api.cdcGetLastCommittedSequence(id);
				var response = new OptionalLongValue();
				sequence.ifPresent(response::setValue);
				return response;
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public it.cavallium.rockserver.core.common.api.CdcPollBatchResult cdcPollBatch(
				it.cavallium.rockserver.core.common.api.CdcPollRequest request)
				throws RocksDBThriftException {
			try {
				int requestedMaxResponseSize = request.isSetMaxResponseBytes()
						? request.getMaxResponseBytes()
						: LEGACY_MAX_CDC_RESPONSE_SIZE;
				if (requestedMaxResponseSize <= 0) {
					throw it.cavallium.rockserver.core.common.RocksDBException.of(
							it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.NULL_ARGUMENT,
							"maxResponseBytes must be positive: " + requestedMaxResponseSize);
				}
				var batch = asyncApi.cdcPollBatchAsync(request.getId(),
						request.isSetFromSeq() ? request.getFromSeq() : null,
						request.getMaxEvents()).block();
				if (batch == null) {
					throw it.cavallium.rockserver.core.common.RocksDBException.of(
							it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.INTERNAL_ERROR,
							"CDC poll completed without a batch");
				}
				return ThriftCdcResponseBudget.build(batch,
						Math.min(requestedMaxResponseSize, maxCdcResponseSize));
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}

		@Override
		public void cdcCommit(String id, long seq) throws RocksDBThriftException {
			try {
				api.cdcCommit(id, seq);
			} catch (it.cavallium.rockserver.core.common.RocksDBException e) {
				throw mapException(e);
			}
		}
	}

	private static Flux<it.cavallium.rockserver.core.common.KVBatch> kvToBatch(List<KV> data) {
		if (data == null || data.isEmpty()) return Flux.empty();
		List<Keys> keysList = new ArrayList<>(data.size());
		List<Buf> valuesList = new ArrayList<>(data.size());
		for (KV kv : data) {
			keysList.add(keysToRecord(kv.getKeys()));
			valuesList.add(keyToRecord(kv.bufferForValue()));
		}
		return Flux.just(new KVBatchRef(keysList, valuesList));
	}

	private static it.cavallium.rockserver.core.common.MergeBatchMode mapMergeBatchMode(MergeBatchMode mode) {
		return switch (mode) {
			case MERGE_WRITE_BATCH -> it.cavallium.rockserver.core.common.MergeBatchMode.MERGE_WRITE_BATCH;
			case MERGE_WRITE_BATCH_NO_WAL -> it.cavallium.rockserver.core.common.MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL;
			case MERGE_SST_INGESTION -> it.cavallium.rockserver.core.common.MergeBatchMode.MERGE_SST_INGESTION;
			case MERGE_SST_INGEST_BEHIND -> it.cavallium.rockserver.core.common.MergeBatchMode.MERGE_SST_INGEST_BEHIND;
		};
	}

	private static it.cavallium.rockserver.core.common.PutBatchMode mapPutBatchMode(PutBatchMode mode) {
		return switch (mode) {
			case WRITE_BATCH -> it.cavallium.rockserver.core.common.PutBatchMode.WRITE_BATCH;
			case WRITE_BATCH_NO_WAL -> it.cavallium.rockserver.core.common.PutBatchMode.WRITE_BATCH_NO_WAL;
			case SST_INGESTION -> it.cavallium.rockserver.core.common.PutBatchMode.SST_INGESTION;
			case SST_INGEST_BEHIND -> it.cavallium.rockserver.core.common.PutBatchMode.SST_INGEST_BEHIND;
		};
	}

	@Override
	public void close() throws IOException {
		LOG.info("Thrift server is shutting down...");
		this.server.stop();
		boolean interrupted = false;
		if (Thread.currentThread() != thriftThread) {
			while (thriftThread.isAlive()) {
				try {
					thriftThread.join();
				} catch (InterruptedException ex) {
					interrupted = true;
				}
			}
		}
		try {
			super.close();
			LOG.info("Thrift server shut down.");
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}
}
