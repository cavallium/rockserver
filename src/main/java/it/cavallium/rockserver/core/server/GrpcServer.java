package it.cavallium.rockserver.core.server;

import static it.cavallium.rockserver.core.common.Utils.toByteArray;
import static it.cavallium.rockserver.core.common.Utils.toBuf;

import com.google.common.annotations.VisibleForTesting;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors.FieldDescriptor;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Context;
import io.grpc.Deadline;
import io.grpc.Metadata;
import io.grpc.ServerCall;
import io.grpc.ServerCall.Listener;
import io.grpc.ServerCallHandler;
import io.grpc.ServerInterceptor;
import io.grpc.Status;
import io.grpc.Status.Code;
import io.grpc.StatusException;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import io.netty.channel.unix.DomainSocketAddress;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.KVBatch.KVBatchRef;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.RequestType.RequestChanged;
import it.cavallium.rockserver.core.common.RequestType.RequestCurrent;
import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import it.cavallium.rockserver.core.common.RequestType.RequestDelta;
import it.cavallium.rockserver.core.common.RequestType.RequestExists;
import it.cavallium.rockserver.core.common.RequestType.RequestForUpdate;
import it.cavallium.rockserver.core.common.RequestType.RequestMulti;
import it.cavallium.rockserver.core.common.RequestType.RequestNothing;
import it.cavallium.rockserver.core.common.RequestType.RequestPrevious;
import it.cavallium.rockserver.core.common.RequestType.RequestPreviousPresence;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.api.proto.Delta;
import it.cavallium.rockserver.core.common.api.proto.FirstAndLast;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.impl.InternalConnection;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import it.cavallium.buffer.Buf;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map.Entry;
import java.util.StringJoiner;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.Supplier;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.Disposables;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.context.ContextView;

public class GrpcServer extends Server {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());
	private static final int LEGACY_GRPC_MAX_INBOUND_MESSAGE_SIZE = 4 * 1024 * 1024;
	private static final String GRPC_LATE_ERROR_HANDLER_CONTEXT_KEY
			= GrpcServer.class.getName() + ".lateErrorHandler";
	private static final Object ITERATOR_OPERATION_LEASE_CONTEXT_KEY = new Object();
	// Reactor exposes sequence-local dropped-error handling through this documented context key, while
	// keeping Hooks.KEY_ON_ERROR_DROPPED package-private.
	private static final String REACTOR_ON_ERROR_DROPPED_CONTEXT_KEY = "reactor.onErrorDropped.local";
	private static final Consumer<Throwable> UNCONTEXTUALIZED_LATE_ERROR_HANDLER
			= GrpcServer::logUncontextualizedLateError;

	private final GrpcServerImpl grpc;
	private final EventLoopGroup elg;
	private final io.grpc.Server server;
	private final RWScheduler scheduler;
	private final boolean ownsScheduler;

	private static void logUncontextualizedLateError(Throwable error) {
		var rocksError = GrpcServerImpl.findRocksDBException(error);
		var status = Status.fromThrowable(error);
		if (rocksError != null) {
			var statusCode = status.getCode() == Code.UNKNOWN ? Code.INTERNAL : status.getCode();
			LOG.warn("Late gRPC request failure after call termination without request context: "
					+ "errorType={}, grpcStatus={}, message={}",
					rocksError.getErrorUniqueId(),
					statusCode,
					GrpcServerImpl.sanitizeForLog(rocksError.getMessage()));
			LOG.debug("Late gRPC request failure stack without request context", error);
			return;
		}
		if (status.getCode() == Code.CANCELLED
				|| error instanceof java.util.concurrent.CancellationException) {
			LOG.debug("Late gRPC cancellation after call termination without request context: description={}",
					GrpcServerImpl.sanitizeForLog(status.getDescription()));
			return;
		}
		if (status.getCode() != Code.UNKNOWN && status.getCode() != Code.INTERNAL) {
			LOG.warn("Late gRPC transport failure after call termination without request context: "
					+ "grpcStatus={}, description={}",
					status.getCode(),
					GrpcServerImpl.sanitizeForLog(status.getDescription()));
			LOG.debug("Late gRPC transport failure stack without request context", error);
			return;
		}
		LOG.error("Unexpected late gRPC request failure after call termination; request context is unavailable",
				error);
	}

	public GrpcServer(RocksDBConnection client, SocketAddress socketAddress) throws IOException {
		super(client);
		if (client instanceof InternalConnection internalConnection) {
			this.scheduler = internalConnection.getScheduler();
			this.ownsScheduler = false;
		} else {
			this.scheduler = new RWScheduler(Runtime.getRuntime().availableProcessors(),
					Runtime.getRuntime().availableProcessors(),
					"grpc-db"
			);
			this.ownsScheduler = true;
		}
		this.grpc = new GrpcServerImpl(this.getClient());
		EventLoopGroup elg;
		Class<? extends ServerChannel> channelType;
		if (socketAddress instanceof DomainSocketAddress _) {
			elg = new EpollEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);
			channelType = EpollServerDomainSocketChannel.class;
		} else {
			elg = new NioEventLoopGroup(Runtime.getRuntime().availableProcessors() * 2);
			channelType = NioServerSocketChannel.class;
		}
		this.elg = elg;
		this.server = NettyServerBuilder
				.forAddress(socketAddress)
				.bossEventLoopGroup(elg)
				.workerEventLoopGroup(elg)
				.directExecutor()
				.channelType(channelType)
				.withChildOption(ChannelOption.SO_KEEPALIVE, false)
				.maxInboundMessageSize(512 * 1024 * 1024)
				.addService(grpc)
				.permitKeepAliveWithoutCalls(true)
				.permitKeepAliveTime(5, TimeUnit.SECONDS)
				.intercept(new GzipCompressorInterceptor())
				.build();
		LOG.info("GRPC RocksDB server is listening at " + socketAddress);
	}

	@Override
	public void start() throws IOException {
		server.start();
	}

	public int getPort() {
		return server.getPort();
	}

	@VisibleForTesting
	public int getActiveIteratorOperationLeaseCountForTesting() {
		return grpc.iteratorOperations.size();
	}

	private static class GzipCompressorInterceptor implements ServerInterceptor {

		@Override
		public <ReqT, RespT> Listener<ReqT> interceptCall(ServerCall<ReqT, RespT> call,
				Metadata headers,
				ServerCallHandler<ReqT, RespT> next) {
			call.setCompression("gzip");
			return next.startCall(call, headers);
		}
	}


	private final class GrpcServerImpl extends ReactorRocksDBServiceGrpc.RocksDBServiceImplBase {

		private static final long ITERATOR_VALUE_PAGE_SIZE = 64L;
		private static final long ITERATOR_ADVANCE_STEP_SIZE = 4_096L;

		private final RocksDBAsyncAPI asyncApi;
        private final RocksDBSyncAPI api;
		private final ConcurrentMap<Long, Object> iteratorOperations = new ConcurrentHashMap<>();

		public GrpcServerImpl(RocksDBConnection client) {
			this.asyncApi = client.getAsyncApi();
            this.api = client.getSyncApi();
		}

		// functions


		@Override
		public Mono<OpenTransactionResponse> openTransaction(OpenTransactionRequest request) {
			return executeSync(() -> {
				var txId = api.openTransaction(request.getTimeoutMs());
				return OpenTransactionResponse.newBuilder().setTransactionId(txId).build();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("openTransaction", request));
		}

		@Override
		public Mono<CloseTransactionResponse> closeTransaction(CloseTransactionRequest request) {
			return executeSync(() -> {
				var committed = api.closeTransaction(request.getTransactionId(), request.getCommit());
                return CloseTransactionResponse.newBuilder().setSuccessful(committed).build();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("closeTransaction", request));
		}

		@Override
		public Mono<Empty> closeFailedUpdate(CloseFailedUpdateRequest request) {
			return executeSync(() -> {
				api.closeFailedUpdate(request.getUpdateId());
				return Empty.getDefaultInstance();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("closeFailedUpdate", request));
		}

		@Override
		public Mono<CreateColumnResponse> createColumn(CreateColumnRequest request) {
			return executeSync(() -> {
				var colId = api.createColumn(request.getName(), mapColumnSchema(request.getSchema()));
				return CreateColumnResponse.newBuilder().setColumnId(colId).build();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("createColumn", request));
		}

		@Override
		public Mono<Empty> deleteColumn(DeleteColumnRequest request) {
			return executeSync(() -> {
				api.deleteColumn(request.getColumnId());
				return Empty.getDefaultInstance();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("deleteColumn", request));
		}

		@Override
		public Mono<GetColumnIdResponse> getColumnId(GetColumnIdRequest request) {
			return executeSync(() -> {
				var colId = api.getColumnId(request.getName());
				return GetColumnIdResponse.newBuilder().setColumnId(colId).build();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("getColumnId", request));
		}

		@Override
		public Mono<EntriesCount> estimateNumKeys(EstimateNumKeysRequest request) {
			return executeSync(() -> EntriesCount.newBuilder()
					.setCount(api.estimateNumKeys(request.getColumnId()))
					.build(), true)
					.transform(this.onErrorMapMonoWithRequestInfo("estimateNumKeys", request));
		}

		@Override
		public Mono<Empty> put(PutRequest request) {
			return executeSync(() -> {
				api.put(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysCount(), request.getData()::getKeys),
						toBuf(request.getData().getValue()),
						new RequestNothing<>()
				);
				return Empty.getDefaultInstance();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("put", request));
		}

		@Override
		public Mono<Empty> delete(DeleteRequest request) {
			return executeSync(() -> {
				api.delete(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getKeysCount(), request::getKeys),
						new RequestNothing<>()
				);
				return Empty.getDefaultInstance();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("delete", request));
		}

		@Override
		public Mono<Empty> deleteRange(DeleteRangeRequest request) {
			return executeSync(() -> {
				api.deleteRange(request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
						mapKeys(request.getEndKeysExclusiveCount(), request::getEndKeysExclusive)
				);
				return Empty.getDefaultInstance();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("deleteRange", request));
		}

		@Override
		public Mono<ExistsMultiResponse> existsMulti(ExistsMultiRequest request) {
			var transportDeadline = Context.current().getDeadline();
			return Mono.defer(() -> {
					var keys = request.getKeysMultiList().stream()
							.map(keyTuple -> mapKeys(keyTuple.getKeysCount(), keyTuple::getKeys))
							.toList();
					return fromCancellableFuture(asyncApi.existsMultiAsync(
							request.getTransactionId(),
							request.getColumnId(),
							keys,
							effectiveReadTimeoutMillis(request.getTimeoutMs(), transportDeadline)));
				})
					.map(present -> ExistsMultiResponse.newBuilder().addAllPresent(present).build())
					.transform(this.onErrorMapMonoWithRequestInfo("existsMulti", request));
		}

		@Override
		public Mono<Empty> merge(MergeRequest request) {
			return executeSync(() -> {
				api.merge(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysCount(), request.getData()::getKeys),
						toBuf(request.getData().getValue()),
						new RequestNothing<>()
				);
				return Empty.getDefaultInstance();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("merge", request));
		}

		@Override
		public Mono<Empty> putBatch(Flux<PutBatchRequest> request) {
			return request.switchOnFirst((firstSignal, requestFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
                    assert firstValue != null;
                    if (!firstValue.hasInitialRequest()) {
						return Mono.<Empty>error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var mode = switch (initialRequest.getMode()) {
						case WRITE_BATCH -> PutBatchMode.WRITE_BATCH;
						case WRITE_BATCH_NO_WAL -> PutBatchMode.WRITE_BATCH_NO_WAL;
						case SST_INGESTION -> PutBatchMode.SST_INGESTION;
						case SST_INGEST_BEHIND -> PutBatchMode.SST_INGEST_BEHIND;
						case UNRECOGNIZED -> throw new UnsupportedOperationException("Unrecognized request \"mode\"");
					};

					var batches = requestFlux
							.skip(1) // skip initial request
							.<KVBatch>handle((putBatchRequest, sink) -> {
								if (!putBatchRequest.hasData()) {
									sink.error( RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
											"Multiple initial requests"));
									return;
								}
								var batch = putBatchRequest.getData();
								try {
									sink.next(mapKVBatch(batch.getEntriesCount(), batch::getEntries));
								} catch (Throwable ex) {
									sink.error(ex);
								}
							});

					return Mono
							.fromFuture(() -> asyncApi.putBatchAsync(initialRequest.getColumnId(), batches, mode))
							.transform(this.onErrorMapMonoWithRequestInfo("putBatch", initialRequest));
				} else if (firstSignal.isOnComplete()) {
					return Mono.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return requestFlux;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Mono<Empty> mergeBatch(Flux<MergeBatchRequest> request) {
			return request.switchOnFirst((firstSignal, requestFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Mono.<Empty>error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var mode = switch (initialRequest.getMode()) {
						case MERGE_WRITE_BATCH -> MergeBatchMode.MERGE_WRITE_BATCH;
						case MERGE_WRITE_BATCH_NO_WAL -> MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL;
						case MERGE_SST_INGESTION -> MergeBatchMode.MERGE_SST_INGESTION;
						case MERGE_SST_INGEST_BEHIND -> MergeBatchMode.MERGE_SST_INGEST_BEHIND;
						case UNRECOGNIZED -> throw new UnsupportedOperationException("Unrecognized request \"mode\"");
					};

					var batches = requestFlux
							.skip(1) // skip initial request
							.<KVBatch>handle((mergeBatchRequest, sink) -> {
								if (!mergeBatchRequest.hasData()) {
									sink.error( RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
											"Multiple initial requests"));
									return;
								}
								var batch = mergeBatchRequest.getData();
								try {
									sink.next(mapKVBatch(batch.getEntriesCount(), batch::getEntries));
								} catch (Throwable ex) {
									sink.error(ex);
								}
							});

  			return Mono
  					.fromFuture(() -> asyncApi.mergeBatchAsync(initialRequest.getColumnId(), batches, mode))
  					.transform(this.onErrorMapMonoWithRequestInfo("mergeBatch", initialRequest));
			} else if (firstSignal.isOnComplete()) {
				return Mono.error(RocksDBException.of(
						RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
  			} else {
  				return requestFlux;
  			}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Mono<Empty> deleteMulti(Flux<DeleteMultiRequest> request) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Mono.<Empty>error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var dataFlux = requestsFlux
							.skip(1)
							.map(deleteRequest -> {
								if (!deleteRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return deleteRequest.getData();
							});
					return deleteMultiDataFlux(initialRequest, dataFlux, "deleteMulti");
				} else if (firstSignal.isOnComplete()) {
					return Mono.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return requestsFlux;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Flux<Previous> deleteMultiGetPrevious(Flux<DeleteMultiRequest> request) {
			return deleteMultiResponseFlux(request, "deleteMultiGetPrevious", new RequestPrevious<>(), previous -> {
				var builder = Previous.newBuilder();
				if (previous != null) {
					builder.setPrevious(Utils.toByteString(previous));
				}
				return builder.build();
			});
		}

		@Override
		public Flux<PreviousPresence> deleteMultiGetPreviousPresence(Flux<DeleteMultiRequest> request) {
			return deleteMultiResponseFlux(request,
					"deleteMultiGetPreviousPresence",
					new RequestPreviousPresence<>(),
					present -> PreviousPresence.newBuilder().setPresent(present).build());
		}

		private Mono<Empty> deleteMultiDataFlux(DeleteMultiInitialRequest initialRequest,
				Flux<DeleteRequest> dataFlux,
				String requestName) {
			return dataFlux
					.publishOn(scheduler.write())
					.doOnNext(data -> api.delete(initialRequest.getTransactionOrUpdateId(),
							initialRequest.getColumnId(),
							mapKeys(data.getKeysCount(), data::getKeys),
							new RequestNothing<>()))
					.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest))
					.then(Mono.just(Empty.getDefaultInstance()));
		}

		private <T, R> Flux<R> deleteMultiResponseFlux(Flux<DeleteMultiRequest> request,
				String requestName,
				RequestDelete<? super Buf, T> requestType,
				Function<T, R> mapper) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Flux.error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var dataFlux = requestsFlux
							.skip(1)
							.map(deleteRequest -> {
								if (!deleteRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return deleteRequest.getData();
							});

					return dataFlux
							.publishOn(scheduler.write())
							.map(data -> mapper.apply(api.delete(initialRequest.getTransactionOrUpdateId(),
									initialRequest.getColumnId(),
									mapKeys(data.getKeysCount(), data::getKeys),
									requestType)))
							.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest));
				} else {
					return Flux.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				}
			});
		}

  		@Override
  		public Mono<Merged> mergeGetMerged(MergeRequest request) {
  			return executeSync(() -> {
  				var merged = api.merge(request.getTransactionOrUpdateId(),
  						request.getColumnId(),
  						mapKeys(request.getData().getKeysCount(), request.getData()::getKeys),
  						toBuf(request.getData().getValue()),
  						RequestType.merged());
  				return Merged.newBuilder()
  						.setMerged(merged != null ? unmapValueHeap(merged) : ByteString.EMPTY)
  						.build();
  			}, false).transform(this.onErrorMapMonoWithRequestInfo("mergeGetMerged", request));
  		}

		@Override
		public Mono<Empty> putMultiList(PutMultiListRequest request) {
			var initialRequest = request.getInitialRequest();
			var dataFlux = Flux.fromIterable(request.getDataList());
			return putMultiDataFlux(initialRequest, dataFlux, "putMultiList");
		}

		@Override
		public Mono<Empty> mergeMulti(Flux<MergeMultiRequest> request) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Mono.<Empty>error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var dataFlux = requestsFlux
							.skip(1) // skip the initial request
							.map(mergeRequest -> {
								if (!mergeRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return mergeRequest.getData();
							});
					return mergeMultiDataFlux(initialRequest, dataFlux, "mergeMulti");
				} else if (firstSignal.isOnComplete()) {
					return Mono.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return requestsFlux;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Flux<Merged> mergeMultiGetMerged(Flux<MergeMultiRequest> request) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Flux.error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var dataFlux = requestsFlux
							.skip(1) // skip the initial request
							.map(mergeRequest -> {
								if (!mergeRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return mergeRequest.getData();
							});

					return dataFlux
							.publishOn(scheduler.write())
							.map(data -> {
								var merged = api.merge(initialRequest.getTransactionOrUpdateId(),
										initialRequest.getColumnId(),
										mapKeys(data.getKeysCount(), data::getKeys),
										toBuf(data.getValue()),
										RequestType.merged());
								return Merged.newBuilder()
										.setMerged(merged != null ? unmapValueHeap(merged) : ByteString.EMPTY)
										.build();
							})
							.onErrorMap(ex -> this.handleError(ex).asRuntimeException());
				} else {
					return Flux.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				}
			});
		}

		private Mono<Empty> mergeMultiDataFlux(MergeMultiInitialRequest initialRequest,
				Flux<KV> dataFlux, String requestName) {
			return dataFlux
					.publishOn(scheduler.write())
					.doOnNext(data -> {
						api.merge(initialRequest.getTransactionOrUpdateId(),
								initialRequest.getColumnId(),
								mapKeys(data.getKeysCount(), data::getKeys),
								toBuf(data.getValue()),
								new RequestNothing<>());
					})
					.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest))
					.then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Mono<Empty> putMulti(Flux<PutMultiRequest> request) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Mono.<Empty>error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var dataFlux = requestsFlux
							.skip(1) // skip the initial request
							.map(putRequest -> {
								if (!putRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return putRequest.getData();
							});
					return putMultiDataFlux(initialRequest, dataFlux, "putMulti");
				} else if (firstSignal.isOnComplete()) {
					return Mono.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return requestsFlux;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Flux<Previous> putMultiGetPrevious(Flux<PutMultiRequest> request) {
			return putMultiResponseFlux(request, "putMultiGetPrevious", new RequestPrevious<>(), previous -> {
				var builder = Previous.newBuilder();
				if (previous != null) {
					builder.setPrevious(Utils.toByteString(previous));
				}
				return builder.build();
			});
		}

		@Override
		public Flux<Delta> putMultiGetDelta(Flux<PutMultiRequest> request) {
			return putMultiResponseFlux(request, "putMultiGetDelta", new RequestDelta<>(), delta -> {
				var builder = Delta.newBuilder();
				if (delta.previous() != null) {
					builder.setPrevious(Utils.toByteString(delta.previous()));
				}
				if (delta.current() != null) {
					builder.setCurrent(Utils.toByteString(delta.current()));
				}
				return builder.build();
			});
		}

		@Override
		public Flux<Changed> putMultiGetChanged(Flux<PutMultiRequest> request) {
			return putMultiResponseFlux(request,
					"putMultiGetChanged",
					new RequestChanged<>(),
					changed -> Changed.newBuilder().setChanged(changed).build());
		}

		@Override
		public Flux<PreviousPresence> putMultiGetPreviousPresence(Flux<PutMultiRequest> request) {
			return putMultiResponseFlux(request,
					"putMultiGetPreviousPresence",
					new RequestPreviousPresence<>(),
					present -> PreviousPresence.newBuilder().setPresent(present).build());
		}

		private <T, R> Flux<R> putMultiResponseFlux(Flux<PutMultiRequest> request,
				String requestName,
				RequestType.RequestPut<? super Buf, T> requestType,
				Function<T, R> mapper) {
			return request.switchOnFirst((firstSignal, requestsFlux) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Flux.error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();
					var dataFlux = requestsFlux
							.skip(1)
							.map(putRequest -> {
								if (!putRequest.hasData()) {
									throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
								}
								return putRequest.getData();
							});

					return dataFlux
							.publishOn(scheduler.write())
							.map(data -> mapper.apply(api.put(initialRequest.getTransactionOrUpdateId(),
									initialRequest.getColumnId(),
									mapKeys(data.getKeysCount(), data::getKeys),
									toBuf(data.getValue()),
									requestType)))
							.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest));
				} else {
					return Flux.error(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				}
			});
		}

		private Mono<Empty> putMultiDataFlux(PutMultiInitialRequest initialRequest,
				Flux<KV> dataFlux, String requestName) {
			return dataFlux
					.publishOn(scheduler.write())
					.doOnNext(data -> {
						api.put(initialRequest.getTransactionOrUpdateId(),
								initialRequest.getColumnId(),
								mapKeys(data.getKeysCount(), data::getKeys),
								toBuf(data.getValue()),
								new RequestNothing<>());
					})
					.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest))
					.then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Mono<Previous> putGetPrevious(PutRequest request) {
			return executeSync(() -> {
				var prev = api.put(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysCount(), request.getData()::getKeys),
						toBuf(request.getData().getValue()),
						new RequestPrevious<>()
				);
				var prevBuilder = Previous.newBuilder();
				if (prev != null) {
					prevBuilder.setPrevious(Utils.toByteString(prev));
				}
				return prevBuilder.build();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("putGetPrevious", request));
		}

		@Override
		public Mono<Delta> putGetDelta(PutRequest request) {
			return executeSync(() -> {
				var delta = api.put(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysCount(), request.getData()::getKeys),
						toBuf(request.getData().getValue()),
						new RequestDelta<>()
				);
				var deltaBuilder = Delta.newBuilder();
				if (delta.previous() != null) {
					deltaBuilder.setPrevious(Utils.toByteString(delta.previous()));
				}
				if (delta.current() != null) {
					deltaBuilder.setCurrent(Utils.toByteString(delta.current()));
				}
				return deltaBuilder.build();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("putGetDelta", request));
		}

		@Override
		public Mono<Changed> putGetChanged(PutRequest request) {
			return executeSync(() -> {
				var changed = api.put(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysCount(), request.getData()::getKeys),
						toBuf(request.getData().getValue()),
						new RequestChanged<>()
				);
				return Changed.newBuilder().setChanged(changed).build();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("putGetChanged", request));
		}

		@Override
		public Mono<PreviousPresence> putGetPreviousPresence(PutRequest request) {
			return executeSync(() -> {
				var present = api.put(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getData().getKeysCount(), request.getData()::getKeys),
						toBuf(request.getData().getValue()),
						new RequestPreviousPresence<>()
				);
				return PreviousPresence.newBuilder().setPresent(present).build();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("putGetPreviousPresence", request));
		}

		@Override
		public Mono<Previous> deleteGetPrevious(DeleteRequest request) {
			return executeSync(() -> {
				var prev = api.delete(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getKeysCount(), request::getKeys),
						new RequestPrevious<>()
				);
				var prevBuilder = Previous.newBuilder();
				if (prev != null) {
					prevBuilder.setPrevious(Utils.toByteString(prev));
				}
				return prevBuilder.build();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("deleteGetPrevious", request));
		}

		@Override
		public Mono<PreviousPresence> deleteGetPreviousPresence(DeleteRequest request) {
			return executeSync(() -> {
				var present = api.delete(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getKeysCount(), request::getKeys),
						new RequestPreviousPresence<>()
				);
				return PreviousPresence.newBuilder().setPresent(present).build();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("deleteGetPreviousPresence", request));
		}

		@Override
		public Mono<GetResponse> get(GetRequest request) {
			return executeSync(() -> {
				var current = api.get(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getKeysCount(), request::getKeys),
						new RequestCurrent<>()
				);
				var responseBuilder = GetResponse.newBuilder();
				if (current != null) {
					responseBuilder.setValue(Utils.toByteString(current));
				}
				return responseBuilder.build();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("get", request));
		}

		@Override
		public Mono<UpdateBegin> getForUpdate(GetRequest request) {
			return executeSync(() -> {
				var forUpdate = api.get(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getKeysCount(), request::getKeys),
						new RequestForUpdate<>()
				);
				var responseBuilder = UpdateBegin.newBuilder();
				responseBuilder.setUpdateId(forUpdate.updateId());
				if (forUpdate.previous() != null) {
					responseBuilder.setPrevious(Utils.toByteString(forUpdate.previous()));
				}
				return responseBuilder.build();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("getForUpdate", request));
		}

		@Override
		public Mono<PreviousPresence> exists(GetRequest request) {
			return executeSync(() -> {
				var exists = api.get(request.getTransactionOrUpdateId(),
						request.getColumnId(),
						mapKeys(request.getKeysCount(), request::getKeys),
						new RequestExists<>()
				);
				return PreviousPresence.newBuilder().setPresent(exists).build();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("exists", request));
		}

		@Override
		public Mono<OpenIteratorResponse> openIterator(OpenIteratorRequest request) {
			return executeScheduled(() -> {
				var iteratorId = api.openIterator(request.getTransactionId(),
						request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
						mapKeys(request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
						request.getReverse(),
						request.getTimeoutMs()
				);
				return OpenIteratorResponse.newBuilder().setIteratorId(iteratorId).build();
			}, scheduler.read(), response -> {
				long iteratorId = response.getIteratorId();
				api.closeIterator(iteratorId);
			}, scheduler.control()).transform(this.onErrorMapMonoWithRequestInfo("openIterator", request));
		}

		@Override
		public Mono<Empty> closeIterator(CloseIteratorRequest request) {
			return executeScheduled(() -> {
					api.closeIterator(request.getIteratorId());
					return Empty.getDefaultInstance();
				}, scheduler.control())
					.transform(this.onErrorMapMonoWithRequestInfo("closeIterator", request));
		}

		@Override
		public Mono<Empty> seekTo(SeekToRequest request) {
			return withIteratorLease(request.getIterationId(), () -> executeCompositeRead(() -> {
				api.seekTo(request.getIterationId(), mapKeys(request.getKeysCount(), request::getKeys));
				return Empty.getDefaultInstance();
			})).transform(this.onErrorMapMonoWithRequestInfo("seekTo", request));
		}

		@Override
		public Mono<Empty> subsequent(SubsequentRequest request) {
			return validateIteratorCounts(request)
					.then(withIteratorLease(request.getIterationId(), () ->
							advanceIterator(request.getIterationId(), request.getSkipCount(), request.getTakeCount())
									.thenReturn(Empty.getDefaultInstance())))
					.transform(this.onErrorMapMonoWithRequestInfo("subsequent", request));
		}

		@Override
		public Mono<PreviousPresence> subsequentExists(SubsequentRequest request) {
			return validateIteratorCounts(request)
					.then(withIteratorLease(request.getIterationId(), () ->
							advanceIterator(request.getIterationId(), request.getSkipCount(), 0)
									.thenMany(iteratorChunks(request.getTakeCount(), ITERATOR_ADVANCE_STEP_SIZE))
									.concatMap(take -> executeCompositeRead(() -> api.subsequent(
											request.getIterationId(), 0, take, new RequestExists<>())), 1)
									.takeUntil(found -> !found)
									.reduce(false, (found, pageFound) -> found || pageFound)
									.map(found -> PreviousPresence.newBuilder().setPresent(found).build())))
					.transform(this.onErrorMapMonoWithRequestInfo("subsequentExists", request));
		}

		@Override
		public Flux<KV> subsequentMultiGet(SubsequentRequest request) {
			return validateIteratorCounts(request)
					.thenMany(withIteratorFluxLease(request.getIterationId(), () ->
							advanceIterator(request.getIterationId(), request.getSkipCount(), 0)
									.thenMany(iteratorChunks(request.getTakeCount(), ITERATOR_VALUE_PAGE_SIZE)
											.concatMap(take -> executeCompositeRead(() -> api.subsequent(
													request.getIterationId(), 0, take, new RequestMulti<>())), 1)
											.takeUntil(values -> values.size() < ITERATOR_VALUE_PAGE_SIZE)
											.concatMapIterable(Function.identity(), 1)
											.map(entry -> KV.newBuilder()
													.setValue(Utils.toByteString(entry))
													.build()))))
					.transform(this.onErrorMapFluxWithRequestInfo("subsequentMultiGet", request));
		}

		@Override
		public Mono<FirstAndLast> reduceRangeFirstAndLast(GetRangeRequest request) {
			var transportDeadline = Context.current().getDeadline();
			return Mono.defer(() -> fromCancellableFuture(asyncApi.reduceRangeAsync(
						request.getTransactionId(),
						request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
						mapKeys(request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
						request.getReverse(),
						RequestType.firstAndLast(),
						effectiveReadTimeoutMillis(request.getTimeoutMs(), transportDeadline))))
					.map(range -> {
						if (range.first() == null || range.last() == null) {
							return FirstAndLast.getDefaultInstance();
						}
						return FirstAndLast.newBuilder()
								.setFirst(unmapKVHeap(range.first()))
								.setLast(unmapKVHeap(range.last()))
								.build();
					})
					.transform(this.onErrorMapMonoWithRequestInfo("reduceRangeFirstAndLast", request));
		}

		@Override
		public Mono<EntriesCount> reduceRangeEntriesCount(GetRangeRequest request) {
			var transportDeadline = Context.current().getDeadline();
			return Mono.defer(() -> fromCancellableFuture(asyncApi.reduceRangeAsync(
						request.getTransactionId(),
						request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
						mapKeys(request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
						request.getReverse(),
						RequestType.entriesCount(),
						effectiveReadTimeoutMillis(request.getTimeoutMs(), transportDeadline))))
					.map(count -> EntriesCount.newBuilder().setCount(count).build())
					.transform(this.onErrorMapMonoWithRequestInfo("reduceRangeEntriesCount", request));
		}

		private long effectiveReadTimeoutMillis(long requestedTimeoutMs, @Nullable Deadline transportDeadline) {
			if (requestedTimeoutMs < 0 || transportDeadline == null) {
				return requestedTimeoutMs;
			}
			long transportRemainingMs = Math.max(0L,
					transportDeadline.timeRemaining(TimeUnit.MILLISECONDS));
			return Math.min(requestedTimeoutMs, transportRemainingMs);
		}

		@Override
		public Flux<KV> getAllInRange(GetRangeRequest request) {
			return getAllInRange(request, RequestType.allInRange(), "getAllInRange");
		}

		@Override
		public Flux<KV> getAllInRangeNoCache(GetRangeRequest request) {
			return getAllInRange(request, RequestType.allInRangeNoCache(), "getAllInRangeNoCache");
		}

		private Flux<KV> getAllInRange(GetRangeRequest request,
				RequestType.RequestGetRange<? super it.cavallium.rockserver.core.common.KV,
						it.cavallium.rockserver.core.common.KV> requestType,
				String requestName) {
			var transportDeadline = Context.current().getDeadline();
			return Flux.defer(() -> Flux
					.from(asyncApi.getRangeAsync(request.getTransactionId(),
							request.getColumnId(),
							mapKeys(request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
							mapKeys(request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
							request.getReverse(),
							requestType,
							effectiveReadTimeoutMillis(request.getTimeoutMs(), transportDeadline))))
					.map(GrpcServerImpl::unmapKVHeap)
					.transform(this.onErrorMapFluxWithRequestInfo(requestName, request));
		}

		@Override
		public Flux<it.cavallium.rockserver.core.common.api.proto.ScanRawResponse> scanRaw(ScanRawRequest request) {
			return Flux.defer(() -> Flux
					.from(asyncApi.scanRawAsync(request.getColumnId(), request.getShardIndex(), request.getShardCount())))
					.map(batch -> {
						var builder = it.cavallium.rockserver.core.common.api.proto.ScanRawResponse.newBuilder();
						var serializedBatchValue = UnsafeByteOperations.unsafeWrap(
								batch.serialized().getBackingByteArray(),
								batch.serialized().getBackingByteArrayOffset(),
								batch.serialized().getBackingByteArrayLength()
						);
						builder.setSerialized(serializedBatchValue);
						return builder.build();
					})
					.limitRate(17, 1)
					.transform(this.onErrorMapFluxWithRequestInfo("scanRaw", request));
		}

		@Override
		public Mono<Empty> flush(FlushRequest request) {
			return executeScheduled(() -> {
				api.flush();
				return Empty.getDefaultInstance();
			}, scheduler.maintenance()).transform(this.onErrorMapMonoWithRequestInfo("flush", request));
		}

		@Override
		public Mono<Empty> compact(CompactRequest request) {
			return executeScheduled(() -> {
				api.compact();
				return Empty.getDefaultInstance();
			}, scheduler.maintenance()).transform(this.onErrorMapMonoWithRequestInfo("compact", request));
		}

		@Override
		public Mono<GetAllColumnDefinitionsResponse> getAllColumnDefinitions(GetAllColumnDefinitionsRequest request) {
			return executeSync(() -> {
				var definitions = api.getAllColumnDefinitions();
				var builder = GetAllColumnDefinitionsResponse.newBuilder();
				for (Entry<String, ColumnSchema> e : definitions.entrySet()) {
					builder.addColumns(Column.newBuilder().setName(e.getKey()).setSchema(unmapColumnSchema(e.getValue())));
				}
 			return builder.build();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("getAllColumnDefinitions", request));
		}

		@Override
		public Mono<UploadMergeOperatorResponse> uploadMergeOperator(UploadMergeOperatorRequest request) {
			return executeSync(() -> {
				var version = api.uploadMergeOperator(request.getOperatorName(), request.getClassName(), request.getJarPayload().toByteArray());
				return UploadMergeOperatorResponse.newBuilder().setVersion(version).build();
			}, false).transform(this.onErrorMapMonoWithRequestInfo("uploadMergeOperator", request));
		}

		@Override
		public Mono<CheckMergeOperatorResponse> checkMergeOperator(CheckMergeOperatorRequest request) {
			return executeSync(() -> {
				var version = api.checkMergeOperator(request.getOperatorName(), request.getHash().toByteArray());
				var builder = CheckMergeOperatorResponse.newBuilder();
				if (version != null) {
					builder.setVersion(version);
				}
				return builder.build();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("checkMergeOperator", request));
		}

            // ============ CDC RPCs ============

            @Override
            public Mono<CdcCreateResponse> cdcCreate(CdcCreateRequest request) {
                return executeSync(() -> {
                    Long fromSeq = request.hasFromSeq() ? request.getFromSeq() : null;
                    var cols = request.getColumnIdsCount() > 0 ? request.getColumnIdsList().stream().map(Long::valueOf).toList() : null;
                    Boolean resolvedValues = request.hasResolvedValues() ? request.getResolvedValues() : null;
                    long startSeq = api.cdcCreate(request.getId(), fromSeq, cols, resolvedValues);
                    return CdcCreateResponse.newBuilder().setStartSeq(startSeq).build();
                }, false).transform(this.onErrorMapMonoWithRequestInfo("cdcCreate", request));
            }

            @Override
            public Mono<Empty> cdcDelete(CdcDeleteRequest request) {
                return executeSync(() -> {
                    api.cdcDelete(request.getId());
                    return Empty.getDefaultInstance();
                }, false).transform(this.onErrorMapMonoWithRequestInfo("cdcDelete", request));
            }

            @Override
            public Flux<it.cavallium.rockserver.core.common.api.proto.CDCEvent> cdcPoll(CdcPollRequest request) {
                return Flux.defer(() -> {
                            long maxEvents = request.getMaxEvents() > 0 ? request.getMaxEvents() : 10_000L;
                            Long fromSeq = request.hasFromSeq() ? request.getFromSeq() : null;
                            int maxResponseBytes = requestedMaxResponseBytes(request);
                            return Flux.from(asyncApi.cdcPollAsync(request.getId(), fromSeq, maxEvents))
                                    .map(event -> CdcResponseBudget.buildEvent(event, maxResponseBytes));
                        })
                        .transform(this.onErrorMapFluxWithRequestInfo("cdcPoll", request));
            }

            @Override
            public Mono<CdcPollResponse> cdcPollBatch(CdcPollRequest request) {
                return Mono.defer(() -> {
                            long maxEvents = request.getMaxEvents() > 0 ? request.getMaxEvents() : 10_000L;
                            Long fromSeq = request.hasFromSeq() ? request.getFromSeq() : null;
                            int maxResponseBytes = requestedMaxResponseBytes(request);
                            return asyncApi.cdcPollBatchAsync(request.getId(), fromSeq, maxEvents)
                                    .map(batch -> CdcResponseBudget.build(batch, maxResponseBytes));
                        })
                        .transform(this.onErrorMapMonoWithRequestInfo("cdcPollBatch", request));
            }

            private static int requestedMaxResponseBytes(CdcPollRequest request) {
                int requestedMaxResponseBytes = request.getMaxResponseBytes();
                if (requestedMaxResponseBytes < 0) {
                    throw Status.INVALID_ARGUMENT
                            .withDescription("maxResponseBytes must not be negative: "
                                    + requestedMaxResponseBytes)
                            .asRuntimeException();
                }
                return requestedMaxResponseBytes > 0
                        ? requestedMaxResponseBytes
                        : LEGACY_GRPC_MAX_INBOUND_MESSAGE_SIZE;
            }

            @Override
            public Mono<Empty> cdcCommit(CdcCommitRequest request) {
                return executeSync(() -> {
                    api.cdcCommit(request.getId(), request.getSeq());
                    return Empty.getDefaultInstance();
                }, false).transform(this.onErrorMapMonoWithRequestInfo("cdcCommit", request));
            }

		// utils

		private <T> Mono<T> executeSync(Callable<T> callable, boolean isReadOnly) {
			return executeScheduled(callable, isReadOnly ? scheduler.interactiveRead() : scheduler.write());
		}

		private <T> Mono<T> executeCompositeRead(Callable<T> callable) {
			return executeScheduled(callable, scheduler.read());
		}

		private Mono<Void> validateIteratorCounts(SubsequentRequest request) {
			if (request.getSkipCount() < 0 || request.getTakeCount() < 0) {
				return Mono.error(RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Iterator skip and take counts must be non-negative"));
			}
			return Mono.empty();
		}

		private Flux<Long> iteratorChunks(long count, long stepSize) {
			return Flux.generate(() -> count, (remaining, sink) -> {
				if (remaining <= 0) {
					sink.complete();
					return 0L;
				}
				long chunk = Math.min(remaining, stepSize);
				sink.next(chunk);
				return remaining - chunk;
			});
		}

		private Mono<Void> advanceIterator(long iteratorId, long skipCount, long takeCount) {
			return advanceIteratorPart(iteratorId, skipCount)
					.then(advanceIteratorPart(iteratorId, takeCount));
		}

		private Mono<Void> advanceIteratorPart(long iteratorId, long count) {
			return iteratorChunks(count, ITERATOR_ADVANCE_STEP_SIZE)
					.concatMap(step -> executeCompositeRead(() -> api.subsequent(
							iteratorId, 0, step, new RequestExists<>())), 1)
					.takeUntil(found -> !found)
					.then();
		}

		private <T> Mono<T> withIteratorLease(long iteratorId, Supplier<Mono<T>> operation) {
			return Mono.defer(() -> {
				var lease = new IteratorOperationLease(iteratorId);
				if (iteratorOperations.putIfAbsent(iteratorId, lease) != null) {
					return Mono.error(concurrentIteratorOperation(iteratorId));
				}
				try {
					return operation.get()
							.doFinally(_ -> lease.operationTerminated())
							.contextWrite(context -> context.put(ITERATOR_OPERATION_LEASE_CONTEXT_KEY, lease));
				} catch (Throwable error) {
					lease.operationTerminated();
					return Mono.error(error);
				}
			});
		}

		private <T> Flux<T> withIteratorFluxLease(long iteratorId, Supplier<Flux<T>> operation) {
			return Flux.defer(() -> {
				var lease = new IteratorOperationLease(iteratorId);
				if (iteratorOperations.putIfAbsent(iteratorId, lease) != null) {
					return Flux.error(concurrentIteratorOperation(iteratorId));
				}
				try {
					return operation.get()
							.doFinally(_ -> lease.operationTerminated())
							.contextWrite(context -> context.put(ITERATOR_OPERATION_LEASE_CONTEXT_KEY, lease));
				} catch (Throwable error) {
					lease.operationTerminated();
					return Flux.error(error);
				}
			});
		}

		/**
		 * A subscriber owns the iterator lease until it terminates and every native task that
		 * it started has actually returned. Cancelling an RPC can interrupt a RocksDB call,
		 * but JNI is allowed to keep running until its own deadline; releasing the lease from
		 * {@code doFinally(CANCEL)} alone would then admit a concurrent operation on the same
		 * native iterator.
		 */
		private final class IteratorOperationLease {

			private static final int OPERATION_TERMINATED = 1 << 31;
			private static final int ACTIVE_TASKS_MASK = Integer.MAX_VALUE;

			private final long iteratorId;
			private final AtomicInteger state = new AtomicInteger();

			private IteratorOperationLease(long iteratorId) {
				this.iteratorId = iteratorId;
			}

			private boolean registerTask() {
				while (true) {
					int current = state.get();
					if ((current & OPERATION_TERMINATED) != 0) {
						return false;
					}
					if ((current & ACTIVE_TASKS_MASK) == ACTIVE_TASKS_MASK) {
						throw new IllegalStateException("Too many active iterator tasks");
					}
					if (state.compareAndSet(current, current + 1)) {
						return true;
					}
				}
			}

			private void taskTerminated() {
				while (true) {
					int current = state.get();
					int activeTasks = current & ACTIVE_TASKS_MASK;
					if (activeTasks == 0) {
						throw new IllegalStateException("Iterator task accounting underflow");
					}
					int updated = (current & OPERATION_TERMINATED) | (activeTasks - 1);
					if (state.compareAndSet(current, updated)) {
						if (updated == OPERATION_TERMINATED) {
							iteratorOperations.remove(iteratorId, this);
						}
						return;
					}
				}
			}

			private void operationTerminated() {
				while (true) {
					int current = state.get();
					if ((current & OPERATION_TERMINATED) != 0) {
						return;
					}
					int updated = current | OPERATION_TERMINATED;
					if (state.compareAndSet(current, updated)) {
						if (updated == OPERATION_TERMINATED) {
							iteratorOperations.remove(iteratorId, this);
						}
						return;
					}
				}
			}
		}

		/** Tracks whether cancellation won before a scheduled callable began running. */
		private final class ScheduledTaskLifecycle {

			private static final int QUEUED = 0;
			private static final int RUNNING = 1;
			private static final int TERMINATED = 2;

			private final IteratorOperationLease iteratorLease;
			private final AtomicInteger state = new AtomicInteger(QUEUED);

			private ScheduledTaskLifecycle(IteratorOperationLease iteratorLease) {
				this.iteratorLease = iteratorLease;
			}

			private boolean start() {
				return state.compareAndSet(QUEUED, RUNNING);
			}

			private void cancelBeforeStart() {
				if (state.compareAndSet(QUEUED, TERMINATED)) {
					taskTerminated();
				}
			}

			private void runningTaskTerminated() {
				if (!state.compareAndSet(RUNNING, TERMINATED)) {
					throw new IllegalStateException("Scheduled task did not terminate from running state");
				}
				taskTerminated();
			}

			private void taskTerminated() {
				iteratorLease.taskTerminated();
			}
		}

		private RocksDBException concurrentIteratorOperation(long iteratorId) {
			return RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Concurrent operation on iterator " + iteratorId + " is not supported");
		}

		/**
		 * Bridge an externally-owned future without delivering its terminal signal after
		 * the RPC has been cancelled. Native calls may finish after cancellation; their
		 * late failures still go through the request-scoped diagnostic handler instead of
		 * Reactor's global onErrorDropped hook.
		 */
		private <T> Mono<T> fromCancellableFuture(CompletableFuture<T> future) {
			return Mono.create(sink -> {
				var emissionLock = new Object();
				var cancelled = new AtomicBoolean();
				sink.onCancel(() -> {
					synchronized (emissionLock) {
						cancelled.set(true);
					}
					future.cancel(true);
				});
				future.whenComplete((value, failure) -> {
					Throwable error = failure instanceof CompletionException completionError
							&& completionError.getCause() != null
							? completionError.getCause()
							: failure;
					boolean late;
					synchronized (emissionLock) {
						late = cancelled.get();
						if (!late) {
							if (error != null) {
								sink.error(error);
							} else {
								sink.success(value);
							}
						}
					}
					if (late && error != null && !(error instanceof java.util.concurrent.CancellationException)) {
						lateErrorHandler(sink.contextView()).accept(error);
					}
				});
			});
		}

		private <T> Mono<T> executeScheduled(Callable<T> callable, reactor.core.scheduler.Scheduler executionScheduler) {
			return executeScheduled(callable, executionScheduler, null, null);
		}

		private <T> Mono<T> executeScheduled(Callable<T> callable,
				reactor.core.scheduler.Scheduler executionScheduler,
				@Nullable Consumer<T> lateSuccessCleanup,
				@Nullable reactor.core.scheduler.Scheduler lateSuccessCleanupScheduler) {
			return Mono.deferContextual(contextView -> {
				var iteratorLease = contextView.<IteratorOperationLease>getOrDefault(
						ITERATOR_OPERATION_LEASE_CONTEXT_KEY, null);
				ScheduledTaskLifecycle taskLifecycle;
				if (iteratorLease != null) {
					if (!iteratorLease.registerTask()) {
						return Mono.error(new java.util.concurrent.CancellationException(
								"Iterator operation terminated before its task could be scheduled"));
					}
					taskLifecycle = new ScheduledTaskLifecycle(iteratorLease);
				} else {
					taskLifecycle = null;
				}
				return Mono.<T>create(sink -> {
					var emissionLock = new Object();
					var cancelled = new AtomicBoolean();
					var task = Disposables.swap();

					// RocksDB JNI calls may keep running after interruption until their native deadline. Serialize
					// cancellation with terminal delivery so a late native error is not emitted to an already-cancelled
					// gRPC subscriber (which Reactor would otherwise report through onErrorDropped).
					sink.onCancel(() -> {
						synchronized (emissionLock) {
							cancelled.set(true);
						}
						if (taskLifecycle != null) {
							taskLifecycle.cancelBeforeStart();
						}
						task.dispose();
					});

					try {
						task.replace(executionScheduler.schedule(() -> {
							if (taskLifecycle != null && !taskLifecycle.start()) {
								return;
							}
							try {
								var result = callable.call();
								boolean lateSuccess;
								synchronized (emissionLock) {
									lateSuccess = cancelled.get();
									if (!lateSuccess) {
										sink.success(result);
									}
								}
								if (lateSuccess && lateSuccessCleanup != null) {
									Runnable cleanup = () -> {
										try {
											lateSuccessCleanup.accept(result);
										} catch (Throwable cleanupError) {
											lateErrorHandler(sink.contextView()).accept(cleanupError);
										}
									};
									try {
										if (lateSuccessCleanupScheduler != null) {
											lateSuccessCleanupScheduler.schedule(cleanup);
										} else {
											cleanup.run();
										}
									} catch (Throwable schedulingError) {
										LOG.debug("Late-success cleanup scheduler rejected the task; "
												+ "running iterator cleanup inline", schedulingError);
										cleanup.run();
									}
								}
							} catch (Throwable error) {
								boolean lateError;
								synchronized (emissionLock) {
									lateError = cancelled.get();
									if (!lateError) {
										sink.error(error);
									}
								}
								if (lateError) {
									lateErrorHandler(sink.contextView()).accept(error);
								}
							} finally {
								if (taskLifecycle != null) {
									taskLifecycle.runningTaskTerminated();
								}
							}
						}));
					} catch (Throwable schedulingError) {
						if (taskLifecycle != null) {
							taskLifecycle.cancelBeforeStart();
						}
						boolean lateError;
						synchronized (emissionLock) {
							lateError = cancelled.get();
							if (!lateError) {
								sink.error(schedulingError);
							}
						}
						if (lateError) {
							lateErrorHandler(sink.contextView()).accept(schedulingError);
						}
					}
				});
			});
		}

		// mappers

		private <T> Function<Flux<T>, Flux<T>> onErrorMapFluxWithRequestInfo(String requestName, Message request) {
			return flux -> {
				var lateErrorHandler = lateRequestErrorHandler(requestName, request);
				return flux
						.onErrorResume(throwable -> Mono.error(mapRequestError(requestName, request, throwable)))
						.contextWrite(context -> context
								.put(GRPC_LATE_ERROR_HANDLER_CONTEXT_KEY, lateErrorHandler)
								.put(REACTOR_ON_ERROR_DROPPED_CONTEXT_KEY, lateErrorHandler));
			};
		}

		private <T> Function<Mono<T>, Mono<T>> onErrorMapMonoWithRequestInfo(String requestName, Message request) {
			return mono -> {
				var lateErrorHandler = lateRequestErrorHandler(requestName, request);
				return mono
						.onErrorResume(throwable -> Mono.error(mapRequestError(requestName, request, throwable)))
						.contextWrite(context -> context
								.put(GRPC_LATE_ERROR_HANDLER_CONTEXT_KEY, lateErrorHandler)
								.put(REACTOR_ON_ERROR_DROPPED_CONTEXT_KEY, lateErrorHandler));
			};
		}

		private Throwable mapRequestError(String requestName, Message request, Throwable throwable) {
			var ex = handleError(throwable).asException();
			if (ex.getStatus().getCode() == Code.INTERNAL && findRocksDBException(throwable) == null) {
				LOG.error("Unexpected internal gRPC request failure: operation={}, requestType={}, request={}",
						requestName,
						request.getDescriptorForType().getFullName(),
						summarizeRequest(request),
						throwable);
				return RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR,
						ex.getCause() != null ? ex.getCause() : throwable);
			}
			return ex;
		}

		private static Consumer<Throwable> lateErrorHandler(ContextView context) {
			return context.getOrDefault(GRPC_LATE_ERROR_HANDLER_CONTEXT_KEY,
					UNCONTEXTUALIZED_LATE_ERROR_HANDLER);
		}

		private static Consumer<Throwable> lateRequestErrorHandler(String requestName, Message request) {
			return error -> logLateRequestError(requestName, request, error);
		}

		private static void logLateRequestError(String requestName, Message request, Throwable error) {
			var requestType = request.getDescriptorForType().getFullName();
			var requestSummary = summarizeRequest(request);
			var rocksDBException = findRocksDBException(error);
			var status = Status.fromThrowable(error);

			if (rocksDBException != null) {
				var statusCode = status.getCode() == Code.UNKNOWN ? Code.INTERNAL : status.getCode();
				LOG.warn("Late gRPC request failure after call termination: operation={}, requestType={}, "
						+ "request={}, errorType={}, grpcStatus={}, message={}",
						requestName,
						requestType,
						requestSummary,
						rocksDBException.getErrorUniqueId(),
						statusCode,
						sanitizeForLog(rocksDBException.getMessage()));
				LOG.debug("Late gRPC request failure stack: operation={}, requestType={}",
						requestName,
						requestType,
						error);
				return;
			}

			if (status.getCode() == Code.CANCELLED
					|| error instanceof java.util.concurrent.CancellationException) {
				LOG.debug("Late gRPC cancellation after call termination: operation={}, requestType={}, request={}, "
						+ "description={}",
						requestName,
						requestType,
						requestSummary,
						sanitizeForLog(status.getDescription()));
				return;
			}

			if (status.getCode() != Code.UNKNOWN && status.getCode() != Code.INTERNAL) {
				LOG.warn("Late gRPC transport failure after call termination: operation={}, requestType={}, request={}, "
						+ "grpcStatus={}, description={}",
						requestName,
						requestType,
						requestSummary,
						status.getCode(),
						sanitizeForLog(status.getDescription()));
				LOG.debug("Late gRPC transport failure stack: operation={}, requestType={}",
						requestName,
						requestType,
						error);
				return;
			}

			LOG.error("Unexpected late gRPC request failure after call termination: operation={}, requestType={}, "
					+ "request={}, grpcStatus={}, description={}",
					requestName,
					requestType,
					requestSummary,
					status.getCode(),
					sanitizeForLog(status.getDescription()),
					error);
		}

		@Nullable
		private static RocksDBException findRocksDBException(Throwable error) {
			var current = error;
			for (int depth = 0; current != null && depth < 32; depth++) {
				if (current instanceof RocksDBException rocksDBException) {
					return rocksDBException;
				}
				var cause = current.getCause();
				if (cause == current) {
					break;
				}
				current = cause;
			}
			return null;
		}

		private static String summarizeRequest(Message request) {
			try {
				return summarizeMessage(request, 0);
			} catch (Throwable summaryError) {
				return "{summaryUnavailable=" + summaryError.getClass().getSimpleName() + "}";
			}
		}

		private static String summarizeMessage(Message message, int depth) {
			var fields = new StringJoiner(", ", "{", "}");
			for (FieldDescriptor field : message.getDescriptorForType().getFields()) {
				var fieldName = field.getJsonName();
				var value = message.getField(field);
				if (field.isRepeated()) {
					fields.add(fieldName + ".count=" + ((List<?>) value).size());
					continue;
				}
				if (field.hasPresence() && !message.hasField(field)) {
					continue;
				}
				fields.add(fieldName + "=" + summarizeFieldValue(field, value, depth));
			}
			return fields.toString();
		}

		private static String summarizeFieldValue(FieldDescriptor field, Object value, int depth) {
			return switch (field.getJavaType()) {
				case BYTE_STRING -> "bytes(" + ((ByteString) value).size() + ")";
				case MESSAGE -> depth < 1
						? summarizeMessage((Message) value, depth + 1)
						: "message(" + ((Message) value).getDescriptorForType().getFullName() + ")";
				case STRING -> "\"" + sanitizeForLog(String.valueOf(value)) + "\"";
				default -> String.valueOf(value);
			};
		}

		private static String sanitizeForLog(@Nullable String value) {
			if (value == null) {
				return "<none>";
			}
			var sanitized = value
					.replace("\\", "\\\\")
					.replace("\r", "\\r")
					.replace("\n", "\\n");
			return sanitized.length() <= 256 ? sanitized : sanitized.substring(0, 253) + "...";
		}

		@Override
		protected Throwable onErrorMap(Throwable throwable) {
			var ex = handleError(throwable).asException();
			if (ex.getStatus().getCode() == Code.INTERNAL && findRocksDBException(throwable) == null) {
				LOG.error("Unexpected internal error during request", ex);
			}
			return ex;
		}

		private static KV unmapKV(it.cavallium.rockserver.core.common.KV kv) {
			if (kv == null) return null;
			return KV.newBuilder()
					.addAllKeys(unmapKeys(kv.keys()))
					.setValue(unmapValue(kv.value()))
					.build();
		}

		private static KV unmapKVHeap(it.cavallium.rockserver.core.common.KV kv) {
			if (kv == null) return null;
			return KV.newBuilder()
					.addAllKeys(unmapKeysHeap(kv.keys()))
					.setValue(unmapValueHeap(kv.value()))
					.build();
		}

		private static List<ByteString> unmapKeys(@NotNull Keys keys) {
			var result = new ArrayList<ByteString>(keys.keys().length);
			for (@NotNull Buf key : keys.keys()) {
				result.add(Utils.toByteString(key));
			}
			return result;
		}

		private static List<ByteString> unmapKeysHeap(@NotNull Keys keys) {
			var result = new ArrayList<ByteString>(keys.keys().length);
			for (@NotNull Buf key : keys.keys()) {
				result.add(UnsafeByteOperations.unsafeWrap(toByteArray(key)));
			}
			return result;
		}

		private static ByteString unmapValue(@Nullable Buf value) {
			if (value == null) return null;
			return Utils.toByteString(value);
		}

		private static ByteString unmapValueHeap(@Nullable Buf value) {
			if (value == null) return null;
			return Utils.toByteString(value);
		}

		private static ColumnSchema mapColumnSchema(it.cavallium.rockserver.core.common.api.proto.ColumnSchema schema) {
			return ColumnSchema.of(mapKeysLength(schema.getFixedKeysCount(), schema::getFixedKeys),
					mapVariableTailKeys(schema.getVariableTailKeysCount(), schema::getVariableTailKeys),
					schema.getHasValue(),
					schema.hasMergeOperatorName() ? schema.getMergeOperatorName() : null,
					schema.hasMergeOperatorVersion() ? schema.getMergeOperatorVersion() : null,
					getMergeOperatorClass(schema)
			);
		}

		private static it.cavallium.rockserver.core.common.api.proto.ColumnSchema unmapColumnSchema(@NotNull ColumnSchema schema) {
			var builder = it.cavallium.rockserver.core.common.api.proto.ColumnSchema.newBuilder()
					.addAllFixedKeys(unmapFixedKeys(schema))
					.addAllVariableTailKeys(unmapVariableTailKeys(schema))
					.setHasValue(schema.hasValue());
			if (schema.mergeOperatorName() != null) {
				builder.setMergeOperatorName(schema.mergeOperatorName());
			}
			if (schema.mergeOperatorVersion() != null) {
				builder.setMergeOperatorVersion(schema.mergeOperatorVersion());
			}
			if (schema.mergeOperatorClass() != null) {
				invokeMergeOperatorClass(builder, schema.mergeOperatorClass());
			}
			return builder.build();
		}

		@Nullable
		private static String getMergeOperatorClass(Object schema) {
			try {
				var hasMethod = schema.getClass().getMethod("hasMergeOperatorClass");
				Boolean has = (Boolean) hasMethod.invoke(schema);
				if (Boolean.TRUE.equals(has)) {
					var getter = schema.getClass().getMethod("getMergeOperatorClass");
					return (String) getter.invoke(schema);
				}
			} catch (NoSuchMethodException e) {
				return null;
			} catch (ReflectiveOperationException e) {
				throw new RuntimeException(e);
			}
			return null;
		}

		private static void invokeMergeOperatorClass(Object builder, String mergeOperatorClass) {
			try {
				var setter = builder.getClass().getMethod("setMergeOperatorClass", String.class);
				setter.invoke(builder, mergeOperatorClass);
			} catch (NoSuchMethodException ignored) {
				// Older generated protos do not expose mergeOperatorClass; ignore to stay backward compatible
			} catch (ReflectiveOperationException e) {
				throw new RuntimeException(e);
			}
		}

		private static Iterable<Integer> unmapFixedKeys(@NotNull ColumnSchema schema) {
			var result = new IntArrayList(schema.fixedLengthKeysCount());
			for (int i = 0; i < schema.fixedLengthKeysCount(); i++) {
				result.add(schema.key(i));
			}
			return result;
		}

		private static Iterable<it.cavallium.rockserver.core.common.api.proto.ColumnHashType> unmapVariableTailKeys(@NotNull ColumnSchema schema) {
			var result = new ArrayList<it.cavallium.rockserver.core.common.api.proto.ColumnHashType>(schema.variableTailKeys().size());
			for (it.cavallium.rockserver.core.common.ColumnHashType variableTailKey : schema.variableTailKeys()) {
				result.add(switch (variableTailKey) {
					case XXHASH32 -> it.cavallium.rockserver.core.common.api.proto.ColumnHashType.XXHASH32;
					case XXHASH8 -> it.cavallium.rockserver.core.common.api.proto.ColumnHashType.XXHASH8;
					case ALLSAME8 -> it.cavallium.rockserver.core.common.api.proto.ColumnHashType.ALLSAME8;
					case FIXEDINTEGER32 -> it.cavallium.rockserver.core.common.api.proto.ColumnHashType.FIXEDINTEGER32;
				});
			}
			return result;
		}

		private static IntList mapKeysLength(int count, Int2IntFunction keyGetterAt) {
			var l = new IntArrayList(count);
			for (int i = 0; i < count; i++) {
				l.add((int) keyGetterAt.apply(i));
			}
			return l;
		}

		private static ObjectList<ColumnHashType> mapVariableTailKeys(int count,
				Int2ObjectFunction<it.cavallium.rockserver.core.common.api.proto.ColumnHashType> variableTailKeyGetterAt) {
			var l = new ObjectArrayList<ColumnHashType>(count);
			for (int i = 0; i < count; i++) {
				l.add(switch (variableTailKeyGetterAt.apply(i)) {
					case XXHASH32 -> ColumnHashType.XXHASH32;
					case XXHASH8 -> ColumnHashType.XXHASH8;
					case ALLSAME8 -> ColumnHashType.ALLSAME8;
					case FIXEDINTEGER32 -> ColumnHashType.FIXEDINTEGER32;
					case UNRECOGNIZED -> throw new UnsupportedOperationException();
				});
			}
			return l;
		}

		private static Keys mapKeys(int count, Int2ObjectFunction<ByteString> keyGetterAt) {
			var segments = new Buf[count];
			for (int i = 0; i < count; i++) {
				segments[i] = toBuf(keyGetterAt.apply(i));
			}
			return new Keys(segments);
		}

		private static List<Keys> mapKeysKV(int count, Int2ObjectFunction<KV> keyGetterAt) {
			var keys = new ArrayList<Keys>(count);
			for (int i = 0; i < count; i++) {
				var k = keyGetterAt.apply(i);
				keys.add(mapKeys(k.getKeysCount(), k::getKeys));
			}
			return keys;
		}

		private static List<Buf> mapValuesKV(int count, Int2ObjectFunction<KV> keyGetterAt) {
			var keys = new ArrayList<Buf>(count);
			for (int i = 0; i < count; i++) {
				keys.add(toBuf(keyGetterAt.get(i).getValue()));
			}
			return keys;
		}

		private static KVBatch mapKVBatch(int count, Int2ObjectFunction<KV> getterAt) {
			var kk = mapKeysKV(count, getterAt);
			var vv = mapValuesKV(count, getterAt);
			return new KVBatchRef(kk, vv);
		}

		private static Status handleError(Throwable ex) {
			if (ex instanceof StatusRuntimeException e && e.getStatus().getCode().equals(Status.CANCELLED.getCode())) {
				return e.getStatus();
			}

			if (ex instanceof CompletionException exx) {
				return handleError(exx.getCause());
			} else {
				return switch (ex) {
					case RocksDBException e -> e.getErrorUniqueId() == RocksDBErrorType.CDC_RESPONSE_TOO_LARGE
							? Status.FAILED_PRECONDITION.withDescription(e.getLocalizedMessage()).withCause(e)
							: Status.INTERNAL.withDescription(e.getLocalizedMessage()).withCause(e);
					case StatusException ex2 -> ex2.getStatus();
					case StatusRuntimeException ex3 -> ex3.getStatus();
					case null, default -> Status.INTERNAL.withCause(ex);
				};
			}
		}
	}

	@Override
	public void close() throws IOException {
		LOG.info("GRPC server is shutting down...");
		var gracefulShutdownTimeout = durationProperty("it.cavallium.rockserver.grpc.server.shutdown-graceful-timeout-ms",
				Duration.ofMinutes(1));
		var forcedShutdownTimeout = durationProperty("it.cavallium.rockserver.grpc.server.shutdown-forced-timeout-ms",
				Duration.ofMinutes(1));
		var schedulerShutdownTimeout = durationProperty("it.cavallium.rockserver.grpc.server.scheduler-shutdown-timeout-ms",
				Duration.ofMinutes(2));
		server.shutdown();
		try {
			if (!server.awaitTermination(gracefulShutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
				server.shutdownNow();
				if (!server.awaitTermination(forcedShutdownTimeout.toMillis(), TimeUnit.MILLISECONDS)) {
					LOG.error("GRPC server did not terminate after forced shutdown");
				}
			}
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOG.error("Server shutdown interrupted", e);
			server.shutdownNow();
		}
		try {
			elg.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			LOG.error("Grpc server event loop shutdown interrupted", e);
		}
		if (ownsScheduler) {
			scheduler.disposeGracefully().timeout(schedulerShutdownTimeout).onErrorResume(ex -> {
				LOG.error("Grpc server executor shutdown timed out, terminating...", ex);
				scheduler.dispose();
				return Mono.empty();
			}).block();
		}
		super.close();
		LOG.info("GRPC server shut down.");
	}

	private static Duration durationProperty(String name, Duration defaultValue) {
		var value = System.getProperty(name);
		if (value == null || value.isBlank()) {
			return defaultValue;
		}
		try {
			long millis = Long.parseLong(value);
			if (millis < 0) {
				LOG.warn("Invalid negative duration for system property {}: {}", name, value);
				return defaultValue;
			}
			return Duration.ofMillis(millis);
		} catch (NumberFormatException ex) {
			LOG.warn("Invalid duration in milliseconds for system property {}: {}", name, value);
			return defaultValue;
		}
	}
}
