package it.cavallium.rockserver.core.server;

import static it.cavallium.rockserver.core.common.Utils.toByteArray;
import static it.cavallium.rockserver.core.common.Utils.toBuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.protobuf.UnsafeByteOperations;
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
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import it.cavallium.rockserver.core.common.cdc.CDCEvent;

public class GrpcServer extends Server {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());

	private final GrpcServerImpl grpc;
	private final EventLoopGroup elg;
	private final io.grpc.Server server;
	private final RWScheduler scheduler;

	public GrpcServer(RocksDBConnection client, SocketAddress socketAddress) throws IOException {
		super(client);
		if (client instanceof InternalConnection internalConnection) {
			this.scheduler = internalConnection.getScheduler();
		} else {
			this.scheduler = new RWScheduler(Runtime.getRuntime().availableProcessors(),
					Runtime.getRuntime().availableProcessors(),
					"grpc-db"
			);
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

		private final RocksDBAsyncAPI asyncApi;
        private final RocksDBSyncAPI api;

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
			}, true).transform(this.onErrorMapMonoWithRequestInfo("closeFailedUpdate", request));
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
					return Mono.just(RocksDBException.of(
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
  				return Mono.just(RocksDBException.of(
  						RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
  			} else {
  				return requestFlux;
  			}
  		}).then(Mono.just(Empty.getDefaultInstance()));
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
					return Mono.just(RocksDBException.of(
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
					return Mono.just(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return requestsFlux;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
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
			return executeSync(() -> {
				var iteratorId = api.openIterator(request.getTransactionId(),
						request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
						mapKeys(request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
						request.getReverse(),
						request.getTimeoutMs()
				);
				return OpenIteratorResponse.newBuilder().setIteratorId(iteratorId).build();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("openIterator", request));
		}

		@Override
		public Mono<Empty> closeIterator(CloseIteratorRequest request) {
			return executeSync(() -> {
				api.closeIterator(request.getIteratorId());
				return Empty.getDefaultInstance();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("closeIterator", request));
		}

		@Override
		public Mono<Empty> seekTo(SeekToRequest request) {
			return executeSync(() -> {
				api.seekTo(request.getIterationId(), mapKeys(request.getKeysCount(), request::getKeys));
				return Empty.getDefaultInstance();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("seekTo", request));
		}

		@Override
		public Mono<Empty> subsequent(SubsequentRequest request) {
			return executeSync(() -> {
				api.subsequent(request.getIterationId(),
						request.getSkipCount(),
						request.getTakeCount(),
						new RequestNothing<>());
				return Empty.getDefaultInstance();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("subsequent", request));
		}

		@Override
		public Mono<PreviousPresence> subsequentExists(SubsequentRequest request) {
			return executeSync(() -> {
				var exists = api.subsequent(request.getIterationId(),
						request.getSkipCount(),
						request.getTakeCount(),
						new RequestExists<>());
				return PreviousPresence.newBuilder().setPresent(exists).build();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("subsequentExists", request));
		}

		@Override
		public Flux<KV> subsequentMultiGet(SubsequentRequest request) {
			return Flux.<KV>create(emitter -> {
				int pageIndex = 0;
				final long pageSize = 16L;
				while (request.getTakeCount() > pageIndex * pageSize) {
					var response = api.subsequent(request.getIterationId(),
							pageIndex == 0 ? request.getSkipCount() : 0,
							Math.min(request.getTakeCount() - pageIndex * pageSize, pageSize),
							new RequestMulti<>()
					);
					for (Buf entry : response) {
						Keys keys = null; // todo: implement
						Buf value = entry;
						emitter.next(KV.newBuilder()
								.addAllKeys(null) // todo: implement
								.setValue(Utils.toByteString(value))
								.build());
					}
					pageIndex++;
				}
				emitter.complete();
			}, FluxSink.OverflowStrategy.BUFFER)
					.subscribeOn(GrpcServer.this.scheduler.read())
					.transform(this.onErrorMapFluxWithRequestInfo("subsequentMultiGet", request));
		}

		@Override
		public Mono<FirstAndLast> reduceRangeFirstAndLast(GetRangeRequest request) {
			return executeSync(() -> {
				var firstAndLast = api.reduceRange(request.getTransactionId(), request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
						mapKeys(request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
						request.getReverse(),
						RequestType.firstAndLast(),
						request.getTimeoutMs()
				);
				var resultBuilder = FirstAndLast.newBuilder();
				if (firstAndLast.first() != null) {
					resultBuilder.setFirst(unmapKVHeap(firstAndLast.first()));
				}
				if (firstAndLast.last() != null) {
					resultBuilder.setLast(unmapKVHeap(firstAndLast.last()));
				}
				return resultBuilder.build();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("reduceRangeFirstAndLast", request));
		}

		@Override
		public Mono<EntriesCount> reduceRangeEntriesCount(GetRangeRequest request) {
			return executeSync(() -> {
				long entriesCount
						= api.reduceRange(request.getTransactionId(),
						request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
						mapKeys(request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
						request.getReverse(),
						RequestType.entriesCount(),
						request.getTimeoutMs()
				);
				return EntriesCount.newBuilder().setCount(entriesCount).build();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("reduceRangeEntriesCount", request));
		}

		@Override
		public Flux<KV> getAllInRange(GetRangeRequest request) {
			return Flux
					.from(asyncApi.getRangeAsync(request.getTransactionId(),
							request.getColumnId(),
							mapKeys(request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
							mapKeys(request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
							request.getReverse(),
							RequestType.allInRange(),
							request.getTimeoutMs()
					))
					.map(GrpcServerImpl::unmapKVHeap)
					.transform(this.onErrorMapFluxWithRequestInfo("getAllInRange", request));
		}

		@Override
		public Flux<it.cavallium.rockserver.core.common.api.proto.ScanRawResponse> scanRaw(ScanRawRequest request) {
			return Flux
					.from(asyncApi.scanRawAsync(request.getColumnId(), request.getShardIndex(), request.getShardCount()))
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
			return executeSync(() -> {
				api.flush();
				return Empty.getDefaultInstance();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("flush", request));
		}

		@Override
		public Mono<Empty> compact(CompactRequest request) {
			return executeSync(() -> {
				api.compact();
				return Empty.getDefaultInstance();
			}, true).transform(this.onErrorMapMonoWithRequestInfo("compact", request));
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
                long maxEvents = request.getMaxEvents() > 0 ? request.getMaxEvents() : 10_000L;
                Long fromSeq = request.hasFromSeq() ? request.getFromSeq() : null;
                return Flux
                        .from(asyncApi.cdcPollAsync(request.getId(), fromSeq, maxEvents))
                        .map(GrpcServerImpl::mapCDCEvent)
                        .transform(this.onErrorMapFluxWithRequestInfo("cdcPoll", request));
            }

            @Override
            public Mono<Empty> cdcCommit(CdcCommitRequest request) {
                return executeSync(() -> {
                    api.cdcCommit(request.getId(), request.getSeq());
                    return Empty.getDefaultInstance();
                }, false).transform(this.onErrorMapMonoWithRequestInfo("cdcCommit", request));
            }

            private static it.cavallium.rockserver.core.common.api.proto.CDCEvent mapCDCEvent(CDCEvent ev) {
                var builder = it.cavallium.rockserver.core.common.api.proto.CDCEvent.newBuilder()
                        .setSeq(ev.seq())
                        .setColumnId(ev.columnId())
                        .setKey(Utils.toByteString(ev.key()));
                if (ev.value() != null && !ev.value().isEmpty()) {
                    builder.setValue(Utils.toByteString(ev.value()));
                }
                var op = switch (ev.op()) {
                    case PUT -> it.cavallium.rockserver.core.common.api.proto.CDCEvent.Op.PUT;
                    case DELETE -> it.cavallium.rockserver.core.common.api.proto.CDCEvent.Op.DELETE;
                    case MERGE -> it.cavallium.rockserver.core.common.api.proto.CDCEvent.Op.MERGE;
                };
                builder.setOp(op);
                return builder.build();
            }

		// utils

		private <T> Mono<T> executeSync(Callable<T> callable, boolean isReadOnly) {
			return Mono.fromCallable(callable).subscribeOn(isReadOnly ? scheduler.read() : scheduler.write());
		}

		// mappers

		private <T> Function<Flux<T>, Flux<T>> onErrorMapFluxWithRequestInfo(String requestName, Message request) {
			return flux -> flux.onErrorResume(throwable -> {
				var ex = handleError(throwable).asException();
				if (ex.getStatus().getCode() == Code.INTERNAL && !(throwable instanceof RocksDBException)) {
					LOG.error("Unexpected internal error during request \"{}\": {}", requestName, request.toString(), ex);
					return Mono.error(RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, ex.getCause()));
				}
				return Mono.error(ex);
			});
		}

		private <T> Function<Mono<T>, Mono<T>> onErrorMapMonoWithRequestInfo(String requestName, Message request) {
			return flux -> flux.onErrorResume(throwable -> {
				var ex = handleError(throwable).asException();
				if (ex.getStatus().getCode() == Code.INTERNAL && !(throwable instanceof RocksDBException)) {
					LOG.error("Unexpected internal error during request \"{}\": {}", requestName, request.toString(), ex);
					return Mono.error(RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, ex.getCause()));
				}
				return Mono.error(ex);
			});
		}

		@Override
		protected Throwable onErrorMap(Throwable throwable) {
			var ex = handleError(throwable).asException();
			if (ex.getStatus().getCode() == Code.INTERNAL && !(throwable.getCause() instanceof RocksDBException)) {
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
				LOG.warn("Connection cancelled: {}", e.getStatus().getDescription());
				return e.getStatus();
			}

			if (ex instanceof CompletionException exx) {
				return handleError(exx.getCause());
			} else {
				return switch (ex) {
					case RocksDBException e -> Status.INTERNAL.withDescription(e.getLocalizedMessage()).withCause(e);
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
		server.shutdown();
		try {
			server.awaitTermination();
		} catch (InterruptedException e) {
			LOG.error("Server shutdown interrupted", e);
		}
		elg.close();
		scheduler.disposeGracefully().timeout(Duration.ofMinutes(2)).onErrorResume(ex -> {
			LOG.error("Grpc server executor shutdown timed out, terminating...", ex);
			scheduler.dispose();
			return Mono.empty();
		}).block();
		super.close();
	}
}
