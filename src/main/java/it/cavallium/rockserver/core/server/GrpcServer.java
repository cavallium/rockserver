package it.cavallium.rockserver.core.server;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegment;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.ServerCallStreamObserver;
import io.grpc.stub.StreamObserver;
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
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestType.RequestChanged;
import it.cavallium.rockserver.core.common.RequestType.RequestCurrent;
import it.cavallium.rockserver.core.common.RequestType.RequestDelta;
import it.cavallium.rockserver.core.common.RequestType.RequestExists;
import it.cavallium.rockserver.core.common.RequestType.RequestForUpdate;
import it.cavallium.rockserver.core.common.RequestType.RequestMulti;
import it.cavallium.rockserver.core.common.RequestType.RequestNothing;
import it.cavallium.rockserver.core.common.RequestType.RequestPrevious;
import it.cavallium.rockserver.core.common.RequestType.RequestPreviousPresence;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.api.proto.Delta;
import it.cavallium.rockserver.core.common.api.proto.FirstAndLast;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.net.SocketAddress;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletionException;

import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

public class GrpcServer extends Server {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());

	private final GrpcServerImpl grpc;
	private final EventLoopGroup elg;
	private final Scheduler executor;
	private final io.grpc.Server server;

	public GrpcServer(RocksDBConnection client, SocketAddress socketAddress) throws IOException {
		super(client);
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
		this.executor = Schedulers.newBoundedElastic(Runtime.getRuntime().availableProcessors() * 2, Schedulers.DEFAULT_BOUNDED_ELASTIC_QUEUESIZE, "server-db-executor");
		this.server = NettyServerBuilder
				.forAddress(socketAddress)
				.bossEventLoopGroup(elg)
				.workerEventLoopGroup(elg)
				.directExecutor()
				.channelType(channelType)
				.withChildOption(ChannelOption.SO_KEEPALIVE, false)
				.maxInboundMessageSize(512 * 1024 * 1024)
				.addService(grpc)
				.build();
		LOG.info("GRPC RocksDB server is listening at " + socketAddress);
	}

	@Override
	public void start() throws IOException {
		server.start();
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
			});
		}

		@Override
		public Mono<CloseTransactionResponse> closeTransaction(CloseTransactionRequest request) {
			return executeSync(() -> {
				var committed = api.closeTransaction(request.getTransactionId(), request.getCommit());
                return CloseTransactionResponse.newBuilder().setSuccessful(committed).build();
			});
		}

		@Override
		public Mono<Empty> closeFailedUpdate(CloseFailedUpdateRequest request) {
			return executeSync(() -> {
				api.closeFailedUpdate(request.getUpdateId());
				return Empty.getDefaultInstance();
			});
		}

		@Override
		public Mono<CreateColumnResponse> createColumn(CreateColumnRequest request) {
			return executeSync(() -> {
				var colId = api.createColumn(request.getName(), mapColumnSchema(request.getSchema()));
				return CreateColumnResponse.newBuilder().setColumnId(colId).build();
			});
		}

		@Override
		public Mono<Empty> deleteColumn(DeleteColumnRequest request) {
			return executeSync(() -> {
				api.deleteColumn(request.getColumnId());
				return Empty.getDefaultInstance();
			});
		}

		@Override
		public Mono<GetColumnIdResponse> getColumnId(GetColumnIdRequest request) {
			return executeSync(() -> {
				var colId = api.getColumnId(request.getName());
				return GetColumnIdResponse.newBuilder().setColumnId(colId).build();
			});
		}

		@Override
		public Mono<Empty> put(PutRequest request) {
			return executeSync(() -> {
				try (var arena = Arena.ofConfined()) {
					api.put(arena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(arena, request.getData().getKeysCount(), request.getData()::getKeys),
							toMemorySegment(arena, request.getData().getValue()),
							new RequestNothing<>()
					);
				}
				return Empty.getDefaultInstance();
			});
		}

		@Override
		public Mono<Empty> putBatch(Flux<PutBatchRequest> request) {
			return request.switchOnFirst((firstSignal, nextRequests) -> {
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

					var batches = nextRequests.map(putBatchRequest -> {
						var batch = putBatchRequest.getData();
						return mapKVBatch(Arena.ofAuto(), batch.getEntriesCount(), batch::getEntries);
					});

					return Mono.fromFuture(() -> asyncApi.putBatchAsync(initialRequest.getColumnId(), batches, mode));
				} else if (firstSignal.isOnComplete()) {
					return Mono.just(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return nextRequests;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Mono<Empty> putMulti(Flux<PutMultiRequest> request) {
			return request.switchOnFirst((firstSignal, nextRequests) -> {
				if (firstSignal.isOnNext()) {
					var firstValue = firstSignal.get();
					assert firstValue != null;
					if (!firstValue.hasInitialRequest()) {
						return Mono.<Empty>error(RocksDBException.of(
								RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "Missing initial request"));
					}
					var initialRequest = firstValue.getInitialRequest();

                    return nextRequests
                            .publishOn(executor)
                            .doOnNext(putRequest -> {
                                var data = putRequest.getData();
                                try (var arena = Arena.ofConfined()) {
                                    api.put(arena,
                                            initialRequest.getTransactionOrUpdateId(),
                                            initialRequest.getColumnId(),
                                            mapKeys(arena, data.getKeysCount(), data::getKeys),
                                            toMemorySegment(arena, data.getValue()),
                                            new RequestNothing<>());
                                }
                            });
				} else if (firstSignal.isOnComplete()) {
					return Mono.just(RocksDBException.of(
							RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
				} else {
					return nextRequests;
				}
			}).then(Mono.just(Empty.getDefaultInstance()));
		}

		@Override
		public Mono<Previous> putGetPrevious(PutRequest request) {
			return executeSync(() -> {
				try (var arena = Arena.ofConfined()) {
					var prev = api.put(arena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(arena, request.getData().getKeysCount(), request.getData()::getKeys),
							toMemorySegment(arena, request.getData().getValue()),
							new RequestPrevious<>()
					);
					var prevBuilder = Previous.newBuilder();
					if (prev != null) {
						prevBuilder.setPrevious(ByteString.copyFrom(prev.asByteBuffer()));
					}
                    return prevBuilder.build();
				}
			});
		}

		@Override
		public Mono<Delta> putGetDelta(PutRequest request) {
			return executeSync(() -> {
				try (var arena = Arena.ofConfined()) {
					var delta = api.put(arena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(arena, request.getData().getKeysCount(), request.getData()::getKeys),
							toMemorySegment(arena, request.getData().getValue()),
							new RequestDelta<>()
					);
					var deltaBuilder = Delta.newBuilder();
					if (delta.previous() != null) {
						deltaBuilder.setPrevious(ByteString.copyFrom(delta.previous().asByteBuffer()));
					}
					if (delta.current() != null) {
						deltaBuilder.setCurrent(ByteString.copyFrom(delta.current().asByteBuffer()));
					}
					return deltaBuilder.build();
				}
			});
		}

		@Override
		public Mono<Changed> putGetChanged(PutRequest request) {
			return executeSync(() -> {
                    try (var arena = Arena.ofConfined()) {
                        var changed = api.put(arena,
                                request.getTransactionOrUpdateId(),
                                request.getColumnId(),
                                mapKeys(arena, request.getData().getKeysCount(), request.getData()::getKeys),
                                toMemorySegment(arena, request.getData().getValue()),
                                new RequestChanged<>()
                        );
                        return Changed.newBuilder().setChanged(changed).build();
                    }
			});
		}

		@Override
		public Mono<PreviousPresence> putGetPreviousPresence(PutRequest request) {
			return executeSync(() -> {
				try (var arena = Arena.ofConfined()) {
					var present = api.put(arena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(arena, request.getData().getKeysCount(), request.getData()::getKeys),
							toMemorySegment(arena, request.getData().getValue()),
							new RequestPreviousPresence<>()
					);
					return PreviousPresence.newBuilder().setPresent(present).build();
				}
			});
		}

		@Override
		public Mono<GetResponse> get(GetRequest request) {
			return executeSync(() -> {
				try (var arena = Arena.ofConfined()) {
					var current = api.get(arena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(arena, request.getKeysCount(), request::getKeys),
							new RequestCurrent<>()
					);
					var responseBuilder = GetResponse.newBuilder();
					if (current != null) {
						responseBuilder.setValue(ByteString.copyFrom(current.asByteBuffer()));
					}
					return responseBuilder.build();
				}
			});
		}

		@Override
		public Mono<UpdateBegin> getForUpdate(GetRequest request) {
			return executeSync(() -> {
				try (var arena = Arena.ofConfined()) {
					var forUpdate = api.get(arena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(arena, request.getKeysCount(), request::getKeys),
							new RequestForUpdate<>()
					);
					var responseBuilder = UpdateBegin.newBuilder();
					responseBuilder.setUpdateId(forUpdate.updateId());
					if (forUpdate.previous() != null) {
						responseBuilder.setPrevious(ByteString.copyFrom(forUpdate.previous().asByteBuffer()));
					}
					return responseBuilder.build();
				}
			});
		}

		@Override
		public Mono<PreviousPresence> exists(GetRequest request) {
			return executeSync(() -> {
				try (var arena = Arena.ofConfined()) {
					var exists = api.get(arena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(arena, request.getKeysCount(), request::getKeys),
							new RequestExists<>()
					);
					return PreviousPresence.newBuilder().setPresent(exists).build();
				}
			});
		}

		@Override
		public Mono<OpenIteratorResponse> openIterator(OpenIteratorRequest request) {
			return executeSync(() -> {
				try (var arena = Arena.ofConfined()) {
					var iteratorId = api.openIterator(arena,
							request.getTransactionId(),
							request.getColumnId(),
							mapKeys(arena, request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
							mapKeys(arena, request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
							request.getReverse(),
							request.getTimeoutMs()
					);
					return OpenIteratorResponse.newBuilder().setIteratorId(iteratorId).build();
				}
			});
		}

		@Override
		public Mono<Empty> closeIterator(CloseIteratorRequest request) {
			return executeSync(() -> {
				api.closeIterator(request.getIteratorId());
				return Empty.getDefaultInstance();
			});
		}

		@Override
		public Mono<Empty> seekTo(SeekToRequest request) {
			return executeSync(() -> {
				try (var arena = Arena.ofConfined()) {
					api.seekTo(arena, request.getIterationId(), mapKeys(arena, request.getKeysCount(), request::getKeys));
				}
				return Empty.getDefaultInstance();
			});
		}

		@Override
		public Mono<Empty> subsequent(SubsequentRequest request) {
			return executeSync(() -> {
				try (var arena = Arena.ofConfined()) {
					api.subsequent(arena, request.getIterationId(),
							request.getSkipCount(),
							request.getTakeCount(),
							new RequestNothing<>());
				}
				return Empty.getDefaultInstance();
			});
		}

		@Override
		public Mono<PreviousPresence> subsequentExists(SubsequentRequest request) {
			return executeSync(() -> {
				try (var arena = Arena.ofConfined()) {
					var exists = api.subsequent(arena, request.getIterationId(),
							request.getSkipCount(),
							request.getTakeCount(),
							new RequestExists<>());
					return PreviousPresence.newBuilder().setPresent(exists).build();
				}
			});
		}

		@Override
		public Flux<KV> subsequentMultiGet(SubsequentRequest request) {
			return Flux.create(emitter -> {
				try (var arena = Arena.ofConfined()) {
					int pageIndex = 0;
					final long pageSize = 16L;
					while (request.getTakeCount() > pageIndex * pageSize) {
						var response = api.subsequent(arena,
								request.getIterationId(),
								pageIndex == 0 ? request.getSkipCount() : 0,
								Math.min(request.getTakeCount() - pageIndex * pageSize, pageSize),
								new RequestMulti<>()
						);
						for (MemorySegment entry : response) {
							Keys keys = null; // todo: implement
							MemorySegment value = entry;
							emitter.next(KV.newBuilder()
									.addAllKeys(null) // todo: implement
									.setValue(ByteString.copyFrom(value.asByteBuffer()))
									.build());
						}
						pageIndex++;
					}
				}
				emitter.complete();
			}, FluxSink.OverflowStrategy.BUFFER);
		}

		@Override
		public Mono<FirstAndLast> reduceRangeFirstAndLast(GetRangeRequest request) {
			return executeSync(() -> {
				try (var arena = Arena.ofConfined()) {
					it.cavallium.rockserver.core.common.FirstAndLast<it.cavallium.rockserver.core.common.KV> firstAndLast
							= api.reduceRange(arena,
							request.getTransactionId(),
							request.getColumnId(),
							mapKeys(arena, request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
							mapKeys(arena, request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
							request.getReverse(),
							RequestType.firstAndLast(),
							request.getTimeoutMs()
					);
					return FirstAndLast.newBuilder()
							.setFirst(unmapKV(firstAndLast.first()))
							.setLast(unmapKV(firstAndLast.last()))
							.build();
				}
			});
		}

		@Override
		public Flux<KV> getAllInRange(GetRangeRequest request) {
			var arena = Arena.ofAuto();
			return Flux
					.from(asyncApi.getRangeAsync(arena,
							request.getTransactionId(),
							request.getColumnId(),
							mapKeys(arena, request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
							mapKeys(arena, request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
							request.getReverse(),
							RequestType.allInRange(),
							request.getTimeoutMs()
					))
					.map(GrpcServerImpl::unmapKV);
		}

		private static void closeArenaSafe(Arena autoArena) {
			if (autoArena != null) {
				try {
					autoArena.close();
				} catch (Exception ex2) {
					LOG.error("Failed to close arena", ex2);
				}
			}
		}

		// utils

		private <T> Mono<T> executeSync(Callable<T> callable) {
			return Mono.fromCallable(callable).subscribeOn(executor);
		}

		// mappers

		private static KV unmapKV(it.cavallium.rockserver.core.common.KV kv) {
			if (kv == null) return null;
			return KV.newBuilder()
					.addAllKeys(unmapKeys(kv.keys()))
					.setValue(unmapValue(kv.value()))
					.build();
		}

		private static List<ByteString> unmapKeys(@NotNull Keys keys) {
			var result = new ArrayList<ByteString>(keys.keys().length);
			for (@NotNull MemorySegment key : keys.keys()) {
				result.add(UnsafeByteOperations.unsafeWrap(key.asByteBuffer()));
			}
			return result;
		}

		private static ByteString unmapValue(@Nullable MemorySegment value) {
			if (value == null) return null;
			return UnsafeByteOperations.unsafeWrap(value.asByteBuffer());
		}

		private static ColumnSchema mapColumnSchema(it.cavallium.rockserver.core.common.api.proto.ColumnSchema schema) {
			return ColumnSchema.of(mapKeysLength(schema.getFixedKeysCount(), schema::getFixedKeys),
					mapVariableTailKeys(schema.getVariableTailKeysCount(), schema::getVariableTailKeys),
					schema.getHasValue()
			);
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
					case UNRECOGNIZED -> throw new UnsupportedOperationException();
				});
			}
			return l;
		}

		private static Keys mapKeys(Arena arena, int count, Int2ObjectFunction<ByteString> keyGetterAt) {
			var segments = new MemorySegment[count];
			for (int i = 0; i < count; i++) {
				segments[i] = toMemorySegment(arena, keyGetterAt.apply(i));
			}
			return new Keys(segments);
		}

		private static List<Keys> mapKeysKV(Arena arena, int count, Int2ObjectFunction<KV> keyGetterAt) {
			var keys = new ArrayList<Keys>(count);
			for (int i = 0; i < count; i++) {
				var k = keyGetterAt.apply(i);
				keys.add(mapKeys(arena, k.getKeysCount(), k::getKeys));
			}
			return keys;
		}

		private static List<MemorySegment> mapValuesKV(Arena arena, int count, Int2ObjectFunction<KV> keyGetterAt) {
			var keys = new ArrayList<MemorySegment>(count);
			for (int i = 0; i < count; i++) {
				keys.add(toMemorySegment(arena, keyGetterAt.get(i).getValue()));
			}
			return keys;
		}

		private static KVBatch mapKVBatch(Arena arena, int count, Int2ObjectFunction<KV> getterAt) {
			return new KVBatch(
					mapKeysKV(arena, count, getterAt),
					mapValuesKV(arena, count, getterAt)
			);
		}

		private static void handleError(StreamObserver<?> responseObserver, Throwable ex) {
			if (ex instanceof StatusRuntimeException e && e.getStatus().getCode().equals(Status.CANCELLED.getCode())) {
				LOG.warn("Connection cancelled: {}", e.getStatus().getDescription());
				return;
			}

			if (ex instanceof CompletionException exx) {
				handleError(responseObserver, exx.getCause());
			} else {
				var serverResponseObserver = ((ServerCallStreamObserver<?>) responseObserver);
				if (!serverResponseObserver.isCancelled()) {
					if (ex instanceof RocksDBException e) {
						responseObserver.onError(Status.INTERNAL
								.withDescription(e.getLocalizedMessage())
								.withCause(e)
								.asException());
					} else {
						responseObserver.onError(Status.INTERNAL
								.withCause(ex)
								.asException());
					}
				} else {
					LOG.error("Unexpected error", ex);
				}
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
			throw new RuntimeException(e);
		}
		elg.close();
		executor.disposeGracefully().timeout(Duration.ofMinutes(2)).onErrorResume(ex -> {
			LOG.error("Grpc server executor shutdown timed out, terminating...", ex);
			executor.dispose();
			return Mono.empty();
		}).block();
		super.close();
	}
}
