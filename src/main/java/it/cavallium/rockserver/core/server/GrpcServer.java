package it.cavallium.rockserver.core.server;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegment;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.Status;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestChanged;
import it.cavallium.rockserver.core.common.RequestType.RequestCurrent;
import it.cavallium.rockserver.core.common.RequestType.RequestDelta;
import it.cavallium.rockserver.core.common.RequestType.RequestExists;
import it.cavallium.rockserver.core.common.RequestType.RequestForUpdate;
import it.cavallium.rockserver.core.common.RequestType.RequestMulti;
import it.cavallium.rockserver.core.common.RequestType.RequestNothing;
import it.cavallium.rockserver.core.common.RequestType.RequestPrevious;
import it.cavallium.rockserver.core.common.RequestType.RequestPreviousPresence;
import it.cavallium.rockserver.core.common.api.proto.Changed;
import it.cavallium.rockserver.core.common.api.proto.CloseFailedUpdateRequest;
import it.cavallium.rockserver.core.common.api.proto.CloseIteratorRequest;
import it.cavallium.rockserver.core.common.api.proto.CloseTransactionRequest;
import it.cavallium.rockserver.core.common.api.proto.CloseTransactionResponse;
import it.cavallium.rockserver.core.common.api.proto.CreateColumnRequest;
import it.cavallium.rockserver.core.common.api.proto.CreateColumnResponse;
import it.cavallium.rockserver.core.common.api.proto.DeleteColumnRequest;
import it.cavallium.rockserver.core.common.api.proto.Delta;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdRequest;
import it.cavallium.rockserver.core.common.api.proto.GetColumnIdResponse;
import it.cavallium.rockserver.core.common.api.proto.GetRequest;
import it.cavallium.rockserver.core.common.api.proto.GetResponse;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.OpenIteratorRequest;
import it.cavallium.rockserver.core.common.api.proto.OpenIteratorResponse;
import it.cavallium.rockserver.core.common.api.proto.OpenTransactionRequest;
import it.cavallium.rockserver.core.common.api.proto.OpenTransactionResponse;
import it.cavallium.rockserver.core.common.api.proto.Previous;
import it.cavallium.rockserver.core.common.api.proto.PreviousPresence;
import it.cavallium.rockserver.core.common.api.proto.PutMultiInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.PutRequest;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.RocksDBServiceImplBase;
import it.cavallium.rockserver.core.common.api.proto.SeekToRequest;
import it.cavallium.rockserver.core.common.api.proto.SubsequentRequest;
import it.cavallium.rockserver.core.common.api.proto.UpdateBegin;
import it.unimi.dsi.fastutil.ints.Int2IntFunction;
import it.unimi.dsi.fastutil.ints.Int2ObjectFunction;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.net.InetSocketAddress;
import java.util.concurrent.CompletionException;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.BiConsumer;
import java.util.function.Function;

import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcServer extends Server {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());

	private final GrpcServerImpl grpc;
	private final io.grpc.Server server;

	public GrpcServer(RocksDBConnection client, String http2Host, int http2Port) throws IOException {
		super(client);
		this.grpc = new GrpcServerImpl(this.getClient());
		this.server = NettyServerBuilder.forAddress(new InetSocketAddress(http2Host, http2Port))
				.addService(grpc)
				.build();
		server.start();
		LOG.info("GRPC RocksDB server is listening at " + http2Host + ":" + http2Port);
	}

	private static final class GrpcServerImpl extends RocksDBServiceImplBase {

		private static final Function<? super Void, Empty> MAP_EMPTY = _ -> Empty.getDefaultInstance();
		private final RocksDBConnection client;

		public GrpcServerImpl(RocksDBConnection client) {
			this.client = client;
		}

		// functions

		@Override
		public void openTransaction(OpenTransactionRequest request,
				StreamObserver<OpenTransactionResponse> responseObserver) {
			client.getAsyncApi()
					.openTransactionAsync(request.getTimeoutMs())
					.whenComplete(handleResponseObserver(
							txId -> OpenTransactionResponse.newBuilder().setTransactionId(txId).build(),
							responseObserver, null));
		}

		@Override
		public void closeTransaction(CloseTransactionRequest request,
				StreamObserver<CloseTransactionResponse> responseObserver) {
			client.getAsyncApi()
					.closeTransactionAsync(request.getTransactionId(), request.getCommit())
					.whenComplete(handleResponseObserver(
							committed -> CloseTransactionResponse.newBuilder().setSuccessful(committed).build(),
							responseObserver, null));
		}

		@Override
		public void closeFailedUpdate(CloseFailedUpdateRequest request, StreamObserver<Empty> responseObserver) {
			client.getAsyncApi()
					.closeFailedUpdateAsync(request.getUpdateId())
					.whenComplete(handleResponseObserver(MAP_EMPTY, responseObserver, null));
		}

		@Override
		public void createColumn(CreateColumnRequest request, StreamObserver<CreateColumnResponse> responseObserver) {
			client.getAsyncApi()
					.createColumnAsync(request.getName(), mapColumnSchema(request.getSchema()))
					.whenComplete(handleResponseObserver(
							colId -> CreateColumnResponse.newBuilder().setColumnId(colId).build(),
							responseObserver, null));
		}

		@Override
		public void deleteColumn(DeleteColumnRequest request, StreamObserver<Empty> responseObserver) {
			client.getAsyncApi()
					.deleteColumnAsync(request.getColumnId())
					.whenComplete(handleResponseObserver(MAP_EMPTY, responseObserver, null));
		}

		@Override
		public void getColumnId(GetColumnIdRequest request, StreamObserver<GetColumnIdResponse> responseObserver) {
			client.getAsyncApi()
					.getColumnIdAsync(request.getName())
					.whenComplete(handleResponseObserver(
							colId -> GetColumnIdResponse.newBuilder().setColumnId(colId).build(),
							responseObserver, null));
		}

		@Override
		public void put(PutRequest request, StreamObserver<Empty> responseObserver) {
			var autoArena = Arena.ofShared();
			client.getAsyncApi()
					.putAsync(autoArena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(autoArena, request.getData().getKeysCount(), request.getData()::getKeys),
							toMemorySegment(autoArena, request.getData().getValue()),
							new RequestNothing<>()
					)
					.whenComplete(handleResponseObserver(MAP_EMPTY, responseObserver, autoArena));
		}

		@Override
		public StreamObserver<PutMultiRequest> putMulti(StreamObserver<Empty> responseObserver) {
			var autoArena = Arena.ofShared();
			return new StreamObserver<>() {
				private boolean initialRequestDone = false;
				private long requestsCount = 0;
				private boolean requestsCountFinalized;
				private final AtomicLong processedRequestsCount = new AtomicLong();
				private PutMultiInitialRequest initialRequest;

				@Override
				public void onNext(PutMultiRequest request) {
					switch (request.getPutMultiRequestTypeCase()) {
						case INITIALREQUEST -> {
							if (initialRequestDone) {
								throw new UnsupportedOperationException("Initial request already done!");
							}
							this.initialRequest = request.getInitialRequest();
							this.initialRequestDone = true;
						}
						case DATA -> {
							if (!initialRequestDone) {
								throw new UnsupportedOperationException("Initial request already done!");
							}
							++requestsCount;
							client.getAsyncApi()
									.putAsync(autoArena,
											initialRequest.getTransactionOrUpdateId(),
											initialRequest.getColumnId(),
											mapKeys(autoArena, request.getData().getKeysCount(), request.getData()::getKeys),
											toMemorySegment(autoArena, request.getData().getValue()),
											new RequestNothing<>()
									)
									.whenComplete((_, error) -> {
										if (error != null) {
											closeArenaSafe(autoArena);
											responseObserver.onError(error);
										} else {
											var newProcessedRequestCount = processedRequestsCount.incrementAndGet();
											if (requestsCountFinalized) {
												if (newProcessedRequestCount == requestsCount) {
													closeArenaSafe(autoArena);
													responseObserver.onCompleted();
												}
											}
										}
									});
						}
						case null, default ->
								throw new UnsupportedOperationException("Unsupported operation: "
										+ request.getPutMultiRequestTypeCase());
					}
				}

				@Override
				public void onError(Throwable t) {
					closeArenaSafe(autoArena);
					responseObserver.onError(t);
				}

				@Override
				public void onCompleted() {
					requestsCountFinalized = true;
					if (requestsCount == 0) {
						closeArenaSafe(autoArena);
						responseObserver.onCompleted();
					}
				}
			};
		}

		@Override
		public void putGetPrevious(PutRequest request, StreamObserver<Previous> responseObserver) {
			var autoArena = Arena.ofShared();
			client.getAsyncApi()
					.putAsync(autoArena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(autoArena, request.getData().getKeysCount(), request.getData()::getKeys),
							toMemorySegment(autoArena, request.getData().getValue()),
							new RequestPrevious<>()
					)
					.whenComplete(handleResponseObserver(
							prev -> {
								var prevBuilder = Previous.newBuilder();
								if (prev != null) {
									prevBuilder.setPrevious(ByteString.copyFrom(prev.asByteBuffer()));
								}
								return prevBuilder.build();
							},
							responseObserver, autoArena));
		}

		@Override
		public void putGetDelta(PutRequest request, StreamObserver<Delta> responseObserver) {
			var autoArena = Arena.ofShared();
			client.getAsyncApi()
					.putAsync(autoArena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(autoArena, request.getData().getKeysCount(), request.getData()::getKeys),
							toMemorySegment(autoArena, request.getData().getValue()),
							new RequestDelta<>()
					)
					.whenComplete(handleResponseObserver(
							delta -> {
								var deltaBuilder = Delta.newBuilder();
								if (delta.previous() != null) {
									deltaBuilder.setPrevious(ByteString.copyFrom(delta.previous().asByteBuffer()));
								}
								if (delta.current() != null) {
									deltaBuilder.setCurrent(ByteString.copyFrom(delta.current().asByteBuffer()));
								}
								return deltaBuilder.build();
							},
							responseObserver, autoArena));
		}

		@Override
		public void putGetChanged(PutRequest request, StreamObserver<Changed> responseObserver) {
			var autoArena = Arena.ofShared();
			client.getAsyncApi()
					.putAsync(autoArena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(autoArena, request.getData().getKeysCount(), request.getData()::getKeys),
							toMemorySegment(autoArena, request.getData().getValue()),
							new RequestChanged<>()
					)
					.whenComplete(handleResponseObserver(
							changed -> Changed.newBuilder().setChanged(changed).build(),
							responseObserver, autoArena));
		}

		@Override
		public void putGetPreviousPresence(PutRequest request, StreamObserver<PreviousPresence> responseObserver) {
			var autoArena = Arena.ofShared();
			client.getAsyncApi()
					.putAsync(autoArena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(autoArena, request.getData().getKeysCount(), request.getData()::getKeys),
							toMemorySegment(autoArena, request.getData().getValue()),
							new RequestPreviousPresence<>()
					)
					.whenComplete(handleResponseObserver(
							present -> PreviousPresence.newBuilder().setPresent(present).build(),
							responseObserver, autoArena));
		}

		@Override
		public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
			var autoArena = Arena.ofShared();
			client.getAsyncApi()
					.getAsync(autoArena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(autoArena, request.getKeysCount(), request::getKeys),
							new RequestCurrent<>()
					)
					.whenComplete(handleResponseObserver(
							current -> {
								var response = GetResponse.newBuilder();
								if (current != null) {
									response.setValue(ByteString.copyFrom(current.asByteBuffer()));
								}
								return response.build();
							},
							responseObserver, autoArena));
		}

		@Override
		public void getForUpdate(GetRequest request, StreamObserver<UpdateBegin> responseObserver) {
			var autoArena = Arena.ofShared();
			client.getAsyncApi()
					.getAsync(autoArena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(autoArena, request.getKeysCount(), request::getKeys),
							new RequestForUpdate<>()
					)
					.whenComplete(handleResponseObserver(
							forUpdate -> {
								var response = UpdateBegin.newBuilder();
								response.setUpdateId(forUpdate.updateId());
								if (forUpdate.previous() != null) {
									response.setPrevious(ByteString.copyFrom(forUpdate.previous().asByteBuffer()));
								}
								return response.build();
							},
							responseObserver, autoArena));
		}

		@Override
		public void exists(GetRequest request, StreamObserver<PreviousPresence> responseObserver) {
			var autoArena = Arena.ofShared();
			client.getAsyncApi()
					.getAsync(autoArena,
							request.getTransactionOrUpdateId(),
							request.getColumnId(),
							mapKeys(autoArena, request.getKeysCount(), request::getKeys),
							new RequestExists<>()
					)
					.whenComplete(handleResponseObserver(
							exists -> PreviousPresence.newBuilder().setPresent(exists).build(),
							responseObserver, autoArena));
		}

		@Override
		public void openIterator(OpenIteratorRequest request, StreamObserver<OpenIteratorResponse> responseObserver) {
			var autoArena = Arena.ofShared();
			client.getAsyncApi()
					.openIteratorAsync(autoArena,
							request.getTransactionId(),
							request.getColumnId(),
							mapKeys(autoArena, request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
							mapKeys(autoArena, request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
							request.getReverse(),
							request.getTimeoutMs()
					)
					.whenComplete(handleResponseObserver(
							iteratorId -> OpenIteratorResponse.newBuilder().setIteratorId(iteratorId).build(),
							responseObserver, autoArena));
		}

		@Override
		public void closeIterator(CloseIteratorRequest request, StreamObserver<Empty> responseObserver) {
			client.getAsyncApi()
					.closeIteratorAsync(request.getIteratorId())
					.whenComplete(handleResponseObserver(MAP_EMPTY, responseObserver, null));
		}

		@Override
		public void seekTo(SeekToRequest request, StreamObserver<Empty> responseObserver) {
			var autoArena = Arena.ofShared();
			client.getAsyncApi()
					.seekToAsync(autoArena, request.getIterationId(), mapKeys(autoArena, request.getKeysCount(), request::getKeys))
					.whenComplete(handleResponseObserver(MAP_EMPTY, responseObserver, autoArena));
		}

		@Override
		public void subsequent(SubsequentRequest request, StreamObserver<Empty> responseObserver) {
			var autoArena = Arena.ofShared();
			client.getAsyncApi()
					.subsequentAsync(autoArena,
							request.getIterationId(),
							request.getSkipCount(),
							request.getTakeCount(),
							new RequestNothing<>()
					)
					.whenComplete(handleResponseObserver(MAP_EMPTY, responseObserver, autoArena));
		}

		@Override
		public void subsequentExists(SubsequentRequest request, StreamObserver<PreviousPresence> responseObserver) {
			var autoArena = Arena.ofShared();
			client.getAsyncApi()
					.subsequentAsync(autoArena,
							request.getIterationId(),
							request.getSkipCount(),
							request.getTakeCount(),
							new RequestExists<>()
					)
					.whenComplete(handleResponseObserver(
							exists -> PreviousPresence.newBuilder().setPresent(exists).build(),
							responseObserver, autoArena));
		}

		@Override
		public void subsequentMultiGet(SubsequentRequest request, StreamObserver<KV> responseObserver) {
			var autoArena = Arena.ofShared();
			subsequentMultiPage(request, responseObserver, 0, autoArena);
		}

		public void subsequentMultiPage(SubsequentRequest request, StreamObserver<KV> responseObserver, int pageIndex, Arena autoArena) {
			final long pageSize = 16L;
			if (request.getTakeCount() > pageIndex * pageSize) {
				client.getAsyncApi()
						.subsequentAsync(autoArena,
								request.getIterationId(),
								pageIndex == 0 ? request.getSkipCount() : 0,
								Math.min(request.getTakeCount() - pageIndex * pageSize, pageSize),
								new RequestMulti<>()
						)
						.whenComplete((response, ex) -> {
							if (ex != null) {
								closeArenaSafe(autoArena);
								responseObserver.onError(ex);
							} else {
								for (MemorySegment entry : response) {
									Keys keys = null; // todo: implement
									MemorySegment value = entry;
									responseObserver.onNext(KV.newBuilder()
											.addAllKeys(null) // todo: implement
											.setValue(ByteString.copyFrom(value.asByteBuffer()))
											.build());
								}
								subsequentMultiPage(request, responseObserver, pageIndex + 1, autoArena);
							}
						});
			} else {
				closeArenaSafe(autoArena);
				responseObserver.onCompleted();
			}
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

		// mappers

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

		// utils

		private static <T> BiConsumer<? super T, Throwable> handleResponseObserver(StreamObserver<T> responseObserver) {
			return (value, ex) -> {
				if (ex != null) {
					responseObserver.onError(ex);
				} else {
					if (value != null) {
						responseObserver.onNext(value);
					}
					responseObserver.onCompleted();
				}
			};
		}

		private static <PREV, T> BiConsumer<? super PREV, Throwable> handleResponseObserver(Function<PREV, T> resultMapper,
																							StreamObserver<T> responseObserver, @Nullable Arena autoArena) {
			return (value, ex) -> {
				if (ex != null) {
					closeArenaSafe(autoArena);
					var cause = ex;
					if (cause instanceof CompletionException completionException) {
						cause = completionException;
					}
					if (cause instanceof it.cavallium.rockserver.core.common.RocksDBException rocksDBException) {
						cause = rocksDBException;
					}
					var error = Status.INTERNAL.withCause(cause)
							.withDescription(cause.toString())
							.asException();
					responseObserver.onError(error);
				} else {
					T mapped;
					try {
						mapped = resultMapper.apply(value);
					} catch (Throwable ex2) {
						closeArenaSafe(autoArena);
						responseObserver.onError(ex2);
						return;
					}
					if (mapped != null) {
						responseObserver.onNext(mapped);
					}
					closeArenaSafe(autoArena);
					responseObserver.onCompleted();
				}
			};
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
		super.close();
	}
}
