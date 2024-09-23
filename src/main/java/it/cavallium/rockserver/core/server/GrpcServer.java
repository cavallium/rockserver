package it.cavallium.rockserver.core.server;

import static it.cavallium.rockserver.core.common.Utils.toMemorySegment;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.netty.NettyServerBuilder;
import io.grpc.stub.StreamObserver;
import io.netty.channel.ChannelOption;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.ServerChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.epoll.EpollServerDomainSocketChannel;
import io.netty.channel.nio.NioEventLoopGroup;
import io.netty.channel.socket.nio.NioServerSocketChannel;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
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
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc.RocksDBServiceImplBase;
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
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicLong;

import org.rocksdb.util.SizeUnit;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class GrpcServer extends Server {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());

	private final GrpcServerImpl grpc;
	private final EventLoopGroup elg;
	private final ExecutorService executor;
	private final io.grpc.Server server;

	public GrpcServer(RocksDBConnection client, String http2Host, int http2Port) throws IOException {
		super(client);
		this.grpc = new GrpcServerImpl(this.getClient());
		EventLoopGroup elg;
		Class<? extends ServerChannel> channelType;
		try {
            elg = new EpollEventLoopGroup(1);
			channelType = EpollServerDomainSocketChannel.class;
        } catch (UnsatisfiedLinkError ex) {
			elg = new NioEventLoopGroup();
			channelType = NioServerSocketChannel.class;
		}
		this.elg = elg;
		this.executor = Executors.newWorkStealingPool(Runtime.getRuntime().availableProcessors() * 2);
		this.server = NettyServerBuilder.forAddress(new InetSocketAddress(http2Host, http2Port))
				.bossEventLoopGroup(elg)
				.workerEventLoopGroup(elg)
				.directExecutor()
				.channelType(channelType)
				.withChildOption(ChannelOption.SO_KEEPALIVE, false)
				.maxInboundMessageSize(Math.toIntExact(128 * SizeUnit.MB))
				.addService(grpc)
				.build();
		server.start();
		LOG.info("GRPC RocksDB server is listening at " + http2Host + ":" + http2Port);
	}

	private final class GrpcServerImpl extends RocksDBServiceImplBase {

        private final RocksDBSyncAPI api;

		public GrpcServerImpl(RocksDBConnection client) {
            this.api = client.getSyncApi();
		}

		// functions

		@Override
		public void openTransaction(OpenTransactionRequest request,
				StreamObserver<OpenTransactionResponse> responseObserver) {
			executor.execute(() -> {
				try {
					var txId = api.openTransaction(request.getTimeoutMs());
					responseObserver.onNext(OpenTransactionResponse.newBuilder().setTransactionId(txId).build());
					responseObserver.onCompleted();
				} catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void closeTransaction(CloseTransactionRequest request,
				StreamObserver<CloseTransactionResponse> responseObserver) {
			executor.execute(() -> {
				try {
					var committed = api.closeTransaction(request.getTransactionId(), request.getCommit());
					var response = CloseTransactionResponse.newBuilder().setSuccessful(committed).build();
					responseObserver.onNext(response);
					responseObserver.onCompleted();
				} catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void closeFailedUpdate(CloseFailedUpdateRequest request, StreamObserver<Empty> responseObserver) {
			executor.execute(() -> {
				try {
					api.closeFailedUpdate(request.getUpdateId());
					responseObserver.onNext(Empty.getDefaultInstance());
					responseObserver.onCompleted();
				} catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void createColumn(CreateColumnRequest request, StreamObserver<CreateColumnResponse> responseObserver) {
			executor.execute(() -> {
				var colId = api.createColumn(request.getName(), mapColumnSchema(request.getSchema()));
				var response = CreateColumnResponse.newBuilder().setColumnId(colId).build();
				responseObserver.onNext(response);
				responseObserver.onCompleted();
			});
		}

		@Override
		public void deleteColumn(DeleteColumnRequest request, StreamObserver<Empty> responseObserver) {
			executor.execute(() -> {
				api.deleteColumn(request.getColumnId());
				responseObserver.onNext(Empty.getDefaultInstance());
				responseObserver.onCompleted();
			});
		}

		@Override
		public void getColumnId(GetColumnIdRequest request, StreamObserver<GetColumnIdResponse> responseObserver) {
			executor.execute(() -> {
				try {
					var colId = api.getColumnId(request.getName());
					var response = GetColumnIdResponse.newBuilder().setColumnId(colId).build();
					responseObserver.onNext(response);
					responseObserver.onCompleted();
				} catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void put(PutRequest request, StreamObserver<Empty> responseObserver) {
			executor.execute(() -> {
				try {
                    try (var arena = Arena.ofConfined()) {
                        api.put(arena,
                                request.getTransactionOrUpdateId(),
                                request.getColumnId(),
                                mapKeys(arena, request.getData().getKeysCount(), request.getData()::getKeys),
                                toMemorySegment(arena, request.getData().getValue()),
                                new RequestNothing<>()
                        );
                    }
					responseObserver.onNext(Empty.getDefaultInstance());
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void putBatch(PutBatchRequest request, StreamObserver<Empty> responseObserver) {
			executor.execute(() -> {
				try {
					try (var arena = Arena.ofConfined()) {
						api.putBatch(arena,
								request.getColumnId(),
								mapKeysKV(arena, request.getDataCount(), request::getData),
								mapValuesKV(arena, request.getDataCount(), request::getData),
								switch (request.getMode()) {
                                    case WRITE_BATCH -> PutBatchMode.WRITE_BATCH;
                                    case WRITE_BATCH_NO_WAL -> PutBatchMode.WRITE_BATCH_NO_WAL;
                                    case SST_INGESTION -> PutBatchMode.SST_INGESTION;
                                    case SST_INGEST_BEHIND -> PutBatchMode.SST_INGEST_BEHIND;
                                    case UNRECOGNIZED -> throw new UnsupportedOperationException("Unrecognized request \"mode\"");
                                }
						);
					}
					responseObserver.onNext(Empty.getDefaultInstance());
					responseObserver.onCompleted();
				} catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public StreamObserver<PutMultiRequest> putMulti(StreamObserver<Empty> responseObserver) {
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
							executor.execute(() -> {
								try {
                                    try (var arena = Arena.ofConfined()) {
                                        api.put(arena,
                                                initialRequest.getTransactionOrUpdateId(),
                                                initialRequest.getColumnId(),
                                                mapKeys(arena, request.getData().getKeysCount(), request.getData()::getKeys),
                                                toMemorySegment(arena, request.getData().getValue()),
                                                new RequestNothing<>());
                                    }
                                } catch (RocksDBException ex) {
									responseObserver.onError(ex);
									return;
								}

								var newProcessedRequestCount = processedRequestsCount.incrementAndGet();
								if (requestsCountFinalized) {
									if (newProcessedRequestCount == requestsCount) {
										responseObserver.onNext(Empty.getDefaultInstance());
										responseObserver.onCompleted();
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
					responseObserver.onError(t);
				}

				@Override
				public void onCompleted() {
					requestsCountFinalized = true;
					if (requestsCount == 0) {
						responseObserver.onCompleted();
					}
				}
			};
		}

		@Override
		public void putGetPrevious(PutRequest request, StreamObserver<Previous> responseObserver) {
			executor.execute(() -> {
				try {
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
                        var response = prevBuilder.build();
						responseObserver.onNext(response);
                    }
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void putGetDelta(PutRequest request, StreamObserver<Delta> responseObserver) {
			executor.execute(() -> {
				try {
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
                        var response = deltaBuilder.build();
                        responseObserver.onNext(response);
                    }
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void putGetChanged(PutRequest request, StreamObserver<Changed> responseObserver) {
			executor.execute(() -> {
				try {
                    try (var arena = Arena.ofConfined()) {
                        var changed = api.put(arena,
                                request.getTransactionOrUpdateId(),
                                request.getColumnId(),
                                mapKeys(arena, request.getData().getKeysCount(), request.getData()::getKeys),
                                toMemorySegment(arena, request.getData().getValue()),
                                new RequestChanged<>()
                        );
                        var response = Changed.newBuilder().setChanged(changed).build();
                        responseObserver.onNext(response);
                    }
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void putGetPreviousPresence(PutRequest request, StreamObserver<PreviousPresence> responseObserver) {
			executor.execute(() -> {
				try {
                    try (var arena = Arena.ofConfined()) {
                        var present = api.put(arena,
                                request.getTransactionOrUpdateId(),
                                request.getColumnId(),
                                mapKeys(arena, request.getData().getKeysCount(), request.getData()::getKeys),
                                toMemorySegment(arena, request.getData().getValue()),
                                new RequestPreviousPresence<>()
                        );
                        var response = PreviousPresence.newBuilder().setPresent(present).build();
                        responseObserver.onNext(response);
                    }
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void get(GetRequest request, StreamObserver<GetResponse> responseObserver) {
			executor.execute(() -> {
				try {
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
                        var response = responseBuilder.build();
                        responseObserver.onNext(response);
                    }
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void getForUpdate(GetRequest request, StreamObserver<UpdateBegin> responseObserver) {
			executor.execute(() -> {
				try {
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
                        var response = responseBuilder.build();
                        responseObserver.onNext(response);
                    }
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void exists(GetRequest request, StreamObserver<PreviousPresence> responseObserver) {
			executor.execute(() -> {
				try {
                    try (var arena = Arena.ofConfined()) {
                        var exists = api.get(arena,
                                request.getTransactionOrUpdateId(),
                                request.getColumnId(),
                                mapKeys(arena, request.getKeysCount(), request::getKeys),
                                new RequestExists<>()
                        );
                        responseObserver.onNext(PreviousPresence.newBuilder().setPresent(exists).build());
                    }
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void openIterator(OpenIteratorRequest request, StreamObserver<OpenIteratorResponse> responseObserver) {
			executor.execute(() -> {
				try {
                    try (var arena = Arena.ofConfined()) {
                        var iteratorId = api.openIterator(arena,
                                request.getTransactionId(),
                                request.getColumnId(),
                                mapKeys(arena, request.getStartKeysInclusiveCount(), request::getStartKeysInclusive),
                                mapKeys(arena, request.getEndKeysExclusiveCount(), request::getEndKeysExclusive),
                                request.getReverse(),
                                request.getTimeoutMs()
                        );
                        responseObserver.onNext(OpenIteratorResponse.newBuilder().setIteratorId(iteratorId).build());
                    }
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void closeIterator(CloseIteratorRequest request, StreamObserver<Empty> responseObserver) {
			executor.execute(() -> {
				try {
					api.closeIterator(request.getIteratorId());
					responseObserver.onNext(Empty.getDefaultInstance());
					responseObserver.onCompleted();
				} catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void seekTo(SeekToRequest request, StreamObserver<Empty> responseObserver) {
			executor.execute(() -> {
				try {
                    try (var arena = Arena.ofConfined()) {
                        api.seekTo(arena, request.getIterationId(), mapKeys(arena, request.getKeysCount(), request::getKeys));
                    }
					responseObserver.onNext(Empty.getDefaultInstance());
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void subsequent(SubsequentRequest request, StreamObserver<Empty> responseObserver) {
			executor.execute(() -> {
				try {
                    try (var arena = Arena.ofConfined()) {
                        api.subsequent(arena, request.getIterationId(),
                                request.getSkipCount(),
                                request.getTakeCount(),
                                new RequestNothing<>());
                    }
					responseObserver.onNext(Empty.getDefaultInstance());
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void subsequentExists(SubsequentRequest request, StreamObserver<PreviousPresence> responseObserver) {
			executor.execute(() -> {
				try {
                    try (var arena = Arena.ofConfined()) {
                        var exists = api.subsequent(arena, request.getIterationId(),
                                request.getSkipCount(),
                                request.getTakeCount(),
                                new RequestExists<>());
                        var response = PreviousPresence.newBuilder().setPresent(exists).build();
                        responseObserver.onNext(response);
                    }
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
		}

		@Override
		public void subsequentMultiGet(SubsequentRequest request, StreamObserver<KV> responseObserver) {
			executor.execute(() -> {
				try {
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
                                responseObserver.onNext(KV.newBuilder()
                                        .addAllKeys(null) // todo: implement
                                        .setValue(ByteString.copyFrom(value.asByteBuffer()))
                                        .build());
                            }
                            pageIndex++;
                        }
                    }
					responseObserver.onCompleted();
                } catch (Throwable ex) {
					responseObserver.onError(ex);
				}
			});
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
		executor.close();
		super.close();
	}
}
