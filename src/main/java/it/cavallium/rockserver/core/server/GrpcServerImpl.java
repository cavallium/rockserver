package it.cavallium.rockserver.core.server;

import static it.cavallium.rockserver.core.common.Utils.toByteArray;
import static it.cavallium.rockserver.core.common.Utils.toBuf;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import com.google.protobuf.Message;
import com.google.protobuf.UnsafeByteOperations;
import io.grpc.Status;
import io.grpc.Status.Code;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.MultiGetPage;
import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.RequestType.RequestDelete;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.api.proto.*;
import it.cavallium.rockserver.core.common.api.proto.Delta;
import it.cavallium.rockserver.core.common.api.proto.FirstAndLast;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.WorkloadAdmission;
import it.cavallium.buffer.Buf;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.Set;

import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Consumer;
import java.util.function.Function;
import java.util.function.ToLongFunction;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * The gRPC service: maps each request to its command and runs it through {@link GrpcRequestContexts},
 * {@link GrpcScheduledCalls} and {@link GrpcIteratorOperations}.
 */
import static it.cavallium.rockserver.core.server.GrpcProtoMapping.*;
import static it.cavallium.rockserver.core.server.GrpcRequestErrors.*;

final class GrpcServerImpl extends ReactorRocksDBServiceGrpc.RocksDBServiceImplBase {

	private static final Logger LOG = LoggerFactory.getLogger(GrpcServer.class.getName());

	static final long ITERATOR_VALUE_PAGE_SIZE = 64L;
	static final long ITERATOR_ADVANCE_STEP_SIZE = 4_096L;
	private static final int WRITE_ELISION_MULTI_STEP_SIZE = 4_096;

	private final GrpcServer server;
	private final RocksDBConnection client;
	final GrpcRequestContexts contexts;
	final GrpcIteratorOperations iterators;
	final GrpcScheduledCalls scheduledCalls;

	GrpcServerImpl(GrpcServer server, RocksDBConnection client) {
		this.server = server;
		this.client = Objects.requireNonNull(client, "client");
		this.contexts = server.contexts;
		this.iterators = new GrpcIteratorOperations(server, contexts);
		this.scheduledCalls = new GrpcScheduledCalls(server.mustCompleteOperations);
	}

	/**
	 * Moves a streaming path onto a new view of the column's workload scheduler, never the proxy scheduler,
	 * whose workers bypass its permits. The stream ends with an error when the context deadline passes or the
	 * view drops a queued drain (shutdown), instead of stopping without a terminal signal.
	 */
	private <T> Function<Flux<T>, Flux<T>> publishOnStreamingScheduler(
			ResolvedRequestContext context,
			OperationFamily family, long columnId) {
		var streamingScheduler = server.columnWorkloadScheduler(columnId).scheduler(context.profile(),
				family,
				context.localMonotonicDeadlineNanos());
		return source -> RWScheduler.publishOn(source, streamingScheduler);
	}

	// functions

	@Override
	public Mono<CapabilitiesResponse> getCapabilities(CapabilitiesRequest request) {
		var capabilities = client.getCapabilities();
		return Mono.just(CapabilitiesResponse.newBuilder()
				.setWorkloadContractVersion(capabilities.workloadContractVersion())
				.setClientSessions(capabilities.clientSessions())
				.setNumberedBatches(capabilities.numberedBatches())
				.build());
	}

	@Override
	public Mono<OpenClientSessionResponse> openClientSession(OpenClientSessionRequest request) {
		return Mono.defer(() -> Mono.fromFuture(client.openClientSessionAsync(
						contexts.mapRequestContext(request.getContext()).value())))
				.map(id -> OpenClientSessionResponse.newBuilder().setSessionId(id).build())
				.transform(this.onErrorMapMonoWithRequestInfo("openClientSession", request));
	}

	@Override
	public Mono<OpenTransactionResponse> openTransaction(OpenTransactionRequest request) {
		return executeSync(request.getContext(), OperationFamily.METADATA, contextualApi -> {
			var txId = contextualApi.openTransaction(
					Duration.ofNanos(request.getTransactionLeaseTtlNanos()));
			return OpenTransactionResponse.newBuilder().setTransactionId(txId).build();
		}).transform(this.onErrorMapMonoWithRequestInfo("openTransaction", request));
	}

	@Override
	public Mono<CloseTransactionResponse> closeTransaction(CloseTransactionRequest request) {
		return Mono.defer(() -> {
			if (request.getCommit()) {
				return executeSync(request.getContext(), OperationFamily.MUTATION, contextualApi -> {
					var committed = contextualApi.closeTransaction(request.getTransactionId(), true);
					return CloseTransactionResponse.newBuilder().setSuccessful(committed).build();
				});
			}
			requireV3(request.getContext().getWorkloadContractVersion());
			return scheduledCalls.executeMustComplete("rollback",
					() -> {
						var rolledBack = contexts.protectedApi().closeTransaction(request.getTransactionId(), false);
						return CloseTransactionResponse.newBuilder().setSuccessful(rolledBack).build();
					},
					server.workloadScheduler(server.scheduler, WorkloadProfile.CONTROL, OperationFamily.CONTROL,
							Long.MAX_VALUE));
		}).transform(this.onErrorMapMonoWithRequestInfo("closeTransaction", request));
	}

	@Override
	public Mono<Empty> closeFailedUpdate(CloseFailedUpdateRequest request) {
		requireV3(request.getWorkloadContractVersion());
		if (server.proxyScheduler != null) {
			// The connection may answer late, as a replicated close does: it must not hold a CONTROL worker meanwhile,
			// and no queue may refuse it, because a refused close keeps its key for the whole lease.
			return scheduledCalls.executeMustCompleteAsync("closeFailedUpdate",
					() -> contexts.protectedAsyncApi().closeFailedUpdateAsync(request.getUpdateId()))
					.thenReturn(Empty.getDefaultInstance())
					.transform(this.onErrorMapMonoWithRequestInfo("closeFailedUpdate", request));
		}
		return scheduledCalls.executeMustComplete("closeFailedUpdate", () -> {
			contexts.protectedApi().closeFailedUpdate(request.getUpdateId());
			return Empty.getDefaultInstance();
		}, server.workloadScheduler(server.scheduler, WorkloadProfile.CONTROL, OperationFamily.CONTROL, Long.MAX_VALUE))
				.transform(this.onErrorMapMonoWithRequestInfo("closeFailedUpdate", request));
	}

	/** Closes an update nobody can learn the id of. Nothing waits for it. */
	private void closeAbandonedUpdate(long updateId) {
		if (server.proxyScheduler == null) {
			contexts.protectedApi().closeFailedUpdate(updateId);
			return;
		}
		contexts.protectedAsyncApi().closeFailedUpdateAsync(updateId).whenComplete((_, error) -> {
			if (error != null && !isRetryOfGoneCaller(error)) {
				server.mustCompleteOperations.countLateProtectedOperationFailure("closeAbandonedUpdate");
				LOG.warn("Failed to close the update {} of a cancelled request", updateId, error);
			}
		});
	}

	/** The answer that tells a caller to run its update again, which means nothing once that caller is gone. */
	static boolean isRetryOfGoneCaller(Throwable error) {
		var rocksError = findRocksDBException(error);
		return rocksError != null && rocksError.getErrorUniqueId() == RocksDBErrorType.UPDATE_RETRY;
	}

	@Override
	public Mono<CreateColumnResponse> createColumn(CreateColumnRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			var colId = contextualApi.createColumn(request.getName(), mapColumnSchema(request.getSchema()));
			return CreateColumnResponse.newBuilder().setColumnId(colId).build();
		}).transform(this.onErrorMapMonoWithRequestInfo("createColumn", request));
	}

	@Override
	public Mono<Empty> deleteColumn(DeleteColumnRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			contextualApi.deleteColumn(request.getColumnId());
			return Empty.getDefaultInstance();
		}).transform(this.onErrorMapMonoWithRequestInfo("deleteColumn", request));
	}

	@Override
	public Mono<DeleteColumnIfExistsResponse> deleteColumnIfExists(DeleteColumnIfExistsRequest request) {
		return executeWrite(request.getContext(), contextualApi -> DeleteColumnIfExistsResponse.newBuilder()
				.setDeleted(contextualApi.deleteColumnIfExists(request.getName()))
				.build())
				.transform(this.onErrorMapMonoWithRequestInfo("deleteColumnIfExists", request));
	}

	@Override
	public Mono<GetColumnIdResponse> getColumnId(GetColumnIdRequest request) {
		return executeSync(request.getContext(), OperationFamily.METADATA, contextualApi -> {
			var colId = contextualApi.getColumnId(request.getName());
			return GetColumnIdResponse.newBuilder().setColumnId(colId).build();
		}).transform(this.onErrorMapMonoWithRequestInfo("getColumnId", request));
	}

        @Override
        public Mono<ColumnTablePropertiesResponse> getTableProperties(GetTablePropertiesRequest request) {
            return executeSync(request.getContext(), OperationFamily.FULL_SCAN_AGGREGATE,
                    api -> {
                        var p = api.getTableProperties(request.getColumnId());
                        return ColumnTablePropertiesResponse.newBuilder()
                                .setTableCount(p.tableCount())
                                .setDataSize(p.dataSize())
                                .setIndexSize(p.indexSize())
                                .setIndexPartitions(p.indexPartitions())
                                .setTopLevelIndexSize(p.topLevelIndexSize())
                                .setFilterSize(p.filterSize())
                                .setRawKeySize(p.rawKeySize())
                                .setRawValueSize(p.rawValueSize())
                                .setNumDataBlocks(p.numDataBlocks())
                                .setNumEntries(p.numEntries())
                                .setNumDeletions(p.numDeletions())
                                .setNumMergeOperands(p.numMergeOperands())
                                .setNumRangeDeletions(p.numRangeDeletions())
                                .setSlowCompressionEstimatedDataSize(p.slowCompressionEstimatedDataSize())
                                .setFastCompressionEstimatedDataSize(p.fastCompressionEstimatedDataSize())
                                .setOldestCreationTime(p.oldestCreationTime())
                                .setNewestCreationTime(p.newestCreationTime())
                                .setOldestKeyTime(p.oldestKeyTime())
                                .putAllFormatVersions(p.formatVersions())
                                .putAllFixedKeyLengths(p.fixedKeyLengths())
                                .putAllIndexKeysAreUserKeys(p.indexKeysAreUserKeys())
                                .putAllIndexValuesAreDeltaEncoded(p.indexValuesAreDeltaEncoded())
                                .putAllColumnFamilyIds(p.columnFamilyIds())
                                .putAllFilterPolicies(p.filterPolicies())
                                .putAllComparators(p.comparators())
                                .putAllMergeOperators(p.mergeOperators())
                                .putAllPrefixExtractors(p.prefixExtractors())
                                .putAllPropertyCollectors(p.propertyCollectors())
                                .putAllCompressions(p.compressions())
                                .build();
                    })
                    .transform(this.onErrorMapMonoWithRequestInfo("getTableProperties", request));
        }

	@Override
	public Mono<EntriesCount> estimateNumKeys(EstimateNumKeysRequest request) {
		return executeSync(request.getContext(), OperationFamily.METADATA, contextualApi -> EntriesCount.newBuilder()
				.setCount(contextualApi.estimateNumKeys(request.getColumnId()))
				.build())
				.transform(this.onErrorMapMonoWithRequestInfo("estimateNumKeys", request));
	}

	@Override
	public Mono<Empty> put(PutRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			contextualApi.put(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getData().getKeysList()),
					toBuf(request.getData().getValue()),
					RequestType.none()
			);
			return Empty.getDefaultInstance();
		}).transform(this.onErrorMapMonoWithRequestInfo("put", request));
	}

	@Override
	public Mono<Empty> putEnsure(PutRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			contextualApi.put(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getData().getKeysList()),
					toBuf(request.getData().getValue()),
					RequestType.ensure());
			return Empty.getDefaultInstance();
		}).transform(this.onErrorMapMonoWithRequestInfo("putEnsure", request));
	}

	@Override
	public Mono<Empty> delete(DeleteRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			contextualApi.delete(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getKeysList()),
					RequestType.none()
			);
			return Empty.getDefaultInstance();
		}).transform(this.onErrorMapMonoWithRequestInfo("delete", request));
	}

	@Override
	public Mono<Empty> deleteRange(DeleteRangeRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			contextualApi.deleteRange(request.getColumnId(),
					mapKeys(request.getStartKeysInclusiveList()),
					mapKeys(request.getEndKeysExclusiveList())
			);
			return Empty.getDefaultInstance();
		}).transform(this.onErrorMapMonoWithRequestInfo("deleteRange", request));
	}

	@Override
	public Mono<it.cavallium.rockserver.core.common.api.proto.MultiGetPage> getMulti(GetMultiRequest request) {
		return Mono.defer(() -> {
			var command = MultiGetTask.mapGetMultiRequest(request);
			return scheduledCalls.fromCancellableFuture(command.handleAsync(contexts.asyncApi(request.getContext())));
		}).map(page -> {
			var response = it.cavallium.rockserver.core.common.api.proto.MultiGetPage.newBuilder()
					.setHasMore(page.hasMore());
			for (var value : page.values()) {
				var slot = MultiGetValue.newBuilder();
				if (value != null) {
					slot.setValue(Utils.toByteString(value));
				}
				response.addValues(slot);
			}
			return response.build();
		}).transform(this.onErrorMapMonoWithRequestInfo("getMulti", request));
	}

	@Override
	public Mono<ExistsMultiResponse> existsMulti(ExistsMultiRequest request) {
		return Mono.defer(() -> {
				var keys = request.getKeysMultiList().stream()
						.map(keyTuple -> mapKeys(keyTuple.getKeysList()))
						.toList();
				return scheduledCalls.fromCancellableFuture(contexts.asyncApi(request.getContext()).existsMultiAsync(
						request.getTransactionId(),
						request.getColumnId(),
						keys));
			})
				.map(present -> ExistsMultiResponse.newBuilder().addAllPresent(present).build())
				.transform(this.onErrorMapMonoWithRequestInfo("existsMulti", request));
	}

	@Override
	public Mono<Empty> merge(MergeRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			contextualApi.merge(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getData().getKeysList()),
					toBuf(request.getData().getValue()),
					RequestType.none()
			);
			return Empty.getDefaultInstance();
		}).transform(this.onErrorMapMonoWithRequestInfo("merge", request));
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
								sink.next(mapKVBatch(batch.getEntriesList()));
							} catch (Throwable ex) {
								sink.error(ex);
							}
						});

				return Mono
						.fromFuture(() -> contexts.asyncApi(initialRequest.getContext()).putBatchAsync(
								initialRequest.getColumnId(), batches, mode))
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
								sink.next(mapKVBatch(batch.getEntriesList()));
							} catch (Throwable ex) {
								sink.error(ex);
							}
						});

				return Mono
						.fromFuture(() -> contexts.asyncApi(initialRequest.getContext()).mergeBatchAsync(
								initialRequest.getColumnId(), batches, mode))
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
		return deleteMultiResponseFlux(request, "deleteMultiGetPrevious", RequestType.previous(), previous -> {
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
				RequestType.previousPresence(),
				present -> PreviousPresence.newBuilder().setPresent(present).build());
	}

	private Mono<Empty> deleteMultiDataFlux(DeleteMultiInitialRequest initialRequest,
			Flux<DeleteRequest> dataFlux,
			String requestName) {
		var context = contexts.mapRequestContext(initialRequest.getContext());
		if (collectsWhole(context)) {
			var contextualApi = contexts.asyncApi(context);
			return collectMulti(context, dataFlux, GrpcServerImpl::encodedInputBytes, requestName)
					.flatMap(data -> Mono.fromFuture(() -> {
						var keys = new ArrayList<Keys>(data.size());
						for (var value : data) {
							keys.add(mapKeys(value.getKeysList()));
						}
						return contextualApi.deleteMultiAsync(
								initialRequest.getTransactionOrUpdateId(),
								initialRequest.getColumnId(),
								keys,
								RequestType.none());
					}))
					.thenReturn(Empty.getDefaultInstance())
					.transform(this.onErrorMapMonoWithRequestInfo(requestName, initialRequest));
		}
		var contextualApi = contexts.syncApi(context);
		var appliedItems = new AppliedItems(requestName);
		return dataFlux
				.transform(publishOnStreamingScheduler(context, OperationFamily.MUTATION, initialRequest.getColumnId()))
				.doOnNext(data -> contextualApi.delete(initialRequest.getTransactionOrUpdateId(),
						initialRequest.getColumnId(),
						mapKeys(data.getKeysList()),
							RequestType.none()))
				.doOnNext(_ -> appliedItems.add(1))
				.onErrorMap(appliedItems::failure)
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
				var context = contexts.mapRequestContext(initialRequest.getContext());
				var dataFlux = requestsFlux
						.skip(1)
						.map(deleteRequest -> {
							if (!deleteRequest.hasData()) {
								throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
							}
							return deleteRequest.getData();
						});
				if (collectsWhole(context)) {
					var contextualApi = contexts.asyncApi(context);
					return collectMulti(context, dataFlux, GrpcServerImpl::encodedInputBytes, requestName)
							.flatMapMany(data -> {
								var keys = new ArrayList<Keys>(data.size());
								for (var value : data) {
									keys.add(mapKeys(value.getKeysList()));
								}
								return Mono.fromFuture(() -> contextualApi.deleteMultiAsync(
										initialRequest.getTransactionOrUpdateId(),
										initialRequest.getColumnId(),
										keys,
										requestType))
									// Mapped first: a result may be null (no previous value), which a Flux cannot carry.
									.flatMapMany(results -> Flux.fromIterable(mapEach(results, mapper)));
							})
							.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest));
				}
				var contextualApi = contexts.syncApi(context);
				var appliedItems = new AppliedItems(requestName);
				return dataFlux
						.transform(publishOnStreamingScheduler(context, OperationFamily.MUTATION, initialRequest.getColumnId()))
						.map(data -> mapper.apply(contextualApi.delete(initialRequest.getTransactionOrUpdateId(),
								initialRequest.getColumnId(),
								mapKeys(data.getKeysList()),
								requestType)))
						.doOnNext(_ -> appliedItems.add(1))
						.onErrorMap(appliedItems::failure)
						.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest));
			} else {
				return Flux.error(RocksDBException.of(
						RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
			}
		});
	}

	@Override
	public Mono<Merged> mergeGetMerged(MergeRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			var merged = contextualApi.merge(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getData().getKeysList()),
					toBuf(request.getData().getValue()),
					RequestType.merged());
			return Merged.newBuilder()
					.setMerged(merged != null ? unmapValueHeap(merged) : ByteString.EMPTY)
					.build();
		}).transform(this.onErrorMapMonoWithRequestInfo("mergeGetMerged", request));
	}

	@Override
	public Mono<Empty> putMultiList(PutMultiListRequest request) {
		return executePutMultiList(request, RequestType.none(), "putMultiList");
	}

	@Override
	public Mono<Empty> putMultiListEnsure(PutMultiListRequest request) {
		return executePutMultiList(request, RequestType.ensure(), "putMultiListEnsure");
	}

	private Mono<Empty> executePutMultiList(PutMultiListRequest request,
			RequestType.RequestPut<? super Buf, ?> requestType,
			String requestName) {
		var initialRequest = request.getInitialRequest();
		var batch = mapKVBatch(request.getDataList());
		return executeWrite(initialRequest.getContext(), contextualApi -> {
			contextualApi.putMulti(initialRequest.getTransactionOrUpdateId(),
					initialRequest.getColumnId(),
					batch.keys(),
					batch.values(),
					requestType);
			return Empty.getDefaultInstance();
		}).transform(this.onErrorMapMonoWithRequestInfo(requestName, initialRequest));
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
				var context = contexts.mapRequestContext(initialRequest.getContext());
				var dataFlux = requestsFlux
						.skip(1) // skip the initial request
						.map(mergeRequest -> {
							if (!mergeRequest.hasData()) {
								throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
							}
							return mergeRequest.getData();
						});
				if (collectsWhole(context)) {
					var contextualApi = contexts.asyncApi(context);
					return collectMulti(context, dataFlux, GrpcServerImpl::encodedInputBytes,
							"mergeMultiGetMerged")
							.flatMapMany(data -> {
								var batch = mapKVBatch(data);
								return Mono.fromFuture(() -> contextualApi.mergeMultiAsync(
										initialRequest.getTransactionOrUpdateId(),
										initialRequest.getColumnId(),
										batch.keys(),
										batch.values(),
										RequestType.merged()))
									.flatMapMany(results -> Flux.fromIterable(mapEach(results, merged -> Merged.newBuilder()
											.setMerged(merged != null ? unmapValueHeap(merged) : ByteString.EMPTY)
											.build())));
							})
							.onErrorMap(ex -> GrpcRequestErrors.handleError(ex).asRuntimeException());
				}
				var contextualApi = contexts.syncApi(context);
				var appliedItems = new AppliedItems("mergeMultiGetMerged");
				return dataFlux
						.transform(publishOnStreamingScheduler(context, OperationFamily.MUTATION, initialRequest.getColumnId()))
						.map(data -> {
							var merged = contextualApi.merge(initialRequest.getTransactionOrUpdateId(),
									initialRequest.getColumnId(),
									mapKeys(data.getKeysList()),
									toBuf(data.getValue()),
									RequestType.merged());
							return Merged.newBuilder()
									.setMerged(merged != null ? unmapValueHeap(merged) : ByteString.EMPTY)
									.build();
						})
						.doOnNext(_ -> appliedItems.add(1))
						.onErrorMap(appliedItems::failure)
						.onErrorMap(ex -> GrpcRequestErrors.handleError(ex).asRuntimeException());
			} else {
				return Flux.error(RocksDBException.of(
						RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
			}
		});
	}

	private Mono<Empty> mergeMultiDataFlux(MergeMultiInitialRequest initialRequest,
			Flux<KV> dataFlux, String requestName) {
		var context = contexts.mapRequestContext(initialRequest.getContext());
		if (collectsWhole(context)) {
			var contextualApi = contexts.asyncApi(context);
			return collectMulti(context, dataFlux, GrpcServerImpl::encodedInputBytes, requestName)
					.flatMap(data -> {
						var batch = mapKVBatch(data);
						return Mono.fromFuture(() -> contextualApi.mergeMultiAsync(
								initialRequest.getTransactionOrUpdateId(),
								initialRequest.getColumnId(),
								batch.keys(),
								batch.values(),
								RequestType.none()));
					})
					.thenReturn(Empty.getDefaultInstance())
					.transform(this.onErrorMapMonoWithRequestInfo(requestName, initialRequest));
		}
		var contextualApi = contexts.syncApi(context);
		var appliedItems = new AppliedItems(requestName);
		return dataFlux
				.transform(publishOnStreamingScheduler(context, OperationFamily.MUTATION, initialRequest.getColumnId()))
				.doOnNext(data -> {
					contextualApi.merge(initialRequest.getTransactionOrUpdateId(),
							initialRequest.getColumnId(),
							mapKeys(data.getKeysList()),
							toBuf(data.getValue()),
							RequestType.none());
				})
				.doOnNext(_ -> appliedItems.add(1))
				.onErrorMap(appliedItems::failure)
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
	public Mono<Empty> putMultiEnsure(Flux<PutMultiRequest> request) {
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
						.map(putRequest -> {
							if (!putRequest.hasData()) {
								throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
							}
							return putRequest.getData();
						});
				return putMultiEnsureDataFlux(initialRequest, dataFlux, "putMultiEnsure");
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
		return putMultiResponseFlux(request, "putMultiGetPrevious", RequestType.previous(), previous -> {
			var builder = Previous.newBuilder();
			if (previous != null) {
				builder.setPrevious(Utils.toByteString(previous));
			}
			return builder.build();
		});
	}

	@Override
	public Flux<Delta> putMultiGetDelta(Flux<PutMultiRequest> request) {
		return putMultiResponseFlux(request, "putMultiGetDelta", RequestType.delta(), delta -> {
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
				RequestType.changed(),
				changed -> Changed.newBuilder().setChanged(changed).build());
	}

	@Override
	public Flux<PreviousPresence> putMultiGetPreviousPresence(Flux<PutMultiRequest> request) {
		return putMultiResponseFlux(request,
				"putMultiGetPreviousPresence",
				RequestType.previousPresence(),
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
				var context = contexts.mapRequestContext(initialRequest.getContext());
				var dataFlux = requestsFlux
						.skip(1)
						.map(putRequest -> {
							if (!putRequest.hasData()) {
								throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST, "Multiple initial requests");
							}
							return putRequest.getData();
						});
				if (collectsWhole(context)) {
					var contextualApi = contexts.asyncApi(context);
					return collectMulti(context, dataFlux, GrpcServerImpl::encodedInputBytes, requestName)
							.flatMapMany(data -> {
								var batch = mapKVBatch(data);
								return Mono.fromFuture(() -> contextualApi.putMultiAsync(
										initialRequest.getTransactionOrUpdateId(),
										initialRequest.getColumnId(),
										batch.keys(),
										batch.values(),
										requestType))
									// Mapped first: a result may be null (no previous value), which a Flux cannot carry.
									.flatMapMany(results -> Flux.fromIterable(mapEach(results, mapper)));
							})
							.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest));
				}
				var contextualApi = contexts.syncApi(context);
				var appliedItems = new AppliedItems(requestName);
				return dataFlux
						.transform(publishOnStreamingScheduler(context, OperationFamily.MUTATION, initialRequest.getColumnId()))
						.map(data -> mapper.apply(contextualApi.put(initialRequest.getTransactionOrUpdateId(),
								initialRequest.getColumnId(),
								mapKeys(data.getKeysList()),
								toBuf(data.getValue()),
								requestType)))
						.doOnNext(_ -> appliedItems.add(1))
						.onErrorMap(appliedItems::failure)
						.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest));
			} else {
				return Flux.error(RocksDBException.of(
						RocksDBException.RocksDBErrorType.PUT_INVALID_REQUEST, "No initial request"));
			}
		});
	}

	private Mono<Empty> putMultiDataFlux(PutMultiInitialRequest initialRequest,
			Flux<KV> dataFlux, String requestName) {
		var context = contexts.mapRequestContext(initialRequest.getContext());
		if (collectsWhole(context)) {
			var contextualApi = contexts.asyncApi(context);
			return collectMulti(context, dataFlux, GrpcServerImpl::encodedInputBytes, requestName)
					.flatMap(data -> {
						var batch = mapKVBatch(data);
						return Mono.fromFuture(() -> contextualApi.putMultiAsync(
								initialRequest.getTransactionOrUpdateId(),
								initialRequest.getColumnId(),
								batch.keys(),
								batch.values(),
								RequestType.none()));
					})
					.thenReturn(Empty.getDefaultInstance())
					.transform(this.onErrorMapMonoWithRequestInfo(requestName, initialRequest));
		}
		var contextualApi = contexts.syncApi(context);
		var appliedItems = new AppliedItems(requestName);
		return dataFlux
				.transform(publishOnStreamingScheduler(context, OperationFamily.MUTATION, initialRequest.getColumnId()))
				.doOnNext(data -> {
					contextualApi.put(initialRequest.getTransactionOrUpdateId(),
							initialRequest.getColumnId(),
							mapKeys(data.getKeysList()),
							toBuf(data.getValue()),
							RequestType.none());
				})
				.doOnNext(_ -> appliedItems.add(1))
				.onErrorMap(appliedItems::failure)
				.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest))
				.then(Mono.just(Empty.getDefaultInstance()));
	}

	private Mono<Empty> putMultiEnsureDataFlux(PutMultiInitialRequest initialRequest,
			Flux<KV> dataFlux,
			String requestName) {
		var context = contexts.mapRequestContext(initialRequest.getContext());
		if (collectsWhole(context)) {
			var contextualApi = contexts.asyncApi(context);
			return collectMulti(context, dataFlux, GrpcServerImpl::encodedInputBytes, requestName)
					.flatMap(data -> {
						var batch = mapKVBatch(data);
						return Mono.fromFuture(() -> contextualApi.putMultiAsync(
								initialRequest.getTransactionOrUpdateId(),
								initialRequest.getColumnId(),
								batch.keys(),
								batch.values(),
								RequestType.ensure()));
					})
					.thenReturn(Empty.getDefaultInstance())
					.transform(this.onErrorMapMonoWithRequestInfo(requestName, initialRequest));
		}
		var contextualApi = contexts.syncApi(context);
		var appliedItems = new AppliedItems(requestName);
		return dataFlux
				.buffer(WRITE_ELISION_MULTI_STEP_SIZE)
				.transform(publishOnStreamingScheduler(context, OperationFamily.MUTATION, initialRequest.getColumnId()))
				.doOnNext(data -> {
					var batch = mapKVBatch(data);
					contextualApi.putMulti(initialRequest.getTransactionOrUpdateId(),
							initialRequest.getColumnId(),
							batch.keys(),
							batch.values(),
							RequestType.ensure());
				})
				.doOnNext(data -> appliedItems.add(data.size()))
				.onErrorMap(appliedItems::failure)
				.transform(this.onErrorMapFluxWithRequestInfo(requestName, initialRequest))
				.then(Mono.just(Empty.getDefaultInstance()));
	}

	/**
	 * What a streamed multi write applied item by item (not {@link #collectsWhole}) has applied so far. Each item is a
	 * write of its own, so once one was applied, a later item's failure flagged "not applied"
	 * ({@link RocksDBException#isNotApplied()}: a deadline that came before its dispatch, a replicated node that refused
	 * its proposal) is true for that item only. The call then fails with {@code INVALID_REQUEST}, not flagged, which
	 * clients do not resend, and says how much was applied, as a partly applied batch does. A failure before any item
	 * was applied, and one that is not flagged, pass unchanged. The new message names the type of the item's failure
	 * but not its text, which says "not applied": a client older than the trailer reads that from the text.
	 */
	static final class AppliedItems {

		private final String requestName;
		// Written by the stream's one onNext at a time, read by its onError.
		private volatile long applied;

		AppliedItems(String requestName) {
			this.requestName = requestName;
		}

		void add(long items) {
			applied += items;
		}

		Throwable failure(Throwable error) {
			long applied = this.applied;
			var rocksError = findRocksDBException(error);
			if (applied == 0 || rocksError == null || !rocksError.isNotApplied()) {
				return error;
			}
			return RocksDBException.of(RocksDBErrorType.INVALID_REQUEST,
					requestName + " was partly applied: " + applied + " items were applied one by one before it failed, so"
							+ " it is not safe to resend as a whole; the item that failed was refused with "
							+ rocksError.getErrorUniqueId());
		}
	}

	/** {@code mapper} applied to each of {@code results}, nulls included. */
	private static <T, R> List<R> mapEach(List<T> results, Function<? super T, ? extends R> mapper) {
		var mapped = new ArrayList<R>(results.size());
		for (T result : results) {
			mapped.add(mapper.apply(result));
		}
		return mapped;
	}

	/**
	 * Heap cost counted per collected item of a numbered multi write besides its bytes: the proto message, its key
	 * list, and the {@code ByteString} and backing array of the value. Each key segment and the value also count
	 * {@link KVBatchChunks#ITEM_OVERHEAD_BYTES}, as the log entry encoding does.
	 */
	static final long NUMBERED_MULTI_ITEM_HEAP_BYTES = 96;
	/**
	 * Most counted bytes of a numbered multi write outside LATENCY ({@link #collectsWhole}): keys, values and the
	 * per-item costs above. About one replicated entry, the unit such a write is applied as.
	 */
	static final long NUMBERED_MULTI_MAX_BYTES = 16L * 1024 * 1024;
	/** Most items: what {@link #NUMBERED_MULTI_MAX_BYTES} holds of the smallest item (one empty key segment). */
	static final int NUMBERED_MULTI_MAX_ITEMS = (int) (NUMBERED_MULTI_MAX_BYTES
			/ (NUMBERED_MULTI_ITEM_HEAP_BYTES + KVBatchChunks.ITEM_OVERHEAD_BYTES));
	/** System property: counted bytes all numbered multi writes being collected on this server hold at once. */
	static final String NUMBERED_MULTI_COLLECT_BYTES_PROPERTY = "rockserver.grpc.numbered-multi-collect-bytes";
	private static final long DEFAULT_NUMBERED_MULTI_COLLECT_BYTES = 4 * NUMBERED_MULTI_MAX_BYTES;

	/**
	 * Counted bytes the numbered multi writes being collected hold now, against {@link #numberedCollectLimit}. They
	 * are held on the heap before the write reaches the node's write admission, so they need a budget of their own.
	 */
	private final AtomicLong numberedCollectBytes = new AtomicLong();
	private final long numberedCollectLimit = numberedCollectLimit();

	private static long numberedCollectLimit() {
		var value = System.getProperty(NUMBERED_MULTI_COLLECT_BYTES_PROPERTY);
		if (value != null && !value.isBlank()) {
			try {
				long parsed = Long.parseLong(value.trim());
				if (parsed > 0) {
					return parsed;
				}
			} catch (NumberFormatException ignored) {
				// default below
			}
			LOG.warn("Invalid byte count for system property {}: {}", NUMBERED_MULTI_COLLECT_BYTES_PROPERTY, value);
		}
		return DEFAULT_NUMBERED_MULTI_COLLECT_BYTES;
	}

	/**
	 * System property: how long a numbered multi write being collected may go without a new item, in milliseconds,
	 * before its collection ends; zero turns the limit off.
	 */
	static final String NUMBERED_MULTI_COLLECT_IDLE_PROPERTY = "rockserver.grpc.numbered-multi-collect-idle-ms";
	private static final Duration DEFAULT_NUMBERED_MULTI_COLLECT_IDLE = Duration.ofSeconds(30);
	private final Duration numberedCollectIdle = GrpcServerSettings.durationProperty(
			NUMBERED_MULTI_COLLECT_IDLE_PROPERTY, DEFAULT_NUMBERED_MULTI_COLLECT_IDLE);

	/** Counted bytes the numbered multi writes being collected hold now, of {@link #numberedCollectLimit}. */
	long numberedCollectBytes() {
		return numberedCollectBytes.get();
	}

	/**
	 * True if a streamed multi write is collected whole and applied as one multi call: under LATENCY (its fan-out
	 * limits), and when it carries a client session's request id. Item by item, every item would carry the same number,
	 * and a replicated server would apply the first and answer every later item from the first's record without applying
	 * it. Collected, the call is one numbered write, applied at most once, whose copies answer with its result.
	 */
	private static boolean collectsWhole(ResolvedRequestContext context) {
		return context.profile() == WorkloadProfile.LATENCY || context.value().requestId() != null;
	}

	/**
	 * The items of a streamed multi write, collected whole ({@link #collectsWhole}). Outside LATENCY each item takes its
	 * counted bytes ({@link #numberedItemBytes}) from the server's collect budget as it arrives, and gives them back when
	 * the collection ends; past {@link #NUMBERED_MULTI_MAX_BYTES} the write is refused as invalid, and when the budget is
	 * full as {@code SERVER_OVERLOADED} flagged not applied, so that its caller knows it can send it again.
	 */
	private <T> Mono<List<T>> collectMulti(ResolvedRequestContext context, Flux<T> dataFlux,
			ToLongFunction<T> encodedInputBytes, String requestName) {
		if (context.profile() == WorkloadProfile.LATENCY) {
			return collectLatencyMulti(dataFlux, encodedInputBytes, requestName);
		}
		return collectNumbered(dataFlux, numberedCollectBytes, numberedCollectLimit, numberedCollectIdle,
				encodedInputBytes, requestName);
	}

	/**
	 * The collection of {@link #collectMulti} outside LATENCY, against the collect budget {@code budget} of at most
	 * {@code limit} bytes: each item takes its bytes through a {@link CollectHold}, which gives them back on every end of
	 * the collection, a cancel included (static for tests).
	 *
	 * <p>A client that goes silent without closing its connection would keep what its stream took until the transport
	 * notices, which can take very long. So a collection that gets no item for {@code idle} (zero: no limit) ends, as a
	 * cancel does, and the call fails with {@code READ_DEADLINE_EXCEEDED} flagged not applied: nothing of a collected
	 * write is applied before its last item. One periodic check per collection, not a timer per item; it fires between
	 * {@code idle} and 1.25 times it.</p>
	 */
	static <T> Mono<List<T>> collectNumbered(Flux<T> dataFlux, AtomicLong budget, long limit, Duration idle,
			ToLongFunction<T> encodedInputBytes, String requestName) {
		return Mono.defer(() -> {
			var buffer = new LatencyMultiBuffer<T>(NUMBERED_MULTI_MAX_ITEMS, NUMBERED_MULTI_MAX_BYTES,
					"numbered " + requestName);
			var hold = new CollectHold(budget, limit);
			var lastItemNanos = new AtomicLong(System.nanoTime());
			var collection = dataFlux
					.doOnNext(value -> {
						lastItemNanos.lazySet(System.nanoTime());
						long bytes = numberedItemBytes(value, encodedInputBytes.applyAsLong(value));
						buffer.add(value, bytes);
						if (!hold.take(bytes)) {
							throw RocksDBException.notApplied(RocksDBErrorType.SERVER_OVERLOADED,
									"numbered " + requestName + ": the server is collecting too many multi writes ("
											+ limit + " bytes), retry later");
						}
					})
					.then(Mono.fromSupplier(buffer::values))
					.doFinally(_ -> hold.end());
			if (idle.isZero() || idle.isNegative()) {
				return collection;
			}
			// The first to end cancels the other: the idle failure cancels the collection (and with it the call's
			// request stream), which gives its bytes back; a collection that ends stops the check.
			return Mono.firstWithSignal(collection, Mono.<List<T>>create(sink -> {
				long idleNanos = idle.toNanos();
				long periodNanos = Math.max(TimeUnit.MILLISECONDS.toNanos(1), idleNanos / 4);
				sink.onDispose(Schedulers.parallel().schedulePeriodically(() -> {
					if (System.nanoTime() - lastItemNanos.get() >= idleNanos) {
						sink.error(RocksDBException.notApplied(RocksDBErrorType.READ_DEADLINE_EXCEEDED,
								"numbered " + requestName + ": no item arrived for " + idle.toMillis()
										+ " ms while the server was collecting the write; it was not applied"));
					}
				}, periodNanos, periodNanos, TimeUnit.NANOSECONDS));
			}));
		});
	}

	/**
	 * The bytes one numbered multi write being collected took from the server's collect budget: each item takes its
	 * own, and {@link #end} gives them all back once, when the collection ends. A late item that races a cancel and
	 * arrives after the end takes nothing (it fails as if the budget were full), so nothing stays taken.
	 */
	static final class CollectHold {

		private final AtomicLong budget;
		private final long limit;
		private long held;
		private boolean ended;

		CollectHold(AtomicLong budget, long limit) {
			this.budget = budget;
			this.limit = limit;
		}

		/** Takes {@code bytes} from the budget: false, taking nothing, if it is full or the collection ended. */
		synchronized boolean take(long bytes) {
			if (ended) {
				return false;
			}
			while (true) {
				long current = budget.get();
				if (current + bytes > limit) {
					return false;
				}
				if (budget.compareAndSet(current, current + bytes)) {
					held += bytes;
					return true;
				}
			}
		}

		/** Gives back what the items took; later items take nothing. */
		synchronized void end() {
			budget.addAndGet(-held);
			held = 0;
			ended = true;
		}
	}

	/** Counted bytes of one item of a numbered multi write: its encoded bytes plus the per-item costs. */
	static long numberedItemBytes(Object value, long encodedBytes) {
		int segments = switch (value) {
			case KV kv -> kv.getKeysCount() + 1;
			case DeleteRequest delete -> delete.getKeysCount();
			default -> 1;
		};
		return encodedBytes + NUMBERED_MULTI_ITEM_HEAP_BYTES + (long) Math.max(1, segments) * KVBatchChunks.ITEM_OVERHEAD_BYTES;
	}

	private <T> Mono<List<T>> collectLatencyMulti(Flux<T> dataFlux,
			ToLongFunction<T> encodedInputBytes,
			String requestName) {
		int maximumItems = server.embeddedDatabase != null
				? server.embeddedDatabase.getWorkloadSettings().latencyFanOutMaxItems()
				: WorkloadAdmission.MAX_LATENCY_ITEMS;
		long maximumBytes = server.embeddedDatabase != null
				? server.embeddedDatabase.getWorkloadSettings().latencyFanOutMaxBytes()
				: WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES;
		return dataFlux.collect(
				() -> new LatencyMultiBuffer<T>(maximumItems, maximumBytes, requestName),
				(buffer, value) -> buffer.add(value, encodedInputBytes.applyAsLong(value)))
				.map(LatencyMultiBuffer::values);
	}

	private static long encodedInputBytes(KV value) {
		long bytes = value.getValue().size();
		for (var key : value.getKeysList()) {
			bytes += key.size();
		}
		return bytes;
	}

	private static long encodedInputBytes(DeleteRequest value) {
		long bytes = 0L;
		for (var key : value.getKeysList()) {
			bytes += key.size();
		}
		return bytes;
	}

	private static final class LatencyMultiBuffer<T> {

		private final int maximumItems;
		private final long maximumBytes;
		private final String requestName;
		private final ArrayList<T> values;
		private long encodedBytes;

		private LatencyMultiBuffer(int maximumItems, long maximumBytes, String requestName) {
			this.maximumItems = maximumItems;
			this.maximumBytes = maximumBytes;
			this.requestName = requestName;
			this.values = new ArrayList<>(Math.min(maximumItems, 256));
		}

		private void add(T value, long valueBytes) {
			if (values.size() >= maximumItems) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						requestName + " item count must not exceed " + maximumItems);
			}
			if (valueBytes > maximumBytes - encodedBytes) {
				throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						requestName + " encoded input must not exceed " + maximumBytes + " bytes");
			}
			values.add(value);
			encodedBytes += valueBytes;
		}

		private List<T> values() {
			return Collections.unmodifiableList(values);
		}
	}

	@Override
	public Mono<Previous> putGetPrevious(PutRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			var prev = contextualApi.put(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getData().getKeysList()),
					toBuf(request.getData().getValue()),
					RequestType.previous()
			);
			var prevBuilder = Previous.newBuilder();
			if (prev != null) {
				prevBuilder.setPrevious(Utils.toByteString(prev));
			}
			return prevBuilder.build();
		}).transform(this.onErrorMapMonoWithRequestInfo("putGetPrevious", request));
	}

	@Override
	public Mono<Delta> putGetDelta(PutRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			var delta = contextualApi.put(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getData().getKeysList()),
					toBuf(request.getData().getValue()),
					RequestType.delta()
			);
			var deltaBuilder = Delta.newBuilder();
			if (delta.previous() != null) {
				deltaBuilder.setPrevious(Utils.toByteString(delta.previous()));
			}
			if (delta.current() != null) {
				deltaBuilder.setCurrent(Utils.toByteString(delta.current()));
			}
			return deltaBuilder.build();
		}).transform(this.onErrorMapMonoWithRequestInfo("putGetDelta", request));
	}

	@Override
	public Mono<Changed> putGetChanged(PutRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			var changed = contextualApi.put(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getData().getKeysList()),
					toBuf(request.getData().getValue()),
					RequestType.changed()
			);
			return Changed.newBuilder().setChanged(changed).build();
		}).transform(this.onErrorMapMonoWithRequestInfo("putGetChanged", request));
	}

	@Override
	public Mono<PreviousPresence> putGetPreviousPresence(PutRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			var present = contextualApi.put(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getData().getKeysList()),
					toBuf(request.getData().getValue()),
					RequestType.previousPresence()
			);
			return PreviousPresence.newBuilder().setPresent(present).build();
		}).transform(this.onErrorMapMonoWithRequestInfo("putGetPreviousPresence", request));
	}

	@Override
	public Mono<Previous> deleteGetPrevious(DeleteRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			var prev = contextualApi.delete(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getKeysList()),
					RequestType.previous()
			);
			var prevBuilder = Previous.newBuilder();
			if (prev != null) {
				prevBuilder.setPrevious(Utils.toByteString(prev));
			}
			return prevBuilder.build();
		}).transform(this.onErrorMapMonoWithRequestInfo("deleteGetPrevious", request));
	}

	@Override
	public Mono<PreviousPresence> deleteGetPreviousPresence(DeleteRequest request) {
		return executeWrite(request.getContext(), contextualApi -> {
			var present = contextualApi.delete(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getKeysList()),
					RequestType.previousPresence()
			);
			return PreviousPresence.newBuilder().setPresent(present).build();
		}).transform(this.onErrorMapMonoWithRequestInfo("deleteGetPreviousPresence", request));
	}

	@Override
	public Mono<GetResponse> get(GetRequest request) {
		return executeSync(request.getContext(), OperationFamily.POINT_LOOKUP, contextualApi -> {
			var current = contextualApi.get(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getKeysList()),
					RequestType.current()
			);
			var responseBuilder = GetResponse.newBuilder();
			if (current != null) {
				responseBuilder.setValue(Utils.toByteString(current));
			}
			return responseBuilder.build();
		}).transform(this.onErrorMapMonoWithRequestInfo("get", request));
	}

	FastGetResponse createFastGetResponse(GetRequest request,
			it.cavallium.rockserver.core.common.RequestContext context,
			Keys keys,
			GrpcGetStrategy strategy,
			@Nullable EmbeddedDB embeddedDatabase) {
		if (request.getTransactionOrUpdateId() == 0 && embeddedDatabase != null) {
			EmbeddedDB.FastGetOutput output = switch (strategy) {
				case EXACT_HEAP -> EmbeddedDB.FastGetOutput.EXACT_HEAP;
				case PINNED -> EmbeddedDB.FastGetOutput.PINNED;
				case AUTOMATIC -> EmbeddedDB.FastGetOutput.AUTOMATIC;
				case LEGACY -> throw new IllegalStateException("legacy Get cannot use the custom binding");
			};
			EmbeddedDB.FastGetResult result = embeddedDatabase.tryGetFast(
					request.getColumnId(), keys, output);
			if (result != null) {
				try {
					return new FastGetResponse(result.isPresent(),
							result.value(),
							result.isPinned(),
							result);
				} catch (Throwable error) {
					result.close();
					throw error;
				}
			}
		}

		Buf current = contexts.syncApi(context).get(request.getTransactionOrUpdateId(),
				request.getColumnId(),
				keys,
				RequestType.current());
		return current != null
				? new FastGetResponse(true, current, false, null)
				: new FastGetResponse(false, null, false, null);
	}

	@Override
	public Mono<UpdateBegin> getForUpdate(GetRequest request) {
		return executeSync(request.getContext(), OperationFamily.MUTATION, contextualApi -> {
			var forUpdate = contextualApi.get(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getKeysList()),
					RequestType.forUpdate()
			);
			var responseBuilder = UpdateBegin.newBuilder();
			responseBuilder.setUpdateId(forUpdate.updateId());
			if (forUpdate.previous() != null) {
				responseBuilder.setPrevious(Utils.toByteString(forUpdate.previous()));
			}
			return responseBuilder.build();
		}, response -> {
			if (request.getTransactionOrUpdateId() == 0L) {
				closeAbandonedUpdate(response.getUpdateId());
			}
		}, server.workloadScheduler(server.scheduler, WorkloadProfile.CONTROL, OperationFamily.CONTROL, Long.MAX_VALUE))
				.transform(this.onErrorMapMonoWithRequestInfo("getForUpdate", request));
	}

	@Override
	public Mono<PreviousPresence> exists(GetRequest request) {
		return executeSync(request.getContext(), OperationFamily.POINT_LOOKUP, contextualApi -> {
			var exists = contextualApi.get(request.getTransactionOrUpdateId(),
					request.getColumnId(),
					mapKeys(request.getKeysList()),
					RequestType.exists()
			);
			return PreviousPresence.newBuilder().setPresent(exists).build();
		}).transform(this.onErrorMapMonoWithRequestInfo("exists", request));
	}

	@Override
	public Mono<OpenIteratorResponse> openIterator(OpenIteratorRequest request) {
		return executeSync(request.getContext(), OperationFamily.BOUNDARY_SEEK, contextualApi -> {
			var iteratorId = contextualApi.openIterator(request.getTransactionId(),
					request.getColumnId(),
					mapKeys(request.getStartKeysInclusiveList()),
					mapKeys(request.getEndKeysExclusiveList()),
					request.getReverse(),
					Duration.ofNanos(request.getIteratorLeaseTtlNanos())
			);
			return OpenIteratorResponse.newBuilder().setIteratorId(iteratorId).build();
		}, response -> {
			long iteratorId = response.getIteratorId();
			contexts.protectedApi().closeIterator(iteratorId);
		}, server.workloadScheduler(server.scheduler, WorkloadProfile.CONTROL, OperationFamily.CONTROL, Long.MAX_VALUE))
				.transform(this.onErrorMapMonoWithRequestInfo("openIterator", request));
	}

	@Override
	public Mono<Empty> closeIterator(CloseIteratorRequest request) {
		requireV3(request.getWorkloadContractVersion());
		return scheduledCalls.executeMustComplete("closeIterator", () -> {
				contexts.protectedApi().closeIterator(request.getIteratorId());
				return Empty.getDefaultInstance();
			}, server.workloadScheduler(server.scheduler, WorkloadProfile.CONTROL, OperationFamily.CONTROL, Long.MAX_VALUE))
				.transform(this.onErrorMapMonoWithRequestInfo("closeIterator", request));
	}

	@Override
	public Mono<Empty> seekTo(SeekToRequest request) {
		return iterators.withIteratorLease(request.getIterationId(), () -> executeCompositeRead(
				request.getContext(), OperationFamily.BOUNDARY_SEEK, contextualApi -> {
			contextualApi.seekTo(request.getIterationId(), mapKeys(request.getKeysList()));
			return Empty.getDefaultInstance();
		})).transform(this.onErrorMapMonoWithRequestInfo("seekTo", request));
	}

	@Override
	public Mono<Empty> subsequent(SubsequentRequest request) {
		return validateIteratorCounts(request)
				.then(validateSubsequentCommand(request, RequestType.exists()))
				.then(iterators.withIteratorLease(request.getIterationId(), () -> {
					if (requiresCooperativeIteratorContinuation(request)) {
						return scheduledCalls.fromCancellableIteratorFuture(() -> contexts.asyncApi(request.getContext()).subsequentAsync(
								request.getIterationId(),
								request.getSkipCount(),
								request.getTakeCount(),
								RequestType.none()))
								.thenReturn(Empty.getDefaultInstance());
					}
					return advanceIterator(request.getContext(), request.getIterationId(),
							request.getSkipCount(), request.getTakeCount())
							.thenReturn(Empty.getDefaultInstance());
				}))
				.transform(this.onErrorMapMonoWithRequestInfo("subsequent", request));
	}

	@Override
	public Mono<PreviousPresence> subsequentExists(SubsequentRequest request) {
		return validateIteratorCounts(request)
				.then(validateSubsequentCommand(request, RequestType.exists()))
				.then(iterators.withIteratorLease(request.getIterationId(), () -> {
					if (requiresCooperativeIteratorContinuation(request)) {
						return scheduledCalls.fromCancellableIteratorFuture(() -> contexts.asyncApi(request.getContext()).subsequentAsync(
								request.getIterationId(),
								request.getSkipCount(),
								request.getTakeCount(),
								RequestType.exists()))
								.map(found -> PreviousPresence.newBuilder().setPresent(found).build());
					}
					return advanceIterator(request.getContext(), request.getIterationId(), request.getSkipCount(), 0)
							.thenMany(iteratorChunks(request.getTakeCount(), ITERATOR_ADVANCE_STEP_SIZE))
							.concatMap(take -> executeCompositeRead(request.getContext(), OperationFamily.RANGE_PAGE,
									contextualApi -> contextualApi.subsequent(
											request.getIterationId(), 0, take, RequestType.exists())), 1)
							.takeUntil(found -> !found)
							.reduce(false, (found, pageFound) -> found || pageFound)
							.map(found -> PreviousPresence.newBuilder().setPresent(found).build());
				}))
				.transform(this.onErrorMapMonoWithRequestInfo("subsequentExists", request));
	}

	@Override
	public Flux<KV> subsequentMultiGet(SubsequentRequest request) {
		return validateIteratorCounts(request)
				.then(validateSubsequentCommand(request, RequestType.multi()))
				.thenMany(iterators.withIteratorFluxLease(request.getIterationId(), () -> {
					if (requiresCooperativeIteratorContinuation(request)) {
						return iterators.cooperativeIteratorMulti(request);
					}
					return advanceIterator(request.getContext(), request.getIterationId(), request.getSkipCount(), 0)
							.thenMany(iteratorChunks(request.getTakeCount(), ITERATOR_VALUE_PAGE_SIZE)
									.concatMap(take -> executeCompositeRead(request.getContext(), OperationFamily.RANGE_PAGE,
											contextualApi -> contextualApi.subsequent(
											request.getIterationId(), 0, take, RequestType.multi())), 1)
									.takeUntil(values -> values.size() < ITERATOR_VALUE_PAGE_SIZE)
									.concatMapIterable(Function.identity(), 1)
									.map(entry -> KV.newBuilder()
											.setValue(Utils.toByteString(entry))
											.build()));
				}))
				.transform(this.onErrorMapFluxWithRequestInfo("subsequentMultiGet", request));
	}

	@Override
	public Mono<FirstAndLast> reduceRangeFirstAndLast(GetRangeRequest request) {
		return Mono.defer(() -> scheduledCalls.fromCancellableFuture(contexts.asyncApi(request.getContext()).reduceRangeAsync(
					request.getTransactionId(),
					request.getColumnId(),
					mapKeys(request.getStartKeysInclusiveList()),
					mapKeys(request.getEndKeysExclusiveList()),
					request.getReverse(),
					RequestType.firstAndLast())))
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
		return Mono.defer(() -> scheduledCalls.fromCancellableFuture(contexts.asyncApi(request.getContext()).reduceRangeAsync(
					request.getTransactionId(),
					request.getColumnId(),
					mapKeys(request.getStartKeysInclusiveList()),
					mapKeys(request.getEndKeysExclusiveList()),
					request.getReverse(),
					RequestType.entriesCount())))
				.map(count -> EntriesCount.newBuilder().setCount(count).build())
				.transform(this.onErrorMapMonoWithRequestInfo("reduceRangeEntriesCount", request));
	}

	@Override
	public Flux<KV> getAllInRange(GetRangeRequest request) {
		return getAllInRange(request, RequestType.allInRange(), "getAllInRange");
	}

	@Override
	public Flux<KV> getAllInRangeNoCache(GetRangeRequest request) {
		return getAllInRange(request, RequestType.allInRangeNoCache(), "getAllInRangeNoCache");
	}

	@Override
	public Mono<it.cavallium.rockserver.core.common.api.proto.RangePage> getRangePage(
			GetRangePageRequest request) {
		return Mono.defer(() -> {
			if (!request.hasBudget()) {
				return Mono.error(RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Range budget is required"));
			}
			final it.cavallium.rockserver.core.common.RangeBudget budget;
			try {
				budget = new it.cavallium.rockserver.core.common.RangeBudget(
						request.getBudget().getMaxItems(), request.getBudget().getMaxBytes());
			} catch (IllegalArgumentException invalid) {
				return Mono.error(RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						invalid.getMessage(), invalid));
			}
			var requestType = switch (request.getRequestTypeValue()) {
				case 1 -> RequestType.<it.cavallium.rockserver.core.common.KV>allInRange();
				case 2 -> RequestType.<it.cavallium.rockserver.core.common.KV>allInRangeNoCache();
				default -> throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
						"Unknown range request type: " + request.getRequestTypeValue());
			};
			var resumeAfter = request.hasResumeAfter()
					? mapKeys(request.getResumeAfter().getKeysList())
					: null;
			return scheduledCalls.fromCancellableFuture(contexts.asyncApi(request.getContext()).getRangePageAsync(
					request.getTransactionId(),
					request.getColumnId(),
					mapKeys(request.getStartKeysInclusiveList()),
					mapKeys(request.getEndKeysExclusiveList()),
					request.getReverse(),
					resumeAfter,
					requestType,
					budget));
		}).map(page -> {
			var response = it.cavallium.rockserver.core.common.api.proto.RangePage.newBuilder()
					.setHasMore(page.hasMore());
			for (var item : page.items()) {
				response.addItems(unmapKVHeap(item));
			}
			if (page.resumeAfter() != null) {
				var resumeAfter = RangeKey.newBuilder();
				for (var key : page.resumeAfter().keys()) {
					resumeAfter.addKeys(Utils.toByteString(key));
				}
				response.setResumeAfter(resumeAfter);
			}
			return response.build();
		}).transform(this.onErrorMapMonoWithRequestInfo("getRangePage", request));
	}

	private Flux<KV> getAllInRange(GetRangeRequest request,
			RequestType.RequestGetRange<? super it.cavallium.rockserver.core.common.KV,
					it.cavallium.rockserver.core.common.KV> requestType,
			String requestName) {
		return Flux.defer(() -> Flux
				.from(contexts.asyncApi(request.getContext()).getRangeAsync(request.getTransactionId(),
						request.getColumnId(),
						mapKeys(request.getStartKeysInclusiveList()),
						mapKeys(request.getEndKeysExclusiveList()),
						request.getReverse(),
						requestType)))
				.map(GrpcProtoMapping::unmapKVHeap)
				.transform(this.onErrorMapFluxWithRequestInfo(requestName, request));
	}

	@Override
	public Flux<ScanRawResponse> scanRaw(ScanRawRequest request) {
		return Flux.defer(() -> {
			var api = contexts.asyncApi(request.getContext());
			if (request.getResumable()) {
				final Set<RawSstToken> completedSsts;
				try {
					completedSsts = request.getCompletedSstTokensList().stream()
							.map(RawSstToken::new)
							.collect(Collectors.toUnmodifiableSet());
				} catch (IllegalArgumentException invalidToken) {
					return Flux.error(Status.INVALID_ARGUMENT
							.withDescription(invalidToken.getMessage())
							.withCause(invalidToken)
							.asRuntimeException());
				}
				var events = Flux.from(api.scanRawResumableAsync(request.getColumnId(),
						request.getShardIndex(), request.getShardCount(), completedSsts));
				// A client built before tagged tokens rejects them: it gets the plain filename, which a replicated
				// server does not skip when it comes back (the file is scanned again), see RawSstToken.
				boolean tagged = request.getAcceptsTaggedSstTokens();
				return events.map(event -> switch (event) {
					case RawScanEvent.Batch batch -> rawScanBatchResponse(
							batch.serialized(), forClient(batch.completedSstToken(), tagged));
					case RawScanEvent.SstCompleted completed ->
							rawScanCompletionResponse(forClient(completed.token(), tagged));
				});
			}
			if (request.getCompletedSstTokensCount() != 0) {
				return Flux.error(Status.INVALID_ARGUMENT
						.withDescription("completed SST tokens require resumable raw scan mode")
						.asRuntimeException());
			}
			// Keep the plain path allocation-neutral: only resumable scans
			// need RawScanEvent wrappers.
			return Flux.from(api.scanRawAsync(
					request.getColumnId(), request.getShardIndex(), request.getShardCount()))
					.map(batch -> rawScanBatchResponse(batch.serialized()));
		})
				.limitRate(17, 1)
				.transform(this.onErrorMapFluxWithRequestInfo("scanRaw", request));
	}

	private static @Nullable RawSstToken forClient(@Nullable RawSstToken token, boolean acceptsTagged) {
		return token == null || acceptsTagged ? token : token.untagged();
	}

	private static ScanRawResponse rawScanBatchResponse(
			Buf serialized) {
		return rawScanBatchResponse(serialized, null);
	}

	private static ScanRawResponse rawScanBatchResponse(
			Buf serialized,
			@Nullable RawSstToken completedSstToken) {
		var serializedBatchValue = UnsafeByteOperations.unsafeWrap(
				serialized.getBackingByteArray(),
				serialized.getBackingByteArrayOffset(),
				serialized.getBackingByteArrayLength());
		var response = ScanRawResponse.newBuilder()
				.setSerialized(serializedBatchValue);
		if (completedSstToken != null) {
			response.setCompletedSstTokenAfterBatch(completedSstToken.value());
		}
		return response.build();
	}

	private static ScanRawResponse rawScanCompletionResponse(
			RawSstToken completedSstToken) {
		return ScanRawResponse.newBuilder()
				.setCompletedSstToken(completedSstToken.value())
				.build();
	}

	@Override
	public Mono<Empty> flush(FlushRequest request) {
		requireV3(request.getWorkloadContractVersion());
		return scheduledCalls.executeScheduled(() -> {
			if (request.getWalOnly()) contexts.protectedApi().flushWal(); else contexts.protectedApi().flush();
			return Empty.getDefaultInstance();
		}, server.workloadScheduler(server.scheduler, WorkloadProfile.PHYSICAL_MAINTENANCE, OperationFamily.FLUSH, Long.MAX_VALUE))
				.transform(this.onErrorMapMonoWithRequestInfo("flush", request));
	}

        @Override
        public Mono<GetSstMetadataResponse> getSstMetadata(GetSstMetadataRequest request) {
            return executeSync(request.getContext(), OperationFamily.METADATA, api -> {
                var metadata = api.getSstMetadata(request.getColumnId(), request.getLevel());
                var builder = GetSstMetadataResponse.newBuilder().setSession(metadata.session())
                        .setColumnId(metadata.columnId()).setColumnName(metadata.columnName())
                        .setNumLevels(metadata.numLevels()).setBaseLevel(metadata.baseLevel())
                        .addAllPaths(metadata.paths());
                for (var file : metadata.files()) {
                    builder.addFiles(SstFileMetadata.newBuilder().setName(file.name()).setLevel(file.level())
                            .setPathId(file.pathId()).setSizeBytes(file.sizeBytes())
                            .setSmallestKeyHex(file.smallestKeyHex()).setLargestKeyHex(file.largestKeyHex())
                            .setBeingCompacted(file.beingCompacted()));
                }
                var response = builder.build();
                if (response.getSerializedSize() > SstMaintenance.MAX_METADATA_RESPONSE_BYTES) {
                    throw SstMaintenance.invalid("SST metadata exceeds 64 MiB; query one level at a time");
                }
                return response;
            })
                    .transform(this.onErrorMapMonoWithRequestInfo("getSstMetadata", request));
        }

        @Override
        public Mono<CompactFilesResponse> compactFiles(CompactFilesRequest request) {
            requireV3(request.getWorkloadContractVersion());
            return Mono.defer(() -> {
                var decoded = new SstMaintenance.Request(request.getColumnId(), request.getSession(),
                        request.getFilesList(), request.getLevel(), request.getOutputPathId(),
                        request.getOutputFileSizeLimit(), request.getMaxInputBytes(), request.getMaxSubcompactions(),
                        request.getExecute());
                return scheduledCalls.executeScheduled(() -> {
                    var result = contexts.protectedApi().compactFiles(decoded);
                    return CompactFilesResponse.newBuilder().setExecuted(result.executed())
                            .addAllInputFiles(result.inputFiles()).addAllOutputFiles(result.outputFiles())
                            .setValidatedInputBytes(result.validatedInputBytes()).setOutputBytes(result.outputBytes())
                            .setElapsedNanos(result.elapsedNanos()).build();
                },
                        server.workloadScheduler(server.columnWorkloadScheduler(request.getColumnId()), WorkloadProfile.PHYSICAL_MAINTENANCE, OperationFamily.COMPACTION, Long.MAX_VALUE));
            }).transform(this.onErrorMapMonoWithRequestInfo("compactFiles", request));
        }

	@Override
	public Mono<Empty> compact(CompactRequest request) {
		requireV3(request.getWorkloadContractVersion());
		return scheduledCalls.executeScheduled(() -> {
			contexts.protectedApi().compact();
			return Empty.getDefaultInstance();
		}, server.workloadScheduler(server.scheduler, WorkloadProfile.PHYSICAL_MAINTENANCE, OperationFamily.COMPACTION, Long.MAX_VALUE))
				.transform(this.onErrorMapMonoWithRequestInfo("compact", request));
	}

	@Override
	public Mono<GetAllColumnDefinitionsResponse> getAllColumnDefinitions(GetAllColumnDefinitionsRequest request) {
		return executeSync(request.getContext(), OperationFamily.METADATA, contextualApi -> {
			var definitions = contextualApi.getAllColumnDefinitions();
			var builder = GetAllColumnDefinitionsResponse.newBuilder();
			for (Entry<String, ColumnSchema> e : definitions.entrySet()) {
				builder.addColumns(Column.newBuilder().setName(e.getKey()).setSchema(unmapColumnSchema(e.getValue())));
			}
 			return builder.build();
		}).transform(this.onErrorMapMonoWithRequestInfo("getAllColumnDefinitions", request));
	}

	@Override
	public Mono<UploadMergeOperatorResponse> uploadMergeOperator(UploadMergeOperatorRequest request) {
		return executeSync(request.getContext(), OperationFamily.MUTATION, contextualApi -> {
			var version = contextualApi.uploadMergeOperator(request.getOperatorName(), request.getClassName(), request.getJarPayload().toByteArray());
			return UploadMergeOperatorResponse.newBuilder().setVersion(version).build();
		}).transform(this.onErrorMapMonoWithRequestInfo("uploadMergeOperator", request));
	}

	@Override
	public Mono<CheckMergeOperatorResponse> checkMergeOperator(CheckMergeOperatorRequest request) {
		return executeSync(request.getContext(), OperationFamily.METADATA, contextualApi -> {
			var version = contextualApi.checkMergeOperator(request.getOperatorName(), request.getHash().toByteArray());
			var builder = CheckMergeOperatorResponse.newBuilder();
			if (version != null) {
				builder.setVersion(version);
			}
			return builder.build();
		}).transform(this.onErrorMapMonoWithRequestInfo("checkMergeOperator", request));
	}

	// utils

	private static void requireV3(int workloadContractVersion) {
		if (workloadContractVersion != RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION) {
			throw RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Request requires workload contract version "
							+ RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION);
		}
	}

	private <T> Mono<T> executeSync(
			it.cavallium.rockserver.core.common.api.proto.RequestContext wireContext,
			OperationFamily family,
			Function<RocksDBSyncAPI, T> operation) {
		return Mono.defer(() -> {
			var context = contexts.mapRequestContext(wireContext);
			var command = contexts.captureCommand(operation);
				var profile = contexts.preAdmit(context, family, command);
				return scheduledCalls.executeScheduled(() -> operation.apply(contexts.syncApi(context)),
						server.workloadScheduler(server.commandWorkloadScheduler(command), profile,
								command.operationFamily(),
								context.localMonotonicDeadlineNanos()),
					command.estimatedBytes());
		});
	}

	private <T> Mono<T> executeSync(
			it.cavallium.rockserver.core.common.api.proto.RequestContext wireContext,
			OperationFamily family,
			Function<RocksDBSyncAPI, T> operation,
			Consumer<T> lateSuccessCleanup,
			Scheduler lateSuccessCleanupScheduler) {
		return Mono.defer(() -> {
			var context = contexts.mapRequestContext(wireContext);
			var command = contexts.captureCommand(operation);
				var profile = contexts.preAdmit(context, family, command);
				return scheduledCalls.executeScheduled(() -> operation.apply(contexts.syncApi(context)),
						server.workloadScheduler(server.commandWorkloadScheduler(command), profile,
								command.operationFamily(),
								context.localMonotonicDeadlineNanos()),
					lateSuccessCleanup,
					lateSuccessCleanupScheduler,
					command.estimatedBytes());
		});
	}

	private <T> Mono<T> executeWrite(
			it.cavallium.rockserver.core.common.api.proto.RequestContext wireContext,
			Function<RocksDBSyncAPI, T> operation) {
		return executeSync(wireContext, OperationFamily.MUTATION, operation);
	}

	private <T> Mono<T> executeCompositeRead(
			it.cavallium.rockserver.core.common.api.proto.RequestContext wireContext,
			OperationFamily family,
			Function<RocksDBSyncAPI, T> operation) {
		return executeSync(wireContext, family, operation);
	}

	private Mono<Void> validateIteratorCounts(SubsequentRequest request) {
		if (request.getSkipCount() < 0 || request.getTakeCount() < 0) {
			return Mono.error(RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
					"Iterator skip and take counts must be non-negative"));
		}
		return Mono.empty();
	}

	private Mono<Void> validateSubsequentCommand(SubsequentRequest request,
			RequestType.RequestIterate<? super Buf, ?> requestType) {
		return Mono.fromRunnable(() -> {
			var context = contexts.mapRequestContext(request.getContext());
			var command = new RocksDBAPICommand.RocksDBAPICommandSingle.Subsequent<>(
					request.getIterationId(),
					request.getSkipCount(),
					request.getTakeCount(),
					requestType);
			contexts.preAdmit(context, OperationFamily.RANGE_PAGE, command);
		});
	}

	private boolean requiresCooperativeIteratorContinuation(SubsequentRequest request) {
		long skipCount = request.getSkipCount();
		long takeCount = request.getTakeCount();
		return contexts.mapRequestContext(request.getContext()).profile() != WorkloadProfile.LATENCY
				&& (skipCount > 1L
						|| takeCount > 1L
						|| (skipCount != 0L && takeCount != 0L));
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

	private Mono<Void> advanceIterator(
			it.cavallium.rockserver.core.common.api.proto.RequestContext context,
			long iteratorId,
			long skipCount,
			long takeCount) {
		return advanceIteratorPart(context, iteratorId, skipCount)
				.then(advanceIteratorPart(context, iteratorId, takeCount));
	}

	private Mono<Void> advanceIteratorPart(
			it.cavallium.rockserver.core.common.api.proto.RequestContext context,
			long iteratorId,
			long count) {
		return iteratorChunks(count, ITERATOR_ADVANCE_STEP_SIZE)
				.concatMap(step -> executeCompositeRead(context, OperationFamily.RANGE_PAGE,
						contextualApi -> contextualApi.subsequent(
								iteratorId, 0, step, RequestType.exists())), 1)
				.takeUntil(found -> !found)
				.then();
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

	Throwable mapRequestError(String requestName, Message request, Throwable throwable) {
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

	@Override
	protected Throwable onErrorMap(Throwable throwable) {
		var ex = handleError(throwable).asException();
		if (ex.getStatus().getCode() == Code.INTERNAL && findRocksDBException(throwable) == null) {
			LOG.error("Unexpected internal error during request", ex);
		}
		return ex;
	}

}
