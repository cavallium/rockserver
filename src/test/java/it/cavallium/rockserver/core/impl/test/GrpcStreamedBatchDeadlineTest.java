package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.Utils.toBuf;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.KVBatch;
import it.cavallium.rockserver.core.common.api.proto.MergeBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.MergeBatchMode;
import it.cavallium.rockserver.core.common.api.proto.MergeBatchRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchMode;
import it.cavallium.rockserver.core.common.api.proto.PutBatchRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc.ReactorRocksDBServiceStub;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

/**
 * Streamed putBatch and mergeBatch on an embedded server wait in the column's MUTATION queue. One that waits there
 * past its request-context timeout must end with an error, even when the client set no gRPC deadline. One whose
 * timeout passes during its final write, or during the write of its last batch, must report that write's own
 * outcome. One whose timeout passes between two batches ends before writing the next one.
 */
@Timeout(60)
class GrpcStreamedBatchDeadlineTest {

	private static final long CONTEXT_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(1);

	@TempDir
	Path tempDir;

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	void streamedBatchQueuedPastItsContextTimeoutEndsWithDeadlineExceeded(boolean merge) throws Exception {
		Path config = tempDir.resolve("rockserver.conf");
		Files.writeString(config, """
				database: {
				  parallelism: {
				    read: 3
				    write: 3
				    workload: { competing-batch-read-maximum-active: 3 }
				  }
				  global: { ingest-behind: false, optimistic: false }
				}
				""");
		var embedded = new EmbeddedConnection(tempDir.resolve("db"), "grpc-streamed-batch-deadline", config);
		var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
		var releaseWrites = new CountDownLatch(1);
		ManagedChannel channel = null;
		try {
			server.start();
			long columnId = embedded.getSyncApi(RequestContext.batch()).createColumn("streamed-batch",
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			RWScheduler scheduler = embedded.getEmbeddedDB().getSchedulerForColumn(columnId);
			int writeWorkers = scheduler.poolSnapshot(RWScheduler.Pool.WRITE).workerCount();
			var writesStarted = new CountDownLatch(writeWorkers);
			for (int index = 0; index < writeWorkers; index++) {
				scheduler.executor(WorkloadProfile.INGEST, OperationFamily.MUTATION, Long.MAX_VALUE).execute(() -> {
					writesStarted.countDown();
					awaitUninterruptibly(releaseWrites);
				});
			}
			assertTrue(writesStarted.await(5, TimeUnit.SECONDS));
			var before = scheduler.poolSnapshot(RWScheduler.Pool.WRITE);

			channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
			// No gRPC deadline, only a request-context timeout
			var stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
			var call = (merge ? mergeBatch(stub, columnId) : putBatch(stub, columnId)).toFuture();
			await(() -> scheduler.poolSnapshot(RWScheduler.Pool.WRITE).queuedTasks() > 0,
					"the batch did not reach the saturated WRITE queue");

			var failure = assertThrows(ExecutionException.class, () -> call.get(15, TimeUnit.SECONDS));
			var status = assertInstanceOf(StatusRuntimeException.class, failure.getCause()).getStatus();
			assertEquals(Status.Code.DEADLINE_EXCEEDED, status.getCode(), status.toString());
			// The expired batch is removed from the queue without running
			await(() -> scheduler.poolSnapshot(RWScheduler.Pool.WRITE).queuedTasks() == 0,
					"the expired batch stayed queued");
			assertEquals(before.startedTasks(), scheduler.poolSnapshot(RWScheduler.Pool.WRITE).startedTasks(),
					"the expired batch must not run");

			releaseWrites.countDown();
			await(() -> scheduler.poolSnapshot(RWScheduler.Pool.WRITE).drainedAndConserved(),
					"WRITE pool did not drain and conserve terminal outcomes");
		} finally {
			releaseWrites.countDown();
			if (channel != null) {
				channel.shutdownNow();
				channel.awaitTermination(5, TimeUnit.SECONDS);
			}
			server.close();
			embedded.closeTesting();
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {"PUT_WRITE_BATCH", "PUT_SST_INGESTION", "MERGE_WRITE_BATCH", "MERGE_SST_INGESTION"})
	void streamedBatchWhoseContextTimeoutPassesDuringItsFinalWriteReportsSuccess(String operation) throws Exception {
		Path config = tempDir.resolve("rockserver.conf");
		Files.writeString(config, """
				database: {
				  global: {
				    ingest-behind: false
				    optimistic: false
				    fallback-column-options: {
				      merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
				    }
				  }
				}
				""");
		var embedded = new EmbeddedConnection(tempDir.resolve("db"), "grpc-streamed-batch-final-write", config);
		var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
		ManagedChannel channel = null;
		try {
			server.start();
			long columnId = embedded.getSyncApi(RequestContext.batch()).createColumn("streamed-batch",
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			var finalWrites = new AtomicInteger();
			// The final write (commit or SST ingestion) outlives the request-context timeout
			embedded.getEmbeddedDB().setBatchFinalWriteObserverForTesting(() -> {
				finalWrites.incrementAndGet();
				sleepUninterruptibly(TimeUnit.NANOSECONDS.toMillis(CONTEXT_TIMEOUT_NANOS) + 500L);
			});

			channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
			var stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
			var call = switch (operation) {
				case "PUT_WRITE_BATCH" -> putBatch(stub, columnId, PutBatchMode.WRITE_BATCH);
				case "PUT_SST_INGESTION" -> putBatch(stub, columnId, PutBatchMode.SST_INGESTION);
				case "MERGE_WRITE_BATCH" -> mergeBatch(stub, columnId, MergeBatchMode.MERGE_WRITE_BATCH);
				case "MERGE_SST_INGESTION" -> mergeBatch(stub, columnId, MergeBatchMode.MERGE_SST_INGESTION);
				default -> throw new IllegalArgumentException(operation);
			};

			call.block(Duration.ofSeconds(15));
			assertEquals(1, finalWrites.get());
			assertEquals(toBuf(new byte[] {1}), embedded.getSyncApi(RequestContext.batch())
					.get(0, columnId, new Keys(toBuf(new byte[Long.BYTES])), RequestType.current()));
		} finally {
			embedded.getEmbeddedDB().setBatchFinalWriteObserverForTesting(null);
			if (channel != null) {
				channel.shutdownNow();
				channel.awaitTermination(5, TimeUnit.SECONDS);
			}
			server.close();
			embedded.closeTesting();
		}
	}

	@ParameterizedTest
	@ValueSource(strings = {"PUT_WRITE_BATCH", "MERGE_WRITE_BATCH", "MERGE_WRITE_BATCH_NO_WAL"})
	void streamedBatchWhoseContextTimeoutPassesDuringItsLastBatchWriteReportsSuccess(String operation)
			throws Exception {
		var embedded = mergeConnection("grpc-streamed-batch-last-write");
		var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
		ManagedChannel channel = null;
		try {
			server.start();
			long columnId = embedded.getSyncApi(RequestContext.batch()).createColumn("streamed-batch",
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			var batchWrites = new AtomicInteger();
			// The write of the last batch outlives the request-context timeout. A merge write batch of a column
			// without buckets applies the batch in that write, leaving nothing for the final write.
			embedded.getEmbeddedDB().setBatchWriteObserverForTesting(() -> {
				batchWrites.incrementAndGet();
				sleepUninterruptibly(TimeUnit.NANOSECONDS.toMillis(CONTEXT_TIMEOUT_NANOS) + 500L);
			});

			channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
			var stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
			var call = switch (operation) {
				case "PUT_WRITE_BATCH" -> putBatch(stub, columnId, PutBatchMode.WRITE_BATCH);
				case "MERGE_WRITE_BATCH" -> mergeBatch(stub, columnId, MergeBatchMode.MERGE_WRITE_BATCH);
				case "MERGE_WRITE_BATCH_NO_WAL" -> mergeBatch(stub, columnId, MergeBatchMode.MERGE_WRITE_BATCH_NO_WAL);
				default -> throw new IllegalArgumentException(operation);
			};

			call.block(Duration.ofSeconds(15));
			assertEquals(1, batchWrites.get());
			assertEquals(toBuf(new byte[] {1}), get(embedded, columnId, 0));
		} finally {
			embedded.getEmbeddedDB().setBatchWriteObserverForTesting(null);
			if (channel != null) {
				channel.shutdownNow();
				channel.awaitTermination(5, TimeUnit.SECONDS);
			}
			server.close();
			embedded.closeTesting();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	void streamedBatchWhoseContextTimeoutPassesBetweenBatchesEndsBeforeTheNextBatch(boolean merge) throws Exception {
		var embedded = mergeConnection("grpc-streamed-batch-between-batches");
		var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
		ManagedChannel channel = null;
		try {
			server.start();
			long columnId = embedded.getSyncApi(RequestContext.batch()).createColumn("streamed-batch",
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			var batchWrites = new AtomicInteger();
			embedded.getEmbeddedDB().setBatchWriteObserverForTesting(batchWrites::incrementAndGet);

			channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
			var stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
			// The second batch comes after the request-context timeout
			var secondBatchDelay = Duration.ofNanos(CONTEXT_TIMEOUT_NANOS).plusMillis(700L);
			var call = (merge
					? stub.mergeBatch(Flux.concat(Flux.just(mergeInitial(columnId, MergeBatchMode.MERGE_WRITE_BATCH),
									MergeBatchRequest.newBuilder().setData(data(0, 1)).build()),
							Mono.delay(secondBatchDelay)
									.map(_ -> MergeBatchRequest.newBuilder().setData(data(1, 2)).build())))
					: stub.putBatch(Flux.concat(Flux.just(putInitial(columnId, PutBatchMode.WRITE_BATCH),
									PutBatchRequest.newBuilder().setData(data(0, 1)).build()),
							Mono.delay(secondBatchDelay)
									.map(_ -> PutBatchRequest.newBuilder().setData(data(1, 2)).build()))))
					.toFuture();

			var failure = assertThrows(ExecutionException.class, () -> call.get(15, TimeUnit.SECONDS));
			var status = assertInstanceOf(StatusRuntimeException.class, failure.getCause()).getStatus();
			assertEquals(Status.Code.DEADLINE_EXCEEDED, status.getCode(), status.toString());
			// Give a late second batch the time to arrive, to prove it is never written
			sleepUninterruptibly(secondBatchDelay.toMillis());
			assertEquals(1, batchWrites.get(), "the batch after the deadline must not be written");
			assertNull(get(embedded, columnId, 1));
			// A merge write batch applies each batch when it is written, so the first batch stays applied and the
			// deadline error is not safe to retry blindly. A put write batch applies nothing before its final write.
			assertEquals(merge ? toBuf(new byte[] {1}) : null, get(embedded, columnId, 0));
		} finally {
			embedded.getEmbeddedDB().setBatchWriteObserverForTesting(null);
			if (channel != null) {
				channel.shutdownNow();
				channel.awaitTermination(5, TimeUnit.SECONDS);
			}
			server.close();
			embedded.closeTesting();
		}
	}

	@ParameterizedTest
	@ValueSource(booleans = {false, true})
	void streamedBatchWhoseEndExpiresInTheQueueReportsWhetherItsBatchesWereApplied(boolean merge) throws Exception {
		Path config = tempDir.resolve("rockserver.conf");
		Files.writeString(config, """
				database: {
				  parallelism: {
				    read: 3
				    write: 3
				    workload: { competing-batch-read-maximum-active: 3 }
				  }
				  global: {
				    ingest-behind: false
				    optimistic: false
				    fallback-column-options: {
				      merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
				    }
				  }
				}
				""");
		var embedded = new EmbeddedConnection(tempDir.resolve("db"), "grpc-streamed-batch-expired-end", config);
		var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
		var releaseWrites = new CountDownLatch(1);
		ManagedChannel channel = null;
		try {
			server.start();
			long columnId = embedded.getSyncApi(RequestContext.batch()).createColumn("streamed-batch",
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			RWScheduler scheduler = embedded.getEmbeddedDB().getSchedulerForColumn(columnId);
			int writeWorkers = scheduler.poolSnapshot(RWScheduler.Pool.WRITE).workerCount();
			var batchWritten = new CountDownLatch(1);
			embedded.getEmbeddedDB().setBatchWriteObserverForTesting(batchWritten::countDown);
			var writesStarted = new CountDownLatch(writeWorkers);
			// Once the only batch was written, every MUTATION worker is held, so the drain that delivers the end of
			// the client stream waits in the queue past the request-context timeout and expires there
			var blockWorkers = Mono.<Void>fromRunnable(() -> {
				awaitUninterruptibly(batchWritten);
				for (int index = 0; index < writeWorkers; index++) {
					scheduler.executor(WorkloadProfile.INGEST, OperationFamily.MUTATION, Long.MAX_VALUE).execute(() -> {
						writesStarted.countDown();
						awaitUninterruptibly(releaseWrites);
					});
				}
				awaitUninterruptibly(writesStarted);
			}).subscribeOn(Schedulers.boundedElastic());

			channel = NettyChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
			var stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
			var call = (merge
					? stub.mergeBatch(Flux.concat(Flux.just(mergeInitial(columnId, MergeBatchMode.MERGE_WRITE_BATCH),
									MergeBatchRequest.newBuilder().setData(data(0, 1)).build()),
							blockWorkers.then(Mono.<MergeBatchRequest>empty())))
					: stub.putBatch(Flux.concat(Flux.just(putInitial(columnId, PutBatchMode.WRITE_BATCH),
									PutBatchRequest.newBuilder().setData(data(0, 1)).build()),
							blockWorkers.then(Mono.<PutBatchRequest>empty()))))
					.toFuture();

			assertTrue(writesStarted.await(5, TimeUnit.SECONDS));
			await(() -> scheduler.poolSnapshot(RWScheduler.Pool.WRITE).queuedTasks() > 0,
					"the end of the stream did not reach the saturated WRITE queue");
			sleepUninterruptibly(TimeUnit.NANOSECONDS.toMillis(CONTEXT_TIMEOUT_NANOS) + 300L);
			releaseWrites.countDown();

			if (merge) {
				// The merge was applied when its batch was written: the expired end does not undo it
				call.get(15, TimeUnit.SECONDS);
				assertEquals(toBuf(new byte[] {1}), get(embedded, columnId, 0));
			} else {
				// The put write batch was still waiting for its final write, which never ran
				var failure = assertThrows(ExecutionException.class, () -> call.get(15, TimeUnit.SECONDS));
				var status = assertInstanceOf(StatusRuntimeException.class, failure.getCause()).getStatus();
				assertEquals(Status.Code.DEADLINE_EXCEEDED, status.getCode(), status.toString());
				assertNull(get(embedded, columnId, 0));
			}
			await(() -> scheduler.poolSnapshot(RWScheduler.Pool.WRITE).drainedAndConserved(),
					"WRITE pool did not drain and conserve terminal outcomes");
		} finally {
			releaseWrites.countDown();
			embedded.getEmbeddedDB().setBatchWriteObserverForTesting(null);
			if (channel != null) {
				channel.shutdownNow();
				channel.awaitTermination(5, TimeUnit.SECONDS);
			}
			server.close();
			embedded.closeTesting();
		}
	}

	private EmbeddedConnection mergeConnection(String name) throws Exception {
		Path config = tempDir.resolve("rockserver.conf");
		Files.writeString(config, """
				database: {
				  global: {
				    ingest-behind: false
				    optimistic: false
				    fallback-column-options: {
				      merge-operator-class: "it.cavallium.rockserver.core.impl.MyStringAppendOperator"
				    }
				  }
				}
				""");
		return new EmbeddedConnection(tempDir.resolve("db"), name, config);
	}

	private static Buf get(EmbeddedConnection embedded, long columnId, int key) {
		return embedded.getSyncApi(RequestContext.batch())
				.get(0, columnId, new Keys(toBuf(key(key))), RequestType.current());
	}

	private static byte[] key(int key) {
		var bytes = new byte[Long.BYTES];
		bytes[Long.BYTES - 1] = (byte) key;
		return bytes;
	}

	private static KVBatch data(int key, int value) {
		return KVBatch.newBuilder()
				.addEntries(KV.newBuilder()
						.addKeys(ByteString.copyFrom(key(key)))
						.setValue(ByteString.copyFrom(new byte[] {(byte) value})))
				.build();
	}

	private static PutBatchRequest putInitial(long columnId, PutBatchMode mode) {
		return PutBatchRequest.newBuilder()
				.setInitialRequest(PutBatchInitialRequest.newBuilder()
						.setColumnId(columnId)
						.setMode(mode)
						.setContext(context()))
				.build();
	}

	private static MergeBatchRequest mergeInitial(long columnId, MergeBatchMode mode) {
		return MergeBatchRequest.newBuilder()
				.setInitialRequest(MergeBatchInitialRequest.newBuilder()
						.setColumnId(columnId)
						.setMode(mode)
						.setContext(context()))
				.build();
	}

	private static it.cavallium.rockserver.core.common.api.proto.RequestContext context() {
		return it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
				.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.INGEST)
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.setTimeoutNanos(CONTEXT_TIMEOUT_NANOS)
				.build();
	}

	private static KVBatch data() {
		return KVBatch.newBuilder()
				.addEntries(KV.newBuilder()
						.addKeys(ByteString.copyFrom(new byte[Long.BYTES]))
						.setValue(ByteString.copyFrom(new byte[] {1})))
				.build();
	}

	private static Mono<Empty> putBatch(ReactorRocksDBServiceStub stub, long columnId) {
		return putBatch(stub, columnId, PutBatchMode.WRITE_BATCH);
	}

	private static Mono<Empty> putBatch(ReactorRocksDBServiceStub stub, long columnId, PutBatchMode mode) {
		return stub.putBatch(Flux.just(PutBatchRequest.newBuilder()
						.setInitialRequest(PutBatchInitialRequest.newBuilder()
								.setColumnId(columnId)
								.setMode(mode)
								.setContext(context()))
						.build(),
				PutBatchRequest.newBuilder().setData(data()).build()));
	}

	private static Mono<Empty> mergeBatch(ReactorRocksDBServiceStub stub, long columnId) {
		return mergeBatch(stub, columnId, MergeBatchMode.MERGE_WRITE_BATCH);
	}

	private static Mono<Empty> mergeBatch(ReactorRocksDBServiceStub stub, long columnId, MergeBatchMode mode) {
		return stub.mergeBatch(Flux.just(MergeBatchRequest.newBuilder()
						.setInitialRequest(MergeBatchInitialRequest.newBuilder()
								.setColumnId(columnId)
								.setMode(mode)
								.setContext(context()))
						.build(),
				MergeBatchRequest.newBuilder().setData(data()).build()));
	}

	private static void await(BooleanSupplier condition, String failure) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5L);
		do {
			if (condition.getAsBoolean()) return;
			Thread.sleep(10L);
		} while (System.nanoTime() < deadline);
		throw new AssertionError(failure);
	}

	private static void sleepUninterruptibly(long millis) {
		long end = System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(millis);
		boolean interrupted = false;
		long remaining;
		while ((remaining = end - System.nanoTime()) > 0L) {
			try {
				Thread.sleep(Math.max(1L, remaining / 1_000_000L));
			} catch (InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) Thread.currentThread().interrupt();
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) Thread.currentThread().interrupt();
	}
}
