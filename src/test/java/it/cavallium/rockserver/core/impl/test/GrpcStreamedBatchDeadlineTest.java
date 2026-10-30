package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import io.grpc.netty.NettyChannelBuilder;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
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
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

/**
 * Streamed putBatch and mergeBatch on an embedded server wait in the column's MUTATION queue. One that waits there
 * past its request-context timeout must end with an error, even when the client set no gRPC deadline.
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
		return stub.putBatch(Flux.just(PutBatchRequest.newBuilder()
						.setInitialRequest(PutBatchInitialRequest.newBuilder()
								.setColumnId(columnId)
								.setMode(PutBatchMode.WRITE_BATCH)
								.setContext(context()))
						.build(),
				PutBatchRequest.newBuilder().setData(data()).build()));
	}

	private static Mono<Empty> mergeBatch(ReactorRocksDBServiceStub stub, long columnId) {
		return stub.mergeBatch(Flux.just(MergeBatchRequest.newBuilder()
						.setInitialRequest(MergeBatchInitialRequest.newBuilder()
								.setColumnId(columnId)
								.setMode(MergeBatchMode.MERGE_WRITE_BATCH)
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
