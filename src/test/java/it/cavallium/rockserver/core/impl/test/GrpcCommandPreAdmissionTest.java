package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.api.proto.DeleteRangeRequest;
import it.cavallium.rockserver.core.common.api.proto.GetRangeRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchMode;
import it.cavallium.rockserver.core.common.api.proto.PutBatchRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.Disposable;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Timeout(30)
class GrpcCommandPreAdmissionTest {

	@Test
	void invalidUnaryRangeAndBatchCommandsNeverEnterTheSchedulerQueue() throws Exception {
		Path root = Files.createTempDirectory("rockserver-grpc-pre-admission");
		Path config = Files.createTempFile("rockserver-grpc-pre-admission", ".conf");
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
		EmbeddedConnection embedded = null;
		GrpcServer server = null;
		ManagedChannel channel = null;
		var blockers = new ArrayList<Disposable>();
		var release = new CountDownLatch(1);
		try {
			embedded = new EmbeddedConnection(root, "grpc-pre-admission", config);
			server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
			server.start();
			channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
					.usePlaintext()
					.build();

			var scheduler = embedded.getScheduler();
			var entered = new CountDownLatch(3);
			for (int worker = 0; worker < 3; worker++) {
				blockers.add(scheduler.scheduler(WorkloadProfile.LATENCY,
						OperationFamily.MUTATION,
						Long.MAX_VALUE).schedule(() -> {
					entered.countDown();
					try {
						release.await();
					} catch (InterruptedException interrupted) {
						Thread.currentThread().interrupt();
					}
				}));
			}
			assertTrue(entered.await(5, TimeUnit.SECONDS), "write workers did not become occupied");
			assertEquals(0, scheduler.queuedTasks(WorkloadProfile.LATENCY));

			var stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
			var context = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
					.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.LATENCY)
					.setDeadlineEpochMillis(System.currentTimeMillis() + Duration.ofSeconds(10).toMillis())
					.build();

			assertInvalid(stub.deleteRange(DeleteRangeRequest.newBuilder()
					.setColumnId(1)
					.setContext(context)
					.build()));
			assertEquals(0, scheduler.queuedTasks(WorkloadProfile.LATENCY),
					"invalid unary command entered the LATENCY queue");

			assertInvalid(stub.getAllInRange(GetRangeRequest.newBuilder()
					.setColumnId(1)
					.setTimeoutMs(1_000)
					.setContext(context)
					.build()).then());
			assertEquals(0, scheduler.queuedTasks(WorkloadProfile.LATENCY),
					"invalid server-streaming range entered the LATENCY queue");

			var initial = PutBatchRequest.newBuilder()
					.setInitialRequest(PutBatchInitialRequest.newBuilder()
							.setColumnId(1)
							.setMode(PutBatchMode.WRITE_BATCH)
							.setContext(context))
					.build();
			assertInvalid(stub.putBatch(Flux.just(initial)));
			assertEquals(0, scheduler.queuedTasks(WorkloadProfile.LATENCY),
					"invalid client-streaming batch entered the LATENCY queue");
		} finally {
			release.countDown();
			for (Disposable blocker : blockers) {
				blocker.dispose();
			}
			if (channel != null) {
				channel.shutdownNow();
				channel.awaitTermination(5, TimeUnit.SECONDS);
			}
			if (server != null) {
				server.close();
			}
			if (embedded != null) {
				embedded.closeTesting();
			}
			Utils.deleteDirectory(root.toString());
			Files.deleteIfExists(config);
		}
	}

	private static void assertInvalid(Mono<?> response) {
		var error = assertThrows(StatusRuntimeException.class,
				() -> response.block(Duration.ofSeconds(5)));
		assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
		assertTrue(error.getStatus().getDescription().contains("PUT_INVALID_REQUEST"),
				() -> "Unexpected error description: " + error.getStatus().getDescription());
	}
}
