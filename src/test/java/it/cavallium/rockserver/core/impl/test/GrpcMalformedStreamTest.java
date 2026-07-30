package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.proto.DeleteMultiInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.DeleteMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.DeleteRequest;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.KVBatch;
import it.cavallium.rockserver.core.common.api.proto.MergeBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.MergeBatchMode;
import it.cavallium.rockserver.core.common.api.proto.MergeBatchRequest;
import it.cavallium.rockserver.core.common.api.proto.MergeMultiInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.MergeMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutBatchMode;
import it.cavallium.rockserver.core.common.api.proto.PutBatchRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiListRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.RequestContext;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.WorkloadProfile;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.concurrent.TimeUnit;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

@Timeout(60)
class GrpcMalformedStreamTest {

	private Path dbDir;
	private Path configFile;
	private EmbeddedConnection embeddedConnection;
	private GrpcServer grpcServer;
	private ManagedChannel channel;
	private ReactorRocksDBServiceGrpc.ReactorRocksDBServiceStub stub;
	private long columnId;

	@BeforeEach
	void setUp() throws IOException {
		dbDir = Files.createTempDirectory("rockserver-grpc-malformed-stream");
		configFile = Files.createTempFile("rockserver-grpc-malformed-stream", ".conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		embeddedConnection = new EmbeddedConnection(dbDir, "grpc-malformed-stream", configFile);
		columnId = embeddedConnection.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch()).createColumn("stream-column",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		grpcServer = new GrpcServer(embeddedConnection, new InetSocketAddress("127.0.0.1", 0));
		grpcServer.start();
		channel = ManagedChannelBuilder.forAddress("127.0.0.1", grpcServer.getPort())
				.usePlaintext()
				.build();
		stub = ReactorRocksDBServiceGrpc.newReactorStub(channel);
	}

	@AfterEach
	void tearDown() throws IOException, InterruptedException {
		if (channel != null) {
			channel.shutdownNow();
			channel.awaitTermination(5, TimeUnit.SECONDS);
		}
		if (grpcServer != null) {
			grpcServer.close();
		}
		if (embeddedConnection != null) {
			embeddedConnection.closeTesting();
		}
		if (dbDir != null) {
			Utils.deleteDirectory(dbDir.toString());
		}
		if (configFile != null) {
			Files.deleteIfExists(configFile);
		}
	}

	@Test
	void emptyClientStreamsAreRejectedByEveryStreamingMutation() {
		assertInvalidRequest(stub.putBatch(Flux.empty()));
		assertInvalidRequest(stub.mergeBatch(Flux.empty()));
		assertInvalidRequest(stub.putMulti(Flux.empty()));
		assertInvalidRequest(stub.putMultiEnsure(Flux.empty()));
		assertInvalidRequest(stub.deleteMulti(Flux.empty()));
		assertInvalidRequest(stub.mergeMulti(Flux.empty()));
	}

	@Test
	void dataBeforeInitialRequestIsRejectedByEveryStreamingMutation() {
		assertInvalidRequest(stub.putBatch(Flux.just(PutBatchRequest.newBuilder()
				.setData(KVBatch.getDefaultInstance()).build())));
		assertInvalidRequest(stub.mergeBatch(Flux.just(MergeBatchRequest.newBuilder()
				.setData(KVBatch.getDefaultInstance()).build())));
		assertInvalidRequest(stub.putMulti(Flux.just(PutMultiRequest.newBuilder()
				.setData(KV.getDefaultInstance()).build())));
		assertInvalidRequest(stub.putMultiEnsure(Flux.just(PutMultiRequest.newBuilder()
				.setData(KV.getDefaultInstance()).build())));
		assertInvalidRequest(stub.deleteMulti(Flux.just(DeleteMultiRequest.newBuilder()
				.setData(DeleteRequest.getDefaultInstance()).build())));
		assertInvalidRequest(stub.mergeMulti(Flux.just(MergeMultiRequest.newBuilder()
				.setData(KV.getDefaultInstance()).build())));
	}

	@Test
	void aSecondInitialRequestIsRejectedByEveryStreamingMutation() {
		var putBatchInitial = PutBatchRequest.newBuilder().setInitialRequest(PutBatchInitialRequest.newBuilder()
				.setColumnId(columnId).setMode(PutBatchMode.WRITE_BATCH)).build();
		assertInvalidRequest(stub.putBatch(Flux.just(putBatchInitial, putBatchInitial)));

		var mergeBatchInitial = MergeBatchRequest.newBuilder().setInitialRequest(MergeBatchInitialRequest.newBuilder()
				.setColumnId(columnId).setMode(MergeBatchMode.MERGE_WRITE_BATCH)).build();
		assertInvalidRequest(stub.mergeBatch(Flux.just(mergeBatchInitial, mergeBatchInitial)));

		var putMultiInitial = PutMultiRequest.newBuilder().setInitialRequest(PutMultiInitialRequest.newBuilder()
				.setColumnId(columnId)).build();
		assertInvalidRequest(stub.putMulti(Flux.just(putMultiInitial, putMultiInitial)));
		assertInvalidRequest(stub.putMultiEnsure(Flux.just(putMultiInitial, putMultiInitial)));

		var deleteMultiInitial = DeleteMultiRequest.newBuilder().setInitialRequest(DeleteMultiInitialRequest.newBuilder()
				.setColumnId(columnId)).build();
		assertInvalidRequest(stub.deleteMulti(Flux.just(deleteMultiInitial, deleteMultiInitial)));

		var mergeMultiInitial = MergeMultiRequest.newBuilder().setInitialRequest(MergeMultiInitialRequest.newBuilder()
				.setColumnId(columnId)).build();
		assertInvalidRequest(stub.mergeMulti(Flux.just(mergeMultiInitial, mergeMultiInitial)));
	}

	@Test
	void responseStreamingMutationsRejectAnEmptyStream() {
		assertInvalidRequest(stub.putMultiGetPrevious(Flux.empty()).then());
		assertInvalidRequest(stub.putMultiGetDelta(Flux.empty()).then());
		assertInvalidRequest(stub.putMultiGetChanged(Flux.empty()).then());
		assertInvalidRequest(stub.putMultiGetPreviousPresence(Flux.empty()).then());
		assertInvalidRequest(stub.deleteMultiGetPrevious(Flux.empty()).then());
		assertInvalidRequest(stub.deleteMultiGetPreviousPresence(Flux.empty()).then());
		assertInvalidRequest(stub.mergeMultiGetMerged(Flux.empty()).then());
	}

	@Test
	void ensureListRouteWritesEveryValue() {
		var context = RequestContext.newBuilder()
				.setProfile(WorkloadProfile.BATCH)
				.setDeadlineEpochMillis(Long.MAX_VALUE)
				.build();
		var firstKey = ByteString.copyFrom(new byte[Long.BYTES]);
		var secondKey = ByteString.copyFrom(new byte[]{0, 0, 0, 0, 0, 0, 0, 1});
		var firstValue = ByteString.copyFromUtf8("first");
		var secondValue = ByteString.copyFromUtf8("second");
		var request = PutMultiListRequest.newBuilder()
				.setInitialRequest(PutMultiInitialRequest.newBuilder()
						.setColumnId(columnId)
						.setContext(context))
				.addData(KV.newBuilder().addKeys(firstKey).setValue(firstValue))
				.addData(KV.newBuilder().addKeys(secondKey).setValue(secondValue))
				.build();

		stub.putMultiListEnsure(request).block(Duration.ofSeconds(10));
		stub.putMultiListEnsure(request).block(Duration.ofSeconds(10));

		var api = embeddedConnection.getSyncApi(it.cavallium.rockserver.core.common.RequestContext.batch());
		assertEquals("first", new String(api.get(0,
				columnId,
				new Keys(Buf.wrap(firstKey.toByteArray())),
				RequestType.current()).toByteArray(), StandardCharsets.UTF_8));
		assertEquals("second", new String(api.get(0,
				columnId,
				new Keys(Buf.wrap(secondKey.toByteArray())),
				RequestType.current()).toByteArray(), StandardCharsets.UTF_8));
	}

	private static void assertInvalidRequest(Mono<?> response) {
		var error = assertThrows(StatusRuntimeException.class,
				() -> response.block(Duration.ofSeconds(10)));
		assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode());
		assertTrue(error.getStatus().getDescription().contains("PUT_INVALID_REQUEST"),
				() -> "Unexpected error description: " + error.getStatus().getDescription());
	}
}
