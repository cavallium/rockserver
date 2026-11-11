package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.ManagedChannel;
import io.grpc.ManagedChannelBuilder;
import io.grpc.Metadata;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.api.proto.DeleteMultiInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.DeleteMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.DeleteRequest;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.MergeMultiInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.MergeMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiInitialRequest;
import it.cavallium.rockserver.core.common.api.proto.PutMultiRequest;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc.ReactorRocksDBServiceStub;
import it.cavallium.rockserver.core.common.api.proto.RequestContext;
import it.cavallium.rockserver.core.common.api.proto.WorkloadProfile;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.net.URI;
import java.time.Duration;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.reactivestreams.Publisher;
import reactor.core.publisher.Flux;

/**
 * A streamed multi write without a request id, outside LATENCY, is applied item by item. Once an item was applied, a
 * later item's failure flagged "not applied" is true for that item only: the call must not end with the
 * {@code rockserver-applied: no} trailer, which would tell its caller that the whole stream can be sent again (R61-1).
 */
@Timeout(60)
class StreamedMultiPartlyAppliedTest {

	private static final Metadata.Key<String> APPLIED = Metadata.Key.of(RocksDBException.APPLIED_METADATA_KEY,
			Metadata.ASCII_STRING_MARSHALLER);

	private static RequestContext context(long timeoutNanos) {
		return RequestContext.newBuilder()
				.setProfile(WorkloadProfile.BATCH)
				.setWorkloadContractVersion(3)
				.setTimeoutNanos(timeoutNanos)
				.build();
	}

	private static KV kv(int key) {
		return KV.newBuilder()
				.addKeys(ByteString.copyFrom(new byte[] {(byte) key}))
				.setValue(ByteString.copyFrom(new byte[] {7}))
				.build();
	}

	private static Flux<PutMultiRequest> puts(long timeoutNanos, int items) {
		var requests = new ArrayList<PutMultiRequest>();
		requests.add(PutMultiRequest.newBuilder()
				.setInitialRequest(PutMultiInitialRequest.newBuilder().setColumnId(1L).setContext(context(timeoutNanos)))
				.build());
		for (int i = 0; i < items; i++) {
			requests.add(PutMultiRequest.newBuilder().setData(kv(i)).build());
		}
		return Flux.fromIterable(requests);
	}

	private static Flux<MergeMultiRequest> merges(int items) {
		var requests = new ArrayList<MergeMultiRequest>();
		requests.add(MergeMultiRequest.newBuilder()
				.setInitialRequest(MergeMultiInitialRequest.newBuilder().setColumnId(1L).setContext(context(Long.MAX_VALUE)))
				.build());
		for (int i = 0; i < items; i++) {
			requests.add(MergeMultiRequest.newBuilder().setData(kv(i)).build());
		}
		return Flux.fromIterable(requests);
	}

	private static Flux<DeleteMultiRequest> deletes(int items) {
		var requests = new ArrayList<DeleteMultiRequest>();
		requests.add(DeleteMultiRequest.newBuilder()
				.setInitialRequest(DeleteMultiInitialRequest.newBuilder().setColumnId(1L).setContext(context(Long.MAX_VALUE)))
				.build());
		for (int i = 0; i < items; i++) {
			requests.add(DeleteMultiRequest.newBuilder()
					.setData(DeleteRequest.newBuilder().addKeys(ByteString.copyFrom(new byte[] {(byte) i})))
					.build());
		}
		return Flux.fromIterable(requests);
	}

	/** Every item-by-item streamed multi write of the service, each sending {@code items} items. */
	private static Map<String, Function<ReactorRocksDBServiceStub, Publisher<?>>> calls(int items) {
		var calls = new LinkedHashMap<String, Function<ReactorRocksDBServiceStub, Publisher<?>>>();
		calls.put("putMulti", stub -> stub.putMulti(puts(Long.MAX_VALUE, items)));
		calls.put("putMultiGetPrevious", stub -> stub.putMultiGetPrevious(puts(Long.MAX_VALUE, items)));
		calls.put("deleteMulti", stub -> stub.deleteMulti(deletes(items)));
		calls.put("deleteMultiGetPrevious", stub -> stub.deleteMultiGetPrevious(deletes(items)));
		calls.put("mergeMulti", stub -> stub.mergeMulti(merges(items)));
		calls.put("mergeMultiGetMerged", stub -> stub.mergeMultiGetMerged(merges(items)));
		return calls;
	}

	private static StatusRuntimeException failure(Publisher<?> call) {
		return assertThrows(StatusRuntimeException.class,
				() -> Flux.from(call).collectList().block(Duration.ofSeconds(20)));
	}

	private static void assertPartlyApplied(String name, StatusRuntimeException error) {
		assertNotEquals(RocksDBException.NOT_APPLIED, error.getTrailers() == null ? null : error.getTrailers().get(APPLIED),
				name + ": an item was applied, so the call must not say \"not applied\": " + error);
		assertEquals(Status.Code.INVALID_ARGUMENT, error.getStatus().getCode(), name + ": " + error);
		assertTrue(error.getStatus().getDescription().contains("partly applied"), name + ": " + error);
		assertEquals(RocksDBException.MAYBE_APPLIED, error.getTrailers().get(APPLIED), name);
	}

	/** A refusal of the second item (as a replicated node's proposer refuses one proposal) after the first was applied. */
	@Test
	void aFlaggedRefusalAfterAnAppliedItemIsPartlyApplied() throws Exception {
		for (var call : calls(3).entrySet()) {
			var backend = new Backend(1, 0L);
			try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
				server.start();
				var channel = channel(server);
				try {
					var error = failure(call.getValue().apply(ReactorRocksDBServiceGrpc.newReactorStub(channel)));
					assertEquals(1, backend.applied.get(), call.getKey());
					assertPartlyApplied(call.getKey(), error);
					assertTrue(error.getStatus().getDescription().contains("1 "), call.getKey() + ": " + error);
				} finally {
					close(channel);
				}
			}
		}
	}

	/**
	 * Review of R61, 2: a refusal as {@code SERVER_OVERLOADED} that is not flagged (a stopping node, a batch that could
	 * not be handed to a thread) after the first item was applied. Callers that classify by type alone resend an
	 * overload, and the whole stream again would apply the first item twice: the call ends "partly applied" too.
	 */
	@Test
	void anUnflaggedOverloadAfterAnAppliedItemIsPartlyApplied() throws Exception {
		for (var call : calls(3).entrySet()) {
			var backend = new Backend(1, 0L, false);
			try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
				server.start();
				var channel = channel(server);
				try {
					var error = failure(call.getValue().apply(ReactorRocksDBServiceGrpc.newReactorStub(channel)));
					assertEquals(1, backend.applied.get(), call.getKey());
					assertPartlyApplied(call.getKey(), error);
					assertTrue(error.getStatus().getDescription().contains("at least 1 "), call.getKey() + ": " + error);
				} finally {
					close(channel);
				}
			}
		}
	}

	/** Nothing applied yet: an unflagged overload of the first item keeps its type, and stays unflagged. */
	@Test
	void anUnflaggedOverloadOfTheFirstItemPassesUnchanged() throws Exception {
		for (var call : calls(3).entrySet()) {
			var backend = new Backend(0, 0L, false);
			try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
				server.start();
				var channel = channel(server);
				try {
					var error = failure(call.getValue().apply(ReactorRocksDBServiceGrpc.newReactorStub(channel)));
					assertEquals(0, backend.applied.get(), call.getKey());
					assertEquals(Status.Code.RESOURCE_EXHAUSTED, error.getStatus().getCode(), call.getKey() + ": " + error);
					assertEquals(RocksDBException.MAYBE_APPLIED, error.getTrailers().get(APPLIED), call.getKey());
				} finally {
					close(channel);
				}
			}
		}
	}

	/** putMultiEnsure applies groups of 4096 items: the second group is refused after the first was applied. */
	@Test
	void aFlaggedRefusalAfterAnAppliedGroupOfPutMultiEnsureIsPartlyApplied() throws Exception {
		var backend = new Backend(1, 0L);
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			var channel = channel(server);
			try {
				var error = failure(ReactorRocksDBServiceGrpc.newReactorStub(channel)
						.putMultiEnsure(puts(Long.MAX_VALUE, 4_096 + 1)));
				assertEquals(1, backend.applied.get());
				assertPartlyApplied("putMultiEnsure", error);
			} finally {
				close(channel);
			}
		}
	}

	/** Nothing applied yet: the refusal of the first item is true for the whole call, and keeps its flag and type. */
	@Test
	void aFlaggedRefusalOfTheFirstItemStaysNotApplied() throws Exception {
		for (var call : calls(3).entrySet()) {
			var backend = new Backend(0, 0L);
			try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
				server.start();
				var channel = channel(server);
				try {
					var error = failure(call.getValue().apply(ReactorRocksDBServiceGrpc.newReactorStub(channel)));
					assertEquals(0, backend.applied.get(), call.getKey());
					assertEquals(Status.Code.RESOURCE_EXHAUSTED, error.getStatus().getCode(), call.getKey() + ": " + error);
					assertEquals(RocksDBException.NOT_APPLIED, error.getTrailers().get(APPLIED), call.getKey());
				} finally {
					close(channel);
				}
			}
		}
	}

	/**
	 * The request deadline passes while the first item is applied. The streaming scheduler usually ends the stream first
	 * ("Workload deadline expired while streaming", not flagged); the flagged refusal before dispatch of
	 * {@code GrpcRequestContexts.remainingContext} needs the deadline to pass between the two checks. Either way the call
	 * must not say "not applied".
	 */
	@Test
	void aDeadlineThatPassesAfterTheFirstItemIsNotFlaggedNotApplied() throws Exception {
		var backend = new Backend(Integer.MAX_VALUE, 1_500L);
		try (var server = new GrpcServer(backend, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			var channel = channel(server);
			try {
				var error = failure(ReactorRocksDBServiceGrpc.newReactorStub(channel)
						.putMulti(puts(TimeUnit.MILLISECONDS.toNanos(500), 3)));
				assertEquals(1, backend.applied.get());
				assertEquals(RocksDBException.MAYBE_APPLIED, error.getTrailers().get(APPLIED), error.toString());
				assertTrue(error.getStatus().getCode() == Status.Code.DEADLINE_EXCEEDED
						|| error.getStatus().getCode() == Status.Code.INVALID_ARGUMENT, error.toString());
			} finally {
				close(channel);
			}
		}
	}

	private static ManagedChannel channel(GrpcServer server) {
		return ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort()).usePlaintext().build();
	}

	private static void close(ManagedChannel channel) throws InterruptedException {
		channel.shutdownNow();
		channel.awaitTermination(5, TimeUnit.SECONDS);
	}

	/** Applies the first {@code accepted} writes (each after {@code applyMillis}), and refuses the next ones, flagged or not. */
	private static final class Backend implements RocksDBConnection {

		private final int accepted;
		private final long applyMillis;
		private final boolean flagged;
		private final AtomicInteger applied = new AtomicInteger();
		private final RocksDBSyncAPI syncApi = new RocksDBSyncAPI() {
			@Override
			public <R, RS, RA> RS requestSync(RocksDBAPICommand<R, RS, RA> request) {
				if (applied.get() >= accepted) {
					if (!flagged) {
						throw RocksDBException.of(RocksDBErrorType.SERVER_OVERLOADED, "The node is stopping");
					}
					throw RocksDBException.notApplied(RocksDBErrorType.SERVER_OVERLOADED,
							"The node is not accepting writes; it was not applied");
				}
				if (applyMillis > 0) {
					try {
						Thread.sleep(applyMillis);
					} catch (InterruptedException e) {
						Thread.currentThread().interrupt();
						throw RocksDBException.of(RocksDBErrorType.INTERNAL_ERROR, "interrupted");
					}
				}
				applied.incrementAndGet();
				return null;
			}
		};

		private Backend(int accepted, long applyMillis) {
			this(accepted, applyMillis, true);
		}

		private Backend(int accepted, long applyMillis, boolean flagged) {
			this.accepted = accepted;
			this.applyMillis = applyMillis;
			this.flagged = flagged;
		}

		public RockserverCapabilities getCapabilities() {
			return RockserverCapabilities.CURRENT;
		}

		@Override
		public URI getUrl() {
			return URI.create("test://partly-applied-backend");
		}

		@Override
		public RocksDBSyncAPI getSyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return syncApi;
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(it.cavallium.rockserver.core.common.RequestContext context) {
			return new RocksDBAsyncAPI() {};
		}

		@Override
		public void close() {
		}
	}
}
