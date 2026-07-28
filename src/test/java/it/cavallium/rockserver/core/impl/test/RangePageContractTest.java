package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.Server;
import io.grpc.ServerBuilder;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.ThriftConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.RangePage;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.cavallium.rockserver.core.server.ThriftServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import org.apache.thrift.TException;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Mono;

@Timeout(60)
@TestInstance(TestInstance.Lifecycle.PER_CLASS)
class RangePageContractTest {

	private static final long TIMEOUT_MS = 10_000L;
	private static final int LATENCY_MAX_ITEMS = 2;
	private static final long LATENCY_MAX_BYTES = 64;
	private static final RangeBudget TWO_ITEMS = new RangeBudget(LATENCY_MAX_ITEMS, LATENCY_MAX_BYTES);

	private EmbeddedConnection embedded;
	private GrpcServer grpcServer;
	private GrpcConnection grpc;
	private ThriftServer thriftServer;
	private ThriftConnection thrift;
	private long columnId;
	private long bucketedColumnId;

	@BeforeAll
	void setUp(@TempDir Path tempDir) throws IOException, TException {
		System.setProperty("rockserver.core.print-config", "false");
		var config = tempDir.resolve("rockserver.conf");
		Files.writeString(config, """
				database: {
				  parallelism: {
				    workload: {
				      latency-range-max-items: %d
				      latency-range-max-bytes: %dB
				    }
				  }
				  global: { ingest-behind: false, optimistic: false }
				}
				""".formatted(LATENCY_MAX_ITEMS, LATENCY_MAX_BYTES));
		embedded = new EmbeddedConnection(tempDir.resolve("db"), "range-page-contract", config);
		var batch = embedded.getSyncApi(RequestContext.batch());
		columnId = batch.createColumn("ranges",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		for (long value = 0; value < 10; value++) {
			batch.put(0, columnId, key(value * 10), value(value * 10), RequestType.none());
		}
		bucketedColumnId = batch.createColumn("bucketed-ranges",
				ColumnSchema.of(IntList.of(1), ObjectList.of(ColumnHashType.ALLSAME8), true));
		for (var variableKey : List.of("D", "B", "A", "C")) {
			batch.put(0,
					bucketedColumnId,
					bucketedKey(variableKey),
					Buf.wrap(variableKey.getBytes(StandardCharsets.UTF_8)),
					RequestType.none());
		}

		grpcServer = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0));
		grpcServer.start();
		grpc = GrpcConnection.forHostAndPort("range-page-contract",
				new Utils.HostAndPort("127.0.0.1", grpcServer.getPort()));

		int thriftPort = findFreePort();
		thriftServer = new ThriftServer(embedded, "127.0.0.1", thriftPort);
		thriftServer.start();
		thrift = new ThriftConnection("range-page-contract", "127.0.0.1", thriftPort);
	}

	@AfterAll
	void tearDown() throws IOException {
		if (thrift != null) {
			thrift.close();
		}
		if (thriftServer != null) {
			thriftServer.close();
		}
		if (grpc != null) {
			grpc.close();
		}
		if (grpcServer != null) {
			grpcServer.close();
		}
		if (embedded != null) {
			embedded.closeTesting();
		}
	}

	@Test
	void embeddedGrpcAndThriftRepeatOriginalBoundsWithExclusiveContinuation() {
		for (var api : apis()) {
			assertPages(api, false, List.of(20L, 30L, 40L, 50L, 60L, 70L));
			assertPages(api, true, List.of(70L, 60L, 50L, 40L, 30L, 20L));
			var noCache = api.getRangePage(0,
					columnId,
					key(20),
					key(80),
					false,
					null,
					RequestType.allInRangeNoCache(),
					TIMEOUT_MS,
					TWO_ITEMS);
			assertEquals(List.of(20L, 30L), values(noCache));
			assertTrue(noCache.hasMore());
		}
	}

	@Test
	void embeddedGrpcAndThriftAsyncViewsExposeTheSameBoundedPageContract() {
		for (var api : asyncApis(RequestContext.latency(Duration.ofSeconds(30)))) {
			var page = api.getRangePageAsync(0,
					columnId,
					key(20),
					key(80),
					false,
					null,
					RequestType.allInRange(),
					TIMEOUT_MS,
					TWO_ITEMS).join();
			assertEquals(List.of(20L, 30L), values(page));
			assertEquals(key(30), page.resumeAfter());
			assertTrue(page.hasMore());
		}
	}

	@Test
	void loweredLatencyCeilingsAcceptTheLimitAndRejectOverLimitWithoutClamping() {
		for (var api : apis(RequestContext.latency(Duration.ofSeconds(30)))) {
			var atLimit = api.getRangePage(0,
					columnId,
					key(0),
					key(100),
					false,
					null,
					RequestType.allInRange(),
					TIMEOUT_MS,
					new RangeBudget(LATENCY_MAX_ITEMS, LATENCY_MAX_BYTES));
			assertEquals(LATENCY_MAX_ITEMS, atLimit.items().size());
			assertTrue(atLimit.hasMore());

			var tooManyItems = assertThrows(RocksDBException.class, () -> api.getRangePage(0,
					columnId, key(0), key(100), false, null, RequestType.allInRange(), TIMEOUT_MS,
					new RangeBudget(LATENCY_MAX_ITEMS + 1, LATENCY_MAX_BYTES)));
			assertEquals(RocksDBErrorType.PUT_INVALID_REQUEST, tooManyItems.getErrorUniqueId());

			var tooManyBytes = assertThrows(RocksDBException.class, () -> api.getRangePage(0,
					columnId, key(0), key(100), false, null, RequestType.allInRange(), TIMEOUT_MS,
					new RangeBudget(LATENCY_MAX_ITEMS, LATENCY_MAX_BYTES + 1)));
			assertEquals(RocksDBErrorType.PUT_INVALID_REQUEST, tooManyBytes.getErrorUniqueId());
		}
	}

	@Test
	void itemAndPublicHardCeilingsHaveParityOnEveryTransport() {
		for (var api : apis()) {
			var error = assertThrows(RocksDBException.class, () -> api.getRangePage(
					0, columnId, key(0), key(100), false, null,
					RequestType.allInRange(), TIMEOUT_MS, new RangeBudget(10, 15)));
			assertEquals(RocksDBErrorType.RANGE_ITEM_TOO_LARGE, error.getErrorUniqueId());

			var tooManyItems = assertThrows(RocksDBException.class, () -> api.getRangePage(
					0, columnId, key(0), key(100), false, null,
					RequestType.allInRange(), TIMEOUT_MS,
					new RangeBudget(RangeBudget.DEFAULT_MAX_ITEMS + 1, RangeBudget.DEFAULT_MAX_BYTES)));
			assertEquals(RocksDBErrorType.PUT_INVALID_REQUEST, tooManyItems.getErrorUniqueId());

			var tooManyBytes = assertThrows(RocksDBException.class, () -> api.getRangePage(
					0, columnId, key(0), key(100), false, null,
					RequestType.allInRange(), TIMEOUT_MS,
					new RangeBudget(RangeBudget.DEFAULT_MAX_ITEMS, RangeBudget.DEFAULT_MAX_BYTES + 1)));
			assertEquals(RocksDBErrorType.PUT_INVALID_REQUEST, tooManyBytes.getErrorUniqueId());
		}
	}

	@Test
	void continuationsWithinOneHashBucketAreDeterministicInBothDirections() {
		for (var api : apis()) {
			assertBucketedPages(api, false, List.of("A", "B", "C", "D"));
			assertBucketedPages(api, true, List.of("D", "C", "B", "A"));
		}
	}

	@Test
	void latencyAndIngestViewsCanUseBoundedPagesOnEveryTransport() {
		for (var context : List.of(RequestContext.latency(Duration.ofSeconds(30)), RequestContext.ingest())) {
			for (var api : apis(context)) {
				var page = api.getRangePage(0,
						columnId,
						key(20),
						key(80),
						false,
						null,
						RequestType.allInRange(),
						TIMEOUT_MS,
						TWO_ITEMS);
				assertEquals(List.of(20L, 30L), values(page));
				assertTrue(page.hasMore());
			}
		}
	}

	@Test
	void transactionPagesUseTheSnapshotCapturedWhenTheTransactionOpenedOnEveryTransport() {
		var transportApis = apis();
		var transactionIds = new ArrayList<Long>();
		var firstPages = new ArrayList<RangePage<KV>>();
		try {
			for (var api : transportApis) {
				transactionIds.add(api.openTransaction(TIMEOUT_MS));
			}
			for (int index = 0; index < transportApis.size(); index++) {
				var first = transportApis.get(index).getRangePage(transactionIds.get(index),
						columnId,
						key(0),
						key(100),
						false,
						null,
						RequestType.allInRange(),
						TIMEOUT_MS,
						new RangeBudget(1, RangeBudget.DEFAULT_MAX_BYTES));
				assertEquals(List.of(0L), values(first));
				firstPages.add(first);
			}

			embedded.getSyncApi(RequestContext.batch())
					.put(0, columnId, key(5), value(5), RequestType.none());
			for (int index = 0; index < transportApis.size(); index++) {
				var second = transportApis.get(index).getRangePage(transactionIds.get(index),
						columnId,
						key(0),
						key(100),
						false,
						firstPages.get(index).resumeAfter(),
						RequestType.allInRange(),
						TIMEOUT_MS,
						new RangeBudget(1, RangeBudget.DEFAULT_MAX_BYTES));
				assertEquals(List.of(10L), values(second),
						"a later autocommit write must not enter the transaction snapshot");
			}
		} finally {
			for (int index = 0; index < transactionIds.size(); index++) {
				transportApis.get(index).closeTransaction(transactionIds.get(index), false);
			}
		}
	}

	@Test
	void pageAndBudgetModelsRejectAmbiguousOrMutableResults() {
		assertThrows(IllegalArgumentException.class, () -> new RangeBudget(0, 1));
		assertThrows(IllegalArgumentException.class, () -> new RangeBudget(1, 0));
		assertThrows(IllegalArgumentException.class, () -> new RangePage<>(List.of(), key(1), false));
		assertThrows(IllegalArgumentException.class, () -> new RangePage<>(List.of(), null, true));

		var mutable = new ArrayList<>(List.of(new KV(key(1), value(1))));
		var page = new RangePage<>(mutable, key(1), false);
		mutable.clear();
		assertEquals(1, page.items().size());
		assertThrows(UnsupportedOperationException.class, () -> page.items().clear());
	}

	@Test
	void grpcConstructionRejectsAnIncompatibleWorkloadContract() throws Exception {
		var server = ServerBuilder.forPort(0)
				.addService(new ReactorRocksDBServiceGrpc.RocksDBServiceImplBase() {
					@Override
					public Mono<CapabilitiesResponse> getCapabilities(CapabilitiesRequest request) {
						return Mono.just(CapabilitiesResponse.newBuilder()
								.setWorkloadContractVersion(1)
								.setBoundedRange(false)
								.build());
					}
				})
				.build()
				.start();
		try {
			var error = assertThrows(RocksDBException.class, () -> GrpcConnection.forHostAndPort(
					"incompatible-range-contract",
					new Utils.HostAndPort("127.0.0.1", server.getPort())));
			assertEquals(RocksDBErrorType.NOT_IMPLEMENTED, error.getErrorUniqueId());
		} finally {
			server.shutdownNow();
			server.awaitTermination();
		}
	}

	@Test
	void grpcConstructionRejectsAPeerWithoutTheCapabilityRpc() throws Exception {
		var server = ServerBuilder.forPort(0)
				.addService(new ReactorRocksDBServiceGrpc.RocksDBServiceImplBase() {})
				.build()
				.start();
		try {
			var error = assertThrows(RocksDBException.class, () -> GrpcConnection.forHostAndPort(
					"missing-range-capability",
					new Utils.HostAndPort("127.0.0.1", server.getPort())));
			assertEquals(RocksDBErrorType.NOT_IMPLEMENTED, error.getErrorUniqueId());
		} finally {
			server.shutdownNow();
			server.awaitTermination();
		}
	}

	private List<RocksDBSyncAPI> apis() {
		return apis(RequestContext.batch());
	}

	private List<RocksDBSyncAPI> apis(RequestContext context) {
		return List.of(
				embedded.getSyncApi(context),
				grpc.getSyncApi(context),
				thrift.getSyncApi(context));
	}

	private List<RocksDBAsyncAPI> asyncApis(RequestContext context) {
		return List.of(
				embedded.getAsyncApi(context),
				grpc.getAsyncApi(context),
				thrift.getAsyncApi(context));
	}

	private void assertPages(RocksDBSyncAPI api, boolean reverse, List<Long> expected) {
		var actual = new ArrayList<Long>();
		Keys resumeAfter = null;
		boolean hasMore;
		do {
			var page = api.getRangePage(0,
					columnId,
					key(20),
					key(80),
					reverse,
					resumeAfter,
					RequestType.allInRange(),
					TIMEOUT_MS,
					TWO_ITEMS);
			assertTrue(page.items().size() <= TWO_ITEMS.maxItems());
			actual.addAll(values(page));
			resumeAfter = page.resumeAfter();
			hasMore = page.hasMore();
		} while (hasMore);
		assertEquals(expected, actual);
		assertFalse(hasMore);
	}

	private void assertBucketedPages(RocksDBSyncAPI api, boolean reverse, List<String> expected) {
		var actual = new ArrayList<String>();
		Keys resumeAfter = null;
		boolean hasMore;
		do {
			var page = api.getRangePage(0,
					bucketedColumnId,
					null,
					null,
					reverse,
					resumeAfter,
					RequestType.allInRange(),
					TIMEOUT_MS,
					TWO_ITEMS);
			actual.addAll(page.items().stream()
					.map(row -> new String(row.keys().keys()[1].toByteArray(), StandardCharsets.UTF_8))
					.toList());
			resumeAfter = page.resumeAfter();
			hasMore = page.hasMore();
		} while (hasMore);
		assertEquals(expected, actual);
	}

	private static List<Long> values(RangePage<KV> page) {
		return page.items().stream().map(row -> row.value().getLong(0)).toList();
	}

	private static int findFreePort() throws IOException {
		try (var socket = new ServerSocket(0)) {
			socket.setReuseAddress(true);
			return socket.getLocalPort();
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf value(long value) {
		return Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
	}

	private static Keys bucketedKey(String variableKey) {
		return new Keys(Buf.wrap(new byte[] {1}), Buf.wrap(variableKey.getBytes(StandardCharsets.UTF_8)));
	}
}
