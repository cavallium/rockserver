package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;
import reactor.test.StepVerifier;

@Timeout(60)
class RangeNoCacheTest {

	private static final long RANGE_TIMEOUT_MS = 10_000;
	private static final long CLEANUP_TIMEOUT_SECONDS = 5;

	@TempDir
	Path tempDir;

	private EmbeddedConnection embeddedConnection;
	private EmbeddedDB embeddedDB;
	private GrpcServer grpcServer;
	private GrpcConnection grpcClient;
	private long columnId;
	private List<KV> rows;

	@BeforeEach
	void setUp() throws IOException {
		System.setProperty("rockserver.core.print-config", "false");
		Path configFile = tempDir.resolve("rockserver.conf");
		Files.writeString(configFile, "database: { global: { ingest-behind: false, optimistic: false } }");
		embeddedConnection = new EmbeddedConnection(tempDir.resolve("db"), "range-no-cache", configFile);
		embeddedDB = embeddedConnection.getInternalDB();

		columnId = embeddedConnection.getSyncApi().createColumn("range-column",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		rows = new ArrayList<>();
		for (long index = 0; index < 10; index++) {
			KV row = new KV(key(index), value(index));
			rows.add(row);
			embeddedConnection.getSyncApi().put(0,
					columnId,
					row.keys(),
					row.value(),
					RequestType.none());
		}

		grpcServer = new GrpcServer(embeddedConnection, new InetSocketAddress("127.0.0.1", 0));
		grpcServer.start();
		grpcClient = GrpcConnection.forHostAndPort("range-no-cache-client",
				new Utils.HostAndPort("127.0.0.1", grpcServer.getPort()));
	}

	@AfterEach
	void tearDown() throws IOException {
		if (grpcClient != null) {
			grpcClient.close();
		}
		if (grpcServer != null) {
			grpcServer.close();
		}
		if (embeddedConnection != null) {
			embeddedConnection.closeTesting();
		}
	}

	@Test
	void embeddedBoundedRangesKeepRowsAndReadOptionsMode() {
		assertBoundedRangeModes(embeddedConnection.getSyncApi());
	}

	@Test
	void grpcBoundedRangesKeepRowsAndReadOptionsMode() {
		assertBoundedRangeModes(grpcClient.getSyncApi());
	}

	@Test
	void embeddedCancellationReleasesRangeResources() throws InterruptedException {
		assertCancellationReleasesRangeResources(embeddedConnection.getAsyncApi());
	}

	@Test
	void grpcCancellationReleasesServerSideRangeResources() throws InterruptedException {
		assertCancellationReleasesRangeResources(grpcClient.getAsyncApi());
	}

	private void assertBoundedRangeModes(RocksDBSyncAPI api) {
		List<KV> expectedForward = List.copyOf(rows.subList(2, 8));
		List<KV> expectedReverse = new ArrayList<>(expectedForward);
		Collections.reverse(expectedReverse);

		List<KV> normalForward = readAndAssertFillCache(api, false, RequestType.allInRange(), true);
		List<KV> noCacheForward = readAndAssertFillCache(api, false, RequestType.allInRangeNoCache(), false);
		assertEquals(expectedForward, normalForward);
		assertEquals(normalForward, noCacheForward,
				"the no-cache request must preserve bounded forward range semantics");

		List<KV> normalReverse = readAndAssertFillCache(api, true, RequestType.allInRange(), true);
		List<KV> noCacheReverse = readAndAssertFillCache(api, true, RequestType.allInRangeNoCache(), false);
		assertEquals(expectedReverse, normalReverse);
		assertEquals(normalReverse, noCacheReverse,
				"the no-cache request must preserve bounded reverse range semantics");
	}

	private List<KV> readAndAssertFillCache(RocksDBSyncAPI api,
			boolean reverse,
			RequestType.RequestGetRange<? super KV, KV> requestType,
			boolean expectedFillCache) {
		var observedFillCache = new CopyOnWriteArrayList<Boolean>();
		embeddedDB.setRangeReadOptionsObserverForTesting(observedFillCache::add);

		List<KV> result;
		try (var range = api.getRange(0,
				columnId,
				key(2),
				key(8),
				reverse,
				requestType,
				RANGE_TIMEOUT_MS)) {
			result = range.toList();
		}

		assertEquals(List.of(expectedFillCache), observedFillCache,
				"a range must create exactly one ReadOptions with the requested fill-cache mode");
		return result;
	}

	private void assertCancellationReleasesRangeResources(RocksDBAsyncAPI api) throws InterruptedException {
		awaitPendingOpsZero();
		var observedFillCache = new CopyOnWriteArrayList<Boolean>();
		embeddedDB.setRangeReadOptionsObserverForTesting(observedFillCache::add);

		StepVerifier.create(Flux.from(api.getRangeAsync(0,
				columnId,
				key(0),
				key(10),
				false,
				RequestType.allInRangeNoCache(),
				RANGE_TIMEOUT_MS)), 1)
				.expectNext(rows.getFirst())
				.thenCancel()
				.verify(Duration.ofSeconds(10));

		awaitPendingOpsZero();
		assertFalse(observedFillCache.isEmpty(), "the cancelled range must have created ReadOptions");
		assertTrue(observedFillCache.stream().noneMatch(Boolean::booleanValue),
				() -> "the cancelled no-cache range unexpectedly enabled fillCache: " + observedFillCache);
	}

	private void awaitPendingOpsZero() throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(CLEANUP_TIMEOUT_SECONDS);
		while (embeddedDB.getPendingOpsCount() != 0 && System.nanoTime() < deadline) {
			Thread.sleep(10);
		}
		assertEquals(0, embeddedDB.getPendingOpsCount(),
				"range cancellation did not release the server-side pending operation");
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf value(long value) {
		return Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value * 10).array());
	}
}
