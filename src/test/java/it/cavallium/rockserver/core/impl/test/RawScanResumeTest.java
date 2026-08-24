package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.grpc.ManagedChannelBuilder;
import io.grpc.Status;
import io.grpc.StatusRuntimeException;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RawScanEvent;
import it.cavallium.rockserver.core.common.RawSstToken;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAsyncAPI;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.ScanRawRequest;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetSocketAddress;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.rocksdb.LiveFileMetaData;
import reactor.core.publisher.BaseSubscriber;
import reactor.test.StepVerifier;

@Timeout(90)
class RawScanResumeTest {

	private static final String COLUMN_NAME = "events";
	private static final int SST_COUNT = 4;
	private static final int KEYS_PER_SST = 32;
	private static final long MIB = 1024L * 1024L;

	@Test
	void customRawScanConcurrencyAndReadaheadDriveACompleteScan(@TempDir Path tempDir) throws Exception {
		Path config = tempDir.resolve("raw-scan-tuning.conf");
		Files.writeString(config, """
				database: {
				  parallelism: {
				    workload: {
				      raw-scan-file-concurrency: 6
				      raw-scan-readahead-bytes: 32MiB
				    }
				  }
				  global: {
				    ingest-behind: false
				    optimistic: false
				    disable-auto-compactions: true
				    disable-write-slowdown: true
				  }
				}
				""");

		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "raw-scan-tuning", config)) {
			var settings = connection.getInternalDB().getWorkloadSettings();
			assertEquals(6, settings.rawScanFileConcurrency());
			assertEquals(32L * MIB, settings.rawScanReadaheadBytes());
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			long columnId = createPopulatedColumn(api);
			assertEquals((long) SST_COUNT * KEYS_PER_SST, decodedRows(scan(api, columnId, Set.of())));
		}
	}

	@Test
	void acknowledgedLiveSstsAreSkippedBeforePinningAndAcrossRestart(@TempDir Path tempDir) throws Exception {
		Path databasePath = tempDir.resolve("db");
		Path config = rawScanConfig(tempDir);
		Set<RawSstToken> completed;

		try (var connection = new EmbeddedConnection(databasePath, "raw-resume", config)) {
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			long columnId = createPopulatedColumn(api);
			Set<RawSstToken> liveSstFileNames = liveColumnSstTokens(connection.getInternalDB());
			int liveSsts = liveSstFileNames.size();
			assertEquals(SST_COUNT, liveSsts, "the fixture needs one immutable SST per flush");

			List<RawScanEvent> firstScan = scan(api, columnId, Set.of());
			assertEquals((long) SST_COUNT * KEYS_PER_SST, decodedRows(firstScan));
			completed = completionTokens(firstScan);
			assertEquals(liveSsts, completed.size());
			assertEquals(liveSstFileNames, completed,
					"completion tokens must be the exact immutable RocksDB SST filenames");

			RawSstToken oneCompletedSst = completed.iterator().next();
			List<RawScanEvent> partialResume = scan(api, columnId, Set.of(oneCompletedSst));
			assertEquals((long) (SST_COUNT - 1) * KEYS_PER_SST, decodedRows(partialResume));
			Set<RawSstToken> partialCompletion = completionTokens(partialResume);
			assertEquals(SST_COUNT - 1, partialCompletion.size());
			assertFalse(partialCompletion.contains(oneCompletedSst));

			var pinnedFiles = new AtomicInteger(-1);
			connection.getInternalDB().setRawScanFilesCapturedObserverForTesting(() ->
					pinnedFiles.set(connection.getInternalDB().getRawScanPinnedFilesForTesting().size()));
			try {
				assertTrue(scan(api, columnId, completed).isEmpty());
				assertEquals(0, pinnedFiles.get(),
						"acknowledged live SSTs must be filtered before hard-link acquisition");
			} finally {
				connection.getInternalDB().setRawScanFilesCapturedObserverForTesting(null);
			}
		}

		try (var restarted = new EmbeddedConnection(databasePath, "raw-resume", config)) {
			RocksDBSyncAPI api = restarted.getSyncApi(RequestContext.batch());
			long columnId = api.getColumnId(COLUMN_NAME);
			assertTrue(scan(api, columnId, completed).isEmpty(),
					"live immutable SST filenames must remain stable across restart");
		}
	}

	@Test
	void compactedReplacementIsNeverSkippedByObsoleteTokens(@TempDir Path tempDir) throws Exception {
		Path config = rawScanConfig(tempDir);
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "raw-resume-compaction", config)) {
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			long columnId = createPopulatedColumn(api);
			Set<RawSstToken> oldTokens = completionTokens(scan(api, columnId, Set.of()));

			api.compact();
			List<RawScanEvent> replacementScan = scan(api, columnId, oldTokens);
			Set<RawSstToken> replacementTokens = completionTokens(replacementScan);
			assertTrue(decodedRows(replacementScan) > 0,
					"a compaction output SST must be scanned even if all input SSTs were acknowledged");
			assertFalse(replacementTokens.isEmpty());
			assertTrue(replacementTokens.stream().noneMatch(oldTokens::contains),
					"compaction outputs must have new RocksDB SST filenames");
		}
	}

	@Test
	void cleanupFailureNeverEmitsAnSstCompletion(@TempDir Path tempDir) throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "raw-resume-cleanup", null)) {
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			long columnId = createPopulatedColumn(api);
			EmbeddedDB db = connection.getInternalDB();
			var observed = new ArrayList<RawScanEvent>();
			db.setRawScanCleanupObserverForTesting(() -> {
				throw new IllegalStateException("synthetic resumable cleanup failure");
			});
			try {
				assertThrows(IllegalStateException.class, () -> db.scanRawResumableAsyncInternal(
						columnId, 0, 1, Set.of(), db.getScheduler().read())
						.doOnNext(observed::add)
						.blockLast(Duration.ofSeconds(30)));
				assertTrue(completionTokens(observed).isEmpty(),
						"a token is acknowledgable only after native reader cleanup succeeds");
			} finally {
				db.setRawScanCleanupObserverForTesting(null);
			}
		}
	}

	@Test
	void finalBatchCompletionNeedsNoAdditionalDownstreamDemand(@TempDir Path tempDir) throws Exception {
		Path config = rawScanConfig(tempDir);
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "raw-resume-demand", config)) {
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			long columnId = createPopulatedColumn(api);
			EmbeddedDB db = connection.getInternalDB();
			StepVerifier.create(db.scanRawResumableAsyncInternal(
					columnId, 0, 1, Set.of(), db.getScheduler().read()), SST_COUNT)
					.expectNextMatches(RawScanResumeTest::isBatchWithCompletion)
					.expectNextMatches(RawScanResumeTest::isBatchWithCompletion)
					.expectNextMatches(RawScanResumeTest::isBatchWithCompletion)
					.expectNextMatches(RawScanResumeTest::isBatchWithCompletion)
					.expectComplete()
					.verify(Duration.ofSeconds(30));
		}
	}

	@Test
	void rangeTombstoneOnlySstStillEmitsACompletion(@TempDir Path tempDir) throws Exception {
		Path config = rawScanConfig(tempDir);
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "raw-resume-empty", config)) {
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			long columnId = api.createColumn(COLUMN_NAME,
					ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
			api.put(0, columnId, intKey(5), Buf.wrap((byte) 1), RequestType.none());
			api.flush();
			api.deleteRange(columnId, intKey(0), intKey(10));
			api.flush();

			Set<RawSstToken> liveSsts = liveColumnSstTokens(connection.getInternalDB());
			assertEquals(2, liveSsts.size(), "the fixture needs one data and one range-tombstone SST");
			List<RawScanEvent> events = scan(api, columnId, Set.of());
			assertEquals(1L, decodedRows(events));
			assertEquals(liveSsts, completionTokens(events));
			assertEquals(1L, events.stream().filter(RawScanEvent.SstCompleted.class::isInstance).count());
		}
	}

	@Test
	void grpcPreservesTypedCompletionEventsAndSkipTokens(@TempDir Path tempDir) throws Exception {
		try (var embedded = new EmbeddedConnection(tempDir.resolve("db"), "raw-resume-grpc", null)) {
			RocksDBSyncAPI embeddedApi = embedded.getSyncApi(RequestContext.batch());
			long columnId = createPopulatedColumn(embeddedApi);
			try (var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
				server.start();
				try (var client = GrpcConnection.forHostAndPort("raw-resume-grpc-client",
						new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
					RocksDBSyncAPI api = client.getSyncApi(RequestContext.batch());
					List<RawScanEvent> events = scan(api, columnId, Set.of());
					assertEquals((long) SST_COUNT * KEYS_PER_SST, decodedRows(events));
					Set<RawSstToken> completed = completionTokens(events);
					assertFalse(completed.isEmpty());
					assertTrue(events.stream()
							.filter(RawScanEvent.Batch.class::isInstance)
							.map(RawScanEvent.Batch.class::cast)
							.allMatch(batch -> batch.completedSstToken() != null),
							"a one-batch SST should carry completion without a separate stream item");
					assertFalse(events.stream().anyMatch(RawScanEvent.SstCompleted.class::isInstance));
					assertTrue(scan(api, columnId, completed).isEmpty());
				}
			}
		}
	}

	@Test
	void repeatedGrpcCancellationCannotRaceNativeRawScanCleanup(@TempDir Path tempDir) throws Exception {
		try (var embedded = new EmbeddedConnection(tempDir.resolve("db"), "raw-resume-grpc-cancel", null)) {
			RocksDBSyncAPI embeddedApi = embedded.getSyncApi(RequestContext.batch());
			int cancellationSsts = 128;
			long columnId = createPopulatedColumn(embeddedApi, cancellationSsts, 1);
			try (var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
				server.start();
				try (var client = GrpcConnection.forHostAndPort("raw-resume-grpc-cancel-client",
						new Utils.HostAndPort("127.0.0.1", server.getPort()))) {
					RocksDBAsyncAPI api = client.getAsyncApi(RequestContext.batch());
					for (int attempt = 0; attempt < 32; attempt++) {
						var firstEvent = new CountDownLatch(1);
						var subscriber = new BaseSubscriber<RawScanEvent>() {
							@Override
							protected void hookOnSubscribe(org.reactivestreams.Subscription subscription) {
								request(1);
							}

							@Override
							protected void hookOnNext(RawScanEvent value) {
								firstEvent.countDown();
								cancel();
							}
						};
						reactor.core.publisher.Flux.from(api.scanRawResumableAsync(columnId, 0, 1, Set.of()))
								.subscribe(subscriber);
						assertTrue(firstEvent.await(10, TimeUnit.SECONDS),
								"raw scan did not emit before cancellation attempt " + attempt);
						awaitNoRawScanPins(embedded.getInternalDB(), Duration.ofSeconds(10));
					}
					assertEquals((long) cancellationSsts,
							api.estimateNumKeysAsync(columnId).get(10, TimeUnit.SECONDS));
				}
			}
		}
	}

	@Test
	void grpcRetainsLegacyCompletionShapeForClientsThatDoNotOptIn(@TempDir Path tempDir) throws Exception {
		try (var embedded = new EmbeddedConnection(tempDir.resolve("db"), "raw-resume-grpc-legacy", null);
				var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
			RocksDBSyncAPI embeddedApi = embedded.getSyncApi(RequestContext.batch());
			long columnId = createPopulatedColumn(embeddedApi);
			server.start();
			var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
					.usePlaintext()
					.build();
			try {
				var context = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
						.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
						.setDeadlineEpochMillis(Long.MAX_VALUE)
						.build();
				var request = ScanRawRequest.newBuilder()
						.setColumnId(columnId)
						.setShardCount(1)
						.setContext(context)
						.setResumable(true)
						.build();
				var responses = RocksDBServiceGrpc.newBlockingStub(channel).scanRaw(request);
				int batches = 0;
				int completions = 0;
				while (responses.hasNext()) {
					var response = responses.next();
					assertFalse(response.hasCompletedSstTokenAfterBatch());
					switch (response.getEventCase()) {
						case SERIALIZED -> batches++;
						case COMPLETEDSSTTOKEN -> completions++;
						case EVENT_NOT_SET -> throw new AssertionError("empty raw-scan response");
					}
				}
				assertEquals(SST_COUNT, batches);
				assertEquals(SST_COUNT, completions);
			} finally {
				channel.shutdownNow();
				assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
			}
		}
	}

	@Test
	void legacyScanIsUnchangedAndResumableScanUsesLiveSstFileNames(@TempDir Path tempDir) throws Exception {
		try (var connection = new EmbeddedConnection(tempDir.resolve("db"), "raw-resume-token-cost", null)) {
			RocksDBSyncAPI api = connection.getSyncApi(RequestContext.batch());
			long columnId = createPopulatedColumn(api);
			Set<RawSstToken> liveSstFileNames = liveColumnSstTokens(connection.getInternalDB());
			try (var batches = api.scanRaw(columnId, 0, 1)) {
				assertEquals(SST_COUNT, batches.count());
			}
			assertEquals(liveSstFileNames, completionTokens(scan(api, columnId, Set.of())));
		}
	}

	@Test
	void grpcRawScanValidationUsesTheNormalErrorContract(@TempDir Path tempDir) throws Exception {
		try (var embedded = new EmbeddedConnection(tempDir.resolve("db"), "raw-resume-grpc-validation", null);
				var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
			server.start();
			var channel = ManagedChannelBuilder.forAddress("127.0.0.1", server.getPort())
					.usePlaintext()
					.build();
			try {
				var stub = RocksDBServiceGrpc.newBlockingStub(channel);
				var missingContext = ScanRawRequest.newBuilder()
						.setColumnId(1L)
						.setShardCount(1)
						.setResumable(true)
						.build();
				var contextFailure = assertThrows(StatusRuntimeException.class,
						() -> stub.scanRaw(missingContext).hasNext());
				assertEquals(Status.Code.INVALID_ARGUMENT, contextFailure.getStatus().getCode());
				assertEquals("RocksDBError: [uid:PUT_INVALID_REQUEST] Request context and workload profile are required",
						contextFailure.getStatus().getDescription());

				var validContext = it.cavallium.rockserver.core.common.api.proto.RequestContext.newBuilder()
						.setProfile(it.cavallium.rockserver.core.common.api.proto.WorkloadProfile.BATCH)
						.setDeadlineEpochMillis(Long.MAX_VALUE)
						.build();
				var invalidToken = missingContext.toBuilder()
						.setContext(validContext)
						.addCompletedSstTokens("not-a-token")
						.build();
				var tokenFailure = assertThrows(StatusRuntimeException.class,
						() -> stub.scanRaw(invalidToken).hasNext());
				assertEquals(Status.Code.INVALID_ARGUMENT, tokenFailure.getStatus().getCode());

				var legacyWithToken = invalidToken.toBuilder()
						.setResumable(false)
						.clearCompletedSstTokens()
						.addCompletedSstTokens("/000001.sst")
						.build();
				var modeFailure = assertThrows(StatusRuntimeException.class,
						() -> stub.scanRaw(legacyWithToken).hasNext());
				assertEquals(Status.Code.INVALID_ARGUMENT, modeFailure.getStatus().getCode());
				assertEquals("completed SST tokens require resumable raw scan mode",
						modeFailure.getStatus().getDescription());

				var legacyWithCoalescing = legacyWithToken.toBuilder()
						.clearCompletedSstTokens()
						.setCoalesceCompletedSstToken(true)
						.build();
				var coalescingFailure = assertThrows(StatusRuntimeException.class,
						() -> stub.scanRaw(legacyWithCoalescing).hasNext());
				assertEquals(Status.Code.INVALID_ARGUMENT, coalescingFailure.getStatus().getCode());
				assertEquals("coalesced SST completion requires resumable raw scan mode",
						coalescingFailure.getStatus().getDescription());
			} finally {
				channel.shutdownNow();
				assertTrue(channel.awaitTermination(5, TimeUnit.SECONDS));
			}
		}
	}

	@Test
	void grpcProxyAdvertisesOnlyTheBackendResumableCapability(@TempDir Path tempDir) throws Exception {
		try (var embedded = new EmbeddedConnection(tempDir.resolve("db"), "raw-resume-capabilities", null)) {
			RocksDBSyncAPI embeddedApi = embedded.getSyncApi(RequestContext.batch());
			long columnId = createPopulatedColumn(embeddedApi);
			try (var upstream = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
				upstream.start();
				try (var upstreamClient = GrpcConnection.forHostAndPort("raw-resume-upstream",
						new Utils.HostAndPort("127.0.0.1", upstream.getPort()));
						var proxy = new GrpcServer(upstreamClient, new InetSocketAddress("127.0.0.1", 0))) {
					proxy.start();
					try (var downstream = GrpcConnection.forHostAndPort("raw-resume-downstream",
							new Utils.HostAndPort("127.0.0.1", proxy.getPort()))) {
						assertTrue(upstreamClient.getCapabilities().resumableRawScan());
						assertTrue(downstream.getCapabilities().resumableRawScan());
						assertEquals((long) SST_COUNT * KEYS_PER_SST,
								decodedRows(scan(downstream.getSyncApi(RequestContext.batch()), columnId, Set.of())));
					}
				}
			}

			var legacyBackend = new CapabilityMaskingConnection(embedded);
			try (var proxy = new GrpcServer(legacyBackend, new InetSocketAddress("127.0.0.1", 0))) {
				proxy.start();
				try (var client = GrpcConnection.forHostAndPort("raw-resume-legacy-proxy",
						new Utils.HostAndPort("127.0.0.1", proxy.getPort()))) {
					assertFalse(client.getCapabilities().resumableRawScan());
					var failure = assertThrows(RocksDBException.class,
							() -> scan(client.getSyncApi(RequestContext.batch()), columnId, Set.of()));
					assertEquals(RocksDBException.RocksDBErrorType.NOT_IMPLEMENTED, failure.getErrorUniqueId());
				}
			}
		}
	}

	@Test
	void tokenValidationRejectsNonCanonicalValues() {
		assertThrows(IllegalArgumentException.class, () -> new RawSstToken(""));
		assertThrows(IllegalArgumentException.class, () -> new RawSstToken("00001.sst"));
		assertThrows(IllegalArgumentException.class, () -> new RawSstToken("000000.sst"));
		assertThrows(IllegalArgumentException.class, () -> new RawSstToken("0000001.sst"));
		assertThrows(IllegalArgumentException.class, () -> new RawSstToken("00000x.sst"));
		assertThrows(IllegalArgumentException.class, () -> new RawSstToken("٠٠٠٠٠١.sst"));
		assertThrows(IllegalArgumentException.class, () -> new RawSstToken("０００００１.sst"));
		assertThrows(IllegalArgumentException.class, () -> new RawSstToken("../000001.sst"));
		assertThrows(IllegalArgumentException.class, () -> new RawSstToken("000001.ldb"));
		assertThrows(IllegalArgumentException.class,
				() -> new RawSstToken("18446744073709551616.sst"));
		assertEquals("/000001.sst", new RawSstToken("/000001.sst").value());
		assertEquals("000001.sst", new RawSstToken("000001.sst").value());
		assertEquals("18446744073709551615.sst",
				new RawSstToken("18446744073709551615.sst").value());
	}

	private static void awaitNoRawScanPins(EmbeddedDB db, Duration timeout) throws InterruptedException {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (!db.getRawScanPinnedFilesForTesting().isEmpty() && System.nanoTime() < deadline) {
			Thread.sleep(10L);
		}
		assertTrue(db.getRawScanPinnedFilesForTesting().isEmpty(),
				"cancelled raw scan did not release every pinned SST");
	}

	private static Path rawScanConfig(Path tempDir) throws Exception {
		Path config = tempDir.resolve("raw-resume.conf");
		Files.writeString(config, """
				database: { global: {
				  ingest-behind: false
				  optimistic: false
				  disable-auto-compactions: true
				  disable-write-slowdown: true
				} }
				""");
		return config;
	}

	private static long createPopulatedColumn(RocksDBSyncAPI api) {
		return createPopulatedColumn(api, SST_COUNT, KEYS_PER_SST);
	}

	private static long createPopulatedColumn(RocksDBSyncAPI api, int sstCount, int keysPerSst) {
		long columnId = api.createColumn(COLUMN_NAME,
				ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), true));
		for (int sst = 0; sst < sstCount; sst++) {
			for (int index = 0; index < keysPerSst; index++) {
				int key = sst * keysPerSst + index;
				Buf serializedKey = Buf.createZeroes(Integer.BYTES);
				serializedKey.setInt(0, key);
				api.put(0, columnId, new Keys(serializedKey),
							Buf.wrap((byte) sst, (byte) index), RequestType.none());
			}
			api.flush();
		}
		return columnId;
	}

	private static List<RawScanEvent> scan(RocksDBSyncAPI api,
			long columnId,
			Set<RawSstToken> completed) {
		try (var events = api.scanRawResumable(columnId, 0, 1, completed)) {
			return events.toList();
		}
	}

	private static long decodedRows(List<RawScanEvent> events) {
		return events.stream()
				.filter(RawScanEvent.Batch.class::isInstance)
				.map(RawScanEvent.Batch.class::cast)
				.mapToLong(batch -> batch.decode().count())
				.sum();
	}

	private static boolean isBatchWithCompletion(RawScanEvent event) {
		return event instanceof RawScanEvent.Batch batch && batch.completedSstToken() != null;
	}

	private static Keys intKey(int value) {
		var key = Buf.createZeroes(Integer.BYTES);
		key.setInt(0, value);
		return new Keys(key);
	}

	private static Set<RawSstToken> completionTokens(List<RawScanEvent> events) {
		var result = new HashSet<RawSstToken>();
		for (var event : events) {
			RawSstToken token = switch (event) {
				case RawScanEvent.Batch batch -> batch.completedSstToken();
				case RawScanEvent.SstCompleted completed -> completed.token();
			};
			if (token != null) {
				assertTrue(result.add(token), "each captured SST must complete exactly once");
			}
		}
		return Set.copyOf(result);
	}

	private static Set<RawSstToken> liveColumnSstTokens(EmbeddedDB db) {
		var tokens = new HashSet<RawSstToken>();
		for (LiveFileMetaData file : db.getDb().get().getLiveFilesMetaData()) {
			if (COLUMN_NAME.equals(new String(file.columnFamilyName(), java.nio.charset.StandardCharsets.UTF_8))
					&& file.fileName().endsWith(".sst")) {
				assertTrue(tokens.add(new RawSstToken(file.fileName())),
						"RocksDB live SST filenames must be unique within the database");
			}
		}
		return Set.copyOf(tokens);
	}

	private record CapabilityMaskingConnection(RocksDBConnection delegate) implements RocksDBConnection {

		@Override
		public URI getUrl() {
			return delegate.getUrl();
		}

		@Override
		public RocksDBSyncAPI getSyncApi(RequestContext context) {
			return delegate.getSyncApi(context);
		}

		@Override
		public RocksDBAsyncAPI getAsyncApi(RequestContext context) {
			return delegate.getAsyncApi(context);
		}

		@Override
		public void close() {
			// The test owns the embedded delegate separately.
		}
	}
}
