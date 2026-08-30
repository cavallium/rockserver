package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrowsExactly;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.composite.CompositeMeterRegistry;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.KVBatch;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

class EnsureWriteElisionTest {

	private static final long MAX_PROBE_BYTES = 2L * 1024L * 1024L;

	@TempDir
	Path tempDir;

	private EmbeddedConnection connection;
	private EmbeddedDB embeddedDB;
	private RocksDBSyncAPI api;
	private SimpleMeterRegistry exportedMetrics;

	@BeforeEach
	void setUp() throws Exception {
		connection = new EmbeddedConnection(tempDir, "ensure-write-elision", null);
		embeddedDB = connection.getInternalDB();
		api = connection.getSyncApi(RequestContext.batch());
		exportedMetrics = new SimpleMeterRegistry();
		((CompositeMeterRegistry) embeddedDB.getMetricsRegistry()).add(exportedMetrics);
	}

	@AfterEach
	void tearDown() throws Exception {
		if (connection != null) {
			connection.close();
		}
		if (exportedMetrics != null) {
			exportedMetrics.close();
		}
	}

	@Test
	void singleEnsureElidesHotEqualValuesAndColdCacheFallsBackWithoutAStorageRead() {
		long columnId = api.createColumn("valued", fixedSchema(true));
		var key = intKey(1);
		var first = value("first");
		var second = value("second");
		api.cdcCreate("single-ensure", null, List.of(columnId), null, java.util.OptionalLong.empty());

		assertNull(api.put(0, columnId, key, first, RequestType.ensure()));
		assertNull(api.put(0, columnId, key, first, RequestType.ensure()));
		assertValue(first, api.get(0, columnId, key, RequestType.current()));
		assertEquals(1L, cdcEventCount("single-ensure"));
		assertEquals(1.0, decisionCount("ensure", "fallback_not_found"));
		assertEquals(1.0, decisionCount("ensure", "elided"));

		assertNull(api.put(0, columnId, key, second, RequestType.ensure()));
		assertValue(second, api.get(0, columnId, key, RequestType.current()));
		assertEquals(2L, cdcEventCount("single-ensure"));
		assertEquals(1.0, decisionCount("ensure", "fallback_different"));

		api.flush();
		assertNull(api.put(0, columnId, key, second, RequestType.ensure()));
		assertEquals(3L, cdcEventCount("single-ensure"),
				"a cold cache-tier miss must retain ordinary Put and CDC behavior");
		assertEquals(1.0, decisionCount("ensure", "fallback_incomplete"));
	}

	@Test
	void keyOnlyAndBucketedLogicalValuesAreDecodedBeforeEqualityIsDecided() {
		long keyOnlyColumn = api.createColumn("key-only", fixedSchema(false));
		var empty = Utils.emptyBuf();
		assertNull(api.put(0, keyOnlyColumn, intKey(1), empty, RequestType.ensure()));
		assertNull(api.put(0, keyOnlyColumn, intKey(1), empty, RequestType.ensure()));
		assertTrue(api.get(0, keyOnlyColumn, intKey(1), RequestType.exists()));

		long bucketedColumn = api.createColumn("bucketed", ColumnSchema.of(
				IntList.of(1),
				ObjectList.of(ColumnHashType.ALLSAME8),
				true));
		var firstKey = bucketKey(7, "alpha");
		var secondKey = bucketKey(7, "beta");
		var first = value("one");
		var second = value("two");
		assertNull(api.put(0, bucketedColumn, firstKey, first, RequestType.ensure()));
		assertNull(api.put(0, bucketedColumn, firstKey, first, RequestType.ensure()));
		assertNull(api.put(0, bucketedColumn, firstKey, second, RequestType.ensure()));
		assertValue(second, api.get(0, bucketedColumn, firstKey, RequestType.current()));

		var result = api.putMulti(0,
				bucketedColumn,
				List.of(firstKey, secondKey),
				List.of(second, first),
				RequestType.ensure());
		assertTrue(result.isEmpty());
		assertValue(second, api.get(0, bucketedColumn, firstKey, RequestType.current()));
		assertValue(first, api.get(0, bucketedColumn, secondKey, RequestType.current()));
	}

	@Test
	void walBackedPutBatchIsIdempotentForKeyOnlyColumnsAndCompatibleWithEnsure() {
		long columnId = api.createColumn("key-only-batch", fixedSchema(false));
		var firstKey = intKey(1);
		var secondKey = intKey(2);
		var empty = Utils.emptyBuf();
		var batch = new KVBatch.KVBatchRef(
				List.of(firstKey, secondKey),
				List.of(empty, empty));

		api.putBatch(columnId, Flux.just(batch), PutBatchMode.WRITE_BATCH);
		api.putBatch(columnId, Flux.just(batch), PutBatchMode.WRITE_BATCH);
		assertTrue(api.get(0, columnId, firstKey, RequestType.exists()));
		assertTrue(api.get(0, columnId, secondKey, RequestType.exists()));

		assertNull(api.put(0, columnId, firstKey, empty, RequestType.ensure()));
		assertTrue(api.get(0, columnId, firstKey, RequestType.exists()));
		api.flush();
		assertTrue(api.get(0, columnId, firstKey, RequestType.exists()));
		assertTrue(api.get(0, columnId, secondKey, RequestType.exists()));
	}

	@Test
	void multiGetIsBoundedAndDirtyPhysicalKeysAreReprobedInInputOrder() {
		long columnId = api.createColumn("multi", fixedSchema(true));
		var probeCalls = new ArrayList<ProbeCall>();
		embeddedDB.setWriteElisionMultiGetObserverForTesting((keys, bytes) -> probeCalls.add(new ProbeCall(keys, bytes)));
		var keys = new ArrayList<Keys>(4_097);
		var values = new ArrayList<Buf>(4_097);
		for (int i = 0; i < 4_097; i++) {
			keys.add(intKey(i));
			values.add(value("v"));
		}

		assertTrue(api.putMulti(0, columnId, keys, values, RequestType.ensure()).isEmpty());
		assertTrue(api.putMulti(0, columnId, keys, values, RequestType.ensure()).isEmpty());
		assertEquals(4, probeCalls.size());
		assertEquals(List.of(4_096, 1, 4_096, 1), probeCalls.stream().map(ProbeCall::keys).toList());
		assertTrue(probeCalls.stream().allMatch(call -> call.keys() <= 4_096 && call.bytes() <= MAX_PROBE_BYTES));

		long orderedColumn = api.createColumn("ordered", fixedSchema(true));
		api.cdcCreate("ordered-ensure", null, List.of(orderedColumn), null, java.util.OptionalLong.empty());
		var duplicate = intKey(9);
		var first = value("first");
		var second = value("second");
		assertTrue(api.putMulti(0,
				orderedColumn,
				List.of(duplicate, duplicate),
				List.of(first, first),
				RequestType.ensure()).isEmpty());
		assertEquals(1L, cdcEventCount("ordered-ensure"),
				"the second duplicate must see the first write and elide");

		assertTrue(api.putMulti(0,
				orderedColumn,
				List.of(duplicate, duplicate),
				List.of(first, second),
				RequestType.ensure()).isEmpty());
		assertValue(second, api.get(0, orderedColumn, duplicate, RequestType.current()));
		assertEquals(2L, cdcEventCount("ordered-ensure"),
				"the different later value must remain an ordinary Put in input order");

		var presenceKey = intKey(10);
		assertEquals(List.of(false, true), api.putMulti(0,
				orderedColumn,
				List.of(presenceKey, presenceKey),
				List.of(first, first),
				RequestType.previousPresence()));
		assertEquals(3L, cdcEventCount("ordered-ensure"),
				"the first previous-presence write must publish CDC and the equal duplicate must elide");
	}

	@Test
	void multiGetRetainsMixedStatusesAndLargeBucketsFallBackToOrdinaryWrites() {
		long directColumn = api.createColumn("mixed-statuses", fixedSchema(true));
		var cold = intKey(1);
		var hotEqual = intKey(2);
		var hotDifferent = intKey(3);
		var deleted = intKey(4);
		var same = value("same");
		var old = value("old");
		var replacement = value("replacement");
		api.put(0, directColumn, cold, same, RequestType.none());
		api.flush();
		api.put(0, directColumn, hotEqual, same, RequestType.none());
		api.put(0, directColumn, hotDifferent, old, RequestType.none());
		api.put(0, directColumn, deleted, old, RequestType.none());
		api.delete(0, directColumn, deleted, RequestType.none());

		assertTrue(api.putMulti(0,
				directColumn,
				List.of(hotEqual, hotDifferent, deleted, cold),
				List.of(same, replacement, replacement, same),
				RequestType.ensure()).isEmpty());
		assertValue(same, api.get(0, directColumn, hotEqual, RequestType.current()));
		assertValue(replacement, api.get(0, directColumn, hotDifferent, RequestType.current()));
		assertValue(replacement, api.get(0, directColumn, deleted, RequestType.current()));
		assertValue(same, api.get(0, directColumn, cold, RequestType.current()));
		var equalFromMemory = decisionCount("ensure", "elided");
		var coldFallback = decisionCount("ensure", "fallback_incomplete");
		assertTrue(equalFromMemory == 1.0 || equalFromMemory == 2.0,
				"the hot equal value must elide; a just-flushed value may still be memory-resident");
		assertEquals(1.0, decisionCount("ensure", "fallback_different"));
		assertEquals(1.0, decisionCount("ensure", "fallback_not_found"));
		assertEquals(2.0, equalFromMemory + coldFallback,
				"the cold logical equality must either be proven from memory or fall back as Incomplete");

		long bucketedColumn = api.createColumn("large-bucket", ColumnSchema.of(
				IntList.of(1),
				ObjectList.of(ColumnHashType.ALLSAME8),
				true));
		var largeKey = bucketKey(8, "large");
		var smallKey = bucketKey(8, "small");
		var oversized = Buf.wrap(new byte[(int) MAX_PROBE_BYTES + 1]);
		api.put(0, bucketedColumn, largeKey, oversized, RequestType.none());
		assertTrue(api.putMulti(0,
				bucketedColumn,
				List.of(smallKey),
				List.of(same),
				RequestType.ensure()).isEmpty());
		assertValue(oversized, api.get(0, bucketedColumn, largeKey, RequestType.current()));
		assertValue(same, api.get(0, bucketedColumn, smallKey, RequestType.current()));
		assertEquals(1.0, decisionCount("ensure", "bypass_oversized"));

		long dirtyBucketedColumn = api.createColumn("dirty-large-bucket", ColumnSchema.of(
				IntList.of(1),
				ObjectList.of(ColumnHashType.ALLSAME8),
				true));
		var dirtyProbeCalls = new ArrayList<ProbeCall>();
		embeddedDB.setWriteElisionMultiGetObserverForTesting(
				(keys, bytes) -> dirtyProbeCalls.add(new ProbeCall(keys, bytes)));
		var dirtyLargeKey = bucketKey(9, "large");
		var dirtySmallKey = bucketKey(9, "small");
		assertTrue(api.putMulti(0,
				dirtyBucketedColumn,
				List.of(dirtyLargeKey, dirtySmallKey),
				List.of(oversized, same),
				RequestType.ensure()).isEmpty());
		assertValue(oversized, api.get(0, dirtyBucketedColumn, dirtyLargeKey, RequestType.current()));
		assertValue(same, api.get(0, dirtyBucketedColumn, dirtySmallKey, RequestType.current()));
		assertEquals(2, dirtyProbeCalls.size(),
				"the later dirty bucket entry must use a fresh bounded status probe");
		assertTrue(dirtyProbeCalls.stream().allMatch(call -> call.bytes() <= MAX_PROBE_BYTES));
		assertEquals(3.0, decisionCount("ensure", "bypass_oversized"));
	}

	@Test
	void oversizedMultiValuesBypassTheProbeAndExplicitWritersKeepOrdinarySemantics() {
		long columnId = api.createColumn("oversized-and-transactions", fixedSchema(true));
		var oversized = Buf.wrap(new byte[(int) MAX_PROBE_BYTES + 1]);
		assertTrue(api.putMulti(0,
				columnId,
				List.of(intKey(1)),
				List.of(oversized),
				RequestType.ensure()).isEmpty());
		assertEquals(1.0, decisionCount("ensure", "bypass_oversized"));
		assertNull(api.put(0, columnId, intKey(11), oversized, RequestType.ensure()),
				"single Ensure remains correct for values larger than the MultiGet budget");
		assertNull(api.put(0, columnId, intKey(11), oversized, RequestType.ensure()));

		var key = intKey(2);
		var value = value("transactional");
		api.cdcCreate("transactional-ensure", null, List.of(columnId), null, java.util.OptionalLong.empty());
		long rollbackTx = api.openTransaction( java.time.Duration.ofMillis(5_000));
		assertNull(api.put(rollbackTx, columnId, key, value, RequestType.ensure()));
		assertFalse(api.get(0, columnId, key, RequestType.exists()));
		assertTrue(api.get(rollbackTx, columnId, key, RequestType.exists()));
		assertTrue(api.closeTransaction(rollbackTx, false));
		assertFalse(api.get(0, columnId, key, RequestType.exists()));
		assertEquals(0L, cdcEventCount("transactional-ensure"));

		long commitTx = api.openTransaction( java.time.Duration.ofMillis(5_000));
		assertTrue(api.putMulti(commitTx,
				columnId,
				List.of(key),
				List.of(value),
				RequestType.ensure()).isEmpty());
		assertTrue(api.closeTransaction(commitTx, true));
		assertValue(value, api.get(0, columnId, key, RequestType.current()));
		assertEquals(1L, cdcEventCount("transactional-ensure"));
		assertEquals(2.0, decisionCount("ensure", "bypass_writer"));

		long equalCommitTx = api.openTransaction( java.time.Duration.ofMillis(5_000));
		assertNull(api.put(equalCommitTx, columnId, key, value, RequestType.ensure()));
		assertTrue(api.closeTransaction(equalCommitTx, true));
		assertEquals(2L, cdcEventCount("transactional-ensure"),
				"an explicit transaction must retain ordinary Put CDC semantics even for equality");

		var updated = value("updated");
		var update = api.get(0, columnId, key, RequestType.forUpdate());
		assertNull(api.put(update.updateId(), columnId, key, updated, RequestType.ensure()));
		assertValue(updated, api.get(0, columnId, key, RequestType.current()));
		assertEquals(3L, cdcEventCount("transactional-ensure"));

		var firstConflictUpdate = api.get(0, columnId, key, RequestType.forUpdate());
		var secondConflictUpdate = api.get(0, columnId, key, RequestType.forUpdate());
		var firstConflictValue = value("first-conflict");
		var secondConflictValue = value("second-conflict");
		assertNull(api.put(firstConflictUpdate.updateId(),
				columnId,
				key,
				firstConflictValue,
				RequestType.ensure()));
		assertThrowsExactly(RocksDBRetryException.class, () -> api.put(secondConflictUpdate.updateId(),
				columnId,
				key,
				secondConflictValue,
				RequestType.ensure()));
		var retriedUpdate = api.get(secondConflictUpdate.updateId(), columnId, key, RequestType.forUpdate());
		assertValue(firstConflictValue, retriedUpdate.previous());
		assertNull(api.put(retriedUpdate.updateId(), columnId, key, secondConflictValue, RequestType.ensure()));
		assertValue(secondConflictValue, api.get(0, columnId, key, RequestType.current()));
		assertEquals(5L, cdcEventCount("transactional-ensure"));
		assertEquals(7.0, decisionCount("ensure", "bypass_writer"));
	}

	@Test
	void previousPresenceUsesTheSameHotEqualityElisionAndSuppressesDuplicateCdc() {
		long columnId = api.createColumn("previous-presence", fixedSchema(true));
		var key = intKey(1);
		var value = value("same");
		api.cdcCreate("previous-presence", null, List.of(columnId), null, java.util.OptionalLong.empty());

		assertFalse(api.put(0, columnId, key, value, RequestType.previousPresence()));
		assertTrue(api.put(0, columnId, key, value, RequestType.previousPresence()));
		assertEquals(1L, cdcEventCount("previous-presence"));
		assertEquals(1.0, decisionCount("previous_presence", "fallback_not_found"));
		assertEquals(1.0, decisionCount("previous_presence", "elided"));
	}

	@Test
	void requestTypeIdIsAppendedWithoutRenumberingExistingIds() {
		assertEquals(14, RequestType.RequestTypeId.ENSURE.ordinal());
		assertEquals(RequestType.RequestTypeId.ENSURE, RequestType.ensure().getRequestTypeId());
		assertEquals(RequestType.RequestTypeId.ALL_IN_RANGE_NO_CACHE.ordinal() + 1,
				RequestType.RequestTypeId.ENSURE.ordinal());
		assertEquals(12, exportedMetrics.find("rockserver.write.elision.decisions").counters().size(),
				"two request types times six decisions must be registered eagerly");
	}

	private long cdcEventCount(String subscription) {
		try (var events = api.cdcPoll(subscription, null, 100_000)) {
			return events.count();
		}
	}

	private double decisionCount(String requestType, String decision) {
		var counter = exportedMetrics.find("rockserver.write.elision.decisions")
				.tags("db", "ensure-write-elision", "request_type", requestType, "decision", decision)
				.counter();
		return counter == null ? 0.0 : counter.count();
	}

	private static ColumnSchema fixedSchema(boolean hasValue) {
		return ColumnSchema.of(IntList.of(Integer.BYTES), ObjectList.of(), hasValue);
	}

	private static Keys intKey(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Integer.BYTES).putInt(value).array()));
	}

	private static Keys bucketKey(int fixed, String variable) {
		return new Keys(Buf.wrap(new byte[]{(byte) fixed}),
				Buf.wrap(variable.getBytes(StandardCharsets.UTF_8)));
	}

	private static Buf value(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static void assertValue(Buf expected, Buf actual) {
		assertTrue(Utils.valueEquals(expected, actual), () -> "Expected " + expected + " but got " + actual);
	}

	private record ProbeCall(int keys, long bytes) {
	}
}
