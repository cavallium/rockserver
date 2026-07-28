package it.cavallium.rockserver.core.impl.test;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.cdc.CdcBatch;
import it.cavallium.rockserver.core.impl.EmbeddedDB;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import reactor.core.publisher.Flux;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;

import static org.junit.jupiter.api.Assertions.*;

import org.junit.jupiter.api.Timeout;

@Timeout(30)
class CdcRobustnessTest {

    @TempDir
    Path tempDir;

    private EmbeddedDB db;
    private long columnId;
    private final String subId = "robust-sub";

    @BeforeEach
    void setUp() throws IOException, RocksDBException {
        db = new EmbeddedDB(tempDir, "test-db", null);
        var schema = ColumnSchema.of(
                IntArrayList.of(4), // Int key
                new ObjectArrayList<>(), 
                true, 
                null, null, null
        );
        columnId = db.createColumn("data", schema);
        db.cdcCreate(subId, null, List.of(columnId), false);
    }

    @AfterEach
    void tearDown() throws IOException {
        if (db != null) {
            db.closeTesting();
        }
    }

    private byte[] intToBytes(int i) {
        return java.nio.ByteBuffer.allocate(4).putInt(i).array();
    }

    @Test
    void testFutureSequenceGracefulHandling() throws RocksDBException {
        // Write 1 item to establish a baseline
        db.put(0, columnId, new Keys(new Buf[]{Buf.wrap(intToBytes(1))}), Buf.wrap("A".getBytes()), RequestType.none());

        CdcBatch b1 = db.cdcPollBatchAsyncInternal(subId, null, 10).block();
        assertNotNull(b1);
        long nextSeq = b1.nextSeq();

        // Ask for far future
        long futureSeq = nextSeq + 1_000_000;
        
        // This should return empty list immediately, NOT throw "Requested sequence not yet written"
        CdcBatch b2 = db.cdcPollBatchAsyncInternal(subId, futureSeq, 10).block();
        assertNotNull(b2);
        assertTrue(b2.events().isEmpty());
		assertEquals(0L, db.getCdcLagForTesting());
		db.put(0, columnId, key(2), Buf.wrap(new byte[] {2}), RequestType.none());
		assertEquals(0L, db.getCdcLagForTesting(),
				"an explicit future cursor must not manufacture lag as the tail grows below it");
    }

    @Test
    void testMemoryLimitEnforcement() throws RocksDBException {
        // 16MB is the limit.
        // We write 5 items of 5MB each.
        int valSize = 5 * 1024 * 1024;
        byte[] bigVal = new byte[valSize];
        
        List<Keys> keys = new ArrayList<>();
        List<Buf> values = new ArrayList<>();
        for (int i = 0; i < 5; i++) {
            keys.add(new Keys(new Buf[]{Buf.wrap(intToBytes(i))}));
            values.add(Buf.wrap(bigVal));
        }

        db.putBatch(columnId, Flux.just(new KVBatch.KVBatchRef(keys, values)), PutBatchMode.WRITE_BATCH);

        // Poll. Should get 3 items (15MB < 16MB). 4th would make it 20MB.
        CdcBatch b1 = db.cdcPollBatchAsyncInternal(subId, null, 100).block();
        assertEquals(3, b1.events().size(), "Should stop after 3 items (15MB)");
        
        // Resume
        CdcBatch b2 = db.cdcPollBatchAsyncInternal(subId, b1.nextSeq(), 100).block();
        assertEquals(2, b2.events().size(), "Should get remaining 2 items");
    }

    @Test
    void testHugeItemAllowed() throws RocksDBException {
        // 1 item of 20MB (exceeds 16MB limit)
        int valSize = 20 * 1024 * 1024;
        byte[] bigVal = new byte[valSize];

        db.put(0, columnId, new Keys(new Buf[]{Buf.wrap(intToBytes(1))}), Buf.wrap(bigVal), RequestType.none());

        CdcBatch b1 = db.cdcPollBatchAsyncInternal(subId, null, 100).block();
        assertEquals(1, b1.events().size(), "Should allow single huge item to ensure progress");
    }
    
    @Test
    void testMaxEventsCap() throws RocksDBException {
        // Write 101 items
         List<Keys> keys = new ArrayList<>();
        List<Buf> values = new ArrayList<>();
        for (int i = 0; i < 110; i++) {
            keys.add(new Keys(new Buf[]{Buf.wrap(intToBytes(i))}));
            values.add(Buf.wrap("v".getBytes()));
        }
        db.putBatch(columnId, Flux.just(new KVBatch.KVBatchRef(keys, values)), PutBatchMode.WRITE_BATCH);
        
        // Request with huge limit, but cap is 10,000. 
        // Hard to test cap 10k without writing 10k. 
        // But we can test that it respects the passed limit if smaller.
        // Just a sanity check.
        
        CdcBatch b1 = db.cdcPollBatchAsyncInternal(subId, null, 50).block();
        assertEquals(50, b1.events().size());
    }

	@Test
	void configuredMutationAndByteQuantaBoundEveryContinuation() throws Exception {
		reopenWithConfig("configured-quanta", """
				database.parallelism.workload.cdc-quantum-max-mutations = 3
				database.parallelism.workload.cdc-quantum-max-bytes = 12B
				database.parallelism.workload.cdc-quantum-max-duration = PT1S
				""");
		long selected = createColumn("selected");
		long ignored = createColumn("ignored");
		long start = db.cdcCreate("configured", null, List.of(selected), false);

		long ignoredTx = db.openTransaction(10_000);
		for (int i = 0; i < 7; i++) {
			db.put(ignoredTx, ignored, key(i), Buf.wrap(new byte[0]), RequestType.none());
		}
		assertTrue(db.closeTransaction(ignoredTx, true));
		for (int i = 0; i < 4; i++) {
			db.put(0, selected, key(100 + i), Buf.wrap(new byte[] {1, 2, 3, 4}), RequestType.none());
		}

		var mutationQuanta = new ArrayList<Long>();
		var byteQuanta = new ArrayList<Long>();
		db.setCdcQuantumObserversForTesting(mutationQuanta::add, byteQuanta::add);
		CdcBatch batch;
		try {
			batch = db.cdcPollBatchAsyncInternal("configured", start, 100)
					.block(Duration.ofSeconds(10));
		} finally {
			db.setCdcQuantumObserversForTesting(null, null);
		}

		assertNotNull(batch);
		assertEquals(4, batch.events().size());
		assertTrue(mutationQuanta.size() >= 4, "the logical poll must reacquire CDC admission between quanta");
		assertTrue(mutationQuanta.stream().allMatch(value -> value > 0L && value <= 3L));
		assertTrue(mutationQuanta.stream().anyMatch(value -> value == 3L));
		assertEquals(mutationQuanta.size(), byteQuanta.size());
		for (int i = 0; i < byteQuanta.size(); i++) {
			assertTrue(byteQuanta.get(i) <= 12L || mutationQuanta.get(i) == 1L,
					"only one indivisible mutation may exceed the configured byte quantum");
		}
		assertTrue(byteQuanta.stream().anyMatch(value -> value == 12L));
		assertEquals(0, db.getActiveCdcPollCursorCount());
	}

	@Test
	void configuredDurationForcesFilteredWalContinuations() throws Exception {
		reopenWithConfig("duration-quanta", """
				database.parallelism.workload.cdc-quantum-max-mutations = 100
				database.parallelism.workload.cdc-quantum-max-bytes = 1MiB
				database.parallelism.workload.cdc-quantum-max-duration = PT0.000000001S
				""");
		long selected = createColumn("selected");
		long ignored = createColumn("ignored");
		long start = db.cdcCreate("duration", null, List.of(selected), false);
		long tx = db.openTransaction(10_000);
		for (int i = 0; i < 6; i++) {
			db.put(tx, ignored, key(i), Buf.wrap(new byte[] {1}), RequestType.none());
		}
		db.put(tx, selected, key(99), Buf.wrap(new byte[] {9}), RequestType.none());
		assertTrue(db.closeTransaction(tx, true));

		var mutationQuanta = new ArrayList<Long>();
		db.setCdcQuantumObserversForTesting(mutationQuanta::add, null);
		try {
			var batch = db.cdcPollBatchAsyncInternal("duration", start, 10)
					.block(Duration.ofSeconds(10));
			assertNotNull(batch);
			assertEquals(1, batch.events().size());
		} finally {
			db.setCdcQuantumObserversForTesting(null, null);
		}
		assertTrue(mutationQuanta.size() >= 7);
		assertTrue(mutationQuanta.stream().allMatch(value -> value == 1L),
				"the one-nanosecond duration must yield after the first sequence-consuming mutation");
	}

	@Test
	void midBatchCursorDoesNotRegressAcrossSmallConfiguredQuanta() throws Exception {
		reopenWithConfig("mid-batch-cursor", """
				database.parallelism.workload.cdc-quantum-max-mutations = 2
				database.parallelism.workload.cdc-quantum-max-duration = PT1S
				""");
		long selected = createColumn("selected");
		long start = db.cdcCreate("mid-batch", null, List.of(selected), false);
		long tx = db.openTransaction(10_000);
		for (int i = 0; i < 10; i++) {
			db.put(tx, selected, key(i), Buf.wrap(new byte[] {(byte) i}), RequestType.none());
		}
		assertTrue(db.closeTransaction(tx, true));

		var complete = db.cdcPollBatchAsyncInternal("mid-batch", start, 100)
				.block(Duration.ofSeconds(10));
		assertNotNull(complete);
		assertEquals(10, complete.events().size());
		long resumeAt = complete.events().get(6).seq();
		var resumed = db.cdcPollBatchAsyncInternal("mid-batch", resumeAt, 100)
				.block(Duration.ofSeconds(10));
		assertNotNull(resumed);
		assertEquals(complete.events().subList(6, 10), resumed.events());
		assertTrue(resumed.nextSeq() >= complete.nextSeq());
	}

	@Test
	void lagIsMaximumAcrossSubscriptionsAndGrowsWhileOneIsIdle() throws Exception {
		db.cdcDelete(subId);
		long selected = columnId;
		long startA = db.cdcCreate("lag-a", null, List.of(selected), false);
		long startB = db.cdcCreate("lag-b", null, List.of(selected), false);
		for (int i = 0; i < 3; i++) {
			db.put(0, selected, key(i), Buf.wrap(new byte[] {(byte) i}), RequestType.none());
		}
		long initialLag = db.getCdcLagForTesting();
		assertTrue(initialLag > 0L);

		var first = db.cdcPollBatchAsyncInternal("lag-a", startA, 100).block(Duration.ofSeconds(10));
		assertNotNull(first);
		assertEquals(3, first.events().size());
		assertTrue(db.getCdcLagForTesting() > 0L,
				"the caught-up subscription must not hide the lagging subscription");

		var second = db.cdcPollBatchAsyncInternal("lag-b", startB, 100).block(Duration.ofSeconds(10));
		assertNotNull(second);
		assertEquals(3, second.events().size());
		assertEquals(0L, db.getCdcLagForTesting());

		db.put(0, selected, key(4), Buf.wrap(new byte[] {4}), RequestType.none());
		assertTrue(db.getCdcLagForTesting() > 0L, "idle subscription lag must grow with the published tail");
		var caughtUpA = db.cdcPollBatchAsyncInternal("lag-a", first.nextSeq(), 100).block(Duration.ofSeconds(10));
		assertNotNull(caughtUpA);
		assertTrue(db.getCdcLagForTesting() > 0L, "lag-b must remain the database maximum");
		var caughtUpB = db.cdcPollBatchAsyncInternal("lag-b", second.nextSeq(), 100).block(Duration.ofSeconds(10));
		assertNotNull(caughtUpB);
		assertEquals(0L, db.getCdcLagForTesting());
	}

	@Test
	void filteredEmptyPageAdvancesLagWithoutInventingAnEvent() throws Exception {
		db.cdcDelete(subId);
		long ignored = createColumn("lag-ignored");
		long start = db.cdcCreate("filtered-lag", null, List.of(columnId), false);
		db.put(0, ignored, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
		assertTrue(db.getCdcLagForTesting() > 0L);

		var batch = db.cdcPollBatchAsyncInternal("filtered-lag", start, 100).block(Duration.ofSeconds(10));
		assertNotNull(batch);
		assertTrue(batch.events().isEmpty());
		assertTrue(batch.nextSeq() > start);
		assertEquals(0L, db.getCdcLagForTesting());
	}

	@Test
	void partiallyCompletedStreamingBatchAdvancesPublishedTail() {
		long previousTail = db.getCdcPublishedTailForTesting();
		var largeValue = Buf.wrap(new byte[4 * 1024 * 1024 + 1]);
		var batch = new KVBatch.KVBatchRef(List.of(key(7)), List.of(largeValue));
		var write = db.putBatchInternal(columnId,
				Flux.<KVBatch>concat(Flux.just(batch), Flux.never()),
				PutBatchMode.WRITE_BATCH);
		try {
			await(() -> db.getCdcPublishedTailForTesting() > previousTail);
			assertTrue(db.getCdcLagForTesting() > 0L,
					"a native batch flush must become visible before the source publisher completes");
		} finally {
			write.cancel(true);
		}
		await(() -> db.getPendingOpsCount() == 0L);
	}

	@Test
	void fixedTailDoesNotChaseConcurrentPublication() throws Exception {
		long start = db.cdcCreate("fixed-tail", null, List.of(columnId), false);
		var appended = new AtomicBoolean();
		db.setCdcPollTailCapturedObserverForTesting(() -> {
			if (appended.compareAndSet(false, true)) {
				db.put(0, columnId, key(1), Buf.wrap(new byte[] {1}), RequestType.none());
			}
		});
		CdcBatch first;
		try {
			first = db.cdcPollBatchAsyncInternal("fixed-tail", start, 100).block(Duration.ofSeconds(10));
		} finally {
			db.setCdcPollTailCapturedObserverForTesting(null);
		}
		assertNotNull(first);
		assertTrue(appended.get());
		assertTrue(first.events().isEmpty());

		var second = db.cdcPollBatchAsyncInternal("fixed-tail", start, 100).block(Duration.ofSeconds(10));
		assertNotNull(second);
		assertEquals(1, second.events().size());
	}

	@Test
	void retainedCdcCursorExpiresAndDrains() throws Exception {
		reopenWithConfig("cursor-age", """
				database.parallelism.workload.retained-snapshot-maximum-age = PT0.05S
				database.parallelism.workload.cdc-quantum-max-mutations = 1
				database.parallelism.workload.cdc-quantum-max-duration = PT1S
				""");
		long selected = createColumn("selected");
		long ignored = createColumn("ignored");
		long start = db.cdcCreate("age", null, List.of(selected), false);
		long tx = db.openTransaction(10_000);
		for (int i = 0; i < 3; i++) {
			db.put(tx, ignored, key(i), Buf.wrap(new byte[] {1}), RequestType.none());
		}
		db.put(tx, selected, key(9), Buf.wrap(new byte[] {9}), RequestType.none());
		assertTrue(db.closeTransaction(tx, true));

		var continuationStarted = new CountDownLatch(1);
		var releaseContinuation = new CountDownLatch(1);
		db.setCdcContinuationObserverForTesting(() -> {
			continuationStarted.countDown();
			while (releaseContinuation.getCount() != 0L) {
				LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
			}
		});
		var poll = db.cdcPollBatchAsyncInternal("age", start, 10).toFuture();
		try {
			assertTrue(continuationStarted.await(5, TimeUnit.SECONDS));
			await(() -> db.getActiveCdcPollCursorCount() == 0);
		} finally {
			releaseContinuation.countDown();
			db.setCdcContinuationObserverForTesting(null);
		}
		var failure = assertThrows(java.util.concurrent.ExecutionException.class,
				() -> poll.get(10, TimeUnit.SECONDS));
		var error = assertInstanceOf(it.cavallium.rockserver.core.common.RocksDBException.class,
				failure.getCause());
		assertEquals(it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.READ_DEADLINE_EXCEEDED,
				error.getErrorUniqueId());
		await(() -> db.getActiveCdcPollCursorCount() == 0 && db.getPendingOpsCount() == 0L);
	}

	@Test
	void latencyProgressesBetweenCdcQuanta() throws Exception {
		reopenWithConfig("chat-progress", """
				database.parallelism.workload.cdc-quantum-max-mutations = 1
				database.parallelism.workload.cdc-quantum-max-duration = PT1S
				""");
		long selected = createColumn("selected");
		long ignored = createColumn("ignored");
		long start = db.cdcCreate("chat-progress", null, List.of(selected), false);
		long tx = db.openTransaction(10_000);
		for (int i = 0; i < 5; i++) {
			db.put(tx, ignored, key(i), Buf.wrap(new byte[] {1}), RequestType.none());
		}
		db.put(tx, selected, key(9), Buf.wrap(new byte[] {9}), RequestType.none());
		assertTrue(db.closeTransaction(tx, true));

		var continuationStarted = new CountDownLatch(1);
		var releaseContinuation = new CountDownLatch(1);
		var first = new AtomicBoolean(true);
		db.setCdcContinuationObserverForTesting(() -> {
			if (first.compareAndSet(true, false)) {
				continuationStarted.countDown();
				while (releaseContinuation.getCount() != 0L) {
					LockSupport.parkNanos(TimeUnit.MILLISECONDS.toNanos(1));
				}
			}
		});
		var poll = db.cdcPollBatchAsyncInternal("chat-progress", start, 10).toFuture();
		try {
			assertTrue(continuationStarted.await(5, TimeUnit.SECONDS));
			var latencyProgress = new CountDownLatch(1);
			db.getScheduler().executor(WorkloadProfile.LATENCY,
					OperationFamily.POINT_LOOKUP,
					System.currentTimeMillis() + 5_000L).execute(latencyProgress::countDown);
			assertTrue(latencyProgress.await(5, TimeUnit.SECONDS),
					"a retained CDC cursor between quanta must not monopolize chat capacity");
		} finally {
			releaseContinuation.countDown();
			db.setCdcContinuationObserverForTesting(null);
		}
		var batch = poll.get(10, TimeUnit.SECONDS);
		assertEquals(1, batch.events().size());
		assertEquals(0, db.getActiveCdcPollCursorCount());
	}

	private void reopenWithConfig(String name, String configuration) throws Exception {
		db.closeTesting();
		db = null;
		Path config = tempDir.resolve(name + ".conf");
		Files.writeString(config, configuration);
		db = new EmbeddedDB(tempDir.resolve(name + "-db"), name, config);
		columnId = 0L;
	}

	private long createColumn(String name) {
		return db.createColumn(name, ColumnSchema.of(
				IntArrayList.of(Integer.BYTES),
				new ObjectArrayList<>(),
				true,
				null,
				null,
				null));
	}

	private Keys key(int value) {
		return new Keys(new Buf[] {Buf.wrap(intToBytes(value))});
	}

	private static void await(BooleanSupplier condition) {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.onSpinWait();
		}
		assertTrue(condition.getAsBoolean());
	}
}
