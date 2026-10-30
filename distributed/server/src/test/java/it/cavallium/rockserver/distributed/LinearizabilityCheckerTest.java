package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTimeoutPreemptively;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.distributed.LinearizabilityChecker.Operation;
import it.cavallium.rockserver.distributed.LinearizabilityChecker.Result;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Random;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class LinearizabilityCheckerTest {

	@Test
	void updatesChainOnEachOtherInTheOrderTheirReadsAllow() {
		// Concurrent, in the order only their values give.
		var history = List.of(
				Operation.update("k", null, "1", 0, 100, OpStatus.OK),
				Operation.update("k", "2", "3", 0, 100, OpStatus.OK),
				Operation.update("k", "1", "2", 0, 100, OpStatus.OK),
				Operation.get("k", "3", 101, 102, OpStatus.OK));
		assertTrue(LinearizabilityChecker.check(history).linearizable());
	}

	@Test
	void aLostUpdateIsNotLinearizable() {
		var history = List.of(
				Operation.update("k", null, "a", 0, 100, OpStatus.OK),
				Operation.update("k", null, "b", 0, 100, OpStatus.OK));
		var result = LinearizabilityChecker.check(history);
		assertFalse(result.linearizable());
		assertTrue(result.description().contains("update(absent -> "), result.description());
	}

	@Test
	void aFailedUpdateDoesNotCountEvenIfItsReadCouldNeverHappen() {
		var history = List.of(
				Operation.update("k", "a value never written", "b", 0, 100, OpStatus.FAILED),
				Operation.get("k", null, 101, 102, OpStatus.OK));
		assertTrue(LinearizabilityChecker.check(history).linearizable());
	}

	@Test
	void aSuccessfulCloseIsAReadAfterTheCommitItReadFrom() {
		// The close ends before the commit is acknowledged, but after it took effect: fine.
		var fine = List.of(
				Operation.update("k", null, "a", 0, 100, OpStatus.OK),
				Operation.get("k", "a", 10, 50, OpStatus.OK));
		assertTrue(LinearizabilityChecker.check(fine).linearizable());
		// A write acknowledged before the close started must be visible to it.
		var stale = List.of(
				Operation.update("k", null, "a", 0, 10, OpStatus.OK),
				Operation.put("k", "w", 20, 30, OpStatus.OK),
				Operation.get("k", "a", 40, 50, OpStatus.OK));
		assertFalse(LinearizabilityChecker.check(stale).linearizable());
	}

	@Test
	void anUnknownUpdateTakesEffectOnlyWhereItsReadAllows() {
		// An unknown update read a, so it cannot take effect once the register holds b.
		var legal = List.of(
				Operation.put("k", "a", 0, 10, OpStatus.OK),
				Operation.update("k", "a", "u", 11, 12, OpStatus.UNKNOWN),
				Operation.get("k", "u", 20, 30, OpStatus.OK));
		assertTrue(LinearizabilityChecker.check(legal).linearizable());
		var illegal = List.of(
				Operation.put("k", "a", 0, 10, OpStatus.OK),
				Operation.update("k", "a", "u", 11, 12, OpStatus.UNKNOWN),
				Operation.put("k", "b", 13, 14, OpStatus.OK),
				Operation.get("k", "b", 15, 16, OpStatus.OK),
				Operation.get("k", "u", 20, 30, OpStatus.OK));
		assertFalse(LinearizabilityChecker.check(illegal).linearizable());
	}

	@Test
	void anUnknownUpdateThatAKeptOneReadFromIsKept() {
		var history = List.of(
				Operation.update("k", null, "1", 0, 10, OpStatus.UNKNOWN),
				Operation.update("k", "1", "2", 11, 12, OpStatus.UNKNOWN),
				Operation.get("k", "2", 20, 30, OpStatus.OK));
		assertTrue(LinearizabilityChecker.check(history).linearizable());
	}

	@Test
	void anUpdateIsRecordedByTheHistory() {
		var history = new History();
		var ok = history.invokeUpdateAt("k", null, "a", System.nanoTime());
		ok.complete(OpStatus.OK);
		var failed = history.invokeUpdateAt("k", "x", "y", System.nanoTime());
		failed.complete(OpStatus.FAILED);
		var get = history.invokeGetAt("k", System.nanoTime());
		get.complete(OpStatus.OK, "a");
		var operations = history.operations();
		assertEquals(Operation.Kind.UPDATE, operations.getFirst().kind());
		assertEquals(null, operations.getFirst().read());
		assertEquals("x", operations.get(1).read());
		assertTrue(LinearizabilityChecker.check(operations).linearizable());
	}

	private static final String K = "k";

	@Test
	void sequentialHistoryIsLinearizable() {
		assertLinearizable(List.of(
				get(null, 0, 1),
				put("a", 2, 3),
				get("a", 4, 5),
				put("b", 6, 7),
				get("b", 8, 9)));
	}

	@Test
	void readsConcurrentWithWriteMaySeeEitherValue() {
		assertLinearizable(List.of(
				put("a", 0, 1),
				put("b", 2, 20),
				get("a", 3, 4),
				get("b", 5, 6),
				get("b", 7, 8)));
		// Concurrent writes may take effect in either order.
		assertLinearizable(List.of(
				put("a", 0, 10),
				put("b", 1, 11),
				get("b", 2, 12),
				get("a", 13, 14)));
	}

	@Test
	void readGoingBackToOldValueIsNotLinearizable() {
		var result = check(List.of(
				put("a", 0, 1),
				put("b", 2, 20),
				get("b", 3, 4),
				get("a", 5, 6)));
		assertNotLinearizable(result);
	}

	@Test
	void staleReadAfterCompletedWriteIsNotLinearizable() {
		var result = check(List.of(
				put("a", 0, 1),
				put("b", 2, 3),
				get("a", 4, 5)));
		assertNotLinearizable(result);
		assertTrue(result.description().contains("get() -> 'a'"), result.description());
	}

	@Test
	void readOfValueNeverWrittenIsNotLinearizable() {
		assertNotLinearizable(check(List.of(
				put("a", 0, 1),
				get("x", 2, 3))));
		assertNotLinearizable(check(List.of(get("x", 0, 1))));
	}

	@Test
	void unknownWriteObservedLaterIsLinearizable() {
		assertLinearizable(List.of(
				put("a", 0, 1),
				Operation.put(K, "b", 2, 0, OpStatus.UNKNOWN),
				get("a", 10, 11),
				get("b", 100, 101),
				get("b", 102, 103)));
	}

	@Test
	void unknownWriteNeverObservedIsLinearizable() {
		assertLinearizable(List.of(
				put("a", 0, 1),
				Operation.put(K, "b", 2, 0, OpStatus.UNKNOWN),
				Operation.put(K, "c", 3, 0, OpStatus.UNKNOWN),
				get("a", 10, 11),
				get("a", 100, 101)));
	}

	@Test
	void unknownWriteCannotBeObservedBeforeItsInvocation() {
		assertNotLinearizable(check(List.of(
				get("b", 0, 1),
				Operation.put(K, "b", 5, 0, OpStatus.UNKNOWN))));
	}

	@Test
	void unknownWriteTakesEffectOnlyOnce() {
		assertNotLinearizable(check(List.of(
				Operation.put(K, "a", 0, 0, OpStatus.UNKNOWN),
				get("a", 1, 2),
				put("b", 3, 4),
				get("a", 5, 6))));
	}

	@Test
	void failedWriteHasNoEffect() {
		assertLinearizable(List.of(
				put("a", 0, 1),
				Operation.put(K, "b", 2, 3, OpStatus.FAILED),
				get("a", 4, 5)));
		assertNotLinearizable(check(List.of(
				put("a", 0, 1),
				Operation.put(K, "b", 2, 3, OpStatus.FAILED),
				get("b", 4, 5))));
	}

	@Test
	void unknownAndFailedReadsAreIgnored() {
		assertLinearizable(List.of(
				put("a", 0, 1),
				Operation.get(K, "garbage", 2, 0, OpStatus.UNKNOWN),
				Operation.get(K, "garbage", 3, 4, OpStatus.FAILED),
				get("a", 5, 6)));
	}

	@Test
	void keysAreIndependentRegisters() {
		var history = List.of(
				Operation.put("x", "1", 0, 1, OpStatus.OK),
				Operation.put("y", "1", 0, 1, OpStatus.OK),
				Operation.get("x", "1", 2, 3, OpStatus.OK),
				Operation.get("y", null, 2, 3, OpStatus.OK));
		var result = check(history);
		assertNotLinearizable(result);
		assertEquals("y", result.key());
	}

	@Test
	void historyRecorderFeedsTheChecker() throws Exception {
		var history = new History();
		var register = new Object();
		String[] value = {null};
		int threads = 4;
		var done = new CountDownLatch(threads);
		for (int t = 0; t < threads; t++) {
			int thread = t;
			Thread.ofVirtual().start(() -> {
				for (int i = 0; i < 50; i++) {
					if (i % 2 == 0) {
						var newValue = thread + "-" + i;
						var op = history.invokePut(K, newValue);
						synchronized (register) {
							value[0] = newValue;
						}
						op.complete(OpStatus.OK);
					} else {
						var op = history.invokeGet(K);
						String read;
						synchronized (register) {
							read = value[0];
						}
						op.complete(OpStatus.OK, read);
					}
				}
				done.countDown();
			});
		}
		assertTrue(done.await(10, TimeUnit.SECONDS));
		history.invokePut(K, "never-completed");
		var operations = history.operations();
		assertEquals(threads * 50 + 1, operations.size());
		assertEquals(OpStatus.UNKNOWN, operations.getLast().status());
		assertLinearizable(operations);
	}

	@Test
	void checksHundredsOfConcurrentOperationsQuickly() {
		var random = new Random(1234);
		var history = new ArrayList<Operation>();
		for (var key : List.of("k1", "k2", "k3")) {
			history.addAll(simulateRegister(key, 4, 50, random));
		}
		assertEquals(600, history.size());
		assertTimeoutPreemptively(Duration.ofSeconds(5), () -> assertLinearizable(history));

		// Corrupt one read in the middle of k2: the checker must explore and reject it, also quickly.
		var corrupted = new ArrayList<>(history);
		int index = indexOfMiddleRead(corrupted, "k2");
		var read = corrupted.get(index);
		corrupted.set(index, Operation.get("k2", "never-written", read.invokeNanos(), read.completeNanos(),
				OpStatus.OK));
		var result = assertTimeoutPreemptively(Duration.ofSeconds(5), () -> check(corrupted));
		assertNotLinearizable(result);
		assertEquals("k2", result.key());
	}

	/**
	 * A linearizable history by construction: {@code clients} clients each run {@code opsPerClient} operations
	 * back to back on one register; each operation takes effect at a random instant within its interval. Some
	 * writes have an unknown outcome (and take effect or not), some reads fail.
	 */
	private static List<Operation> simulateRegister(String key, int clients, int opsPerClient, Random random) {
		record Planned(Operation.Kind kind, String value, long invoke, long complete, long effect, OpStatus status) {}
		var planned = new ArrayList<Planned>();
		for (int client = 0; client < clients; client++) {
			long time = random.nextInt(10_000);
			for (int i = 0; i < opsPerClient; i++) {
				long duration = 1_000 + random.nextInt(10_000);
				long effect = time + random.nextLong(duration + 1);
				var kind = random.nextBoolean() ? Operation.Kind.PUT : Operation.Kind.GET;
				var status = OpStatus.OK;
				int roll = random.nextInt(20);
				if (roll == 0) {
					status = kind == Operation.Kind.PUT ? OpStatus.UNKNOWN : OpStatus.FAILED;
				}
				var value = kind == Operation.Kind.PUT ? key + "-c" + client + "-" + i : null;
				planned.add(new Planned(kind, value, time, time + duration, effect, status));
				time += duration + random.nextInt(5_000);
			}
		}
		// Replay the effects in order to compute what each read returned.
		var byEffect = new ArrayList<>(planned);
		byEffect.sort(Comparator.comparingLong(Planned::effect));
		var result = new ArrayList<Operation>();
		String register = null;
		for (var op : byEffect) {
			if (op.kind() == Operation.Kind.PUT) {
				boolean applied = op.status() == OpStatus.OK || random.nextBoolean();
				if (applied) {
					register = op.value();
				}
				result.add(Operation.put(key, op.value(), op.invoke(), op.complete(), op.status()));
			} else {
				var read = op.status() == OpStatus.OK ? register : null;
				result.add(Operation.get(key, read, op.invoke(), op.complete(), op.status()));
			}
		}
		return result;
	}

	private static int indexOfMiddleRead(List<Operation> history, String key) {
		var reads = new ArrayList<Integer>();
		for (int i = 0; i < history.size(); i++) {
			var op = history.get(i);
			if (op.key().equals(key) && op.kind() == Operation.Kind.GET && op.status() == OpStatus.OK) {
				reads.add(i);
			}
		}
		return reads.get(reads.size() / 2);
	}

	private static Operation put(String value, long invoke, long complete) {
		return Operation.put(K, value, invoke, complete, OpStatus.OK);
	}

	private static Operation get(String value, long invoke, long complete) {
		return Operation.get(K, value, invoke, complete, OpStatus.OK);
	}

	private static Result check(List<Operation> history) {
		return LinearizabilityChecker.check(history);
	}

	private static void assertLinearizable(List<Operation> history) {
		var result = check(history);
		assertTrue(result.linearizable(), result.description());
	}

	private static void assertNotLinearizable(Result result) {
		assertFalse(result.linearizable(), "expected a violation");
		assertTrue(result.description().contains("not linearizable"), result.description());
	}
}
