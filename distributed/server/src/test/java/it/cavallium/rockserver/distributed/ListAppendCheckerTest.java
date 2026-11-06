package it.cavallium.rockserver.distributed;

import static it.cavallium.rockserver.distributed.ListAppendChecker.MicroOp.append;
import static it.cavallium.rockserver.distributed.ListAppendChecker.MicroOp.read;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.distributed.ListAppendChecker.Anomaly;
import it.cavallium.rockserver.distributed.ListAppendChecker.MicroOp;
import it.cavallium.rockserver.distributed.ListAppendChecker.Outcome;
import it.cavallium.rockserver.distributed.ListAppendChecker.Transaction;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import org.junit.jupiter.api.Test;

/** Known anomalous histories, each found and classified, and serial histories, found clean. */
class ListAppendCheckerTest {

	private static Transaction ok(String id, MicroOp... ops) {
		return new Transaction(id, Outcome.COMMITTED, List.of(ops));
	}

	private static Transaction aborted(String id, MicroOp... ops) {
		return new Transaction(id, Outcome.ABORTED, List.of(ops));
	}

	private static Transaction unknown(String id, MicroOp... ops) {
		return new Transaction(id, Outcome.UNKNOWN, List.of(ops));
	}

	private static Set<Anomaly> anomalies(Transaction... history) {
		var result = ListAppendChecker.check(List.of(history));
		return result.anomalies().keySet();
	}

	@Test
	void serialHistoryIsClean() {
		var result = ListAppendChecker.check(List.of(
				ok("T1", append("x", 1), append("y", 1)),
				ok("T2", read("x", List.of(1L)), append("x", 2)),
				ok("T3", read("x", List.of(1L, 2L)), read("y", List.of(1L))),
				ok("T4", read("y", List.of(1L)), append("y", 2), read("y", List.of(1L, 2L)))));
		assertTrue(result.valid(), result.description());
		assertEquals(4, result.transactions());
	}

	@Test
	void writeCycleIsG0() {
		// T1 is before T2 on x, after it on y.
		var found = anomalies(
				ok("T1", append("x", 1), append("y", 1)),
				ok("T2", append("x", 2), append("y", 2)),
				ok("T3", read("x", List.of(1L, 2L)), read("y", List.of(2L, 1L))));
		assertTrue(found.contains(Anomaly.G0), found::toString);
	}

	@Test
	void circularInformationFlowIsG1c() {
		// Each read the other's append.
		var found = anomalies(
				ok("T1", append("x", 1), read("y", List.of(1L))),
				ok("T2", append("y", 1), read("x", List.of(1L))));
		assertEquals(Set.of(Anomaly.G1C), found);
	}

	@Test
	void readSkewIsGSingle() {
		// T1 saw T2's y but not T2's x.
		var found = anomalies(
				ok("T1", read("x", List.of()), read("y", List.of(1L))),
				ok("T2", append("x", 1), append("y", 1)),
				ok("T3", read("x", List.of(1L))));
		assertEquals(Set.of(Anomaly.G_SINGLE), found);
	}

	@Test
	void writeSkewIsG2AndAllowedBySnapshotIsolation() {
		var result = ListAppendChecker.check(List.of(
				ok("T1", read("x", List.of()), append("y", 1)),
				ok("T2", read("y", List.of()), append("x", 1)),
				ok("T3", read("x", List.of(1L)), read("y", List.of(1L)))));
		assertEquals(Set.of(Anomaly.G2), result.anomalies().keySet(), result.description());
		assertTrue(result.snapshotIsolated());
		assertFalse(result.valid());
	}

	@Test
	void abortedReadIsG1a() {
		var found = anomalies(
				aborted("T1", append("x", 1)),
				ok("T2", read("x", List.of(1L))));
		assertEquals(Set.of(Anomaly.G1A), found);
	}

	@Test
	void intermediateReadIsG1b() {
		var found = anomalies(
				ok("T1", append("x", 1), append("x", 2)),
				ok("T2", read("x", List.of(1L))),
				ok("T3", read("x", List.of(1L, 2L))));
		assertEquals(Set.of(Anomaly.G1B), found);
	}

	@Test
	void divergingReadsAreIncompatible() {
		var found = anomalies(
				ok("T1", append("x", 1)),
				ok("T2", append("x", 2)),
				ok("T3", read("x", List.of(1L, 2L))),
				ok("T4", read("x", List.of(2L, 1L))));
		assertTrue(found.contains(Anomaly.INCOMPATIBLE_ORDER), found::toString);
	}

	@Test
	void duplicatesGarbageAndInternalReads() {
		assertTrue(anomalies(ok("T1", append("x", 1)), ok("T2", read("x", List.of(1L, 1L))))
				.contains(Anomaly.DUPLICATE));
		assertEquals(Set.of(Anomaly.GARBAGE), anomalies(ok("T1", read("x", List.of(7L)))));
		assertEquals(Set.of(Anomaly.INTERNAL), anomalies(ok("T1", append("x", 1), read("x", List.of()))));
	}

	@Test
	void unknownTransactionCommittedIfSeenIgnoredOtherwise() {
		// Seen: it takes part in the cycle like a committed one.
		var found = anomalies(
				unknown("T1", append("x", 1), read("y", List.of(1L))),
				ok("T2", append("y", 1), read("x", List.of(1L))));
		assertTrue(found.isEmpty(), "the unknown transaction's read is not trusted: " + found);
		found = anomalies(
				unknown("T1", append("x", 1), append("y", 1)),
				ok("T2", append("x", 2), append("y", 2)),
				ok("T3", read("x", List.of(1L, 2L)), read("y", List.of(2L, 1L))));
		assertTrue(found.contains(Anomaly.G0), found::toString);
		// Never seen: no effect, and no anomaly.
		assertTrue(anomalies(unknown("T1", append("x", 1)), ok("T2", read("x", List.of()))).isEmpty());
	}

	@Test
	void sameElementAppendedTwiceIsAHistoryError() {
		assertThrows(IllegalArgumentException.class, () -> anomalies(
				ok("T1", append("x", 1)), ok("T2", append("x", 1))));
	}

	/** A random serial execution: whatever the interleaving of keys and reads, nothing is reported. */
	@Test
	void randomSerialExecutionsAreClean() {
		var random = new Random(7);
		for (int round = 0; round < 50; round++) {
			var state = new HashMap<String, List<Long>>();
			var history = new ArrayList<Transaction>();
			long element = 0;
			for (int t = 0; t < 40; t++) {
				var ops = new ArrayList<MicroOp>();
				var local = new HashMap<String, List<Long>>();
				for (int i = 0, count = 1 + random.nextInt(4); i < count; i++) {
					var key = "k" + random.nextInt(3);
					var list = local.computeIfAbsent(key, k -> new ArrayList<>(state.getOrDefault(k, List.of())));
					if (random.nextBoolean()) {
						ops.add(append(key, ++element));
						list.add(element);
					} else {
						ops.add(read(key, list));
					}
				}
				var outcome = Outcome.values()[random.nextInt(3)];
				history.add(new Transaction("T" + t, outcome, ops));
				if (outcome == Outcome.COMMITTED || (outcome == Outcome.UNKNOWN && random.nextBoolean())) {
					local.forEach(state::put);
				}
			}
			// A final read of every key, as the fault tests do.
			var finalOps = new ArrayList<MicroOp>();
			for (Map.Entry<String, List<Long>> entry : state.entrySet()) {
				finalOps.add(read(entry.getKey(), entry.getValue()));
			}
			history.add(new Transaction("final", Outcome.COMMITTED, finalOps));
			var result = ListAppendChecker.check(history);
			assertTrue(result.valid(), "round " + round + ": " + result.description());
		}
	}
}
