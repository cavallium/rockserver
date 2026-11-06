package it.cavallium.rockserver.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType.RequestTypeId;
import java.nio.ByteBuffer;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;

/**
 * Property-based check of {@link ConflictTracker} against a reference model of the log (jqwik: failures shrink to a
 * shortest sequence of steps). A sequence of steps registers entries (some never appended, as Ratis may drop an entry
 * after {@code preAppendTransaction}), applies appended entries in log order, prunes, and validates transactions at
 * snapshots up to the applied index. The model knows, for every key, the log index of its last appended write.
 *
 * <p>Safety, whatever the steps and however small the history cap: a transaction is reported as conflicting whenever
 * one of its keys was written by an appended entry after its snapshot (applied or not yet). Precision, without drops,
 * pruning or a cap: it conflicts exactly then.</p>
 */
class ConflictTrackerPropertyTest {

	private static final long COLUMN = 3;
	private static final int KEY_SPACE = 12;

	sealed interface Step {}

	/** Registers an entry writing {@code keys}; {@code appended} false: Ratis dropped it after registration. */
	record Register(Set<Integer> keys, boolean appended) implements Step {}

	/** Applies the oldest appended entry not applied yet. */
	record Apply() implements Step {}

	/** Validates a transaction on {@code keys} whose snapshot is {@code back} entries below the applied index. */
	record Check(Set<Integer> keys, int back) implements Step {}

	/** Prunes the history up to {@code back} entries below the applied index. */
	record Prune(int back) implements Step {}

	@Provide
	Arbitrary<List<Step>> anySteps() {
		return steps(true);
	}

	@Provide
	Arbitrary<List<Step>> stepsWithoutDropsOrPruning() {
		return steps(false);
	}

	private static Arbitrary<List<Step>> steps(boolean faults) {
		var keys = Arbitraries.integers().between(0, KEY_SPACE - 1).set().ofMinSize(1).ofMaxSize(4);
		Arbitrary<Step> register = Combinators.combine(keys, faults ? Arbitraries.of(true, true, true, false)
				: Arbitraries.just(true)).as(Register::new);
		Arbitrary<Step> apply = Arbitraries.just(new Apply());
		Arbitrary<Step> check = Combinators.combine(keys, Arbitraries.integers().between(0, 6)).as(Check::new);
		Arbitrary<Step> prune = Arbitraries.integers().between(0, 6).map(Prune::new);
		var step = faults
				? Arbitraries.frequencyOf(
						Tuple.of(4, register), Tuple.of(4, apply),
						Tuple.of(4, check), Tuple.of(1, prune))
				: Arbitraries.frequencyOf(
						Tuple.of(4, register), Tuple.of(4, apply),
						Tuple.of(4, check));
		return step.list().ofMinSize(1).ofMaxSize(80);
	}

	@Property(tries = 400)
	void anEntryWrittenAfterTheSnapshotAlwaysConflicts(@ForAll("anySteps") List<Step> steps,
			@ForAll("historyCaps") long maxKeys) {
		run(steps, new ConflictTracker(maxKeys), false);
	}

	@Property(tries = 400)
	void withoutDropsPruningOrACapTheTrackerIsExact(@ForAll("stepsWithoutDropsOrPruning") List<Step> steps) {
		run(steps, new ConflictTracker(), true);
	}

	@Provide
	Arbitrary<Long> historyCaps() {
		return Arbitraries.of(1L, 2L, 5L, 1_000L, ConflictTracker.DEFAULT_MAX_KEYS);
	}

	private record Appended(ConflictTracker.Token token, long index) {}

	private static void run(List<Step> steps, ConflictTracker tracker, boolean exact) {
		long logIndex = 0;
		long applied = 0;
		var queue = new ArrayDeque<Appended>();
		Map<Integer, Long> lastWrite = new HashMap<>();
		for (int s = 0; s < steps.size(); s++) {
			var step = steps.get(s);
			switch (step) {
				case Register register -> {
					var writes = new ArrayList<LogEntry>();
					for (int key : register.keys()) {
						writes.add(new LogEntry.Put(COLUMN, key(key), Buf.wrap(new byte[] {1}), RequestTypeId.NOTHING));
					}
					var token = tracker.register(writes);
					if (register.appended()) {
						long index = ++logIndex;
						queue.add(new Appended(token, index));
						for (int key : register.keys()) {
							lastWrite.put(key, index);
						}
					}
				}
				case Apply _ -> {
					var next = queue.poll();
					if (next != null) {
						tracker.resolve(next.token(), next.index());
						applied = next.index();
					}
				}
				case Prune prune -> tracker.prune(Math.max(0, applied - prune.back()));
				case Check check -> {
					long snapshot = Math.max(0, applied - check.back());
					boolean mustConflict = false;
					for (int key : check.keys()) {
						mustConflict |= lastWrite.getOrDefault(key, 0L) > snapshot;
					}
					boolean conflicts = tracker.conflicts(guarded(check.keys(), snapshot));
					if (mustConflict && !conflicts) {
						throw new AssertionError("missed conflict at step " + s + ": keys " + check.keys() + ", snapshot "
								+ snapshot + ", last writes " + lastWrite);
					}
					if (exact && conflicts != mustConflict) {
						throw new AssertionError("false conflict at step " + s + ": keys " + check.keys() + ", snapshot "
								+ snapshot + ", last writes " + lastWrite);
					}
				}
			}
		}
	}

	private static LogEntry.Guarded guarded(Set<Integer> keys, long snapshot) {
		var columns = new long[keys.size()];
		var hashes = new long[keys.size()];
		int i = 0;
		for (int key : keys) {
			columns[i] = COLUMN;
			hashes[i] = ConflictTracker.keyHash(COLUMN, key(key));
			i++;
		}
		return new LogEntry.Guarded(1, snapshot, columns, hashes, List.of(), false);
	}

	private static Keys key(int value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}
}
