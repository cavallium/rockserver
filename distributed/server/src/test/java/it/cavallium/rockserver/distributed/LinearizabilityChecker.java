package it.cavallium.rockserver.distributed;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.BitSet;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;

/**
 * Linearizability checker for histories of registers (one register per key, initially absent), using the
 * Wing &amp; Gong search with Lowe's memoization of {@code (linearized operations, register value)} pairs, as in
 * Porcupine. A history is linearizable iff every key's sub-history is (P-compositionality), so keys are checked
 * one at a time.
 *
 * <p>How statuses are interpreted:</p>
 * <ul>
 *   <li>{@link OpStatus#OK}: took effect exactly once between invocation and completion;</li>
 *   <li>{@link OpStatus#FAILED}: had no effect, so it is ignored;</li>
 *   <li>{@link OpStatus#UNKNOWN}: a put may take effect at any time after its invocation, or never (its completion
 *       is treated as infinite); a get with an unknown result tells nothing, so it is ignored.</li>
 * </ul>
 */
public final class LinearizabilityChecker {

	/** How much of the linearizable prefix a counterexample shows. */
	private static final int SHOWN_PREFIX_OPERATIONS = 20;

	private LinearizabilityChecker() {}

	/**
	 * One operation on the register {@code key}.
	 *
	 * @param value the value written by a put, or the value read by a get (null = absent)
	 * @param invokeNanos {@link System#nanoTime()} before the request was sent
	 * @param completeNanos {@link System#nanoTime()} after the reply arrived; ignored for {@link OpStatus#UNKNOWN}
	 */
	public record Operation(String key,
			Kind kind,
			@Nullable String value,
			long invokeNanos,
			long completeNanos,
			OpStatus status) {

		public enum Kind { PUT, GET }

		public Operation {
			Objects.requireNonNull(key, "key");
			Objects.requireNonNull(kind, "kind");
			Objects.requireNonNull(status, "status");
			if (kind == Kind.PUT && value == null) {
				throw new IllegalArgumentException("A put needs a value");
			}
			if (status != OpStatus.UNKNOWN && (completeNanos < invokeNanos || completeNanos == Long.MAX_VALUE)) {
				throw new IllegalArgumentException("Invalid completion time for " + status + " operation");
			}
		}

		public static Operation put(String key, String value, long invokeNanos, long completeNanos, OpStatus status) {
			return new Operation(key, Kind.PUT, value, invokeNanos, completeNanos, status);
		}

		public static Operation get(String key,
				@Nullable String value,
				long invokeNanos,
				long completeNanos,
				OpStatus status) {
			return new Operation(key, Kind.GET, value, invokeNanos, completeNanos, status);
		}
	}

	/**
	 * @param key the first key found not linearizable, null if the history is linearizable
	 * @param description a summary, or a counterexample: the longest linearizable prefix found for {@code key} and
	 *                    the operation that could not be linearized after it
	 */
	public record Result(boolean linearizable, @Nullable String key, String description) {}

	public static Result check(List<Operation> history) {
		Map<String, List<Operation>> byKey = history.stream()
				.collect(Collectors.groupingBy(Operation::key, TreeMap::new, Collectors.toList()));
		for (var entry : byKey.entrySet()) {
			var result = new KeyCheck(entry.getKey(), entry.getValue()).run();
			if (!result.linearizable()) {
				return result;
			}
		}
		return new Result(true, null,
				"Linearizable: " + history.size() + " operations on " + byKey.size() + " keys");
	}

	/** Search over the operations of one key. */
	private static final class KeyCheck {

		private final String key;
		private final Operation[] ops;
		private final long baseNanos;
		private final Node head = new Node(-1, true, Long.MIN_VALUE);

		private int bestDepth = -1;
		private List<Integer> bestPrefix = List.of();
		private int bestStuck;
		private List<Integer> bestPending = List.of();
		private @Nullable String bestState;

		KeyCheck(String key, List<Operation> history) {
			this.key = key;
			this.ops = relevant(history).toArray(Operation[]::new);
			this.baseNanos = history.stream().mapToLong(Operation::invokeNanos).min().orElse(0);
		}

		/**
		 * Drops what cannot affect the outcome: failed operations, unknown gets, and unknown puts whose value no
		 * acknowledged get returned. Such a put can always be left out of a linearization: no read follows it before
		 * the next put, since every read would have returned its value.
		 */
		private static List<Operation> relevant(List<Operation> history) {
			Set<String> readValues = new HashSet<>();
			for (var op : history) {
				if (op.kind() == Operation.Kind.GET && op.status() == OpStatus.OK && op.value() != null) {
					readValues.add(op.value());
				}
			}
			var result = new ArrayList<Operation>();
			for (var op : history) {
				boolean keep = switch (op.status()) {
					case OK -> true;
					case FAILED -> false;
					case UNKNOWN -> op.kind() == Operation.Kind.PUT && readValues.contains(op.value());
				};
				if (keep) {
					result.add(op);
				}
			}
			return result;
		}

		Result run() {
			buildEventList();
			String state = null;
			var linearized = new BitSet(ops.length);
			var cache = new HashSet<CacheKey>();
			var calls = new ArrayDeque<Frame>();
			Node entry = head.next;
			while (head.next != null) {
				if (entry.isCall) {
					var op = ops[entry.op];
					boolean legal = op.kind() == Operation.Kind.PUT || Objects.equals(state, op.value());
					if (legal) {
						String newState = op.kind() == Operation.Kind.PUT ? op.value() : state;
						linearized.set(entry.op);
						if (cache.add(new CacheKey((BitSet) linearized.clone(), newState))) {
							calls.push(new Frame(entry, state));
							state = newState;
							lift(entry);
							entry = head.next;
							continue;
						}
						linearized.clear(entry.op);
					}
					entry = entry.next;
				} else {
					if (ops[entry.op].status() == OpStatus.UNKNOWN) {
						// Unknown completions sort last: every acknowledged operation is already linearized.
						return new Result(true, null, "Linearizable");
					}
					recordStuck(calls, entry, state);
					if (calls.isEmpty()) {
						return counterexample();
					}
					var top = calls.pop();
					state = top.stateBefore;
					linearized.clear(top.call.op);
					unlift(top.call);
					entry = top.call.next;
				}
			}
			return new Result(true, null, "Linearizable");
		}

		private void buildEventList() {
			var events = new ArrayList<Node>(ops.length * 2);
			for (int i = 0; i < ops.length; i++) {
				var call = new Node(i, true, ops[i].invokeNanos());
				long completion = ops[i].status() == OpStatus.UNKNOWN ? Long.MAX_VALUE : ops[i].completeNanos();
				var ret = new Node(i, false, completion);
				call.match = ret;
				events.add(call);
				events.add(ret);
			}
			// On equal timestamps calls go first: the operations are then treated as concurrent.
			events.sort(Comparator.comparingLong((Node n) -> n.time).thenComparing(n -> !n.isCall)
					.thenComparingInt(n -> n.op));
			Node previous = head;
			for (var event : events) {
				previous.next = event;
				event.prev = previous;
				previous = event;
			}
		}

		/** Removes a call and its return from the event list. */
		private static void lift(Node call) {
			call.prev.next = call.next;
			call.next.prev = call.prev;
			var ret = call.match;
			ret.prev.next = ret.next;
			if (ret.next != null) {
				ret.next.prev = ret.prev;
			}
		}

		/** Undoes {@link #lift}, in reverse order. */
		private static void unlift(Node call) {
			var ret = call.match;
			ret.prev.next = ret;
			if (ret.next != null) {
				ret.next.prev = ret;
			}
			call.prev.next = call;
			call.next.prev = call;
		}

		private void recordStuck(ArrayDeque<Frame> calls, Node stuckReturn, @Nullable String state) {
			if (calls.size() <= bestDepth) {
				return;
			}
			bestDepth = calls.size();
			var prefix = new ArrayList<Integer>(calls.size());
			calls.descendingIterator().forEachRemaining(frame -> prefix.add(frame.call.op));
			bestPrefix = prefix;
			bestStuck = stuckReturn.op;
			var pending = new ArrayList<Integer>();
			for (Node n = head.next; n != stuckReturn; n = n.next) {
				if (n.isCall) {
					pending.add(n.op);
				}
			}
			bestPending = pending;
			bestState = state;
		}

		private Result counterexample() {
			var text = new StringBuilder();
			text.append("Key '").append(key).append("' is not linearizable (").append(ops.length)
					.append(" relevant operations, times relative to the first invocation).\n");
			text.append("Longest linearizable prefix found: ").append(bestPrefix.size()).append(" operations");
			int shown = Math.min(bestPrefix.size(), SHOWN_PREFIX_OPERATIONS);
			if (shown < bestPrefix.size()) {
				text.append(", the last ").append(shown).append(" are");
			}
			text.append(":\n");
			for (int op : bestPrefix.subList(bestPrefix.size() - shown, bestPrefix.size())) {
				text.append("  ").append(describe(op)).append('\n');
			}
			text.append("Then the register holds ").append(quote(bestState)).append(" and ")
					.append(describe(bestStuck)).append(" must take effect before it completes,")
					.append(" but none of the pending operations can come next:\n");
			for (int op : bestPending) {
				text.append("  ").append(describe(op)).append('\n');
			}
			return new Result(false, key, text.toString());
		}

		private String describe(int index) {
			var op = ops[index];
			var call = op.kind() == Operation.Kind.PUT
					? "put(" + quote(op.value()) + ")"
					: "get() -> " + quote(op.value());
			var end = op.status() == OpStatus.UNKNOWN ? "never" : millis(op.completeNanos());
			return "#" + index + " " + call + " " + op.status() + " [" + millis(op.invokeNanos()) + ", " + end + "]";
		}

		private String millis(long nanos) {
			return String.format("%.3fms", (nanos - baseNanos) / 1e6);
		}

		private static String quote(@Nullable String value) {
			return value == null ? "absent" : "'" + value + "'";
		}
	}

	/** An invocation or completion event in the doubly linked event list. */
	private static final class Node {
		final int op;
		final boolean isCall;
		final long time;
		Node match;
		Node prev;
		Node next;

		Node(int op, boolean isCall, long time) {
			this.op = op;
			this.isCall = isCall;
			this.time = time;
		}
	}

	private record Frame(Node call, @Nullable String stateBefore) {}

	private record CacheKey(BitSet linearized, @Nullable String state) {}
}
