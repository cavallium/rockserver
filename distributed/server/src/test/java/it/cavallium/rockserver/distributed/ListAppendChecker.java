package it.cavallium.rockserver.distributed;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.EnumMap;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;
import org.jetbrains.annotations.Nullable;

/**
 * Checks a history of list-append transactions for isolation anomalies, the way Elle does (Kingsbury and Alvaro,
 * "Elle: Inferring Isolation Anomalies from Experimental Observations", VLDB 2020).
 *
 * <p>Every key holds a list. A transaction is a sequence of micro-operations: {@code append(k, e)} adds the element
 * {@code e} at the end of {@code k}'s list, {@code read(k)} returns the whole list. Every element is appended at most
 * once in the whole history, so a read tells exactly which transactions wrote what it saw, and in which order: the
 * order of each key's versions is the order of its longest read, and every other read of it must be a prefix of that
 * one. From that the checker builds the dependency graph of Adya's thesis between committed transactions:</p>
 * <ul>
 *   <li>{@code ww}: T1 appended the element right before one T2 appended to the same key;</li>
 *   <li>{@code wr}: T2 read a list whose last element (not counting its own appends) T1 appended;</li>
 *   <li>{@code rw}: T1 read a list that the element T2 appended next extends (an anti-dependency).</li>
 * </ul>
 * <p>and reports, with an example cycle each:</p>
 * <ul>
 *   <li>{@link Anomaly#G0} (write cycle): a cycle of {@code ww} edges only;</li>
 *   <li>{@link Anomaly#G1C} (circular information flow): a cycle of {@code ww} and {@code wr} edges, at least one
 *   {@code wr};</li>
 *   <li>{@link Anomaly#G_SINGLE} (read skew): a cycle with exactly one {@code rw} edge;</li>
 *   <li>{@link Anomaly#G2} (anti-dependency cycle, e.g. write skew): a cycle with two or more {@code rw} edges;</li>
 * </ul>
 * <p>and the anomalies visible without a cycle: {@link Anomaly#G1A} (a read saw an element of an aborted
 * transaction), {@link Anomaly#G1B} (a read saw a transaction's append to a key but not its later append to the same
 * key), {@link Anomaly#INCOMPATIBLE_ORDER} (two reads of a key that are not prefixes of one another),
 * {@link Anomaly#DUPLICATE} (an element twice in a list), {@link Anomaly#GARBAGE} (an element nobody appended) and
 * {@link Anomaly#INTERNAL} (a read inside a transaction that does not end with its own earlier appends).</p>
 *
 * <p>Serializability forbids all of them; snapshot isolation allows {@code G2} only. A transaction whose outcome is
 * unknown counts as committed if a committed read saw one of its elements, and is ignored otherwise; its reads are
 * never trusted. Only committed reads order a key's versions. Real-time order is not checked (that would be strict
 * serializability): the register workloads cover linearizability.</p>
 */
public final class ListAppendChecker {

	static final int MAX_EXAMPLES = 5;

	private ListAppendChecker() {}

	/** What a transaction is known to have done. */
	public enum Outcome {
		/** It committed. */
		COMMITTED,
		/** It certainly had no effect (rolled back, conflicted, or never reached a server). */
		ABORTED,
		/** It may or may not have committed. */
		UNKNOWN
	}

	public enum Anomaly {
		G0, G1A, G1B, G1C, G_SINGLE, G2, INCOMPATIBLE_ORDER, DUPLICATE, GARBAGE, INTERNAL;

		/** Allowed under snapshot isolation. */
		public boolean allowedBySnapshotIsolation() {
			return this == G2;
		}
	}

	/** One micro-operation: an append of {@code element}, or a read that returned {@code list} (null if unknown). */
	public record MicroOp(boolean append, String key, long element, @Nullable List<Long> list) {

		public static MicroOp append(String key, long element) {
			return new MicroOp(true, key, element, null);
		}

		public static MicroOp read(String key, @Nullable List<Long> list) {
			return new MicroOp(false, key, 0, list == null ? null : List.copyOf(list));
		}

		@Override
		public String toString() {
			return append ? "append(" + key + ", " + element + ")" : "read(" + key + ") = " + list;
		}
	}

	public record Transaction(String id, Outcome outcome, List<MicroOp> ops) {

		public Transaction {
			Objects.requireNonNull(id, "id");
			Objects.requireNonNull(outcome, "outcome");
			ops = List.copyOf(ops);
		}

		@Override
		public String toString() {
			return id + " " + outcome + " " + ops;
		}
	}

	/** The anomalies found, each with up to {@link #MAX_EXAMPLES} descriptions. */
	public record Result(Map<Anomaly, List<String>> anomalies, int transactions, int edges) {

		public boolean valid() {
			return anomalies.isEmpty();
		}

		/** True if every anomaly found is allowed by snapshot isolation. */
		public boolean snapshotIsolated() {
			return anomalies.keySet().stream().allMatch(Anomaly::allowedBySnapshotIsolation);
		}

		public String description() {
			if (anomalies.isEmpty()) {
				return "No anomaly in " + transactions + " committed transactions (" + edges + " dependency edges)";
			}
			var text = new StringBuilder("Anomalies in " + transactions + " committed transactions:");
			anomalies.forEach((anomaly, examples) -> {
				text.append("\n  ").append(anomaly).append(':');
				examples.forEach(example -> text.append("\n    ").append(example));
			});
			return text.toString();
		}
	}

	private enum EdgeType { WW, WR, RW }

	private record Edge(int from, int to, EdgeType type, String key) {}

	public static Result check(List<Transaction> history) {
		var anomalies = new EnumMap<Anomaly, List<String>>(Anomaly.class);
		var transactions = List.copyOf(history);
		int n = transactions.size();

		// Who appended each element, and each writer's appends per key in order.
		var writerOf = new HashMap<String, Map<Long, Integer>>();
		var appendsOf = new ArrayList<Map<String, List<Long>>>();
		for (int t = 0; t < n; t++) {
			var appends = new HashMap<String, List<Long>>();
			for (var op : transactions.get(t).ops()) {
				if (op.append()) {
					var previous = writerOf.computeIfAbsent(op.key(), _ -> new HashMap<>()).put(op.element(), t);
					if (previous != null) {
						throw new IllegalArgumentException("Element " + op.element() + " of key " + op.key()
								+ " appended twice, by " + transactions.get(previous).id() + " and "
								+ transactions.get(t).id());
					}
					appends.computeIfAbsent(op.key(), _ -> new ArrayList<>()).add(op.element());
				}
			}
			appendsOf.add(appends);
		}

		// An unknown transaction whose element a committed read saw did commit.
		var committed = new boolean[n];
		for (int t = 0; t < n; t++) {
			committed[t] = transactions.get(t).outcome() == Outcome.COMMITTED;
		}
		for (var transaction : transactions) {
			if (transaction.outcome() != Outcome.COMMITTED) {
				continue;
			}
			for (var op : transaction.ops()) {
				if (!op.append() && op.list() != null) {
					var writers = writerOf.getOrDefault(op.key(), Map.of());
					for (long element : op.list()) {
						var writer = writers.get(element);
						if (writer != null && transactions.get(writer).outcome() == Outcome.UNKNOWN) {
							committed[writer] = true;
						}
					}
				}
			}
		}

		// Committed reads: element checks, internal consistency, and the version order of each key.
		var longest = new HashMap<String, List<Long>>();
		var longestReader = new HashMap<String, String>();
		for (var transaction : transactions) {
			if (transaction.outcome() != Outcome.COMMITTED) {
				continue;
			}
			var ownAppends = new HashMap<String, List<Long>>();
			var lastRead = new HashMap<String, List<Long>>();
			for (var op : transaction.ops()) {
				if (op.append()) {
					ownAppends.computeIfAbsent(op.key(), _ -> new ArrayList<>()).add(op.element());
					continue;
				}
				var list = op.list();
				if (list == null) {
					continue;
				}
				if (new HashSet<>(list).size() != list.size()) {
					report(anomalies, Anomaly.DUPLICATE, transaction.id() + " " + op);
				}
				var writers = writerOf.getOrDefault(op.key(), Map.of());
				for (long element : list) {
					var writer = writers.get(element);
					if (writer == null) {
						report(anomalies, Anomaly.GARBAGE, transaction.id() + " " + op + ": nobody appended " + element);
					} else if (transactions.get(writer).outcome() == Outcome.ABORTED) {
						report(anomalies, Anomaly.G1A, transaction.id() + " " + op + ": " + element
								+ " was appended by " + transactions.get(writer));
					}
				}
				var own = ownAppends.getOrDefault(op.key(), List.of());
				var previous = lastRead.get(op.key());
				// A read shows the transaction's own appends at the end, and repeats what it read before.
				if (!endsWith(list, own) || (previous != null && !isPrefix(previous, list))) {
					report(anomalies, Anomaly.INTERNAL, transaction.id() + " " + op + " after its own appends " + own
							+ (previous != null ? " and an earlier read " + previous : ""));
				}
				lastRead.put(op.key(), list);
				var current = longest.get(op.key());
				if (current == null || isPrefix(current, list)) {
					longest.put(op.key(), list);
					longestReader.put(op.key(), transaction.id());
				} else if (!isPrefix(list, current)) {
					report(anomalies, Anomaly.INCOMPATIBLE_ORDER, "key " + op.key() + ": " + transaction.id()
							+ " read " + list + ", " + longestReader.get(op.key()) + " read " + current);
				}
			}
		}

		// Position of each element in its key's version order.
		var position = new HashMap<String, Map<Long, Integer>>();
		longest.forEach((key, list) -> {
			var positions = new HashMap<Long, Integer>();
			for (int i = 0; i < list.size(); i++) {
				positions.putIfAbsent(list.get(i), i);
			}
			position.put(key, positions);
		});

		var edges = new LinkedHashMap<List<Object>, Edge>();
		// ww: consecutive versions of a key by different transactions.
		longest.forEach((key, list) -> {
			var writers = writerOf.getOrDefault(key, Map.of());
			for (int i = 1; i < list.size(); i++) {
				var from = writers.get(list.get(i - 1));
				var to = writers.get(list.get(i));
				if (from != null && to != null && !from.equals(to) && committed[from] && committed[to]) {
					addEdge(edges, new Edge(from, to, EdgeType.WW, key));
				}
			}
		});
		// wr and rw from every committed read, without the reader's own appends at its end.
		for (int t = 0; t < n; t++) {
			var transaction = transactions.get(t);
			if (transaction.outcome() != Outcome.COMMITTED) {
				continue;
			}
			var ownAppends = new HashMap<String, List<Long>>();
			for (var op : transaction.ops()) {
				if (op.append()) {
					ownAppends.computeIfAbsent(op.key(), _ -> new ArrayList<>()).add(op.element());
					continue;
				}
				if (op.list() == null) {
					continue;
				}
				var observed = withoutSuffix(op.list(), ownAppends.getOrDefault(op.key(), List.of()));
				var writers = writerOf.getOrDefault(op.key(), Map.of());
				if (!observed.isEmpty()) {
					long last = observed.getLast();
					var writer = writers.get(last);
					if (writer != null && writer != t) {
						if (committed[writer]) {
							addEdge(edges, new Edge(writer, t, EdgeType.WR, op.key()));
						}
						var writerAppends = appendsOf.get(writer).getOrDefault(op.key(), List.of());
						if (!writerAppends.isEmpty() && writerAppends.getLast() != last) {
							report(anomalies, Anomaly.G1B, transaction.id() + " " + op + ": " + last
									+ " is not the last append of " + transactions.get(writer).id() + " to " + op.key()
									+ " " + writerAppends);
						}
					}
				}
				// The version that follows what was read, if the order knows it.
				var order = longest.get(op.key());
				var positions = position.get(op.key());
				int next;
				if (observed.isEmpty()) {
					next = 0;
				} else if (positions != null && positions.containsKey(observed.getLast())) {
					next = positions.get(observed.getLast()) + 1;
				} else {
					next = -1;
				}
				if (order != null && next > 0) {
					// Later appends of the same writer are part of the version read (an intermediate read, G1b).
					var lastWriter = writers.get(order.get(next - 1));
					while (next < order.size() && lastWriter != null && lastWriter.equals(writers.get(order.get(next)))) {
						next++;
					}
				}
				if (order != null && next >= 0 && next < order.size()) {
					var overwriter = writers.get(order.get(next));
					if (overwriter != null && overwriter != t && committed[overwriter]) {
						addEdge(edges, new Edge(t, overwriter, EdgeType.RW, op.key()));
					}
				}
			}
		}

		findCycles(transactions, List.copyOf(edges.values()), anomalies);
		int counted = 0;
		for (boolean c : committed) {
			counted += c ? 1 : 0;
		}
		return new Result(Collections.unmodifiableMap(anomalies), counted, edges.size());
	}

	/**
	 * For every edge whose ends share a strongly connected component, looks for the shortest way back with the
	 * edge types that decide the anomaly: writes only (G0), writes and reads (G1c, and G-single when the edge is an
	 * anti-dependency), or anything (G2). One example per component for G0 and G1c, one per anti-dependency edge
	 * otherwise, up to {@link #MAX_EXAMPLES} each.
	 */
	private static void findCycles(List<Transaction> transactions, List<Edge> edges,
			Map<Anomaly, List<String>> anomalies) {
		int n = transactions.size();
		var all = adjacency(n, edges, Set.of(EdgeType.WW, EdgeType.WR, EdgeType.RW));
		var writesAndReads = adjacency(n, edges, Set.of(EdgeType.WW, EdgeType.WR));
		var writes = adjacency(n, edges, Set.of(EdgeType.WW));
		var component = components(n, all);

		var g0Components = new HashSet<Integer>();
		var g1cComponents = new HashSet<Integer>();
		for (var edge : edges) {
			if (component[edge.from()] != component[edge.to()]) {
				continue;
			}
			switch (edge.type()) {
				case WW -> {
					if (!g0Components.contains(component[edge.from()])) {
						var back = path(writes, edge.to(), edge.from(), component);
						if (back != null) {
							g0Components.add(component[edge.from()]);
							report(anomalies, Anomaly.G0, cycle(transactions, edge, back));
						}
					}
				}
				case WR -> {
					if (!g1cComponents.contains(component[edge.from()])) {
						var back = path(writesAndReads, edge.to(), edge.from(), component);
						if (back != null) {
							g1cComponents.add(component[edge.from()]);
							report(anomalies, Anomaly.G1C, cycle(transactions, edge, back));
						}
					}
				}
				case RW -> {
					var single = path(writesAndReads, edge.to(), edge.from(), component);
					if (single != null) {
						report(anomalies, Anomaly.G_SINGLE, cycle(transactions, edge, single));
					} else {
						// No way back without another anti-dependency: the cycle has at least two.
						var back = path(all, edge.to(), edge.from(), component);
						if (back != null) {
							report(anomalies, Anomaly.G2, cycle(transactions, edge, back));
						}
					}
				}
			}
		}
	}

	private static List<List<Edge>> adjacency(int n, List<Edge> edges, Set<EdgeType> types) {
		var adjacency = new ArrayList<List<Edge>>(n);
		for (int i = 0; i < n; i++) {
			adjacency.add(new ArrayList<>());
		}
		for (var edge : edges) {
			if (types.contains(edge.type())) {
				adjacency.get(edge.from()).add(edge);
			}
		}
		return adjacency;
	}

	/** Strongly connected components (Tarjan, iterative): the component number of each vertex. */
	private static int[] components(int n, List<List<Edge>> adjacency) {
		var index = new int[n];
		var low = new int[n];
		var component = new int[n];
		var onStack = new boolean[n];
		Arrays.fill(index, -1);
		var stack = new ArrayDeque<Integer>();
		int counter = 0;
		int components = 0;
		for (int root = 0; root < n; root++) {
			if (index[root] >= 0) {
				continue;
			}
			// Frames of {vertex, next edge position}.
			var frames = new ArrayDeque<int[]>();
			frames.push(new int[] {root, 0});
			index[root] = low[root] = counter++;
			stack.push(root);
			onStack[root] = true;
			while (!frames.isEmpty()) {
				var frame = frames.peek();
				int v = frame[0];
				var out = adjacency.get(v);
				if (frame[1] < out.size()) {
					int w = out.get(frame[1]++).to();
					if (index[w] < 0) {
						index[w] = low[w] = counter++;
						stack.push(w);
						onStack[w] = true;
						frames.push(new int[] {w, 0});
					} else if (onStack[w]) {
						low[v] = Math.min(low[v], index[w]);
					}
					continue;
				}
				frames.pop();
				if (!frames.isEmpty()) {
					int parent = frames.peek()[0];
					low[parent] = Math.min(low[parent], low[v]);
				}
				if (low[v] == index[v]) {
					int w;
					do {
						w = stack.pop();
						onStack[w] = false;
						component[w] = components;
					} while (w != v);
					components++;
				}
			}
		}
		return component;
	}

	/** A shortest path of edges from {@code from} to {@code to} inside their component, or null. */
	private static @Nullable List<Edge> path(List<List<Edge>> adjacency, int from, int to, int[] component) {
		if (from == to) {
			return List.of();
		}
		var via = new HashMap<Integer, Edge>();
		var queue = new ArrayDeque<Integer>();
		queue.add(from);
		via.put(from, null);
		while (!queue.isEmpty()) {
			int v = queue.poll();
			for (var edge : adjacency.get(v)) {
				int w = edge.to();
				if (component[w] != component[from] || via.containsKey(w)) {
					continue;
				}
				via.put(w, edge);
				if (w == to) {
					var path = new ArrayList<Edge>();
					for (Edge step = edge; step != null; step = via.get(step.from())) {
						path.add(step);
					}
					Collections.reverse(path);
					return path;
				}
				queue.add(w);
			}
		}
		return null;
	}

	private static String cycle(List<Transaction> transactions, Edge first, List<Edge> back) {
		var steps = new ArrayList<Edge>();
		steps.add(first);
		steps.addAll(back);
		var text = steps.stream()
				.map(edge -> transactions.get(edge.from()).id() + " -" + edge.type().name().toLowerCase() + "("
						+ edge.key() + ")-> ")
				.collect(Collectors.joining());
		var involved = steps.stream().map(Edge::from).distinct()
				.map(t -> "\n      " + transactions.get(t))
				.collect(Collectors.joining());
		return text + transactions.get(first.from()).id() + involved;
	}

	private static void addEdge(Map<List<Object>, Edge> edges, Edge edge) {
		edges.putIfAbsent(List.of(edge.from(), edge.to(), edge.type()), edge);
	}

	private static void report(Map<Anomaly, List<String>> anomalies, Anomaly anomaly, String description) {
		var examples = anomalies.computeIfAbsent(anomaly, _ -> new ArrayList<>());
		if (examples.size() < MAX_EXAMPLES) {
			examples.add(description);
		}
	}

	private static boolean isPrefix(List<Long> prefix, List<Long> list) {
		return prefix.size() <= list.size() && list.subList(0, prefix.size()).equals(prefix);
	}

	private static boolean endsWith(List<Long> list, List<Long> suffix) {
		return suffix.size() <= list.size() && list.subList(list.size() - suffix.size(), list.size()).equals(suffix);
	}

	/** {@code list} without {@code suffix} at its end; unchanged if it does not end with it. */
	private static List<Long> withoutSuffix(List<Long> list, List<Long> suffix) {
		return endsWith(list, suffix) ? list.subList(0, list.size() - suffix.size()) : list;
	}
}
