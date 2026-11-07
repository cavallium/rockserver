package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.rockserver.distributed.RemovalRule.Answer;
import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import it.cavallium.rockserver.distributed.RemovalRule.Outcome;
import it.cavallium.rockserver.distributed.RemovalRule.Own;
import it.cavallium.rockserver.distributed.RemovalRule.Probe;
import it.cavallium.rockserver.distributed.RemovalRule.Reason;
import it.cavallium.rockserver.distributed.RemovalRule.Verdict;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.stream.Collectors;
import net.jqwik.api.Arbitraries;
import net.jqwik.api.Arbitrary;
import net.jqwik.api.Combinators;
import net.jqwik.api.Example;
import net.jqwik.api.ForAll;
import net.jqwik.api.Property;
import net.jqwik.api.Provide;
import net.jqwik.api.Tuple;
import net.jqwik.api.statistics.Statistics;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;

/**
 * R12-6: the removal rule ({@link RemovalRule#decide}) and the facts it decides on ({@link MembershipFacts}, and the
 * version-1 answer of {@link TermService}) checked against a model of Raft logs and Ratis' configurations (jqwik:
 * failures shrink to a shortest list of steps). The facts come from the real code, run on a stub division of each
 * model node ({@link ModelDivision}), so facts and rule are checked together.
 *
 * <p>The model, per node: its log (term; ordinary or configuration entry), snapshot, commit and applied indexes, the
 * history of applied configurations, {@code raft-meta.conf} and Ratis' configurations in memory, kept apart as Ratis
 * keeps them: an append or truncation removes the configurations in memory from its first index on, a snapshot install
 * writes the leader's latest configuration (committed or not) to the file and into memory, an apply rewrites only the
 * file, and a restart rebuilds the memory from the file and the log (round-11 finding 1). Steps: elections (votes by
 * log up-to-dateness from a majority of each half of the candidate's configuration, voters whose configuration holds
 * the candidate; the term-start entry copies the new half, R11-1), ordinary appends, membership changes (joint, then
 * stable, each only after the one before is committed; an add of a node that has a log is refused, but x and y are
 * wiped and join as a new incarnation), replication with truncation only on conflict, commit, apply, snapshots,
 * installs (also one that stops between Ratis' snapshot index and the history swap, R12-2), crashes and restarts
 * (commit index back to the applied one).</p>
 *
 * <p>Checked after every step: the committed prefixes of all nodes agree (sanity of the model); every committed
 * configuration a running node reports, and every probe it answers committed, is a configuration entry the model
 * committed (same halves, same term when known), and a probe it answers absent is not a configuration entry of its own
 * log (R12-2); with every member answering in this version, no INCONSISTENT verdict; and S1: a REMOVED verdict for x or
 * y (running, in its current incarnation), with members answering in version 2, in version 1, pending or not at all,
 * never comes while the newest committed configuration holds it in its new half, nor while a node other than it,
 * electable without it, holds a configuration with it in the new half after the committed prefix ("electable" ignores
 * terms, which may grow in any continuation, so it over-approximates what may still commit). S2, at the end, after
 * quiescing (a fresh leader of the newest committed configuration's new half, every change finished, everything
 * replicated, committed and applied): x or y, a voter once and out of that new half, is REMOVED.</p>
 *
 * <p>Not modelled: timing (current-term constraints of votes, leader leases), RemovalCheck's budget and the asynchrony
 * of its rounds (answers are taken atomically in each round; the rounds are replayed with the probes of the K* of the
 * round before, and a member that did not answer is not asked again, as {@link RemovalCheck} does), HEARD_LEADER, and
 * the 200 ms window of the member file (written at once here). Version-1 answers are not held to the fact oracle: their
 * committed flag is the finding-1 flaw of older versions, which the rule must survive (R12-1). S2 leaves out a
 * subject whose current incarnation became a voter only through an add that never committed: its own log holds that
 * add's configuration after K*, on K*'s branch and of a term not lower, which the rule cannot tell from one that may
 * still commit; add-peer reported the failure, and a retried add-peer refuses the node's log, so the operator wipes it
 * (design: "Removal and waiting outside", residuals).</p>
 */
class RemovalRulePropertyTest {

	static final List<String> UNIVERSE = List.of("a", "b", "c", "d", "x", "y");
	static final List<String> SUBJECTS = List.of("x", "y");

	// ---------------------------------------------------------------------------------------------------------------
	// Steps

	sealed interface Step {}

	/** Node {@code n} campaigns. */
	record Elect(int n) implements Step {}

	/** Leader {@code n} appends an ordinary entry. */
	record Append(int n) implements Step {}

	/** Leader {@code n} starts a membership change to {@code target} (node indexes). */
	record SetConf(int n, Set<Integer> target) implements Step {}

	/**
	 * Leader {@code n} adds {@code node} to its configuration, or removes it: one node at a time, as add-peer and
	 * remove-peer do. A change of several nodes at once is not generated: Ratis' term-start entry copies only the new
	 * half of an uncommitted joint configuration and then commits by the new half alone, which, when a majority of the
	 * new half and a majority of the old one can be disjoint, lets a majority of the old half elect a leader without
	 * the committed entries (round-13 finding, outside the removal rule; the model's sanity check finds it at once).
	 * {@code lag}: an added node that the staging brings up to date by a snapshot install gets only the snapshot, as
	 * Ratis' staging ends once the node is within its catch-up gap of the leader's commit index, so that the leader
	 * may install a later snapshot after the joint configuration (round-13 review, finding 1).
	 */
	record Change(int n, int node, boolean lag) implements Step {}

	/** Leader {@code n} appends the stable configuration after its committed joint one. */
	record FinishConf(int n) implements Step {}

	/** Leader {@code n} sends {@code f} its log up to {@code back} entries before its end, and its commit index. */
	record Replicate(int n, int f, int back) implements Step {}

	/**
	 * A round of leader {@code n}: it sends its whole log to every node it sends to, advances its commit index, and
	 * sends the new commit index.
	 */
	record Broadcast(int n) implements Step {}

	/** Leader {@code n} advances its commit index. */
	record AdvanceCommit(int n) implements Step {}

	/** Node {@code n} applies up to {@code back} entries before its commit index. */
	record Apply(int n, int back) implements Step {}

	/** Node {@code n} takes a snapshot {@code back} entries before its applied index, and purges its log up to it. */
	record Snapshot(int n, int back) implements Step {}

	/** Leader {@code n} installs its snapshot on {@code f}; {@code window}: f stops before the history swap. */
	record Install(int n, int f, boolean window) implements Step {}

	/** Node {@code n} swaps in the history of the snapshot it installs. */
	record FinishInstall(int n) implements Step {}

	record Crash(int n) implements Step {}

	record Restart(int n) implements Step {}

	/** The seeded configuration: {a, b, c, x}, and d and y if set. */
	record Setup(boolean withD, boolean withY) {}

	// ---------------------------------------------------------------------------------------------------------------
	// Model

	/** A log entry: its term, and both halves of a configuration (null for an ordinary entry). */
	record Entry(long term, List<String> newHalf, List<String> oldHalf) {
		boolean conf() {
			return newHalf != null;
		}

		@Override
		public String toString() {
			return conf() ? "C" + term + newHalf + (oldHalf.isEmpty() ? "" : "/" + oldHalf) : "e" + term;
		}
	}

	static final class Node {
		final String id;
		int incarnation;
		boolean join;
		boolean memberFile;
		/** {@link InstalledConfiguration}, written as RaftTraffic writes it: before the follower acts on a chunk. */
		InstalledConfiguration installed;
		/** The index of the joint configuration that added this incarnation (-1: seeded). */
		long addIndex = -1;
		boolean up;
		boolean leader;
		long term;
		/** The whole log, the purged prefix included (index i at position i - 1): the model's truth. */
		List<Entry> log = new ArrayList<>();
		long snapshot;
		long commit;
		long applied;
		List<Conf> history = new ArrayList<>();
		long completeFrom;
		/** {@code raft-meta.conf}; null: none written. */
		Conf file;
		/** Ratis' configurations in memory (term 0), by index. */
		TreeMap<Long, Conf> mem = new TreeMap<>();
		/** The configuration Ratis starts with (index -1): the seeded peers, none for a --join node. */
		Conf initial;
		/** In the window of an install (R12-2): the history to swap in. */
		List<Conf> pendingHistory;
		long pendingCompleteFrom;

		Node(String id) {
			this.id = id;
		}

		long last() {
			return log.size();
		}

		long lastTerm() {
			return log.isEmpty() ? 0 : log.getLast().term();
		}

		/** The term at {@code index} of the whole log, 0 if none. */
		long termAt(long index) {
			return index >= 1 && index <= log.size() ? log.get((int) index - 1).term() : 0;
		}

		Entry at(long index) {
			return log.get((int) index - 1);
		}

		Conf current() {
			return mem.isEmpty() ? initial : mem.lastEntry().getValue();
		}

		/** Ratis' configurations after a restart: the file, then the configurations of the log after the snapshot. */
		TreeMap<Long, Conf> rebuiltMem() {
			var rebuilt = new TreeMap<Long, Conf>();
			if (file != null) {
				rebuilt.put(file.index(), file);
			}
			for (long i = snapshot + 1; i <= last(); i++) {
				var e = at(i);
				if (e.conf()) {
					rebuilt.put(i, ratisConf(i, e));
				}
			}
			return rebuilt;
		}

		Conf restartConf() {
			var rebuilt = rebuiltMem();
			return rebuilt.isEmpty() ? initial : rebuilt.lastEntry().getValue();
		}

		boolean inWindow() {
			return pendingHistory != null;
		}

		boolean upToDateAs(Node other) {
			return lastTerm() > other.lastTerm() || (lastTerm() == other.lastTerm() && last() >= other.last());
		}
	}

	static Conf ratisConf(long index, Entry e) {
		return new Conf(0, index, e.newHalf(), e.oldHalf());
	}

	static List<List<String>> halves(Conf conf) {
		return conf.oldHalf().isEmpty() ? List.of(conf.newHalf()) : List.of(conf.newHalf(), conf.oldHalf());
	}

	static final class Model {
		final Map<String, Node> nodes = new LinkedHashMap<>();
		final Conf seeded;
		/** The committed entries: every node's committed prefix agrees with it. */
		final List<Entry> chosen = new ArrayList<>();
		final List<String> trace = new ArrayList<>();
		int removedVerdicts;
		private final Map<Node, RaftServer.Division> divisions = new HashMap<>();

		Model(Setup setup) {
			var ids = new ArrayList<>(List.of("a", "b", "c"));
			if (setup.withD()) {
				ids.add("d");
			}
			ids.add("x");
			if (setup.withY()) {
				ids.add("y");
			}
			seeded = new Conf(0, -1, ids, List.of());
			for (var id : UNIVERSE) {
				var node = new Node(id);
				boolean seed = ids.contains(id);
				node.join = !seed;
				node.up = seed;
				node.initial = seed ? seeded : new Conf(0, -1, List.of(), List.of());
				nodes.put(id, node);
			}
		}

		long maxCommit() {
			return chosen.size();
		}

		Node node(int index) {
			return nodes.get(UNIVERSE.get(Math.floorMod(index, UNIVERSE.size())));
		}

		/**
		 * Node {@code index} if {@code ok} holds for it, else one of the nodes for which it holds (by {@code index}), a
		 * node for which it does not if none (the step's own guard then refuses it). Fixed step lists name their nodes.
		 */
		Node pick(int index, java.util.function.Predicate<Node> ok) {
			var n = node(index);
			if (ok.test(n)) {
				return n;
			}
			var candidates = nodes.values().stream().filter(ok).toList();
			return candidates.isEmpty() ? n : candidates.get(Math.floorMod(index, candidates.size()));
		}

		/** Node {@code index} if it leads, else one of the leaders (by {@code index}), null if none. */
		Node leaderNode(int index) {
			var n = node(index);
			if (n.up && n.leader && !n.inWindow()) {
				return n;
			}
			var leaders = nodes.values().stream().filter(node -> node.up && node.leader && !node.inWindow()).toList();
			return leaders.isEmpty() ? null : leaders.get(Math.floorMod(index, leaders.size()));
		}

		/** Applies {@code step}; false if its guard fails (then nothing changed). */
		boolean apply(Step step) {
			boolean done = switch (step) {
				case Elect s -> elect(pick(s.n(), n -> n.up && !n.inWindow() && n.current().holds(n.id)));
				case Append s -> append(leaderNode(s.n()));
				case SetConf s -> setConf(leaderNode(s.n()), s.target(), false);
				case Change s -> {
					var leader = leaderNode(s.n());
					if (leader == null) {
						yield false;
					}
					var target = new java.util.LinkedHashSet<>(leader.current().newHalf());
					var id = UNIVERSE.get(Math.floorMod(s.node(), UNIVERSE.size()));
					if (!target.remove(id)) {
						target.add(id);
					}
					yield setConf(leader, target.stream().map(UNIVERSE::indexOf).collect(Collectors.toSet()), s.lag());
				}
				case FinishConf s -> finishConf(leaderNode(s.n()));
				case Replicate s -> {
					var leader = leaderNode(s.n());
					yield leader != null && replicate(leader, pick(s.f(), f -> f != leader && f.up && !f.inWindow()
							&& sendsTo(leader, f) && commonPrefix(leader, f) >= leader.snapshot), s.back(), false);
				}
				case Broadcast s -> broadcast(leaderNode(s.n()));
				case AdvanceCommit s -> advanceCommit(leaderNode(s.n()));
				case Apply s -> applyUpTo(pick(s.n(), n -> n.up && !n.inWindow() && n.commit - s.back() > n.applied),
						s.back());
				case Snapshot s -> snapshot(pick(s.n(), n -> n.up && !n.inWindow() && n.applied - s.back() > n.snapshot),
						s.back());
				case Install s -> {
					var leader = leaderNode(s.n());
					if (leader == null) {
						yield false;
					}
					var f = pick(s.f(), node -> node != leader && node.up && !node.inWindow() && sendsTo(leader, node)
							&& commonPrefix(leader, node) < Math.max(leader.snapshot, leader.applied));
					long p = commonPrefix(leader, f);
					if (leader.snapshot <= p && leader.applied > p && !leader.inWindow()) {
						// The leader takes a snapshot first (a Snapshot step folded in, so that installs happen).
						leader.snapshot = leader.applied;
						trace.add(new Snapshot(UNIVERSE.indexOf(leader.id), 0).toString());
					}
					yield install(leader, f, s.window(), false);
				}
				case FinishInstall s -> finishInstall(pick(s.n(), Node::inWindow));
				case Crash s -> crash(pick(s.n(), n -> n.up && !n.inWindow()));
				case Restart s -> restart(pick(s.n(), n -> !n.up));
			};
			if (done) {
				trace.add(step.toString());
				updateChosen();
			}
			return done;
		}

		boolean elect(Node n) {
			if (!n.up || n.inWindow()) {
				return false;
			}
			var conf = n.current();
			if (!conf.holds(n.id)) {
				return false;
			}
			var granted = new ArrayList<Node>();
			for (var half : halves(conf)) {
				int votes = 0;
				for (var id : half) {
					var v = nodes.get(id);
					if (v == n || (v.up && v.current().holds(n.id) && n.upToDateAs(v))) {
						votes++;
						granted.add(v);
					}
				}
				if (votes < RemovalRule.majority(half.size())) {
					return false;
				}
			}
			long term = nodes.values().stream().mapToLong(node -> node.term).max().orElse(0) + 1;
			for (var v : granted) {
				v.term = term;
				v.leader = false;
			}
			n.term = term;
			n.leader = true;
			appendEntry(n, new Entry(term, conf.newHalf(), List.of()));
			return true;
		}

		static void appendEntry(Node leader, Entry entry) {
			leader.log.add(entry);
			if (entry.conf()) {
				leader.mem.put(leader.last(), ratisConf(leader.last(), entry));
			}
		}

		boolean append(Node n) {
			if (n == null) {
				return false;
			}
			appendEntry(n, new Entry(n.term, null, null));
			return true;
		}

		boolean setConf(Node n, Set<Integer> targetIndexes, boolean lag) {
			if (n == null) {
				return false;
			}
			var cur = n.current();
			if (!cur.oldHalf().isEmpty() || cur.index() < 0 || cur.index() > n.commit) {
				return false;
			}
			var target = targetIndexes.stream().sorted().map(i -> UNIVERSE.get(Math.floorMod(i, UNIVERSE.size())))
					.distinct().toList();
			if (target.isEmpty() || Set.copyOf(target).equals(Set.copyOf(cur.newHalf()))) {
				return false;
			}
			var added = target.stream().filter(id -> !cur.newHalf().contains(id)).map(nodes::get).toList();
			for (var a : added) {
				if (a == n || (!a.log.isEmpty() && !SUBJECTS.contains(a.id)) || a.inWindow()) {
					return false;
				}
			}
			for (var a : added) {
				if (SUBJECTS.contains(a.id)) {
					wipe(a);
				}
				a.up = true;
				// add-peer brings it up to date first (staging): no member file from this.
				if (!replicate(n, a, 0, true)) {
					install(n, a, false, true);
					if (!lag) {
						replicate(n, a, 0, true);
					}
				}
				if (!n.leader) {
					return true;
				}
			}
			appendEntry(n, new Entry(n.term, target, cur.newHalf()));
			for (var a : added) {
				a.addIndex = n.last();
			}
			return true;
		}

		/** A new incarnation of {@code a}: wiped, started with --join. */
		static void wipe(Node a) {
			a.incarnation++;
			a.join = true;
			a.memberFile = false;
			a.installed = null;
			a.addIndex = Long.MAX_VALUE;
			a.leader = false;
			a.term = 0;
			a.log = new ArrayList<>();
			a.snapshot = 0;
			a.commit = 0;
			a.applied = 0;
			a.history = new ArrayList<>();
			a.completeFrom = 0;
			a.file = null;
			a.mem = new TreeMap<>();
			a.initial = new Conf(0, -1, List.of(), List.of());
			a.pendingHistory = null;
		}

		boolean finishConf(Node n) {
			if (n == null) {
				return false;
			}
			var cur = n.current();
			if (cur.oldHalf().isEmpty() || cur.index() > n.commit) {
				return false;
			}
			appendEntry(n, new Entry(n.term, cur.newHalf(), List.of()));
			return true;
		}

		/**
		 * Whether leader {@code n} sends to {@code f}: a peer of its configuration, or of the one before it (Ratis keeps
		 * the log appenders of a configuration's peers until the next one is in place, so the entry that drops a peer,
		 * a term-start entry among them, still reaches it).
		 */
		static boolean sendsTo(Node n, Node f) {
			var cur = n.current();
			if (cur.holds(f.id)) {
				return true;
			}
			var before = n.mem.lowerEntry(cur.index());
			return before != null && before.getValue().holds(f.id);
		}

		/** The length of the longest common prefix of two logs. */
		static long commonPrefix(Node a, Node b) {
			long p = 0;
			while (p < a.last() && p < b.last() && a.at(p + 1).equals(b.at(p + 1))) {
				p++;
			}
			return p;
		}

		boolean replicate(Node n, Node f, int back, boolean staging) {
			if (n == null || f == n || !f.up || f.inWindow() || (!staging && !sendsTo(n, f))) {
				return false;
			}
			if (f.term > n.term) {
				n.leader = false;
				n.term = f.term;
				return true;
			}
			long p = commonPrefix(n, f);
			long t = Math.max(0, n.last() - Math.max(0, back));
			if (t > p && p < n.snapshot) {
				return false;
			}
			f.term = n.term;
			f.leader = false;
			if (t > p) {
				while (f.last() > p) {
					f.log.removeLast();
				}
				f.mem.tailMap(p + 1, true).clear();
				for (long i = p + 1; i <= t; i++) {
					var e = n.at(i);
					f.log.add(e);
					if (e.conf()) {
						f.mem.put(i, ratisConf(i, e));
					}
				}
			}
			f.commit = Math.max(f.commit, Math.min(n.commit, t));
			if (!staging && n.current().holds(f.id) && f.current().inNew(f.id)) {
				f.memberFile = true;
			}
			return true;
		}

		boolean broadcast(Node n) {
			if (n == null) {
				return false;
			}
			boolean any = false;
			for (int pass = 0; pass < 2; pass++) {
				for (var f : nodes.values()) {
					if (n.leader) {
						any |= replicate(n, f, 0, false);
					}
				}
				if (pass == 0) {
					advanceCommit(n);
				}
			}
			return any;
		}

		boolean advanceCommit(Node n) {
			if (n == null) {
				return false;
			}
			var conf = n.current();
			for (long index = n.last(); index > n.commit; index--) {
				if (n.termAt(index) != n.term) {
					continue;
				}
				boolean all = true;
				for (var half : halves(conf)) {
					final long at = index;
					long count = half.stream().filter(id -> nodes.get(id).termAt(at) == n.term).count();
					all &= count >= RemovalRule.majority(half.size());
				}
				if (all) {
					n.commit = index;
					return true;
				}
			}
			return false;
		}

		boolean applyUpTo(Node n, int back) {
			if (!n.up || n.inWindow()) {
				return false;
			}
			long target = n.commit - Math.max(0, back);
			if (target <= n.applied) {
				return false;
			}
			for (long i = n.applied + 1; i <= target; i++) {
				var e = n.at(i);
				if (e.conf()) {
					n.history.add(new Conf(e.term(), i, e.newHalf(), e.oldHalf()));
					n.file = ratisConf(i, e);
				}
			}
			n.applied = target;
			return true;
		}

		boolean snapshot(Node n, int back) {
			if (!n.up || n.inWindow()) {
				return false;
			}
			long s = n.applied - Math.max(0, back);
			if (s <= n.snapshot) {
				return false;
			}
			n.snapshot = s;
			return true;
		}

		boolean install(Node n, Node f, boolean window, boolean staging) {
			if (n == null || f == n || !f.up || f.inWindow() || n.snapshot == 0 || (!staging && !sendsTo(n, f))) {
				return false;
			}
			long p = commonPrefix(n, f);
			var latest = n.current();
			if (p < n.snapshot) {
				// RaftTraffic, before Ratis sees the chunk (and may reject it for its term).
				f.installed = InstalledConfiguration.next(f.installed, n.term, latest.index(), latest.newHalf(),
						latest.oldHalf());
			}
			if (f.term > n.term) {
				n.leader = false;
				n.term = f.term;
				return true;
			}
			if (p >= n.snapshot) {
				return false;
			}
			long s = n.snapshot;
			f.term = n.term;
			f.leader = false;
			f.log = new ArrayList<>(n.log.subList(0, (int) s));
			f.snapshot = s;
			f.commit = s;
			f.file = latest;
			f.mem.tailMap(p + 1, true).clear();
			f.mem.put(latest.index(), latest);
			var history = n.history.stream().filter(conf -> conf.index() <= s)
					.collect(Collectors.toCollection(ArrayList::new));
			if (window) {
				f.pendingHistory = history;
				f.pendingCompleteFrom = n.completeFrom;
			} else {
				f.history = history;
				f.completeFrom = n.completeFrom;
				f.applied = s;
			}
			return true;
		}

		boolean finishInstall(Node n) {
			if (!n.inWindow()) {
				return false;
			}
			n.history = n.pendingHistory;
			n.completeFrom = n.pendingCompleteFrom;
			n.applied = n.snapshot;
			n.pendingHistory = null;
			return true;
		}

		boolean crash(Node n) {
			if (!n.up || n.inWindow()) {
				return false;
			}
			n.up = false;
			n.leader = false;
			return true;
		}

		boolean restart(Node n) {
			if (n.up) {
				return false;
			}
			n.up = true;
			n.leader = false;
			n.commit = n.applied;
			n.mem = n.rebuiltMem();
			// Ratis runs it as a follower when its configuration holds it: recordMembership writes the member file
			// (round-13 review, finding 1).
			var conf = n.current();
			if (conf.index() >= 0 && conf.inNew(n.id)) {
				n.memberFile = true;
			}
			return true;
		}

		void updateChosen() {
			divisions.clear();
			for (var n : nodes.values()) {
				for (long i = 1; i <= n.commit; i++) {
					if (i <= chosen.size()) {
						if (!chosen.get((int) i - 1).equals(n.at(i))) {
							fail("committed prefixes disagree at " + i + " on " + n.id + ": " + n.at(i) + " vs "
									+ chosen.get((int) i - 1) + "\n" + this);
						}
					} else {
						chosen.add(n.at(i));
					}
				}
			}
		}

		/** The newest committed configuration (index -1: the seeded one), with its term. */
		Conf newestCommitted() {
			for (long i = chosen.size(); i >= 1; i--) {
				var e = chosen.get((int) i - 1);
				if (e.conf()) {
					return new Conf(e.term(), i, e.newHalf(), e.oldHalf());
				}
			}
			return seeded;
		}

		// -- facts, from the real code

		RaftServer.Division division(Node n) {
			return divisions.computeIfAbsent(n, node -> {
				var log = new TreeMap<Long, RaftProtos.LogEntryProto>();
				for (long i = node.snapshot + 1; i <= node.last(); i++) {
					var e = node.at(i);
					log.put(i, e.conf() ? ModelDivision.configurationEntry(e.term(), i, e.newHalf(), e.oldHalf())
							: ModelDivision.ordinaryEntry(e.term(), i));
				}
				var conf = node.current();
				return ModelDivision.of(conf.index(), conf.newHalf(), conf.oldHalf(), UNIVERSE, log, node.snapshot,
						node.commit, node.term);
			});
		}

		CommittedConfigurations history(Node n) {
			return new CommittedConfigurations(n.history, n.completeFrom);
		}

		Own own(Node n) {
			return MembershipFacts.own(division(n), history(n), n.id, !n.join, n.memberFile, n.installed);
		}

		@Override
		public String toString() {
			var out = new StringBuilder("steps: " + trace + "\nchosen (" + chosen.size() + "): " + chosen + "\n");
			for (var n : nodes.values()) {
				out.append(n.id).append(n.incarnation > 0 ? "#" + n.incarnation : "").append(n.up ? " up" : " down")
						.append(n.leader ? " leader" : "").append(n.join ? " join" : "")
						.append(n.memberFile ? " member" : "").append(" term ").append(n.term).append(" snap ")
						.append(n.snapshot).append(" commit ").append(n.commit).append(" applied ").append(n.applied)
						.append(n.inWindow() ? " WINDOW" : "").append("\n  log ").append(n.log).append("\n  conf ")
						.append(n.current()).append(" file ").append(n.file).append(" history ").append(n.history)
						.append(" from ").append(n.completeFrom).append('\n');
			}
			return out.toString();
		}
	}

	// ---------------------------------------------------------------------------------------------------------------
	// Answers and checks

	enum Mode {
		V2, V1, PENDING, NONE
	}

	/** One check of {@code subject}: rounds as {@link RemovalCheck} asks them, the answers of each taken atomically. */
	static Verdict check(Model model, Node subject, Map<String, Mode> modes) {
		Conf kStar = null;
		var answers = new LinkedHashMap<String, Answer>();
		Verdict verdict = null;
		for (int round = 0; round < 4; round++) {
			var own = model.own(subject);
			var members = RemovalCheck.members(own, kStar, UNIVERSE);
			var probes = RemovalCheck.probes(own, kStar);
			for (var id : members) {
				var previous = answers.get(id);
				if (previous != null && previous.kind() == Answer.Kind.NONE) {
					continue;
				}
				answers.put(id, answer(model, model.nodes.get(id), modes.getOrDefault(id, Mode.V2), probes));
			}
			answers.keySet().retainAll(members);
			verdict = RemovalRule.decide(own, answers);
			if (verdict.outcome() != Outcome.UNDECIDED || verdict.reason() == Reason.INCONSISTENT
					|| Objects.equals(verdict.kStar(), kStar)) {
				return verdict;
			}
			kStar = verdict.kStar();
		}
		return verdict;
	}

	static Answer answer(Model model, Node n, Mode mode, List<TermIndex> probes) {
		if (!n.up || mode == Mode.NONE) {
			return Answer.NONE;
		}
		if (mode == Mode.PENDING) {
			return Answer.PENDING;
		}
		var division = model.division(n);
		if (mode == Mode.V1) {
			return Answer.of(RemovalCheck.fromVersion1(TermService.versionOneAnswer(division)));
		}
		var report = MembershipFacts.report(division, model.history(n), probes);
		checkProbe(model, n, probes.get(0), report.ownProbe());
		if (probes.size() > 1) {
			checkProbe(model, n, probes.get(1), report.kStarProbe());
		}
		return Answer.of(report);
	}

	/** The fact oracle on a probe answer of {@code n}. */
	static void checkProbe(Model model, Node n, TermIndex probe, Probe answer) {
		long index = probe.getIndex();
		if (answer == Probe.COMMITTED) {
			assertTrue(index >= 1 && index <= model.chosen.size() && model.chosen.get((int) index - 1).conf()
					&& model.chosen.get((int) index - 1).term() == probe.getTerm(),
					() -> n.id + " answered COMMITTED for " + probe + ", not a committed configuration\n" + model);
		}
		if (answer == Probe.ABSENT) {
			boolean holds = index >= 1 && index <= n.last() && n.at(index).conf() && n.termAt(index) == probe.getTerm();
			assertTrue(!holds, () -> n.id + " answered ABSENT for " + probe + ", which its log holds\n" + model);
		}
	}

	/** The fact oracle: what a running node reports committed is a configuration the model committed. */
	static void checkFacts(Model model) {
		for (var n : model.nodes.values()) {
			if (!n.up) {
				continue;
			}
			var committed = MembershipFacts.committed(model.division(n), model.history(n));
			if (committed == null || committed.index() < 0) {
				continue;
			}
			long index = committed.index();
			boolean ok = index <= model.chosen.size() && model.chosen.get((int) index - 1).conf();
			if (ok) {
				var e = model.chosen.get((int) index - 1);
				ok = Set.copyOf(e.newHalf()).equals(Set.copyOf(committed.newHalf()))
						&& Set.copyOf(e.oldHalf()).equals(Set.copyOf(Objects.requireNonNull(committed.oldHalf())))
						&& (!committed.termKnown() || committed.term() == e.term());
			}
			assertTrue(ok, () -> n.id + " reports " + committed + " committed, which the model did not commit\n" + model);
		}
	}

	/** S1 for a REMOVED verdict of {@code subject}. */
	static void checkRemovedIsSafe(Model model, Node subject, Verdict verdict, Map<String, Mode> modes) {
		var newest = model.newestCommitted();
		assertTrue(!newest.inNew(subject.id), () -> "S1 (a): " + subject.id + " " + verdict + " with modes " + modes
				+ ", but the newest committed configuration " + newest + " holds it\n" + model);
		long maxCommit = model.maxCommit();
		for (var n : model.nodes.values()) {
			if (n == subject || n.last() < maxCommit) {
				// A log that lacks a committed entry never leads (Leader Completeness), so its tail never commits.
				continue;
			}
			boolean onBranch = true;
			for (long i = 1; i <= maxCommit && onBranch; i++) {
				onBranch = n.at(i).equals(model.chosen.get((int) i - 1));
			}
			if (!onBranch) {
				continue;
			}
			// What may still commit through n: a configuration entry of its log after the committed prefix (it leads
			// with Ratis' configuration now or after a restart), or Ratis' configuration itself, which its term-start
			// entry copies (with that same configuration).
			var tail = new ArrayList<Conf>();
			for (long i = maxCommit + 1; i <= n.last(); i++) {
				var e = n.at(i);
				if (e.conf()) {
					tail.add(ratisConf(i, e));
				}
			}
			for (var electoral : List.of(n.current(), n.restartConf())) {
				var candidates = new ArrayList<>(tail);
				candidates.add(electoral);
				for (var conf : candidates) {
					if (conf.inNew(subject.id) && (conf.index() < 0 || conf.index() > maxCommit)
							&& electableWithout(model, n, electoral, subject)) {
						fail("S1 (b): " + subject.id + " " + verdict + " with modes " + modes + ", but " + n.id
								+ " holds " + conf + " and is electable without it under " + electoral + "\n" + model);
					}
				}
			}
		}
	}

	static boolean electableWithout(Model model, Node n, Conf conf, Node subject) {
		if (!conf.holds(n.id)) {
			return false;
		}
		for (var half : halves(conf)) {
			long votes = half.stream().filter(id -> !id.equals(subject.id))
					.filter(id -> model.nodes.get(id) == n || n.upToDateAs(model.nodes.get(id))).count();
			if (votes < RemovalRule.majority(half.size())) {
				return false;
			}
		}
		return true;
	}

	static Map<String, Mode> randomModes(Random random) {
		var modes = new LinkedHashMap<String, Mode>();
		for (var id : UNIVERSE) {
			int r = random.nextInt(10);
			modes.put(id, r < 6 ? Mode.V2 : r < 8 ? Mode.V1 : r < 9 ? Mode.PENDING : Mode.NONE);
		}
		return modes;
	}

	/** The checks after a step: facts, then for x and y S1 under several sets of answers, and no false alarm. */
	static void checkState(Model model, Random random) {
		checkFacts(model);
		for (var id : SUBJECTS) {
			var subject = model.nodes.get(id);
			if (!subject.up) {
				continue;
			}
			for (var modes : List.of(Map.<String, Mode>of(), randomModes(random), randomModes(random))) {
				var verdict = check(model, subject, modes);
				if (modes.isEmpty()) {
					assertTrue(verdict.reason() != Reason.INCONSISTENT,
							() -> "false alarm for " + id + ": " + verdict + "\n" + model);
				}
				if (verdict.outcome() == Outcome.REMOVED) {
					model.removedVerdicts++;
					checkRemovedIsSafe(model, subject, verdict, modes);
				}
			}
		}
	}

	/**
	 * Quiesces the model: installs finished, the newest committed configuration's members up, a fresh leader among
	 * them, every change finished, everything replicated to the members, committed and applied. False if that fails
	 * (no leader could be elected, or it lost its leadership).
	 */
	static boolean quiesce(Model model) {
		for (var n : model.nodes.values()) {
			model.finishInstall(n);
		}
		var members = model.newestCommitted().newHalf();
		for (var id : members) {
			model.restart(model.nodes.get(id));
		}
		var candidates = new ArrayList<>(members.stream().map(model.nodes::get).toList());
		candidates.sort((p, q) -> Long.compare(q.lastTerm() * 1_000_000 + q.last(), p.lastTerm() * 1_000_000 + p.last()));
		Node leader = null;
		for (var candidate : candidates) {
			if (model.elect(candidate)) {
				leader = candidate;
				break;
			}
		}
		if (leader == null) {
			return false;
		}
		for (int round = 0; round < 8; round++) {
			for (var id : UNIVERSE) {
				var f = model.nodes.get(id);
				if (f == leader || !f.up || !leader.current().holds(id)) {
					continue;
				}
				if (!model.replicate(leader, f, 0, false)) {
					model.install(leader, f, false, false);
					model.replicate(leader, f, 0, false);
				}
				if (!leader.leader) {
					return false;
				}
			}
			model.advanceCommit(leader);
			model.finishConf(leader);
		}
		for (var n : model.nodes.values()) {
			if (n.up) {
				model.applyUpTo(n, 0);
			}
		}
		model.updateChosen();
		var cur = leader.current();
		return leader.leader && cur.oldHalf().isEmpty() && cur.index() == model.newestCommitted().index();
	}

	/** S2: after quiescing, x or y, a voter once and out of the newest configuration's new half, is REMOVED. */
	static void checkLiveness(Model model) {
		if (!quiesce(model)) {
			Statistics.label("S2").collect("not quiesced");
			return;
		}
		var newest = model.newestCommitted();
		for (var id : SUBJECTS) {
			var subject = model.nodes.get(id);
			boolean voterOnce = !subject.join || subject.memberFile;
			if (!voterOnce || newest.holds(id) || subject.leader) {
				continue;
			}
			if (subject.join && !addCommitted(model, subject)) {
				Statistics.label("S2").collect("skipped: its add never committed");
				continue;
			}
			Statistics.label("S2").collect("checked");
			model.restart(subject);
			model.finishInstall(subject);
			model.updateChosen();
			var verdict = check(model, subject, Map.of());
			assertEquals(Reason.REMOVED, verdict.reason(), () -> "S2: " + id + " " + verdict + "\n" + model);
		}
	}

	/** Whether the add of {@code subject}'s current incarnation committed: a committed configuration holds it. */
	static boolean addCommitted(Model model, Node subject) {
		for (long i = Math.max(1, subject.addIndex); i <= model.maxCommit(); i++) {
			var e = model.chosen.get((int) i - 1);
			if (e.conf() && e.newHalf().contains(subject.id)) {
				return true;
			}
		}
		return false;
	}

	static Model run(Setup setup, List<Step> steps, long seed, boolean strict) {
		var model = new Model(setup);
		var random = new Random(seed);
		for (var step : steps) {
			boolean applied = model.apply(step);
			if (strict) {
				assertTrue(applied, () -> "step not applicable: " + step + "\n" + model);
			}
			if (applied) {
				checkState(model, random);
			}
		}
		return model;
	}

	/** What a random history reached, for jqwik's statistics in the test output. */
	static void collect(Model model) {
		Statistics.label("steps applied").collect(Math.min(model.trace.size() / 10 * 10, 60));
		Statistics.label("committed entries").collect(Math.min(model.maxCommit() / 5 * 5, 30));
		Statistics.label("x or y removed by a committed configuration")
				.collect(SUBJECTS.stream().anyMatch(id -> !model.newestCommitted().inNew(id)
						&& model.nodes.get(id).incarnation == 0 && model.seeded.inNew(id)));
		Statistics.label("snapshot installs").collect(model.trace.stream().anyMatch(t -> t.startsWith("Install")));
		Statistics.label("REMOVED verdicts seen").collect(model.removedVerdicts > 0);
	}

	// ---------------------------------------------------------------------------------------------------------------
	// Properties

	@Provide
	Arbitrary<Setup> setups() {
		return Combinators.combine(Arbitraries.of(true, false), Arbitraries.of(true, false)).as(Setup::new);
	}

	@Provide
	Arbitrary<List<Step>> histories() {
		var node = Arbitraries.integers().between(0, UNIVERSE.size() - 1);
		Arbitrary<Step> elect = node.map(Elect::new);
		Arbitrary<Step> append = node.map(Append::new);
		Arbitrary<Step> setConf = Combinators.combine(node, node, Arbitraries.of(false, false, true)).as(Change::new);
		Arbitrary<Step> finish = node.map(FinishConf::new);
		Arbitrary<Step> replicate = Combinators.combine(node, node, Arbitraries.of(0, 0, 0, 1, 2)).as(Replicate::new);
		Arbitrary<Step> broadcast = node.map(Broadcast::new);
		Arbitrary<Step> commit = node.map(AdvanceCommit::new);
		Arbitrary<Step> apply = Combinators.combine(node, Arbitraries.of(0, 0, 1, 3)).as(Apply::new);
		Arbitrary<Step> snapshot = Combinators.combine(node, Arbitraries.integers().between(0, 3)).as(Snapshot::new);
		Arbitrary<Step> install = Combinators.combine(node, node, Arbitraries.of(false, false, true)).as(Install::new);
		Arbitrary<Step> finishInstall = node.map(FinishInstall::new);
		Arbitrary<Step> crash = node.map(Crash::new);
		Arbitrary<Step> restart = node.map(Restart::new);
		return Arbitraries.frequencyOf(Tuple.of(4, elect), Tuple.of(4, append), Tuple.of(3, setConf),
				Tuple.of(3, finish), Tuple.of(6, replicate), Tuple.of(6, broadcast), Tuple.of(3, commit),
				Tuple.of(5, apply), Tuple.of(5, snapshot), Tuple.of(5, install), Tuple.of(1, finishInstall),
				Tuple.of(3, crash), Tuple.of(3, restart)).list().ofMinSize(30).ofMaxSize(120);
	}

	/** Facts, S1 and no false alarm after every step of a random history. */
	@Property(tries = 2000)
	void aRemovalIsNeverConcludedWhileTheNodeIsOrMayBecomeAMember(@ForAll("setups") Setup setup,
			@ForAll("histories") List<Step> steps, @ForAll long seed) {
		collect(run(setup, steps, seed, false));
	}

	/** S2 at the end of a random history, after quiescing. */
	@Property(tries = 1000)
	void aRemovedNodeExitsOnceTheClusterIsQuiet(@ForAll("setups") Setup setup, @ForAll("histories") List<Step> steps,
			@ForAll long seed) {
		var model = run(setup, steps, seed, false);
		collect(model);
		checkLiveness(model);
	}

	// ---------------------------------------------------------------------------------------------------------------
	// Examples: the scenarios of rounds 11 and 12. Node indexes: a 0, b 1, c 2, d 3, x 4.

	static final int A = 0;
	static final int B = 1;
	static final int C = 2;
	static final int D = 3;
	static final int X = 4;
	static final Setup FIVE = new Setup(true, false);

	static List<Step> steps(Object... steps) {
		var list = new ArrayList<Step>();
		for (var step : steps) {
			if (step instanceof Step s) {
				list.add(s);
			} else {
				for (var s : (List<?>) step) {
					list.add((Step) s);
				}
			}
		}
		return list;
	}

	static List<Step> repeat(int times, Step step) {
		return Collections.nCopies(times, step);
	}

	static List<Step> toAll(int leader, int... followers) {
		var list = new ArrayList<Step>();
		for (var f : followers) {
			list.add(new Replicate(leader, f, 0));
		}
		return list;
	}

	/** R11-1: a stable configuration without x from a term-start entry, dropped by the next leader: x is a member. */
	@Example
	void aFlattenedRemovalThatWasDroppedIsNoRemoval() {
		var model = run(FIVE, steps(new Elect(A), toAll(A, B, C, D, X), new AdvanceCommit(A), toAll(A, B, C, D, X),
				new SetConf(A, Set.of(A, B, C, D)), new Replicate(A, X, 0), new Elect(A), new Replicate(A, X, 0),
				new Crash(A), new Elect(B), toAll(B, C, D), new AdvanceCommit(B), new Crash(X), new Restart(X)), 1, true);
		var x = model.nodes.get("x");
		assertTrue(x.current().oldHalf().isEmpty() && !x.current().holds("x"), () -> "flattened\n" + model);
		assertEquals(Reason.MEMBER, check(model, x, Map.of()).reason());
		checkLiveness(model);
	}

	/** R11-2: x led terms with appends that reached nobody, then was removed at a lower index and a higher term. */
	@Example
	void aRemovedNodeWithALongerTailOfALowerTermIsRemoved() {
		var model = run(FIVE, steps(new Elect(X), toAll(X, A, B, C, D), new AdvanceCommit(X), toAll(X, A, B, C, D),
				repeat(5, new Append(X)), new Elect(X), repeat(3, new Append(X)), new Crash(X), new Elect(A),
				toAll(A, B, C, D), new AdvanceCommit(A), new SetConf(A, Set.of(A, B, C, D)), toAll(A, B, C, D),
				new AdvanceCommit(A), new FinishConf(A), toAll(A, B, C, D), new AdvanceCommit(A), toAll(A, B, C, D),
				new Restart(X)), 2, true);
		assertEquals(Reason.REMOVED, check(model, model.nodes.get("x"), Map.of()).reason());
	}

	/**
	 * Round-11 finding 1, then its purge (round-13 review): c installs a snapshot from a, whose latest configuration J
	 * (removing x) is not committed; the next leader b drops J; c restarts before it applies b's configuration, and
	 * Ratis takes J from raft-meta.conf again. Then c applies and purges past J's index: J stays in Ratis' memory and
	 * must not be reported committed (the fact oracle fails at c6925a7).
	 */
	@Example
	void aStaleConfigurationOfRatisIsNeverACommittedFact() {
		var model = run(FIVE, steps(new Elect(A), toAll(A, B, C, D, X), new AdvanceCommit(A), toAll(A, B, C, D, X),
				repeat(2, new Append(A)), toAll(A, B, D), new AdvanceCommit(A), new Apply(A, 0), new Snapshot(A, 0),
				repeat(4, new Append(A)), new SetConf(A, Set.of(A, B, C, D)), new Install(A, C, false), new Crash(A),
				new Elect(B), repeat(5, new Append(B)), toAll(B, C, D, X), new AdvanceCommit(B), toAll(B, C, D, X),
				new Crash(C), new Restart(C), new Replicate(B, C, 0), new Apply(C, 0), new Snapshot(C, 0)), 3, true);
		var c = model.nodes.get("c");
		assertTrue(c.current().index() == 8 && c.current().oldHalf().contains("x") && !c.at(8).conf()
				&& c.snapshot >= 8, () -> "J is stale and purged\n" + model);
		assertEquals(Reason.MEMBER, check(model, model.nodes.get("x"), Map.of()).reason());
	}

	/**
	 * R12-3: x installs a snapshot from a, whose latest configuration (holding x, removing d) is not committed, and goes
	 * down; b drops it and removes x below its index; x restarts with it past the end of its log. x must exit (S2); at
	 * c6925a7 it waits outside with OWN_LATER_CONFIGURATION.
	 */
	@Example
	void aConfigurationFromAnInstallPastTheEndOfTheLogDoesNotKeepARemovedNodeWaiting() {
		var model = run(FIVE, steps(new Elect(A), toAll(A, B, C, D, X), new AdvanceCommit(A), toAll(A, B, C, D, X),
				repeat(2, new Append(A)), toAll(A, B, C, D), new AdvanceCommit(A), new Apply(A, 0), new Snapshot(A, 0),
				repeat(6, new Append(A)), new SetConf(A, Set.of(A, B, C, X)), new Install(A, X, false), new Crash(X),
				new Crash(A), new Elect(B), toAll(B, C, D), new AdvanceCommit(B), new SetConf(B, Set.of(A, B, C, D)),
				toAll(B, C, D), new AdvanceCommit(B), new FinishConf(B), toAll(B, C, D), new AdvanceCommit(B),
				toAll(B, C, D), new Restart(X)), 4, true);
		var x = model.nodes.get("x");
		assertTrue(x.current().index() > x.last() && x.current().holds("x"), () -> "past x's log\n" + model);
		assertEquals(Reason.REMOVED, check(model, x, Map.of()).reason(), model::toString);
		checkLiveness(model);
	}

	/**
	 * Round-13 review, finding 1: x, removed and then wiped and added again, is brought up to date only to a's snapshot
	 * (Ratis' staging gap); a appends the joint add D and installs a later snapshot on x, with D past its end; x
	 * restarts, Ratis runs it with D and it writes the member file. b, c and d have not got D yet, and a does not
	 * answer: D may still commit through a, so x must wait (OWN_LATER_CONFIGURATION). D's term bound (a's term) is not
	 * below K*'s. With the R12-3 rule (no bound), the verdict is REMOVED and S1 (b) fails.
	 */
	@Example
	void aReAddFromAnInstallPastTheEndOfTheLogKeepsTheNodeWaiting() {
		var model = run(FIVE, steps(new Elect(A), toAll(A, B, C, D, X), new AdvanceCommit(A), toAll(A, B, C, D, X),
				new Change(A, X, false), toAll(A, B, C, D), new AdvanceCommit(A), new FinishConf(A), toAll(A, B, C, D),
				new AdvanceCommit(A), toAll(A, B, C, D), repeat(2, new Append(A)), toAll(A, B, C, D), new AdvanceCommit(A),
				new Apply(A, 0), new Snapshot(A, 0), repeat(2, new Append(A)), toAll(A, B, C, D), new AdvanceCommit(A),
				new Apply(A, 0), new Change(A, X, true), new Install(A, X, false), new Crash(X), new Restart(X)), 7, true);
		var x = model.nodes.get("x");
		assertTrue(x.incarnation == 1 && x.memberFile && x.current().index() > x.last() && x.current().inNew("x")
				&& !x.current().oldHalf().isEmpty(), () -> "D past x's log\n" + model);
		var modes = Map.of("a", Mode.NONE);
		var verdict = check(model, x, modes);
		if (verdict.outcome() == Outcome.REMOVED) {
			checkRemovedIsSafe(model, x, verdict, modes);
		}
		assertEquals(Reason.OWN_LATER_CONFIGURATION, verdict.reason(), () -> verdict + "\n" + model);
	}

	/**
	 * R12-2: d installs a snapshot that covers the committed removal of x, and stops between Ratis' snapshot index and
	 * the history swap: its probe of K* must not say absent (the probe oracle fails at c6925a7).
	 */
	@Example
	void aProbeDuringAnInstallIsNotAbsent() {
		var model = run(FIVE, steps(new Elect(A), toAll(A, B, C, D, X), new AdvanceCommit(A), toAll(A, B, C, D, X),
				new Crash(D), new SetConf(A, Set.of(A, B, C, D)), toAll(A, B, C, X), new AdvanceCommit(A),
				new FinishConf(A), toAll(A, B, C), new AdvanceCommit(A), toAll(A, B, C), repeat(2, new Append(A)),
				toAll(A, B, C), new AdvanceCommit(A), new Apply(A, 0), new Snapshot(A, 0), new Restart(D),
				new Install(A, D, true)), 5, true);
		var d = model.nodes.get("d");
		assertTrue(d.inWindow(), model::toString);
		var kStar = model.newestCommitted();
		var probe = MembershipFacts.probe(model.division(d), model.history(d), kStar.term(), kStar.index());
		assertTrue(probe != Probe.ABSENT, () -> "d answers ABSENT for K* " + kStar + "\n" + model);
	}

	/**
	 * R12-1: b and c, of a version without Configuration2, hold the stale J of finding 1 (new half {b, c}, removing a,
	 * d and x) and answer it committed. x, a member of the committed configuration, must not be removed on their
	 * answers (S1 (a) fails at c6925a7).
	 */
	@Example
	void aStaleVersionOneMajorityDoesNotRemove() {
		var model = run(FIVE, steps(new Elect(A), toAll(A, B, C, D, X), new AdvanceCommit(A), toAll(A, B, C, D, X),
				repeat(2, new Append(A)), toAll(A, D, X), new AdvanceCommit(A), new Apply(A, 0), new Snapshot(A, 0),
				repeat(4, new Append(A)), new SetConf(A, Set.of(B, C)), new Install(A, B, false),
				new Install(A, C, false), new Crash(A), new Elect(D), repeat(6, new Append(D)), toAll(D, B, C, X),
				new AdvanceCommit(D), toAll(D, B, C, X), new Crash(B), new Restart(B), new Crash(C), new Restart(C),
				toAll(D, B, C)), 6, true);
		var x = model.nodes.get("x");
		assertTrue(model.nodes.get("b").current().newHalf().equals(List.of("b", "c")), () -> "stale J\n" + model);
		var modes = Map.of("b", Mode.V1, "c", Mode.V1);
		var verdict = check(model, x, modes);
		if (verdict.outcome() == Outcome.REMOVED) {
			checkRemovedIsSafe(model, x, verdict, modes);
		}
		assertEquals(Reason.NEEDS_TERM, verdict.reason(), verdict::toString);
		assertEquals(Reason.MEMBER, check(model, x, Map.of()).reason());
	}
}
