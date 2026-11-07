package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.distributed.RemovalRule.Answer;
import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import it.cavallium.rockserver.distributed.RemovalRule.Probe;
import java.lang.reflect.Proxy;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;

/**
 * R11 review, finding 1: Ratis' configuration of a node is not always the entry at its index. A snapshot install
 * writes the leader's latest configuration J (index c, not committed) to {@code raft-meta.conf}; a later leader E
 * replaces the entry at c with an ordinary one; the node restarts before it applies E's first configuration (at e &lt;
 * c), and Ratis loads J again and keeps it against the lower configurations of the log. The node must not report J
 * committed (it took the commit index and the term at c from E's entry), nor answer a probe of (E's term, c) as a
 * committed configuration.
 */
class MembershipFactsTest {

	private static final List<String> ALL = List.of("n0", "n1", "n2", "n3", "x");
	private static final List<String> WITHOUT_X = List.of("n0", "n1", "n2", "n3");

	private static RaftPeer peer(String id) {
		return RaftPeer.newBuilder().setId(id).build();
	}

	private static List<RaftPeer> peers(List<String> ids) {
		return ids.stream().map(MembershipFactsTest::peer).toList();
	}

	private static RaftProtos.LogEntryProto configurationEntry(long term, long index, List<String> current,
			List<String> previous) {
		var conf = RaftProtos.RaftConfigurationProto.newBuilder();
		peers(current).forEach(peer -> conf.addPeers(peer.getRaftPeerProto()));
		peers(previous).forEach(peer -> conf.addOldPeers(peer.getRaftPeerProto()));
		return RaftProtos.LogEntryProto.newBuilder().setTerm(term).setIndex(index).setConfigurationEntry(conf).build();
	}

	private static RaftProtos.LogEntryProto ordinaryEntry(long term, long index) {
		return RaftProtos.LogEntryProto.newBuilder().setTerm(term).setIndex(index)
				.setStateMachineLogEntry(RaftProtos.StateMachineLogEntryProto.getDefaultInstance()).build();
	}

	@SuppressWarnings("unchecked")
	private static <T> T proxy(Class<T> type, java.util.function.BiFunction<String, Object[], Object> handler) {
		return (T) Proxy.newProxyInstance(MembershipFactsTest.class.getClassLoader(), new Class<?>[] {type},
				(_, method, args) -> switch (method.getName()) {
					case "toString" -> type.getSimpleName();
					case "hashCode" -> 0;
					case "equals" -> false;
					default -> handler.apply(method.getName(), args);
				});
	}

	/** A division whose Ratis configuration is {@code current}/{@code previous} at {@code confIndex}. */
	private static RaftServer.Division division(long confIndex, List<String> current, List<String> previous,
			TreeMap<Long, RaftProtos.LogEntryProto> log, long snapshotIndex, long commitIndex) {
		var conf = proxy(RaftConfiguration.class, (method, _) -> switch (method) {
			case "getLogEntryIndex" -> confIndex;
			case "getCurrentPeers" -> peers(current);
			case "getPreviousPeers" -> peers(previous);
			case "getAllPeers" -> peers(ALL);
			default -> throw new UnsupportedOperationException(method);
		});
		var raftLog = proxy(RaftLog.class, (method, args) -> switch (method) {
			case "getTermIndex" -> {
				var entry = log.get((Long) args[0]);
				yield entry == null ? null : TermIndex.valueOf(entry.getTerm(), entry.getIndex());
			}
			case "get" -> log.get((Long) args[0]);
			case "getEntries" -> log.subMap((Long) args[0], (Long) args[1]).values().stream()
					.map(LogEntryHeader::valueOf).toArray(LogEntryHeader[]::new);
			case "getSnapshotIndex" -> snapshotIndex;
			case "getLastCommittedIndex" -> commitIndex;
			default -> throw new UnsupportedOperationException(method);
		});
		var info = proxy(DivisionInfo.class, (method, _) -> switch (method) {
			case "getLifeCycleState" -> LifeCycle.State.RUNNING;
			case "getLeaderId" -> null;
			case "getCurrentTerm" -> 3L;
			default -> throw new UnsupportedOperationException(method);
		});
		return proxy(RaftServer.Division.class, (method, _) -> switch (method) {
			case "getInfo" -> info;
			case "getRaftConf" -> conf;
			case "getRaftLog" -> raftLog;
			default -> throw new UnsupportedOperationException(method);
		});
	}

	/**
	 * F's log after the restart: entries 10..25 after a snapshot at 9; E (term 3) wrote its term-start configuration
	 * C_old (with X) at 15 and ordinary entries after it, 20 among them; its commit index is 25. F applied up to 9
	 * only, so its history ends with a configuration at 5. Ratis' configuration is J (removing X) at 20, from
	 * {@code raft-meta.conf}.
	 */
	private static TreeMap<Long, RaftProtos.LogEntryProto> logOfE() {
		var log = new TreeMap<Long, RaftProtos.LogEntryProto>();
		for (long index = 10; index <= 25; index++) {
			log.put(index, index == 15 ? configurationEntry(3, 15, ALL, List.of()) : ordinaryEntry(index < 15 ? 1 : 3,
					index));
		}
		return log;
	}

	private static CommittedConfigurations historyUpTo5() {
		return new CommittedConfigurations(List.of(new Conf(1, 5, ALL, List.of())), 0);
	}

	@Test
	void aStaleConfigurationOverAnotherLeadersEntryIsNotCommitted() {
		var division = division(20, WITHOUT_X, ALL, logOfE(), 9, 25);
		var history = historyUpTo5();

		var committed = assertNotNullAndGet(MembershipFacts.committed(division, history));
		assertNotEquals(20, committed.index(), "J taken as committed from E's entry at its index: " + committed);
		assertTrue(committed.inNew("x"), committed.toString());
		var latest = MembershipFacts.latest(division, history);
		assertEquals(20, latest.index());
		assertFalse(latest.termKnown(), "J given the term of E's entry: " + latest);
		assertEquals(Probe.ABSENT, MembershipFacts.probe(division, history, 3, 20),
				"(3, 20) is an ordinary entry: no configuration has that term and index");
		assertEquals(Probe.COMMITTED, MembershipFacts.probe(division, history, 3, 15), "C_old, E's configuration");
	}

	/**
	 * Round-13 review: the same stale J once F purged its log up to 25 (a snapshot at 25). The entry at 20 is gone, so
	 * nothing in the log tells J from E's ordinary entry; the history, complete from 0, recorded C_old at 15 and nothing at
	 * 20, so J was never committed. Below the first index the history covers, the snapshot still decides (a database of
	 * an older version).
	 */
	@Test
	void aStaleConfigurationOverAPurgedEntryIsNotCommitted() {
		var division = division(20, WITHOUT_X, ALL, new TreeMap<>(), 25, 25);
		var cOld = new Conf(3, 15, ALL, List.of());
		var history = new CommittedConfigurations(List.of(new Conf(1, 5, ALL, List.of()), cOld), 0);
		var committed = assertNotNullAndGet(MembershipFacts.committed(division, history));
		assertEquals(cOld, committed, "J taken as committed from the snapshot: " + committed);
		assertFalse(MembershipFacts.latest(division, history).termKnown());
		assertEquals(cOld, MembershipFacts.report(division, history, List.of()).committed());
		// A history that does not cover 20 cannot tell: the snapshot covers J's index, as before.
		var older = new CommittedConfigurations(List.of(new Conf(1, 5, ALL, List.of())), 22);
		assertEquals(20, assertNotNullAndGet(MembershipFacts.committed(division, older)).index());
		// The same configuration recorded at 20: committed, with the recorded term.
		var j = new Conf(4, 20, WITHOUT_X, ALL);
		var recorded = new CommittedConfigurations(List.of(new Conf(1, 5, ALL, List.of()), cOld, j), 0);
		assertEquals(j, MembershipFacts.committed(division, recorded));
	}

	/**
	 * R12-2: during a snapshot install Ratis moves its snapshot index (to 30 here) before the state machine swaps in the
	 * snapshot's history; meanwhile the old history (applied up to about 12) has no record for a configuration
	 * committed in between. A missing record in the snapshot is unknown, never absent: absent would drop a blocker.
	 */
	@Test
	void aConfigurationInTheSnapshotWithNoRecordIsUnknown() {
		var division = division(5, ALL, List.of(), new TreeMap<>(), 30, 30);
		var history = new CommittedConfigurations(List.of(new Conf(1, 5, ALL, List.of())), 0);
		assertEquals(Probe.UNKNOWN, MembershipFacts.probe(division, history, 2, 20), "no record at 20");
		assertEquals(Probe.COMMITTED, MembershipFacts.probe(division, history, 1, 5));
		assertEquals(Probe.ABSENT, MembershipFacts.probe(division, history, 2, 5), "another term recorded at 5");
	}

	/** The asker X of the scenario: before the fix, F's answers and the members' probes removed it. */
	@Test
	void xIsNotRemovedOnTheStaleConfiguration() {
		var f = division(20, WITHOUT_X, ALL, logOfE(), 9, 25);
		var history = historyUpTo5();
		// The members of J's new half hold E's log, with C_old as their configuration at 15.
		var member = division(15, ALL, List.of(), logOfE(), 9, 25);
		var answers = new LinkedHashMap<String, Answer>();
		answers.put("n0", Answer.of(MembershipFacts.report(f, history, List.of(TermIndex.valueOf(1, 5)))));
		long[] probe = {MembershipFacts.latest(f, history).term(), 20};
		for (var id : List.of("n1", "n2", "n3")) {
			answers.put(id, Answer.of(MembershipFacts.report(member, history,
					List.of(TermIndex.valueOf(1, 5), TermIndex.valueOf(probe[0], probe[1])))));
		}
		var own = new RemovalRule.Own("x", new Conf(1, 5, ALL, List.of()), new Conf(1, 5, ALL, List.of()),
				_ -> RemovalRule.BEYOND, true, false);
		var verdict = RemovalRule.decide(own, Map.copyOf(answers));
		assertNotEquals(RemovalRule.Outcome.REMOVED, verdict.outcome(), verdict.toString());
	}

	@Test
	void theSameConfigurationAtItsIndexIsCommitted() {
		var log = logOfE();
		log.put(20L, configurationEntry(3, 20, WITHOUT_X, ALL));
		var division = division(20, WITHOUT_X, ALL, log, 9, 25);
		var history = historyUpTo5();
		var committed = assertNotNullAndGet(MembershipFacts.committed(division, history));
		assertEquals(20, committed.index());
		assertEquals(3, committed.term());
		assertEquals(3, MembershipFacts.latest(division, history).term());
		assertEquals(Probe.COMMITTED, MembershipFacts.probe(division, history, 3, 20));
	}

	@Test
	void anotherConfigurationAtItsIndexDoesNotCount() {
		var log = logOfE();
		log.put(20L, configurationEntry(3, 20, ALL, List.of()));
		var division = division(20, WITHOUT_X, ALL, log, 9, 25);
		var history = historyUpTo5();
		assertEquals(5, assertNotNullAndGet(MembershipFacts.committed(division, history)).index());
		assertFalse(MembershipFacts.latest(division, history).termKnown());
	}

	/** In the snapshot: the history gives the term when it recorded the same configuration at that index. */
	@Test
	void aPurgedConfigurationTakesItsTermFromTheHistory() {
		var division = division(5, ALL, List.of(), logOfE(), 9, 25);
		var history = historyUpTo5();
		var committed = assertNotNullAndGet(MembershipFacts.committed(division, history));
		assertEquals(new Conf(1, 5, ALL, List.of()), committed);
		assertEquals(1, MembershipFacts.latest(division, history).term());
	}

	private static Conf assertNotNullAndGet(Conf conf) {
		assertNotNull(conf);
		return conf;
	}
}
