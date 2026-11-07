package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.distributed.RemovalRule.Answer;
import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import it.cavallium.rockserver.distributed.RemovalRule.Own;
import it.cavallium.rockserver.distributed.RemovalRule.Probe;
import it.cavallium.rockserver.distributed.RemovalRule.Reason;
import it.cavallium.rockserver.distributed.RemovalRule.Report;
import it.cavallium.rockserver.distributed.RemovalRule.Verdict;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.function.Supplier;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * One membership check (R11): asks the members, in rounds, for their configurations ({@link TermService#CONFIGURATION2},
 * or {@link TermService#CONFIGURATION} of a member that predates it), and decides with {@link RemovalRule#decide}.
 * {@link ReplicatedNode} runs it at the start of a node with an existing data directory and, every few election
 * timeouts, while the node hears no leader.
 *
 * <p>The members asked are those of the halves of the node's own latest and committed configurations and of the
 * current K*, recomputed every round, plus the nodes of its peer list. Each round asks them in parallel, with the probes of the node's own latest
 * configuration entry and of the current K*. The check returns as soon as the verdict is not undecided, or when
 * asking again cannot change it (every member answered, none is starting, blocking or behind K*), or when its budget
 * ends. A member that cannot be reached counts as no answer ({@link Answer#NONE}) and is not asked again in this
 * check; the next check asks it again.</p>
 */
final class RemovalCheck {

	private static final Logger LOG = LoggerFactory.getLogger(RemovalCheck.class);
	private static final long RETRY_MS = 100;

	/** A version-1 answer: a member's latest configuration index, whether it is committed there, and its new half. */
	record PeerConfiguration(long index, boolean committed, List<String> members) {}

	/** Asks member {@code id} at {@code address}; completes with its answer, never exceptionally. */
	@FunctionalInterface
	interface Asker {
		CompletableFuture<Answer> ask(String id, String address, List<TermIndex> probes, Duration timeout);
	}

	private RemovalCheck() {}

	/**
	 * A version-1 answer as a report: a committed one gives the committed and the latest configuration (term and old
	 * half unknown), one that is not committed gives the latest only. No probe answers: they are {@link Probe#UNKNOWN}.
	 */
	static Report fromVersion1(PeerConfiguration answer) {
		var conf = new Conf(RemovalRule.UNKNOWN_TERM, answer.index(), answer.members(), null);
		return new Report(1, false, false, RemovalRule.UNKNOWN_TERM, -1, answer.committed() ? conf : null, conf,
				Probe.UNKNOWN, RemovalRule.UNKNOWN_TERM, -1, Probe.UNKNOWN, Map.of());
	}

	/**
	 * Runs a check for the node whose facts {@code own} gives (read again every round), asking members at the
	 * addresses of {@code known} (id to Raft address; the reports add more), for at most {@code budget}.
	 */
	static Verdict check(Supplier<Own> own, Map<String, String> known, RaftGroupId groupId, Duration budget) {
		var channels = new HashMap<String, ManagedChannel>();
		var versionOne = ConcurrentHashMap.<String>newKeySet();
		try {
			return check(own, known, budget, (id, address, probes, timeout) -> {
				ManagedChannel channel;
				synchronized (channels) {
					channel = channels.computeIfAbsent(id + "@" + address, _ -> PeerChannels.open(address));
				}
				return ask(channel, groupId, id, probes, timeout, versionOne);
			});
		} finally {
			channels.values().forEach(ManagedChannel::shutdownNow);
		}
	}

	/** Asks one member: {@code Configuration2}, or {@code Configuration} if it predates that. */
	static CompletableFuture<Answer> ask(ManagedChannel channel, RaftGroupId groupId, String id, List<TermIndex> probes,
			Duration timeout, Set<String> versionOne) {
		if (versionOne.contains(id)) {
			return askVersion1(channel, groupId, id, timeout);
		}
		return TermService.configuration2(channel, groupId, probes, timeout).handle((report, error) -> {
			if (error == null) {
				return CompletableFuture.completedFuture(report == null ? Answer.PENDING : Answer.of(report));
			}
			if (unimplemented(error)) {
				versionOne.add(id);
				return askVersion1(channel, groupId, id, timeout);
			}
			LOG.debug("member {} did not tell its configurations", id, error);
			return CompletableFuture.completedFuture(Answer.NONE);
		}).thenCompose(answer -> answer);
	}

	private static CompletableFuture<Answer> askVersion1(ManagedChannel channel, RaftGroupId groupId, String id,
			Duration timeout) {
		return TermService.configuration(channel, groupId, timeout).handle((answer, error) -> {
			if (error != null) {
				// Older than both methods (UNIMPLEMENTED), or unreachable.
				LOG.debug("member {} did not tell its configuration", id, error);
				return Answer.NONE;
			}
			return answer == null ? Answer.PENDING : Answer.of(fromVersion1(answer));
		});
	}

	private static boolean unimplemented(Throwable error) {
		var cause = error instanceof CompletionException && error.getCause() != null ? error.getCause() : error;
		return Status.fromThrowable(cause).getCode() == Status.Code.UNIMPLEMENTED;
	}

	/** {@link #check(Supplier, Map, RaftGroupId, Duration)} with the members asked through {@code asker}. */
	static Verdict check(Supplier<Own> ownSource, Map<String, String> known, Duration budget, Asker asker) {
		long deadline = System.nanoTime() + budget.toNanos();
		var addresses = new LinkedHashMap<>(known);
		var answers = new LinkedHashMap<String, Answer>();
		var probed = new HashMap<String, List<TermIndex>>();
		Conf kStar = null;
		Verdict verdict;
		while (true) {
			var own = ownSource.get();
			var members = members(own, kStar, known.keySet());
			var probes = probes(own, kStar);
			long left = deadline - System.nanoTime();
			var timeout = Duration.ofNanos(Math.max(TimeUnit.MILLISECONDS.toNanos(50), left));
			var calls = new LinkedHashMap<String, CompletableFuture<Answer>>();
			for (var id : members) {
				var previous = answers.get(id);
				if (previous != null && previous.kind() == Answer.Kind.NONE) {
					continue;
				}
				var address = addresses.get(id);
				if (address == null) {
					answers.put(id, Answer.NONE);
					continue;
				}
				calls.put(id, asker.ask(id, address, probes, timeout));
				probed.put(id, probes);
			}
			for (var call : calls.entrySet()) {
				Answer answer;
				try {
					answer = call.getValue().get(timeout.toNanos() + TimeUnit.SECONDS.toNanos(1), TimeUnit.NANOSECONDS);
				} catch (InterruptedException ex) {
					Thread.currentThread().interrupt();
					answer = Answer.NONE;
				} catch (ExecutionException | java.util.concurrent.TimeoutException | RuntimeException ex) {
					answer = Answer.NONE;
				}
				answers.put(call.getKey(), answer);
				if (answer.report() != null) {
					answer.report().addresses().forEach(addresses::putIfAbsent);
				}
			}
			answers.keySet().retainAll(members);
			verdict = RemovalRule.decide(own, answers);
			if (verdict.outcome() != RemovalRule.Outcome.UNDECIDED || verdict.reason() == Reason.INCONSISTENT) {
				return verdict;
			}
			var next = verdict.kStar();
			boolean again = (next != null && !next.equals(kStar))
					|| worthAsking(verdict, answers, members(own, next, known.keySet()));
			kStar = next;
			if (!again || deadline - System.nanoTime() <= TimeUnit.MILLISECONDS.toNanos(RETRY_MS)
					|| Thread.currentThread().isInterrupted()) {
				return verdict;
			}
			try {
				Thread.sleep(RETRY_MS);
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
				return verdict;
			}
		}
	}

	/** Whether asking again may change an undecided {@code verdict}. */
	private static boolean worthAsking(Verdict verdict, Map<String, Answer> answers, Set<String> members) {
		for (var id : members) {
			var answer = answers.get(id);
			if (answer == null || answer.kind() == Answer.Kind.PENDING) {
				return true;
			}
		}
		var kStar = verdict.kStar();
		return switch (verdict.reason()) {
			case MEMBER_LATER_CONFIGURATION, OWN_LATER_CONFIGURATION -> true;
			case NO_MAJORITY -> kStar != null && answers.entrySet().stream().anyMatch(entry -> kStar.inNew(entry.getKey())
					&& entry.getValue().report() != null && !verdict.confirmers().contains(entry.getKey()));
			default -> false;
		};
	}

	/**
	 * The members to ask: the halves of the node's own latest and committed configurations and of K*, and the nodes it
	 * knows the address of ({@code known}: its peer list, so that a node that joins asks the cluster), but itself.
	 * Asking more nodes only adds committed facts and blockers: a node outside K*'s new half never confirms.
	 */
	static Set<String> members(Own own, @Nullable Conf kStar, Collection<String> known) {
		var members = new LinkedHashSet<String>();
		for (var conf : java.util.Arrays.asList(own.latest(), own.committed(), kStar)) {
			if (conf != null) {
				members.addAll(conf.newHalf());
				if (conf.oldHalf() != null) {
					members.addAll(conf.oldHalf());
				}
			}
		}
		members.addAll(known);
		members.remove(own.self());
		return members;
	}

	/** The probes: the node's own latest configuration entry, then K* (or its own committed one before there is a K*). */
	static List<TermIndex> probes(Own own, @Nullable Conf kStar) {
		var probes = new ArrayList<TermIndex>(2);
		var latest = own.latest();
		probes.add(latest == null ? TermIndex.valueOf(RemovalRule.UNKNOWN_TERM, -1)
				: TermIndex.valueOf(latest.term(), latest.index()));
		var second = kStar != null ? kStar : own.committed();
		if (second != null) {
			probes.add(TermIndex.valueOf(second.term(), second.index()));
		}
		return probes;
	}

	/** Id to Raft address of {@code members} but {@code self}, from {@code known}. */
	static Map<String, String> addressesOf(String self, Collection<String> members, Map<String, String> known) {
		var addresses = new LinkedHashMap<String, String>();
		for (var member : members) {
			var address = known.get(member);
			if (!member.equals(self) && address != null) {
				addresses.put(member, address);
			}
		}
		return addresses;
	}
}
