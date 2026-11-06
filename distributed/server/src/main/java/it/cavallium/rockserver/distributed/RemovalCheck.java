package it.cavallium.rockserver.distributed;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Asks the other members of the configuration a node knows whether their configuration still holds it: the removal
 * check of a node whose own log never received the configuration that removed it (see
 * {@link ReplicatedNode}'s {@code requireNotRemoved}).
 *
 * <p>Raft commits the configuration that removes a node with a majority of the old members, which need not include
 * that node: the leader then stops sending to it, and its own log keeps a configuration that holds it. Restarted, it
 * would wait outside the cluster. So a node that is or was a member asks each member of its own configuration for its
 * latest configuration ({@link TermService#CONFIGURATION}). It counts as removed once a majority of the members it knows
 * (itself included, which never says so) answer with a committed configuration, newer than its own, whose new half
 * leaves it out. A committed configuration never goes away again, so this cannot refuse a member; an answer that is not
 * committed yet is asked again, until the check is decided or its time is up. A member that cannot be reached, or that
 * predates the method, does not count: without a majority of answers the node starts as before.</p>
 */
final class RemovalCheck {

	private static final Logger LOG = LoggerFactory.getLogger(RemovalCheck.class);
	private static final long RETRY_MS = 100;

	/** What one member's answer says about this node. */
	enum Answer {
		/** A committed configuration, newer than this node's, that leaves it out. */
		REMOVED,
		/** A committed configuration that holds this node, or one not newer than its own. */
		MEMBER,
		/** Not decided yet: the member is starting, or its configuration is not committed yet. Asked again. */
		PENDING,
		/** No answer: unreachable, or a node that predates the method. */
		NONE
	}

	/** A member's latest configuration: its log index, whether it is committed there, and its (new) members. */
	record PeerConfiguration(long index, boolean committed, List<String> members) {}

	private RemovalCheck() {}

	/** What {@code answer} (null: the member has no group yet) says about node {@code self}, whose own is at {@code ownIndex}. */
	static Answer judge(@Nullable PeerConfiguration answer, String self, long ownIndex) {
		if (answer == null || !answer.committed()) {
			return Answer.PENDING;
		}
		if (answer.members().contains(self) || answer.index() <= ownIndex) {
			return Answer.MEMBER;
		}
		return Answer.REMOVED;
	}

	/**
	 * The decision for a configuration of {@code knownMembers} members (the node included): true if {@code removed}
	 * members, a majority, say it was removed; false if they can no longer become one, with {@code pending} members
	 * still to answer; null while undecided.
	 */
	static @Nullable Boolean decide(int knownMembers, int removed, int pending) {
		int majority = knownMembers / 2 + 1;
		if (removed >= majority) {
			return true;
		}
		if (removed + pending < majority) {
			return false;
		}
		return null;
	}

	/**
	 * Asks the members of {@code addresses} (id to Raft address, this node left out) about node {@code self}, whose
	 * configuration of {@code knownMembers} members is at log index {@code ownIndex}, for at most {@code budget}.
	 *
	 * @return the members that say it was removed, if they are a majority; null otherwise
	 */
	static @Nullable List<String> removedBy(String self, RaftGroupId groupId, long ownIndex, int knownMembers,
			Map<String, String> addresses, Duration budget) {
		long deadline = System.nanoTime() + budget.toNanos();
		var removed = new ArrayList<String>();
		var pending = new LinkedHashMap<>(addresses);
		var channels = new HashMap<String, ManagedChannel>();
		try {
			while (true) {
				var calls = new LinkedHashMap<String, CompletableFuture<@Nullable PeerConfiguration>>();
				long left = deadline - System.nanoTime();
				Duration timeout = Duration.ofNanos(Math.max(TimeUnit.MILLISECONDS.toNanos(50), left));
				pending.forEach((id, address) -> calls.put(id, TermService.configuration(
						channels.computeIfAbsent(id, _ -> PeerChannels.open(address)), groupId, timeout)));
				calls.forEach((id, call) -> {
					Answer answer;
					try {
						answer = judge(call.get(timeout.toNanos() + TimeUnit.SECONDS.toNanos(1), TimeUnit.NANOSECONDS), self,
								ownIndex);
					} catch (InterruptedException ex) {
						Thread.currentThread().interrupt();
						answer = Answer.NONE;
					} catch (Exception ex) {
						LOG.debug("{}: member {} did not tell its configuration", self, id, ex);
						answer = Answer.NONE;
					}
					switch (answer) {
						case REMOVED -> {
							removed.add(id);
							pending.remove(id);
						}
						case MEMBER, NONE -> pending.remove(id);
						case PENDING -> {}
					}
				});
				var decision = decide(knownMembers, removed.size(), pending.size());
				if (decision != null) {
					return decision ? removed : null;
				}
				if (System.nanoTime() + TimeUnit.MILLISECONDS.toNanos(RETRY_MS) - deadline >= 0
						|| Thread.currentThread().isInterrupted()) {
					LOG.warn("{}: could not tell whether it is still a member: {} of {} members say it was removed, {} did not"
							+ " decide in {}; starting", self, removed.size(), knownMembers, pending.keySet(), budget);
					return null;
				}
				Thread.sleep(RETRY_MS);
			}
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
			return null;
		} finally {
			channels.values().forEach(ManagedChannel::shutdownNow);
		}
	}

	/** Id to Raft address of {@code members} but {@code self}, from either source. */
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
