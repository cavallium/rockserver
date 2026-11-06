package it.cavallium.rockserver.distributed;

import java.io.Closeable;
import java.io.IOException;
import java.time.Duration;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.thirdparty.io.grpc.ConnectivityState;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * Read indexes for the point reads a follower serves from its own database (Raft dissertation §6.4, TiKV follower
 * read). The follower asks the node it knows as leader, through {@link TermService#READ_INDEX} on its Raft port, for
 * the commit index at the time the request arrived there, confirmed by that leader's {@link ReadBarrier}. Once its
 * own applied index reaches it, the follower has every entry committed before its read arrived, so every write
 * acknowledged before then.
 *
 * <p>Concurrent reads share one request, and at most one request is in flight: reads that arrive while one is in
 * flight wait for the next, because a request sent before a read arrived may reach the leader before a write that was
 * acknowledged before the read arrived. The futures complete on gRPC threads: callers must move blocking work
 * elsewhere.</p>
 */
final class FollowerReads implements Closeable {

	/** The node knows no leader, or only itself: the read is forwarded instead. */
	static final class NoLeaderException extends IOException {
		NoLeaderException(String message) {
			super(message);
		}
	}

	private final RockserverStateMachine stateMachine;
	private final RaftGroupId groupId;
	private final String selfId;
	private final Duration timeout;
	private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
	private final Object lock = new Object();
	/** Reads waiting for the next request. Guarded by {@link #lock}. */
	private List<CompletableFuture<Long>> next = new ArrayList<>();
	/** Guarded by {@link #lock}. */
	private boolean inFlight;
	private volatile boolean closed;

	/**
	 * @param timeout how long one request may take
	 */
	FollowerReads(RockserverStateMachine stateMachine, RaftGroupId groupId, String selfId, Duration timeout) {
		this.stateMachine = stateMachine;
		this.groupId = groupId;
		this.selfId = selfId;
		this.timeout = timeout;
	}

	/** A read index from the current leader, asked after this call. */
	CompletableFuture<Long> readIndex() {
		var future = new CompletableFuture<Long>();
		List<CompletableFuture<Long>> batch = null;
		synchronized (lock) {
			if (closed) {
				return CompletableFuture.failedFuture(new IOException("Closed"));
			}
			next.add(future);
			if (!inFlight) {
				batch = next;
				next = new ArrayList<>();
				inFlight = true;
			}
		}
		if (batch != null) {
			send(batch);
		}
		return future;
	}

	/** Sends one request for {@code batch}, then one for the reads that arrived meanwhile, until none is left. */
	private void send(List<CompletableFuture<Long>> batch) {
		CompletableFuture<Long> reply;
		try {
			reply = TermService.readIndex(channel(leaderRaftAddress()), groupId, timeout);
		} catch (IOException | RuntimeException e) {
			reply = CompletableFuture.failedFuture(e);
		}
		reply.whenComplete((index, error) -> {
			for (var future : batch) {
				if (error == null) {
					future.complete(index);
				} else {
					future.completeExceptionally(error);
				}
			}
			List<CompletableFuture<Long>> following;
			synchronized (lock) {
				if (next.isEmpty() || closed) {
					inFlight = false;
					following = null;
				} else {
					following = next;
					next = new ArrayList<>();
				}
			}
			if (following != null) {
				send(following);
			}
		});
	}

	/** The Raft address of the leader this node knows. */
	private String leaderRaftAddress() throws IOException {
		var division = stateMachine.division();
		if (division == null) {
			throw new NoLeaderException("Not started");
		}
		var leaderId = division.getInfo().getLeaderId();
		if (leaderId == null || leaderId.toString().equals(selfId)) {
			throw new NoLeaderException("No other node is known as leader");
		}
		var conf = division.getRaftConf();
		for (var peers : List.of(conf.getCurrentPeers(), conf.getPreviousPeers())) {
			for (RaftPeer peer : peers) {
				if (peer.getId().equals(leaderId) && peer.getAddress() != null) {
					return peer.getAddress();
				}
			}
		}
		throw new NoLeaderException("The leader " + leaderId + " is not in the configuration");
	}

	private ManagedChannel channel(String address) {
		if (channels.size() > 8) {
			// Addresses of members that left: keep the current members only.
			dropUnusedChannels();
		}
		var channel = channels.computeIfAbsent(address,
				_ -> NettyChannelBuilder.forTarget(address).usePlaintext().build());
		if (closed) {
			closeChannels();
		}
		// As in ReadBarrier: a leader that is back must be reached at once, not after gRPC's growing backoff.
		if (channel.getState(false) == ConnectivityState.TRANSIENT_FAILURE) {
			channel.resetConnectBackoff();
		}
		return channel;
	}

	private void dropUnusedChannels() {
		var division = stateMachine.division();
		if (division == null) {
			return;
		}
		var addresses = new HashSet<@Nullable String>();
		var conf = division.getRaftConf();
		conf.getCurrentPeers().forEach(peer -> addresses.add(peer.getAddress()));
		conf.getPreviousPeers().forEach(peer -> addresses.add(peer.getAddress()));
		channels.entrySet().removeIf(entry -> {
			if (addresses.contains(entry.getKey())) {
				return false;
			}
			entry.getValue().shutdown();
			return true;
		});
	}

	private void closeChannels() {
		for (var address : List.copyOf(channels.keySet())) {
			var channel = channels.remove(address);
			if (channel != null) {
				channel.shutdownNow();
			}
		}
	}

	@Override
	public void close() {
		List<CompletableFuture<Long>> waiting;
		synchronized (lock) {
			closed = true;
			waiting = next;
			next = new ArrayList<>();
		}
		for (var future : waiting) {
			future.completeExceptionally(new IOException("Closed"));
		}
		// Requests in flight fail with their channels, and complete their reads.
		closeChannels();
	}
}
