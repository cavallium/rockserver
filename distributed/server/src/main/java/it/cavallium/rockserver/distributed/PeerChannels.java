package it.cavallium.rockserver.distributed;

import java.io.Closeable;
import java.util.Collection;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import org.apache.ratis.thirdparty.io.grpc.ConnectivityState;
import org.apache.ratis.thirdparty.io.grpc.ManagedChannel;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.jetbrains.annotations.Nullable;

/**
 * Plaintext gRPC channels to other members' Raft ports, for the {@link TermService} calls of {@link ReadBarrier},
 * {@link FollowerReads} and {@link ClusterCheck}. A cache keeps one channel per address, so a peer whose address
 * changes gets a new one; {@link #open} makes a one-off channel for a caller that calls a peer rarely.
 *
 * <p>While a peer is down, gRPC waits longer and longer (up to two minutes) between attempts to reach it, and fails
 * calls at once in between. These calls must reach a peer as soon as it is back, so {@link #get} makes a fresh attempt
 * when it finds the channel failed: at most one connection attempt per call.</p>
 */
final class PeerChannels implements Closeable {

	private final Map<String, ManagedChannel> channels = new ConcurrentHashMap<>();
	private volatile boolean closed;

	/** A new channel to {@code address}, which the caller must shut down. */
	static ManagedChannel open(String address) {
		return NettyChannelBuilder.forTarget(address).usePlaintext().build();
	}

	/** The channel to {@code address}, made on first use. After {@link #close}, the returned channel is shut down. */
	ManagedChannel get(String address) {
		var channel = channels.computeIfAbsent(address, PeerChannels::open);
		if (closed) {
			// close() may have swept the channels before this one was added.
			closeAll();
		}
		if (channel.getState(false) == ConnectivityState.TRANSIENT_FAILURE) {
			channel.resetConnectBackoff();
		}
		return channel;
	}

	/** The number of cached channels. */
	int size() {
		return channels.size();
	}

	/**
	 * Shuts down the channels to every address not in {@code addresses}, such as members that left. {@code shutdown},
	 * not {@code shutdownNow}: calls already made on them finish.
	 */
	void retainOnly(Collection<@Nullable String> addresses) {
		var keep = new HashSet<>(addresses);
		channels.entrySet().removeIf(entry -> {
			if (keep.contains(entry.getKey())) {
				return false;
			}
			entry.getValue().shutdown();
			return true;
		});
	}

	/** Shuts down every channel now (calls in flight fail), and the channels {@link #get} makes later. */
	@Override
	public void close() {
		closed = true;
		closeAll();
	}

	private void closeAll() {
		for (var address : List.copyOf(channels.keySet())) {
			var channel = channels.remove(address);
			if (channel != null) {
				channel.shutdownNow();
			}
		}
	}
}
