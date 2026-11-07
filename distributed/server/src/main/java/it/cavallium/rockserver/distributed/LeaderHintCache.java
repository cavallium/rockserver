package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.util.function.Function;
import java.util.function.LongSupplier;
import org.apache.ratis.protocol.RaftPeerId;
import org.jetbrains.annotations.Nullable;

/**
 * The leader's API address as text, for the leader hint of every response, cached by the leader and the address
 * version of the forwarder ({@link LeaderForwarder#addressVersion()}): looked up again only when either changes.
 */
final class LeaderHintCache {

	private record Cached(RaftPeerId leader, long version, @Nullable String address) {}

	private volatile @Nullable Cached cached;

	/**
	 * The API address of {@code leader} as {@code host:port}, or null if unknown. The version is read before the
	 * lookup, so that an address that changes during the lookup is looked up again at the next call (R18-2).
	 */
	@Nullable String hint(RaftPeerId leader, LongSupplier addressVersion,
			Function<String, @Nullable HostAndPort> apiAddress) {
		long version = addressVersion.getAsLong();
		var current = cached;
		if (current != null && current.version() == version && current.leader().equals(leader)) {
			return current.address();
		}
		var api = apiAddress.apply(leader.toString());
		var address = api == null ? null : api.host() + ":" + api.port();
		cached = new Cached(leader, version, address);
		return address;
	}
}
