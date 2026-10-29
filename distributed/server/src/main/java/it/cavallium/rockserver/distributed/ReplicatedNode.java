package it.cavallium.rockserver.distributed;

import it.cavallium.rockserver.core.client.RocksDBConnection;
import java.io.IOException;
import org.jetbrains.annotations.Nullable;

/**
 * One member of a replicated Rockserver cluster: the local database, the Raft server and the Rockserver gRPC API
 * (bound to this node's {@link PeerConfig#apiPort()}). Any member serves the full API; requests that need the
 * leader are forwarded to it.
 */
public final class ReplicatedNode implements AutoCloseable {

	private ReplicatedNode() {}

	/** Starts the node; it joins the group and serves the API. */
	public static ReplicatedNode start(NodeConfig config) throws IOException {
		throw new UnsupportedOperationException("TODO");
	}

	public NodeConfig config() {
		throw new UnsupportedOperationException("TODO");
	}

	/** In-process view of the cluster API through this node, with the same routing as the gRPC API. */
	public RocksDBConnection connection() {
		throw new UnsupportedOperationException("TODO");
	}

	public boolean isLeader() {
		throw new UnsupportedOperationException("TODO");
	}

	public @Nullable String leaderId() {
		throw new UnsupportedOperationException("TODO");
	}

	public long appliedIndex() {
		throw new UnsupportedOperationException("TODO");
	}

	/** Graceful stop: hands leadership to another member first, then closes everything. */
	@Override
	public void close() throws IOException {
		throw new UnsupportedOperationException("TODO");
	}

	/** Abrupt stop for tests: no leadership hand-off. */
	public void halt() throws IOException {
		throw new UnsupportedOperationException("TODO");
	}
}
