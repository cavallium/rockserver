package it.cavallium.rockserver.core.common;

import org.jetbrains.annotations.Nullable;

/**
 * A connection behind a server that knows which node of its cluster leads, such as a replicated node. The gRPC server
 * then sends the leader's API address to every client, in the response metadata {@link #METADATA_KEY} (in the
 * headers, or in the trailers of a call that fails before them), and a client connected to several nodes may send
 * the calls that the leader serves straight to it. See {@code LeaderHintRouter}.
 */
public interface LeaderHint {

	/** Response metadata key: the leader's API address, {@code host:port}. */
	String METADATA_KEY = "rockserver-leader";

	/** The current leader's API address, {@code host:port}, as the members reach it; null if no leader is known. */
	@Nullable String leaderApiAddress();
}
