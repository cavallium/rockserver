package it.cavallium.rockserver.distributed;

/**
 * Command line entry point.
 *
 * <pre>
 * run   --node-id ID --peer ID=RAFTHOST:PORT/APIHOST:PORT (repeat per member) --data-dir DIR
 *       [--rockserver-config FILE] [--raft-bind-host HOST] [--api-bind-host HOST] [--group-id UUID]
 *       [--snapshot-every N] [--election-timeout-ms MIN:MAX] [--request-timeout-ms N]
 * admin --peer ... [--group-id UUID] status | transfer-leader ID | add-peer ID=... | remove-peer ID
 * </pre>
 *
 * <p>{@code run} prints {@code READY <node-id>} on stdout once the node serves the API, and stops gracefully
 * (leadership hand-off) on SIGTERM.</p>
 */
public final class ReplicatedServerMain {

	private ReplicatedServerMain() {}

	public static void main(String[] args) throws Exception {
		throw new UnsupportedOperationException("TODO");
	}
}
