package it.cavallium.rockserver.distributed;

import java.util.Objects;

/**
 * One cluster member.
 *
 * @param id stable member id, also the Ratis peer id
 * @param raftHost host other members use to reach this member's Raft port
 * @param raftPort Raft (Ratis gRPC) port, 1 to 65535
 * @param apiHost host clients and other members use to reach the Rockserver API
 * @param apiPort Rockserver gRPC API port, 1 to 65535
 */
public record PeerConfig(String id, String raftHost, int raftPort, String apiHost, int apiPort) {

	public PeerConfig {
		Objects.requireNonNull(id, "id");
		Objects.requireNonNull(raftHost, "raftHost");
		Objects.requireNonNull(apiHost, "apiHost");
		if (id.isBlank() || id.contains(",") || id.contains("=")) {
			throw new IllegalArgumentException("Invalid peer id: " + id);
		}
		// R61-12: any number parsed; a node then failed at bind, after the start-up checks, with exit 1.
		requirePort("Raft", raftPort);
		requirePort("API", apiPort);
	}

	private static void requirePort(String which, int port) {
		if (port < 1 || port > 65535) {
			throw new IllegalArgumentException("Invalid " + which + " port " + port + ": not in 1 to 65535");
		}
	}

	public String raftAddress() {
		return raftHost + ":" + raftPort;
	}

	/** Parses {@code id=raftHost:raftPort/apiHost:apiPort}. */
	public static PeerConfig parse(String spec) {
		int eq = spec.indexOf('=');
		int slash = spec.indexOf('/', eq + 1);
		if (eq <= 0 || slash < 0) {
			throw new IllegalArgumentException("Expected id=raftHost:raftPort/apiHost:apiPort, got: " + spec);
		}
		var raft = hostPort(spec.substring(eq + 1, slash));
		var api = hostPort(spec.substring(slash + 1));
		return new PeerConfig(spec.substring(0, eq), raft.host, raft.port, api.host, api.port);
	}

	public String format() {
		return id + "=" + raftHost + ":" + raftPort + "/" + apiHost + ":" + apiPort;
	}

	private record HostPort(String host, int port) {}

	private static HostPort hostPort(String value) {
		int colon = value.lastIndexOf(':');
		if (colon <= 0) {
			throw new IllegalArgumentException("Expected host:port, got: " + value);
		}
		int port;
		try {
			port = Integer.parseInt(value.substring(colon + 1));
		} catch (NumberFormatException ex) {
			throw new IllegalArgumentException("Expected host:port with a port in 1 to 65535, got: " + value);
		}
		return new HostPort(value.substring(0, colon), port);
	}
}
