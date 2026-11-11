package it.cavallium.rockserver.distributed;

import java.util.ArrayList;
import java.util.List;
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

	/**
	 * The first pair of members of {@code peers} that share a Raft address or an API address, as a refusal text naming
	 * both ids; {@code null} when there is none (R61-15). Hosts are compared case-insensitively and ports as numbers;
	 * nothing is resolved, so two names of one machine are not caught. A member's own Raft and API addresses are not
	 * compared with each other.
	 */
	public static String sharedAddress(List<PeerConfig> peers) {
		for (int i = 0; i < peers.size(); i++) {
			for (int j = i + 1; j < peers.size(); j++) {
				var a = peers.get(i);
				var b = peers.get(j);
				if (a.id().equals(b.id())) {
					continue;
				}
				if (a.raftPort == b.raftPort && a.raftHost.equalsIgnoreCase(b.raftHost)) {
					return "members " + a.id() + " and " + b.id() + " share the Raft address " + a.raftAddress();
				}
				if (a.apiPort == b.apiPort && a.apiHost.equalsIgnoreCase(b.apiHost)) {
					return "members " + a.id() + " and " + b.id() + " share the API address " + a.apiHost + ":" + a.apiPort;
				}
			}
		}
		return null;
	}

	/** {@code peers} without the members that share an address with an earlier one: what a client needs of them. */
	public static List<PeerConfig> withDistinctAddresses(List<PeerConfig> peers) {
		var kept = new ArrayList<PeerConfig>();
		for (var peer : peers) {
			var next = new ArrayList<>(kept);
			next.add(peer);
			if (sharedAddress(next) == null) {
				kept.add(peer);
			}
		}
		return kept;
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
