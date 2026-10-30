package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;

/**
 * Command line entry point.
 *
 * <pre>
 * run   --node-id ID --peer ID=RAFTHOST:PORT/APIHOST:PORT (repeat per member) --data-dir DIR
 *       [--rockserver-config FILE] [--raft-bind-host HOST] [--api-bind-host HOST] [--group-id UUID]
 *       [--snapshot-every N] [--election-timeout-ms MIN:MAX] [--request-timeout-ms N]
 *       [--max-transaction-lease-ms N]
 * admin --peer ... [--group-id UUID] status | transfer-leader ID | add-peer ID=... | remove-peer ID
 * </pre>
 *
 * <p>{@code run} prints {@code READY <node-id>} on stdout once the node serves the API, and stops gracefully
 * (leadership hand-off) on SIGTERM. For {@code add-peer} the new member must already run with the new peer list.</p>
 */
public final class ReplicatedServerMain {

	private ReplicatedServerMain() {}

	public static void main(String[] args) throws Exception {
		if (args.length == 0) {
			usage("missing command");
		}
		var rest = List.of(args).subList(1, args.length);
		switch (args[0]) {
			case "run" -> run(new Options(rest));
			case "admin" -> admin(new Options(rest));
			default -> usage("unknown command " + args[0]);
		}
	}

	private static void run(Options options) throws IOException, InterruptedException {
		var config = new NodeConfig(options.required("--node-id"),
				options.peers(),
				Path.of(options.required("--data-dir")),
				options.value("--rockserver-config") != null ? Path.of(options.value("--rockserver-config")) : null,
				options.value("--raft-bind-host"),
				options.value("--api-bind-host"),
				options.value("--group-id", NodeConfig.DEFAULT_GROUP_ID),
				Duration.ofMillis(Long.parseLong(options.value("--election-timeout-ms", "1000:2000").split(":")[0])),
				Duration.ofMillis(Long.parseLong(options.value("--election-timeout-ms", "1000:2000").split(":")[1])),
				Long.parseLong(options.value("--snapshot-every", String.valueOf(NodeConfig.DEFAULT_SNAPSHOT_EVERY_ENTRIES))),
				Duration.ofMillis(Long.parseLong(options.value("--request-timeout-ms", "10000"))),
				Duration.ofMillis(Long.parseLong(options.value("--max-transaction-lease-ms",
						String.valueOf(NodeConfig.DEFAULT_MAX_TRANSACTION_LEASE.toMillis())))));
		options.requireConsumed();
		ReplicatedNode node;
		try {
			node = ReplicatedNode.start(config);
		} catch (IOException | RuntimeException e) {
			// Ratis and gRPC leave non-daemon threads behind, which would keep the JVM running without a node.
			System.err.println("Failed to start node " + config.nodeId() + ":");
			e.printStackTrace();
			System.exit(1);
			return;
		}
		Runtime.getRuntime().addShutdownHook(new Thread(() -> {
			try {
				node.close();
			} catch (IOException ex) {
				System.err.println("Failed to stop cleanly: " + ex);
			}
		}, "rockserver-shutdown"));
		System.out.println("READY " + config.nodeId());
		System.out.flush();
		var failure = node.stopped().join();
		if (failure != null) {
			// The replica cannot apply the log: exit, so that a supervisor notices and an operator repairs it.
			System.err.println("Node " + config.nodeId() + " stopped after a replica failure: " + failure);
			System.exit(3);
		}
	}

	private static void admin(Options options) throws IOException {
		var peers = options.peers();
		var groupId = RaftGroupId.valueOf(UUID.fromString(options.value("--group-id", NodeConfig.DEFAULT_GROUP_ID)));
		var command = options.positional();
		if (command.isEmpty()) {
			usage("missing admin command");
		}
		var group = RaftGroup.valueOf(groupId, peers.stream().map(ReplicatedServerMain::raftPeer).toList());
		var anyConfig = NodeConfig.defaults(peers.getFirst().id(), peers, Path.of("."));
		try (var client = RaftClient.newBuilder()
				.setClientId(ClientId.randomId())
				.setRaftGroup(group)
				.setProperties(RaftSettings.client(anyConfig))
				.build()) {
			RaftClientReply reply = switch (command.getFirst()) {
				case "status" -> {
					for (PeerConfig peer : peers) {
						try {
							var info = client.getGroupManagementApi(RaftPeerId.valueOf(peer.id())).info(groupId);
							System.out.println(peer.id() + ": " + info.getRoleInfoProto().getRole()
									+ ", commit infos " + info.getCommitInfos());
						} catch (IOException ex) {
							System.out.println(peer.id() + ": unreachable (" + ex.getMessage() + ")");
						}
					}
					yield null;
				}
				case "transfer-leader" -> client.admin().transferLeadership(RaftPeerId.valueOf(arg(command)), 10_000);
				case "add-peer" -> {
					var added = PeerConfig.parse(arg(command));
					var members = new ArrayList<>(peers.stream().map(ReplicatedServerMain::raftPeer).toList());
					members.removeIf(p -> p.getId().toString().equals(added.id()));
					members.add(raftPeer(added));
					yield client.admin().setConfiguration(members);
				}
				case "remove-peer" -> {
					var removed = arg(command);
					yield client.admin().setConfiguration(peers.stream()
							.filter(p -> !p.id().equals(removed))
							.map(ReplicatedServerMain::raftPeer)
							.toList());
				}
				default -> {
					usage("unknown admin command " + command.getFirst());
					yield null;
				}
			};
			if (reply != null) {
				System.out.println(reply.isSuccess() ? "OK" : "FAILED: " + reply.getException());
				if (!reply.isSuccess()) {
					System.exit(1);
				}
			}
		}
	}

	private static String arg(List<String> command) {
		if (command.size() != 2) {
			usage(command.getFirst() + " takes exactly one argument");
		}
		return command.get(1);
	}

	private static RaftPeer raftPeer(PeerConfig peer) {
		return RaftPeer.newBuilder().setId(peer.id()).setAddress(peer.raftAddress()).build();
	}

	private static void usage(String problem) {
		System.err.println("Error: " + problem);
		System.err.println("""
				Usage:
				  run   --node-id ID --peer ID=RAFTHOST:PORT/APIHOST:PORT ... --data-dir DIR
				        [--rockserver-config FILE] [--raft-bind-host HOST] [--api-bind-host HOST] [--group-id UUID]
				        [--snapshot-every N] [--election-timeout-ms MIN:MAX] [--request-timeout-ms N]
				        [--max-transaction-lease-ms N]
				  admin --peer ... [--group-id UUID] status | transfer-leader ID | add-peer ID=... | remove-peer ID""");
		System.exit(2);
	}

	/** Minimal "--name value" parser; {@code --peer} may repeat. */
	private static final class Options {
		private final List<String[]> pairs = new ArrayList<>();
		private final List<String> positional = new ArrayList<>();
		private final List<String> consumed = new ArrayList<>();

		Options(List<String> args) {
			for (int i = 0; i < args.size(); i++) {
				var arg = args.get(i);
				if (arg.startsWith("--")) {
					if (i + 1 >= args.size()) {
						usage("missing value for " + arg);
					}
					pairs.add(new String[] {arg, args.get(++i)});
				} else {
					positional.add(arg);
				}
			}
		}

		String value(String name) {
			consumed.add(name);
			String result = null;
			for (String[] pair : pairs) {
				if (pair[0].equals(name)) {
					result = pair[1];
				}
			}
			return result;
		}

		String value(String name, String fallback) {
			var result = value(name);
			return result != null ? result : fallback;
		}

		String required(String name) {
			var result = value(name);
			if (result == null) {
				usage("missing " + name);
			}
			return result;
		}

		List<PeerConfig> peers() {
			consumed.add("--peer");
			var peers = pairs.stream().filter(p -> p[0].equals("--peer")).map(p -> PeerConfig.parse(p[1])).toList();
			if (peers.isEmpty()) {
				usage("at least one --peer is required");
			}
			return peers;
		}

		List<String> positional() {
			return positional;
		}

		void requireConsumed() {
			for (String[] pair : pairs) {
				if (!consumed.contains(pair[0])) {
					usage("unknown option " + pair[0]);
				}
			}
		}
	}
}
