package it.cavallium.rockserver.distributed;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicy;
import org.apache.ratis.thirdparty.io.grpc.Status;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyChannelBuilder;
import org.apache.ratis.util.TimeDuration;
import org.jetbrains.annotations.Nullable;

/**
 * Command line entry point.
 *
 * <pre>
 * run   --node-id ID --peer ID=RAFTHOST:PORT/APIHOST:PORT (repeat per member) --data-dir DIR
 *       [--rockserver-config FILE] [--raft-bind-host HOST] [--api-bind-host HOST] [--group-id UUID]
 *       [--snapshot-every N] [--election-timeout-ms MIN:MAX] [--request-timeout-ms N]
 *       [--max-transaction-lease-ms N] [--log-segment-bytes N] [--pipelined-updates] [--join]
 *       [--jvm-pause-limit-ms N] [--snapshot-every-bytes N] [--parallel-apply-threads N]
 * admin --peer ... [--group-id UUID] [--timeout-ms N]
 *       status | transfer-leader ID | add-peer ID=... | remove-peer ID | snapshot ID
 *       | checksum [--allow-skipped] | backup NAME | backup-status NAME INDEX
 * repair-log --data-dir DIR [--group-id UUID] [--dry-run]
 * restore-backup --backup DIR --data-dir DIR [--rockserver-config FILE]
 * </pre>
 *
 * <p>{@code checksum} has every replica digest its data at one log position and compares the digests; it prints each
 * member's, and prints {@code OK} only if every member of {@code --peer} gave a digest and all of them are equal. It
 * exits with 1 if they differ, and with 4 if a member gave none (down, failed, or not done within {@code --timeout-ms});
 * with {@code --allow-skipped} a member that skipped the position (a snapshot install jumped over it) does not count
 * as missing. {@code backup NAME} has every replica write a checkpoint of its database at one log position to
 * {@code <data-dir>/backups/NAME}; it prints where each member wrote it, and prints {@code OK} only if every member of
 * {@code --peer} wrote it; it exits with 1 if a member failed and with 4 if a member gave no result in time.
 * {@code backup-status NAME INDEX} asks the members again about that backup ({@code INDEX} is the log index that
 * {@code backup} printed), without proposing anything, with the same output and exit codes. {@code restore-backup} prepares an empty data directory to seed a new cluster from such a backup. See
 * {@link ReplicaOps} and {@code docs/distributed/runbooks.md}.</p>
 *
 * <p>{@code run} prints {@code READY <node-id>} on stdout once the node serves the API, and stops gracefully
 * (leadership hand-off) on SIGTERM. For {@code add-peer} the new member must already run, with the new peer list and
 * with {@code --join}, however long before the {@code add-peer}. It exits with 3 if its replica failed or its Raft server
 * closed, so that a supervisor notices. {@code repair-log} runs on a stopped node, after a power loss left it unable to read its Raft log: see
 * {@link RaftLogRepair}. It exits with 0 if the log is valid or was repaired (with {@code --dry-run}: could be) and
 * with 1 if it refuses.</p>
 *
 * <p>Run a node with {@code -XX:+ExitOnOutOfMemoryError}: it warns without it. See "Heap" in the design document.</p>
 *
 * <p>{@code admin} changes end with {@code OK}, or with {@code FAILED: ...} and exit code 1, within {@code --timeout-ms}
 * (10 minutes by default), which must cover the catch-up of a new member. A leader that started a change goes on with it
 * after that: see {@code status}, and run the command again. {@code add-peer} first asks the new node what it holds, and
 * refuses a node that does not run or that has a log of its own. {@code snapshot} makes a member take a snapshot now.</p>
 *
 * <p>{@code --pipelined-updates}, off by default, lets a leader pass a key to its next {@code getForUpdate} update as
 * soon as the previous update's commit is admitted to the log: see "Transactions" in the design document, which also
 * tells what it changes for clients.</p>
 *
 * <p>{@code --join} starts a node outside the group, so that it waits for {@code add-peer} without an election timer:
 * a leader answers a vote request from a node outside its configuration by closing that node's Raft server. It can stay
 * in the command line of the node: once the node has a configuration of its own, the option changes nothing. See
 * "Replacing a node" in the design document.</p>
 *
 * <p>{@code --jvm-pause-limit-ms}, 60000 by default, is the longest pause of the JVM that a node survives: see "Long
 * pauses" in the design document.</p>
 *
 * <p>{@code --snapshot-every-bytes}, 4 GiB by default, makes a node also take a snapshot once it applied that many bytes
 * of log entries since its latest one, besides every {@code --snapshot-every} entries.</p>
 *
 * <p>{@code --parallel-apply-threads}, 0 (off) by default, applies the data entries of each group on that many threads
 * (at least 2), split by key, with the result of the serial apply: see "Partitioned apply" in the design document.</p>
 */
public final class ReplicatedServerMain {

	private static final Duration DEFAULT_ADMIN_TIMEOUT = Duration.ofMinutes(10);
	private static final TimeDuration ADMIN_RETRY_SLEEP = TimeDuration.valueOf(500, java.util.concurrent.TimeUnit.MILLISECONDS);
	/** How long {@code add-peer} waits for an answer of the new node and of each member, per round. */
	private static final Duration PEER_ANSWER_TIMEOUT = Duration.ofSeconds(3);
	private static final long SEED_SNAPSHOT_POLL_MS = 200;

	private ReplicatedServerMain() {}

	public static void main(String[] args) throws Exception {
		// Netty's leak detector records a stack trace for sampled buffers: about 3% of the leader's event loop under
		// load. Off unless the operator chose a level (-Dio.netty.leakDetection.level=...).
		if (System.getProperty("io.netty.leakDetection.level") == null) {
			System.setProperty("io.netty.leakDetection.level", "disabled");
		}
		if (args.length == 0) {
			usage("missing command");
		}
		var rest = List.of(args).subList(1, args.length);
		switch (args[0]) {
			case "run" -> run(new Options(rest));
			case "admin" -> admin(new Options(rest));
			case "repair-log" -> repairLog(new Options(rest));
			case "restore-backup" -> restoreBackup(new Options(rest));
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
						String.valueOf(NodeConfig.DEFAULT_MAX_TRANSACTION_LEASE.toMillis())))),
				Long.parseLong(options.value("--log-segment-bytes", String.valueOf(NodeConfig.DEFAULT_LOG_SEGMENT_BYTES))),
				options.flag("--pipelined-updates"),
				options.flag("--join"),
				Duration.ofMillis(Long.parseLong(options.value("--jvm-pause-limit-ms",
						String.valueOf(NodeConfig.DEFAULT_JVM_PAUSE_LIMIT.toMillis())))),
				Long.parseLong(options.value("--snapshot-every-bytes",
						String.valueOf(NodeConfig.DEFAULT_SNAPSHOT_EVERY_BYTES))),
				Integer.parseInt(options.value("--parallel-apply-threads", "0")));
		options.requireConsumed();
		warnWithoutExitOnOutOfMemory();
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
			// The replica cannot apply the log, or Raft closed: exit, so that a supervisor notices.
			System.err.println("Node " + config.nodeId() + " stopped after a failure: " + failure);
			System.exit(3);
		}
	}

	/**
	 * An {@link OutOfMemoryError} on a thread of Ratis does not stop the JVM, and Ratis goes on without what it was doing.
	 * In the appender that sends a snapshot, that ends the catch-up of a new member as if it had finished. See "Heap" in
	 * the design document.
	 */
	private static void warnWithoutExitOnOutOfMemory() {
		var diagnostics = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
		if (diagnostics != null && !Boolean.parseBoolean(diagnostics.getVMOption("ExitOnOutOfMemoryError").getValue())) {
			System.err.println("WARNING: this JVM runs without -XX:+ExitOnOutOfMemoryError: an out of memory error on a"
					+ " thread of Ratis can leave a cluster without a leader. See \"Heap\" in the design document");
		}
	}

	private static void admin(Options options) throws IOException, InterruptedException {
		var peers = options.peers();
		var groupId = RaftGroupId.valueOf(UUID.fromString(options.value("--group-id", NodeConfig.DEFAULT_GROUP_ID)));
		var timeout = Duration.ofMillis(Long.parseLong(options.value("--timeout-ms",
				String.valueOf(DEFAULT_ADMIN_TIMEOUT.toMillis()))));
		var command = options.positional();
		if (command.isEmpty()) {
			usage("missing admin command");
		}
		long deadline = System.nanoTime() + timeout.toNanos();
		var group = RaftGroup.valueOf(groupId, peers.stream().map(ReplicatedServerMain::raftPeer).toList());
		var anyConfig = NodeConfig.defaults(peers.getFirst().id(), peers, Path.of("."));
		try (var client = RaftClient.newBuilder()
				.setClientId(ClientId.randomId())
				.setRaftGroup(group)
				.setProperties(RaftSettings.client(anyConfig))
				.setRetryPolicy(retryUntil(deadline))
				.build()) {
			RaftClientReply reply = switch (command.getFirst()) {
				case "status" -> {
					for (PeerConfig peer : peers) {
						try {
							var info = client.getGroupManagementApi(RaftPeerId.valueOf(peer.id())).info(groupId);
							System.out.println(peer.id() + ": " + info.getRoleInfoProto().getRole()
									+ ", commit infos " + info.getCommitInfos());
						} catch (IOException | RuntimeException ex) {
							System.out.println(peer.id() + ": unreachable (" + ex.getMessage() + ")");
						}
					}
					yield null;
				}
				case "transfer-leader" -> attempt(timeout, () -> client.admin().transferLeadership(
						RaftPeerId.valueOf(arg(command)), 10_000));
				case "add-peer" -> {
					var added = PeerConfig.parse(arg(command));
					requireFitToJoin(added, peers, groupId, deadline);
					requireCapabilities(added, peers, groupId);
					var members = new ArrayList<>(peers.stream().map(ReplicatedServerMain::raftPeer).toList());
					members.removeIf(p -> p.getId().toString().equals(added.id()));
					members.add(raftPeer(added));
					yield attempt(timeout, () -> client.admin().setConfiguration(members));
				}
				case "remove-peer" -> {
					var removed = arg(command);
					yield attempt(timeout, () -> client.admin().setConfiguration(peers.stream()
							.filter(p -> !p.id().equals(removed))
							.map(ReplicatedServerMain::raftPeer)
							.toList()));
				}
				case "snapshot" -> {
					var api = client.getSnapshotManagementApi(RaftPeerId.valueOf(arg(command)));
					yield attempt(timeout, () -> api.create(timeout.toMillis()));
				}
				case "checksum" -> {
					boolean allowSkipped = options.flag("--allow-skipped");
					if (command.size() != 1) {
						usage("checksum takes no argument");
					}
					// Not idempotent: proposed again only after an answer that says nothing was proposed.
					long index = proposeOp(peers, groupId, ReplicaOps.OP_PROPOSE_CHECKSUM, new byte[0], false, deadline);
					System.out.println("checksum at log index " + index);
					var results = awaitResults(peers, groupId, ReplicaOps.OP_CHECKSUM_RESULT,
							java.nio.ByteBuffer.allocate(Long.BYTES).putLong(index).array(), deadline);
					exitOn(checksumVerdict(index, peers.stream().map(PeerConfig::id).toList(), results, allowSkipped));
					System.out.println("OK");
					yield null;
				}
				case "backup" -> {
					var name = arg(command);
					if (!ReplicaOps.validBackupName(name)) {
						usage("invalid backup name " + name);
					}
					// A request id makes it safe to propose again after an answer that was lost.
					long requestId = new java.security.SecureRandom().nextLong() | 1;
					long index = proposeOp(peers, groupId, ReplicaOps.OP_PROPOSE_BACKUP,
							ReplicaOps.backupPayload(requestId, name), true, deadline);
					System.out.println("backup " + name + " at log index " + index);
					yield awaitBackup(peers, groupId, name, index, deadline);
				}
				case "backup-status" -> {
					if (command.size() != 3 || !ReplicaOps.validBackupName(command.get(1))) {
						usage("backup-status takes a backup name and the log index that backup printed");
					}
					yield awaitBackup(peers, groupId, command.get(1), Long.parseLong(command.get(2)), deadline);
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

	/**
	 * Proposes a cluster operation through the first member that takes it; returns the log index of its entry. After an
	 * answer that leaves open whether the entry was proposed (a timeout, a leader change), it tries again only if the
	 * operation is {@code idempotent}: otherwise it fails and says so.
	 */
	private static long proposeOp(List<PeerConfig> peers, RaftGroupId groupId, byte op, byte[] payload,
			boolean idempotent, long deadlineNanos) throws InterruptedException {
		while (true) {
			String last = null;
			for (PeerConfig peer : peers) {
				try {
					var reply = ReplicaOps.call(peer.raftAddress(), groupId, op, payload, remaining(deadlineNanos)).join();
					return java.nio.ByteBuffer.wrap(reply).getLong();
				} catch (CompletionException ex) {
					var status = Status.fromThrowable(ex.getCause());
					if (status.getCode() == Status.Code.FAILED_PRECONDITION || status.getCode() == Status.Code.INVALID_ARGUMENT
							|| status.getCode() == Status.Code.UNIMPLEMENTED) {
						fail(peer.id() + " refused: " + status.getDescription());
					}
					if (!idempotent && !notProposed(status.getCode())) {
						fail("the answer of " + peer.id() + " (" + status + ") does not tell whether the operation was"
								+ " proposed: look for it in the logs of the members, or run the command again");
					}
					last = peer.id() + ": " + status;
				}
			}
			if (System.nanoTime() - deadlineNanos > 0) {
				fail("no member took the request within the time limit (--timeout-ms); last answer " + last);
			}
			Thread.sleep(SEED_SNAPSHOT_POLL_MS);
		}
	}

	/**
	 * Whether a failed call to propose an operation certainly proposed nothing: the node was not there (UNAVAILABLE: no
	 * connection, or the node is starting) or serves no such group (NOT_FOUND).
	 */
	static boolean notProposed(Status.Code code) {
		return code == Status.Code.UNAVAILABLE || code == Status.Code.NOT_FOUND;
	}

	/** Exit code of {@code checksum} and {@code backup} when a member gave no final result, or no usable one. */
	static final int EXIT_INCOMPLETE = 4;

	/** Why a cluster operation is not OK, and the exit code to end with. */
	record Verdict(int exitCode, String problem) {}

	/**
	 * The verdict on a checksum at {@code index}: null (OK) only if every member gave a digest and all are equal. With
	 * {@code allowSkipped}, a member that skipped the position ({@link ReplicaOps.State#NONE}) is not counted.
	 */
	static @Nullable Verdict checksumVerdict(long index, List<String> members, java.util.Map<String, ReplicaOps.Result> results,
			boolean allowSkipped) {
		var digests = results.values().stream().filter(r -> r.state() == ReplicaOps.State.DONE)
				.map(r -> r.detail() + "/" + r.keys()).distinct().count();
		if (digests > 1) {
			return new Verdict(1, "the replicas hold different data at log index " + index + ": see the lines above, and"
					+ " \"Checksums\" in docs/distributed/runbooks.md");
		}
		var missing = members.stream().filter(id -> {
			var result = results.get(id);
			return result == null || result.state() == ReplicaOps.State.FAILED || result.state() == ReplicaOps.State.PENDING
					|| (result.state() == ReplicaOps.State.NONE && !allowSkipped);
		}).toList();
		if (!missing.isEmpty() || digests == 0) {
			return new Verdict(EXIT_INCOMPLETE, "no digest from " + (missing.isEmpty() ? "any member" : missing)
					+ ": the check is incomplete, run it again (see the lines above)");
		}
		return null;
	}

	/** The verdict on a backup: null (OK) only if every member wrote it. */
	static @Nullable Verdict backupVerdict(List<String> members, java.util.Map<String, ReplicaOps.Result> results) {
		var failed = members.stream().filter(id -> results.containsKey(id)
				&& results.get(id).state() != ReplicaOps.State.DONE).toList();
		if (!failed.isEmpty()) {
			return new Verdict(1, "the backup is not on " + failed + ": see the lines above");
		}
		var missing = members.stream().filter(id -> !results.containsKey(id)).toList();
		if (!missing.isEmpty()) {
			return new Verdict(EXIT_INCOMPLETE, "no result from " + missing + " yet: run backup-status later");
		}
		return null;
	}

	private static void exitOn(@Nullable Verdict verdict) {
		if (verdict != null) {
			System.out.println("FAILED: " + verdict.problem());
			System.exit(verdict.exitCode());
		}
	}

	private static @Nullable RaftClientReply awaitBackup(List<PeerConfig> peers, RaftGroupId groupId, String name,
			long index, long deadlineNanos) throws InterruptedException {
		var results = awaitResults(peers, groupId, ReplicaOps.OP_BACKUP_RESULT, ReplicaOps.backupPayload(index, name),
				deadlineNanos);
		exitOn(backupVerdict(peers.stream().map(PeerConfig::id).toList(), results));
		System.out.println("OK");
		return null;
	}

	/**
	 * Asks every member for its result of a cluster operation until each one has a final one, or until the deadline;
	 * prints them.
	 */
	private static java.util.Map<String, ReplicaOps.Result> awaitResults(List<PeerConfig> peers, RaftGroupId groupId,
			byte op, byte[] payload, long deadlineNanos) throws InterruptedException {
		var results = new java.util.TreeMap<String, ReplicaOps.Result>();
		while (results.size() < peers.size() && System.nanoTime() - deadlineNanos < 0) {
			for (PeerConfig peer : peers) {
				if (results.containsKey(peer.id())) {
					continue;
				}
				try {
					var result = ReplicaOps.decodeResult(ReplicaOps.call(peer.raftAddress(), groupId, op, payload,
							PEER_ANSWER_TIMEOUT).join());
					if (result.state() != ReplicaOps.State.PENDING) {
						results.put(peer.id(), result);
					}
				} catch (CompletionException unanswered) {
					// Down or busy: asked again.
				}
			}
			if (results.size() < peers.size()) {
				Thread.sleep(SEED_SNAPSHOT_POLL_MS);
			}
		}
		for (PeerConfig peer : peers) {
			var result = results.get(peer.id());
			System.out.println(peer.id() + ": " + (result == null ? "no result within the time limit"
					: result.state() + (result.keys() > 0 ? " " + result.keys() + " keys" : "") + " " + result.detail()));
		}
		return results;
	}

	private static Duration remaining(long deadlineNanos) {
		return Duration.ofNanos(Math.max(1_000_000, deadlineNanos - System.nanoTime()));
	}

	/**
	 * Fails the command if {@code added} lacks a capability that the cluster uses already, or that every member that
	 * answered supports, which the leader may turn on at any moment (before the new node is in the configuration): it
	 * would stop on the first entry that it cannot read. The leader also asks a node that is catching up before it turns
	 * a capability on (see {@link ReplicaOps#pollPeers}). See {@link Capabilities}.
	 */
	private static void requireCapabilities(PeerConfig added, List<PeerConfig> peers, RaftGroupId groupId) {
		long clusterVersion = 0;
		long commonSupported = -1;
		boolean anyAnswered = false;
		for (PeerConfig member : peers) {
			if (member.id().equals(added.id())) {
				continue;
			}
			try {
				var reply = java.nio.ByteBuffer.wrap(ReplicaOps.call(member.raftAddress(), groupId,
						ReplicaOps.OP_CAPABILITIES, new byte[0], PEER_ANSWER_TIMEOUT).join());
				commonSupported &= reply.getLong();
				clusterVersion |= reply.getLong();
				anyAnswered = true;
			} catch (CompletionException unanswered) {
				if (Status.fromThrowable(unanswered.getCause()).getCode() == Status.Code.UNIMPLEMENTED) {
					// A member of a version before the cluster versions: the leader cannot raise the version past it.
					commonSupported = 0;
					anyAnswered = true;
				}
				// A member down holds the version where it is.
			}
		}
		long supported;
		try {
			supported = java.nio.ByteBuffer.wrap(ReplicaOps.call(added.raftAddress(), groupId, ReplicaOps.OP_CAPABILITIES,
					new byte[0], PEER_ANSWER_TIMEOUT).join()).getLong();
		} catch (CompletionException ex) {
			if (Status.fromThrowable(ex.getCause()).getCode() != Status.Code.UNIMPLEMENTED) {
				fail("node " + added.id() + " did not tell its capabilities: " + ex.getCause().getMessage());
			}
			supported = 0;
		}
		long missing = missingCapabilities(clusterVersion, anyAnswered ? commonSupported : 0, supported);
		if (missing != 0) {
			fail("node " + added.id() + " runs a version without " + Capabilities.describe(missing) + ", which this cluster"
					+ " uses or is about to use: it would stop on the first such entry. Upgrade it first");
		}
	}

	/**
	 * What a node supporting {@code supported} lacks to join a cluster at {@code clusterVersion} whose members all
	 * support {@code membersSupport}.
	 */
	static long missingCapabilities(long clusterVersion, long membersSupport, long supported) {
		return (clusterVersion | membersSupport) & ~supported;
	}

	/** Ratis' default retries at once and for ever: a change that cannot happen would flood the leader, with no end. */
	private static RetryPolicy retryUntil(long deadlineNanos) {
		return _ -> System.nanoTime() - deadlineNanos < 0 ? () -> ADMIN_RETRY_SLEEP : RetryPolicy.NO_RETRY_ACTION;
	}

	@FunctionalInterface
	private interface AdminCall {
		RaftClientReply run() throws IOException;
	}

	private static RaftClientReply attempt(Duration timeout, AdminCall call) {
		try {
			return call.run();
		} catch (IOException ex) {
			fail("no result within the time limit of " + timeout.toMillis() + " ms (--timeout-ms), or the leader refused: "
					+ ex.getMessage() + ". A leader that started a change goes on with it: see the status, and run the command"
					+ " again");
			throw new AssertionError(ex);
		}
	}

	private static void fail(String message) {
		System.out.println("FAILED: " + message);
		System.exit(1);
	}

	/**
	 * Fails the command if {@code added} does not run or holds a log of its own. It then waits until every member that
	 * answers has applied the genesis entry and, on a seeded cluster, holds a snapshot, which it takes right after: a new
	 * member cannot catch up from a leader without one.
	 */
	private static void requireFitToJoin(PeerConfig added, List<PeerConfig> peers, RaftGroupId groupId, long deadlineNanos)
			throws InterruptedException {
		var members = peers.stream().filter(peer -> !peer.id().equals(added.id())).toList();
		while (true) {
			var answers = members.stream().map(member -> nodeState(member, groupId)).toList();
			var target = nodeState(added, groupId);
			var known = answers.stream().map(ReplicatedServerMain::answerOrNull).filter(Objects::nonNull).toList();
			var clusterId = known.stream().map(NodeState::clusterId).filter(Objects::nonNull).findFirst().orElse(null);
			String waitingFor = null;
			try {
				var state = target.join();
				if (state == null) {
					fail("node " + added.id() + " at " + added.raftAddress() + " has no group " + groupId.getUuid()
							+ ": start it with the same --group-id as the cluster");
					return;
				}
				var refusal = state.refusalToJoin(added.id(), clusterId);
				if (refusal != null) {
					fail(refusal);
					return;
				}
			} catch (CompletionException ex) {
				if (Status.fromThrowable(ex.getCause()).getCode() != Status.Code.ABORTED) {
					fail("node " + added.id() + " does not answer at " + added.raftAddress() + ": start it first, with --join and"
							+ " the same --group-id (" + ex.getCause().getMessage() + ")");
					return;
				}
				waitingFor = "node " + added.id() + ", which is installing a snapshot";
			}
			if (waitingFor == null) {
				waitingFor = memberNotReady(known);
			}
			if (waitingFor == null) {
				return;
			}
			if (System.nanoTime() - deadlineNanos > 0) {
				fail("no result within the time limit (--timeout-ms): still waiting for " + waitingFor);
				return;
			}
			Thread.sleep(SEED_SNAPSHOT_POLL_MS);
		}
	}

	/**
	 * What keeps a new member from getting a state from any member that may lead, null if nothing does. Until a member has
	 * applied the genesis entry nobody can tell whether it needs a snapshot.
	 */
	private static @Nullable String memberNotReady(List<NodeState> members) {
		if (members.stream().anyMatch(member -> member.clusterId() == null)) {
			return "a member that has not applied the first entry of the cluster yet";
		}
		if (members.stream().anyMatch(member -> member.seeded() && !member.hasSnapshot())) {
			return "a member of this seeded cluster that has no snapshot yet, which a new member needs";
		}
		return null;
	}

	private static CompletableFuture<@Nullable NodeState> nodeState(PeerConfig peer, RaftGroupId groupId) {
		var channel = NettyChannelBuilder.forTarget(peer.raftAddress()).usePlaintext().build();
		return TermService.nodeState(channel, groupId, "", PEER_ANSWER_TIMEOUT).whenComplete((_, _) -> channel.shutdown());
	}

	private static @Nullable NodeState answerOrNull(CompletableFuture<@Nullable NodeState> answer) {
		try {
			return answer.join();
		} catch (CompletionException ex) {
			return null;
		}
	}

	private static void repairLog(Options options) throws IOException {
		var dataDir = Path.of(options.required("--data-dir"));
		var groupId = options.value("--group-id", NodeConfig.DEFAULT_GROUP_ID);
		var dryRun = options.flag("--dry-run");
		options.requireConsumed();
		var report = RaftLogRepair.run(dataDir, groupId, dryRun);
		report.lines().forEach(System.out::println);
		if (!report.success()) {
			System.exit(1);
		}
	}

	private static void restoreBackup(Options options) throws IOException {
		var backup = Path.of(options.required("--backup"));
		var dataDir = Path.of(options.required("--data-dir"));
		var rockserverConfig = options.value("--rockserver-config") != null
				? Path.of(options.value("--rockserver-config")) : null;
		options.requireConsumed();
		try {
			var applied = ReplicaOps.restore(backup, dataDir, rockserverConfig);
			System.out.println("OK: " + dataDir + " holds the data up to log index " + applied.getIndex()
					+ " of the old cluster, and belongs to no cluster. Start every node of the new cluster from a copy of the same"
					+ " backup, without --join");
		} catch (IOException | RuntimeException ex) {
			fail(String.valueOf(ex));
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
				        [--max-transaction-lease-ms N] [--log-segment-bytes N] [--pipelined-updates] [--join]
				        [--jvm-pause-limit-ms N]
				  admin --peer ... [--group-id UUID] [--timeout-ms N]
				        status | transfer-leader ID | add-peer ID=... | remove-peer ID | snapshot ID
				        | checksum [--allow-skipped] | backup NAME | backup-status NAME INDEX
				  repair-log --data-dir DIR [--group-id UUID] [--dry-run]
				  restore-backup --backup DIR --data-dir DIR [--rockserver-config FILE]""");
		System.exit(2);
	}

	/** Minimal "--name value" parser; {@code --peer} may repeat, and a {@link #FLAGS flag} takes no value. */
	private static final class Options {
		private static final List<String> FLAGS = List.of("--pipelined-updates", "--join", "--dry-run", "--allow-skipped");

		private final List<String[]> pairs = new ArrayList<>();
		private final List<String> positional = new ArrayList<>();
		private final List<String> consumed = new ArrayList<>();

		Options(List<String> args) {
			for (int i = 0; i < args.size(); i++) {
				var arg = args.get(i);
				if (FLAGS.contains(arg)) {
					pairs.add(new String[] {arg, "true"});
				} else if (arg.startsWith("--")) {
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

		boolean flag(String name) {
			return value(name) != null;
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
