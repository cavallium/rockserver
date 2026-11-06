package it.cavallium.rockserver.distributed;

import com.sun.management.HotSpotDiagnosticMXBean;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.lang.management.ManagementFactory;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.function.Consumer;
import java.util.function.IntConsumer;
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
import org.rocksdb.NativeLibraryLoader;

/**
 * Command line entry point.
 *
 * <pre>
 * run   --node-id ID --peer ID=RAFTHOST:PORT/APIHOST:PORT (repeat per member) --data-dir DIR
 *       [--rockserver-config FILE] [--raft-bind-host HOST] [--api-bind-host HOST] [--group-id UUID]
 *       [--snapshot-every N] [--election-timeout-ms MIN:MAX] [--request-timeout-ms N]
 *       [--max-transaction-lease-ms N] [--log-segment-bytes N] [--pipelined-updates] [--join]
 *       [--jvm-pause-limit-ms N] [--snapshot-every-bytes N] [--parallel-apply-threads N]
 *       [--allow-without-exit-on-oom]
 * admin --peer ... [--group-id UUID] [--timeout-ms N]
 *       status | transfer-leader ID | add-peer ID=... | remove-peer ID | snapshot ID
 *       | checksum [--allow-skipped] | backup NAME | backup-status NAME INDEX
 * repair-log --data-dir DIR [--group-id UUID] [--dry-run]
 * export-seed --data-dir DIR --target DIR [--rockserver-config FILE]
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
 * <p>Run a node with {@code -XX:+ExitOnOutOfMemoryError}: {@code run} refuses to start without it, unless
 * {@code --allow-without-exit-on-oom} is given (for development only). Running out of direct memory on a thread of
 * replication stops the node too,
 * with exit code 3, see {@link DirectMemoryWatch}. See "Heap" in the design document.</p>
 *
 * <p>{@code run} exits with 4, and does not start, if the configuration in its log no longer contains the node: it was
 * removed from the cluster, and a supervisor must not start it again. See "Replacing a node" in the design document.
 * It exits with 5, and does not start, if its database has a cluster version with capabilities this version lacks (a
 * downgrade, or a newer cluster's backup). Every command's exit codes: "Exit codes" in
 * {@code docs/distributed/runbooks.md}; 4 means "removed" for {@code run} and "incomplete" for {@code admin}.</p>
 *
 * <p>Unless {@code -Djava.io.tmpdir} is given, {@code run} extracts the RocksDB library into {@code DATA-DIR/tmp}, and
 * deletes the copies that earlier runs left there; a given one outside the data directory is warned about.</p>
 *
 * <p>{@code export-seed} runs on a stopped node and copies its database into a new directory that can seed a new
 * cluster, the last resort of "No quorum" in the design document (see {@link NodeStore#exportSeed}).</p>
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
	private static final TimeDuration ADMIN_RETRY_SLEEP = TimeDuration.valueOf(500, TimeUnit.MILLISECONDS);
	/** How long {@code add-peer} waits for an answer of the new node and of each member, per round. */
	private static final Duration PEER_ANSWER_TIMEOUT = Duration.ofSeconds(3);
	private static final long SEED_SNAPSHOT_POLL_MS = 200;

	private ReplicatedServerMain() {}

	/**
	 * How the commands end the process: {@link System#exit} unless a test replaced it (with one that throws, to run a
	 * command in its own JVM and read the exit code). The code after a call never runs: if the replacement returns,
	 * {@link #exit} throws.
	 */
	static volatile IntConsumer exiter = System::exit;

	/**
	 * How the shutdown hook ends the process once the node stopped after a failure: {@link Runtime#halt}, unless a test
	 * replaced it. A test also replaces {@link #shutdownHooks}, to run the hook itself.
	 */
	static volatile IntConsumer halter = code -> Runtime.getRuntime().halt(code);
	/** Where {@code run} registers its shutdown hook: the JVM's, unless a test replaced it. */
	static volatile Consumer<Thread> shutdownHooks = hook -> Runtime.getRuntime().addShutdownHook(hook);

	private static void exit(int code) {
		exiter.accept(code);
		throw new IllegalStateException("exit(" + code + ") returned");
	}

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
			case "export-seed" -> exportSeed(new Options(rest));
			case "restore-backup" -> restoreBackup(new Options(rest));
			default -> usage("unknown command " + args[0]);
		}
	}

	private static void run(Options options) throws IOException, InterruptedException {
		// Before anything makes a temporary file: the JDK reads the property once.
		useNodeTemporaryDirectory(Path.of(options.required("--data-dir")));
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
		boolean allowWithoutExitOnOom = options.flag("--allow-without-exit-on-oom");
		options.requireConsumed();
		requireExitOnOutOfMemory(allowWithoutExitOnOom);
		var directMemoryWatch = DirectMemoryWatch.start(error -> {
			// Like -XX:+ExitOnOutOfMemoryError: at once, without shutdown hooks.
			System.err.println("Node " + config.nodeId() + " ran out of direct memory, exiting: " + error);
			System.err.flush();
			Runtime.getRuntime().halt(3);
		});
		ReplicatedNode node;
		try {
			node = ReplicatedNode.start(config);
		} catch (ReplicatedNode.RemovedException e) {
			System.err.println("Node " + config.nodeId() + " does not start: " + e.getMessage());
			exit(4);
			return;
		} catch (ReplicatedNode.NewerVersionException e) {
			System.err.println("Node " + config.nodeId() + " does not start: " + e.getMessage());
			exit(5);
			return;
		} catch (IOException | RuntimeException e) {
			// Ratis and gRPC leave non-daemon threads behind, which would keep the JVM running without a node.
			System.err.println("Failed to start node " + config.nodeId() + ":");
			e.printStackTrace();
			exit(1);
			return;
		}
		serve(node, directMemoryWatch);
	}

	/**
	 * The rest of {@code run} once the node started: registers the shutdown hook, then waits until the node stops and
	 * exits with {@link #failedExitStatus} if it failed. The main thread's {@code exit} is a {@link System#exit}, which runs
	 * the hook, and the hook ends with {@link Runtime#halt}: so both give the same status, or the hook's 3 would replace
	 * the 4 of a node that found out it was removed and the 5 of a replica that met a newer cluster version
	 * ({@code RestartPreventExitStatus=4 5} relies on those).
	 */
	static void serve(ReplicatedNode node, @Nullable DirectMemoryWatch directMemoryWatch) {
		var nodeId = node.config().nodeId();
		shutdownHooks.accept(new Thread(() -> {
			int status = closeOnSignal(node);
			// A replica failure that raced the signal: its own stop found the node already closing, so the main thread
			// below saw a normal stop, and the JVM would exit with the signal's status. Exit with the failure's.
			// After a failure the main thread's own exit also runs this hook, which then halts with the same status.
			if (status != 0) {
				System.err.println("Node " + nodeId + " stopped after a failure during its shutdown");
				System.err.flush();
				halter.accept(status);
			}
		}, "rockserver-shutdown"));
		System.out.println("READY " + nodeId);
		System.out.flush();
		var failure = node.stopped().join();
		if (directMemoryWatch != null) {
			directMemoryWatch.close();
		}
		int status = failedExitStatus(node);
		if (status != 0) {
			// The replica cannot apply the log, or Raft closed: exit, so that a supervisor notices.
			System.err.println("Node " + nodeId + " stopped after a failure: "
					+ (failure != null ? failure : "replica failed"));
			// When a signal already started the shutdown, this blocks; the hook then halts once the node stopped.
			exit(status);
		}
	}

	/**
	 * The shutdown hook's stop: closes the node, then waits until it stopped, also when a replica failure had already
	 * started closing it (then {@link ReplicatedNode#close} returns at once). Halting before that would exit while RocksDB
	 * still closes, whose destructors can hang the JVM (see {@code ReplicatedNode.awaitDatabaseClosed}). The wait is
	 * bounded, twice the database's own close timeout. Returns {@link #failedExitStatus}.
	 */
	static int closeOnSignal(ReplicatedNode node) {
		try {
			node.close();
		} catch (IOException ex) {
			System.err.println("Failed to stop cleanly: " + ex);
		}
		try {
			node.stopped().get(ReplicatedNode.DATABASE_CLOSE_TIMEOUT.multipliedBy(2).toMillis(),
					TimeUnit.MILLISECONDS);
		} catch (TimeoutException ex) {
			System.err.println("Node " + node.config().nodeId() + " did not stop within "
					+ ReplicatedNode.DATABASE_CLOSE_TIMEOUT.multipliedBy(2));
		} catch (ExecutionException ex) {
			// stopped() completes normally, with the cause as value.
		} catch (InterruptedException ex) {
			Thread.currentThread().interrupt();
		}
		return failedExitStatus(node);
	}

	/**
	 * The exit status of a node that stopped, used by both the main thread and the shutdown hook: 0 if it stopped without
	 * a failure, else {@link #exitStatusOf} its failure.
	 */
	static int failedExitStatus(ReplicatedNode node) {
		var stopped = node.stopped();
		var failure = stopped.isDone() && !stopped.isCompletedExceptionally() ? stopped.join() : null;
		return failure != null ? exitStatusOf(failure) : node.isFailed() ? 3 : 0;
	}

	/**
	 * The exit status for the failure that stopped a running node, the same as at start: 4 if it found out, while it waited
	 * outside the cluster, that it was removed; 5 if its replica met a cluster version it cannot read in its log or in an
	 * installed snapshot; else 3 (its replica failed, or its Raft server closed by itself).
	 */
	static int exitStatusOf(Throwable failure) {
		for (var cause = failure; cause != null; cause = cause.getCause()) {
			if (cause instanceof ReplicatedNode.RemovedException) {
				return 4;
			}
		}
		return ReplicatedNode.NewerVersionException.in(failure) ? 5 : 3;
	}

	/**
	 * An {@link OutOfMemoryError} on a thread of Ratis does not stop the JVM, and Ratis goes on without what it was doing.
	 * In the appender that sends a snapshot, that ends the catch-up of a new member as if it had finished, and with
	 * {@code add-peer} leaving a lost node in the configuration the cluster ends with no leader. See "Heap" in the design
	 * document. So a node refuses to start without {@code -XX:+ExitOnOutOfMemoryError}, unless told otherwise.
	 */
	private static void requireExitOnOutOfMemory(boolean allowWithout) {
		var diagnostics = ManagementFactory.getPlatformMXBean(HotSpotDiagnosticMXBean.class);
		if (diagnostics == null) {
			System.err.println("WARNING: cannot tell whether this JVM runs with -XX:+ExitOnOutOfMemoryError");
			return;
		}
		if (Boolean.parseBoolean(diagnostics.getVMOption("ExitOnOutOfMemoryError").getValue())) {
			return;
		}
		var problem = "this JVM runs without -XX:+ExitOnOutOfMemoryError: an out of memory error on a thread of Ratis can"
				+ " leave a cluster without a leader. See \"Heap\" in the design document";
		if (allowWithout) {
			System.err.println("WARNING: " + problem);
			return;
		}
		System.err.println("Error: " + problem + ". Add the option, or --allow-without-exit-on-oom (development only)");
		exit(2);
	}

	/**
	 * RocksDB's Java binding extracts its native library (17 MB) into the temporary directory at every start, and a killed
	 * JVM leaves it behind: on a shared {@code /tmp} with a quota, nodes then stop being able to start. Without an explicit
	 * {@code -Djava.io.tmpdir} the node extracts it into {@code DATA-DIR/tmp} itself, before any RocksDB class loads, and
	 * deletes the copies left there by earlier runs (a loaded library stays usable when its file is deleted).
	 *
	 * <p>Setting {@code java.io.tmpdir} here does not move the JDK's own temporary files: the JDK reads the property once,
	 * at VM start-up ({@code File.createTempFile} and {@code Files.createTempFile} keep the start-up value). It is set
	 * for the libraries that read it when they need it, such as Netty for its native transports. Pass
	 * {@code -Djava.io.tmpdir=DATA-DIR/tmp} to move everything.</p>
	 */
	private static void useNodeTemporaryDirectory(Path dataDir) throws IOException {
		useNodeTemporaryDirectory(dataDir, ManagementFactory.getRuntimeMXBean().getInputArguments().stream()
				.anyMatch(argument -> argument.startsWith("-Djava.io.tmpdir=")));
	}

	/** See {@link #useNodeTemporaryDirectory(Path)}; {@code explicit} if {@code -Djava.io.tmpdir} was given. */
	static void useNodeTemporaryDirectory(Path dataDir, boolean explicit) throws IOException {
		var dataRoot = dataDir.toAbsolutePath().normalize();
		if (explicit) {
			var given = Path.of(System.getProperty("java.io.tmpdir")).toAbsolutePath().normalize();
			if (!given.startsWith(dataRoot)) {
				System.err.println("WARNING: -Djava.io.tmpdir=" + given + " is outside the data directory " + dataRoot
						+ ": every start leaves a 17 MB copy of the RocksDB library there if the node is killed");
			}
			return;
		}
		var temporary = dataRoot.resolve("tmp");
		Files.createDirectories(temporary);
		try (var entries = Files.list(temporary)) {
			for (var entry : entries.filter(Files::isRegularFile)
					.filter(file -> file.getFileName().toString().contains("rocksdbjni")).toList()) {
				Files.deleteIfExists(entry);
			}
		}
		// RocksDB.loadLibrary() asks the same loader with the ROCKSDB_SHAREDLIB_DIR directory (by default none, which
		// means File.createTempFile, in the start-up java.io.tmpdir); once this call has loaded the library, that call
		// finds it loaded and extracts nothing.
		NativeLibraryLoader.getInstance().loadLibrary(temporary.toString());
		System.setProperty("java.io.tmpdir", temporary.toString());
	}

	private static void exportSeed(Options options) throws IOException {
		var dataDir = Path.of(options.required("--data-dir"));
		var target = Path.of(options.required("--target"));
		var rockserverConfig = options.value("--rockserver-config") != null
				? Path.of(options.value("--rockserver-config")) : null;
		options.requireConsumed();
		var exported = NodeStore.exportSeed(dataDir.resolve("db"), rockserverConfig, target);
		System.out.println("Exported the database of cluster " + exported.clusterId() + ", applied up to index "
				+ exported.applied().getIndex() + " of term " + exported.applied().getTerm() + ", to " + target
				+ ". Compare the applied index with the other replicas' before seeding a new cluster with it");
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
							ByteBuffer.allocate(Long.BYTES).putLong(index).array(), deadline);
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
					long requestId = new SecureRandom().nextLong() | 1;
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
					exit(1);
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
					return ByteBuffer.wrap(reply).getLong();
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
	static @Nullable Verdict checksumVerdict(long index, List<String> members, Map<String, ReplicaOps.Result> results,
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
	static @Nullable Verdict backupVerdict(List<String> members, Map<String, ReplicaOps.Result> results) {
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
			exit(verdict.exitCode());
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
	private static Map<String, ReplicaOps.Result> awaitResults(List<PeerConfig> peers, RaftGroupId groupId,
			byte op, byte[] payload, long deadlineNanos) throws InterruptedException {
		var results = new TreeMap<String, ReplicaOps.Result>();
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
				var reply = ByteBuffer.wrap(ReplicaOps.call(member.raftAddress(), groupId,
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
			supported = ByteBuffer.wrap(ReplicaOps.call(added.raftAddress(), groupId, ReplicaOps.OP_CAPABILITIES,
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
		exit(1);
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
				waitingFor = memberNotReady(known, members.size());
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
	 * applied the genesis entry nobody can tell whether it needs a snapshot. {@code members} are the answers of the
	 * members that answered, of {@code memberCount}: at least a majority must answer, since a check of the members that
	 * answered says nothing when none or few did (a leader among the silent ones may have no snapshot, and the change
	 * needs a majority anyway).
	 */
	static @Nullable String memberNotReady(List<NodeState> members, int memberCount) {
		int majority = memberCount / 2 + 1;
		if (members.size() < majority) {
			return "answers from a majority of the members (" + members.size() + " of " + memberCount + " answered)";
		}
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
			exit(1);
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
				        [--jvm-pause-limit-ms N] [--snapshot-every-bytes N] [--parallel-apply-threads N]
				        [--allow-without-exit-on-oom]
				  admin --peer ... [--group-id UUID] [--timeout-ms N]
				        status | transfer-leader ID | add-peer ID=... | remove-peer ID | snapshot ID
				        | checksum [--allow-skipped] | backup NAME | backup-status NAME INDEX
				  repair-log --data-dir DIR [--group-id UUID] [--dry-run]
				  export-seed --data-dir DIR --target DIR [--rockserver-config FILE]
				  restore-backup --backup DIR --data-dir DIR [--rockserver-config FILE]""");
		exit(2);
	}

	/** Minimal "--name value" parser; {@code --peer} may repeat, and a {@link #FLAGS flag} takes no value. */
	private static final class Options {
		private static final List<String> FLAGS = List.of("--pipelined-updates", "--join", "--dry-run",
				"--allow-without-exit-on-oom", "--allow-skipped");

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
