package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBSyncAPI;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.BooleanSupplier;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import org.apache.ratis.client.RaftClient;
import org.apache.ratis.client.RaftClientConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.protocol.ClientId;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.retry.RetryPolicies;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * Three {@link ReplicatedServerMain} JVMs under a seeded random fault schedule, with clients that keep working.
 *
 * <p>The schedule runs one fault at a time (kill and restart, graceful restart, pause, isolation, a 2-1
 * partition with the target on the minority side, a one-way cut of the target's outgoing links, or a single cut
 * link), each followed by its repair, so at most one node is unavailable at any time and the majority keeps
 * serving. Meanwhile:</p>
 * <ul>
 *   <li>register clients, each bound to one node or failing over across all of them, put unique values and read
 *   them back; the history must be linearizable;</li>
 *   <li>transfer clients move money between accounts with optimistic transactions ({@code getForUpdate}, puts,
 *   commit) and an auditor reads every balance in transactions; every consistent read, and the final state, must
 *   hold the initial total with no negative balance.</li>
 * </ul>
 * <p>Afterwards every replica must hold identical data. The fault schedule depends only on the seed, printed at the
 * start: rerun with {@code -D}{@value #SEED_PROPERTY}{@code =<seed>} to replay it (the workload interleaving still
 * varies). {@value #SECONDS_PROPERTY} sets the length of the schedule in seconds, and
 * {@value #MAX_UNAVAILABILITY_PROPERTY} the longest tolerated gap between acknowledged operations. Times in the
 * output are relative to the test start, with the wall clock time to match the node logs (add
 * {@code -Dorg.slf4j.simpleLogger.showDateTime=true}, which the nodes inherit, to timestamp those).</p>
 */
@Timeout(value = 10, unit = TimeUnit.MINUTES)
class RandomFaultTest {

	static final String SEED_PROPERTY = "rockserver.randomFault.seed";
	static final String SECONDS_PROPERTY = "rockserver.randomFault.seconds";
	static final String MAX_UNAVAILABILITY_PROPERTY = "rockserver.randomFault.maxUnavailableMillis";
	private static final long DEFAULT_SECONDS = 90;

	private static final int NODES = 3;
	private static final List<String> SERVER_ARGS = List.of("--election-timeout-ms", "400:800",
			"--request-timeout-ms", "5000",
			// Small enough that restarted nodes regularly catch up from a snapshot instead of the log.
			"--snapshot-every", "1000");
	private static final Duration READY_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration CONVERGE_TIMEOUT = Duration.ofSeconds(60);
	private static final Duration WORKER_STOP_TIMEOUT = Duration.ofSeconds(60);
	/** No acknowledged multi-endpoint operation for this long means the cluster was unavailable. */
	private static final Duration MAX_UNAVAILABILITY = Duration.ofMillis(Long.getLong(MAX_UNAVAILABILITY_PROPERTY,
			15_000));

	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext SETUP_CONTEXT = RequestContext.batch(Duration.ofSeconds(10));
	private static final RequestContext OP_CONTEXT = RequestContext.latency(Duration.ofSeconds(2));
	private static final RequestContext TX_CONTEXT = RequestContext.batch(Duration.ofSeconds(3));
	private static final Duration CALL_RETRY = Duration.ofSeconds(5);
	private static final Duration FINAL_RETRY = Duration.ofSeconds(60);
	private static final Duration TX_LEASE = Duration.ofSeconds(10);

	private static final int REGISTER_KEYS = 16;
	private static final int HOT_COUNTERS = 2;
	private static final int HOT_WORKERS = 4;
	private static final int ACCOUNTS = 8;
	private static final long INITIAL_BALANCE = 1_000;
	private static final long TOTAL = ACCOUNTS * INITIAL_BALANCE;
	private static final int MAX_TRANSFER = 150;
	private static final int CONFLICT_RETRIES = 20;
	private static final Duration AUDIT_PERIOD = Duration.ofMillis(250);

	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;

	@Test
	void registersStayLinearizableAndTransfersKeepTheTotalUnderRandomFaults() throws Exception {
		long seed = Long.getLong(SEED_PROPERTY, new SecureRandom().nextLong());
		var length = Duration.ofSeconds(Long.getLong(SECONDS_PROPERTY, DEFAULT_SECONDS));
		var schedule = FaultStep.schedule(new Random(seed), length);
		var repro = "seed " + seed + " (rerun with -D" + SEED_PROPERTY + "=" + seed + ")";
		log("RandomFaultTest " + repro + ", " + schedule.size() + " faults over " + length + ", data in " + root);
		schedule.forEach(step -> log("  planned: " + step));

		try (var cluster = ProcessCluster.start(root, NODES, SERVER_ARGS);
				var probe = new RaftProbe(cluster);
				var setup = new ClusterClient(cluster.apiAddresses(), FINAL_RETRY, true)) {
			cluster.waitReady(READY_TIMEOUT);
			long registers = setup.createColumn("registers", SCHEMA, SETUP_CONTEXT).valueOrThrow();
			long bank = setup.createColumn("bank", SCHEMA, SETUP_CONTEXT).valueOrThrow();
			for (int account = 0; account < ACCOUNTS; account++) {
				setup.put(bank, key(account), balance(INITIAL_BALANCE), SETUP_CONTEXT).valueOrThrow();
			}

			long hotColumn = setup.createColumn("hot", SCHEMA, SETUP_CONTEXT).valueOrThrow();
			var hot = new HotCounters(cluster.apiAddresses(), hotColumn, 0, HOT_COUNTERS, HOT_WORKERS);
			hot.start();
			var workload = new Workload(cluster, registers, bank, new Random(seed ^ 0x5DEECE66DL));
			workload.start();
			var executed = new ArrayList<ExecutedStep>();
			try {
				for (var step : schedule) {
					executed.add(step.run(cluster, probe));
				}
			} finally {
				cluster.network().heal();
				workload.stop();
				hot.stop();
			}
			log("Faults done; " + workload.summary());
			workload.rethrowWorkerFailures();

			// Every node is back: the final state must be readable, and the history checkable, through any node.
			var finalRegisters = workload.finalRegisterReads(setup);
			var finalBalances = workload.finalAudit(setup);
			assertBalancesHold(finalBalances, "final balances, " + repro);
			hot.check(key -> setup.get(hotColumn, HotCounters.key(key), SETUP_CONTEXT).valueOrThrow(), 1);

			assertTrue(workload.registerWrites.get() > 0, "no register write was acknowledged, " + repro);
			assertTrue(workload.committedTransfers.get() > 0, "no transfer committed, " + repro);
			assertTrue(workload.consistentAudits.get() > 0, "no audit read a consistent state, " + repro);
			assertEquals(List.of(), List.copyOf(workload.auditViolations), "audits broke the invariant, " + repro);

			var operations = workload.history.operations();
			long checkStart = System.nanoTime();
			var linearizability = LinearizabilityChecker.check(operations);
			assertTrue(linearizability.linearizable(), () -> repro + "\n" + linearizability.description());
			log(linearizability.description() + " (checked in "
					+ Duration.ofNanos(System.nanoTime() - checkStart).toMillis() + " ms)");

			reportAvailability(executed, workload, repro);

			awaitConverged(cluster, probe, repro);
			for (var id : cluster.nodeIds()) {
				cluster.stop(id);
			}
			assertReplicasIdentical(cluster, registers, bank, finalRegisters, repro);
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Fault schedule

	private enum Fault {
		/** SIGKILL, then a restart after the hold. */
		KILL,
		/** SIGTERM (graceful stop with leadership hand-off), then a restart after the hold. */
		RESTART,
		/** SIGSTOP for the hold, then SIGCONT. */
		PAUSE,
		/** Every link from and to the target cut. */
		ISOLATE,
		/** The cluster split 2-1, the target alone on the minority side. */
		PARTITION,
		/** Only the connections the target opens are cut: it still receives, and answers on, inbound ones. */
		ONE_WAY,
		/** One link pair cut, between the target and one other node; the third node still reaches both. */
		CUT_LINK
	}

	/** Which node a step hits, resolved when it runs (the leader is only known then). */
	private enum Role { LEADER, FOLLOWER, ANY }

	/**
	 * One planned step. {@code pick} chooses among the nodes matching {@code role}, and the other node of a
	 * {@link Fault#CUT_LINK}, so the schedule is a function of the seed alone.
	 */
	private record FaultStep(Fault fault, Role role, int pick, Duration hold, Duration quiet) {

		static List<FaultStep> schedule(Random random, Duration length) {
			var steps = new ArrayList<FaultStep>();
			var faults = Fault.values();
			var roles = Role.values();
			long planned = 0;
			while (planned < length.toMillis()) {
				var step = new FaultStep(faults[random.nextInt(faults.length)],
						roles[random.nextInt(roles.length)],
						random.nextInt(1_000),
						Duration.ofMillis(500 + random.nextInt(4_500)),
						Duration.ofMillis(500 + random.nextInt(2_500)));
				steps.add(step);
				planned += step.hold.toMillis() + step.quiet.toMillis();
			}
			return steps;
		}

		ExecutedStep run(ProcessCluster cluster, RaftProbe probe) throws IOException {
			var leader = probe.leader(cluster.nodeIds());
			var target = target(cluster.nodeIds(), leader);
			var others = cluster.nodeIds().stream().filter(id -> !id.equals(target)).toList();
			long start = System.nanoTime();
			log(String.format("[%s] %s node %s (role %s, leader %s), hold %d ms", clock(), fault, target, role,
					leader.orElse("unknown"), hold.toMillis()));
			switch (fault) {
				case KILL -> {
					cluster.kill(target);
					waitFor(hold);
					cluster.start(target);
					cluster.waitReady(target, READY_TIMEOUT);
				}
				case RESTART -> {
					cluster.stop(target);
					waitFor(hold);
					cluster.start(target);
					cluster.waitReady(target, READY_TIMEOUT);
				}
				case PAUSE -> {
					cluster.pause(target);
					waitFor(hold);
					cluster.resume(target);
				}
				case ISOLATE -> {
					cluster.network().isolate(target);
					waitFor(hold);
					cluster.network().heal();
				}
				case PARTITION -> {
					cluster.network().partition(Set.of(target), Set.copyOf(others));
					waitFor(hold);
					cluster.network().heal();
				}
				case ONE_WAY -> {
					others.forEach(other -> cluster.network().blockLink(target, other));
					waitFor(hold);
					cluster.network().heal();
				}
				case CUT_LINK -> {
					var other = others.get(pick % others.size());
					cluster.network().blockLink(target, other);
					cluster.network().blockLink(other, target);
					waitFor(hold);
					cluster.network().heal();
				}
			}
			long repaired = System.nanoTime();
			waitFor(quiet);
			for (var id : cluster.nodeIds()) {
				if (!cluster.isRunning(id)) {
					throw new AssertionError("Node " + id + " died after " + fault + " of node " + target + "\n"
							+ cluster.logTail(id));
				}
			}
			return new ExecutedStep(this, target, leader.map(target::equals).orElse(false), start, repaired);
		}

		private String target(List<String> ids, Optional<String> leader) {
			var candidates = switch (role) {
				case LEADER -> leader.map(List::of).orElse(ids);
				case FOLLOWER -> leader.map(l -> ids.stream().filter(id -> !id.equals(l)).toList()).orElse(ids);
				case ANY -> ids;
			};
			return candidates.get(pick % candidates.size());
		}

		@Override
		public String toString() {
			return fault + " " + role + " (pick " + pick + "), hold " + hold.toMillis() + " ms, then quiet "
					+ quiet.toMillis() + " ms";
		}
	}

	/** A step as it ran: on which node, and when (nanoTime) the fault started and was repaired. */
	private record ExecutedStep(FaultStep step, String target, boolean targetWasLeader, long startNanos,
			long repairedNanos) {}

	// ---------------------------------------------------------------------------------------------------------
	// Workload

	/** The concurrent clients and what they observed. */
	private static final class Workload {

		private final ProcessCluster cluster;
		private final long registers;
		private final long bank;
		private final Random random;
		private final AtomicBoolean stopping = new AtomicBoolean();
		private final List<ClusterClient> clients = new ArrayList<>();
		private final List<Future<?>> workers = new ArrayList<>();
		private final ExecutorService executor = Executors.newThreadPerTaskExecutor(
				Thread.ofPlatform().name("random-fault-worker-", 0).daemon().factory());

		final History history = new History();
		/** Invocation and completion (nanoTime) of every acknowledged write of a multi-endpoint client. */
		final ConcurrentLinkedQueue<long[]> acknowledgedWrites = new ConcurrentLinkedQueue<>();
		/** Completion (nanoTime) of every acknowledged operation of a multi-endpoint client. */
		final ConcurrentLinkedQueue<Long> acknowledged = new ConcurrentLinkedQueue<>();
		final ConcurrentLinkedQueue<String> auditViolations = new ConcurrentLinkedQueue<>();
		/** Makes every written register value unique. */
		private final AtomicLong valueSequence = new AtomicLong();
		final AtomicLong registerWrites = new AtomicLong();
		final AtomicLong registerReads = new AtomicLong();
		final AtomicLong registerUnknown = new AtomicLong();
		final AtomicLong registerFailed = new AtomicLong();
		final AtomicLong committedTransfers = new AtomicLong();
		final AtomicLong skippedTransfers = new AtomicLong();
		final AtomicLong conflicts = new AtomicLong();
		final AtomicLong unknownTransfers = new AtomicLong();
		final AtomicLong failedTransfers = new AtomicLong();
		final AtomicLong consistentAudits = new AtomicLong();
		final AtomicLong failedAudits = new AtomicLong();

		Workload(ProcessCluster cluster, long registers, long bank, Random random) {
			this.cluster = cluster;
			this.registers = registers;
			this.bank = bank;
			this.random = random;
		}

		void start() {
			var ids = cluster.nodeIds();
			// One register client bound to each node: it sees that node fail and recover.
			for (var id : ids) {
				var single = new LinkedHashMap<String, HostAndPort>();
				single.put(id, cluster.apiAddress(id));
				spawn("register-" + id, client(single, false),
						(client, rnd) -> registerOp("n" + id, client, rnd, false));
			}
			// Two failing-over register clients, starting on different nodes.
			for (int i = 0; i < 2; i++) {
				var name = "m" + i;
				spawn("register-" + name, client(rotated(i), false),
						(client, rnd) -> registerOp(name, client, rnd, true));
			}
			// Transfers retry ambiguous commits: running a transfer twice still keeps the total.
			for (int i = 0; i < 3; i++) {
				spawn("transfer-" + i, client(rotated(i), true), this::transfer);
			}
			spawn("audit", client(rotated(2), true), this::audit);
		}

		private ClusterClient client(Map<String, HostAndPort> addresses,
				boolean retryAmbiguous) {
			var client = new ClusterClient(addresses, CALL_RETRY, retryAmbiguous);
			clients.add(client);
			return client;
		}

		/** Every node, in failover order starting at node {@code first}. */
		private Map<String, HostAndPort> rotated(int first) {
			var ids = cluster.nodeIds();
			var result = new LinkedHashMap<String, HostAndPort>();
			for (int i = 0; i < ids.size(); i++) {
				var id = ids.get((first + i) % ids.size());
				result.put(id, cluster.apiAddress(id));
			}
			return result;
		}

		private interface Operation {
			void run(ClusterClient client, Random random) throws Exception;
		}

		private void spawn(String name, ClusterClient client, Operation operation) {
			var workerRandom = new Random(random.nextLong());
			workers.add(executor.submit(() -> {
				Thread.currentThread().setName("random-fault-" + name);
				while (!stopping.get()) {
					operation.run(client, workerRandom);
				}
				return null;
			}));
		}

		private void registerOp(String worker, ClusterClient client, Random random, boolean multiEndpoint) {
			int index = random.nextInt(REGISTER_KEYS);
			var key = "r" + index;
			if (random.nextInt(4) == 0) {
				updateRegister(worker, client, random, index, key);
			} else if (random.nextBoolean()) {
				var value = worker + "-" + valueSequence.incrementAndGet();
				var pending = history.invokePut(key, value);
				long invoked = System.nanoTime();
				var result = client.put(registers, key(index), text(value), OP_CONTEXT);
				pending.complete(result.status());
				count(result.status(), registerWrites);
				if (multiEndpoint && result.status() == OpStatus.OK) {
					long completed = System.nanoTime();
					acknowledgedWrites.add(new long[] {invoked, completed});
					acknowledged.add(completed);
				}
			} else {
				var pending = history.invokeGet(key);
				var result = client.get(registers, key(index), OP_CONTEXT);
				pending.complete(result.status(), result.acknowledged() ? text(result.value()) : null);
				count(result.status(), registerReads);
				if (multiEndpoint && result.status() == OpStatus.OK) {
					acknowledged.add(System.nanoTime());
				}
			}
		}

		/** A read-modify-write of a register, or one in four times the read of an update closed without writing. */
		private void updateRegister(String worker, ClusterClient client, Random random, int index, String key) {
			long invoked = System.nanoTime();
			var opened = client.read(OP_CONTEXT,
					api -> api.get(0, registers, key(index), RequestType.<Buf>forUpdate()));
			if (!opened.acknowledged()) {
				return;
			}
			var update = opened.value();
			var read = update.previous() == null ? null : text(update.previous());
			if (random.nextInt(4) == 0) {
				var close = history.invokeGetAt(key, invoked);
				try {
					client.api(opened.node(), OP_CONTEXT).closeFailedUpdate(update.updateId());
					close.complete(OpStatus.OK, read);
				} catch (RuntimeException ex) {
					close.complete(OpStatus.FAILED, null);
				}
				return;
			}
			var value = worker + "-" + valueSequence.incrementAndGet();
			var pending = history.invokeUpdateAt(key, read, value, invoked);
			try {
				client.api(opened.node(), OP_CONTEXT).put(update.updateId(), registers, key(index), text(value),
						RequestType.none());
				pending.complete(OpStatus.OK);
			} catch (RuntimeException ex) {
				pending.complete(HotCounters.isRetry(ex) ? OpStatus.FAILED : OpStatus.UNKNOWN);
			}
		}

		private void count(OpStatus status, AtomicLong ok) {
			switch (status) {
				case OK -> ok.incrementAndGet();
				case UNKNOWN -> registerUnknown.incrementAndGet();
				case FAILED -> registerFailed.incrementAndGet();
			}
		}

		/** One transfer between two random accounts, run again (fresh reads) while its commit conflicts. */
		private void transfer(ClusterClient client, Random random) {
			int from = random.nextInt(ACCOUNTS);
			int to = (from + 1 + random.nextInt(ACCOUNTS - 1)) % ACCOUNTS;
			long amount = 1 + random.nextInt(MAX_TRANSFER);
			for (int attempt = 0; attempt < CONFLICT_RETRIES && !stopping.get(); attempt++) {
				var moved = new AtomicBoolean();
				var result = client.transaction(TX_CONTEXT, TX_LEASE, (api, tx) -> {
					long fromBalance = readForUpdate(api, tx, from);
					long toBalance = readForUpdate(api, tx, to);
					moved.set(fromBalance >= amount);
					if (moved.get()) {
						api.put(tx, bank, key(from), balance(fromBalance - amount), RequestType.none());
						api.put(tx, bank, key(to), balance(toBalance + amount), RequestType.none());
					}
				});
				switch (result.status()) {
					case OK -> {
						if (!result.value()) {
							conflicts.incrementAndGet();
							continue;
						}
						(moved.get() ? committedTransfers : skippedTransfers).incrementAndGet();
					}
					case UNKNOWN -> unknownTransfers.incrementAndGet();
					case FAILED -> failedTransfers.incrementAndGet();
				}
				return;
			}
		}

		private long readForUpdate(RocksDBSyncAPI api, long tx, int account) {
			var update = api.get(tx, bank, key(account), RequestType.<Buf>forUpdate());
			if (update.previous() == null) {
				throw new AssertionError("Account " + account + " vanished");
			}
			return balance(update.previous());
		}

		/** Alternates snapshot reads (one range read) and validated reads ({@code getForUpdate} of all, commit). */
		private void audit(ClusterClient client, Random random) throws InterruptedException {
			boolean snapshot = random.nextBoolean();
			var balances = readBalances(client, snapshot);
			if (balances.isPresent()) {
				consistentAudits.incrementAndGet();
				var problem = invariantViolation(balances.get());
				if (problem != null) {
					var kind = snapshot ? "snapshot" : "validated";
					auditViolations.add(kind + " audit at " + clock() + ": " + problem);
				}
			} else {
				failedAudits.incrementAndGet();
			}
			Thread.sleep(AUDIT_PERIOD);
		}

		/** Every balance as of one consistent state, empty if the audit did not complete. */
		Optional<long[]> readBalances(ClusterClient client, boolean snapshot) {
			var seen = new AtomicReference<long[]>();
			var result = client.transaction(TX_CONTEXT, TX_LEASE, (api, tx) -> {
				seen.set(null);
				var read = new long[ACCOUNTS];
				if (snapshot) {
					// A range read inside a transaction uses the transaction's snapshot.
					try (var range = api.getRange(tx, bank, null, null, false, RequestType.allInRange())) {
						var entries = range.toList();
						if (entries.size() != ACCOUNTS) {
							throw new AssertionError("Expected " + ACCOUNTS + " accounts, read " + entries.size());
						}
						for (var entry : entries) {
							int account = (int) ByteBuffer.wrap(entry.keys().keys()[0].toByteArray()).getLong();
							read[account] = balance(entry.value());
						}
					}
				} else {
					for (int account = 0; account < ACCOUNTS; account++) {
						read[account] = readForUpdate(api, tx, account);
					}
				}
				seen.set(read);
			});
			// A snapshot read is consistent even if the (read-only) commit failed; a validated one only if it
			// committed, which proves that none of the keys changed after the transaction's snapshot.
			boolean consistent = snapshot ? seen.get() != null
					: result.status() == OpStatus.OK && Boolean.TRUE.equals(result.value());
			return consistent ? Optional.of(seen.get()) : Optional.empty();
		}

		void stop() throws Exception {
			stopping.set(true);
			executor.shutdown();
			if (!executor.awaitTermination(WORKER_STOP_TIMEOUT.toNanos(), TimeUnit.NANOSECONDS)) {
				executor.shutdownNow();
				throw new AssertionError("Workers did not stop within " + WORKER_STOP_TIMEOUT);
			}
			clients.forEach(ClusterClient::close);
		}

		void rethrowWorkerFailures() throws Exception {
			for (var worker : workers) {
				worker.get();
			}
		}

		/** Reads every register through the failing-over client, recording the reads in the history. */
		Map<String, String> finalRegisterReads(ClusterClient client) {
			var values = new TreeMap<String, String>();
			for (int index = 0; index < REGISTER_KEYS; index++) {
				var key = "r" + index;
				var pending = history.invokeGet(key);
				var result = client.get(registers, key(index), OP_CONTEXT);
				pending.complete(result.status(), result.acknowledged() ? text(result.value()) : null);
				assertEquals(OpStatus.OK, result.status(), () -> "final read of " + key + ": " + result.error());
				values.put(key, text(result.value()));
			}
			return values;
		}

		long[] finalAudit(ClusterClient client) {
			long deadline = System.nanoTime() + FINAL_RETRY.toNanos();
			while (true) {
				var balances = readBalances(client, true);
				if (balances.isPresent()) {
					return balances.get();
				}
				if (System.nanoTime() - deadline > 0) {
					throw new AssertionError("No final audit succeeded within " + FINAL_RETRY);
				}
			}
		}

		String summary() {
			return String.format("registers: %d writes ok, %d reads ok, %d unknown, %d failed;"
							+ " transfers: %d committed, %d skipped (low balance), %d conflicts, %d unknown, %d failed;"
							+ " audits: %d consistent, %d incomplete",
					registerWrites.get(), registerReads.get(), registerUnknown.get(), registerFailed.get(),
					committedTransfers.get(), skippedTransfers.get(), conflicts.get(), unknownTransfers.get(),
					failedTransfers.get(), consistentAudits.get(), failedAudits.get());
		}
	}

	private static String invariantViolation(long[] balances) {
		long sum = 0;
		for (long balance : balances) {
			sum += balance;
		}
		boolean negative = Arrays.stream(balances).anyMatch(balance -> balance < 0);
		if (sum == TOTAL && !negative) {
			return null;
		}
		return "sum " + sum + " (expected " + TOTAL + "), balances " + Arrays.toString(balances);
	}

	private static void assertBalancesHold(long[] balances, String what) {
		var problem = invariantViolation(balances);
		if (problem != null) {
			fail(what + ": " + problem);
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Availability

	/**
	 * Prints, for each fault, how long the failing-over clients waited for their first acknowledged write invoked
	 * after the fault started (failover time when the target was the leader), and fails if those clients went
	 * without any acknowledged operation for longer than {@link #MAX_UNAVAILABILITY}.
	 */
	private static void reportAvailability(List<ExecutedStep> steps, Workload workload, String repro) {
		var writes = new ArrayList<>(workload.acknowledgedWrites);
		var completions = workload.acknowledged.stream().sorted().toList();
		long worstGap = 0;
		long worstGapEnd = 0;
		for (int i = 1; i < completions.size(); i++) {
			long gap = completions.get(i) - completions.get(i - 1);
			if (gap > worstGap) {
				worstGap = gap;
				worstGapEnd = completions.get(i);
			}
		}
		var leaderFailovers = new ArrayList<Long>();
		for (var executed : steps) {
			long firstWrite = writes.stream()
					.filter(write -> write[0] >= executed.startNanos())
					.mapToLong(write -> write[1])
					.min()
					.orElse(Long.MAX_VALUE);
			long recovery = firstWrite == Long.MAX_VALUE ? -1 : (firstWrite - executed.startNanos()) / 1_000_000;
			if (executed.targetWasLeader() && executed.step().fault() != Fault.CUT_LINK && recovery >= 0) {
				leaderFailovers.add(recovery);
			}
			log(String.format("  %-9s node %s%s: first acknowledged write after %d ms, fault lasted %d ms",
					executed.step().fault(), executed.target(), executed.targetWasLeader() ? " (leader)" : "",
					recovery, (executed.repairedNanos() - executed.startNanos()) / 1_000_000));
		}
		var sortedFailovers = leaderFailovers.stream().sorted().toList();
		log("Leader faults: " + sortedFailovers.size() + ", failover ms " + sortedFailovers
				+ (sortedFailovers.isEmpty() ? "" : ", median " + sortedFailovers.get(sortedFailovers.size() / 2)));
		log("Longest gap between acknowledged multi-endpoint operations: " + worstGap / 1_000_000 + " ms, ending at "
				+ clockOf(worstGapEnd));
		assertTrue(worstGap < MAX_UNAVAILABILITY.toNanos(), "clients failing over saw no acknowledged operation for "
				+ worstGap / 1_000_000 + " ms (ending at " + clockOf(worstGapEnd) + "), " + repro);
	}

	// ---------------------------------------------------------------------------------------------------------
	// Replicas

	/** Waits until every node applied the whole committed log of the current leader. */
	private static void awaitConverged(ProcessCluster cluster, RaftProbe probe, String repro) {
		var last = new AtomicReference<Map<String, RaftProbe.PeerInfo>>();
		await(CONVERGE_TIMEOUT, () -> {
			var infos = new LinkedHashMap<String, RaftProbe.PeerInfo>();
			for (var id : cluster.nodeIds()) {
				probe.info(id).ifPresent(info -> infos.put(id, info));
			}
			last.set(infos);
			if (infos.size() != cluster.nodeIds().size()) {
				return false;
			}
			var leaders = infos.values().stream().filter(RaftProbe.PeerInfo::leader).toList();
			if (leaders.size() != 1) {
				return false;
			}
			long committed = leaders.getFirst().committed();
			return infos.values().stream().allMatch(info -> info.applied() == committed);
		}, () -> "replicas did not converge: " + last.get() + ", " + repro);
		log("Converged: " + last.get());
	}

	/**
	 * Opens every stopped node's database in this JVM and compares the user columns: names, ids, schemas and every
	 * key and value. The final register reads and the bank invariant must hold on each replica too.
	 */
	private static void assertReplicasIdentical(ProcessCluster cluster,
			long registers,
			long bank,
			Map<String, String> finalRegisters,
			String repro) throws IOException {
		var contents = new LinkedHashMap<String, Map<String, ColumnContents>>();
		for (var id : cluster.nodeIds()) {
			contents.put(id, replicaContents(cluster.dataDir(id).resolve("db"), "replica-" + id));
		}
		var reference = contents.values().iterator().next();
		var referenceId = cluster.nodeIds().getFirst();
		assertEquals(Set.of("registers", "bank", "hot"), reference.keySet(), "user columns of replica " + referenceId);
		for (var entry : contents.entrySet()) {
			var replica = entry.getValue();
			assertEquals(reference.keySet(), replica.keySet(), "columns of replica " + entry.getKey() + ", " + repro);
			for (var column : reference.keySet()) {
				var expected = reference.get(column);
				var actual = replica.get(column);
				assertEquals(expected.id(), actual.id(), "id of " + column + " on " + entry.getKey() + ", " + repro);
				assertEquals(expected.schema(), actual.schema(), "schema of " + column + " on " + entry.getKey());
				if (!expected.data().equals(actual.data())) {
					fail("Replica " + entry.getKey() + " differs from replica " + referenceId + " in column " + column
							+ ": " + difference(expected.data(), actual.data()) + ", " + repro);
				}
			}
		}
		assertEquals(registers, reference.get("registers").id());
		assertEquals(bank, reference.get("bank").id());
		var registerData = reference.get("registers").data();
		var stored = new TreeMap<String, String>();
		registerData.forEach((key, value) -> stored.put("r" + HexFormat.fromHexDigitsToLong(key), hexText(value)));
		var expectedRegisters = new TreeMap<String, String>();
		finalRegisters.forEach((key, value) -> {
			if (value != null) {
				expectedRegisters.put(key, value);
			}
		});
		assertEquals(expectedRegisters, stored, "replicated registers differ from the final reads, " + repro);
		var balances = new long[ACCOUNTS];
		var bankData = reference.get("bank").data();
		assertEquals(ACCOUNTS, bankData.size(), "accounts on the replicas, " + repro);
		bankData.forEach((key, value) -> balances[(int) HexFormat.fromHexDigitsToLong(key)]
				= HexFormat.fromHexDigitsToLong(value));
		assertBalancesHold(balances, "replicated balances, " + repro);
		log("Replicas identical: " + contents.keySet() + ", " + registerData.size() + " registers, balances "
				+ Arrays.toString(balances));
	}

	/** One user column of a replica; keys and values in hex, ordered by key. */
	private record ColumnContents(long id, ColumnSchema schema, TreeMap<String, String> data) {}

	private static Map<String, ColumnContents> replicaContents(Path dbDir, String name) throws IOException {
		var columns = new TreeMap<String, ColumnContents>();
		// The nodes run without --rockserver-config, so the default configuration opens their databases.
		try (var connection = new EmbeddedConnection(dbDir, name, null)) {
			var api = connection.getSyncApi(SETUP_CONTEXT);
			for (var definition : api.getAllColumnDefinitions().entrySet()) {
				if (definition.getKey().equals(NodeStore.META_COLUMN)) {
					continue;
				}
				long id = api.getColumnId(definition.getKey());
				var data = new TreeMap<String, String>();
				try (var range = api.getRange(0, id, null, null, false, RequestType.allInRange())) {
					range.forEach(kv -> data.put(hex(kv.keys()), kv.value() == null ? "" : hex(kv.value())));
				}
				columns.put(definition.getKey(), new ColumnContents(id, definition.getValue(), data));
			}
		}
		return columns;
	}

	private static String difference(TreeMap<String, String> expected, TreeMap<String, String> actual) {
		var keys = new TreeSet<>(expected.keySet());
		keys.addAll(actual.keySet());
		return keys.stream()
				.filter(key -> !Objects.equals(expected.get(key), actual.get(key)))
				.limit(5)
				.map(key -> "key " + key + ": " + expected.get(key) + " vs " + actual.get(key))
				.collect(Collectors.joining("; "));
	}

	// ---------------------------------------------------------------------------------------------------------
	// Raft group info, straight from each node's Raft port

	/** Asks nodes for their Raft role and log positions, without retries and with a short timeout. */
	private static final class RaftProbe implements AutoCloseable {

		record PeerInfo(boolean leader, long term, long applied, long committed) {}

		private final RaftGroupId groupId = RaftGroupId.valueOf(UUID.fromString(NodeConfig.DEFAULT_GROUP_ID));
		private final RaftClient client;

		RaftProbe(ProcessCluster cluster) {
			var peers = cluster.nodeIds().stream()
					.map(id -> RaftPeer.newBuilder()
							.setId(id)
							.setAddress(cluster.raftAddress(id).host() + ":" + cluster.raftAddress(id).port())
							.build())
					.toList();
			var properties = new RaftProperties();
			RaftClientConfigKeys.Rpc.setRequestTimeout(properties, TimeDuration.valueOf(1, TimeUnit.SECONDS));
			this.client = RaftClient.newBuilder()
					.setClientId(ClientId.randomId())
					.setRaftGroup(RaftGroup.valueOf(groupId, peers))
					.setProperties(properties)
					.setRetryPolicy(RetryPolicies.noRetry())
					.build();
		}

		/** Empty if the node does not answer (down, paused, starting). */
		Optional<PeerInfo> info(String id) {
			try {
				var reply = client.getGroupManagementApi(RaftPeerId.valueOf(id)).info(groupId);
				var role = reply.getRoleInfoProto();
				var log = reply.getLogInfoProto();
				boolean leader = role.getRole() == RaftPeerRole.LEADER;
				long term = leader ? role.getLeaderInfo().getTerm() : log.getLastEntry().getTerm();
				long applied = log.getApplied().getIndex();
				return Optional.of(new PeerInfo(leader, term, applied, log.getCommitted().getIndex()));
			} catch (IOException | RuntimeException e) {
				return Optional.empty();
			}
		}

		/** The node that claims leadership in the highest term, if any answers. */
		Optional<String> leader(List<String> ids) {
			String leader = null;
			long term = -1;
			for (var id : ids) {
				var info = info(id);
				if (info.isPresent() && info.get().leader() && info.get().term() > term) {
					leader = id;
					term = info.get().term();
				}
			}
			return Optional.ofNullable(leader);
		}

		@Override
		public void close() throws IOException {
			client.close();
		}
	}

	// ---------------------------------------------------------------------------------------------------------
	// Helpers

	private static final long EPOCH_NANOS = System.nanoTime();

	private static void waitFor(Duration duration) {
		try {
			Thread.sleep(duration);
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
			throw new AssertionError("Interrupted", e);
		}
	}

	private static void await(Duration timeout, BooleanSupplier condition, Supplier<String> what) {
		long deadline = System.nanoTime() + timeout.toNanos();
		while (!condition.getAsBoolean()) {
			if (System.nanoTime() - deadline > 0) {
				fail(what.get());
			}
			waitFor(Duration.ofMillis(100));
		}
	}

	private static String clock() {
		return clockOf(System.nanoTime());
	}

	private static String clockOf(long nanos) {
		var wall = LocalTime.now().minusNanos(System.nanoTime() - nanos).truncatedTo(ChronoUnit.MILLIS);
		return String.format("%.3fs (%s)", (nanos - EPOCH_NANOS) / 1e9, wall);
	}

	private static void log(String message) {
		System.out.println(message);
		System.out.flush();
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf balance(long value) {
		return Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array());
	}

	private static long balance(Buf value) {
		return ByteBuffer.wrap(value.toByteArray()).getLong();
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	private static String text(Buf value) {
		return value == null ? null : new String(value.toByteArray(), StandardCharsets.UTF_8);
	}

	private static String hex(Keys keys) {
		var result = new StringBuilder();
		for (var segment : keys.keys()) {
			result.append(hex(segment));
		}
		return result.toString();
	}

	private static String hex(Buf value) {
		return HexFormat.of().formatHex(value.toByteArray());
	}

	private static String hexText(String hex) {
		return new String(HexFormat.of().parseHex(hex), StandardCharsets.UTF_8);
	}
}
