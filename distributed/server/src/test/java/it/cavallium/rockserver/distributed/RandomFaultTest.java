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
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.SecureRandom;
import java.time.Duration;
import java.time.LocalTime;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.HexFormat;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Properties;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import it.cavallium.rockserver.distributed.ListAppendChecker.MicroOp;
import it.cavallium.rockserver.distributed.ListAppendChecker.Outcome;
import it.cavallium.rockserver.distributed.ListAppendChecker.Transaction;
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
import org.jetbrains.annotations.Nullable;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * Three {@link ReplicatedServerMain} JVMs under a seeded random fault schedule, with clients that keep working.
 *
 * <p>The schedule runs one fault at a time (kill and restart, graceful restart, pause, isolation, a 2-1
 * partition with the target on the minority side, a one-way cut of the target's outgoing links, a single cut
 * link, slow or throttled links, a blackhole, a flapping link, or lost and cut client links), each followed by its
 * repair, so at most one node is unavailable at any time and the majority keeps serving. With
 * {@link #overlapping()} (the subclass {@code OverlappingRandomFaultTest}) a second fault starts while the first one
 * holds, possibly costing the majority: then only safety is checked, not availability. Meanwhile:</p>
 * <ul>
 *   <li>register clients, each bound to one node or failing over across all of them, put unique values and read
 *   them back; the history must be linearizable;</li>
 *   <li>transfer clients move money between accounts with optimistic transactions ({@code getForUpdate}, puts,
 *   commit) and an auditor reads every balance in transactions; every consistent read, and the final state, must
 *   hold the initial total with no negative balance;</li>
 *   <li>list-append clients run transactions of reads and appends ({@code getForUpdate} and put) on a few lists;
 *   {@link ListAppendChecker} must find no anomaly at all (every read is validated at commit, so committed
 *   transactions must be serializable), and every committed append must be in the final lists.</li>
 * </ul>
 * <p>Every node's persisted Raft term ({@code raft-meta}) is sampled throughout and must never decrease. Workload
 * clients reach the nodes through {@link ProcessCluster#clientLink client links}, so client-side faults hit
 * them; setup and final reads go direct.</p>
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
	static final String OVERLAP_PROPERTY = "rockserver.randomFault.overlap";
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
	private static final int MINIMUM_HOT_INCREMENTS = 20;
	private static final int ACCOUNTS = 8;
	private static final long INITIAL_BALANCE = 1_000;
	private static final long TOTAL = ACCOUNTS * INITIAL_BALANCE;
	private static final int MAX_TRANSFER = 150;
	private static final int CONFLICT_RETRIES = 20;
	private static final Duration AUDIT_PERIOD = Duration.ofMillis(250);
	private static final int LIST_KEYS = 6;
	private static final int APPEND_WORKERS = 3;
	private static final int MAX_MICRO_OPS = 4;
	private static final Duration TERM_SAMPLE_PERIOD = Duration.ofMillis(100);

	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;

	/** What the nodes run with: {@code PipelinedRandomFaultTest} changes it. */
	List<String> serverArgs() {
		return SERVER_ARGS;
	}

	/** Whether faults overlap (see the class comment): {@code OverlappingRandomFaultTest} turns it on. */
	boolean overlapping() {
		return Boolean.getBoolean(OVERLAP_PROPERTY);
	}

	/** Extra JVM options of a node: {@code ClockSkewRandomFaultTest} skews clocks with them. */
	List<String> jvmOptions(String nodeId) {
		return List.of();
	}

	@Test
	void registersStayLinearizableAndTransfersKeepTheTotalUnderRandomFaults() throws Exception {
		long seed = Long.getLong(SEED_PROPERTY, new SecureRandom().nextLong());
		var length = Duration.ofSeconds(Long.getLong(SECONDS_PROPERTY, DEFAULT_SECONDS));
		boolean overlap = overlapping();
		var schedule = overlap ? CompoundStep.schedule(new Random(seed), length)
				: FaultStep.schedule(new Random(seed), length).stream().map(CompoundStep::single).toList();
		var repro = "seed " + seed + " (rerun with -D" + SEED_PROPERTY + "=" + seed + ")";
		log(getClass().getSimpleName() + " " + repro + ", " + schedule.size() + (overlap ? " overlapping" : "")
				+ " faults over " + length + ", data in " + root);
		schedule.forEach(step -> log("  planned: " + step));

		try (var cluster = ProcessCluster.create(root, NODES, 0, serverArgs());
				var probe = new RaftProbe(cluster);
				var setup = new ClusterClient(cluster.apiAddresses(), FINAL_RETRY, true)) {
			for (var id : cluster.members()) {
				cluster.setJvmOptions(id, jvmOptions(id));
				cluster.start(id);
			}
			cluster.waitReady(READY_TIMEOUT);
			var terms = new TermMonitor(cluster);
			long registers = setup.createColumn("registers", SCHEMA, SETUP_CONTEXT).valueOrThrow();
			long bank = setup.createColumn("bank", SCHEMA, SETUP_CONTEXT).valueOrThrow();
			for (int account = 0; account < ACCOUNTS; account++) {
				setup.put(bank, key(account), balance(INITIAL_BALANCE), SETUP_CONTEXT).valueOrThrow();
			}

			long lists = setup.createColumn("lists", SCHEMA, SETUP_CONTEXT).valueOrThrow();
			long hotColumn = setup.createColumn("hot", SCHEMA, SETUP_CONTEXT).valueOrThrow();
			var hot = new HotCounters(cluster.apiAddresses(), hotColumn, 0, HOT_COUNTERS, HOT_WORKERS);
			hot.start();
			var workload = new Workload(cluster, registers, bank, lists, new Random(seed ^ 0x5DEECE66DL));
			workload.start();
			var executed = new ArrayList<ExecutedStep>();
			try {
				for (var step : schedule) {
					executed.add(step.run(cluster, probe));
					hot.awaitChainAfterFault(cluster.pipelinedUpdates());
				}
			} finally {
				cluster.network().heal();
				cluster.healClientLinks();
				workload.stop();
				hot.stop();
				terms.close();
			}
			log("Faults done; " + workload.summary());
			workload.rethrowWorkerFailures();

			// Every node is back: the final state must be readable, and the history checkable, through any node.
			var finalRegisters = workload.finalRegisterReads(setup);
			var finalBalances = workload.finalAudit(setup);
			assertBalancesHold(finalBalances, "final balances, " + repro);
			hot.check(key -> setup.get(hotColumn, HotCounters.key(key), SETUP_CONTEXT).valueOrThrow(), MINIMUM_HOT_INCREMENTS,
					cluster.pipelinedUpdates());

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

			var finalLists = workload.finalListReads(setup);
			var appendCheck = ListAppendChecker.check(workload.appendHistory());
			log(appendCheck.description());
			assertTrue(appendCheck.valid(), () -> repro + "\n" + appendCheck.description());
			assertTrue(workload.committedAppendTransactions.get() > 0, "no list-append transaction committed, " + repro);
			workload.assertCommittedAppendsKept(finalLists, repro);

			log(terms.summary());
			assertEquals(List.of(), terms.violations(), "a node's Raft term went down, " + repro);
			// Without these the check above passes vacuously, for example if Ratis moved or renamed its meta file.
			assertEquals(new TreeSet<>(cluster.nodeIds()), new TreeSet<>(terms.sampledNodes()),
					"the Raft term of some node was never read, " + repro);
			if (executed.stream().anyMatch(step -> step.targetWasLeader()
					&& (step.step().fault() == Fault.KILL || step.step().fault() == Fault.RESTART))) {
				assertTrue(terms.increases() > 0, "the leader was stopped but no term increase was seen, " + repro);
			}

			reportAvailability(executed, workload, repro, !overlap);

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
		CUT_LINK,
		/** Every link from and to the target delayed by 20 to 200 ms each way, with as much jitter. */
		SLOW,
		/** Every link from and to the target capped at 64 to 512 KiB/s each way. */
		THROTTLE,
		/** Every link from and to the target silent (bytes held, nothing reset): failure seen only by timeouts. */
		BLACKHOLE,
		/** The target isolated and healed again every 50 to 400 ms: repeated elections, terms must still only grow. */
		FLAP,
		/** The target's client link carries requests but drops every answer: committed writes go unacknowledged. */
		CLIENT_LOSS,
		/** The target's client link cut: its clients fail over, the node itself is fine. */
		CLIENT_CUT;

		/** Whether it stops or freezes the target's process (two of them never hit one node at once). */
		boolean process() {
			return this == KILL || this == RESTART || this == PAUSE;
		}
	}

	/** Which node a step hits, resolved when it runs (the leader is only known then). */
	private enum Role { LEADER, FOLLOWER, ANY }

	/** Undoes one injected fault. */
	@FunctionalInterface
	private interface Repair {
		void run() throws IOException;
	}

	/**
	 * One planned step. {@code pick} chooses among the nodes matching {@code role}, the other node of a
	 * {@link Fault#CUT_LINK}, and the parameters of shaped and flapping links, so the schedule is a function of the
	 * seed alone.
	 */
	private record FaultStep(Fault fault, Role role, int pick, Duration hold, Duration quiet) {

		static List<FaultStep> schedule(Random random, Duration length) {
			var steps = new ArrayList<FaultStep>();
			long planned = 0;
			while (planned < length.toMillis()) {
				var step = random(random);
				steps.add(step);
				planned += step.hold.toMillis() + step.quiet.toMillis();
			}
			return steps;
		}

		static FaultStep random(Random random) {
			var faults = Fault.values();
			var roles = Role.values();
			return new FaultStep(faults[random.nextInt(faults.length)],
					roles[random.nextInt(roles.length)],
					random.nextInt(1_000),
					Duration.ofMillis(500 + random.nextInt(4_500)),
					Duration.ofMillis(500 + random.nextInt(2_500)));
		}

		/**
		 * Starts the fault on {@code target} and returns how to end it. A network fault registers the links it
		 * holds in {@code held}, so a {@link Fault#FLAP flapping} fault running at the same time leaves them alone.
		 */
		Repair inject(ProcessCluster cluster, String target, HeldLinks held) {
			var network = cluster.network();
			var others = cluster.nodeIds().stream().filter(id -> !id.equals(target)).toList();
			List<List<String>> heldLinks = switch (fault) {
				case ISOLATE, PARTITION, SLOW, THROTTLE, BLACKHOLE -> network.linksOf(target);
				case ONE_WAY -> others.stream().map(other -> List.of(target, other)).toList();
				case CUT_LINK -> {
					var other = others.get(pick % others.size());
					yield List.of(List.of(target, other), List.of(other, target));
				}
				default -> List.of();
			};
			if (heldLinks.isEmpty()) {
				return injectHeld(cluster, network, target, others, held, heldLinks);
			}
			// Registering and cutting are atomic for a flapper, which checks the registry under the same lock.
			synchronized (held) {
				held.add(heldLinks);
				return injectHeld(cluster, network, target, others, held, heldLinks);
			}
		}

		private Repair injectHeld(ProcessCluster cluster, Network network, String target, List<String> others,
				HeldLinks held, List<List<String>> heldLinks) {
			switch (fault) {
				case KILL -> {
					cluster.kill(target);
					return () -> {
						cluster.start(target);
						cluster.waitReady(target, READY_TIMEOUT);
					};
				}
				case RESTART -> {
					cluster.stop(target);
					return () -> {
						cluster.start(target);
						cluster.waitReady(target, READY_TIMEOUT);
					};
				}
				case PAUSE -> {
					cluster.pause(target);
					return () -> cluster.resume(target);
				}
				case ISOLATE -> network.isolate(target);
				case PARTITION -> network.partition(Set.of(target), Set.copyOf(others));
				case ONE_WAY -> others.forEach(other -> network.blockLink(target, other));
				case CUT_LINK -> {
					var other = others.get(pick % others.size());
					network.blockLink(target, other);
					network.blockLink(other, target);
				}
				case SLOW -> {
					var delay = Duration.ofMillis(20 + pick % 181);
					var shaping = LinkProxy.Shaping.latency(delay, delay);
					network.linksOf(target).forEach(link -> network.shapeLink(link.get(0), link.get(1), shaping));
				}
				case THROTTLE -> {
					var shaping = LinkProxy.Shaping.bandwidth((64L << (pick % 4)) * 1024);
					network.linksOf(target).forEach(link -> network.shapeLink(link.get(0), link.get(1), shaping));
				}
				case BLACKHOLE -> network.linksOf(target)
						.forEach(link -> network.blackholeLink(link.get(0), link.get(1)));
				case FLAP -> {
					var flapping = new AtomicBoolean(true);
					var flapRandom = new Random(pick);
					// The flapper cuts and restores only the links of the target that no other fault holds, so an
					// overlapping network fault keeps its links for its whole hold.
					var cutByFlapper = new ArrayList<List<String>>();
					var flapper = Thread.ofPlatform().name("random-fault-flap-" + target).daemon().start(() -> {
						boolean cut = false;
						while (flapping.get()) {
							if (cut) {
								held.restoreFree(network, cutByFlapper);
								cutByFlapper.clear();
							} else {
								cutByFlapper.addAll(held.cutFree(network, network.linksOf(target)));
							}
							cut = !cut;
							try {
								Thread.sleep(50 + flapRandom.nextInt(351));
							} catch (InterruptedException e) {
								return;
							}
						}
					});
					return () -> {
						flapping.set(false);
						flapper.interrupt();
						try {
							flapper.join();
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
						}
						held.restoreFree(network, cutByFlapper);
					};
				}
				case CLIENT_LOSS -> {
					cluster.clientLink(target).dropReplies();
					return () -> cluster.clientLink(target).unblock();
				}
				case CLIENT_CUT -> {
					cluster.clientLink(target).block();
					return () -> cluster.clientLink(target).unblock();
				}
			}
			// Every other fault is in the network between nodes. Healing it all also ends an overlapping network
			// fault, which is repaired at the same time anyway (both are repaired once both holds are over).
			return () -> {
				synchronized (held) {
					held.remove(heldLinks);
					network.heal();
				}
			};
		}

		String target(List<String> ids, Optional<String> leader) {
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

	/** The links held by the network faults of the running step, as {@code [src, dst]} pairs. */
	private static final class HeldLinks {

		private final Set<List<String>> links = new HashSet<>();

		synchronized void add(List<List<String>> faulted) {
			links.addAll(faulted);
		}

		synchronized void remove(List<List<String>> faulted) {
			faulted.forEach(links::remove);
		}

		/** Cuts every link of {@code candidates} that no fault holds, and returns those. */
		synchronized List<List<String>> cutFree(Network network, List<List<String>> candidates) {
			var cut = candidates.stream().filter(link -> !links.contains(link)).toList();
			cut.forEach(link -> network.blockLink(link.get(0), link.get(1)));
			return cut;
		}

		/** Restores every link of {@code cut} that no fault took over meanwhile. */
		synchronized void restoreFree(Network network, List<List<String>> cut) {
			cut.stream().filter(link -> !links.contains(link))
					.forEach(link -> network.restoreLink(link.get(0), link.get(1)));
		}
	}

	/**
	 * A fault, and optionally a second one that starts {@code offset} into the first one's hold and holds for its
	 * own time. Both are repaired, the second first, once both holds are over. Two process faults never hit the
	 * same node; two network faults may touch the same links, and repairing the first heals both. A flapping link
	 * never touches the links another network fault holds (see {@link HeldLinks}).
	 */
	private record CompoundStep(FaultStep first, @Nullable FaultStep second,
			Duration offset) {

		static CompoundStep single(FaultStep step) {
			return new CompoundStep(step, null, Duration.ZERO);
		}

		static List<CompoundStep> schedule(Random random, Duration length) {
			var steps = new ArrayList<CompoundStep>();
			long planned = 0;
			while (planned < length.toMillis()) {
				var first = FaultStep.random(random);
				var second = FaultStep.random(random);
				var offset = Duration.ofMillis(random.nextLong(first.hold().toMillis()));
				var step = new CompoundStep(first, second, offset);
				steps.add(step);
				planned += step.holdMillis() + second.quiet().toMillis();
			}
			return steps;
		}

		long holdMillis() {
			return second == null ? first.hold().toMillis()
					: Math.max(first.hold().toMillis(), offset.toMillis() + second.hold().toMillis());
		}

		ExecutedStep run(ProcessCluster cluster, RaftProbe probe) throws IOException {
			var ids = cluster.nodeIds();
			var leader = probe.leader(ids);
			var target = first.target(ids, leader);
			long start = System.nanoTime();
			log(String.format("[%s] %s node %s (role %s, leader %s), hold %d ms", clock(), first.fault(), target,
					first.role(), leader.orElse("unknown"), first.hold().toMillis()));
			var held = new HeldLinks();
			var firstRepair = first.inject(cluster, target, held);
			Repair secondRepair = null;
			if (second != null) {
				waitFor(offset);
				var secondLeader = probe.leader(ids);
				var candidates = first.fault().process() && second.fault().process()
						? ids.stream().filter(id -> !id.equals(target)).toList() : ids;
				var secondTarget = second.target(candidates, secondLeader.filter(candidates::contains));
				log(String.format("[%s]   overlapping %s node %s (role %s, leader %s), hold %d ms", clock(),
						second.fault(), secondTarget, second.role(), secondLeader.orElse("unknown"),
						second.hold().toMillis()));
				secondRepair = second.inject(cluster, secondTarget, held);
			}
			waitFor(Duration.ofMillis(Math.max(0, start / 1_000_000 + holdMillis() - System.nanoTime() / 1_000_000)));
			if (secondRepair != null) {
				secondRepair.run();
			}
			firstRepair.run();
			long repaired = System.nanoTime();
			waitFor((second != null ? second : first).quiet());
			for (var id : ids) {
				if (!cluster.isRunning(id)) {
					throw new AssertionError("Node " + id + " died after " + this + "\n" + cluster.logTail(id));
				}
			}
			return new ExecutedStep(first, target, leader.map(target::equals).orElse(false), start, repaired);
		}

		@Override
		public String toString() {
			return second == null ? first.toString()
					: first + ", overlapped after " + offset.toMillis() + " ms by " + second;
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
		private final long lists;
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
		/** Every list-append transaction attempt, with what is known of its outcome. */
		private final ConcurrentLinkedQueue<Transaction> appendTransactions = new ConcurrentLinkedQueue<>();
		/** Makes every appended element unique. */
		private final AtomicLong elementSequence = new AtomicLong();
		final AtomicLong committedAppendTransactions = new AtomicLong();
		final AtomicLong abortedAppendTransactions = new AtomicLong();
		final AtomicLong unknownAppendTransactions = new AtomicLong();

		Workload(ProcessCluster cluster, long registers, long bank, long lists, Random random) {
			this.cluster = cluster;
			this.registers = registers;
			this.bank = bank;
			this.lists = lists;
			this.random = random;
		}

		void start() {
			var ids = cluster.nodeIds();
			// One register client bound to each node: it sees that node fail and recover.
			for (var id : ids) {
				var single = new LinkedHashMap<String, HostAndPort>();
				single.put(id, cluster.proxiedApiAddress(id));
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
			// No ambiguous retries: a transaction run twice would append its elements twice.
			for (int i = 0; i < APPEND_WORKERS; i++) {
				var name = "append-" + i;
				spawn(name, client(rotated(i), false), (client, rnd) -> appendTransaction(name, client, rnd));
			}
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
				result.put(id, cluster.proxiedApiAddress(id));
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

		/**
		 * One list-append transaction of 1 to {@value #MAX_MICRO_OPS} reads and appends on random lists. Every list is
		 * read with {@code getForUpdate} the first time the transaction touches it, then kept locally, so the
		 * transaction never relies on reading its own writes. Each attempt appends fresh elements; attempts that the
		 * client restarted (nothing applied) are recorded as aborted.
		 */
		private void appendTransaction(String worker, ClusterClient client, Random random) {
			int count = 1 + random.nextInt(MAX_MICRO_OPS);
			var keys = new int[count];
			var appends = new boolean[count];
			for (int i = 0; i < count; i++) {
				keys[i] = random.nextInt(LIST_KEYS);
				appends[i] = random.nextBoolean();
			}
			var attempts = new ArrayList<List<MicroOp>>();
			var result = client.transaction(TX_CONTEXT, TX_LEASE, (api, tx) -> {
				var ops = new ArrayList<MicroOp>();
				synchronized (attempts) {
					attempts.add(ops);
				}
				var local = new TreeMap<Integer, List<Long>>();
				for (int i = 0; i < count; i++) {
					var key = "l" + keys[i];
					var list = local.get(keys[i]);
					if (list == null) {
						var update = api.get(tx, lists, key(keys[i]), RequestType.<Buf>forUpdate());
						list = new ArrayList<>(elements(update.previous()));
						local.put(keys[i], list);
						ops.add(MicroOp.read(key, list));
					} else if (!appends[i]) {
						ops.add(MicroOp.read(key, list));
					}
					if (appends[i]) {
						long element = elementSequence.incrementAndGet();
						list.add(element);
						api.put(tx, lists, key(keys[i]), elementsValue(list), RequestType.none());
						ops.add(MicroOp.append(key, element));
					}
				}
			});
			var outcome = switch (result.status()) {
				case OK -> Boolean.TRUE.equals(result.value()) ? Outcome.COMMITTED : Outcome.ABORTED;
				case UNKNOWN -> Outcome.UNKNOWN;
				case FAILED -> Outcome.ABORTED;
			};
			(switch (outcome) {
				case COMMITTED -> committedAppendTransactions;
				case ABORTED -> abortedAppendTransactions;
				case UNKNOWN -> unknownAppendTransactions;
			}).incrementAndGet();
			synchronized (attempts) {
				for (int i = 0; i < attempts.size(); i++) {
					// Only the last attempt may have reached a commit: the client restarts a transaction only when
					// nothing of it was applied.
					var attemptOutcome = i == attempts.size() - 1 ? outcome : Outcome.ABORTED;
					appendTransactions.add(new Transaction(worker + "#" + elementSequence.get() + "." + i,
							attemptOutcome, attempts.get(i)));
				}
			}
		}

		List<Transaction> appendHistory() {
			return List.copyOf(appendTransactions);
		}

		/** Reads every list in one validated transaction through the direct client, and adds it to the history. */
		Map<String, List<Long>> finalListReads(ClusterClient client) {
			long deadline = System.nanoTime() + FINAL_RETRY.toNanos();
			while (true) {
				var read = new TreeMap<String, List<Long>>();
				var result = client.transaction(TX_CONTEXT, TX_LEASE, (api, tx) -> {
					read.clear();
					for (int index = 0; index < LIST_KEYS; index++) {
						var update = api.get(tx, lists, key(index), RequestType.<Buf>forUpdate());
						read.put("l" + index, elements(update.previous()));
					}
				});
				if (result.status() == OpStatus.OK && Boolean.TRUE.equals(result.value())) {
					appendTransactions.add(new Transaction("final", Outcome.COMMITTED, read.entrySet().stream()
							.map(entry -> MicroOp.read(entry.getKey(), entry.getValue()))
							.toList()));
					return read;
				}
				if (System.nanoTime() - deadline > 0) {
					throw new AssertionError("No final list read committed within " + FINAL_RETRY + ": " + result.error());
				}
			}
		}

		/** Every element appended by a committed transaction is in its list at the end. */
		void assertCommittedAppendsKept(Map<String, List<Long>> finalLists, String repro) {
			var lost = new ArrayList<String>();
			for (var transaction : appendTransactions) {
				if (transaction.outcome() != Outcome.COMMITTED) {
					continue;
				}
				for (var op : transaction.ops()) {
					if (op.append() && !finalLists.getOrDefault(op.key(), List.of()).contains(op.element())) {
						lost.add(transaction.id() + " " + op);
					}
				}
			}
			assertEquals(List.of(), lost.stream().limit(10).toList(), "committed appends missing from the lists, "
					+ repro);
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
							+ " audits: %d consistent, %d incomplete;"
							+ " list appends: %d committed, %d aborted, %d unknown",
					registerWrites.get(), registerReads.get(), registerUnknown.get(), registerFailed.get(),
					committedTransfers.get(), skippedTransfers.get(), conflicts.get(), unknownTransfers.get(),
					failedTransfers.get(), consistentAudits.get(), failedAudits.get(),
					committedAppendTransactions.get(), abortedAppendTransactions.get(), unknownAppendTransactions.get());
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
	/** Faults that cost a targeted leader its leadership, so that clients must fail over. */
	private static final Set<Fault> LEADER_LOSING_FAULTS = Set.of(Fault.KILL, Fault.RESTART, Fault.PAUSE, Fault.ISOLATE,
			Fault.PARTITION, Fault.ONE_WAY, Fault.BLACKHOLE, Fault.FLAP);

	private static void reportAvailability(List<ExecutedStep> steps, Workload workload, String repro,
			boolean assertAvailable) {
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
			if (executed.targetWasLeader() && LEADER_LOSING_FAULTS.contains(executed.step().fault()) && recovery >= 0) {
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
		// Overlapping faults may take the majority away: then the gap is only reported.
		assertTrue(!assertAvailable || worstGap < MAX_UNAVAILABILITY.toNanos(), "clients failing over saw no acknowledged operation for "
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
		assertEquals(Set.of("registers", "bank", "lists", "hot"), reference.keySet(), "user columns of replica " + referenceId);
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
	// Raft terms

	/**
	 * Samples every node's persisted current term, from the {@code term} line of the {@code raft-meta} file in its
	 * Raft storage, which Ratis replaces atomically before it acts in a new term. The file is read even while the
	 * node is down or frozen. A term lower than one seen before on the same node is a violation: a node that forgot
	 * its term could vote twice in it.
	 */
	private static final class TermMonitor implements AutoCloseable {

		private final ProcessCluster cluster;
		private final Map<String, Long> last = new ConcurrentHashMap<>();
		private final ConcurrentLinkedQueue<String> violations = new ConcurrentLinkedQueue<>();
		private final AtomicLong samples = new AtomicLong();
		private final AtomicLong changes = new AtomicLong();
		private final AtomicBoolean closed = new AtomicBoolean();
		private final Thread thread;

		TermMonitor(ProcessCluster cluster) {
			this.cluster = cluster;
			this.thread = Thread.ofPlatform().name("random-fault-terms").daemon().start(this::loop);
		}

		private void loop() {
			while (!closed.get()) {
				sample();
				try {
					Thread.sleep(TERM_SAMPLE_PERIOD);
				} catch (InterruptedException e) {
					return;
				}
			}
		}

		private void sample() {
			samples.incrementAndGet();
			for (var id : cluster.nodeIds()) {
				var term = term(cluster.dataDir(id));
				if (term.isEmpty()) {
					continue;
				}
				var previous = last.put(id, term.get());
				if (previous != null && term.get() < previous) {
					violations.add("node " + id + " at " + clock() + ": term " + term.get() + " after " + previous);
				} else if (previous != null && term.get() > previous) {
					changes.incrementAndGet();
				}
			}
		}

		static Optional<Long> term(Path dataDir) {
			var file = dataDir.resolve("raft").resolve(NodeConfig.DEFAULT_GROUP_ID).resolve("current")
					.resolve("raft-meta");
			try (var in = Files.newInputStream(file)) {
				var properties = new Properties();
				properties.load(in);
				var term = properties.getProperty("term");
				return term == null ? Optional.empty() : Optional.of(Long.parseLong(term.strip()));
			} catch (IOException | NumberFormatException e) {
				// Not written yet, or being replaced.
				return Optional.empty();
			}
		}

		List<String> violations() {
			return List.copyOf(violations);
		}

		/** The nodes whose term was read at least once. */
		Set<String> sampledNodes() {
			return Set.copyOf(last.keySet());
		}

		long increases() {
			return changes.get();
		}

		String summary() {
			return "Raft terms: " + last + " after " + samples.get() + " samples, " + changes.get()
					+ " term increases seen, " + violations.size() + " decreases";
		}

		@Override
		public void close() {
			closed.set(true);
			thread.interrupt();
			try {
				thread.join();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
			sample();
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

	/** A list value: its elements in decimal, comma-separated; absent is the empty list. */
	private static List<Long> elements(Buf value) {
		if (value == null) {
			return List.of();
		}
		var text = text(value);
		if (text.isEmpty()) {
			return List.of();
		}
		return Arrays.stream(text.split(",")).map(Long::parseLong).toList();
	}

	private static Buf elementsValue(List<Long> elements) {
		return text(elements.stream().map(String::valueOf).collect(Collectors.joining(",")));
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
