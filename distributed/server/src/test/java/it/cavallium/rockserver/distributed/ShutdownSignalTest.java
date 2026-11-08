package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.io.UncheckedIOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

/**
 * DD-7h: a SIGTERM that races a replica failure. The shutdown hook's stop ({@link ReplicatedServerMain#closeOnSignal})
 * returns only once the failed node stopped, and then gives the failure's exit status, 3. Also a signal during the start
 * of {@code run}, and an error thrown by that start (R28-4, R28-5). The process version (a real SIGTERM) is still to
 * run.
 */
class ShutdownSignalTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void aSignalDuringAReplicaFailureWaitsForTheStopAndExitsWithThree() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var failing = cluster.anyFollower();
		var api = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));
		long column = api.createColumn("c", SCHEMA);
		cluster.awaitConverged(TIMEOUT);
		failing.stateMachine().testHooks().applyFailure = new IllegalStateException("Injected apply failure");
		api.put(0, column, new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(1).array())),
				Buf.wrap(new byte[] {1}), RequestType.none());
		long deadline = System.nanoTime() + TIMEOUT.toNanos();
		while (!failing.isFailed()) {
			assertTrue(System.nanoTime() < deadline, "the replica did not fail");
			Thread.onSpinWait();
		}
		// The failure's own stop is running, or about to: the signal's stop must wait for it either way.
		assertEquals(3, ReplicatedServerMain.closeOnSignal(failing));
		assertTrue(failing.stopped().isDone(), "the hook halts only once the node stopped");
	}

	/**
	 * Ratis closes the division by itself (as its log worker does after a disk error) and the signal lands before
	 * {@code watchRaft} noticed: the signal's stop is the failure's stop, and gives 3.
	 */
	@Test
	void aSignalRightAfterRaftClosedByItselfExitsWithThree() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var failing = cluster.anyFollower();
		failing.stateMachine().division().close();
		assertEquals(3, ReplicatedServerMain.closeOnSignal(failing));
		assertTrue(failing.stopped().isDone(), "the hook halts only once the node stopped");
	}

	/**
	 * R6-2: a running node that stops with a failure that has its own exit status (4 removed, 5 newer cluster version).
	 * The main thread exits with it, and its {@code System.exit} runs the shutdown hook, which halts: the halt must give the
	 * same status, or {@code RestartPreventExitStatus=4 5} never sees it. The test runs {@link ReplicatedServerMain#serve}
	 * with the exit, the hook registration and the halt replaced, and then runs the hook as {@code System.exit} would.
	 */
	@ParameterizedTest
	@ValueSource(ints = {4, 5})
	void aFailureWithItsOwnStatusExitsWithItAlsoThroughTheShutdownHook(int expected) throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var failing = cluster.anyFollower();
		var api = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));
		long column = api.createColumn("c", SCHEMA);
		cluster.awaitConverged(TIMEOUT);

		var previousExiter = ReplicatedServerMain.exiter;
		var previousHalter = ReplicatedServerMain.halter;
		var previousHooks = ReplicatedServerMain.shutdownHooks;
		var exited = new CompletableFuture<Integer>();
		var halted = new AtomicReference<Integer>();
		var hook = new AtomicReference<Thread>();
		ReplicatedServerMain.exiter = code -> {
			exited.complete(code);
			throw new ExitCalled();
		};
		ReplicatedServerMain.halter = code -> {
			halted.set(code);
			throw new ExitCalled();
		};
		ReplicatedServerMain.shutdownHooks = hook::set;
		try {
			var main = Thread.ofPlatform().name("test-serve").start(() -> {
				try {
					ReplicatedServerMain.serve(failing, null);
				} catch (ExitCalled expectedExit) {
					// The exit returned to the test instead.
				}
			});
			var failure = expected == 4
					? new ReplicatedNode.RemovedException("Injected: removed while waiting outside the cluster")
					: ReplicatedNode.NewerVersionException.of(Long.MIN_VALUE, Long.MIN_VALUE);
			failing.stateMachine().testHooks().applyFailure = new UncheckedIOException(failure);
			api.put(0, column, new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(1).array())),
					Buf.wrap(new byte[] {1}), RequestType.none());

			assertEquals(expected, exited.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS), "the main thread's exit status");
			main.join(TIMEOUT.toMillis());
			// System.exit runs the shutdown hooks: the hook stops the node again, and halts.
			assertNotNull(hook.get(), "the hook was registered");
			try {
				hook.get().run();
			} catch (ExitCalled expectedHalt) {
				// The halt returned to the test instead.
			}
			assertEquals(expected, halted.get(), "the shutdown hook's halt status");
		} finally {
			ReplicatedServerMain.exiter = previousExiter;
			ReplicatedServerMain.halter = previousHalter;
			ReplicatedServerMain.shutdownHooks = previousHooks;
		}
	}

	/**
	 * R28-4: a SIGTERM while {@code run} is still starting its node (opening RocksDB, loading the Raft log, checking its
	 * membership) is not lost. The shutdown hook is registered before the start: it waits for the start to end, then
	 * stops the node it returned, as for a running node. Before, the hook was registered only after the start, so the
	 * JVM exited with RocksDB and Ratis open, whose destructors can hang it until systemd's SIGKILL.
	 */
	@Test
	void aSignalDuringTheStartWaitsForItAndStopsTheNode() throws Exception {
		cluster = new InProcessCluster(temp, 1, c -> c).startAll();
		var node = cluster.awaitLeader(TIMEOUT);
		var previousExiter = ReplicatedServerMain.exiter;
		var previousHalter = ReplicatedServerMain.halter;
		var previousHooks = ReplicatedServerMain.shutdownHooks;
		var previousStarter = ReplicatedServerMain.starter;
		var exited = new AtomicReference<Integer>();
		var halted = new AtomicReference<Integer>();
		var hook = new AtomicReference<Thread>();
		var starting = new CompletableFuture<Void>();
		var release = new CompletableFuture<Void>();
		ReplicatedServerMain.exiter = code -> {
			exited.set(code);
			throw new ExitCalled();
		};
		ReplicatedServerMain.halter = code -> {
			halted.set(code);
			throw new ExitCalled();
		};
		ReplicatedServerMain.shutdownHooks = hook::set;
		ReplicatedServerMain.starter = config -> {
			starting.complete(null);
			release.join();
			return node;
		};
		try {
			var main = Thread.ofPlatform().daemon(true).name("test-run").start(() -> {
				try {
					ReplicatedServerMain.startAndServe(node.config(), null);
				} catch (ExitCalled unexpected) {
					// Recorded in exited.
				}
			});
			starting.get(TIMEOUT.toMillis(), TimeUnit.MILLISECONDS);
			assertNotNull(hook.get(), "the hook is registered before the node starts");
			// The signal: the JVM runs the hook while the start goes on.
			var signal = Thread.ofPlatform().daemon(true).name("test-signal").start(() -> {
				try {
					hook.get().run();
				} catch (ExitCalled unexpected) {
					// Recorded in halted.
				}
			});
			signal.join(300);
			assertTrue(signal.isAlive(), "the hook waits for the start");
			release.complete(null);
			signal.join(TIMEOUT.toMillis());
			assertFalse(signal.isAlive(), "the hook ends once it stopped the started node");
			assertTrue(node.stopped().isDone(), "the hook stopped the node that the start returned");
			main.join(TIMEOUT.toMillis());
			assertFalse(main.isAlive(), "run ends once its node stopped");
			assertNull(exited.get(), "a stop by the signal is not a failure");
			assertNull(halted.get(), "a stop by the signal is not a failure");
		} finally {
			release.complete(null);
			ReplicatedServerMain.exiter = previousExiter;
			ReplicatedServerMain.halter = previousHalter;
			ReplicatedServerMain.shutdownHooks = previousHooks;
			ReplicatedServerMain.starter = previousStarter;
		}
	}

	/**
	 * R28-5: an {@link Error} thrown by the start (a class that does not load from a half-replaced jar, for one) ends
	 * {@code run} with exit code 1, as an exception does: threads that the start left behind would otherwise keep the JVM
	 * running without a node, which a supervisor does not restart. A hook registered for the start ends at once.
	 */
	@Test
	void anErrorDuringTheStartExitsWithOne() throws Exception {
		var previousExiter = ReplicatedServerMain.exiter;
		var previousHalter = ReplicatedServerMain.halter;
		var previousHooks = ReplicatedServerMain.shutdownHooks;
		var previousStarter = ReplicatedServerMain.starter;
		var exited = new AtomicReference<Integer>();
		var halted = new AtomicReference<Integer>();
		var hook = new AtomicReference<Thread>();
		ReplicatedServerMain.exiter = code -> {
			exited.set(code);
			throw new ExitCalled();
		};
		ReplicatedServerMain.halter = code -> {
			halted.set(code);
			throw new ExitCalled();
		};
		ReplicatedServerMain.shutdownHooks = hook::set;
		ReplicatedServerMain.starter = _ -> {
			throw new NoClassDefFoundError("Injected: a class of a half-replaced jar");
		};
		try {
			var config = NodeConfig.defaults("n0", List.of(PeerConfig.parse("n0=127.0.0.1:1/127.0.0.1:2")), temp);
			var thrown = new AtomicReference<Throwable>();
			try {
				ReplicatedServerMain.startAndServe(config, null);
			} catch (ExitCalled expectedExit) {
				// The exit returned to the test instead.
			} catch (Throwable unexpected) {
				thrown.set(unexpected);
			}
			assertNull(thrown.get(), () -> "the start's error escaped run: " + thrown.get());
			assertEquals(1, exited.get(), "the exit status");
			if (hook.get() != null) {
				// System.exit runs the hook: with no node started it has nothing to stop.
				var signal = Thread.ofPlatform().daemon(true).name("test-exit-hook").start(hook.get());
				signal.join(TIMEOUT.toMillis());
				assertFalse(signal.isAlive(), "the hook does not wait for a start that failed");
				assertNull(halted.get());
			}
		} finally {
			ReplicatedServerMain.exiter = previousExiter;
			ReplicatedServerMain.halter = previousHalter;
			ReplicatedServerMain.shutdownHooks = previousHooks;
			ReplicatedServerMain.starter = previousStarter;
		}
	}

	@Test
	void theExitStatusOfAFailureLooksThroughItsCauses() {
		assertEquals(3, ReplicatedServerMain.exitStatusOf(new IllegalStateException("apply failed")));
		assertEquals(4, ReplicatedServerMain.exitStatusOf(new ReplicatedNode.RemovedException("removed")));
		assertEquals(4, ReplicatedServerMain.exitStatusOf(
				new IllegalStateException(new ReplicatedNode.RemovedException("removed"))));
		assertEquals(5, ReplicatedServerMain.exitStatusOf(ReplicatedNode.NewerVersionException.of(Long.MIN_VALUE, Long.MIN_VALUE)));
		assertEquals(5, ReplicatedServerMain.exitStatusOf(
				new UncheckedIOException(ReplicatedNode.NewerVersionException.of(Long.MIN_VALUE, Long.MIN_VALUE))));
	}

	private static final class ExitCalled extends RuntimeException {
		ExitCalled() {
			super(null, null, false, false);
		}
	}
}
