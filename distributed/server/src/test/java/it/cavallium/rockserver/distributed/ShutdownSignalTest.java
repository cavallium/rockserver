package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
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
 * returns only once the failed node stopped, and then gives the failure's exit status, 3. The process version (a real
 * SIGTERM) is still to run.
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
