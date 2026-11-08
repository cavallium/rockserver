package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.rpc.RpcType;
import org.junit.jupiter.api.Test;

/**
 * CR2-3, R6-1: a follower read asks for each commit index once; the appenders' heartbeats are pinned against real
 * servers in {@code RatisContractTest.aCommitHeartbeatSendsTheCommitIndexAtOnce}.
 */
class CommitHeartbeatsTest {

	private final AtomicLong commit = new AtomicLong(5);
	private final CommitHeartbeats heartbeats = new CommitHeartbeats(commit::get);

	@Test
	void eachCommitIndexIsAskedForOnce() {
		heartbeats.send();
		heartbeats.send();
		heartbeats.send();
		assertEquals(5, heartbeats.wanted());
		assertEquals(1, heartbeats.sent(), "the same commit index: asked for once");
		commit.set(6);
		heartbeats.send();
		assertEquals(6, heartbeats.wanted(), "a higher commit index needs a heartbeat of its own");
		assertEquals(2, heartbeats.sent());
		assertEquals(2, heartbeats.skipped());
	}

	@Test
	void nothingBeforeACommitIndexOrWhenDisabled() {
		commit.set(-1);
		heartbeats.send();
		commit.set(9);
		heartbeats.enabled = false;
		heartbeats.send();
		assertEquals(-1, heartbeats.wanted());
		assertEquals(0, heartbeats.sent());
	}

	/** Ratis finds the transport by its class name, and builds it with or without the node's instance. */
	@Test
	void ratisFindsTheTransportByName() {
		var type = RpcType.valueOf(new CommitHeartbeats.Rpc().name());
		assertInstanceOf(CommitHeartbeats.Rpc.class, type);
		assertInstanceOf(GrpcFactory.class, type.newFactory(new Parameters()));
		var parameters = new Parameters();
		parameters.put(CommitHeartbeats.PARAMETER, heartbeats, CommitHeartbeats.class);
		assertInstanceOf(CommitHeartbeats.Factory.class, type.newFactory(parameters));
	}

	/**
	 * R7-1: a wake given while the appender is not waiting is kept for its next wait. With Ratis' {@code AwaitForSignal}
	 * (3.3.1) the first wait below parks for its whole timeout: {@code signal()} sets the flag it replaces.
	 */
	@Test
	void aWakeGivenBeforeTheWaitIsNotLost() throws Exception {
		var signal = new CommitHeartbeats.WakeSignal("test", () -> true);
		signal.signal();
		long start = System.nanoTime();
		assertTrue(signal.await(5, TimeUnit.SECONDS), "the wait takes the earlier wake");
		assertTrue(System.nanoTime() - start < TimeUnit.SECONDS.toNanos(1), "and returns at once");
		assertFalse(signal.await(20, TimeUnit.MILLISECONDS), "one wake, one wait");
		signal.signal();
		signal.signal();
		assertTrue(signal.await(0, TimeUnit.MILLISECONDS), "a wait of 0 takes a wake too");
		assertFalse(signal.await(20, TimeUnit.MILLISECONDS), "two wakes before a wait count once");

		// A wake given while it waits still ends the wait.
		var waiting = new CountDownLatch(1);
		var result = new boolean[1];
		var waiter = Thread.ofPlatform().start(() -> {
			waiting.countDown();
			try {
				result[0] = signal.await(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		waiting.await();
		Thread.sleep(50);
		signal.signal();
		waiter.join(TimeUnit.SECONDS.toMillis(5));
		assertFalse(waiter.isAlive());
		assertTrue(result[0]);
	}

	/**
	 * R7-1 follow-up: a wake given while the appender is not waiting is kept only when it would make the appender send a
	 * commit heartbeat. Any other early wake is dropped as Ratis drops it, so that the wakes of every commit and reply do
	 * not each buy the appender another pass, and at its limit of appends in flight another empty heartbeat. A wake
	 * given while it waits still wakes it. Before this, every early wake was kept, and the first wait below returned.
	 */
	@Test
	void anEarlyWakeIsKeptOnlyWhenACommitHeartbeatIsWanted() throws Exception {
		var wanted = new java.util.concurrent.atomic.AtomicBoolean(false);
		var signal = new CommitHeartbeats.WakeSignal("test", wanted::get);
		signal.signal();
		signal.signal();
		assertFalse(signal.await(20, TimeUnit.MILLISECONDS), "an early wake that wants no commit heartbeat is dropped");
		assertEquals(0, signal.kept());
		wanted.set(true);
		signal.signal();
		assertTrue(signal.await(0, TimeUnit.MILLISECONDS), "an early wake that wants a commit heartbeat is kept");
		assertEquals(1, signal.kept());
		wanted.set(false);

		var waiting = new CountDownLatch(1);
		var result = new boolean[1];
		var waiter = Thread.ofPlatform().start(() -> {
			waiting.countDown();
			try {
				result[0] = signal.await(10, TimeUnit.SECONDS);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			}
		});
		waiting.await();
		// Not a fixed sleep (R26-5): a waiter descheduled past it would take the wake below as an early one, which this
		// condition drops, and park for 10 s.
		awaitWaiting(signal);
		signal.signal();
		waiter.join(TimeUnit.SECONDS.toMillis(5));
		assertFalse(waiter.isAlive());
		assertTrue(result[0], "a wake given while waiting wakes, whatever the condition says");
		assertEquals(1, signal.kept(), "that wake was not an early one");
		assertEquals(3, signal.awaits());
	}

	/** Until a thread is inside {@code signal}'s wait, holding its count. */
	private static void awaitWaiting(CommitHeartbeats.WakeSignal signal) throws InterruptedException {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		while (signal.waiting() == 0) {
			if (System.nanoTime() > deadline) {
				throw new AssertionError("the waiter never waited");
			}
			Thread.sleep(1);
		}
	}

	/**
	 * R7-2: resends after broken streams do not reopen the window of the first send, and there are at most
	 * {@link CommitHeartbeats.CommitWindow#MAX_RESENDS} per commit index; a higher commit index starts over.
	 */
	@Test
	void resendsAfterBrokenStreamsAreBoundedPerCommitIndex() {
		var window = new CommitHeartbeats.CommitWindow();
		long ms = TimeUnit.MILLISECONDS.toNanos(1);
		long windowNanos = 100 * ms;
		assertFalse(window.streamBroke(0, windowNanos), "nothing sent, nothing to send again");
		window.sent(7, 0);
		assertTrue(window.open(99 * ms, windowNanos));
		int resends = 0;
		// The stream breaks every 10 ms and the heartbeat goes out again on each new stream.
		for (long t = 10 * ms; t < 1000 * ms; t += 10 * ms) {
			if (window.streamBroke(t, windowNanos)) {
				resends++;
				assertEquals(-1, window.sentCommit(), "a heartbeat lost with its stream counts as unsent");
				window.sent(7, t);
			}
		}
		assertEquals(CommitHeartbeats.CommitWindow.MAX_RESENDS, resends);
		assertFalse(window.open(100 * ms, windowNanos), "resends did not move the window of the first send");
		assertEquals(7, window.sentCommit());

		// Only within the window, also below the limit.
		window.sent(8, 2000 * ms);
		assertTrue(window.streamBroke(2010 * ms, windowNanos));
		window.sent(8, 2010 * ms);
		assertFalse(window.streamBroke(2100 * ms, windowNanos), "the window of commit index 8 closed");

		// A request carrying commit index 0 (Ratis' heartbeat with an append in flight) changes nothing.
		window.sent(0, 3000 * ms);
		assertEquals(8, window.sentCommit());
		assertFalse(window.open(3000 * ms, windowNanos));
	}
}
