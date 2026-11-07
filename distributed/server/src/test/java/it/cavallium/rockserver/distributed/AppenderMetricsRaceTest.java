package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.time.Duration;
import java.util.concurrent.CyclicBarrier;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.grpc.metrics.GrpcServerMetrics;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftGroupMemberId;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;

/**
 * R9-1: the leader makes a follower's appender again when Ratis' metric registry race leaves the appender's
 * {@link GrpcServerMetrics} without a registry ({@link CommitHeartbeats#retryingMissingRegistry}).
 */
@Timeout(120)
class AppenderMetricsRaceTest {

	private static final Duration WAIT = Duration.ofSeconds(60);

	/**
	 * Pins the upstream race on the server side, as {@code RatisContractTest} does for the client proxies: each
	 * {@code GrpcLogAppender} makes a {@link GrpcServerMetrics} named after its division's member id, shared by every
	 * appender of that division. One made while another closes can get no registry, and then its constructor throws
	 * a {@link NullPointerException}, which {@link CommitHeartbeats#fromMetrics} recognizes; made again, it works. A
	 * Ratis that fixes the race fails here after {@link #WAIT}, and the retry can go.
	 */
	@Test
	void anAppenderMetricsMadeWhileAnotherClosesCanFailAndWorksWhenMadeAgain() throws Exception {
		var memberId = RaftGroupMemberId.valueOf(RaftPeerId.valueOf("n0"), RaftGroupId.randomId()).toString();
		var pool = Executors.newFixedThreadPool(2);
		NullPointerException failure = null;
		int generations = 0;
		try {
			var previous = new GrpcServerMetrics(memberId);
			long deadline = System.nanoTime() + WAIT.toNanos();
			while (failure == null) {
				int made = generations;
				assertTrue(System.nanoTime() - deadline < 0, () -> "no failed appender metrics in " + made + " generations");
				var closing = previous;
				var start = new CyclicBarrier(2);
				var closed = pool.submit(() -> {
					start.await();
					closing.unregister();
					return null;
				});
				var next = pool.submit(() -> {
					start.await();
					return new GrpcServerMetrics(memberId);
				});
				closed.get();
				try {
					previous = next.get();
				} catch (java.util.concurrent.ExecutionException ex) {
					if (!(ex.getCause() instanceof NullPointerException npe)) {
						throw ex;
					}
					failure = npe;
					previous = null;
				}
				generations++;
			}
		} finally {
			pool.shutdownNow();
		}
		System.out.println("Appender metrics without a registry after " + generations + " generations");
		assertTrue(CommitHeartbeats.fromMetrics(failure), "recognized as the metrics race");
		var made = CommitHeartbeats.retryingMissingRegistry("n1", () -> new GrpcServerMetrics(memberId));
		assertNotNull(made.getRegistry());
		made.unregister();
	}

	private static NullPointerException fromMetricsFrame() {
		var npe = new NullPointerException("Cannot invoke \"RatisMetricRegistry.counter(String)\" because the return value"
				+ " of \"getRegistry()\" is null");
		npe.setStackTrace(new StackTraceElement[] {
				new StackTraceElement("org.apache.ratis.grpc.metrics.GrpcServerMetrics", "<init>", "GrpcServerMetrics.java", 70),
				new StackTraceElement("org.apache.ratis.grpc.server.GrpcLogAppender", "<init>", "GrpcLogAppender.java", 200),
		});
		return npe;
	}

	@Test
	void anAppenderWhoseMetricsGotNoRegistryIsMadeAgain() {
		var calls = new AtomicInteger();
		var made = CommitHeartbeats.retryingMissingRegistry("n1", () -> {
			if (calls.incrementAndGet() < 3) {
				throw fromMetricsFrame();
			}
			return "appender";
		});
		assertEquals("appender", made);
		assertEquals(3, calls.get());
	}

	@Test
	void anotherNullPointerExceptionIsNotRetried() {
		var calls = new AtomicInteger();
		var other = new NullPointerException("elsewhere");
		other.setStackTrace(new StackTraceElement[] {
				new StackTraceElement("org.apache.ratis.grpc.server.GrpcLogAppender", "<init>", "GrpcLogAppender.java", 190),
				new StackTraceElement("org.apache.ratis.metrics.Whatever", "after", "Whatever.java", 1),
		});
		assertFalse(CommitHeartbeats.fromMetrics(other));
		var thrown = assertThrows(NullPointerException.class, () -> CommitHeartbeats.retryingMissingRegistry("n1", () -> {
			calls.incrementAndGet();
			throw other;
		}));
		assertSame(other, thrown);
		assertEquals(1, calls.get());
	}

	@Test
	void aRaceThatGoesOnFailsAfterTheLastAttempt() {
		var calls = new AtomicInteger();
		assertThrows(NullPointerException.class, () -> CommitHeartbeats.retryingMissingRegistry("n1", () -> {
			calls.incrementAndGet();
			throw fromMetricsFrame();
		}));
		assertEquals(CommitHeartbeats.APPENDER_ATTEMPTS, calls.get());
	}
}
