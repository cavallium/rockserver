package it.cavallium.rockserver.distributed;

import org.junit.jupiter.api.Tag;
import static org.junit.jupiter.api.Assertions.assertEquals;

import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.time.Duration;
import java.util.EnumSet;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/**
 * {@link ClusterCheck} against followers that serve {@link TermService} on a real gRPC server: each follower is told
 * once per term, a follower that does not answer is asked again only after the retry pause, and a new term asks
 * every follower again.
 */
@Tag("slow")
class ClusterCheckTest {

	private static final RaftGroupId GROUP = RaftGroupId.valueOf(UUID.randomUUID());
	private static final long SECOND = TimeUnit.SECONDS.toNanos(1);
	/** Longer than the retry pause of the check (5 s). */
	private static final long AFTER_RETRY = 6 * SECOND;

	/** One follower's answers and how many times it was asked. */
	private static final class Follower {
		final AtomicInteger asked = new AtomicInteger();
		volatile boolean answering = true;
		volatile String clusterId = "cluster-a";
		Server server;

		String address() {
			return "127.0.0.1:" + server.getPort();
		}
	}

	private final Map<String, Follower> followers = new ConcurrentHashMap<>();

	@AfterEach
	void tearDown() throws Exception {
		for (var follower : followers.values()) {
			follower.server.shutdownNow().awaitTermination(5, TimeUnit.SECONDS);
		}
	}

	private Follower follower(String id) throws Exception {
		var follower = new Follower();
		TermService.NodeStateSource source = (_, _) -> {
			follower.asked.incrementAndGet();
			if (!follower.answering) {
				throw new IllegalStateException("installing a snapshot");
			}
			return new NodeState(10, false, true, false, follower.clusterId);
		};
		var builder = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0));
		follower.server = TermService.customizer(() -> null, new HostAndPort("127.0.0.1", 1), source)
				.customize(builder, EnumSet.of(GrpcServices.Type.SERVER))
				.build()
				.start();
		followers.put(id, follower);
		return follower;
	}

	/** Waits until {@code follower} was asked {@code times} times, then a moment for the answer to be handled. */
	private static void awaitAsked(Follower follower, int times) throws InterruptedException {
		InProcessCluster.await(Duration.ofSeconds(10), () -> follower.asked.get() >= times);
		Thread.sleep(200);
		assertEquals(times, follower.asked.get());
	}

	@Test
	void eachFollowerIsToldOncePerTermAndAgainInANewTerm() throws Exception {
		var a = follower("a");
		var b = follower("b");
		var check = new ClusterCheck("leader", GROUP, Duration.ofSeconds(2));
		var addresses = Map.of("a", a.address(), "b", b.address());
		long now = 0;
		check.check(3, "cluster-a", addresses, now);
		awaitAsked(a, 1);
		awaitAsked(b, 1);
		// Checked: not asked again in this term, however late.
		check.check(3, "cluster-a", addresses, now + AFTER_RETRY);
		check.check(3, "cluster-a", addresses, now + 10 * AFTER_RETRY);
		Thread.sleep(300);
		assertEquals(1, a.asked.get());
		assertEquals(1, b.asked.get());
		// A new term asks everyone again, at once.
		check.check(4, "cluster-a", addresses, now + 10 * AFTER_RETRY + 1);
		awaitAsked(a, 2);
		awaitAsked(b, 2);
	}

	@Test
	void aFollowerThatDoesNotAnswerIsAskedAgainOnlyAfterThePause() throws Exception {
		var a = follower("a");
		a.answering = false;
		var check = new ClusterCheck("leader", GROUP, Duration.ofSeconds(2));
		var addresses = Map.of("a", a.address());
		check.check(3, "cluster-a", addresses, 0);
		awaitAsked(a, 1);
		check.check(3, "cluster-a", addresses, SECOND);
		check.check(3, "cluster-a", addresses, 4 * SECOND);
		Thread.sleep(300);
		assertEquals(1, a.asked.get(), "asked again within the pause");
		check.check(3, "cluster-a", addresses, AFTER_RETRY);
		awaitAsked(a, 2);
		// Answering now: checked, and not asked again in this term.
		a.answering = true;
		check.check(3, "cluster-a", addresses, 2 * AFTER_RETRY);
		awaitAsked(a, 3);
		check.check(3, "cluster-a", addresses, 4 * AFTER_RETRY);
		Thread.sleep(300);
		assertEquals(3, a.asked.get());
	}

	/** A follower of another cluster stops on its own; the leader keeps telling it, at the retry pace. */
	@Test
	void aFollowerOfAnotherClusterIsNotMarkedChecked() throws Exception {
		var a = follower("a");
		a.clusterId = "cluster-b";
		var check = new ClusterCheck("leader", GROUP, Duration.ofSeconds(2));
		var addresses = Map.of("a", a.address());
		check.check(3, "cluster-a", addresses, 0);
		awaitAsked(a, 1);
		check.check(3, "cluster-a", addresses, AFTER_RETRY);
		awaitAsked(a, 2);
	}

	/** An unreachable follower costs one attempt per pause, not one per call. */
	@Test
	void anUnreachableFollowerIsTriedOncePerPause() throws Exception {
		var a = follower("a");
		var check = new ClusterCheck("leader", GROUP, Duration.ofMillis(500));
		// A port nothing listens on: the call fails; then the follower comes up at a reachable address.
		int closedPort;
		try (var socket = new ServerSocket(0)) {
			closedPort = socket.getLocalPort();
		}
		check.check(3, "cluster-a", Map.of("a", "127.0.0.1:" + closedPort), 0);
		Thread.sleep(700);
		check.check(3, "cluster-a", Map.of("a", a.address()), SECOND);
		Thread.sleep(300);
		assertEquals(0, a.asked.get(), "asked within the pause after the failure");
		check.check(3, "cluster-a", Map.of("a", a.address()), AFTER_RETRY);
		awaitAsked(a, 1);
	}
}
