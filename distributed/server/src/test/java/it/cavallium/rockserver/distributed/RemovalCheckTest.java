package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.distributed.RemovalRule.Answer;
import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import it.cavallium.rockserver.distributed.RemovalRule.Own;
import it.cavallium.rockserver.distributed.RemovalRule.Probe;
import it.cavallium.rockserver.distributed.RemovalRule.Reason;
import it.cavallium.rockserver.distributed.RemovalRule.Report;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.grpc.server.GrpcServices;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.thirdparty.io.grpc.Server;
import org.apache.ratis.thirdparty.io.grpc.ServerServiceDefinition;
import org.apache.ratis.thirdparty.io.grpc.netty.NettyServerBuilder;
import org.apache.ratis.thirdparty.io.grpc.stub.ServerCalls;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;

/** R11: one membership check (the rounds of {@link RemovalCheck}) and the wire of TermService's Configuration methods. */
class RemovalCheckTest {

	private static final RaftGroupId GROUP = RaftGroupId.valueOf(UUID.randomUUID());
	private final List<Server> servers = new ArrayList<>();

	@AfterEach
	void tearDown() {
		servers.forEach(Server::shutdownNow);
	}

	private static Conf stable(long term, long index, String... ids) {
		return new Conf(term, index, List.of(ids), List.of());
	}

	private static Report report(Conf committed, Conf latest) {
		return new Report(2, true, true, 3, committed.index(), committed, latest, Probe.UNKNOWN, 0, -1, Probe.UNKNOWN,
				Map.of());
	}

	private static Own own(Conf conf) {
		return new Own("x", conf, conf, index -> index <= conf.index() ? conf.term() : RemovalRule.BEYOND, true, false);
	}

	private static int freePort() throws Exception {
		try (var socket = new ServerSocket(0)) {
			return socket.getLocalPort();
		}
	}

	/** Members that cannot be reached do not count, and do not hold the start up for the whole budget. */
	@Test
	void unreachableMembersLeaveItUndecidedAtOnce() throws Exception {
		int port = freePort();
		long start = System.nanoTime();
		var own = stable(1, 5, "x", "a", "b");
		var verdict = RemovalCheck.check(() -> new Own("x", null, own, i -> 1, true, false),
				Map.of("a", "127.0.0.1:" + port, "b", "127.0.0.1:" + port), GROUP, Duration.ofSeconds(20));
		assertEquals(Reason.NO_COMMITTED_CONFIGURATION, verdict.reason());
		assertEquals(List.of("a", "b"), verdict.unreachable());
		assertTrue(System.nanoTime() - start < Duration.ofSeconds(10).toNanos());
	}

	/** A member that is still starting is asked again, and K*'s members join the round. */
	@Test
	void pendingMembersAndNewMembersOfKStarAreAskedAgain() {
		var own = stable(1, 5, "x", "a", "b");
		var removal = stable(3, 9, "a", "b", "c");
		var asked = new ConcurrentHashMap<String, AtomicInteger>();
		RemovalCheck.Asker asker = (id, _, probes, _) -> {
			int round = asked.computeIfAbsent(id, _ -> new AtomicInteger()).incrementAndGet();
			if (id.equals("b") && round < 3) {
				return CompletableFuture.completedFuture(Answer.PENDING);
			}
			if (id.equals("c")) {
				// Asked with the probe of K*.
				assertEquals(TermIndex.valueOf(3, 9), probes.get(1));
			}
			return CompletableFuture.completedFuture(Answer.of(report(removal, removal)));
		};
		var verdict = RemovalCheck.check(() -> own(own), Map.of("a", "a:1", "b", "b:1", "c", "c:1"),
				Duration.ofSeconds(20), asker);
		assertEquals(Reason.REMOVED, verdict.reason());
		assertTrue(asked.containsKey("c"), "c, a member of K* only, was asked");
	}

	/** A member that blocks is asked until it stops blocking, within the budget. */
	@Test
	void aBlockerIsAskedUntilTheBudgetEnds() {
		var own = stable(1, 5, "x", "a", "b");
		var removal = stable(3, 9, "a", "b");
		var later = stable(3, 12, "x", "a", "b");
		RemovalCheck.Asker asker = (id, _, _, _) -> CompletableFuture.completedFuture(
				Answer.of(id.equals("a") ? report(removal, later) : report(removal, removal)));
		long start = System.nanoTime();
		var verdict = RemovalCheck.check(() -> own(own), Map.of("a", "a:1", "b", "b:1"), Duration.ofMillis(600), asker);
		assertEquals(Reason.MEMBER_LATER_CONFIGURATION, verdict.reason());
		assertTrue(System.nanoTime() - start >= Duration.ofMillis(400).toNanos());
	}

	/** Version 2 through the real handler and client: every field comes back, and the probes are answered. */
	@Test
	void aConfiguration2AnswerTravelsWhole() throws Exception {
		var committed = new Conf(3, 9, List.of("a", "b"), List.of("x", "a", "b"));
		var latest = new Conf(RemovalRule.UNKNOWN_TERM, 12, List.of("a", "b"), List.of());
		var seen = new ArrayList<List<TermIndex>>();
		TermService.ConfigurationSource source = (group, probes) -> {
			seen.add(probes);
			return group.equals(GROUP) ? new Report(2, true, false, 7, 11, committed, latest, Probe.COMMITTED,
					probes.get(1).getTerm(), probes.get(1).getIndex(), Probe.ABSENT, Map.of("a", "127.0.0.1:1")) : null;
		};
		var server = start(TermService.customizer(() -> null, new HostAndPort("127.0.0.1", 1), (_, _) -> null,
				_ -> CompletableFuture.failedFuture(new IllegalStateException()), null,
				(_, _) -> CompletableFuture.failedFuture(new IllegalStateException()), null, source));
		var channel = PeerChannels.open("127.0.0.1:" + server.getPort());
		try {
			var probes = List.of(TermIndex.valueOf(1, 5), TermIndex.valueOf(3, 9));
			var report = TermService.configuration2(channel, GROUP, probes, Duration.ofSeconds(10)).get();
			assertEquals(probes, seen.getFirst());
			assertTrue(report.running());
			assertEquals(7, report.currentTerm());
			assertEquals(11, report.commitIndex());
			assertEquals(committed, report.committed());
			assertEquals(latest, report.latest());
			assertEquals(Probe.COMMITTED, report.ownProbe());
			assertEquals(Probe.ABSENT, report.probeOf(stable(3, 9, "a", "b")));
			assertEquals(Map.of("a", "127.0.0.1:1"), report.addresses());
			assertNull(TermService.configuration2(channel, RaftGroupId.valueOf(UUID.randomUUID()), probes,
					Duration.ofSeconds(10)).get(), "no such group");
			// Through the asker of a check.
			var answer = RemovalCheck.ask(channel, GROUP, "a", probes, Duration.ofSeconds(10),
					ConcurrentHashMap.newKeySet()).get();
			assertEquals(Answer.Kind.REPORT, answer.kind());
		} finally {
			channel.shutdownNow();
		}
	}

	/** A member that predates Configuration2 is asked Configuration (version 1); one that predates both is no answer. */
	@Test
	void olderMembersAreAskedTheOlderMethodOrCountAsNoAnswer() throws Exception {
		ServerCalls.UnaryMethod<byte[], byte[]> v1 = (_, response) -> {
			var members = "a\nb\nc".getBytes(StandardCharsets.UTF_8);
			response.onNext(ByteBuffer.allocate(Long.BYTES + 1 + members.length).putLong(9).put((byte) 1).put(members)
					.array());
			response.onCompleted();
		};
		var onlyV1 = start((builder, _) -> builder.addService(ServerServiceDefinition.builder(TermService.SERVICE_NAME)
				.addMethod(TermService.CONFIGURATION, ServerCalls.asyncUnaryCall(v1)).build()));
		ServerCalls.UnaryMethod<byte[], byte[]> address = (_, response) -> {
			response.onNext(new byte[0]);
			response.onCompleted();
		};
		var neither = start((builder, _) -> builder.addService(ServerServiceDefinition.builder(TermService.SERVICE_NAME)
				.addMethod(TermService.API_ADDRESS, ServerCalls.asyncUnaryCall(address)).build()));
		var probes = List.of(TermIndex.valueOf(1, 5), TermIndex.valueOf(1, 5));
		var versionOne = ConcurrentHashMap.<String>newKeySet();
		var channel = PeerChannels.open("127.0.0.1:" + onlyV1.getPort());
		var channel2 = PeerChannels.open("127.0.0.1:" + neither.getPort());
		try {
			var answer = RemovalCheck.ask(channel, GROUP, "a", probes, Duration.ofSeconds(10), versionOne).get();
			assertEquals(Answer.Kind.REPORT, answer.kind());
			assertEquals(1, answer.report().version());
			assertEquals(9, answer.report().committed().index());
			assertEquals(List.of("a", "b", "c"), answer.report().committed().newHalf());
			assertTrue(versionOne.contains("a"), "remembered for the next round");
			assertEquals(Answer.Kind.NONE, RemovalCheck.ask(channel2, GROUP, "b", probes, Duration.ofSeconds(10),
					versionOne).get().kind());
		} finally {
			channel.shutdownNow();
			channel2.shutdownNow();
		}
	}

	private Server start(GrpcServices.Customizer customizer) throws Exception {
		var builder = NettyServerBuilder.forAddress(new InetSocketAddress("127.0.0.1", 0));
		var server = customizer.customize(builder, EnumSet.of(GrpcServices.Type.SERVER)).build().start();
		servers.add(server);
		return server;
	}
}
