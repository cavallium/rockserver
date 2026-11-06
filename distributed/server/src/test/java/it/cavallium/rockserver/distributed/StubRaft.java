package it.cavallium.rockserver.distributed;

import java.lang.reflect.InvocationHandler;
import java.lang.reflect.Proxy;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.raftlog.RaftLog;
import org.jetbrains.annotations.Nullable;

/**
 * A Ratis server of one division that a test drives by hand, for components that only read who leads, the term and
 * the configuration, and submit client requests: {@link LeaderForwarder}, {@link Proposer}, {@link LocalCommands}.
 * Every other method throws. Install it into a {@link RockserverStateMachine} that was never started with
 * {@link #installInto}.
 */
final class StubRaft {

	volatile @Nullable String leaderId;
	volatile boolean leader;
	volatile boolean leaderReady;
	volatile long term = 1;
	volatile List<RaftPeer> currentPeers = List.of();
	volatile List<RaftPeer> previousPeers = List.of();
	/** The followers a leader replicates to besides the configuration, such as a peer being added (staging). */
	volatile List<RaftPeer> stagingFollowers = List.of();
	volatile long lastCommittedIndex;
	/** How many times {@code getRoleInfoProto} was read: once per {@link ReplicaOps#pollPeers} on this division. */
	final AtomicInteger roleInfoReads = new AtomicInteger();
	/** Answers {@code submitClientRequestAsync}. */
	volatile Function<RaftClientRequest, CompletableFuture<RaftClientReply>> submit =
			_ -> CompletableFuture.failedFuture(new UnsupportedOperationException("submit"));
	/**
	 * Called at each read of the leader id, after the value to answer was read: a change made here is seen by the next
	 * read only, as a change that Ratis makes right after the caller read it.
	 */
	volatile Runnable onLeaderIdRead = () -> {};

	final RaftServer server = proxy(RaftServer.class, (method, args) -> switch (method) {
		case "getDivision" -> division();
		case "submitClientRequestAsync" -> submit.apply((RaftClientRequest) args[0]);
		default -> unsupported(method);
	});

	private final RaftConfiguration conf = proxy(RaftConfiguration.class, (method, _) -> switch (method) {
		case "getCurrentPeers" -> currentPeers;
		case "getPreviousPeers" -> previousPeers;
		case "getAllPeers" -> {
			var all = new LinkedHashSet<>(currentPeers);
			all.addAll(previousPeers);
			yield List.copyOf(all);
		}
		default -> unsupported(method);
	});

	private final DivisionInfo info = proxy(DivisionInfo.class, (method, _) -> switch (method) {
		case "getLeaderId" -> {
			var id = leaderId;
			onLeaderIdRead.run();
			yield id == null ? null : RaftPeerId.valueOf(id);
		}
		case "isLeader" -> leader;
		case "isLeaderReady" -> leaderReady;
		case "getCurrentTerm" -> term;
		case "getRoleInfoProto" -> {
			roleInfoReads.incrementAndGet();
			var leaderInfo = RaftProtos.LeaderInfoProto.newBuilder();
			for (var follower : stagingFollowers) {
				leaderInfo.addFollowerInfo(RaftProtos.ServerRpcProto.newBuilder().setId(follower.getRaftPeerProto()));
			}
			yield RaftProtos.RoleInfoProto.newBuilder().setLeaderInfo(leaderInfo).build();
		}
		default -> unsupported(method);
	});

	private final RaftLog log = proxy(RaftLog.class, (method, _) -> switch (method) {
		case "getLastCommittedIndex" -> lastCommittedIndex;
		default -> unsupported(method);
	});

	private final RaftServer.Division division = proxy(RaftServer.Division.class, (method, _) -> switch (method) {
		case "getInfo" -> info;
		case "getRaftLog" -> log;
		case "getRaftConf" -> conf;
		case "getStateMachine" -> null;
		default -> unsupported(method);
	});

	RaftServer.Division division() {
		return division;
	}

	Supplier<RaftServer> serverSupplier() {
		return () -> server;
	}

	/** Makes {@code stateMachine.division()} answer this stub's division. */
	void installInto(RockserverStateMachine stateMachine) throws ReflectiveOperationException {
		var field = RockserverStateMachine.class.getDeclaredField("server");
		field.setAccessible(true);
		field.set(stateMachine, server);
	}

	@FunctionalInterface
	private interface Handler {
		Object handle(String method, Object[] args) throws Throwable;
	}

	private static Object unsupported(String method) {
		throw new UnsupportedOperationException("StubRaft does not answer " + method);
	}

	@SuppressWarnings("unchecked")
	private static <T> T proxy(Class<T> type, Handler handler) {
		InvocationHandler invocation = (self, method, args) -> switch (method.getName()) {
			case "equals" -> self == args[0];
			case "hashCode" -> System.identityHashCode(self);
			case "toString" -> "StubRaft." + type.getSimpleName();
			default -> handler.handle(method.getName(), args);
		};
		return (T) Proxy.newProxyInstance(StubRaft.class.getClassLoader(), new Class<?>[] {type}, invocation);
	}
}
