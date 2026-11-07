package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.function.Supplier;
import org.apache.ratis.grpc.GrpcUtil;
import org.apache.ratis.proto.RaftProtos.RaftPeerProto;
import org.apache.ratis.proto.RaftProtos.RaftPeerRole;
import org.apache.ratis.proto.RaftProtos.SetConfigurationRequestProto;
import org.apache.ratis.proto.grpc.AdminProtocolServiceGrpc;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.exceptions.NotLeaderException;
import org.apache.ratis.protocol.exceptions.ReconfigurationInProgressException;
import org.apache.ratis.protocol.exceptions.SetConfigurationException;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.thirdparty.io.grpc.ForwardingServerCallListener;
import org.apache.ratis.thirdparty.io.grpc.Metadata;
import org.apache.ratis.thirdparty.io.grpc.ServerCall;
import org.apache.ratis.thirdparty.io.grpc.ServerCallHandler;
import org.apache.ratis.thirdparty.io.grpc.ServerInterceptor;
import org.apache.ratis.util.ProtoUtils;
import org.jetbrains.annotations.Nullable;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The leader's guard on Ratis' {@code setConfiguration} (admin service, through gRPC): it refuses a change of more than
 * one voter ({@link VoterChange}), whoever sends it, an {@code admin} of an older version or the Ratis tools included.
 * Installed on the gRPC server that runs the admin service, through {@link TermService#customizer}; every other call
 * passes untouched. In-process calls ({@code RaftServer.Division.getRaftServer().setConfigurationAsync}, tests only) do
 * not go through gRPC and skip it.
 *
 * <ul>
 *   <li>On a node that serves no such group, the request passes unchanged.</li>
 *   <li>On a node that is not the leader, it checks the same against that node's configuration (its current half,
 *   if joint): a refusal goes back as {@link NotLeaderException} (the client asks the leader, which checks again), and
 *   what passes is rewritten as below. Ratis handles the request only at half-close, through the division's executor,
 *   and the node may have won an election by then: the compare-and-set makes it apply the change only if its
 *   configuration is still the one checked. Otherwise Ratis answers that it is not the leader.</li>
 *   <li>On a leader whose configuration is a joint one, it answers {@link ReconfigurationInProgressException} itself
 *   (what Ratis would answer), which the client retries.</li>
 *   <li>Otherwise it computes the configuration the request asks for ({@code SET_UNCONDITIONALLY} and
 *   {@code COMPARE_AND_SET}: the request's servers and listeners; {@code ADD}: the current ones plus the request's).
 *   A change of more than one voter is refused with {@link SetConfigurationException} carrying
 *   {@link VoterChange#refusal}, unless the call has the header {@link #UNSAFE_HEADER} (then it logs a warning). A
 *   compare-and-set whose current configuration is not the live one is refused as Ratis refuses it, without the
 *   marker.</li>
 *   <li>What it lets through becomes a {@code COMPARE_AND_SET} against the configuration it checked, with the same call
 *   id: if another change commits between this check and Ratis' own handling (under the server's lock), Ratis refuses
 *   it as different, and the client's next attempt is checked again.</li>
 * </ul>
 *
 * <p>A refusal goes back as Ratis sends its own errors ({@link GrpcUtil#wrapException}), so a Ratis client gets the
 * {@link SetConfigurationException}, which it does not retry.</p>
 */
final class MembershipGuard implements ServerInterceptor {

	private static final Logger LOG = LoggerFactory.getLogger(MembershipGuard.class);

	/** The header of {@code admin --unsafe-multi-voter-change}: with value {@code 1}, the guard lets any change through. */
	static final String UNSAFE_HEADER = "rockserver-unsafe-membership-change";
	static final Metadata.Key<String> UNSAFE = Metadata.Key.of(UNSAFE_HEADER, Metadata.ASCII_STRING_MARSHALLER);

	private static final String SET_CONFIGURATION = AdminProtocolServiceGrpc.getSetConfigurationMethod()
			.getFullMethodName();

	private final Supplier<RaftServer> server;

	MembershipGuard(Supplier<RaftServer> server) {
		this.server = server;
	}

	/** What to do with a request: forward {@code forward}, or answer {@code refusal}; exactly one is not null. */
	record Decision(@Nullable SetConfigurationRequestProto forward, @Nullable IOException refusal) {

		static Decision pass(SetConfigurationRequestProto request) {
			return new Decision(request, null);
		}

		static Decision refuse(IOException refusal) {
			return new Decision(null, refusal);
		}
	}

	@Override
	public <Q, R> ServerCall.Listener<Q> interceptCall(ServerCall<Q, R> call, Metadata headers,
			ServerCallHandler<Q, R> next) {
		if (!call.getMethodDescriptor().getFullMethodName().equals(SET_CONFIGURATION)) {
			return next.startCall(call, headers);
		}
		boolean unsafe = "1".equals(headers.get(UNSAFE));
		return new ForwardingServerCallListener.SimpleForwardingServerCallListener<>(next.startCall(call, headers)) {
			private boolean refused;

			@Override
			@SuppressWarnings("unchecked")
			public void onMessage(Q message) {
				if (!(message instanceof SetConfigurationRequestProto request)) {
					super.onMessage(message);
					return;
				}
				var decision = check(request, unsafe);
				if (decision.refusal() != null) {
					refused = true;
					var error = GrpcUtil.wrapException(decision.refusal());
					call.close(error.getStatus(), error.getTrailers() != null ? error.getTrailers() : new Metadata());
					return;
				}
				super.onMessage((Q) decision.forward());
			}

			@Override
			public void onHalfClose() {
				// The call is closed already; the handler would answer a unary call without a request.
				if (!refused) {
					super.onHalfClose();
				}
			}
		};
	}

	private Decision check(SetConfigurationRequestProto request, boolean unsafe) {
		RaftServer.Division division;
		try {
			division = server.get().getDivision(RaftGroupId.valueOf(request.getRpcRequest().getRaftGroupId().getId()));
		} catch (IOException | RuntimeException noSuchGroup) {
			return Decision.pass(request);
		}
		boolean leader = division.getInfo().isLeader();
		var conf = division.getRaftConf();
		if (leader && (!conf.getPreviousPeers().isEmpty() || !conf.getPreviousPeers(RaftPeerRole.LISTENER).isEmpty())) {
			return Decision.refuse(new ReconfigurationInProgressException("Reconfiguration is already in progress: "
					+ conf));
		}
		var servers = conf.getCurrentPeers();
		var listeners = conf.getCurrentPeers(RaftPeerRole.LISTENER);
		if (!leader) {
			// Ratis handles the request only at half-close, through the division's executor: the node may lead by then.
			return decide(request, servers, listeners, unsafe, () -> notLeader(division, conf));
		}
		var decision = decide(request, servers, listeners, unsafe);
		if (decision.forward() != null && unsafe) {
			var change = VoterChange.between(servers, ProtoUtils.toRaftPeers(decision.forward().getPeersList()));
			if (!change.safe()) {
				LOG.warn("{}: a change of {} voters at once goes through, because the request has the header {}: voters {}"
								+ " become {}. If the leader changes before the change commits, committed entries can be lost unless"
								+ " every majority of the old voters that can still vote meets every majority of the new ones",
						division.getMemberId(), change.size(), UNSAFE_HEADER, VoterChange.ids(servers),
						protoIds(decision.forward().getPeersList()));
			}
		}
		return decision;
	}

	/**
	 * What the guard does with {@code request} on a leader whose stable configuration has {@code servers} and
	 * {@code listeners}.
	 */
	static Decision decide(SetConfigurationRequestProto request, Collection<RaftPeer> servers,
			Collection<RaftPeer> listeners, boolean unsafe) {
		List<RaftPeerProto> targetServers;
		List<RaftPeerProto> targetListeners;
		switch (request.getMode()) {
			case ADD -> {
				targetServers = union(servers, request.getPeersList());
				targetListeners = union(listeners, request.getListenersList());
			}
			case COMPARE_AND_SET -> {
				var askedServers = ProtoUtils.toRaftPeers(request.getCurrentPeersList());
				var askedListeners = ProtoUtils.toRaftPeers(request.getCurrentListenersList());
				if (!VoterChange.ids(askedServers).equals(VoterChange.ids(servers))
						|| !VoterChange.ids(askedListeners).equals(VoterChange.ids(listeners))) {
					return Decision.refuse(new SetConfigurationException("Failed to set configuration: current configuration"
							+ " (servers " + VoterChange.ids(servers) + ", listeners " + VoterChange.ids(listeners)
							+ ") is different than the request (servers " + VoterChange.ids(askedServers) + ", listeners "
							+ VoterChange.ids(askedListeners) + ")"));
				}
				targetServers = request.getPeersList();
				targetListeners = request.getListenersList();
			}
			default -> {
				targetServers = request.getPeersList();
				targetListeners = request.getListenersList();
			}
		}
		var change = VoterChange.between(servers, ProtoUtils.toRaftPeers(targetServers));
		if (!change.safe() && !unsafe) {
			return Decision.refuse(new SetConfigurationException(VoterChange.refusal(change, servers)));
		}
		return Decision.pass(request.toBuilder()
				.setMode(SetConfigurationRequestProto.Mode.COMPARE_AND_SET)
				.clearPeers().addAllPeers(targetServers)
				.clearListeners().addAllListeners(targetListeners)
				.clearCurrentPeers().addAllCurrentPeers(protos(servers))
				.clearCurrentListeners().addAllCurrentListeners(protos(listeners))
				.build());
	}

	/**
	 * What the guard does with {@code request} on a node whose configuration has {@code servers} and {@code listeners}
	 * (its current half, if joint): on the leader ({@code notLeader} null) as {@link #decide(SetConfigurationRequestProto,
	 * Collection, Collection, boolean)}; on another node the same, except that a refusal becomes {@code notLeader}'s
	 * exception, so that the client asks the leader, which checks again. What passes is a compare-and-set against this
	 * node's configuration: if the node leads by the time Ratis handles it, Ratis applies it only if its configuration is
	 * still the one checked here.
	 */
	static Decision decide(SetConfigurationRequestProto request, Collection<RaftPeer> servers,
			Collection<RaftPeer> listeners, boolean unsafe, @Nullable Supplier<NotLeaderException> notLeader) {
		var decision = decide(request, servers, listeners, unsafe);
		if (notLeader != null && decision.refusal() != null) {
			return Decision.refuse(notLeader.get());
		}
		return decision;
	}

	/** What Ratis answers on a node that is not the leader: the leader it knows, if any, and the peers it knows. */
	private static NotLeaderException notLeader(RaftServer.Division division, RaftConfiguration conf) {
		var leaderId = division.getInfo().getLeaderId();
		var peers = new ArrayList<RaftPeer>(conf.getAllPeers());
		RaftPeer suggested = leaderId == null ? null : conf.getPeer(leaderId);
		return new NotLeaderException(division.getMemberId(), suggested, peers);
	}

	/** {@code current} with the peers of {@code added}, which replace a current one of the same id, as Ratis' ADD does. */
	private static List<RaftPeerProto> union(Collection<RaftPeer> current, List<RaftPeerProto> added) {
		var byId = new LinkedHashMap<String, RaftPeerProto>();
		current.forEach(peer -> byId.put(peer.getId().toString(), peer.getRaftPeerProto()));
		ProtoUtils.toRaftPeers(added).forEach(peer -> byId.put(peer.getId().toString(), peer.getRaftPeerProto()));
		return new ArrayList<>(byId.values());
	}

	private static List<RaftPeerProto> protos(Collection<RaftPeer> peers) {
		return peers.stream().map(RaftPeer::getRaftPeerProto).toList();
	}

	private static Object protoIds(List<RaftPeerProto> peers) {
		return VoterChange.ids(ProtoUtils.toRaftPeers(peers));
	}
}
