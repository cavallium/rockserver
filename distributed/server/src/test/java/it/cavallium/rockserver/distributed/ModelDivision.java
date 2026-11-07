package it.cavallium.rockserver.distributed;

import java.lang.reflect.Proxy;
import java.util.List;
import java.util.TreeMap;
import java.util.function.BiFunction;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.DivisionInfo;
import org.apache.ratis.server.RaftConfiguration;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogEntryHeader;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.util.LifeCycle;

/**
 * A stub Ratis {@link RaftServer.Division} for the membership facts ({@link MembershipFacts}, the version-1 answer of
 * {@link TermService}): Ratis' configuration, a log (the entries after the snapshot), the snapshot and commit indexes,
 * and the current term. Only the methods those read are implemented.
 */
final class ModelDivision {

	private ModelDivision() {}

	static RaftPeer peer(String id) {
		return RaftPeer.newBuilder().setId(id).build();
	}

	static List<RaftPeer> peers(List<String> ids) {
		return ids.stream().map(ModelDivision::peer).toList();
	}

	static RaftProtos.LogEntryProto configurationEntry(long term, long index, List<String> current,
			List<String> previous) {
		var conf = RaftProtos.RaftConfigurationProto.newBuilder();
		peers(current).forEach(peer -> conf.addPeers(peer.getRaftPeerProto()));
		peers(previous).forEach(peer -> conf.addOldPeers(peer.getRaftPeerProto()));
		return RaftProtos.LogEntryProto.newBuilder().setTerm(term).setIndex(index).setConfigurationEntry(conf).build();
	}

	static RaftProtos.LogEntryProto ordinaryEntry(long term, long index) {
		return RaftProtos.LogEntryProto.newBuilder().setTerm(term).setIndex(index)
				.setStateMachineLogEntry(RaftProtos.StateMachineLogEntryProto.getDefaultInstance()).build();
	}

	@SuppressWarnings("unchecked")
	private static <T> T proxy(Class<T> type, BiFunction<String, Object[], Object> handler) {
		return (T) Proxy.newProxyInstance(ModelDivision.class.getClassLoader(), new Class<?>[] {type},
				(_, method, args) -> switch (method.getName()) {
					case "toString" -> type.getSimpleName();
					case "hashCode" -> 0;
					case "equals" -> false;
					default -> handler.apply(method.getName(), args);
				});
	}

	/**
	 * A division whose Ratis configuration is {@code current}/{@code previous} at {@code confIndex}, with the peers
	 * {@code all} (for addresses), whose log holds {@code log} (the entries after {@code snapshotIndex}).
	 */
	static RaftServer.Division of(long confIndex, List<String> current, List<String> previous, List<String> all,
			TreeMap<Long, RaftProtos.LogEntryProto> log, long snapshotIndex, long commitIndex, long currentTerm) {
		var conf = proxy(RaftConfiguration.class, (method, _) -> switch (method) {
			case "getLogEntryIndex" -> confIndex;
			case "getCurrentPeers" -> peers(current);
			case "getPreviousPeers" -> peers(previous);
			case "getAllPeers" -> peers(all);
			default -> throw new UnsupportedOperationException(method);
		});
		var raftLog = proxy(RaftLog.class, (method, args) -> switch (method) {
			case "getTermIndex" -> {
				var entry = log.get((Long) args[0]);
				yield entry == null ? null : TermIndex.valueOf(entry.getTerm(), entry.getIndex());
			}
			case "get" -> log.get((Long) args[0]);
			case "getEntries" -> log.subMap((Long) args[0], (Long) args[1]).values().stream()
					.map(LogEntryHeader::valueOf).toArray(LogEntryHeader[]::new);
			case "getSnapshotIndex" -> snapshotIndex;
			case "getLastCommittedIndex" -> commitIndex;
			default -> throw new UnsupportedOperationException(method);
		});
		var info = proxy(DivisionInfo.class, (method, _) -> switch (method) {
			case "getLifeCycleState" -> LifeCycle.State.RUNNING;
			case "getLeaderId" -> null;
			case "getCurrentTerm" -> currentTerm;
			default -> throw new UnsupportedOperationException(method);
		});
		return proxy(RaftServer.Division.class, (method, _) -> switch (method) {
			case "getInfo" -> info;
			case "getRaftConf" -> conf;
			case "getRaftLog" -> raftLog;
			default -> throw new UnsupportedOperationException(method);
		});
	}
}
