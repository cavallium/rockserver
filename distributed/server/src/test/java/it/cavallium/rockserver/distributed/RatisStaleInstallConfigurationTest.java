package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.grpc.GrpcConfigKeys;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftGroup;
import org.apache.ratis.protocol.RaftGroupId;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.rpc.SupportedRpcType;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.statemachine.impl.BaseStateMachine;
import org.apache.ratis.statemachine.impl.SimpleStateMachineStorage;
import org.apache.ratis.util.TimeDuration;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * Pins a behaviour of Ratis 3.3.1 that {@link InstalledConfiguration} depends on (round 15, R14-1): a snapshot chunk
 * that Ratis rejects because its leader's term is below the current one (NOT_LEADER) still replaces Ratis'
 * configuration with the chunk's ({@code SnapshotInstallationHandler.installSnapshotImpl} applies the configuration
 * of every reply that is not an exception). So {@link RaftTraffic} records every chunk, whatever Ratis answers. If
 * this test fails after a Ratis upgrade, revisit that, and the upstream issue N-R15-1 (design: "Removal and waiting
 * outside", residuals).
 */
@Timeout(120)
class RatisStaleInstallConfigurationTest {

	@TempDir
	Path tempDir;

	@Test
	void aChunkThatRatisRejectsForItsTermStillReplacesItsConfiguration() throws Exception {
		var groupId = RaftGroupId.randomId();
		var peer = RaftPeer.newBuilder().setId("n0")
				.setAddress("127.0.0.1:" + ProcessCluster.freePorts(1, List.of()).getFirst()).build();
		var properties = new RaftProperties();
		RaftConfigKeys.Rpc.setType(properties, SupportedRpcType.GRPC);
		var address = peer.getAddress();
		GrpcConfigKeys.Server.setHost(properties, "127.0.0.1");
		GrpcConfigKeys.Server.setPort(properties, Integer.parseInt(address.substring(address.lastIndexOf(':') + 1)));
		RaftServerConfigKeys.setStorageDir(properties, List.of(tempDir.resolve("n0").toFile()));
		RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(300, TimeUnit.MILLISECONDS));
		RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(600, TimeUnit.MILLISECONDS));
		try (var server = RaftServer.newBuilder().setServerId(peer.getId()).setGroup(RaftGroup.valueOf(groupId, peer))
				.setProperties(properties).setStateMachine(new Machine()).setOption(RaftStorage.StartupOption.FORMAT)
				.build()) {
			server.start();
			var division = server.getDivision(groupId);
			long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(60);
			while (!division.getInfo().isLeader() && System.nanoTime() - deadline < 0) {
				Thread.sleep(20);
			}
			assertTrue(division.getInfo().isLeader(), "the single node leads");
			// A new leader appends its start configuration entry, and the state machine updater persists the
			// configuration of every entry it applies to raft-meta.conf through the same raft-meta.conf.tmp that
			// installSnapshot uses, with no lock between the two (stock Ratis 3.3.1, round 66): an install that
			// overlaps that apply loses the rename and fails, in either thread. Install only once that entry applied.
			while ((division.getRaftLog().getNextIndex() < 1
					|| division.getInfo().getLastAppliedIndex() < division.getRaftLog().getNextIndex() - 1)
					&& System.nanoTime() - deadline < 0) {
				Thread.sleep(5);
			}
			assertTrue(division.getRaftLog().getNextIndex() >= 1
					&& division.getInfo().getLastAppliedIndex() >= division.getRaftLog().getNextIndex() - 1,
					"the start configuration entry is applied");
			long term = division.getInfo().getCurrentTerm();
			assertTrue(term >= 1, "term " + term);
			long index = division.getRaftLog().getNextIndex() + 100;
			var stranger = RaftPeerId.valueOf("z");
			var conf = RaftProtos.RaftConfigurationProto.newBuilder().addPeers(peer.getRaftPeerProto())
					.addPeers(RaftProtos.RaftPeerProto.newBuilder().setId(stranger.toByteString())
							.setAddress("127.0.0.1:1"));
			var request = RaftProtos.InstallSnapshotRequestProto.newBuilder()
					.setServerRequest(RaftProtos.RaftRpcRequestProto.newBuilder().setRequestorId(stranger.toByteString())
							.setReplyId(peer.getId().toByteString())
							.setRaftGroupId(RaftProtos.RaftGroupIdProto.newBuilder().setId(groupId.toByteString()))
							.setCallId(1))
					.setLeaderTerm(term - 1)
					.setSnapshotChunk(RaftProtos.InstallSnapshotRequestProto.SnapshotChunkProto.newBuilder()
							.setRequestId("stale").setRequestIndex(0)
							.setTermIndex(RaftProtos.TermIndexProto.newBuilder().setTerm(term - 1).setIndex(index))
							.setTotalSize(0).setDone(true))
					.setLastRaftConfigurationLogEntryProto(RaftProtos.LogEntryProto.newBuilder().setTerm(term - 1)
							.setIndex(index).setConfigurationEntry(conf))
					.build();
			var reply = server.installSnapshot(request);
			assertEquals(RaftProtos.InstallSnapshotResult.NOT_LEADER, reply.getResult(), reply.toString());
			var after = division.getRaftConf();
			assertEquals(index, after.getLogEntryIndex(), after.toString());
			assertNotNull(after.getPeer(stranger), after.toString());
		}
	}

	private static final class Machine extends BaseStateMachine {
		private final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();

		@Override
		public void initialize(RaftServer server, RaftGroupId groupId, RaftStorage raftStorage) throws IOException {
			super.initialize(server, groupId, raftStorage);
			storage.init(raftStorage);
		}

		@Override
		public SimpleStateMachineStorage getStateMachineStorage() {
			return storage;
		}
	}
}
