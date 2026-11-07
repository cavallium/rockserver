package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.cavallium.rockserver.distributed.RemovalRule.Conf;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** Round-13 review, finding 1: the term bound of a configuration a snapshot install gave Ratis. */
class InstalledConfigurationTest {

	private static final List<String> ABX = List.of("a", "b", "x");
	private static final List<String> AB = List.of("a", "b");

	@Test
	void theBoundIsTheHighestLeaderTermForTheSameConfigurationAndResetsForAnother(@TempDir Path raftDir)
			throws Exception {
		assertNull(InstalledConfiguration.read(raftDir));
		InstalledConfiguration.update(raftDir, 3, 40, ABX, AB);
		var d = new Conf(RemovalRule.UNKNOWN_TERM, 40, List.of("x", "b", "a"), List.of("b", "a"));
		assertEquals(3, InstalledConfiguration.boundOf(InstalledConfiguration.read(raftDir), d));
		// A chunk of a later leader with the same configuration raises it; one of an earlier term does not lower it.
		InstalledConfiguration.update(raftDir, 5, 40, ABX, AB);
		InstalledConfiguration.update(raftDir, 4, 40, ABX, AB);
		assertEquals(5, InstalledConfiguration.boundOf(InstalledConfiguration.read(raftDir), d));
		// Another configuration replaces the record, which then bounds nothing of the first one.
		InstalledConfiguration.update(raftDir, 6, 41, AB, List.of());
		assertEquals(RemovalRule.UNKNOWN_TERM, InstalledConfiguration.boundOf(InstalledConfiguration.read(raftDir), d));
		assertEquals(6, InstalledConfiguration.boundOf(InstalledConfiguration.read(raftDir),
				new Conf(RemovalRule.UNKNOWN_TERM, 41, AB, List.of())));
		// Other halves at the same index, or an unknown old half: no bound.
		assertEquals(RemovalRule.UNKNOWN_TERM, InstalledConfiguration.boundOf(InstalledConfiguration.read(raftDir),
				new Conf(RemovalRule.UNKNOWN_TERM, 41, ABX, List.of())));
		assertEquals(RemovalRule.UNKNOWN_TERM, InstalledConfiguration.boundOf(InstalledConfiguration.read(raftDir),
				new Conf(RemovalRule.UNKNOWN_TERM, 41, AB, null)));
		// A damaged file bounds nothing.
		Files.writeString(raftDir.resolve(InstalledConfiguration.FILE), "41\n");
		assertNull(InstalledConfiguration.read(raftDir));
	}

	/** What RaftTraffic records from a chunk: the configuration's index and halves, with the leader's term. */
	@Test
	void aChunkIsRecordedWithItsLeadersTerm(@TempDir Path raftDir) throws Exception {
		var conf = RaftProtos.RaftConfigurationProto.newBuilder();
		for (var id : ABX) {
			conf.addPeers(RaftProtos.RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8(id)));
		}
		for (var id : AB) {
			conf.addOldPeers(RaftProtos.RaftPeerProto.newBuilder().setId(ByteString.copyFromUtf8(id)));
		}
		var request = RaftProtos.InstallSnapshotRequestProto.newBuilder().setLeaderTerm(7)
				.setLastRaftConfigurationLogEntryProto(RaftProtos.LogEntryProto.newBuilder().setIndex(40)
						.setConfigurationEntry(conf))
				.build();
		InstalledConfiguration.record(raftDir, request);
		assertEquals(new InstalledConfiguration(40, 7, ABX, AB), InstalledConfiguration.read(raftDir));
		// A chunk without a configuration changes nothing.
		InstalledConfiguration.record(raftDir, RaftProtos.InstallSnapshotRequestProto.newBuilder().setLeaderTerm(9)
				.build());
		assertEquals(new InstalledConfiguration(40, 7, ABX, AB), InstalledConfiguration.read(raftDir));
	}
}
