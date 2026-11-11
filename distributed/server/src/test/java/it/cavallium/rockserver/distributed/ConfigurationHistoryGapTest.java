package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.RequestType;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import org.apache.ratis.proto.RaftProtos;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.server.protocol.TermIndex;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * R11 review, finding 2: the history of committed configurations must not say that it is complete where it misses
 * one. In round 11 a probe of a purged entry then answered {@link RemovalRule.Probe#ABSENT} for a committed
 * configuration; since R12-2 a missing record answers {@link RemovalRule.Probe#UNKNOWN}, so this is defence in depth,
 * and {@link MembershipFacts#committed} still reads where the history is complete.
 */
class ConfigurationHistoryGapTest {

	private NodeMetrics metrics;

	@org.junit.jupiter.api.AfterEach
	void closeMetrics() {
		if (metrics != null) {
			metrics.close();
		}
	}

	@TempDir
	Path temp;

	private static RaftProtos.RaftConfigurationProto conf(String... ids) {
		var conf = RaftProtos.RaftConfigurationProto.newBuilder();
		for (var id : ids) {
			conf.addPeers(RaftPeer.newBuilder().setId(id).build().getRaftPeerProto());
		}
		return conf.build();
	}

	/**
	 * Route (a): a snapshot install's notification (term 0, from a gRPC thread) lands between the updater thread's
	 * notification of a configuration entry and its position; the entry must still be recorded with the position.
	 */
	@Test
	void anInstallNotificationDoesNotDropTheUpdatersConfiguration() throws Exception {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp);
		metrics = new NodeMetrics("n0");
		var stateMachine = new RockserverStateMachine(config, _ -> {}, metrics);
		stateMachine.notifyConfigurationChanged(4, 30, conf("n0", "n1", "n2"));
		CompletableFuture.runAsync(() -> stateMachine.notifyConfigurationChanged(0, 40, conf("n0", "n1"))).get();
		// What notifyTermIndexUpdated(4, 30) on this thread hands to the apply thread with the position (the apply
		// thread of a state machine that was never initialized cannot apply it, so the test reads it here).
		var pending = field(stateMachine, "pendingConfiguration");
		assertNotNull(pending, "the configuration at (4, 30) was dropped by the install notification");
		var recorded = (RemovalRule.Conf) component(pending, 1);
		assertEquals(new RemovalRule.Conf(4, 30, List.of("n0", "n1", "n2"), List.of()), recorded);
		assertEquals(Thread.currentThread(), component(pending, 0));
	}

	private static Object component(Object record, int position) throws ReflectiveOperationException {
		var accessor = record.getClass().getRecordComponents()[position].getAccessor();
		accessor.setAccessible(true);
		return accessor.invoke(record);
	}

	private static Object field(Object owner, String name) throws ReflectiveOperationException {
		var field = owner.getClass().getDeclaredField(name);
		field.setAccessible(true);
		return field.get(owner);
	}

	/**
	 * Route (b): a version that does not keep the history (a rolling upgrade) applied entries after the last record,
	 * and may have applied configurations without recording them: the history is complete only after its applied
	 * position, not from the index that the last record carried.
	 */
	@Test
	void entriesAppliedByAVersionWithoutTheHistoryMakeItCompleteOnlyAfterThem() throws Exception {
		try (var store = NodeStore.open(temp.resolve("db"), null, "gap")) {
			var db = store.db();
			long meta = store.metaColumnId();
			assertEquals(0, store.configurationHistoryFrom(), "an empty database");
			db.put(0L, meta, NodeStore.key(NodeStore.APPLIED), NodeStore.encodeTermIndex(TermIndex.valueOf(2, 10)),
					RequestType.none());
			assertEquals(11, store.configurationHistoryFrom(), "applied by a version without the history");
			// The value of the first R11 commits, the complete-from index alone: entries an older version applied after it
			// (a rolling upgrade, or its snapshot) left it as it was.
			db.put(0L, meta, NodeStore.key(NodeStore.CONFIGURATION_HISTORY),
					Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(0).array()), RequestType.none());
			assertEquals(11, store.configurationHistoryFrom(), "an 8-byte value says nothing about the applied position");
			db.put(0L, meta, NodeStore.key(NodeStore.CONFIGURATION_HISTORY), NodeStore.encodeHistory(0, 10),
					RequestType.none());
			assertEquals(0, store.configurationHistoryFrom(), "recorded up to the applied position");

			// An older version applies up to 20, with configurations it does not record.
			db.put(0L, meta, NodeStore.key(NodeStore.APPLIED), NodeStore.encodeTermIndex(TermIndex.valueOf(3, 20)),
					RequestType.none());
			assertEquals(21, store.configurationHistoryFrom(), "entries 11..20 were applied without the history");

		}
	}
}
