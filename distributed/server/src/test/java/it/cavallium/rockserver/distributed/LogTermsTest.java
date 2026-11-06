package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/** {@link LogTerms} reads what a stopped node's log holds, and finds where two logs disagree. */
class LogTermsTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	@Test
	void readsTheTermsOfAStoppedClusterAndTheyMatch() throws Exception {
		cluster = new InProcessCluster(temp, 3, c -> c).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		long term = leader.stateMachine().division().getInfo().getCurrentTerm();
		var api = leader.connection().getSyncApi(RequestContext.batch(TIMEOUT));
		long column = api.createColumn("c", ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		for (long i = 0; i < 50; i++) {
			api.put(0, column, new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(i).array())),
					Buf.wrap(new byte[] {1}), RequestType.none());
		}
		cluster.awaitConverged(TIMEOUT);
		long applied = leader.appliedIndex();
		var ids = cluster.ids();
		for (var id : ids) {
			cluster.stop(id);
		}
		var first = LogTerms.read(cluster.config(ids.getFirst()).dataDir());
		assertTrue(first.lastKey() >= applied, "log ends at " + first.lastKey() + ", applied " + applied);
		assertTrue(first.get(applied) >= term, String.valueOf(first.get(applied)));
		for (var id : ids) {
			var log = LogTerms.read(cluster.config(id).dataDir());
			assertTrue(log.containsKey(applied), id);
			assertEquals(-1, LogTerms.firstDisagreement(first, log), id);
		}
	}

	@Test
	void findsTheFirstDisagreement() {
		Map<Long, Long> a = new HashMap<>(Map.of(1L, 1L, 2L, 1L, 3L, 2L, 4L, 2L));
		assertEquals(-1, LogTerms.firstDisagreement(a, Map.of(3L, 2L, 4L, 2L, 5L, 3L)));
		assertEquals(3, LogTerms.firstDisagreement(a, Map.of(2L, 1L, 3L, 3L, 4L, 3L)));
	}
}
