package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBRetryException;
import java.io.IOException;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CompletionException;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeoutException;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

/**
 * Who leads, as {@link LocalCommands} tells the routing, against a {@link StubRaft} division; and how a failed
 * leadership barrier is reported to clients.
 */
class LocalCommandsTest {

	@TempDir
	Path temp;
	private NodeMetrics metrics;
	private java.util.concurrent.ExecutorService inline;

	@AfterEach
	void tearDown() {
		if (metrics != null) {
			metrics.close();
		}
		if (inline != null) {
			inline.shutdownNow();
		}
	}

	private record Setup(StubRaft raft, LocalCommands commands) {}

	private Setup setup(boolean withDivision) throws Exception {
		var peers = List.of(new PeerConfig("n0", "127.0.0.1", 1, "127.0.0.1", 2));
		var config = NodeConfig.defaults("n0", peers, temp);
		metrics = new NodeMetrics("n0");
		inline = Executors.newVirtualThreadPerTaskExecutor();
		var stateMachine = new RockserverStateMachine(config, _ -> {}, metrics);
		var raft = new StubRaft();
		if (withDivision) {
			raft.installInto(stateMachine);
		}
		// The barrier is not used by the calls tested here.
		return new Setup(raft, new LocalCommands(stateMachine, null, metrics, inline));
	}

	@Test
	void withoutADivisionNothingLeads() throws Exception {
		var s = setup(false);
		assertFalse(s.commands().isLeader());
		assertEquals(-1, s.commands().currentTerm());
		assertThrows(LocalCommands.NotLeader.class, () -> s.commands().currentSession());
	}

	@Test
	void onlyAReadyLeaderLeadsAndGetsTheSessionOfItsTerm() throws Exception {
		var s = setup(true);
		s.raft().term = 3;
		assertFalse(s.commands().isLeader());
		assertThrows(LocalCommands.NotLeader.class, () -> s.commands().currentSession());

		s.raft().leader = true;
		assertFalse(s.commands().isLeader(), "a leader that is not ready yet serves nothing");
		assertThrows(LocalCommands.NotLeader.class, () -> s.commands().currentSession());

		s.raft().leaderReady = true;
		assertTrue(s.commands().isLeader());
		assertEquals(3, s.commands().currentTerm());
		var session = s.commands().currentSession();
		assertEquals(3, session.term());
		assertSame(session, s.commands().currentSession());

		s.raft().leader = false;
		assertFalse(s.commands().isLeader());
		assertThrows(LocalCommands.NotLeader.class, () -> s.commands().currentSession());
	}

	@Test
	void barrierFailuresMapToWhatAClientCanDoAboutThem() {
		assertInstanceOf(RocksDBRetryException.class, LocalCommands.barrierFailure(
				new CompletionException(new ReadBarrier.NotLeaderException("deposed"))));
		var timeout = assertInstanceOf(RocksDBException.class, LocalCommands.barrierFailure(new TimeoutException()));
		assertEquals(RocksDBErrorType.SERVER_OVERLOADED, timeout.getErrorUniqueId());
		var lag = assertInstanceOf(RocksDBException.class,
				LocalCommands.barrierFailure(new ReadBarrier.ApplyLagException(9, 4)));
		assertEquals(RocksDBErrorType.SERVER_OVERLOADED, lag.getErrorUniqueId());
		var runtime = new IllegalStateException("as is");
		assertSame(runtime, LocalCommands.barrierFailure(new CompletionException(runtime)));
		var other = assertInstanceOf(RocksDBException.class, LocalCommands.barrierFailure(new IOException("io")));
		assertEquals(RocksDBErrorType.INTERNAL_ERROR, other.getErrorUniqueId());
	}
}
