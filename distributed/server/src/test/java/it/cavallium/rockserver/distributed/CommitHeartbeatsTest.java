package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.conf.Parameters;
import org.apache.ratis.grpc.GrpcFactory;
import org.apache.ratis.rpc.RpcType;
import org.junit.jupiter.api.Test;

/**
 * CR2-3, R6-1: a follower read asks for each commit index once; the appenders' heartbeats are pinned against real
 * servers in {@code RatisContractTest.aCommitHeartbeatSendsTheCommitIndexAtOnce}.
 */
class CommitHeartbeatsTest {

	private final AtomicLong commit = new AtomicLong(5);
	private final CommitHeartbeats heartbeats = new CommitHeartbeats(commit::get);

	@Test
	void eachCommitIndexIsAskedForOnce() {
		heartbeats.send();
		heartbeats.send();
		heartbeats.send();
		assertEquals(5, heartbeats.wanted());
		assertEquals(1, heartbeats.sent(), "the same commit index: asked for once");
		commit.set(6);
		heartbeats.send();
		assertEquals(6, heartbeats.wanted(), "a higher commit index needs a heartbeat of its own");
		assertEquals(2, heartbeats.sent());
		assertEquals(2, heartbeats.skipped());
	}

	@Test
	void nothingBeforeACommitIndexOrWhenDisabled() {
		commit.set(-1);
		heartbeats.send();
		commit.set(9);
		heartbeats.enabled = false;
		heartbeats.send();
		assertEquals(-1, heartbeats.wanted());
		assertEquals(0, heartbeats.sent());
	}

	/** Ratis finds the transport by its class name, and builds it with or without the node's instance. */
	@Test
	void ratisFindsTheTransportByName() {
		var type = RpcType.valueOf(new CommitHeartbeats.Rpc().name());
		assertInstanceOf(CommitHeartbeats.Rpc.class, type);
		assertInstanceOf(GrpcFactory.class, type.newFactory(new Parameters()));
		var parameters = new Parameters();
		parameters.put(CommitHeartbeats.PARAMETER, heartbeats, CommitHeartbeats.class);
		assertInstanceOf(CommitHeartbeats.Factory.class, type.newFactory(parameters));
	}
}
