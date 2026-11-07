package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.apache.ratis.client.impl.RaftClientRpcWithProxy;
import org.apache.ratis.protocol.RaftClientReply;
import org.apache.ratis.protocol.RaftClientRequest;
import org.apache.ratis.protocol.RaftPeer;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.TimeoutIOException;
import org.apache.ratis.util.PeerProxyMap;
import org.junit.jupiter.api.Test;

/**
 * DD-4: {@link Proposer#resetConnection} returns once Ratis took the failed proxy out of its map, before the old channel
 * closed (which waits up to 3 s for a frozen peer), so a resend to the same peer gets a new proxy instead of the one
 * being closed. R9-2: it replaces only the proxy the attempt was sent on.
 */
class ProposerResetTest {

	private static final RaftPeerId PEER = RaftPeerId.valueOf("n1");

	/** A proxy whose close blocks until released, like the close of a channel with a call to a frozen peer. */
	private static final class SlowProxy implements Closeable {
		final CountDownLatch release;
		final CountDownLatch closing = new CountDownLatch(1);
		volatile boolean closed;

		SlowProxy(CountDownLatch release) {
			this.release = release;
		}

		@Override
		public void close() {
			closing.countDown();
			try {
				release.await(10, TimeUnit.SECONDS);
			} catch (InterruptedException ex) {
				Thread.currentThread().interrupt();
			}
			closed = true;
		}
	}

	private static final class Rpc extends RaftClientRpcWithProxy<SlowProxy> {
		Rpc(PeerProxyMap<SlowProxy> proxies) {
			super(proxies);
		}

		@Override
		public RaftClientReply sendRequest(RaftClientRequest request) {
			throw new UnsupportedOperationException();
		}
	}

	@Test
	void theResendTakesANewProxyWhileTheOldOneCloses() throws Exception {
		var release = new CountDownLatch(1);
		var proxies = new PeerProxyMap<SlowProxy>("test", _ -> new SlowProxy(release));
		var rpc = new Rpc(proxies);
		try {
			rpc.addRaftPeers(List.of(RaftPeer.newBuilder().setId(PEER).setAddress("127.0.0.1:1").build()));
			var failed = proxies.getProxy(PEER);
			long start = System.nanoTime();
			Proposer.resetConnection(rpc, PEER, failed, new TimeoutIOException("attempt timed out"));
			long millis = (System.nanoTime() - start) / 1_000_000;
			assertTrue(millis < 1_000, () -> "took " + millis + " ms");
			var resend = proxies.getProxy(PEER);
			assertNotSame(failed, resend, "the resend does not use the proxy being reset");
			assertTrue(failed.closing.await(5, TimeUnit.SECONDS), "the old proxy is closed");
			assertFalse(failed.closed, "still closing: the reset did not wait for it");
			release.countDown();
			assertSame(resend, proxies.getProxy(PEER), "the reset left the new proxy alone");
			assertFalse(resend.closed);
		} finally {
			release.countDown();
		}
	}

	@Test
	void anErrorThatDoesNotResetReturnsAtOnceAndKeepsTheProxy() throws Exception {
		var release = new CountDownLatch(1);
		var proxies = new PeerProxyMap<SlowProxy>("test", _ -> new SlowProxy(release));
		var rpc = new Rpc(proxies);
		try {
			rpc.addRaftPeers(List.of(RaftPeer.newBuilder().setId(PEER).setAddress("127.0.0.1:1").build()));
			var kept = proxies.getProxy(PEER);
			Proposer.resetConnection(rpc, PEER, kept, new IOException("not a connection error"));
			assertSame(kept, proxies.getProxy(PEER));
		} finally {
			release.countDown();
		}
	}

	/**
	 * R9-2: attempts in flight on one broken proxy fail together. The first one's reset replaces it; the later ones,
	 * sent on the same proxy, must not reset (and close) the replacement, which the resends already use.
	 */
	@Test
	void attemptsThatFailedOnAReplacedProxyKeepItsReplacement() throws Exception {
		var release = new CountDownLatch(1);
		var proxies = new PeerProxyMap<SlowProxy>("test", _ -> new SlowProxy(release));
		var rpc = new Rpc(proxies);
		try {
			rpc.addRaftPeers(List.of(RaftPeer.newBuilder().setId(PEER).setAddress("127.0.0.1:1").build()));
			var broken = proxies.getProxy(PEER);
			Proposer.resetBrokenProxy(rpc, PEER, broken, new NullPointerException("first attempt"));
			var replacement = proxies.getProxy(PEER);
			assertNotSame(broken, replacement);
			for (int i = 0; i < 3; i++) {
				Proposer.resetBrokenProxy(rpc, PEER, broken, new NullPointerException("a later attempt on the same proxy"));
				Proposer.resetConnection(rpc, PEER, broken, new TimeoutIOException("a later attempt on the same proxy"));
			}
			release.countDown();
			// The reset threads that would have closed it have had their time.
			Thread.sleep(200);
			assertSame(replacement, proxies.getProxy(PEER), "the replacement was kept");
			assertFalse(replacement.closing.getCount() == 0, "the replacement was not closed");
		} finally {
			release.countDown();
		}
	}
}
