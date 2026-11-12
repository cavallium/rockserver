package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.Test;

/** The leader hint cache of {@link ReplicatedConnection#leaderApiAddress()}, keyed by leader and address version. */
class LeaderHintCacheTest {

	private final AtomicLong version = new AtomicLong();
	private final Map<String, HostAndPort> addresses = new HashMap<>();
	private final AtomicInteger lookups = new AtomicInteger();
	private final LeaderHintCache cache = new LeaderHintCache();

	private String hint(String leader) {
		return cache.hint(RaftPeerId.valueOf(leader), version::get, peerId -> {
			lookups.incrementAndGet();
			return addresses.get(peerId);
		});
	}

	@Test
	void ipv6HostsAreInBracketsWhetherOrNotTheConfigHadThem() {
		addresses.put("n1", new HostAndPort("[::1]", 5001));
		addresses.put("n2", new HostAndPort("::1", 5002));
		addresses.put("n3", new HostAndPort("node-3", 5003));
		assertEquals("[::1]:5001", hint("n1"));
		assertEquals("[::1]:5002", hint("n2"));
		assertEquals("node-3:5003", hint("n3"));
	}

	@Test
	void theSameLeaderAndVersionAreNotLookedUpAgain() {
		addresses.put("n1", new HostAndPort("10.0.0.1", 5001));
		assertEquals("10.0.0.1:5001", hint("n1"));
		assertEquals("10.0.0.1:5001", hint("n1"));
		assertEquals("10.0.0.1:5001", hint("n1"));
		assertEquals(1, lookups.get());
	}

	@Test
	void aNewVersionIsLookedUpAndFollowsAMovedPort() {
		addresses.put("n1", new HostAndPort("10.0.0.1", 5001));
		assertEquals("10.0.0.1:5001", hint("n1"));
		addresses.put("n1", new HostAndPort("10.0.0.1", 6001));
		assertEquals("10.0.0.1:5001", hint("n1"), "looked up without a version change");
		version.incrementAndGet();
		assertEquals("10.0.0.1:6001", hint("n1"));
		assertEquals(2, lookups.get());
	}

	@Test
	void anotherLeaderIsLookedUpAtTheSameVersion() {
		addresses.put("n1", new HostAndPort("10.0.0.1", 5001));
		addresses.put("n2", new HostAndPort("10.0.0.2", 5002));
		assertEquals("10.0.0.1:5001", hint("n1"));
		assertEquals("10.0.0.2:5002", hint("n2"));
		assertEquals("10.0.0.1:5001", hint("n1"));
		assertEquals(3, lookups.get());
	}

	@Test
	void anUnknownAddressIsCachedAsNullUntilTheVersionChanges() {
		assertNull(hint("n1"));
		assertNull(hint("n1"));
		assertEquals(1, lookups.get());
		addresses.put("n1", new HostAndPort("10.0.0.1", 5001));
		version.incrementAndGet();
		assertEquals("10.0.0.1:5001", hint("n1"));
	}

	/** The version is read before the lookup: an address recorded during the lookup is looked up at the next call. */
	@Test
	void aChangeDuringTheLookupIsSeenByTheNextCall() {
		addresses.put("n1", new HostAndPort("10.0.0.1", 5001));
		var racing = cache.hint(RaftPeerId.valueOf("n1"), version::get, peerId -> {
			lookups.incrementAndGet();
			var old = addresses.get(peerId);
			// The forwarder records the new address, then raises the version, while this lookup runs.
			addresses.put(peerId, new HostAndPort("10.0.0.1", 6001));
			version.incrementAndGet();
			return old;
		});
		assertEquals("10.0.0.1:5001", racing);
		assertEquals("10.0.0.1:6001", hint("n1"));
		assertEquals(2, lookups.get());
	}
}
