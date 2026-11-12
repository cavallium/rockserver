package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assumptions.assumeTrue;

import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.InetAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.time.Duration;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;

/**
 * R67-1: three nodes on {@code [::1]} and a client given the same list. The nodes send the leader's address as
 * {@code [::1]:port}; the client lists {@code ::1} without brackets, and still takes the hint: its leader-bound calls then
 * use a channel to the leader. Skipped on a machine without an IPv6 loopback.
 */
@Timeout(120)
class Ipv6LeaderHintTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);

	@TempDir
	Path temp;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	private static boolean hasIpv6Loopback() {
		try {
			return NetworkInterface.getByInetAddress(InetAddress.getByName("::1")) != null;
		} catch (Exception e) {
			return false;
		}
	}

	@Test
	void clientOfBracketedIpv6NodesSendsLeaderCallsStraightToTheLeader() throws Exception {
		assumeTrue(hasIpv6Loopback(), "no IPv6 loopback on this machine");
		cluster = new InProcessCluster(temp, 3, "[::1]", c -> c).startAll();
		cluster.awaitLeader(TIMEOUT);
		var addresses = new java.util.ArrayList<Utils.HostAndPort>();
		for (var node : cluster.runningNodes()) {
			var self = node.config().self();
			addresses.addAll(Utils.parseHostAndPorts("[::1]:" + self.apiPort()));
		}
		try (var connection = GrpcConnection.forHostsAndPorts("ipv6-client", addresses)) {
			var api = connection.getAsyncApi(RequestContext.batch(TIMEOUT));
			long column = api.createColumnAsync("c", SCHEMA).join();
			// Compared with the leader of the moment, in the wait: an election during the test moves both (review of R67).
			var seen = new String[2];
			try {
				InProcessCluster.await(TIMEOUT, () -> {
					var key = new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(System.nanoTime()).array()));
					api.putAsync(0, column, key, Buf.wrap(new byte[]{1}), RequestType.none()).join();
					var leader = cluster.awaitLeader(TIMEOUT);
					if (leader == null) {
						return false;
					}
					seen[0] = "[::1]:" + leader.config().self().apiPort();
					seen[1] = connection.leaderChannelAddressForTesting();
					return seen[0].equals(seen[1]);
				});
			} finally {
				// On a timeout too: what the leader was and what the client had.
				if (seen[0] != null && !seen[0].equals(seen[1])) {
					assertNotNull(seen[1], "no leader channel; the leader is " + seen[0]);
					assertEquals(seen[0], seen[1]);
				}
			}
		}
	}
}
