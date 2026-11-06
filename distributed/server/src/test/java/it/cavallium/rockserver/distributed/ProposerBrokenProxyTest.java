package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.time.Duration;
import org.apache.ratis.client.impl.RaftClientRpcWithProxy;
import org.apache.ratis.grpc.client.GrpcClientProtocolClient;
import org.apache.ratis.grpc.metrics.intercept.client.MetricClientInterceptor;
import org.apache.ratis.metrics.RatisMetrics;
import org.apache.ratis.protocol.RaftPeerId;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.CleanupMode;
import org.junit.jupiter.api.io.TempDir;

/**
 * R8-4: a node copes with a Ratis client proxy of its proposer whose metric interceptor has no registry, which Ratis
 * 3.3.1 can build when an older proxy to the same peer closes in the same moment
 * ({@code RatisContractTest.aProxyMadeWhileTheOneBeforeItClosesCanGetNoMetricRegistry}; the proposer closes old proxies
 * while it makes new ones, {@link Proposer#resetConnection}). Every call of such a proxy throws a
 * {@link NullPointerException} at its start, and so does its close. The tests set that state by reflection, the
 * registry being a final field (the JVM warns about it), on the proxy to the leader (writes through the follower must
 * replace it, {@link Proposer#resetBrokenProxy}) or to the other follower (a proxy that no attempt replaces, as the one
 * to a halted leader in {@code RustClientLiveTest}: stopping the node must not fail on it).
 */
@Timeout(180)
class ProposerBrokenProxyTest {

	private static final Duration TIMEOUT = Duration.ofSeconds(30);
	private static final ColumnSchema SCHEMA = ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true);
	private static final RequestContext CONTEXT = RequestContext.batch(Duration.ofSeconds(20));

	/** Kept when a test fails, for the node logs. */
	@TempDir(cleanup = CleanupMode.ON_SUCCESS)
	Path root;
	private InProcessCluster cluster;

	@AfterEach
	void tearDown() throws Exception {
		if (cluster != null) {
			cluster.close();
		}
	}

	private static Keys key(long value) {
		return new Keys(Buf.wrap(ByteBuffer.allocate(Long.BYTES).putLong(value).array()));
	}

	private static Buf text(String value) {
		return Buf.wrap(value.getBytes(StandardCharsets.UTF_8));
	}

	@SuppressWarnings("unchecked")
	private static RaftClientRpcWithProxy<GrpcClientProtocolClient> rpc(ReplicatedNode node) {
		return (RaftClientRpcWithProxy<GrpcClientProtocolClient>) node.proposer().client().getClientRpc();
	}

	/** Leaves {@code proxy}'s metric interceptor without a registry, as Ratis' race does. */
	private static void breakMetrics(GrpcClientProtocolClient proxy) throws ReflectiveOperationException {
		var interceptorField = GrpcClientProtocolClient.class.getDeclaredField("metricClientInterceptor");
		interceptorField.setAccessible(true);
		var metricsField = MetricClientInterceptor.class.getDeclaredField("metrics");
		metricsField.setAccessible(true);
		var metrics = (RatisMetrics) metricsField.get(interceptorField.get(proxy));
		var registryField = RatisMetrics.class.getDeclaredField("registry");
		registryField.setAccessible(true);
		registryField.set(metrics, null);
		assertNull(metrics.getRegistry());
		assertThrows(NullPointerException.class, metrics::unregister, "its close fails as in Ratis' race");
	}

	private ReplicatedNode followerOtherThan(ReplicatedNode leader, ReplicatedNode follower) {
		return cluster.runningNodes().stream().filter(node -> node != leader && node != follower).findFirst()
				.orElseThrow();
	}

	@Test
	void aWriteThroughAFollowerReplacesItsBrokenProxyToTheLeader() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		var api = follower.connection().getSyncApi(CONTEXT);
		var proxies = rpc(follower).getProxies();
		var leaderId = RaftPeerId.valueOf(leader.config().nodeId());
		// A new proxy that made no call yet, as Ratis' race leaves it: a proxy that already counted a call keeps the
		// counter it made and goes on working.
		proxies.resetProxy(leaderId);
		var broken = proxies.getProxy(leaderId);
		breakMetrics(broken);

		// Without the replacement every attempt fails the same way until the write's deadline.
		long column = api.createColumn("c", SCHEMA);
		api.put(0, column, key(1), text("a"), RequestType.none());
		assertNotSame(broken, proxies.getProxy(leaderId), "the broken proxy was replaced");
		cluster.awaitConverged(TIMEOUT);
		for (var node : cluster.runningNodes()) {
			var value = node.stateMachine().store().db().get(0, column, key(1), RequestType.current());
			assertEquals("a", new String(value.toByteArray(), StandardCharsets.UTF_8), node.config().nodeId());
		}
	}

	@Test
	void aBrokenProxyThatNoAttemptReplacedDoesNotFailTheStop() throws Exception {
		cluster = new InProcessCluster(root, 3, config -> config).startAll();
		var leader = cluster.awaitLeader(TIMEOUT);
		var follower = cluster.anyFollower();
		var other = followerOtherThan(leader, follower);
		// Writes go to the leader only, so nothing replaces the proxy to the other follower.
		breakMetrics(rpc(follower).getProxies().getProxy(RaftPeerId.valueOf(other.config().nodeId())));
		follower.connection().getSyncApi(CONTEXT).createColumn("c", SCHEMA);

		var closing = cluster;
		cluster = null;
		// Before R8-4: IOException "Failed to stop node ..." (the CompletionException of Ratis' proxy map, around the
		// NullPointerException of the proxy's close).
		closing.close();
	}
}
