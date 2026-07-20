package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.cluster.ClusterColumnMetadata;
import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.ClusterMetadata;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.PartitionDescriptor;
import it.cavallium.rockserver.core.cluster.PartitionSplitState;
import it.cavallium.rockserver.core.cluster.ReplicaGroupMetadata;
import it.cavallium.rockserver.core.cluster.ReplicaMember;
import it.cavallium.rockserver.core.cluster.ReplicaRole;
import it.cavallium.rockserver.core.cluster.ShardId;
import it.cavallium.rockserver.core.cluster.client.PartitionPointReadSubmitter;
import it.cavallium.rockserver.core.cluster.client.PartitionReadFailureKind;
import it.cavallium.rockserver.core.cluster.client.PartitionReadRouteException;
import it.cavallium.rockserver.core.cluster.client.PartitionRouteCache;
import it.cavallium.rockserver.core.cluster.client.RouteTarget;
import it.cavallium.rockserver.core.cluster.client.RoutingPolicy;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommand;
import it.cavallium.rockserver.core.cluster.command.PartitionPointReadCommandCodec;
import it.cavallium.rockserver.core.cluster.command.ReplicatedLogCommit;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.replication.ratis.RatisFeatureUnavailableException;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionClient;
import it.cavallium.rockserver.core.replication.ratis.RatisPartitionDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisPeerDescriptor;
import it.cavallium.rockserver.core.replication.ratis.RatisProcessAdapterRpcException;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.net.URI;
import java.nio.file.Path;
import java.nio.charset.StandardCharsets;
import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CompletionException;

import org.junit.jupiter.api.Test;

class PartitionPointReadSubmitterTest {

	private static final ClusterId CLUSTER_ID = new ClusterId(new UUID(0, 1));
	private static final ShardId PARTITION = new ShardId(7);
	private static final NodeId N1 = NodeId.of("n1");
	private static final NodeId N2 = NodeId.of("n2");
	private static final int ROUTED_KEY = 0x18;

	@Test
	void rejectsNullInputsWithExplicitDiagnostics() {
		var resolver = new RecordingResolver();
		var cache = new PartitionRouteCache(metadata(7, N1));

		assertIllegalArgument("routeCache must not be null",
				() -> new PartitionPointReadSubmitter(null, resolver, () -> 0));
		assertIllegalArgument("ratisClientResolver must not be null",
				() -> new PartitionPointReadSubmitter(cache, null, () -> 0));
		assertIllegalArgument("currentTimeMillis must not be null",
				() -> new PartitionPointReadSubmitter(cache, resolver, null));

		var submitter = new PartitionPointReadSubmitter(cache, resolver, () -> 0);
		assertIllegalArgument("command must not be null",
				() -> submitter.readLinearizable(null, 7, 1000));
		assertIllegalArgument("policy must not be null",
				() -> submitter.read(command(3), 7, null, 1000));
	}

	@Test
	void linearizablePointReadRoutesToLeaderAndUsesRatisReadPath() {
		var resolver = new RecordingResolver();
		resolver.client.queryResponses.add(CompletableFuture.completedFuture(bytes("value")));
		var submitter = submitter(metadata(7, N1), resolver, 0);
		var command = command(3);

		var result = submitter.readLinearizable(command, 7, 1000);

		assertArrayEquals(bytes("value"), result);
		assertEquals(N1, resolver.targets.getFirst().replica().nodeId());
		assertArrayEquals(PartitionPointReadCommandCodec.encode(command), resolver.client.queries.getFirst());
		assertEquals(0, resolver.client.proposals.size());
		assertEquals(command, PartitionPointReadCommandCodec.decode(resolver.client.queries.getFirst()));
	}

	@Test
	void explicitStalePointReadRoutesToFreshFollowerAndUsesStaleReadPath() {
		var resolver = new RecordingResolver();
		resolver.client.staleResponses.add(CompletableFuture.completedFuture(bytes("stale-value")));
		var submitter = submitter(metadata(7, N1), resolver, 0);
		var command = command(3);

		var result = submitter.read(command, 7, RoutingPolicy.follower(20), 1000);

		assertArrayEquals(bytes("stale-value"), result);
		assertEquals(N2, resolver.targets.getFirst().replica().nodeId());
		assertArrayEquals(PartitionPointReadCommandCodec.encode(command), resolver.client.staleQueries.getFirst());
		assertEquals(List.of(20L), resolver.client.staleMinAppliedIndexes);
		assertEquals(0, resolver.client.queries.size());
		assertEquals(0, resolver.client.proposals.size());
	}

	@Test
	void explicitLocalPointReadUsesLocalReadPath() {
		var resolver = new RecordingResolver();
		resolver.client.localResponses.add(CompletableFuture.completedFuture(bytes("local-value")));
		var submitter = submitter(metadata(7, N1), resolver, 0);

		var result = submitter.read(command(3), 7, RoutingPolicy.local(), 1000);

		assertArrayEquals(bytes("local-value"), result);
		assertEquals(N1, resolver.targets.getFirst().replica().nodeId());
		assertEquals(1, resolver.client.localQueries.size());
		assertEquals(0, resolver.client.queries.size());
		assertEquals(0, resolver.client.staleQueries.size());
		assertEquals(0, resolver.client.proposals.size());
	}

	@Test
	void linearizablePointReadReroutesAfterLeaderMetadataChangeAndObservesCommittedValue() {
		var routeCache = new PartitionRouteCache(metadata(7, N1));
		var resolver = new PerNodeResolver();
		resolver.put(N1, "committed-before-leader-change");
		resolver.put(N2, "committed-before-leader-change");
		var submitter = new PartitionPointReadSubmitter(routeCache, resolver, () -> 0);
		var command = command(3);

		var beforeLeaderChange = submitter.readLinearizable(command, 7, 1000);

		assertArrayEquals(bytes("committed-before-leader-change"), beforeLeaderChange);
		assertEquals(N1, resolver.targets.getFirst().replica().nodeId());
		resolver.put(N1, "committed-after-leader-change");
		resolver.put(N2, "committed-after-leader-change");
		routeCache.updateMetadata(metadata(8, N2));

		var afterLeaderChange = submitter.readLinearizable(command, 8, 1000);

		assertArrayEquals(bytes("committed-after-leader-change"), afterLeaderChange);
		assertEquals(N2, resolver.targets.get(1).replica().nodeId());
		assertEquals(command, PartitionPointReadCommandCodec.decode(resolver.queries.getFirst()));
		assertEquals(command, PartitionPointReadCommandCodec.decode(resolver.queries.get(1)));
		assertEquals(0, resolver.proposals.size());
	}

	@Test
	void stalePointReadWithoutFreshReplicaStaysTyped() {
		var resolver = new RecordingResolver();
		var submitter = submitter(metadata(7, N1), resolver, 0);

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.read(command(3), 7, RoutingPolicy.follower(100), 1000));

		assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
		assertEquals("read route failed: no replica can satisfy read policy for shard " + PARTITION,
				error.getMessage());
		assertEquals(0, resolver.targets.size());
	}

	@Test
	void pointReadOutsidePartitionRangeStaysTyped() {
		var resolver = new RecordingResolver();
		var submitter = submitter(metadata(7, N1), resolver, 0);
		var outsidePartition = new PartitionPointReadCommand(3, 4, new Keys(buf(0x40)));

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.readLinearizable(outsidePartition, 7, 1000));

		assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
		assertEquals("read route failed: no partition owns calculated key", error.getMessage());
		assertEquals(0, resolver.targets.size());
	}

	@Test
	void staleMetadataEpochFailsBeforeResolvingClient() {
		var resolver = new RecordingResolver();
		var submitter = submitter(metadata(7, N1), resolver, 0);

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.readLinearizable(command(3), 6, 1000));

		assertEquals(PartitionReadFailureKind.STALE_ROUTE, error.kind());
		assertEquals(0, resolver.targets.size());
	}

	@Test
	void futureMetadataEpochFailsBeforeResolvingClient() {
		var resolver = new RecordingResolver();
		var submitter = submitter(metadata(7, N1), resolver, 0);

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.readLinearizable(command(3), 8, 1000));

		assertEquals(PartitionReadFailureKind.STALE_ROUTE, error.kind());
		assertTrue(error.getMessage().contains("future read route metadata epoch 8; current epoch is 7"));
		assertEquals(0, resolver.targets.size());
	}

	@Test
	void expiredDeadlineFailsBeforeRoutingCommand() {
		var resolver = new RecordingResolver();
		var submitter = submitter(metadata(7, N1), resolver, 10);
		var invalidIfRouted = new PartitionPointReadCommand(3, 404, new Keys(buf(0x18)));

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.readLinearizable(invalidIfRouted, 7, 10));

		assertEquals(PartitionReadFailureKind.DEADLINE_EXPIRED, error.kind());
		assertEquals("linearizable point read deadline expired before routing", error.getMessage());
		assertEquals(0, resolver.targets.size());
	}

	@Test
	void expiredFollowerDeadlineReportsPolicySpecificOperationBeforeRoutingCommand() {
		var resolver = new RecordingResolver();
		var submitter = submitter(metadata(7, N1), resolver, 10);
		var invalidIfRouted = new PartitionPointReadCommand(3, 404, new Keys(buf(0x18)));

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.read(invalidIfRouted, 7, RoutingPolicy.follower(0), 10));

		assertEquals(PartitionReadFailureKind.DEADLINE_EXPIRED, error.kind());
		assertEquals("follower point read deadline expired before routing", error.getMessage());
		assertEquals(0, resolver.targets.size());
	}

	@Test
	void timeoutCancelsLinearizableQuery() {
		var resolver = new RecordingResolver();
		var response = new CompletableFuture<byte[]>();
		resolver.client.queryResponses.add(response);
		var submitter = submitter(metadata(7, N1), resolver, 0);

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.readLinearizable(command(3), 7, 1));

		assertEquals(PartitionReadFailureKind.TIMEOUT, error.kind());
		assertTrue(response.isCancelled());
	}

	@Test
	void interruptionCancelsLinearizableQueryAndRestoresInterruptFlag() {
		var resolver = new RecordingResolver();
		var response = new CompletableFuture<byte[]>();
		resolver.client.queryResponses.add(response);
		var submitter = submitter(metadata(7, N1), resolver, 0);

		Thread.currentThread().interrupt();
		try {
			var error = assertThrows(PartitionReadRouteException.class,
					() -> submitter.readLinearizable(command(3), 7, 1000));

			assertEquals(PartitionReadFailureKind.TIMEOUT, error.kind());
			assertTrue(response.isCancelled());
			assertTrue(Thread.interrupted());
		} finally {
			clearInterruptedStatus();
		}
	}

	@Test
	void checkedPointReadFailureUsesQueryFailedKind() {
		var resolver = new RecordingResolver();
		var checkedFailure = new Exception("checked point read failed");
		resolver.client.queryResponses.add(CompletableFuture.failedFuture(checkedFailure));
		var submitter = submitter(metadata(7, N1), resolver, 0);

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.readLinearizable(command(3), 7, 1000));

		assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
		assertSame(checkedFailure, error.getCause());
		assertEquals(1, resolver.targets.size());
	}

	@Test
	void runtimePointReadFailuresUseQueryFailedKind() {
		var asyncResolver = new RecordingResolver();
		var asyncFailure = new IllegalStateException("async point read failed");
		asyncResolver.client.queryResponses.add(CompletableFuture.failedFuture(asyncFailure));
		var asyncSubmitter = submitter(metadata(7, N1), asyncResolver, 0);

		var asyncError = assertThrows(PartitionReadRouteException.class,
				() -> asyncSubmitter.readLinearizable(command(3), 7, 1000));

		assertEquals(PartitionReadFailureKind.QUERY_FAILED, asyncError.kind());
		assertEquals("linearizable point read failed", asyncError.getMessage());
		assertSame(asyncFailure, asyncError.getCause());
		assertEquals(1, asyncResolver.targets.size());
		assertEquals(1, asyncResolver.client.queries.size());

		var syncResolver = new RecordingResolver();
		var syncFailure = new IllegalStateException("sync point read failed");
		syncResolver.client.queryFailure = syncFailure;
		var syncSubmitter = submitter(metadata(7, N1), syncResolver, 0);

		var syncError = assertThrows(PartitionReadRouteException.class,
				() -> syncSubmitter.readLinearizable(command(3), 7, 1000));

		assertEquals(PartitionReadFailureKind.QUERY_FAILED, syncError.kind());
		assertEquals("linearizable point read failed", syncError.getMessage());
		assertSame(syncFailure, syncError.getCause());
		assertEquals(1, syncResolver.targets.size());
		assertEquals(0, syncResolver.client.queries.size());
	}

	@Test
	void adapterRpcPointReadFailureUsesQueryFailedKind() {
		var resolver = new RecordingResolver();
		var adapterFailure = new RatisProcessAdapterRpcException("ADAPTER_ERROR", "adapter query failed");
		resolver.client.queryResponses.add(CompletableFuture.failedFuture(adapterFailure));
		var submitter = submitter(metadata(7, N1), resolver, 0);

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.readLinearizable(command(3), 7, 1000));

		assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
		assertSame(adapterFailure, error.getCause());
		assertTrue(error.getMessage().contains(
				"linearizable point read failed with adapter error ADAPTER_ERROR: adapter query failed"));
		assertEquals(1, resolver.targets.size());
	}

	@Test
	void nullPointReadClientStaysTyped() {
		var resolver = new RecordingResolver();
		resolver.returnNullClient = true;
		var submitter = submitter(metadata(7, N1), resolver, 0);

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.readLinearizable(command(3), 7, 1000));

		assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
		assertTrue(error.getMessage().contains("resolver returned null Ratis client"));
		assertEquals(1, resolver.targets.size());
		assertEquals(0, resolver.client.queries.size());
	}

	@Test
	void pointReadResolverFailuresStayTyped() {
		var resolver = new RecordingResolver();
		var resolverFailure = new IllegalStateException("resolver failed");
		resolver.resolveFailure = resolverFailure;
		var submitter = submitter(metadata(7, N1), resolver, 0);

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.readLinearizable(command(3), 7, 1000));

		assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
		assertEquals("linearizable point read failed", error.getMessage());
		assertSame(resolverFailure, error.getCause());
		assertEquals(1, resolver.targets.size());
		assertEquals(0, resolver.client.queries.size());
	}

	@Test
	void nullPointReadQueryFutureStaysTyped() {
		var resolver = new RecordingResolver();
		resolver.client.returnNullLinearizableQuery = true;
		var submitter = submitter(metadata(7, N1), resolver, 0);

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.readLinearizable(command(3), 7, 1000));

		assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
		assertTrue(error.getMessage().contains("returned null query future"));
		assertEquals(1, resolver.targets.size());
		assertEquals(1, resolver.client.queries.size());
	}

	@Test
	void unsupportedStaleAndLocalPointReadsStayTyped() {
		var resolver = new PerNodeResolver();
		var submitter = new PartitionPointReadSubmitter(new PartitionRouteCache(metadata(7, N1)), resolver, () -> 0);

		var staleError = assertThrows(PartitionReadRouteException.class,
				() -> submitter.read(command(3), 7, RoutingPolicy.follower(20), 1000));

		assertEquals(PartitionReadFailureKind.QUERY_FAILED, staleError.kind());
		assertEquals("follower point read is unsupported", staleError.getMessage());
		assertInstanceOf(RatisFeatureUnavailableException.class, staleError.getCause());

		var localError = assertThrows(PartitionReadRouteException.class,
				() -> submitter.read(command(3), 7, RoutingPolicy.local(), 1000));

		assertEquals(PartitionReadFailureKind.QUERY_FAILED, localError.kind());
		assertEquals("local point read is unsupported", localError.getMessage());
		assertInstanceOf(RatisFeatureUnavailableException.class, localError.getCause());
		assertEquals(2, resolver.targets.size());
	}

	@Test
	void fatalPointReadFailureRetainsSeverity() {
		var resolver = new RecordingResolver();
		var fatalFailure = new AssertionError("fatal point read failed");
		resolver.client.queryResponses.add(CompletableFuture.failedFuture(fatalFailure));
		var submitter = submitter(metadata(7, N1), resolver, 0);

		var error = assertThrows(AssertionError.class,
				() -> submitter.readLinearizable(command(3), 7, 1000));

		assertSame(fatalFailure, error);
		assertEquals(1, resolver.targets.size());
	}

	@Test
	void wrappedFatalPointReadFailuresRetainSeverity() {
		var asyncResolver = new RecordingResolver();
		var asyncFailure = new AssertionError("wrapped async fatal point read failed");
		asyncResolver.client.queryResponses.add(CompletableFuture.failedFuture(new CompletionException(asyncFailure)));
		var asyncSubmitter = submitter(metadata(7, N1), asyncResolver, 0);

		var asyncError = assertThrows(AssertionError.class,
				() -> asyncSubmitter.readLinearizable(command(3), 7, 1000));

		assertSame(asyncFailure, asyncError);
		assertEquals(1, asyncResolver.targets.size());
		assertEquals(1, asyncResolver.client.queries.size());

		var syncResolver = new RecordingResolver();
		var syncFailure = new AssertionError("wrapped sync fatal point read failed");
		syncResolver.client.queryFailure = new CompletionException(syncFailure);
		var syncSubmitter = submitter(metadata(7, N1), syncResolver, 0);

		var syncError = assertThrows(AssertionError.class,
				() -> syncSubmitter.readLinearizable(command(3), 7, 1000));

		assertSame(syncFailure, syncError);
		assertEquals(1, syncResolver.targets.size());
		assertEquals(0, syncResolver.client.queries.size());
	}

	@Test
	void rejectsSchemaEpochMismatchBeforeRatisQuery() {
		var resolver = new RecordingResolver();
		var submitter = submitter(metadata(7, N1), resolver, 0);

		var error = assertThrows(PartitionReadRouteException.class,
				() -> submitter.readLinearizable(command(2), 7, 1000));

		assertEquals(PartitionReadFailureKind.QUERY_FAILED, error.kind());
		assertEquals("read route failed: partition point read schema epoch does not match column schema epoch",
				error.getMessage());
		assertInstanceOf(IllegalArgumentException.class, error.getCause());
		assertEquals(0, resolver.targets.size());
	}

	private static PartitionPointReadSubmitter submitter(ClusterMetadata metadata,
														 RecordingResolver resolver,
														 long nowMillis) {
		return new PartitionPointReadSubmitter(new PartitionRouteCache(metadata), resolver, () -> nowMillis);
	}

	@SuppressWarnings("ResultOfMethodCallIgnored")
	private static void clearInterruptedStatus() {
		Thread.interrupted();
	}

	private static PartitionPointReadCommand command(long schemaEpoch) {
		return new PartitionPointReadCommand(schemaEpoch, 4, new Keys(buf(ROUTED_KEY)));
	}

	private static ClusterMetadata metadata(long epoch, NodeId leader) {
		var group = new ReplicaGroupMetadata(PARTITION, 3, leader, peers(), 3);
		var column = new ClusterColumnMetadata(4, "accounts", schema(), 3);
		var partition = new PartitionDescriptor(
				PARTITION,
				buf(0x10),
				buf(0x20),
				3,
				"group-7",
				peers(),
				leader,
				Map.of("rack", "rack-a"),
				PartitionSplitState.ACTIVE
		);
		return new ClusterMetadata(
				CLUSTER_ID,
				epoch,
				Map.of(PARTITION, group),
				Map.of(4L, column),
				Map.of(PARTITION, partition)
		);
	}

	private static List<ReplicaMember> peers() {
		return List.of(
				new ReplicaMember(N1, URI.create("grpc://127.0.0.1:10001"), ReplicaRole.VOTER, 25, 25, true),
				new ReplicaMember(N2, URI.create("grpc://127.0.0.1:10002"), ReplicaRole.VOTER, 24, 24, true)
		);
	}

	private static ColumnSchema schema() {
		return ColumnSchema.of(IntArrayList.of(1), ObjectArrayList.of(), true);
	}

	private static Buf buf(int... values) {
		var bytes = new byte[values.length];
		for (int i = 0; i < values.length; i++) {
			bytes[i] = (byte) values[i];
		}
		return Buf.wrap(bytes);
	}

	private static byte[] bytes(String value) {
		return value.getBytes(StandardCharsets.UTF_8);
	}

	private static void assertIllegalArgument(String message, Runnable operation) {
		var error = assertThrows(IllegalArgumentException.class, operation::run);
		assertEquals(message, error.getMessage());
	}

	private static final class RecordingResolver implements PartitionPointReadSubmitter.RatisClientResolver {

		private final RecordingRatisClient client = new RecordingRatisClient();
		private final List<RouteTarget> targets = new ArrayList<>();
		private boolean returnNullClient;
		private RuntimeException resolveFailure;

		@Override
		public RatisPartitionClient resolve(RouteTarget target) {
			targets.add(target);
			if (resolveFailure != null) {
				throw resolveFailure;
			}
			if (returnNullClient) {
				return null;
			}
			return client;
		}
	}

	private static final class PerNodeResolver implements PartitionPointReadSubmitter.RatisClientResolver {

		private final Map<NodeId, byte[]> values = new HashMap<>();
		private final Map<NodeId, PerNodeRatisClient> clients = new HashMap<>();
		private final List<RouteTarget> targets = new ArrayList<>();
		private final List<byte[]> queries = new ArrayList<>();
		private final List<byte[]> proposals = new ArrayList<>();

		private void put(NodeId nodeId, String value) {
			values.put(nodeId, bytes(value));
		}

		@Override
		public RatisPartitionClient resolve(RouteTarget target) {
			targets.add(target);
			return clients.computeIfAbsent(target.replica().nodeId(), nodeId -> new PerNodeRatisClient(this, nodeId));
		}
	}

	private record PerNodeRatisClient(PerNodeResolver resolver, NodeId nodeId) implements RatisPartitionClient {

		@Override
		public RatisPartitionDescriptor descriptor() {
			return new RatisPartitionDescriptor(
					"7",
					"group-7",
					nodeId.value(),
					List.of(new RatisPeerDescriptor(nodeId.value(), "grpc://127.0.0.1:10001", "rack-a")),
					Path.of("/tmp/partition-point-read-leader-change-test")
			);
		}

		@Override
		public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
			return CompletableFuture.completedFuture(new ReplicatedLogCommit(index, true));
		}

		@Override
		public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
			resolver.queries.add(request.clone());
			var value = resolver.values.get(nodeId);
			return CompletableFuture.completedFuture(value == null ? null : value.clone());
		}

		@Override
		public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
			resolver.proposals.add(payload.clone());
			return CompletableFuture.completedFuture(new ReplicatedLogCommit(resolver.proposals.size(), true));
		}

		@Override
		public void close() {
		}
	}

	private static final class RecordingRatisClient implements RatisPartitionClient {

		private final Queue<CompletableFuture<byte[]>> queryResponses = new ArrayDeque<>();
		private final Queue<CompletableFuture<byte[]>> staleResponses = new ArrayDeque<>();
		private final Queue<CompletableFuture<byte[]>> localResponses = new ArrayDeque<>();
		private final List<byte[]> queries = new ArrayList<>();
		private final List<byte[]> staleQueries = new ArrayList<>();
		private final List<Long> staleMinAppliedIndexes = new ArrayList<>();
		private final List<byte[]> localQueries = new ArrayList<>();
		private final List<byte[]> proposals = new ArrayList<>();
		private RuntimeException queryFailure;
		private boolean returnNullLinearizableQuery;

		@Override
		public RatisPartitionDescriptor descriptor() {
			return new RatisPartitionDescriptor(
					"7",
					"group-7",
					"n1",
					List.of(new RatisPeerDescriptor("n1", "grpc://127.0.0.1:10001", "rack-a")),
					Path.of("/tmp/partition-point-read-test")
			);
		}

		@Override
		public CompletableFuture<ReplicatedLogCommit> waitForCommit(long index) {
			return CompletableFuture.completedFuture(new ReplicatedLogCommit(index, true));
		}

		@Override
		public CompletableFuture<byte[]> queryLinearizable(byte[] request) {
			if (queryFailure != null) {
				throw queryFailure;
			}
			queries.add(request.clone());
			if (returnNullLinearizableQuery) {
				return null;
			}
			return queryResponses.remove();
		}

		@Override
		public CompletableFuture<byte[]> queryStale(byte[] request, long minAppliedIndex) {
			staleQueries.add(request.clone());
			staleMinAppliedIndexes.add(minAppliedIndex);
			return staleResponses.remove();
		}

		@Override
		public CompletableFuture<byte[]> queryLocal(byte[] request) {
			localQueries.add(request.clone());
			return localResponses.remove();
		}

		@Override
		public CompletableFuture<ReplicatedLogCommit> propose(byte[] payload) {
			proposals.add(payload.clone());
			return CompletableFuture.completedFuture(new ReplicatedLogCommit(proposals.size(), true));
		}

		@Override
		public void close() {
		}
	}
}
