package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.google.protobuf.ByteString;
import com.google.protobuf.Empty;
import io.grpc.ManagedChannel;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import io.grpc.netty.NettyChannelBuilder;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.RockserverCapabilities;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesRequest;
import it.cavallium.rockserver.core.common.api.proto.CapabilitiesResponse;
import it.cavallium.rockserver.core.common.api.proto.ClientRequestId;
import it.cavallium.rockserver.core.common.api.proto.KV;
import it.cavallium.rockserver.core.common.api.proto.MergeRequest;
import it.cavallium.rockserver.core.common.api.proto.OpenClientSessionRequest;
import it.cavallium.rockserver.core.common.api.proto.OpenClientSessionResponse;
import it.cavallium.rockserver.core.common.api.proto.ReactorRocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.RequestContext;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.common.api.proto.WorkloadProfile;
import it.cavallium.rockserver.core.server.GrpcServer;
import java.net.InetSocketAddress;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import reactor.core.publisher.Mono;

/**
 * R58-1: a core {@code GrpcServer} in front of a cluster that offers client sessions (a proxy, {@code Main
 * --database-url http://a,b,c}) forwards each write through a {@code GrpcConnection} that numbers writes in a session
 * of its own. A downstream client that numbered the write itself (in a session the proxy opened for it upstream)
 * sends the same id again after a lost answer; the proxy must pass that id through instead of numbering each copy
 * anew, or the cluster applies the merge twice. A write the downstream client did not number is still numbered by the
 * proxy's own session.
 */
@Timeout(60)
class GrpcProxyCallerRequestIdTest {

	/** The proxy connection's own session upstream. */
	private static final long PROXY_SESSION = 5;
	/** The downstream client's session (opened upstream through the proxy). */
	private static final long DOWNSTREAM_SESSION = 42;

	private record Id(long session, long sequence) {}

	@Test
	void aProxyPassesTheCallersRequestIdThroughSoTheClusterAppliesAResentMergeOnce() throws Exception {
		run(WorkloadProfile.LATENCY, TimeUnit.SECONDS.toNanos(20), true);
	}

	/**
	 * Review of R58, 1: the context of a SessionWrites caller with {@code RequestContext.ingest()} (no timeout) and no
	 * gRPC deadline. GrpcRequestContexts used to serve such a context from its cached no-deadline API, built from a
	 * context with no request id, so the caller's id was dropped before the proxy's connection saw it.
	 */
	@Test
	void aProxyPassesTheRequestIdOfACallerWithNoTimeoutThrough() throws Exception {
		run(WorkloadProfile.INGEST, Long.MAX_VALUE, false);
	}

	private void run(WorkloadProfile profile, long timeoutNanos, boolean deadline) throws Exception {
		var seen = new CopyOnWriteArrayList<Id>();
		Set<Id> applied = ConcurrentHashMap.newKeySet();
		var counter = new AtomicInteger();
		var upstreamService = new ReactorRocksDBServiceGrpc.RocksDBServiceImplBase() {
			@Override
			public Mono<CapabilitiesResponse> getCapabilities(CapabilitiesRequest request) {
				return Mono.just(CapabilitiesResponse.newBuilder()
						.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
						.setClientSessions(true).build());
			}

			@Override
			public Mono<OpenClientSessionResponse> openClientSession(OpenClientSessionRequest request) {
				return Mono.just(OpenClientSessionResponse.newBuilder().setSessionId(PROXY_SESSION).build());
			}

			@Override
			public Mono<Empty> merge(MergeRequest request) {
				var id = request.getContext().getRequestId();
				var key = new Id(id.getSessionId(), id.getSequence());
				seen.add(key);
				// As the cluster: a numbered write takes effect once per id; every copy gets the first answer.
				if (applied.add(key)) {
					counter.incrementAndGet();
				}
				return Mono.just(Empty.getDefaultInstance());
			}
		};
		Server upstream = ServerBuilder.forPort(0).addService(upstreamService).build().start();
		GrpcConnection proxyClient = null;
		GrpcServer proxy = null;
		ManagedChannel channel = null;
		try {
			proxyClient = GrpcConnection.forHostAndPort("proxy-upstream", new HostAndPort("127.0.0.1",
					upstream.getPort()));
			proxy = new GrpcServer(proxyClient, new InetSocketAddress("127.0.0.1", 0));
			proxy.start();
			channel = NettyChannelBuilder.forAddress("127.0.0.1", proxy.getPort()).usePlaintext().build();
			var stub = RocksDBServiceGrpc.newBlockingStub(channel);
			if (deadline) {
				stub = stub.withDeadlineAfter(30, TimeUnit.SECONDS);
			}

			var numbered = merge(profile, timeoutNanos, ClientRequestId.newBuilder().setSessionId(DOWNSTREAM_SESSION).setSequence(7)
					.setCompletedBelow(7).build());
			// The first copy, then the copy a downstream client sends after losing the answer to the first.
			stub.merge(numbered);
			stub.merge(numbered);

			assertEquals(List.of(new Id(DOWNSTREAM_SESSION, 7), new Id(DOWNSTREAM_SESSION, 7)), seen,
					"the proxy must forward the caller's request id unchanged");
			assertEquals(1, counter.get(), "the resent merge was applied twice");

			stub.merge(merge(profile, timeoutNanos, null));
			assertEquals(3, seen.size());
			assertEquals(PROXY_SESSION, seen.get(2).session(), "a write the caller did not number is numbered by the proxy");
			assertEquals(2, counter.get());
		} finally {
			if (channel != null) {
				channel.shutdownNow();
			}
			if (proxy != null) {
				proxy.close();
			}
			if (proxyClient != null) {
				proxyClient.close();
			}
			upstream.shutdownNow();
			upstream.awaitTermination(5, TimeUnit.SECONDS);
		}
	}

	private static MergeRequest merge(WorkloadProfile profile, long timeoutNanos, ClientRequestId id) {
		var context = RequestContext.newBuilder().setProfile(profile)
				.setWorkloadContractVersion(RockserverCapabilities.REQUIRED_WORKLOAD_CONTRACT_VERSION)
				.setTimeoutNanos(timeoutNanos);
		if (id != null) {
			context.setRequestId(id);
		}
		return MergeRequest.newBuilder().setTransactionOrUpdateId(0).setColumnId(1)
				.setData(KV.newBuilder().addKeys(ByteString.copyFrom(new byte[] {1}))
						.setValue(ByteString.copyFrom(new byte[] {1})))
				.setContext(context).build();
	}
}
