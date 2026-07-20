package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import com.google.protobuf.ByteString;
import io.grpc.CallOptions;
import io.grpc.Channel;
import io.grpc.ClientCall;
import io.grpc.ClientInterceptor;
import io.grpc.ClientInterceptors;
import io.grpc.ForwardingClientCall;
import io.grpc.ForwardingClientCallListener;
import io.grpc.ManagedChannel;
import io.grpc.Metadata;
import io.grpc.MethodDescriptor;
import io.grpc.Status;
import io.grpc.netty.NettyChannelBuilder;
import io.netty.channel.EventLoopGroup;
import io.netty.channel.epoll.Epoll;
import io.netty.channel.epoll.EpollDomainSocketChannel;
import io.netty.channel.epoll.EpollEventLoopGroup;
import io.netty.channel.unix.DomainSocketAddress;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.GrpcConnection;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.api.proto.GetRequest;
import it.cavallium.rockserver.core.common.api.proto.GetResponse;
import it.cavallium.rockserver.core.common.api.proto.RocksDBServiceGrpc;
import it.cavallium.rockserver.core.server.GrpcServer;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.net.Inet4Address;
import java.net.Inet6Address;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.NetworkInterface;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Collections;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Assumptions;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.Timeout;
import org.junit.jupiter.api.io.TempDir;
import org.junit.jupiter.api.parallel.ResourceLock;
import org.junit.jupiter.api.parallel.Resources;

@Timeout(180)
@ResourceLock(Resources.SYSTEM_PROPERTIES)
class GrpcFastGetTest {

	private static final int[] CORRECTNESS_SIZES = {
			0, 1, 32, 256, 1_024, 4_096, 65_536, 2 * 1_024 * 1_024
	};
	private static final Metadata.Key<String> GRPC_ENCODING = Metadata.Key.of(
			"grpc-encoding", Metadata.ASCII_STRING_MARSHALLER);

	@TempDir
	Path tempDir;

	@Test
	void stockClientDecodesLegacyHeapPinnedAndAutomaticResponses() throws Exception {
		String previousStrategy = System.getProperty("rockserver.grpc.fast-get.strategy");
		try (var embedded = openEmbedded("all-strategies")) {
			long columnId = populate(embedded);
			for (String strategy : new String[]{"legacy", "exact-heap", "pinned", "automatic"}) {
				System.setProperty("rockserver.grpc.fast-get.strategy", strategy);
				try (var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
					server.start();
					try (var client = ClientHandle.forTcp("127.0.0.1", server.getPort())) {
						assertAllValues(client.stub(), columnId);
						assertTrue(client.encoding().get() == null
								|| client.encoding().get().equals("identity"));
					}
				}
			}
			assertEquals(0, embedded.getInternalDB().getPendingOpsCount());
		} finally {
			restoreProperty("rockserver.grpc.fast-get.strategy", previousStrategy);
		}
	}

	@Test
	void stockClientDecodesAutomaticResponsesOverUnixSocket() throws Exception {
		Assumptions.assumeTrue(Epoll.isAvailable(),
				() -> "Epoll unavailable: " + Epoll.unavailabilityCause());
		String previousStrategy = System.getProperty("rockserver.grpc.fast-get.strategy");
		Path socket = tempDir.resolve("fast-get.sock");
		try (var embedded = openEmbedded("unix")) {
			long columnId = populate(embedded);
			System.setProperty("rockserver.grpc.fast-get.strategy", "automatic");
			try (var server = new GrpcServer(embedded, new DomainSocketAddress(socket.toFile()))) {
				server.start();
				try (var client = ClientHandle.forUnix(socket)) {
					assertAllValues(client.stub(), columnId);
					assertTrue(client.encoding().get() == null
							|| client.encoding().get().equals("identity"));
				}
			}
			assertEquals(0, embedded.getInternalDB().getPendingOpsCount());
		} finally {
			restoreProperty("rockserver.grpc.fast-get.strategy", previousStrategy);
			Files.deleteIfExists(socket);
		}
	}

	@Test
	void stockClientDecodesAutomaticResponsesOverIpv6Loopback() throws Exception {
		InetAddress loopback = InetAddress.getByName("::1");
		Assumptions.assumeTrue(loopback instanceof Inet6Address
				&& NetworkInterface.getByInetAddress(loopback) != null,
				"IPv6 loopback is unavailable");
		String previousStrategy = System.getProperty("rockserver.grpc.fast-get.strategy");
		try (var embedded = openEmbedded("ipv6")) {
			long columnId = populate(embedded);
			System.setProperty("rockserver.grpc.fast-get.strategy", "automatic");
			try (var server = new GrpcServer(embedded, new InetSocketAddress(loopback, 0))) {
				server.start();
				try (var client = ClientHandle.forTcp(loopback.getHostAddress(), server.getPort())) {
					assertAllValues(client.stub(), columnId);
					assertTrue(client.encoding().get() == null
							|| client.encoding().get().equals("identity"));
				}
			}
			assertEquals(0, embedded.getInternalDB().getPendingOpsCount());
		} finally {
			restoreProperty("rockserver.grpc.fast-get.strategy", previousStrategy);
		}
	}

	@Test
	void customBindingPreservesProxyAndTransactionFallbacks() throws Exception {
		String previousStrategy = System.getProperty("rockserver.grpc.fast-get.strategy");
		try (var embedded = openEmbedded("fallbacks")) {
			long columnId = populate(embedded);
			var api = embedded.getSyncApi();
			long transactionId = api.openTransaction(TimeUnit.MINUTES.toMillis(1));
			byte[] transactionValue = value(42, 513);
			api.put(transactionId, columnId, keys(42), Buf.wrap(transactionValue), RequestType.none());
			System.setProperty("rockserver.grpc.fast-get.strategy", "automatic");
			try (var upstream = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
				upstream.start();
				try (var proxyConnection = GrpcConnection.forHostAndPort("fast-get-proxy",
						new Utils.HostAndPort("127.0.0.1", upstream.getPort()));
						var proxy = new GrpcServer(proxyConnection, new InetSocketAddress("127.0.0.1", 0))) {
					proxy.start();
					try (var client = ClientHandle.forTcp("127.0.0.1", proxy.getPort())) {
						assertAllValues(client.stub(), columnId);
						GetResponse transactional = client.stub().get(request(columnId, 42, transactionId));
						assertTrue(transactional.hasValue());
						assertArrayEquals(transactionValue, transactional.getValue().toByteArray());
					}
				}
			} finally {
				api.closeTransaction(transactionId, false);
			}
			assertEquals(0, embedded.getInternalDB().getPendingOpsCount());
		} finally {
			restoreProperty("rockserver.grpc.fast-get.strategy", previousStrategy);
		}
	}

	@Test
	void remotePeerUsesGzipAndStockClientDecodesPinnedResponse() throws Exception {
		InetAddress remoteAddress = findNonLoopbackIpv4();
		Assumptions.assumeTrue(remoteAddress != null, "No routable non-loopback IPv4 address is available");
		String previousStrategy = System.getProperty("rockserver.grpc.fast-get.strategy");
		try (var embedded = openEmbedded("gzip")) {
			long columnId = populate(embedded);
			System.setProperty("rockserver.grpc.fast-get.strategy", "pinned");
			try (var server = new GrpcServer(embedded, new InetSocketAddress("0.0.0.0", 0))) {
				server.start();
				try (var client = ClientHandle.forTcp(remoteAddress.getHostAddress(), server.getPort())) {
					assertValue(client.stub(), columnId, CORRECTNESS_SIZES.length - 1,
							CORRECTNESS_SIZES[CORRECTNESS_SIZES.length - 1]);
					assertEquals("gzip", client.encoding().get());
				}
			}
			assertEquals(0, embedded.getInternalDB().getPendingOpsCount());
		} finally {
			restoreProperty("rockserver.grpc.fast-get.strategy", previousStrategy);
		}
	}

	@Test
	void cancellationDuringLargePinnedResponseReleasesDatabaseAndColumnLeases() throws Exception {
		String previousStrategy = System.getProperty("rockserver.grpc.fast-get.strategy");
		try (var embedded = openEmbedded("cancel")) {
			var api = embedded.getSyncApi();
			long columnId = api.createColumn("cancel",
					ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
			int valueSize = 32 * 1024 * 1024;
			api.put(0, columnId, keys(7), Buf.wrap(value(7, valueSize)), RequestType.none());
			System.setProperty("rockserver.grpc.fast-get.strategy", "pinned");

			try (var server = new GrpcServer(embedded, new InetSocketAddress("127.0.0.1", 0))) {
				server.start();
				try (var client = ClientHandle.forTcp("127.0.0.1", server.getPort())) {
					CountDownLatch headersReceived = new CountDownLatch(1);
					CountDownLatch closed = new CountDownLatch(1);
					AtomicReference<ClientCall<GetRequest, GetResponse>> callRef = new AtomicReference<>();
					ClientCall<GetRequest, GetResponse> call = client.channel().newCall(
							RocksDBServiceGrpc.getGetMethod(), CallOptions.DEFAULT);
					callRef.set(call);
					call.start(new ClientCall.Listener<>() {
						@Override
						public void onHeaders(Metadata headers) {
							headersReceived.countDown();
							callRef.get().cancel("test cancellation during response framing", null);
						}

						@Override
						public void onClose(Status status, Metadata trailers) {
							closed.countDown();
						}
					}, new Metadata());
					call.request(1);
					call.sendMessage(request(columnId, 7));
					call.halfClose();

					assertTrue(headersReceived.await(10, TimeUnit.SECONDS));
					assertTrue(closed.await(10, TimeUnit.SECONDS));
					awaitPendingOperations(embedded, 0);
					api.deleteColumn(columnId);
				}
			}
		} finally {
			restoreProperty("rockserver.grpc.fast-get.strategy", previousStrategy);
		}
	}

	private EmbeddedConnection openEmbedded(String name) throws Exception {
		Path config = tempDir.resolve(name + ".conf");
		Files.writeString(config, """
				database: {
				  global: {
				    enable-fast-get: true
				    ingest-behind: false
				    optimistic: false
				    use-direct-io: false
				    maximum-open-files: -1
				    fallback-column-options: { cache-index-and-filter-blocks: true }
				  }
				}
				""");
		return new EmbeddedConnection(tempDir.resolve(name + "-db"), name, config);
	}

	private static long populate(EmbeddedConnection embedded) {
		var api = embedded.getSyncApi();
		long columnId = api.createColumn("fast-get",
				ColumnSchema.of(IntList.of(Long.BYTES), ObjectList.of(), true));
		for (int index = 0; index < CORRECTNESS_SIZES.length; index++) {
			api.put(0,
					columnId,
					keys(index),
					Buf.wrap(value(index, CORRECTNESS_SIZES[index])),
					RequestType.none());
		}
		return columnId;
	}

	private static void assertAllValues(RocksDBServiceGrpc.RocksDBServiceBlockingStub stub, long columnId) {
		GetResponse missing = stub.get(request(columnId, Long.MAX_VALUE));
		assertFalse(missing.hasValue());
		for (int index = 0; index < CORRECTNESS_SIZES.length; index++) {
			assertValue(stub, columnId, index, CORRECTNESS_SIZES[index]);
		}
	}

	private static void assertValue(RocksDBServiceGrpc.RocksDBServiceBlockingStub stub,
			long columnId,
			long key,
			int size) {
		GetResponse response = stub.get(request(columnId, key));
		assertTrue(response.hasValue(), "an empty value must remain distinct from a missing key");
		assertArrayEquals(value(key, size), response.getValue().toByteArray());
	}

	private static GetRequest request(long columnId, long key) {
		return request(columnId, key, 0);
	}

	private static GetRequest request(long columnId, long key, long transactionId) {
		return GetRequest.newBuilder()
				.setTransactionOrUpdateId(transactionId)
				.setColumnId(columnId)
				.addKeys(ByteString.copyFrom(keyBytes(key)))
				.build();
	}

	private static Keys keys(long key) {
		return new Keys(Buf.wrap(keyBytes(key)));
	}

	private static byte[] keyBytes(long key) {
		return ByteBuffer.allocate(Long.BYTES).putLong(key).array();
	}

	private static byte[] value(long key, int size) {
		byte[] value = new byte[size];
		for (int index = 0; index < value.length; index++) {
			value[index] = (byte) (key * 31 + index * 17);
		}
		return value;
	}

	private static InetAddress findNonLoopbackIpv4() throws Exception {
		for (NetworkInterface networkInterface : Collections.list(NetworkInterface.getNetworkInterfaces())) {
			if (!networkInterface.isUp()) {
				continue;
			}
			for (InetAddress address : Collections.list(networkInterface.getInetAddresses())) {
				if (address instanceof Inet4Address && !address.isLoopbackAddress()) {
					return address;
				}
			}
		}
		return null;
	}

	private static void restoreProperty(String name, String value) {
		if (value == null) {
			System.clearProperty(name);
		} else {
			System.setProperty(name, value);
		}
	}

	private static void awaitPendingOperations(EmbeddedConnection embedded, long expected) throws Exception {
		long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
		do {
			if (embedded.getInternalDB().getPendingOpsCount() == expected) {
				return;
			}
			Thread.sleep(10);
		} while (System.nanoTime() < deadline);
		assertEquals(expected, embedded.getInternalDB().getPendingOpsCount());
	}

	private record ClientHandle(ManagedChannel channel,
			@org.jetbrains.annotations.Nullable EventLoopGroup eventLoop,
			RocksDBServiceGrpc.RocksDBServiceBlockingStub stub,
			AtomicReference<String> encoding) implements AutoCloseable {

		private static ClientHandle forTcp(String host, int port) {
			return build(NettyChannelBuilder.forAddress(host, port), null);
		}

		private static ClientHandle forUnix(Path socket) {
			var eventLoop = new EpollEventLoopGroup(1);
			return build(NettyChannelBuilder.forAddress(new DomainSocketAddress(socket.toFile()))
					.eventLoopGroup(eventLoop)
					.channelType(EpollDomainSocketChannel.class), eventLoop);
		}

		private static ClientHandle build(NettyChannelBuilder builder,
				@org.jetbrains.annotations.Nullable EventLoopGroup eventLoop) {
			AtomicReference<String> encoding = new AtomicReference<>();
			ManagedChannel channel = builder.directExecutor()
					.usePlaintext()
					.maxInboundMessageSize(64 * 1024 * 1024)
					.build();
			Channel intercepted = ClientInterceptors.intercept(channel, new EncodingInterceptor(encoding));
			return new ClientHandle(channel, eventLoop, RocksDBServiceGrpc.newBlockingStub(intercepted), encoding);
		}

		@Override
		public void close() throws InterruptedException {
			channel.shutdownNow();
			channel.awaitTermination(10, TimeUnit.SECONDS);
			if (eventLoop != null) {
				eventLoop.shutdownGracefully(0, 5, TimeUnit.SECONDS).sync();
			}
		}
	}

	private record EncodingInterceptor(AtomicReference<String> encoding) implements ClientInterceptor {

		@Override
		public <ReqT, RespT> ClientCall<ReqT, RespT> interceptCall(MethodDescriptor<ReqT, RespT> method,
				CallOptions callOptions,
				Channel next) {
			return new ForwardingClientCall.SimpleForwardingClientCall<>(next.newCall(method, callOptions)) {
				@Override
				public void start(Listener<RespT> responseListener, Metadata headers) {
					super.start(new ForwardingClientCallListener.SimpleForwardingClientCallListener<>(responseListener) {
						@Override
						public void onHeaders(Metadata headers) {
							encoding.set(headers.get(GRPC_ENCODING));
							super.onHeaders(headers);
						}
					}, headers);
				}
			};
		}
	}
}
