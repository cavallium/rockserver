package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.IOException;
import java.io.InputStream;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketTimeoutException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.Duration;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class LinkProxyTest {

	private static final InetAddress LOOPBACK = InetAddress.getLoopbackAddress();
	/** Upper bound for "promptly": far below any TCP or test timeout, far above scheduling noise. */
	private static final Duration PROMPT = Duration.ofSeconds(2);
	private static final int READ_TIMEOUT_MILLIS = 10_000;

	@Test
	void forwardsBytesBothWays() throws Exception {
		try (var echo = new EchoServer(new InetSocketAddress(LOOPBACK, 0));
				var proxy = new LinkProxy(new InetSocketAddress(LOOPBACK, 0), echo.address());
				var socket = connect(proxy.listenAddress())) {
			var payload = new byte[4 * 1024 * 1024];
			new Random(42).nextBytes(payload);
			// Write and read concurrently: the payload is larger than the socket buffers.
			var writer = CompletableFuture.runAsync(() -> {
				try {
					socket.getOutputStream().write(payload);
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
			var received = socket.getInputStream().readNBytes(payload.length);
			writer.get(10, TimeUnit.SECONDS);
			assertArrayEquals(payload, received);
		}
	}

	@Test
	void forwardsHalfClose() throws Exception {
		// The server answers only after the client finished sending, then closes.
		try (var server = new ServerSocket(0, 50, LOOPBACK);
				var proxy = new LinkProxy(new InetSocketAddress(LOOPBACK, 0), localAddress(server));
				var socket = connect(proxy.listenAddress())) {
			var serverSide = CompletableFuture.runAsync(() -> {
				try (var accepted = server.accept()) {
					var request = accepted.getInputStream().readAllBytes();
					accepted.getOutputStream().write(("got " + new String(request, StandardCharsets.UTF_8))
							.getBytes(StandardCharsets.UTF_8));
				} catch (IOException e) {
					throw new RuntimeException(e);
				}
			});
			socket.getOutputStream().write("request".getBytes(StandardCharsets.UTF_8));
			socket.shutdownOutput();
			var response = socket.getInputStream().readAllBytes();
			serverSide.get(10, TimeUnit.SECONDS);
			assertEquals("got request", new String(response, StandardCharsets.UTF_8));
		}
	}

	@Test
	void blockCutsConnectionsUntilUnblocked() throws Exception {
		try (var echo = new EchoServer(new InetSocketAddress(LOOPBACK, 0));
				var proxy = new LinkProxy(new InetSocketAddress(LOOPBACK, 0), echo.address());
				var existing = connect(proxy.listenAddress())) {
			assertEchoes(existing, "before");
			var serverSideEnded = echo.nextConnectionEnd();

			proxy.block();
			assertCutPromptly(existing);
			serverSideEnded.get(PROMPT.toMillis(), TimeUnit.MILLISECONDS);
			try (var duringBlock = new Socket()) {
				// Accepted and reset at once, or refused: either way no byte gets through.
				try {
					duringBlock.connect(proxy.listenAddress(), READ_TIMEOUT_MILLIS);
					duringBlock.setSoTimeout(READ_TIMEOUT_MILLIS);
					duringBlock.getOutputStream().write("lost".getBytes(StandardCharsets.UTF_8));
				} catch (IOException expected) {
					// Reset before or while writing.
				}
				assertCutPromptly(duringBlock);
			}
			assertEquals(1, echo.connections(), "a connection reached the server while blocked");

			proxy.unblock();
			try (var after = connect(proxy.listenAddress())) {
				assertEchoes(after, "after");
			}
		}
	}

	@Test
	void resetsClientWhenTargetIsDown() throws Exception {
		InetSocketAddress deadTarget;
		try (var server = new ServerSocket(0, 50, LOOPBACK)) {
			deadTarget = localAddress(server);
		}
		try (var proxy = new LinkProxy(new InetSocketAddress(LOOPBACK, 0), deadTarget);
				var socket = connect(proxy.listenAddress())) {
			assertCutPromptly(socket);
		}
	}

	/** The {@link Network} relies on this: Linux lets 127.0.0.k:port and 127.0.0.1:port be bound by two sockets. */
	@Test
	void loopbackAliasBindsSamePortAsLoopback() throws Exception {
		var alias = InetAddress.getByName("127.0.0.10");
		try (var direct = new EchoServer(new InetSocketAddress(LOOPBACK, 0));
				var proxy = new LinkProxy(new InetSocketAddress(alias, direct.address().getPort()), direct.address());
				var viaAlias = connect(proxy.listenAddress());
				var viaLoopback = connect(direct.address())) {
			assertEquals(direct.address().getPort(), proxy.listenAddress().getPort());
			assertEchoes(viaAlias, "alias");
			assertEchoes(viaLoopback, "loopback");
			assertEquals(alias, viaAlias.getInetAddress());

			proxy.block();
			assertCutPromptly(viaAlias);
			assertEchoes(viaLoopback, "loopback still works");
		}
	}

	@Test
	void networkCutsOnlyTheLinksOfIsolatedNode(@TempDir Path dir) throws Exception {
		var servers = new LinkedHashMap<String, EchoServer>();
		try {
			for (var id : List.of("1", "2", "3")) {
				servers.put(id, new EchoServer(new InetSocketAddress(LOOPBACK, 0)));
			}
			var ports = new LinkedHashMap<String, List<Integer>>();
			servers.forEach((id, server) -> ports.put(id, List.of(server.address().getPort())));
			try (var network = Network.start(ports, dir)) {
				assertAllLinks(network, servers, Set.of());

				network.isolate("1");
				assertAllLinks(network, servers, Set.of("1->2", "1->3", "2->1", "3->1"));

				network.heal();
				network.partition(Set.of("1", "2"), Set.of("3"));
				assertAllLinks(network, servers, Set.of("1->3", "2->3", "3->1", "3->2"));

				network.heal();
				network.blockLink("2", "3");
				assertAllLinks(network, servers, Set.of("2->3"));

				network.heal();
				assertAllLinks(network, servers, Set.of());
			}
		} finally {
			servers.values().forEach(EchoServer::close);
		}
	}

	/** Resolves names in a JVM started the way {@link ProcessCluster} starts nodes. */
	@Test
	void hostsFileRoutesPeerNamesThroughLinks(@TempDir Path dir) throws Exception {
		var ports = new LinkedHashMap<String, List<Integer>>();
		for (var id : List.of("1", "2", "3")) {
			ports.put(id, List.of(freePort()));
		}
		try (var network = Network.start(ports, dir)) {
			var process = new ProcessBuilder(Path.of(System.getProperty("java.home"), "bin", "java").toString(),
					"-cp", System.getProperty("java.class.path"),
					"-Djdk.net.hosts.file=" + network.hostsFile("2"),
					Resolve.class.getName(), "node-1", "node-2", "node-3", "localhost")
					.redirectErrorStream(true)
					.start();
			var output = new String(process.getInputStream().readAllBytes(), StandardCharsets.UTF_8);
			assertTrue(process.waitFor(30, TimeUnit.SECONDS), "resolver JVM did not exit");
			assertEquals(0, process.exitValue(), output);
			var expected = "node-1=" + network.linkAddress("2", "1").getHostAddress() + "\n"
					+ "node-2=127.0.0.1\n"
					+ "node-3=" + network.linkAddress("2", "3").getHostAddress() + "\n"
					+ "localhost=127.0.0.1\n";
			assertEquals(expected, output);
			assertTrue(Files.readString(network.hostsFile("2")).contains("node-3"));
		}
	}

	/** Entry point of the resolver JVM: prints {@code name=address} per argument. */
	public static final class Resolve {
		public static void main(String[] args) throws IOException {
			for (var name : args) {
				System.out.print(name + "=" + InetAddress.getByName(name).getHostAddress() + "\n");
			}
		}
	}

	private static void assertAllLinks(Network network, Map<String, EchoServer> servers, Set<String> blocked)
			throws IOException {
		for (var src : servers.keySet()) {
			for (var dst : servers.keySet()) {
				if (src.equals(dst)) {
					continue;
				}
				var link = src + "->" + dst;
				int port = servers.get(dst).address().getPort();
				var address = new InetSocketAddress(network.linkAddress(src, dst), port);
				try (var socket = new Socket()) {
					if (blocked.contains(link)) {
						try {
							socket.connect(address, READ_TIMEOUT_MILLIS);
						} catch (IOException expected) {
							continue;
						}
						assertCutPromptly(socket);
					} else {
						socket.connect(address, READ_TIMEOUT_MILLIS);
						socket.setSoTimeout(READ_TIMEOUT_MILLIS);
						assertEchoes(socket, link);
					}
				}
			}
		}
	}

	private static void assertEchoes(Socket socket, String message) throws IOException {
		var bytes = (message + "\n").getBytes(StandardCharsets.UTF_8);
		socket.getOutputStream().write(bytes);
		assertArrayEquals(bytes, socket.getInputStream().readNBytes(bytes.length), "echo of " + message);
	}

	/** The connection ends (EOF or reset) well before the read timeout. */
	private static void assertCutPromptly(Socket socket) throws IOException {
		long start = System.nanoTime();
		socket.setSoTimeout(READ_TIMEOUT_MILLIS);
		try {
			InputStream in = socket.getInputStream();
			int read;
			do {
				read = in.read();
			} while (read >= 0);
		} catch (SocketTimeoutException e) {
			throw new AssertionError("Connection still open after " + READ_TIMEOUT_MILLIS + " ms", e);
		} catch (IOException expected) {
			// Reset.
		}
		var elapsed = Duration.ofNanos(System.nanoTime() - start);
		assertTrue(elapsed.compareTo(PROMPT) < 0, "Connection cut after " + elapsed);
	}

	private static Socket connect(InetSocketAddress address) throws IOException {
		var socket = new Socket();
		socket.connect(address, READ_TIMEOUT_MILLIS);
		socket.setSoTimeout(READ_TIMEOUT_MILLIS);
		return socket;
	}

	private static InetSocketAddress localAddress(ServerSocket server) {
		return new InetSocketAddress(server.getInetAddress(), server.getLocalPort());
	}

	private static int freePort() throws IOException {
		try (var socket = new ServerSocket(0, 1, LOOPBACK)) {
			return socket.getLocalPort();
		}
	}

	/** Echoes every byte back on every accepted connection, and tells when each connection ends. */
	private static final class EchoServer implements AutoCloseable {

		private final ServerSocket server;
		private final LinkedBlockingQueue<CompletableFuture<Void>> ends = new LinkedBlockingQueue<>();
		private final Set<Socket> open = ConcurrentHashMap.newKeySet();
		private final AtomicInteger connections = new AtomicInteger();

		EchoServer(InetSocketAddress address) throws IOException {
			server = new ServerSocket();
			server.bind(address);
			Thread.ofVirtual().start(this::acceptLoop);
		}

		InetSocketAddress address() {
			return localAddress(server);
		}

		int connections() {
			return connections.get();
		}

		/** Completes when the server side of the next accepted connection sees EOF or a reset. */
		CompletableFuture<Void> nextConnectionEnd() throws InterruptedException {
			var end = ends.poll(READ_TIMEOUT_MILLIS, TimeUnit.MILLISECONDS);
			if (end == null) {
				throw new AssertionError("No connection reached the server");
			}
			return end;
		}

		private void acceptLoop() {
			while (!server.isClosed()) {
				try {
					var socket = server.accept();
					connections.incrementAndGet();
					open.add(socket);
					var end = new CompletableFuture<Void>();
					ends.add(end);
					Thread.ofVirtual().start(() -> echo(socket, end));
				} catch (IOException e) {
					return;
				}
			}
		}

		private void echo(Socket socket, CompletableFuture<Void> end) {
			try (socket) {
				socket.getInputStream().transferTo(socket.getOutputStream());
			} catch (IOException ignored) {
				// The test cut the connection.
			} finally {
				open.remove(socket);
				end.complete(null);
			}
		}

		@Override
		public void close() {
			try {
				server.close();
			} catch (IOException ignored) {
				// Nothing to release.
			}
			open.forEach(socket -> {
				try {
					socket.close();
				} catch (IOException ignored) {
					// Already closed.
				}
			});
		}
	}
}
