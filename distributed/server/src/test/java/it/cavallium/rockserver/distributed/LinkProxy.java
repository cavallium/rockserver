package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * TCP proxy for one directed link: accepts connections on a listen address and forwards each one to a target.
 *
 * <p>"Directed" refers to who opens the connection: bytes flow both ways on every forwarded connection, so
 * blocking the link {@code A -> B} cuts the connections A opened to B (including B's replies on them) but not the
 * ones B opened to A.</p>
 *
 * <p>{@link #block()} resets every forwarded connection and resets each new one right after accepting it. A real
 * partition is a blackhole (packets vanish and the peer only notices through timeouts); this proxy instead makes
 * the link fail immediately. Raft's safety does not depend on timing, so the two differ only in how quickly the
 * failure is detected, and fast detection keeps tests short. The listening socket stays bound while blocked, so
 * {@link #unblock()} can never fail to rebind the port.</p>
 *
 * <p>Half-closes are forwarded: when one side shuts down its output, the proxy shuts down the output towards the
 * other side and keeps forwarding the opposite direction. A read or write error on either side resets both.</p>
 */
public final class LinkProxy implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(LinkProxy.class);
	private static final int CONNECT_TIMEOUT_MILLIS = 5_000;
	private static final int BUFFER_SIZE = 16 * 1024;

	private final ServerSocket server;
	private final InetSocketAddress target;
	private final Set<Connection> connections = ConcurrentHashMap.newKeySet();
	private final Thread acceptor;
	private volatile boolean blocked;
	private volatile boolean closed;

	/** Binds {@code listen} (port 0 picks a free port) and starts forwarding to {@code target}. */
	public LinkProxy(InetSocketAddress listen, InetSocketAddress target) throws IOException {
		this.target = target;
		this.server = new ServerSocket();
		try {
			server.setReuseAddress(true);
			server.bind(listen, 128);
		} catch (IOException e) {
			server.close();
			throw e;
		}
		this.acceptor = Thread.ofVirtual()
				.name("link-proxy-" + server.getLocalSocketAddress() + "->" + target)
				.start(this::acceptLoop);
	}

	public InetSocketAddress listenAddress() {
		return (InetSocketAddress) server.getLocalSocketAddress();
	}

	public InetSocketAddress target() {
		return target;
	}

	/** Resets every open connection and every future one, until {@link #unblock()}. */
	public void block() {
		blocked = true;
		connections.forEach(Connection::reset);
	}

	/** Forwards new connections again. Connections reset while blocked stay closed. */
	public void unblock() {
		blocked = false;
	}

	public boolean isBlocked() {
		return blocked;
	}

	@Override
	public void close() {
		closed = true;
		try {
			server.close();
		} catch (IOException e) {
			LOG.debug("Failed to close {}", server, e);
		}
		connections.forEach(Connection::reset);
		try {
			acceptor.join();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public String toString() {
		return "LinkProxy[" + listenAddress() + " -> " + target + (blocked ? ", blocked" : "") + "]";
	}

	private void acceptLoop() {
		while (!closed) {
			Socket client;
			try {
				client = server.accept();
			} catch (IOException e) {
				if (!closed) {
					LOG.warn("{} stopped accepting connections", this, e);
				}
				return;
			}
			var connection = new Connection(client);
			// Register before checking the flag: block() sets the flag before resetting registered connections,
			// so every connection is either seen by block() or sees the flag here.
			connections.add(connection);
			if (blocked || closed) {
				connection.reset();
			} else {
				Thread.ofVirtual().name("link-proxy-connection").start(connection::forward);
			}
		}
	}

	/** One forwarded connection: the accepted client socket and the socket towards the target. */
	private final class Connection {

		private final Socket client;
		private final Socket upstream = new Socket();

		Connection(Socket client) {
			this.client = client;
		}

		void forward() {
			try {
				client.setTcpNoDelay(true);
				upstream.setTcpNoDelay(true);
				upstream.connect(target, CONNECT_TIMEOUT_MILLIS);
			} catch (IOException e) {
				// The target is down (or block() closed the socket): fail the client side just as abruptly.
				reset();
				return;
			}
			var toTarget = Thread.ofVirtual().name("link-proxy-upstream").start(() -> pump(client, upstream));
			pump(upstream, client);
			try {
				toTarget.join();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				reset();
			}
			closeQuietly(client);
			closeQuietly(upstream);
			connections.remove(this);
		}

		/** Copies {@code from} to {@code to} until end of stream, then half-closes {@code to}. */
		private void pump(Socket from, Socket to) {
			var buffer = new byte[BUFFER_SIZE];
			try {
				InputStream in = from.getInputStream();
				OutputStream out = to.getOutputStream();
				int read;
				while ((read = in.read(buffer)) >= 0) {
					out.write(buffer, 0, read);
				}
				to.shutdownOutput();
			} catch (IOException e) {
				reset();
			}
		}

		/** Closes both sockets with a TCP reset (linger 0), so both peers notice at once. */
		void reset() {
			resetQuietly(client);
			resetQuietly(upstream);
			connections.remove(this);
		}

		private static void resetQuietly(Socket socket) {
			try {
				if (!socket.isClosed()) {
					socket.setSoLinger(true, 0);
				}
			} catch (IOException ignored) {
				// Already closed by another thread.
			}
			closeQuietly(socket);
		}

		private static void closeQuietly(Socket socket) {
			try {
				socket.close();
			} catch (IOException ignored) {
				// Nothing left to release.
			}
		}
	}
}
