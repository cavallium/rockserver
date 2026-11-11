package it.cavallium.rockserver.distributed;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.net.BindException;
import java.net.ConnectException;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.time.Duration;
import java.util.Arrays;
import java.util.Set;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import org.jetbrains.annotations.Nullable;
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
 *
 * <p>Besides cutting it, a link can be made slow or silent, both ways at once:</p>
 * <ul>
 *   <li>{@link #shape} delays every chunk of bytes by a fixed latency plus a random jitter, and caps the bandwidth.
 *   Bytes keep their order, as on a TCP connection: a chunk never overtakes an earlier one, so jitter shows up as
 *   bursts, not as reordering (reordering below TCP is invisible to the peers).</li>
 *   <li>{@link #blackhole} stops forwarding without failing anything: open connections stay open and new ones are
 *   accepted but go nowhere, like a path that drops every packet. TCP retransmits for minutes before giving up, so
 *   the bytes are held, not lost, and flow again after {@link #unblock()}, unless an endpoint's own timeout (a Raft
 *   heartbeat, a gRPC deadline or keep-alive) closed the connection first. The proxy reads at most
 *   {@value #MAX_QUEUED_CHUNKS} chunks per direction meanwhile, then stops reading, so the sender's buffers fill
 *   up as they would.</li>
 * </ul>
 *
 * <p>A connection to a target that is down is accepted and then reset, which a client cannot tell from a connection
 * reset after its request was forwarded. For clients that must know whether a request may have arrived (test clients
 * classifying a write as certainly failed), {@link #refuseWhileTargetDown()} makes the proxy behave like the dead
 * target instead: it closes its listening socket ({@link #refuseConnections()}), so connections are refused, and binds
 * the same port again once the target accepts connections ({@link #acceptWhenTargetUp()}).</p>
 */
public final class LinkProxy implements AutoCloseable {

	private static final Logger LOG = LoggerFactory.getLogger(LinkProxy.class);
	private static final int CONNECT_TIMEOUT_MILLIS = 5_000;
	private static final int BUFFER_SIZE = 16 * 1024;
	/** Chunks read but not yet forwarded, per direction of a connection, before the reader stops reading. */
	private static final int MAX_QUEUED_CHUNKS = 256;
	private static final long POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(50);
	private static final long TARGET_PROBE_MILLIS = 50;

	/**
	 * How a {@link #shape shaped} link forwards: each chunk after {@code delay} plus a uniform random part of
	 * {@code jitter}, and at most {@code bytesPerSecond} per direction over all connections (0 = unlimited).
	 */
	public record Shaping(Duration delay, Duration jitter, long bytesPerSecond) {

		public static final Shaping NONE = new Shaping(Duration.ZERO, Duration.ZERO, 0);

		public Shaping {
			if (delay.isNegative() || jitter.isNegative() || bytesPerSecond < 0) {
				throw new IllegalArgumentException("Negative shaping: " + delay + ", " + jitter + ", " + bytesPerSecond);
			}
		}

		public static Shaping latency(Duration delay, Duration jitter) {
			return new Shaping(delay, jitter, 0);
		}

		public static Shaping bandwidth(long bytesPerSecond) {
			return new Shaping(Duration.ZERO, Duration.ZERO, bytesPerSecond);
		}

		boolean isNone() {
			return delay.isZero() && jitter.isZero() && bytesPerSecond == 0;
		}
	}

	/** Null while connections are refused ({@link #refuseConnections()}). Changed under {@link #listening}. */
	private volatile @Nullable ServerSocket server;
	/** The address bound first, bound again by {@link #acceptConnections()}. */
	private final InetSocketAddress listenAddress;
	/** Guards binding and closing {@link #server}; the acceptor waits on it while connections are refused. */
	private final Object listening = new Object();
	private volatile boolean refuseWhileTargetDown;
	/** Set while a thread waits for the target to accept connections again ({@link #acceptWhenTargetUp()}). */
	private final AtomicBoolean watchingTarget = new AtomicBoolean();
	/** Calls of {@link #acceptWhenTargetUp()}: one that a running watcher may have missed starts a new watcher. */
	private final AtomicLong watchRequests = new AtomicLong();
	private final InetSocketAddress target;
	private final Set<Connection> connections = ConcurrentHashMap.newKeySet();
	private final Thread acceptor;
	private volatile boolean blocked;
	private volatile boolean dropReplies;
	private volatile boolean blackholed;
	private volatile Shaping shaping = Shaping.NONE;
	/**
	 * Bumped by every change of {@link #shaping} or {@link #blackholed}: a writer holding a chunk recomputes when it
	 * is due, so queued bytes follow the link's current state, not the one they were read under.
	 */
	private volatile long stateGeneration;
	private volatile boolean closed;
	/** Guards nothing: what stalled writers wait on until {@link #unblock()} or a reset. */
	private final Object released = new Object();
	/** When each direction may send its next byte under the bandwidth cap, in {@link System#nanoTime()} units. */
	private final Pacer requestPacer = new Pacer();
	private final Pacer replyPacer = new Pacer();

	/** Binds {@code listen} (port 0 picks a free port) and starts forwarding to {@code target}. */
	public LinkProxy(InetSocketAddress listen, InetSocketAddress target) throws IOException {
		this.target = target;
		var socket = bind(listen);
		this.server = socket;
		this.listenAddress = (InetSocketAddress) socket.getLocalSocketAddress();
		this.acceptor = Thread.ofVirtual()
				.name("link-proxy-" + listenAddress + "->" + target)
				.start(this::acceptLoop);
	}

	private static ServerSocket bind(InetSocketAddress address) throws IOException {
		var socket = new ServerSocket();
		try {
			socket.setReuseAddress(true);
			socket.bind(address, 128);
		} catch (IOException e) {
			socket.close();
			throw e;
		}
		return socket;
	}

	public InetSocketAddress listenAddress() {
		return listenAddress;
	}

	/**
	 * From now on, a connection whose target refuses it makes the proxy refuse connections too, until the target
	 * accepts them again: see the class comment. The connection that found the target down is still reset.
	 */
	public void refuseWhileTargetDown() {
		refuseWhileTargetDown = true;
	}

	/**
	 * Closes the listening socket, so that new connections are refused as by a closed port, until
	 * {@link #acceptConnections()} or {@link #acceptWhenTargetUp()}. Open connections are left alone.
	 */
	public void refuseConnections() {
		synchronized (listening) {
			var socket = server;
			server = null;
			if (socket != null) {
				try {
					socket.close();
				} catch (IOException e) {
					LOG.debug("Failed to close {}", socket, e);
				}
			}
		}
	}

	/** Binds the listening port again after {@link #refuseConnections()}; nothing if it is bound. */
	public void acceptConnections() throws IOException {
		synchronized (listening) {
			if (server == null && !closed) {
				server = bind(listenAddress);
				listening.notifyAll();
			}
		}
	}

	/** Whether the listening socket is bound (see {@link #refuseConnections()}). */
	public boolean isAccepting() {
		return server != null;
	}

	/**
	 * Binds the listening port again, after {@link #refuseConnections()}, as soon as the target accepts a connection
	 * (probed every {@value #TARGET_PROBE_MILLIS} ms by a thread of its own). A call that comes while a watcher is
	 * still running, after that watcher bound the port (a kill right after a restart: the port was closed again), is
	 * not lost: the watcher starts another one as it ends.
	 */
	public void acceptWhenTargetUp() {
		watchRequests.incrementAndGet();
		if (!watchingTarget.compareAndSet(false, true)) {
			return;
		}
		Thread.ofVirtual().name("link-proxy-target-watch-" + target).start(() -> {
			long served = -1;
			try {
				while (!closed) {
					long requests = watchRequests.get();
					if (targetAccepts()) {
						try {
							acceptConnections();
						} catch (BindException e) {
							// The port is not free yet: closing a ServerSocket while the acceptor is blocked in accept()
							// releases the port only once that thread has left it. Probe and bind again.
							LOG.debug("{} cannot bind yet, retrying", this, e);
							Thread.sleep(TARGET_PROBE_MILLIS);
							continue;
						}
						served = requests;
						return;
					}
					Thread.sleep(TARGET_PROBE_MILLIS);
				}
			} catch (IOException e) {
				LOG.error("{} cannot listen again", this, e);
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
			} finally {
				watchingTarget.set(false);
				// Requests after the bind found this watcher still marked running, and returned.
				if (served >= 0 && watchRequests.get() != served && !closed) {
					acceptWhenTargetUp();
				}
			}
		});
	}

	private boolean targetAccepts() {
		try (var probe = new Socket()) {
			probe.connect(target, CONNECT_TIMEOUT_MILLIS);
			return true;
		} catch (IOException e) {
			return false;
		}
	}

	public InetSocketAddress target() {
		return target;
	}

	/** Resets every open connection and every future one, until {@link #unblock()}. */
	public void block() {
		blocked = true;
		connections.forEach(Connection::reset);
	}

	/**
	 * Keeps forwarding to the target but drops what it sends back, on open connections too, until {@link #unblock()}:
	 * requests are carried out and their answers are lost, and neither peer gets an error.
	 */
	public void dropReplies() {
		dropReplies = true;
	}

	/**
	 * Holds every byte in both directions, on open and new connections, without failing anything, until
	 * {@link #unblock()}: see the class comment.
	 */
	public void blackhole() {
		blackholed = true;
		stateGeneration++;
	}

	/**
	 * Delays and throttles both directions from now on, until {@link #unblock()} or another call. Bytes already
	 * queued are held by the new shaping too.
	 */
	public void shape(Shaping shaping) {
		this.shaping = shaping;
		resetPacers();
		stateGeneration++;
		wakeStalled();
	}

	/**
	 * Forwards new connections and replies again, at full speed: bytes held or queued under a blackhole or a shaping
	 * are forwarded at once, in order. Connections reset while blocked stay closed, and
	 * connections whose replies were dropped are reset now: their byte streams lost data in the middle, which TCP
	 * never does, so the peers could not make sense of what follows.
	 */
	public void unblock() {
		blocked = false;
		blackholed = false;
		shaping = Shaping.NONE;
		resetPacers();
		stateGeneration++;
		if (dropReplies) {
			dropReplies = false;
			connections.stream().filter(connection -> connection.lostBytes).forEach(Connection::reset);
		}
		wakeStalled();
	}

	public boolean isBlackholed() {
		return blackholed;
	}

	public Shaping shaping() {
		return shaping;
	}

	public boolean isBlocked() {
		return blocked;
	}

	/** Whether some open connection already lost reply bytes to {@link #dropReplies()}: a hook for tests. */
	boolean hasLostReplies() {
		return connections.stream().anyMatch(connection -> connection.lostBytes);
	}

	@Override
	public void close() {
		closed = true;
		refuseConnections();
		synchronized (listening) {
			listening.notifyAll();
		}
		connections.forEach(Connection::reset);
		wakeStalled();
		try {
			acceptor.join();
		} catch (InterruptedException e) {
			Thread.currentThread().interrupt();
		}
	}

	@Override
	public String toString() {
		return "LinkProxy[" + listenAddress + " -> " + target + (blocked ? ", blocked" : "")
				+ (server == null ? ", refusing" : "")
				+ (blackholed ? ", blackholed" : "") + (shaping.isNone() ? "" : ", " + shaping) + "]";
	}

	private void resetPacers() {
		requestPacer.reset();
		replyPacer.reset();
	}

	private void wakeStalled() {
		synchronized (released) {
			released.notifyAll();
		}
	}

	/** Shares one direction's bandwidth among the connections of the link. */
	private static final class Pacer {

		private long nextFreeNanos = System.nanoTime();

		/** Forgets reservations made under an earlier bandwidth cap. */
		synchronized void reset() {
			nextFreeNanos = System.nanoTime();
		}

		/** When {@code bytes} ready at {@code readyNanos} may start, reserving the time they take. */
		synchronized long reserve(long readyNanos, int bytes, long bytesPerSecond) {
			long start = nextFreeNanos - readyNanos > 0 ? nextFreeNanos : readyNanos;
			nextFreeNanos = start + bytes * 1_000_000_000L / bytesPerSecond;
			return start;
		}
	}

	private void acceptLoop() {
		while (!closed) {
			var socket = server;
			if (socket == null) {
				synchronized (listening) {
					if (server == null && !closed) {
						try {
							listening.wait(TimeUnit.NANOSECONDS.toMillis(POLL_NANOS));
						} catch (InterruptedException e) {
							Thread.currentThread().interrupt();
							return;
						}
					}
				}
				continue;
			}
			Socket client;
			try {
				client = socket.accept();
			} catch (IOException e) {
				if (closed) {
					return;
				}
				if (server != socket) {
					// Closed by refuseConnections().
					continue;
				}
				LOG.warn("{} stopped accepting connections", this, e);
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
		/** Set by {@link #reset}: the threads of this connection stop. */
		private volatile boolean isReset;
		/** Some reply bytes were dropped: the rest of the stream is garbage to the client. */
		private volatile boolean lostBytes;

		Connection(Socket client) {
			this.client = client;
		}

		void forward() {
			// A blackholed link accepts connections but lets nothing through, not even the connection to the target.
			while (blackholed && !isReset && !closed) {
				awaitRelease();
			}
			if (isReset || closed) {
				reset();
				return;
			}
			try {
				client.setTcpNoDelay(true);
				upstream.setTcpNoDelay(true);
				upstream.connect(target, CONNECT_TIMEOUT_MILLIS);
			} catch (IOException e) {
				// The target is down (or block() closed the socket): fail the client side just as abruptly.
				if (refuseWhileTargetDown && e instanceof ConnectException && !isReset && !closed) {
					refuseConnections();
					acceptWhenTargetUp();
				}
				reset();
				return;
			}
			var toTarget = Thread.ofVirtual().name("link-proxy-upstream").start(() -> pump(client, upstream, false));
			pump(upstream, client, true);
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

		/**
		 * Copies {@code from} to {@code to} until end of stream, then half-closes {@code to}. While the link is
		 * neither shaped nor blackholed and nothing is queued, the reading thread writes each chunk itself; otherwise
		 * chunks go through a queue to a writer thread that holds each one until it is due.
		 */
		private void pump(Socket from, Socket to, boolean replies) {
			var direction = new Direction(to, replies ? replyPacer : requestPacer);
			var buffer = new byte[BUFFER_SIZE];
			try {
				InputStream in = from.getInputStream();
				OutputStream out = to.getOutputStream();
				int read;
				while ((read = in.read(buffer)) >= 0) {
					if (replies && dropReplies) {
						lostBytes = true;
						continue;
					}
					var currentShaping = shaping;
					// Only this thread adds chunks, so with none queued the writer is idle and cannot interleave.
					if (direction.queued.get() == 0 && !blackholed && currentShaping.isNone()) {
						out.write(buffer, 0, read);
					} else {
						direction.enqueue(new Chunk(Arrays.copyOf(buffer, read), System.nanoTime(), false));
					}
				}
				direction.enqueue(new Chunk(new byte[0], System.nanoTime(), true));
				direction.awaitWriter();
			} catch (IOException e) {
				reset();
			} catch (InterruptedException e) {
				Thread.currentThread().interrupt();
				reset();
			}
		}

		private void awaitRelease() {
			synchronized (released) {
				try {
					released.wait(TimeUnit.NANOSECONDS.toMillis(POLL_NANOS));
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					reset();
				}
			}
		}

		/** Bytes read from one side and when they were read, or the end of the stream ({@code end}). */
		private record Chunk(byte[] bytes, long readNanos, boolean end) {}

		/** One direction of the connection: the queue towards {@code to} and its writer thread, started on demand. */
		private final class Direction {

			private final Socket to;
			private final Pacer pacer;
			private final BlockingQueue<Chunk> queue = new ArrayBlockingQueue<>(MAX_QUEUED_CHUNKS);
			/** Chunks enqueued and not yet written, END included. */
			final AtomicInteger queued = new AtomicInteger();
			private @Nullable Thread writer;
			/** When the last chunk was due: a later chunk is never due earlier, so bytes keep their order. */
			private long lastDueNanos = Long.MIN_VALUE;

			Direction(Socket to, Pacer pacer) {
				this.to = to;
				this.pacer = pacer;
			}

			void enqueue(Chunk chunk) throws InterruptedException {
				if (chunk.end() && writer == null && !blackholed && shaping.isNone()) {
					// Nothing was ever queued and nothing holds the end: half-close directly. Otherwise the end of the
					// stream (a FIN) is held like the bytes before it, so a blackhole does not let it through.
					shutdownOutput();
					return;
				}
				if (writer == null) {
					writer = Thread.ofVirtual().name("link-proxy-writer").start(this::writeLoop);
				}
				queued.incrementAndGet();
				while (!queue.offer(chunk, POLL_NANOS, TimeUnit.NANOSECONDS)) {
					if (isReset || closed) {
						return;
					}
				}
			}

			void awaitWriter() throws InterruptedException {
				if (writer != null) {
					writer.join();
				}
			}

			private void writeLoop() {
				try {
					OutputStream out = to.getOutputStream();
					while (!isReset && !closed) {
						var chunk = queue.poll(POLL_NANOS, TimeUnit.NANOSECONDS);
						if (chunk == null) {
							continue;
						}
						holdUntilDue(chunk);
						if (isReset || closed) {
							return;
						}
						if (chunk.end()) {
							shutdownOutput();
							queued.decrementAndGet();
							return;
						}
						out.write(chunk.bytes());
						queued.decrementAndGet();
					}
				} catch (IOException e) {
					reset();
				} catch (InterruptedException e) {
					Thread.currentThread().interrupt();
					reset();
				}
			}

			/**
			 * Waits while the link is blackholed, then until the chunk's delay passed and the bandwidth allows it,
			 * under the link's current shaping. When the shaping or the blackhole changes meanwhile (an
			 * {@link #unblock()} above all), it starts over, so a healed link forwards its backlog at once.
			 */
			private void holdUntilDue(Chunk chunk) throws InterruptedException {
				while (true) {
					while (blackholed && !isReset && !closed) {
						awaitRelease();
					}
					long generation = stateGeneration;
					var currentShaping = shaping;
					long due = chunk.readNanos() + currentShaping.delay().toNanos();
					long jitter = currentShaping.jitter().toNanos();
					if (jitter > 0) {
						due += ThreadLocalRandom.current().nextLong(jitter + 1);
					}
					if (lastDueNanos != Long.MIN_VALUE && lastDueNanos - due > 0) {
						due = lastDueNanos;
					}
					long orderDue = due;
					if (currentShaping.bytesPerSecond() > 0) {
						due = pacer.reserve(due, chunk.bytes().length, currentShaping.bytesPerSecond());
					}
					long wait;
					while ((wait = due - System.nanoTime()) > 0 && !isReset && !closed
							&& generation == stateGeneration) {
						TimeUnit.NANOSECONDS.sleep(Math.min(wait, POLL_NANOS));
					}
					if (generation == stateGeneration || isReset || closed) {
						lastDueNanos = orderDue;
						return;
					}
				}
			}

			private void shutdownOutput() {
				try {
					to.shutdownOutput();
				} catch (IOException e) {
					reset();
				}
			}
		}

		/** Closes both sockets with a TCP reset (linger 0), so both peers notice at once. */
		void reset() {
			isReset = true;
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
