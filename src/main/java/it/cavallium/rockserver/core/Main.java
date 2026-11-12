package it.cavallium.rockserver.core;

import static it.cavallium.rockserver.core.client.EmbeddedConnection.PRIVATE_MEMORY_URL;
import static java.util.Objects.requireNonNull;

import it.cavallium.rockserver.core.client.ClientBuilder;
import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.impl.JvmExit;
import it.cavallium.rockserver.core.impl.ProcessHalt;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.cavallium.rockserver.core.server.ServerBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;
import org.jetbrains.annotations.VisibleForTesting;
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	private static final Logger LOG = LoggerFactory.getLogger("rockserver-core");
	/** How often the shutdown hook looks at the thread it waits for ({@link #awaitShutdownComplete}). */
	private static final long SHUTDOWN_POLL_MILLIS = 200;

	public static void main(String[] args) throws IOException, URISyntaxException {
		run(args, () -> {});
	}

	@VisibleForTesting
	static void run(String[] args, Runnable shutdownCompleteCallback) throws IOException, URISyntaxException {
		run(args, shutdownCompleteCallback, () -> {});
	}

	/**
	 * {@code connectedCallback} runs on the calling thread during the start, once the database is open and before the
	 * servers are built: a test's way to make the start slow, fail or exit.
	 */
	@VisibleForTesting
	static void run(String[] args, Runnable shutdownCompleteCallback, Runnable connectedCallback)
			throws IOException, URISyntaxException {
		requireNonNull(shutdownCompleteCallback, "shutdownCompleteCallback");
		requireNonNull(connectedCallback, "connectedCallback");
		ArgumentParser parser = ArgumentParsers.newFor("rockserver-core").build()
				.defaultHelp(true)
				.description("RocksDB server core");
		parser.addArgument("-u", "--database-url")
				.type(String.class)
				.setDefault(PRIVATE_MEMORY_URL.toString())
				.help("Specify database http://host:port (gRPC; a comma-separated list http://host1:port1,host2:port2 for the"
						+ " nodes of a cluster), or rocksdb://hostname:port, or unix://<path>, or file://<path>");
		parser.addArgument("-l", "--thrift-listen-url")
				.type(String.class)
				.setDefault("http://127.0.0.1:5332")
				.help("Specify database http://hostname:port, or unix://<path>, or file://<path>");
		parser.addArgument("-L", "--grpc-listen-url")
				.type(String.class)
				.setDefault("http://127.0.0.1:5333")
				.help("Specify database http://hostname:port, or unix://<path>, or file://<path>");
		parser.addArgument("-n", "--name")
				.type(String.class)
				.setDefault("main")
				.help("Specify database name");
		parser.addArgument("-c", "--config")
				.type(String.class)
				.help("Specify the rockserver-core.conf file path. Do not set if the database is not local");
		parser.addArgument("-p", "--print-default-config")
				.type(Boolean.class)
				.setDefault(false)
				.help("Print the default configs");
		Namespace ns = null;
		try {
			ns = parser.parseArgs(args);
		} catch (ArgumentParserException e) {
			parser.handleError(e);
			System.exit(1);
		}
		var clientBuilder = new ClientBuilder();

		if (ns.getBoolean("print_default_config")) {
			requireNonNull(Main.class.getClassLoader()
					.getResourceAsStream("it/cavallium/rockserver/core/resources/default.conf"))
					.transferTo(System.out);
			System.exit(0);
			return;
		}

		LOG.info("Starting...");
		RocksDBLoader.loadLibrary();

		LOG.info("RocksDB version: {}", RocksDB.rocksdbVersion());

		var rawDatabaseUrl = ns.getString("database_url");
		var rawThriftListenUrl = ns.getString("thrift_listen_url");
		var rawGrpcListenUrl = ns.getString("grpc_listen_url");
		var name = ns.getString("name");
		var config = ns.getString("config");

		var databaseUrl = Utils.parseConnectionUrl(rawDatabaseUrl);
		var thriftListenUrl = new URI(rawThriftListenUrl);
		var grpcListenUrl = new URI(rawGrpcListenUrl);

		if (config != null) {
			if (!databaseUrl.getScheme().equals("file")) {
				LOG.error("Do not set --config if the database is not local!");
				System.exit(1);
				return;
			} else {
				clientBuilder.setEmbeddedConfig(Path.of(config));
			}
		}

		var databaseUrlScheme = databaseUrl.getScheme();
		switch (databaseUrlScheme) {
			case "unix" -> clientBuilder.setUnixSocket(UnixDomainSocketAddress.of(Path.of(databaseUrl.getPath())));
			case "file" -> clientBuilder.setEmbeddedPath(Path.of((databaseUrl.getAuthority() != null ? databaseUrl.getAuthority() : "") + databaseUrl.getPath()).normalize());
			case "memory" -> clientBuilder.setEmbeddedInMemory(true);
			case "http" -> {
				clientBuilder.setHttpAddresses(Utils.parseHostAndPorts(databaseUrl));
				clientBuilder.setUseThrift(false);
			}
			case "rocksdb" -> clientBuilder.setAddress(Utils.parseHostAndPort(databaseUrl));
			case null, default -> throw new IllegalArgumentException("Invalid scheme \"" + databaseUrlScheme + "\" for database url url: " + databaseUrl);
		}

		var thriftServerBuilder = new ServerBuilder();
		buildServerAddress(thriftServerBuilder, thriftListenUrl, true);
		var grpcServerBuilder = new ServerBuilder();
		buildServerAddress(grpcServerBuilder, grpcListenUrl, false);

		clientBuilder.setName(name);

		CountDownLatch shutdownRequested = new CountDownLatch(1);
		CountDownLatch shutdownComplete = new CountDownLatch(1);
		// The thread that starts, serves and closes everything below, and the only one that releases shutdownComplete.
		Thread running = Thread.currentThread();
		Thread shutdownHook = Thread.ofPlatform()
				.name("DB shutdown hook")
				.unstarted(() -> {
					shutdownRequested.countDown();
					awaitShutdownComplete(shutdownComplete, running);
				});
		Runtime.getRuntime().addShutdownHook(shutdownHook);

		try {
			LOG.info("Connecting...");
			try (var connection = clientBuilder.build()) {
				try {
					LOG.info("Connected to {}", connection);
					connectedCallback.run();

					thriftServerBuilder.setClient(connection);
					grpcServerBuilder.setClient(connection);

					try (var thrift = thriftServerBuilder.build(); var grpc = grpcServerBuilder.build()) {
						if (shutdownRequested.getCount() != 0) {
							thrift.start();
							grpc.start();
							LOG.info("Rockserver is ready");
							shutdownRequested.await();
						}
						LOG.info("Shutting down...");
					}
				} catch (Exception ex) {
					LOG.error("Unexpected error", ex);
				}
			}
			LOG.info("Shut down successfully");
		} finally {
			// A JVM shutdown waits for hook threads, not for this ordinary main thread. Keep the hook alive until all
			// try-with-resources scopes above have closed gRPC, Thrift, and the database connection.
			try {
				shutdownCompleteCallback.run();
			} finally {
				shutdownComplete.countDown();
				try {
					Runtime.getRuntime().removeShutdownHook(shutdownHook);
				} catch (IllegalStateException ignored) {
					// Shutdown is already in progress, so this hook is the thread waiting on shutdownComplete above.
				}
			}
		}
	}

	/**
	 * The shutdown hook's wait until {@code running}, the thread of {@code run}, has closed the servers and the
	 * database and released {@code shutdownComplete}. The wait has no limit: a SIGTERM during a start waits for the start
	 * and then for the close, a SIGTERM while serving waits for the close, however long they take.
	 *
	 * <p>With one exception (R66-5). {@link System#exit} runs the shutdown hooks on the thread that called it and waits
	 * for them, and a later {@code System.exit} waits behind the shutdown under way, for good. If {@code running} itself
	 * is inside {@code System.exit} ({@link JvmExit#exiting}: something in the start or in a close called it, before or
	 * after a signal), it never returns, so it never closes anything nor releases the latch: this hook and that thread
	 * would wait for each other and the process would stay, serving nothing and deaf to SIGTERM. The hook then prints
	 * where that thread stands and ends the process with 1 ({@link ProcessHalt}, no further hooks nor destructors: the
	 * database is still open and only that thread could close it). That leaves what a crash leaves, which the next
	 * start recovers from.</p>
	 *
	 * <p>The latch is read again after the look at the thread: a caller of {@code run} that exits once {@code run}
	 * returned is inside {@code System.exit} too, with the latch released, and is no reason to halt.</p>
	 */
	private static void awaitShutdownComplete(CountDownLatch shutdownComplete, Thread running) {
		boolean interrupted = false;
		try {
			while (true) {
				try {
					if (shutdownComplete.await(SHUTDOWN_POLL_MILLIS, TimeUnit.MILLISECONDS)) {
						return;
					}
				} catch (InterruptedException ex) {
					interrupted = true;
					continue;
				}
				boolean exiting = JvmExit.exiting(running);
				if (shutdownComplete.getCount() == 0) {
					return;
				}
				if (exiting) {
					var message = new StringBuilder("Rockserver cannot shut down: the thread that runs it called System.exit,"
							+ " which waits for this shutdown and so never returns (below is where that thread stands);"
							+ " halting with 1");
					for (var frame : running.getStackTrace()) {
						message.append(System.lineSeparator()).append("\tat ").append(frame);
					}
					System.err.println(message);
					System.err.flush();
					System.out.flush();
					ProcessHalt.halt(1);
					return;
				}
			}
		} finally {
			if (interrupted) {
				Thread.currentThread().interrupt();
			}
		}
	}

	private static void buildServerAddress(ServerBuilder serverBuilder, URI listenUrl, boolean useThrift) {
		var thriftListenUrlScheme = listenUrl.getScheme();
		switch (thriftListenUrlScheme) {
			case "unix" -> serverBuilder.setUnixSocket(UnixDomainSocketAddress.of(Path.of(listenUrl.getPath())));
			case "http" -> {
				serverBuilder.setHttpAddress(new HostAndPort(listenUrl.getHost(), Utils.parsePort(listenUrl)));
				serverBuilder.setUseThrift(useThrift);
			}
			case "rocksdb" -> serverBuilder.setAddress(Utils.parseHostAndPort(listenUrl));
			case null, default -> throw new IllegalArgumentException("Invalid scheme \"" + thriftListenUrlScheme + "\" for listen url: " + listenUrl);
		}
	}
}
