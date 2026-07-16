package it.cavallium.rockserver.core;

import static it.cavallium.rockserver.core.client.EmbeddedConnection.PRIVATE_MEMORY_URL;
import static java.util.Objects.requireNonNull;

import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import it.cavallium.rockserver.core.server.ServerBuilder;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
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

	public static void main(String[] args) throws IOException, URISyntaxException {
		run(args, () -> {});
	}

	@VisibleForTesting
	static void run(String[] args, Runnable shutdownCompleteCallback) throws IOException, URISyntaxException {
		requireNonNull(shutdownCompleteCallback, "shutdownCompleteCallback");
		ArgumentParser parser = ArgumentParsers.newFor("rockserver-core").build()
				.defaultHelp(true)
				.description("RocksDB server core");
		parser.addArgument("-u", "--database-url")
				.type(String.class)
				.setDefault(PRIVATE_MEMORY_URL.toString())
				.help("Specify database rocksdb://hostname:port, or unix://<path>, or file://<path>");
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
		var clientBuilder = new it.cavallium.rockserver.core.client.ClientBuilder();

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

		var databaseUrl = new URI(rawDatabaseUrl);
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
				clientBuilder.setHttpAddress(Utils.parseHostAndPort(databaseUrl));
				clientBuilder.setUseThrift(false);
			}
			case "rocksdb" -> clientBuilder.setAddress(Utils.parseHostAndPort(databaseUrl));
			case null, default -> throw new IllegalArgumentException("Invalid scheme \"" + databaseUrlScheme + "\" for database url url: " + databaseUrl);
		}

		var thriftServerBuilder = new it.cavallium.rockserver.core.server.ServerBuilder();
		buildServerAddress(thriftServerBuilder, thriftListenUrl, true);
		var grpcServerBuilder = new it.cavallium.rockserver.core.server.ServerBuilder();
		buildServerAddress(grpcServerBuilder, grpcListenUrl, false);

		clientBuilder.setName(name);

		CountDownLatch shutdownRequested = new CountDownLatch(1);
		CountDownLatch shutdownComplete = new CountDownLatch(1);
		Thread shutdownHook = Thread.ofPlatform()
				.name("DB shutdown hook")
				.unstarted(() -> {
					shutdownRequested.countDown();
					awaitUninterruptibly(shutdownComplete);
				});
		Runtime.getRuntime().addShutdownHook(shutdownHook);

		try {
			LOG.info("Connecting...");
			try (var connection = clientBuilder.build()) {
				try {
					LOG.info("Connected to {}", connection);

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

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException ex) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
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
