package it.cavallium.rockserver.core;

import static it.cavallium.rockserver.core.client.EmbeddedConnection.PRIVATE_MEMORY_URL;
import static java.util.Objects.requireNonNull;

import io.vertx.core.file.impl.FileSystemImpl;
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
import org.rocksdb.RocksDB;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Main {

	private static final Logger LOG = LoggerFactory.getLogger("rockserver-core");

	public static void main(String[] args) throws IOException, URISyntaxException {
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
		// Preload classes
		try {FileSystemImpl.delete(Path.of(".nonexistent-file"), false);} catch (Throwable ex) {} // preload FileSystemImpl

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
				System.err.println("Do not set --config if the database is not local!");
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
		try (var connection = clientBuilder.build()) {
			try {
				LOG.info("Connected to {}", connection);

				thriftServerBuilder.setClient(connection);
				grpcServerBuilder.setClient(connection);

				CountDownLatch shutdownLatch = new CountDownLatch(1);
				Runtime.getRuntime().addShutdownHook(Thread.ofPlatform()
						.name("DB shutdown hook").unstarted(shutdownLatch::countDown));

				try (var thrift = thriftServerBuilder.build(); var grpc = grpcServerBuilder.build()) {
					thrift.start();
					grpc.start();
					shutdownLatch.await();
					LOG.info("Shutting down...");
				}
			} catch (Exception ex) {
				LOG.error("Unexpected error", ex);
			}
		}
		LOG.info("Shut down successfully");
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
