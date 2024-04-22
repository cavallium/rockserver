package it.cavallium.rockserver.core;

import static it.cavallium.rockserver.core.client.EmbeddedConnection.PRIVATE_MEMORY_URL;
import static java.util.Objects.requireNonNull;

import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;
import java.util.concurrent.CountDownLatch;
import java.util.logging.Level;
import java.util.logging.Logger;
import net.sourceforge.argparse4j.ArgumentParsers;
import net.sourceforge.argparse4j.inf.ArgumentParser;
import net.sourceforge.argparse4j.inf.ArgumentParserException;
import net.sourceforge.argparse4j.inf.Namespace;

public class Main {

	private static final Logger LOG = Logger.getLogger("rockserver-core");

	public static void main(String[] args) throws IOException, URISyntaxException {
		ArgumentParser parser = ArgumentParsers.newFor("rockserver-core").build()
				.defaultHelp(true)
				.description("RocksDB server core");
		parser.addArgument("-u", "--database-url")
				.type(String.class)
				.setDefault(PRIVATE_MEMORY_URL.toString())
				.help("Specify database rocksdb://hostname:port, or unix://<path>, or file://<path>");
		parser.addArgument("-l", "--listen-url")
				.type(String.class)
				.setDefault("http://127.0.0.1:5332")
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
		var serverBuilder = new it.cavallium.rockserver.core.server.ServerBuilder();

		if (ns.getBoolean("print_default_config")) {
			requireNonNull(Main.class.getClassLoader()
					.getResourceAsStream("it/cavallium/rockserver/core/resources/default.conf"))
					.transferTo(System.out);
			System.exit(0);
			return;
		}

		LOG.info("Starting...");
		RocksDBLoader.loadLibrary();

		var rawDatabaseUrl = ns.getString("database_url");
		var rawListenUrl = ns.getString("listen_url");
		var name = ns.getString("name");
		var config = ns.getString("config");

		var databaseUrl = new URI(rawDatabaseUrl);
		var listenUrl = new URI(rawListenUrl);

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
			case "rocksdb" -> clientBuilder.setAddress(Utils.parseHostAndPort(databaseUrl));
			case null, default -> throw new IllegalArgumentException("Invalid scheme \"" + databaseUrlScheme + "\" for database url url: " + databaseUrl);
		}

		var listenUrlScheme = listenUrl.getScheme();
		switch (listenUrlScheme) {
			case "unix" -> serverBuilder.setUnixSocket(UnixDomainSocketAddress.of(Path.of(listenUrl.getPath())));
			case "http" -> serverBuilder.setHttpAddress(listenUrl.getHost(), Utils.parsePort(listenUrl));
			case "rocksdb" -> serverBuilder.setAddress(Utils.parseHostAndPort(listenUrl));
			case null, default -> throw new IllegalArgumentException("Invalid scheme \"" + listenUrlScheme + "\" for listen url: " + listenUrl);
		}

		clientBuilder.setName(name);
		try (var connection = clientBuilder.build()) {
			LOG.log(Level.INFO, "Connected to {0}", connection);

			serverBuilder.setClient(connection);

			CountDownLatch shutdownLatch = new CountDownLatch(1);
			Runtime.getRuntime().addShutdownHook(new Thread(shutdownLatch::countDown));

			try (var server = serverBuilder.build()) {
				shutdownLatch.await();
				LOG.info("Shutting down...");
			}
		} catch (InterruptedException e) {
			throw new RuntimeException("Interrupted", e);
		}
		LOG.info("Shut down successfully");
	}
}
