package it.cavallium.rockserver.core;

import static it.cavallium.rockserver.core.client.EmbeddedConnection.PRIVATE_MEMORY_URL;
import static java.util.Objects.requireNonNull;

import inet.ipaddr.HostName;

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
import org.rocksdb.RocksDB;

public class Main {

	private static final Logger LOG = Logger.getLogger("rockserver-core");

	public static void main(String[] args) throws IOException, URISyntaxException {
		ArgumentParser parser = ArgumentParsers.newFor("rockserver-core").build()
				.defaultHelp(true)
				.description("RocksDB server core");
		parser.addArgument("-u", "--url")
				.type(String.class)
				.setDefault(PRIVATE_MEMORY_URL.toString())
				.help("Specify database rocksdb://hostname:port, or unix://<path>, or file://<path>");
		parser.addArgument("-n", "--name")
				.type(String.class)
				.setDefault("main")
				.help("Specify database name");
		parser.addArgument("-c", "--config")
				.type(Path.class)
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
		RocksDB.loadLibrary();

		var rawUrl = ns.getString("url");
		var name = ns.getString("name");
		var config = ns.getString("config");

		var url = new URI(rawUrl);

		if (config != null) {
			if (!url.getScheme().equals("file")) {
				System.err.println("Do not set --config if the database is not local!");
				System.exit(1);
				return;
			} else {
				clientBuilder.setEmbeddedConfig(Path.of(config));
			}
		}

		switch (url.getScheme()) {
			case "unix" -> clientBuilder.setUnixSocket(UnixDomainSocketAddress.of(Path.of(url.getPath())));
			case "file" -> clientBuilder.setEmbeddedPath(Path.of(url.getPath()));
			case "memory" -> clientBuilder.setEmbeddedInMemory(true);
			case "rocksdb" -> clientBuilder.setAddress(new HostName(url.getHost()).asInetSocketAddress());
			default -> throw new IllegalArgumentException("Invalid scheme: " + url.getScheme());
		}

		clientBuilder.setName(name);
		try (var connection = clientBuilder.build()) {
			LOG.log(Level.INFO, "Connected to {0}", connection);
			CountDownLatch shutdownLatch = new CountDownLatch(1);
			Runtime.getRuntime().addShutdownHook(new Thread(shutdownLatch::countDown));
			LOG.info("Shutting down...");
		}
		LOG.info("Shut down successfully");
	}
}
