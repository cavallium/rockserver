package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.client.LoggingClient;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import java.io.Closeable;
import java.io.IOException;

public abstract class Server implements Closeable {

	private final RocksDBConnection client;

	public Server(RocksDBConnection client) {
		this.client = new LoggingClient(client);
	}

	public RocksDBConnection getClient() {
		return client;
	}

	public abstract void start() throws IOException;

	@Override
	public void close() throws IOException {
	}
}
