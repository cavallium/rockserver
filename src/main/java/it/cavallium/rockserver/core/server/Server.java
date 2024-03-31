package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.client.LoggingClient;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import java.io.Closeable;
import java.io.IOException;

public class Server implements Closeable {

	private final RocksDBConnection client;

	public Server(RocksDBConnection client) {
		this.client = new LoggingClient(client);
	}

	public RocksDBConnection getClient() {
		return client;
	}

	@Override
	public void close() throws IOException {
		client.close();
	}
}
