package it.cavallium.rockserver.core.client;

import java.io.IOException;

public abstract class BaseConnection implements RocksDBConnection {

	private final String name;

	public BaseConnection(String name) {
		this.name = name;
	}

	@Override
	public void close() throws IOException {

	}

	@Override
	public String toString() {
		return "db \"" + name + "\" (" + getUrl() + ")";
	}
}
