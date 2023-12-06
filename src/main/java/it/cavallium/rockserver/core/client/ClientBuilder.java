package it.cavallium.rockserver.core.client;

import java.net.InetSocketAddress;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;

public class ClientBuilder {

	private InetSocketAddress iNetAddress;
	private UnixDomainSocketAddress unixAddress;
	private Path embeddedPath;
	private String name;
	private Path embeddedConfig;
	private boolean embeddedInMemory;

	public void setEmbeddedPath(Path path) {
		this.embeddedPath = path;
	}

	public void setEmbeddedInMemory(boolean inMemory) {
		this.embeddedInMemory = inMemory;
	}

	public void setUnixSocket(UnixDomainSocketAddress address) {
		this.unixAddress = address;
	}

	public void setAddress(InetSocketAddress address) {
		this.iNetAddress = address;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setEmbeddedConfig(Path embeddedConfig) {
		this.embeddedConfig = embeddedConfig;
	}

	public RocksDBConnection build() {
		if (embeddedInMemory) {
			return new EmbeddedConnection(null, name, embeddedConfig);
		} else if (embeddedPath != null) {
			return new EmbeddedConnection(embeddedPath, name, embeddedConfig);
		} else if (unixAddress != null) {
			return new SocketConnectionUnix(unixAddress, name);
		} else if (iNetAddress != null) {
			return new SocketConnectionInet(iNetAddress, name);
		} else {
			throw new UnsupportedOperationException("Please set a connection type");
		}
	}
}
