package it.cavallium.rockserver.core.client;

import java.net.InetSocketAddress;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;

public class ClientBuilder {

	private InetSocketAddress iNetAddress;
	private UnixDomainSocketAddress unixAddress;
	private Path embedded;
	private String name;
	private Path embeddedConfig;

	public void setEmbedded(Path path) {
		this.embedded = path;
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
		if (embedded != null) {
			return new EmbeddedConnection(embedded, name, embeddedConfig);
		} else if (unixAddress != null) {
			return new SocketConnectionUnix(unixAddress, name);
		} else if (iNetAddress != null) {
			return new SocketConnectionInet(iNetAddress, name);
		} else {
			throw new UnsupportedOperationException("Please set a connection type");
		}
	}
}
