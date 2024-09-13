package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.IOException;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;

public class ClientBuilder {

	private HostAndPort httpAddress;
	private HostAndPort iNetAddress;
	private UnixDomainSocketAddress unixAddress;
	private Path embeddedPath;
	private String name;
	private Path embeddedConfig;
	private boolean embeddedInMemory;
	private boolean useThrift;

	public void setEmbeddedPath(Path path) {
		this.embeddedPath = path;
	}

	public void setEmbeddedInMemory(boolean inMemory) {
		this.embeddedInMemory = inMemory;
	}

	public void setUnixSocket(UnixDomainSocketAddress address) {
		this.unixAddress = address;
	}

	public void setHttpAddress(HostAndPort httpAddress) {
		this.httpAddress = httpAddress;
	}

	public void setAddress(HostAndPort address) {
		this.iNetAddress = address;
	}

	public void setUseThrift(boolean useThrift) {
		this.useThrift = useThrift;
	}

	public void setName(String name) {
		this.name = name;
	}

	public void setEmbeddedConfig(Path embeddedConfig) {
		this.embeddedConfig = embeddedConfig;
	}

	public RocksDBConnection build() throws IOException {
		if (embeddedInMemory) {
			return new EmbeddedConnection(null, name, embeddedConfig);
		} else if (embeddedPath != null) {
			return new EmbeddedConnection(embeddedPath, name, embeddedConfig);
		} else if (unixAddress != null) {
			throw new UnsupportedOperationException("Not implemented: unix socket");
		} else if (httpAddress != null) {
			if (useThrift) {
				throw new UnsupportedOperationException("Not implemented: thrift http2 address");
			} else {
				return new GrpcConnection(name, httpAddress);
			}
		} else if (iNetAddress != null) {
			throw new UnsupportedOperationException("Not implemented: inet address");
		} else {
			throw new UnsupportedOperationException("Please set a connection type");
		}
	}
}
