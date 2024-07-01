package it.cavallium.rockserver.core.server;

import it.cavallium.rockserver.core.client.ClientBuilder;
import it.cavallium.rockserver.core.client.EmbeddedConnection;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;

public class ServerBuilder {

	private InetSocketAddress iNetAddress;
	private UnixDomainSocketAddress unixAddress;
	private String http2Host;
	private int http2Port;
	private boolean useThrift;
	private RocksDBConnection client;

	public void setUnixSocket(UnixDomainSocketAddress address) {
		this.unixAddress = address;
	}

	public void setAddress(InetSocketAddress address) {
		this.iNetAddress = address;
	}

	public void setHttpAddress(String host, int port) {
		this.http2Host = host;
		this.http2Port = port;
	}

	public void setUseThrift(boolean useThrift) {
		this.useThrift = useThrift;
	}

	public void setClient(RocksDBConnection client) {
		this.client = client;
	}

	public Server build() throws IOException {
		if (http2Host != null) {
			if (useThrift) {
				return new ThriftServer(client, http2Host, http2Port);
			} else {
				return new GrpcServer(client, http2Host, http2Port);
			}
		} else if (unixAddress != null) {
			throw new UnsupportedOperationException("Not implemented: unix socket");
		} else if (iNetAddress != null) {
			throw new UnsupportedOperationException("Not implemented: inet address");
		} else {
			throw new UnsupportedOperationException("Please set a connection type");
		}
	}
}
