package it.cavallium.rockserver.core.server;

import io.netty.channel.unix.DomainSocketAddress;
import it.cavallium.rockserver.core.client.RocksDBConnection;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.UnixDomainSocketAddress;

public class ServerBuilder {

	private HostAndPort iNetAddress;
	private UnixDomainSocketAddress unixAddress;
	private HostAndPort http2Address;
	private boolean useThrift;
	private RocksDBConnection client;
	private it.cavallium.rockserver.core.common.export.ExportAsyncAPI exports;
	private it.cavallium.rockserver.core.common.ReplicaPhysicalAPI physical;

	public void setUnixSocket(UnixDomainSocketAddress address) {
		this.unixAddress = address;
	}

	public void setAddress(HostAndPort address) {
		this.iNetAddress = address;
	}

	public void setHttpAddress(HostAndPort address) {
		this.http2Address = address;
	}

	public void setUseThrift(boolean useThrift) {
		this.useThrift = useThrift;
	}

	public void setClient(RocksDBConnection client) {
		this.client = client;
	}

	/** Borrow an optional export route; this builder and the server do not close it. */
	public void setExportAPI(it.cavallium.rockserver.core.common.export.ExportAsyncAPI exports) {
		this.exports = exports;
	}

	/** Borrow explicitly authorized node/operator scope; never inferred from the DATA connection. */
	public void setReplicaPhysicalAPI(it.cavallium.rockserver.core.common.ReplicaPhysicalAPI physical) {
		this.physical = physical;
	}

	public Server build() throws IOException {
		if (http2Address != null) {
			if (useThrift) {
				return new ThriftServer(client, http2Address.host(), http2Address.port(), exports, physical);
			} else {
				return new GrpcServer(client, new InetSocketAddress(http2Address.host(), http2Address.port()), exports, physical);
			}
		} else if (unixAddress != null) {
			return new GrpcServer(client, new DomainSocketAddress(unixAddress.getPath().toFile()), exports, physical);
		} else if (iNetAddress != null) {
			throw new UnsupportedOperationException("Not implemented: inet address");
		} else {
			throw new UnsupportedOperationException("Please set a connection type");
		}
	}
}
