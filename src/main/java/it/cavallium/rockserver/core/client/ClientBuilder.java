package it.cavallium.rockserver.core.client;

import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.io.IOException;
import java.net.URI;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Path;
import java.util.List;

public class ClientBuilder {

	private List<HostAndPort> httpAddresses;
	private HostAndPort iNetAddress;
	private UnixDomainSocketAddress unixAddress;
	private Path embeddedPath;
	private String name;
	private Path embeddedConfig;
	private boolean embeddedInMemory;
	private boolean useThrift;
	private boolean deferred;

	/**
	 * Defer remote connection construction and capability validation until first use. The futures of the deferred
	 * client report the call's own outcome: a cancel before the connection exists sends nothing, one after is the
	 * call's cancel (a numbered write that sent a copy refuses it and ends OUTCOME_UNKNOWN), as without deferral. A call
	 * that fails before the connection exists was never sent, and says so as without deferral: its deadline spent while
	 * connecting is a {@code READ_DEADLINE_EXCEEDED}, a closed client a {@code CLIENT_CLOSED}, both
	 * {@link it.cavallium.rockserver.core.common.RocksDBException#isNotApplied() not applied}; a failure to connect keeps
	 * its own exception (R48-4).
	 */
	public void setDeferred(boolean deferred) {
		this.deferred = deferred;
	}

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
		this.httpAddresses = httpAddress != null ? List.of(httpAddress) : null;
	}

	/**
	 * Several nodes serving the same database, such as a replicated cluster: the client uses the first that accepts
	 * a connection and moves to the next when it is lost (see {@link GrpcConnection#forHostsAndPorts}).
	 */
	public void setHttpAddresses(List<HostAndPort> httpAddresses) {
		if (httpAddresses != null && httpAddresses.isEmpty()) {
			throw new IllegalArgumentException("At least one address is required");
		}
		this.httpAddresses = httpAddresses != null ? List.copyOf(httpAddresses) : null;
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
			var path = unixAddress.getPath();
			var connectionName = name;
			return deferred
					? new DeferredRockserverConnection(URI.create("unix://" + path),
							() -> GrpcConnection.forPath(connectionName, path))
					: GrpcConnection.forPath(connectionName, path);
		} else if (httpAddresses != null) {
			if (useThrift) {
				throw new UnsupportedOperationException("Not implemented: thrift http2 address");
			} else if (httpAddresses.size() == 1) {
				var address = httpAddresses.getFirst();
				var connectionName = name;
				return deferred
						? new DeferredRockserverConnection(URI.create("http://" + address.host() + ":" + address.port()),
								() -> GrpcConnection.forHostAndPort(connectionName, address))
						: GrpcConnection.forHostAndPort(connectionName, address);
			} else {
				var addresses = httpAddresses;
				var connectionName = name;
				return deferred
						? new DeferredRockserverConnection(GrpcConnection.httpUrl(addresses),
								() -> GrpcConnection.forHostsAndPorts(connectionName, addresses))
						: GrpcConnection.forHostsAndPorts(connectionName, addresses);
			}
		} else if (iNetAddress != null) {
			throw new UnsupportedOperationException("Not implemented: inet address");
		} else {
			throw new UnsupportedOperationException("Please set a connection type");
		}
	}
}
