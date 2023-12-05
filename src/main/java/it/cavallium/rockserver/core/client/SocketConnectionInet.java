package it.cavallium.rockserver.core.client;

import java.net.InetSocketAddress;
import java.net.URI;

public class SocketConnectionInet extends SocketConnection {

	public SocketConnectionInet(InetSocketAddress address, String name) {
		super(address, name);
	}

	@Override
	public InetSocketAddress getAddress() {
		return (InetSocketAddress) super.getAddress();
	}

	@Override
	public URI getUrl() {
		return URI.create("rocksdb://" + getAddress().getHostString());
	}

}
