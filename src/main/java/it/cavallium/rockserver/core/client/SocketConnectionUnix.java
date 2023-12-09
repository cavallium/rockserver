package it.cavallium.rockserver.core.client;

import java.io.IOException;
import java.net.URI;
import java.net.UnixDomainSocketAddress;
import java.nio.file.Files;

public class SocketConnectionUnix extends SocketConnection {

	public SocketConnectionUnix(UnixDomainSocketAddress address, String name) {
		super(address, name);
	}

	@Override
	public UnixDomainSocketAddress getAddress() {
		return (UnixDomainSocketAddress) super.getAddress();
	}

	@Override
	public void close() throws IOException {
		super.close();
		Files.deleteIfExists(getAddress().getPath());
	}

	@Override
	public URI getUrl() {
		return URI.create("unix://" + getAddress().getPath());
	}
}
