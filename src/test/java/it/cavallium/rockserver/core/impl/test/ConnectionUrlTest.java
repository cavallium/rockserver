package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.common.Utils;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;

class ConnectionUrlTest {

	@Test
	void aListOfHostsKeepsItsOrder() throws Exception {
		assertEquals(
				List.of(new HostAndPort("node-3", 7000), new HostAndPort("node-1", 7001), new HostAndPort("node-2", 7002)),
				Utils.parseHostAndPorts(Utils.parseConnectionUrl("http://node-3:7000,node-1:7001,node-2:7002")));
	}

	@Test
	void aMissingPortIsTheDefaultPort() throws Exception {
		assertEquals(
				List.of(new HostAndPort("a", Utils.DEFAULT_PORT), new HostAndPort("b", 7000)),
				Utils.parseHostAndPorts(Utils.parseConnectionUrl("http://a,b:7000")));
	}

	@Test
	void aTrailingSlashIsAccepted() throws Exception {
		assertEquals(
				List.of(new HostAndPort("a", 1), new HostAndPort("b", 2)),
				Utils.parseHostAndPorts(Utils.parseConnectionUrl("http://a:1,b:2/")));
	}

	@Test
	void ipv6LiteralsAreWrittenInBrackets() throws Exception {
		assertEquals(
				List.of(new HostAndPort("::1", 7000), new HostAndPort("fe80::2", Utils.DEFAULT_PORT),
						new HostAndPort("10.0.0.1", 7001)),
				Utils.parseHostAndPorts(Utils.parseConnectionUrl("http://[::1]:7000,[fe80::2],10.0.0.1:7001")));
	}

	@ParameterizedTest
	@ValueSource(strings = {"http://localhost:7000", "http://localhost", "http://10.1.2.3:7000", "http://[::1]:7000"})
	void aSingleHostGivesTheSameAddressAsBefore(String url) throws Exception {
		var uri = new URI(url);
		assertEquals(uri, Utils.parseConnectionUrl(url));
		assertEquals(List.of(Utils.parseHostAndPort(uri)), Utils.parseHostAndPorts(uri));
	}

	@ParameterizedTest
	@ValueSource(strings = {"unix:///tmp/rockserver.sock", "file:///data/db", "file://relative/path", "memory://private"})
	void otherFormsParseAsBefore(String url) throws Exception {
		assertEquals(new URI(url), Utils.parseConnectionUrl(url));
	}

	@ParameterizedTest
	@ValueSource(strings = {"a:1,", ",a:1", "a:1,,b:2", "a:notaport", "a:0", "a:65536", "[::1", "[::1]x", "::1", ":5", "[]:5"})
	void invalidListsAreRefused(String hosts) {
		assertThrows(IllegalArgumentException.class, () -> Utils.parseHostAndPorts(hosts));
	}

	@Test
	void aUrlWithoutHostIsRefused() throws URISyntaxException {
		assertThrows(IllegalArgumentException.class, () -> Utils.parseHostAndPorts(new URI("http:///path")));
	}

	@Test
	void formattingIsTheInverseOfParsing() {
		var hosts = "[::1]:7000,node-1:7001,10.0.0.1:7002";
		assertEquals(hosts, Utils.formatHostAndPorts(Utils.parseHostAndPorts(hosts)));
	}
}
