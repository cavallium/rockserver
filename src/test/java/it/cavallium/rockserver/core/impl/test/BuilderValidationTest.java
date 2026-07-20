package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.client.ClientBuilder;
import it.cavallium.rockserver.core.common.Utils.HostAndPort;
import it.cavallium.rockserver.core.server.ServerBuilder;

import org.junit.jupiter.api.Test;

class BuilderValidationTest {

    @Test
    void clientBuilderRejectsMissingConnectionTypeWithStateError() {
        var failure = assertThrows(IllegalStateException.class, () -> new ClientBuilder().build());

        assertEquals("Please set a connection type", failure.getMessage());
    }

    @Test
    void clientBuilderRejectsUnsupportedThriftHttpAddressWithArgumentError() {
        var builder = new ClientBuilder();
        builder.setUseThrift(true);
        builder.setHttpAddress(new HostAndPort("localhost", 8080));

        var failure = assertThrows(IllegalArgumentException.class, builder::build);

        assertEquals("Thrift clients do not support HTTP/2 addresses", failure.getMessage());
    }

    @Test
    void clientBuilderRejectsUnsupportedThriftInetAddressWithArgumentError() {
        var builder = new ClientBuilder();
        builder.setUseThrift(true);
        builder.setAddress(new HostAndPort("localhost", 8080));

        var failure = assertThrows(IllegalArgumentException.class, builder::build);

        assertEquals("Thrift clients do not support inet addresses", failure.getMessage());
    }

    @Test
    void serverBuilderRejectsMissingConnectionTypeWithStateError() {
        var failure = assertThrows(IllegalStateException.class, () -> new ServerBuilder().build());

        assertEquals("Please set a connection type", failure.getMessage());
    }

    @Test
    void serverBuilderRejectsUnsupportedThriftInetAddressWithArgumentError() {
        var builder = new ServerBuilder();
        builder.setUseThrift(true);
        builder.setAddress(new HostAndPort("localhost", 8080));

        var failure = assertThrows(IllegalArgumentException.class, builder::build);

        assertEquals("Thrift servers do not support inet addresses", failure.getMessage());
    }
}
