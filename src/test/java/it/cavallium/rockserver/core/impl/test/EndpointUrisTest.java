package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.cluster.EndpointUris;

import java.net.InetSocketAddress;
import java.net.URI;

import org.junit.jupiter.api.Test;

class EndpointUrisTest {

    @Test
    void rejectsNullInputsWithExplicitDiagnostics() {
        assertIllegalArgument("endpoint must not be null",
                () -> EndpointUris.requireGrpcUri(null, "endpoint"));
        assertIllegalArgument("description must not be null",
                () -> EndpointUris.requireGrpcUri(URI.create("grpc://127.0.0.1:19001"), null));
        assertIllegalArgument("endpoint must not be null",
                () -> EndpointUris.requireEndpointUri(null, "endpoint"));
        assertIllegalArgument("description must not be null",
                () -> EndpointUris.requireEndpointUri(URI.create("http://127.0.0.1:8080"), null));
        assertIllegalArgument("endpoint must not be null",
                () -> EndpointUris.requireSocketAddress(null, "endpoint"));
        assertIllegalArgument("description must not be null",
                () -> EndpointUris.requireSocketAddress(new InetSocketAddress("127.0.0.1", 8080), null));
        assertIllegalArgument("description must not be null",
                () -> EndpointUris.requireSocketHostAndPort("127.0.0.1", 8080, null));
    }

    @Test
    void validatesUriAndSocketEndpointShape() {
        assertEquals(URI.create("grpc://127.0.0.1:19001"),
                EndpointUris.requireGrpcUri(URI.create("grpc://127.0.0.1:19001"), "endpoint"));
        assertEquals(URI.create("http://127.0.0.1:8080"),
                EndpointUris.requireEndpointUri(URI.create("http://127.0.0.1:8080"), "endpoint"));

        assertIllegalArgument("endpoint must use grpc scheme",
                () -> EndpointUris.requireGrpcUri(URI.create("http://127.0.0.1:19001"), "endpoint"));
        assertIllegalArgument("endpoint must include a host",
                () -> EndpointUris.requireEndpointUri(URI.create("http://:8080"), "endpoint"));
        assertIllegalArgument("endpoint port must be between 1 and 65535",
                () -> EndpointUris.requireEndpointUri(URI.create("http://127.0.0.1:0"), "endpoint"));
        assertIllegalArgument("endpoint must not include user info, path, query, or fragment",
                () -> EndpointUris.requireEndpointUri(URI.create("http://127.0.0.1:8080/path"), "endpoint"));
        assertIllegalArgument("endpoint host must not be blank",
                () -> EndpointUris.requireSocketHostAndPort("", 8080, "endpoint"));
        assertIllegalArgument("endpoint port must be between 1 and 65535",
                () -> EndpointUris.requireSocketHostAndPort("127.0.0.1", 0, "endpoint"));
    }

    private static void assertIllegalArgument(String message, Runnable operation) {
        var error = assertThrows(IllegalArgumentException.class, operation::run);
        assertEquals(message, error.getMessage());
    }
}
