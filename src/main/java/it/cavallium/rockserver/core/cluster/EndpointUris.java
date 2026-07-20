package it.cavallium.rockserver.core.cluster;

import java.net.InetSocketAddress;
import java.net.URI;

public final class EndpointUris {

    private EndpointUris() {
    }

    public static URI requireGrpcUri(URI uri, String description) {
        var requiredDescription = requireArgument(description, "description");
        var requiredUri = requireArgument(uri, requiredDescription);
        if (!"grpc".equals(requiredUri.getScheme())) {
            throw new IllegalArgumentException(requiredDescription + " must use grpc scheme");
        }
        return requireEndpointUri(requiredUri, requiredDescription);
    }

    public static URI requireEndpointUri(URI uri, String description) {
        var requiredDescription = requireArgument(description, "description");
        var requiredUri = requireArgument(uri, requiredDescription);
        if (requiredUri.getScheme() == null || requiredUri.getScheme().isBlank()) {
            throw new IllegalArgumentException(requiredDescription + " must include a scheme");
        }
        if (requiredUri.getHost() == null || requiredUri.getHost().isBlank()) {
            throw new IllegalArgumentException(requiredDescription + " must include a host");
        }
        if (requiredUri.getPort() < 1 || requiredUri.getPort() > 65_535) {
            throw new IllegalArgumentException(requiredDescription + " port must be between 1 and 65535");
        }
        if (requiredUri.getUserInfo() != null
                || (requiredUri.getPath() != null && !requiredUri.getPath().isEmpty())
                || requiredUri.getQuery() != null
                || requiredUri.getFragment() != null) {
            throw new IllegalArgumentException(
                    requiredDescription + " must not include user info, path, query, or fragment"
            );
        }
        return requiredUri;
    }

    public static void requireSocketAddress(InetSocketAddress address, String description) {
        var requiredDescription = requireArgument(description, "description");
        var requiredAddress = requireArgument(address, requiredDescription);
        requireSocketHostAndPort(requiredAddress.getHostString(), requiredAddress.getPort(), requiredDescription);
    }

    public static void requireSocketHostAndPort(String host, int port, String description) {
        var requiredDescription = requireArgument(description, "description");
        if (host == null || host.isBlank()) {
            throw new IllegalArgumentException(requiredDescription + " host must not be blank");
        }
        if (port <= 0 || port > 65_535) {
            throw new IllegalArgumentException(requiredDescription + " port must be between 1 and 65535");
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
