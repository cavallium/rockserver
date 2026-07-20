package it.cavallium.rockserver.core.replication.ratis;

import java.net.URI;
import java.util.Locale;

public record RatisPeerDescriptor(String peerId, String endpoint, String failureDomain) {

    public RatisPeerDescriptor {
        RatisAdapterBoundary.requireSingleLineNonBlank(peerId, "peerId");
        endpoint = requireGrpcEndpoint(endpoint);
        RatisAdapterBoundary.requireSingleLineNonBlank(failureDomain, "failureDomain");
    }

    private static String requireGrpcEndpoint(String endpoint) {
        var requiredEndpoint = RatisAdapterBoundary.requireNonBlank(endpoint, "endpoint");
        URI uri;
        try {
            uri = URI.create(requiredEndpoint);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("endpoint must be a URI: " + requiredEndpoint, e);
        }
        if (!"grpc".equalsIgnoreCase(uri.getScheme())) {
            throw new IllegalArgumentException("endpoint scheme must be grpc: " + requiredEndpoint);
        }
        if (uri.getHost() == null || uri.getHost().isBlank()) {
            throw new IllegalArgumentException("endpoint must include a host: " + requiredEndpoint);
        }
        if (uri.getPort() <= 0 || uri.getPort() > 65535) {
            throw new IllegalArgumentException("endpoint port must be between 1 and 65535: " + requiredEndpoint);
        }
        if (hasEndpointDecorations(uri)) {
            throw new IllegalArgumentException("endpoint must not include user info, path, query, or fragment: "
                    + requiredEndpoint);
        }
        return "grpc://" + canonicalHost(uri.getHost()) + ":" + uri.getPort();
    }

    private static String canonicalHost(String host) {
        var canonicalHost = host.toLowerCase(Locale.ROOT);
        if (canonicalHost.indexOf(':') >= 0
                && !(canonicalHost.startsWith("[") && canonicalHost.endsWith("]"))) {
            return "[" + canonicalHost + "]";
        }
        return canonicalHost;
    }

    private static boolean hasEndpointDecorations(URI uri) {
        return uri.getRawUserInfo() != null
                || (uri.getRawPath() != null && !uri.getRawPath().isEmpty())
                || uri.getRawQuery() != null
                || uri.getRawFragment() != null;
    }
}
