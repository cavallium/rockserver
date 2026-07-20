package it.cavallium.rockserver.core.cluster.metadata;

import it.cavallium.rockserver.core.cluster.ClusterId;
import it.cavallium.rockserver.core.cluster.EndpointUris;
import it.cavallium.rockserver.core.cluster.NodeId;
import it.cavallium.rockserver.core.cluster.internal.BoundedInputStream;
import it.cavallium.rockserver.core.cluster.internal.BoundedOutputStream;
import it.cavallium.rockserver.core.cluster.internal.DurableAtomicFileWriter;
import it.cavallium.rockserver.core.cluster.internal.StrictProperties;

import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.LinkOption;
import java.nio.file.Path;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.ArrayList;
import java.util.Properties;
import java.util.regex.Pattern;

public final class BootstrapManifestStore {

    private static final String CLUSTER_ID_KEY = "clusterId";
    private static final String METADATA_GROUP_ID_KEY = "metadataGroupId";
    private static final String BOOTSTRAP_EPOCH_KEY = "bootstrapEpoch";
    private static final String PEER_PREFIX = "peer.";
    public static final int MAX_BOOTSTRAP_MANIFEST_BYTES = 1024 * 1024;
    private static final String BOOTSTRAP_MANIFEST_DESCRIPTION = "bootstrap manifest payload";
    private static final Pattern KNOWN_KEY = Pattern.compile(
            "clusterId|metadataGroupId|bootstrapEpoch"
                    + "|peer\\.\\d+\\.(nodeId|publicEndpoint|ratisEndpoint|failureDomain)"
    );

    private BootstrapManifestStore() {
    }

    public static BootstrapManifest read(Path path) throws IOException {
        var requiredPath = requireArgument(path, "path");
        var attributes = Files.readAttributes(requiredPath, BasicFileAttributes.class, LinkOption.NOFOLLOW_LINKS);
        if (!attributes.isRegularFile()) {
            throw new IOException("Bootstrap manifest path must be a regular file: " + requiredPath);
        }
        requirePayloadLength(attributes.size());
        try (var input = Files.newInputStream(requiredPath, LinkOption.NOFOLLOW_LINKS);
             var reader = new InputStreamReader(
                     new BoundedInputStream(input, MAX_BOOTSTRAP_MANIFEST_BYTES, BOOTSTRAP_MANIFEST_DESCRIPTION),
                     StandardCharsets.UTF_8
             )) {
            return fromProperties(StrictProperties.load(reader, BOOTSTRAP_MANIFEST_DESCRIPTION));
        }
    }

    public static void write(Path path, BootstrapManifest manifest) throws IOException {
        var resolvedPath = requireArgument(path, "path");
        var resolvedManifest = requireArgument(manifest, "manifest");
        DurableAtomicFileWriter.write(resolvedPath, tempPath -> writeProperties(tempPath, toProperties(resolvedManifest)));
    }

    public static BootstrapManifest fromProperties(Properties properties) {
        var requiredProperties = requireArgument(properties, "properties");
        rejectUnknownKeys(requiredProperties);
        var peers = new ArrayList<BootstrapPeer>();
        for (int index = 0; ; index++) {
            var prefix = PEER_PREFIX + index + ".";
            if (!hasPeer(requiredProperties, prefix)) {
                requireNoLaterPeers(requiredProperties, index);
                break;
            }
            peers.add(new BootstrapPeer(
                    NodeId.of(required(requiredProperties, prefix + "nodeId")),
                    requiredEndpointUri(requiredProperties, prefix + "publicEndpoint"),
                    requiredGrpcUri(requiredProperties, prefix + "ratisEndpoint"),
                    required(requiredProperties, prefix + "failureDomain")
            ));
        }
        return new BootstrapManifest(
                ClusterId.parse(required(requiredProperties, CLUSTER_ID_KEY)),
                required(requiredProperties, METADATA_GROUP_ID_KEY),
                peers,
                requiredBootstrapEpoch(requiredProperties)
        );
    }

    public static Properties toProperties(BootstrapManifest manifest) {
        var requiredManifest = requireArgument(manifest, "manifest");
        var properties = new Properties();
        properties.setProperty(CLUSTER_ID_KEY, requiredManifest.clusterId().toString());
        properties.setProperty(METADATA_GROUP_ID_KEY, requiredManifest.metadataGroupId());
        properties.setProperty(BOOTSTRAP_EPOCH_KEY, Long.toString(requiredManifest.bootstrapEpoch()));
        for (int index = 0; index < requiredManifest.initialPeers().size(); index++) {
            var peer = requiredManifest.initialPeers().get(index);
            var prefix = PEER_PREFIX + index + ".";
            properties.setProperty(prefix + "nodeId", peer.nodeId().toString());
            properties.setProperty(prefix + "publicEndpoint", peer.publicEndpoint().toString());
            properties.setProperty(prefix + "ratisEndpoint", peer.ratisEndpoint().toString());
            properties.setProperty(prefix + "failureDomain", peer.failureDomain());
        }
        return properties;
    }

    private static void rejectUnknownKeys(Properties properties) {
        for (var keyObject : properties.keySet()) {
            var key = keyObject.toString();
            if (!KNOWN_KEY.matcher(key).matches()) {
                throw new IllegalArgumentException("Unknown bootstrap manifest key: " + key);
            }
        }
    }

    private static boolean hasPeer(Properties properties, String prefix) {
        return properties.containsKey(prefix + "nodeId")
                || properties.containsKey(prefix + "publicEndpoint")
                || properties.containsKey(prefix + "ratisEndpoint")
                || properties.containsKey(prefix + "failureDomain");
    }

    private static void requireNoLaterPeers(Properties properties, int missingIndex) {
        for (var keyObject : properties.keySet()) {
            var key = keyObject.toString();
            if (!key.startsWith(PEER_PREFIX)) {
                continue;
            }
            var indexEnd = key.indexOf('.', PEER_PREFIX.length());
            if (indexEnd < 0) {
                continue;
            }
            var index = parsePeerIndex(key, key.substring(PEER_PREFIX.length(), indexEnd));
            if (index > missingIndex) {
                throw new IllegalArgumentException("bootstrap manifest peer indexes must be contiguous");
            }
        }
    }

    private static String required(Properties properties, String key) {
        var value = properties.getProperty(key);
        if (value == null || value.isBlank()) {
            throw new IllegalArgumentException("Missing bootstrap manifest key: " + key);
        }
        return value;
    }

    private static long requiredBootstrapEpoch(Properties properties) {
        var value = required(properties, BOOTSTRAP_EPOCH_KEY);
        try {
            var parsed = Long.parseLong(value);
            if (!Long.toString(parsed).equals(value)) {
                throw new IllegalArgumentException("Non-canonical bootstrap manifest long key: "
                        + BOOTSTRAP_EPOCH_KEY);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid bootstrap manifest long key: "
                    + BOOTSTRAP_EPOCH_KEY, e);
        }
    }

    private static int parsePeerIndex(String key, String index) {
        try {
            var parsed = Integer.parseInt(index);
            if (parsed < 0) {
                throw new IllegalArgumentException("Negative bootstrap manifest peer index: " + key);
            }
            if (!Integer.toString(parsed).equals(index)) {
                throw new IllegalArgumentException("Non-canonical bootstrap manifest peer index: " + key);
            }
            return parsed;
        } catch (NumberFormatException e) {
            throw new IllegalArgumentException("Invalid bootstrap manifest peer index: " + key, e);
        }
    }

    private static URI requiredGrpcUri(Properties properties, String key) {
        return EndpointUris.requireGrpcUri(requiredUri(properties, key), "Bootstrap manifest key " + key);
    }

    private static URI requiredEndpointUri(Properties properties, String key) {
        return EndpointUris.requireEndpointUri(requiredUri(properties, key), "Bootstrap manifest key " + key);
    }

    private static URI requiredUri(Properties properties, String key) {
        var value = required(properties, key);
        try {
            return URI.create(value);
        } catch (IllegalArgumentException e) {
            throw new IllegalArgumentException("Bootstrap manifest key must be a URI: " + key, e);
        }
    }

    private static void writeProperties(Path path, Properties properties) throws IOException {
        try (var output = Files.newOutputStream(path)) {
            var writer = new OutputStreamWriter(
                    new BoundedOutputStream(output, MAX_BOOTSTRAP_MANIFEST_BYTES, BOOTSTRAP_MANIFEST_DESCRIPTION),
                    StandardCharsets.UTF_8
            );
            properties.store(writer, "Rockserver bootstrap manifest");
            writer.flush();
        }
    }

    private static void requirePayloadLength(long length) {
        if (length > MAX_BOOTSTRAP_MANIFEST_BYTES) {
            throw new IllegalArgumentException("Invalid " + BOOTSTRAP_MANIFEST_DESCRIPTION + " length: " + length);
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }
}
