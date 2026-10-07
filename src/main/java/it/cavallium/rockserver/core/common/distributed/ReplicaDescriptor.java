package it.cavallium.rockserver.core.common.distributed;

import java.util.Objects;
import java.util.UUID;

/** Immutable node/incarnation identity and a credential-free Raft host:port endpoint. */
public record ReplicaDescriptor(UUID nodeId, UUID nodeIncarnation, UUID replicaId,
                                String peerId, String address, Role role) {
    public enum Role { VOTER, LISTENER }
    static final int MAX_PEER_BYTES = 128;
    static final int MAX_ADDRESS_BYTES = 256;

    public ReplicaDescriptor {
        Objects.requireNonNull(nodeId, "nodeId");
        Objects.requireNonNull(nodeIncarnation, "nodeIncarnation");
        Objects.requireNonNull(replicaId, "replicaId");
        Objects.requireNonNull(role, "role");
        if (peerId == null || peerId.isEmpty() || peerId.length() > MAX_PEER_BYTES
                || !peerId.matches("[A-Za-z0-9._-]+")) throw new IllegalArgumentException("Invalid bounded ASCII peer ID");
        if (address == null || address.length() > MAX_ADDRESS_BYTES || !address.matches("[A-Za-z0-9._\\-\\[\\]:]+")) {
            throw new IllegalArgumentException("Invalid credential-free ASCII endpoint");
        }
        int colon = address.lastIndexOf(':');
        if (colon <= 0 || colon == address.length() - 1) throw new IllegalArgumentException("Endpoint requires host:port");
        String host = address.substring(0, colon);
        if (host.contains(":") && !(host.startsWith("[") && host.endsWith("]"))) {
            throw new IllegalArgumentException("IPv6 endpoint requires brackets");
        }
        String port = address.substring(colon + 1);
        if (!port.matches("[0-9]{1,5}")) throw new IllegalArgumentException("Invalid endpoint port");
        int number = Integer.parseInt(port);
        if (number < 1 || number > 65535) throw new IllegalArgumentException("Endpoint port outside valid range");
    }
}
