package it.cavallium.rockserver.core.cluster;

import java.net.URI;

public record ReplicaMember(NodeId nodeId,
							URI endpoint,
							ReplicaRole role,
							long matchIndex,
							long appliedIndex,
							boolean servingReads) {

	public ReplicaMember {
		requireArgument(nodeId, "nodeId");
		endpoint = EndpointUris.requireEndpointUri(endpoint, "replica member endpoint");
		requireArgument(role, "role");
		if (matchIndex < 0) {
			throw new IllegalArgumentException("matchIndex must be >= 0");
		}
		if (appliedIndex < 0) {
			throw new IllegalArgumentException("appliedIndex must be >= 0");
		}
		if (appliedIndex > matchIndex) {
			throw new IllegalArgumentException("appliedIndex must be <= matchIndex");
		}
	}

	public boolean isVoter() {
		return role == ReplicaRole.VOTER;
	}

	private static <T> T requireArgument(T value, String name) {
		if (value == null) {
			throw new IllegalArgumentException(name + " must not be null");
		}
		return value;
	}
}
