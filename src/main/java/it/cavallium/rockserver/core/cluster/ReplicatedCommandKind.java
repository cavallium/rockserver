package it.cavallium.rockserver.core.cluster;

public enum ReplicatedCommandKind {
	REPLICATED_WRITE(true, true),
	REPLICATED_METADATA(true, true),
	ROUTED_READ(false, false),
	LOCAL_SESSION(false, false),
	LOCAL_ADMIN(false, false),
	CDC(false, false),
	UNSUPPORTED(false, false);

	private final boolean entersReplicatedLog;
	private final boolean mutatesReplicatedState;

	ReplicatedCommandKind(boolean entersReplicatedLog, boolean mutatesReplicatedState) {
		this.entersReplicatedLog = entersReplicatedLog;
		this.mutatesReplicatedState = mutatesReplicatedState;
	}

	public boolean entersReplicatedLog() {
		return entersReplicatedLog;
	}

	public boolean mutatesReplicatedState() {
		return mutatesReplicatedState;
	}
}
