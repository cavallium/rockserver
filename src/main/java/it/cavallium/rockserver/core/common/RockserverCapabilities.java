package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;

/**
 * Protocol capabilities exposed by a Rockserver connection.
 *
 * @param workloadContractVersion the workload contract the server implements
 * @param clientSessions the server is replicated and numbers client writes: a client can open a session
 *                       ({@code openClientSession}) and resend a write with its request id after an unknown outcome,
 *                       without applying it twice
 */
public record RockserverCapabilities(int workloadContractVersion, boolean clientSessions) {

	public static final int REQUIRED_WORKLOAD_CONTRACT_VERSION = 3;
	public static final RockserverCapabilities CURRENT =
			new RockserverCapabilities(REQUIRED_WORKLOAD_CONTRACT_VERSION);

	public RockserverCapabilities(int workloadContractVersion) {
		this(workloadContractVersion, false);
	}

	public RockserverCapabilities withClientSessions(boolean value) {
		return new RockserverCapabilities(workloadContractVersion, value);
	}

	public void requireCompatible() {
		if (workloadContractVersion != REQUIRED_WORKLOAD_CONTRACT_VERSION) {
			throw RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,
					"Incompatible Rockserver capabilities: workloadContractVersion="
							+ workloadContractVersion
							+ "; required workloadContractVersion=" + REQUIRED_WORKLOAD_CONTRACT_VERSION
							+ " (bounded ranges and resumable raw scans are mandatory)");
		}
	}
}
