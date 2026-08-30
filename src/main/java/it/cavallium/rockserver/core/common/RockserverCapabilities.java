package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;

/** Protocol capabilities exposed by a Rockserver connection. */
public record RockserverCapabilities(int workloadContractVersion) {

	public static final int REQUIRED_WORKLOAD_CONTRACT_VERSION = 3;
	public static final RockserverCapabilities CURRENT =
			new RockserverCapabilities(REQUIRED_WORKLOAD_CONTRACT_VERSION);

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
