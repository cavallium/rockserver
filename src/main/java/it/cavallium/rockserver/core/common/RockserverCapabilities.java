package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;

/** Mandatory protocol capabilities required by workload-aware clients. */
public record RockserverCapabilities(int workloadContractVersion, boolean boundedRange) {

	public static final int REQUIRED_WORKLOAD_CONTRACT_VERSION = 2;
	public static final RockserverCapabilities CURRENT =
			new RockserverCapabilities(REQUIRED_WORKLOAD_CONTRACT_VERSION, true);

	public void requireCompatible() {
		if (workloadContractVersion != REQUIRED_WORKLOAD_CONTRACT_VERSION || !boundedRange) {
			throw RocksDBException.of(RocksDBErrorType.NOT_IMPLEMENTED,
					"Incompatible Rockserver capabilities: workloadContractVersion="
							+ workloadContractVersion + ", boundedRange=" + boundedRange
							+ "; required workloadContractVersion=" + REQUIRED_WORKLOAD_CONTRACT_VERSION
							+ " with bounded ranges");
		}
	}
}
