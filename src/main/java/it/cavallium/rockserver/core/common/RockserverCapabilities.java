package it.cavallium.rockserver.core.common;

import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;

/** Protocol capabilities exposed by a Rockserver connection. */
public record RockserverCapabilities(int workloadContractVersion,
		boolean boundedRange,
		boolean resumableRawScan) {

	public static final int REQUIRED_WORKLOAD_CONTRACT_VERSION = 2;
	public static final RockserverCapabilities COMPATIBLE_BASELINE =
			new RockserverCapabilities(REQUIRED_WORKLOAD_CONTRACT_VERSION, true, false);
	public static final RockserverCapabilities CURRENT =
			new RockserverCapabilities(REQUIRED_WORKLOAD_CONTRACT_VERSION, true, true);

	/** Preserve the pre-extension constructor while defaulting optional capabilities safely. */
	public RockserverCapabilities(int workloadContractVersion, boolean boundedRange) {
		this(workloadContractVersion, boundedRange, false);
	}

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
