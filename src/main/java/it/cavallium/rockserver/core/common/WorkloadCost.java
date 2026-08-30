package it.cavallium.rockserver.core.common;

/** Shared byte-cost contract between command estimation and scheduler deficit accounting. */
public final class WorkloadCost {

	public static final long QUANTUM_BYTES = 2L * 1024L * 1024L;
	public static final int MAX_UNITS = 16;
	public static final long MAX_ESTIMATED_BYTES = QUANTUM_BYTES * MAX_UNITS;

	private WorkloadCost() {
	}
}
