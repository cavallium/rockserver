package it.cavallium.rockserver.core.common;

/**
 * Server-derived operation and resource-cost family.
 *
 * <p>This value is never caller-selected. Rockserver derives it from the concrete
 * request, records it in metrics together with the workload profile, and validates
 * the pair before admission.</p>
 */
public enum OperationFamily {
	METADATA,
	POINT_LOOKUP,
	BOUNDARY_SEEK,
	BOUNDED_FAN_OUT,
	RANGE_PAGE,
	FULL_SCAN_AGGREGATE,
	WAL_PAGE,
	MUTATION,
	CONTROL,
	FLUSH,
	COMPACTION
}
