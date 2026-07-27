package it.cavallium.rockserver.core.impl;

import static it.cavallium.rockserver.core.common.OperationFamily.BOUNDED_FAN_OUT;
import static it.cavallium.rockserver.core.common.OperationFamily.BOUNDARY_SEEK;
import static it.cavallium.rockserver.core.common.OperationFamily.COMPACTION;
import static it.cavallium.rockserver.core.common.OperationFamily.FLUSH;
import static it.cavallium.rockserver.core.common.OperationFamily.FULL_SCAN_AGGREGATE;
import static it.cavallium.rockserver.core.common.OperationFamily.METADATA;
import static it.cavallium.rockserver.core.common.OperationFamily.MUTATION;
import static it.cavallium.rockserver.core.common.OperationFamily.POINT_LOOKUP;
import static it.cavallium.rockserver.core.common.OperationFamily.RANGE_PAGE;
import static it.cavallium.rockserver.core.common.OperationFamily.WAL_PAGE;
import static it.cavallium.rockserver.core.common.WorkloadProfile.ANALYTICAL;
import static it.cavallium.rockserver.core.common.WorkloadProfile.BATCH;
import static it.cavallium.rockserver.core.common.WorkloadProfile.CDC;
import static it.cavallium.rockserver.core.common.WorkloadProfile.CONTROL;
import static it.cavallium.rockserver.core.common.WorkloadProfile.INGEST;
import static it.cavallium.rockserver.core.common.WorkloadProfile.LATENCY;
import static it.cavallium.rockserver.core.common.WorkloadProfile.PHYSICAL_MAINTENANCE;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.Map;
import java.util.Objects;

/** Authoritative profile/operation compatibility contract used before admission. */
public final class WorkloadAdmission {

	private static final Map<WorkloadProfile, EnumSet<OperationFamily>> ALLOWED = allowedCombinations();

	private WorkloadAdmission() {
	}

	/** Validate a caller-selected context against Rockserver's derived operation family. */
	public static void validateClient(RequestContext context, OperationFamily family) {
		Objects.requireNonNull(context, "context");
		if (!context.profile().isClientSelectable()) {
			throw mismatch(context.profile(), family, "profile is server-owned");
		}
		validate(context.profile(), family);
	}

	/**
	 * Resolve a public command to the profile Rockserver actually schedules. Protected
	 * operation families always override the caller's view context.
	 */
	public static WorkloadProfile resolve(RequestContext context, OperationFamily family) {
		Objects.requireNonNull(context, "context");
		Objects.requireNonNull(family, "family");
		var resolved = switch (family) {
			case CONTROL -> WorkloadProfile.CONTROL;
			case WAL_PAGE -> WorkloadProfile.CDC;
			case FLUSH, COMPACTION -> WorkloadProfile.PHYSICAL_MAINTENANCE;
			default -> context.profile();
		};
		if (resolved.isClientSelectable()) {
			validateClient(context, family);
		} else {
			validate(resolved, family);
		}
		return resolved;
	}

	/** Resolve using both the server-derived family and protected command ownership. */
	public static WorkloadProfile resolve(RequestContext context, RocksDBAPICommand<?, ?, ?> command) {
		Objects.requireNonNull(command, "command");
		var protectedProfile = command.protectedProfile();
		if (protectedProfile == null) {
			validateClient(context, command.operationFamily());
			return context.profile();
		}
		validate(protectedProfile, command.operationFamily());
		return protectedProfile;
	}

	/** Validate a server-derived protected or client profile/family pair. */
	public static void validate(WorkloadProfile profile, OperationFamily family) {
		Objects.requireNonNull(profile, "profile");
		Objects.requireNonNull(family, "family");
		if (!ALLOWED.get(profile).contains(family)) {
			throw mismatch(profile, family, "combination is not permitted by the workload contract");
		}
	}

	public static boolean isAllowed(WorkloadProfile profile, OperationFamily family) {
		Objects.requireNonNull(profile, "profile");
		Objects.requireNonNull(family, "family");
		return ALLOWED.get(profile).contains(family);
	}

	private static RocksDBException mismatch(WorkloadProfile profile,
			OperationFamily family,
			String reason) {
		return RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
				"Invalid workload profile " + profile + " for " + family + ": " + reason);
	}

	private static Map<WorkloadProfile, EnumSet<OperationFamily>> allowedCombinations() {
		var allowed = new EnumMap<WorkloadProfile, EnumSet<OperationFamily>>(WorkloadProfile.class);
		allowed.put(CONTROL, EnumSet.of(OperationFamily.CONTROL));
		allowed.put(LATENCY, EnumSet.of(METADATA,
				POINT_LOOKUP,
				BOUNDARY_SEEK,
				BOUNDED_FAN_OUT,
				RANGE_PAGE,
				MUTATION));
		allowed.put(ANALYTICAL, EnumSet.of(METADATA,
				POINT_LOOKUP,
				BOUNDARY_SEEK,
				BOUNDED_FAN_OUT,
				RANGE_PAGE,
				FULL_SCAN_AGGREGATE));
		allowed.put(INGEST, EnumSet.of(METADATA,
				POINT_LOOKUP,
				BOUNDED_FAN_OUT,
				RANGE_PAGE,
				MUTATION));
		allowed.put(CDC, EnumSet.of(WAL_PAGE, MUTATION, FLUSH));
		allowed.put(BATCH, EnumSet.of(METADATA,
				POINT_LOOKUP,
				BOUNDARY_SEEK,
				BOUNDED_FAN_OUT,
				RANGE_PAGE,
				FULL_SCAN_AGGREGATE,
				MUTATION));
		allowed.put(PHYSICAL_MAINTENANCE, EnumSet.of(FLUSH, COMPACTION));
		if (allowed.size() != WorkloadProfile.values().length) {
			throw new ExceptionInInitializerError("Every workload profile must have an explicit operation matrix");
		}
		return Map.copyOf(allowed);
	}
}
