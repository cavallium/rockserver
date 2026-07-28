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

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.util.Arrays;
import java.util.EnumMap;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Authoritative profile/operation compatibility contract used before admission. */
public final class WorkloadAdmission {

	public static final int MAX_LATENCY_ITEMS = 256;
	public static final long MAX_LATENCY_ENCODED_INPUT_BYTES = 2L * 1024 * 1024;
	public static final long MAX_LATENCY_ITERATOR_ADVANCE = 4_096;

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

	/** Validate the concrete command in addition to its server-derived family. */
	public static void validateClient(RequestContext context, RocksDBAPICommand<?, ?, ?> command) {
		Objects.requireNonNull(context, "context");
		Objects.requireNonNull(command, "command");
		validateClient(context, command.operationFamily());
		validateCommand(context.profile(), command);
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
		Objects.requireNonNull(context, "context");
		Objects.requireNonNull(command, "command");
		var protectedProfile = command.protectedProfile();
		if (protectedProfile == null) {
			validateClient(context, command);
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

	private static RocksDBException mismatch(WorkloadProfile profile,
			RocksDBAPICommand<?, ?, ?> command,
			String reason) {
		return RocksDBException.of(RocksDBErrorType.PUT_INVALID_REQUEST,
				"Invalid workload profile " + profile + " for " + command.getClass().getSimpleName() + ": " + reason);
	}

	private static void validateCommand(WorkloadProfile profile, RocksDBAPICommand<?, ?, ?> command) {
		switch (command) {
			case RocksDBAPICommandSingle.CreateColumn _, RocksDBAPICommandSingle.UploadMergeOperator _,
					RocksDBAPICommandSingle.DeleteColumn _, RocksDBAPICommandSingle.DeleteColumnIfExists _,
					RocksDBAPICommandStream.ScanRaw _ -> requireProfile(profile, command, BATCH);
			case RocksDBAPICommandSingle.DeleteRange _, RocksDBAPICommandSingle.PutBatch _,
					RocksDBAPICommandSingle.MergeBatch _ -> requireProfile(profile, command, INGEST, BATCH);
			case RocksDBAPICommandStream.GetRange<?> _ -> requireProfile(profile, command, ANALYTICAL, BATCH);
			case RocksDBAPICommandSingle.CloseTransaction close when close.commit() -> {
				if (profile == LATENCY) {
					throw mismatch(profile, command,
							"LATENCY mutations must be point or fixed multi-operations");
				}
			}
			case RocksDBAPICommandSingle.Put<?> put -> validateLatencyInput(profile,
					command,
					1,
					encodedBytes(put.keys(), put.value()));
			case RocksDBAPICommandSingle.Delete<?> delete -> validateLatencyInput(profile,
					command,
					1,
					encodedBytes(delete.keys()));
			case RocksDBAPICommandSingle.Merge<?> merge -> validateLatencyInput(profile,
					command,
					1,
					encodedBytes(merge.keys(), merge.value()));
			case RocksDBAPICommandSingle.DeleteMulti<?> delete -> validateLatencyInput(profile,
					command,
					delete.keys().size(),
					encodedBytes(delete.keys()));
			case RocksDBAPICommandSingle.PutMulti<?> put -> validateLatencyInput(profile,
					command,
					Math.max(put.keys().size(), put.values().size()),
					encodedBytes(put.keys(), put.values()));
			case RocksDBAPICommandSingle.MergeMulti<?> merge -> validateLatencyInput(profile,
					command,
					Math.max(merge.keys().size(), merge.values().size()),
					encodedBytes(merge.keys(), merge.values()));
			case RocksDBAPICommandSingle.ExistsMulti exists -> validateLatencyInput(profile,
					command,
					exists.keys().size(),
					encodedBytes(exists.keys()));
			case RocksDBAPICommandSingle.Subsequent<?> subsequent -> {
				if (profile == LATENCY && exceedsIteratorLimit(subsequent.skipCount(), subsequent.takeCount())) {
					throw mismatch(profile, command,
							"iterator skip + take must not exceed " + MAX_LATENCY_ITERATOR_ADVANCE);
				}
			}
			case RocksDBAPICommandSingle.OpenTransaction _, RocksDBAPICommandSingle.CloseTransaction _,
					RocksDBAPICommandSingle.CloseFailedUpdate _, RocksDBAPICommandSingle.CheckMergeOperator _,
					RocksDBAPICommandSingle.GetColumnId _, RocksDBAPICommandSingle.EstimateNumKeys _,
					RocksDBAPICommandSingle.Get<?> _, RocksDBAPICommandSingle.OpenIterator _,
					RocksDBAPICommandSingle.CloseIterator _, RocksDBAPICommandSingle.SeekTo _,
					RocksDBAPICommandSingle.ReduceRange<?> _, RocksDBAPICommandStream.CdcPoll _,
					RocksDBAPICommand.Flush _, RocksDBAPICommand.Compact _,
					RocksDBAPICommand.GetAllColumnDefinitions _, RocksDBAPICommand.CdcCreate _,
					RocksDBAPICommand.CdcDelete _, RocksDBAPICommand.CdcGetEarliestAvailableSequence _,
					RocksDBAPICommand.CdcGetLastCommittedSequence _, RocksDBAPICommand.CdcCommit _ -> {
				// The operation-family matrix or protected server ownership is sufficient.
			}
		}
	}

	private static void requireProfile(WorkloadProfile profile,
			RocksDBAPICommand<?, ?, ?> command,
			WorkloadProfile... permitted) {
		for (var candidate : permitted) {
			if (profile == candidate) {
				return;
			}
		}
		throw mismatch(profile, command, "command is restricted to " + Arrays.toString(permitted));
	}

	private static void validateLatencyInput(WorkloadProfile profile,
			RocksDBAPICommand<?, ?, ?> command,
			int items,
			long encodedBytes) {
		if (profile != LATENCY) {
			return;
		}
		if (items > MAX_LATENCY_ITEMS) {
			throw mismatch(profile, command, "item count must not exceed " + MAX_LATENCY_ITEMS);
		}
		if (encodedBytes > MAX_LATENCY_ENCODED_INPUT_BYTES) {
			throw mismatch(profile,
					command,
					"encoded input must not exceed " + MAX_LATENCY_ENCODED_INPUT_BYTES + " bytes");
		}
	}

	private static boolean exceedsIteratorLimit(long skipCount, long takeCount) {
		return skipCount > MAX_LATENCY_ITERATOR_ADVANCE
				|| takeCount > MAX_LATENCY_ITERATOR_ADVANCE
				|| skipCount > MAX_LATENCY_ITERATOR_ADVANCE - takeCount;
	}

	private static long encodedBytes(Keys keys, Buf... values) {
		long total = encodedBytes(keys);
		for (var value : values) {
			total = addSize(total, value.size());
		}
		return total;
	}

	private static long encodedBytes(List<Keys> keys) {
		long total = 0;
		for (var key : keys) {
			total = addSize(total, encodedBytes(key));
		}
		return total;
	}

	private static long encodedBytes(List<Keys> keys, List<Buf> values) {
		long total = encodedBytes(keys);
		for (var value : values) {
			total = addSize(total, value.size());
		}
		return total;
	}

	private static long encodedBytes(Keys keys) {
		long total = 0;
		for (var key : keys.keys()) {
			total = addSize(total, key.size());
		}
		return total;
	}

	private static long addSize(long total, long bytes) {
		if (total > MAX_LATENCY_ENCODED_INPUT_BYTES || bytes > MAX_LATENCY_ENCODED_INPUT_BYTES - total) {
			return MAX_LATENCY_ENCODED_INPUT_BYTES + 1;
		}
		return total + bytes;
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
