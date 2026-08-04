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
import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;

/** Authoritative profile/operation compatibility contract used before admission. */
public final class WorkloadAdmission {

	public static final int MAX_LATENCY_ITEMS = 256;
	public static final long MAX_LATENCY_ENCODED_INPUT_BYTES = 2L * 1024 * 1024;
	public static final long MAX_LATENCY_ITERATOR_ADVANCE = 4_096;

	private static final long[] ALLOWED_MASKS = allowedCombinations();

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
		validateClient(context, command, MAX_LATENCY_ITEMS, MAX_LATENCY_ENCODED_INPUT_BYTES);
	}

	/**
	 * Validate a concrete command against server-resolved LATENCY limits. The configured
	 * values may lower, but never raise, the public workload-contract ceilings.
	 */
	public static void validateClient(RequestContext context,
			RocksDBAPICommand<?, ?, ?> command,
			int maximumLatencyItems,
			long maximumLatencyEncodedInputBytes) {
		validateClient(context,
				command,
				maximumLatencyItems,
				maximumLatencyEncodedInputBytes,
				RangeBudget.DEFAULT_MAX_ITEMS,
				RangeBudget.DEFAULT_MAX_BYTES);
	}

	/** Validate a concrete command against all server-resolved bounded-input maxima. */
	public static void validateClient(RequestContext context,
			RocksDBAPICommand<?, ?, ?> command,
			int maximumLatencyItems,
			long maximumLatencyEncodedInputBytes,
			int maximumLatencyRangeItems,
			long maximumLatencyRangeBytes) {
		Objects.requireNonNull(context, "context");
		Objects.requireNonNull(command, "command");
		validateLatencyLimits(maximumLatencyItems, maximumLatencyEncodedInputBytes);
		validateLatencyRangeLimits(maximumLatencyRangeItems, maximumLatencyRangeBytes);
		validateClient(context, command.operationFamily());
		validateCommand(context.profile(),
				command,
				maximumLatencyItems,
				maximumLatencyEncodedInputBytes,
				maximumLatencyRangeItems,
				maximumLatencyRangeBytes);
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
		return resolve(context, command, MAX_LATENCY_ITEMS, MAX_LATENCY_ENCODED_INPUT_BYTES);
	}

	/** Resolve a command while enforcing server-configured LATENCY input maxima. */
	public static WorkloadProfile resolve(RequestContext context,
			RocksDBAPICommand<?, ?, ?> command,
			int maximumLatencyItems,
			long maximumLatencyEncodedInputBytes) {
		return resolve(context,
				command,
				maximumLatencyItems,
				maximumLatencyEncodedInputBytes,
				RangeBudget.DEFAULT_MAX_ITEMS,
				RangeBudget.DEFAULT_MAX_BYTES);
	}

	/** Resolve a command while enforcing server-configured LATENCY input and range maxima. */
	public static WorkloadProfile resolve(RequestContext context,
			RocksDBAPICommand<?, ?, ?> command,
			int maximumLatencyItems,
			long maximumLatencyEncodedInputBytes,
			int maximumLatencyRangeItems,
			long maximumLatencyRangeBytes) {
		Objects.requireNonNull(context, "context");
		Objects.requireNonNull(command, "command");
		var protectedProfile = command.protectedProfile();
		if (protectedProfile == null) {
			validateClient(context,
					command,
					maximumLatencyItems,
					maximumLatencyEncodedInputBytes,
					maximumLatencyRangeItems,
					maximumLatencyRangeBytes);
			return context.profile();
		}
		validate(protectedProfile, command.operationFamily());
		return protectedProfile;
	}

	/** Validate a server-derived protected or client profile/family pair. */
	public static void validate(WorkloadProfile profile, OperationFamily family) {
		Objects.requireNonNull(profile, "profile");
		Objects.requireNonNull(family, "family");
		if (!isAllowedOrdinal(profile, family)) {
			throw mismatch(profile, family, "combination is not permitted by the workload contract");
		}
	}

	public static boolean isAllowed(WorkloadProfile profile, OperationFamily family) {
		Objects.requireNonNull(profile, "profile");
		Objects.requireNonNull(family, "family");
		return isAllowedOrdinal(profile, family);
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

	private static void validateCommand(WorkloadProfile profile,
			RocksDBAPICommand<?, ?, ?> command,
			int maximumLatencyItems,
			long maximumLatencyEncodedInputBytes,
			int maximumLatencyRangeItems,
			long maximumLatencyRangeBytes) {
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
					encodedBytes(put.keys(), put.value()),
					maximumLatencyItems,
					maximumLatencyEncodedInputBytes);
			case RocksDBAPICommandSingle.Delete<?> delete -> validateLatencyInput(profile,
					command,
					1,
					encodedBytes(delete.keys()),
					maximumLatencyItems,
					maximumLatencyEncodedInputBytes);
			case RocksDBAPICommandSingle.Merge<?> merge -> validateLatencyInput(profile,
					command,
					1,
					encodedBytes(merge.keys(), merge.value()),
					maximumLatencyItems,
					maximumLatencyEncodedInputBytes);
			case RocksDBAPICommandSingle.DeleteMulti<?> delete -> validateLatencyInput(profile,
					command,
					delete.keys().size(),
					encodedBytes(delete.keys()),
					maximumLatencyItems,
					maximumLatencyEncodedInputBytes);
			case RocksDBAPICommandSingle.PutMulti<?> put -> validateLatencyInput(profile,
					command,
					Math.max(put.keys().size(), put.values().size()),
					encodedBytes(put.keys(), put.values()),
					maximumLatencyItems,
					maximumLatencyEncodedInputBytes);
			case RocksDBAPICommandSingle.MergeMulti<?> merge -> validateLatencyInput(profile,
					command,
					Math.max(merge.keys().size(), merge.values().size()),
					encodedBytes(merge.keys(), merge.values()),
					maximumLatencyItems,
					maximumLatencyEncodedInputBytes);
			case RocksDBAPICommandSingle.ExistsMulti exists -> validateLatencyInput(profile,
					command,
					exists.keys().size(),
					encodedBytes(exists.keys()),
					maximumLatencyItems,
					maximumLatencyEncodedInputBytes);
			case RocksDBAPICommandSingle.Subsequent<?> subsequent -> {
				if (profile == LATENCY && exceedsIteratorLimit(subsequent.skipCount(), subsequent.takeCount())) {
					throw mismatch(profile, command,
							"iterator skip + take must not exceed " + MAX_LATENCY_ITERATOR_ADVANCE);
				}
			}
			case RocksDBAPICommandSingle.GetRangePage<?> page -> validateRangeBudget(profile,
					command,
					page.budget(),
					maximumLatencyRangeItems,
					maximumLatencyRangeBytes);
			case RocksDBAPICommandSingle.OpenTransaction _, RocksDBAPICommandSingle.CloseTransaction _,
					RocksDBAPICommandSingle.CloseFailedUpdate _, RocksDBAPICommandSingle.CheckMergeOperator _,
					RocksDBAPICommandSingle.GetColumnId _, RocksDBAPICommandSingle.EstimateNumKeys _,
					RocksDBAPICommandSingle.Get<?> _,
					RocksDBAPICommandSingle.OpenIterator _,
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
			long encodedBytes,
			int maximumLatencyItems,
			long maximumLatencyEncodedInputBytes) {
		if (profile != LATENCY) {
			return;
		}
		if (items > maximumLatencyItems) {
			throw mismatch(profile, command, "item count must not exceed " + maximumLatencyItems);
		}
		if (encodedBytes > maximumLatencyEncodedInputBytes) {
			throw mismatch(profile,
					command,
					"encoded input must not exceed " + maximumLatencyEncodedInputBytes + " bytes");
		}
	}

	private static void validateLatencyLimits(int maximumLatencyItems,
			long maximumLatencyEncodedInputBytes) {
		if (maximumLatencyItems < 1 || maximumLatencyItems > MAX_LATENCY_ITEMS) {
			throw new IllegalArgumentException("LATENCY item limit must be between 1 and " + MAX_LATENCY_ITEMS);
		}
		if (maximumLatencyEncodedInputBytes < 1
				|| maximumLatencyEncodedInputBytes > MAX_LATENCY_ENCODED_INPUT_BYTES) {
			throw new IllegalArgumentException("LATENCY encoded-input limit must be between 1 and "
					+ MAX_LATENCY_ENCODED_INPUT_BYTES);
		}
	}

	private static void validateLatencyRangeLimits(int maximumLatencyRangeItems,
			long maximumLatencyRangeBytes) {
		if (maximumLatencyRangeItems < 1 || maximumLatencyRangeItems > RangeBudget.DEFAULT_MAX_ITEMS) {
			throw new IllegalArgumentException("LATENCY range item limit must be between 1 and "
					+ RangeBudget.DEFAULT_MAX_ITEMS);
		}
		if (maximumLatencyRangeBytes < 1 || maximumLatencyRangeBytes > RangeBudget.DEFAULT_MAX_BYTES) {
			throw new IllegalArgumentException("LATENCY range byte limit must be between 1 and "
					+ RangeBudget.DEFAULT_MAX_BYTES);
		}
	}

	private static void validateRangeBudget(WorkloadProfile profile,
			RocksDBAPICommand<?, ?, ?> command,
			RangeBudget budget,
			int maximumLatencyRangeItems,
			long maximumLatencyRangeBytes) {
		Objects.requireNonNull(budget, "budget");
		int maximumItems = profile == LATENCY
				? maximumLatencyRangeItems
				: RangeBudget.DEFAULT_MAX_ITEMS;
		long maximumBytes = profile == LATENCY
				? maximumLatencyRangeBytes
				: RangeBudget.DEFAULT_MAX_BYTES;
		if (budget.maxItems() > maximumItems) {
			throw mismatch(profile, command, "range maxItems must not exceed " + maximumItems);
		}
		if (budget.maxBytes() > maximumBytes) {
			throw mismatch(profile, command, "range maxBytes must not exceed " + maximumBytes);
		}
	}

	private static boolean exceedsIteratorLimit(long skipCount, long takeCount) {
		return skipCount > MAX_LATENCY_ITERATOR_ADVANCE
				|| takeCount > MAX_LATENCY_ITERATOR_ADVANCE
				|| skipCount > MAX_LATENCY_ITERATOR_ADVANCE - takeCount;
	}

	private static long encodedBytes(Keys keys, Buf... values) {
		long total = encodedBytes(keys);
		if (values == null) {
			throw unexpectedNull("values");
		}
		for (var value : values) {
			if (value == null) {
				throw unexpectedNull("value");
			}
			total = addSize(total, value.size());
		}
		return total;
	}

	private static long encodedBytes(List<Keys> keys) {
		if (keys == null) {
			throw unexpectedNull("keys");
		}
		long total = 0;
		for (var key : keys) {
			total = addSize(total, encodedBytes(key));
		}
		return total;
	}

	private static long encodedBytes(List<Keys> keys, List<Buf> values) {
		long total = encodedBytes(keys);
		if (values == null) {
			throw unexpectedNull("values");
		}
		for (var value : values) {
			if (value == null) {
				throw unexpectedNull("value");
			}
			total = addSize(total, value.size());
		}
		return total;
	}

	private static long encodedBytes(Keys keys) {
		if (keys == null || keys.keys() == null) {
			throw unexpectedNull("keys");
		}
		long total = 0;
		for (var key : keys.keys()) {
			if (key == null) {
				throw unexpectedNull("key");
			}
			total = addSize(total, key.size());
		}
		return total;
	}

	private static RocksDBException unexpectedNull(String name) {
		return RocksDBException.of(RocksDBErrorType.UNEXPECTED_NULL_VALUE, name);
	}

	private static long addSize(long total, long bytes) {
		if (total > MAX_LATENCY_ENCODED_INPUT_BYTES || bytes > MAX_LATENCY_ENCODED_INPUT_BYTES - total) {
			return MAX_LATENCY_ENCODED_INPUT_BYTES + 1;
		}
		return total + bytes;
	}

	private static boolean isAllowedOrdinal(WorkloadProfile profile, OperationFamily family) {
		return (ALLOWED_MASKS[profile.ordinal()] & (1L << family.ordinal())) != 0L;
	}

	private static long[] allowedCombinations() {
		if (OperationFamily.values().length > Long.SIZE) {
			throw new ExceptionInInitializerError("The operation matrix exceeds one ordinal mask");
		}
		var allowed = new long[WorkloadProfile.values().length];
		allowed[CONTROL.ordinal()] = mask(OperationFamily.CONTROL);
		allowed[LATENCY.ordinal()] = mask(METADATA,
				POINT_LOOKUP,
				BOUNDARY_SEEK,
				BOUNDED_FAN_OUT,
				RANGE_PAGE,
				MUTATION);
		allowed[ANALYTICAL.ordinal()] = mask(METADATA,
				POINT_LOOKUP,
				BOUNDARY_SEEK,
				BOUNDED_FAN_OUT,
				RANGE_PAGE,
				FULL_SCAN_AGGREGATE);
		allowed[INGEST.ordinal()] = mask(METADATA,
				POINT_LOOKUP,
				BOUNDED_FAN_OUT,
				RANGE_PAGE,
				MUTATION);
		allowed[CDC.ordinal()] = mask(WAL_PAGE, MUTATION, FLUSH);
		allowed[BATCH.ordinal()] = mask(METADATA,
				POINT_LOOKUP,
				BOUNDARY_SEEK,
				BOUNDED_FAN_OUT,
				RANGE_PAGE,
				FULL_SCAN_AGGREGATE,
				MUTATION);
		allowed[PHYSICAL_MAINTENANCE.ordinal()] = mask(FLUSH, COMPACTION);
		for (long allowedMask : allowed) {
			if (allowedMask == 0L) {
				throw new ExceptionInInitializerError("Every workload profile must have an explicit operation matrix");
			}
		}
		return allowed;
	}

	private static long mask(OperationFamily... families) {
		long result = 0L;
		for (var family : families) {
			result |= 1L << family.ordinal();
		}
		return result;
	}
}
