package it.cavallium.rockserver.core.impl.test;

import static it.cavallium.rockserver.core.common.WorkloadProfile.ANALYTICAL;
import static it.cavallium.rockserver.core.common.WorkloadProfile.BATCH;
import static it.cavallium.rockserver.core.common.WorkloadProfile.CDC;
import static it.cavallium.rockserver.core.common.WorkloadProfile.CONTROL;
import static it.cavallium.rockserver.core.common.WorkloadProfile.INGEST;
import static it.cavallium.rockserver.core.common.WorkloadProfile.LATENCY;
import static it.cavallium.rockserver.core.common.WorkloadProfile.PHYSICAL_MAINTENANCE;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RawSstToken;
import it.cavallium.rockserver.core.common.RangeBudget;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.common.WorkloadCost;
import it.cavallium.rockserver.core.impl.WorkloadAdmission;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
import java.util.AbstractList;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.Set;
import java.util.function.BiFunction;
import java.util.function.IntFunction;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;

class WorkloadAdmissionTest {

	private static final Map<WorkloadProfile, EnumSet<OperationFamily>> EXPECTED = Map.of(
			CONTROL, EnumSet.of(OperationFamily.CONTROL),
			LATENCY, EnumSet.of(OperationFamily.METADATA,
					OperationFamily.POINT_LOOKUP,
					OperationFamily.BOUNDARY_SEEK,
					OperationFamily.BOUNDED_FAN_OUT,
					OperationFamily.RANGE_PAGE,
					OperationFamily.MUTATION),
			ANALYTICAL, EnumSet.of(OperationFamily.METADATA,
					OperationFamily.POINT_LOOKUP,
					OperationFamily.BOUNDARY_SEEK,
					OperationFamily.BOUNDED_FAN_OUT,
					OperationFamily.RANGE_PAGE,
					OperationFamily.FULL_SCAN_AGGREGATE),
			INGEST, EnumSet.of(OperationFamily.METADATA,
					OperationFamily.POINT_LOOKUP,
					OperationFamily.BOUNDED_FAN_OUT,
					OperationFamily.RANGE_PAGE,
					OperationFamily.MUTATION),
			CDC, EnumSet.of(OperationFamily.WAL_PAGE,
					OperationFamily.MUTATION,
					OperationFamily.FLUSH),
			BATCH, EnumSet.of(OperationFamily.METADATA,
					OperationFamily.POINT_LOOKUP,
					OperationFamily.BOUNDARY_SEEK,
					OperationFamily.BOUNDED_FAN_OUT,
					OperationFamily.RANGE_PAGE,
					OperationFamily.FULL_SCAN_AGGREGATE,
					OperationFamily.MUTATION),
			PHYSICAL_MAINTENANCE, EnumSet.of(OperationFamily.FLUSH, OperationFamily.COMPACTION));

	private static final List<WorkloadProfile> CLIENT_PROFILES = List.of(LATENCY, ANALYTICAL, INGEST, BATCH);
	private static final Keys EMPTY_KEYS = new Keys(Buf.create(0));
	private static final ColumnSchema TEST_SCHEMA = ColumnSchema.of(IntList.of(1), ObjectList.of(), true);
	private static final Map<String, OperationFamily> EXPECTED_COMMAND_FAMILIES = Map.ofEntries(
			Map.entry("openTransaction", OperationFamily.METADATA),
			Map.entry("commitTransaction", OperationFamily.MUTATION),
			Map.entry("rollbackTransaction", OperationFamily.CONTROL),
			Map.entry("closeFailedUpdate", OperationFamily.CONTROL),
			Map.entry("createColumn", OperationFamily.MUTATION),
			Map.entry("uploadMergeOperator", OperationFamily.MUTATION),
			Map.entry("checkMergeOperator", OperationFamily.METADATA),
			Map.entry("deleteColumn", OperationFamily.MUTATION),
			Map.entry("deleteColumnIfExists", OperationFamily.MUTATION),
			Map.entry("getColumnId", OperationFamily.METADATA),
			Map.entry("estimateNumKeys", OperationFamily.METADATA),
			Map.entry("put", OperationFamily.MUTATION),
			Map.entry("delete", OperationFamily.MUTATION),
			Map.entry("deleteMulti", OperationFamily.MUTATION),
			Map.entry("deleteRange", OperationFamily.MUTATION),
			Map.entry("putMulti", OperationFamily.MUTATION),
			Map.entry("putBatch", OperationFamily.MUTATION),
			Map.entry("merge", OperationFamily.MUTATION),
			Map.entry("mergeMulti", OperationFamily.MUTATION),
			Map.entry("mergeBatch", OperationFamily.MUTATION),
			Map.entry("get", OperationFamily.POINT_LOOKUP),
			Map.entry("existsMulti", OperationFamily.BOUNDED_FAN_OUT),
			Map.entry("openIterator", OperationFamily.BOUNDARY_SEEK),
			Map.entry("closeIterator", OperationFamily.CONTROL),
			Map.entry("seekTo", OperationFamily.BOUNDARY_SEEK),
			Map.entry("subsequent", OperationFamily.RANGE_PAGE),
			Map.entry("reduceBoundary", OperationFamily.BOUNDARY_SEEK),
			Map.entry("reduceExact", OperationFamily.FULL_SCAN_AGGREGATE),
			Map.entry("getRangePage", OperationFamily.RANGE_PAGE),
			Map.entry("getRange", OperationFamily.RANGE_PAGE),
			Map.entry("scanRaw", OperationFamily.RANGE_PAGE),
			Map.entry("scanRawResumable", OperationFamily.RANGE_PAGE),
			Map.entry("cdcPoll", OperationFamily.WAL_PAGE),
			Map.entry("flush", OperationFamily.FLUSH),
			Map.entry("compact", OperationFamily.COMPACTION),
			Map.entry("getAllColumnDefinitions", OperationFamily.METADATA),
			Map.entry("cdcCreate", OperationFamily.MUTATION),
			Map.entry("cdcDelete", OperationFamily.MUTATION),
			Map.entry("cdcEarliest", OperationFamily.WAL_PAGE),
			Map.entry("cdcLastCommitted", OperationFamily.WAL_PAGE),
			Map.entry("cdcCommit", OperationFamily.MUTATION));

	@Test
	void everyProfileAndOperationPairHasTheDocumentedResult() {
		for (var profile : WorkloadProfile.values()) {
			for (var family : OperationFamily.values()) {
				boolean expected = EXPECTED.get(profile).contains(family);
				assertEquals(expected, WorkloadAdmission.isAllowed(profile, family),
						profile + " + " + family);
				if (expected) {
					assertDoesNotThrow(() -> WorkloadAdmission.validate(profile, family));
				} else {
					var error = assertThrows(RocksDBException.class,
							() -> WorkloadAdmission.validate(profile, family));
					assertTrue(error.getMessage().contains(profile.name()));
					assertTrue(error.getMessage().contains(family.name()));
				}
			}
		}
	}

	@Test
	void everyConcreteCommandHasAnExplicitResultForEveryClientProfile() {
		var commands = commandExpectations();
		assertEquals(EXPECTED_COMMAND_FAMILIES.keySet(),
				commands.stream().map(CommandExpectation::name).collect(java.util.stream.Collectors.toSet()),
				"Every command variant must have an explicit family expectation");
		assertEquals(permittedConcreteCommandTypes(),
				commands.stream().map(expectation -> expectation.command().getClass()).collect(java.util.stream.Collectors.toSet()),
				"Every sealed command subtype must have an admission expectation");

		for (var expectation : commands) {
			var expectedFamily = EXPECTED_COMMAND_FAMILIES.get(expectation.name());
			assertEquals(expectedFamily, expectation.command().operationFamily(), expectation.name());
			assertEquals(expectation.protectedProfile(), expectation.command().protectedProfile(), expectation.name());
			assertTrue(expectation.command().estimatedBytes() >= 0L,
					expectation.name() + " produced a negative scheduler cost estimate");
			assertEquals(expectation.name().equals("getRangePage") ? 4 : 1,
					RWScheduler.taskCost(expectation.command().estimatedBytes()),
					expectation.name() + " scheduler cost");
			for (var profile : CLIENT_PROFILES) {
				var context = context(profile);
				if (expectation.protectedProfile() != null) {
					var resolved = assertDoesNotThrow(() -> WorkloadAdmission.resolve(context, expectation.command()));
					assertEquals(expectation.protectedProfile(),
							resolved,
							expectation.name() + " from " + profile);
					assertEquals(expectedPool(resolved, expectedFamily),
							RWScheduler.resourcePool(resolved, expectedFamily), expectation.name());
				} else if (expectation.allowedProfiles().contains(profile)) {
					var resolved = assertDoesNotThrow(() -> WorkloadAdmission.resolve(context, expectation.command()));
					assertEquals(profile,
							resolved,
							expectation.name() + " from " + profile);
					assertEquals(expectedPool(resolved, expectedFamily),
							RWScheduler.resourcePool(resolved, expectedFamily), expectation.name());
				} else {
					var error = assertThrows(RocksDBException.class,
							() -> WorkloadAdmission.resolve(context, expectation.command()),
							expectation.name() + " from " + profile);
					assertTrue(error.getMessage().contains(profile.name()), expectation.name());
				}
			}
		}
	}

	private static RWScheduler.Pool expectedPool(WorkloadProfile profile, OperationFamily family) {
		if (profile == CONTROL) return RWScheduler.Pool.CONTROL;
		if (profile == PHYSICAL_MAINTENANCE) return RWScheduler.Pool.PHYSICAL;
		return switch (family) {
			case MUTATION, FLUSH -> RWScheduler.Pool.WRITE;
			case CONTROL -> RWScheduler.Pool.CONTROL;
			case COMPACTION -> RWScheduler.Pool.PHYSICAL;
			case METADATA, POINT_LOOKUP, BOUNDARY_SEEK, BOUNDED_FAN_OUT,
					RANGE_PAGE, FULL_SCAN_AGGREGATE, WAL_PAGE -> RWScheduler.Pool.READ;
		};
	}

	@Test
	void schedulerByteEstimatesCoverBoundedPayloadsWithoutOverflow() {
		assertEquals(7L, new RocksDBAPICommandSingle.Get<>(
				0, 1, keys(7), RequestType.current()).estimatedBytes());
		assertEquals(12L, new RocksDBAPICommandSingle.Put<>(
				0, 1, keys(5), buffer(7), RequestType.none()).estimatedBytes());
		assertEquals(15L, new RocksDBAPICommandSingle.PutMulti<>(
				0, 1, List.of(keys(2), keys(3)), List.of(buffer(4), buffer(6)), RequestType.none())
				.estimatedBytes());
		assertEquals(11L, new RocksDBAPICommandSingle.ExistsMulti(
				0, 1, List.of(keys(5), keys(6))).estimatedBytes());
		assertEquals(19L, new RocksDBAPICommandSingle.UploadMergeOperator(
				"operator", "Type", new byte[19]).estimatedBytes());
		assertEquals(17L, new RocksDBAPICommandSingle.CheckMergeOperator(
				"operator", new byte[17]).estimatedBytes());
		assertEquals(7L, new RocksDBAPICommandSingle.Delete<>(
				0, 1, keys(7), RequestType.none()).estimatedBytes());
		assertEquals(5L, new RocksDBAPICommandSingle.DeleteMulti<>(
				0, 1, List.of(keys(2), keys(3)), RequestType.none()).estimatedBytes());
		assertEquals(7L, new RocksDBAPICommandSingle.DeleteRange(
				1, keys(3), keys(4)).estimatedBytes());
		assertEquals(12L, new RocksDBAPICommandSingle.Merge<>(
				0, 1, keys(5), buffer(7), RequestType.none()).estimatedBytes());
		assertEquals(15L, new RocksDBAPICommandSingle.MergeMulti<>(
				0, 1, List.of(keys(2), keys(3)), List.of(buffer(4), buffer(6)), RequestType.none())
				.estimatedBytes());
		assertEquals(7L, new RocksDBAPICommandSingle.OpenIterator(
				0, 1, keys(3), keys(4), false, Duration.ofSeconds(1)).estimatedBytes());
		assertEquals(3L, new RocksDBAPICommandSingle.SeekTo(1, keys(3)).estimatedBytes());
		assertEquals(7L, new RocksDBAPICommandSingle.ReduceRange<>(
				0, 1, keys(3), keys(4), false, RequestType.firstAndLast()).estimatedBytes());
		assertEquals(71L, new RocksDBAPICommandSingle.GetRangePage<>(
				0, 1, keys(3), keys(4), false, null,
				RequestType.allInRange(), new RangeBudget(8, 64)).estimatedBytes());
		assertEquals(7L, new RocksDBAPICommandStream.GetRange<>(
				0, 1, keys(3), keys(4), false, RequestType.allInRange()).estimatedBytes());
		assertEquals((long) RawSstToken.MAX_CHARACTERS * Character.BYTES,
				new RocksDBAPICommandStream.ScanRawResumable(
						1, 0, 1, Set.of(new RawSstToken("000123.sst"))).estimatedBytes());

		var saturated = new RocksDBAPICommandSingle.GetRangePage<>(
				0, 1, keys(1), null, false, null,
				RequestType.allInRange(), new RangeBudget(1, Long.MAX_VALUE));
		assertEquals(WorkloadCost.MAX_ESTIMATED_BYTES, saturated.estimatedBytes());
	}

	@Test
	void schedulerByteEstimateStopsReadingOnceMaximumCostIsKnown() {
		var quantum = buffer((int) WorkloadCost.QUANTUM_BYTES);
		List<Buf> values = new AbstractList<>() {
			@Override
			public Buf get(int index) {
				if (index >= WorkloadCost.MAX_UNITS) {
					throw new AssertionError("estimator read beyond the scheduler cost ceiling");
				}
				return quantum;
			}

			@Override
			public int size() {
				return WorkloadCost.MAX_UNITS + 1;
			}
		};
		var command = new RocksDBAPICommandSingle.PutMulti<>(
				0, 1, List.of(), values, RequestType.none());

		assertEquals(WorkloadCost.MAX_ESTIMATED_BYTES, command.estimatedBytes());
	}

	@Test
	void everyLatencyPointMutationChecksEncodedBytesBelowAtAndAboveLimit() {
		List<IntFunction<RocksDBAPICommand<?, ?, ?>>> commands = List.of(
				bytes -> new RocksDBAPICommandSingle.Put<>(0, 1, EMPTY_KEYS, buffer(bytes), RequestType.none()),
				bytes -> new RocksDBAPICommandSingle.Delete<>(0, 1, keys(bytes), RequestType.none()),
				bytes -> new RocksDBAPICommandSingle.Merge<>(0, 1, EMPTY_KEYS, buffer(bytes), RequestType.none()));

		for (var command : commands) {
			assertLatencyAllowed(command.apply((int) WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES - 1));
			assertLatencyAllowed(command.apply((int) WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES));
			assertLatencyRejected(command.apply((int) WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES + 1));
		}
	}

	@Test
	void getForUpdateUsesMutationResourcesAndRejectsAnalyticalCallers() {
		var forUpdate = new RocksDBAPICommandSingle.Get<>(
				0, 1, EMPTY_KEYS, RequestType.forUpdate());

		assertEquals(OperationFamily.MUTATION, forUpdate.operationFamily());
		assertEquals(LATENCY, WorkloadAdmission.resolve(context(LATENCY), forUpdate));
		assertEquals(INGEST, WorkloadAdmission.resolve(context(INGEST), forUpdate));
		assertEquals(BATCH, WorkloadAdmission.resolve(context(BATCH), forUpdate));
		assertThrows(RocksDBException.class,
				() -> WorkloadAdmission.resolve(context(ANALYTICAL), forUpdate));
	}

	@Test
	void everyLatencyPointReadChecksEncodedBytesBelowAtAndAboveLimit() {
		List<java.util.function.Function<Keys, RocksDBAPICommand<?, ?, ?>>> commands = List.of(
				key -> new RocksDBAPICommandSingle.Get<>(0, 1, key, RequestType.current()),
				key -> new RocksDBAPICommandSingle.Get<>(0, 1, key, RequestType.exists()),
				key -> new RocksDBAPICommandSingle.Get<>(0, 1, key, RequestType.forUpdate()));
		for (var command : commands) {
			assertLatencyAllowed(command.apply(
					keys((int) WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES - 1)));
			assertLatencyAllowed(command.apply(
					keys((int) WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES)));
			assertLatencyRejected(command.apply(
					keys((int) WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES + 1)));
		}
	}

	@Test
	void everyLatencyFixedMultiAndExistsChecksItemAndEncodedByteLimits() {
		List<BiFunction<Integer, Integer, RocksDBAPICommand<?, ?, ?>>> commands = List.of(
				(count, bytes) -> new RocksDBAPICommandSingle.DeleteMulti<>(
						0, 1, keysList(count, bytes), RequestType.none()),
				(count, bytes) -> new RocksDBAPICommandSingle.PutMulti<>(
						0, 1, emptyKeysList(count), valuesList(count, bytes), RequestType.none()),
				(count, bytes) -> new RocksDBAPICommandSingle.MergeMulti<>(
						0, 1, emptyKeysList(count), valuesList(count, bytes), RequestType.none()),
				(count, bytes) -> new RocksDBAPICommandSingle.ExistsMulti(
						0, 1, keysList(count, bytes)));

		for (var command : commands) {
			assertLatencyAllowed(command.apply(WorkloadAdmission.MAX_LATENCY_ITEMS - 1, 0));
			assertLatencyAllowed(command.apply(WorkloadAdmission.MAX_LATENCY_ITEMS, 0));
			assertLatencyRejected(command.apply(WorkloadAdmission.MAX_LATENCY_ITEMS + 1, 0));
			assertLatencyAllowed(command.apply(1, (int) WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES - 1));
			assertLatencyAllowed(command.apply(1, (int) WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES));
			assertLatencyRejected(command.apply(1, (int) WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES + 1));
		}
	}

	@Test
	void configuredLatencyLimitsApplyToEveryPointMutationFixedMultiAndExistsCommand() {
		int configuredItems = 3;
		long configuredBytes = 8;
		List<BiFunction<Integer, Integer, RocksDBAPICommand<?, ?, ?>>> commands = List.of(
				(count, bytes) -> new RocksDBAPICommandSingle.DeleteMulti<>(
						0, 1, keysList(count, bytes), RequestType.none()),
				(count, bytes) -> new RocksDBAPICommandSingle.PutMulti<>(
						0, 1, emptyKeysList(count), valuesList(count, bytes), RequestType.none()),
				(count, bytes) -> new RocksDBAPICommandSingle.MergeMulti<>(
						0, 1, emptyKeysList(count), valuesList(count, bytes), RequestType.none()),
				(count, bytes) -> new RocksDBAPICommandSingle.ExistsMulti(
						0, 1, keysList(count, bytes)));

		for (var command : commands) {
			assertConfiguredLatencyAllowed(command.apply(configuredItems - 1, 0), configuredItems, configuredBytes);
			assertConfiguredLatencyAllowed(command.apply(configuredItems, 0), configuredItems, configuredBytes);
			assertConfiguredLatencyRejected(command.apply(configuredItems + 1, 0), configuredItems, configuredBytes);
			assertConfiguredLatencyAllowed(command.apply(1, (int) configuredBytes - 1), configuredItems, configuredBytes);
			assertConfiguredLatencyAllowed(command.apply(1, (int) configuredBytes), configuredItems, configuredBytes);
			assertConfiguredLatencyRejected(command.apply(1, (int) configuredBytes + 1), configuredItems, configuredBytes);
		}

		List<IntFunction<RocksDBAPICommand<?, ?, ?>>> pointMutations = List.of(
				bytes -> new RocksDBAPICommandSingle.Put<>(0, 1, EMPTY_KEYS, buffer(bytes), RequestType.none()),
				bytes -> new RocksDBAPICommandSingle.Delete<>(0, 1, keys(bytes), RequestType.none()),
				bytes -> new RocksDBAPICommandSingle.Merge<>(0, 1, EMPTY_KEYS, buffer(bytes), RequestType.none()));
		for (var command : pointMutations) {
			assertConfiguredLatencyAllowed(command.apply((int) configuredBytes - 1), configuredItems, configuredBytes);
			assertConfiguredLatencyAllowed(command.apply((int) configuredBytes), configuredItems, configuredBytes);
			assertConfiguredLatencyRejected(command.apply((int) configuredBytes + 1), configuredItems, configuredBytes);
		}

		var aboveConfiguredExists = new RocksDBAPICommandSingle.ExistsMulti(
				0, 1, emptyKeysList(configuredItems + 1));
		for (var profile : List.of(ANALYTICAL, INGEST, BATCH)) {
			assertEquals(profile, WorkloadAdmission.resolve(
					context(profile), aboveConfiguredExists, configuredItems, configuredBytes));
		}
	}

	@Test
	void rangePagesUseConfiguredLatencyLimitsAndPublicHardCeilingsForOtherProfiles() {
		int configuredItems = 2;
		long configuredBytes = 64;
		var atConfiguredLimit = rangePage(new RangeBudget(configuredItems, configuredBytes));
		assertEquals(LATENCY, WorkloadAdmission.resolve(context(LATENCY),
				atConfiguredLimit,
				WorkloadAdmission.MAX_LATENCY_ITEMS,
				WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES,
				configuredItems,
				configuredBytes));

		for (var overConfigured : List.of(
				rangePage(new RangeBudget(configuredItems + 1, configuredBytes)),
				rangePage(new RangeBudget(configuredItems, configuredBytes + 1)))) {
			assertThrows(RocksDBException.class, () -> WorkloadAdmission.resolve(context(LATENCY),
					overConfigured,
					WorkloadAdmission.MAX_LATENCY_ITEMS,
					WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES,
					configuredItems,
					configuredBytes));
		}

		var publicLimit = rangePage(RangeBudget.DEFAULT);
		for (var profile : List.of(ANALYTICAL, INGEST, BATCH)) {
			assertEquals(profile, WorkloadAdmission.resolve(context(profile), publicLimit,
					WorkloadAdmission.MAX_LATENCY_ITEMS,
					WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES,
					configuredItems,
					configuredBytes));
		}
		for (var abovePublicLimit : List.of(
				rangePage(new RangeBudget(RangeBudget.DEFAULT_MAX_ITEMS + 1, RangeBudget.DEFAULT_MAX_BYTES)),
				rangePage(new RangeBudget(RangeBudget.DEFAULT_MAX_ITEMS, RangeBudget.DEFAULT_MAX_BYTES + 1)))) {
			for (var profile : List.of(ANALYTICAL, INGEST, BATCH)) {
				assertThrows(RocksDBException.class, () -> WorkloadAdmission.resolve(context(profile),
						abovePublicLimit,
						WorkloadAdmission.MAX_LATENCY_ITEMS,
						WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES,
						configuredItems,
						configuredBytes));
			}
		}
	}

	@Test
	void latencyIteratorAdvanceChecksCombinedLimitBelowAtAndAbove() {
		assertLatencyAllowed(subsequent(0, WorkloadAdmission.MAX_LATENCY_ITERATOR_ADVANCE - 1));
		assertLatencyAllowed(subsequent(1, WorkloadAdmission.MAX_LATENCY_ITERATOR_ADVANCE - 1));
		assertLatencyRejected(subsequent(1, WorkloadAdmission.MAX_LATENCY_ITERATOR_ADVANCE));
		assertLatencyRejected(subsequent(Long.MAX_VALUE, Long.MAX_VALUE));
	}

	@Test
	void nonLatencyProfilesDoNotInheritLatencySizeCeilings() {
		var oversizedMutation = new RocksDBAPICommandSingle.Put<>(
				0,
				1,
				EMPTY_KEYS,
				buffer((int) WorkloadAdmission.MAX_LATENCY_ENCODED_INPUT_BYTES + 1),
				RequestType.none());
		assertEquals(INGEST, WorkloadAdmission.resolve(RequestContext.ingest(), oversizedMutation));
		assertEquals(BATCH, WorkloadAdmission.resolve(RequestContext.batch(), oversizedMutation));

		var oversizedExists = new RocksDBAPICommandSingle.ExistsMulti(
				0,
				1,
				emptyKeysList(WorkloadAdmission.MAX_LATENCY_ITEMS + 1));
		assertEquals(ANALYTICAL, WorkloadAdmission.resolve(RequestContext.analytical(), oversizedExists));
		assertEquals(INGEST, WorkloadAdmission.resolve(RequestContext.ingest(), oversizedExists));
		assertEquals(BATCH, WorkloadAdmission.resolve(RequestContext.batch(), oversizedExists));
	}

	@Test
	void exactAggregatesRemainForbiddenInLatencyAndIngest() {
		var exactCount = new RocksDBAPICommandSingle.ReduceRange<>(
				0, 1, EMPTY_KEYS, null, false, RequestType.entriesCount());
		assertThrows(RocksDBException.class, () -> WorkloadAdmission.resolve(context(LATENCY), exactCount));
		assertThrows(RocksDBException.class, () -> WorkloadAdmission.resolve(context(INGEST), exactCount));
		assertEquals(ANALYTICAL, WorkloadAdmission.resolve(context(ANALYTICAL), exactCount));
		assertEquals(BATCH, WorkloadAdmission.resolve(context(BATCH), exactCount));
	}

	@Test
	void protectedProfilesCannotBeConstructedAsClientContexts() {
		for (var profile : EnumSet.of(CONTROL, CDC, PHYSICAL_MAINTENANCE)) {
			assertFalse(profile.isClientSelectable());
			assertThrows(IllegalArgumentException.class,
					() -> new RequestContext(profile, Long.MAX_VALUE));
		}
	}

	@Test
	void publicProfilesHaveExplicitRelativeTimeoutFactories() {
		assertTrue(LATENCY.isClientSelectable());
		assertTrue(ANALYTICAL.isClientSelectable());
		assertTrue(INGEST.isClientSelectable());
		assertTrue(BATCH.isClientSelectable());
		assertEquals(LATENCY, RequestContext.latency(java.time.Duration.ofSeconds(5)).profile());
		assertEquals(ANALYTICAL, RequestContext.analytical().profile());
		assertEquals(INGEST, RequestContext.ingest().profile());
		assertEquals(BATCH, RequestContext.batch().profile());
		assertEquals(Duration.ofSeconds(5).toNanos(),
				RequestContext.latency(Duration.ofSeconds(5)).timeoutNanos());
		assertEquals(Duration.ofSeconds(5).toNanos(),
				RequestContext.analytical(Duration.ofSeconds(5)).timeoutNanos());
		assertEquals(Duration.ofSeconds(5).toNanos(),
				RequestContext.ingest(Duration.ofSeconds(5)).timeoutNanos());
		assertEquals(Duration.ofSeconds(5).toNanos(),
				RequestContext.batch(Duration.ofSeconds(5)).timeoutNanos());
		assertSame(RequestContext.analytical(), RequestContext.analytical());
		assertSame(RequestContext.ingest(), RequestContext.ingest());
		assertSame(RequestContext.batch(), RequestContext.batch());
		assertEquals(Long.MAX_VALUE - 1L,
				RequestContext.batch(Duration.ofSeconds(Long.MAX_VALUE)).timeoutNanos());
		assertThrows(IllegalArgumentException.class,
				() -> new RequestContext(LATENCY, Long.MAX_VALUE));
		assertThrows(IllegalArgumentException.class,
				() -> RequestContext.latency(java.time.Duration.ZERO));
	}

	private static List<CommandExpectation> commandExpectations() {
		var all = profiles(LATENCY, ANALYTICAL, INGEST, BATCH);
		var mutation = profiles(LATENCY, INGEST, BATCH);
		var ingestOrBatch = profiles(INGEST, BATCH);
		var analyticalOrBatch = profiles(ANALYTICAL, BATCH);
		var seek = profiles(LATENCY, ANALYTICAL, BATCH);
		var batch = profiles(BATCH);
		var commands = new ArrayList<CommandExpectation>();
		commands.add(client("openTransaction", new RocksDBAPICommandSingle.OpenTransaction(Duration.ofSeconds(1)), all));
		commands.add(client("commitTransaction", new RocksDBAPICommandSingle.CloseTransaction(1, true), ingestOrBatch));
		commands.add(protectedCommand("rollbackTransaction", new RocksDBAPICommandSingle.CloseTransaction(1, false), CONTROL));
		commands.add(protectedCommand("closeFailedUpdate", new RocksDBAPICommandSingle.CloseFailedUpdate(1), CONTROL));
		commands.add(client("createColumn", new RocksDBAPICommandSingle.CreateColumn("column", TEST_SCHEMA), batch));
		commands.add(client("uploadMergeOperator", new RocksDBAPICommandSingle.UploadMergeOperator("merge", "Type", new byte[0]), batch));
		commands.add(client("checkMergeOperator", new RocksDBAPICommandSingle.CheckMergeOperator("merge", new byte[0]), all));
		commands.add(client("deleteColumn", new RocksDBAPICommandSingle.DeleteColumn(1), batch));
		commands.add(client("deleteColumnIfExists", new RocksDBAPICommandSingle.DeleteColumnIfExists("column"), batch));
		commands.add(client("getColumnId", new RocksDBAPICommandSingle.GetColumnId("column"), all));
		commands.add(client("estimateNumKeys", new RocksDBAPICommandSingle.EstimateNumKeys(1), all));
		commands.add(client("put", new RocksDBAPICommandSingle.Put<>(0, 1, EMPTY_KEYS, buffer(0), RequestType.none()), mutation));
		commands.add(client("delete", new RocksDBAPICommandSingle.Delete<>(0, 1, EMPTY_KEYS, RequestType.none()), mutation));
		commands.add(client("deleteMulti", new RocksDBAPICommandSingle.DeleteMulti<>(0, 1, List.of(EMPTY_KEYS), RequestType.none()), mutation));
		commands.add(client("deleteRange", new RocksDBAPICommandSingle.DeleteRange(1, EMPTY_KEYS, null), ingestOrBatch));
		commands.add(client("putMulti", new RocksDBAPICommandSingle.PutMulti<>(
				0, 1, List.of(EMPTY_KEYS), List.of(buffer(0)), RequestType.none()), mutation));
		commands.add(client("putBatch", new RocksDBAPICommandSingle.PutBatch(
				1, Flux.empty(), PutBatchMode.WRITE_BATCH), ingestOrBatch));
		commands.add(client("merge", new RocksDBAPICommandSingle.Merge<>(
				0, 1, EMPTY_KEYS, buffer(0), RequestType.none()), mutation));
		commands.add(client("mergeMulti", new RocksDBAPICommandSingle.MergeMulti<>(
				0, 1, List.of(EMPTY_KEYS), List.of(buffer(0)), RequestType.none()), mutation));
		commands.add(client("mergeBatch", new RocksDBAPICommandSingle.MergeBatch(
				1, Flux.empty(), MergeBatchMode.MERGE_WRITE_BATCH), ingestOrBatch));
		commands.add(client("get", new RocksDBAPICommandSingle.Get<>(0, 1, EMPTY_KEYS, RequestType.current()), all));
		commands.add(client("existsMulti", new RocksDBAPICommandSingle.ExistsMulti(
				0, 1, List.of(EMPTY_KEYS)), all));
		commands.add(client("openIterator", new RocksDBAPICommandSingle.OpenIterator(
				0, 1, EMPTY_KEYS, null, false, Duration.ofSeconds(1)), seek));
		commands.add(protectedCommand("closeIterator", new RocksDBAPICommandSingle.CloseIterator(1), CONTROL));
		commands.add(client("seekTo", new RocksDBAPICommandSingle.SeekTo(1, EMPTY_KEYS), seek));
		commands.add(client("subsequent", subsequent(0, 1), all));
		commands.add(client("reduceBoundary", new RocksDBAPICommandSingle.ReduceRange<>(
				0, 1, EMPTY_KEYS, null, false, RequestType.firstAndLast()), seek));
		commands.add(client("reduceExact", new RocksDBAPICommandSingle.ReduceRange<>(
				0, 1, EMPTY_KEYS, null, false, RequestType.entriesCount()), analyticalOrBatch));
		commands.add(client("getRangePage", new RocksDBAPICommandSingle.GetRangePage<>(
				0, 1, EMPTY_KEYS, null, false, null, RequestType.allInRange(), RangeBudget.DEFAULT), all));
		commands.add(client("getRange", new RocksDBAPICommandStream.GetRange<>(
				0, 1, EMPTY_KEYS, null, false, RequestType.allInRange()), analyticalOrBatch));
		commands.add(client("scanRaw", new RocksDBAPICommandStream.ScanRaw(1, 0, 1), batch));
		commands.add(client("scanRawResumable",
				new RocksDBAPICommandStream.ScanRawResumable(1, 0, 1, Set.of()), batch));
		commands.add(protectedCommand("cdcPoll", new RocksDBAPICommandStream.CdcPoll("cdc", null, 1), CDC));
		commands.add(protectedCommand("flush", new RocksDBAPICommand.Flush(), PHYSICAL_MAINTENANCE));
		commands.add(protectedCommand("compact", new RocksDBAPICommand.Compact(), PHYSICAL_MAINTENANCE));
		commands.add(client("getAllColumnDefinitions", new RocksDBAPICommand.GetAllColumnDefinitions(), all));
		commands.add(protectedCommand("cdcCreate", new RocksDBAPICommand.CdcCreate(
				"cdc", null, null, null, OptionalLong.empty()), CDC));
		commands.add(protectedCommand("cdcDelete", new RocksDBAPICommand.CdcDelete("cdc"), CDC));
		commands.add(protectedCommand("cdcEarliest", new RocksDBAPICommand.CdcGetEarliestAvailableSequence(), CDC));
		commands.add(protectedCommand("cdcLastCommitted", new RocksDBAPICommand.CdcGetLastCommittedSequence("cdc"), CDC));
		commands.add(protectedCommand("cdcCommit", new RocksDBAPICommand.CdcCommit("cdc", 1), CDC));
		return List.copyOf(commands);
	}

	private static Set<Class<?>> permittedConcreteCommandTypes() {
		var result = new HashSet<Class<?>>();
		collectConcreteCommandTypes(RocksDBAPICommand.class, result);
		return Set.copyOf(result);
	}

	private static void collectConcreteCommandTypes(Class<?> type, Set<Class<?>> result) {
		for (var permitted : type.getPermittedSubclasses()) {
			if (permitted.isInterface()) {
				collectConcreteCommandTypes(permitted, result);
			} else {
				result.add(permitted);
			}
		}
	}

	private static CommandExpectation client(String name,
			RocksDBAPICommand<?, ?, ?> command,
			Set<WorkloadProfile> allowedProfiles) {
		return new CommandExpectation(name, command, allowedProfiles, null);
	}

	private static CommandExpectation protectedCommand(String name,
			RocksDBAPICommand<?, ?, ?> command,
			WorkloadProfile protectedProfile) {
		return new CommandExpectation(name, command, Set.of(), protectedProfile);
	}

	private static Set<WorkloadProfile> profiles(WorkloadProfile... profiles) {
		return Set.of(profiles);
	}

	private static RequestContext context(WorkloadProfile profile) {
		return switch (profile) {
			case LATENCY -> RequestContext.latency(java.time.Duration.ofMinutes(1));
			case ANALYTICAL -> RequestContext.analytical();
			case INGEST -> RequestContext.ingest();
			case BATCH -> RequestContext.batch();
			default -> throw new IllegalArgumentException("Not a client profile: " + profile);
		};
	}

	private static RocksDBAPICommandSingle.GetRangePage<?> rangePage(RangeBudget budget) {
		return new RocksDBAPICommandSingle.GetRangePage<>(
				0, 1, EMPTY_KEYS, null, false, null, RequestType.allInRange(), budget);
	}

	private static void assertLatencyAllowed(RocksDBAPICommand<?, ?, ?> command) {
		assertEquals(LATENCY, assertDoesNotThrow(() -> WorkloadAdmission.resolve(context(LATENCY), command)));
	}

	private static void assertLatencyRejected(RocksDBAPICommand<?, ?, ?> command) {
		assertThrows(RocksDBException.class, () -> WorkloadAdmission.resolve(context(LATENCY), command));
	}

	private static void assertConfiguredLatencyAllowed(RocksDBAPICommand<?, ?, ?> command,
			int maximumItems,
			long maximumBytes) {
		assertEquals(LATENCY, assertDoesNotThrow(() -> WorkloadAdmission.resolve(
				context(LATENCY), command, maximumItems, maximumBytes)));
	}

	private static void assertConfiguredLatencyRejected(RocksDBAPICommand<?, ?, ?> command,
			int maximumItems,
			long maximumBytes) {
		assertThrows(RocksDBException.class, () -> WorkloadAdmission.resolve(
				context(LATENCY), command, maximumItems, maximumBytes));
	}

	private static RocksDBAPICommandSingle.Subsequent<Void> subsequent(long skip, long take) {
		return new RocksDBAPICommandSingle.Subsequent<>(1, skip, take, RequestType.none());
	}

	private static Buf buffer(int size) {
		return Buf.createZeroes(size);
	}

	private static Keys keys(int size) {
		return new Keys(buffer(size));
	}

	private static List<Keys> emptyKeysList(int count) {
		return java.util.Collections.nCopies(count, EMPTY_KEYS);
	}

	private static List<Keys> keysList(int count, int encodedBytes) {
		if (count == 0) {
			return List.of();
		}
		var keys = new ArrayList<Keys>(count);
		keys.add(keys(encodedBytes));
		for (int i = 1; i < count; i++) {
			keys.add(EMPTY_KEYS);
		}
		return keys;
	}

	private static List<Buf> valuesList(int count, int encodedBytes) {
		if (count == 0) {
			return List.of();
		}
		var values = new ArrayList<Buf>(count);
		values.add(buffer(encodedBytes));
		for (int i = 1; i < count; i++) {
			values.add(buffer(0));
		}
		return values;
	}

	private record CommandExpectation(String name,
			RocksDBAPICommand<?, ?, ?> command,
			Set<WorkloadProfile> allowedProfiles,
			WorkloadProfile protectedProfile) {
	}
}
