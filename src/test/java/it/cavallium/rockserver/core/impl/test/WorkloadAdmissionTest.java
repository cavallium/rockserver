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
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.MergeBatchMode;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.PutBatchMode;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RequestType;
import it.cavallium.rockserver.core.common.RocksDBAPICommand;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandSingle;
import it.cavallium.rockserver.core.common.RocksDBAPICommand.RocksDBAPICommandStream;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.WorkloadAdmission;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.time.Duration;
import java.time.Instant;
import java.util.ArrayList;
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
		assertEquals(permittedConcreteCommandTypes(),
				commands.stream().map(expectation -> expectation.command().getClass()).collect(java.util.stream.Collectors.toSet()),
				"Every sealed command subtype must have an admission expectation");

		for (var expectation : commands) {
			for (var profile : CLIENT_PROFILES) {
				var context = context(profile);
				if (expectation.protectedProfile() != null) {
					assertEquals(expectation.protectedProfile(),
							assertDoesNotThrow(() -> WorkloadAdmission.resolve(context, expectation.command())),
							expectation.name() + " from " + profile);
				} else if (expectation.allowedProfiles().contains(profile)) {
					assertEquals(profile,
							assertDoesNotThrow(() -> WorkloadAdmission.resolve(context, expectation.command())),
							expectation.name() + " from " + profile);
				} else {
					var error = assertThrows(RocksDBException.class,
							() -> WorkloadAdmission.resolve(context, expectation.command()),
							expectation.name() + " from " + profile);
					assertTrue(error.getMessage().contains(profile.name()), expectation.name());
				}
			}
		}
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
	void everyLatencyFixedMultiAndExistsChecksItemAndEncodedByteLimits() {
		List<BiFunction<Integer, Integer, RocksDBAPICommand<?, ?, ?>>> commands = List.of(
				(count, bytes) -> new RocksDBAPICommandSingle.DeleteMulti<>(
						0, 1, keysList(count, bytes), RequestType.none()),
				(count, bytes) -> new RocksDBAPICommandSingle.PutMulti<>(
						0, 1, emptyKeysList(count), valuesList(count, bytes), RequestType.none()),
				(count, bytes) -> new RocksDBAPICommandSingle.MergeMulti<>(
						0, 1, emptyKeysList(count), valuesList(count, bytes), RequestType.none()),
				(count, bytes) -> new RocksDBAPICommandSingle.ExistsMulti(
						0, 1, keysList(count, bytes), 1_000));

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
				emptyKeysList(WorkloadAdmission.MAX_LATENCY_ITEMS + 1),
				1_000);
		assertEquals(ANALYTICAL, WorkloadAdmission.resolve(RequestContext.analytical(), oversizedExists));
		assertEquals(INGEST, WorkloadAdmission.resolve(RequestContext.ingest(), oversizedExists));
		assertEquals(BATCH, WorkloadAdmission.resolve(RequestContext.batch(), oversizedExists));
	}

	@Test
	void exactAggregatesRemainForbiddenInLatencyAndIngest() {
		var exactCount = new RocksDBAPICommandSingle.ReduceRange<>(
				0, 1, EMPTY_KEYS, null, false, RequestType.entriesCount(), 1_000);
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
					() -> new RequestContext(profile, RequestContext.NO_DEADLINE));
		}
	}

	@Test
	void publicProfilesHaveExplicitFactoriesAndLatencyRequiresADeadline() {
		assertTrue(LATENCY.isClientSelectable());
		assertTrue(ANALYTICAL.isClientSelectable());
		assertTrue(INGEST.isClientSelectable());
		assertTrue(BATCH.isClientSelectable());
		assertEquals(LATENCY, RequestContext.latency(Instant.now().plusSeconds(5)).profile());
		assertEquals(ANALYTICAL, RequestContext.analytical().profile());
		assertEquals(INGEST, RequestContext.ingest().profile());
		assertEquals(BATCH, RequestContext.batch().profile());
		assertThrows(IllegalArgumentException.class,
				() -> new RequestContext(LATENCY, RequestContext.NO_DEADLINE));
		assertThrows(IllegalArgumentException.class,
				() -> RequestContext.latency(Duration.ZERO));
	}

	private static List<CommandExpectation> commandExpectations() {
		var all = profiles(LATENCY, ANALYTICAL, INGEST, BATCH);
		var mutation = profiles(LATENCY, INGEST, BATCH);
		var ingestOrBatch = profiles(INGEST, BATCH);
		var analyticalOrBatch = profiles(ANALYTICAL, BATCH);
		var seek = profiles(LATENCY, ANALYTICAL, BATCH);
		var batch = profiles(BATCH);
		var commands = new ArrayList<CommandExpectation>();
		commands.add(client("openTransaction", new RocksDBAPICommandSingle.OpenTransaction(1_000), all));
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
				0, 1, List.of(EMPTY_KEYS), 1_000), all));
		commands.add(client("openIterator", new RocksDBAPICommandSingle.OpenIterator(
				0, 1, EMPTY_KEYS, null, false, 1_000), seek));
		commands.add(protectedCommand("closeIterator", new RocksDBAPICommandSingle.CloseIterator(1), CONTROL));
		commands.add(client("seekTo", new RocksDBAPICommandSingle.SeekTo(1, EMPTY_KEYS), seek));
		commands.add(client("subsequent", subsequent(0, 1), all));
		commands.add(client("reduceBoundary", new RocksDBAPICommandSingle.ReduceRange<>(
				0, 1, EMPTY_KEYS, null, false, RequestType.firstAndLast(), 1_000), seek));
		commands.add(client("reduceExact", new RocksDBAPICommandSingle.ReduceRange<>(
				0, 1, EMPTY_KEYS, null, false, RequestType.entriesCount(), 1_000), analyticalOrBatch));
		commands.add(client("getRange", new RocksDBAPICommandStream.GetRange<>(
				0, 1, EMPTY_KEYS, null, false, RequestType.allInRange(), 1_000), analyticalOrBatch));
		commands.add(client("scanRaw", new RocksDBAPICommandStream.ScanRaw(1, 0, 1), batch));
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
			case LATENCY -> RequestContext.latency(Duration.ofMinutes(1));
			case ANALYTICAL -> RequestContext.analytical();
			case INGEST -> RequestContext.ingest();
			case BATCH -> RequestContext.batch();
			default -> throw new IllegalArgumentException("Not a client profile: " + profile);
		};
	}

	private static void assertLatencyAllowed(RocksDBAPICommand<?, ?, ?> command) {
		assertEquals(LATENCY, assertDoesNotThrow(() -> WorkloadAdmission.resolve(context(LATENCY), command)));
	}

	private static void assertLatencyRejected(RocksDBAPICommand<?, ?, ?> command) {
		assertThrows(RocksDBException.class, () -> WorkloadAdmission.resolve(context(LATENCY), command));
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
