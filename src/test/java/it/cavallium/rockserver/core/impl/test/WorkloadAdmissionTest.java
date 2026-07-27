package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.WorkloadAdmission;
import java.time.Duration;
import java.time.Instant;
import java.util.EnumSet;
import java.util.Map;
import org.junit.jupiter.api.Test;

class WorkloadAdmissionTest {

	private static final Map<WorkloadProfile, EnumSet<OperationFamily>> EXPECTED = Map.of(
			WorkloadProfile.CONTROL, EnumSet.of(OperationFamily.CONTROL),
			WorkloadProfile.LATENCY, EnumSet.of(OperationFamily.METADATA,
					OperationFamily.POINT_LOOKUP,
					OperationFamily.BOUNDARY_SEEK,
					OperationFamily.BOUNDED_FAN_OUT,
					OperationFamily.RANGE_PAGE,
					OperationFamily.MUTATION),
			WorkloadProfile.ANALYTICAL, EnumSet.of(OperationFamily.METADATA,
					OperationFamily.POINT_LOOKUP,
					OperationFamily.BOUNDARY_SEEK,
					OperationFamily.BOUNDED_FAN_OUT,
					OperationFamily.RANGE_PAGE,
					OperationFamily.FULL_SCAN_AGGREGATE),
			WorkloadProfile.INGEST, EnumSet.of(OperationFamily.METADATA,
					OperationFamily.POINT_LOOKUP,
					OperationFamily.BOUNDED_FAN_OUT,
					OperationFamily.RANGE_PAGE,
					OperationFamily.MUTATION),
			WorkloadProfile.CDC, EnumSet.of(OperationFamily.WAL_PAGE,
					OperationFamily.MUTATION,
					OperationFamily.FLUSH),
			WorkloadProfile.BATCH, EnumSet.of(OperationFamily.METADATA,
					OperationFamily.POINT_LOOKUP,
					OperationFamily.BOUNDARY_SEEK,
					OperationFamily.BOUNDED_FAN_OUT,
					OperationFamily.RANGE_PAGE,
					OperationFamily.FULL_SCAN_AGGREGATE,
					OperationFamily.MUTATION),
			WorkloadProfile.PHYSICAL_MAINTENANCE,
			EnumSet.of(OperationFamily.FLUSH, OperationFamily.COMPACTION));

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
	void latencyCannotDisguiseAFullExactCount() {
		var context = RequestContext.latency(Duration.ofSeconds(5));
		assertThrows(RocksDBException.class,
				() -> WorkloadAdmission.validateClient(context, OperationFamily.FULL_SCAN_AGGREGATE));
	}

	@Test
	void protectedProfilesCannotBeConstructedAsClientContexts() {
		for (var profile : EnumSet.of(WorkloadProfile.CONTROL,
				WorkloadProfile.CDC,
				WorkloadProfile.PHYSICAL_MAINTENANCE)) {
			assertFalse(profile.isClientSelectable());
			assertThrows(IllegalArgumentException.class,
					() -> new RequestContext(profile, RequestContext.NO_DEADLINE));
		}
	}

	@Test
	void publicProfilesHaveExplicitFactoriesAndLatencyRequiresADeadline() {
		assertTrue(WorkloadProfile.LATENCY.isClientSelectable());
		assertTrue(WorkloadProfile.ANALYTICAL.isClientSelectable());
		assertTrue(WorkloadProfile.INGEST.isClientSelectable());
		assertTrue(WorkloadProfile.BATCH.isClientSelectable());
		assertEquals(WorkloadProfile.LATENCY,
				RequestContext.latency(Instant.now().plusSeconds(5)).profile());
		assertEquals(WorkloadProfile.ANALYTICAL, RequestContext.analytical().profile());
		assertEquals(WorkloadProfile.INGEST, RequestContext.ingest().profile());
		assertEquals(WorkloadProfile.BATCH, RequestContext.batch().profile());
		assertThrows(IllegalArgumentException.class,
				() -> new RequestContext(WorkloadProfile.LATENCY, RequestContext.NO_DEADLINE));
		assertThrows(IllegalArgumentException.class,
				() -> RequestContext.latency(Duration.ZERO));
	}
}
