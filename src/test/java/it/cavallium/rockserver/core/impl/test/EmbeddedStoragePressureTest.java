package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.StoragePressureSignal;
import java.math.BigInteger;
import org.junit.jupiter.api.Test;
import org.rocksdb.util.SizeUnit;

class EmbeddedStoragePressureTest {

	private static final long PENDING_COMPACTION_THRESHOLD = 64L * SizeUnit.GB;

	@Test
	void productionStallDebtIsNotSummedAgainstOneColumnLimit() {
		long senderIndexPending = 55_348_529_560L;
		long messagesV2Pending = 27_874_362_756L;
		long overlayPending = 11_311_192_896L;
		assertTrue(BigInteger.valueOf(senderIndexPending)
				.add(BigInteger.valueOf(messagesV2Pending))
				.add(BigInteger.valueOf(overlayPending))
				.compareTo(BigInteger.valueOf(PENDING_COMPACTION_THRESHOLD)) > 0,
				"fixture must reproduce the old aggregate false positive");

		var signal = new StoragePressureSignal();
		signal.reset(0L, 0L);
		signal.observeColumn(1L, senderIndexPending, PENDING_COMPACTION_THRESHOLD);
		signal.observeColumn(2L, messagesV2Pending, PENDING_COMPACTION_THRESHOLD);
		signal.observeColumn(3L, overlayPending, PENDING_COMPACTION_THRESHOLD);

		assertFalse(signal.pressured(),
				"pending-compaction limits are per column family, not database-wide sums");
	}

	@Test
	void oneColumnAtOrAboveItsLimitActivatesPressure() {
		var signal = new StoragePressureSignal();
		signal.reset(0L, 0L);
		signal.observeColumn(7L, PENDING_COMPACTION_THRESHOLD, PENDING_COMPACTION_THRESHOLD);

		assertTrue(signal.pressured());
		assertTrue(signal.hasReason(StoragePressureSignal.REASON_PENDING_COMPACTION));
		assertEquals(7L, signal.triggeringColumnId());
		assertEquals(PENDING_COMPACTION_THRESHOLD, signal.triggeringPendingCompactionBytes());
		assertEquals(PENDING_COMPACTION_THRESHOLD, signal.triggeringPendingCompactionLimit());
	}

	@Test
	void writeStopActivatesPressureWithoutCompactionDebt() {
		var signal = new StoragePressureSignal();
		signal.reset(1L, 0L);

		assertTrue(signal.pressured());
		assertEquals(StoragePressureSignal.REASON_WRITE_STOPPED, signal.reasonMask());
	}

	@Test
	void unsignedNativePropertyOverflowFailsClosed() {
		var signal = new StoragePressureSignal();
		signal.reset(0L, 0L);
		signal.observeColumn(9L, Long.MIN_VALUE, PENDING_COMPACTION_THRESHOLD);

		assertTrue(signal.pressured());
		assertEquals(Long.MIN_VALUE, signal.maximumPendingCompactionBytes());
	}

	@Test
	void actualDelayedWriteRateIsAuthoritativeWithoutCompactionDebt() {
		var signal = new StoragePressureSignal();
		signal.reset(0L, 1L);

		assertTrue(signal.pressured());
		assertEquals(StoragePressureSignal.REASON_DELAYED_WRITE, signal.reasonMask());
		assertEquals(1L, signal.actualDelayedWriteRate());
	}

	@Test
	void configuredDisabledLimitsDoNotCreateProactivePressure() {
		var signal = new StoragePressureSignal();
		signal.reset(0L, 0L);
		signal.observeColumn(1L, Long.MIN_VALUE, 0L);
		signal.observeColumn(2L, Long.MIN_VALUE, Long.MAX_VALUE);

		assertFalse(signal.pressured());
	}

	@Test
	void explicitOverrideStillAppliesWhenConfiguredSlowdownIsDisabled() {
		var signal = new StoragePressureSignal(PENDING_COMPACTION_THRESHOLD);
		signal.reset(0L, 0L);
		signal.observeColumn(11L, PENDING_COMPACTION_THRESHOLD, Long.MAX_VALUE);

		assertTrue(signal.pressured());
		assertEquals(PENDING_COMPACTION_THRESHOLD, signal.pendingCompactionLimitOverride());
		assertEquals(PENDING_COMPACTION_THRESHOLD, signal.triggeringPendingCompactionLimit());
	}

	@Test
	void invalidEffectiveLimitFailsClosed() {
		var signal = new StoragePressureSignal();
		signal.reset(0L, 0L);
		signal.observeColumn(1L, 0L, Long.MIN_VALUE);

		assertTrue(signal.pressured());
		assertEquals(StoragePressureSignal.REASON_SIGNAL_FAILURE, signal.reasonMask());
	}

	@Test
	void resetClearsEveryPreviousReasonAndTrigger() {
		var signal = new StoragePressureSignal();
		signal.reset(1L, 5L);
		signal.observeColumn(23L, PENDING_COMPACTION_THRESHOLD, PENDING_COMPACTION_THRESHOLD);
		signal.markSignalFailure();
		assertTrue(signal.pressured());

		signal.reset(0L, 0L);

		assertFalse(signal.pressured());
		assertEquals(0, signal.reasonMask());
		assertEquals(-1L, signal.triggeringColumnId());
		assertEquals(0L, signal.triggeringPendingCompactionBytes());
		assertEquals(0L, signal.triggeringPendingCompactionLimit());
	}

	@Test
	void signalFailureComposesWithAuthoritativeReasons() {
		var signal = new StoragePressureSignal();
		signal.reset(1L, 2L);
		signal.markSignalFailure();

		assertEquals(StoragePressureSignal.REASON_WRITE_STOPPED
					| StoragePressureSignal.REASON_DELAYED_WRITE
					| StoragePressureSignal.REASON_SIGNAL_FAILURE,
				signal.reasonMask());
	}
}
