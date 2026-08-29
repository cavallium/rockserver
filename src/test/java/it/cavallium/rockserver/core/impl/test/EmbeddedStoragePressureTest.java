package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.rockserver.core.impl.EmbeddedDB;
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

		assertFalse(EmbeddedDB.storagePressureForTesting(
				false, senderIndexPending, messagesV2Pending, overlayPending),
				"pending-compaction limits are per column family, not database-wide sums");
	}

	@Test
	void oneColumnAtOrAboveItsLimitActivatesPressure() {
		assertTrue(EmbeddedDB.storagePressureForTesting(false, PENDING_COMPACTION_THRESHOLD));
		assertTrue(EmbeddedDB.storagePressureForTesting(
				false, 0L, PENDING_COMPACTION_THRESHOLD + 1L, 0L));
	}

	@Test
	void writeStopActivatesPressureWithoutCompactionDebt() {
		assertTrue(EmbeddedDB.storagePressureForTesting(true, 0L, 0L));
	}

	@Test
	void unsignedNativePropertyOverflowFailsClosed() {
		assertTrue(EmbeddedDB.storagePressureForTesting(false, Long.MIN_VALUE));
	}
}
