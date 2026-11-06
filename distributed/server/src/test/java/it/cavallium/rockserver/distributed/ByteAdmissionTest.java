package it.cavallium.rockserver.distributed;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;

/** The byte admission of {@link Proposer}, and how it is sized. */
class ByteAdmissionTest {

	@Test
	void refusesWhatDoesNotFitAndTakesNothing() {
		var admission = new ByteAdmission(100);
		assertTrue(admission.tryAcquire(60));
		assertFalse(admission.tryAcquire(41));
		assertEquals(60, admission.used());
		assertTrue(admission.tryAcquire(40));
		assertFalse(admission.tryAcquire(1));
		admission.release(60);
		assertTrue(admission.tryAcquire(60));
		admission.release(60);
		admission.release(40);
		assertEquals(0, admission.used());
	}

	@Test
	void neverExceedsTheLimitUnderContention() throws InterruptedException {
		var admission = new ByteAdmission(1000);
		var maxSeen = new AtomicLong();
		var done = new CountDownLatch(8);
		for (int t = 0; t < 8; t++) {
			Thread.ofPlatform().start(() -> {
				for (int i = 0; i < 20_000; i++) {
					if (admission.tryAcquire(300)) {
						maxSeen.accumulateAndGet(admission.used(), Math::max);
						admission.release(300);
					}
				}
				done.countDown();
			});
		}
		done.await();
		assertTrue(maxSeen.get() <= 1000, () -> "used " + maxSeen.get());
		assertEquals(0, admission.used());
	}

	@Test
	void theDefaultIsAnEighthOfTheHeapAndAlwaysFitsTheLargestEntryTwice() {
		long gib = 1L << 30;
		assertEquals(gib / 2, Proposer.writeAdmissionBytes(null, 4 * gib));
		assertEquals(gib / 2, Proposer.writeAdmissionBytes(" ", 4 * gib));
		long floor = 2 * (Proposer.MAX_ENTRY_BODY_BYTES + Proposer.REQUEST_OVERHEAD_BYTES);
		assertEquals(floor, Proposer.writeAdmissionBytes(null, 64L << 20));
		assertEquals(3 * gib, Proposer.writeAdmissionBytes(String.valueOf(3 * gib), 4 * gib));
		assertEquals(floor, Proposer.writeAdmissionBytes("1", 4 * gib));
	}

	@Test
	void anAttemptOfTheLargestEntryAlwaysGetsAPermit() {
		assertTrue(Proposer.ATTEMPT_BYTES_IN_FLIGHT >= RaftSettings.MAX_ENTRY_BYTES);
		assertTrue(new ByteAdmission(Proposer.ATTEMPT_BYTES_IN_FLIGHT).tryAcquire(RaftSettings.MAX_ENTRY_BYTES));
	}
}
