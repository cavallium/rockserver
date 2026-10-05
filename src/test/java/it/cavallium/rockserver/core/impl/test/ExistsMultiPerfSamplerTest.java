package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.ExistsMultiPerfSampler;

import static org.junit.jupiter.api.Assertions.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.rocksdb.PerfLevel;

class ExistsMultiPerfSamplerTest {
	static final class Fake implements ExistsMultiPerfSampler.Access {
		PerfLevel level = PerfLevel.DISABLE;
		int reads;
		boolean fail;
		public PerfLevel level() { reads++; return level; }
		public void level(PerfLevel value) { reads++; level = value; }
		public ExistsMultiPerfSampler.Counters counters() {
			reads++;
			if (fail) throw new IllegalStateException("diagnostic JNI failure");
			return new ExistsMultiPerfSampler.Counters(reads, reads, reads, reads, reads, reads);
		}
	}

	@Test void disabledAndExpiredMakeNoDiagnosticCalls() {
		var clock = new AtomicLong(); var access = new Fake();
		var disabled = new ExistsMultiPerfSampler(0, 0, 0, clock::get, ignored -> fail(), () -> fail());
		assertNull(disabled.begin(access)); assertEquals(0, access.reads);
		var enabled = new ExistsMultiPerfSampler(10, 20, 3, clock::get, ignored -> {}, () -> fail());
		clock.set(20); assertNull(enabled.begin(access)); assertEquals(0, access.reads);
	}

	@Test void budgetIntervalAndNestedSamplingAreBounded() {
		var clock = new AtomicLong(); var access = new Fake(); var observed = new AtomicInteger();
		var sampler = new ExistsMultiPerfSampler(10, 100, 2, clock::get, ignored -> observed.incrementAndGet(), () -> fail());
		var first = sampler.begin(access); assertNotNull(first);
		clock.set(10); assertNull(sampler.begin(new Fake()));
		sampler.finish(first); assertEquals(PerfLevel.DISABLE, access.level);
		var second = sampler.begin(access); assertNotNull(second); sampler.finish(second);
		clock.set(30); assertNull(sampler.begin(access)); assertEquals(2, observed.get());
	}

	@Test void restoresLevelsBeforeObserverEvenWhenObserverThrows() {
		for (var prior : new PerfLevel[]{PerfLevel.DISABLE, PerfLevel.ENABLE_COUNT,
				PerfLevel.ENABLE_TIME_EXCEPT_FOR_MUTEX, PerfLevel.ENABLE_TIME_AND_CPU_TIME_EXCEPT_FOR_MUTEX, PerfLevel.ENABLE_TIME}) {
			var access = new Fake(); access.level = prior; var failures = new AtomicInteger();
			var sampler = new ExistsMultiPerfSampler(1, 10, 1, () -> 0, ignored -> {
				assertEquals(prior, access.level); throw new IllegalStateException("observer");
			}, failures::incrementAndGet);
			var sample = sampler.begin(access); assertNotNull(sample);
			assertEquals(prior.getValue() < 3 ? PerfLevel.ENABLE_TIME_EXCEPT_FOR_MUTEX : prior, access.level);
			sampler.finish(sample); assertEquals(prior, access.level); assertEquals(1, failures.get());
		}
	}

	@Test void diagnosticFailuresAndBusinessCancellationPreserveCleanup() {
		var clock = new AtomicLong(); var access = new Fake(); var failures = new AtomicInteger();
		var sampler = new ExistsMultiPerfSampler(1, 20, 4, clock::get, ignored -> {}, failures::incrementAndGet);
		access.fail = true; assertNull(sampler.begin(access)); assertEquals(PerfLevel.DISABLE, access.level);
		access.fail = false; clock.incrementAndGet(); var sample = sampler.begin(access); assertNotNull(sample);
		var original = new java.util.concurrent.CancellationException("business");
		assertSame(original, assertThrows(java.util.concurrent.CancellationException.class, () -> {
			try { throw original; } finally { access.fail = true; sampler.finish(sample); }
		}));
		assertEquals(PerfLevel.DISABLE, access.level); assertEquals(2, failures.get());
		access.fail = false; clock.incrementAndGet(); var after = sampler.begin(access); assertNotNull(after); sampler.finish(after);
	}

	@Test void uninitializedIsNotRaisedOrRestoredWithInvalidSetter() {
		var access = new Fake(); access.level = PerfLevel.UNINITIALIZED;
		var sampler = new ExistsMultiPerfSampler(1, 10, 1, () -> 0, ignored -> fail(), () -> fail());
		assertNull(sampler.begin(access)); assertEquals(1, access.reads);
	}
	@Test void scheduledWindowUsesOnlyMonotonicTimeAndDoesNotRestartExpiredWindow() {
		var clock = new AtomicLong(10); var access = new Fake();
		var future = new ExistsMultiPerfSampler(1, 20, 2, clock::get, 30, ignored -> {}, () -> fail());
		assertNull(future.begin(access)); assertEquals(0, access.reads);
		clock.set(30); var sample = future.begin(access); assertNotNull(sample); future.finish(sample);
		clock.set(50); assertNull(future.begin(access));
		var expired = new ExistsMultiPerfSampler(1, 20, 2, clock::get, 20, ignored -> fail(), () -> fail());
		assertNull(expired.begin(access));
	}

	@Test void throwingClockCannotEscapeIntoBusinessCall() {
		var throwing = new java.util.concurrent.atomic.AtomicBoolean();
		var failures = new AtomicInteger(); var access = new Fake();
		var sampler = new ExistsMultiPerfSampler(1, 100, 1, () -> {
			if (throwing.get()) throw new IllegalStateException("clock");
			return 0;
		}, ignored -> fail(), failures::incrementAndGet);
		throwing.set(true); assertNull(sampler.begin(access));
		assertEquals(0, access.reads); assertEquals(1, failures.get());
	}

	@Test void failureObserverRunsAfterLevelRestorationAndClearedNestedGuard() {
		var clock = new AtomicLong(); var access = new Fake(); var observed = new AtomicInteger();
		var probe = new ExistsMultiPerfSampler(1, 100, 1, clock::get, ignored -> {}, () -> fail());
		var sampler = new ExistsMultiPerfSampler(1, 100, 1, clock::get, ignored -> fail(), () -> {
			assertEquals(PerfLevel.DISABLE, access.level);
			var other = probe.begin(new Fake()); assertNotNull(other); probe.finish(other); observed.incrementAndGet();
		});
		var sample = sampler.begin(access); assertNotNull(sample);
		access.fail = true; sampler.finish(sample); assertEquals(1, observed.get());
	}

	@Test void counterResetDiscardsNegativeDeltas() {
		var access = new Fake(); var failures = new AtomicInteger();
		var sampler = new ExistsMultiPerfSampler(1, 100, 1, () -> 0, ignored -> fail(), failures::incrementAndGet);
		var sample = sampler.begin(access); assertNotNull(sample);
		access.reads = -100; sampler.finish(sample);
		assertEquals(PerfLevel.DISABLE, access.level); assertEquals(1, failures.get());
	}

	@Test void clockFailureAfterRaisingLevelRestoresBeforeFailureObserver() {
		var calls = new AtomicInteger(); var access = new Fake(); var failures = new AtomicInteger();
		var sampler = new ExistsMultiPerfSampler(1, 100, 1, () -> {
			if (calls.incrementAndGet() == 3) throw new IllegalStateException("clock after counters");
			return 0;
		}, ignored -> fail(), () -> { assertEquals(PerfLevel.DISABLE, access.level); failures.incrementAndGet(); });
		assertNull(sampler.begin(access)); assertEquals(PerfLevel.DISABLE, access.level); assertEquals(1, failures.get());
	}

	@Test void originalNativeExceptionSurvivesThrowingObserver() {
		var access = new Fake();
		var sampler = new ExistsMultiPerfSampler(1, 100, 1, () -> 0, ignored -> { throw new AssertionError("observer"); }, () -> { throw new AssertionError("failure observer"); });
		var sample = sampler.begin(access); assertNotNull(sample);
		var original = new org.rocksdb.RocksDBException("original native failure");
		assertSame(original, assertThrows(org.rocksdb.RocksDBException.class, () -> {
			try { throw original; } finally { sampler.finish(sample); }
		}));
		assertEquals(PerfLevel.DISABLE, access.level);
	}

}
