package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;

import java.lang.reflect.Constructor;
import java.lang.reflect.Method;
import java.util.function.LongSupplier;
import org.junit.jupiter.api.Test;

class SchedulerDeadlineClockTest {

	@Test
	void bindAndNowSamplesEachClockExactlyOnce() {
		var epoch = new CountingSource(1_000L);
		var nano = new CountingSource(10_000L);
		var clock = clock(epoch, nano);
		var sample = sample();
		epoch.resetCalls();
		nano.resetCalls();

		bind(clock, 1_250L, sample);

		assertEquals(0L, nowNanos(sample));
		assertEquals(MILLISECONDS.toNanos(250L), deadlineNanos(sample));
		assertEquals(1L, epoch.calls(), "binding must retain one wall sample for jump detection");
		assertEquals(1L, nano.calls(), "binding and admission must share one monotonic sample");
	}

	@Test
	void bindAndNowPublishesTheSameSampleForExpiredAndJumpedBindings() {
		var epoch = new CountingSource(1_000L);
		var nano = new CountingSource(10_000L);
		var clock = clock(epoch, nano);
		var sample = sample();

		nano.add(MILLISECONDS.toNanos(40L));
		epoch.add(40L);
		epoch.resetCalls();
		nano.resetCalls();
		bind(clock, 1_039L, sample);
		assertEquals(MILLISECONDS.toNanos(40L), nowNanos(sample));
		assertEquals(nowNanos(sample), deadlineNanos(sample));
		assertEquals(1L, epoch.calls());
		assertEquals(1L, nano.calls());

		nano.add(MILLISECONDS.toNanos(10L));
		epoch.add(-10_000L);
		epoch.resetCalls();
		nano.resetCalls();
		bind(clock, 2_000L, sample);
		assertEquals(MILLISECONDS.toNanos(50L), nowNanos(sample));
		assertEquals(MILLISECONDS.toNanos(11_010L), deadlineNanos(sample),
				"a wall discontinuity must establish a new affine generation at the sampled now");
		assertEquals(1L, epoch.calls());
		assertEquals(1L, nano.calls());
	}

	private static Object clock(LongSupplier epoch, LongSupplier nano) {
		try {
			Class<?> type = Class.forName("it.cavallium.rockserver.core.impl.SchedulerDeadlineClock");
			Method testing = type.getDeclaredMethod("testing", LongSupplier.class, LongSupplier.class);
			testing.setAccessible(true);
			return testing.invoke(null, epoch, nano);
		} catch (ReflectiveOperationException failure) {
			throw new AssertionError(failure);
		}
	}

	private static Object sample() {
		try {
			Class<?> type = Class.forName(
					"it.cavallium.rockserver.core.impl.SchedulerDeadlineClock$DeadlineSample");
			Constructor<?> constructor = type.getDeclaredConstructor();
			constructor.setAccessible(true);
			return constructor.newInstance();
		} catch (ReflectiveOperationException failure) {
			throw new AssertionError(failure);
		}
	}

	private static void bind(Object clock, long deadlineEpochMillis, Object sample) {
		try {
			Method bind = clock.getClass().getDeclaredMethod("bindDeadlineEpochMillis",
					long.class,
					sample.getClass());
			bind.setAccessible(true);
			bind.invoke(clock, deadlineEpochMillis, sample);
		} catch (ReflectiveOperationException failure) {
			throw new AssertionError(failure);
		}
	}

	private static long nowNanos(Object sample) {
		return sampleLong(sample, "nowNanos");
	}

	private static long deadlineNanos(Object sample) {
		return sampleLong(sample, "deadlineNanos");
	}

	private static long sampleLong(Object sample, String methodName) {
		try {
			Method method = sample.getClass().getDeclaredMethod(methodName);
			method.setAccessible(true);
			return (long) method.invoke(sample);
		} catch (ReflectiveOperationException failure) {
			throw new AssertionError(failure);
		}
	}

	private static final class CountingSource implements LongSupplier {

		private long value;
		private long calls;

		private CountingSource(long value) {
			this.value = value;
		}

		@Override
		public long getAsLong() {
			calls++;
			return value;
		}

		private void add(long delta) {
			value += delta;
		}

		private long calls() {
			return calls;
		}

		private void resetCalls() {
			calls = 0L;
		}
	}
}
