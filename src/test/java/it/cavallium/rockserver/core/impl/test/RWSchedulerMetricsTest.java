package it.cavallium.rockserver.core.impl.test;

import static java.util.concurrent.TimeUnit.SECONDS;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.micrometer.core.instrument.Counter;
import io.micrometer.core.instrument.Gauge;
import io.micrometer.core.instrument.Measurement;
import io.micrometer.core.instrument.Meter;
import io.micrometer.core.instrument.Timer;
import io.micrometer.core.instrument.distribution.DistributionStatisticConfig;
import io.micrometer.core.instrument.distribution.HistogramSnapshot;
import io.micrometer.core.instrument.distribution.pause.PauseDetector;
import io.micrometer.core.instrument.simple.SimpleMeterRegistry;
import it.cavallium.rockserver.core.common.OperationFamily;
import it.cavallium.rockserver.core.common.RequestContext;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import it.cavallium.rockserver.core.impl.RWScheduler;
import it.cavallium.rockserver.core.impl.WorkloadAdmission;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BooleanSupplier;
import java.util.function.Predicate;
import java.util.function.Supplier;
import java.util.function.ToDoubleFunction;
import java.util.stream.Collectors;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

class RWSchedulerMetricsTest {

	private static final String DATABASE = "metrics-db";
	private static final String RESOURCE = "resource";
	private static final String PROFILE = "profile";
	private static final String OPERATION = "operation";

	@Test
	void registersBoundedMetricHandlesAndCapacityGaugesAtConstruction() {
		var registry = new SimpleMeterRegistry();
		var scheduler = scheduler(registry, "metric-schema");
		try {
			assertMeter(registry,
					"rockserver.workload.queue.wait",
					Meter.Type.TIMER,
					taskTags());
			assertMeter(registry,
					"rockserver.workload.execution",
					Meter.Type.TIMER,
					taskTags());
			assertMeter(registry,
					"rockserver.workload.quantums",
					Meter.Type.COUNTER,
					taskTags());
			assertMeter(registry,
					"rockserver.workload.outcomes",
					Meter.Type.COUNTER,
					with(taskTags(), "outcome", "run"));
			assertMeter(registry,
					"rockserver.workload.rejections",
					Meter.Type.COUNTER,
					with(taskTags(), "reason", "queue_full"));
			assertMeter(registry,
					"rockserver.workload.cancellations",
					Meter.Type.COUNTER,
					taskTags());
			assertMeter(registry,
					"rockserver.workload.failures",
					Meter.Type.COUNTER,
					taskTags());
			assertMeter(registry,
					"rockserver.workload.worker.failures",
					Meter.Type.COUNTER,
					Map.of("database", DATABASE, RESOURCE, "read"));
			assertMeter(registry,
					"rockserver.workload.admission",
					Meter.Type.COUNTER,
					with(taskTags(), "result", "accepted"));

			assertGauge(registry,
					"rockserver.workload.queued",
					Map.of("database", DATABASE, RESOURCE, "read", PROFILE, "batch"),
					0.0);
			assertGauge(registry,
					"rockserver.workload.active",
					Map.of("database", DATABASE, RESOURCE, "read", PROFILE, "batch"),
					0.0);
			assertGauge(registry,
					"rockserver.workload.parked",
					Map.of("database", DATABASE, RESOURCE, "read", PROFILE, "batch"),
					0.0);
			assertGauge(registry,
					"rockserver.workload.outstanding",
					Map.of("database", DATABASE, RESOURCE, "read", PROFILE, "batch"),
					0.0);
			assertGauge(registry,
					"rockserver.workload.submission.attempts",
					Map.of("database", DATABASE, RESOURCE, "read", PROFILE, "batch"),
					0.0);
			assertGauge(registry,
					"rockserver.workload.worker.limit",
					Map.of("database", DATABASE, RESOURCE, "read"),
					1.0);
			assertGauge(registry,
					"rockserver.workload.queue.capacity",
					Map.of("database", DATABASE, RESOURCE, "read", PROFILE, "batch"),
					1.0);
			assertGauge(registry,
					"rockserver.workload.outstanding.limit",
					Map.of("database", DATABASE, RESOURCE, "read", PROFILE, "batch"),
					2.0);
			assertTrue(registry.find("rockserver.workload.outstanding")
					.tags("database", DATABASE, RESOURCE, "read", PROFILE, "control")
					.gauge() == null,
					"zero-capacity profile gauges must not consume permanent registry cardinality");
			assertGauge(registry,
					"rockserver.workload.storage.pressure",
					Map.of("database", DATABASE),
					0.0);
		} finally {
			scheduler.dispose();
			registry.close();
		}
	}

	@Test
	void gaugeCardinalityContainsOnlyProfilesThatCanRouteToEachPool() {
		var registry = new SimpleMeterRegistry();
		var scheduler = scheduler(registry, "metric-routing-cardinality");
		try {
			var profileGauges = registry.getMeters().stream()
					.filter(meter -> meter.getId().getType() == Meter.Type.GAUGE)
					.filter(meter -> meter.getId().getName().startsWith("rockserver.workload."))
					.filter(meter -> meter.getId().getTag(PROFILE) != null)
					.toList();
			assertEquals(11L * 7L, profileGauges.size(),
					"eleven routable profile/pool pairs expose seven bounded gauges each");
			assertTrue(profileGauges.stream().noneMatch(meter ->
					"write".equals(meter.getId().getTag(RESOURCE))
							&& "analytical".equals(meter.getId().getTag(PROFILE))));
		} finally {
			scheduler.dispose();
			registry.close();
		}
	}

	@Test
	void recordsExistingTimersCountersAndTerminalMetrics() throws Exception {
		var registry = new SimpleMeterRegistry();
		var scheduler = scheduler(registry, "metric-values");
		var release = new CountDownLatch(1);
		try {
			var completed = new CompletableFuture<Void>();
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.METADATA,
					RequestContext.NO_DEADLINE).execute(() -> completed.complete(null));
			completed.get(5, SECONDS);
			assertEventually(() -> timerCount(registry,
					"rockserver.workload.execution",
					"batch",
					"metadata") == 1L);

			assertEquals(1L, timerCount(registry, "rockserver.workload.queue.wait", "batch", "metadata"));
			assertEquals(1L, timerCount(registry, "rockserver.workload.execution", "batch", "metadata"));
			assertEquals(1.0, counter(registry, "rockserver.workload.quantums", "batch", "metadata"));
			assertEquals(1.0,
					counter(registry, "rockserver.workload.outcomes", "batch", "metadata", "outcome", "run"));
			assertEquals(1.0,
					counter(registry, "rockserver.workload.admission", "batch", "metadata", "result", "accepted"));

			var failureRan = new CountDownLatch(1);
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.POINT_LOOKUP,
					RequestContext.NO_DEADLINE).execute(() -> {
				failureRan.countDown();
				throw new IllegalStateException("expected test failure");
			});
			assertTrue(failureRan.await(5, SECONDS));
			assertEventually(() -> counter(registry,
							"rockserver.workload.failures",
							"batch",
							"point_lookup") == 1.0
					&& timerCount(registry,
							"rockserver.workload.execution",
							"batch",
							"point_lookup") == 1L);
			assertEquals(1L, timerCount(registry, "rockserver.workload.queue.wait", "batch", "point_lookup"));
			assertEquals(1L, timerCount(registry, "rockserver.workload.execution", "batch", "point_lookup"));
			assertEquals(1.0, counter(registry, "rockserver.workload.quantums", "batch", "point_lookup"));

			var blockerStarted = new CountDownLatch(1);
			var view = scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE);
			view.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));

			var cancelled = new MetricTask();
			view.execute(cancelled);
			assertTrue(cancelled.cancel(false));
			assertTrue(scheduler.removeQueuedTask(view, cancelled));

			var queued = new MetricTask();
			view.execute(queued);
			var overloaded = new MetricTask();
			var overload = assertThrows(RocksDBException.class, () -> view.execute(overloaded));
			assertEquals(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED, overload.getErrorUniqueId());

			var expired = new MetricTask();
			assertThrows(RocksDBException.class, () -> scheduler.executor(
					WorkloadProfile.LATENCY,
					OperationFamily.BOUNDARY_SEEK,
					System.currentTimeMillis()).execute(expired));

			release.countDown();
			queued.get(5, SECONDS);

			assertEquals(1.0,
					counter(registry, "rockserver.workload.cancellations", "batch", "range_page"));
			assertEquals(1.0,
					counter(registry,
							"rockserver.workload.outcomes",
							"batch",
							"range_page",
							"outcome",
							"cancellation"));
			assertEquals(1.0,
					counter(registry,
							"rockserver.workload.rejections",
							"batch",
							"range_page",
							"reason",
							"queue_full"));
			assertEquals(1.0,
					counter(registry,
							"rockserver.workload.outcomes",
							"batch",
							"range_page",
							"outcome",
							"overload"));
			assertEquals(1.0,
					counter(registry,
							"rockserver.workload.rejections",
							"latency",
							"boundary_seek",
							"reason",
							"deadline"));
			assertEquals(1.0,
					counter(registry,
							"rockserver.workload.outcomes",
							"latency",
							"boundary_seek",
							"outcome",
							"deadline"));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).drainedAndConserved());
			assertGauge(registry,
					"rockserver.workload.submission.attempts",
					Map.of("database", DATABASE, RESOURCE, "read", PROFILE, "batch"),
					6.0);
			assertGauge(registry,
					"rockserver.workload.outstanding",
					Map.of("database", DATABASE, RESOURCE, "read", PROFILE, "batch"),
					0.0);
		} finally {
			release.countDown();
			scheduler.dispose();
			registry.close();
		}
	}

	@Test
	void everyBoundedHandleIsResolvedBeforeHotPathsRun() throws Exception {
		var registry = new InstrumentedRegistry(id -> false);
		var scheduler = scheduler(registry, "metric-cache");
		var release = new CountDownLatch(1);
		try {
			assertTrue(registry.registrationAttempts() > 0);
			registry.clearAndSeal();

			for (var profile : WorkloadProfile.values()) {
				for (var family : OperationFamily.values()) {
					if (WorkloadAdmission.isAllowed(profile, family)) {
						var completed = new CompletableFuture<Void>();
						scheduler.executor(profile, family, RequestContext.NO_DEADLINE)
								.execute(() -> completed.complete(null));
						completed.get(5, SECONDS);
					}
				}
			}

			var failureRan = new CountDownLatch(1);
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.POINT_LOOKUP,
					RequestContext.NO_DEADLINE).execute(() -> {
				failureRan.countDown();
				throw new IllegalStateException("expected test failure");
			});
			assertTrue(failureRan.await(5, SECONDS));
			var afterFailure = new CompletableFuture<Void>();
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.POINT_LOOKUP,
					RequestContext.NO_DEADLINE).execute(() -> afterFailure.complete(null));
			afterFailure.get(5, SECONDS);

			exerciseCancellationAndOverload(scheduler, release);
			assertThrows(RocksDBException.class, () -> scheduler.executor(
					WorkloadProfile.LATENCY,
					OperationFamily.BOUNDARY_SEEK,
					System.currentTimeMillis()).execute(new MetricTask()));
			assertEventually(() -> scheduler.poolSnapshot(RWScheduler.Pool.READ).activeTasks() == 0);

			assertEquals(0,
					registry.postSealRegistrationAttempts(),
					"admission, execution, rejection, cancellation, and failure must use cached handles");
		} finally {
			release.countDown();
			scheduler.dispose();
			registry.close();
		}
	}

	@Test
	void gaugeScrapingNeverChecksQueuedCommandCancellation() throws Exception {
		var registry = new SimpleMeterRegistry();
		var scheduler = scheduler(registry, "gauge-scrape");
		var blockerStarted = new CountDownLatch(1);
		var release = new CountDownLatch(1);
		var probe = new CancellationProbeTask();
		try {
			var view = scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.RANGE_PAGE,
					RequestContext.NO_DEADLINE);
			view.execute(() -> {
				blockerStarted.countDown();
				awaitUninterruptibly(release);
			});
			assertTrue(blockerStarted.await(5, SECONDS));
			view.execute(probe);

			int scraped = 0;
			for (int pass = 0; pass < 5; pass++) {
				for (var meter : registry.getMeters()) {
					if (meter instanceof Gauge gauge) {
						assertFalse(Double.isInfinite(gauge.value()));
						scraped++;
					}
				}
			}
			assertTrue(scraped > 0);
			assertEquals(0,
					probe.cancellationChecks(),
					"gauge observation must not inspect or terminally process queued commands");
			assertFalse(probe.ran());
			assertFalse(probe.isDisposedWithoutChecking());

			release.countDown();
			assertTrue(probe.completed().await(5, SECONDS));
		} finally {
			release.countDown();
			scheduler.dispose();
			registry.close();
		}
	}

	@Test
	void individualRegistrationFailuresFallBackWithoutBreakingConstruction() throws Exception {
		var targets = List.of(
				new RegistrationFailureTarget("rockserver.workload.queue.wait", taskTags()),
				new RegistrationFailureTarget("rockserver.workload.outcomes", with(taskTags(), "outcome", "run")),
				new RegistrationFailureTarget("rockserver.workload.admission", with(taskTags(), "result", "accepted")),
				new RegistrationFailureTarget("rockserver.workload.queued",
						Map.of("database", DATABASE, RESOURCE, "read", PROFILE, "batch")),
				new RegistrationFailureTarget("rockserver.workload.outstanding",
						Map.of("database", DATABASE, RESOURCE, "read", PROFILE, "batch")),
				new RegistrationFailureTarget("rockserver.workload.worker.limit",
						Map.of("database", DATABASE, RESOURCE, "read")),
				new RegistrationFailureTarget("rockserver.workload.queue.capacity",
						Map.of("database", DATABASE, RESOURCE, "read", PROFILE, "batch")),
				new RegistrationFailureTarget("rockserver.workload.storage.pressure",
						Map.of("database", DATABASE)));

		for (var target : targets) {
			var registry = new InstrumentedRegistry(target::matches);
			RWScheduler scheduler = null;
			try {
				scheduler = scheduler(registry, "registration-failure-" + target.name());
				assertEquals(1,
						registry.registrationFailures(),
						() -> "target meter was not registered: " + target);
				var completed = new CompletableFuture<Void>();
				scheduler.executor(WorkloadProfile.BATCH,
						OperationFamily.RANGE_PAGE,
						RequestContext.NO_DEADLINE).execute(() -> completed.complete(null));
				completed.get(5, SECONDS);
			} finally {
				if (scheduler != null) {
					scheduler.dispose();
				}
				registry.close();
			}
		}
	}

	@Test
	void individualRecordingFailuresCannotBreakAdmissionOrWorkerExecution() throws Exception {
		var registry = new InstrumentedRegistry(id -> false);
		var scheduler = scheduler(registry, "recording-failure");
		var release = new CountDownLatch(1);
		var failures = Set.of(
				"rockserver.workload.admission",
				"rockserver.workload.queue.wait",
				"rockserver.workload.execution",
				"rockserver.workload.quantums",
				"rockserver.workload.outcomes",
				"rockserver.workload.failures",
				"rockserver.workload.rejections",
				"rockserver.workload.cancellations");
		try {
			registry.failNextRecordings(failures);

			var completed = new CompletableFuture<Void>();
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.METADATA,
					RequestContext.NO_DEADLINE).execute(() -> completed.complete(null));
			completed.get(5, SECONDS);

			var failureRan = new CountDownLatch(1);
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.POINT_LOOKUP,
					RequestContext.NO_DEADLINE).execute(() -> {
				failureRan.countDown();
				throw new IllegalStateException("expected test failure");
			});
			assertTrue(failureRan.await(5, SECONDS));
			exerciseCancellationAndOverload(scheduler, release);

			var afterFailures = new CompletableFuture<Void>();
			scheduler.executor(WorkloadProfile.BATCH,
					OperationFamily.METADATA,
					RequestContext.NO_DEADLINE).execute(() -> afterFailures.complete(null));
			afterFailures.get(5, SECONDS);
			assertEventually(() -> registry.recordingFailures().equals(failures));
		} finally {
			release.countDown();
			scheduler.dispose();
			registry.close();
		}
	}

	private static RWScheduler scheduler(SimpleMeterRegistry registry, String name) {
		return RWScheduler.forTesting(1, 1, 1, 1, 1, name, registry, DATABASE);
	}

	private static void exerciseCancellationAndOverload(RWScheduler scheduler,
			CountDownLatch release) throws Exception {
		var blockerStarted = new CountDownLatch(1);
		var view = scheduler.executor(WorkloadProfile.BATCH,
				OperationFamily.RANGE_PAGE,
				RequestContext.NO_DEADLINE);
		view.execute(() -> {
			blockerStarted.countDown();
			awaitUninterruptibly(release);
		});
		assertTrue(blockerStarted.await(5, SECONDS));

		var cancelled = new MetricTask();
		view.execute(cancelled);
		assertTrue(cancelled.cancel(false));
		assertTrue(scheduler.removeQueuedTask(view, cancelled));

		var queued = new MetricTask();
		view.execute(queued);
		assertThrows(RocksDBException.class, () -> view.execute(new MetricTask()));
		release.countDown();
		queued.get(5, SECONDS);
	}

	private static Map<String, String> taskTags() {
		return Map.of(
				"database", DATABASE,
				RESOURCE, "read",
				PROFILE, "batch",
				OPERATION, "range_page");
	}

	private static Map<String, String> with(Map<String, String> tags, String key, String value) {
		var result = new java.util.HashMap<>(tags);
		result.put(key, value);
		return Map.copyOf(result);
	}

	private static void assertMeter(SimpleMeterRegistry registry,
			String name,
			Meter.Type type,
			Map<String, String> tags) {
		var meter = requireMeter(registry, name, tags);
		assertEquals(type, meter.getId().getType());
		assertEquals(tags.keySet(), meter.getId().getTags().stream()
				.map(io.micrometer.core.instrument.Tag::getKey)
				.collect(Collectors.toUnmodifiableSet()));
	}

	private static void assertGauge(SimpleMeterRegistry registry,
			String name,
			Map<String, String> tags,
			double expected) {
		var meter = requireMeter(registry, name, tags);
		assertEquals(Meter.Type.GAUGE, meter.getId().getType());
		assertEquals(tags.keySet(), meter.getId().getTags().stream()
				.map(io.micrometer.core.instrument.Tag::getKey)
				.collect(Collectors.toUnmodifiableSet()));
		assertEquals(expected, ((Gauge) meter).value());
	}

	private static Meter requireMeter(SimpleMeterRegistry registry,
			String name,
			Map<String, String> tags) {
		var meter = registry.getMeters().stream()
				.filter(candidate -> candidate.getId().getName().equals(name))
				.filter(candidate -> hasTags(candidate.getId(), tags))
				.findFirst()
				.orElse(null);
		assertNotNull(meter, () -> "missing meter " + name + " " + tags);
		return meter;
	}

	private static boolean hasTags(Meter.Id id, Map<String, String> tags) {
		return tags.entrySet().stream().allMatch(tag -> tag.getValue().equals(id.getTag(tag.getKey())));
	}

	private static long timerCount(SimpleMeterRegistry registry,
			String name,
			String profile,
			String operation) {
		return registry.get(name)
				.tags("database", DATABASE, RESOURCE, "read", PROFILE, profile, OPERATION, operation)
				.timer()
				.count();
	}

	private static double counter(SimpleMeterRegistry registry,
			String name,
			String profile,
			String operation,
			String... extraTags) {
		var tags = new ArrayList<String>();
		tags.add("database");
		tags.add(DATABASE);
		tags.add(RESOURCE);
		tags.add("read");
		tags.add(PROFILE);
		tags.add(profile);
		tags.add(OPERATION);
		tags.add(operation);
		tags.addAll(List.of(extraTags));
		return registry.get(name).tags(tags.toArray(String[]::new)).counter().count();
	}

	private static void assertEventually(BooleanSupplier condition) throws InterruptedException {
		long deadline = System.nanoTime() + SECONDS.toNanos(5);
		while (!condition.getAsBoolean() && System.nanoTime() < deadline) {
			Thread.sleep(5L);
		}
		assertTrue(condition.getAsBoolean());
	}

	private static void awaitUninterruptibly(CountDownLatch latch) {
		boolean interrupted = false;
		while (true) {
			try {
				latch.await();
				break;
			} catch (InterruptedException ignored) {
				interrupted = true;
			}
		}
		if (interrupted) {
			Thread.currentThread().interrupt();
		}
	}

	private record RegistrationFailureTarget(String name, Map<String, String> tags) {

		private boolean matches(Meter.Id id) {
			return name.equals(id.getName()) && hasTags(id, tags);
		}
	}

	private static final class MetricTask extends CompletableFuture<Void> implements Runnable, Disposable {

		private final AtomicInteger disposals = new AtomicInteger();

		@Override
		public void run() {
			complete(null);
		}

		@Override
		public void dispose() {
			disposals.incrementAndGet();
		}

		@Override
		public boolean isDisposed() {
			return disposals.get() > 0;
		}
	}

	private static final class CancellationProbeTask implements Runnable, Disposable {

		private final AtomicInteger cancellationChecks = new AtomicInteger();
		private final AtomicBoolean disposed = new AtomicBoolean();
		private final AtomicBoolean ran = new AtomicBoolean();
		private final CountDownLatch completed = new CountDownLatch(1);

		@Override
		public void run() {
			ran.set(true);
			completed.countDown();
		}

		@Override
		public void dispose() {
			disposed.set(true);
		}

		@Override
		public boolean isDisposed() {
			cancellationChecks.incrementAndGet();
			return disposed.get();
		}

		private int cancellationChecks() {
			return cancellationChecks.get();
		}

		private boolean isDisposedWithoutChecking() {
			return disposed.get();
		}

		private boolean ran() {
			return ran.get();
		}

		private CountDownLatch completed() {
			return completed;
		}
	}

	private static final class InstrumentedRegistry extends SimpleMeterRegistry {

		private final Predicate<Meter.Id> registrationFailure;
		private final AtomicInteger registrationAttempts = new AtomicInteger();
		private final AtomicInteger registrationFailures = new AtomicInteger();
		private final AtomicInteger postSealRegistrationAttempts = new AtomicInteger();
		private final Set<String> failNextRecordings = ConcurrentHashMap.newKeySet();
		private final Set<String> recordingFailures = ConcurrentHashMap.newKeySet();
		private volatile boolean sealed;

		private InstrumentedRegistry(Predicate<Meter.Id> registrationFailure) {
			this.registrationFailure = registrationFailure;
		}

		private int registrationAttempts() {
			return registrationAttempts.get();
		}

		private int registrationFailures() {
			return registrationFailures.get();
		}

		private int postSealRegistrationAttempts() {
			return postSealRegistrationAttempts.get();
		}

		private Set<String> recordingFailures() {
			return Set.copyOf(recordingFailures);
		}

		private void clearAndSeal() {
			clear();
			sealed = true;
		}

		private void failNextRecordings(Set<String> names) {
			failNextRecordings.addAll(names);
		}

		private void beforeRegistration(Meter.Id id) {
			registrationAttempts.incrementAndGet();
			if (sealed) {
				postSealRegistrationAttempts.incrementAndGet();
				throw new IllegalStateException("meter lookup after registry was sealed: " + id);
			}
			if (registrationFailures.get() == 0 && registrationFailure.test(id)) {
				registrationFailures.incrementAndGet();
				throw new IllegalStateException("expected meter registration failure: " + id);
			}
		}

		private void beforeRecording(Meter.Id id) {
			if (failNextRecordings.remove(id.getName())) {
				recordingFailures.add(id.getName());
				throw new IllegalStateException("expected meter recording failure: " + id);
			}
		}

		@Override
		protected Counter newCounter(Meter.Id id) {
			beforeRegistration(id);
			return new GuardedCounter(super.newCounter(id), this::beforeRecording);
		}

		@Override
		protected Timer newTimer(Meter.Id id,
				DistributionStatisticConfig distributionStatisticConfig,
				PauseDetector pauseDetector) {
			beforeRegistration(id);
			return new GuardedTimer(super.newTimer(id, distributionStatisticConfig, pauseDetector),
					this::beforeRecording);
		}

		@Override
		protected <T> Gauge newGauge(Meter.Id id, T object, ToDoubleFunction<T> valueFunction) {
			beforeRegistration(id);
			return super.newGauge(id, object, valueFunction);
		}

		@Override
		protected Meter newMeter(Meter.Id id, Meter.Type type, Iterable<Measurement> measurements) {
			beforeRegistration(id);
			return super.newMeter(id, type, measurements);
		}
	}

	private record GuardedCounter(Counter delegate,
			java.util.function.Consumer<Meter.Id> beforeRecording) implements Counter {

		@Override
		public void increment(double amount) {
			beforeRecording.accept(getId());
			delegate.increment(amount);
		}

		@Override
		public double count() {
			return delegate.count();
		}

		@Override
		public Meter.Id getId() {
			return delegate.getId();
		}
	}

	private record GuardedTimer(Timer delegate,
			java.util.function.Consumer<Meter.Id> beforeRecording) implements Timer {

		@Override
		public void record(long amount, TimeUnit unit) {
			beforeRecording.accept(getId());
			delegate.record(amount, unit);
		}

		@Override
		public <T> T record(Supplier<T> supplier) {
			beforeRecording.accept(getId());
			return delegate.record(supplier);
		}

		@Override
		public <T> T recordCallable(Callable<T> callable) throws Exception {
			beforeRecording.accept(getId());
			return delegate.recordCallable(callable);
		}

		@Override
		public void record(Runnable runnable) {
			beforeRecording.accept(getId());
			delegate.record(runnable);
		}

		@Override
		public long count() {
			return delegate.count();
		}

		@Override
		public double totalTime(TimeUnit unit) {
			return delegate.totalTime(unit);
		}

		@Override
		public double max(TimeUnit unit) {
			return delegate.max(unit);
		}

		@Override
		public TimeUnit baseTimeUnit() {
			return delegate.baseTimeUnit();
		}

		@Override
		public HistogramSnapshot takeSnapshot() {
			return delegate.takeSnapshot();
		}

		@Override
		public Meter.Id getId() {
			return delegate.getId();
		}
	}
}
