package it.cavallium.rockserver.core.server;


import io.micrometer.core.instrument.MeterRegistry;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import java.time.Duration;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.LongAdder;

import java.util.concurrent.TimeUnit;
import reactor.util.context.ContextView;

/**
 * The must-complete operations (rollbacks, closes) a server has accepted: they run to the end even when their call is
 * cancelled, and {@link GrpcServer#close()} waits for them. Also counts the ones whose caller was gone when they failed.
 */
final class MustCompleteOperationTracker {

	private final MeterRegistry metricsRegistry;

	private final Object monitor = new Object();
	private int acceptedOperations;
	private boolean accepting = true;

	MustCompleteOperationTracker(MeterRegistry metricsRegistry) {
		this.metricsRegistry = metricsRegistry;
	}

	boolean register() {
		synchronized (monitor) {
			if (!accepting) {
				return false;
			}
			acceptedOperations++;
			return true;
		}
	}

	void operationTerminated() {
		synchronized (monitor) {
			if (acceptedOperations == 0) {
				throw new IllegalStateException("Must-complete operation accounting underflow");
			}
			acceptedOperations--;
			if (acceptedOperations == 0) {
				monitor.notifyAll();
			}
		}
	}

	DrainResult stopAcceptingAndAwait(Duration timeout) {
		long timeoutNanos = GrpcServerSettings.saturatedTimeoutNanos(timeout);
		long remainingNanos = timeoutNanos;
		long waitStartedNanos = System.nanoTime();
		boolean interrupted = false;
		synchronized (monitor) {
			accepting = false;
			while (acceptedOperations != 0) {
				if (remainingNanos <= 0L) {
					return new DrainResult(false, interrupted);
				}
				try {
					TimeUnit.NANOSECONDS.timedWait(monitor, remainingNanos);
				} catch (InterruptedException _) {
					interrupted = true;
				}
				remainingNanos = GrpcServerSettings.remainingTimeoutNanos(timeoutNanos, waitStartedNanos);
			}
			return new DrainResult(true, interrupted);
		}
	}

	int acceptedOperations() {
		synchronized (monitor) {
			return acceptedOperations;
		}
	}

	private static final String LATE_PROTECTED_OPERATION_FAILURE_METRIC
			= "rockserver.grpc.protected.late.failures";

	final AtomicLong cancelledMustCompleteOperations = new AtomicLong();

	final ConcurrentMap<String, LongAdder> lateProtectedOperationFailures = new ConcurrentHashMap<>();

	record DrainResult(boolean drained, boolean interrupted) {}

	void recordLateProtectedOperationFailure(String operation,
			Throwable error,
			ContextView context) {
		countLateProtectedOperationFailure(operation);
		GrpcRequestErrors.lateErrorHandler(context).accept(error);
	}

	void countLateProtectedOperationFailure(String operation) {
		lateProtectedOperationFailures.computeIfAbsent(operation, _ -> new LongAdder()).increment();
		metricsRegistry.counter(LATE_PROTECTED_OPERATION_FAILURE_METRIC, "operation", operation).increment();
	}
}
