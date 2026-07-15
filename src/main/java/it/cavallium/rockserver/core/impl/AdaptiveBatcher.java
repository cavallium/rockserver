package it.cavallium.rockserver.core.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.Exceptions;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;
import reactor.util.context.Context;

/**
 * A high-performance, adaptive batching operator for Project Reactor.
 * <p>
 * This operator buffers elements into batches, dynamically adjusting the batch size
 * between a configured minimum and maximum based on the data flow rate. If a batch
 * does not fill up within a specified timeout, it is emitted partially.
 * <p>
 * This implementation is non-blocking. A drain loop serializes state transitions and
 * emissions, while short critical sections protect producer access to the active batch.
 */
public final class AdaptiveBatcher {

    /**
     * Buffers elements into batches with sizes that adapt based on data flow rate and time.
     *
     * @param <T>          The type of elements in the stream.
     * @param source       The source Flux to buffer.
     * @param minBatchSize The minimum number of items to include in a batch.
     * @param maxBatchSize The maximum number of items to include in a batch.
     * @param timeout      The maximum duration to wait before emitting a partial batch.
     * @return A new Flux that emits lists of buffered items.
     */
    public static <T> Flux<List<T>> buffer(Flux<T> source, int minBatchSize, int maxBatchSize, Duration timeout) {
        // Use a dedicated parallel scheduler for timers, which is a common practice for such tasks.
        return buffer(source, minBatchSize, maxBatchSize, timeout, Schedulers.parallel());
    }

    /**
     * Overloaded buffer method for providing a custom Scheduler for timers.
     * Useful for testing or specific performance tuning.
     *
     * @param <T>          The type of elements in the stream.
     * @param source       The source Flux to buffer.
     * @param minBatchSize The minimum number of items to include in a batch.
     * @param maxBatchSize The maximum number of items to include in a batch.
     * @param timeout      The maximum duration to wait before emitting a partial batch.
     * @param scheduler    The scheduler on which to run the timeout tasks.
     * @return A new Flux that emits lists of buffered items.
     */
    public static <T> Flux<List<T>> buffer(Flux<T> source, int minBatchSize, int maxBatchSize, Duration timeout, Scheduler scheduler) {
        Objects.requireNonNull(source, "source must not be null");
        Objects.requireNonNull(timeout, "timeout must not be null");
        Objects.requireNonNull(scheduler, "scheduler must not be null");
        if (minBatchSize <= 0) {
            throw new IllegalArgumentException("minBatchSize must be greater than zero");
        }
        if (maxBatchSize < minBatchSize) {
            throw new IllegalArgumentException("maxBatchSize must be greater than or equal to minBatchSize");
        }
        if (timeout.isZero() || timeout.isNegative()) {
            throw new IllegalArgumentException("timeout must be greater than zero");
        }
        long timeoutNanos = timeout.toNanos();
        return Flux.create(sink ->
                source.subscribe(new AdaptiveSubscriber<>(sink, minBatchSize, maxBatchSize, timeoutNanos, scheduler)),
            FluxSink.OverflowStrategy.ERROR);
    }

    private static final class AdaptiveSubscriber<T> extends BaseSubscriber<T> {

        // --- Immutable State ---
        private final FluxSink<List<T>> sink;
        private final int minBatchSize;
        private final int maxBatchSize;
        private final long timeoutNanos;
        private final Scheduler scheduler;

        // --- Upstream State ---
        private Subscription upstreamSubscription;
        // Guarded by this. Includes requests reserved immediately before calling request(n),
        // so a synchronous publisher cannot race the accounting.
        private long upstreamOutstanding;

        // --- Mutable State (managed by the drain loop) ---
        private List<T> buffer;
        private int currentBatchLimit;
        private boolean prefetchedForCurrentBatch;
        private volatile boolean done; // Flag to indicate completion or error from upstream
        private Throwable error; // Error from upstream
        private boolean terminalEmitted; // To ensure terminal signal is emitted only once

        // --- Concurrency Control ---
        private final AtomicInteger wip = new AtomicInteger(); // "Work-in-progress" counter for drain loop
        private final AtomicLong requestedBatches = new AtomicLong(); // Downstream backpressure
        private final AtomicReference<Disposable> timeoutTask = new AtomicReference<>();
        private final AtomicLong timeoutGeneration = new AtomicLong();

        private static final Disposable SCHEDULING = () -> { };

        // Generation of the timer requesting a flush. Comparing generations prevents a
        // canceled timer that was already running from flushing a newer batch.
        private volatile long timeoutTriggeredGeneration;

        AdaptiveSubscriber(FluxSink<List<T>> sink, int minBatchSize, int maxBatchSize, long timeoutNanos, Scheduler scheduler) {
            this.sink = sink;
            this.minBatchSize = minBatchSize;
            this.maxBatchSize = maxBatchSize;
            this.timeoutNanos = timeoutNanos;
            this.scheduler = scheduler;

            this.currentBatchLimit = minBatchSize;
            this.buffer = new ArrayList<>(minBatchSize);

            this.sink.onDispose(this); // Ensure cancel() is called on downstream cancellation
            this.sink.onRequest(this::onDownstreamRequest);
        }

        @Override
        public Context currentContext() {
            return sink.currentContext();
        }

        private void onDownstreamRequest(long n) {
            if (n <= 0) {
                return;
            }
            addRequest(requestedBatches, n);
            // Flush an already-fired timeout before requesting late synchronous items.
            // Otherwise those items could fill the timed-out batch and incorrectly turn
            // a partial/slow batch into a full/fast one.
            drain();
            requestNextBatchIfNeeded();
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            this.upstreamSubscription = subscription;
            synchronized (this) {
                upstreamOutstanding = minBatchSize;
            }
            // Keep at most one batch in memory while downstream is paused. Requesting the
            // maximum here can collapse the entire prefetch into one oversized first batch.
            subscription.request(minBatchSize);
        }

        @Override
        protected void hookOnNext(T value) {
            boolean firstItem;
            boolean batchReady;
            boolean shouldPrefetch;
            synchronized (this) {
                if (buffer != null) {
                    if (upstreamOutstanding > 0) {
                        upstreamOutstanding--;
                    }
                    firstItem = buffer.isEmpty();
                    buffer.add(value);
                    batchReady = buffer.size() >= currentBatchLimit;
                    shouldPrefetch = !prefetchedForCurrentBatch
                        && requestedBatches.get() != 0
                        && buffer.size() >= Math.max(1, currentBatchLimit - 1);
                } else {
                    // Operator has been terminated, drop the signal.
                    return;
                }
            }
            if (shouldPrefetch) {
                requestNextBatchIfNeeded();
            }
            // Intermediate items need neither an atomic drain-loop round trip nor repeated
            // buffer inspection. Only the first item arms the timer and a full batch drains.
            if (firstItem && !batchReady) {
                startTimeout();
            }
            if (batchReady) {
                // Once a batch is full its partial-flush timeout is obsolete, even if
                // downstream backpressure prevents immediate delivery.
                cancelTimeout();
                drain();
            }
        }

        @Override
        protected void hookOnComplete() {
            done = true;
            drain();
        }

        @Override
        protected void hookOnError(Throwable throwable) {
            error = throwable;
            done = true;
            drain();
        }

        @Override
        protected void hookOnCancel() {
            cancelTimeout();
            synchronized (this) {
                buffer = null;
                upstreamOutstanding = 0;
                prefetchedForCurrentBatch = false;
            }
        }

        private void onTimeout(long generation) {
            if (timeoutGeneration.get() != generation) {
                return;
            }
            timeoutTriggeredGeneration = generation;
            drain();
        }

        /**
         * The core of the operator. This method serializes all state changes and emissions.
         * It is triggered by onNext, onComplete, onTimeout, and downstream requests.
         * The `wip` counter ensures only one thread executes the logic at a time.
         */
        private void drain() {
            // Attempt to acquire the "work" lock. If another thread is already draining, it will handle our event.
            if (wip.getAndIncrement() != 0) {
                return;
            }

            int missed = 1;
            while (true) {
                if (sink.isCancelled()) {
                    return;
                }

                if (done && error != null) {
                    terminate(error);
                    return;
                }
                if (done) {
                    cancelTimeout();
                }

                long downstreamDemand = requestedBatches.get();
                long emittedCount = 0;

                while (emittedCount != downstreamDemand) {
                    if (done) {
                        Throwable terminalError = error;
                        if (terminalError != null) {
                            terminate(terminalError);
                            return;
                        }
                        cancelTimeout();
                        List<T> finalBatch = takeFinalBatch();
                        if (finalBatch != null) {
                            sink.next(finalBatch);
                            emittedCount++;
                            continue;
                        }
                        terminalEmitted = true;
                        sink.complete();
                        return;
                    }

                    long triggeredGeneration = timeoutTriggeredGeneration;
                    boolean isTimeout = triggeredGeneration != 0
                        && triggeredGeneration == timeoutGeneration.get();
                    List<T> batchToEmit;
                    long requestMore;
                    synchronized (this) {
                        int bufferSize = buffer != null ? buffer.size() : 0;
                        if (bufferSize == 0 || (!isTimeout && bufferSize < currentBatchLimit)) {
                            if (isTimeout && bufferSize == 0) {
                                timeoutTriggeredGeneration = 0;
                            }
                            break;
                        }

                        // A batch that filled exactly as its timer fired is still a fast,
                        // full batch and should grow rather than shrink.
                        boolean partialTimeout = isTimeout && bufferSize < currentBatchLimit;
                        int emittedBatchSize = partialTimeout ? bufferSize : currentBatchLimit;
                        int nextBatchLimit = partialTimeout
                            ? Math.max(minBatchSize, currentBatchLimit / 2)
                            : (int) Math.min(maxBatchSize, (long) currentBatchLimit * 2L);

                        if (emittedBatchSize == bufferSize) {
                            batchToEmit = buffer;
                            buffer = new ArrayList<>(nextBatchLimit);
                        } else {
                            // Requests already in flight cannot be retracted when a timeout
                            // shrinks the target. Split that delayed burst at the new limit.
                            batchToEmit = new ArrayList<>(buffer.subList(0, emittedBatchSize));
                            List<T> remainder = new ArrayList<>(nextBatchLimit);
                            remainder.addAll(buffer.subList(emittedBatchSize, bufferSize));
                            buffer = remainder;
                        }
                        currentBatchLimit = nextBatchLimit;
                        prefetchedForCurrentBatch = false;

                        // A timeout can leave part of the previous request in flight. Reuse
                        // that demand instead of adding a complete second batch on top of it.
                        requestMore = Math.max(0L,
                            nextBatchLimit - (long) buffer.size() - upstreamOutstanding);
                        upstreamOutstanding += requestMore;
                    }

                    // Invalidate the old timer before invoking downstream code. A slow
                    // subscriber must not let that timer expire against the next batch.
                    cancelTimeout();
                    timeoutTriggeredGeneration = 0;

                    if (requestMore > 0 && !done && !sink.isCancelled()) {
                        upstreamSubscription.request(requestMore);
                    }

                    sink.next(batchToEmit);
                    emittedCount++;
                }

                if (emittedCount > 0 && downstreamDemand != Long.MAX_VALUE) {
                    produced(requestedBatches, emittedCount);
                }

                if (done) {
                    Throwable terminalError = error;
                    if (terminalError != null) {
                        terminate(terminalError);
                        return;
                    }
                    if (isBufferEmpty()) {
                        cancelTimeout();
                        terminalEmitted = true;
                        sink.complete();
                        return;
                    }
                }

                if (!done && needsTimeout()) {
                    startTimeout();
                }

                // Exit the loop if no more work was detected during this pass.
                // If work was added while we were busy (missed > 1), loop again.
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        private boolean isBufferEmpty() {
            synchronized (this) {
                return buffer == null || buffer.isEmpty();
            }
        }

        private List<T> takeFinalBatch() {
            synchronized (this) {
                upstreamOutstanding = 0;
                if (buffer == null || buffer.isEmpty()) {
                    buffer = null;
                    return null;
                }

                int emittedBatchSize = Math.min(currentBatchLimit, buffer.size());
                if (emittedBatchSize == buffer.size()) {
                    List<T> finalBatch = buffer;
                    buffer = null;
                    return finalBatch;
                }

                List<T> finalBatch = new ArrayList<>(buffer.subList(0, emittedBatchSize));
                buffer = new ArrayList<>(buffer.subList(emittedBatchSize, buffer.size()));
                return finalBatch;
            }
        }

        private boolean needsTimeout() {
            synchronized (this) {
                return buffer != null && !buffer.isEmpty() && buffer.size() < currentBatchLimit;
            }
        }

        private void requestNextBatchIfNeeded() {
            Subscription subscription = upstreamSubscription;
            if (subscription == null || done || sink.isCancelled() || requestedBatches.get() == 0) {
                return;
            }

            long requestMore;
            synchronized (this) {
                if (buffer == null
                        || prefetchedForCurrentBatch
                        || requestedBatches.get() == 0
                        || timeoutTriggeredGeneration != 0) {
                    return;
                }
                int prefetchThreshold = Math.max(1, currentBatchLimit - 1);
                if (buffer.size() < prefetchThreshold) {
                    return;
                }

                int nextBatchLimit = (int) Math.min(maxBatchSize, (long) currentBatchLimit * 2L);
                long remainingCurrentBatch = Math.max(0, currentBatchLimit - buffer.size());
                long targetOutstanding = remainingCurrentBatch + nextBatchLimit;
                requestMore = Math.max(0L, targetOutstanding - upstreamOutstanding);
                upstreamOutstanding += requestMore;
                prefetchedForCurrentBatch = true;
            }

            if (requestMore > 0 && !done && !sink.isCancelled()) {
                subscription.request(requestMore);
            }
        }

        private void terminate(Throwable throwable) {
            if (terminalEmitted) {
                return;
            }
            terminalEmitted = true;
            cancelTimeout();
            synchronized (this) {
                buffer = null;
                upstreamOutstanding = 0;
            }
            sink.error(throwable);
        }

        private void startTimeout() {
            if (sink.isCancelled() || timeoutTask.get() != null) {
                return;
            }
            if (!timeoutTask.compareAndSet(null, SCHEDULING)) {
                return;
            }
            if (sink.isCancelled()) {
                timeoutTask.compareAndSet(SCHEDULING, null);
                return;
            }

            long generation = timeoutGeneration.incrementAndGet();
            Disposable newTask;
            try {
                newTask = scheduler.schedule(() -> onTimeout(generation), timeoutNanos, TimeUnit.NANOSECONDS);
            } catch (Throwable throwable) {
                Exceptions.throwIfFatal(throwable);
                timeoutTask.compareAndSet(SCHEDULING, null);
                error = throwable;
                done = true;
                upstreamSubscription.cancel();
                drain();
                return;
            }
            if (!timeoutTask.compareAndSet(SCHEDULING, newTask)) {
                newTask.dispose();
            }
        }

        private void cancelTimeout() {
            timeoutGeneration.incrementAndGet();
            Disposable currentTask = timeoutTask.getAndSet(null);
            if (currentTask != null && currentTask != SCHEDULING) {
                currentTask.dispose();
            }
        }

        /**
         * Atomically adds a value to a requester count, capping at Long.MAX_VALUE.
         * This replaces the now-removed BackpressureUtils.add method.
         */
        static void addRequest(AtomicLong requested, long n) {
            for (;;) {
                long r = requested.get();
                if (r == Long.MAX_VALUE) {
                    return;
                }
                long u = r + n;
                if (u < 0L) { // Overflow
                    u = Long.MAX_VALUE;
                }
                if (requested.compareAndSet(r, u)) {
                    return;
                }
            }
        }

        /**
         * Atomically subtracts a value from a requester count, without going below zero.
         * This replaces the now-removed BackpressureUtils.produced method.
         */
        static void produced(AtomicLong requested, long n) {
            for (;;) {
                long r = requested.get();
                if (r == Long.MAX_VALUE) {
                    return;
                }
                long u = r - n;
                if (u < 0L) {
                    // This indicates an error state (producing more than requested).
                    // Log or throw an exception in a real application if this is critical.
                    u = 0L;
                }
                if (requested.compareAndSet(r, u)) {
                    return;
                }
            }
        }
    }
}
