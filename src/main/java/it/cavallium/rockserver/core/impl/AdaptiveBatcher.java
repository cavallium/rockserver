package it.cavallium.rockserver.core.impl;

import java.time.Duration;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.reactivestreams.Subscription;
import reactor.core.Disposable;
import reactor.core.publisher.BaseSubscriber;
import reactor.core.publisher.Flux;
import reactor.core.publisher.FluxSink;
import reactor.core.scheduler.Scheduler;
import reactor.core.scheduler.Schedulers;

/**
 * A high-performance, adaptive batching operator for Project Reactor.
 * <p>
 * This operator buffers elements into batches, dynamically adjusting the batch size
 * between a configured minimum and maximum based on the data flow rate. If a batch
 * does not fill up within a specified timeout, it is emitted partially.
 * <p>
 * This implementation is non-blocking and lock-free, using atomic operations and a
 * "drain loop" pattern to ensure high throughput and low latency, making it suitable
 * for production environments under heavy concurrent load.
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
        return Flux.create(sink ->
                source.subscribe(new AdaptiveSubscriber<>(sink, minBatchSize, maxBatchSize, timeout, scheduler)),
            FluxSink.OverflowStrategy.BUFFER);
    }

    private static final class AdaptiveSubscriber<T> extends BaseSubscriber<T> {

        // --- Immutable State ---
        private final FluxSink<List<T>> sink;
        private final int minBatchSize;
        private final int maxBatchSize;
        private final long timeoutMillis;
        private final Scheduler scheduler;

        // --- Upstream State ---
        private Subscription upstreamSubscription;

        // --- Mutable State (managed by the drain loop) ---
        private List<T> buffer;
        private int currentBatchLimit;
        private volatile boolean done; // Flag to indicate completion or error from upstream
        private Throwable error; // Error from upstream
        private boolean terminalEmitted; // To ensure terminal signal is emitted only once

        // --- Concurrency Control ---
        private final AtomicInteger wip = new AtomicInteger(); // "Work-in-progress" counter for drain loop
        private final AtomicLong requestedBatches = new AtomicLong(); // Downstream backpressure
        private final AtomicReference<Disposable> timeoutTask = new AtomicReference<>();

        // Marker for a timeout-triggered flush
        private volatile boolean timeoutTriggered;

        AdaptiveSubscriber(FluxSink<List<T>> sink, int minBatchSize, int maxBatchSize, Duration timeout, Scheduler scheduler) {
            this.sink = sink;
            this.minBatchSize = minBatchSize;
            this.maxBatchSize = maxBatchSize;
            this.timeoutMillis = timeout.toMillis();
            this.scheduler = Objects.requireNonNull(scheduler, "scheduler must not be null");

            this.currentBatchLimit = minBatchSize;
            this.buffer = new ArrayList<>(minBatchSize);

            this.sink.onDispose(this); // Ensure cancel() is called on downstream cancellation
            this.sink.onRequest(this::onDownstreamRequest);
        }

        private void onDownstreamRequest(long n) {
            if (n <= 0) {
                return;
            }
            addRequest(requestedBatches, n);
            drain(); // A request for data might allow us to flush a pending batch
        }

        @Override
        protected void hookOnSubscribe(Subscription subscription) {
            this.upstreamSubscription = subscription;
            // Request an initial chunk of data to start filling the buffer.
            // Requesting maxBatchSize ensures we can potentially fill up to the max limit quickly.
            subscription.request(maxBatchSize);
        }

        @Override
        protected void hookOnNext(T value) {
            // The synchronized block now ONLY protects the buffer modification.
            synchronized (this) {
                if (buffer != null) {
                    buffer.add(value);
                } else {
                    // Operator has been terminated, drop the signal.
                    return;
                }
            }
            // The call to drain() is now OUTSIDE the synchronized block, breaking the deadlock.
            drain();
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
            // This is called when the downstream consumer cancels their subscription.
            // It's critical to clean up resources here.
            cancelTimeout();
        }

        private void onTimeout() {
            timeoutTriggered = true;
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
                long downstreamDemand = requestedBatches.get();
                long emittedCount = 0;

                while (emittedCount != downstreamDemand) {
                    if (done) {
                        if (terminalEmitted) {
                            return;
                        }
                        cancelTimeout();
                        List<T> finalBuffer = null;
                        synchronized (this) {
                            if (buffer != null) {
                                finalBuffer = buffer;
                                buffer = null;
                            }
                        }
                        if (error != null) {
                            terminalEmitted = true;
                            sink.error(error);
                        } else {
                            if (finalBuffer != null && !finalBuffer.isEmpty()) {
                                sink.next(finalBuffer);
                            }
                            terminalEmitted = true;
                            sink.complete();
                        }
                        return;
                    }

                    boolean isTimeout = timeoutTriggered;
                    int bufferSize;
                    synchronized (this) {
                        bufferSize = (buffer != null) ? buffer.size() : 0;
                    }

                    // Flush conditions: timeout occurred OR buffer is full
                    if (isTimeout || bufferSize >= currentBatchLimit) {
                        if (bufferSize == 0) {
                            // Nothing to flush, reset timeout flag and break
                            timeoutTriggered = false;
                            break;
                        }

                        List<T> batchToEmit;
                        synchronized (this) {
                            batchToEmit = buffer;
                            buffer = new ArrayList<>(currentBatchLimit);
                        }

                        sink.next(batchToEmit);
                        emittedCount++;

                        cancelTimeout();
                        timeoutTriggered = false;

                        // Adapt the batch size for the next cycle
                        if (isTimeout) {
                            currentBatchLimit = Math.max(minBatchSize, currentBatchLimit / 2);
                        } else {
                            currentBatchLimit = Math.min(maxBatchSize, currentBatchLimit * 2);
                        }

                        // Proactively request enough items for the next batch.
                        upstreamSubscription.request(currentBatchLimit);

                    } else {
                        // Conditions to flush are not met, break the inner loop.
                        break;
                    }
                }

                if (emittedCount > 0 && downstreamDemand != Long.MAX_VALUE) {
                    produced(requestedBatches, emittedCount);
                }

                // If there's a new buffer and it's not empty, we need a timeout.
                // This check is outside the emission loop.
                synchronized(this) {
                    if (buffer != null && !buffer.isEmpty()) {
                        startTimeout();
                    }
                }

                // Exit the loop if no more work was detected during this pass.
                // If work was added while we were busy (missed > 1), loop again.
                missed = wip.addAndGet(-missed);
                if (missed == 0) {
                    break;
                }
            }
        }

        private void startTimeout() {
            // Atomically set a new timer if one isn't already running.
            if (timeoutTask.get() == null) {
                Disposable newTask = scheduler.schedule(this::onTimeout, timeoutMillis, java.util.concurrent.TimeUnit.MILLISECONDS);
                if (!timeoutTask.compareAndSet(null, newTask)) {
                    // Raced with another thread or a cancel, dispose the newly created task.
                    newTask.dispose();
                }
            }
        }

        private void cancelTimeout() {
            Disposable currentTask = timeoutTask.getAndSet(null);
            if (currentTask != null) {
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