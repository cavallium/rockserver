/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.ratis.server.impl;

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.server.leader.LogAppender;
import org.apache.ratis.util.TimeDuration;

import java.util.*;
import java.util.concurrent.*;
import java.util.function.Predicate;
import java.util.function.Supplier;

/** Fresh sealed read rounds; neither completed commit indexes nor in-flight rounds are reusable barriers. */
class ReadIndexHeartbeats {
  static final int MAX_WAITERS = 1024;
  static final long MAX_RETAINED_BYTES = 128 * 1024;
  static final long WAITER_BYTES = 128;
  static final int MAX_NODE_PUBLICATIONS = 4096;
  private static final Semaphore PUBLICATIONS = new Semaphore(MAX_NODE_PUBLICATIONS);
  private static final Executor FAILURE_PUBLISHER = task -> Thread.startVirtualThread(task);
  // A joint configuration may contain both bounded old and new replica sets.
  static final int MAX_APPENDERS = 32;
  private static final class AppenderCapacityException extends IllegalArgumentException {
    AppenderCapacityException() { super("Read round appender budget exceeded"); }
  }
  static final long BATCH_NANOS = TimeUnit.MILLISECONDS.toNanos(1);
  private static final long COMMIT_POLL_NANOS = TimeUnit.MILLISECONDS.toNanos(10);

  interface Handle { void cancel(); }
  interface Timer {
    long now();
    // Must enqueue, never invoke the callback inline.
    Handle schedule(Runnable task, long delayNanos);
  }

  private static final ScheduledThreadPoolExecutor SHARED = new ScheduledThreadPoolExecutor(2, task -> {
    Thread thread = new Thread(task, "ratis-read-round-timer");
    thread.setDaemon(true);
    return thread;
  });
  static {
    SHARED.setRemoveOnCancelPolicy(true);
    SHARED.setKeepAliveTime(30, TimeUnit.SECONDS);
    SHARED.allowCoreThreadTimeOut(true);
  }
  private static final Timer TIMER = new Timer() {
    public long now() { return System.nanoTime(); }
    public Handle schedule(Runnable task, long delay) {
      ScheduledFuture<?> scheduled = SHARED.schedule(task, Math.max(0, delay), TimeUnit.NANOSECONDS);
      return () -> scheduled.cancel(false);
    }
  };

  /** Snapshot obtained atomically under the server monitor, never under this tracker's monitor. */
  static final class Context {
    final long term, readIndex, committedIndex;
    final Object configuration;
    final List<LogAppender> appenders;
    final Predicate<Predicate<RaftPeerId>> hasMajority;
    Context(long term, Object configuration, List<LogAppender> appenders, long readIndex,
            long committedIndex, Predicate<Predicate<RaftPeerId>> hasMajority) {
      if (appenders.size() > MAX_APPENDERS) throw new AppenderCapacityException();
      this.term = term;
      this.configuration = Objects.requireNonNull(configuration);
      this.appenders = List.copyOf(appenders);
      this.readIndex = readIndex;
      this.committedIndex = committedIndex;
      this.hasMajority = Objects.requireNonNull(hasMajority);
    }
    boolean sameGeneration(Context other) {
      if (other == null || term != other.term || configuration != other.configuration
          || appenders.size() != other.appenders.size()) return false;
      for (LogAppender appender : appenders) {
        if (other.appenders.stream().noneMatch(current -> current == appender)) return false;
      }
      return true;
    }
  }

  private static final class Waiter {
    final long minimum, bytes, deadline;
    final CompletableFuture<Long> future = new CompletableFuture<>();
    boolean released, publicationOwned, publicationReleased;
    Waiter(long minimum, long bytes, long deadline) { this.minimum = minimum; this.bytes = bytes; this.deadline = deadline; }
  }
  private static final class Ack {
    final LogAppender appender;
    final long nextCallId;
    boolean acknowledged;
    Ack(LogAppender appender) { this.appender = appender; this.nextCallId = appender.getCallId(); }
  }
  private static final class Round {
    final ArrayList<Waiter> waiters;
    Context context;
    long target;
    Map<RaftPeerId, Ack> acknowledgements;
    Round(List<Waiter> waiters) { this.waiters = new ArrayList<>(waiters); }
  }
  private static final class Outcome {
    final long index;
    final Throwable failure;
    Outcome(long index, Throwable failure) { this.index = index; this.failure = failure; }
  }

  private final Supplier<Context> contextSupplier;
  private final Supplier<CompletableFuture<Long>> readiness;
  private final Executor executor, failurePublisher;
  private final Timer timer;
  private final long timeoutNanos, maxBytes;
  private final int maxWaiters;
  private final ArrayList<Waiter> queued = new ArrayList<>();
  private final LinkedHashMap<Waiter, Outcome> completions = new LinkedHashMap<>();
  private Round active;
  private Throwable stopped;
  private boolean readinessAttached, ready, workScheduled, workRequested, completionScheduled, failureScheduled;
  private int retainedWaiters;
  private long retainedBytes;
  private Handle sealTimer, deadlineTimer, commitTimer;

  ReadIndexHeartbeats(Supplier<Context> context, Supplier<CompletableFuture<Long>> readiness,
                      TimeDuration timeout, Executor executor) {
    this(context, readiness, timeout.toLong(TimeUnit.NANOSECONDS), executor, TIMER, MAX_WAITERS, MAX_RETAINED_BYTES, FAILURE_PUBLISHER);
  }
  ReadIndexHeartbeats(Supplier<Context> context, Supplier<CompletableFuture<Long>> readiness,
                      long timeoutNanos, Executor executor, Timer timer, int maxWaiters, long maxBytes, Executor failurePublisher) {
    if (timeoutNanos <= 0 || timeoutNanos > Long.MAX_VALUE / 4 || maxWaiters <= 0 || maxBytes <= 0) {
      throw new IllegalArgumentException("Invalid read round limits");
    }
    this.contextSupplier = Objects.requireNonNull(context);
    this.readiness = Objects.requireNonNull(readiness);
    this.executor = Objects.requireNonNull(executor);
    this.failurePublisher = Objects.requireNonNull(failurePublisher);
    this.timer = Objects.requireNonNull(timer);
    this.timeoutNanos = timeoutNanos;
    this.maxWaiters = maxWaiters;
    this.maxBytes = maxBytes;
  }

  CompletableFuture<Long> request(Long minimumIndex, long bytes) {
    Waiter waiter;
    synchronized (this) {
      if (stopped != null) return failed(stopped);
      if (minimumIndex != null && minimumIndex < 0 || bytes < WAITER_BYTES
          || retainedWaiters >= maxWaiters || bytes > maxBytes - retainedBytes) {
        return failed(new ReadIndexException("Read waiter budget or index is invalid"));
      }
      if (!PUBLICATIONS.tryAcquire()) return failed(new ReadIndexException("Node read publication budget exhausted"));
      waiter = new Waiter(minimumIndex == null ? 0 : minimumIndex, bytes, timer.now() + timeoutNanos);
      retainedWaiters++;
      retainedBytes += bytes;
      queued.add(waiter);
      scheduleDeadline();
      scheduleSeal();
    }
    waiter.future.whenComplete((index, failure) -> { if (waiter.future.isCancelled()) cancel(waiter); });
    requestWork();
    return waiter.future;
  }

  private static CompletableFuture<Long> failed(Throwable failure) {
    CompletableFuture<Long> future = new CompletableFuture<>();
    future.completeExceptionally(failure);
    return future;
  }

  private synchronized void scheduleSeal() {
    if (stopped == null && ready && active == null && !queued.isEmpty() && sealTimer == null) {
      sealTimer = timer.schedule(this::seal, BATCH_NANOS);
    }
  }
  private void seal() {
    synchronized (this) {
      sealTimer = null;
      if (stopped != null || !ready || active != null || queued.isEmpty()) return;
      // SEALING is published before context acquisition; later arrivals belong only to the next batch.
      active = new Round(queued);
      queued.clear();
    }
    requestWork();
  }

  private void requestWork() {
    synchronized (this) {
      if (stopped != null) return;
      workRequested = true;
      if (workScheduled) return;
      workScheduled = true;
    }
    try { executor.execute(this::work); }
    catch (RejectedExecutionException unavailable) {
      synchronized (this) { workScheduled = false; }
      failListeners(new ReadIndexException("Read execution resource unavailable", unavailable));
    }
  }

  private void work() {
    try {
      boolean attach;
      synchronized (this) {
        workRequested = false;
        attach = !readinessAttached && stopped == null;
        if (attach) readinessAttached = true;
      }
      if (attach) {
        readiness.get().whenComplete((index, failure) -> {
          if (failure != null) failListeners(new ReadIndexException("Leader startup did not complete", failure));
          else synchronized (ReadIndexHeartbeats.this) { ready = true; scheduleSeal(); }
        });
      }
      Round round;
      synchronized (this) { round = active; }
      if (round == null) return;
      Context current = contextSupplier.get();
      if (round.context == null) {
        long target = current.readIndex;
        synchronized (this) {
          if (active != round || stopped != null) return;
          for (Waiter waiter : round.waiters) target = Math.max(target, waiter.minimum);
        }
        Map<RaftPeerId, Ack> acks = new HashMap<>();
        // Capture ALL next IDs before publishing the initialized round and before ANY trigger.
        for (LogAppender appender : current.appenders) {
          if (acks.put(appender.getFollowerId(), new Ack(appender)) != null) {
            throw new ReadIndexException("Duplicate read round appender");
          }
        }
        Context checked = contextSupplier.get();
        if (!current.sameGeneration(checked)) { invalidate(); return; }
        synchronized (this) {
          if (active != round || stopped != null) return;
          round.context = current;
          round.target = target;
          round.acknowledgements = acks;
        }
        // Transport callbacks can arrive synchronously; the complete round is now registered.
        for (LogAppender appender : current.appenders) appender.triggerHeartbeat();
      }
      Set<RaftPeerId> observedAcks = new HashSet<>();
      synchronized (this) {
        if (active != round || stopped != null) return;
        round.acknowledgements.forEach((id, ack) -> { if (ack.acknowledged) observedAcks.add(id); });
      }
      // Quorum evidence must predate this current-generation check. A later ACK cannot be
      // combined with a context sampled before a configuration/term change.
      current = contextSupplier.get();
      if (!round.context.sameGeneration(current)) { invalidate(); return; }
      boolean complete;
      synchronized (this) {
        if (active != round || stopped != null) return;
        complete = current.committedIndex >= round.target && round.context.hasMajority.test(observedAcks::contains);
        if (complete) {
          active = null;
          for (Waiter waiter : round.waiters) completions.put(waiter, new Outcome(round.target, null));
          scheduleSeal();
          scheduleDeadline();
        } else if (current.committedIndex < round.target && commitTimer == null) {
          commitTimer = timer.schedule(() -> {
            synchronized (ReadIndexHeartbeats.this) { commitTimer = null; }
            requestWork();
          }, COMMIT_POLL_NANOS);
        }
      }
      if (complete) dispatchCompletions();
    } catch (AppenderCapacityException capacity) {
      synchronized (this) { detachPending(new ReadIndexException(capacity.getMessage(), capacity)); }
      dispatchCompletions();
    } catch (Exception failure) {
      failListeners(new ReadIndexException("Unable to establish fresh read round", failure));
    } finally {
      boolean again;
      synchronized (this) { workScheduled = false; again = workRequested && stopped == null; }
      if (again) requestWork();
    }
  }

  void onAppendEntriesReply(LogAppender appender, AppendEntriesReplyProto reply) {
    boolean changed = false;
    synchronized (this) {
      Round round = active;
      if (round == null || round.context == null || stopped != null || reply == null
          || reply.getTerm() != round.context.term || !reply.getServerReply().getSuccess()
          || reply.getFollowerCommit() < round.target) return;
      Ack ack = round.acknowledgements.get(appender.getFollowerId());
      if (ack != null && ack.appender == appender && !ack.acknowledged
          && appender.getCallIdComparator().compare(reply.getServerReply().getCallId(), ack.nextCallId) >= 0) {
        ack.acknowledged = true;
        changed = true;
      }
    }
    if (changed) requestWork();
  }

  /** Same-term membership/appender replacement fails pending reads retryably, without reusing acknowledgements. */
  void invalidate() {
    synchronized (this) { detachPending(new ReadIndexException("Read round generation changed")); }
    dispatchCompletions();
  }
  void failListeners(Exception failure) {
    synchronized (this) {
      if (stopped != null) return;
      stopped = failure;
      detachPending(failure);
      // A decided but not yet published result also fails when leadership/shutdown invalidates its owner.
      completions.replaceAll((waiter, old) -> new Outcome(0, failure));
      scheduleDeadline();
    }
    dispatchCompletions();
  }
  private void detachPending(Throwable failure) {
    for (Waiter waiter : queued) completions.put(waiter, new Outcome(0, failure));
    queued.clear();
    if (active != null) {
      for (Waiter waiter : active.waiters) completions.put(waiter, new Outcome(0, failure));
      active = null;
    }
    if (sealTimer != null) { sealTimer.cancel(); sealTimer = null; }
    if (deadlineTimer != null) { deadlineTimer.cancel(); deadlineTimer = null; }
    if (commitTimer != null) { commitTimer.cancel(); commitTimer = null; }
    // Generation/capacity changes can leave an earlier decided result waiting for publication.
    scheduleDeadline();
  }

  private void dispatchCompletions() {
    boolean success, failure;
    synchronized (this) {
      success = !completionScheduled && completions.values().stream().anyMatch(outcome -> outcome.failure == null);
      failure = !failureScheduled && completions.values().stream().anyMatch(outcome -> outcome.failure != null);
      if (success) completionScheduled = true;
      if (failure) failureScheduled = true;
    }
    // Timer work only detaches failures and dispatches bounded publications. Synchronous future
    // observers run elsewhere; they cannot occupy deadline threads. Success uses the server executor.
    if (failure) timer.schedule(() -> complete(false), 0);
    if (success) {
      try { executor.execute(() -> complete(true)); }
      catch (RejectedExecutionException rejected) {
        synchronized (this) {
          completionScheduled = false;
          completions.replaceAll((waiter, old) -> new Outcome(0,
              new ReadIndexException("Read completion execution resource unavailable", rejected)));
        }
        dispatchCompletions();
      }
    }
  }
  private void complete(boolean success) {
    for (;;) {
      Waiter waiter;
      Outcome outcome;
      synchronized (this) {
        Iterator<Map.Entry<Waiter, Outcome>> iterator = completions.entrySet().iterator();
        Map.Entry<Waiter, Outcome> entry = null;
        while (iterator.hasNext()) {
          Map.Entry<Waiter, Outcome> candidate = iterator.next();
          if ((candidate.getValue().failure == null) == success) { entry = candidate; break; }
        }
        if (entry == null) {
          if (success) completionScheduled = false; else failureScheduled = false;
          return;
        }
        waiter = entry.getKey(); outcome = entry.getValue(); iterator.remove();
        if (outcome.failure == null && timer.now() - waiter.deadline >= 0) {
          outcome = new Outcome(0, new ReadIndexException("Fresh read round timed out before publication"));
        }
        waiter.publicationOwned = true;
        release(waiter);
        scheduleDeadline();
      }
      // State and budgets are detached before promise publication; no tracker/server monitor is held.
      if (outcome.failure == null) publish(waiter, outcome);
      else {
        final Outcome failure = outcome;
        try { failurePublisher.execute(() -> publish(waiter, failure)); }
        catch (RejectedExecutionException rejected) {
          // Ownership stays with this waiter; the node-bounded virtual fallback is never shut down.
          FAILURE_PUBLISHER.execute(() -> publish(waiter, failure));
        }
      }
    }
  }

  private void publish(Waiter waiter, Outcome outcome) {
    try {
      if (outcome.failure == null) waiter.future.complete(outcome.index);
      else waiter.future.completeExceptionally(outcome.failure);
    } finally { releasePublication(waiter); }
  }
  private synchronized void releasePublication(Waiter waiter) {
    if (!waiter.publicationReleased) {
      waiter.publicationReleased = true;
      PUBLICATIONS.release();
    }
  }

  private synchronized void cancel(Waiter waiter) {
    queued.remove(waiter);
    if (active != null) {
      active.waiters.remove(waiter);
      if (active.waiters.isEmpty()) active = null;
    }
    completions.remove(waiter);
    release(waiter);
    if (!waiter.publicationOwned) releasePublication(waiter);
    scheduleSeal();
    scheduleDeadline();
  }
  private void release(Waiter waiter) {
    if (!waiter.released) {
      waiter.released = true;
      retainedWaiters--;
      retainedBytes -= waiter.bytes;
    }
  }
  private synchronized void scheduleDeadline() {
    if (deadlineTimer != null) { deadlineTimer.cancel(); deadlineTimer = null; }
    long earliest = Long.MAX_VALUE;
    for (Waiter waiter : queued) earliest = Math.min(earliest, waiter.deadline - timer.now());
    if (active != null) for (Waiter waiter : active.waiters) earliest = Math.min(earliest, waiter.deadline - timer.now());
    for (var entry : completions.entrySet()) {
      if (entry.getValue().failure == null) earliest = Math.min(earliest, entry.getKey().deadline - timer.now());
    }
    if (earliest != Long.MAX_VALUE) deadlineTimer = timer.schedule(this::expire, Math.max(0, earliest));
  }
  private void expire() {
    synchronized (this) {
      deadlineTimer = null;
      long now = timer.now();
      Outcome timeout = new Outcome(0, new ReadIndexException("Fresh read round timed out"));
      queued.removeIf(waiter -> { if (now - waiter.deadline < 0) return false; completions.put(waiter, timeout); return true; });
      if (active != null) {
        active.waiters.removeIf(waiter -> { if (now - waiter.deadline < 0) return false; completions.put(waiter, timeout); return true; });
        if (active.waiters.isEmpty()) active = null;
      }
      completions.replaceAll((waiter, outcome) -> outcome.failure == null && now - waiter.deadline >= 0 ? timeout : outcome);
      scheduleSeal();
      scheduleDeadline();
    }
    dispatchCompletions();
  }
}
