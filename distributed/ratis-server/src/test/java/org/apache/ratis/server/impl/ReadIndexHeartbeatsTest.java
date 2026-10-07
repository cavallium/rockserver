package org.apache.ratis.server.impl;

import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto;
import org.apache.ratis.proto.RaftProtos.RaftRpcReplyProto;
import org.apache.ratis.protocol.RaftPeerId;
import org.apache.ratis.protocol.exceptions.ReadIndexException;
import org.apache.ratis.server.leader.LogAppender;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Proxy;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicLong;
import java.util.function.Predicate;

import static org.junit.jupiter.api.Assertions.*;

/** Deterministic round/ownership tests only: no transport or real-time sleeps. */
class ReadIndexHeartbeatsTest {
  static final long TIMEOUT = TimeUnit.SECONDS.toNanos(1);
  static final class Timer implements ReadIndexHeartbeats.Timer {
    static final class Task implements ReadIndexHeartbeats.Handle {
      final long at, order;
      final Runnable run;
      boolean cancelled;
      Task(long at, long order, Runnable run) { this.at = at; this.order = order; this.run = run; }
      public void cancel() { cancelled = true; }
    }
    long now, order;
    final PriorityQueue<Task> tasks = new PriorityQueue<>(Comparator.comparingLong((Task t) -> t.at).thenComparingLong(t -> t.order));
    public long now() { return now; }
    public ReadIndexHeartbeats.Handle schedule(Runnable run, long delay) {
      var task = new Task(now + delay, order++, run); tasks.add(task); return task;
    }
    void advance(long nanos) {
      long until = now + nanos;
      int steps = 0;
      while (!tasks.isEmpty() && tasks.peek().at <= until) {
        var task = tasks.remove(); now = task.at;
        if (!task.cancelled) task.run.run();
        assertTrue(++steps < 10000, "timer must not spin");
      }
      now = until;
    }
  }
  static final class ExecutorQueue implements Executor {
    final ArrayDeque<Runnable> tasks = new ArrayDeque<>();
    boolean reject;
    public void execute(Runnable task) {
      if (reject) throw new RejectedExecutionException("fixture");
      tasks.add(task);
    }
    void one() { assertFalse(tasks.isEmpty()); tasks.remove().run(); }
    void drain() {
      int steps = 0;
      while (!tasks.isEmpty()) { one(); assertTrue(++steps < 10000); }
    }
  }
  static final class Peer {
    final RaftPeerId id;
    final AtomicLong next = new AtomicLong(10);
    final LogAppender appender;
    int triggers;
    Runnable onTrigger = () -> {};
    Peer(String name) {
      id = RaftPeerId.valueOf(name);
      appender = (LogAppender) Proxy.newProxyInstance(LogAppender.class.getClassLoader(), new Class<?>[]{LogAppender.class},
          (proxy, method, args) -> switch (method.getName()) {
            case "getFollowerId" -> id;
            case "getCallId" -> next.get();
            case "getCallIdComparator" -> Comparator.<Long>naturalOrder();
            case "triggerHeartbeat" -> { triggers++; onTrigger.run(); yield null; }
            case "toString" -> name;
            default -> throw new AssertionError("Unexpected appender call " + method);
          });
    }
  }
  static final class Fixture {
    final Timer timer = new Timer();
    final ExecutorQueue executor = new ExecutorQueue();
    final CompletableFuture<Long> ready = new CompletableFuture<>();
    final List<Peer> peers = new ArrayList<>(List.of(new Peer("a"), new Peer("b")));
    long term = 1, index = 7, committed = 7;
    Object generation = new Object();
    Predicate<Predicate<RaftPeerId>> majority = ack -> ack.test(peers.getFirst().id) || ack.test(peers.getLast().id);
    final ReadIndexHeartbeats rounds;
    Fixture() { this(16, 16 * ReadIndexHeartbeats.WAITER_BYTES); }
    Fixture(int count, long bytes) { this(count, bytes, Runnable::run); }
    Fixture(int count, long bytes, Executor publisher) {
      rounds = new ReadIndexHeartbeats(() -> new ReadIndexHeartbeats.Context(term, generation,
          peers.stream().map(p -> p.appender).toList(), index, committed, majority),
          () -> ready, TIMEOUT, executor, timer, count, bytes, publisher);
      ready.complete(0L);
    }
    CompletableFuture<Long> request(Long minimum) { return rounds.request(minimum, ReadIndexHeartbeats.WAITER_BYTES); }
    void start() { executor.drain(); timer.advance(ReadIndexHeartbeats.BATCH_NANOS); executor.drain(); }
    void reply(Peer peer, long call, long replyTerm, long followerCommit, boolean success) {
      rounds.onAppendEntriesReply(peer.appender, AppendEntriesReplyProto.newBuilder().setTerm(replyTerm)
          .setFollowerCommit(followerCommit).setServerReply(RaftRpcReplyProto.newBuilder()
          .setCallId(call).setSuccess(success)).build());
    }
    void ack(Peer peer) { reply(peer, peer.next.getAndIncrement(), term, committed, true); }
    void timeout() { timer.advance(TIMEOUT); }
  }
  static void failed(CompletableFuture<Long> future) {
    assertTrue(future.isDone());
    assertInstanceOf(ReadIndexException.class, assertThrows(CompletionException.class, future::join).getCause());
  }

  @Test void completedIndexAndSealedRoundAreNeverReusedForLaterReads() {
    var f = new Fixture();
    var first = f.request(null); f.start();
    var next = f.request(null); // arrives after seal
    f.ack(f.peers.getFirst()); f.executor.drain();
    assertEquals(7, first.join()); assertFalse(next.isDone());
    f.start();
    f.reply(f.peers.getFirst(), 10, 1, 7, true); f.executor.drain();
    assertFalse(next.isDone(), "old round ACK cannot count");
    f.ack(f.peers.getFirst()); f.executor.drain(); assertEquals(7, next.join());
    var warmed = f.request(null); f.start();
    assertFalse(warmed.isDone());
    f.ack(f.peers.getFirst()); f.executor.drain(); assertEquals(7, warmed.join());
  }

  @Test void replyMustMatchAssignedCallTermCommitSuccessAndAppenderIdentity() {
    var f = new Fixture(); var read = f.request(null); f.start();
    var peer = f.peers.getFirst();
    f.reply(peer, 9, 1, 7, true);
    f.reply(peer, 10, 2, 7, true);
    f.reply(peer, 10, 1, 6, true);
    f.reply(peer, 10, 1, 7, false);
    f.reply(new Peer("a"), 10, 1, 7, true);
    f.executor.drain(); assertFalse(read.isDone());
    f.ack(peer); f.executor.drain(); assertEquals(7, read.join());
  }

  @Test void allWatermarksAreRegisteredBeforeSynchronousHeartbeatCallbacks() {
    var f = new Fixture();
    f.majority = ack -> f.peers.stream().allMatch(peer -> ack.test(peer.id));
    for (var peer : f.peers) peer.onTrigger = () -> f.ack(peer);
    var read = f.request(null); f.start();
    assertEquals(7, read.join());
    assertEquals(1, f.peers.getFirst().triggers); assertEquals(1, f.peers.getLast().triggers);
  }

  @Test void jointPredicateAndGenerationReplacementDoNotReuseQuorumEvidence() {
    var f = new Fixture();
    // Simulated old majority = leader+a, new majority = leader+b.
    f.majority = ack -> ack.test(f.peers.getFirst().id) && ack.test(f.peers.getLast().id);
    var read = f.request(null); f.start(); f.ack(f.peers.getFirst()); f.executor.drain();
    assertFalse(read.isDone()); f.ack(f.peers.getLast()); f.executor.drain(); assertEquals(7, read.join());
    var pending = f.request(null); f.start();
    f.generation = new Object(); f.ack(f.peers.getFirst()); f.executor.drain(); f.timer.advance(0); failed(pending);
    var replaced = f.request(null); f.start();
    f.peers.set(0, new Peer("a")); f.ack(f.peers.getLast()); f.executor.drain(); f.timer.advance(0); failed(replaced);
    var recovered = f.request(null); f.start();
    f.ack(f.peers.getFirst()); f.ack(f.peers.getLast()); f.executor.drain(); assertEquals(7, recovered.join());
  }

  @Test void actualJointConfigurationRequiresBothVoterMajoritiesAndExcludesListeners() {
    var f = new Fixture(); var leader = RaftPeerId.valueOf("leader");
    var oldPeers = List.of(leader, f.peers.getFirst().id, RaftPeerId.valueOf("old-extra"));
    var newPeers = List.of(leader, f.peers.getLast().id, RaftPeerId.valueOf("new-extra"));
    java.util.function.Function<List<RaftPeerId>, List<org.apache.ratis.protocol.RaftPeer>> members = ids ->
        ids.stream().map(id -> org.apache.ratis.protocol.RaftPeer.newBuilder().setId(id).build()).toList();
    var listener = new Peer("listener"); f.peers.add(listener);
    var listeners = members.apply(List.of(listener.id));
    var conf = RaftConfigurationImpl.newBuilder().setOldConf(members.apply(oldPeers), listeners)
        .setConf(members.apply(newPeers), listeners).build();
    f.generation = conf; f.majority = ack -> conf.hasMajority(ack, leader);
    var read = f.request(null); f.start();
    f.ack(listener); f.ack(f.peers.getFirst()); f.executor.drain(); assertFalse(read.isDone());
    f.ack(f.peers.get(1)); f.executor.drain(); assertEquals(7, read.join());
  }

  @Test void successfulRoundStillWaitsForActualCompletedApplyBarrierBeforeQuery() {
    var f = new Fixture();
    var applied = new ReadRequests(6, new org.apache.ratis.conf.RaftProperties());
    var queries = new java.util.concurrent.atomic.AtomicInteger();
    var query = f.request(null).thenCompose(index -> applied.waitToAdvance(index,
        () -> null))
        .thenApply(index -> queries.incrementAndGet());
    try {
      f.start(); f.ack(f.peers.getFirst()); f.executor.drain();
      assertFalse(query.isDone()); assertEquals(0, queries.get());
      applied.getAppliedIndexConsumer().accept(7);
      assertEquals(1, query.join()); assertEquals(1, queries.get());
    } finally { applied.fail(new ReadIndexException("fixture cleanup")); }
  }

  @Test void startupAndMinimumIndexMustCompleteBeforeSuccessAndSingletonNeedsNoPeer() {
    var timer = new Timer(); var executor = new ExecutorQueue(); var ready = new CompletableFuture<Long>();
    var committed = new AtomicLong(7);
    var generation = new Object();
    var rounds = new ReadIndexHeartbeats(() -> new ReadIndexHeartbeats.Context(1, generation, List.of(), 7,
        committed.get(), ack -> true), () -> ready, TIMEOUT, executor, timer, 2, 256, Runnable::run);
    var read = rounds.request(9L, 128); executor.drain(); timer.advance(ReadIndexHeartbeats.BATCH_NANOS);
    assertFalse(read.isDone()); ready.complete(7L); timer.advance(ReadIndexHeartbeats.BATCH_NANOS); executor.drain();
    assertFalse(read.isDone()); committed.set(9); timer.advance(TimeUnit.MILLISECONDS.toNanos(10)); executor.drain();
    assertEquals(9, read.join());
  }

  @Test void decidedButStalledPublicationTimesOutAndReleasesAdmission() {
    var f = new Fixture(1, 128); var read = f.request(null); f.start();
    f.ack(f.peers.getFirst()); f.executor.one(); // work decides, success publisher remains queued
    assertFalse(read.isDone()); f.timeout(); failed(read);
    var later = f.request(null); assertFalse(later.isDone(), "timed out completion releases its slot");
    f.executor.drain(); assertTrue(read.isCompletedExceptionally(), "late publisher cannot resurrect success");
    later.cancel(false); f.executor.drain(); f.timer.advance(0);
  }

  @Test void invalidationAndLaterCapacityFailureKeepEarlierDecidedReadDeadline() {
    for (boolean capacity : List.of(false, true)) {
      var f = new Fixture(2, 256); var first = f.request(null); f.start();
      f.ack(f.peers.getFirst()); f.executor.one(); // leave first success publisher stalled
      if (capacity) {
        f.peers.clear();
        for (int i = 0; i <= ReadIndexHeartbeats.MAX_APPENDERS; i++) f.peers.add(new Peer("n" + i));
        var second = f.request(null);
        f.executor.tasks.removeLast().run(); // readiness work, keep earlier publisher queued
        f.timer.advance(ReadIndexHeartbeats.BATCH_NANOS);
        f.executor.tasks.removeLast().run(); // capacity failure
        f.timer.advance(0); failed(second);
      } else f.rounds.invalidate();
      f.timeout(); failed(first);
      var next = f.request(null); assertFalse(next.isDone(), "decided waiter slot was released");
      next.cancel(false); f.executor.drain(); f.timer.advance(0);
      assertTrue(first.isCompletedExceptionally());
    }
  }

  @Test void delayedPublisherChecksDeadlineEvenBeforeTimeoutCallbackRuns() {
    var f = new Fixture(1, 128); var read = f.request(null); f.start();
    f.ack(f.peers.getFirst()); f.executor.one();
    f.timer.now += TIMEOUT; // timer thread itself delayed
    f.executor.drain(); failed(read);
    var next = f.request(null); assertFalse(next.isDone()); next.cancel(false);
  }

  @Test void cancellationTimeoutStopAndRejectedExecutionConserveWaiterBudgets() {
    var f = new Fixture(1, 128);
    var queued = f.request(null); assertTrue(queued.cancel(false));
    var active = f.request(null); f.start(); assertTrue(active.cancel(false));
    var decided = f.request(null); f.start(); f.ack(f.peers.getFirst()); f.executor.one();
    assertTrue(decided.cancel(false)); f.executor.drain();
    var timeout = f.request(null); f.start(); f.timeout(); failed(timeout);
    var stopped = f.request(null); f.start(); f.rounds.failListeners(new ReadIndexException("stepdown"));
    f.timer.advance(0); failed(stopped); failed(f.request(null));
    var rejected = new Fixture(1, 128); rejected.executor.reject = true;
    var failure = rejected.request(null); rejected.timer.advance(0); failed(failure);
  }

  @Test void shutdownAndSuccessPublicationRejectionCannotResurrectDecidedReads() {
    for (boolean reject : List.of(false, true)) {
      var f = new Fixture(1, 128); var read = f.request(null); f.start();
      f.ack(f.peers.getFirst());
      if (reject) f.executor.reject = true;
      f.executor.one(); // decides; publication is queued or rejected
      if (!reject) f.rounds.failListeners(new ReadIndexException("stepdown after decision"));
      f.timer.advance(0); failed(read);
      f.executor.reject = false; f.executor.drain();
      assertTrue(read.isCompletedExceptionally());
      if (reject) {
        var next = f.request(null); assertFalse(next.isDone()); next.cancel(false);
      } else failed(f.request(null));
    }
  }

  @Test void blockedFailureObserverCannotOccupyDeadlineThreadsOrAnotherPublication() throws Exception {
    var entered = new CountDownLatch(1); var release = new CountDownLatch(1); var returned = new CountDownLatch(1);
    var published = new CountDownLatch(2);
    var f = new Fixture(2, 256, task -> Thread.startVirtualThread(() -> {
      try { task.run(); } finally { published.countDown(); }
    }));
    var first = f.request(null);
    first.whenComplete((index, failure) -> {
      entered.countDown();
      try { assertTrue(release.await(5, TimeUnit.SECONDS)); }
      catch (InterruptedException interrupted) { throw new AssertionError(interrupted); }
      finally { returned.countDown(); }
    });
    try {
      f.start(); f.timeout(); assertTrue(entered.await(5, TimeUnit.SECONDS));
      var second = f.request(null); f.start(); f.timeout();
      var exception = assertThrows(ExecutionException.class, () -> second.get(5, TimeUnit.SECONDS));
      assertInstanceOf(ReadIndexException.class, exception.getCause());
    } finally {
      release.countDown(); assertTrue(returned.await(5, TimeUnit.SECONDS));
      assertTrue(published.await(5, TimeUnit.SECONDS));
    }
  }

  @Test void rejectedFailurePublisherFallsBackWithoutLosingFutureOrDispatchOwnership() throws Exception {
    var f = new Fixture(2, 256, task -> { throw new RejectedExecutionException("publisher stopped"); });
    var first = f.request(null); var second = f.request(null); f.start(); f.timeout();
    for (var read : List.of(first, second)) {
      var failure = assertThrows(ExecutionException.class, () -> read.get(5, TimeUnit.SECONDS));
      assertInstanceOf(ReadIndexException.class, failure.getCause());
    }
    var later = f.request(null); f.start(); f.timeout();
    assertInstanceOf(ReadIndexException.class, assertThrows(ExecutionException.class,
        () -> later.get(5, TimeUnit.SECONDS)).getCause());
  }

  @Test void unpublishedFailuresConsumeNodeBudgetUntilPublicationActuallyReturns() {
    var publisher = new ExecutorQueue();
    var fixtures = new ArrayList<Fixture>();
    var futures = new ArrayList<CompletableFuture<Long>>();
    // No real threads: hold detached failure publishers queued and exhaust the shared node budget.
    for (int group = 0; group < ReadIndexHeartbeats.MAX_NODE_PUBLICATIONS / ReadIndexHeartbeats.MAX_WAITERS; group++) {
      var f = new Fixture(ReadIndexHeartbeats.MAX_WAITERS, ReadIndexHeartbeats.MAX_RETAINED_BYTES, publisher);
      fixtures.add(f);
      for (int i = 0; i < ReadIndexHeartbeats.MAX_WAITERS; i++) futures.add(f.request(null));
      f.timeout();
    }
    try {
      var extra = new Fixture(); failed(extra.request(null));
      assertTrue(futures.stream().noneMatch(CompletableFuture::isDone));
    } finally { publisher.drain(); fixtures.forEach(f -> f.executor.drain()); }
    assertTrue(futures.stream().allMatch(CompletableFuture::isCompletedExceptionally));
    var fresh = new Fixture(); var read = fresh.request(null); assertFalse(read.isDone()); read.cancel(false);
  }

  @Test void appenderCapacityFailureIsRecoverableAfterTopologyShrinks() {
    var f = new Fixture(1, 128); f.peers.clear();
    for (int i = 0; i <= ReadIndexHeartbeats.MAX_APPENDERS; i++) f.peers.add(new Peer("n" + i));
    var oversized = f.request(null); f.start(); f.timer.advance(0); failed(oversized);
    f.peers.clear(); f.majority = ack -> true;
    var next = f.request(null); f.start(); assertEquals(7, next.join());
  }
}
