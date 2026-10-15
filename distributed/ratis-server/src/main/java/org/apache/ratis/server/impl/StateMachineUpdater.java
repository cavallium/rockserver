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

import org.apache.ratis.conf.RaftProperties;
import org.apache.ratis.metrics.Timekeeper;
import org.apache.ratis.proto.RaftProtos.CommitInfoProto;
import org.apache.ratis.protocol.Message;
import org.apache.ratis.protocol.exceptions.StateMachineException;
import org.apache.ratis.server.RaftServerConfigKeys;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.raftlog.RaftLog;
import org.apache.ratis.proto.RaftProtos.LogEntryProto;
import org.apache.ratis.server.raftlog.RaftLogIOException;
import org.apache.ratis.server.raftlog.RaftLogIndex;
import org.apache.ratis.server.util.ServerStringUtils;
import org.apache.ratis.statemachine.SnapshotInfo;
import org.apache.ratis.statemachine.StateMachine;
import org.apache.ratis.statemachine.SnapshotRetentionPolicy;
import org.apache.ratis.util.*;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Consumer;
import java.util.function.LongConsumer;
import java.util.stream.LongStream;

/**
 * This class tracks the log entries that have been committed in a quorum and
 * applies them to the state machine. We let a separate thread do this work
 * asynchronously so that this will not block normal raft protocol.
 * <p>
 * If the auto log compaction is enabled, the state machine updater thread will
 * trigger a snapshot of the state machine by calling
 * {@link StateMachine#takeSnapshot} when the log size exceeds a limit.
 */
class StateMachineUpdater implements Runnable {
  static final Logger LOG = LoggerFactory.getLogger(StateMachineUpdater.class);

  enum State {
    RUNNING, STOP, RELOAD, EXCEPTION
  }

  private final Consumer<Object> infoIndexChange;
  private final Consumer<Object> debugIndexChange;
  private final String name;

  private final StateMachine stateMachine;
  private final CommittedApplyStateMachine.Admission committedAdmission;
  private final RaftServerImpl server;
  private final RaftLog raftLog;

  private final boolean triggerSnapshotWhenStopEnabled;

  private final boolean triggerSnapshotWhenRemoveEnabled;

  private final Long autoSnapshotThreshold;
  private final boolean purgeUptoSnapshotIndex;

  private final Thread updater;
  private final AwaitForSignal awaitForSignal;

  private final RaftLogIndex appliedIndex;
  private final RaftLogIndex snapshotIndex;
  private final AtomicReference<Long> stopIndex = new AtomicReference<>();
  private volatile State state = State.RUNNING;
  private volatile RockserverRaftServers.CoveredPrefix coveredPrefix;
  private volatile RockserverRaftServers.DiscoveryDrain discoveryDrain;
  private CompletableFuture<Void> selectedApplications=CompletableFuture.completedFuture(null);
  private Throwable discoveryFailure;
  private TermIndex observedReloadCut;
  private CompletableFuture<Void> observedReload;

  synchronized CompletableFuture<Void> observeLocalReload(TermIndex cut) throws IOException {
    if (state != State.RUNNING || stopIndex.get() != null || observedReload != null || coveredPrefix != null || discoveryDrain != null) {
      throw new IOException("State machine updater is busy/stopping");
    }
    observedReloadCut = cut;
    observedReload = new CompletableFuture<>();
    return observedReload.copy();
  }

  synchronized void requireCoveredAvailable() throws IOException {
    if (state != State.RUNNING || stopIndex.get() != null || observedReload != null || coveredPrefix != null || discoveryDrain != null)
      throw new IOException("Updater already owns work or is stopping");
  }
  synchronized void registerCoveredPrefix(RockserverRaftServers.CoveredPrefix permit) throws IOException {
    requireCoveredAvailable(); coveredPrefix = permit;
  }
  synchronized void registerDiscoveryDrain(RockserverRaftServers.DiscoveryDrain drain) throws IOException {
    requireCoveredAvailable(); discoveryDrain = drain;
  }
  boolean holdsDiscoveryDrain(RockserverRaftServers.DiscoveryDrain drain) { return discoveryDrain == drain; }
  private void retireDiscoveryDrain(RockserverRaftServers.DiscoveryDrain drain, Throwable failure) {
    synchronized (this) { if (discoveryDrain != drain) return; discoveryDrain = null; }
    if (failure != null) { drain.gate.deferDiscovery(); drain.drained.completeExceptionally(failure); drain.retired.completeExceptionally(failure); }
    else drain.retired.complete(null);
  }
  /** The sole updater keeps pumping selected native continuations; it never parks on their future. */
  private boolean consumeDiscoveryDrain() throws IOException,InterruptedException {
    var drain=discoveryDrain;if(drain==null)return false;
    if(committedAdmission!=null)committedAdmission.pump();
    if(!selectedApplications.isDone()){awaitForSignal.await(100,TimeUnit.MILLISECONDS);return true;}
    if(discoveryFailure!=null){var failure=discoveryFailure;discoveryFailure=null;retireDiscoveryDrain(drain,failure);state=State.EXCEPTION;failLocalReload(failure);server.close();return true;}
    if(selectedApplications.isCompletedExceptionally()) {
      drain.gate.deferDiscovery();retireDiscoveryDrain(drain,new IOException("Selected application/callback failed during discovery drain"));return true;
    }
    if(stopIndex.get()!=null){drain.gate.deferDiscovery();stop();return true;}
    if(drain.abortRequested){drain.gate.deferDiscovery();drain.drained.completeExceptionally(new IOException("Discovery updater hold aborted"));retireDiscoveryDrain(drain,null);return true;}
    if(drain.releaseRequested) {
      if(!drain.drained.isDone() || drain.drained.isCompletedExceptionally() || !drain.gate.discoveryPending()){drain.gate.deferDiscovery();retireDiscoveryDrain(drain,new IOException("Discovery release lacks actual drain or sticky fence"));}
      else retireDiscoveryDrain(drain,null);return true;
    }
    if(!drain.drained.isDone()) {
      boolean valid;
      synchronized(drain.gate){valid=drain.sameOwner() && drain.eligible() && drain.gate.revocationGeneration()==drain.generation && (drain.role!=RockserverRaftServers.DiscoveryRole.GENESIS || drain.gate.revision()==drain.initialRevision);if(valid)drain.revision=drain.gate.revision();}
      if(!valid){drain.gate.deferDiscovery();retireDiscoveryDrain(drain,new IOException("Discovery owner revoked before updater drain"));return true;}
      drain.drained.complete(null);
    }
    awaitForSignal.await(100,TimeUnit.MILLISECONDS);return true;
  }

  private void retireCoveredPrefix(RockserverRaftServers.CoveredPrefix permit, Throwable failure) {
    synchronized (this) { if (coveredPrefix != permit) return; coveredPrefix = null; }
    if (failure != null) { permit.drained.completeExceptionally(failure); permit.retired.completeExceptionally(failure); }
    else permit.retired.complete(null);
  }
  /** Fixed scalar quantum before ordinary readHead/native/CFG dispatch. */
  private boolean consumeCoveredPrefix() throws IOException, InterruptedException {
    var permit = coveredPrefix;
    if (permit == null) return false;
    if (permit.abortRequested) {
      permit.gate.deferDiscovery();
      synchronized (this) { if (coveredPrefix == permit) coveredPrefix = null; }
      permit.drained.completeExceptionally(new IOException("Covered prefix aborted")); permit.retired.complete(null); return true;
    }
    if (permit.releaseRequested) {
      if (!permit.drained.isDone() || !permit.published()) throw new IOException("Covered prefix release lacks exact published native owner");
      retireCoveredPrefix(permit, null); return true;
    }
    if (permit.drained.isDone() && !permit.gate.discoveryPending() && permit.sameLocalOwner()) {
      awaitForSignal.await(100, TimeUnit.MILLISECONDS); return true;
    }
    if (!permit.sameLocalOwner() || !permit.gate.discoveryPending() || permit.gate.revision() != permit.revision)
      throw new IOException("Covered prefix recovery owner revoked");
    if (!permit.started || appliedIndex.get() == permit.applied.getIndex()) {
      awaitForSignal.await(100, TimeUnit.MILLISECONDS); return true;
    }
    for (int visited = 0; visited < 128 && appliedIndex.get() < permit.applied.getIndex(); visited++) {
      if (stopIndex.get() != null || permit.abortRequested) break;
      if (!permit.sameLocalOwner() || !permit.gate.discoveryPending() || permit.gate.revision() != permit.revision)
        throw new IOException("Covered prefix recovery owner changed during quantum");
      long next = appliedIndex.get() + 1;
      var header = raftLog.getEntries(next, next + 1);
      if (header == null || header.length != 1 || header[0].getIndex() != next
          || header[0].getTerm() < permit.precedingTerm || next == permit.applied.getIndex() && header[0].getTerm() != permit.applied.getTerm())
        throw new IOException("Covered prefix cached header changed");
      permit.precedingTerm = header[0].getTerm();
      if (appliedIndex.incrementAndGet(debugIndexChange) != next) throw new IOException("Covered cursor ownership changed");
      notifyAppliedIndex(next);
    }
    if (appliedIndex.get() == permit.applied.getIndex()) permit.drained.complete(null);
    return true;
  }

  boolean isStopRequested() { return stopIndex.get() != null; }

  synchronized CompletableFuture<Void> localReloadCompletion() throws IOException {
    if (observedReload == null) throw new IOException("No actual local reload");
    return observedReload.copy();
  }

  void finishLocalReload(Throwable failure) {
    final CompletableFuture<Void> completed;
    synchronized (this) {
      completed = observedReload;
      observedReload = null;
      observedReloadCut = null;
    }
    if (completed != null && failure != null) completed.completeExceptionally(failure);
  }

  private void failLocalReload(Throwable failure) {
    final CompletableFuture<Void> completed;
    synchronized (this) { completed = observedReload; }
    if (completed != null) completed.completeExceptionally(failure);
  }


  private final SnapshotRetentionPolicy snapshotRetentionPolicy;

  private final MemoizedSupplier<StateMachineMetrics> stateMachineMetrics;

  private final LongConsumer appliedIndexConsumer;

  private volatile boolean isRemoving;

  StateMachineUpdater(StateMachine stateMachine, RaftServerImpl server,
      ServerState serverState, long lastAppliedIndex, RaftProperties properties, LongConsumer appliedIndexConsumer) {
    this.name = ServerStringUtils.generateUnifiedName(serverState.getMemberId(), getClass());
    this.appliedIndexConsumer = appliedIndexConsumer;
    this.infoIndexChange = s -> LOG.info("{}: {}", name, s);
    this.debugIndexChange = s -> LOG.debug("{}: {}", name, s);

    this.stateMachine = stateMachine;
    this.committedAdmission = stateMachine instanceof CommittedApplyStateMachine admitted
        ? admitted.committedApplyAdmission() : null;
    if (committedAdmission != null) committedAdmission.validateSegmentBytes(
        org.apache.ratis.server.RaftServerConfigKeys.Log.segmentSizeMax(properties).getSize());
    if (committedAdmission != null) committedAdmission.bindWakeup(this::notifyUpdater);
    this.server = server;
    this.raftLog = serverState.getLog();

    this.appliedIndex = new RaftLogIndex("appliedIndex", lastAppliedIndex);
    this.snapshotIndex = new RaftLogIndex("snapshotIndex", lastAppliedIndex);

    this.triggerSnapshotWhenStopEnabled = RaftServerConfigKeys.Snapshot.triggerWhenStopEnabled(properties);
    this.triggerSnapshotWhenRemoveEnabled = RaftServerConfigKeys.Snapshot.triggerWhenRemoveEnabled(properties);
    final boolean autoSnapshot = RaftServerConfigKeys.Snapshot.autoTriggerEnabled(properties);
    this.autoSnapshotThreshold = autoSnapshot? RaftServerConfigKeys.Snapshot.autoTriggerThreshold(properties): null;
    final int numSnapshotFilesRetained = RaftServerConfigKeys.Snapshot.retentionFileNum(properties);
    this.snapshotRetentionPolicy = new SnapshotRetentionPolicy() {
      @Override
      public int getNumSnapshotsRetained() {
        return numSnapshotFilesRetained;
      }
    };
    this.purgeUptoSnapshotIndex = RaftServerConfigKeys.Log.purgeUptoSnapshotIndex(properties);
    updater = Daemon.newBuilder().setName(name).setRunnable(this)
        .setThreadGroup(server.getThreadGroup()).build();
    this.awaitForSignal = new AwaitForSignal(name);
    this.stateMachineMetrics = MemoizedSupplier.valueOf(
        () -> StateMachineMetrics.getStateMachineMetrics(server, appliedIndex, stateMachine));
  }

  void start() {
    //wait for RaftServerImpl and ServerState constructors to complete
    stateMachineMetrics.get();
    updater.start();
    notifyAppliedIndex(appliedIndex.get());
  }

  private void stop() {
    state = State.STOP;
    failLocalReload(new IOException("State machine updater stopped during local reload"));
    try {
      LOG.info("{}: closing {}, lastApplied={}", name,
          JavaUtils.getClassSimpleName(stateMachine.getClass()), stateMachine.getLastAppliedTermIndex());
      closeCommittedMachine(stateMachine, committedAdmission);
      if (stateMachineMetrics.isInitialized()) {
        stateMachineMetrics.get().unregister();
      }
    } catch(Throwable t) {
      LOG.warn(name + ": Failed to close " + JavaUtils.getClassSimpleName(stateMachine.getClass())
          + " " + stateMachine, t);
    } finally {
      var covered = coveredPrefix;
      if (covered != null) retireCoveredPrefix(covered, new IOException("Updater stopped during covered prefix ownership"));
      var creation = discoveryDrain;
      if (creation != null) retireDiscoveryDrain(creation, new IOException("Updater stopped during Discovery hold"));
    }
  }

  /**
   * Stop the updater thread after all the committed transactions
   * have been applied to the state machine.
   */
  void stopAndJoin() throws InterruptedException {
    if (committedAdmission != null) committedAdmission.stop();
    if (state == State.EXCEPTION) {
      stop();
      return;
    }
    if (stopIndex.compareAndSet(null, raftLog.getLastCommittedIndex())) {
      notifyUpdater();
      LOG.info("{}: set stopIndex = {}", this, stopIndex);
    }
    updater.join();
  }

  void reloadStateMachine() {
    state = State.RELOAD;
    if (committedAdmission != null) committedAdmission.wakeup();
    notifyUpdater();
  }

  void notifyUpdater() {
    awaitForSignal.signal();
  }

  @Override
  public String toString() {
    return name;
  }

  @Override
  public void run() {
    CompletableFuture<Void> applyLogFutures = CompletableFuture.completedFuture(null);
    for(; state != State.STOP; ) {
      try {
        if (consumeDiscoveryDrain()) continue;
        if (discoveryDrain==null && stopIndex.get() != null && (committedAdmission != null || coveredPrefix != null || !server.isInitialImageReady())) { stop(); break; }
        if (consumeCoveredPrefix()) continue;
        waitForCommit(applyLogFutures);
        if(discoveryDrain!=null)continue;
        if (discoveryDrain==null && stopIndex.get() != null && (committedAdmission != null || coveredPrefix != null || !server.isInitialImageReady())) { stop(); break; }

        if (state == State.RELOAD) {
          reload();
        }

        applyLogFutures = applyLog(applyLogFutures);
        if (committedAdmission != null) committedAdmission.pump();
        checkAndTakeSnapshot(applyLogFutures);

        if (discoveryDrain==null && shouldStop()) {
          if (committedAdmission == null) applyLogFutures.get();
          stop();
        }
      } catch (Throwable t) {
        if(discoveryDrain!=null && !selectedApplications.isDone()) {discoveryFailure=t;discoveryDrain.gate.deferDiscovery();continue;}
        if (t instanceof InterruptedException && state == State.STOP) {
          Thread.currentThread().interrupt();
          LOG.info("{} was interrupted.  Exiting ...", this);
        } else {
          state = State.EXCEPTION;
          failLocalReload(t);
          LOG.error(this + " caught a Throwable.", t);
          server.close();
        }
      }
    }
  }

  private void waitForCommit(CompletableFuture<?> applyLogFutures) throws InterruptedException, ExecutionException {
    // When a peer starts, the committed is initialized to 0.
    // It will be updated only after the leader contacts other peers.
    // Thus it is possible to have applied > committed initially.
    final long applied = getLastAppliedIndex();
    for(; (applied >= raftLog.getLastCommittedIndex() || !server.isStateMachineProgressReady())
        && state == State.RUNNING && !shouldStop() && stopIndex.get() == null && coveredPrefix == null && discoveryDrain == null; ) {
      if (server.isInitialImageReady() && server.getSnapshotRequestHandler().shouldTriggerTakingSnapshot()) {
        takeSnapshot(applyLogFutures);
      }
      if (awaitForSignal.await(100, TimeUnit.MILLISECONDS)
          && (state != State.RUNNING || server.isStateMachineProgressReady())) {
        return;
      }
    }
  }

  private void reload() throws IOException {
    Preconditions.assertTrue(stateMachine.getLifeCycleState() == LifeCycle.State.PAUSED);

    stateMachine.reinitialize();

    final SnapshotInfo snapshot = stateMachine.getLatestSnapshot();
    Objects.requireNonNull(snapshot, "snapshot == null");
    final long i = snapshot.getIndex();
    snapshotIndex.setUnconditionally(i, infoIndexChange);
    appliedIndex.setUnconditionally(i, infoIndexChange);
    notifyAppliedIndex(i);
    state = State.RUNNING;
    CompletableFuture<Void> completed;
    synchronized (this) {
      if (observedReload != null && (stopIndex.get() != null
          || !(stateMachine instanceof RecoveryEligibilityStateMachine recovery)
          || !recovery.recoveryEligibilityGate().discoveryPending()
          || !snapshot.getTermIndex().equals(observedReloadCut)
          || !observedReloadCut.equals(stateMachine.getLastAppliedTermIndex())
          || stateMachine.getLifeCycleState() != LifeCycle.State.RUNNING
          || appliedIndex.get() != observedReloadCut.getIndex()
          || snapshotIndex.get() != observedReloadCut.getIndex())) {
        throw new IOException("Local checkpoint reload changed its expected cut/lifecycle");
      }
      completed = observedReload;
    }
    if (completed != null) completed.complete(null);
  }

  /** Defer snapshot publication instead of parking the sole native continuation pump. */
  static boolean applicationsReady(CompletableFuture<?> applications, CommittedApplyStateMachine.Admission admission) {
    if (!applications.isDone() && admission != null) admission.pump();
    return admission == null || applications.isDone();
  }
  /** Settle observer waits by closing the SM; its native ownership tokens are settled separately. */
  static void closeCommittedMachine(StateMachine machine, CommittedApplyStateMachine.Admission admission) throws IOException {
    if (admission != null) admission.stop();
    machine.close();
  }

  @FunctionalInterface interface EntryReader { LogEntryProto get(long index) throws IOException; }
  record AdmittedHead(LogEntryProto entry, CommittedApplyStateMachine.Descriptor descriptor,
      CommittedApplyStateMachine.ApplyPermit permit) {}

  /** Exact production two-stage boundary, also exercised without a server or physical log. */
  static AdmittedHead readHead(CommittedApplyStateMachine.Admission admission, EntryReader reader, long index)
      throws IOException, InterruptedException {
    LogEntryProto entry;
    CommittedApplyStateMachine.Descriptor descriptor = null;
    CommittedApplyStateMachine.ApplyPermit permit = null;
    try {
      try (CommittedApplyStateMachine.ReadPermit read = admission.awaitRead(index)) {
        try (var admittedRead = LogResourceScope.admittedRead()) {
          entry = reader.get(index);
        }
        if (entry != null) {
          var decoded = admission.describe(entry);
          descriptor = new CommittedApplyStateMachine.Descriptor(decoded.term(),decoded.index(),decoded.bodyCase(),decoded.lane(),decoded.bytes(),decoded.digest(),read.generation());
          permit = admission.tryDispatch(descriptor);
        }
      }
      if (entry != null && permit == null) {
        entry = null; // Only the scalar descriptor survives the execution wait.
        permit = admission.awaitDispatch(descriptor);
        admission.validate(descriptor);
        try (var admittedRead = LogResourceScope.admittedRead()) {
          entry = reader.get(index);
        }
        var reread = entry == null ? null : admission.describe(entry);
        var checked = reread == null ? null : new CommittedApplyStateMachine.Descriptor(reread.term(),reread.index(),reread.bodyCase(),reread.lane(),reread.bytes(),reread.digest(),descriptor.generation());
        if (checked == null || !descriptor.sameEntry(checked)) {
          permit.close();
          return new AdmittedHead(null, null, null);
        }
      }
      if (permit != null) admission.offer(descriptor, permit);
      return new AdmittedHead(entry, descriptor, permit);
    } catch (IOException | InterruptedException | RuntimeException | Error failure) {
      if (permit != null && !permit.claimed()) permit.close();
      throw failure;
    }
  }

  private CompletableFuture<Void> applyLog(CompletableFuture<Void> applyLogFutures) throws RaftLogIOException, InterruptedException {
    final long committed = raftLog.getLastCommittedIndex();
    for(long applied; (applied = getLastAppliedIndex()) < committed && state == State.RUNNING
        && server.isStateMachineProgressReady() && !shouldStop() && discoveryDrain == null; ) {
      final long nextIndex = applied + 1;
      // This is the selection linearization point shared with hold registration. No IO under it.
      synchronized(this){if(discoveryDrain!=null)return applyLogFutures;}
      if (server.isGenesisMetadataOnly()) {
        final var headers = raftLog.getEntries(nextIndex, nextIndex + 1);
        if (headers == null || headers.length != 1 || headers[0] == null || headers[0].getIndex() != nextIndex)
          throw new RaftLogIOException("Creation metadata header is missing or contradictory");
        final var body = headers[0].getLogEntryBodyCase();
        if (body != LogEntryProto.LogEntryBodyCase.CONFIGURATIONENTRY && body != LogEntryProto.LogEntryBodyCase.METADATAENTRY) {
          // No USER body hydration, admission or application while creation authority is restricted.
          awaitForSignal.await(100, TimeUnit.MILLISECONDS);
          return applyLogFutures;
        }
      }
      LogEntryProto next;
      CommittedApplyStateMachine.ApplyPermit permit = null;
      CommittedApplyStateMachine.Descriptor descriptor = null;
      if (committedAdmission == null) {
        next = raftLog.get(nextIndex);
      } else {
        try {
          final AdmittedHead head = readHead(committedAdmission, raftLog::get, nextIndex);
          next = head.entry(); descriptor = head.descriptor(); permit = head.permit();
        } catch (InterruptedException interrupted) {
          Thread.currentThread().interrupt();
          if (permit != null) permit.close();
          break;
        } catch (CommittedApplyStateMachine.Retry retry) {
          if (permit != null) permit.close();
          return applyLogFutures;
        } catch (CommittedApplyStateMachine.Stopped stopped) {
          if (permit != null) permit.close();
          stop();
          return applyLogFutures;
        } catch (IOException stoppedOrFailed) {
          if (permit != null) permit.close();
          throw new RaftLogIOException(stoppedOrFailed);
        }
      }
      if (next != null) {
        if (permit != null) {
          try { if (state != State.RUNNING) throw new CommittedApplyStateMachine.Retry(); committedAdmission.validate(descriptor); }
          catch (CommittedApplyStateMachine.Retry | CommittedApplyStateMachine.Stopped retry) { committedAdmission.withdraw(descriptor, permit); permit.close(); return applyLogFutures; }
          catch (IOException invalid) { committedAdmission.withdraw(descriptor, permit); permit.close(); throw new RaftLogIOException(invalid); }
        }
        if (LOG.isTraceEnabled()) {
          LOG.trace("{}: applying nextIndex={}, nextLog={}", this, nextIndex, LogProtoUtils.toLogEntryString(next));
        } else {
          LOG.debug("{}: applying nextIndex={}", this, nextIndex);
        }

        final CompletableFuture<Message> f;
        try { f = server.applyLogToStateMachine(next); }
        finally {
          if (permit != null) {
            committedAdmission.withdraw(descriptor, permit);
            if (!permit.claimed()) permit.close();
          }
        }
        if (permit != null && !permit.claimed() && next.hasStateMachineLogEntry()) {
          try { committedAdmission.validate(descriptor); }
          catch (CommittedApplyStateMachine.Retry | CommittedApplyStateMachine.Stopped retry) { return applyLogFutures; }
          catch (IOException invalid) { throw new RaftLogIOException(invalid); }
        }
        final long incremented = appliedIndex.incrementAndGet(debugIndexChange);
        Preconditions.assertTrue(incremented == nextIndex);
        if (f != null) {
          var delivered=f.thenAccept(m -> notifyAppliedIndex(incremented));
          selectedApplications=CompletableFuture.allOf(selectedApplications,delivered);
          var exceptionHandledFuture=delivered.exceptionally(ex -> {
            LOG.error("Exception while {}: applying txn index={}",this,nextIndex,ex);return null;
          });
          applyLogFutures=applyLogFutures.thenCombine(exceptionHandledFuture,(v,message)->null);
        } else {
          notifyAppliedIndex(incremented);
        }
      } else {
        LOG.debug("{}: logEntry {} is null. There may be snapshot to load. state:{}",
            this, nextIndex, state);
        break;
      }
    }
    return applyLogFutures;
  }

  private void checkAndTakeSnapshot(CompletableFuture<?> futures)
      throws ExecutionException, InterruptedException {
    // check if need to trigger a snapshot
    if (discoveryDrain==null && shouldTakeSnapshot()) {
      takeSnapshot(futures);
    }
  }

  private void takeSnapshot(CompletableFuture<?> applyLogFutures) throws ExecutionException, InterruptedException {
    final long i;
    if (!applicationsReady(applyLogFutures, committedAdmission)) return;
    applyLogFutures.get();
    try {
      try(UncheckedAutoCloseable ignored = Timekeeper.start(stateMachineMetrics.get().getTakeSnapshotTimer())) {
        i = stateMachine.takeSnapshot();
      }
      server.getSnapshotRequestHandler().completeTakingSnapshot(i);

      final long lastAppliedIndex = getLastAppliedIndex();
      if (i > lastAppliedIndex) {
        throw new StateMachineException(
            "Bug in StateMachine: snapshot index = " + i + " > appliedIndex = " + lastAppliedIndex
            + "; StateMachine class=" +  stateMachine.getClass().getName() + ", stateMachine=" + stateMachine);
      }
      stateMachine.getStateMachineStorage().cleanupOldSnapshots(snapshotRetentionPolicy);
    } catch (IOException e) {
      LOG.error(name + ": Failed to take snapshot", e);
      return;
    }

    if (i >= 0) {
      LOG.info("{}: Took a snapshot at index {}", name, i);
      snapshotIndex.updateIncreasingly(i, infoIndexChange);

      final long purgeIndex;
      if (purgeUptoSnapshotIndex) {
        // We can purge up to snapshot index even if all the peers do not have
        // commitIndex up to this snapshot index.
        purgeIndex = i;
      } else {
        final LongStream commitIndexStream = server.getCommitInfos().stream().mapToLong(
            CommitInfoProto::getCommitIndex);
        purgeIndex = LongStream.concat(LongStream.of(i), commitIndexStream).min().orElse(i);
      }
      raftLog.purge(purgeIndex);
    }
  }

  private boolean shouldStop() {
    return Optional.ofNullable(stopIndex.get()).filter(i -> i <= getLastAppliedIndex()).isPresent();
  }

  private boolean shouldTakeSnapshot() {
    if (state == State.RUNNING && server.getSnapshotRequestHandler().shouldTriggerTakingSnapshot()) {
      return true;
    }
    if (autoSnapshotThreshold == null) {
      return false;
    } else if (shouldStop()) {
      return shouldTakeSnapshotAtStop() && getLastAppliedIndex() - snapshotIndex.get() > 0;
    }
    return state == State.RUNNING &&
        getStateMachineLastAppliedIndex() - snapshotIndex.get() >= autoSnapshotThreshold;
  }

  /**
   * In view of the three variables triggerSnapshotWhenStopEnabled, triggerSnapshotWhenRemoveEnabled and isRemoving,
   * we can draw the following 8 combination:
   * true true true => true
   * true true false => true
   * true false true => false
   * true false false => true
   * false true true => true
   * false true false =>  false
   * false false true => false
   * false false false => false
   * @return result
   */
  private boolean shouldTakeSnapshotAtStop() {
    return isRemoving ? triggerSnapshotWhenRemoveEnabled : triggerSnapshotWhenStopEnabled;
  }

  void setRemoving() {
    this.isRemoving = true;
  }

  long getLastAppliedIndex() {
    return appliedIndex.get();
  }

  private void notifyAppliedIndex(long index) {
    appliedIndexConsumer.accept(index);
  }

  long getStateMachineLastAppliedIndex() {
    return Optional.ofNullable(stateMachine.getLastAppliedTermIndex())
        .map(TermIndex::getIndex)
        .orElse(RaftLog.INVALID_LOG_INDEX);
  }
}
