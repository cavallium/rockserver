package org.apache.ratis.server.impl;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.*;
import org.apache.ratis.statemachine.*;
import org.apache.ratis.statemachine.impl.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real no-socket division/log/updater; native A and GROUP S are deliberately different. */
class RecoveryCoveredPrefixTest {
  @TempDir Path root;
  static final TermIndex S = TermIndex.valueOf(1, 1), A = TermIndex.valueOf(1, 4), CFG = TermIndex.valueOf(1, 3);
  static final RaftConfigurationProto CONFIGURATION = RaftConfigurationProto.newBuilder()
      .addAllPeers(RecoveryEligibilityTest.PEERS.stream().map(RaftPeer::getRaftPeerProto).toList()).build();
  static final class NativeMachine extends BaseStateMachine implements RecoveryEligibilityStateMachine {
    final Gate gate = new Gate();
    final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    final TermIndex nativeCut, initialSnapshot;
    final List<Long> effects = new CopyOnWriteArrayList<>();
    final AtomicInteger hydration = new AtomicInteger(), configurations = new AtomicInteger();
    final CountDownLatch tail = new CountDownLatch(1);
    SnapshotInfo snapshot;
    NativeMachine(TermIndex nativeCut) { this(nativeCut, S); }
    NativeMachine(TermIndex nativeCut, TermIndex initialSnapshot) {
      this.nativeCut = nativeCut; this.initialSnapshot = initialSnapshot; gate.deferDiscovery();
    }
    public Gate recoveryEligibilityGate() { return gate; }
    public StateMachineStorage getStateMachineStorage() { return storage; }
    public SnapshotInfo getLatestSnapshot() { return snapshot; }
    public void initialize(RaftServer server, RaftGroupId group, RaftStorage raftStorage) throws IOException {
      super.initialize(server, group, raftStorage); storage.init(raftStorage);
      checkpoint(initialSnapshot);
      setLastAppliedTermIndex(nativeCut);
      getLifeCycle().transition(LifeCycle.State.STARTING); getLifeCycle().transition(LifeCycle.State.RUNNING);
    }
    void checkpoint(TermIndex cut) throws IOException {
      Path file = storage.getSnapshotFile(cut.getTerm(), cut.getIndex()).toPath();
      if (!Files.exists(file)) {
        Files.write(file, new byte[]{7});
        try (var channel = java.nio.channels.FileChannel.open(file, StandardOpenOption.WRITE)) { channel.force(true); }
      }
      final org.apache.ratis.io.MD5Hash checksum;
      try { checksum = org.apache.ratis.io.MD5Hash.newInstance(java.security.MessageDigest.getInstance("MD5").digest(Files.readAllBytes(file))); }
      catch (java.security.NoSuchAlgorithmException unavailable) { throw new IOException(unavailable); }
      snapshot = new SingleFileSnapshotInfo(new FileInfo(file, checksum), cut.getTerm(), cut.getIndex());
    }
    public void pause() {
      getLifeCycle().transition(LifeCycle.State.PAUSING); getLifeCycle().transition(LifeCycle.State.PAUSED);
    }
    public void reinitialize() throws IOException {
      snapshot = storage.loadLatestSnapshot();
      if (snapshot == null || !snapshot.getTermIndex().equals(nativeCut)
          || !Arrays.equals(new byte[]{7}, Files.readAllBytes(snapshot.getFiles().getFirst().getPath())))
        throw new IOException("Fallback checkpoint does not match actual native cut");
      setLastAppliedTermIndex(nativeCut);
      getLifeCycle().transition(LifeCycle.State.STARTING); getLifeCycle().transition(LifeCycle.State.RUNNING);
    }
    @Override public DataApi data() {
      return new DataApi() {
        public CompletableFuture<ByteString> read(LogEntryProto entry) {
          hydration.incrementAndGet(); throw new AssertionError("Covered external data was hydrated");
        }
      };
    }
    public CompletableFuture<Message> applyTransaction(TransactionContext transaction) {
      long index = transaction.getLogEntry().getIndex();
      effects.add(index); updateLastAppliedTermIndex(transaction.getLogEntry().getTerm(), index);
      tail.countDown(); return CompletableFuture.completedFuture(Message.valueOf("applied"));
    }
    public void notifyConfigurationChanged(long term, long index, RaftConfigurationProto configuration) {
      configurations.incrementAndGet(); throw new AssertionError("Covered configuration was replayed");
    }
    public void notifyTermIndexUpdated(long term, long index) {
      if (index <= nativeCut.getIndex()) throw new AssertionError("Covered native prefix was replayed");
      super.notifyTermIndexUpdated(term, index);
    }
  }
  RecoveryEligibilityTest.Fixture open(Path path, NativeMachine machine) throws Exception {
    return new RecoveryEligibilityTest().new Fixture(path, machine);
  }
  static LogEntryProto data(long index, boolean external) {
    var body = StateMachineLogEntryProto.newBuilder()
        .setClientId(ClientId.valueOf(new UUID(81, 200)).toByteString()).setCallId(index)
        .setLogData(ByteString.copyFromUtf8("data-" + index));
    if (external) body.setStateMachineEntry(StateMachineEntryProto.newBuilder().setStateMachineData(ByteString.copyFromUtf8("external")));
    return LogEntryProto.newBuilder().setTerm(1).setIndex(index).setStateMachineLogEntry(body).build();
  }
  void seed(Path path)throws Exception{seed(path,true);}
  void seed(Path path,boolean loggedConfiguration) throws Exception {
    try (var fixture = open(path, new NativeMachine(S))) {
      fixture.server.changeToFollowerAndPersistMetadata(1, false, "retained covered log").join();
      RecoveryEligibilityTest.append(fixture.server, List.of(data(2, false),
          loggedConfiguration ? LogEntryProto.newBuilder().setTerm(1).setIndex(3).setConfigurationEntry(CONFIGURATION).build() : data(3,false),
          data(4, true), data(5, false), LogEntryProto.newBuilder().setTerm(1).setIndex(6)
              .setMetadataEntry(MetadataProto.newBuilder().setCommitIndex(5)).build()));
    }
  }
  @Test void coveredNativePrefixUsesRealHeadersAndHoldsTailUntilActualRelease() throws Exception {
    Path path = root.resolve("covered"); seed(path); var machine = new NativeMachine(A);
    try (var fixture = open(path, machine)) {
      var updater = fixture.server.getState().getStateMachineUpdater();
      assertEquals(S.getIndex(), updater.getLastAppliedIndex());
      assertTrue(fixture.server.getRaftLog().getLastCommittedIndex() >= 5);
      var permit = RockserverRaftServers.coverLocalPrefix(fixture.server, machine, A, CFG, CONFIGURATION).orElseThrow();
      var cancelled = permit.drained(); cancelled.cancel(false);
      assertFalse(permit.drained().isDone()); assertEquals(S.getIndex(), updater.getLastAppliedIndex());
      permit.start().get(5, TimeUnit.SECONDS);
      assertEquals(A.getIndex(), updater.getLastAppliedIndex()); assertEquals(A, machine.getLastAppliedTermIndex());
      assertEquals(S, machine.getLatestSnapshot().getTermIndex()); assertEquals(S.getIndex(), fixture.server.getRaftLog().getSnapshotIndex());
      assertFalse(Files.exists(machine.storage.getSnapshotFile(A.getTerm(), A.getIndex()).toPath()));
      assertTrue(machine.effects.isEmpty()); assertEquals(0, machine.hydration.get()); assertEquals(0, machine.configurations.get());
      assertTrue(machine.gate.discoveryPending()); assertFalse(fixture.vote(false).getServerReply().getSuccess());
      assertFalse(machine.tail.await(150, TimeUnit.MILLISECONDS));
      assertTrue(machine.gate.settleDiscovery(machine.gate.revision(), RecoveryEligibilityStateMachine.Evidence.recovered(S, A, S,
          CFG.getTerm(), CFG.getIndex(), CONFIGURATION)));
      assertFalse(machine.tail.await(150, TimeUnit.MILLISECONDS)); // Published role alone cannot release the owner.
      permit.release().get(5, TimeUnit.SECONDS);
      assertTrue(machine.tail.await(5, TimeUnit.SECONDS)); assertEquals(List.of(5L), machine.effects);
      assertEquals(0, machine.hydration.get()); assertEquals(0, machine.configurations.get());
    }
  }
  @Test void nativeBootstrapConfigurationReusesTheRetainedFloorWithoutAConfigRewrite()throws Exception {
    Path path=root.resolve("bootstrap");seed(path,false);var machine=new NativeMachine(A);
    try(var fixture=open(path,machine)) {
      var initial=TermIndex.valueOf(0,-1);var permit=RockserverRaftServers.coverLocalPrefix(fixture.server,machine,A,initial,CONFIGURATION).orElseThrow();
      permit.start().get(5,TimeUnit.SECONDS);assertTrue(machine.effects.isEmpty());assertEquals(A.getIndex(),fixture.server.getState().getStateMachineUpdater().getLastAppliedIndex());assertEquals(S.getIndex(),fixture.server.getRaftLog().getSnapshotIndex());
      assertTrue(machine.gate.settleDiscovery(machine.gate.revision(),RecoveryEligibilityStateMachine.Evidence.recovered(S,A,S,0,-1,CONFIGURATION)));permit.release().get(5,TimeUnit.SECONDS);assertTrue(machine.tail.await(5,TimeUnit.SECONDS));assertEquals(List.of(5L),machine.effects);assertEquals(0,machine.configurations.get());assertEquals(0,machine.hydration.get());
    }
  }
  @Test void missingCommitFallsBackAndContradictoryConfigurationFailsBeforeMutation() throws Exception {
    Path path = root.resolve("refusal"); seed(path); var machine = new NativeMachine(A);
    try (var fixture = open(path, machine)) {
      var updater = fixture.server.getState().getStateMachineUpdater();
      var wrong = CONFIGURATION.toBuilder().clearPeers().build();
      assertThrows(IOException.class, () -> RockserverRaftServers.coverLocalPrefix(fixture.server, machine, A, CFG, wrong));
      assertEquals(S.getIndex(), updater.getLastAppliedIndex()); assertTrue(machine.effects.isEmpty());
      var permit = RockserverRaftServers.coverLocalPrefix(fixture.server, machine, A, CFG, CONFIGURATION).orElseThrow();
      permit.abort().get(5, TimeUnit.SECONDS);
      assertThrows(ExecutionException.class, () -> permit.drained().get(5, TimeUnit.SECONDS));
      assertEquals(S.getIndex(), updater.getLastAppliedIndex()); assertTrue(machine.gate.discoveryPending());
    }
    try (var fixture = open(root.resolve("no-commit"), new NativeMachine(A))) {
      assertTrue(RockserverRaftServers.coverLocalPrefix(fixture.server, fixture.server.getStateMachine(), A, CFG, CONFIGURATION).isEmpty());
      assertEquals(S.getIndex(), fixture.server.getState().getStateMachineUpdater().getLastAppliedIndex());
    }
  }
  @Test void partialAbortPreservesRealCursorAndAllowsWarmCheckpointFallback() throws Exception {
    Path path = root.resolve("partial-abort"); seed(path); var machine = new NativeMachine(A);
    try (var fixture = open(path, machine)) {
      var updater = fixture.server.getState().getStateMachineUpdater();
      var permit = RockserverRaftServers.coverLocalPrefix(fixture.server, machine, A, CFG, CONFIGURATION).orElseThrow();
      var consumerField = StateMachineUpdater.class.getDeclaredField("appliedIndexConsumer"); consumerField.setAccessible(true);
      var original = (java.util.function.LongConsumer)consumerField.get(updater);
      var retired = new CompletableFuture<Void>();
      consumerField.set(updater, (java.util.function.LongConsumer)index -> {
        original.accept(index);
        if (index == S.getIndex() + 1) permit.abort().whenComplete((ignored, failure) -> {
          if (failure == null) retired.complete(null); else retired.completeExceptionally(failure);
        });
      });
      try {
        permit.start(); retired.get(5, TimeUnit.SECONDS);
        assertThrows(ExecutionException.class, () -> permit.drained().get(5, TimeUnit.SECONDS));
        assertEquals(2, updater.getLastAppliedIndex()); assertEquals(S.getIndex(), fixture.server.getRaftLog().getSnapshotIndex());
        assertEquals(A, machine.getLastAppliedTermIndex()); assertTrue(machine.effects.isEmpty()); assertEquals(0, machine.hydration.get());
        assertTrue(RockserverRaftServers.coverLocalPrefix(fixture.server, machine, A, CFG, CONFIGURATION).isEmpty());
        assertEquals(2, updater.getLastAppliedIndex());
        machine.checkpoint(A); machine.pause();
        RockserverRaftServers.reloadLocalCheckpoint(fixture.server, machine, A).get(5, TimeUnit.SECONDS);
        assertEquals(A.getIndex(), updater.getLastAppliedIndex()); assertEquals(A.getIndex(), fixture.server.getRaftLog().getSnapshotIndex());
        assertTrue(machine.gate.discoveryPending()); assertTrue(machine.effects.isEmpty()); assertEquals(0, machine.hydration.get());
      } finally { consumerField.set(updater, original); }
    }
  }
  @Test void missingPhysicalCoveredPrefixFallsBackBeforeHeaderRead() throws Exception {
    Path path = root.resolve("missing-prefix"); var physicalFloor = TermIndex.valueOf(1, 3);
    try (var seed = open(path, new NativeMachine(physicalFloor, physicalFloor))) {
      seed.server.changeToFollowerAndPersistMetadata(1, false, "retained suffix after missing prefix").join();
      RecoveryEligibilityTest.append(seed.server, List.of(data(4, true), data(5, false),
          LogEntryProto.newBuilder().setTerm(1).setIndex(6).setMetadataEntry(MetadataProto.newBuilder().setCommitIndex(5)).build()));
    }
    var machine = new NativeMachine(A);
    try (var fixture = open(path, machine)) {
      var updater = fixture.server.getState().getStateMachineUpdater();
      assertEquals(4, fixture.server.getRaftLog().getStartIndex()); assertEquals(S.getIndex(), fixture.server.getRaftLog().getSnapshotIndex());
      assertTrue(fixture.server.getRaftLog().getLastCommittedIndex() >= A.getIndex());
      assertTrue(RockserverRaftServers.coverLocalPrefix(fixture.server, machine, A, CFG, CONFIGURATION).isEmpty());
      assertEquals(S.getIndex(), updater.getLastAppliedIndex()); assertTrue(machine.gate.discoveryPending());
      assertTrue(machine.effects.isEmpty()); assertEquals(0, machine.hydration.get());
    }
  }
  @Test void shutdownRetiresDormantOwnerWithoutStartingNativeReplay() throws Exception {
    Path path = root.resolve("stop"); seed(path); var machine = new NativeMachine(A);
    try (var fixture = open(path, machine)) {
      var permit = RockserverRaftServers.coverLocalPrefix(fixture.server, machine, A, CFG, CONFIGURATION).orElseThrow();
      fixture.server.close();
      assertThrows(ExecutionException.class, () -> permit.abort().get(5, TimeUnit.SECONDS));
      assertThrows(ExecutionException.class, () -> permit.drained().get(5, TimeUnit.SECONDS));
      assertTrue(machine.effects.isEmpty()); assertTrue(machine.gate.discoveryPending());
    }
  }
}
