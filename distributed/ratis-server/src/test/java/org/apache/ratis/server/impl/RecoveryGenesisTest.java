package org.apache.ratis.server.impl;

import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.protocol.*;
import org.apache.ratis.server.RaftServer;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.storage.RaftStorage;
import org.apache.ratis.server.storage.FileInfo;
import org.apache.ratis.statemachine.*;
import org.apache.ratis.statemachine.impl.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.LifeCycle;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Actual original division progress with no invented native A or GROUP image. */
class RecoveryGenesisTest {
  @TempDir Path root;
  static class GenesisMachine extends BaseStateMachine implements RecoveryEligibilityStateMachine {
    final Gate gate = new Gate();
    final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    final List<Long> applied = new CopyOnWriteArrayList<>();
    final List<RaftConfigurationProto> configurations = new CopyOnWriteArrayList<>();
    final CountDownLatch configured = new CountDownLatch(1);
    GenesisMachine() { gate.deferDiscovery(); }
    public Gate recoveryEligibilityGate() { return gate; }
    public StateMachineStorage getStateMachineStorage() { return storage; }
    public SnapshotInfo getLatestSnapshot() { return null; }
    public void initialize(RaftServer server, RaftGroupId group, RaftStorage raftStorage) throws IOException {
      super.initialize(server, group, raftStorage); storage.init(raftStorage);
      getLifeCycle().transition(LifeCycle.State.STARTING); getLifeCycle().transition(LifeCycle.State.RUNNING);
    }
    public void notifyTermIndexUpdated(long term, long index) {
      super.notifyTermIndexUpdated(term, index); applied.add(index);
      if (index == 1) configured.countDown();
    }
    public void notifyConfigurationChanged(long term, long index, RaftConfigurationProto configuration) {
      configurations.add(configuration);
    }
  }
  static final class ColdGenesisMachine extends GenesisMachine {
    final TermIndex nativeApplied, groupCut;
    SnapshotInfo snapshot;
    ColdGenesisMachine(TermIndex nativeApplied, TermIndex groupCut) { this.nativeApplied = nativeApplied; this.groupCut = groupCut; }
    public SnapshotInfo getLatestSnapshot() { return snapshot; }
    public void initialize(RaftServer server, RaftGroupId group, RaftStorage raftStorage) throws IOException {
      super.initialize(server, group, raftStorage);
      if (groupCut != null) {
        Path file = storage.getSnapshotFile(groupCut.getTerm(), groupCut.getIndex()).toPath();
        Files.write(file, new byte[]{17});
        try (var channel = java.nio.channels.FileChannel.open(file, StandardOpenOption.WRITE)) { channel.force(true); }
        final org.apache.ratis.io.MD5Hash hash;
        try { hash = org.apache.ratis.io.MD5Hash.newInstance(java.security.MessageDigest.getInstance("MD5").digest(new byte[]{17})); }
        catch (java.security.NoSuchAlgorithmException unavailable) { throw new IOException(unavailable); }
        snapshot = new SingleFileSnapshotInfo(new FileInfo(file, hash), groupCut.getTerm(), groupCut.getIndex());
      }
      if (nativeApplied != null) setLastAppliedTermIndex(nativeApplied);
    }
    @Override public DataApi data() {
      return new DataApi() { public CompletableFuture<ByteString> read(LogEntryProto entry) {
        throw new AssertionError("Read-only GENESIS qualification hydrated USER data");
      }};
    }
  }
  static LogEntryProto metadata(long index, long committed) {
    return LogEntryProto.newBuilder().setTerm(1).setIndex(index).setMetadataEntry(MetadataProto.newBuilder().setCommitIndex(committed)).build();
  }
  RecoveryEligibilityTest.Fixture open(String name, GenesisMachine machine, boolean chunks) throws Exception {
    return new RecoveryEligibilityTest().new Fixture(root.resolve(name), machine, chunks);
  }
  static void genesis(GenesisMachine machine) {
    assertTrue(machine.gate.settleDiscovery(machine.gate.revision(), RecoveryEligibilityStateMachine.Evidence.genesis()));
  }
  @Test void explicitGenesisEnablesRealOriginalVotingAppendNoopAndConfigurationWithoutBaseline() throws Exception {
    var machine = new GenesisMachine();
    try (var fixture = open("progress", machine, true)) {
      assertFalse(fixture.server.isVotingEligible()); assertFalse(fixture.server.isInitialImageReady());
      genesis(machine);
      var evidence = machine.gate.snapshot();
      assertEquals(RecoveryEligibilityStateMachine.Mode.GENESIS, evidence.mode());
      assertNull(evidence.nativeCut()); assertNull(evidence.imageCut()); assertNull(evidence.snapshotCut()); assertNull(evidence.configuration());
      assertNull(machine.getLatestSnapshot()); assertEquals(-1, machine.getLastAppliedTermIndex().getIndex());
      assertEquals(-1, fixture.server.getRaftLog().getSnapshotIndex());
      assertTrue(fixture.server.isInitialImageReady()); assertTrue(fixture.server.isVotingEligible());
      assertThrows(IOException.class,()->fixture.server.setConfigurationAsync(null));
      assertTrue(fixture.vote(true).getServerReply().getSuccess()); assertTrue(fixture.vote(false).getServerReply().getSuccess());
      var configuration = RecoveryEligibilityTest.configuration(fixture.server);
      // LeaderStateImpl.StartupLogEntry uses a full CONFIGURATIONENTRY as its term-start NOOP.
      var entries = List.of(LogEntryProto.newBuilder().setTerm(2).setIndex(0).setConfigurationEntry(configuration).build(),
          LogEntryProto.newBuilder().setTerm(2).setIndex(1).setMetadataEntry(MetadataProto.newBuilder()).build());
      var request = ServerProtoUtils.toAppendEntriesRequestProto(RaftGroupMemberId.valueOf(RecoveryEligibilityTest.LEADER, RecoveryEligibilityTest.GROUP),
          RecoveryEligibilityTest.SELF, 2, entries, 1, false, null, List.of(), 11, null);
      assertEquals(AppendEntriesReplyProto.AppendResult.SUCCESS, fixture.server.appendEntries(request).getResult());
      assertTrue(machine.configured.await(5, TimeUnit.SECONDS));
      assertEquals(List.of(0L, 1L), machine.applied); assertEquals(List.of(configuration), machine.configurations);
      assertEquals(TermIndex.valueOf(2, 1), machine.getLastAppliedTermIndex());
      assertNull(machine.getLatestSnapshot()); assertNull(machine.gate.snapshot().nativeCut());
      assertEquals(-1, fixture.server.getRaftLog().getSnapshotIndex());
      assertThrows(IllegalStateException.class, machine.gate::snapshotPermit);
      machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.suspendedGenesis());
      assertFalse(fixture.server.isInitialImageReady());assertFalse(fixture.server.isVotingEligible());
      assertThrows(IOException.class,()->fixture.server.setConfigurationAsync(null));
      assertFalse(fixture.vote(true).getServerReply().getSuccess());assertFalse(fixture.vote(false).getServerReply().getSuccess());
      assertEquals(TermIndex.valueOf(2,1),machine.getLastAppliedTermIndex());
      assertThrows(IllegalStateException.class,machine.gate::snapshotPermit);
    }
  }
  @Test void genesisRejectsBothSnapshotReceivePathsBeforeAnyLeaderFileOrConfigurationMutation() throws Exception {
    for (boolean suspended : List.of(false,true)) for (boolean chunks : List.of(true, false)) {
      var machine = new GenesisMachine();
      try (var fixture = open("receive-" + suspended + "-" + chunks, machine, chunks)) {
        genesis(machine);if(suspended)machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.suspendedGenesis());
        var originalMetadata = fixture.server.getRaftStorage().getMetadataFile().getMetadata();
        var originalConfiguration = RecoveryEligibilityTest.configuration(fixture.server);
        long originalNext = fixture.server.getRaftLog().getNextIndex();
        var notification = InstallSnapshotRequestProto.newBuilder().setServerRequest(RecoveryEligibilityTest.request(19)).setLeaderTerm(3)
            .setNotification(InstallSnapshotRequestProto.NotificationProto.newBuilder().setFirstAvailableTermIndex(TermIndex.valueOf(3, 6).toProto())).build();
        Path target = machine.storage.getSnapshotFile(3, 5).toPath();
        String name = fixture.server.getRaftStorage().getStorageDir().getRoot().toPath().relativize(target).toString();
        for (boolean done : List.of(false, true)) {
          var chunk = InstallSnapshotRequestProto.newBuilder().setServerRequest(RecoveryEligibilityTest.request(20)).setLeaderTerm(3)
              .setSnapshotChunk(InstallSnapshotRequestProto.SnapshotChunkProto.newBuilder().setRequestId("genesis-image")
                  .setRequestIndex(0).setTermIndex(TermIndex.valueOf(3, 5).toProto()).setDone(done)
                  .addFileChunks(FileChunkProto.newBuilder().setFilename(name).setOffset(0).setDone(done).setData(ByteString.copyFromUtf8("image")))).build();
          assertThrows(IOException.class, () -> fixture.server.installSnapshot(chunk));
        }
        assertThrows(IOException.class, () -> fixture.server.installSnapshot(notification));
        assertEquals(originalMetadata, fixture.server.getRaftStorage().getMetadataFile().getMetadata());
        assertEquals(originalConfiguration, RecoveryEligibilityTest.configuration(fixture.server));
        assertNull(fixture.server.getState().getLeaderId()); assertEquals(originalNext, fixture.server.getRaftLog().getNextIndex());
        assertFalse(Files.exists(target)); assertNull(machine.getLatestSnapshot()); assertTrue(machine.applied.isEmpty());
        assertTrue(machine.configurations.isEmpty()); assertEquals(-1, machine.getLastAppliedTermIndex().getIndex());
        assertEquals(suspended?RecoveryEligibilityStateMachine.Mode.GENESIS_SUSPENDED:RecoveryEligibilityStateMachine.Mode.GENESIS, machine.gate.snapshot().mode());
      }
    }
  }
  @Test void coldGenesisQualifierAcceptsMetadataConfigurationPrefixWithoutMovingActualCursor() throws Exception {
    var group = TermIndex.valueOf(1, 1); var nativeA = TermIndex.valueOf(1, 4); var cfg = TermIndex.valueOf(1, 3);
    RaftConfigurationProto configuration;
    try (var seed = open("cold-prefix", new ColdGenesisMachine(group, group), true)) {
      configuration = RecoveryEligibilityTest.configuration(seed.server);
      seed.server.changeToFollowerAndPersistMetadata(1, false, "cold original metadata prefix").join();
      RecoveryEligibilityTest.append(seed.server, List.of(metadata(2, 1), LogEntryProto.newBuilder().setTerm(1).setIndex(3)
          .setConfigurationEntry(configuration).build(), metadata(4, 1), metadata(5, 4), metadata(6, 5)));
    }
    var machine = new ColdGenesisMachine(nativeA, group);
    try (var fixture = open("cold-prefix", machine, true)) {
      var updater = fixture.server.getState().getStateMachineUpdater();
      assertEquals(1, updater.getLastAppliedIndex()); assertTrue(machine.gate.discoveryPending());
      assertTrue(RockserverRaftServers.hasGenesisPrefix(fixture.server, machine, nativeA, cfg, configuration));
      assertEquals(1, updater.getLastAppliedIndex()); assertEquals(nativeA, machine.getLastAppliedTermIndex());
      assertEquals(group, machine.getLatestSnapshot().getTermIndex()); assertEquals(1, fixture.server.getRaftLog().getSnapshotIndex());
      assertTrue(machine.applied.isEmpty()); assertTrue(machine.configurations.isEmpty()); assertTrue(machine.gate.discoveryPending());
      assertThrows(IOException.class, () -> RockserverRaftServers.hasGenesisPrefix(fixture.server, machine, nativeA, cfg,
          configuration.toBuilder().clearPeers().build()));
    }
    var empty = new ColdGenesisMachine(null, null);
    try (var fixture = open("cold-empty", empty, true)) {
      assertTrue(RockserverRaftServers.hasGenesisPrefix(fixture.server, empty, null, TermIndex.valueOf(0, -1),
          RecoveryEligibilityTest.configuration(fixture.server)));
      assertEquals(-1, fixture.server.getState().getStateMachineUpdater().getLastAppliedIndex());
      assertNull(empty.getLatestSnapshot()); assertTrue(empty.gate.discoveryPending());
    }
  }
  @Test void coldGenesisQualifierRejectsUserTailAndMissingOrTruncatedProofWithoutEffects() throws Exception {
    var group = TermIndex.valueOf(1, 1); var nativeA = TermIndex.valueOf(1, 2); var bootstrap = TermIndex.valueOf(0, -1);
    RaftConfigurationProto configuration;
    try (var seed = open("cold-user", new ColdGenesisMachine(group, group), true)) {
      configuration = RecoveryEligibilityTest.configuration(seed.server);
      seed.server.changeToFollowerAndPersistMetadata(1, false, "original prefix followed by USER").join();
      var user = LogEntryProto.newBuilder().setTerm(1).setIndex(3).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder()
          .setClientId(ClientId.valueOf(new UUID(81, 300)).toByteString()).setCallId(3)
          .setLogData(ByteString.copyFromUtf8("user"))
          .setStateMachineEntry(StateMachineEntryProto.newBuilder().setStateMachineData(ByteString.copyFromUtf8("external")))).build();
      RecoveryEligibilityTest.append(seed.server, List.of(metadata(2, 1), user, metadata(4, 3)));
    }
    var userMachine = new ColdGenesisMachine(nativeA, group);
    try (var fixture = open("cold-user", userMachine, true)) {
      assertFalse(RockserverRaftServers.hasGenesisPrefix(fixture.server, userMachine, nativeA, bootstrap, configuration));
      assertEquals(1, fixture.server.getState().getStateMachineUpdater().getLastAppliedIndex());
      assertTrue(userMachine.applied.isEmpty()); assertTrue(userMachine.gate.discoveryPending());
    }
    var absent = new ColdGenesisMachine(nativeA, group);
    try (var fixture = open("cold-absent", absent, true)) {
      assertFalse(RockserverRaftServers.hasGenesisPrefix(fixture.server, absent, nativeA, bootstrap, configuration));
      assertEquals(1, fixture.server.getState().getStateMachineUpdater().getLastAppliedIndex());
    }
    var laterGroup = TermIndex.valueOf(1, 3);
    try (var seed = open("cold-truncated", new ColdGenesisMachine(laterGroup, laterGroup), true)) {
      seed.server.changeToFollowerAndPersistMetadata(1, false, "retained metadata suffix").join();
      RecoveryEligibilityTest.append(seed.server, List.of(metadata(4, 3), metadata(5, 4)));
    }
    var truncated = new ColdGenesisMachine(TermIndex.valueOf(1, 4), group);
    try (var fixture = open("cold-truncated", truncated, true)) {
      assertEquals(4, fixture.server.getRaftLog().getStartIndex());
      assertFalse(RockserverRaftServers.hasGenesisPrefix(fixture.server, truncated, TermIndex.valueOf(1, 4), bootstrap, configuration));
      assertEquals(1, fixture.server.getState().getStateMachineUpdater().getLastAppliedIndex());
      assertTrue(truncated.applied.isEmpty()); assertTrue(truncated.gate.discoveryPending());
    }
  }
  @Test void suspendedGenesisDrainsCommittedMetadataButDoesNotHydrateUserHead() throws Exception {
    RaftConfigurationProto configuration;
    try(var seed=open("suspended-tail",new ColdGenesisMachine(null,null),true)) {
      configuration=RecoveryEligibilityTest.configuration(seed.server);
      seed.server.changeToFollowerAndPersistMetadata(1,false,"retained creation tail").join();
      var user=LogEntryProto.newBuilder().setTerm(1).setIndex(3).setStateMachineLogEntry(StateMachineLogEntryProto.newBuilder()
          .setClientId(ClientId.randomId().toByteString()).setCallId(3).setLogData(ByteString.copyFromUtf8("user"))
          .setStateMachineEntry(StateMachineEntryProto.newBuilder().setStateMachineData(ByteString.copyFromUtf8("external")))).build();
      RecoveryEligibilityTest.append(seed.server,List.of(LogEntryProto.newBuilder().setTerm(1).setIndex(0).setConfigurationEntry(configuration).build(),metadata(1,0),metadata(2,1),user,metadata(4,3)));
    }
    var machine=new ColdGenesisMachine(null,null);
    try(var fixture=open("suspended-tail",machine,true)) {
      assertTrue(machine.gate.settleDiscovery(machine.gate.revision(),RecoveryEligibilityStateMachine.Evidence.suspendedGenesis()));
      long deadline=System.nanoTime()+TimeUnit.SECONDS.toNanos(5);
      var updater=fixture.server.getState().getStateMachineUpdater();
      while(updater.getLastAppliedIndex()<2 && System.nanoTime()<deadline)Thread.sleep(2);
      assertEquals(2,updater.getLastAppliedIndex());assertEquals(List.of(0L,1L,2L),machine.applied);
      assertEquals(List.of(configuration),machine.configurations);
      assertFalse(fixture.server.isInitialImageReady());assertFalse(fixture.server.isVotingEligible());
      assertThrows(IOException.class,()->fixture.server.setConfigurationAsync(null));
      assertFalse(fixture.vote(true).getServerReply().getSuccess());
      Thread.sleep(150);assertEquals(2,updater.getLastAppliedIndex());assertTrue(fixture.server.getInfo().isAlive());
      assertThrows(IllegalStateException.class,machine.gate::snapshotPermit);assertNull(machine.getLatestSnapshot());
    }
  }
  static final class BlockedGenesisMachine extends GenesisMachine {
    final CountDownLatch entered = new CountDownLatch(1), proceed = new CountDownLatch(1);
    final java.util.concurrent.atomic.AtomicBoolean marker = new java.util.concurrent.atomic.AtomicBoolean(true);
    volatile boolean failCallback;
    @Override public void notifyTermIndexUpdated(long term, long index) {
      if (index == 0) {
        entered.countDown();
        try { if (!proceed.await(10, TimeUnit.SECONDS)) throw new AssertionError("Metadata callback was not released"); }
        catch (InterruptedException interrupted) { Thread.currentThread().interrupt(); throw new AssertionError(interrupted); }
        if (failCallback) throw new IllegalStateException("Controlled selected metadata callback failure");
      }
      assertTrue(marker.get(), "Metadata callback dispatched after GENESIS marker retirement");
      super.notifyTermIndexUpdated(term, index);
    }
  }
  static void appendGenesisMetadata(RecoveryEligibilityTest.Fixture fixture) throws Exception {
    var request = ServerProtoUtils.toAppendEntriesRequestProto(
        RaftGroupMemberId.valueOf(RecoveryEligibilityTest.LEADER, RecoveryEligibilityTest.GROUP), RecoveryEligibilityTest.SELF,
        1, List.of(metadata(0, 0), metadata(1, 0)), 1, false, null, List.of(), 31, null);
    assertEquals(AppendEntriesReplyProto.AppendResult.SUCCESS, fixture.server.appendEntries(request).getResult());
  }
  @Test void genesisDrainAcknowledgesActualCallbackReturnAndHoldsThroughMarkerPublication() throws Exception {
    var machine = new BlockedGenesisMachine();
    try (var fixture = open("drain-boundary", machine, true)) {
      genesis(machine); appendGenesisMetadata(fixture);
      RockserverRaftServers.DiscoveryDrain hold;
      try {
        assertTrue(machine.entered.await(5, TimeUnit.SECONDS));
        machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.suspendedGenesis());
        hold = RockserverRaftServers.holdDiscovery(fixture.server,machine,RockserverRaftServers.DiscoveryRole.GENESIS);
        assertThrows(IOException.class, hold::requireHeld); assertFalse(hold.drained().isDone());
        var canceled = hold.drained(); canceled.cancel(false); assertFalse(hold.drained().isDone());
        assertThrows(IOException.class, () -> RockserverRaftServers.holdDiscovery(fixture.server,machine,RockserverRaftServers.DiscoveryRole.GENESIS));
      } finally { machine.proceed.countDown(); }
      hold.drained().get(5, TimeUnit.SECONDS); hold.requireHeld();
      assertEquals(List.of(0L), machine.applied); assertEquals(0, fixture.server.getState().getStateMachineUpdater().getLastAppliedIndex());
      synchronized (machine.gate) { hold.requireHeld(); machine.marker.set(false); machine.gate.deferDiscovery(); }
      assertThrows(IOException.class, hold::requireHeld);
      Thread.sleep(150); assertTrue(hold.drained().isDone());
      assertThrows(IOException.class, fixture.server.getState().getStateMachineUpdater()::requireCoveredAvailable);
      assertEquals(List.of(0L), machine.applied);
      hold.release().get(5, TimeUnit.SECONDS);
      fixture.server.getState().getStateMachineUpdater().requireCoveredAvailable();
      Thread.sleep(150); assertEquals(List.of(0L), machine.applied); assertTrue(machine.gate.discoveryPending());
      assertNull(machine.getLatestSnapshot()); assertEquals(-1, fixture.server.getRaftLog().getSnapshotIndex());
    }
  }
  @Test void genesisDrainReleaseWithoutDeferredDiscoveryFencesAndFailsRetirement() throws Exception {
    var machine = new GenesisMachine();
    try (var fixture = open("drain-release-unfenced", machine, true)) {
      assertTrue(machine.gate.settleDiscovery(machine.gate.revision(), RecoveryEligibilityStateMachine.Evidence.suspendedGenesis()));
      var hold = RockserverRaftServers.holdDiscovery(fixture.server,machine,RockserverRaftServers.DiscoveryRole.GENESIS);
      hold.drained().get(5, TimeUnit.SECONDS); hold.requireHeld();
      assertFalse(machine.gate.discoveryPending());
      assertThrows(ExecutionException.class, () -> hold.release().get(5, TimeUnit.SECONDS));
      assertTrue(machine.gate.discoveryPending());
      assertThrows(IOException.class, hold::requireHeld);
      fixture.server.getState().getStateMachineUpdater().requireCoveredAvailable();
      assertTrue(machine.applied.isEmpty()); assertNull(machine.getLatestSnapshot());
      assertEquals(-1, fixture.server.getRaftLog().getSnapshotIndex());
    }
  }
  @Test void genesisDrainAbortAndCallbackFailureRetireOnlyAfterSelectedCallbackReturns() throws Exception {
    for (boolean failure : List.of(false, true)) {
      var machine = new BlockedGenesisMachine(); machine.failCallback = failure;
      try (var fixture = open("drain-abort-" + failure, machine, true)) {
        genesis(machine); appendGenesisMetadata(fixture);
        CompletableFuture<Void> retired; RockserverRaftServers.DiscoveryDrain hold;
        try {
          assertTrue(machine.entered.await(5, TimeUnit.SECONDS)); machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.suspendedGenesis());
          hold = RockserverRaftServers.holdDiscovery(fixture.server,machine,RockserverRaftServers.DiscoveryRole.GENESIS);
          retired = failure ? hold.release() : hold.abort();
          assertFalse(retired.isDone()); assertFalse(hold.drained().isDone());
        } finally { machine.proceed.countDown(); }
        if (failure) assertThrows(ExecutionException.class, () -> retired.get(5, TimeUnit.SECONDS));
        else retired.get(5, TimeUnit.SECONDS);
        assertThrows(ExecutionException.class, () -> hold.drained().get(5, TimeUnit.SECONDS));
        assertTrue(machine.applied.size() <= 1); assertFalse(machine.applied.contains(1L));
      }
    }
  }
  @Test void genesisDrainRevocationBeforeAcknowledgementWaitsForSelectedCallback() throws Exception {
    var machine = new BlockedGenesisMachine();
    try (var fixture = open("drain-revoked", machine, true)) {
      genesis(machine); appendGenesisMetadata(fixture);
      RockserverRaftServers.DiscoveryDrain hold;
      try {
        assertTrue(machine.entered.await(5, TimeUnit.SECONDS)); machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.suspendedGenesis());
        hold = RockserverRaftServers.holdDiscovery(fixture.server,machine,RockserverRaftServers.DiscoveryRole.GENESIS); machine.gate.deferDiscovery();
        assertFalse(hold.drained().isDone());
      } finally { machine.proceed.countDown(); }
      assertThrows(ExecutionException.class, () -> hold.drained().get(5, TimeUnit.SECONDS));
      assertThrows(ExecutionException.class, () -> hold.abort().get(5, TimeUnit.SECONDS));
      assertEquals(List.of(0L), machine.applied); assertTrue(machine.gate.discoveryPending());
    }
  }
  @Test void genesisDrainStopRetainsActualOwnerWhileSelectedMetadataCallbackIsBlocked() throws Exception {
    var machine = new BlockedGenesisMachine();
    try (var fixture = open("drain-stop", machine, true); var closer = Executors.newVirtualThreadPerTaskExecutor()) {
      genesis(machine); appendGenesisMetadata(fixture);
      try {
        assertTrue(machine.entered.await(5, TimeUnit.SECONDS)); machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.suspendedGenesis());
        var hold = RockserverRaftServers.holdDiscovery(fixture.server,machine,RockserverRaftServers.DiscoveryRole.GENESIS);
        var closing = closer.submit(fixture.server::close);
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        var updater = fixture.server.getState().getStateMachineUpdater();
        while (!updater.isStopRequested() && System.nanoTime() < deadline) Thread.sleep(1);
        assertTrue(updater.isStopRequested()); assertFalse(hold.drained().isDone()); assertFalse(closing.isDone());
        var retired = hold.abort(); assertFalse(retired.isDone()); machine.proceed.countDown();
        assertThrows(ExecutionException.class, () -> retired.get(5, TimeUnit.SECONDS));
        assertThrows(ExecutionException.class, () -> hold.drained().get(5, TimeUnit.SECONDS));
        closing.get(5, TimeUnit.SECONDS); assertEquals(List.of(0L), machine.applied);
      } finally { machine.proceed.countDown(); }
    }
  }
  @Test void genesisNeverExportsOrRevivesAnOldSnapshotPermitAndDiscoveryRemainsSticky() {
    var gate = new RecoveryEligibilityStateMachine.Gate(); gate.publish(RecoveryEligibilityStateMachine.Evidence.normal());
    var old = gate.snapshotPermit(); gate.publish(RecoveryEligibilityStateMachine.Evidence.genesis());
    assertThrows(IllegalStateException.class, old::requireCurrent); assertThrows(IllegalStateException.class, gate::snapshotPermit);
    gate.publish(RecoveryEligibilityStateMachine.Evidence.normal()); assertThrows(IllegalStateException.class, old::requireCurrent);
    gate.snapshotPermit().requireCurrent(); gate.deferDiscovery();
    long stale = gate.revision(); gate.publish(RecoveryEligibilityStateMachine.Evidence.genesis());
    assertTrue(gate.discoveryPending()); assertEquals(RecoveryEligibilityStateMachine.Mode.MISSING, gate.snapshot().mode());
    assertFalse(gate.settleDiscovery(stale, RecoveryEligibilityStateMachine.Evidence.genesis()));
    assertTrue(gate.settleDiscovery(gate.revision(), RecoveryEligibilityStateMachine.Evidence.genesis()));
    assertFalse(gate.discoveryPending()); assertThrows(IllegalStateException.class, gate::snapshotPermit);
  }
}
