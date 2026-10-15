package org.apache.ratis.server.impl;

import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.ratis.RaftConfigKeys;
import org.apache.ratis.conf.*;
import org.apache.ratis.datastream.SupportedDataStreamType;
import org.apache.ratis.proto.RaftProtos.*;
import org.apache.ratis.proto.RaftProtos.AppendEntriesReplyProto.AppendResult;
import org.apache.ratis.protocol.*;
import org.apache.ratis.rpc.*;
import org.apache.ratis.server.*;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.server.raftlog.LogProtoUtils;
import org.apache.ratis.server.storage.*;
import org.apache.ratis.statemachine.*;
import org.apache.ratis.statemachine.impl.*;
import org.apache.ratis.thirdparty.com.google.protobuf.ByteString;
import org.apache.ratis.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Real division/RPC handlers, storage, updater and snapshot installer; the transport never opens a socket. */
public class RecoveryEligibilityTest {
  @TempDir Path root;
  static final RaftPeerId SELF = RaftPeerId.valueOf("receiver"), LEADER = RaftPeerId.valueOf("source");
  static final List<RaftPeer> PEERS = List.of(RaftPeer.newBuilder().setId(SELF).setAddress("receiver:0").build(),
      RaftPeer.newBuilder().setId(LEADER).setAddress("source:0").build());
  static final RaftGroupId GROUP = RaftGroupId.valueOf(new UUID(81, 1));
  static final TermIndex IMAGE = TermIndex.valueOf(1, 5);

  public static final class InProcessRpcType implements RpcType {
    public String name() { return getClass().getName(); }
    public RpcFactory newFactory(Parameters parameters) { return new InProcessFactory(parameters.get("test.rpc", RpcControl.class)); }
  }
  static final class RpcControl {
    final CountDownLatch preVoteEntered = new CountDownLatch(1), preVoteReply = new CountDownLatch(1);
    volatile boolean allowPreVote;
  }
  static final class InProcessFactory implements ServerFactory {
    private final RpcControl control;
    InProcessFactory(RpcControl control) { this.control = control; }
    public RpcType getRpcType() { return new InProcessRpcType(); }
    public RaftServerRpc newRaftServerRpc(RaftServer server) { return new InProcessRpc(control); }
  }
  static final class InProcessRpc implements RaftServerRpc {
    private final RpcControl control;
    InProcessRpc(RpcControl control) { this.control = control; }
    public RpcType getRpcType() { return new InProcessRpcType(); }
    public void start() { fail("No network server startup is permitted in this fixture"); }
    public InetSocketAddress getInetSocketAddress() { return InetSocketAddress.createUnresolved("in-process", 0); }
    public void addRaftPeers(Collection<RaftPeer> peers) { }
    public void handleException(RaftPeerId peer, Exception failure, boolean reconnect) { }
    public void close() { }
    public RequestVoteReplyProto requestVote(RequestVoteRequestProto request) throws IOException {
      if (control == null || !control.allowPreVote || !request.getPreVote()) { throw new AssertionError("Unexpected outbound election/self-vote"); }
      control.preVoteEntered.countDown();
      try { if (!control.preVoteReply.await(5, TimeUnit.SECONDS)) { throw new IOException("controlled pre-vote response timed out"); } }
      catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException(failure); }
      return ServerProtoUtils.toRequestVoteReplyProto(SELF, RaftGroupMemberId.valueOf(LEADER, GROUP), true,
          request.getCandidateTerm(), false, TermIndex.valueOf(1, 5), request.getServerRequest().getCallId());
    }
    public AppendEntriesReplyProto appendEntries(AppendEntriesRequestProto request) { throw new AssertionError("Unexpected outbound append"); }
    public InstallSnapshotReplyProto installSnapshot(InstallSnapshotRequestProto request) { throw new AssertionError("Unexpected outbound install"); }
    public StartLeaderElectionReplyProto startLeaderElection(StartLeaderElectionRequestProto request) { throw new AssertionError("Unexpected outbound transfer"); }
  }

  static final class Machine extends BaseStateMachine implements RecoveryEligibilityStateMachine {
    final Gate gate = new Gate();
    final SimpleStateMachineStorage storage = new SimpleStateMachineStorage();
    volatile SnapshotInfo snapshot;
    volatile boolean nativeImage, userReady;
    volatile long configurationTerm, configurationIndex = -1;
    volatile RaftConfigurationProto configuration;
    final List<Long> applied = new CopyOnWriteArrayList<>();
    final CountDownLatch appliedConfiguration = new CountDownLatch(1), reloaded = new CountDownLatch(1);
    volatile CountDownLatch eligibilityEntered, eligibilityContinue;
    volatile CountDownLatch reloadEntered, reloadContinue;
    volatile boolean failReload;
    final AtomicInteger controlledEligibilityReads = new AtomicInteger();
    @Override public TermIndex getLastAppliedTermIndex() {
      var entered = eligibilityEntered; var proceed = eligibilityContinue;
      if (entered != null && proceed != null && Thread.currentThread().getName().equals("controlled-vote")
          && controlledEligibilityReads.incrementAndGet() == 2) {
        entered.countDown();
        try { if (!proceed.await(5, TimeUnit.SECONDS)) { throw new AssertionError("eligibility barrier timeout"); } }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new AssertionError(failure); }
      }
      return super.getLastAppliedTermIndex();
    }
    @Override public void notifyLeaderChanged(RaftGroupMemberId member, RaftPeerId leader) {
      assertFalse(Thread.holdsLock(gate), "SM leader callback entered under recovery Gate");
    }
    @Override public void notifyExtendedNoLeader(RoleInfoProto role) {
      assertFalse(Thread.holdsLock(gate), "SM follower callback entered under recovery Gate");
    }
    @Override public void notifyNotLeader(Collection<TransactionContext> pending) {
      assertFalse(Thread.holdsLock(gate), "SM shutdown callback entered under recovery Gate");
    }
    public Gate recoveryEligibilityGate() { return gate; }
    public SnapshotInfo getLatestSnapshot() { return snapshot; }
    public StateMachineStorage getStateMachineStorage() { return storage; }
    public void initialize(RaftServer server, RaftGroupId group, RaftStorage raftStorage) throws IOException {
      super.initialize(server, group, raftStorage); storage.init(raftStorage);
      getLifeCycle().transition(LifeCycle.State.STARTING); getLifeCycle().transition(LifeCycle.State.RUNNING);
    }
    public void pause() {
      getLifeCycle().transition(LifeCycle.State.PAUSING); getLifeCycle().transition(LifeCycle.State.PAUSED);
    }
    public void reinitialize() throws IOException {
      if (reloadEntered != null) {
        reloadEntered.countDown();
        try { if (!reloadContinue.await(5, TimeUnit.SECONDS)) throw new IOException("Reload fixture timed out"); }
        catch (InterruptedException failure) { Thread.currentThread().interrupt(); throw new IOException(failure); }
      }
      if (failReload) throw new IOException("Controlled checkpoint reload failure");
      snapshot = storage.loadLatestSnapshot();
      if (snapshot == null || !Arrays.equals(Files.readAllBytes(snapshot.getFiles().getFirst().getPath()), new byte[]{11, 22, 33})) {
        throw new IOException("Missing actual installed test image");
      }
      nativeImage = true; setLastAppliedTermIndex(snapshot.getTermIndex()); publish();
      getLifeCycle().transition(LifeCycle.State.STARTING); getLifeCycle().transition(LifeCycle.State.RUNNING);
      reloaded.countDown();
    }
    void publish() {
      gate.publish(Evidence.recovered(snapshot.getTermIndex(), getLastAppliedTermIndex(), snapshot.getTermIndex(),
          configurationTerm, configurationIndex, configuration));
    }
    void snapshotFile(TermIndex cut) throws Exception {
      Path path = storage.getSnapshotFile(cut.getTerm(), cut.getIndex()).toPath();
      Files.write(path, new byte[]{11, 22, 33});
      try (var file = FileChannel.open(path, StandardOpenOption.WRITE)) { file.force(true); }
      var info = new FileInfo(path, org.apache.ratis.io.MD5Hash.newInstance(MessageDigest.getInstance("MD5").digest(new byte[]{11, 22, 33})));
      snapshot = new SingleFileSnapshotInfo(info, cut.getTerm(), cut.getIndex());
      setLastAppliedTermIndex(cut);
    }
    @Override public void notifyTermIndexUpdated(long term, long index) {
      if (!nativeImage) { throw new AssertionError("Historical apply before actual image at " + index); }
      applied.add(index); super.notifyTermIndexUpdated(term, index); publish();
      if (index == configurationIndex) appliedConfiguration.countDown();
    }
    @Override public void notifyConfigurationChanged(long term, long index, RaftConfigurationProto proto) {
      if (!nativeImage) { throw new AssertionError("Historical configuration before actual image"); }
      configurationTerm = term; configurationIndex = index; configuration = proto;
      updateLastAppliedTermIndex(term, index); publish();
    }
  }

  final class Fixture implements AutoCloseable {
    final RaftServerProxy proxy;
    final RaftServerImpl server;
    final Machine machine;
    final RpcControl control = new RpcControl();
    Fixture(Path path, BaseStateMachine stateMachine) throws Exception {
      this(path, stateMachine, true);
    }
    Fixture(Path path, BaseStateMachine stateMachine, boolean installSnapshotEnabled) throws Exception {
      this.machine = stateMachine instanceof Machine ? (Machine)stateMachine : null;
      var properties = new RaftProperties();
      RaftConfigKeys.Rpc.setType(properties, new InProcessRpcType());
      RaftConfigKeys.DataStream.setType(properties, SupportedDataStreamType.DISABLED);
      RaftServerConfigKeys.setStorageDir(properties, List.of(path.toFile()));
      RaftServerConfigKeys.setStorageFreeSpaceMin(properties, SizeInBytes.valueOf(0));
      RaftServerConfigKeys.Rpc.setTimeoutMin(properties, TimeDuration.valueOf(1, TimeUnit.HOURS));
      RaftServerConfigKeys.Rpc.setTimeoutMax(properties, TimeDuration.valueOf(2, TimeUnit.HOURS));
      RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMin(properties, TimeDuration.valueOf(1, TimeUnit.HOURS));
      RaftServerConfigKeys.Rpc.setFirstElectionTimeoutMax(properties, TimeDuration.valueOf(2, TimeUnit.HOURS));
      RaftServerConfigKeys.Log.Appender.setInstallSnapshotEnabled(properties, installSnapshotEnabled);
      RaftServerConfigKeys.LeaderElection.setPreVote(properties, true);
      RaftServerConfigKeys.Notification.setNoLeaderTimeout(properties, TimeDuration.ZERO);
      var parameters = new Parameters(); parameters.put("test.rpc", control, RpcControl.class);
      proxy = new RaftServerProxy(SELF, ignored -> stateMachine, properties, parameters, null);
      server = new RaftServerImpl(RaftGroup.valueOf(GROUP, PEERS), stateMachine, proxy, RaftStorage.StartupOption.RECOVER);
      if (machine != null && machine.configuration == null) { machine.configuration = configuration(server); }
      assertTrue(server.start());
    }
    RequestVoteReplyProto vote(boolean preVote) throws IOException {
      return server.requestVote(ServerProtoUtils.toRequestVoteRequestProto(
          RaftGroupMemberId.valueOf(LEADER, GROUP), SELF, 2, TermIndex.valueOf(1, 100), preVote));
    }
    InstallSnapshotRequestProto snapshot(TermIndex cut, int index, int offset, byte[] data, boolean done) throws Exception {
      Path target = machine.storage.getSnapshotFile(cut.getTerm(), cut.getIndex()).toPath();
      String relative = server.getRaftStorage().getStorageDir().getRoot().toPath().relativize(target).toString();
      var file = FileChunkProto.newBuilder().setFilename(relative).setOffset(offset).setDone(done).setData(ByteString.copyFrom(data));
      if (done) { file.setFileDigest(ByteString.copyFrom(MessageDigest.getInstance("MD5").digest(new byte[]{11,22,33}))); }
      return InstallSnapshotRequestProto.newBuilder().setServerRequest(request(9)).setLeaderTerm(2)
          .setSnapshotChunk(InstallSnapshotRequestProto.SnapshotChunkProto.newBuilder().setRequestId("image")
              .setRequestIndex(index).setTermIndex(cut.toProto()).setDone(done).addFileChunks(file)).build();
    }
    public void close() {
      if (machine != null) { machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.missing()); }
      server.close(); proxy.close();
    }
  }
  static RaftConfigurationProto configuration(RaftServerImpl server) {
    return LogProtoUtils.toRaftConfigurationProtoBuilder(server.getRaftConf()).build();
  }
  static RaftRpcRequestProto request(long call) {
    return RaftRpcRequestProto.newBuilder().setRequestorId(LEADER.toByteString()).setReplyId(SELF.toByteString())
        .setRaftGroupId(ProtoUtils.toRaftGroupIdProtoBuilder(GROUP)).setCallId(call).build();
  }
  static boolean timeout(FollowerState state) throws Exception {
    var method = FollowerState.class.getDeclaredMethod("roleChangeChecking", TimeDuration.class);
    method.setAccessible(true); return (boolean)method.invoke(state, TimeDuration.ZERO);
  }
  static void append(RaftServerImpl server, List<LogEntryProto> entries) throws Exception {
    for (var future : server.getRaftLog().append(entries)) { future.get(5, TimeUnit.SECONDS); }
  }

  @Test void missingConfiguredVoterRejectsBothVotesTimeoutsAndForcedTransferWithoutPersistingGrant() throws Exception {
    try (var fixture = new Fixture(root.resolve("missing"), new Machine())) {
      var server = fixture.server; var follower = server.getRole().getFollowerState().orElseThrow();
      final var originalMetadata = server.getRaftStorage().getMetadataFile().getMetadata();
      for (int n = 0; n < 3; n++) {
        assertFalse(fixture.vote(true).getServerReply().getSuccess());
        assertFalse(fixture.vote(false).getServerReply().getSuccess());
        assertFalse(timeout(follower)); assertTrue(follower.isAlive());
      }
      assertEquals(originalMetadata, server.getRaftStorage().getMetadataFile().getMetadata());
      assertEquals(0, server.getState().getCurrentTerm());
      var transfer = ServerProtoUtils.toStartLeaderElectionRequestProto(RaftGroupMemberId.valueOf(LEADER, GROUP), SELF, IMAGE);
      assertFalse(server.startLeaderElection(transfer).getServerReply().getSuccess());
      assertFalse(server.changeToCandidate(true));
      assertEquals(originalMetadata, server.getRaftStorage().getMetadataFile().getMetadata());
      assertEquals(RaftPeerRole.FOLLOWER, server.getInfo().getCurrentRole());
      fixture.machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.normal());
      assertTrue(timeout(follower)); // The same timeout owner remains live after restoration.
      assertTrue(fixture.vote(false).getServerReply().getSuccess());
      assertEquals(LEADER, server.getRaftStorage().getMetadataFile().getMetadata().getVotedFor());
    }
  }

  @Test void realNativeSnapshotLogAndConfigurationCombinationEnablesVotingWhileUserReadinessStillPending() throws Exception {
    try (var fixture = new Fixture(root.resolve("tuple"), new Machine())) {
      var machine = fixture.machine; var server = fixture.server;
      machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.recovered(IMAGE, IMAGE, IMAGE, 0, -1, machine.configuration));
      assertFalse(fixture.vote(false).getServerReply().getSuccess()); // Native receipt alone.
      machine.snapshotFile(IMAGE); machine.nativeImage = true;
      assertFalse(server.isVotingEligible()); // Snapshot publication without actual LOG floor.
      server.getRaftLog().onSnapshotInstalled(IMAGE.getIndex()).get(5, TimeUnit.SECONDS);
      machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.missing());
      assertFalse(fixture.vote(false).getServerReply().getSuccess()); // Snapshot/config alone.
      var wrong = machine.configuration.toBuilder().clearPeers().build();
      machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.recovered(IMAGE, IMAGE, IMAGE, 0, -1, wrong));
      assertTrue(server.isInitialImageReady()); assertFalse(server.isVotingEligible());
      machine.publish(); assertFalse(server.isVotingEligible()); // Durable snapshot cannot repair an older Ratis metadata term by assertion.
      server.changeToFollowerAndPersistMetadata(1, false, "installed recovery baseline").join();
      assertFalse(machine.userReady); assertTrue(server.isVotingEligible());
      assertTrue(fixture.vote(true).getServerReply().getSuccess()); assertTrue(fixture.vote(false).getServerReply().getSuccess());
      machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.missing());
      server.getRole().shutdownFollowerState(); server.getRole().transitionRole(RaftPeerRole.CANDIDATE);
      assertThrows(IOException.class, () -> server.initEligibleElection(LeaderElection.Phase.ELECTION));
      assertEquals(2, server.getState().getCurrentTerm()); // Revocation never increments/self-votes.
      assertEquals(LEADER, server.getRaftStorage().getMetadataFile().getMetadata().getVotedFor());
      assertEquals(RaftPeerRole.FOLLOWER, server.getInfo().getCurrentRole());
      server.getRole().shutdownFollowerState(); server.getRole().transitionRole(RaftPeerRole.CANDIDATE);
      server.changeToLeader(); assertEquals(RaftPeerRole.FOLLOWER, server.getInfo().getCurrentRole());
    }
  }

  @Test void partialThenCompleteActualSnapshotInstallRemainsAvailableWhileVotingClosed() throws Exception {
    try (var fixture = new Fixture(root.resolve("snapshot"), new Machine())) {
      var first = fixture.server.installSnapshot(fixture.snapshot(IMAGE, 0, 0, new byte[]{11}, false));
      assertEquals(InstallSnapshotResult.SUCCESS, first.getResult()); assertFalse(fixture.server.isVotingEligible());
      assertNull(fixture.machine.snapshot); assertTrue(fixture.machine.applied.isEmpty());
      var last = fixture.server.installSnapshot(fixture.snapshot(IMAGE, 1, 1, new byte[]{22,33}, true));
      assertEquals(InstallSnapshotResult.SUCCESS, last.getResult());
      assertTrue(fixture.machine.reloaded.await(5, TimeUnit.SECONDS));
      assertTrue(fixture.server.isInitialImageReady()); assertTrue(fixture.server.isVotingEligible());
      assertFalse(fixture.machine.userReady); assertEquals(IMAGE, fixture.machine.getLastAppliedTermIndex());
      assertTrue(fixture.vote(false).getServerReply().getSuccess());
      long revision = fixture.machine.gate.revision();
      var corrupt = fixture.snapshot(TermIndex.valueOf(1, 6), 0, 0, new byte[]{44}, true);
      assertThrows(IOException.class, () -> fixture.server.installSnapshot(corrupt));
      assertTrue(fixture.machine.gate.revision() > revision);
      assertTrue(fixture.server.isVotingEligible()); assertEquals(IMAGE, fixture.machine.getLastAppliedTermIndex());
    }
  }

  @Test void persistedCommitAndHeartbeatWaitForActualImageThenReloadPreservesAndAppliesCommittedConfigurationTail() throws Exception {
    Path path = root.resolve("committed-tail"); var sourceConfiguration = RaftConfigurationProto.newBuilder()
        .addAllPeers(PEERS.stream().map(RaftPeer::getRaftPeerProto).toList()).build();
    var targetConfiguration = sourceConfiguration.toBuilder().addListeners(RaftPeer.newBuilder()
        .setId("observer").setAddress("observer:0").setStartupRole(RaftPeerRole.LISTENER).build().getRaftPeerProto()).build();
    LogEntryProto sourceCfg = LogEntryProto.newBuilder().setTerm(1).setIndex(99).setConfigurationEntry(sourceConfiguration).build();
    LogEntryProto targetCfg = LogEntryProto.newBuilder().setTerm(1).setIndex(100).setConfigurationEntry(targetConfiguration).build();
    try (var seed = new Fixture(path, new Machine())) {
      seed.server.changeToFollowerAndPersistMetadata(1, false, "durable committed tail").join();
      var entries = new ArrayList<LogEntryProto>();
      for (int n = 0; n < 99; n++) { entries.add(LogEntryProto.newBuilder().setTerm(1).setIndex(n).setMetadataEntry(MetadataProto.newBuilder()).build()); }
      entries.add(sourceCfg); entries.add(targetCfg);
      entries.add(LogEntryProto.newBuilder().setTerm(1).setIndex(101).setMetadataEntry(MetadataProto.newBuilder().setCommitIndex(100)).build());
      append(seed.server, entries); assertTrue(seed.machine.applied.isEmpty());
    }
    var machine = new Machine(); machine.configurationTerm = 1; machine.configurationIndex = 99; machine.configuration = sourceConfiguration;
    try (var fixture = new Fixture(path, machine)) {
      var server = fixture.server;
      assertEquals(100, server.getRaftLog().getLastCommittedIndex()); assertEquals(100, server.getRaftConf().getLogEntryIndex());
      assertFalse(machine.appliedConfiguration.await(150, TimeUnit.MILLISECONDS)); assertTrue(machine.applied.isEmpty());
      var heartbeat = ServerProtoUtils.toAppendEntriesRequestProto(RaftGroupMemberId.valueOf(LEADER, GROUP), SELF,
          2, List.of(), 101, false, TermIndex.valueOf(1,101), List.of(), 4, null);
      assertEquals(AppendResult.INCONSISTENCY, server.appendEntries(heartbeat).getResult());
      assertEquals(100, server.getRaftLog().getLastCommittedIndex()); assertTrue(machine.applied.isEmpty());
      var cut = TermIndex.valueOf(1,99);
      var snapshot = fixture.snapshot(cut, 0, 0, new byte[]{11,22,33}, true).toBuilder()
          .setLastRaftConfigurationLogEntryProto(sourceCfg).build();
      assertEquals(InstallSnapshotResult.SUCCESS, server.installSnapshot(snapshot).getResult());
      assertTrue(machine.appliedConfiguration.await(5, TimeUnit.SECONDS));
      assertEquals(List.of(100L), machine.applied); assertEquals(100, server.getRaftLog().getLastCommittedIndex());
      assertEquals(100, server.getRaftConf().getLogEntryIndex()); assertEquals(targetCfg, server.getRaftLog().get(100));
      assertTrue(server.isVotingEligible()); assertFalse(machine.userReady);
      long next = server.getRaftLog().getNextIndex();
      append(server, List.of(LogEntryProto.newBuilder().setTerm(2).setIndex(next).setMetadataEntry(MetadataProto.newBuilder().setCommitIndex(100)).build()));
      assertEquals(next + 1, server.getRaftLog().getNextIndex()); // Durable tail wasn't reset to the snapshot floor.
    }
  }

  @Test void ordinaryStateMachineRetainsVotingWithoutOptionalRecoveryInterface() throws Exception {
    try (var fixture = new Fixture(root.resolve("ordinary"), new BaseStateMachine())) {
      assertTrue(fixture.server.isInitialImageReady()); assertTrue(fixture.server.isVotingEligible());
      assertTrue(fixture.vote(false).getServerReply().getSuccess());
      assertEquals(LEADER, fixture.server.getRaftStorage().getMetadataFile().getMetadata().getVotedFor());
    }
  }

  @Test void revocationJoinsActualAdmittedVotePersistenceBeforeReturning() throws Exception {
    try (var fixture = new Fixture(root.resolve("revocation"), new Machine());
         var workers = Executors.newVirtualThreadPerTaskExecutor()) {
      var machine = fixture.machine; machine.snapshotFile(IMAGE); machine.nativeImage = true;
      fixture.server.getRaftLog().onSnapshotInstalled(IMAGE.getIndex()).get(5, TimeUnit.SECONDS); machine.publish();
      fixture.server.changeToFollowerAndPersistMetadata(1, false, "installed recovery baseline").join();
      long oldRevision = machine.gate.revision();
      machine.eligibilityEntered = new CountDownLatch(1); machine.eligibilityContinue = new CountDownLatch(1);
      var vote = workers.submit(() -> { Thread.currentThread().setName("controlled-vote"); return fixture.vote(false); });
      assertTrue(machine.eligibilityEntered.await(5, TimeUnit.SECONDS));
      var revocationStarted = new CountDownLatch(1);
      var revoke = workers.submit(() -> { revocationStarted.countDown(); machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.missing()); });
      assertTrue(revocationStarted.await(5, TimeUnit.SECONDS));
      try { assertThrows(TimeoutException.class, () -> revoke.get(100, TimeUnit.MILLISECONDS)); }
      finally { machine.eligibilityContinue.countDown(); }
      assertTrue(vote.get(5, TimeUnit.SECONDS).getServerReply().getSuccess()); revoke.get(5, TimeUnit.SECONDS);
      machine.eligibilityEntered = null; machine.eligibilityContinue = null;
      assertEquals(LEADER, fixture.server.getRaftStorage().getMetadataFile().getMetadata().getVotedFor());
      assertFalse(machine.gate.publishIfCurrent(oldRevision, RecoveryEligibilityStateMachine.Evidence.normal()));
      assertFalse(fixture.vote(false).getServerReply().getSuccess());
    }
  }

  @Test void runningPreVoteRevocationRestoresFollowerWithoutPersistingSelfVote() throws Exception {
    try (var fixture = new Fixture(root.resolve("running-election"), new Machine())) {
      var machine = fixture.machine; machine.snapshotFile(IMAGE); machine.nativeImage = true;
      fixture.server.getRaftLog().onSnapshotInstalled(IMAGE.getIndex()).get(5, TimeUnit.SECONDS); machine.publish();
      fixture.server.changeToFollowerAndPersistMetadata(1, false, "installed recovery baseline").join();
      final var originalMetadata = fixture.server.getRaftStorage().getMetadataFile().getMetadata();
      fixture.control.allowPreVote = true;
      assertTrue(fixture.server.changeToCandidate(false));
      assertTrue(fixture.control.preVoteEntered.await(5, TimeUnit.SECONDS));
      machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.missing());
      fixture.control.preVoteReply.countDown();
      long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
      while (!fixture.server.getInfo().isFollower() && System.nanoTime() < deadline) { TimeUnit.MILLISECONDS.sleep(1); }
      assertTrue(fixture.server.getInfo().isFollower()); assertEquals(1, fixture.server.getState().getCurrentTerm());
      assertEquals(originalMetadata, fixture.server.getRaftStorage().getMetadataFile().getMetadata());
      var follower = fixture.server.getRole().getFollowerState().orElseThrow(); assertFalse(timeout(follower));
      machine.publish(); assertTrue(timeout(follower));
    }
  }
  @Test void restoredVotingEvidenceDoesNotRestoreRevokedSnapshotPermits() {
    var gate = new RecoveryEligibilityStateMachine.Gate();
    gate.publish(RecoveryEligibilityStateMachine.Evidence.normal());
    var old = gate.snapshotPermit();
    var revoked = gate.revoke();
    gate.restoreUnchanged(revoked);
    assertEquals(RecoveryEligibilityStateMachine.Mode.NORMAL, gate.snapshot().mode());
    assertThrows(IllegalStateException.class, old::requireCurrent);
    var fresh = gate.snapshotPermit();
    gate.publish(RecoveryEligibilityStateMachine.Evidence.normal());
    fresh.requireCurrent();
    gate.publish(RecoveryEligibilityStateMachine.Evidence.missing());
    gate.publish(RecoveryEligibilityStateMachine.Evidence.normal());
    assertThrows(IllegalStateException.class, fresh::requireCurrent);
  }

  @Test void localCheckpointReloadObservesActualUpdaterAndKeepsDiscoveryClosed() throws Exception {
    var path = root.resolve("local-reload");
    var originalConfiguration = RaftConfigurationProto.newBuilder()
        .addAllPeers(PEERS.stream().map(RaftPeer::getRaftPeerProto).toList()).build();
    var newerConfiguration = originalConfiguration.toBuilder().addListeners(RaftPeer.newBuilder()
        .setId("observer").setAddress("observer:0").setStartupRole(RaftPeerRole.LISTENER).build().getRaftPeerProto()).build();
    var newerEntry = LogEntryProto.newBuilder().setTerm(1).setIndex(6).setConfigurationEntry(newerConfiguration).build();
    try (var seed = new Fixture(path, new Machine())) {
      seed.server.changeToFollowerAndPersistMetadata(1, false, "durable local reload tail").join();
      var entries = new ArrayList<LogEntryProto>();
      for (int n = 0; n < 5; n++) entries.add(LogEntryProto.newBuilder().setTerm(1).setIndex(n)
          .setMetadataEntry(MetadataProto.newBuilder()).build());
      entries.add(LogEntryProto.newBuilder().setTerm(1).setIndex(5).setConfigurationEntry(originalConfiguration).build());
      entries.add(newerEntry);
      entries.add(LogEntryProto.newBuilder().setTerm(1).setIndex(7)
          .setMetadataEntry(MetadataProto.newBuilder().setCommitIndex(6)).build());
      append(seed.server, entries);
    }
    var machine = new Machine(); machine.gate.deferDiscovery();
    machine.configurationTerm = 1; machine.configurationIndex = 5; machine.configuration = originalConfiguration;
    try (var fixture = new Fixture(path, machine)) {
      assertEquals(6, fixture.server.getRaftConf().getLogEntryIndex());
      var baseline = TermIndex.valueOf(1, 2);
      machine.snapshotFile(baseline); machine.nativeImage = true;
      machine.snapshotFile(IMAGE); machine.pause();
      machine.reloadEntered = new CountDownLatch(1); machine.reloadContinue = new CountDownLatch(1);
      assertThrows(IOException.class, () -> RockserverRaftServers.reloadLocalCheckpoint(
          fixture.server, machine, baseline));
      assertThrows(IOException.class, () -> RockserverRaftServers.reloadLocalCheckpoint(
          fixture.server, new Machine(), IMAGE));
      assertTrue(fixture.server.getState().getStateMachineUpdater().getLastAppliedIndex() < IMAGE.getIndex());
      var observer = RockserverRaftServers.reloadLocalCheckpoint(fixture.server, machine, IMAGE);
      var actualReload = fixture.server.getState().getStateMachineUpdater().localReloadCompletion();
      var failedLog = CompletableFuture.<Long>failedFuture(new IOException("Controlled original log failure"));
      var failedAfterDrain = RockserverRaftServers.awaitLocalReload(actualReload, failedLog);
      try {
        assertTrue(machine.reloadEntered.await(5, TimeUnit.SECONDS));
        assertFalse(observer.isDone()); assertFalse(failedAfterDrain.isDone());
        assertThrows(IOException.class, () -> RockserverRaftServers.reloadLocalCheckpoint(fixture.server, machine, IMAGE));
        assertFalse(fixture.vote(false).getServerReply().getSuccess());
        assertFalse(fixture.server.isInitialImageReady()); assertTrue(machine.applied.isEmpty());
        assertThrows(IOException.class, () -> fixture.server.installSnapshot(
            fixture.snapshot(TermIndex.valueOf(1, 6), 0, 0, new byte[]{11,22,33}, true)));
        observer.cancel(false);
      } finally { machine.reloadContinue.countDown(); }
      assertTrue(machine.reloaded.await(5, TimeUnit.SECONDS));
      // The canceled observer cannot release the actual updater/log operation.
      var log = (org.apache.ratis.server.raftlog.segmented.SegmentedRaftLog)fixture.server.getRaftLog();
      log.snapshotInstallationCompletion(IMAGE.getIndex()).get(5, TimeUnit.SECONDS);
      actualReload.get(5, TimeUnit.SECONDS);
      assertThrows(ExecutionException.class, () -> failedAfterDrain.get(5, TimeUnit.SECONDS));
      assertEquals(IMAGE.getIndex(), fixture.server.getState().getStateMachineUpdater().getLastAppliedIndex());
      assertEquals(LifeCycle.State.RUNNING, machine.getLifeCycleState());
      assertTrue(machine.gate.discoveryPending()); assertFalse(fixture.server.isInitialImageReady());
      assertFalse(fixture.vote(true).getServerReply().getSuccess()); assertTrue(machine.applied.isEmpty());
      assertTrue(machine.gate.settleDiscovery(machine.gate.revision(), RecoveryEligibilityStateMachine.Evidence.recovered(
          IMAGE, IMAGE, IMAGE, machine.configurationTerm, machine.configurationIndex, machine.configuration)));
      assertTrue(fixture.server.isInitialImageReady());
      fixture.server.getState().getStateMachineUpdater().notifyUpdater();
      assertTrue(machine.appliedConfiguration.await(5, TimeUnit.SECONDS));
      assertEquals(List.of(6L), machine.applied);
      assertEquals(newerEntry, fixture.server.getRaftLog().get(6));
      assertEquals(6, fixture.server.getRaftConf().getLogEntryIndex());
      assertTrue(fixture.server.isVotingEligible());
    }
  }

  @Test void localCheckpointContradictingKnownLogTermRejectsBeforeRegistration() throws Exception {
    var machine = new Machine(); machine.gate.deferDiscovery();
    try (var fixture = new Fixture(root.resolve("local-reload-wrong-term"), machine)) {
      var entries = new ArrayList<LogEntryProto>();
      for (int n = 0; n <= IMAGE.getIndex(); n++) entries.add(LogEntryProto.newBuilder()
          .setTerm(2).setIndex(n).setMetadataEntry(MetadataProto.newBuilder()).build());
      append(fixture.server, entries);
      machine.snapshotFile(IMAGE); machine.nativeImage = true; machine.pause();
      var updater = fixture.server.getState().getStateMachineUpdater();
      long applied = updater.getLastAppliedIndex(), floor = fixture.server.getRaftLog().getSnapshotIndex();
      assertEquals(TermIndex.valueOf(2, IMAGE.getIndex()), fixture.server.getRaftLog().getTermIndex(IMAGE.getIndex()));
      assertThrows(IOException.class, () -> RockserverRaftServers.reloadLocalCheckpoint(fixture.server, machine, IMAGE));
      assertThrows(IOException.class, updater::localReloadCompletion);
      assertEquals(applied, updater.getLastAppliedIndex()); assertEquals(floor, fixture.server.getRaftLog().getSnapshotIndex());
      assertEquals(entries.getLast(), fixture.server.getRaftLog().get(IMAGE.getIndex()));
      assertEquals(1, machine.reloaded.getCount()); assertTrue(machine.gate.discoveryPending());
    }
  }

  @Test void localCheckpointBelowRetainedLogFloorRejectsBeforeRegistration() throws Exception {
    var machine = new Machine(); machine.gate.deferDiscovery();
    try (var fixture = new Fixture(root.resolve("local-reload-old-floor"), machine)) {
      var floor = TermIndex.valueOf(1, 8);
      machine.snapshotFile(floor); machine.nativeImage = true;
      fixture.server.getRaftLog().onSnapshotInstalled(floor.getIndex()).get(5, TimeUnit.SECONDS);
      machine.snapshotFile(IMAGE); machine.pause();
      var updater = fixture.server.getState().getStateMachineUpdater();
      long originalApplied = updater.getLastAppliedIndex();
      assertEquals(floor.getIndex(), fixture.server.getRaftLog().getSnapshotIndex());
      assertThrows(IOException.class, () -> RockserverRaftServers.reloadLocalCheckpoint(fixture.server, machine, IMAGE));
      assertThrows(IOException.class, updater::localReloadCompletion);
      assertEquals(originalApplied, updater.getLastAppliedIndex());
      assertEquals(1, machine.reloaded.getCount()); assertEquals(LifeCycle.State.PAUSED, machine.getLifeCycleState());
      assertEquals(floor.getIndex(), fixture.server.getRaftLog().getSnapshotIndex());
      assertTrue(machine.gate.discoveryPending());
    }
  }

  @Test void localCheckpointReloadStopWaitsForActualBlockedReinitialize() throws Exception {
    var machine = new Machine(); machine.gate.deferDiscovery();
    try (var fixture = new Fixture(root.resolve("local-reload-stop"), machine);
         var workers = Executors.newVirtualThreadPerTaskExecutor()) {
      machine.snapshotFile(IMAGE); machine.nativeImage = true; machine.pause();
      machine.reloadEntered = new CountDownLatch(1); machine.reloadContinue = new CountDownLatch(1);
      var observer = RockserverRaftServers.reloadLocalCheckpoint(fixture.server, machine, IMAGE);
      assertTrue(machine.reloadEntered.await(5, TimeUnit.SECONDS));
      var actualUpdater = fixture.server.getState().getStateMachineUpdater();
      var actualReload = actualUpdater.localReloadCompletion();
      var callbackUnlocked = actualReload.handle((ignored, failure) -> {
        assertFalse(Thread.holdsLock(actualUpdater), "Reload stop callback entered under updater monitor"); return null;
      });
      var close = workers.submit(() -> fixture.server.close());
      try {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        var updater = fixture.server.getState().getStateMachineUpdater();
        while (!updater.isStopRequested() && System.nanoTime() < deadline) TimeUnit.MILLISECONDS.sleep(1);
        assertTrue(updater.isStopRequested());
        assertFalse(close.isDone()); assertFalse(observer.isDone()); assertFalse(actualReload.isDone());
        var cancelled = observer.copy(); cancelled.cancel(false);
        assertFalse(observer.isDone());
      } finally { machine.reloadContinue.countDown(); }
      assertThrows(ExecutionException.class, () -> observer.get(5, TimeUnit.SECONDS));
      close.get(5, TimeUnit.SECONDS); callbackUnlocked.get(5, TimeUnit.SECONDS);
      assertTrue(machine.gate.discoveryPending());
    }
  }

  @Test void localCheckpointReloadFailureSettlesObserverAndKeepsFence() throws Exception {
    var machine = new Machine(); machine.gate.deferDiscovery();
    try (var fixture = new Fixture(root.resolve("local-reload-failed"), machine)) {
      machine.snapshotFile(IMAGE); machine.nativeImage = true; machine.pause(); machine.failReload = true;
      machine.reloadEntered = new CountDownLatch(1); machine.reloadContinue = new CountDownLatch(1);
      var reload = RockserverRaftServers.reloadLocalCheckpoint(fixture.server, machine, IMAGE);
      CompletableFuture<?> callbackUnlocked;
      try {
        assertTrue(machine.reloadEntered.await(5, TimeUnit.SECONDS));
        var updater = fixture.server.getState().getStateMachineUpdater();
        callbackUnlocked = updater.localReloadCompletion().handle((ignored, failure) -> {
          assertFalse(Thread.holdsLock(updater), "Reload failure callback entered under updater monitor"); return null;
        });
      } finally { machine.reloadContinue.countDown(); }
      assertThrows(ExecutionException.class, () -> reload.get(5, TimeUnit.SECONDS));
      callbackUnlocked.get(5, TimeUnit.SECONDS);
      assertTrue(machine.gate.discoveryPending()); assertFalse(fixture.server.isVotingEligible());
    }
  }

  @Test void discoveryFenceSurvivesOrdinaryPublicationAndRejectsChunksUntilExplicitSettlement() throws Exception {
    var machine = new Machine(); machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.normal());
    var oldPermit = machine.gate.snapshotPermit(); machine.gate.deferDiscovery();
    try (var fixture = new Fixture(root.resolve("discovery-chunks"), machine)) {
      final var originalMetadata = fixture.server.getRaftStorage().getMetadataFile().getMetadata();
      long staleRevision = machine.gate.revision();
      machine.gate.publish(RecoveryEligibilityStateMachine.Evidence.normal());
      assertTrue(machine.gate.discoveryPending());
      assertEquals(RecoveryEligibilityStateMachine.Mode.MISSING, machine.gate.snapshot().mode());
      assertThrows(IllegalStateException.class, oldPermit::requireCurrent);
      assertThrows(IllegalStateException.class, machine.gate::snapshotPermit);
      assertFalse(machine.gate.settleDiscovery(staleRevision, RecoveryEligibilityStateMachine.Evidence.normal()));
      var revoked = machine.gate.revoke(); machine.gate.restoreUnchanged(revoked);
      assertTrue(machine.gate.discoveryPending());
      for (boolean done : List.of(false, true)) {
        var request = fixture.snapshot(IMAGE, 0, 0, done ? new byte[]{11,22,33} : new byte[]{11}, done);
        assertThrows(IOException.class, () -> fixture.server.installSnapshot(request));
      }
      assertEquals(originalMetadata, fixture.server.getRaftStorage().getMetadataFile().getMetadata());
      assertNull(machine.snapshot); assertFalse(machine.nativeImage); assertTrue(machine.applied.isEmpty());
      assertFalse(fixture.server.isInitialImageReady()); assertFalse(fixture.vote(false).getServerReply().getSuccess());
      assertFalse(Files.exists(machine.storage.getSnapshotFile(1, 5).toPath()));
      assertTrue(machine.gate.settleDiscovery(machine.gate.revision(), RecoveryEligibilityStateMachine.Evidence.missing()));
      assertFalse(machine.gate.discoveryPending()); assertFalse(fixture.server.isVotingEligible());
      assertEquals(InstallSnapshotResult.SUCCESS, fixture.server.installSnapshot(
          fixture.snapshot(IMAGE, 0, 0, new byte[]{11,22,33}, true)).getResult());
      assertTrue(machine.reloaded.await(5, TimeUnit.SECONDS)); assertTrue(fixture.server.isVotingEligible());
      assertFalse(machine.gate.settleDiscovery(machine.gate.revision(), RecoveryEligibilityStateMachine.Evidence.normal()));
      assertThrows(IllegalStateException.class, oldPermit::requireCurrent);
    }
  }

  @Test void discoveryFenceRejectsSnapshotNotificationsBeforeLeaderMetadataOrStateMachineEffects() throws Exception {
    var machine = new Machine(); machine.gate.deferDiscovery();
    try (var fixture = new Fixture(root.resolve("discovery-notification"), machine, false)) {
      final var originalMetadata = fixture.server.getRaftStorage().getMetadataFile().getMetadata();
      var notification = InstallSnapshotRequestProto.newBuilder().setServerRequest(request(9)).setLeaderTerm(2)
          .setNotification(InstallSnapshotRequestProto.NotificationProto.newBuilder()
              .setFirstAvailableTermIndex(TermIndex.valueOf(1, 6).toProto())).build();
      assertThrows(IOException.class, () -> fixture.server.installSnapshot(notification));
      assertEquals(originalMetadata, fixture.server.getRaftStorage().getMetadataFile().getMetadata());
      assertNull(fixture.server.getState().getLeaderId());
      assertNull(machine.snapshot); assertFalse(machine.nativeImage); assertTrue(machine.applied.isEmpty());
      assertTrue(machine.gate.discoveryPending());
    }
  }

}
