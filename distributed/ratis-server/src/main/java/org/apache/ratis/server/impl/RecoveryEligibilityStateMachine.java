package org.apache.ratis.server.impl;

import java.util.Objects;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.StateMachine;

/** Optional durable native recovery baseline for voting, independent of final USER readiness. */
public interface RecoveryEligibilityStateMachine extends StateMachine {
  enum Mode { NORMAL, MISSING, RECOVERY, GENESIS, GENESIS_SUSPENDED }

  /** All fields describe already completed native effects and an already durably published Ratis snapshot. */
  record Evidence(Mode mode, TermIndex imageCut, TermIndex nativeCut, TermIndex snapshotCut,
                  long configurationTerm, long configurationIndex, RaftConfigurationProto configuration) {
    public Evidence {
      Objects.requireNonNull(mode);
      if (mode == Mode.RECOVERY) {
        Objects.requireNonNull(imageCut); Objects.requireNonNull(nativeCut);
        Objects.requireNonNull(snapshotCut); Objects.requireNonNull(configuration);
        if (imageCut.getTerm() < 1 || imageCut.getIndex() < 0 || snapshotCut.getTerm() < 1
            || !covers(nativeCut, imageCut) || !covers(nativeCut, snapshotCut)
            || configurationTerm < 0 || configurationIndex < -1
            || configurationIndex == -1 && configurationTerm != 0
            || configurationIndex > nativeCut.getIndex() || configurationTerm > nativeCut.getTerm()
            || configurationIndex == nativeCut.getIndex() && configurationTerm != nativeCut.getTerm()) {
          throw new IllegalArgumentException("Recovery evidence exceeds its actual native prefix");
        }
      } else if (imageCut != null || nativeCut != null || snapshotCut != null || configuration != null
          || (mode == Mode.GENESIS || mode == Mode.GENESIS_SUSPENDED) && (configurationTerm != 0 || configurationIndex != -1)) {
        throw new IllegalArgumentException("Non-recovery mode cannot carry a baseline");
      }
    }
    public static Evidence normal() { return new Evidence(Mode.NORMAL, null, null, null, 0, -1, null); }
    /** Explicit original creation authority, with no installed image/native/configuration baseline. */
    public static Evidence genesis() { return new Evidence(Mode.GENESIS, null, null, null, 0, -1, null); }
    public static Evidence suspendedGenesis() { return new Evidence(Mode.GENESIS_SUSPENDED, null, null, null, 0, -1, null); }
    public static Evidence missing() { return new Evidence(Mode.MISSING, null, null, null, 0, -1, null); }
    public static Evidence recovered(TermIndex imageCut, TermIndex nativeCut, TermIndex snapshotCut,
        long configurationTerm, long configurationIndex, RaftConfigurationProto configuration) {
      return new Evidence(Mode.RECOVERY, imageCut, nativeCut, snapshotCut,
          configurationTerm, configurationIndex, configuration);
    }
    static boolean covers(TermIndex actual, TermIndex required) {
      return actual != null && required != null && actual.getIndex() >= required.getIndex()
          && actual.getTerm() >= required.getTerm()
          && (actual.getIndex() != required.getIndex() || actual.getTerm() == required.getTerm());
    }
  }

  /**
   * Lock order is division monitor then Gate. Publication contains only an immutable assignment.
   * Publish MISSING before replacing any native/snapshot baseline, outside native, apply-lane and
   * division locks. It joins any already admitted vote/self-vote persistence before returning.
   * Core post-pause invalidation may publish in division-to-Gate order after native locks are released.
   * Publication never enters a division lock or runs callbacks/IO. Core eligibility reads use cached
   * metadata only; core holds division-to-Gate across vote/self-vote grant and Raft metadata fsync.
   * That required Raft vote-file force is the only file IO admitted under Gate.
   * Adapter settlement may publish an already prepared native activation token under Gate;
   * this changes cached foreground eligibility only and must not acquire the native apply lane. User SM role
   * callbacks, native IO and apply-lane acquisition remain outside Gate.
   */
  final class Gate {
    private record Publication(long revision, long revocationGeneration, boolean discoveryPending, Evidence evidence) { }
    record Revocation(long revision, Evidence previous) { }
    private volatile boolean sourceRetirementPending;
    private volatile Publication publication = new Publication(0, 0, false, Evidence.missing());
    /** Install before constructing the division; ordinary recovery publications cannot clear this fence. */
    public synchronized void deferDiscovery() {
      publication = new Publication(Math.incrementExact(publication.revision()),
          Math.incrementExact(publication.revocationGeneration()), true, Evidence.missing());
    }
    /** After a terminal receipt is issued, its native/CAT recipe owns every retry/restart. */
    synchronized void beginSourceRetirement() {sourceRetirementPending=true;publication=new Publication(Math.incrementExact(publication.revision()),Math.incrementExact(publication.revocationGeneration()),publication.discoveryPending(),publication.evidence());}
    /** Only a pre-receipt retirement owner may resume cohort catch-up; every old export stays revoked. */
    synchronized void resumeSourceCatchup(){if(!sourceRetirementPending)throw new IllegalStateException("Source seal is not active");sourceRetirementPending=false;publication=new Publication(Math.incrementExact(publication.revision()),Math.incrementExact(publication.revocationGeneration()),publication.discoveryPending(),publication.evidence());}
    public boolean sourceRetirementPending(){return sourceRetirementPending;}
    // The sole updater can finish admitted native work while voting/export remain revoked.
    Evidence retirementDrainEvidence(){return publication.evidence();}
    public boolean discoveryPending() { return publication.discoveryPending(); }
    /** Caller has forced the selected graph/role and qualified this completed native baseline. */
    public synchronized boolean settleDiscovery(long expectedRevision, Evidence qualified) {
      Objects.requireNonNull(qualified);
      if (!publication.discoveryPending() || publication.revision() != expectedRevision) { return false; }
      publication = new Publication(Math.incrementExact(publication.revision()),
          publication.revocationGeneration(), false, qualified);
      return true;
    }
    /** Captured before artifact qualification; an intervening MISSING publication permanently revokes it. */
    public static final class SnapshotPermit {
      private final Gate owner;
      private final long generation;
      private SnapshotPermit(Gate owner, long generation) { this.owner = owner; this.generation = generation; }
      public void requireCurrent() {
        final Publication current = owner.publication;
        if (owner.sourceRetirementPending || current.discoveryPending() || (current.evidence().mode() == Mode.MISSING || current.evidence().mode() == Mode.GENESIS || current.evidence().mode() == Mode.GENESIS_SUSPENDED) || current.revocationGeneration() != generation) {
          throw new IllegalStateException("Snapshot source eligibility was revoked");
        }
      }
      public void requireOwner(Gate expected) {
        if (owner != expected) { throw new IllegalStateException("Snapshot permit belongs to another recovery Gate"); }
        requireCurrent();
      }
    }
    public SnapshotPermit snapshotPermit() {
      final Publication current = publication;
      if (sourceRetirementPending || current.discoveryPending() || (current.evidence().mode() == Mode.MISSING || current.evidence().mode() == Mode.GENESIS || current.evidence().mode() == Mode.GENESIS_SUSPENDED)) {
        throw new IllegalStateException("Missing native recovery state cannot produce a snapshot");
      }
      return new SnapshotPermit(this, current.revocationGeneration());
    }
    public Evidence snapshot() {
      final Publication current = publication;
      return sourceRetirementPending || current.discoveryPending() ? Evidence.missing() : current.evidence();
    }
    public long revision() { return publication.revision(); }
    public long revocationGeneration() { return publication.revocationGeneration(); }
    public synchronized void publish(Evidence next) {
      Objects.requireNonNull(next);
      final long generation = (next.mode() == Mode.MISSING || next.mode() == Mode.GENESIS || next.mode() == Mode.GENESIS_SUSPENDED)
          ? Math.incrementExact(publication.revocationGeneration()) : publication.revocationGeneration();
      publication = new Publication(Math.incrementExact(publication.revision()), generation,
          publication.discoveryPending(), next);
    }
    /** Capture revision before native/file reads; stale completions cannot undo pause/replacement revocation. */
    public synchronized boolean publishIfCurrent(long expectedRevision, Evidence next) {
      if (publication.revision() != expectedRevision) { return false; }
      publish(next); return true;
    }
    synchronized Revocation revoke() {
      final Evidence previous = snapshot(); publish(Evidence.missing());
      return new Revocation(revision(), previous);
    }
    synchronized void restoreUnchanged(Revocation revoked) {
      publishIfCurrent(revoked.revision(), revoked.previous());
    }
  }

  /** One constant holder per division; this accessor and SM snapshot/applied metadata getters must never do IO or acquire native locks. */
  Gate recoveryEligibilityGate();
}
