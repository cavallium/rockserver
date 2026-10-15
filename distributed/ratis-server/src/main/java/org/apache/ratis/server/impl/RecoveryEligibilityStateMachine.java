package org.apache.ratis.server.impl;

import java.util.Objects;
import org.apache.ratis.proto.RaftProtos.RaftConfigurationProto;
import org.apache.ratis.server.protocol.TermIndex;
import org.apache.ratis.statemachine.StateMachine;

/** Optional durable native recovery baseline for voting, independent of final USER readiness. */
public interface RecoveryEligibilityStateMachine extends StateMachine {
  enum Mode { NORMAL, MISSING, RECOVERY }

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
      } else if (imageCut != null || nativeCut != null || snapshotCut != null || configuration != null) {
        throw new IllegalArgumentException("Non-recovery mode cannot carry a baseline");
      }
    }
    public static Evidence normal() { return new Evidence(Mode.NORMAL, null, null, null, 0, -1, null); }
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
   * Never acquire a division lock, run callbacks, or perform native/file IO while holding Gate.
   */
  final class Gate {
    private record Publication(long revision, Evidence evidence) { }
    record Revocation(long revision, Evidence previous) { }
    private volatile Publication publication = new Publication(0, Evidence.missing());
    public Evidence snapshot() { return publication.evidence(); }
    public long revision() { return publication.revision(); }
    public synchronized void publish(Evidence next) {
      publication = new Publication(Math.incrementExact(publication.revision()), Objects.requireNonNull(next));
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
