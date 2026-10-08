package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;

/** Recover one existing native preparation. Completion is local; other participants and consumption remain independent. */
public final class RatisParticipantRecovery implements AutoCloseable {
    public enum Action { WAITING, RETRIED_HOME, ABORT_ATTEMPTED, FINALIZED, VERIFIED, ACK_ATTEMPTED, COMPLETE }
    public record Step(Action action,boolean complete) {}
    private final RatisDurableParticipantRecovery journal;
    private final RatisTransactionStatuses homes;
    private final RatisRoutedClient data;
    private final RatisTransactionRecovery.ControlIds controls;
    private final RatisTransactionRecovery.FinalizationAuthority authority;
    private final boolean takeover;
    private boolean active,closed;

    /** takeover authorizes new ABORT attempts; previously frozen requests are resolved regardless of this policy. */
    public RatisParticipantRecovery(RatisDurableParticipantRecovery journal,RatisTransactionStatuses homes,RatisRoutedClient data,
            RatisTransactionRecovery.ControlIds controls,RatisTransactionRecovery.FinalizationAuthority authority,boolean takeover) throws IOException {
        this.journal=Objects.requireNonNull(journal); this.homes=Objects.requireNonNull(homes); this.data=Objects.requireNonNull(data);
        this.controls=Objects.requireNonNull(controls); this.authority=Objects.requireNonNull(authority); this.takeover=takeover; journal.requireClients(homes,data);
    }
    /** One synchronous CONTROL quantum, at most one state-changing RPC; clients and journal are borrowed. */
    public Step step() throws IOException {
        synchronized (this) { if (closed || active) throw new IOException("Participant recovery closed or step active"); active=true; }
        try { return advance(); } finally { synchronized (this) { active=false; } }
    }
    private Step advance() throws IOException {
        journal.refreshHome(); journal.resolveHomeFromObserved(); var snapshot=journal.snapshot();
        if (snapshot.pendingHome()!=null && snapshot.homeResult()==null && !snapshot.homeSuperseded()) { journal.executeHome(); return result(Action.RETRIED_HOME); }
        var home=snapshot.observedHome();
        if (home.phase()==TransactionStatus.Phase.PREPARING) {
            if (!takeover) return result(Action.WAITING);
            return transition(snapshot,home.decide(TransactionStatus.Phase.ABORT),Action.ABORT_ATTEMPTED);
        }
        UUID group=snapshot.participantRoute().groupId();
        if (snapshot.outcome()==null) {
            if (journal.resolveParticipant()!=null) return result(Action.VERIFIED);
            snapshot=journal.snapshot();
            // A forced append receipt only needs native proof lookup; it must not require new write authority.
            if (snapshot.finalizationReceipt()!=null) { journal.executeFinalization(); return result(Action.VERIFIED); }
            try (var grant=Objects.requireNonNull(authority.authorize(group,home.transactionId(),snapshot.finalizeControl()))) {
                journal.freezeFinalization(data.restoreFinalization(grant.value(),snapshot.participantRoute())); journal.executeFinalization();
            } catch (IOException failure) { throw failure; } catch (Exception failure) { throw new IOException("Participant finalization authority failed",failure); }
            return result(Action.FINALIZED);
        }
        if (!home.participants().stream().filter(p -> p.groupId().equals(group)).findFirst().orElseThrow().finalized()) return transition(snapshot,home.finalized(group),Action.ACK_ATTEMPTED);
        return result(Action.COMPLETE);
    }
    private Step transition(RatisDurableParticipantRecovery.Snapshot snapshot,TransactionStatus next,Action action) throws IOException {
        journal.queueHome(homes.prepare(Objects.requireNonNull(controls.nextAfter(snapshot.lastHome())),snapshot.observedHome(),next)); journal.executeHome(); return result(action);
    }
    private Step result(Action action) throws IOException {
        var snapshot=journal.snapshot(); var home=snapshot.observedHome(); UUID group=snapshot.participantRoute().groupId();
        return new Step(action,snapshot.outcome()!=null && home.phase()!=TransactionStatus.Phase.PREPARING && home.participants().stream().filter(p -> p.groupId().equals(group)).findFirst().orElseThrow().finalized());
    }
    @Override public synchronized void close() throws IOException { if (active) throw new IOException("Participant recovery step active; ownership retained"); closed=true; }
}
