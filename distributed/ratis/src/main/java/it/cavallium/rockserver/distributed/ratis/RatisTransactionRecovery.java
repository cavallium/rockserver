package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;

/** Recovery takeover: retry uncertain intents, abort otherwise-undecided homes, finish terminal decisions, never consume. */
public final class RatisTransactionRecovery implements AutoCloseable {
    @FunctionalInterface public interface ControlIds {
        OperationId nextAfter(OperationId last) throws IOException;
    }
    public enum Action { RETRIED_HOME, REGISTRATION_ATTEMPTED, ABORT_ATTEMPTED, FINALIZED, ACK_ATTEMPTED, VERIFIED, HOME_RETIRED, COMPLETE }
    /** Action identifies the attempted quantum; home CAS success/conflict remains in the journal's homeResult. */
    public record Step(Action action,UUID participant,boolean complete) {}

    private final RatisDurableTransaction journal;
    private final RatisTransactionStatuses homes;
    private final RatisTransactionBindings data;
    private final ControlIds controls;
    private final List<UUID> participants;
    private int cursor;
    private boolean active,closed;

    public RatisTransactionRecovery(RatisDurableTransaction journal,RatisTransactionStatuses homes,RatisTransactionBindings data,
            ControlIds controls) throws IOException {
        this.journal=Objects.requireNonNull(journal); this.homes=Objects.requireNonNull(homes); this.data=Objects.requireNonNull(data);
        this.controls=Objects.requireNonNull(controls);
        journal.requireClients(homes,data);
        participants=journal.snapshot().initial().participants().stream().map(TransactionStatus.Participant::groupId).toList();
    }

    /** At most one transaction write. A failed participant advances the cursor so the next step can visit another. */
    public Step step() throws IOException { return perform(false); }
    /** Terminal decisions are immutable; a healthy coordinator has already refreshed the home for this step. */
    Step stepTerminal() throws IOException { return perform(true); }
    private Step perform(boolean terminalOnly) throws IOException {
        synchronized (this) {
            if (closed) throw new IOException("Transaction recovery is closed");
            if (active) throw new IOException("Transaction recovery step already active");
            active=true;
        }
        try {
            if (journal.snapshot().homeRetirementProof()!=null) return new Step(Action.HOME_RETIRED,null,true);
            if (!terminalOnly) return advance();
            var snapshot=journal.snapshot();
            if (snapshot.observedHome()==null || !snapshot.observedHome().terminal() || snapshot.homeResult()==null && !snapshot.homeSuperseded()) throw new IOException("Terminal completion requires a verified decision and resolved home request");
            return finishTerminal(snapshot);
        }
        catch (RatisHomeRetiredException retired) { return new Step(Action.HOME_RETIRED,null,true); }
        finally { synchronized (this) { active=false; } }
    }

    private Step advance() throws IOException {
        journal.refreshHome();
        journal.resolveHomeFromObserved();
        var snapshot=journal.snapshot();
        if (snapshot.pendingHome()!=null && snapshot.homeResult()==null && !snapshot.homeSuperseded()) {
            journal.executeHome();
            return result(Action.RETRIED_HOME,null);
        }
        var home=snapshot.observedHome();
        if (home==null) {
            journal.queueHome(homes.prepare(nextControl(snapshot),journal.registrationExpected(),snapshot.initial()));
            journal.executeHome();
            return result(Action.REGISTRATION_ATTEMPTED,null);
        }
        if(home.reserved()) {
            if(!journal.noEffectReservationState(home))throw new IOException("HOME reservation has participant effects");
            journal.queueRecovery(home.abortReservation(homes.sourceClosedWithoutCredit(home)?TransactionStatus.NoEffectOutcome.SOURCE_CLOSED:TransactionStatus.NoEffectOutcome.SESSION_EXPIRED));journal.executeHome();return result(Action.ABORT_ATTEMPTED,null);
        }
        if (home.phase()==TransactionStatus.Phase.PREPARING) {
            journal.queueRecovery(home.decide(TransactionStatus.Phase.ABORT));
            journal.executeHome();
            return result(Action.ABORT_ATTEMPTED,null);
        }
        return finishTerminal(snapshot);
    }
    private Step finishTerminal(RatisDurableTransaction.Snapshot snapshot) throws IOException {
        var home=snapshot.observedHome();
        if(journal.noEffectReservationState(home) && home.terminal())return new Step(Action.COMPLETE,null,true);
        if (complete(snapshot)) return new Step(Action.COMPLETE,null,true);
        UUID group=participants.get(cursor);
        cursor=(cursor+1)%participants.size();
        var progress=snapshot.participant(group);
        if (progress.outcome()==null) progress=journal.resolveParticipant(group);
        if (progress.outcome()==null && progress.retirementProof()==null) {
            var route=snapshot.pendingHome().descriptor();
            var value=new ParticipantFinalization(route.groupId(),route.rangeEpoch(),group,progress.intent().finalizeControl(),home);
            journal.freezeFinalization(data.restoreFinalization(value,progress.intent().route()));
            try { journal.executeFinalization(group); } catch (RatisParticipantRetiredException retired) { /* The journal forced the proof before exposure. */ }
            return result(Action.FINALIZED,group);
        }
        // A fresh query may accept another coordinator's winning proof without claiming this journal's RPC succeeded.
        snapshot=journal.snapshot(); home=snapshot.observedHome();
        var participant=home.participants().stream().filter(p -> p.groupId().equals(group)).findFirst().orElseThrow();
        if (!participant.finalized()) {
            journal.queueRecovery(home.finalized(group));
            journal.executeHome();
            return result(Action.ACK_ATTEMPTED,group);
        }
        return result(Action.VERIFIED,group);
    }
    private OperationId nextControl(RatisDurableTransaction.Snapshot snapshot) throws IOException {
        return Objects.requireNonNull(controls.nextAfter(snapshot.lastHome()),"Missing recovery operation identity");
    }
    private Step result(Action action,UUID participant) throws IOException { return new Step(action,participant,complete(journal.snapshot())); }
    private static boolean complete(RatisDurableTransaction.Snapshot snapshot) {
        return snapshot.observedHome()!=null && snapshot.observedHome().fullyFinalized()
                && snapshot.participants().stream().allMatch(p -> p.outcome()!=null || p.retirementProof()!=null
                        && (snapshot.observedHome().phase()==TransactionStatus.Phase.ABORT || snapshot.observedHome().gcEligible()));
    }
    /** Borrowed clients/journal remain open. Active RPC and file ownership must settle before close can succeed. */
    @Override public synchronized void close() throws IOException {
        if (active) throw new IOException("Recovery step active; ownership retained");
        closed=true;
    }
}
