package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;

/** Healthy forward policy over frozen accepted intents; terminal completion shares the recovery policy. Never consumes. */
public final class RatisTransactionCoordinator implements AutoCloseable {
    @FunctionalInterface public interface PreparationAuthority {
        Authorization authorize(UUID transaction,RatisDurableTransaction.ParticipantProgress participant) throws IOException;
    }
    /** The provider retains ownership of pins and preserves claimed/uncertain proposal ownership on close. */
    public record Authorization(PreparedParticipant.Preparation value,boolean negative,AutoCloseable owner) implements AutoCloseable {
        public Authorization { Objects.requireNonNull(value); Objects.requireNonNull(owner); }
        @Override public void close() throws Exception { owner.close(); }
    }
    public enum Action {
        RETRIED_HOME, REGISTRATION_ATTEMPTED, PREPARED, PREPARATION_REJECTED, VOTE_ATTEMPTED, COMMIT_ATTEMPTED,
        ABORT_ATTEMPTED, FINALIZED, ACK_ATTEMPTED, VERIFIED, COMPLETE
    }
    /** CAS actions identify attempts; PREPARED/REJECTED identify durable votes. Completion never implies consumption. */
    public record Step(Action action,UUID participant,boolean complete) {}

    private final RatisDurableTransaction journal;
    private final RatisTransactionStatuses homes;
    private final RatisRoutedClient data;
    private final RatisTransactionRecovery.ControlIds controls;
    private final PreparationAuthority authority;
    private final RatisTransactionRecovery terminal;
    private final List<UUID> participants;
    private int cursor;
    private boolean active,closed;

    public RatisTransactionCoordinator(RatisDurableTransaction journal,RatisTransactionStatuses homes,RatisRoutedClient data,
            RatisTransactionRecovery.ControlIds controls,PreparationAuthority authority,RatisTransactionRecovery.FinalizationAuthority finalizations) throws IOException {
        this.journal=Objects.requireNonNull(journal); this.homes=Objects.requireNonNull(homes); this.data=Objects.requireNonNull(data);
        this.controls=Objects.requireNonNull(controls); this.authority=Objects.requireNonNull(authority);
        journal.requireClients(homes,data);
        participants=journal.snapshot().initial().participants().stream().map(TransactionStatus.Participant::groupId).toList();
        terminal=new RatisTransactionRecovery(journal,homes,data,controls,Objects.requireNonNull(finalizations));
    }
    /** One active step, at most one transaction write; an unavailable participant does not starve later participants. */
    public Step step() throws IOException {
        synchronized (this) {
            if (closed) throw new IOException("Transaction coordinator is closed");
            if (active) throw new IOException("Transaction coordinator step already active");
            active=true;
        }
        try { return advance(); }
        finally { synchronized (this) { active=false; } }
    }
    private Step advance() throws IOException {
        journal.refreshHome(); journal.resolveHomeFromObserved();
        var snapshot=journal.snapshot();
        if (snapshot.pendingHome()!=null && snapshot.homeResult()==null && !snapshot.homeSuperseded()) {
            journal.executeHome(); return result(Action.RETRIED_HOME,null);
        }
        var home=snapshot.observedHome();
        if (home==null) {
            journal.queueHome(homes.prepare(nextControl(snapshot),null,snapshot.initial())); journal.executeHome();
            return result(Action.REGISTRATION_ATTEMPTED,null);
        }
        if (home.phase()!=TransactionStatus.Phase.PREPARING) {
            var step=terminal.stepTerminal(); return new Step(Action.valueOf(step.action().name()),step.participant(),step.complete());
        }
        if (snapshot.participants().stream().anyMatch(p -> p.vote()!=null && !p.vote().prepared())) return decide(snapshot,TransactionStatus.Phase.ABORT);
        boolean ready=true;
        for (var participant:home.participants()) {
            var vote=snapshot.participant(participant.groupId()).vote();
            if (vote==null || !vote.prepared() || participant.prepared()==null) { ready=false; continue; }
            if (!vote.position().equals(participant.prepared())) throw new IOException("Home vote differs from verified native vote");
        }
        if (ready) return decide(snapshot,TransactionStatus.Phase.COMMIT);
        UUID group=nextIncomplete(snapshot);
        var progress=journal.resolveParticipant(group);
        var participant=home.participants().stream().filter(p -> p.groupId().equals(group)).findFirst().orElseThrow();
        if (participant.prepared()!=null) {
            var vote=progress.vote();
            if (vote==null || !vote.prepared() || !participant.prepared().equals(vote.position())) throw new IOException("Home reported a prepared vote without matching native proof");
        }
        if (progress.vote()!=null) {
            if (!progress.vote().prepared()) return decide(journal.snapshot(),TransactionStatus.Phase.ABORT);
            if (participant.prepared()==null) {
                snapshot=journal.snapshot(); home=snapshot.observedHome();
                var next=home.prepared(group,progress.intent().acceptedBusiness().payloadDigest(),progress.vote().position());
                journal.queueHome(homes.prepare(nextControl(snapshot),home,next)); journal.executeHome();
                return result(Action.VOTE_ATTEMPTED,group);
            }
            return result(Action.VERIFIED,group);
        }
        final Authorization authorization;
        try { authorization=Objects.requireNonNull(authority.authorize(home.transactionId(),progress)); }
        catch (RatisPreparationUnavailableException unavailable) { return decide(journal.snapshot(),TransactionStatus.Phase.ABORT); }
        try (authorization) {
            // Restore validates the original route; freeze additionally checks exact accepted business/control/registration.
            journal.freezePreparation(data.restoreParticipant(authorization.value(),progress.intent().route(),authorization.negative()));
            var vote=journal.executePreparation(group);
            return result(vote.prepared() ? Action.PREPARED : Action.PREPARATION_REJECTED,group);
        } catch (IOException failure) { throw failure; }
        catch (Exception failure) { throw new IOException("Preparation authority failed",failure); }
    }
    private UUID nextIncomplete(RatisDurableTransaction.Snapshot snapshot) throws IOException {
        for (int i=0;i<participants.size();i++) {
            UUID group=participants.get(cursor); cursor=(cursor+1)%participants.size();
            var progress=snapshot.participant(group);
            var participant=snapshot.observedHome().participants().stream().filter(p -> p.groupId().equals(group)).findFirst().orElseThrow();
            if (progress.vote()==null || participant.prepared()==null) return group;
        }
        throw new IOException("No incomplete participant but transaction is not ready");
    }
    private Step decide(RatisDurableTransaction.Snapshot snapshot,TransactionStatus.Phase decision) throws IOException {
        var home=snapshot.observedHome();
        if (home==null || home.phase()!=TransactionStatus.Phase.PREPARING) throw new IOException("Home changed before decision");
        if (decision==TransactionStatus.Phase.ABORT) journal.queueRecovery(home.decide(decision));
        else journal.queueHome(homes.prepare(nextControl(snapshot),home,home.decide(decision)));
        journal.executeHome();
        return result(decision==TransactionStatus.Phase.COMMIT ? Action.COMMIT_ATTEMPTED : Action.ABORT_ATTEMPTED,null);
    }
    private OperationId nextControl(RatisDurableTransaction.Snapshot snapshot) throws IOException {
        return Objects.requireNonNull(controls.nextAfter(snapshot.lastHome()),"Missing coordinator operation identity");
    }
    private Step result(Action action,UUID group) throws IOException {
        var snapshot=journal.snapshot();
        boolean complete=snapshot.observedHome()!=null && snapshot.observedHome().fullyFinalized() && snapshot.participants().stream().allMatch(p -> p.outcome()!=null);
        return new Step(action,group,complete);
    }
    /** Pins, authority providers, journal and clients are borrowed. Only this policy's terminal helper is closed. */
    @Override public synchronized void close() throws IOException {
        if (active) throw new IOException("Coordinator step active; ownership retained");
        if (closed) return;
        terminal.close(); closed=true;
    }
}
