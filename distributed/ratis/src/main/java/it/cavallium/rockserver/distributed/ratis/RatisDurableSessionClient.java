package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/** Durable exclusive user window. Journal transitions serialize; disjoint synchronous RPC owners may overlap. */
public final class RatisDurableSessionClient implements AutoCloseable {
    public static final class Pending {
        private final RatisDurableSessionClient owner; private final RatisSessionClient.Pending pending;
        private CommandResult result; private boolean consumed,executing;
        private Pending(RatisDurableSessionClient owner,RatisSessionClient.Pending pending,CommandResult result,boolean consumed) {
            this.owner=owner; this.pending=pending; this.result=result; this.consumed=consumed;
        }
        public DistributedCommand command() { return pending.command(); }
        public RangeDescriptor descriptor() { return pending.descriptor(); }
        public boolean consumed() { owner.lane.lock(); try { return consumed; } finally { owner.lane.unlock(); } }
        /** Only durably saved results are exposed. Consumed holes remain marked until their contiguous prefix drains. */
        public CommandResult savedResult() { owner.lane.lock(); try { return result; } finally { owner.lane.unlock(); } }
    }
    private final RatisSessionClient client; private final RatisSessionClient.Limits limits; private final SessionWindowJournal journal;
    private final ReentrantLock lane=new ReentrantLock(); private final TreeMap<Long,Pending> retained=new TreeMap<>();
    private int active; private boolean closed,fenced,journalClosed;
    private RatisDurableSessionClient(RatisSessionClient client,RatisSessionClient.Limits limits,SessionWindowJournal journal) {
        this.client=client; this.limits=limits; this.journal=journal;
        for (var entry:client.retained()) retained.put(entry.command().operationId().sequence(),new Pending(this,entry,client.resolved(entry),client.consumed(entry)));
    }
    /** Only for one newly issued, exclusively owned grant. Never recreate a missing used-grant journal. */
    public static RatisDurableSessionClient create(Path path,RatisRoutedClient routed,SessionEpochLedger issuance,
            SessionEpochLedger.Grant grant,RatisSessionClient.Limits limits) throws IOException {
        var client=new RatisSessionClient(routed,issuance,grant,limits); var journal=SessionWindowJournal.open(path,routed,true);
        try { journal.write(grant,limits,client.snapshot()); return new RatisDurableSessionClient(client,limits,journal); }
        catch (IOException | RuntimeException | Error failure) { try { journal.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    /** Reopen exact pending identities even after grant expiry; native authority decides retry outcomes. */
    public static RatisDurableSessionClient reopen(Path path,RatisRoutedClient routed) throws IOException {
        var journal=SessionWindowJournal.open(path,routed,false);
        try { var saved=journal.read(); return new RatisDurableSessionClient(RatisSessionClient.restore(routed,saved.grant(),saved.limits(),saved.state()),saved.limits(),journal); }
        catch (IOException | RuntimeException | Error failure) { try { journal.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure; }
    }
    public SessionEpochLedger.Grant grant() { return client.grant(); }
    RatisSessionClient.Limits limits() { return limits; }
    boolean pristine() throws IOException {
        lane.lock(); try { requireOpen(); var state=client.snapshot();
            return state.nextSequence()==0 && state.acknowledgedThrough()==-1 && !state.exhausted() && state.entries().isEmpty();
        } finally { lane.unlock(); }
    }
    public List<Pending> pending() throws IOException { lane.lock(); try { requireOpen(); return List.copyOf(retained.values()); } finally { lane.unlock(); } }
    public long acknowledgedThrough() throws IOException { lane.lock(); try { requireOpen(); return client.acknowledgedThrough(); } finally { lane.unlock(); } }
    public Pending prepare(List<Mutation> mutations,List<KeyPredicate> predicates) throws IOException {
        lane.lock(); try { requireOpen(); var pending=client.prepare(mutations,predicates); checkpoint(); return retain(pending); } finally { lane.unlock(); }
    }
    public Pending prepare(List<Mutation> mutations) throws IOException { return prepare(mutations,List.of()); }
    public Pending prepareAcknowledgement(UUID column,byte[] routingKey) throws IOException {
        lane.lock(); try { requireOpen(); var pending=client.prepareAcknowledgement(column,routingKey); checkpoint(); return retain(pending); } finally { lane.unlock(); }
    }
    private Pending retain(RatisSessionClient.Pending entry) {
        var pending=new Pending(this,entry,null,false); retained.put(entry.command().operationId().sequence(),pending); return pending;
    }
    public CommandResult execute(Pending pending) throws IOException {
        lane.lock();
        try {
            requireOpen(); owned(pending); if (pending.result!=null) return pending.result;
            if (pending.executing) throw new IOException("This durable operation is already executing"); pending.executing=true; active++;
        } finally { lane.unlock(); }
        try {
            var result=client.execute(pending.pending); lane.lock();
            try { requireOpen(); checkpoint(); pending.result=result; return result; } finally { lane.unlock(); }
        } finally { lane.lock(); try { pending.executing=false; active--; } finally { lane.unlock(); } }
    }
    /** Application must durably consume the returned outcome first. Persist consumption before any later piggyback ACK. */
    public void acknowledge(Pending pending) throws IOException {
        lane.lock();
        try {
            requireOpen(); owned(pending); if (pending.result==null) throw new IllegalStateException("No durably resolved outcome to consume");
            if (pending.consumed) return; client.acknowledge(pending.pending); checkpoint(); pending.consumed=true;
            retained.headMap(client.acknowledgedThrough(),true).clear();
        } finally { lane.unlock(); }
    }
    /** Separate durable maintenance owner still controls first-use binding identities. */
    public RatisRoutedClient.PreparedCommand prepareBinding(RatisSessionEpochs epochs,OperationId issuer,Pending pending,long acknowledgedThrough) throws IOException {
        lane.lock(); try { requireOpen(); owned(pending); active++; } finally { lane.unlock(); }
        try { return client.prepareBinding(epochs,issuer,pending.pending,acknowledgedThrough); }
        finally { lane.lock(); try { active--; } finally { lane.unlock(); } }
    }
    private void checkpoint() throws IOException {
        try { journal.write(client.grant(),limits,client.snapshot()); } catch (IOException | RuntimeException failure) { fenced=true; throw failure; }
    }
    private void owned(Pending pending) { if (pending==null || pending.owner!=this) throw new IllegalArgumentException("Operation belongs to another durable session"); }
    private void requireOpen() throws IOException { if (closed || fenced) throw new IOException("Durable session is closed/fenced; reopen its journal"); }
    @Override public void close() throws IOException {
        lane.lock(); try { if (journalClosed) return; if (active!=0) throw new IOException("Durable session RPC work is active"); closed=true; journal.close(); journalClosed=true; } finally { lane.unlock(); }
    }
}
