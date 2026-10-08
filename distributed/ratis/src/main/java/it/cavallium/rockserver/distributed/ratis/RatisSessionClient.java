package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;

import java.io.IOException;
import java.util.List;
import java.util.Objects;
import java.util.TreeMap;
import java.util.UUID;

/** Bounded client sequence window. The caller owns the routed client and durably consumes results before acknowledging. */
public final class RatisSessionClient {
    public record Limits(int outstanding, long commandBytes, long resultBytes) {
        public Limits {
            if (outstanding < 1 || outstanding > 4096 || commandBytes < 1 || resultBytes < 8) {
                throw new IllegalArgumentException("Invalid session window limits");
            }
        }
        public static Limits defaults() { return new Limits(32,16L*1024*1024,16L*1024*1024); }
    }
    public static final class Pending {
        private final RatisSessionClient owner;
        private final RatisRoutedClient.PreparedCommand prepared;
        private final int commandBytes, resultBytes;
        private CommandResult result;
        private boolean executing, consumed;
        private Pending(RatisSessionClient owner,RatisRoutedClient.PreparedCommand prepared,int commandBytes,int resultBytes) {
            this.owner=owner; this.prepared=prepared; this.commandBytes=commandBytes; this.resultBytes=resultBytes;
        }
        public DistributedCommand command() { return prepared.command(); }
        public RangeDescriptor descriptor() { return prepared.descriptor(); }
    }
    record WindowEntry(RatisRoutedClient.PreparedCommand prepared,CommandResult result,boolean consumed) {}
    record WindowState(long nextSequence,long acknowledgedThrough,boolean exhausted,List<WindowEntry> entries) {
        WindowState { entries=List.copyOf(entries); }
    }
    private final RatisRoutedClient routed;
    private final SessionEpochLedger.Grant grant;
    private final Limits limits;
    private final TreeMap<Long,Pending> pending = new TreeMap<>();
    private long nextSequence, acknowledgedThrough=-1, commandBytes, resultBytes;
    private boolean exhausted;

    /**
     * One exclusive sequence owner for a newly issued grant. Never reconstruct with a previously used grant:
     * use RatisDurableSessionClient for recovery, or issue a new grant on restart.
     * The verified issuance image does not substitute for range-side lifetime authority.
     */
    public RatisSessionClient(RatisRoutedClient routed,SessionEpochLedger ledger,SessionEpochLedger.Grant grant,Limits limits) {
        this(routed,grant,limits);
        var directory=routed.directory();
        if (!ledger.clusterId().equals(directory.clusterId()) || !ledger.databaseId().equals(directory.databaseId())
                || !ledger.isLive(new OperationId(grant.sessionId(),grant.epoch(),0))
                || !ledger.grants().get((int)(grant.epoch()-ledger.minimumEpoch())).equals(grant)) {
            throw new IllegalArgumentException("Session grant is not live in this database's issuance image");
        }
    }
    private RatisSessionClient(RatisRoutedClient routed,SessionEpochLedger.Grant grant,Limits limits) {
        this.routed=Objects.requireNonNull(routed); this.grant=Objects.requireNonNull(grant); this.limits=Objects.requireNonNull(limits);
    }
    synchronized WindowState snapshot() {
        return new WindowState(nextSequence,acknowledgedThrough,exhausted,pending.values().stream()
                .map(entry -> new WindowEntry(entry.prepared,entry.result,entry.consumed)).toList());
    }
    /** Journal recovery is separate from fresh issuance; an expired grant may still have saved outcomes to resolve. */
    static RatisSessionClient restore(RatisRoutedClient routed,SessionEpochLedger.Grant grant,Limits limits,WindowState state) {
        var client=new RatisSessionClient(routed,grant,limits);
        long ack=state.acknowledgedThrough(), next=state.nextSequence();
        if (ack< -1 || next<0 || state.exhausted() && next!=Long.MAX_VALUE || ack>next
                || ack==Long.MAX_VALUE && !state.exhausted()) throw new IllegalArgumentException("Invalid recovered sequence state");
        long count=ack==Long.MAX_VALUE ? 0 : next-(ack+1);
        if (count<0 || count>limits.outstanding()) throw new IllegalArgumentException("Recovered sequence window exceeds bounds");
        if (state.exhausted() && ack!=Long.MAX_VALUE) {
            if (count==limits.outstanding()) throw new IllegalArgumentException("Recovered final sequence exceeds window"); count++;
        }
        if (count!=state.entries().size()) throw new IllegalArgumentException("Recovered sequence window is not contiguous");
        long sequence=ack==Long.MAX_VALUE ? Long.MAX_VALUE : ack+1;
        for (var saved:state.entries()) {
            var prepared=routed.restoreSessionCommand(saved.prepared().command(),saved.prepared().descriptor()); var command=prepared.command(); var id=command.operationId();
            if (!id.sessionId().equals(grant.sessionId()) || id.epoch()!=grant.epoch() || id.sequence()!=sequence
                    || command.acknowledgedThrough()>ack || saved.consumed() && saved.result()==null
                    || client.pending.isEmpty() && saved.consumed()) throw new IllegalArgumentException("Invalid recovered command/outcome ownership");
            int bytes=DistributedCommandCodec.encodedSize(command), result=maximumResult(command);
            if (bytes>limits.commandBytes()-client.commandBytes || result>limits.resultBytes()-client.resultBytes) throw new IllegalArgumentException("Recovered byte window exceeds limits");
            if (saved.result()!=null) validateResult(command,saved.result());
            var entry=new Pending(client,prepared,bytes,result); entry.result=saved.result(); entry.consumed=saved.consumed();
            client.pending.put(sequence,entry); client.commandBytes+=bytes; client.resultBytes+=result;
            if (sequence!=Long.MAX_VALUE) sequence++;
        }
        client.nextSequence=next; client.acknowledgedThrough=ack; client.exhausted=state.exhausted(); return client;
    }
    synchronized java.util.List<Pending> retained() { return java.util.List.copyOf(pending.values()); }
    synchronized CommandResult resolved(Pending entry) { requireOwner(entry); return entry.result; }
    synchronized boolean consumed(Pending entry) { requireOwner(entry); return entry.consumed; }
    private static int maximumResult(DistributedCommand command) {
        return command.mutations().stream().allMatch(m -> m.returnMode()==Mutation.ReturnMode.NONE) ? 8 : CommandResult.MAX_BYTES;
    }
    private static void validateResult(DistributedCommand command,CommandResult result) {
        long count=command.mutations().stream().filter(m -> m.returnMode()!=Mutation.ReturnMode.NONE).count();
        if (result.status()==CommandResult.Status.SUCCESS && result.valueCount()!=count) throw new IllegalArgumentException("Outcome differs from command result plan");
    }
    public SessionEpochLedger.Grant grant() { return grant; }
    public synchronized int outstanding() { return pending.size(); }
    public synchronized long acknowledgedThrough() { return acknowledgedThrough; }

    public synchronized Pending prepare(List<Mutation> mutations,List<KeyPredicate> predicates) {
        requireCapacity();
        var id=new OperationId(grant.sessionId(),grant.epoch(),nextSequence);
        var prepared=acknowledgedThrough < 0 ? routed.prepare(id,mutations,predicates)
                : routed.prepare(id,mutations,predicates,acknowledgedThrough);
        return retain(prepared);
    }
    public Pending prepare(List<Mutation> mutations) { return prepare(mutations,List.of()); }

    /** Explicitly drains receipts on a selected range even when no further data command is needed there. */
    public synchronized Pending prepareAcknowledgement(UUID column,byte[] routingKey) {
        requireCapacity();
        if (acknowledgedThrough < 0) throw new IllegalStateException("No consumed contiguous results to acknowledge");
        return retain(routed.prepareAcknowledgement(new OperationId(grant.sessionId(),grant.epoch(),nextSequence),column,routingKey,acknowledgedThrough));
    }
    private void requireCapacity() {
        if (exhausted) throw new IllegalStateException("Session sequence space exhausted");
        if (pending.size() >= limits.outstanding()) throw new IllegalStateException("Session outstanding window exhausted");
    }
    private Pending retain(RatisRoutedClient.PreparedCommand prepared) {
        int encoded=DistributedCommandCodec.encodedSize(prepared.command());
        int maximumResult=maximumResult(prepared.command());
        if (encoded > limits.commandBytes()-commandBytes || maximumResult > limits.resultBytes()-resultBytes) {
            throw new IllegalStateException("Session command/result byte window exhausted");
        }
        var entry=new Pending(this,prepared,encoded,maximumResult);
        pending.put(nextSequence,entry); commandBytes+=encoded; resultBytes+=maximumResult;
        if (nextSequence==Long.MAX_VALUE) exhausted=true; else nextSequence++;
        return entry;
    }

    /** Prepare first-use binding for a pending route. A separate maintenance sequencer owns the issuer identity and retry. */
    public RatisRoutedClient.PreparedCommand prepareBinding(RatisSessionEpochs epochs,OperationId issuer,Pending target,long acknowledgedThrough) throws IOException {
        synchronized (this) { requireOwner(target); }
        var command=epochs.prepareSessionBinding(routed.directory(),target.descriptor().groupId(),issuer,grant,limits.outstanding(),acknowledgedThrough);
        if (command.rangeEpoch()!=target.command().rangeEpoch()) throw new IllegalStateException("Pending route changed incarnation before binding");
        return routed.prepareSessionControl(command);
    }

    /** Failure leaves the immutable command in the window for explicit same-identity retry. */
    public CommandResult execute(Pending entry) throws IOException {
        synchronized (this) {
            requireOwner(entry);
            if (entry.result != null) return entry.result;
            if (entry.executing) throw new IOException("This session operation is already executing");
            entry.executing=true;
        }
        try {
            CommandResult result=routed.execute(entry.prepared);
            try { validateResult(entry.command(),result); } catch (IllegalArgumentException malformed) { throw new IOException("Invalid session outcome",malformed); }
            synchronized (this) { entry.result=result; }
            return result;
        } finally {
            synchronized (this) { entry.executing=false; }
        }
    }
    /** Call only after the application has durably consumed the returned outcome, including conflicts. */
    public synchronized void acknowledge(Pending entry) {
        requireOwner(entry);
        if (entry.result==null) throw new IllegalStateException("Unresolved operation cannot be acknowledged");
        entry.consumed=true;
        while (!pending.isEmpty() && pending.firstEntry().getValue().consumed) {
            var first=pending.pollFirstEntry();
            acknowledgedThrough=first.getKey();
            commandBytes-=first.getValue().commandBytes; resultBytes-=first.getValue().resultBytes;
        }
    }
    private void requireOwner(Pending entry) {
        if (entry==null || entry.owner!=this) throw new IllegalArgumentException("Operation belongs to another session client");
    }
}
