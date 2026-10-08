package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.*;

/** Durable catalogue clock CAS stream. The caller owns the borrowed epoch facade and client. */
public final class RatisCatalogueClock implements AutoCloseable {
    private static final int MAGIC=0x5243434a, MAX_JOURNAL=2*SessionEpochLedger.MAX_BYTES+4096;
    public enum Outcome { OBSERVED, APPLIED, CONFLICT, EXPIRED }
    public record Tick(Outcome outcome,SessionAuthority authority) {}
    private record State(UUID session,long sequence,long acknowledged,SessionAuthority observed,DistributedCommand pending) {}
    private final Path journal;
    private final RatisSessionEpochs epochs;
    private final byte[] identity;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private State state;
    private boolean busy,closed,fenced;

    /** Parent directory must exist on durable storage; journal and stable .lock/.pending files belong to this owner. */
    public RatisCatalogueClock(Path journal,RatisSessionEpochs epochs) throws IOException {
        this.journal=journal.toAbsolutePath(); this.epochs=Objects.requireNonNull(epochs); identity=epochs.identityDigest();
        if (!Files.isDirectory(this.journal.getParent())) throw new IOException("Catalogue clock journal parent is absent");
        lockChannel=FileChannel.open(this.journal.resolveSibling(this.journal.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired=null;
        try {
            try { acquired=lockChannel.tryLock(); }
            catch (java.nio.channels.OverlappingFileLockException owned) { throw new IOException("Catalogue clock journal already owned",owned); }
            if (acquired==null) throw new IOException("Catalogue clock journal already owned");
            lock=acquired;
            if (Files.exists(this.journal)) {
                var input=new DataInputStream(new ByteArrayInputStream(SessionJournalFiles.read(this.journal,MAX_JOURNAL)));
                if (input.readInt()!=MAGIC || input.readInt()!=1 || !Arrays.equals(input.readNBytes(32),identity)) {
                    throw new IOException("Wrong catalogue clock journal identity/version");
                }
                state=decode(input);
                if (input.available()!=0) throw new IOException("Trailing catalogue clock journal data");
                SessionJournalFiles.force(this.journal); SessionJournalFiles.force(this.journal.getParent());
            } else {
                state=new State(UUID.randomUUID(),0,-1,null,null); persist(state);
            }
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { lockChannel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }

    public synchronized DistributedCommand pendingCommand() { return state.pending(); }
    public synchronized SessionAuthority observed() { return state.observed(); }

    /** Retries pending bytes first; wall-clock rollback observes the current cut without consuming an identity. */
    public Tick advance(long proposedTimeMillis) throws IOException {
        State before;
        synchronized (this) {
            requireOpen();
            if (busy) throw new IllegalStateException("Catalogue clock advance is active");
            busy=true; before=state;
        }
        try {
            if (before.pending()==null) {
                var ledger=epochs.read();
                if (ledger==null) throw new IOException("Session catalogue is not initialized");
                var source=SessionAuthority.from(ledger);
                try { if (before.observed()!=null) source.requireAtLeast(before.observed()); }
                catch (IllegalArgumentException regressed) { throw new IOException("Catalogue clock source regressed",regressed); }
                if (proposedTimeMillis<=ledger.logicalTimeMillis()) {
                    if (source.equals(before.observed())) return new Tick(Outcome.OBSERVED,source);
                    var next=new State(before.session(),before.sequence(),before.acknowledged(),source,null);
                    synchronized (this) { persist(next); state=next; }
                    return new Tick(Outcome.OBSERVED,source);
                }
                if (before.acknowledged()==Long.MAX_VALUE) throw new IOException("Catalogue clock sequence space exhausted");
                var operation=new OperationId(before.session(),0,before.sequence());
                var replacement=ledger.advanceTime(proposedTimeMillis);
                var command=before.acknowledged()<0 ? epochs.prepare(operation,ledger,replacement)
                        : epochs.prepare(operation,ledger,replacement,before.acknowledged());
                var next=new State(before.session(),before.sequence(),before.acknowledged(),source,command);
                synchronized (this) { persist(next); state=next; }
                before=next;
            }
            var result=epochs.execute(before.pending());
            Outcome outcome=switch (result.status()) {
                case SUCCESS -> Outcome.APPLIED;
                case CONFLICT -> Outcome.CONFLICT;
                case SESSION_EXPIRED -> Outcome.EXPIRED;
                default -> throw new IOException("Unexpected catalogue clock outcome");
            };
            var observed=outcome==Outcome.APPLIED ? SessionAuthority.from(replacement(before.pending())) : before.observed();
            var next=new State(before.session(),before.sequence()==Long.MAX_VALUE ? Long.MAX_VALUE : before.sequence()+1,
                    before.sequence(),observed,null);
            synchronized (this) { persist(next); state=next; }
            return new Tick(outcome,observed);
        } finally { synchronized (this) { busy=false; } }
    }

    private void requireOpen() throws IOException {
        if (closed || fenced || !lock.isValid()) throw new IOException("Catalogue clock is closed or fenced; reopen its journal");
    }
    @Override public synchronized void close() throws IOException {
        if (closed && !lockChannel.isOpen()) return;
        if (busy) throw new IOException("Catalogue clock advance is still active");
        closed=true;
        try { if (lock.isValid()) lock.release(); } finally { lockChannel.close(); }
    }
    private void persist(State next) throws IOException {
        try {
            var bytes=new ByteArrayOutputStream(); var output=new DataOutputStream(bytes);
            output.writeInt(MAGIC); output.writeInt(1); output.write(identity);
            uuid(output,next.session()); output.writeLong(next.sequence()); output.writeLong(next.acknowledged());
            blob(output,next.observed()==null ? null : new RangeSessionControl(next.observed(),null,0).encode());
            blob(output,next.pending()==null ? null : DistributedCommandCodec.encode(next.pending()));
            output.flush(); SessionJournalFiles.write(journal,bytes.toByteArray(),MAX_JOURNAL);
        } catch (IOException failure) { fenced=true; throw failure; }
    }
    private State decode(DataInputStream input) throws IOException {
        var session=uuid(input); long sequence=input.readLong(), acknowledged=input.readLong();
        byte[] cut=blob(input,RangeSessionControl.ADVANCE_BYTES), bytes=blob(input,MAX_JOURNAL);
        if (sequence<0 || acknowledged< -1 || (acknowledged==Long.MAX_VALUE ? sequence!=Long.MAX_VALUE : sequence!=acknowledged+1)) {
            throw new IOException("Invalid catalogue clock sequence state");
        }
        try {
            var control=cut==null ? null : RangeSessionControl.decode(cut);
            if (control!=null && control.binding()) throw new IOException("Invalid catalogue clock observed cut");
            var observed=control==null ? null : control.authority();
            if (acknowledged>=0 && observed==null) throw new IOException("Consumed catalogue clock stream has no observed cut");
            var pending=bytes==null ? null : DistributedCommandCodec.decode(bytes);
            if (pending!=null) {
                epochs.validatePrepared(pending);
                if (observed==null || acknowledged==Long.MAX_VALUE || !pending.operationId().equals(new OperationId(session,0,sequence))
                        || pending.acknowledgedThrough()!=acknowledged || pending.predicates().size()!=1
                        || pending.mutations().size()!=1 || pending.predicates().getFirst().expectedValue()==null) {
                    throw new IOException("Invalid catalogue clock pending identity/CAS");
                }
                var expected=SessionEpochLedger.decode(pending.predicates().getFirst().expectedValue());
                var replacement=replacement(pending);
                if (replacement.logicalTimeMillis()<=expected.logicalTimeMillis()
                        || !replacement.equals(expected.advanceTime(replacement.logicalTimeMillis()))) {
                    throw new IOException("Pending catalogue command is not an exact clock advance");
                }
                if (observed!=null) SessionAuthority.from(expected).requireAtLeast(observed);
                var exact=acknowledged<0 ? epochs.prepare(pending.operationId(),expected,replacement)
                        : epochs.prepare(pending.operationId(),expected,replacement,acknowledged);
                if (!Arrays.equals(bytes,DistributedCommandCodec.encode(exact))) throw new IOException("Noncanonical catalogue clock pending command");
            }
            return new State(session,sequence,acknowledged,observed,pending);
        } catch (IllegalArgumentException | ArithmeticException invalid) { throw new IOException("Invalid catalogue clock journal state",invalid); }
    }
    private static SessionEpochLedger replacement(DistributedCommand command) { return SessionEpochLedger.decode(command.mutations().getFirst().value()); }
    private static void blob(DataOutputStream output,byte[] bytes) throws IOException {
        output.writeInt(bytes==null ? -1 : bytes.length); if (bytes!=null) output.write(bytes);
    }
    private static byte[] blob(DataInputStream input,int maximum) throws IOException {
        int size=input.readInt(); if (size== -1) return null;
        if (size<0 || size>maximum || size>input.available()) throw new IOException("Invalid catalogue clock blob size");
        return input.readNBytes(size);
    }
    private static void uuid(DataOutputStream output,UUID id) throws IOException { output.writeLong(id.getMostSignificantBits()); output.writeLong(id.getLeastSignificantBits()); }
    private static UUID uuid(DataInputStream input) throws IOException { return new UUID(input.readLong(),input.readLong()); }
}
