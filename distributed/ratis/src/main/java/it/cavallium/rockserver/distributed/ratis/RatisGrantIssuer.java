package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

/** Durable one-user grant allocation. Caller owns the borrowed catalogue facade and issuance authority. */
public final class RatisGrantIssuer implements AutoCloseable {
    private static final int MAGIC=0x5247494a, MAX_BYTES=2*SessionEpochLedger.MAX_BYTES+4096;
    public enum Phase { IDLE, PENDING, RETRY, ISSUED, EXPIRED, EXHAUSTED }
    public record Issued(SessionEpochLedger issuance,SessionEpochLedger.Grant grant) {
        public Issued {
            Objects.requireNonNull(issuance); Objects.requireNonNull(grant);
            if (issuance.generation()<1 || issuance.grants().isEmpty() || !issuance.grants().getLast().equals(grant)
                    || grant.issuedAtMillis()!=issuance.logicalTimeMillis()) throw new IllegalArgumentException("Grant is not the frozen issuance append");
        }
    }
    private record State(UUID user,UUID issuer,long sequence,long acknowledged,Phase phase,DistributedCommand pending,Issued issued) {}
    private final Path path;
    private final RatisSessionEpochs epochs;
    private final byte[] identity;
    private final long expiresAtMillis;
    private final FileChannel channel;
    private final FileLock lock;
    private State state;
    private boolean busy,closed,fenced;

    public static RatisGrantIssuer create(Path path,RatisSessionEpochs epochs,long expiresAtMillis) throws IOException {
        if (expiresAtMillis<=0) throw new IllegalArgumentException("Grant expiry must be positive");
        return new RatisGrantIssuer(path,epochs,true,expiresAtMillis);
    }
    public static RatisGrantIssuer reopen(Path path,RatisSessionEpochs epochs) throws IOException { return new RatisGrantIssuer(path,epochs,false,0); }
    private RatisGrantIssuer(Path path,RatisSessionEpochs epochs,boolean create,long requestedExpiry) throws IOException {
        this.path=path.toAbsolutePath(); this.epochs=Objects.requireNonNull(epochs); identity=epochs.identityDigest();
        if (!Files.isDirectory(this.path.getParent())) throw new IOException("Grant issuer journal parent is absent");
        channel=FileChannel.open(this.path.resolveSibling(this.path.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired=null;
        try {
            try { acquired=channel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Grant issuer journal already owned",owned); }
            if (acquired==null) throw new IOException("Grant issuer journal already owned"); lock=acquired;
            if (create==Files.exists(this.path)) throw new IOException(create ? "Grant issuer journal already exists" : "Grant issuer journal is absent");
            if (create) {
                expiresAtMillis=requestedExpiry; UUID user=UUID.randomUUID(), issuer;
                do { issuer=UUID.randomUUID(); } while (issuer.equals(user));
                state=new State(user,issuer,0,-1,Phase.IDLE,null,null); persist(state);
            } else {
                var input=new DataInputStream(new ByteArrayInputStream(SessionJournalFiles.read(this.path,MAX_BYTES)));
                if (input.readInt()!=MAGIC || input.readInt()!=1 || !Arrays.equals(input.readNBytes(32),identity)) throw new IOException("Wrong grant issuer journal identity/version");
                expiresAtMillis=input.readLong(); if (expiresAtMillis<=0) throw new IOException("Invalid journaled grant expiry");
                state=decode(input); if (input.available()!=0) throw new IOException("Trailing grant issuer journal bytes");
                SessionJournalFiles.force(this.path); SessionJournalFiles.force(this.path.getParent());
            }
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    public synchronized Phase phase() { return state.phase(); }
    public synchronized DistributedCommand preparedCommand() { return state.pending(); }
    public synchronized Issued issued() { return state.issued(); }
    public long expiresAtMillis() { return expiresAtMillis; }

    /** At most one CAS is sent; conflict advances only this stream's durable consumed prefix. */
    public Phase step() throws IOException {
        State before;
        synchronized (this) {
            requireOpen(); if (busy) throw new IllegalStateException("Grant issuer step is active");
            if (terminal(state.phase())) return state.phase(); busy=true; before=state;
        }
        try {
            if (before.pending()==null) {
                var ledger=epochs.read(); if (ledger==null) throw new IOException("Session catalogue is not initialized");
                if (expiresAtMillis<=ledger.logicalTimeMillis()) {
                    var expired=new State(before.user(),before.issuer(),before.sequence(),before.acknowledged(),Phase.EXPIRED,null,null);
                    save(expired); return Phase.EXPIRED;
                }
                // Validate lifetime/capacity before allocating or persisting an attempt identity.
                var replacement=ledger.issue(before.user(),expiresAtMillis);
                if (before.acknowledged()==Long.MAX_VALUE) throw new IOException("Grant issuer sequence space exhausted");
                var id=new OperationId(before.issuer(),0,before.sequence());
                var pending=before.acknowledged()<0 ? epochs.prepare(id,ledger,replacement) : epochs.prepare(id,ledger,replacement,before.acknowledged());
                before=new State(before.user(),before.issuer(),before.sequence(),before.acknowledged(),Phase.PENDING,pending,null);
                save(before);
            }
            var result=epochs.execute(before.pending());
            if (result.valueCount()!=0 || result.status()==CommandResult.Status.RESULT_TOO_LARGE) throw new IOException("Unexpected grant issuance outcome");
            Phase phase; Issued issued=null;
            switch (result.status()) {
                case SUCCESS -> { var image=replacement(before.pending()); issued=new Issued(image,image.grants().getLast()); phase=Phase.ISSUED; }
                case CONFLICT -> phase=before.sequence()==Long.MAX_VALUE ? Phase.EXHAUSTED : Phase.RETRY;
                case SESSION_EXPIRED -> phase=Phase.EXPIRED;
                default -> throw new IOException("Unexpected grant issuance outcome");
            }
            var next=new State(before.user(),before.issuer(),before.sequence()==Long.MAX_VALUE ? Long.MAX_VALUE : before.sequence()+1,
                    before.sequence(),phase,null,issued);
            save(next); return phase;
        } finally { synchronized (this) { busy=false; } }
    }
    private synchronized void save(State next) throws IOException { persist(next); state=next; }
    private void requireOpen() throws IOException { if (closed || fenced || !lock.isValid()) throw new IOException("Grant issuer is closed/fenced; reopen its journal"); }
    private static boolean terminal(Phase phase) { return phase==Phase.ISSUED || phase==Phase.EXPIRED || phase==Phase.EXHAUSTED; }
    private void persist(State next) throws IOException {
        try {
            var bytes=new ByteArrayOutputStream(); var output=new DataOutputStream(bytes);
            output.writeInt(MAGIC); output.writeInt(1); output.write(identity); output.writeLong(expiresAtMillis);
            uuid(output,next.user()); uuid(output,next.issuer()); output.writeLong(next.sequence()); output.writeLong(next.acknowledged()); output.writeByte(wire(next.phase()));
            blob(output,next.pending()==null ? null : DistributedCommandCodec.encode(next.pending()));
            blob(output,next.issued()==null ? null : next.issued().issuance().encode()); output.flush();
            SessionJournalFiles.write(path,bytes.toByteArray(),MAX_BYTES);
        } catch (IOException failure) { fenced=true; throw failure; }
    }
    private State decode(DataInputStream input) throws IOException {
        UUID user=uuid(input), issuer=uuid(input); long sequence=input.readLong(), acknowledged=input.readLong(); Phase phase=phase(input.readUnsignedByte());
        byte[] command=blob(input,DistributedCommandCodec.MAX_ENCODED_BYTES), issuedBytes=blob(input,SessionEpochLedger.MAX_BYTES);
        if (user.equals(issuer) || sequence<0 || acknowledged< -1 || (acknowledged==Long.MAX_VALUE ? sequence!=Long.MAX_VALUE : sequence!=acknowledged+1)
                || (phase==Phase.PENDING)!=(command!=null) || (phase==Phase.ISSUED)!=(issuedBytes!=null)
                || phase==Phase.IDLE && (sequence!=0 || acknowledged!=-1) || phase==Phase.RETRY && (acknowledged<0 || acknowledged==Long.MAX_VALUE)
                || phase==Phase.EXHAUSTED && acknowledged!=Long.MAX_VALUE || command!=null && acknowledged==Long.MAX_VALUE) {
            throw new IOException("Invalid grant issuer phase/sequence state");
        }
        try {
            DistributedCommand pending=null; Issued issued=null;
            if (command!=null) {
                pending=DistributedCommandCodec.decode(command); epochs.validatePrepared(pending);
                if (!pending.operationId().equals(new OperationId(issuer,0,sequence)) || pending.acknowledgedThrough()!=acknowledged
                        || pending.predicates().size()!=1 || pending.mutations().size()!=1 || pending.predicates().getFirst().expectedValue()==null) {
                    throw new IOException("Invalid grant issuer pending identity/CAS");
                }
                var expected=SessionEpochLedger.decode(pending.predicates().getFirst().expectedValue()); var replacement=replacement(pending);
                if (!replacement.equals(expected.issue(user,expiresAtMillis))) throw new IOException("Pending grant issuer command is not the exact append");
                var exact=acknowledged<0 ? epochs.prepare(pending.operationId(),expected,replacement) : epochs.prepare(pending.operationId(),expected,replacement,acknowledged);
                if (!Arrays.equals(command,DistributedCommandCodec.encode(exact))) throw new IOException("Noncanonical grant issuer command");
            }
            if (issuedBytes!=null) {
                var image=SessionEpochLedger.decode(issuedBytes);
                if (!Arrays.equals(issuedBytes,image.encode()) || image.grants().isEmpty() || acknowledged<0) throw new IOException("Invalid issued grant image");
                var grant=image.grants().getLast();
                if (!grant.sessionId().equals(user) || grant.expiresAtMillis()!=expiresAtMillis) throw new IOException("Issued grant identity changed");
                issued=new Issued(image,grant);
                // Reconstruct the immediate pre-append shape to validate identity without a fresh read.
                var previous=new SessionEpochLedger(image.clusterId(),image.databaseId(),image.generation()-1,image.logicalTimeMillis(),
                        image.minimumEpoch(),image.nextEpoch()-1,image.grants().subList(0,image.grants().size()-1));
                var id=new OperationId(issuer,0,acknowledged);
                if (acknowledged==0) epochs.prepare(id,previous,image);
                else epochs.prepare(id,previous,image,acknowledged-1);
            }
            return new State(user,issuer,sequence,acknowledged,phase,pending,issued);
        } catch (RuntimeException invalid) { throw new IOException("Invalid grant issuer journal state",invalid); }
    }
    @Override public synchronized void close() throws IOException {
        if (closed && !channel.isOpen()) return; if (busy) throw new IOException("Grant issuer step is active"); closed=true;
        IOException failure=null;
        try { if (lock.isValid()) lock.release(); } catch (IOException cleanup) { failure=cleanup; }
        try { channel.close(); } catch (IOException cleanup) { if (failure==null) failure=cleanup; else failure.addSuppressed(cleanup); }
        if (failure!=null) throw failure;
    }
    private static int wire(Phase phase) { return switch(phase) { case IDLE -> 0; case PENDING -> 1; case RETRY -> 2; case ISSUED -> 3; case EXPIRED -> 4; case EXHAUSTED -> 5; }; }
    private static Phase phase(int wire) throws IOException { return switch(wire) { case 0 -> Phase.IDLE; case 1 -> Phase.PENDING; case 2 -> Phase.RETRY; case 3 -> Phase.ISSUED; case 4 -> Phase.EXPIRED; case 5 -> Phase.EXHAUSTED; default -> throw new IOException("Unknown grant issuer phase"); }; }
    private static SessionEpochLedger replacement(DistributedCommand command) { return SessionEpochLedger.decode(command.mutations().getFirst().value()); }
    private static void blob(DataOutputStream output,byte[] bytes) throws IOException { output.writeInt(bytes==null ? -1 : bytes.length); if (bytes!=null) output.write(bytes); }
    private static byte[] blob(DataInputStream input,int maximum) throws IOException {
        int size=input.readInt(); if (size== -1) return null;
        if (size<0 || size>maximum || size>input.available()) throw new IOException("Invalid grant issuer journal blob size"); return input.readNBytes(size);
    }
    private static UUID uuid(DataInputStream input) throws IOException { return new UUID(input.readLong(),input.readLong()); }
    private static void uuid(DataOutputStream output,UUID id) throws IOException { output.writeLong(id.getMostSignificantBits()); output.writeLong(id.getLeastSignificantBits()); }
}
