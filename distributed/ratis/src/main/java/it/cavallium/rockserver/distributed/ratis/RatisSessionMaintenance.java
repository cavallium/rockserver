package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.channels.FileLock;
import java.nio.file.*;
import java.util.*;

/** One pending durable maintenance stream for a range incarnation. Caller owns the borrowed clients. */
public final class RatisSessionMaintenance implements AutoCloseable {
    private static final int MAGIC=0x52534d4a, MAX_JOURNAL=RangeDirectoryCodec.MAX_ENCODED_BYTES+16384;
    private record State(UUID session,long epoch,long nextSequence,long acknowledgedThrough,SessionAuthority propagated,
                         RatisRoutedClient.PreparedCommand pending) {}
    private final RatisSessionEpochs epochs;
    private final RatisRoutedClient routed;
    private final UUID group;
    private final long rangeEpoch;
    private final Path journal;
    private final byte[] identity;
    private final FileChannel lockChannel;
    private final FileLock lock;
    private State state;
    private boolean executing, closed, fenced;

    /** Parent directory must already exist on durable storage. Journal and stable .lock/.pending files belong to this owner. */
    public RatisSessionMaintenance(Path journal,RatisSessionEpochs epochs,RatisRoutedClient routed,UUID group) throws IOException {
        this.journal=journal.toAbsolutePath(); this.epochs=Objects.requireNonNull(epochs); this.routed=Objects.requireNonNull(routed);
        this.group=Objects.requireNonNull(group); identity=routed.maintenanceIdentityDigest();
        if (!Files.isDirectory(this.journal.getParent())) throw new IOException("Maintenance journal parent is absent");
        lockChannel=FileChannel.open(this.journal.resolveSibling(this.journal.getFileName()+".lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired=null;
        try {
            try { acquired=lockChannel.tryLock(); }
            catch (java.nio.channels.OverlappingFileLockException owned) { throw new IOException("Maintenance journal already owned",owned); }
            if (acquired==null) throw new IOException("Maintenance journal already owned");
            lock=acquired;
            if (Files.exists(this.journal)) {
                byte[] bytes=readJournal(); var input=new DataInputStream(new ByteArrayInputStream(bytes));
                if (input.readInt()!=MAGIC || !Arrays.equals(input.readNBytes(32),identity) || !uuid(input).equals(group)) {
                    throw new IOException("Wrong maintenance journal identity");
                }
                rangeEpoch=input.readLong();
                state=decode(input);
                if (input.available()!=0) throw new IOException("Trailing maintenance journal data");
                // Also seals an atomic rename whose previous owner could not confirm the directory force.
                force(this.journal); force(this.journal.getParent());
            } else {
                rangeEpoch=target(routed.directory()).rangeEpoch();
                state=new State(UUID.randomUUID(),0,0,-1,null,null); persist(state);
            }
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); }
            catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { lockChannel.close(); }
            catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    public synchronized DistributedCommand pendingCommand() { return state.pending()==null ? null : state.pending().command(); }
    public synchronized SessionAuthority propagated() { return state.propagated(); }
    public long rangeEpoch() { return rangeEpoch; }
    public DistributedCommand prepareAdvance() throws IOException { return prepare(null,0); }
    public DistributedCommand prepareBinding(SessionEpochLedger.Grant grant,int window) throws IOException {
        return prepare(Objects.requireNonNull(grant),window);
    }
    private DistributedCommand prepare(SessionEpochLedger.Grant grant,int window) throws IOException {
        final State before;
        synchronized (this) {
            requireOpen();
            if (executing || state.pending()!=null) throw new IllegalStateException("Maintenance control is still unresolved");
            executing=true; before=state;
        }
        try {
            var directory=routed.directory(); var descriptor=target(directory);
            if (descriptor.rangeEpoch()!=rangeEpoch) throw new IOException("Maintenance range changed incarnation");
            var ledger=epochs.read(); if (ledger==null) throw new IOException("Session catalogue is not initialized");
            var authority=SessionAuthority.from(ledger);
            if (before.propagated()!=null) authority.requireAtLeast(before.propagated());
            if (ledger.minimumEpoch()<before.epoch()) throw new IOException("Maintenance epoch floor regressed");
            long epoch=before.epoch(), sequence=before.nextSequence(), acknowledged=before.acknowledgedThrough();
            if (ledger.minimumEpoch()>epoch) { epoch=ledger.minimumEpoch(); sequence=0; acknowledged=-1; }
            if (acknowledged==Long.MAX_VALUE) throw new IOException("Maintenance sequence space exhausted");
            var control=grant==null ? RangeSessionControl.advance(ledger) : RangeSessionControl.bind(ledger,grant,window);
            var command=DistributedCommand.sessionControl(group,rangeEpoch,directory.schemaDigest(),new OperationId(before.session(),epoch,sequence),control,acknowledged);
            var prepared=routed.prepareSessionControl(command);
            var next=new State(before.session(),epoch,sequence,acknowledged,before.propagated(),prepared);
            synchronized (this) { persist(next); state=next; }
            return command;
        } finally { synchronized (this) { executing=false; } }
    }
    /** Retry the exact journaled control. A terminal result is durably consumed before another identity is admitted. */
    public CommandResult execute() throws IOException {
        final State before;
        synchronized (this) {
            requireOpen();
            if (executing || state.pending()==null) throw new IllegalStateException("No idle pending maintenance control");
            executing=true; before=state;
        }
        try {
            var result=routed.execute(before.pending());
            synchronized (this) {
                var propagated=result.status()==CommandResult.Status.SUCCESS ? before.pending().command().sessionControl().authority() : before.propagated();
                long sequence=before.nextSequence();
                var next=new State(before.session(),before.epoch(),sequence==Long.MAX_VALUE ? sequence : sequence+1,sequence,propagated,null);
                persist(next); state=next;
            }
            return result;
        } finally { synchronized (this) { executing=false; } }
    }
    private RangeDescriptor target(RangeDirectory directory) throws IOException {
        return directory.ranges().stream().filter(range -> range.groupId().equals(group) && range.topologyState()==RangeDescriptor.TopologyState.ACTIVE)
                .findFirst().orElseThrow(() -> new IOException("Maintenance range is not active"));
    }
    private void requireOpen() throws IOException {
        if (closed || fenced || !lock.isValid()) throw new IOException("Maintenance owner is closed or fenced; reopen its journal");
    }
    @Override public synchronized void close() throws IOException {
        if (closed && !lockChannel.isOpen()) return;
        if (executing) throw new IOException("Maintenance execution is still active");
        // Invalidate operations before releasing ownership, including when native close reports a failure.
        closed=true;
        try { if (lock.isValid()) lock.release(); }
        finally { lockChannel.close(); }
    }
    private void persist(State next) throws IOException {
        try {
            var bytes=new ByteArrayOutputStream(); var output=new DataOutputStream(bytes);
            output.writeInt(MAGIC); output.write(identity); uuid(output,group); output.writeLong(rangeEpoch);
            uuid(output,next.session()); output.writeLong(next.epoch()); output.writeLong(next.nextSequence()); output.writeLong(next.acknowledgedThrough());
            blob(output,next.propagated()==null ? null : new RangeSessionControl(next.propagated(),null,0).encode());
            blob(output,next.pending()==null ? null : DistributedCommandCodec.encode(next.pending().command()));
            if (next.pending()!=null) route(output,next.pending().descriptor());
            output.flush(); byte[] body=bytes.toByteArray();
            SessionJournalFiles.write(journal,body,MAX_JOURNAL);
        } catch (IOException failure) { fenced=true; throw failure; }
    }
    private byte[] readJournal() throws IOException { return SessionJournalFiles.read(journal,MAX_JOURNAL); }
    private State decode(DataInputStream input) throws IOException {
        var session=uuid(input); long epoch=input.readLong(), sequence=input.readLong(), acknowledged=input.readLong();
        byte[] propagated=blob(input,RangeSessionControl.ADVANCE_BYTES), pending=blob(input,256);
        if (rangeEpoch<0 || epoch<0 || sequence<0 || acknowledged< -1
                || (acknowledged==Long.MAX_VALUE ? sequence!=Long.MAX_VALUE : sequence!=acknowledged+1)) throw new IOException("Invalid maintenance sequence state");
        try {
            RangeSessionControl cut=propagated==null ? null : RangeSessionControl.decode(propagated);
            if (cut!=null && (cut.binding() || !cut.authority().clusterId().equals(routed.directory().clusterId())
                    || !cut.authority().databaseId().equals(routed.directory().databaseId()) || cut.authority().minimumEpoch()>epoch)) {
                throw new IOException("Invalid maintenance completion cut");
            }
            RatisRoutedClient.PreparedCommand prepared=null;
            if (pending!=null) {
                var command=DistributedCommandCodec.decode(pending); var descriptor=route(input);
                if (!command.operationId().equals(new OperationId(session,epoch,sequence)) || command.acknowledgedThrough()!=acknowledged
                        || !command.groupId().equals(group) || command.rangeEpoch()!=rangeEpoch) throw new IOException("Pending maintenance identity changed");
                prepared=routed.restoreSessionControl(command,descriptor);
            }
            return new State(session,epoch,sequence,acknowledged,cut==null ? null : cut.authority(),prepared);
        } catch (IllegalArgumentException invalid) { throw new IOException("Invalid maintenance journal state",invalid); }
    }
    private static void route(DataOutputStream output,RangeDescriptor route) throws IOException {
        uuid(output,route.rangeId()); uuid(output,route.groupId()); output.writeLong(route.rangeEpoch()); output.writeLong(route.configurationVersion());
        blob(output,route.lowerInclusive()); blob(output,route.upperExclusive()); output.writeInt(route.replicas().size());
        for (var replica:route.replicas()) {
            uuid(output,replica.nodeId()); uuid(output,replica.nodeIncarnation()); uuid(output,replica.replicaId());
            output.writeUTF(replica.peerId()); output.writeUTF(replica.address()); output.writeBoolean(replica.role()==ReplicaDescriptor.Role.VOTER);
        }
    }
    private static RangeDescriptor route(DataInputStream input) throws IOException {
        var range=uuid(input); var group=uuid(input); long epoch=input.readLong(), configuration=input.readLong();
        byte[] lower=blob(input,RangeDirectoryCodec.MAX_ENCODED_BYTES), upper=blob(input,RangeDirectoryCodec.MAX_ENCODED_BYTES);
        int count=input.readInt(); if (count<1 || count>RangeDirectoryCodec.MAX_REPLICAS) throw new IOException("Invalid maintenance replica count");
        var replicas=new ArrayList<ReplicaDescriptor>();
        for (int i=0;i<count;i++) replicas.add(new ReplicaDescriptor(uuid(input),uuid(input),uuid(input),input.readUTF(),input.readUTF(),
                input.readBoolean() ? ReplicaDescriptor.Role.VOTER : ReplicaDescriptor.Role.LISTENER));
        return new RangeDescriptor(range,group,epoch,configuration,lower,upper,RangeDescriptor.TopologyState.ACTIVE,replicas);
    }
    private static void blob(DataOutputStream output,byte[] bytes) throws IOException {
        output.writeInt(bytes==null ? -1 : bytes.length); if (bytes!=null) output.write(bytes);
    }
    private static byte[] blob(DataInputStream input,int maximum) throws IOException {
        int size=input.readInt(); if (size== -1) return null;
        if (size<0 || size>maximum || size>input.available()) throw new IOException("Invalid maintenance blob size");
        return input.readNBytes(size);
    }
    private static void uuid(DataOutputStream output,UUID id) throws IOException { output.writeLong(id.getMostSignificantBits()); output.writeLong(id.getLeastSignificantBits()); }
    private static UUID uuid(DataInputStream input) throws IOException { return new UUID(input.readLong(),input.readLong()); }
    private static void force(Path path) throws IOException { SessionJournalFiles.force(path); }
}
