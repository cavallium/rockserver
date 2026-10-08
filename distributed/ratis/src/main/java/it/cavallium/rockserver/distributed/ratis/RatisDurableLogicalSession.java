package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.security.*;
import java.util.*;
import java.util.concurrent.locks.ReentrantLock;

/** Durable grant-to-window handoff and cached-schema logical operations. Borrowed clients remain caller-owned. */
public final class RatisDurableLogicalSession implements AutoCloseable {
    public enum Phase { INITIAL, PREPARING, ACTIVE }
    public record Progress(Phase phase,RatisGrantIssuer.Phase issuance,SessionEpochLedger.Grant grant) {}
    public static final class Pending {
        private final RatisDurableLogicalSession owner; private final RatisDurableSessionClient.Pending pending;
        private Pending(RatisDurableLogicalSession owner,RatisDurableSessionClient.Pending pending) { this.owner=owner; this.pending=pending; }
        public OperationId operationId() { return pending.command().operationId(); }
        public DistributedCommand command() { return pending.command(); }
        public RangeDescriptor descriptor() { return pending.descriptor(); }
        public CommandResult savedResult() { return pending.savedResult(); }
        public boolean consumed() { return pending.consumed(); }
    }
    private static final int MAGIC=0x44534c53, BODY_BYTES=109;
    private final Path root,intent; private final DistributedSchema schema; private final RatisSessionEpochs epochs;
    private final RatisRoutedClient routed; private final byte[] identity; private final long expires;
    private final RatisSessionClient.Limits limits; private final FileChannel channel; private final FileLock lock;
    private final ReentrantLock lane=new ReentrantLock(); private RatisGrantIssuer issuer; private RatisDurableSessionClient window;
    private Phase phase; private SessionEpochLedger.Grant activatedGrant; private int active; private boolean advancing,fenced,closeRequested,closed;
    public static RatisDurableLogicalSession create(Path root,DistributedSchema schema,RatisSessionEpochs epochs,RatisRoutedClient routed,
            long expiresAtMillis,RatisSessionClient.Limits limits) throws IOException {
        return new RatisDurableLogicalSession(root,schema,epochs,routed,true,expiresAtMillis,limits);
    }
    public static RatisDurableLogicalSession reopen(Path root,DistributedSchema schema,RatisSessionEpochs epochs,RatisRoutedClient routed) throws IOException {
        return new RatisDurableLogicalSession(root,schema,epochs,routed,false,0,null);
    }
    private RatisDurableLogicalSession(Path root,DistributedSchema schema,RatisSessionEpochs epochs,RatisRoutedClient routed,
            boolean create,long expires,RatisSessionClient.Limits limits) throws IOException {
        this.root=root.toAbsolutePath(); this.intent=this.root.resolve("session.bin"); this.schema=Objects.requireNonNull(schema);
        this.epochs=Objects.requireNonNull(epochs); this.routed=Objects.requireNonNull(routed); schema.requireMatches(routed.directory()); epochs.requireDatabaseBinding(schema.clusterId(),schema.databaseId());
        try { identity=MessageDigest.getInstance("SHA-256").digest(ByteBuffer.allocate(96).put(schema.digest()).put(epochs.identityDigest()).put(routed.sessionIdentityDigest()).array()); }
        catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        if (!Files.isDirectory(this.root)) throw new IOException("Logical session root is absent");
        if (create) validate(expires,limits);
        channel=FileChannel.open(this.root.resolve("session.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE); FileLock acquired=null;
        try {
            try { acquired=channel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Logical session root already owned",owned); }
            if (acquired==null) throw new IOException("Logical session root already owned"); lock=acquired;
            if (create) {
                try (var files=Files.list(this.root)) { if (files.anyMatch(path -> !path.getFileName().toString().equals("session.lock"))) throw new IOException("New logical session root contains existing state"); }
                this.expires=expires; this.limits=limits; phase=Phase.INITIAL; persist(phase);
            } else {
                byte[] bytes=SessionJournalFiles.read(intent,256); var input=ByteBuffer.wrap(bytes);
                if (bytes.length!=BODY_BYTES || input.getInt()!=MAGIC || input.getInt()!=1) throw new IOException("Invalid logical session intent");
                int stage=input.get() & 255; byte[] bound=new byte[32]; input.get(bound);
                if (stage>2 || !Arrays.equals(bound,identity)) throw new IOException("Logical session intent identity/phase changed");
                this.expires=input.getLong();
                try { this.limits=new RatisSessionClient.Limits(input.getInt(),input.getLong(),input.getLong()); validate(this.expires,this.limits); }
                catch (IllegalArgumentException invalid) { throw new IOException("Invalid logical session limits/expiry",invalid); }
                long epoch=input.getLong(); UUID session=new UUID(input.getLong(),input.getLong()); long issuedAt=input.getLong(), expiresAt=input.getLong();
                if (stage==2) {
                    try { activatedGrant=new SessionEpochLedger.Grant(epoch,session,issuedAt,expiresAt); }
                    catch (IllegalArgumentException invalid) { throw new IOException("Invalid activated grant identity",invalid); }
                } else if (epoch!=0 || !session.equals(new UUID(0,0)) || issuedAt!=0 || expiresAt!=0) throw new IOException("Unexposed intent contains activated grant identity");
                phase=switch(stage) { case 0 -> Phase.INITIAL; case 1 -> Phase.PREPARING; case 2 -> Phase.ACTIVE; default -> throw new IOException("Unknown logical session phase"); }; SessionJournalFiles.force(intent); SessionJournalFiles.force(this.root);
            }
            Path issuing=this.root.resolve("issuer.bin"), data=this.root.resolve("window.bin");
            if (phase==Phase.INITIAL) {
                if (Files.exists(data)) throw new IOException("Unexposed initialization contains a user window");
                issuer=Files.exists(issuing) ? RatisGrantIssuer.reopen(issuing,epochs) : RatisGrantIssuer.create(issuing,epochs,this.expires);
                if (issuer.phase()!=RatisGrantIssuer.Phase.IDLE) throw new IOException("Unexposed initialization contains an issued command");
                requireExpiry(); persist(Phase.PREPARING); phase=Phase.PREPARING;
            } else { issuer=RatisGrantIssuer.reopen(issuing,epochs); requireExpiry(); }
            if (phase==Phase.ACTIVE) {
                if (issuer.issued()==null) throw new IOException("Active logical session has no successful grant issuance");
                window=RatisDurableSessionClient.reopen(data,routed); requireWindow(false);
            } else if (Files.exists(data) && issuer.issued()==null) throw new IOException("Unissued logical session contains a user window");
        } catch (IOException | RuntimeException | Error failure) {
            try { if (window!=null) window.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { if (issuer!=null) issuer.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure;
        }
    }
    private static void validate(long expires,RatisSessionClient.Limits limits) {
        Objects.requireNonNull(limits);
        if (expires<=0 || limits.commandBytes()>SessionWindowJournal.MAX_WINDOW_BYTES || limits.resultBytes()>SessionWindowJournal.MAX_WINDOW_BYTES) throw new IllegalArgumentException("Invalid durable logical session limits/expiry");
    }
    private void requireExpiry() throws IOException { if (issuer.expiresAtMillis()!=expires) throw new IOException("Logical issuance expiry changed"); }
    private void requireWindow(boolean pristine) throws IOException {
        if (!window.grant().equals(issuer.issued().grant()) || !window.limits().equals(limits) || pristine && !window.pristine()
                || !pristine && !window.grant().equals(activatedGrant)) throw new IOException("Logical window differs from pristine issuance handoff");
    }
    /** At most one issuance CAS, followed by the durable handoff if issuance succeeds. Conflicts require another explicit step. */
    public Progress advance() throws IOException {
        lane.lock(); try { requireOpen(); if (phase==Phase.ACTIVE) return progress(); if (advancing) throw new IOException("Logical issuance is active"); advancing=true; active++; } finally { lane.unlock(); }
        try {
            issuer.step(); lane.lock();
            try {
                requireOpen(); if (issuer.issued()!=null) {
                    Path path=root.resolve("window.bin");
                    if (window==null) window=Files.exists(path) ? RatisDurableSessionClient.reopen(path,routed)
                            : RatisDurableSessionClient.create(path,routed,issuer.issued().issuance(),issuer.issued().grant(),limits);
                    requireWindow(true); persist(Phase.ACTIVE); activatedGrant=issuer.issued().grant(); phase=Phase.ACTIVE;
                }
                return progress();
            } finally { lane.unlock(); }
        } finally { lane.lock(); try { advancing=false; active--; } finally { lane.unlock(); } }
    }
    public Progress progress() { lane.lock(); try { return new Progress(phase,issuer.phase(),phase==Phase.ACTIVE ? issuer.issued().grant() : null); } finally { lane.unlock(); } }
    public DistributedSchema schema() { return schema; }
    public long getColumnId(String name) { return schema.column(name).apiId(); }
    public List<Pending> pending() throws IOException { return run(() -> window.pending().stream().map(entry -> new Pending(this,entry)).toList()); }
    public long acknowledgedThrough() throws IOException { return run(() -> window.acknowledgedThrough()); }
    public Buf get(long column,Keys keys) throws IOException {
        return run(() -> {
            var binding=RatisLogicalClient.supported(schema,column); byte[] value=routed.read(binding.columnId(),schema.encodeFixedKey(binding.columnId(),keys));
            if (value!=null && !binding.schema().hasValue() && value.length!=0) throw new IOException("Nonempty key-only value"); return value==null ? null : Buf.wrap(value);
        });
    }
    public Pending preparePut(long column,Keys keys,Buf value,Mutation.ReturnMode returns) throws IOException { return prepare(List.of(RatisLogicalClient.Change.put(column,keys,value,returns)),List.of()); }
    public Pending prepareDelete(long column,Keys keys,Mutation.ReturnMode returns) throws IOException { return prepare(List.of(RatisLogicalClient.Change.delete(column,keys,returns)),List.of()); }
    public Pending prepare(List<RatisLogicalClient.Change> changes,List<RatisLogicalClient.Condition> conditions) throws IOException {
        return run(() -> { var plan=RatisLogicalClient.plan(schema,changes,conditions); return new Pending(this,window.prepare(plan.mutations(),plan.predicates())); });
    }
    public CommandResult execute(Pending pending) throws IOException { return run(() -> window.execute(owned(pending))); }
    public void acknowledge(Pending pending) throws IOException { run(() -> { window.acknowledge(owned(pending)); return null; }); }
    public Pending prepareAcknowledgement(long column,Keys keys) throws IOException {
        return run(() -> { var binding=RatisLogicalClient.supported(schema,column); return new Pending(this,window.prepareAcknowledgement(binding.columnId(),schema.encodeFixedKey(binding.columnId(),keys))); });
    }
    /** Explicit durable first-use binding; ordinary execution performs no catalogue lookup. */
    public CommandResult ensureBinding(Pending pending) throws IOException {
        return run(() -> {
            owned(pending); Path directory=Files.createDirectories(root.resolve("bindings")); SessionJournalFiles.force(directory); SessionJournalFiles.force(root);
            try (var maintenance=new RatisSessionMaintenance(directory.resolve(pending.descriptor().groupId()+".bin"),epochs,routed,pending.descriptor().groupId())) {
                if (maintenance.rangeEpoch()!=pending.command().rangeEpoch()) throw new IOException("Logical pending range incarnation changed");
                var command=maintenance.pendingCommand(); if (command==null) command=maintenance.prepareBinding(window.grant(),limits.outstanding());
                var control=command.sessionControl();
                if (control==null || !control.binding() || !control.grant().equals(window.grant()) || control.outstandingWindow()!=limits.outstanding()
                        || !command.groupId().equals(pending.command().groupId()) || command.rangeEpoch()!=pending.command().rangeEpoch()) throw new IOException("Recovered binding differs from logical grant/window");
                return maintenance.execute();
            }
        });
    }
    private RatisDurableSessionClient.Pending owned(Pending pending) { if (pending==null || pending.owner!=this) throw new IllegalArgumentException("Foreign logical pending operation"); return pending.pending; }
    @FunctionalInterface private interface Work<T> { T run() throws IOException; }
    private <T> T run(Work<T> work) throws IOException {
        lane.lock(); try { requireOpen(); if (phase!=Phase.ACTIVE) throw new IOException("Logical session is not active"); window.acknowledgedThrough(); if (active>=32) throw new IOException("Logical session call budget exhausted"); active++; } finally { lane.unlock(); }
        try { return work.run(); } finally { lane.lock(); try { active--; } finally { lane.unlock(); } }
    }
    private void requireOpen() throws IOException { if (closed || closeRequested || fenced || !lock.isValid()) throw new IOException("Logical session is closed/fenced; reopen its root"); }
    private void persist(Phase stage) throws IOException {
        try {
            var output=ByteBuffer.allocate(BODY_BYTES).putInt(MAGIC).putInt(1).put((byte)switch(stage) { case INITIAL -> 0; case PREPARING -> 1; case ACTIVE -> 2; })
                    .put(identity).putLong(expires).putInt(limits.outstanding()).putLong(limits.commandBytes()).putLong(limits.resultBytes());
            if (stage==Phase.ACTIVE) {
                var grant=issuer.issued().grant(); output.putLong(grant.epoch()).putLong(grant.sessionId().getMostSignificantBits()).putLong(grant.sessionId().getLeastSignificantBits())
                        .putLong(grant.issuedAtMillis()).putLong(grant.expiresAtMillis());
            } else output.put(new byte[40]);
            SessionJournalFiles.write(intent,output.array(),256);
        }
        catch (IOException failure) { fenced=true; throw failure; }
    }
    @Override public void close() throws IOException {
        lane.lock();
        try {
            if (closed) return; if (active!=0) throw new IOException("Logical session work is active"); closeRequested=true;
            if (window!=null) window.close(); issuer.close();
            IOException failure=null;
            try { if (lock.isValid()) lock.release(); } catch (IOException cleanup) { failure=cleanup; }
            try { channel.close(); } catch (IOException cleanup) { if (failure==null) failure=cleanup; else failure.addSuppressed(cleanup); }
            if (failure!=null) throw failure; closed=true;
        } finally { lane.unlock(); }
    }
}
