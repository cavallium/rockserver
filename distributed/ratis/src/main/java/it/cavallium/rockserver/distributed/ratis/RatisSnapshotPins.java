package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import it.cavallium.rockserver.core.impl.rocksdb.NativeRangeSstExport;
import java.io.IOException;
import java.security.*;
import java.time.Duration;
import java.util.*;
import java.util.function.LongSupplier;
import org.rocksdb.RocksDBException;

/** Bounded owner-specific historical pins. Leadership changes do not retire them; deadlines and native ownership do. */
final class RatisSnapshotPins implements AutoCloseable {
    static final int MAX_CAPTURES=16, TOMBSTONE_BYTES=136, MAX_REPLY_BYTES=DistributedCommandCodec.MAX_ENCODED_BYTES;
    static final long MAX_BYTES=16L*1024*1024;
    @FunctionalInterface interface CaptureFactory {
        ReplicatedRangeStore.ScopedSnapshot capture(RatisSnapshotFences homes,SnapshotFence fence) throws Exception;
    }
    @FunctionalInterface interface ValueReader {
        byte[] get(ReplicatedRangeStore.ScopedSnapshot pin,UUID column,byte[] key) throws Exception;
    }
    @FunctionalInterface interface PinCloser {
        void close(ReplicatedRangeStore.ScopedSnapshot pin) throws Exception;
    }
    @FunctionalInterface interface CertifiedCapture {
        ScopedTransactionRpc.Handle capture(UUID id,RatisSnapshotFences homes,SnapshotFence fence,long opened,Duration ttl,java.util.function.Consumer<ScopedTransactionRpc.Handle> ownership) throws Exception;
    }
    @FunctionalInterface interface CertifiedCloser {
        boolean close(ScopedTransactionRpc.Handle handle) throws Exception;
    }
    @FunctionalInterface interface CertifiedLive {
        boolean live(ScopedTransactionRpc.Handle handle);
    }
    record Captured(SnapshotFenceStatus.Capture token,ScopedTransactionRpc.Handle handle) {}
    record Limits(int captures,long bytes,Duration maximumTtl) {
        Limits {
            Objects.requireNonNull(maximumTtl);
            if (captures<1 || captures>MAX_CAPTURES || bytes<TOMBSTONE_BYTES || bytes>MAX_BYTES || maximumTtl.isNegative()
                    || maximumTtl.isZero() || maximumTtl.compareTo(Duration.ofSeconds(60))>0) throw new IllegalArgumentException("Invalid snapshot pin limits");
        }
        static Limits defaults() { return new Limits(MAX_CAPTURES,MAX_BYTES,Duration.ofSeconds(60)); }
    }
    record Stats(int entries,int pins,long bytes) {}
    private static final class Entry {
        final UUID fenceId,snapshotId;
        final byte[] digest;
        final long opened,ttl,anchorEpoch;
        SnapshotFence fence;
        ReplicatedRangeStore.ScopedSnapshot pin;
        final RatisSnapshotCaptureRequest.Mode mode;
        ScopedTransactionRpc.Handle handle;
        SnapshotFenceStatus.Capture token;
        long bytes;
        boolean busy=true,closed,permanent,cleaning,compacted;
        Entry(SnapshotFence fence,UUID snapshotId,byte[] digest,long opened,long ttl,long bytes,RatisSnapshotCaptureRequest.Mode mode) {
            this.fence=fence; fenceId=fence.fenceId(); this.snapshotId=snapshotId; this.digest=digest; this.opened=opened; this.ttl=ttl; this.bytes=bytes; anchorEpoch=fence.operation().epoch();
            this.mode=mode;
        }
    }
    private final UUID owner,group;
    private final Limits limits;
    private final LongSupplier nanos;
    private final CaptureFactory factory;
    private final ValueReader reader;
    private final PinCloser closer;
    private final CertifiedCapture certifiedCapture;
    private final CertifiedCloser certifiedCloser;
    private final CertifiedLive certifiedLive;
    private final Map<UUID,Entry> entries=new HashMap<>(),snapshots=new HashMap<>();
    private long chargedBytes,minimumEpoch;
    private int pins;
    private boolean closed;

    RatisSnapshotPins(UUID owner,UUID group,Limits limits,LongSupplier nanos,CaptureFactory factory) {
        this(owner,group,limits,nanos,factory,ReplicatedRangeStore.ScopedSnapshot::get);
    }
    RatisSnapshotPins(UUID owner,UUID group,Limits limits,LongSupplier nanos,CaptureFactory factory,ValueReader reader) {
        this(owner,group,limits,nanos,factory,reader,ReplicatedRangeStore.ScopedSnapshot::close);
    }
    RatisSnapshotPins(UUID owner,UUID group,Limits limits,LongSupplier nanos,CaptureFactory factory,ValueReader reader,PinCloser closer) {
        this(owner,group,limits,nanos,factory,reader,closer,null,null,null);
    }
    RatisSnapshotPins(UUID owner,UUID group,Limits limits,LongSupplier nanos,CaptureFactory factory,ValueReader reader,PinCloser closer,CertifiedCapture certifiedCapture,CertifiedCloser certifiedCloser,CertifiedLive certifiedLive) {
        this.owner=Objects.requireNonNull(owner); this.group=Objects.requireNonNull(group); this.limits=Objects.requireNonNull(limits);
        this.nanos=Objects.requireNonNull(nanos); this.factory=Objects.requireNonNull(factory); this.reader=Objects.requireNonNull(reader); this.closer=Objects.requireNonNull(closer);
        this.certifiedCapture=certifiedCapture; this.certifiedCloser=certifiedCloser; this.certifiedLive=certifiedLive;
        if ((certifiedCapture==null)!=(certifiedCloser==null) || (certifiedCapture==null)!=(certifiedLive==null)) throw new IllegalArgumentException("Incomplete certified capture ownership");
    }
    SnapshotFenceStatus.Capture capture(RatisSnapshotFences homes,SnapshotFence fence,UUID expectedOwner,UUID snapshotId,Duration ttl,LongSupplier floor) throws Exception {
        return capture(homes,fence,expectedOwner,snapshotId,ttl,floor,RatisSnapshotCaptureRequest.Mode.READ_ONLY).token();
    }
    Captured capture(RatisSnapshotFences homes,SnapshotFence fence,UUID expectedOwner,UUID snapshotId,Duration ttl,LongSupplier floor,RatisSnapshotCaptureRequest.Mode mode) throws Exception {
        requireOwner(expectedOwner); Objects.requireNonNull(homes); Objects.requireNonNull(fence); Objects.requireNonNull(snapshotId); Objects.requireNonNull(ttl); Objects.requireNonNull(floor);
        Objects.requireNonNull(mode);
        if (mode==RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED && certifiedCapture==null) throw new UnsupportedOperationException("Writable capture is not configured");
        if (ttl.isNegative() || ttl.isZero() || ttl.compareTo(limits.maximumTtl())>0) throw new IllegalArgumentException("Snapshot pin TTL exceeds advertised limit");
        long duration=ttl.toNanos(); byte[] definition=fence.encode(),digest=hash(definition); var participant=fence.participant(group);
        long bytes=TOMBSTONE_BYTES+(long)definition.length+RangeMutationHistory.accountedScopeBytes(participant.scope());
        Entry existing;
        synchronized (this) {
            if (closed) throw lost("Registry is closed");
            existing=entries.get(fence.fenceId());
        }
        if (existing!=null) return retry(existing,snapshotId,duration,digest,mode);
        // The native floor may wait for the apply lane; never hold the registry monitor there.
        expire(); retireBefore(floor.getAsLong()); final Entry entry; final boolean retry;
        synchronized (this) {
            if (closed) throw lost("Registry is closed");
            existing=entries.get(fence.fenceId()); retry=existing!=null;
            if (retry) entry=existing;
            else {
                if (fence.operation().epoch()<minimumEpoch) throw lost("Snapshot business epoch was retired");
                if (snapshots.containsKey(snapshotId)) throw new IllegalArgumentException("Snapshot ID belongs to another fence");
                if (entries.size()>=limits.captures() || bytes>limits.bytes()-chargedBytes) throw new IOException("Snapshot pin entry/metadata admission exhausted");
                entry=new Entry(fence,snapshotId,digest,nanos.getAsLong(),duration,bytes,mode);
                entries.put(entry.fenceId,entry); snapshots.put(snapshotId,entry); chargedBytes+=bytes;
            }
        }
        if (retry) return retry(entry,snapshotId,duration,digest,mode);
        boolean success=false; Throwable failure=null;
        try {
            ApplyPosition position;
            if (mode==RatisSnapshotCaptureRequest.Mode.READ_ONLY) {
                var pin=Objects.requireNonNull(factory.capture(homes,fence),"Capture factory returned no pin");
                synchronized (this) { entry.pin=pin; entry.permanent=true; pins++; }
                if (!pin.groupId().equals(group) || pin.rangeEpoch()!=participant.rangeEpoch() || !Arrays.equals(pin.schemaDigest(),participant.schemaDigest())
                        || !Arrays.equals(ScopedTransactionRpc.scopeBytes(pin.scope()),ScopedTransactionRpc.scopeBytes(participant.scope()))) throw lost("Capture factory returned another scope/range");
                position=pin.position();
            } else {
                var handle=Objects.requireNonNull(certifiedCapture.capture(snapshotId,homes,fence,entry.opened,ttl,published -> {
                    synchronized (this) {
                        if (entry.handle!=null) throw new IllegalStateException("Capture ownership was published twice");
                        entry.handle=Objects.requireNonNull(published); entry.permanent=true; pins++;
                    }
                }),"Capture factory returned no handle");
                if (!handle.equals(entry.handle)) throw new IllegalStateException("Capture factory changed its published owner");
                if (!handle.owner().equals(owner)) throw lost("Capture factory returned another owner");
                if (!certifiedLive.live(handle)) throw lost("Writable capture lost its original scoped owner");
                position=handle.position();
            }
            var token=new SnapshotFenceStatus.Capture(owner,snapshotId,position);
            synchronized (this) { entry.token=token; ensureLive(entry); }
            success=true; return new Captured(token,entry.handle);
        } catch (Exception problem) {
            synchronized (this) { if (expired(entry) || entry.closed || closed) { entry.permanent=true; failure=new RatisSnapshotLostException("Capture lease expired or invalidated",problem); } else failure=problem; }
            throw (Exception)failure;
        } catch (Error problem) { failure=problem; throw problem; }
        finally { finish(entry,!success,failure); }
    }
    private Captured retry(Entry existing,UUID snapshotId,long ttl,byte[] digest,RatisSnapshotCaptureRequest.Mode mode) throws Exception {
        synchronized (this) {
            if (!existing.snapshotId.equals(snapshotId) || existing.ttl!=ttl || existing.mode!=mode || !Arrays.equals(existing.digest,digest)) throw new IllegalArgumentException("Snapshot capture retry changed ID, definition, mode or TTL");
            if (existing.busy) throw new IOException("Snapshot capture/operation is still running");
            ensureLive(existing);
            if (existing.token==null) throw lost("Capture has no historical pin");
            if (mode==RatisSnapshotCaptureRequest.Mode.READ_ONLY) return new Captured(existing.token,null);
            existing.busy=true;
        }
        boolean retire=false; Throwable failure=null;
        try {
            if (!certifiedLive.live(existing.handle)) { retire=true; throw lost("Original writable capture owner is no longer valid"); }
            synchronized (this) { ensureLive(existing); return new Captured(existing.token,existing.handle); }
        } catch (Exception | Error problem) { failure=problem; throw problem; }
        finally { finish(existing,retire,failure); }
    }
    /** The verified floor fences unknown old requests; live pins keep their original historical lease. */
    void retireBefore(long floor) throws Exception {
        if (floor<0) throw new IllegalArgumentException("Negative snapshot retention floor");
        synchronized (this) { minimumEpoch=Math.max(minimumEpoch,floor); }
        cleanup();
    }
    byte[] get(SnapshotFenceStatus.Capture token,UUID column,byte[] key) throws Exception {
        requireOwner(Objects.requireNonNull(token).ownerIncarnation()); Objects.requireNonNull(column); Objects.requireNonNull(key);
        if (key.length>MAX_REPLY_BYTES) throw new IllegalArgumentException("Snapshot point key exceeds request bound");
        byte[] owned=key.clone();
        return withPin(token,pin -> {
            byte[] value=reader.get(pin,column,owned);
            if (value!=null && value.length>MAX_REPLY_BYTES) throw new IOException("Snapshot point value exceeds reply bound");
            return value;
        });
    }
    PhysicalRangePage page(SnapshotFenceStatus.Capture token,UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws Exception {
        requireOwner(Objects.requireNonNull(token).ownerIncarnation()); Objects.requireNonNull(column);
        if (items<1 || items>PhysicalRangePage.MAX_ITEMS || bytes<PhysicalRangePage.HEADER_BYTES || bytes>PhysicalRangePage.MAX_BYTES
                || (long)(lower==null ? 0 : lower.length)+(upper==null ? 0 : upper.length)>TransactionScope.MAX_BOUND_BYTES
                || resume!=null && resume.length>PhysicalRangePage.MAX_BYTES) throw new IllegalArgumentException("Invalid snapshot page budget/bounds");
        byte[] ownedLower=lower==null ? null : lower.clone(),ownedUpper=upper==null ? null : upper.clone(),ownedResume=resume==null ? null : resume.clone();
        return withPin(token,pin -> {
            var page=pin.page(column,ownedLower,ownedUpper,reverse,ownedResume,items,bytes);
            if (page.payloadBytes()>PhysicalRangePage.MAX_BYTES) throw new IOException("Snapshot page exceeds reply bound"); return page;
        });
    }
    @FunctionalInterface interface ChunkWriter {
        NativeRangeSstExport.Chunk write(ReplicatedRangeStore.ScopedSnapshot pin) throws Exception;
    }
    /** The trusted exporter runs as bounded BATCH work and may force its descriptor before returning. */
    NativeRangeSstExport.Chunk export(SnapshotFenceStatus.Capture token,ChunkWriter writer) throws Exception {
        Objects.requireNonNull(writer);
        return withPin(token,pin -> {
            var chunk=Objects.requireNonNull(writer.write(pin)); var manifest=chunk.manifest();
            if (!manifest.groupId().equals(pin.groupId()) || manifest.rangeEpoch()!=pin.rangeEpoch()
                    || !manifest.position().equals(token.position()) || !Arrays.equals(manifest.schemaDigest(),pin.schemaDigest())
                    || !pin.scope().containsRange(manifest.columnId(),manifest.lowerInclusive(),manifest.upperExclusive()))
                throw new IllegalArgumentException("Export artifact differs from its original captured projection");
            return chunk;
        });
    }
    void requireBinding(SnapshotFenceStatus.Capture token,ScopedTransactionRpc.SnapshotBinding binding) throws RatisSnapshotLostException {
        requireOwner(Objects.requireNonNull(token).ownerIncarnation());
        synchronized (this) {
            var entry=snapshots.get(token.snapshotId()); requireToken(entry,token);
            if (!entry.fenceId.equals(binding.fenceId()) || !Arrays.equals(entry.digest,binding.digest())) throw lost("Token belongs to another frozen fence");
        }
    }
    void release(SnapshotFenceStatus.Capture token) throws Exception {
        requireOwner(Objects.requireNonNull(token).ownerIncarnation());
        synchronized (this) { var entry=snapshots.get(token.snapshotId()); requireToken(entry,token); entry.closed=true; }
        cleanup();
    }
    void expire() throws Exception {
        synchronized (this) {
            for (var entry:entries.values()) if (expired(entry)) { entry.permanent=true; entry.closed=true; }
        }
        cleanup();
    }
    /** Storage replacement invalidates leases while retaining their identities; it never permits recapture under the old request. */
    void invalidate() throws Exception {
        synchronized (this) { for (var entry:entries.values()) { entry.permanent=true; entry.closed=true; } }
        cleanup();
    }
    synchronized Stats stats() { return new Stats(entries.size(),pins,chargedBytes); }
    private Entry begin(SnapshotFenceStatus.Capture token) throws Exception {
        requireOwner(Objects.requireNonNull(token).ownerIncarnation()); expire();
        synchronized (this) {
            var entry=snapshots.get(token.snapshotId()); requireToken(entry,token); ensureLive(entry);
            if (entry.busy || entry.cleaning) throw new IOException("Snapshot operation is already running");
            entry.busy=true; return entry;
        }
    }
    @FunctionalInterface private interface Read<T> { T read(ReplicatedRangeStore.ScopedSnapshot pin) throws Exception; }
    private <T> T withPin(SnapshotFenceStatus.Capture token,Read<T> operation) throws Exception {
        var entry=begin(token); boolean retire=false; Throwable failure=null;
        try {
            if (entry.handle!=null) throw new IllegalArgumentException("Writable captures use their scoped transaction handle");
            T value=operation.read(entry.pin); synchronized (this) { ensureLive(entry); } return value;
        } catch (IllegalArgumentException | PhysicalRangePage.LimitExceededException problem) { failure=problem; throw problem; }
        catch (IllegalStateException | RocksDBException problem) {
            retire=true; var lost=new RatisSnapshotLostException("Native historical pin is unavailable",problem); failure=lost; throw lost;
        } catch (Exception | Error problem) { failure=problem; throw problem; }
        finally { finish(entry,retire,failure); }
    }
    private void requireOwner(UUID expected) throws RatisSnapshotLostException { if (!owner.equals(expected)) throw lost("Owner incarnation changed"); }
    private void requireToken(Entry entry,SnapshotFenceStatus.Capture token) throws RatisSnapshotLostException { if (entry==null || !token.equals(entry.token)) throw lost("Unknown or changed snapshot token"); }
    private boolean expired(Entry entry) { return nanos.getAsLong()-entry.opened>=entry.ttl; }
    private void ensureLive(Entry entry) throws RatisSnapshotLostException { if (closed || entry.closed || expired(entry)) throw lost("Historical pin expired or closed"); }
    private void finish(Entry entry,boolean retire,Throwable primary) throws Exception {
        synchronized (this) {
            entry.busy=false;
            if (expired(entry)) { entry.permanent=true; entry.closed=true; }
            if (retire || closed) entry.closed=true;
        }
        try { cleanup(); }
        catch (Exception | Error cleanup) { if (primary==null) throw cleanup; if (cleanup!=primary) primary.addSuppressed(cleanup); }
    }
    /** Busy operations and failed closes retain both the native owner and its complete charge. */
    private void cleanup() throws Exception {
        final List<Entry> release;
        synchronized (this) {
            for (var entry:List.copyOf(entries.values())) {
                if (entry.closed && !entry.busy && !entry.cleaning && entry.compacted && entry.anchorEpoch<minimumEpoch) remove(entry);
            }
            release=entries.values().stream().filter(e -> e.closed && !e.busy && !e.cleaning && !e.compacted).toList(); release.forEach(e -> e.cleaning=true);
        }
        Throwable failure=null;
        for (var entry:release) {
            boolean success=false;
            try { if (entry.pin!=null) closer.close(entry.pin); success=entry.handle==null || certifiedCloser.close(entry.handle); }
            catch (Throwable problem) { if (failure==null) failure=problem; else if (failure!=problem) failure.addSuppressed(problem); }
            finally {
                synchronized (this) {
                    entry.cleaning=false;
                    if (success) {
                        if (entry.pin!=null || entry.handle!=null) { entry.pin=null; entry.handle=null; pins--; }
                        entry.fence=null;
                        if (entry.permanent && entry.anchorEpoch>=minimumEpoch) { chargedBytes+=TOMBSTONE_BYTES-entry.bytes; entry.bytes=TOMBSTONE_BYTES; entry.compacted=true; }
                        else remove(entry);
                    }
                }
            }
        }
        if (failure instanceof Exception exception) throw exception; if (failure instanceof Error error) throw error;
    }
    private void remove(Entry entry) {
        entries.remove(entry.fenceId,entry); snapshots.remove(entry.snapshotId,entry); chargedBytes-=entry.bytes;
    }
    @Override public void close() throws Exception {
        synchronized (this) { closed=true; for (var entry:entries.values()) entry.closed=true; }
        cleanup();
    }
    private static RatisSnapshotLostException lost(String reason) { return new RatisSnapshotLostException(reason); }
    private static byte[] hash(byte[] bytes) {
        try { return MessageDigest.getInstance("SHA-256").digest(bytes); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
    }
}
