package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

/** Exclusive issued-grant reservations, forced before any capture can use them. No identities are recycled. */
public final class RatisTransactionIdentities implements RatisRocksDBAdapter.AllocationSource,AutoCloseable {
    public static final int BLOCK_SIZE=RatisSnapshotClient.CONTROL_SLOTS+RatisAtomicClient.CONTROL_SLOTS+2,MAX_PENDING=16;
    private static final int MAGIC=0x54584944,BYTES=64,FILE_BYTES=BYTES+32;
    private final Path root,file;
    private final SessionEpochLedger.Grant grant;
    private final int window;
    private final RWScheduler scheduler;
    private final FileChannel channel;
    private final FileLock lock;
    private final ReentrantLock lane=new ReentrantLock();
    private final ReentrantLock persistence=new ReentrantLock();
    @FunctionalInterface interface Checkpoint { void write(Path file,byte[] bytes) throws IOException; }
    private final Checkpoint checkpoint;
    private long next,ordinal;
    private int pending;
    private boolean closed,fenced;
    public static RatisTransactionIdentities create(Path root,SessionEpochLedger.Grant grant,int outstandingWindow,RWScheduler scheduler) throws IOException {
        return create(root,grant,outstandingWindow,scheduler,(file,bytes) -> SessionJournalFiles.write(file,bytes,FILE_BYTES));
    }
    static RatisTransactionIdentities create(Path root,SessionEpochLedger.Grant grant,int window,RWScheduler scheduler,Checkpoint checkpoint) throws IOException { return new RatisTransactionIdentities(root,grant,window,scheduler,true,checkpoint); }
    public static RatisTransactionIdentities reopen(Path root,SessionEpochLedger.Grant grant,int outstandingWindow,RWScheduler scheduler) throws IOException {
        return new RatisTransactionIdentities(root,grant,outstandingWindow,scheduler,false,(file,bytes) -> SessionJournalFiles.write(file,bytes,FILE_BYTES));
    }
    /** Grant and adequate DATA/HOME windows must be installed and exclusively owned by this allocator. */
    private RatisTransactionIdentities(Path directory,SessionEpochLedger.Grant grant,int window,RWScheduler scheduler,boolean create,Checkpoint checkpoint) throws IOException {
        root=Objects.requireNonNull(directory).toAbsolutePath().normalize(); file=root.resolve("identities.bin"); this.grant=Objects.requireNonNull(grant); this.scheduler=Objects.requireNonNull(scheduler);
        this.checkpoint=Objects.requireNonNull(checkpoint);
        if (window<BLOCK_SIZE || window>RangeSessionControl.MAX_OUTSTANDING) throw new IllegalArgumentException("Transaction identity window cannot contain a complete reserved block"); this.window=window;
        if (!Files.isDirectory(root)) throw new IOException("Identity directory is absent");
        var marker=root.resolve("identities.lock");
        if (create && Files.exists(marker,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Identity ownership marker exists; never recreate missing used state");
        channel=FileChannel.open(marker,StandardOpenOption.CREATE,StandardOpenOption.WRITE); FileLock acquired=null;
        try {
            try { acquired=channel.tryLock(); } catch (OverlappingFileLockException busy) { throw new IOException("Transaction identity directory already owned",busy); }
            if (acquired==null) throw new IOException("Transaction identity directory already owned"); lock=acquired;
            if (create==Files.exists(file)) throw new IOException(create ? "Identity journal already exists" : "Identity journal is absent");
            if (create) {
                try (var paths=Files.list(root)) { if (paths.anyMatch(path -> !path.getFileName().toString().equals("identities.lock"))) throw new IOException("New identity root contains existing state"); }
                persist(0,0);
            } else {
                byte[] bytes=SessionJournalFiles.read(file,FILE_BYTES); var input=ByteBuffer.wrap(bytes);
                if (bytes.length!=BYTES || input.getInt()!=MAGIC) throw new IOException("Invalid identity journal");
                var saved=new SessionEpochLedger.Grant(input.getLong(),new UUID(input.getLong(),input.getLong()),input.getLong(),input.getLong());
                int savedWindow=input.getInt(); next=input.getLong(); ordinal=input.getLong();
                if (!saved.equals(grant) || savedWindow!=window || ordinal<0 || next<0 || next>window || ordinal>window/BLOCK_SIZE || next!=ordinal*BLOCK_SIZE || !Arrays.equals(bytes,encode(next,ordinal))) throw new IOException("Identity journal binding or reservation counter changed");
                SessionJournalFiles.force(file); SessionJournalFiles.force(root);
            }
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { channel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure;
        }
    }
    @Override public CompletableFuture<RatisRocksDBAdapter.Allocation> allocate(RequestContext context) {
        long deadline=scheduler.resolveMonotonicDeadline(context); final Task task;
        lane.lock();
        try {
            if (closed || fenced) return CompletableFuture.failedFuture(new IOException("Identity allocator closed/fenced; reopen its journal"));
            if (pending>=MAX_PENDING) return CompletableFuture.failedFuture(RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,"Identity allocation capacity exhausted"));
            pending++; task=new Task(deadline);
        } finally { lane.unlock(); }
        try { scheduler.executor(context.profile(),OperationFamily.MUTATION,deadline).execute(task,4096); }
        catch (RuntimeException rejected) { task.reject(rejected); }
        return task.result.copy();
    }
    private RatisRocksDBAdapter.Allocation reserve(long deadline) throws IOException {
        persistence.lock();
        try {
            checkDeadline(deadline); final long base,id;
            lane.lock();
            try {
                if (closed || fenced) throw new IOException("Identity allocator closed/fenced");
                if (next>window-BLOCK_SIZE) throw RocksDBException.of(RocksDBException.RocksDBErrorType.SERVER_OVERLOADED,"Issued identity window exhausted; renew the exclusively bound grant");
                base=next; id=ordinal;
            } finally { lane.unlock(); }
            persist(base+BLOCK_SIZE,id+1);
            lane.lock(); try { next=base+BLOCK_SIZE; ordinal=id+1; } finally { lane.unlock(); }
            var snapshot=new RatisSnapshotClient.Identity(uuid(id,(byte)1),operation(base),operation(base+1));
            long business=base+RatisSnapshotClient.CONTROL_SLOTS+1L;
            var commit=new RatisAtomicClient.Identity(uuid(id,(byte)2),operation(business),operation(business+1));
            return new RatisRocksDBAdapter.Allocation(root.resolve("tx-"+id+"-snapshot.bin"),root.resolve("tx-"+id+"-commit.bin"),new RatisTransactionClient.Identity(snapshot,commit));
        } finally { persistence.unlock(); }
    }
    private OperationId operation(long sequence) { return new OperationId(grant.sessionId(),grant.epoch(),sequence); }
    private UUID uuid(long id,byte role) { return UUID.nameUUIDFromBytes(ByteBuffer.allocate(33).putLong(grant.sessionId().getMostSignificantBits()).putLong(grant.sessionId().getLeastSignificantBits()).putLong(grant.epoch()).putLong(id).put(role).array()); }
    private byte[] encode(long next,long ordinal) {
        return ByteBuffer.allocate(BYTES).putInt(MAGIC).putLong(grant.epoch()).putLong(grant.sessionId().getMostSignificantBits()).putLong(grant.sessionId().getLeastSignificantBits())
                .putLong(grant.issuedAtMillis()).putLong(grant.expiresAtMillis()).putInt(window).putLong(next).putLong(ordinal).array();
    }
    private void persist(long next,long ordinal) throws IOException {
        try { checkpoint.write(file,encode(next,ordinal)); }
        catch (IOException | RuntimeException failure) { lane.lock(); try { fenced=true; } finally { lane.unlock(); } throw failure; }
    }
    private void checkDeadline(long deadline) throws IOException { if (deadline!=Long.MAX_VALUE && scheduler.isMonotonicDeadlineExpired(deadline)) throw new IOException("Identity allocation deadline expired",new TimeoutException()); }
    private final class Task implements Runnable,RWScheduler.RejectionAwareTask {
        final long deadline; final AtomicInteger phase=new AtomicInteger(); final CompletableFuture<RatisRocksDBAdapter.Allocation> result=new CompletableFuture<>();
        Task(long deadline) { this.deadline=deadline; }
        void check() throws IOException { checkDeadline(deadline); }
        public void run() {
            if (!phase.compareAndSet(0,1)) return; RatisRocksDBAdapter.Allocation allocation=null; Throwable failure=null;
            try { check(); allocation=reserve(deadline); check(); } catch (Throwable problem) { failure=problem; }
            phase.set(2); finish(allocation,failure); if (failure instanceof Error fatal) throw fatal;
        }
        public void reject(RuntimeException failure) { if (phase.compareAndSet(0,2)) finish(null,failure); }
        void finish(RatisRocksDBAdapter.Allocation allocation,Throwable failure) {
            lane.lock(); try { pending--; } finally { lane.unlock(); }
            if (failure==null) result.complete(allocation); else result.completeExceptionally(failure);
        }
    }
    public long nextSequence() { lane.lock(); try { return next; } finally { lane.unlock(); } }
    @Override public void close() throws IOException {
        lane.lock();
        try { if (closed) return; if (pending!=0) throw new IOException("Identity allocation work active; journal ownership retained"); closed=true; try { lock.release(); } finally { channel.close(); } }
        finally { lane.unlock(); }
    }
}
