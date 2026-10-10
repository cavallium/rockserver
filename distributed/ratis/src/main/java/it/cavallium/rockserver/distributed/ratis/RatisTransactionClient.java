package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.RWScheduler;
import java.io.IOException;
import java.nio.file.*;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.CompletableFuture;

/** Consistent read/write transactions over original certified captures and exact durable commit recovery. */
public final class RatisTransactionClient implements AutoCloseable {
    public record Limits(RatisSnapshotClient.Limits snapshots,RatisAtomicClient.Limits commits) {
        public Limits { Objects.requireNonNull(snapshots); Objects.requireNonNull(commits); }
        public static Limits defaults() { return new Limits(RatisSnapshotClient.Limits.defaults(),RatisAtomicClient.Limits.defaults()); }
    }
    /** Both disjoint identity blocks must already be reserved in the same issued grant. */
    public record Identity(RatisSnapshotClient.Identity snapshot,RatisAtomicClient.Identity commit) {
        public Identity {
            Objects.requireNonNull(snapshot); Objects.requireNonNull(commit);
            var first=snapshot.business(); var second=commit.business();
            if (snapshot.fence().equals(commit.transaction()) || !first.sessionId().equals(second.sessionId()) || first.epoch()!=second.epoch()
                    || first.sequence()<=commit.lastControl().sequence() && second.sequence()<=snapshot.lastControl().sequence()) throw new IllegalArgumentException("Transaction identities require unique IDs and disjoint blocks in one grant");
        }
    }
    public record Stats(RatisSnapshotClient.Stats snapshots,RatisAtomicClient.Stats commits) {}
    private final DistributedSchema schema;
    private final RangeDirectory directory;
    private final RatisRoutedClient data;
    private final RatisSnapshotClient snapshots;
    private final RatisAtomicClient commits;
    private final Map<Path,Lease> leases=new LinkedHashMap<>();
    private final Map<Path,Cleanup> cleanups=new LinkedHashMap<>();
    private boolean closing,closed;

    /** Infrastructure and grant maintenance are borrowed; only the bounded client wrappers are owned. */
    public RatisTransactionClient(DistributedSchema schema,RatisSnapshotFences snapshotHomes,RatisTransactionStatuses transactionHomes,
                                  RatisRoutedClient data,RWScheduler scheduler,Limits limits) {
        this.schema=Objects.requireNonNull(schema); this.data=Objects.requireNonNull(data); Objects.requireNonNull(limits); data.requireKnownSchema(schema); directory=data.directory(schema);
        snapshots=new RatisSnapshotClient(schema,snapshotHomes,data,scheduler,limits.snapshots(),RatisSnapshotCaptureRequest.Mode.WRITE_CERTIFIED);
        commits=new RatisAtomicClient(schema,transactionHomes,data,scheduler,limits.commits());
    }
    public synchronized Lease open(Path snapshotPath,Path commitPath,Identity identity,TransactionScope scope,Duration ttl,RequestContext context) throws IOException {
        requireOpen(); Objects.requireNonNull(identity); Path capture=Objects.requireNonNull(snapshotPath).toAbsolutePath().normalize(),commit=Objects.requireNonNull(commitPath).toAbsolutePath().normalize();
        if (capture.equals(commit) || owned(capture) || owned(commit)) throw new IOException("Transaction journal path already owned or overlaps its snapshot");
        if (Files.exists(capture,LinkOption.NOFOLLOW_LINKS) || Files.exists(commit,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Mutable transaction journals must be new; reopen only an exact durable commit");
        var snapshot=snapshots.open(capture,identity.snapshot(),scope,ttl,context); var lease=new Lease(snapshot,commit,identity); leases.put(capture,lease); return lease;
    }
    /** Recover an exact already-published commit; ephemeral staged prefixes have no reopen API. */
    public synchronized RatisAtomicClient.Operation reopenCommit(Path path,RequestContext context) throws IOException {
        requireOpen(); Path commit=Objects.requireNonNull(path).toAbsolutePath().normalize();
        if (owned(commit)) throw new IOException("Mutable transaction lease owns this commit path");
        return commits.reopen(commit,context);
    }
    /** Only discard an original writable snapshot's captures; never reopen its ephemeral prefix. */
    public synchronized Cleanup reopenForRelease(Path path,RequestContext context) throws IOException {
        requireOpen(); Path capture=Objects.requireNonNull(path).toAbsolutePath().normalize();
        if (owned(capture)) throw new IOException("Transaction journal path already owned");
        var snapshot=snapshots.reopenForRelease(capture,context); var cleanup=new Cleanup(snapshot); cleanups.put(capture,cleanup); return cleanup;
    }
    private boolean owned(Path path) { return leases.containsKey(path) || cleanups.containsKey(path) || commits.operation(path)!=null || leases.values().stream().anyMatch(lease -> lease.commitPath.equals(path)); }
    public synchronized Stats stats() { return new Stats(snapshots.stats(),commits.stats()); }
    private synchronized void requireOpen() throws IOException { if (closing || closed) throw new IOException("Transaction client closing/closed"); }
    private synchronized void remove(Lease lease) { leases.remove(lease.snapshot.path(),lease); }

    public final class Cleanup implements AutoCloseable {
        private final RatisSnapshotClient.Lease snapshot;
        private boolean closed;
        private Cleanup(RatisSnapshotClient.Lease snapshot) { this.snapshot=snapshot; }
        public CompletableFuture<Void> completion() { return snapshot.releaseCompletion(); }
        public synchronized CompletableFuture<Void> resume(RequestContext context) {
            if (closed) return CompletableFuture.failedFuture(new IOException("Transaction cleanup closed"));
            try { requireOpen(); return snapshot.release(context); }
            catch (IOException failure) { return CompletableFuture.failedFuture(failure); }
        }
        @Override public void close() throws IOException {
            synchronized (this) { if (closed) return; }
            snapshot.close(); synchronized (this) { closed=true; }
            synchronized (RatisTransactionClient.this) { cleanups.remove(snapshot.path(),this); }
        }
    }

    public final class Lease implements AutoCloseable {
        private final RatisSnapshotClient.Lease snapshot;
        private final Path commitPath;
        private final Identity identity;
        private RatisAtomicClient.Operation operation;
        private boolean commitFinished,closingLease,closed;
        private Lease(RatisSnapshotClient.Lease snapshot,Path commitPath,Identity identity) { this.snapshot=snapshot; this.commitPath=commitPath; this.identity=identity; }
        public Identity identity() { return identity; }
        public Path snapshotPath() { return snapshot.path(); }
        public Path commitPath() { return commitPath; }
        public CompletableFuture<Void> completion() { return snapshot.completion(); }
        public synchronized RatisDurableSnapshot.Snapshot snapshot() throws IOException { requireLease(); return snapshot.snapshot(); }
        public synchronized RatisAtomicClient.Operation commitOperation() { return operation; }
        private void requireLease() throws IOException { if (closed || closingLease) throw new IOException("Transaction lease closing/closed"); }
        public synchronized CompletableFuture<Void> resume(RequestContext context) {
            try { requireLease(); requireOpen(); if (operation!=null) throw new IOException("Resume the exact commit operation instead of recapturing"); return snapshot.resume(context); }
            catch (IOException failure) { return CompletableFuture.failedFuture(failure); }
        }
        public synchronized CompletableFuture<Buf> get(long column,Keys key,RequestContext context) {
            try { requireLease(); requireOpen(); return snapshot.get(column,key,context); }
            catch (IOException failure) { return CompletableFuture.failedFuture(failure); }
        }
        public synchronized CompletableFuture<RangePage<KV>> page(long column,Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget,RequestContext context) {
            try { requireLease(); requireOpen(); return snapshot.page(column,lower,upper,reverse,resume,budget,context); }
            catch (IOException failure) { return CompletableFuture.failedFuture(failure); }
        }
        public synchronized RatisSnapshotClient.Lease.Cursor openCursor(long column,Keys lower,Keys upper,boolean reverse) throws IOException {
            requireLease(); requireOpen(); return snapshot.openCursor(column,lower,upper,reverse);
        }
        /** Replace or append the complete logical prefix; never interpret this argument as a delta. */
        public synchronized CompletableFuture<Map<UUID,CommandResult>> stage(List<RatisLogicalClient.Change> changes,List<RatisLogicalClient.Condition> conditions,RequestContext context) {
            try {
                requireLease(); requireOpen(); if (operation!=null) throw new IOException("Commit already owns the frozen prefix");
                changes=List.copyOf(changes); conditions=List.copyOf(conditions);
                var physical=changes.isEmpty() && conditions.isEmpty() ? null : RatisLogicalClient.plan(schema,changes,conditions,RatisTransactionPlan.MAX_BYTES);
                var plan=RatisTransactionPlan.captured(directory,identity.commit().business(),physical==null ? List.of() : physical.mutations(),physical==null ? List.of() : physical.predicates(),snapshot.snapshot());
                return snapshot.stagePrefix(plan,context);
            } catch (Exception failure) { return CompletableFuture.failedFuture(failure); }
        }
        public synchronized RatisAtomicClient.Operation commit(RequestContext context) throws IOException {
            requireLease(); requireOpen(); if (operation!=null) return operation;
            var accepted=snapshot.preparedPrefix(identity.commit());
            try { operation=commits.executeAccepted(commitPath,identity.commit(),accepted,context); return operation; }
            catch (IOException | RuntimeException failure) { snapshot.undoCommitFreeze(); throw failure; }
        }
        public synchronized CompletableFuture<Void> release(RequestContext context) {
            try {
                requireLease(); requireOpen();
                if (operation!=null && !commitFinished) {
                    if (operation.busy() || !operation.terminalReady()) throw new IOException("Commit is unfinished or unknown; resume it before releasing captures");
                    operation.close(); snapshot.finishCommitOwnership(); commitFinished=true;
                }
                return snapshot.release(context);
            } catch (IOException failure) { return CompletableFuture.failedFuture(failure); }
        }
        public CompletableFuture<Void> rollback(RequestContext context) { return release(context); }
        @Override public void close() throws IOException {
            synchronized (this) { if (closed) return; requireLease(); if (operation!=null && operation.busy()) throw new IOException("Commit work active; ownership retained"); closingLease=true; }
            try {
                if (operation!=null && !commitFinished) { operation.close(); snapshot.finishCommitOwnership(); synchronized (this) { commitFinished=true; } }
                snapshot.close(); synchronized (this) { closed=true; } remove(this);
            } finally { synchronized (this) { closingLease=false; } }
        }
    }
    @Override public void close() throws IOException {
        List<Lease> owners; List<Cleanup> cleanupOwners;
        synchronized (this) { if (closed) return; closing=true; owners=List.copyOf(leases.values()); cleanupOwners=List.copyOf(cleanups.values()); }
        IOException failure=null;
        for (var lease:owners) try { lease.close(); } catch (IOException unavailable) { if (failure==null) failure=unavailable; else failure.addSuppressed(unavailable); }
        for (var cleanup:cleanupOwners) try { cleanup.close(); } catch (IOException unavailable) { if (failure==null) failure=unavailable; else failure.addSuppressed(unavailable); }
        // Active lease resources must settle before either wrapper can release any remaining ownership.
        if (failure!=null) throw failure;
        try { commits.close(); } catch (IOException unavailable) { failure=unavailable; }
        if (failure==null) try { snapshots.close(); } catch (IOException unavailable) { failure=unavailable; }
        if (failure!=null) throw failure; synchronized (this) { closed=true; }
    }
}
