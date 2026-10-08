package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.nio.file.*;
import java.nio.channels.*;
import java.util.*;

/** Bounded local discovery and synchronous recovery ownership. Journals are retained; clients remain borrowed. */
public final class RatisParticipantRecoveryRegistry implements RatisSnapshotReadinessHelper.Recovery,AutoCloseable {
    public static final int MAX_FILES=4096, MAX_VISITS=32;
    public record Limits(int maxWorkers,int maxFiles,long maxBytes) {
        public Limits {
            if (maxWorkers<1 || maxWorkers>MAX_FILES || maxFiles<1 || maxFiles>MAX_FILES || maxBytes<1
                    || maxBytes>(long)MAX_FILES*RatisDurableParticipantRecovery.MAX_BYTES) throw new IllegalArgumentException("Invalid participant recovery registry limits");
        }
        public static Limits defaults() { return new Limits(16,MAX_FILES,1024L*1024*1024); }
    }
    public record Snapshot(int openWorkers,int persistedFiles,long reservedBytes) {}
    private record Key(UUID group,UUID transaction) {
        String filename() { return group+"-"+transaction+".bin"; }
    }
    private static final class FileEntry {
        final Key key;
        final Path path;
        long bytes,temporaryBytes;
        boolean persisted,reserved,queued,complete;
        byte[] preparedDigest;
        ApplyPosition preparedPosition;
        FileEntry(Key key,Path path,long bytes,boolean persisted) { this.key=key; this.path=path; this.bytes=bytes; this.persisted=persisted; }
    }
    private static final class Worker {
        final FileEntry file;
        final RatisDurableParticipantRecovery journal;
        final RatisParticipantRecovery driver;
        boolean retiring,complete;
        Worker(FileEntry file,RatisDurableParticipantRecovery journal,RatisParticipantRecovery driver) { this.file=file; this.journal=journal; this.driver=driver; }
    }
    private final Path directory;
    private final RatisTransactionStatuses homes;
    private final RatisRoutedClient data;
    private final boolean takeover;
    private final Set<UUID> allowedGroups;
    private final Limits limits;
    private final FileChannel lockChannel;
    private final FileLock directoryLock;
    private final Map<Key,FileEntry> files=new LinkedHashMap<>();
    private final Map<Key,Worker> workers=new LinkedHashMap<>();
    private final ArrayDeque<Key> pending=new ArrayDeque<>();
    private long reservedBytes;
    private int opening,persistedFiles;
    private boolean discovered,active,stopping,closing;

    public RatisParticipantRecoveryRegistry(Path existingDirectory,RatisTransactionStatuses homes,RatisRoutedClient data,
            boolean takeover,Set<UUID> allowedGroups,Limits limits) throws IOException {
        this.directory=Objects.requireNonNull(existingDirectory).toAbsolutePath().normalize(); this.homes=Objects.requireNonNull(homes); this.data=Objects.requireNonNull(data);
        this.takeover=takeover;
        this.allowedGroups=Set.copyOf(Objects.requireNonNull(allowedGroups)); this.limits=Objects.requireNonNull(limits);
        if (this.allowedGroups.size()>MAX_FILES) throw new IllegalArgumentException("Too many allowed recovery groups");
        homes.requireDatabaseBinding(data.directory().clusterId(),data.directory().databaseId());
        if (!Files.isDirectory(directory,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Participant recovery directory must already exist");
        var lockPath=directory.resolve("registry.lock");
        if (Files.exists(lockPath,LinkOption.NOFOLLOW_LINKS)) requireEmptyLock(lockPath);
        lockChannel=FileChannel.open(lockPath,StandardOpenOption.CREATE,StandardOpenOption.WRITE);
        FileLock acquired=null;
        try {
            try { acquired=lockChannel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Participant recovery directory already owned",owned); }
            if (acquired==null) throw new IOException("Participant recovery directory already owned"); directoryLock=acquired;
        } catch (IOException | RuntimeException | Error failure) {
            try { if (acquired!=null) acquired.release(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            try { lockChannel.close(); } catch (IOException cleanup) { failure.addSuppressed(cleanup); } throw failure;
        }
    }
    /** One bounded startup scan; files beyond worker capacity remain candidates, never replayed before their journal is inspected. */
    public int discover() throws IOException {
        begin();
        try {
            synchronized (this) { if (discovered) return 0; }
            scan(); int added=0;
            final int visits; synchronized (this) { visits=Math.min(MAX_VISITS,pending.size()); }
            for (int i=0;i<visits;i++) {
                final FileEntry file;
                synchronized (this) { if (stopping || workers.size()+opening>=limits.maxWorkers()) break; file=poll(); }
                if (file==null) break;
                try { if (open(file,null)!=null) added++; }
                finally { synchronized (this) { requeue(file); } }
            }
            return added;
        } finally { end(); }
    }
    /** Visits and opens at most 32 candidates/workers. A failing visit is rotated before the exception is returned. */
    public int cycle(int visits) throws IOException {
        if (visits<1 || visits>MAX_VISITS) throw new IllegalArgumentException("Invalid participant recovery quantum");
        begin();
        try {
            scan(); final int count; synchronized (this) { count=Math.min(visits,pending.size()); }
            int steps=0;
            for (int i=0;i<count;i++) {
                final FileEntry file; synchronized (this) { if (stopping) break; file=poll(); }
                if (file==null) break;
                try {
                    Worker worker; synchronized (this) { worker=workers.get(file.key); }
                    if (worker!=null && worker.retiring) { retire(worker); continue; }
                    if (worker==null && file.reserved) { releaseReservation(file); if (!file.persisted || file.complete) continue; }
                    if (worker==null) worker=open(file,null);
                    if (worker==null) continue;
                    synchronized (this) { if (stopping) break; }
                    run(worker); steps++;
                } finally { synchronized (this) { requeue(file); } }
            }
            return steps;
        } finally { end(); }
    }
    /** Admit and force recovery ownership without proposing any home or participant mutation. */
    public boolean register(UUID group,PreparedParticipant prepared) throws IOException { return visit(group,prepared,false); }
    @Override public boolean step(UUID group,PreparedParticipant prepared) throws IOException { return visit(group,prepared,true); }
    private boolean visit(UUID group,PreparedParticipant prepared,boolean advance) throws IOException {
        Objects.requireNonNull(group); Objects.requireNonNull(prepared);
        if (!allowedGroups.contains(group) || !group.equals(prepared.preparation().business().groupId())) throw new IllegalArgumentException("Participant recovery group is not locally allowed");
        begin();
        try {
            scan(); var key=new Key(group,prepared.preparation().transactionId()); FileEntry file;
            synchronized (this) {
                file=files.get(key);
                if (file==null) {
                    if (files.size()>=limits.maxFiles()) throw new IOException("Participant recovery file admission exhausted");
                    file=new FileEntry(key,directory.resolve(key.filename()),0,false); files.put(key,file);
                }
                if (file.complete && file.preparedDigest!=null) { requireSelected(file,prepared); return false; }
                unqueue(file);
            }
            try {
                Worker worker; synchronized (this) { worker=workers.get(key); }
                if (worker!=null && worker.retiring) { retire(worker); worker=null; }
                if (worker==null) worker=open(file,prepared);
                if (worker==null) return false;
                if (!worker.journal.snapshot().prepared().equals(prepared)) throw new IOException("Recovery journal differs from selected native preparation");
                synchronized (this) { if (stopping) return false; }
                if (!advance) return true;
                var result=run(worker); return result.action()!=RatisParticipantRecovery.Action.WAITING;
            } finally { synchronized (this) { requeue(file); } }
        } finally { end(); }
    }
    private static void requireSelected(FileEntry file,PreparedParticipant selected) throws IOException {
        if (!file.preparedPosition.equals(selected.position()) || !Arrays.equals(file.preparedDigest,selected.preparation().digest())) throw new IOException("Completed recovery native preparation changed");
    }
    public synchronized Snapshot stats() { return new Snapshot(workers.size()+opening,persistedFiles,reservedBytes); }

    private void scan() throws IOException {
        synchronized (this) { if (discovered) return; }
        var found=new LinkedHashMap<Key,FileEntry>(); var temporary=new HashMap<Key,Long>(); long bytes=0; int visited=0;
        try (var paths=Files.newDirectoryStream(directory)) {
            for (var path:paths) {
                if (++visited>3L*limits.maxFiles()+16) throw new IOException("Participant recovery directory entry bound exceeded");
                String name=path.getFileName().toString();
                if (name.equals("registry.lock")) { requireEmptyLock(path); continue; }
                if (name.endsWith(".bin.lock")) { var key=parse(name.substring(0,name.length()-5)); requireAllowed(key); requireEmptyLock(path); continue; }
                if (name.endsWith(".bin.pending")) {
                    var key=parse(name.substring(0,name.length()-8)); requireAllowed(key); long size=temporarySize(path);
                    if (temporary.size()>=limits.maxFiles() || size>limits.maxBytes()-bytes) throw new IOException("Recovery pending-file bound exceeded");
                    temporary.put(key,size); bytes+=size; continue;
                }
                if (!name.endsWith(".bin")) throw new IOException("Unknown participant recovery directory entry");
                Key key=parse(name); requireAllowed(key);
                if (!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Participant recovery journal must be a regular file");
                long size=fileSize(path); if (found.size()>=limits.maxFiles() || size>limits.maxBytes()-bytes) throw new IOException("Persisted recovery file/byte bound exceeded");
                found.put(key,new FileEntry(key,path,size,true)); bytes+=size;
            }
        }
        for (var entry:temporary.entrySet()) {
            var file=found.get(entry.getKey()); if (file==null) throw new IOException("Orphan participant recovery pending file requires inspection"); file.temporaryBytes=entry.getValue();
        }
        var ordered=new ArrayList<>(found.values()); ordered.sort(Comparator.comparing(file -> file.key.filename()));
        synchronized (this) {
            for (var file:ordered) { files.put(file.key,file); enqueue(file); }
            persistedFiles=ordered.size(); reservedBytes=bytes; discovered=true;
        }
    }
    private void requireAllowed(Key key) throws IOException { if (!allowedGroups.contains(key.group)) throw new IOException("Persisted recovery group is not locally allowed"); }
    private static void requireEmptyLock(Path path) throws IOException { if (!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || Files.size(path)!=0) throw new IOException("Recovery lock must be an empty regular file"); }
    private static long temporarySize(Path path) throws IOException {
        if (!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Recovery pending journal must be a regular file");
        long size=Files.size(path); if (size>RatisDurableParticipantRecovery.MAX_BYTES) throw new IOException("Recovery pending journal exceeds byte bound"); return size;
    }
    private static Key parse(String name) throws IOException {
        if (name.length()!=77 || name.charAt(36)!='-' || !name.endsWith(".bin")) throw new IOException("Noncanonical participant recovery filename");
        try {
            var key=new Key(UUID.fromString(name.substring(0,36)),UUID.fromString(name.substring(37,73)));
            if (!name.equals(key.filename())) throw new IllegalArgumentException("Noncanonical UUID spelling"); return key;
        } catch (IllegalArgumentException invalid) { throw new IOException("Noncanonical participant recovery filename",invalid); }
    }
    private static long fileSize(Path path) throws IOException {
        long size=Files.size(path); if (size<32 || size>RatisDurableParticipantRecovery.MAX_BYTES) throw new IOException("Participant recovery journal size exceeds bound"); return size;
    }
    private Worker open(FileEntry file,PreparedParticipant selected) throws IOException {
        Worker existing; synchronized (this) { existing=workers.get(file.key); }
        if (existing!=null) {
            if (selected!=null && !selected.equals(existing.journal.snapshot().prepared())) throw new IOException("Owned recovery journal/native preparation changed"); return existing;
        }
        if (file.reserved) releaseReservation(file); // A previous failed size check retained accounting after its journal closed.
        makeRoom(); reserve(file);
        RatisDurableParticipantRecovery journal=null;
        try {
            if (file.persisted) journal=RatisDurableParticipantRecovery.reopen(file.path,homes,data);
            else {
                if (selected==null) throw new IOException("Unpublished recovery file has no native preparation input");
                var route=data.directory().ranges().stream().filter(r -> r.groupId().equals(file.key.group)).findFirst().orElseThrow(() -> new IOException("Allowed participant route is absent"));
                journal=RatisDurableParticipantRecovery.create(file.path,homes,data,selected,route);
                synchronized (this) { file.persisted=true; persistedFiles++; }
            }
            var snapshot=journal.snapshot();
            if (!file.key.group.equals(snapshot.participantRoute().groupId()) || !file.key.transaction.equals(snapshot.prepared().preparation().transactionId())
                    || selected!=null && !selected.equals(snapshot.prepared())) throw new IOException("Recovery filename/selected proof differs from journal identity");
            file.preparedDigest=snapshot.prepared().preparation().digest(); file.preparedPosition=snapshot.prepared().position();
            if (complete(snapshot)) { file.complete=true; journal.close(); journal=null; releaseReservation(file); return null; }
            var driver=new RatisParticipantRecovery(journal,homes,data,takeover); var worker=new Worker(file,journal,driver);
            synchronized (this) { workers.put(file.key,worker); opening--; } return worker;
        } catch (IOException | RuntimeException | Error failure) {
            boolean retained=false;
            if (journal!=null) try { journal.close(); } catch (IOException cleanup) {
                failure.addSuppressed(cleanup); var owner=new Worker(file,journal,null); owner.retiring=true; owner.complete=file.complete;
                synchronized (this) { workers.put(file.key,owner); opening--; } retained=true;
            }
            if (!retained) try { releaseReservation(file); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    private void makeRoom() throws IOException {
        Worker idle=null;
        synchronized (this) {
            if (stopping) throw new IOException("Participant recovery registry is stopping");
            if (workers.size()+opening<limits.maxWorkers()) return;
            if (!workers.isEmpty()) { idle=workers.values().iterator().next(); workers.remove(idle.file.key); workers.put(idle.file.key,idle); }
        }
        if (idle==null) throw new IOException("Participant recovery worker admission exhausted"); retire(idle);
        synchronized (this) { requeue(idle.file); }
    }
    private synchronized void reserve(FileEntry file) throws IOException {
        if (file.reserved || workers.size()+opening>=limits.maxWorkers()) throw new IOException("Participant recovery worker already owned/exhausted");
        long additional=RatisDurableParticipantRecovery.MAX_BYTES-file.bytes;
        if (additional>limits.maxBytes()-reservedBytes) throw new IOException("Participant recovery byte admission exhausted");
        file.reserved=true; opening++; reservedBytes+=additional;
    }
    private void releaseReservation(FileEntry file) throws IOException {
        boolean exists=Files.exists(file.path,LinkOption.NOFOLLOW_LINKS); long actual=exists ? fileSize(file.path) : 0;
        long temporary=remainingTemporary(file);
        if (exists && !Files.isRegularFile(file.path,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Recovery journal ceased being a regular file");
        if (!exists && !file.persisted) { if (temporary!=0 || Files.exists(file.path.resolveSibling(file.path.getFileName()+".pending"),LinkOption.NOFOLLOW_LINKS)) throw new IOException("Unpublished recovery pending data requires inspection"); cleanupUnpublishedLock(file); }
        synchronized (this) {
            if (!file.reserved) return;
            if (file.persisted && !exists) throw new IOException("Persisted recovery journal disappeared");
            if (!file.persisted && exists) { file.persisted=true; persistedFiles++; }
            reservedBytes+=actual-RatisDurableParticipantRecovery.MAX_BYTES+temporary-file.temporaryBytes; file.bytes=actual; file.temporaryBytes=temporary; file.reserved=false; opening--;
            if (!file.persisted && !exists) { files.remove(file.key,file); unqueue(file); }
        }
    }
    /** Directory ownership and a successful journal-lock acquisition prove this empty unpublished sidecar is ours to retire. */
    private void cleanupUnpublishedLock(FileEntry file) throws IOException {
        var path=file.path.resolveSibling(file.path.getFileName()+".lock");
        if (Files.exists(path,LinkOption.NOFOLLOW_LINKS)) {
            requireEmptyLock(path);
            try (var channel=FileChannel.open(path,StandardOpenOption.WRITE)) {
                final FileLock acquired;
                try { acquired=channel.tryLock(); } catch (OverlappingFileLockException owned) { throw new IOException("Unpublished recovery journal remains owned",owned); }
                if (acquired==null) throw new IOException("Unpublished recovery journal remains owned");
                try (acquired) {
                    if (Files.exists(file.path,LinkOption.NOFOLLOW_LINKS)) throw new IOException("Recovery journal appeared during unpublished cleanup");
                    Files.delete(path);
                }
            }
        }
        SessionJournalFiles.force(directory);
    }
    private RatisParticipantRecovery.Step run(Worker worker) throws IOException {
        try {
            var step=worker.driver.step();
            if (step.complete()) { worker.complete=true; worker.file.complete=true; retire(worker); }
            return step;
        } catch (IOException | RuntimeException | Error failure) {
            try { retire(worker); } catch (IOException cleanup) { failure.addSuppressed(cleanup); }
            throw failure;
        }
    }
    private void retire(Worker worker) throws IOException {
        worker.retiring=true; if (worker.driver!=null) worker.driver.close(); worker.journal.close();
        // Keep ownership/accounting if cleanup or size verification fails; a later cycle/close retries cleanup only.
        boolean exists=Files.isRegularFile(worker.file.path,LinkOption.NOFOLLOW_LINKS); if (!exists) throw new IOException("Owned participant recovery journal disappeared");
        long bytes=fileSize(worker.file.path),temporary=remainingTemporary(worker.file);
        synchronized (this) {
            if (workers.remove(worker.file.key,worker)) {
                reservedBytes+=bytes-RatisDurableParticipantRecovery.MAX_BYTES+temporary-worker.file.temporaryBytes; worker.file.bytes=bytes; worker.file.temporaryBytes=temporary; worker.file.reserved=false;
            }
            if (worker.complete) unqueue(worker.file);
        }
    }
    private static long remainingTemporary(FileEntry file) throws IOException {
        var path=file.path.resolveSibling(file.path.getFileName()+".pending"); return Files.exists(path,LinkOption.NOFOLLOW_LINKS) ? temporarySize(path) : 0;
    }
    private static boolean complete(RatisDurableParticipantRecovery.Snapshot snapshot) {
        return snapshot.outcome()!=null && snapshot.observedHome().phase()!=TransactionStatus.Phase.PREPARING
                && snapshot.observedHome().participants().stream().filter(p -> p.groupId().equals(snapshot.participantRoute().groupId())).findFirst().orElseThrow().finalized();
    }
    private synchronized void begin() throws IOException { if (stopping || active || closing) throw new IOException("Participant recovery registry closed or operation active"); active=true; }
    private synchronized void end() { active=false; }
    private FileEntry poll() { var key=pending.pollFirst(); if (key==null) return null; var file=files.get(key); file.queued=false; return file; }
    private void enqueue(FileEntry file) { if (!file.queued) { file.queued=true; pending.addLast(file.key); } }
    private void unqueue(FileEntry file) { if (file.queued) { pending.remove(file.key); file.queued=false; } }
    private void requeue(FileEntry file) {
        var worker=workers.get(file.key);
        if (file.reserved || file.persisted && (!file.complete || worker!=null && worker.retiring)) enqueue(file);
    }
    @Override public void close() throws IOException {
        final List<Worker> owned;
        synchronized (this) { stopping=true; if (active || closing) throw new IOException("Participant recovery registry active; ownership retained"); closing=true; owned=List.copyOf(workers.values()); }
        IOException failure=null;
        try {
            for (var worker:owned) try { retire(worker); } catch (IOException cleanup) { if (failure==null) failure=cleanup; else if (failure!=cleanup) failure.addSuppressed(cleanup); }
            final List<FileEntry> reservations;
            synchronized (this) { reservations=files.values().stream().filter(file -> file.reserved && !workers.containsKey(file.key)).toList(); }
            for (var file:reservations) try { releaseReservation(file); } catch (IOException cleanup) { if (failure==null) failure=cleanup; else if (failure!=cleanup) failure.addSuppressed(cleanup); }
            if (failure!=null) throw failure;
            if (directoryLock.isValid()) directoryLock.release(); lockChannel.close();
        } finally { synchronized (this) { closing=false; } }
    }
}
