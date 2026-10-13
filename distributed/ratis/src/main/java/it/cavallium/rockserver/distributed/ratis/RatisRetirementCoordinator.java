package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.ReplicatedRangeStore;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.security.*;
import java.util.*;

/** Node-bounded retirement hints and durable exact request replay. This driver never consumes client outcomes. */
public final class RatisRetirementCoordinator implements AutoCloseable {
    public static final int MAX_SOURCES=8192,MAX_VISITS=32,SCAN_ROWS=16;
    public enum Mode { TRANSACTION, SNAPSHOT, ORPHAN }
    @FunctionalInterface public interface Reader { ReplicatedRangeStore.RetirementPage read(byte[] after,int scans) throws IOException; }
    public record Source(UUID group,Mode mode,Reader reader) {
        public Source { Objects.requireNonNull(group); Objects.requireNonNull(mode); Objects.requireNonNull(reader); }
    }
    private static final class Scan {
        final Source source; byte[] after; int round;
        Scan(Source source) { this.source=source; }
    }
    private final Path directory;
    private final byte[] binding;
    private final RatisRetirementJournal.Limits limits;
    private RatisRetirementJournal journal;
    private final RatisTransactionStatuses homes;
    private final RatisSnapshotFences snapshots;
    private final Map<UUID,RatisRoutedClient> data;
    private final List<Scan> scans;
    private final ArrayDeque<UUID> retries=new ArrayDeque<>();
    private int cursor;
    private boolean retryTurn,active,closed;
    private volatile Throwable lastFailure;

    public RatisRetirementCoordinator(Path directory,RatisTransactionStatuses homes,RatisSnapshotFences snapshots,Map<UUID,RatisRoutedClient> data,List<Source> sources,RatisRetirementJournal.Limits limits) throws IOException {
        this.directory=Objects.requireNonNull(directory); this.homes=homes; this.snapshots=snapshots; this.data=Map.copyOf(data); this.limits=Objects.requireNonNull(limits);
        Objects.requireNonNull(sources); if (sources.size()>MAX_SOURCES) throw new IllegalArgumentException("Too many retirement sources");
        var keys=new HashSet<String>();
        for (var source:sources) {
            if (!keys.add(source.group()+":"+source.mode()) || source.mode()==Mode.SNAPSHOT && snapshots==null || source.mode()!=Mode.SNAPSHOT && homes==null) throw new IllegalArgumentException("Invalid retirement source/client binding");
        }
        scans=sources.stream().map(Scan::new).toList();
        if(this.data.isEmpty() || this.data.size()>DatabaseDomainRoster.MAX_DOMAINS)throw new IllegalArgumentException("Invalid retirement DATA domain scope");
        var bytes=ByteBuffer.allocate(70+48*this.data.size()).putInt(this.data.size());
        UUID cluster=null,database=null;
        for(var domain:this.data.keySet().stream().sorted(RangeDirectoryCodec.UUID_ORDER).toList()){var client=this.data.get(domain);var route=client.directory();if(!domain.equals(route.domainId()) || cluster!=null && (!cluster.equals(route.clusterId()) || !database.equals(route.databaseId())))throw new IllegalArgumentException("Retirement DATA database/domain differs");cluster=route.clusterId();database=route.databaseId();bytes.putLong(domain.getMostSignificantBits()).putLong(domain.getLeastSignificantBits()).put(client.sessionIdentityDigest());}
        bytes.put((byte)(homes==null ? 0 : 1)).put(homes==null ? new byte[32] : homes.identityDigest())
                .put((byte)(snapshots==null ? 0 : 1)).put(snapshots==null ? new byte[32] : snapshots.identityDigest());
        try { binding=MessageDigest.getInstance("SHA-256").digest(bytes.array()); } catch (NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        journal=RatisRetirementJournal.open(directory,binding,limits); resetRetries();
    }
    public Throwable lastFailure() { return lastFailure; }
    public synchronized RatisRetirementJournal.Stats stats() throws IOException { requireOpen(); return journal.stats(); }
    private void requireOpen() throws IOException { if (closed) throw new IOException("Retirement coordinator closed"); }
    private void resetRetries() throws IOException { retries.clear(); for (var operation:journal.pending()) retries.addLast(operation.targetGroup()); }

    /** Each visit examines one bounded local page or retries one exact frame, with at most one mutation request. */
    public int cycle(int visits) throws IOException {
        if (visits<1 || visits>MAX_VISITS) throw new IllegalArgumentException("Invalid retirement quantum");
        synchronized (this) { requireOpen(); if (active) throw new IOException("Retirement quantum already active"); active=true; }
        IOException failure=null; int attempts=0;
        try {
            if (journal.fenced()) { journal.close(); journal=RatisRetirementJournal.open(directory,binding,limits); resetRetries(); }
            for (int n=0;n<visits;n++) {
                try {
                    boolean retry=!retries.isEmpty() && (retryTurn || scans.isEmpty()); retryTurn=!retryTurn;
                    if (retry) { if (retryOne()) attempts++; }
                    else if (!scans.isEmpty()) { if (discoverOne()) attempts++; }
                } catch (IOException unavailable) { if (failure==null) failure=unavailable; else if (failure!=unavailable) failure.addSuppressed(unavailable); }
                catch (RuntimeException invalid) { var unavailable=new IOException("Retirement visit failed",invalid); if (failure==null) failure=unavailable; else failure.addSuppressed(unavailable); }
                if (journal.fenced()) break;
            }
            return attempts;
        } catch (IOException unavailable) { failure=unavailable; return attempts; }
        finally { lastFailure=failure; synchronized (this) { active=false; } }
    }
    private boolean retryOne() throws IOException {
        UUID group=retries.pollFirst(); if (group==null) return false;
        var operation=journal.pending().stream().filter(op -> op.targetGroup().equals(group)).findFirst().orElse(null); if (operation==null) return false;
        try { journal.invoke(operation,() -> execute(operation)); journal.remove(operation); return true; }
        finally { if (journal.pending().stream().anyMatch(op -> op.targetGroup().equals(group))) retries.addLast(group); }
    }
    private boolean submit(RatisRetirementOperation operation) throws IOException {
        if (!journal.reserve(operation)) return false;
        if (!retries.contains(operation.targetGroup())) retries.addLast(operation.targetGroup());
        journal.invoke(operation,() -> execute(operation)); journal.remove(operation); retries.remove(operation.targetGroup()); return true;
    }
    private void execute(RatisRetirementOperation operation) throws IOException {
        switch (operation.type()) {
            case PARTICIPANT -> data(operation.participant().consumed().domainId()).retireParticipant(operation.participant());
            case HOME -> { if (homes==null) throw new IOException("Transaction HOME client missing"); homes.retire(operation.home()); }
            case SNAPSHOT_GATE -> data(operation.snapshotGate().consumed().fence().domainId()).retireSnapshotGate(operation.snapshotGate());
            case SNAPSHOT_HOME -> { if (snapshots==null) throw new IOException("Snapshot HOME client missing"); snapshots.retire(operation.snapshotHome()); }
            case RELEASE -> {var client=data(operation.release().fence().domainId());client.executeSnapshotGate(client.prepareSnapshotGate(operation.release()));}
        }
    }
    private boolean discoverOne() throws IOException {
        var scan=scans.get(cursor); cursor=(cursor+1)%scans.size();
        byte[] before=scan.after; int round=scan.round;
        var page=scan.source.reader().read(before,SCAN_ROWS);
        if (page==null) return false;
        if (!page.groupId().equals(scan.source.group()) || page.position()==null) throw new IOException("Retirement source identity changed");
        byte[] next=page.next(); int keyBytes=scan.source.mode()==Mode.ORPHAN ? 32 : 16;
        if (next!=null && (next.length!=keyBytes || before!=null && Arrays.compareUnsigned(next,before)<=0) || page.more() && next==null) throw new IOException("Retirement discovery cursor did not advance");
        scan.after=page.more() ? next : null; if (!page.more()) scan.round=(scan.round+1)%SnapshotFence.MAX_PARTICIPANTS;
        var candidate=page.candidate(); if (candidate==null) return false;
        if (scan.source.mode()==Mode.SNAPSHOT ? candidate.snapshot()==null || candidate.orphan() : candidate.transaction()==null || candidate.orphan()!=(scan.source.mode()==Mode.ORPHAN)) throw new IOException("Retirement candidate role changed");
        if (candidate.snapshot()!=null) return snapshot(page,candidate.snapshot(),round);
        return transaction(page,candidate.transaction(),candidate.orphan(),round);
    }
    private boolean transaction(ReplicatedRangeStore.RetirementPage page,TransactionStatus status,boolean orphan,int round) throws IOException {
        var identity=new HomeIdentity(page.groupId(),page.rangeEpoch(),page.schemaDigest(),page.columnId(),status);
        if (orphan) return submit(RatisRetirementOperation.home(new HomeRetirement(identity,null,List.of())));
        var participants=status.participants(); int start=(Math.floorMod(status.transactionId().hashCode(),participants.size())+round)%participants.size();
        var participant=participants.get(start); var original=new ParticipantIdentity(page.groupId(),page.rangeEpoch(),participant.groupId(),status);
        var data=data(status.domainId());
        var proof=data.participantFence(original);
        if (proof==null) {
            if (data.stateView(participant.groupId()).minimumSessionEpoch()<=status.operation().epoch()) return false;
            var outcome=data.finalizedParticipant(participant.groupId(),status.transactionId());
            if (outcome!=null && outcome.finalization().decision().sameDefinition(status) && outcome.finalization().homeGroup().equals(page.groupId()) && outcome.finalization().homeEpoch()==page.rangeEpoch())
                return submit(RatisRetirementOperation.participant(new ParticipantRetirement(outcome.finalization(),outcome.position(),status)));
            proof=data.participantFence(original); if (proof==null) return false;
        }
        if (page.minimumEpoch()<=status.operation().epoch()) return false;
        var proofs=new ArrayList<ParticipantFenceProof>(participants.size());
        for (int n=0;n<participants.size();n++) {
            var p=participants.get(n); var actual=n==start ? proof : data.participantFence(new ParticipantIdentity(page.groupId(),page.rangeEpoch(),p.groupId(),status));
            if (actual==null) return false; proofs.add(actual);
        }
        return submit(RatisRetirementOperation.home(new HomeRetirement(identity,status,proofs)));
    }
    private boolean snapshot(ReplicatedRangeStore.RetirementPage page,SnapshotFenceStatus status,int round) throws IOException {
        var fence=status.fence(); var participants=fence.participants(); int start=(Math.floorMod(fence.fenceId().hashCode(),participants.size())+round)%participants.size(); UUID group=participants.get(start).groupId();
        var data=data(fence.domainId());
        var proof=data.snapshotFence(fence,group);
        if (proof==null) {
            if (data.stateView(group).minimumSessionEpoch()<=fence.operation().epoch()) return false;
            final SnapshotGate gate;
            try { gate=data.snapshotGate(fence,group); }
            catch (IOException unavailableOrChanged) { proof=data.snapshotFence(fence,group); if (proof==null) throw unavailableOrChanged; return finishSnapshot(page,status,start,proof); }
            if (gate!=null) {
                if (gate.active()) return submit(RatisRetirementOperation.release(RatisSnapshotRecord.release(status,group)));
                if (gate.released()!=null) return submit(RatisRetirementOperation.snapshotGate(new SnapshotGateRetirement(status,group,gate.released())));
            }
            proof=data.snapshotFence(fence,group); if (proof==null) return false;
        }
        return finishSnapshot(page,status,start,proof);
    }
    private boolean finishSnapshot(ReplicatedRangeStore.RetirementPage page,SnapshotFenceStatus status,int start,SnapshotFenceProof proof) throws IOException {
        var fence=status.fence(); if (page.minimumEpoch()<=fence.operation().epoch()) return false;
        var proofs=new ArrayList<SnapshotFenceProof>(fence.participants().size());
        for (int n=0;n<fence.participants().size();n++) {
            UUID group=fence.participants().get(n).groupId(); var actual=n==start ? proof : data(fence.domainId()).snapshotFence(fence,group); if (actual==null) return false; proofs.add(actual);
        }
        var identity=new SnapshotHomeIdentity(page.groupId(),page.rangeEpoch(),page.schemaDigest(),page.columnId(),fence);
        return submit(RatisRetirementOperation.snapshotHome(new SnapshotHomeRetirement(identity,status,proofs)));
    }
    private RatisRoutedClient data(UUID domain)throws IOException{var client=data.get(domain);if(client==null)throw new IOException("Retirement participant domain is not configured");return client;}
    @Override public synchronized void close() throws IOException { if (closed) return; if (active) throw new IOException("Retirement quantum active; ownership retained"); journal.close(); closed=true; }
}
