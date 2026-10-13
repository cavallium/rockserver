package it.cavallium.rockserver.distributed.ratis;

import org.apache.ratis.server.impl.LogResourceStateMachine;
import org.apache.ratis.protocol.exceptions.ServerNotReadyException;
import java.io.IOException;
import java.util.*;
import java.util.concurrent.atomic.*;

/** Hard shared ownership ledgers for decoded intake, log residency/headers, and bounded cold reads. */
public final class RatisLogResourceBudget implements LogResourceStateMachine.Resources,AutoCloseable {
    public record Limits(int intakeOwners,long intakeBytes,int replyOwners,long residentBytes,long headerEntries,int segments,
                         int readOwners,long readBytes,int decodeOwners,long decodeBytes,int protectedOwners,long protectedIntakeBytes,int protectedReplyOwners) {
        public Limits { if(intakeOwners<1 || intakeBytes<1 || replyOwners<1 || residentBytes<1 || headerEntries<1 || segments<1 || readOwners<1 || readBytes<1 || decodeOwners<1 || decodeBytes<1 || protectedOwners<1 || protectedIntakeBytes<1 || protectedReplyOwners<1)throw new IllegalArgumentException("Log-resource limits must be positive"); }
        public static Limits defaults() { return new Limits(64,160L<<20,128,160L<<20,262144,4096,2,224L<<20,2,160L<<20,8,64L<<10,16); }
    }
    public record Usage(long intakeBytes,int intakeOwners,int replies,long residentBytes,long headers,int segments,
                        long readBytes,int readOwners,long decodeBytes,int decodeOwners,long protectedBytes,int protectedOwners,int protectedReplies,long controlBytes,int controlOwners,int classificationOwners) {
        static final Usage ZERO=new Usage(0,0,0,0,0,0,0,0,0,0,0,0,0,0,0,0);
        Usage plus(Usage d,int direction) { return new Usage(Math.addExact(intakeBytes,d.intakeBytes*direction),Math.addExact(intakeOwners,d.intakeOwners*direction),Math.addExact(replies,d.replies*direction),Math.addExact(residentBytes,d.residentBytes*direction),Math.addExact(headers,d.headers*direction),Math.addExact(segments,d.segments*direction),Math.addExact(readBytes,d.readBytes*direction),Math.addExact(readOwners,d.readOwners*direction),Math.addExact(decodeBytes,d.decodeBytes*direction),Math.addExact(decodeOwners,d.decodeOwners*direction),Math.addExact(protectedBytes,d.protectedBytes*direction),Math.addExact(protectedOwners,d.protectedOwners*direction),Math.addExact(protectedReplies,d.protectedReplies*direction),Math.addExact(controlBytes,d.controlBytes*direction),Math.addExact(controlOwners,d.controlOwners*direction),Math.addExact(classificationOwners,d.classificationOwners*direction)); }
    }
    public static final class Shared {
        private final Limits limits; private final AtomicReference<Usage> used=new AtomicReference<>(Usage.ZERO);
        public Shared(Limits limits) { this.limits=Objects.requireNonNull(limits); }
        public Usage snapshot() { return used.get(); }
        private boolean acquire(Usage delta) { for(;;) { var before=used.get(); final Usage next; try { next=before.plus(delta,1); } catch(ArithmeticException tooLarge) { return false; } if(!within(next))return false; if(used.compareAndSet(before,next))return true; } }
        private boolean within(Usage u) { return u.intakeBytes<=limits.intakeBytes && u.intakeOwners<=limits.intakeOwners && u.replies<=limits.replyOwners && u.residentBytes<=limits.residentBytes && u.headers<=limits.headerEntries && u.segments<=limits.segments && u.readBytes<=limits.readBytes && u.readOwners<=limits.readOwners && u.decodeBytes<=limits.decodeBytes && u.decodeOwners<=limits.decodeOwners && u.protectedBytes<=limits.protectedIntakeBytes && u.protectedOwners<=limits.protectedOwners && u.protectedReplies<=limits.protectedReplyOwners && u.controlBytes<=limits.intakeBytes && u.controlOwners<=limits.protectedOwners && u.classificationOwners<=limits.protectedOwners; }
        private void release(Usage delta) { for(;;) { var before=used.get(); var next=before.plus(delta,-1); if(next.intakeBytes<0 || next.intakeOwners<0 || next.replies<0 || next.residentBytes<0 || next.headers<0 || next.segments<0 || next.readBytes<0 || next.readOwners<0 || next.decodeBytes<0 || next.decodeOwners<0 || next.protectedBytes<0 || next.protectedOwners<0 || next.protectedReplies<0 || next.controlBytes<0 || next.controlOwners<0 || next.classificationOwners<0)throw new IllegalStateException("Log-resource ownership underflow"); if(used.compareAndSet(before,next))return; } }
    }
    @FunctionalInterface public interface DiskReserve { DiskLease reserve(long physicalGrowthUpperBound,boolean protectedProgress) throws IOException; }
    /** The runway owner retains physical charge until a later complete census; this notification is not a refund. */
    @FunctionalInterface public interface DiskLease { void actualFinished(); }
    private final UUID group;
    private final Shared node,pool,local;
    private final DiskReserve disk;
    private final AtomicBoolean stopped=new AtomicBoolean();
    public RatisLogResourceBudget(UUID group,Shared node,Shared pool,Limits groupLimits,DiskReserve disk) {
        this.group=Objects.requireNonNull(group); this.node=Objects.requireNonNull(node); this.pool=Objects.requireNonNull(pool); local=new Shared(groupLimits); this.disk=Objects.requireNonNull(disk);
        if(node==pool)throw new IllegalArgumentException("Node and pool ledgers must be distinct");
    }
    public UUID groupId() { return group; }
    public Usage snapshot() { return local.snapshot(); }
    private void requireAdmission() throws IOException { if(stopped.get())throw unavailable("admission closed"); }
    private IOException unavailable(String reason) { return new ServerNotReadyException("Raft log resource budget for "+group+": "+reason); }
    private LogResourceStateMachine.Lease acquire(Usage delta,boolean decodeOnly) throws IOException {
        requireAdmission(); var owners=decodeOnly ? List.of(node) : List.of(node,pool,local); var acquired=new ArrayList<Shared>(owners.size());
        for(var owner:owners) { if(!owner.acquire(delta)) { acquired.forEach(shared->shared.release(delta)); throw unavailable("capacity exhausted"); } acquired.add(owner); }
        if(stopped.get()) { acquired.forEach(shared->shared.release(delta)); throw unavailable("admission closed"); }
        return new LogResourceStateMachine.Lease() { final AtomicBoolean released=new AtomicBoolean(); public void close() { if(released.compareAndSet(false,true))acquired.forEach(owner->owner.release(delta)); } };
    }
    private static Usage intake(long bytes,boolean protectedProgress) {
        long weight=Math.addExact(Math.multiplyExact(bytes,2),4096);
        return protectedProgress ? new Usage(0,0,0,0,0,0,0,0,0,0,weight,1,0,0,0,0) : new Usage(weight,1,0,0,0,0,0,0,0,0,0,0,0,0,0,0);
    }
    @Override public LogResourceStateMachine.Lease reserveStream() throws IOException { return acquire(new Usage(0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0),true); }
    @Override public LogResourceStateMachine.Lease reserveClassification(long bytes) throws IOException {
        if(bytes<0)throw new IllegalArgumentException("Negative classification scratch");
        // Scratch shares the actual decoder byte ceiling but has its own finite task count.
        return acquire(new Usage(0,0,0,0,0,0,0,0,bytes,0,0,0,0,0,0,1),false);
    }
    @Override public LogResourceStateMachine.Lease reserveDecode(long maximumSerializedBytes) throws IOException {
        if(maximumSerializedBytes<0)throw new IllegalArgumentException("Negative decode bound"); long bytes=Math.addExact(Math.multiplyExact(maximumSerializedBytes,2),65536);
        return acquire(new Usage(0,0,0,0,0,0,0,0,bytes,1,0,0,0,0,0,0),true);
    }
    @Override public LogResourceStateMachine.Lease reserveIntake(long serializedBytes,boolean protectedHeartbeat) throws IOException {
        if(serializedBytes<0 || protectedHeartbeat && serializedBytes>16*1024)throw unavailable("protected progress is not a small heartbeat"); return acquire(intake(serializedBytes,protectedHeartbeat),false);
    }
    @Override public LogResourceStateMachine.Lease reserveControlIntake(long bytes) throws IOException {
        if(bytes<0)throw new IllegalArgumentException("Negative control intake");
        return acquire(new Usage(0,0,0,0,0,0,0,0,0,0,0,0,0,Math.addExact(Math.multiplyExact(bytes,2),4096),1,0),false);
    }
    @Override public LogResourceStateMachine.Lease reserveReply(boolean protectedHeartbeat) throws IOException { return acquire(protectedHeartbeat ? new Usage(0,0,0,0,0,0,0,0,0,0,0,0,1,0,0,0) : new Usage(0,0,1,0,0,0,0,0,0,0,0,0,0,0,0,0),false); }
    @Override public LogResourceStateMachine.Lease reserveResident(long bytes,long headers,long segments) throws IOException {
        if(bytes<0 || headers<0 || segments<0 || segments>Integer.MAX_VALUE)throw new IllegalArgumentException("Invalid resident footprint"); return acquire(new Usage(0,0,0,bytes,headers,(int)segments,0,0,0,0,0,0,0,0,0,0),false);
    }
    @Override public LogResourceStateMachine.Lease reserveRead(long bytes) throws IOException { if(bytes<0)throw new IllegalArgumentException("Negative read bound"); return acquire(new Usage(0,0,0,0,0,0,bytes,1,0,0,0,0,0,0,0,0),false); }
    @Override public LogResourceStateMachine.ResidentReservation reserveInventory(long bytes,long headers,long segments) throws IOException { reserveResident(bytes,headers,segments); return new Residency(bytes,headers,segments); }
    @Override public LogResourceStateMachine.AppendReservation reserveAppend(LogResourceStateMachine.AppendFootprint footprint) throws IOException {
        var intakeLease=footprint.protectedProgress() && footprint.headers()>0 ? reserveControlIntake(footprint.serializedIntakeBytes()) : reserveIntake(footprint.serializedIntakeBytes(),footprint.protectedProgress());
        final LogResourceStateMachine.Lease resident;
        try { resident=reserveResident(footprint.residentBytes(),footprint.headers(),footprint.segments()); } catch(IOException|RuntimeException failure) { intakeLease.close(); throw failure; }
        final DiskLease diskLease;
        try { diskLease=disk.reserve(footprint.diskUpperBound(),footprint.protectedProgress()); } catch(IOException|RuntimeException failure) { resident.close(); intakeLease.close(); throw failure; }
        return new Reservation(footprint,intakeLease,diskLease);
    }
    private final class Residency implements LogResourceStateMachine.ResidentReservation {
        private final AtomicReference<Usage> remaining; private final AtomicBoolean closed=new AtomicBoolean();
        Residency(long bytes,long headers,long segments) { remaining=new AtomicReference<>(new Usage(0,0,0,bytes,headers,Math.toIntExact(segments),0,0,0,0,0,0,0,0,0,0)); }
        private Usage take(long bytes,long headers,long segments) {
            if(bytes<0 || headers<0 || segments<0 || segments>Integer.MAX_VALUE)throw new IllegalArgumentException("Invalid resident transfer"); var delta=new Usage(0,0,0,bytes,headers,(int)segments,0,0,0,0,0,0,0,0,0,0);
            for(;;) { var before=remaining.get(); if(before.residentBytes<bytes || before.headers<headers || before.segments<segments || closed.get())throw new IllegalStateException("Resident reservation exhausted/closed"); if(remaining.compareAndSet(before,before.plus(delta,-1)))return delta; }
        }
        @Override public LogResourceStateMachine.ResidentReservation split(long bytes,long headers,long segments) { take(bytes,headers,segments); return new Residency(bytes,headers,segments); }
        @Override public LogResourceStateMachine.Lease transferResident(long bytes,long headers,long segments) {
            var delta=take(bytes,headers,segments);
            return new LogResourceStateMachine.Lease() { final AtomicBoolean released=new AtomicBoolean(); public void close() { if(released.compareAndSet(false,true)) { node.release(delta); pool.release(delta); local.release(delta); } } };
        }
        @Override public void close() { if(closed.compareAndSet(false,true)) { var delta=remaining.getAndSet(Usage.ZERO); node.release(delta); pool.release(delta); local.release(delta); } }
    }
    private final class Reservation implements LogResourceStateMachine.AppendReservation {
        private final Residency residency; private final AtomicBoolean finished=new AtomicBoolean();
        private final LogResourceStateMachine.Lease intake; private final DiskLease diskOwner;
        Reservation(LogResourceStateMachine.AppendFootprint f,LogResourceStateMachine.Lease intake,DiskLease disk) { residency=new Residency(f.residentBytes(),f.headers(),f.segments()); this.intake=intake; diskOwner=disk; }
        @Override public LogResourceStateMachine.Lease transferResident(long bytes,long headers,long segments) { return residency.transferResident(bytes,headers,segments); }
        @Override public LogResourceStateMachine.ResidentReservation split(long bytes,long headers,long segments) { return residency.split(bytes,headers,segments); }
        @Override public void actualFinished() { if(finished.compareAndSet(false,true)) { intake.close(); diskOwner.actualFinished(); } }
        @Override public void close() { if(!finished.get())throw new IllegalStateException("Append work has not actually returned"); residency.close(); }
    }
    @Override public LogResourceStateMachine.AppendReservation reserveMetadata(LogResourceStateMachine.AppendFootprint footprint,LogResourceStateMachine.ResidentReservation allowance) throws IOException {
        if(!footprint.protectedProgress())throw new IllegalArgumentException("Committed metadata requires protected progress");
        // This is a local scalar metadata row, already prepaid by admitted data; no new user intake owner.
        final DiskLease diskOwner=disk.reserve(footprint.diskUpperBound(),true);
        return new LogResourceStateMachine.AppendReservation() {
            final AtomicBoolean finished=new AtomicBoolean();
            public LogResourceStateMachine.Lease transferResident(long bytes,long headers,long segments) { return allowance.transferResident(bytes,headers,segments); }
            public LogResourceStateMachine.ResidentReservation split(long bytes,long headers,long segments) { return allowance.split(bytes,headers,segments); }
            public void actualFinished() { if(finished.compareAndSet(false,true))diskOwner.actualFinished(); }
            public void close() { if(!finished.get())throw new IllegalStateException("Metadata writer has not returned"); allowance.close(); }
        };
    }
    @Override public LogResourceStateMachine.DiskReservation reserveDiskGrowth(long bytes,boolean protectedProgress) throws IOException { var reservation=disk.reserve(bytes,protectedProgress); return reservation::actualFinished; }
    @Override public long metadataReservationsLimit() { return Math.min(local.limits.headerEntries,Math.min(local.limits.residentBytes/512,local.limits.segments/2)); }
    public void stopAdmissions() { stopped.set(true); }
    @Override public void close() { stopAdmissions(); if(!local.snapshot().equals(Usage.ZERO))throw new IllegalStateException("Raft log owners remain retained"); }
}
