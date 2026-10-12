package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.rocksdb.*;

/** Apply-lane-owned event retention. No WAL tailing or Raft-log pinning. */
final class NativeRangeCdc {
    static final int MAX_RECORDS=4096;
    static final long MAX_RETAINED_BYTES=64L*1024*1024;
    static final long MAX_AGE_MILLIS=24L*60*60*1000;
    private static final long ROW_OVERHEAD=58;
    private final RocksDB db;
    private final ColumnFamilyHandle state;
    private final byte[] prefix,summaryKey,agePrefix;
    private final DistributedCdcCheckpoint.Lineage lineage;
    private record Entry(long term,int bytes,int mutations,long age) {}
    private TreeMap<Long,Entry> sizes=new TreeMap<>();
    private long retainedBytes,lastClock=-1;
    private boolean initialized,terminalCommitted;
    private ApplyPosition terminalCut;
    private DistributedCdcCheckpoint.Cursor floor=DistributedCdcCheckpoint.Cursor.beginning();

    /** Bounded image metadata validation; only event metadata and matching age cells survive this scan. */
    static final class ImageState {
        DistributedCdcCheckpoint.Lineage lineage;
        private final TreeMap<Long,Entry> records=new TreeMap<>();
        private final TreeMap<Long,Long> ages=new TreeMap<>();
        private long bytes,lastTerm;
        private byte[] summary;
        private DistributedCdcCheckpoint.Cursor floor=DistributedCdcCheckpoint.Cursor.beginning();
        private long clock=-1;
        private NativeTopologyFreeze freeze,terminal;
        void accept(byte[] key,byte[] value,RangeOwnership.Owner owner,ApplyPosition cut) {
            if(key.length==17 && key[16]==0x60) { if(freeze!=null)throw new IllegalArgumentException("Duplicate CDC native terminal proof");freeze=NativeTopologyFreeze.decode(value);freeze.requireCut(cut);return; }
            if(key.length<=16 || key[16]!=0x50 && key[16]!=0x51 && key[16]!=0x52) return;
            if(key[16]==0x50) {
                if(key.length!=25 || records.size()>=MAX_RECORDS || value.length>DistributedCdcRecord.MAX_BYTES) throw new IllegalArgumentException("Invalid CDC image record shape/capacity");
                var record=DistributedCdcRecord.decode(value); var actual=record.lineage();
                if(terminal!=null)throw new IllegalArgumentException("CDC events follow terminal barrier");
                if(record.barrier()!=null)terminal=record.barrier();
                if(!actual.groupId().equals(owner.groupId()) || actual.rangeEpoch()!=owner.epoch() || lineage!=null && !lineage.equals(actual)) throw new IllegalArgumentException("CDC image lineage changed");
                lineage=actual; long index=record.position().index();
                if(ByteBuffer.wrap(key,17,8).getLong()!=index || !records.isEmpty() && records.lastKey()>=index || lastTerm>record.position().term()) throw new IllegalArgumentException("CDC image order/entry identity changed");
                requireCursorCut(record.end(),cut); lastTerm=record.position().term();
                records.put(index,new Entry(record.position().term(),value.length,record.mutationCount(),-1));
                bytes+=ROW_OVERHEAD+value.length; if(bytes>MAX_RETAINED_BYTES) throw new IllegalArgumentException("CDC image byte capacity exceeded");
            } else if(key[16]==0x51) {
                if(key.length!=17 || summary!=null || value.length!=80) throw new IllegalArgumentException("Invalid CDC image retention summary"); summary=value.clone();
            } else {
                if(key.length!=25 || value.length!=8 || ages.size()>=MAX_RECORDS) throw new IllegalArgumentException("Invalid CDC age cell shape/capacity");
                long index=ByteBuffer.wrap(key,17,8).getLong(),age=ByteBuffer.wrap(value).getLong();
                if(index<0 || age < -1 || !ages.isEmpty() && ages.lastKey()>=index || ages.put(index,age)!=null) throw new IllegalArgumentException("Invalid CDC age cell order/identity");
            }
        }
        void finish(RangeOwnership.Owner owner,ApplyPosition cut) {
            if(terminal!=null && !terminal.equals(freeze))throw new IllegalArgumentException("CDC barrier differs from committed native FREEZE");
            if(summary==null) { if(!records.isEmpty() || !ages.isEmpty()) throw new IllegalArgumentException("CDC image rows lack retention summary"); return; }
            var input=ByteBuffer.wrap(summary); var actual=new DistributedCdcCheckpoint.Lineage(uuid(input),uuid(input),input.getLong());
            floor=new DistributedCdcCheckpoint.Cursor(new ApplyPosition(input.getLong(),input.getLong()),input.getInt());
            int count=input.getInt(); long declaredBytes=input.getLong(); clock=input.getLong();
            if(!actual.groupId().equals(owner.groupId()) || actual.rangeEpoch()!=owner.epoch() || lineage!=null && !lineage.equals(actual)
                    || count!=records.size() || declaredBytes!=bytes || clock < -1 || !floor.equals(DistributedCdcCheckpoint.Cursor.beginning()) && floor.mutationOrdinal()!=DistributedCommandCodec.MAX_MUTATIONS
                    || !records.keySet().equals(ages.keySet())) throw new IllegalArgumentException("CDC image summary/age cells differ from retained rows");
            requireCursorCut(floor,cut);
            if(freeze!=null) {
                if(!records.isEmpty() && records.lastKey()>freeze.position().index())throw new IllegalArgumentException("CDC retained event follows original FREEZE");
                if(!floor.covers(DistributedCdcCheckpoint.Cursor.after(freeze.position())) && terminal==null)throw new IllegalArgumentException("CDC retained history lacks committed terminal barrier");
            }
            if(!records.isEmpty() && floor.position().term()!=0 && floor.position().index()>=records.firstKey()) throw new IllegalArgumentException("CDC image rows precede eviction floor");
            long previous=-1; boolean pending=false;
            for(var entry:ages.entrySet()) {
                long age=entry.getValue();
                if(age==-1) pending=true;
                else {
                    if(pending || age<previous || age>clock || clock-age>=MAX_AGE_MILLIS) throw new IllegalArgumentException("CDC ages differ from committed clock/order");
                    previous=age;
                }
                var record=records.get(entry.getKey()); records.put(entry.getKey(),new Entry(record.term(),record.bytes(),record.mutations(),age));
            }
            lineage=actual;
        }
        void requireAuthorityClock(long authorityClock) {
            if(authorityClock < -1 || summary!=null && clock!=authorityClock) throw new IllegalArgumentException("CDC retention clock differs from persisted authority");
        }
    }
    void validateImage(java.nio.file.Path image,NativeRangeImage.Limits limits,ApplyPosition cut) throws java.io.IOException {
        try {
            var validation=new ImageState(); var result=NativeRangeImage.visit(image,limits,(section,column,key,value)->{
                if(section==NativeRangeImage.Section.STATE) validation.accept(key,value,new RangeOwnership.Owner(lineage.groupId(),lineage.rangeEpoch()),cut);
            });
            validation.finish(result.owner(),result.position());
            if(validation.lineage!=null && !lineage.equals(validation.lineage)) throw new java.io.IOException("Snapshot CDC lineage differs from configured range");
        } catch(IllegalArgumentException | IllegalStateException invalid) { throw new java.io.IOException("Invalid snapshot CDC lineage",invalid); }
    }

    NativeRangeCdc(RocksDB db,ColumnFamilyHandle state,DistributedCdcCheckpoint.Lineage lineage) {
        this.db=db; this.state=state; this.lineage=lineage;
        var group=lineage.groupId();
        prefix=ByteBuffer.allocate(17).putLong(group.getMostSignificantBits()).putLong(group.getLeastSignificantBits()).put((byte)0x50).array();
        summaryKey=prefix.clone(); summaryKey[16]=0x51; agePrefix=prefix.clone(); agePrefix[16]=0x52;
    }
    private byte[] key(long index) { return ByteBuffer.allocate(25).put(prefix).putLong(index).array(); }
    private byte[] ageKey(long index) { return ByteBuffer.allocate(25).put(agePrefix).putLong(index).array(); }
    void recover(ApplyPosition cut,long baselineClock) throws RocksDBException {
        if(baselineClock < -1) throw new IllegalArgumentException("Invalid CDC baseline clock");
        var validation=new ImageState(); var owner=new RangeOwnership.Owner(lineage.groupId(),lineage.rangeEpoch());
        byte[] upper=prefix.clone(); upper[16]=0x53;
        try(var low=new Slice(prefix);var high=new Slice(upper);var options=new ReadOptions().setFillCache(false).setIterateLowerBound(low).setIterateUpperBound(high);var iterator=db.newIterator(state,options)) {
            iterator.seekToFirst(); var sizing=new byte[0];
            while(iterator.isValid()) {
                int keyBytes=iterator.key(sizing),valueBytes=iterator.value(sizing);
                if(keyBytes!=17 && keyBytes!=25 || valueBytes>DistributedCdcRecord.MAX_BYTES) throw new IllegalStateException("CDC recovery capacity/shape exceeded");
                validation.accept(iterator.key(),iterator.value(),owner,cut); iterator.next();
            }
            iterator.status();
        }
        byte[] freezeKey=summaryKey.clone();freezeKey[16]=0x60;byte[] freeze=db.get(state,freezeKey);if(freeze!=null)validation.accept(freezeKey,freeze,owner,cut);
        validation.finish(owner,cut); validation.requireAuthorityClock(baselineClock);
        if(validation.lineage!=null && !lineage.equals(validation.lineage)) throw new IllegalStateException("CDC recovered lineage differs");
        sizes=validation.records; retainedBytes=validation.bytes; lastClock=validation.summary==null ? baselineClock : validation.clock;
        floor=validation.summary==null && cut!=null ? DistributedCdcCheckpoint.Cursor.after(cut) : validation.floor;
        initialized=validation.summary!=null;terminalCommitted=validation.freeze!=null;terminalCut=validation.freeze==null ? null : validation.freeze.position();
    }
    private void requireRecord(DistributedCdcRecord record,ApplyPosition cut) {
        if(!record.lineage().equals(lineage)) throw new IllegalStateException("CDC record lineage differs"); requireCursorCut(record.end(),cut);
    }
    private static void requireCursorCut(DistributedCdcCheckpoint.Cursor cursor,ApplyPosition cut) {
        if(cursor.equals(DistributedCdcCheckpoint.Cursor.beginning())) return;
        if(cut==null || cursor.position().index()>cut.index() || cursor.position().term()>cut.term()
                || cursor.position().index()==cut.index() && cursor.position().term()!=cut.term()) throw new IllegalStateException("CDC cursor exceeds completed native cut");
    }
    /** Prepare bounded evictions in the same native batch; publish bookkeeping only after db.write returns. */
    Runnable append(WriteBatch batch,DistributedCdcRecord record) throws RocksDBException {
        if(terminalCommitted || !lineage.equals(record.lineage()) || sizes.containsKey(record.position().index()) || !sizes.isEmpty() && sizes.lastKey()>=record.position().index()) throw new IllegalArgumentException("CDC append is not new ordered progress");
        byte[] encoded=record.encode(); long bytes=retainedBytes+ROW_OVERHEAD+encoded.length; var nextFloor=floor;
        var evictions=new ArrayList<Long>(); var candidates=sizes.entrySet().iterator();
        while(sizes.size()-evictions.size()>=MAX_RECORDS || bytes>MAX_RETAINED_BYTES) {
            if(!candidates.hasNext()) throw new IllegalArgumentException("CDC record exceeds retention capacity");
            var first=candidates.next(); evictions.add(first.getKey());
            bytes-=ROW_OVERHEAD+first.getValue().bytes(); nextFloor=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(first.getValue().term(),first.getKey())); batch.delete(state,key(first.getKey())); batch.delete(state,ageKey(first.getKey()));
        }
        batch.put(state,key(record.position().index()),encoded); batch.put(state,ageKey(record.position().index()),ByteBuffer.allocate(8).putLong(-1).array());
        batch.put(state,summaryKey,summary(nextFloor,sizes.size()-evictions.size()+1,bytes,lastClock)); long publishedBytes=bytes; var publishedFloor=nextFloor;
        var metadata=new Entry(record.position().term(),encoded.length,record.mutationCount(),-1);
        return ()->{ evictions.forEach(sizes::remove); sizes.put(record.position().index(),metadata); retainedBytes=publishedBytes; floor=publishedFloor; initialized=true;terminalCommitted=record.barrier()!=null;terminalCut=record.barrier()==null ? null : record.barrier().position(); };
    }
    /** Evict cached anchored metadata, then anchor pending cohorts only at a strictly newer committed tick. */
    Runnable advanceClock(WriteBatch batch,long logicalTimeMillis) throws RocksDBException {
        if(logicalTimeMillis<0) throw new IllegalArgumentException("Negative CDC logical clock");
        if(logicalTimeMillis<=lastClock) return ()->{};
        var first=sizes.firstEntry(); var last=sizes.lastEntry();
        // Anchors are ordered and pending rows form a suffix; unchanged ticks need no map copy.
        if(last==null || last.getValue().age()!=-1 && logicalTimeMillis-first.getValue().age()<MAX_AGE_MILLIS) {
            batch.put(state,summaryKey,summary(floor,sizes.size(),retainedBytes,logicalTimeMillis));
            return ()->{ lastClock=logicalTimeMillis; initialized=true; };
        }
        var next=new TreeMap<Long,Entry>(); var nextFloor=floor; long bytes=retainedBytes;
        for(var row:sizes.entrySet()) {
            long index=row.getKey(); var entry=row.getValue();
            if(entry.age()>=0 && logicalTimeMillis-entry.age()>=MAX_AGE_MILLIS) {
                batch.delete(state,key(index)); batch.delete(state,ageKey(index)); bytes-=ROW_OVERHEAD+entry.bytes();
                nextFloor=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(entry.term(),index));
            } else {
                if(entry.age()==-1) { entry=new Entry(entry.term(),entry.bytes(),entry.mutations(),logicalTimeMillis); batch.put(state,ageKey(index),ByteBuffer.allocate(8).putLong(logicalTimeMillis).array()); }
                next.put(index,entry);
            }
        }
        batch.put(state,summaryKey,summary(nextFloor,next.size(),bytes,logicalTimeMillis)); long publishedBytes=bytes; var publishedFloor=nextFloor;
        return ()->{ sizes=next; retainedBytes=publishedBytes; floor=publishedFloor; lastClock=logicalTimeMillis; initialized=true; };
    }
    private byte[] summary(DistributedCdcCheckpoint.Cursor floor,int count,long bytes,long clock) {
        var summary=ByteBuffer.allocate(80); uuid(summary,lineage.rangeId()); uuid(summary,lineage.groupId()); summary.putLong(lineage.rangeEpoch());
        return summary.putLong(floor.position().term()).putLong(floor.position().index()).putInt(floor.mutationOrdinal()).putInt(count).putLong(bytes).putLong(clock).array();
    }
    Runnable initialize(WriteBatch batch) throws RocksDBException {
        if(initialized) return ()->{};
        batch.put(state,summaryKey,summary(floor,0,0,lastClock)); return ()->initialized=true;
    }
    Runnable importedBaseline(WriteBatch batch,ApplyPosition cut,long baselineClock) throws RocksDBException {
        if(baselineClock < -1) throw new IllegalArgumentException("Invalid imported CDC baseline clock");
        if(!sizes.isEmpty()) throw new IllegalStateException("Imported range already has CDC events");
        var nextFloor=DistributedCdcCheckpoint.Cursor.after(cut);
        batch.put(state,summaryKey,summary(nextFloor,0,0,baselineClock)); return ()->{ floor=nextFloor; lastClock=baselineClock; initialized=true; };
    }
    ReplicatedRangeStore.CdcPage poll(DistributedCdcCheckpoint.Cursor from,int maxRecords,int maxBytes,ApplyPosition cut) throws RocksDBException {
        requireCursorCut(from,cut);
        if(terminalCut!=null && (from.position().index()>terminalCut.index() || from.position().index()==terminalCut.index() && from.position().term()!=terminalCut.term()))throw new IllegalArgumentException("Sealed CDC cursor exceeds original terminal boundary");
        if(!from.covers(floor)) throw new ReplicatedRangeStore.CdcGap(new ReplicatedRangeStore.CdcPage(lineage,cut,floor,from,false,List.of()));
        var boundary=sizes.get(from.position().index());
        if(boundary!=null && boundary.mutations()==0 && (boundary.term()!=from.position().term() || from.mutationOrdinal()!=DistributedCommandCodec.MAX_MUTATIONS))throw new IllegalArgumentException("Terminal CDC barrier requires its exact EOF cursor");
        boolean verified=from.equals(floor) || from.equals(DistributedCdcCheckpoint.Cursor.beginning()) || cut!=null && from.equals(DistributedCdcCheckpoint.Cursor.after(cut))
                || boundary!=null && boundary.term()==from.position().term() && (from.mutationOrdinal()==DistributedCommandCodec.MAX_MUTATIONS || from.mutationOrdinal()<boundary.mutations());
        var records=new ArrayList<DistributedCdcRecord>(); int bytes=0; var next=from;
        for(var entry:sizes.tailMap(from.position().index(),true).entrySet()) {
            long index=entry.getKey(); var metadata=entry.getValue(); var end=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(metadata.term(),index));
            if(from.covers(end)) continue;
            int charge=4+metadata.bytes();
            if(charge>maxBytes-bytes) {
                if(records.isEmpty()) throw new ReplicatedRangeStore.CdcTooLarge(new ReplicatedRangeStore.CdcPage(lineage,cut,floor,from,verified,List.of()),charge);
                break;
            }
            var record=DistributedCdcRecord.decode(db.get(state,key(index))); requireRecord(record,cut);
            if(record.encodedSize()!=metadata.bytes() || !record.end().equals(end)) throw new IllegalStateException("CDC record differs from retained metadata");
            records.add(record); bytes+=charge; next=record.end(); if(records.size()==maxRecords) break;
        }
        // Only retained event boundaries can be qualified again after the completed cut advances.
        return new ReplicatedRangeStore.CdcPage(lineage,cut,floor,next,verified,records);
    }
    DistributedCdcCheckpoint.Lineage lineage() { return lineage; }
    private static UUID uuid(ByteBuffer bytes) { return new UUID(bytes.getLong(),bytes.getLong()); }
    private static void uuid(ByteBuffer bytes,UUID id) { bytes.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
}
