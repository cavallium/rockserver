package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;
import org.rocksdb.*;

/** Apply-lane-owned event retention. No WAL tailing or Raft-log pinning. */
final class NativeRangeCdc {
    static final int MAX_RECORDS=4096;
    static final long MAX_RETAINED_BYTES=64L*1024*1024;
    private final RocksDB db;
    private final ColumnFamilyHandle state;
    private final byte[] prefix,summaryKey;
    private final DistributedCdcCheckpoint.Lineage lineage;
    private record Entry(long term,int bytes) {}
    private TreeMap<Long,Entry> sizes=new TreeMap<>();
    private long retainedBytes;
    private boolean initialized;
    private DistributedCdcCheckpoint.Cursor floor=DistributedCdcCheckpoint.Cursor.beginning();

    /** Bounded image metadata validation; no retained event payloads or native handles. */
    static final class ImageState {
        DistributedCdcCheckpoint.Lineage lineage;
        private int count; private long bytes,firstIndex=-1,lastIndex=-1,lastTerm;
        private byte[] summary;
        void accept(byte[] key,byte[] value,RangeOwnership.Owner owner,ApplyPosition cut) {
            if(key.length<=16 || key[16]!=0x50 && key[16]!=0x51) return;
            if(key[16]==0x50) {
                if(key.length!=25 || count>=MAX_RECORDS || value.length>DistributedCdcRecord.MAX_BYTES) throw new IllegalArgumentException("Invalid CDC image record shape/capacity");
                var record=DistributedCdcRecord.decode(value); var actual=record.lineage();
                if(!actual.groupId().equals(owner.groupId()) || actual.rangeEpoch()!=owner.epoch() || lineage!=null && !lineage.equals(actual)) throw new IllegalArgumentException("CDC image lineage changed");
                lineage=actual; long index=record.position().index();
                if(ByteBuffer.wrap(key,17,8).getLong()!=index || lastIndex>=index || lastTerm>record.position().term()) throw new IllegalArgumentException("CDC image order/entry identity changed");
                requireCursorCut(record.end(),cut); if(count==0) firstIndex=index; lastIndex=index; lastTerm=record.position().term(); count++;
                bytes+=25L+value.length; if(bytes>MAX_RETAINED_BYTES) throw new IllegalArgumentException("CDC image byte capacity exceeded");
            } else {
                if(key.length!=17 || summary!=null || value.length!=72) throw new IllegalArgumentException("Invalid CDC image retention summary"); summary=value.clone();
            }
        }
        void finish(RangeOwnership.Owner owner,ApplyPosition cut) {
            if(summary==null) { if(count!=0) throw new IllegalArgumentException("CDC image records lack retention summary"); return; }
            var input=ByteBuffer.wrap(summary); var actual=new DistributedCdcCheckpoint.Lineage(uuid(input),uuid(input),input.getLong());
            var floor=new DistributedCdcCheckpoint.Cursor(new ApplyPosition(input.getLong(),input.getLong()),input.getInt());
            if(!actual.groupId().equals(owner.groupId()) || actual.rangeEpoch()!=owner.epoch() || lineage!=null && !lineage.equals(actual)
                    || input.getInt()!=count || input.getLong()!=bytes || !floor.equals(DistributedCdcCheckpoint.Cursor.beginning()) && floor.mutationOrdinal()!=DistributedCommandCodec.MAX_MUTATIONS)
                throw new IllegalArgumentException("CDC image summary differs from retained rows");
            requireCursorCut(floor,cut);
            if(count>0 && floor.position().term()!=0 && floor.position().index()>=firstIndex) throw new IllegalArgumentException("CDC image rows precede eviction floor"); lineage=actual;
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
        summaryKey=prefix.clone(); summaryKey[16]=0x51;
    }
    private byte[] key(long index) { return ByteBuffer.allocate(25).put(prefix).putLong(index).array(); }
    void recover(ApplyPosition cut) throws RocksDBException {
        var recovered=new TreeMap<Long,Entry>(); long bytes=0; var validation=new ImageState(); var owner=new RangeOwnership.Owner(lineage.groupId(),lineage.rangeEpoch());
        try(var low=new Slice(prefix);var high=new Slice(summaryKey);var options=new ReadOptions().setFillCache(false).setIterateLowerBound(low).setIterateUpperBound(high);var iterator=db.newIterator(state,options)) {
            iterator.seekToFirst(); var sizing=new byte[0];
            while(iterator.isValid()) {
                if(recovered.size()>=MAX_RECORDS || iterator.key(sizing)!=25 || iterator.value(sizing)>DistributedCdcRecord.MAX_BYTES) throw new IllegalStateException("CDC recovery capacity/shape exceeded");
                byte[] encoded=iterator.value(); var record=DistributedCdcRecord.decode(encoded); requireRecord(record,cut);
                validation.accept(iterator.key(),encoded,owner,cut);
                if(!Arrays.equals(iterator.key(),key(record.position().index())) || recovered.put(record.position().index(),new Entry(record.position().term(),encoded.length))!=null) throw new IllegalStateException("CDC record key changed");
                bytes+=25L+encoded.length; if(bytes>MAX_RETAINED_BYTES) throw new IllegalStateException("CDC recovery byte capacity exceeded"); iterator.next();
            }
            iterator.status();
        }
        byte[] summary=db.get(state,summaryKey); var recoveredFloor=DistributedCdcCheckpoint.Cursor.beginning();
        if(summary!=null) validation.accept(summaryKey,summary,owner,cut);
        validation.finish(owner,cut);
        if(summary==null) {
            if(!recovered.isEmpty()) throw new IllegalStateException("CDC records lack retention summary");
            if(cut!=null) recoveredFloor=DistributedCdcCheckpoint.Cursor.after(cut);
        }
        else {
            if(summary.length!=72) throw new IllegalStateException("Invalid CDC retention summary");
            var input=ByteBuffer.wrap(summary); var stored=new DistributedCdcCheckpoint.Lineage(uuid(input),uuid(input),input.getLong());
            recoveredFloor=new DistributedCdcCheckpoint.Cursor(new ApplyPosition(input.getLong(),input.getLong()),input.getInt());
            if(!lineage.equals(stored) || input.getInt()!=recovered.size() || input.getLong()!=bytes) throw new IllegalStateException("CDC retention summary differs from rows");
            requireCursorCut(recoveredFloor,cut);
            if(!recovered.isEmpty() && recoveredFloor.position().term()!=0 && recovered.firstKey()<=recoveredFloor.position().index()) throw new IllegalStateException("CDC retained rows precede eviction floor");
        }
        sizes=recovered; retainedBytes=bytes; floor=recoveredFloor; initialized=summary!=null;
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
        if(!lineage.equals(record.lineage()) || sizes.containsKey(record.position().index()) || !sizes.isEmpty() && sizes.lastKey()>=record.position().index()) throw new IllegalArgumentException("CDC append is not new ordered progress");
        byte[] encoded=record.encode(); long bytes=retainedBytes+25L+encoded.length; var nextFloor=floor;
        var evictions=new ArrayList<Long>(); var candidates=sizes.entrySet().iterator();
        while(sizes.size()-evictions.size()>=MAX_RECORDS || bytes>MAX_RETAINED_BYTES) {
            if(!candidates.hasNext()) throw new IllegalArgumentException("CDC record exceeds retention capacity");
            var first=candidates.next(); evictions.add(first.getKey());
            bytes-=25L+first.getValue().bytes(); nextFloor=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(first.getValue().term(),first.getKey())); batch.delete(state,key(first.getKey()));
        }
        batch.put(state,key(record.position().index()),encoded);
        batch.put(state,summaryKey,summary(nextFloor,sizes.size()-evictions.size()+1,bytes)); long publishedBytes=bytes; var publishedFloor=nextFloor;
        var metadata=new Entry(record.position().term(),encoded.length);
        return ()->{ evictions.forEach(sizes::remove); sizes.put(record.position().index(),metadata); retainedBytes=publishedBytes; floor=publishedFloor; initialized=true; };
    }
    private byte[] summary(DistributedCdcCheckpoint.Cursor floor,int count,long bytes) {
        var summary=ByteBuffer.allocate(72); uuid(summary,lineage.rangeId()); uuid(summary,lineage.groupId()); summary.putLong(lineage.rangeEpoch());
        return summary.putLong(floor.position().term()).putLong(floor.position().index()).putInt(floor.mutationOrdinal()).putInt(count).putLong(bytes).array();
    }
    Runnable initialize(WriteBatch batch) throws RocksDBException {
        if(initialized) return ()->{};
        batch.put(state,summaryKey,summary(floor,0,0)); return ()->initialized=true;
    }
    Runnable importedBaseline(WriteBatch batch,ApplyPosition cut) throws RocksDBException {
        if(!sizes.isEmpty()) throw new IllegalStateException("Imported range already has CDC events");
        var nextFloor=DistributedCdcCheckpoint.Cursor.after(cut);
        batch.put(state,summaryKey,summary(nextFloor,0,0)); return ()->{ floor=nextFloor; initialized=true; };
    }
    ReplicatedRangeStore.CdcPage poll(DistributedCdcCheckpoint.Cursor from,int maxRecords,int maxBytes,ApplyPosition cut) throws RocksDBException {
        requireCursorCut(from,cut);
        if(!from.covers(floor)) throw new ReplicatedRangeStore.CdcGap(new ReplicatedRangeStore.CdcPage(lineage,cut,floor,from,List.of()));
        var records=new ArrayList<DistributedCdcRecord>(); int bytes=0; var next=from;
        for(var entry:sizes.tailMap(from.position().index(),true).entrySet()) {
            long index=entry.getKey(); var metadata=entry.getValue(); var end=DistributedCdcCheckpoint.Cursor.after(new ApplyPosition(metadata.term(),index));
            if(from.covers(end)) continue;
            int charge=4+metadata.bytes();
            if(charge>maxBytes-bytes) {
                if(records.isEmpty()) throw new ReplicatedRangeStore.CdcTooLarge(new ReplicatedRangeStore.CdcPage(lineage,cut,floor,from,List.of()),charge);
                break;
            }
            var record=DistributedCdcRecord.decode(db.get(state,key(index))); requireRecord(record,cut);
            if(record.encodedSize()!=metadata.bytes() || !record.end().equals(end)) throw new IllegalStateException("CDC record differs from retained metadata");
            records.add(record); bytes+=charge; next=record.end(); if(records.size()==maxRecords) break;
        }
        if(records.size()<maxRecords && (sizes.isEmpty() || next.position().index()>=sizes.lastKey()) && cut!=null) next=DistributedCdcCheckpoint.Cursor.after(cut);
        return new ReplicatedRangeStore.CdcPage(lineage,cut,floor,next,records);
    }
    DistributedCdcCheckpoint.Lineage lineage() { return lineage; }
    private static UUID uuid(ByteBuffer bytes) { return new UUID(bytes.getLong(),bytes.getLong()); }
    private static void uuid(ByteBuffer bytes,UUID id) { bytes.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
}
