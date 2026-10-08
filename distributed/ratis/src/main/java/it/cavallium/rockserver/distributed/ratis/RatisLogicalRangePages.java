package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.distributed.*;
import java.io.IOException;
import java.util.*;

/** Logical pages over one owned cut. Physical rows stay complete; continuation includes collision-member keys. */
final class RatisLogicalRangePages {
    static final int MAX_SCANNED_ROWS=4096;
    static final long MAX_SCANNED_BYTES=16L*1024*1024;
    private RatisLogicalRangePages() {}
    static Keys copy(Keys keys) { return keys==null ? null : new Keys(Arrays.stream(keys.keys()).map(key -> Buf.wrap(key.toByteArray())).toArray(Buf[]::new)); }
    private static byte[] bound(DistributedSchema schema,UUID column,Keys keys) { return keys==null || keys.keys().length==0 ? null : schema.encodeLogicalKey(column,keys).physicalKey(); }
    static RangePage<KV> read(RatisRoutedClient.ScopedTransaction scoped,TransactionScope scope,DistributedSchema schema,
                             long column,Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget) throws IOException {
        Objects.requireNonNull(budget);
        if (budget.maxItems()>RangeBudget.DEFAULT_MAX_ITEMS || budget.maxBytes()>RangeBudget.DEFAULT_MAX_BYTES) throw new IllegalArgumentException("Logical page exceeds advertised item/byte limit");
        var bounds=bounds(scoped,scope,schema,column,lower,upper); var binding=bounds.binding(); UUID id=binding.columnId();
        byte[] low=bounds.lower(), high=bounds.upper();
        var continuation=resume==null ? null : schema.encodeLogicalKey(id,resume);
        byte[] physical=continuation==null ? null : continuation.physicalKey();
        if (physical!=null && (low!=null && Arrays.compareUnsigned(physical,low)<0 || high!=null && Arrays.compareUnsigned(physical,high)>=0)) throw new IllegalArgumentException("Logical continuation outside original bounds");
        var reader=new Reader(scoped,schema,binding,low,high,reverse,continuation);
        var items=new ArrayList<KV>(); long bytes=0;
        for (KV next;(next=reader.next())!=null;) {
            long size=next.value()==null ? 0 : next.value().size(); for (var key:next.keys().keys()) size+=key.size();
            if (items.size()==budget.maxItems() || size>budget.maxBytes()-bytes) {
                if (items.isEmpty()) throw RocksDBException.of(RocksDBException.RocksDBErrorType.RANGE_ITEM_TOO_LARGE,"Logical range item exceeds page byte budget");
                return new RangePage<>(items,items.getLast().keys(),true);
            }
            items.add(next); bytes+=size;
        }
        return items.isEmpty() ? RangePage.empty() : new RangePage<>(items,items.getLast().keys(),false);
    }
    private record Bounds(DistributedSchema.Column binding,byte[] lower,byte[] upper) {}
    private static Bounds bounds(RatisRoutedClient.ScopedTransaction scoped,TransactionScope scope,DistributedSchema schema,long column,Keys lower,Keys upper) {
        var binding=RatisLogicalClient.supported(schema,column); UUID id=binding.columnId();
        byte[] low=bound(schema,id,lower), high=bound(schema,id,upper);
        if (low==null && scoped.descriptor().lowerInclusive()!=null || high==null && scoped.descriptor().upperExclusive()!=null) throw new IllegalArgumentException("Unbounded logical page crosses its transaction participant");
        if (low!=null && high!=null && Arrays.compareUnsigned(low,high)>=0 || !scope.containsRange(id,low,high)) throw new IllegalArgumentException("Logical page outside declared scope/bounds");
        return new Bounds(binding,low,high);
    }
    static void validateBounds(RatisRoutedClient.ScopedTransaction scoped,TransactionScope scope,DistributedSchema schema,long column,Keys lower,Keys upper) {
        bounds(scoped,scope,schema,column,lower,upper);
    }
    private record Element(DistributedBucket.Member member,List<byte[]> tails) {}
    private static int compare(List<byte[]> a,List<byte[]> b) {
        for (int i=0;i<a.size();i++) { int order=Arrays.compareUnsigned(a.get(i),b.get(i)); if (order!=0) return order; } return 0;
    }
    private static final class Reader {
        final RatisRoutedClient.ScopedTransaction scoped; final DistributedSchema schema; final DistributedSchema.Column binding;
        final byte[] lower,upper; final boolean reverse,bucket;
        byte[] physicalResume,bucketKey; List<Element> members=List.of(); int memberIndex;
        Iterator<PhysicalRangePage.Row> rows=Collections.emptyIterator(); boolean more=true;
        int scanned; long scannedBytes;
        Reader(RatisRoutedClient.ScopedTransaction scoped,DistributedSchema schema,DistributedSchema.Column binding,byte[] lower,byte[] upper,boolean reverse,DistributedSchema.EncodedKey resume) throws IOException {
            this.scoped=scoped; this.schema=schema; this.binding=binding; this.lower=lower; this.upper=upper; this.reverse=reverse;
            bucket=binding.schema().variableLengthKeysCount()!=0; physicalResume=resume==null ? null : resume.physicalKey();
            if (bucket && resume!=null) {
                byte[] value=scoped.get(binding.columnId(),physicalResume); charge(1,physicalResume.length+(value==null ? 0L : value.length));
                loadBucket(physicalResume,value); var tails=resume.variableKeys();
                while (memberIndex<members.size() && (reverse ? compare(members.get(memberIndex).tails(),tails)>=0 : compare(members.get(memberIndex).tails(),tails)<=0)) memberIndex++;
            }
        }
        void charge(int rows,long bytes) throws IOException {
            scanned+=rows; scannedBytes+=bytes;
            if (scanned>MAX_SCANNED_ROWS || scannedBytes>MAX_SCANNED_BYTES) throw new IOException("Logical range scan quantum exhausted");
        }
        void loadBucket(byte[] physical,byte[] value) throws IOException {
            bucketKey=physical; var ordered=new ArrayList<Element>();
            try { for (var member:schema.decodeBucket(binding.columnId(),physical,value).members()) ordered.add(new Element(member,member.tails())); }
            catch (IllegalArgumentException invalid) { throw new IOException("Stored bucket cannot be served completely",invalid); }
            ordered.sort((a,b) -> reverse ? compare(b.tails(),a.tails()) : compare(a.tails(),b.tails())); members=ordered; memberIndex=0;
        }
        KV next() throws IOException {
            for (;;) {
                if (memberIndex<members.size()) {
                    var member=members.get(memberIndex++); var shape=binding.schema(); var keys=new Buf[shape.keysCount()]; int offset=0;
                    for (int i=0;i<shape.fixedLengthKeysCount();i++) { int width=shape.key(i); keys[i]=Buf.wrap(Arrays.copyOfRange(bucketKey,offset,offset+width)); offset+=width; }
                    for (int i=0;i<member.tails().size();i++) keys[shape.fixedLengthKeysCount()+i]=Buf.wrap(member.tails().get(i).clone());
                    return new KV(new Keys(keys),Buf.wrap(member.member().value()));
                }
                if (!rows.hasNext()) {
                    if (!more) return null;
                    if (scanned==MAX_SCANNED_ROWS || scannedBytes>=MAX_SCANNED_BYTES) throw new IOException("Logical range scan quantum exhausted");
                    var page=scoped.page(binding.columnId(),lower,upper,reverse,physicalResume,Math.min(128,MAX_SCANNED_ROWS-scanned),
                            (int)Math.min(PhysicalRangePage.MAX_BYTES,MAX_SCANNED_BYTES-scannedBytes));
                    charge(page.rows().size(),page.payloadBytes()); rows=page.rows().iterator(); more=page.hasMore();
                    if (!rows.hasNext()) return null;
                }
                var row=rows.next(); physicalResume=row.key();
                if (bucket) { loadBucket(physicalResume,row.value()); continue; }
                try {
                    var keys=schema.decodeFixedKey(binding.columnId(),physicalResume); byte[] value=row.value();
                    if (!binding.schema().hasValue() && value.length!=0) throw new IOException("Nonempty key-only value");
                    return new KV(keys,Buf.wrap(value));
                } catch (IllegalArgumentException invalid) { throw new IOException("Stored fixed key differs from schema",invalid); }
            }
        }
    }
}
