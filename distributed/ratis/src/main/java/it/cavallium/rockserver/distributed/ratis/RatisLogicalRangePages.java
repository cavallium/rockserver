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
    interface PhysicalReader {
        byte[] get(UUID column,byte[] key) throws IOException;
        PhysicalRangePage page(UUID column,byte[] lower,byte[] upper,boolean reverse,byte[] resume,int items,int bytes) throws IOException;
    }
    private RatisLogicalRangePages() {}
    static Keys copy(Keys keys) { return keys==null ? null : new Keys(Arrays.stream(keys.keys()).map(key -> Buf.wrap(key.toByteArray())).toArray(Buf[]::new)); }
    static byte[] bound(DistributedSchema schema,UUID column,Keys keys) {
        if (keys==null || keys.keys().length==0) return null;
        var shape=schema.column(column).schema(); var parts=keys.keys();
        if (parts.length<shape.keysCount() && parts.length<=shape.fixedLengthKeysCount()) {
            long bytes=0;
            for (int i=0;i<parts.length;i++) { if (Objects.requireNonNull(parts[i]).size()!=shape.key(i)) throw new IllegalArgumentException("Logical fixed-prefix component width differs from schema"); bytes+=parts[i].size(); }
            if (bytes>DistributedCommandCodec.MAX_ENCODED_BYTES) throw new IllegalArgumentException("Logical fixed prefix exceeds byte budget");
            byte[] prefix=new byte[(int)bytes]; int offset=0;
            for (var part:parts) { byte[] value=part.toByteArray(); System.arraycopy(value,0,prefix,offset,value.length); offset+=value.length; }
            return prefix;
        }
        return schema.encodeLogicalKey(column,keys).physicalKey();
    }
    static byte[] upperBound(DistributedSchema schema,UUID column,Keys keys) {
        byte[] physical=bound(schema,column,keys); if (physical==null || schema.column(column).schema().variableLengthKeysCount()==0 || keys.keys().length<schema.column(column).schema().keysCount()) return physical;
        int end=physical.length-1; while (end>=0 && Byte.toUnsignedInt(physical[end])==255) end--;
        if (end<0) return null; byte[] next=Arrays.copyOf(physical,end+1); next[end]++; return next;
    }
    static RangePage<KV> read(RatisRoutedClient.ScopedTransaction scoped,TransactionScope scope,DistributedSchema schema,
                             long column,Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget) throws IOException {
        bounds(scoped,scope,schema,column,lower,upper);
        return read(new PhysicalReader() {
            public byte[] get(UUID id,byte[] key) throws IOException { return scoped.get(id,key); }
            public PhysicalRangePage page(UUID id,byte[] low,byte[] high,boolean back,byte[] cursor,int items,int bytes) throws IOException { return scoped.page(id,low,high,back,cursor,items,bytes); }
        },scope,schema,column,lower,upper,reverse,resume,budget);
    }
    static RangePage<KV> read(PhysicalReader physicalReader,TransactionScope scope,DistributedSchema schema,
                             long column,Keys lower,Keys upper,boolean reverse,Keys resume,RangeBudget budget) throws IOException {
        Objects.requireNonNull(budget);
        if (budget.maxItems()>RangeBudget.DEFAULT_MAX_ITEMS || budget.maxBytes()>RangeBudget.DEFAULT_MAX_BYTES) throw new IllegalArgumentException("Logical page exceeds advertised item/byte limit");
        var bounds=bounds(scope,schema,column,lower,upper); var binding=bounds.binding(); UUID id=binding.columnId();
        byte[] low=bounds.lower(), high=bounds.upper();
        var continuation=resume==null ? null : schema.encodeLogicalKey(id,resume);
        if (continuation!=null && (low!=null && Arrays.compareUnsigned(continuation.physicalKey(),low)<0 || high!=null && Arrays.compareUnsigned(continuation.physicalKey(),high)>=0
                || bounds.logicalLower()!=null && compare(continuation,bounds.logicalLower())<0 || bounds.logicalUpper()!=null && compare(continuation,bounds.logicalUpper())>=0)) throw new IllegalArgumentException("Logical continuation outside original bounds");
        var reader=new Reader(physicalReader,schema,binding,low,high,bounds.logicalLower(),bounds.logicalUpper(),reverse,continuation);
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
    private record Bounds(DistributedSchema.Column binding,byte[] lower,byte[] upper,DistributedSchema.EncodedKey logicalLower,DistributedSchema.EncodedKey logicalUpper) {}
    private static Bounds bounds(RatisRoutedClient.ScopedTransaction scoped,TransactionScope scope,DistributedSchema schema,long column,Keys lower,Keys upper) {
        var bounds=bounds(scope,schema,column,lower,upper);
        if (bounds.lower()==null && scoped.descriptor().lowerInclusive()!=null || bounds.upper()==null && scoped.descriptor().upperExclusive()!=null) throw new IllegalArgumentException("Unbounded logical page crosses its transaction participant");
        return bounds;
    }
    private static Bounds bounds(TransactionScope scope,DistributedSchema schema,long column,Keys lower,Keys upper) {
        var binding=RatisLogicalClient.supported(schema,column); UUID id=binding.columnId();
        var logicalLower=lower==null || lower.keys().length!=binding.schema().keysCount() ? null : schema.encodeLogicalKey(id,lower);
        var logicalUpper=upper==null || upper.keys().length!=binding.schema().keysCount() ? null : schema.encodeLogicalKey(id,upper);
        byte[] low=bound(schema,id,lower),high=upperBound(schema,id,upper);
        if (low!=null && high!=null && Arrays.compareUnsigned(low,high)>=0 || logicalLower!=null && logicalUpper!=null && compare(logicalLower,logicalUpper)>=0 || !scope.containsRange(id,low,high)) throw new IllegalArgumentException("Logical page outside declared scope/bounds");
        return new Bounds(binding,low,high,logicalLower,logicalUpper);
    }
    static void validateBounds(RatisRoutedClient.ScopedTransaction scoped,TransactionScope scope,DistributedSchema schema,long column,Keys lower,Keys upper) {
        bounds(scoped,scope,schema,column,lower,upper);
    }
    static void validateBounds(TransactionScope scope,DistributedSchema schema,long column,Keys lower,Keys upper) {
        bounds(scope,schema,column,lower,upper);
    }
    static void validateContainedBounds(DistributedSchema schema,long column,Keys originalLower,Keys originalUpper,Keys lower,Keys upper) {
        var binding=RatisLogicalClient.supported(schema,column); UUID id=binding.columnId();
        var scope=new TransactionScope(List.of(TransactionScope.Span.range(id,bound(schema,id,originalLower),upperBound(schema,id,originalUpper))));
        var original=bounds(scope,schema,column,originalLower,originalUpper);
        var requested=bounds(scope,schema,column,lower,upper);
        if (original.logicalLower()!=null && (requested.logicalLower()!=null
                    ? compare(requested.logicalLower(),original.logicalLower())<0
                    : requested.lower()==null || Arrays.compareUnsigned(requested.lower(),original.logicalLower().physicalKey())<=0)
                || original.logicalUpper()!=null && (requested.logicalUpper()!=null
                    ? compare(requested.logicalUpper(),original.logicalUpper())>0
                    : requested.upper()==null || Arrays.compareUnsigned(requested.upper(),original.logicalUpper().physicalKey())>0))
            throw new IllegalArgumentException("Logical range widens its original capture");
    }
    private record Element(DistributedBucket.Member member,List<byte[]> tails) {}
    private static int compare(List<byte[]> a,List<byte[]> b) {
        for (int i=0;i<a.size();i++) { int order=Arrays.compareUnsigned(a.get(i),b.get(i)); if (order!=0) return order; } return 0;
    }
    private static int compare(DistributedSchema.EncodedKey a,DistributedSchema.EncodedKey b) {
        int physical=Arrays.compareUnsigned(a.physicalKey(),b.physicalKey()); return physical!=0 ? physical : compare(a.variableKeys(),b.variableKeys());
    }
    private static final class Reader {
        final PhysicalReader physicalReader; final DistributedSchema schema; final DistributedSchema.Column binding;
        final byte[] lower,upper; final DistributedSchema.EncodedKey logicalLower,logicalUpper; final boolean reverse,bucket;
        byte[] physicalResume,bucketKey; List<Element> members=List.of(); int memberIndex;
        Iterator<PhysicalRangePage.Row> rows=Collections.emptyIterator(); boolean more=true;
        int scanned; long scannedBytes;
        Reader(PhysicalReader physicalReader,DistributedSchema schema,DistributedSchema.Column binding,byte[] lower,byte[] upper,DistributedSchema.EncodedKey logicalLower,DistributedSchema.EncodedKey logicalUpper,boolean reverse,DistributedSchema.EncodedKey resume) throws IOException {
            this.physicalReader=physicalReader; this.schema=schema; this.binding=binding; this.lower=lower; this.upper=upper; this.logicalLower=logicalLower; this.logicalUpper=logicalUpper; this.reverse=reverse;
            bucket=binding.schema().variableLengthKeysCount()!=0; physicalResume=resume==null ? null : resume.physicalKey();
            if (bucket && resume!=null) {
                byte[] value=physicalReader.get(binding.columnId(),physicalResume); charge(1,physicalResume.length+(value==null ? 0L : value.length));
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
            try {
                for (var member:schema.decodeBucket(binding.columnId(),physical,value).members()) {
                    var tails=member.tails();
                    if (logicalLower!=null && Arrays.equals(physical,logicalLower.physicalKey()) && compare(tails,logicalLower.variableKeys())<0
                            || logicalUpper!=null && Arrays.equals(physical,logicalUpper.physicalKey()) && compare(tails,logicalUpper.variableKeys())>=0) continue;
                    ordered.add(new Element(member,tails));
                }
            }
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
                    var page=physicalReader.page(binding.columnId(),lower,upper,reverse,physicalResume,Math.min(128,MAX_SCANNED_ROWS-scanned),
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
