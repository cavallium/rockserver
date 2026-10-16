package it.cavallium.rockserver.core.common.distributed;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import java.io.*;
import java.util.*;

/** Canonical big-endian logical input: tagged header, count, key-component lengths/bytes, nullable value lengths/bytes. */
public final class RemoteMutationInputs {
    private RemoteMutationInputs() {}
    public static byte[] point(long column,int writeKind,RequestType.RequestTypeId callback,boolean multi,List<Keys> keys,List<Buf> values) {
        return digest(out -> { out.writeByte(0);out.writeLong(column);out.writeInt(writeKind);out.writeInt(callback.ordinal());out.writeBoolean(multi);out.writeInt(keys.size());for(int n=0;n<keys.size();n++){key(out,keys.get(n));value(out,values.isEmpty()?null:values.get(n));} });
    }
    public static byte[] range(long column,Keys lower,Keys upper) { return digest(out -> {out.writeByte(1);out.writeLong(column);key(out,lower);key(out,upper);}); }
    public static byte[] bulkHeader(long column,boolean merge) {return digest(out -> {out.writeByte(2);out.writeLong(column);out.writeBoolean(merge);});}
    public static byte[] batch(long column,boolean merge,KVBatch batch) { if(batch.keys().size()!=batch.values().size() || batch.keys().size()>4096)throw new IllegalArgumentException("Invalid batch input count");return digest(out -> {out.writeByte(3);out.writeLong(column);out.writeBoolean(merge);out.writeInt(batch.keys().size());for(int n=0;n<batch.keys().size();n++){key(out,batch.keys().get(n));value(out,batch.values().get(n));}}); }
    public static byte[] atomic(List<AtomicMutation> mutations,List<AtomicPredicate> predicates,long ttlMillis) {
        atomicSize(mutations,predicates,ttlMillis);
        return digest(out -> {out.writeByte(4);out.writeLong(ttlMillis);out.writeInt(mutations.size());for(var mutation:mutations){out.writeLong(mutation.column());key(out,mutation.key());out.writeByte(mutation.kind().ordinal());value(out,mutation.value());out.writeByte(mutation.returns().ordinal());}out.writeInt(predicates.size());for(var predicate:predicates){out.writeLong(predicate.column());key(out,predicate.key());value(out,predicate.expected());}});
    }
    /** Size preflight precedes backend copies, identity allocation and native captures. */
    public static long atomicSize(List<AtomicMutation> mutations,List<AtomicPredicate> predicates,long ttlMillis) {
        Objects.requireNonNull(mutations);Objects.requireNonNull(predicates);
        if(ttlMillis<1 || ttlMillis>60000 || mutations.size()+predicates.size()<1 || (long)mutations.size()+predicates.size()>256)throw new IllegalArgumentException("Invalid atomic count or lifetime");
        long bytes=17;
        for(var mutation:mutations){Objects.requireNonNull(mutation);bytes=Math.addExact(bytes,14+keySize(mutation.key())+(mutation.value()==null?0:mutation.value().size()));if(bytes>2*1024*1024)throw new IllegalArgumentException("Atomic input exceeds byte bound");}
        for(var predicate:predicates){Objects.requireNonNull(predicate);bytes=Math.addExact(bytes,12+keySize(predicate.key())+(predicate.expected()==null?0:predicate.expected().size()));if(bytes>2*1024*1024)throw new IllegalArgumentException("Atomic input exceeds byte bound");}
        return bytes;
    }
    private static long keySize(Keys key){var parts=Objects.requireNonNull(key).keys();if(parts.length>DistributedSchemaCodec.MAX_KEY_COMPONENTS)throw new IllegalArgumentException("Atomic logical key component count exceeds bound");long size=4;for(var part:parts){size=Math.addExact(size,4L+Objects.requireNonNull(part).size());if(size>2*1024*1024)throw new IllegalArgumentException("Atomic key exceeds byte bound");}return size;}
    private static void key(DataOutputStream out,Keys keys) throws IOException {out.writeInt(keys==null?-1:keys.keys().length);if(keys!=null)for(var component:keys.keys())value(out,component);}
    private static void value(DataOutputStream out,Buf value) throws IOException {out.writeInt(value==null?-1:value.size());if(value!=null){byte[] page=new byte[Math.min(64*1024,value.size())];for(int offset=0;offset<value.size();){int count=Math.min(page.length,value.size()-offset);for(int n=0;n<count;n++)page[n]=value.getByte(offset+n);out.write(page,0,count);offset+=count;}}}
    @FunctionalInterface private interface Input {void write(DataOutputStream out) throws IOException;}
    private static byte[] digest(Input input) {try {var digest=java.security.MessageDigest.getInstance("SHA-256");var bytes=new java.security.DigestOutputStream(OutputStream.nullOutputStream(),digest);input.write(new DataOutputStream(bytes));return digest.digest();}catch(IOException impossible){throw new UncheckedIOException(impossible);}catch(java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
}
