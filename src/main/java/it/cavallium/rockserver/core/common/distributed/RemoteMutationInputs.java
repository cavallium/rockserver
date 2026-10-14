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
    private static void key(DataOutputStream out,Keys keys) throws IOException {out.writeInt(keys==null?-1:keys.keys().length);if(keys!=null)for(var component:keys.keys())value(out,component);}
    private static void value(DataOutputStream out,Buf value) throws IOException {out.writeInt(value==null?-1:value.size());if(value!=null)out.write(value.toByteArray());}
    @FunctionalInterface private interface Input {void write(DataOutputStream out) throws IOException;}
    private static byte[] digest(Input input) {try {var digest=java.security.MessageDigest.getInstance("SHA-256");var bytes=new java.security.DigestOutputStream(OutputStream.nullOutputStream(),digest);input.write(new DataOutputStream(bytes));return digest.digest();}catch(IOException impossible){throw new UncheckedIOException(impossible);}catch(java.security.NoSuchAlgorithmException impossible){throw new AssertionError(impossible);}}
}
