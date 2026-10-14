package it.cavallium.rockserver.distributed.ratis;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/** One forced bounded snapshot per request; caller owns the root lock and serializes access. */
final class RatisSchemaClientJournal {
    static final int MAX_BYTES=32*1024*1024, MAX_FIELDS=4096;
    private static final int MAGIC=0x5253434a;
    static void write(Path file,Map<String,byte[]> fields) throws IOException {writeOwned(file,fields,RatisJournalAccess.standalone());}
    static void writeOwned(Path file,Map<String,byte[]> fields,RatisJournalAccess access) throws IOException {
        if (fields.isEmpty() || fields.size()>MAX_FIELDS) throw new IOException("Schema request journal fields exhausted");
        long size=8;
        for (var entry:fields.entrySet()) {
            String key=entry.getKey(); byte[] value=entry.getValue();
            if (key==null || key.length()>128 || value==null) throw new IOException("Invalid schema journal field");
            int utfBytes=0;
            for (int i=0;i<key.length();i++) { char c=key.charAt(i); utfBytes+=c>=1 && c<=127 ? 1 : c<=2047 ? 2 : 3; }
            size+=6L+utfBytes+value.length;
            if (size>MAX_BYTES-32) throw new IOException("Schema request journal exceeds bound");
        }
        var bytes=new ByteArrayOutputStream((int)size); var out=new DataOutputStream(bytes); out.writeInt(MAGIC); out.writeInt(fields.size());
        for (var key:fields.keySet().stream().sorted().toList()) {
            out.writeUTF(key); byte[] value=fields.get(key); out.writeInt(value.length); out.write(value);
        }
        out.flush(); access.write(file,bytes.toByteArray(),MAX_BYTES);
    }
    static Map<String,byte[]> read(Path file) throws IOException {return readOwned(file,RatisJournalAccess.standalone());}
    static Map<String,byte[]> readOwned(Path file,RatisJournalAccess access) throws IOException {
        var in=new DataInputStream(new ByteArrayInputStream(access.read(file,MAX_BYTES)));
        if (in.readInt()!=MAGIC) throw new IOException("Invalid schema request journal"); int count=in.readInt();
        if (count<1 || count>MAX_FIELDS) throw new IOException("Invalid schema journal field count");
        var fields=new HashMap<String,byte[]>();
        for (int i=0;i<count;i++) {
            String key=in.readUTF(); int n=in.readInt(); if (key.length()>128 || n<0 || n>in.available() || fields.containsKey(key)) throw new IOException("Invalid schema journal field"); fields.put(key,in.readNBytes(n));
        }
        if (in.available()!=0) throw new IOException("Trailing schema journal"); return fields;
    }
}
