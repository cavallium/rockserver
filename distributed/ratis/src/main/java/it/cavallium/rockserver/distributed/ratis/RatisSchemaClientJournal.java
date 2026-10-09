package it.cavallium.rockserver.distributed.ratis;

import java.io.*;
import java.nio.file.*;
import java.util.*;

/** One forced bounded snapshot per request; caller owns the root lock and serializes access. */
final class RatisSchemaClientJournal {
    static final int MAX_BYTES=32*1024*1024, MAX_FIELDS=4096;
    private static final int MAGIC=0x5253434a;
    static void write(Path file,Map<String,byte[]> fields) throws IOException {
        var bytes=new ByteArrayOutputStream(); var out=new DataOutputStream(bytes); out.writeInt(MAGIC); out.writeInt(fields.size());
        if (fields.size()>MAX_FIELDS) throw new IOException("Schema request journal fields exhausted");
        for (var key:fields.keySet().stream().sorted().toList()) {
            if (key.length()>128 || fields.get(key)==null) throw new IOException("Invalid schema journal field");
            out.writeUTF(key); byte[] value=fields.get(key); out.writeInt(value.length); out.write(value);
            if (bytes.size()>MAX_BYTES-32) throw new IOException("Schema request journal exceeds bound");
        }
        out.flush(); SessionJournalFiles.write(file,bytes.toByteArray(),MAX_BYTES);
    }
    static Map<String,byte[]> read(Path file) throws IOException {
        var in=new DataInputStream(new ByteArrayInputStream(SessionJournalFiles.read(file,MAX_BYTES)));
        if (in.readInt()!=MAGIC) throw new IOException("Invalid schema request journal"); int count=in.readInt();
        if (count<1 || count>MAX_FIELDS) throw new IOException("Invalid schema journal field count");
        var fields=new HashMap<String,byte[]>();
        for (int i=0;i<count;i++) {
            String key=in.readUTF(); int n=in.readInt(); if (key.length()>128 || n<0 || n>in.available() || fields.containsKey(key)) throw new IOException("Invalid schema journal field"); fields.put(key,in.readNBytes(n));
        }
        if (in.available()!=0) throw new IOException("Trailing schema journal"); return fields;
    }
}
