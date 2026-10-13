package it.cavallium.rockserver.distributed.ratis;

import java.io.*;
import java.nio.channels.*;
import java.nio.file.*;
import java.util.*;

/** Forced current restore state. Unknown force fences this owner; cold reopen reuses the exact bytes. */
final class RatisDatabaseRestoreJournal implements AutoCloseable {
    static final int MAX_BYTES=128*1024*1024;
    private final Path file;private final FileChannel channel;private final FileLock lock;private boolean fenced;
    final Map<String,byte[]> fields;
    RatisDatabaseRestoreJournal(Path root,Map<String,byte[]> initial)throws IOException{
        if(!Files.isDirectory(root,LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(root))throw new IOException("Restore journal root absent/foreign");file=root.resolve("restore.state");channel=FileChannel.open(root.resolve("restore.lock"),StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);FileLock acquired=null;
        try{acquired=channel.tryLock();if(acquired==null)throw new IOException("Restore already owned");lock=acquired;if(Files.exists(file,LinkOption.NOFOLLOW_LINKS)){if(initial!=null)throw new IOException("Restore retained; reopen original");fields=decode(SessionJournalFiles.read(file,MAX_BYTES));SessionJournalFiles.force(file);SessionJournalFiles.force(root);}else{if(initial==null)throw new IOException("Restore journal absent");fields=new HashMap<>(initial);save();}}catch(IOException|RuntimeException|Error failure){if(acquired!=null)acquired.close();channel.close();throw failure;}
    }
    void save()throws IOException{if(fenced || !lock.isValid())throw new IOException("Restore journal fenced");try{var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);if(fields.size()>4096)throw new IOException("Restore metadata field capacity exceeded");out.writeInt(fields.size());for(var key:fields.keySet().stream().sorted().toList()){if(key.length()>128)throw new IOException("Restore field name exceeded");out.writeUTF(key);byte[] value=fields.get(key);out.writeInt(value.length);out.write(value);if(bytes.size()>MAX_BYTES-32)throw new IOException("Restore metadata byte capacity exceeded");}out.flush();SessionJournalFiles.write(file,bytes.toByteArray(),MAX_BYTES);}catch(IOException failure){fenced=true;throw failure;}}
    private static Map<String,byte[]> decode(byte[] bytes)throws IOException{var in=new DataInputStream(new ByteArrayInputStream(bytes));int count=in.readInt();if(count<1 || count>4096)throw new IOException("Invalid restore field count");var fields=new HashMap<String,byte[]>();for(int i=0;i<count;i++){String key=in.readUTF();int n=in.readInt();if(key.length()>128 || n<0 || n>in.available() || fields.containsKey(key))throw new IOException("Invalid restore field");fields.put(key,in.readNBytes(n));}if(in.available()!=0)throw new IOException("Trailing restore state");return fields;}
    public void close()throws IOException{try{lock.close();}finally{channel.close();}}
}
