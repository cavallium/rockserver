package it.cavallium.rockserver.distributed.ratis;

import java.io.IOException;
import java.nio.channels.*;
import java.nio.file.*;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.*;

/** Root/inode admission precedes opening another marker FD: closing that FD could release this process's fcntl lock. */
final class RatisBackendRootOwnership {
    private record Identity(boolean directory,Object inode) {}
    private static final Map<Identity,RatisBackendRootOwnership> ACTIVE=new HashMap<>();
    private final List<Identity> claims=new ArrayList<>(2);
    private final Path root;
    private final FileChannel channel;
    private final FileLock lock;
    private int references;
    private boolean released;
    RatisBackendRootOwnership(Path root,RatisJournalAccess access)throws IOException {
        this.root=root;Path marker=root.resolve("backend.lock");access.register(marker,32);Files.createDirectories(root);
        synchronized(ACTIVE){
            var directory=identity(root,true);var existing=Files.exists(marker,LinkOption.NOFOLLOW_LINKS)?identity(marker,false):null;
            if(ACTIVE.containsKey(directory) || existing!=null && ACTIVE.containsKey(existing))throw new IOException("Backend root already owned in this process");
            if(ACTIVE.size()>8190)throw new IOException("Backend root lock admission exhausted");
            claims.add(directory);if(existing!=null)claims.add(existing);for(var claim:claims)ACTIVE.put(claim,this);
            FileChannel opened=null;FileLock acquired=null;
            try{
                opened=FileChannel.open(marker,StandardOpenOption.CREATE,StandardOpenOption.WRITE,LinkOption.NOFOLLOW_LINKS);
                var actual=identity(marker,false);if(!claims.contains(actual)){if(ACTIVE.containsKey(actual))throw new IOException("Backend marker inode changed while acquiring ownership");claims.add(actual);ACTIVE.put(actual,this);}
                acquired=opened.tryLock();if(acquired==null)throw new IOException("Backend root already owned");channel=opened;lock=acquired;
            }catch(IOException|RuntimeException failure){if(opened!=null)try{opened.close();}catch(IOException cleanup){failure.addSuppressed(cleanup);}for(var claim:claims)ACTIVE.remove(claim,this);throw failure;}
        }
    }
    private static Identity identity(Path path,boolean directory)throws IOException {
        var attributes=Files.readAttributes(path,BasicFileAttributes.class,LinkOption.NOFOLLOW_LINKS);
        if(directory?!attributes.isDirectory():!attributes.isRegularFile() || attributes.size()!=0)throw new IOException("Invalid backend root/ownership marker");
        if(attributes.fileKey()==null)throw new IOException("Backend root requires stable filesystem inode identity");return new Identity(directory,attributes.fileKey());
    }
    synchronized boolean released(){return released;}
    synchronized RatisJournalAccess.RootLease retain(Path expected)throws IOException {
        if(released || references>=2 || !root.equals(expected))throw new IOException("Backend capability root changed or released");references++;
        return new RatisJournalAccess.RootLease(){private boolean closed;public void close()throws IOException{synchronized(RatisBackendRootOwnership.this){if(closed)return;if(references==1){synchronized(ACTIVE){channel.close();released=true;for(var claim:claims)ACTIVE.remove(claim,RatisBackendRootOwnership.this);}}references--;closed=true;}}};
    }
}
