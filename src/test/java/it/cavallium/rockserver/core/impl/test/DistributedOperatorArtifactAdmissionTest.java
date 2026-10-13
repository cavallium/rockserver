package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.rocksdb.DistributedOperatorArtifacts;

import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
import java.io.*;
import java.nio.channels.FileChannel;
import java.nio.file.*;
import java.security.MessageDigest;
import java.util.*;
import java.util.concurrent.atomic.AtomicBoolean;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class DistributedOperatorArtifactAdmissionTest {
    @TempDir Path root;
    @FunctionalInterface private interface Force {void force(Path path) throws IOException;}
    private DistributedOperatorArtifacts cache(Path pool) throws Exception {
        var constructor=DistributedOperatorArtifacts.class.getDeclaredConstructor(Path.class);constructor.setAccessible(true);
        try {return (DistributedOperatorArtifacts)constructor.newInstance(pool);}catch(java.lang.reflect.InvocationTargetException failure){throw cause(failure);}
    }
    private DistributedOperatorArtifacts cache(Path pool,Force forcing) throws Exception {
        var type=Class.forName(DistributedOperatorArtifacts.class.getName()+"$Force");
        var force=java.lang.reflect.Proxy.newProxyInstance(type.getClassLoader(),new Class<?>[]{type},(proxy,method,args)->{forcing.force((Path)args[0]);return null;});
        var constructor=DistributedOperatorArtifacts.class.getDeclaredConstructor(Path.class,type);constructor.setAccessible(true);
        try {return (DistributedOperatorArtifacts)constructor.newInstance(pool,force);}catch(java.lang.reflect.InvocationTargetException failure){throw cause(failure);}
    }
    private Exception cause(java.lang.reflect.InvocationTargetException failure) throws Error {
        if(failure.getCause() instanceof Error error)throw error;
        return failure.getCause() instanceof Exception exception ? exception : failure;
    }
    private void qualify(DistributedOperatorArtifacts owner,DistributedSchema source,Map<UUID,String> names,boolean virgin) throws Exception {
        var method=DistributedOperatorArtifacts.class.getDeclaredMethod("qualifyBootstrap",Collection.class,Map.class,Map.class,boolean.class);method.setAccessible(true);
        try {method.invoke(owner,List.of(source),Map.of(),names,virgin);}catch(java.lang.reflect.InvocationTargetException failure){throw cause(failure);}
    }
    private Path directory(Path pool) {return pool.resolveSibling(pool.getFileName()+".operators");}
    private DistributedSchema schema() {return new DistributedSchema(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),0,0,List.of());}
    private String digest(Path file) throws Exception {
        var sha=MessageDigest.getInstance("SHA-256");byte[] buffer=new byte[8192];
        try(var input=Files.newInputStream(file)) {for(int n;(n=input.read(buffer))!=-1;)sha.update(buffer,0,n);}
        return HexFormat.of().formatHex(sha.digest());
    }
    private void payload(Path cache,int ordinal,long size) throws Exception {
        Path temporary=cache.resolve("payload");
        try(var file=new RandomAccessFile(temporary.toFile(),"rw")) {file.setLength(size);file.writeInt(ordinal);}
        Files.move(temporary,cache.resolve(digest(temporary)+".jar"));
    }
    @Test void coldInventoryIncludesUnreferencedPayloadAndExactBootstrapReopen() throws Exception {
        Path pool=root.resolve("cold"),cache=Files.createDirectory(directory(pool));payload(cache,1,4096);
        var source=schema();var names=Map.of(UUID.randomUUID(),"plain");
        var owner=cache(pool);qualify(owner,source,names,true);
        var before=DistributedOperatorArtifacts.inventory(pool);
        assertEquals(4096,before.jarPayloadBytes());assertEquals(1,before.artifacts());assertTrue(before.metadataBytes()>0);assertEquals(0,before.pendingFiles());
        qualify(cache(pool),source,names,false);
        assertEquals(before,DistributedOperatorArtifacts.inventory(pool));
        assertEquals(152L<<20,DistributedOperatorArtifacts.MAX_CACHE_BYTES);
    }
    @Test void aggregatePayloadOverflowRejectsBeforeNativeCacheCanOpen() throws Exception {
        Path pool=root.resolve("overflow"),cache=Files.createDirectory(directory(pool));
        for(int n=0;n<17;n++)payload(cache,n,8L<<20);
        var failure=assertThrows(IOException.class,()->cache(pool));
        assertTrue(failure.getMessage().contains("ceiling"));assertFalse(Files.exists(cache.resolve("bindings")));
    }
    @Test void foreignAndSymlinkFilesAndMultiplePendingOwnersReject() throws Exception {
        Path pool=root.resolve("invalid"),cache=Files.createDirectory(directory(pool));
        Files.write(cache.resolve("foreign"),new byte[]{1});assertThrows(IOException.class,()->cache(pool));Files.delete(cache.resolve("foreign"));
        Path outside=root.resolve("outside");Files.write(outside,new byte[]{1});Path link=cache.resolve("a".repeat(64)+".jar");Files.createSymbolicLink(link,outside);
        assertThrows(IOException.class,()->cache(pool));Files.delete(link);
        Files.write(cache.resolve(".operator-1.tmp"),new byte[32]);Files.write(cache.resolve(".operator-2.tmp"),new byte[32]);
        assertThrows(IOException.class,()->cache(pool));try(var files=Files.list(cache)){assertEquals(2,files.count());}
    }
    @Test void unknownPendingCleanupFencesNewAllocationUntilForcedRetrySettles() throws Exception {
        Path pool=root.resolve("pending"),cache=Files.createDirectory(directory(pool));Path pending=cache.resolve(".operator-1.tmp");Files.write(pending,new byte[64]);
        var fail=new AtomicBoolean(true);var owner=cache(pool,path->{
            if(path.equals(cache) && fail.get())throw new IOException("Unknown operator pending cleanup force");
            try(var channel=FileChannel.open(path,StandardOpenOption.READ)){channel.force(true);}
        });
        assertEquals(1,DistributedOperatorArtifacts.inventory(pool).pendingFiles());
        var source=schema();var names=Map.of(UUID.randomUUID(),"original");
        for(int n=0;n<3;n++) {
            assertThrows(IOException.class,()->qualify(owner,source,names,true));
            assertFalse(Files.exists(cache.resolve("bindings")));assertEquals(0,DistributedOperatorArtifacts.inventory(pool).pendingFiles());
        }
        fail.set(false);qualify(owner,source,names,true);
        assertEquals(0,DistributedOperatorArtifacts.inventory(pool).pendingFiles());
        qualify(cache(pool),source,names,false);
        assertFalse(Files.exists(pending));
    }
}
