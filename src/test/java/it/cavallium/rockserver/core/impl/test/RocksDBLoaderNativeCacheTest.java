package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.rocksdb.RocksDBLoader;
import java.io.InputStream;
import java.lang.reflect.*;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.nio.file.*;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

/** Exercises extraction identities only; fake binaries are never passed to a native loader. */
class RocksDBLoaderNativeCacheTest {
    @FunctionalInterface interface NativeResource {InputStream open(String name)throws IOException;}
    record Selected(Object value,String sha256,String resource){}
    private static Object resources(NativeResource input)throws Exception {var type=Class.forName(RocksDBLoader.class.getName()+"$NativeResource");return Proxy.newProxyInstance(RocksDBLoader.class.getClassLoader(),new Class<?>[]{type},(proxy,method,args)->{if(method.getName().equals("open"))return input.open((String)args[0]);throw new AssertionError(method);});}
    private static Object invoke(Method method,Object target,Object... arguments)throws Exception {method.setAccessible(true);try{return method.invoke(target,arguments);}catch(InvocationTargetException failure){if(failure.getCause() instanceof Exception cause)throw cause;if(failure.getCause() instanceof Error cause)throw cause;throw new AssertionError(failure.getCause());}}
    private static Selected selectNative(String primary,String name,String fallback,String fallbackName,NativeResource input)throws Exception {var type=Class.forName(RocksDBLoader.class.getName()+"$NativeResource");var method=RocksDBLoader.class.getDeclaredMethod("selectNative",String.class,String.class,String.class,String.class,type);var value=invoke(method,null,primary,name,fallback,fallbackName,resources(input));return new Selected(value,(String)invoke(value.getClass().getDeclaredMethod("sha256"),value),(String)invoke(value.getClass().getDeclaredMethod("resource"),value));}
    private static Path extractNative(Path root,Selected selected,NativeResource input)throws Exception {var method=RocksDBLoader.class.getDeclaredMethod("extractNative",Path.class,Class.forName(RocksDBLoader.class.getName()+"$EmbeddedNative"),Class.forName(RocksDBLoader.class.getName()+"$NativeResource"));return (Path)invoke(method,null,root,selected.value(),resources(input));}
    @TempDir Path root;
    @Test void differentEmbeddedBytesAtSameDeclaredVersionNeverReuseOldLibrary()throws Exception {
        Path version=root.resolve("same-declared-version");Files.createDirectories(version);byte[] old={9,9},first={1,2,3},second={4,5,6};Files.write(version.resolve("mock.so"),old);
        var a=selectNative("embedded","mock.so",null,null,name->new ByteArrayInputStream(first));var b=selectNative("embedded","mock.so",null,null,name->new ByteArrayInputStream(second));
        var one=extractNative(version,a,name->new ByteArrayInputStream(first));var two=extractNative(version,b,name->new ByteArrayInputStream(second));assertNotEquals(one,two);assertArrayEquals(first,Files.readAllBytes(one));assertArrayEquals(second,Files.readAllBytes(two));assertArrayEquals(old,Files.readAllBytes(version.resolve("mock.so")));
        Files.write(one,second);assertEquals(one,extractNative(version,a,name->new ByteArrayInputStream(first)));assertArrayEquals(first,Files.readAllBytes(one));
        assertThrows(IOException.class,()->extractNative(version.resolve("changed"),a,name->new ByteArrayInputStream(second)));assertFalse(Files.exists(version.resolve("changed").resolve("sha256-"+a.sha256()).resolve("mock.so")));
    }
    @Test void selectedFallbackAndConcurrentPublicationUseBoundedBuffersAndCompleteSameBytes()throws Exception {
        byte[] bytes=new byte[200_017];new Random(13).nextBytes(bytes);var readSize=new AtomicInteger();var names=new CopyOnWriteArrayList<String>();
        NativeResource resources=name->{names.add(name);if(name.equals("primary"))return null;assertEquals("fallback",name);return new ByteArrayInputStream(bytes){public synchronized int read(byte[] target,int offset,int count){readSize.accumulateAndGet(count,Math::max);return super.read(target,offset,count);}};};
        var selected=selectNative("primary","primary.so","fallback","fallback.so",resources);assertEquals(List.of("primary","fallback"),names);assertEquals("fallback",selected.resource());
        try(var executor=Executors.newVirtualThreadPerTaskExecutor()){var start=new CountDownLatch(1);var first=executor.submit(()->{start.await();return extractNative(root,selected,resources);});var second=executor.submit(()->{start.await();return extractNative(root,selected,resources);});start.countDown();var a=first.get(5,TimeUnit.SECONDS);var b=second.get(5,TimeUnit.SECONDS);assertEquals(a,b);assertEquals("fallback.so",a.getFileName().toString());assertArrayEquals(bytes,Files.readAllBytes(a));try(var files=Files.list(a.getParent())){assertEquals(List.of(a),files.toList());}}
        assertTrue(readSize.get()<=64*1024);assertFalse(Files.exists(root.resolve("primary.so")));assertThrows(RuntimeException.class,()->selectNative("absent","none.so",null,null,name->null));
    }
}
