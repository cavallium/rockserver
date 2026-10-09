package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.common.distributed.DistributedSchema;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;
import static org.junit.jupiter.api.Assertions.*;

class DistributedOperatorArtifactsTest {
    @TempDir Path root;
    private static Object cache(Path path) throws Exception {
        var type=Class.forName("it.cavallium.rockserver.core.impl.rocksdb.DistributedOperatorArtifacts");
        var constructor=type.getDeclaredConstructor(Path.class); constructor.setAccessible(true); return constructor.newInstance(path);
    }
    private static Object invoke(Object cache,String method,Class<?>[] types,Object... arguments) throws Exception {
        var target=cache.getClass().getDeclaredMethod(method,types); target.setAccessible(true);
        try { return target.invoke(cache,arguments); }
        catch(java.lang.reflect.InvocationTargetException failure) {
            if(failure.getCause() instanceof Exception cause) throw cause;
            if(failure.getCause() instanceof Error cause) throw cause;
            throw failure;
        }
    }
    private static void qualify(Object cache,DistributedSchema schema,Map<UUID,String> plain,boolean virgin) throws Exception {
        invoke(cache,"qualifyBootstrap",new Class<?>[]{DistributedSchema.class,Map.class,Map.class,boolean.class},schema,Map.of(),plain,virgin);
    }
    @Test void staleConcurrentCacheCannotOverwritePublishedBootstrapOrImmutableRows() throws Exception {
        var path=root.resolve("pool"); var schema=new DistributedSchema(UUID.randomUUID(),UUID.randomUUID(),UUID.randomUUID(),0,0,List.of());
        var first=cache(path); var stale=cache(path); UUID one=UUID.randomUUID(),two=UUID.randomUUID();
        qualify(first,schema,Map.of(one,"first"),true);
        var directory=(Path)invoke(first,"directory",new Class<?>[]{}); byte[] original=Files.readAllBytes(directory.resolve("bindings"));
        assertThrows(java.io.IOException.class,() -> qualify(stale,schema,Map.of(two,"second"),true));
        assertArrayEquals(original,Files.readAllBytes(directory.resolve("bindings")));
        var reopened=cache(path); qualify(reopened,schema,Map.of(one,"first"),false);
        assertEquals(true,invoke(reopened,"contains",new Class<?>[]{String.class},"first"));
        assertEquals(false,invoke(reopened,"contains",new Class<?>[]{String.class},"second"));
    }
}
