package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.rocksdb.NativeStartupBridge;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.InvocationTargetException;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import org.junit.jupiter.api.Test;
import static java.lang.foreign.ValueLayout.*;
import static org.junit.jupiter.api.Assertions.*;

class NativeStartupBridgeTest {
    private static MemorySegment encode(Arena arena, NativeStartupBridge.CacheConfig config, String compatibility) throws Exception {
        var method = NativeStartupBridge.class.getDeclaredMethod("encodeCacheConfig", Arena.class, NativeStartupBridge.CacheConfig.class, String.class);
        method.setAccessible(true);
        try { return (MemorySegment) method.invoke(null, arena, config, compatibility); }
        catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException failure) throw failure;
            throw e;
        }
    }
    @Test void encodesExactNativeLayoutAndScopedStringsWithoutBindingNativeSymbols() throws Exception {
        var config = new NativeStartupBridge.CacheConfig(1, -1, 101, 102, 103, 104, 105,
                106, 107, 108, 109, 110, 111, 112, 113, 8, 4, .5,
                Path.of("cache/../snapshots"), "metadata");
        try (var arena = Arena.ofConfined()) {
            var data = encode(arena, config, "sha256-test");
            assertEquals(160, data.byteSize());
            assertEquals(0, data.address() % 8);
            assertEquals(160, data.get(JAVA_INT, 0));
            assertEquals(1, data.get(JAVA_INT, 4));
            assertEquals(1, data.get(JAVA_INT, 8));
            assertEquals(-1, data.get(JAVA_INT, 12));
            for (int i = 0; i < 13; i++) assertEquals(101 + i, data.get(JAVA_LONG, 16L + i * 8L));
            assertEquals(8, data.get(JAVA_INT, 120));
            assertEquals(4, data.get(JAVA_INT, 124));
            assertEquals(.5, data.get(JAVA_DOUBLE, 128));
            assertEquals(config.directory().toAbsolutePath().normalize().toString(), data.get(ADDRESS, 136).reinterpret(4096).getString(0));
            assertEquals("metadata", data.get(ADDRESS, 144).reinterpret(4096).getString(0));
            assertEquals("sha256-test", data.get(ADDRESS, 152).reinterpret(4096).getString(0));
        }
    }

    @Test void rejectsEmbeddedNulBeforeCallingNativeCode() {
        var config = new NativeStartupBridge.CacheConfig(0, 0, 1, 1, 1, 1, 64, 1, 1,
                1, 1, 1, 0, 0, 0, 1, 1, .5, Path.of("snapshots"), "bad\0name");
        try (var arena = Arena.ofConfined()) {
            assertThrows(IllegalArgumentException.class, () -> encode(arena, config, "hash"));
        }
    }
}
