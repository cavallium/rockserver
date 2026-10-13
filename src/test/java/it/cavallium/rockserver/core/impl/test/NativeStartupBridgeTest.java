package it.cavallium.rockserver.core.impl.test;

import it.cavallium.rockserver.core.impl.rocksdb.NativeStartupBridge;
import java.lang.foreign.MemorySegment;
import java.lang.reflect.InvocationTargetException;

import java.lang.foreign.Arena;
import java.nio.file.Path;
import java.nio.file.Files;
import org.junit.jupiter.api.io.TempDir;
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
    private static final String CACHE_STATS = """
            {"sample_candidates":1,"sampled":2,"dropped":3,"snapshots":4,"snapshot_bytes":5,
             "aborts":6,"errors":7,"rejected":8,"data_hits":9,"index_hits":10,"filter_hits":11,
             "dictionary_hits":12,"replay_bytes":13,"lookup_reserved_bytes":14,
             "oversized_decoder_rejections":15,"feedback":16}
            """;

    private static NativeStartupBridge.CacheStats parseStats(String json) throws Exception {
        var method = NativeStartupBridge.class.getDeclaredMethod("parseCacheStats", String.class);
        method.setAccessible(true);
        try { return (NativeStartupBridge.CacheStats) method.invoke(null, json); }
        catch (InvocationTargetException e) {
            if (e.getCause() instanceof RuntimeException failure) throw failure;
            throw e;
        }
    }

    @Test void mapsAllNativeCacheCountersByName() throws Exception {
        var stats = parseStats(CACHE_STATS);
        long[] actual = {stats.sampleCandidates(), stats.sampled(), stats.dropped(), stats.snapshots(),
                stats.snapshotBytes(), stats.aborts(), stats.errors(), stats.rejected(), stats.dataHits(),
                stats.indexHits(), stats.filterHits(), stats.dictionaryHits(), stats.replayBytes(),
                stats.lookupReservedBytes(), stats.oversizedDecoderRejections(), stats.feedback()};
        for (int i = 0; i < actual.length; i++) assertEquals(i + 1, actual[i]);
        assertEquals(0, parseStats(CACHE_STATS.replace("\"data_hits\":9", "\"data_hits\":0")).dataHits());
    }

    @Test void rejectsMissingMalformedAndNonIntegralNativeCacheStats() {
        for (String json : new String[]{"not JSON", "{}",
                CACHE_STATS.replace("\"data_hits\":9", "\"data_hits\":null"),
                CACHE_STATS.replace("\"data_hits\":9", "\"data_hits\":1.5"),
                CACHE_STATS.replace("\"data_hits\":9", "\"data_hits\":-1")}) {
            var failure = assertThrows(IllegalStateException.class, () -> parseStats(json));
            assertEquals("Invalid native snapshot cache stats", failure.getMessage());
            assertNotNull(failure.getCause());
        }
    }

    @Test void artifactFingerprintFitsNativeCompatibilityBound(@TempDir Path directory) throws Exception {
        Path artifact = Files.writeString(directory.resolve("native.so"), "abc");
        var method = NativeStartupBridge.class.getDeclaredMethod("artifactFingerprint", Path.class);
        method.setAccessible(true);
        String fingerprint = (String) method.invoke(null, artifact);
        assertEquals("ba7816bf8f01cfea414140de5dae2223b00361a396177a9cb410ff61f20015ad", fingerprint);
        assertTrue(fingerprint.matches("[0-9a-f]{64}"));
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
