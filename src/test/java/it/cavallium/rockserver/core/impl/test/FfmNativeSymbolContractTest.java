package it.cavallium.rockserver.core.impl.test;

import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import java.lang.foreign.Linker;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.SymbolLookup;

import static org.junit.jupiter.api.Assertions.assertTrue;

public class FfmNativeSymbolContractTest {

    @Test
    public void nativeSymbolsRequiredByFfmMergeOperatorAreResolvable() {
        RocksDB.loadLibrary();
        var symbols = new SymbolLookups(SymbolLookup.loaderLookup(), Linker.nativeLinker().defaultLookup());

        assertResolvable("malloc", symbols);
        assertResolvable("free", symbols);
        assertResolvable("rocksdb_mergeoperator_create", symbols);
        assertResolvable("rocksdb_mergeoperator_destroy", symbols);
    }

    private static void assertResolvable(String name, SymbolLookups symbols) {
        assertTrue(
                symbols.loaderLookup().find(name)
                        .or(() -> symbols.defaultLookup().find(name))
                        .filter(segment -> !segment.equals(MemorySegment.NULL))
                        .isPresent(),
                () -> "Native symbol is not resolvable: " + name
        );
    }

    private record SymbolLookups(SymbolLookup loaderLookup, SymbolLookup defaultLookup) {
    }
}
