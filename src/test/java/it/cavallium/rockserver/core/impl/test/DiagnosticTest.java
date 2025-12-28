package it.cavallium.rockserver.core.impl.test;

import org.junit.jupiter.api.Test;
import org.rocksdb.RocksDB;

import java.lang.foreign.Linker;
import java.lang.foreign.SymbolLookup;
import java.util.Map;
import java.util.Optional;

public class DiagnosticTest {

    @Test
    public void diagnose() {
        System.out.println("=== THREAD DIAGNOSTICS ===");
        Map<Thread, StackTraceElement[]> traces = Thread.getAllStackTraces();
        for (Thread t : traces.keySet()) {
            System.out.printf("Thread: %s (id=%d, daemon=%b, state=%s)\n", t.getName(), t.getId(), t.isDaemon(), t.getState());
        }
        System.out.println("==========================");

        System.out.println("=== SYMBOL DIAGNOSTICS ===");
        RocksDB.loadLibrary();
        Linker linker = Linker.nativeLinker();
        SymbolLookup loaderLookup = SymbolLookup.loaderLookup();
        SymbolLookup defaultLookup = linker.defaultLookup();

        checkSymbol("malloc", loaderLookup, defaultLookup);
        checkSymbol("free", loaderLookup, defaultLookup);
        checkSymbol("je_malloc", loaderLookup, defaultLookup);
        checkSymbol("je_free", loaderLookup, defaultLookup);
        checkSymbol("rocksdb_malloc", loaderLookup, defaultLookup);
        checkSymbol("rocksdb_free", loaderLookup, defaultLookup);
        checkSymbol("rocksdb_mergeoperator_create", loaderLookup, defaultLookup);
        
        System.out.println("==========================");
    }

    private void checkSymbol(String name, SymbolLookup l1, SymbolLookup l2) {
        Optional<java.lang.foreign.MemorySegment> s1 = l1.find(name);
        Optional<java.lang.foreign.MemorySegment> s2 = l2.find(name);
        System.out.printf("Symbol '%s': loader=%s, default=%s\n", name, s1.isPresent(), s2.isPresent());
    }
}
