package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.impl.rocksdb.RocksDBObjects;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.Test;

@SuppressWarnings("resource")
class RocksDBObjectsTest {

    @Test
    void closeClosesReferencesInReverseOrder() {
        var events = new ArrayList<String>();
        var refs = new RocksDBObjects(
                () -> events.add("first"),
                () -> events.add("second"),
                () -> events.add("third")
        );

        refs.close();

        assertEquals(List.of("third", "second", "first"), events);
    }

    @Test
    void closeAggregatesFailuresAndContinuesClosing() {
        var events = new ArrayList<String>();
        var firstFailure = new IOException("first");
        var secondFailure = new IllegalStateException("second");
        var refs = new RocksDBObjects(
                () -> {
                    events.add("first");
                    throw firstFailure;
                },
                () -> {
                    events.add("second");
                    throw secondFailure;
                },
                () -> events.add("third")
        );

        var failure = assertThrows(IllegalStateException.class, refs::close);

        assertEquals(List.of("third", "second", "first"), events);
        assertEquals("Failed to close RocksDB object reference", failure.getMessage());
        assertSame(secondFailure, failure.getCause());
        assertEquals(1, failure.getSuppressed().length);
        assertSame(firstFailure, failure.getSuppressed()[0]);
    }

    @Test
    void addRejectsNullReference() {
        var refs = new RocksDBObjects();

        var failure = assertThrows(IllegalArgumentException.class, () -> refs.add(null));

        assertEquals("RocksDB object reference must not be null", failure.getMessage());
    }

    @Test
    void constructorRejectsNullReferenceArray() {
        var failure = assertThrows(IllegalArgumentException.class, () -> new RocksDBObjects((AutoCloseable[]) null));

        assertEquals("RocksDB object references must not be null", failure.getMessage());
    }

    @Test
    void asListReturnsImmutableSnapshot() {
        var refs = new RocksDBObjects();
        AutoCloseable first = () -> {
        };
        AutoCloseable second = () -> {
        };
        refs.add(first);

        var snapshot = refs.asList();
        refs.add(second);

        assertEquals(List.of(first), snapshot);
        assertThrows(UnsupportedOperationException.class, () -> snapshot.add(second));
    }
}
