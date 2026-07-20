package it.cavallium.rockserver.core.impl.rocksdb;

import java.util.ArrayList;
import java.util.List;

public class RocksDBObjects implements AutoCloseable {
	private final List<AutoCloseable> refs;

	public RocksDBObjects() {
		this.refs = new ArrayList<>();
	}

    public RocksDBObjects(AutoCloseable... refs) {
        if (refs == null) {
            throw new IllegalArgumentException("RocksDB object references must not be null");
        }
        this.refs = new ArrayList<>(refs.length);
        for (AutoCloseable ref : refs) {
            add(ref);
        }
    }

    public void add(AutoCloseable ref) {
        if (ref == null) {
            throw new IllegalArgumentException("RocksDB object reference must not be null");
        }
        this.refs.add(ref);
    }

    public List<AutoCloseable> asList() {
        return List.copyOf(refs);
    }

    @Override
    public void close() {
        IllegalStateException exception = null;
        for (int i = refs.size() - 1; i >= 0; i--) {
            try {
                refs.get(i).close();
            } catch (Exception e) {
                if (exception == null) {
                    exception = new IllegalStateException("Failed to close RocksDB object reference", e);
                } else {
                    exception.addSuppressed(e);
                }
            }
        }
        if (exception != null) {
            throw exception;
        }
    }
}
