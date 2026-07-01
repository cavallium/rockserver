package it.cavallium.rockserver.core.impl.rocksdb;

import java.util.ArrayList;
import java.util.List;
import org.rocksdb.AbstractImmutableNativeReference;

public class RocksDBObjects implements AutoCloseable {
	private final List<AutoCloseable> refs;

	public RocksDBObjects(int size) {
		this.refs = new ArrayList<>(size);
	}
	public RocksDBObjects() {
		this.refs = new ArrayList<>();
	}

	public RocksDBObjects(AutoCloseable... refs) {
		this(refs.length);
		for (AutoCloseable ref : refs) {
			add(ref);
		}
	}

 public void add(AutoCloseable ref) {
                this.refs.add(ref);
        }
        public List<AutoCloseable> asList() {
                return List.copyOf(refs);
        }

	@Override
	public void close() {
		RuntimeException exception = null;
		for (int i = refs.size() - 1; i >= 0; i--) {
			try {
				var ref = refs.get(i);
				if (ref instanceof AbstractImmutableNativeReference nativeRef && !nativeRef.isOwningHandle()) {
					continue;
				}
				ref.close();
			} catch (Exception e) {
				if (exception == null) {
					exception = new RuntimeException(e);
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
