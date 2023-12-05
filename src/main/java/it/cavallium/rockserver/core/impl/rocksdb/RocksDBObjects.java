package it.cavallium.rockserver.core.impl.rocksdb;

import java.util.ArrayList;
import java.util.List;

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

	@Override
	public void close() {
		for (int i = refs.size() - 1; i >= 0; i--) {
			try {
				refs.get(i).close();
			} catch (Exception e) {
				throw new RuntimeException(e);
			}
		}
	}
}
