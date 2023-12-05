package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.Closeable;
import org.rocksdb.AbstractNativeReference;

public record REntry<T extends AbstractNativeReference>(T val, RocksDBObjects objs) implements Closeable {

	@Override
	public void close() {
		val.close();
		objs.close();
	}
}
