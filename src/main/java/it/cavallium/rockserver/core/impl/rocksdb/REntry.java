package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.Closeable;
import org.rocksdb.AbstractNativeReference;
import org.rocksdb.Status.Code;

public record REntry<T extends AbstractNativeReference>(T val, RocksDBObjects objs) implements Closeable {

	@Override
	public void close() {
		val.close();
		objs.close();
	}
}
