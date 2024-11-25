package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.Closeable;
import org.rocksdb.AbstractNativeReference;
import reactor.util.annotation.Nullable;

public record REntry<T extends AbstractNativeReference>(T val, @Nullable Long expirationTimestamp,
																												RocksDBObjects objs) implements Closeable {

	@Override
	public void close() {
		val.close();
		objs.close();
	}
}
