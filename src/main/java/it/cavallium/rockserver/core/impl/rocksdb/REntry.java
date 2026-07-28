package it.cavallium.rockserver.core.impl.rocksdb;

import it.cavallium.rockserver.core.common.WorkloadProfile;
import java.io.Closeable;
import org.rocksdb.AbstractNativeReference;
import reactor.util.annotation.Nullable;

public record REntry<T extends AbstractNativeReference>(T val, @Nullable Long expirationTimestamp,
		RocksDBObjects objs, WorkloadProfile workloadProfile) implements Closeable {

	@Override
	public void close() {
		try {
			val.close();
		} finally {
			objs.close();
		}
	}
}
