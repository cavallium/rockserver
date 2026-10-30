package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.Closeable;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import org.rocksdb.Transaction;

/**
 * A RocksDB transaction. When {@code untracked}, its writes and internal reads skip conflict tracking: only for a
 * transaction whose keys have no other writer, such as the replicated apply.
 */
public record Tx(Transaction val,
		boolean isFromGetForUpdate,
		long expirationTimestamp,
		RocksDBObjects objs,
		WorkloadProfile workloadProfile,
		boolean untracked)
		implements Closeable, DBWriter {

	public Tx(Transaction val, boolean isFromGetForUpdate, long expirationTimestamp, RocksDBObjects objs,
			WorkloadProfile workloadProfile) {
		this(val, isFromGetForUpdate, expirationTimestamp, objs, workloadProfile, false);
	}

	@Override
	public void close() {
		val.close();
		objs.close();
	}

	@Override
	public void writePending() throws RocksDBException {

	}
}
