package it.cavallium.rockserver.core.impl.rocksdb;

import java.io.Closeable;
import java.util.Arrays;
import java.util.List;
import org.rocksdb.ColumnFamilyHandle;
import static it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType.RESTRICTED_TRANSACTION;

import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.WorkloadProfile;
import org.rocksdb.Transaction;

public record Tx(Transaction val,
		boolean isFromGetForUpdate,
		long expirationTimestamp,
		RocksDBObjects objs,
		WorkloadProfile workloadProfile,
		List<ScopeSpan> scope)
		implements Closeable, DBWriter {

	/** Validated physical intervals; a point includes its whole collision bucket. */
	public record ScopeSpan(ColumnFamilyHandle column, byte[] lower, byte[] upper) {}

	public void requirePoint(ColumnFamilyHandle column, byte[] key) {
		if (scope == null) return;
		for (var span : scope) {
			if (span.column() == column && (span.lower() == null || compareBounds(key, span.lower()) >= 0)
					&& (span.upper() == null || compareBounds(key, span.upper()) < 0)) return;
		}
		throw RocksDBException.of(RESTRICTED_TRANSACTION, "Access outside declared transaction scope");
	}

	public void requireRange(ColumnFamilyHandle column, byte[] lower, byte[] upper) {
		if (scope == null) return;
		byte[] covered = lower;
		boolean started = false;
		for (var span : scope) {
			if (span.column() != column) continue;
			if (covered == null ? span.lower() != null
					: span.lower() != null && compareBounds(span.lower(), covered) > 0) break;
			if (span.upper() != null && covered != null && compareBounds(span.upper(), covered) <= 0) continue;
			started = true;
			covered = span.upper();
			if (covered == null || upper != null && compareBounds(covered, upper) >= 0) return;
		}
		if (started && upper != null && covered != null && compareBounds(covered, upper) >= 0) return;
		throw RocksDBException.of(RESTRICTED_TRANSACTION, "Range outside declared transaction scope");
	}

	/** Column keys have a fixed physical width: compact prefixes imply zero suffixes. */
	public static int compareBounds(byte[] left, byte[] right) {
		int common = Math.min(left.length, right.length);
		int order = Arrays.compareUnsigned(left, 0, common, right, 0, common);
		if (order != 0) return order;
		for (int i = common; i < left.length; i++) if (left[i] != 0) return 1;
		for (int i = common; i < right.length; i++) if (right[i] != 0) return -1;
		return 0;
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
