package it.cavallium.rockserver.core.impl.test;

import static java.lang.Integer.rotateLeft;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;

/**
 * Frozen copy of the composite-key builder that {@code ColumnInstance.calculateKey} used before keys were
 * built straight into one exact {@code byte[]}. Composite keys are an on-disk format, so the reference keeps
 * its own scalar XXHash32 and writes through the generic {@link Buf} setters, exactly like the old code.
 */
public final class LegacyColumnKeyBuilder {

	private static final int PRIME1 = 0x9E3779B1;
	private static final int PRIME2 = 0x85EBCA77;
	private static final int PRIME3 = 0xC2B2AE3D;
	private static final int PRIME4 = 0x27D4EB2F;
	private static final int PRIME5 = 0x165667B1;

	private LegacyColumnKeyBuilder() {
	}

	public static Buf calculateKey(ColumnSchema schema, Buf[] keys) {
		if (schema.keysCount() != keys.length) {
			throw RocksDBException.of(RocksDBErrorType.KEYS_COUNT_MISMATCH,
					"Keys count must be equal to the column keys count. Expected: " + schema.keysCount()
							+ ", got: " + keys.length);
		}
		int finalKeySizeBytes = 0;
		for (int i : schema.keys()) {
			finalKeySizeBytes += i;
		}
		Buf finalKey;
		if (keys.length == 0) {
			finalKey = Buf.create(0);
		} else if (keys.length == 1 && schema.variableLengthKeysCount() == 0) {
			finalKey = keys[0];
		} else {
			finalKey = Buf.createZeroes(finalKeySizeBytes);
			int offsetBytes = 0;
			for (int i = 0; i < schema.keysCount(); i++) {
				var computedKeyAtI = computeKeyAt(schema, i, keys);
				var computedKeyAtISize = computedKeyAtI.size();
				finalKey.setBytesFromBuf(offsetBytes, computedKeyAtI, 0, computedKeyAtISize);
				offsetBytes += computedKeyAtISize;
			}
		}
		if (finalKeySizeBytes != finalKey.size()) {
			throw RocksDBException.of(RocksDBErrorType.RAW_KEY_LENGTH_MISMATCH,
					"Keys size must be equal to the column keys size. Expected: "
							+ finalKeySizeBytes + ", got: " + finalKey.size());
		}
		return finalKey;
	}

	private static Buf computeKeyAt(ColumnSchema schema, int i, Buf[] keys) {
		if (i < schema.keysCount() - schema.variableLengthKeysCount()) {
			if (keys[i].size() != schema.key(i)) {
				throw RocksDBException.of(RocksDBErrorType.KEY_LENGTH_MISMATCH,
						"Key at index " + i + " has a different length than expected! Expected: " + schema.key(i)
								+ ", received: " + keys[i].size());
			}
			return keys[i];
		} else {
			var tailKey = schema.variableTailKey(i);
			var hashResult = Buf.createZeroes(tailKey.bytesSize());
			hash(tailKey, keys[i], hashResult);
			return hashResult;
		}
	}

	private static void hash(ColumnHashType type, Buf inputData, Buf hashResult) {
		switch (type) {
			case XXHASH32 -> hashResult.setInt(0, xxHash32(inputData.toByteArray()));
			case XXHASH8 -> hashResult.setByte(0, (byte) xxHash32(inputData.toByteArray()));
			case ALLSAME8 -> hashResult.setByte(0, (byte) 0);
			case FIXEDINTEGER32 -> {
				if (inputData.size() != Integer.BYTES) {
					throw new UnsupportedOperationException("Input type is not 32 bits");
				}
				hashResult.setInt(0, inputData.getInt(0));
			}
		}
	}

	/**
	 * Scalar XXHash32 with seed 0, byte by byte.
	 */
	public static int xxHash32(byte[] buf) {
		int off = 0;
		final int len = buf.length;
		final int end = len;
		int h32;
		if (len >= 16) {
			final int limit = end - 16;
			int v1 = PRIME1 + PRIME2;
			int v2 = PRIME2;
			int v3 = 0;
			int v4 = -PRIME1;
			do {
				v1 = rotateLeft(v1 + readIntLE(buf, off) * PRIME2, 13) * PRIME1;
				v2 = rotateLeft(v2 + readIntLE(buf, off + 4) * PRIME2, 13) * PRIME1;
				v3 = rotateLeft(v3 + readIntLE(buf, off + 8) * PRIME2, 13) * PRIME1;
				v4 = rotateLeft(v4 + readIntLE(buf, off + 12) * PRIME2, 13) * PRIME1;
				off += 16;
			} while (off <= limit);
			h32 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
		} else {
			h32 = PRIME5;
		}
		h32 += len;
		while (off <= end - 4) {
			h32 += readIntLE(buf, off) * PRIME3;
			h32 = rotateLeft(h32, 17) * PRIME4;
			off += 4;
		}
		while (off < end) {
			h32 += (buf[off] & 0xFF) * PRIME5;
			h32 = rotateLeft(h32, 11) * PRIME1;
			++off;
		}
		h32 ^= h32 >>> 15;
		h32 *= PRIME2;
		h32 ^= h32 >>> 13;
		h32 *= PRIME3;
		h32 ^= h32 >>> 16;
		return h32;
	}

	private static int readIntLE(byte[] buf, int i) {
		return (buf[i] & 0xFF) | ((buf[i + 1] & 0xFF) << 8) | ((buf[i + 2] & 0xFF) << 16) | ((buf[i + 3] & 0xFF) << 24);
	}
}
