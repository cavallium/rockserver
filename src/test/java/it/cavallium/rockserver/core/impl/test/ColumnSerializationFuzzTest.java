package it.cavallium.rockserver.core.impl.test;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.SerializedKVBatch.SerializedKVBatchRef;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class ColumnSerializationFuzzTest {

	private static final int MAX_FIXED_KEYS = 4;
	private static final int MAX_VARIABLE_KEYS = 4;
	private static final int MAX_FIXED_KEY_SIZE = 64;
	private static final int MAX_VARIABLE_KEY_SIZE = 2_048;
	private static final int MAX_VALUE_SIZE = 4_096;
	private static final int MAX_BUCKET_INPUT_SIZE = 64 * 1_024;

	@FuzzTest(maxDuration = "30s")
	void columnKeysValuesAndSerializedBatchRoundTrip(FuzzedDataProvider data) {
		Fixture fixture = fixture(data, false);
		ColumnInstance column = fixture.column;
		Buf calculatedKey = column.calculateKey(fixture.keys);
		Buf calculatedValue = calculatedValue(column, fixture.keys, fixture.value);

		Buf[] decodedKeys = column.decodeKeys(calculatedKey, calculatedValue);
		Buf decodedValue = column.hasBuckets()
				? column.decodeValue(calculatedValue)
				: normalizeValue(fixture.value);
		assertBufArrayEquals(fixture.keys, decodedKeys);
		assertBufEquals(normalizeValue(fixture.value), decodedValue);
		assertBufEquals(calculatedKey, column.calculateKey(decodedKeys));

		int prefixSize = data.consumeInt(0, 16);
		byte[] prefix = bytes(data, prefixSize);
		int initialSlack = data.consumeInt(0, 8);
		Buf transcoded = Buf.createZeroes(prefixSize + initialSlack);
		transcoded.setBytesFromBuf(0, Buf.wrap(prefix), 0, prefix.length);
		int keysSize = column.transcodeBatchKeys(calculatedKey, calculatedValue, prefixSize, transcoded);
		assertArrayEquals(prefix, transcoded.copyOfRange(0, prefixSize).asArray());

		Buf serialized = serializedBatch(transcoded.copyOfRange(prefixSize, prefixSize + keysSize), decodedValue);
		KV expected = new KV(new Keys(fixture.keys), decodedValue);
		assertBufEquals(SerializedKVBatchFuzzTest.encode(List.of(expected)), serialized);
		assertEquals(List.of(expected), new SerializedKVBatchRef(serialized).decode().toList());
	}

	@FuzzTest(maxDuration = "30s")
	void malformedBucketEnvelopesRejectConsistentlyOrCanonicalize(FuzzedDataProvider data) {
		Fixture fixture = fixture(data, true);
		ColumnInstance column = fixture.column;
		Buf calculatedKey = column.calculateKey(fixture.keys);
		Buf arbitraryBucket = Buf.wrap(data.consumeBytes(MAX_BUCKET_INPUT_SIZE));

		TranscodeAttempt transcode = tryTranscode(column, calculatedKey, arbitraryBucket);
		DecodeAttempt decode = tryDecode(column, calculatedKey, arbitraryBucket);
		assertEquals(transcode.accepted(), decode.accepted(),
				"key transcoding and key/value decoding disagreed on malformed input");
		if (!decode.accepted()) {
			return;
		}

		Buf recalculatedKey = column.calculateKey(decode.keys);
		Buf canonicalBucketKey = column.computeBucketElementKey(column.getBucketElementKeys(decode.keys));
		Buf canonicalBucket = column.computeBucketElementKeyValue(canonicalBucketKey, decode.value);
		assertBufEquals(arbitraryBucket, canonicalBucket);

		TranscodeAttempt canonicalTranscode = tryTranscode(column, recalculatedKey, canonicalBucket);
		assertTrue(canonicalTranscode.accepted());
		assertBufEquals(transcode.encodedKeys, canonicalTranscode.encodedKeys);
		assertBufArrayEquals(decode.keys, column.decodeKeys(recalculatedKey, canonicalBucket));
		assertBufEquals(decode.value, column.decodeValue(canonicalBucket));
	}

	private static TranscodeAttempt tryTranscode(ColumnInstance column, Buf calculatedKey, Buf bucketValue) {
		try {
			Buf output = Buf.create();
			int written = column.transcodeBatchKeys(calculatedKey, bucketValue, 0, output);
			return new TranscodeAttempt(output.copyOfRange(0, written), true);
		} catch (RocksDBException expectedRejection) {
			return new TranscodeAttempt(null, false);
		}
	}

	private static DecodeAttempt tryDecode(ColumnInstance column, Buf calculatedKey, Buf bucketValue) {
		try {
			return new DecodeAttempt(
					column.decodeKeys(calculatedKey, bucketValue),
					column.decodeValue(bucketValue),
					true);
		} catch (RocksDBException expectedRejection) {
			return new DecodeAttempt(null, null, false);
		}
	}

	private static Buf calculatedValue(ColumnInstance column, Buf[] keys, Buf value) {
		if (!column.hasBuckets()) {
			return value;
		}
		Buf bucketKey = column.computeBucketElementKey(column.getBucketElementKeys(keys));
		return column.computeBucketElementKeyValue(bucketKey, value);
	}

	private static Buf normalizeValue(Buf value) {
		return value != null ? value : Buf.create();
	}

	private static Buf serializedBatch(Buf encodedKeys, Buf value) {
		Buf serialized = Buf.createZeroes(Integer.BYTES + encodedKeys.size() + Integer.BYTES + value.size());
		int offset = 0;
		serialized.setIntLE(offset, 1);
		offset += Integer.BYTES;
		serialized.setBytesFromBuf(offset, encodedKeys, 0, encodedKeys.size());
		offset += encodedKeys.size();
		serialized.setIntLE(offset, value.size());
		offset += Integer.BYTES;
		serialized.setBytesFromBuf(offset, value, 0, value.size());
		return serialized;
	}

	private static Fixture fixture(FuzzedDataProvider data, boolean requireBucket) {
		int fixedKeysCount = data.consumeInt(0, MAX_FIXED_KEYS);
		int variableKeysCount = data.consumeInt(requireBucket ? 1 : 0, MAX_VARIABLE_KEYS);
		IntArrayList fixedKeySizes = new IntArrayList(fixedKeysCount);
		ObjectArrayList<ColumnHashType> variableKeyHashes = new ObjectArrayList<>(variableKeysCount);
		Buf[] keys = new Buf[fixedKeysCount + variableKeysCount];

		for (int i = 0; i < fixedKeysCount; i++) {
			int size = data.consumeInt(1, MAX_FIXED_KEY_SIZE);
			fixedKeySizes.add(size);
			keys[i] = Buf.wrap(bytes(data, size));
		}
		for (int i = 0; i < variableKeysCount; i++) {
			ColumnHashType hash = data.pickValue(ColumnHashType.values());
			variableKeyHashes.add(hash);
			int size = hash == ColumnHashType.FIXEDINTEGER32
					? Integer.BYTES
					: data.consumeInt(0, MAX_VARIABLE_KEY_SIZE);
			keys[fixedKeysCount + i] = Buf.wrap(bytes(data, size));
		}

		boolean hasValue = data.consumeBoolean();
		ColumnSchema schema = ColumnSchema.of(fixedKeySizes, variableKeyHashes, hasValue);
		// A synthetic codec-only ColumnInstance has no native handle and must not be closed.
		ColumnInstance column = new ColumnInstance(null, schema);
		Buf value = hasValue ? Buf.wrap(bytes(data, data.consumeInt(0, MAX_VALUE_SIZE))) : null;
		return new Fixture(column, keys, value);
	}

	private static byte[] bytes(FuzzedDataProvider data, int size) {
		byte[] result = new byte[size];
		byte[] consumed = data.consumeBytes(size);
		System.arraycopy(consumed, 0, result, 0, consumed.length);
		return result;
	}

	private static void assertBufArrayEquals(Buf[] expected, Buf[] actual) {
		assertEquals(expected.length, actual.length);
		for (int i = 0; i < expected.length; i++) {
			assertBufEquals(expected[i], actual[i]);
		}
	}

	private static void assertBufEquals(Buf expected, Buf actual) {
		assertArrayEquals(expected.asArray(), actual.asArray());
	}

	private record Fixture(ColumnInstance column, Buf[] keys, Buf value) {
	}

	private record TranscodeAttempt(Buf encodedKeys, boolean accepted) {
	}

	private record DecodeAttempt(Buf[] keys, Buf value, boolean accepted) {
	}
}
