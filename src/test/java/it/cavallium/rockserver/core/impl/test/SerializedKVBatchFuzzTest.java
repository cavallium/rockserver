package it.cavallium.rockserver.core.impl.test;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.KV;
import it.cavallium.rockserver.core.common.Keys;
import it.cavallium.rockserver.core.common.SerializedKVBatch;
import it.cavallium.rockserver.core.common.SerializedKVBatch.SerializedKVBatchRef;

import java.io.ByteArrayOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import static org.junit.jupiter.api.Assertions.*;

class SerializedKVBatchFuzzTest {

	private static final int MAX_BATCH_SIZE = 32;
	private static final int MAX_KEYS = 8;
	private static final int MAX_KEY_SIZE = 512;
	private static final int MAX_VALUE_SIZE = 2_048;
	private static final int MAX_ARBITRARY_INPUT_SIZE = 64 * 1_024;

	@FuzzTest(maxDuration = "30s")
	void canonicalBatchesRoundTripAndRemainLazy(FuzzedDataProvider data) {
		List<KV> expected = batch(data);
		Buf encoded = encode(expected);
		SerializedKVBatch serialized = new SerializedKVBatchRef(encoded);

		assertEquals(expected, serialized.decode().toList());
		assertEquals(encoded, encode(serialized.decode().toList()));

		int prefixLength = data.consumeInt(0, expected.size());
		assertEquals(expected.subList(0, prefixLength), serialized.decode().limit(prefixLength).toList());
	}

	@FuzzTest(maxDuration = "30s")
	void everyCanonicalTruncationIsRejected(FuzzedDataProvider data) {
		Buf encoded = encode(batch(data));
		int truncatedSize = data.consumeInt(0, encoded.size() - 1);
		SerializedKVBatch truncated = new SerializedKVBatchRef(
				Buf.wrap(encoded.asArray(), 0, truncatedSize));

		try {
			List<KV> decoded = truncated.decode().toList();
			fail("Decoder accepted a canonical batch truncated from " + encoded.size() + " to " + truncatedSize
					+ " and returned " + decoded.size() + " records");
		} catch (IndexOutOfBoundsException | IllegalArgumentException | NegativeArraySizeException expected) {
			// Malformed length-prefixed input is outside the SerializedKVBatch contract.
		}
	}

	@FuzzTest(maxDuration = "30s")
	void boundedArbitraryInputEitherRejectsOrDecodesDeterministically(byte[] input) {
		byte[] boundedInput = input.length <= MAX_ARBITRARY_INPUT_SIZE
				? input
				: Arrays.copyOf(input, MAX_ARBITRARY_INPUT_SIZE);
		SerializedKVBatch serialized = new SerializedKVBatchRef(Buf.wrap(boundedInput));

		try {
			List<KV> first = serialized.decode().toList();
			List<KV> second = serialized.decode().toList();
			assertEquals(first, second);
			assertTrue(first.size() <= boundedInput.length / 5, "decoded more records than the input can encode");
			assertEquals(first, new SerializedKVBatchRef(encode(first)).decode().toList());
		} catch (IndexOutOfBoundsException | IllegalArgumentException | NegativeArraySizeException expected) {
			// Rejection is valid; unchecked errors outside this set remain fuzz findings.
		}
	}

	private static List<KV> batch(FuzzedDataProvider data) {
		int batchSize = data.consumeInt(0, MAX_BATCH_SIZE);
		List<KV> batch = new ArrayList<>(batchSize);
		for (int i = 0; i < batchSize; i++) {
			int keysCount = data.consumeInt(0, MAX_KEYS);
			Buf[] keys = new Buf[keysCount];
			for (int j = 0; j < keysCount; j++) {
				keys[j] = Buf.wrap(bytes(data, data.consumeInt(0, MAX_KEY_SIZE)));
			}
			Buf value = Buf.wrap(bytes(data, data.consumeInt(0, MAX_VALUE_SIZE)));
			batch.add(new KV(new Keys(keys), value));
		}
		return List.copyOf(batch);
	}

	static Buf encode(List<KV> batch) {
		ByteArrayOutputStream output = new ByteArrayOutputStream();
		writeIntLE(output, batch.size());
		for (KV kv : batch) {
			Buf[] keys = kv.keys().keys();
			if (keys.length > 0xFF) {
				throw new IllegalArgumentException("Too many keys: " + keys.length);
			}
			output.write(keys.length);
			for (Buf key : keys) {
				writeIntLE(output, key.size());
				output.writeBytes(key.asArray());
			}
			Buf value = kv.value();
			int valueSize = value != null ? value.size() : 0;
			writeIntLE(output, valueSize);
			if (value != null) {
				output.writeBytes(value.asArray());
			}
		}
		return Buf.wrap(output.toByteArray());
	}

	private static byte[] bytes(FuzzedDataProvider data, int size) {
		byte[] result = new byte[size];
		byte[] consumed = data.consumeBytes(size);
		System.arraycopy(consumed, 0, result, 0, consumed.length);
		return result;
	}

	private static void writeIntLE(ByteArrayOutputStream output, int value) {
		for (int shift = 0; shift < Integer.SIZE; shift += Byte.SIZE) {
			output.write(value >>> shift);
		}
	}
}
