package it.cavallium.rockserver.core.impl.test;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import com.google.protobuf.ByteString;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.impl.Bucket;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;

import java.util.*;

import static org.junit.jupiter.api.Assertions.*;

class BucketFuzzTest {

	private static final int MAX_VARIABLE_KEYS = 4;
	private static final int MAX_KEY_SIZE = 512;
	private static final int MAX_VALUE_SIZE = 2_048;
	private static final int MAX_OPERATIONS = 64;
	private static final int MAX_ELEMENTS = 16;

	@FuzzTest(maxDuration = "30s")
	void mutationsStayEquivalentToAnOrderedMapAcrossSerialization(FuzzedDataProvider data) {
		Fixture fixture = fixture(data);
		Bucket bucket = new Bucket(fixture.column);
		LinkedHashMap<ModelKey, ByteString> model = new LinkedHashMap<>();
		int operations = data.consumeInt(1, MAX_OPERATIONS);

		for (int operationIndex = 0; operationIndex < operations; operationIndex++) {
			Buf[] keys = chooseKeys(data, fixture.hashes, model);
			ModelKey modelKey = modelKey(keys);
			switch (data.consumeInt(0, 2)) {
				case 0 -> {
					Buf value = fixture.hasValue
							? Buf.wrap(bytes(data, data.consumeInt(0, MAX_VALUE_SIZE)))
							: (data.consumeBoolean() ? null : Buf.create());
					ByteString expectedPrevious = model.put(modelKey, byteString(value));
					assertValueEquals(expectedPrevious, bucket.addElement(keys, value));
				}
				case 1 -> assertValueEquals(model.remove(modelKey), bucket.removeElement(keys));
				case 2 -> assertValueEquals(model.get(modelKey), bucket.getElement(keys));
				default -> throw new AssertionError();
			}

			if ((operationIndex & 3) == 0 || data.consumeBoolean()) {
				assertBucketMatches(bucket, model);
			}
		}

		assertBucketMatches(bucket, model);
		Buf serialized = bucket.toSegment();
		assertEquals(model.size(), Bucket.readElementCount(serialized));
		Bucket decoded = new Bucket(fixture.column, serialized);
		assertBucketMatches(decoded, model);
		assertArrayEquals(serialized.asArray(), decoded.toSegment().asArray());
	}

	@FuzzTest(maxDuration = "30s")
	void everyNonEmptyCanonicalTruncationIsRejected(FuzzedDataProvider data) {
		Fixture fixture = fixture(data);
		Bucket bucket = new Bucket(fixture.column);
		int elements = data.consumeInt(1, MAX_ELEMENTS);
		for (int i = 0; i < elements; i++) {
			Buf[] keys = keys(data, fixture.hashes);
			Buf value = fixture.hasValue
					? Buf.wrap(bytes(data, data.consumeInt(0, MAX_VALUE_SIZE)))
					: null;
			bucket.addElement(keys, value);
		}

		Buf serialized = bucket.toSegment();
		int truncatedSize = data.consumeInt(1, serialized.size() - 1);
		Buf truncated = serialized.copyOfRange(0, truncatedSize);
		Bucket decoded;
		try {
			decoded = new Bucket(fixture.column, truncated);
		} catch (IndexOutOfBoundsException | IllegalArgumentException | AssertionError expected) {
			// A non-empty canonical encoding cannot lose a trailing byte and remain complete.
			return;
		}
		fail("Accepted bucket truncation from " + serialized.size() + " to " + truncatedSize
				+ " bytes as " + decoded.getElements().size() + " elements");
	}

	private static void assertBucketMatches(Bucket bucket, LinkedHashMap<ModelKey, ByteString> model) {
		assertEquals(model.size(), bucket.getElements().size());
		var expectedIterator = model.entrySet().iterator();
		for (Map.Entry<Buf[], Buf> actual : bucket.getElements()) {
			Map.Entry<ModelKey, ByteString> expected = expectedIterator.next();
			assertEquals(expected.getKey(), modelKey(actual.getKey()));
			assertValueEquals(expected.getValue(), actual.getValue());
			assertValueEquals(expected.getValue(), bucket.getElement(actual.getKey()));
		}
	}

	private static Buf[] chooseKeys(FuzzedDataProvider data,
	                                ColumnHashType[] hashes,
	                                LinkedHashMap<ModelKey, ByteString> model) {
		if (!model.isEmpty() && data.consumeBoolean()) {
			int selected = data.consumeInt(0, model.size() - 1);
			ModelKey key = new ArrayList<>(model.keySet()).get(selected);
			return key.keys.stream().map(bytes -> Buf.wrap(bytes.toByteArray())).toArray(Buf[]::new);
		}
		return keys(data, hashes);
	}

	private static Buf[] keys(FuzzedDataProvider data, ColumnHashType[] hashes) {
		Buf[] keys = new Buf[hashes.length];
		for (int i = 0; i < hashes.length; i++) {
			int size = hashes[i] == ColumnHashType.FIXEDINTEGER32
					? Integer.BYTES
					: data.consumeInt(0, MAX_KEY_SIZE);
			keys[i] = Buf.wrap(bytes(data, size));
		}
		return keys;
	}

	private static ModelKey modelKey(Buf[] keys) {
		return new ModelKey(Arrays.stream(keys)
				.map(key -> ByteString.copyFrom(key.asArray()))
				.toList());
	}

	private static ByteString byteString(Buf value) {
		return value != null ? ByteString.copyFrom(value.asArray()) : ByteString.EMPTY;
	}

	private static void assertValueEquals(ByteString expected, Buf actual) {
		if (expected == null) {
			assertNull(actual);
		} else {
			assertEquals(expected, ByteString.copyFrom(actual.asArray()));
		}
	}

	private static Fixture fixture(FuzzedDataProvider data) {
		int variableKeys = data.consumeInt(1, MAX_VARIABLE_KEYS);
		ObjectArrayList<ColumnHashType> hashTypes = new ObjectArrayList<>(variableKeys);
		ColumnHashType[] hashes = new ColumnHashType[variableKeys];
		for (int i = 0; i < variableKeys; i++) {
			hashes[i] = data.pickValue(ColumnHashType.values());
			hashTypes.add(hashes[i]);
		}
		boolean hasValue = data.consumeBoolean();
		ColumnSchema schema = ColumnSchema.of(new IntArrayList(), hashTypes, hasValue);
		// This synthetic codec-only instance has no native handle and must not be closed.
		return new Fixture(new ColumnInstance(null, schema), hashes, hasValue);
	}

	private static byte[] bytes(FuzzedDataProvider data, int size) {
		byte[] result = new byte[size];
		byte[] consumed = data.consumeBytes(size);
		System.arraycopy(consumed, 0, result, 0, consumed.length);
		return result;
	}

	private record Fixture(ColumnInstance column, ColumnHashType[] hashes, boolean hasValue) {
	}

	private record ModelKey(List<ByteString> keys) {
	}
}
