package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.buffer.Buf;
import it.cavallium.buffer.MemorySegmentBuf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.common.ColumnSchema;
import it.cavallium.rockserver.core.common.RocksDBException;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import it.cavallium.rockserver.core.impl.XXHash32;
import it.unimi.dsi.fastutil.ints.IntArrayList;
import it.unimi.dsi.fastutil.ints.IntList;
import it.unimi.dsi.fastutil.objects.ObjectArrayList;
import it.unimi.dsi.fastutil.objects.ObjectList;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.util.SplittableRandom;
import net.jpountz.xxhash.XXHashFactory;
import org.junit.jupiter.api.Test;

/**
 * Composite keys are an on-disk format: {@link ColumnInstance#calculateKeyBytes(Buf[])} and
 * {@link ColumnInstance#calculateKey(Buf[])} must stay byte-identical to the frozen {@link LegacyColumnKeyBuilder}
 * for every schema shape, hash type and {@link Buf} kind.
 */
class ColumnKeyLayoutEquivalenceTest {

	private static final int[] EDGE_SIZES = {0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65};
	private static final ColumnHashType[] HASH_TYPES = ColumnHashType.values();

	private enum Kind {HEAP, SLICE, NATIVE}

	@Test
	void frozenXxHash32MatchesLz4() {
		var lz4 = XXHashFactory.safeInstance().hash32();
		var random = new SplittableRandom(1);
		assertEquals(0x02CC5D05, LegacyColumnKeyBuilder.xxHash32(new byte[0]));
		for (int len = 0; len < 300; len++) {
			byte[] data = bytes(random, len);
			assertEquals(lz4.hash(data, 0, len, 0), LegacyColumnKeyBuilder.xxHash32(data));
		}
	}

	@Test
	void xxHash32VariantsAgreeAtAnyOffset() {
		var xxHash = XXHash32.getInstance();
		var random = new SplittableRandom(2);
		for (int len = 0; len < 200; len++) {
			byte[] padded = bytes(random, len + 9);
			int off = random.nextInt(10);
			byte[] exact = java.util.Arrays.copyOfRange(padded, off, off + len);
			int expected = LegacyColumnKeyBuilder.xxHash32(exact);
			assertEquals(expected, xxHash.hash(padded, off, len, 0));
			assertEquals(expected, xxHash.hash(Buf.wrap(padded), off, len, 0));
			assertEquals(expected, xxHash.hash(nativeBuf(exact), 0, len, 0));
		}
	}

	@Test
	void hashIntoMatchesHash() {
		var random = new SplittableRandom(3);
		for (var type : HASH_TYPES) {
			for (int size : type == ColumnHashType.FIXEDINTEGER32 ? new int[] {4} : EDGE_SIZES) {
				for (var kind : Kind.values()) {
					var input = buf(kind, bytes(random, size), random);
					var expected = Buf.createZeroes(type.bytesSize());
					type.hash(input, expected);
					byte[] out = new byte[type.bytesSize() + 3];
					type.hashInto(input, out, 2);
					assertArrayEquals(expected.toByteArray(), java.util.Arrays.copyOfRange(out, 2, 2 + type.bytesSize()),
							type + " " + kind + " " + size);
				}
			}
		}
	}

	@Test
	void productionShapes() {
		var random = new SplittableRandom(4);
		var shapes = new ColumnSchema[] {
				ColumnSchema.of(IntList.of(8, 4), ObjectList.of(), true),
				ColumnSchema.of(IntList.of(8, 8, 3), ObjectList.of(), true),
				ColumnSchema.of(IntList.of(14, 16, 1, 8), ObjectList.of(), true),
				ColumnSchema.of(IntList.of(12), ObjectList.of(), true),
				ColumnSchema.of(IntList.of(4), ObjectList.of(ColumnHashType.XXHASH32, ColumnHashType.XXHASH32), true),
		};
		for (var schema : shapes) {
			for (int i = 0; i < 200; i++) {
				assertSameKey(schema, keys(schema, random, Kind.values()[i % 3]));
			}
		}
	}

	@Test
	void randomSchemas() {
		var random = new SplittableRandom(5);
		for (int iteration = 0; iteration < 20_000; iteration++) {
			var schema = randomSchema(random);
			assertSameKey(schema, keys(schema, random, null));
		}
	}

	@Test
	void mismatchesThrowTheLegacyErrors() {
		var random = new SplittableRandom(6);
		var single = ColumnSchema.of(IntList.of(12), ObjectList.of(), true);
		var multi = ColumnSchema.of(IntList.of(8, 4), ObjectList.of(), true);
		var tailed = ColumnSchema.of(IntList.of(4), ObjectList.of(ColumnHashType.FIXEDINTEGER32), true);
		for (var kind : Kind.values()) {
			assertSameError(single, new Buf[] {buf(kind, bytes(random, 11), random)}, RocksDBErrorType.RAW_KEY_LENGTH_MISMATCH);
			assertSameError(single, new Buf[0], RocksDBErrorType.KEYS_COUNT_MISMATCH);
			assertSameError(multi, new Buf[] {buf(kind, bytes(random, 8), random), buf(kind, bytes(random, 5), random)},
					RocksDBErrorType.KEY_LENGTH_MISMATCH);
			assertSameError(multi, new Buf[] {buf(kind, bytes(random, 8), random)}, RocksDBErrorType.KEYS_COUNT_MISMATCH);
			var badInteger = new Buf[] {buf(kind, bytes(random, 4), random), buf(kind, bytes(random, 3), random)};
			assertThrows(UnsupportedOperationException.class, () -> LegacyColumnKeyBuilder.calculateKey(tailed, badInteger));
			assertThrows(UnsupportedOperationException.class, () -> new ColumnInstance(null, tailed).calculateKeyBytes(badInteger));
		}
	}

	@Test
	void decodeKeysCopiesEveryBufKind() {
		var random = new SplittableRandom(7);
		var schema = ColumnSchema.of(IntList.of(14, 16, 1, 8), ObjectList.of(), true);
		var column = new ColumnInstance(null, schema);
		for (var kind : Kind.values()) {
			var keys = keys(schema, random, kind);
			byte[] key = column.calculateKeyBytes(keys);
			var decoded = column.decodeKeys(buf(kind, key, random), null);
			for (int i = 0; i < keys.length; i++) {
				assertArrayEquals(keys[i].toByteArray(), decoded[i].toByteArray());
			}
		}
	}

	private static void assertSameKey(ColumnSchema schema, Buf[] keys) {
		var column = new ColumnInstance(null, schema);
		byte[] expected;
		try {
			expected = LegacyColumnKeyBuilder.calculateKey(schema, keys).toByteArray();
		} catch (UnsupportedOperationException e) {
			assertThrows(UnsupportedOperationException.class, () -> column.calculateKeyBytes(keys));
			return;
		}
		byte[] actual = column.calculateKeyBytes(keys);
		assertArrayEquals(expected, actual, schema::toString);
		assertArrayEquals(expected, column.calculateKey(keys).toByteArray(), schema::toString);
		assertEquals(column.finalKeySizeBytes(), actual.length);
		if (keys.length > 0) {
			// The result must never alias an input array: write elision stores it as a map key
			assertNotSame(keys[0].getBackingByteArrayStrict(), actual);
		}
	}

	private static void assertSameError(ColumnSchema schema, Buf[] keys, RocksDBErrorType type) {
		var legacy = assertThrows(RocksDBException.class, () -> LegacyColumnKeyBuilder.calculateKey(schema, keys));
		var actual = assertThrows(RocksDBException.class, () -> new ColumnInstance(null, schema).calculateKeyBytes(keys));
		var viaBuf = assertThrows(RocksDBException.class, () -> new ColumnInstance(null, schema).calculateKey(keys));
		assertEquals(type, legacy.getErrorUniqueId());
		assertEquals(legacy.getErrorUniqueId(), actual.getErrorUniqueId());
		assertEquals(legacy.getMessage(), actual.getMessage());
		assertEquals(legacy.getErrorUniqueId(), viaBuf.getErrorUniqueId());
	}

	private static ColumnSchema randomSchema(SplittableRandom random) {
		var fixed = new IntArrayList();
		int fixedCount = random.nextInt(5);
		for (int i = 0; i < fixedCount; i++) {
			fixed.add(random.nextBoolean() ? 1 + random.nextInt(19) : EDGE_SIZES[1 + random.nextInt(EDGE_SIZES.length - 1)]);
		}
		var tails = new ObjectArrayList<ColumnHashType>();
		int tailCount = random.nextInt(4);
		for (int i = 0; i < tailCount; i++) {
			tails.add(HASH_TYPES[random.nextInt(HASH_TYPES.length)]);
		}
		return ColumnSchema.of(fixed, tails, random.nextBoolean());
	}

	private static Buf[] keys(ColumnSchema schema, SplittableRandom random, Kind onlyKind) {
		var keys = new Buf[schema.keysCount()];
		for (int i = 0; i < keys.length; i++) {
			int size;
			if (i < schema.fixedLengthKeysCount()) {
				size = schema.key(i);
			} else if (schema.variableTailKey(i) == ColumnHashType.FIXEDINTEGER32) {
				size = random.nextInt(20) == 0 ? 3 : Integer.BYTES;
			} else {
				size = random.nextBoolean() ? EDGE_SIZES[random.nextInt(EDGE_SIZES.length)] : random.nextInt(130);
			}
			var kind = onlyKind != null ? onlyKind : Kind.values()[random.nextInt(3)];
			keys[i] = buf(kind, bytes(random, size), random);
		}
		return keys;
	}

	private static Buf buf(Kind kind, byte[] data, SplittableRandom random) {
		return switch (kind) {
			case HEAP -> Buf.wrap(data);
			case SLICE -> {
				int head = 1 + random.nextInt(7);
				byte[] padded = bytes(random, data.length + head + random.nextInt(7));
				System.arraycopy(data, 0, padded, head, data.length);
				yield Buf.wrap(padded, head, head + data.length);
			}
			case NATIVE -> nativeBuf(data);
		};
	}

	private static Buf nativeBuf(byte[] data) {
		var segment = Arena.ofAuto().allocate(Math.max(1, data.length)).asSlice(0, data.length);
		segment.copyFrom(MemorySegment.ofArray(data));
		return new MemorySegmentBuf(segment);
	}

	private static byte[] bytes(SplittableRandom random, int size) {
		byte[] data = new byte[size];
		random.nextBytes(data);
		return data;
	}
}
