package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import it.cavallium.buffer.Buf;
import it.cavallium.buffer.MemorySegmentBuf;
import it.cavallium.rockserver.core.impl.NativeStaging;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import org.junit.jupiter.api.Test;

class NativeStagingTest {

	private static final int[] EDGE_SIZES = {0, 1, 15, 16, 17, 31, 32, 33, 63, 64, 65, 512, 1536, 3072};

	@Test
	void stagedBufKeysAreExactContiguousCopies() {
		var random = new SplittableRandom(1);
		var expected = new ArrayList<byte[]>();
		var keys = new ArrayList<Buf>();
		for (int size : EDGE_SIZES) {
			byte[] bytes = randomBytes(random, size);
			expected.add(bytes);
			// Array-backed Buf with a non-zero offset into a larger array
			var padded = new byte[size + 7];
			System.arraycopy(bytes, 0, padded, 3, size);
			keys.add(Buf.wrap(padded, 3, 3 + size));
		}
		for (int i = 0; i < 300; i++) {
			byte[] bytes = randomBytes(random, 12 + random.nextInt(8));
			expected.add(bytes);
			keys.add(Buf.wrap(bytes));
		}
		try (var arena = Arena.ofConfined()) {
			var staged = NativeStaging.stageKeys(arena, keys);
			assertStaged(expected, staged);
		}
	}

	@Test
	void stagedMemorySegmentBufKeysAreCopied() {
		var random = new SplittableRandom(2);
		try (var source = Arena.ofConfined(); var arena = Arena.ofConfined()) {
			var expected = new ArrayList<byte[]>();
			var keys = new ArrayList<Buf>();
			for (int size : EDGE_SIZES) {
				byte[] bytes = randomBytes(random, size);
				expected.add(bytes);
				var segment = source.allocate(Math.max(1, size)).asSlice(0, size);
				MemorySegment.copy(bytes, 0, segment, ValueLayout.JAVA_BYTE, 0, size);
				keys.add(new MemorySegmentBuf(segment));
			}
			assertStaged(expected, NativeStaging.stageKeys(arena, keys));
		}
	}

	@Test
	void stagedByteArrayKeysAreExactContiguousCopies() {
		var random = new SplittableRandom(3);
		var expected = new ArrayList<byte[]>();
		for (int size : EDGE_SIZES) {
			expected.add(randomBytes(random, size));
		}
		for (int i = 0; i < 300; i++) {
			expected.add(randomBytes(random, 12 + random.nextInt(8)));
		}
		try (var arena = Arena.ofConfined()) {
			assertStaged(expected, NativeStaging.stageKeys(arena, expected.toArray(byte[][]::new)));
			assertEquals(0, NativeStaging.stageKeys(arena, new byte[0][]).length);
			assertEquals(0, NativeStaging.stageKeys(arena, List.of()).length);
		}
	}

	@Test
	void allocatedSlicesHaveExactCapacityAndDoNotOverlap() {
		var sizes = new long[EDGE_SIZES.length];
		for (int i = 0; i < EDGE_SIZES.length; i++) {
			sizes[i] = EDGE_SIZES[i];
		}
		try (var arena = Arena.ofConfined()) {
			var slices = NativeStaging.allocateSlices(arena, sizes);
			assertEquals(sizes.length, slices.length);
			for (int i = 0; i < slices.length; i++) {
				assertEquals(sizes[i], slices[i].byteSize());
				// multiGetByteBuffers uses the ByteBuffer capacity as the slot size
				assertEquals(sizes[i], slices[i].asByteBuffer().capacity());
				for (long j = 0; j < sizes[i]; j++) {
					assertEquals(0, slices[i].get(ValueLayout.JAVA_BYTE, j), "slices must start zeroed");
				}
			}
			for (int i = 0; i < slices.length; i++) {
				slices[i].fill((byte) (i + 1));
			}
			for (int i = 0; i < slices.length; i++) {
				for (long j = 0; j < sizes[i]; j++) {
					assertEquals((byte) (i + 1), slices[i].get(ValueLayout.JAVA_BYTE, j), "slice " + i + " was overwritten");
				}
			}
		}
	}

	@Test
	void contentEqualsMatchesTheScalarLoop() {
		var random = new SplittableRandom(4);
		var sizes = new ArrayList<Integer>();
		for (int size : EDGE_SIZES) {
			sizes.add(size);
		}
		for (int i = 0; i < 200; i++) {
			sizes.add(random.nextInt(4097));
		}
		try (var arena = Arena.ofConfined()) {
			for (int size : sizes) {
				byte[] bytes = randomBytes(random, size);
				var actual = arena.allocate(size + 5L, 1);
				MemorySegment.copy(bytes, 0, actual, ValueLayout.JAVA_BYTE, 0, size);
				var candidates = new ArrayList<Buf>();
				candidates.add(Buf.wrap(bytes.clone()));
				var padded = new byte[size + 9];
				System.arraycopy(bytes, 0, padded, 4, size);
				candidates.add(Buf.wrap(padded, 4, 4 + size));
				var segment = arena.allocate(Math.max(1, size)).asSlice(0, size);
				MemorySegment.copy(bytes, 0, segment, ValueLayout.JAVA_BYTE, 0, size);
				candidates.add(new MemorySegmentBuf(segment));
				if (size > 0) {
					for (int position : new int[] {0, size / 2, size - 1, random.nextInt(size)}) {
						var different = bytes.clone();
						different[position] ^= (byte) (1 + random.nextInt(255));
						candidates.add(Buf.wrap(different));
					}
					candidates.add(Buf.wrap(bytes, 0, size - 1));
				}
				candidates.add(Buf.wrap(Arrays.copyOf(bytes, size + 1)));
				for (var expected : candidates) {
					for (int length : new int[] {size, Math.max(0, size - 1), size + 1}) {
						assertEquals(scalarEquals(actual, expected, length),
								NativeStaging.contentEquals(actual, expected, length),
								"size " + size + " length " + length + " expected " + expected.size());
					}
					// A native value shorter than the requested length never matches
					var shorter = actual.asSlice(0, Math.max(0, size - 1));
					assertEquals(scalarEquals(shorter, expected, size),
							NativeStaging.contentEquals(shorter, expected, size));
				}
				assertTrue(NativeStaging.contentEquals(actual, Buf.wrap(bytes), size));
			}
		}
	}

	private static void assertStaged(List<byte[]> expected, MemorySegment[] staged) {
		assertEquals(expected.size(), staged.length);
		long previousEnd = -1L;
		for (int i = 0; i < staged.length; i++) {
			assertEquals(expected.get(i).length, staged[i].byteSize());
			assertEquals(expected.get(i).length, staged[i].asByteBuffer().capacity());
			assertTrue(staged[i].isNative());
			assertArrayEquals(expected.get(i), staged[i].toArray(ValueLayout.JAVA_BYTE));
			if (previousEnd >= 0L) {
				assertEquals(previousEnd, staged[i].address(), "slices must be contiguous in one block");
			}
			previousEnd = staged[i].address() + staged[i].byteSize();
		}
	}

	/**
	 * The byte loop that {@link NativeStaging#contentEquals} replaced.
	 */
	private static boolean scalarEquals(MemorySegment actual, Buf expected, int length) {
		if (length != expected.size() || actual.byteSize() < length) {
			return false;
		}
		for (int i = 0; i < length; i++) {
			if (actual.get(ValueLayout.JAVA_BYTE, i) != expected.getByte(i)) {
				return false;
			}
		}
		return true;
	}

	private static byte[] randomBytes(SplittableRandom random, int length) {
		var bytes = new byte[length];
		for (int i = 0; i < length; i++) {
			bytes[i] = (byte) random.nextInt(256);
		}
		return bytes;
	}
}
