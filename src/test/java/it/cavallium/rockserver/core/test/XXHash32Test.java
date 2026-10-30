package it.cavallium.rockserver.core.test;

import it.cavallium.buffer.MemorySegmentBuf;
import it.cavallium.rockserver.core.common.ColumnHashType;
import it.cavallium.rockserver.core.impl.XXHash32;
import it.cavallium.buffer.Buf;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Arrays;
import java.util.Random;
import java.util.concurrent.ThreadLocalRandom;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

public class XXHash32Test {

	private static final net.jpountz.xxhash.XXHash32 REFERENCE = net.jpountz.xxhash.XXHashFactory.safeInstance()
			.hash32();
	private static final int MAX_LENGTH = 1100;

	@Test
	public void testMemorySegment() {
		var safeXxhash32 = REFERENCE;
		var myXxhash32 = XXHash32.getInstance();
		for (int runs = 0; runs < 3; runs++) {
			for (int len = 0; len < 600; len++) {
				byte[] bytes = new byte[len];
				ThreadLocalRandom.current().nextBytes(bytes);
				var hash = safeXxhash32.hash(bytes, 0, bytes.length, Integer.MIN_VALUE);
				var result = Buf.createZeroes(Integer.BYTES);
				myXxhash32.hash(Buf.wrap(bytes), 0, bytes.length, Integer.MIN_VALUE, result);
				var resultInt = result.getInt(0);
				Assertions.assertEquals(hash, resultInt);
			}
		}
	}

	@Test
	public void testBytes() {
		var myXxhash32 = XXHash32.getInstance();
		for (int runs = 0; runs < 3; runs++) {
			for (int len = 0; len < 600; len++) {
				byte[] bytes = new byte[len];
				ThreadLocalRandom.current().nextBytes(bytes);
				var hash = myXxhash32.hash(bytes, 0, bytes.length, Integer.MIN_VALUE);
				var result = Buf.createZeroes(Integer.BYTES);
				myXxhash32.hash(Buf.wrap(bytes), 0, bytes.length, Integer.MIN_VALUE, result);
				var resultInt = result.getInt(0);
				Assertions.assertEquals(hash, resultInt);
			}
		}
	}

	/**
	 * Every length up to {@value MAX_LENGTH} (all stripe and tail combinations), random offsets and seeds, through
	 * byte[], heap Buf, heap sub-list at an odd offset, heap and native MemorySegmentBuf, against lz4-java.
	 */
	@Test
	public void testAllBufKindsMatchReference() {
		var subject = XXHash32.getInstance();
		var random = new Random(0x5eed);
		try (var arena = Arena.ofConfined()) {
			for (int len = 0; len <= MAX_LENGTH; len++) {
				int seed = len % 3 == 0 ? 0 : random.nextInt();
				int prefix = 1 + random.nextInt(16);
				int suffix = random.nextInt(8);
				byte[] container = new byte[prefix + len + suffix];
				random.nextBytes(container);
				int expected = REFERENCE.hash(container, prefix, len, seed);
				String message = "len=" + len + ", prefix=" + prefix + ", seed=" + seed;

				Assertions.assertEquals(expected, subject.hash(container, prefix, len, seed), message);

				byte[] exact = Arrays.copyOfRange(container, prefix, prefix + len);
				Assertions.assertEquals(expected, subject.hash(Buf.wrap(exact), 0, len, seed), message);
				Assertions.assertEquals(expected, subject.hash(Buf.wrap(container), prefix, len, seed), message);

				Buf subList = Buf.wrap(container).subList(prefix, prefix + len);
				Assertions.assertEquals(expected, subject.hash(subList, 0, len, seed), message);
				// Slice of a slice: offsets relative to the sub-list
				if (len > 0) {
					int innerOff = random.nextInt(len);
					int innerLen = random.nextInt(len - innerOff + 1);
					Assertions.assertEquals(REFERENCE.hash(container, prefix + innerOff, innerLen, seed),
							subject.hash(subList, innerOff, innerLen, seed), message + ", innerOff=" + innerOff);
				}

				MemorySegment heapSegment = MemorySegment.ofArray(container).asSlice(prefix, len);
				Assertions.assertEquals(expected, subject.hash(new MemorySegmentBuf(heapSegment), 0, len, seed), message);

				MemorySegment nativeSegment = arena.allocate(len + prefix, 1).asSlice(prefix, len);
				MemorySegment.copy(container, prefix, nativeSegment, ValueLayout.JAVA_BYTE, 0, len);
				Assertions.assertEquals(expected, subject.hash(new MemorySegmentBuf(nativeSegment), 0, len, seed), message);

				Buf result = Buf.createZeroes(2 * Integer.BYTES);
				result.setInt(Integer.BYTES, 0x5a17c0de);
				subject.hash(subList, 0, len, seed, result);
				Assertions.assertEquals(expected, result.getInt(0), message);
				Assertions.assertEquals(0x5a17c0de, result.getInt(Integer.BYTES), message);
			}
		}
	}

	/**
	 * The column hash types persist these bytes inside physical keys: XXHASH32 is the big-endian hash, XXHASH8 its
	 * low byte, both with seed 0.
	 */
	@Test
	public void testColumnHashTypes() {
		var random = new Random(0xc01);
		int[] edgeLengths = {0, 1, 3, 4, 5, 15, 16, 17, 31, 32, 33, 63, 64, 65, 120, 255, 256, 257, 1024};
		for (int len : edgeLengths) {
			byte[] container = new byte[len + 3];
			random.nextBytes(container);
			int expected = REFERENCE.hash(container, 3, len, 0);
			for (Buf input : new Buf[] {Buf.wrap(container).subList(3, 3 + len),
					new MemorySegmentBuf(MemorySegment.ofArray(container).asSlice(3, len))}) {
				Buf hash32 = Buf.createZeroes(Integer.BYTES);
				ColumnHashType.XXHASH32.hash(input, hash32);
				Assertions.assertEquals(expected, hash32.getInt(0), "len=" + len);
				Buf hash8 = Buf.createZeroes(Byte.BYTES);
				ColumnHashType.XXHASH8.hash(input, hash8);
				Assertions.assertEquals((byte) expected, hash8.getByte(0), "len=" + len);
			}
		}
	}

	@Test
	public void testOutOfBounds() {
		var subject = XXHash32.getInstance();
		Buf subList = Buf.wrap(new byte[32]).subList(4, 20);
		Assertions.assertThrows(IndexOutOfBoundsException.class, () -> subject.hash(subList, 0, 17, 0));
		Assertions.assertThrows(IndexOutOfBoundsException.class, () -> subject.hash(subList, -1, 4, 0));
		Assertions.assertThrows(IndexOutOfBoundsException.class, () -> subject.hash(new byte[8], 5, 4, 0));
	}
}
