package it.cavallium.rockserver.core.impl.test;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.impl.XXHash32;
import net.jpountz.xxhash.XXHashFactory;

import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertEquals;

class XXHash32FuzzTest {

	private static final int MAX_INPUT_SIZE = 64 * 1_024;
	private static final net.jpountz.xxhash.XXHash32 REFERENCE = XXHashFactory.safeInstance().hash32();
	private static final XXHash32 SUBJECT = XXHash32.getInstance();

	@FuzzTest(maxDuration = "30s")
	void byteArrayAndBufImplementationsMatchReferenceAcrossSlices(FuzzedDataProvider data) {
		int seed = data.consumeInt();
		int offsetSelector = data.consumeInt();
		int lengthSelector = data.consumeInt();
		byte[] input = data.consumeBytes(MAX_INPUT_SIZE);
		int offset = Math.floorMod(offsetSelector, input.length + 1);
		int length = Math.floorMod(lengthSelector, input.length - offset + 1);

		int expected = REFERENCE.hash(input, offset, length, seed);
		assertEquals(expected, SUBJECT.hash(input, offset, length, seed));

		Buf result = Buf.createZeroes(2 * Integer.BYTES);
		result.setInt(Integer.BYTES, 0x5a17c0de);
		SUBJECT.hash(Buf.wrap(input), offset, length, seed, result);
		assertEquals(expected, result.getInt(0));
		assertEquals(0x5a17c0de, result.getInt(Integer.BYTES));

		byte[] copiedSlice = Arrays.copyOfRange(input, offset, offset + length);
		assertEquals(expected, SUBJECT.hash(copiedSlice, 0, copiedSlice.length, seed));
		Buf copiedResult = Buf.createZeroes(Integer.BYTES);
		SUBJECT.hash(Buf.wrap(copiedSlice), 0, copiedSlice.length, seed, copiedResult);
		assertEquals(expected, copiedResult.getInt(0));
	}
}
