package it.cavallium.rockserver.core.impl.test;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import it.cavallium.rockserver.core.config.DataSize;

import java.math.BigInteger;

import static org.junit.jupiter.api.Assertions.assertEquals;

class DataSizeFuzzTest {

	private static final String SCALE_CHARACTERS = "KMGTPEZY";
	private static final BigInteger LONG_MIN = BigInteger.valueOf(Long.MIN_VALUE);
	private static final BigInteger LONG_MAX = BigInteger.valueOf(Long.MAX_VALUE);

	@FuzzTest(maxDuration = "30s")
	void preciseFormattingRoundTripsEveryLong(FuzzedDataProvider data) {
		long value = data.consumeLong();
		long otherValue = data.consumeLong();
		DataSize size = new DataSize(value);
		DataSize reparsed = new DataSize(size.toString(true));

		assertEquals(value, reparsed.longValue());
		assertEquals(size, reparsed);
		assertEquals(Long.hashCode(value), size.hashCode());
		assertEquals(Long.compare(value, otherValue), size.compareTo(new DataSize(otherValue)));
		assertEquals(clampToInt(value), size.intValue());
	}

	@FuzzTest(maxDuration = "30s")
	void generatedUnitsMatchBigIntegerModel(FuzzedDataProvider data) {
		int exponent = data.consumeInt(0, SCALE_CHARACTERS.length());
		boolean binary = data.consumeBoolean();
		boolean lowerCase = data.consumeBoolean();
		BigInteger scale = BigInteger.valueOf(binary ? 1_024L : 1_000L).pow(exponent);
		long minimum = LONG_MIN.divide(scale).longValueExact();
		long maximum = LONG_MAX.divide(scale).longValueExact();
		long number = data.consumeLong(minimum, maximum);
		String unit = unit(exponent, binary, lowerCase);
		String text = decorate(number, unit, data);

		long expected = BigInteger.valueOf(number).multiply(scale).longValueExact();
		DataSize parsed = new DataSize(text);
		assertEquals(expected, parsed.longValue());
		assertEquals(expected, new DataSize(parsed.toString(true)).longValue());
	}

	@FuzzTest(maxDuration = "30s")
	void arbitraryTextEitherRejectsCleanlyOrCanonicalizes(FuzzedDataProvider data) {
		String text = data.consumeString(256);
		DataSize parsed;
		try {
			parsed = new DataSize(text);
		} catch (IllegalArgumentException expected) {
			// Malformed text and values outside the signed 64-bit range are rejected by contract.
			return;
		}
		DataSize canonical = new DataSize(parsed.toString(true));
		assertEquals(parsed, canonical);
		assertEquals(parsed.longValue(), canonical.longValue());
	}

	private static int clampToInt(long value) {
		if (value >= Integer.MAX_VALUE) {
			return Integer.MAX_VALUE;
		}
		if (value <= Integer.MIN_VALUE) {
			return Integer.MIN_VALUE;
		}
		return (int) value;
	}

	private static String unit(int exponent, boolean binary, boolean lowerCase) {
		if (exponent == 0) {
			return binary ? "" : "B";
		}
		char scale = SCALE_CHARACTERS.charAt(exponent - 1);
		if (lowerCase) {
			scale = Character.toLowerCase(scale);
		}
		return scale + (binary ? "iB" : "B");
	}

	private static String decorate(long number, String unit, FuzzedDataProvider data) {
		String digits = Long.toString(number);
		if (data.consumeBoolean() && digits.length() > 1) {
			int insertionPoint = data.consumeInt(1, digits.length() - 1);
			digits = digits.substring(0, insertionPoint) + '_' + digits.substring(insertionPoint);
		}
		String prefix = data.consumeBoolean() ? " \t" : "";
		String separator = data.consumeBoolean() ? " _ " : "";
		String suffix = data.consumeBoolean() ? "\n" : "";
		return prefix + digits + separator + unit + suffix;
	}
}
