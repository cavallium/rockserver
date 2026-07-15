package it.cavallium.rockserver.core.impl.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;

import it.cavallium.rockserver.core.config.DataSize;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import org.junit.jupiter.params.provider.ValueSource;

class DataSizeTest {

	@ParameterizedTest
	@MethodSource("validSizes")
	void parsesSupportedRepresentations(String text, long expectedBytes) {
		assertEquals(expectedBytes, new DataSize(text).longValue());
	}

	private static Stream<Arguments> validSizes() {
		return Stream.of(
				Arguments.of("", 0L),
				Arguments.of("-0", 0L),
				Arguments.of("+0", 0L),
				Arguments.of("1", 1L),
				Arguments.of("-1", -1L),
				Arguments.of("1B", 1L),
				Arguments.of("1KB", 1_000L),
				Arguments.of("2MiB", 2L * 1024 * 1024),
				Arguments.of(" 1_024 KiB ", 1024L * 1024),
				Arguments.of("9EB", 9_000_000_000_000_000_000L),
				Arguments.of("inf", Long.MAX_VALUE),
				Arguments.of("infinite", Long.MAX_VALUE),
				Arguments.of("∞b", Long.MAX_VALUE)
		);
	}

	@ParameterizedTest
	@ValueSource(strings = {
			"+1", "--1", "1-", "K", "1.5KB", "1bit", "1b", "1K", "1Ki", "1Kx", "1KiX",
			"1XB", "1KBB", "9223372036854775808", "-9223372036854775809", "10EB"
	})
	void rejectsMalformedOrOverflowingRepresentations(String text) {
		assertThrows(IllegalArgumentException.class, () -> new DataSize(text));
	}

	@Test
	void constantsMatchTheirNamesAndParser() {
		assertEquals(new DataSize("1KB"), DataSize.KB);
		assertEquals(new DataSize("1MB"), DataSize.MB);
		assertEquals(new DataSize("1GB"), DataSize.GB);
		assertEquals(new DataSize("1TB"), DataSize.TB);
		assertEquals(new DataSize("1PB"), DataSize.PB);
		assertEquals(new DataSize("1EB"), DataSize.EB);
		assertEquals(new DataSize("1KiB"), DataSize.KIB);
		assertEquals(new DataSize("1MiB"), DataSize.MIB);
		assertEquals(new DataSize("1GiB"), DataSize.GIB);
		assertEquals(new DataSize("1TiB"), DataSize.TIB);
		assertEquals(new DataSize("1PiB"), DataSize.PIB);
		assertEquals(new DataSize("1EiB"), DataSize.EIB);
	}

	@Test
	void numberConversionsClampBothIntegerBounds() {
		assertEquals(Integer.MAX_VALUE, new DataSize(Long.MAX_VALUE).intValue());
		assertEquals(Integer.MIN_VALUE, new DataSize(Long.MIN_VALUE).intValue());
		assertEquals(123, new DataSize(123).intValue());
		assertEquals(123L, new DataSize(123).longValue());
		assertEquals(123.0f, new DataSize(123).floatValue());
		assertEquals(123.0d, new DataSize(123).doubleValue());
	}

	@Test
	void comparisonEqualityAndNullableHelpersAreValueBased() {
		var one = new DataSize(1);
		var anotherOne = new DataSize("1B");
		var two = new DataSize(2);

		assertEquals(one, anotherOne);
		assertEquals(one.hashCode(), anotherOne.hashCode());
		assertNotEquals(one, two);
		assertNotEquals(one, 1L);
		assertEquals(-1, one.compareTo(two));
		assertEquals(1, two.compareTo(one));
		assertEquals(0, one.compareTo(anotherOne));
		assertNull(DataSize.get(null));
		assertEquals(1L, DataSize.get(one));
		assertEquals(2L, DataSize.getOrElse(null, two));
		assertEquals(1L, DataSize.getOrElse(one, two));
	}

	@Test
	void formattingChoosesExactUnitsWhenPossible() {
		assertEquals("0B", DataSize.ZERO.toString());
		assertEquals("1kB", DataSize.KB.toString());
		assertEquals("1KiB", DataSize.KIB.toString());
		assertEquals("1536B", new DataSize(1536).toString());
		assertEquals("1KiB", new DataSize(1536).toString(false));
		assertEquals("-1KiB", new DataSize(-1024).toString());
	}
}
