package it.cavallium.rockserver.distributed;

import static java.nio.charset.StandardCharsets.UTF_8;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Delta;
import it.cavallium.rockserver.core.common.RocksDBException.RocksDBErrorType;
import it.cavallium.rockserver.distributed.ApplyResult.Failed;
import it.cavallium.rockserver.distributed.ApplyResult.Ok;
import java.io.ByteArrayOutputStream;
import java.lang.management.ManagementFactory;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.stream.Stream;
import org.junit.jupiter.api.Named;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.EnumSource;
import org.junit.jupiter.params.provider.MethodSource;

class ResultCodecTest {

	static Stream<Named<ApplyResult>> results() {
		return Stream.of(
				Named.of("null", ApplyResult.OK_NULL),
				Named.of("buf", new Ok(buf("value"))),
				Named.of("empty buf", new Ok(Buf.wrap())),
				Named.of("true", new Ok(true)),
				Named.of("false", new Ok(false)),
				Named.of("zero", new Ok(0L)),
				Named.of("min long", new Ok(Long.MIN_VALUE)),
				Named.of("max long", new Ok(Long.MAX_VALUE)),
				Named.of("delta", new Ok(new Delta<>(buf("old"), buf("new")))),
				Named.of("delta, both null", new Ok(new Delta<Buf>(null, null))),
				Named.of("delta, created", new Ok(new Delta<>(null, buf("new")))),
				Named.of("delta, deleted", new Ok(new Delta<>(buf("old"), null))),
				Named.of("delta, empty values", new Ok(new Delta<>(Buf.wrap(), Buf.wrap()))),
				Named.of("empty list", new Ok(List.of())),
				Named.of("list with nulls", new Ok(Arrays.asList(buf("a"), null, Buf.wrap(), null))),
				Named.of("mixed nested list", new Ok(Arrays.asList(
						true,
						7L,
						new Delta<>(null, buf("x")),
						List.of(List.of(), Collections.singletonList(null)),
						buf("y")))),
				Named.of("deepest list", new Ok(nestedLists(ResultCodec.MAX_LIST_DEPTH))),
				Named.of("large list", new Ok(largeList())),
				Named.of("failure", new Failed(RocksDBErrorType.COLUMN_NOT_FOUND, "Column 7 not found")),
				Named.of("failure, empty message", new Failed(RocksDBErrorType.COLUMN_EXISTS, "")),
				Named.of("failure, unicode message", new Failed(RocksDBErrorType.INTERNAL_ERROR, "è 列 🚀")));
	}

	@ParameterizedTest
	@MethodSource("results")
	void roundTripsEveryResult(ApplyResult result) {
		byte[] encoded = ResultCodec.encode(result);
		ApplyResult decoded = ResultCodec.decode(encoded);
		assertEquals(result, decoded);
		assertArrayEquals(encoded, ResultCodec.encode(decoded), "re-encoding a decoded result changed its bytes");
	}

	@ParameterizedTest
	@MethodSource("results")
	void encodingIsDeterministic(ApplyResult result) {
		assertArrayEquals(ResultCodec.encode(result), ResultCodec.encode(result));
	}

	@ParameterizedTest
	@EnumSource(RocksDBErrorType.class)
	void roundTripsEveryErrorType(RocksDBErrorType type) {
		var failed = new Failed(type, "message for " + type);
		assertEquals(failed, ResultCodec.decode(ResultCodec.encode(failed)));
	}

	@Test
	void encodesFailureMessagesThatAreNotValidUnicode() {
		var failed = new Failed(RocksDBErrorType.INTERNAL_ERROR, "bad \uD800 surrogate \uDC00");
		assertEquals(new Failed(RocksDBErrorType.INTERNAL_ERROR, "bad ? surrogate ?"),
				ResultCodec.decode(ResultCodec.encode(failed)));
	}

	@Test
	void rejectsUnsupportedValueTypesAtEncode() {
		List<Object> unsupported = List.of(
				1,
				"string",
				new byte[] {1},
				Map.of(),
				new Delta<>("previous", null),
				new Delta<>(null, 3L),
				List.of(buf("ok"), 1),
				List.of(List.of(Map.of())));
		for (Object value : unsupported) {
			assertThrows(IllegalArgumentException.class, () -> ResultCodec.encode(new Ok(value)), value::toString);
		}
	}

	@Test
	void rejectsListsNestedTooDeeplyAtEncode() {
		var tooDeep = new Ok(nestedLists(ResultCodec.MAX_LIST_DEPTH + 1));
		assertThrows(IllegalArgumentException.class, () -> ResultCodec.encode(tooDeep));
	}

	@Test
	void rejectsDeepNestingAtDecodeWithoutOverflowingTheStack() {
		assertRejected(nestedNullLists(200_000), "200000 nested lists");
		assertRejected(nestedNullLists(ResultCodec.MAX_LIST_DEPTH + 1), "one level too deep");
		assertEquals(new Ok(nestedNullListsValue(ResultCodec.MAX_LIST_DEPTH)),
				ResultCodec.decode(nestedNullLists(ResultCodec.MAX_LIST_DEPTH)));
		assertEquals(ApplyResult.OK_NULL, ResultCodec.decode(nestedNullLists(0)));
	}

	@Test
	void rejectsNestedOversizedListsWithoutReservingMemoryForThem() {
		// Every nested list claims as many elements as there are bytes left, then the first element is invalid
		int size = 1 << 20;
		var buffer = ByteBuffer.allocate(size);
		buffer.put(ResultCodec.encode(ApplyResult.OK_NULL)[0]);
		byte listType = ResultCodec.encode(new Ok(List.of()))[1];
		for (int depth = 0; depth < ResultCodec.MAX_LIST_DEPTH; depth++) {
			buffer.put(listType);
			buffer.putInt(buffer.remaining() - Integer.BYTES);
		}
		while (buffer.hasRemaining()) {
			buffer.put((byte) -1);
		}
		byte[] malformed = buffer.array();
		long allocated = allocatedBytes(() -> assertRejected(malformed, "nested lists claiming the whole input"));
		assertTrue(allocated < size, "Rejecting " + size + " bytes allocated " + allocated + " bytes");
	}

	static long allocatedBytes(Runnable action) {
		var threads = (com.sun.management.ThreadMXBean) ManagementFactory.getThreadMXBean();
		long before = threads.getCurrentThreadAllocatedBytes();
		action.run();
		return threads.getCurrentThreadAllocatedBytes() - before;
	}

	/** The encoding of {@code depth} lists nested inside each other around a null, built from a one-level encoding. */
	private static byte[] nestedNullLists(int depth) {
		// Ok tag, then the list header (type, size 1), then the null value
		byte[] oneLevel = ResultCodec.encode(new Ok(Collections.singletonList(null)));
		assertEquals(7, oneLevel.length);
		var bytes = new ByteArrayOutputStream();
		bytes.write(oneLevel[0]);
		for (int i = 0; i < depth; i++) {
			bytes.write(oneLevel, 1, 5);
		}
		bytes.write(oneLevel[6]);
		return bytes.toByteArray();
	}

	@ParameterizedTest
	@MethodSource("results")
	void rejectsEveryTruncation(ApplyResult result) {
		byte[] encoded = ResultCodec.encode(result);
		if (encoded.length > 4096) {
			// Every prefix of the large sample would take quadratic time; its structure is covered by the others.
			encoded = Arrays.copyOf(encoded, 4096);
		}
		for (int length = 0; length < encoded.length; length++) {
			assertRejected(Arrays.copyOf(encoded, length), "truncated to " + length + " bytes");
		}
	}

	@ParameterizedTest
	@MethodSource("results")
	void rejectsTrailingBytes(ApplyResult result) {
		byte[] encoded = ResultCodec.encode(result);
		assertRejected(Arrays.copyOf(encoded, encoded.length + 1), "one trailing byte");
	}

	@Test
	void rejectsUnknownTags() {
		byte[] encoded = ResultCodec.encode(ApplyResult.OK_NULL);
		for (byte tag : new byte[] {0, 3, -1}) {
			byte[] copy = encoded.clone();
			copy[0] = tag;
			assertRejected(copy, "result tag " + tag);
		}
		for (byte type : new byte[] {7, 100, -1}) {
			byte[] copy = encoded.clone();
			copy[1] = type;
			assertRejected(copy, "value type " + type);
		}
	}

	@Test
	void rejectsUnknownErrorType() {
		// tag, name length (4), "COLUMN_EXISTS", message length (4)
		byte[] encoded = ResultCodec.encode(new Failed(RocksDBErrorType.COLUMN_EXISTS, ""));
		encoded[5 + "COLUMN_EXISTS".length() - 1] = 'Z';
		assertRejected(encoded, "unknown error type name");
	}

	@Test
	void rejectsNegativeAndOversizedLengths() {
		// tag, value type, length (4)
		byte[] bufValue = ResultCodec.encode(new Ok(Buf.wrap()));
		byte[] list = ResultCodec.encode(new Ok(List.of()));
		for (int length : new int[] {-1, 1, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
			assertRejected(withInt(bufValue, 2, length), "buf length " + length);
			assertRejected(withInt(list, 2, length), "list size " + length);
		}
		// tag, value type, previous length (4), current length (4)
		byte[] delta = ResultCodec.encode(new Ok(new Delta<Buf>(null, null)));
		for (int length : new int[] {-2, 1, Integer.MAX_VALUE, Integer.MIN_VALUE}) {
			assertRejected(withInt(delta, 2, length), "delta previous length " + length);
			assertRejected(withInt(delta, 6, length), "delta current length " + length);
		}
	}

	@Test
	void corruptedInputIsDecodedOrRejectedNeverCrashes() {
		var random = new Random(0xC0DEC);
		var samples = results().map(Named::getPayload).filter(r -> ResultCodec.encode(r).length < 4096).toList();
		for (ApplyResult result : samples) {
			byte[] encoded = ResultCodec.encode(result);
			for (int attempt = 0; attempt < 300; attempt++) {
				byte[] corrupted = encoded.clone();
				int changes = 1 + random.nextInt(3);
				for (int i = 0; i < changes; i++) {
					corrupted[random.nextInt(corrupted.length)] = (byte) random.nextInt(256);
				}
				try {
					ResultCodec.decode(corrupted);
				} catch (IllegalArgumentException expected) {
					// rejected cleanly
				} catch (Throwable unexpected) {
					fail("Corrupted " + result + " threw " + unexpected, unexpected);
				}
			}
		}
	}

	private static void assertRejected(byte[] data, String description) {
		try {
			ApplyResult decoded = ResultCodec.decode(data);
			fail("Decoded malformed input (" + description + ") as " + decoded);
		} catch (IllegalArgumentException expected) {
			// rejected cleanly
		} catch (Throwable unexpected) {
			fail("Malformed input (" + description + ") threw " + unexpected, unexpected);
		}
	}

	private static byte[] withInt(byte[] data, int offset, int value) {
		byte[] copy = data.clone();
		ByteBuffer.wrap(copy).putInt(offset, value);
		return copy;
	}

	private static Object nestedNullListsValue(int depth) {
		Object value = null;
		for (int i = 0; i < depth; i++) {
			value = Collections.singletonList(value);
		}
		return value;
	}

	/** {@code depth} lists nested inside each other; the innermost one holds a buffer. */
	private static Object nestedLists(int depth) {
		Object value = buf("leaf");
		for (int i = 0; i < depth; i++) {
			value = List.of(value);
		}
		return value;
	}

	private static List<Object> largeList() {
		var list = new ArrayList<>();
		for (int i = 0; i < 20_000; i++) {
			list.add(switch (i % 4) {
				case 0 -> buf("value-" + i);
				case 1 -> null;
				case 2 -> (long) i;
				default -> new Delta<>(buf("p" + i), null);
			});
		}
		return list;
	}

	private static Buf buf(String value) {
		return Buf.wrap(value.getBytes(UTF_8));
	}
}
