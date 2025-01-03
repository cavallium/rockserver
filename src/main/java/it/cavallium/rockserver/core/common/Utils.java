package it.cavallium.rockserver.core.common;

import static it.cavallium.rockserver.core.impl.ColumnInstance.BIG_ENDIAN_BYTES;
import static java.lang.foreign.MemorySegment.NULL;
import static java.util.Objects.requireNonNullElse;

import com.google.protobuf.ByteString;
import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.net.URI;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.HexFormat;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.jetbrains.annotations.Contract;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Utils {

	@SuppressWarnings("resource")
	private static final MemorySegment DUMMY_EMPTY_VALUE = Arena
			.global()
			.allocate(1)
			.copyFrom(MemorySegment.ofArray(new byte[] {-1}))
			.asReadOnly();
	public static final int DEFAULT_PORT = 5333;

	public static MemorySegment dummyEmptyValue() {
		return DUMMY_EMPTY_VALUE;
	}

	/**
	 * Returns the value of the {@code int} argument, throwing an exception if the value overflows an {@code char}.
	 *
	 * @param value the int value
	 * @return the argument as a char
	 * @throws ArithmeticException if the {@code argument} overflows a char
	 * @since 1.8
	 */
	public static char toCharExact(int value) {
		if (value < Character.MIN_VALUE || value > Character.MAX_VALUE) {
			throw new ArithmeticException("char overflow");
		} else {
			return (char) value;
		}
	}

	/**
	 * Returns the value of the {@code long} argument, throwing an exception if the value overflows an {@code char}.
	 *
	 * @param value the long value
	 * @return the argument as a char
	 * @throws ArithmeticException if the {@code argument} overflows a char
	 * @since 1.8
	 */
	public static char toCharExact(long value) {
		if (value < Character.MIN_VALUE || value > Character.MAX_VALUE) {
			throw new ArithmeticException("char overflow");
		} else {
			return (char) value;
		}
	}

	public static @NotNull MemorySegment toMemorySegment(Arena arena, ByteString value) {
		var buf = value.asReadOnlyByteBuffer();
		if (buf.isDirect()) {
			return MemorySegment.ofBuffer(buf);
		} else {
			return arena.allocate(value.size()).copyFrom(MemorySegment.ofBuffer(buf));
		}
	}

	@Contract(value = "!null -> !null; null -> null", pure = true)
	public static MemorySegment toMemorySegment(byte... array) {
		if (array != null) {
			return MemorySegment.ofArray(array);
		} else {
			return null;
		}
	}

	@Contract("null, !null -> fail; _, null -> null")
	public static @Nullable MemorySegment toMemorySegment(Arena arena, byte... array) {
		if (array != null) {
			assert arena != null;
			// todo: replace with allocateArray when graalvm adds it
			return arena.allocate(array.length).copyFrom(MemorySegment.ofArray(array));
		} else {
			return null;
		}
	}

	@Contract(value = "!null -> !null; null -> null", pure = true)
	public static MemorySegment toMemorySegmentSimple(int... array) {
		if (array != null) {
			var newArray = new byte[array.length];
			for (int i = 0; i < array.length; i++) {
				newArray[i] = (byte) array[i];
			}
			return MemorySegment.ofArray(newArray);
		} else {
			return null;
		}
	}

	@Contract("null, !null -> fail; _, null -> null")
	public static MemorySegment toMemorySegmentSimple(Arena arena, int... array) {
		if (array != null) {
			assert arena != null;
			var newArray = new byte[array.length];
			for (int i = 0; i < array.length; i++) {
				newArray[i] = (byte) array[i];
			}
			// todo: replace with allocateArray when graalvm adds it
			return arena.allocate(newArray.length).copyFrom(MemorySegment.ofArray(newArray));
		} else {
			return null;
		}
	}

	public static byte @NotNull[] toByteArray(@NotNull MemorySegment memorySegment) {
		return memorySegment.toArray(BIG_ENDIAN_BYTES);
	}

	public static <T, U> List<U> mapList(Collection<T> input, Function<T, U> mapper) {
		var result = new ArrayList<U>(input.size());
		input.forEach(t -> result.add(mapper.apply(t)));
		return result;
	}

	public static void deleteDirectory(String path) throws RocksDBException {
		try (Stream<Path> pathStream = Files.walk(Path.of(path))) {
			pathStream.sorted(Comparator.reverseOrder()).forEach(f -> {
				try {
					Files.deleteIfExists(f);
				} catch (IOException e) {
					throw RocksDBException.of(RocksDBException.RocksDBErrorType.DIRECTORY_DELETE, e);
				}
			});
		} catch (IOException e) {
			throw RocksDBException.of(RocksDBException.RocksDBErrorType.DIRECTORY_DELETE, e);
		}
	}

	public static boolean valueEquals(MemorySegment previousValue, MemorySegment currentValue) {
		previousValue = requireNonNullElse(previousValue, NULL);
		currentValue = requireNonNullElse(currentValue, NULL);
		return MemorySegment.mismatch(previousValue, 0, previousValue.byteSize(), currentValue, 0, currentValue.byteSize())
				== -1;
	}

	public static int valueHash(MemorySegment value) {
		value = requireNonNullElse(value, NULL);
		int hash = 7;
		var len = value.byteSize();
		for (long i = 0; i < len; i++) {
			hash = hash * 31 + value.get(ValueLayout.JAVA_BYTE, i);
		}
		return hash;
	}

	public static HostAndPort parseHostAndPort(URI uri) {
		return new HostAndPort(uri.getHost(), parsePort(uri));
	}

	public static int parsePort(URI uri) {
		var port = uri.getPort();
		if (port == -1) {
			return DEFAULT_PORT;
		} else {
			return port;
		}
	}

	public static String toPrettyString(MemorySegment s) {
		var b = s.toArray(BIG_ENDIAN_BYTES);
		return HexFormat.of().formatHex(b);
	}

	public record HostAndPort(String host, int port) {}
}
