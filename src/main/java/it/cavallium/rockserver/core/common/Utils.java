package it.cavallium.rockserver.core.common;

import static it.cavallium.rockserver.core.impl.ColumnInstance.BIG_ENDIAN_BYTES;
import static java.lang.foreign.MemorySegment.NULL;
import static java.util.Objects.requireNonNullElse;

import java.io.IOException;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Comparator;
import java.util.List;
import java.util.function.Function;
import java.util.stream.Stream;

import org.jetbrains.annotations.NotNull;

public class Utils {
	@SuppressWarnings("resource")
	private static final MemorySegment DUMMY_EMPTY_VALUE = Arena
			.global()
			.allocate(ValueLayout.JAVA_BYTE, (byte) -1)
			.asReadOnly();

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

	@NotNull
	public static MemorySegment toMemorySegment(byte... array) {
		if (array != null) {
			return MemorySegment.ofArray(array);
		} else {
			return MemorySegment.NULL;
		}
	}

	public static MemorySegment toMemorySegment(Arena arena, byte... array) {
		if (array != null) {
			return arena.allocateArray(ValueLayout.JAVA_BYTE, array);
		} else {
			return MemorySegment.NULL;
		}
	}

	@NotNull
	public static MemorySegment toMemorySegmentSimple(int... array) {
		if (array != null) {
			var newArray = new byte[array.length];
			for (int i = 0; i < array.length; i++) {
				newArray[i] = (byte) array[i];
			}
			return MemorySegment.ofArray(newArray);
		} else {
			return MemorySegment.NULL;
		}
	}

	@NotNull
	public static MemorySegment toMemorySegmentSimple(Arena arena, int... array) {
		if (array != null) {
			var newArray = new byte[array.length];
			for (int i = 0; i < array.length; i++) {
				newArray[i] = (byte) array[i];
			}
			return arena.allocateArray(ValueLayout.JAVA_BYTE, newArray);
		} else {
			return MemorySegment.NULL;
		}
	}

    public static byte[] toByteArray(MemorySegment memorySegment) {
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
		return MemorySegment.mismatch(previousValue, 0, previousValue.byteSize(), currentValue, 0, currentValue.byteSize()) == -1;
	}
}
