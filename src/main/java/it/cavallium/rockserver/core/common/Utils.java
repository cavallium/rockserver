package it.cavallium.rockserver.core.common;

import static it.cavallium.rockserver.core.impl.ColumnInstance.BIG_ENDIAN_BYTES;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class Utils {

	/**
	 * Returns the value of the {@code int} argument, throwing an exception if the value overflows an {@code char}.
	 *
	 * @param value the int value
	 * @return the argument as a char
	 * @throws ArithmeticException if the {@code argument} overflows a char
	 * @since 1.8
	 */
	public static char toCharExact(int value) {
		if ((char) value != value) {
			throw new ArithmeticException("char overflow");
		}
		return (char) value;
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
		if ((char) value != value) {
			throw new ArithmeticException("char overflow");
		}
		return (char) value;
	}

	@NotNull
	public static MemorySegment toMemorySegment(Arena arena, byte @Nullable [] array) {
		if (array != null) {
			return arena.allocateArray(BIG_ENDIAN_BYTES, array);
		} else {
			return MemorySegment.NULL;
		}
	}
}
