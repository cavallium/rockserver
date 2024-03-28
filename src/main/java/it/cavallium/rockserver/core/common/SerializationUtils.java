package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.BufDataInput;
import it.cavallium.buffer.BufDataOutput;
import it.cavallium.rockserver.core.impl.ColumnInstance;
import java.lang.foreign.MemorySegment;
import org.jetbrains.annotations.NotNull;
import org.jetbrains.annotations.Nullable;

public class SerializationUtils {
	public static void serializeMemorySegment(BufDataOutput out, @NotNull MemorySegment segment) {
		var array = segment.toArray(ColumnInstance.BIG_ENDIAN_BYTES);
		out.writeInt(array.length);
		out.writeBytes(array, 0, array.length);
	}
	public static MemorySegment deserializeMemorySegment(BufDataInput in) {
		return MemorySegment.ofArray(in.readNBytes(in.readInt()));
	}

	public static void serializeNullableMemorySegmentArray(BufDataOutput out, @NotNull MemorySegment @Nullable [] array) {
		out.writeInt(array != null ? array.length : -1);
		if (array != null) {
			for (MemorySegment memorySegment : array) {
				serializeMemorySegment(out, memorySegment);
			}
		}
	}

	public static void serializeMemorySegmentArray(BufDataOutput out, @NotNull MemorySegment @NotNull [] array) {
		out.writeInt(array.length);
		for (MemorySegment memorySegment : array) {
			serializeMemorySegment(out, memorySegment);
		}
	}

	public static @NotNull MemorySegment @Nullable [] deserializeNullableMemorySegmentArray(BufDataInput in) {
		int size = in.readInt();
		if (size == -1) {
			return null;
		} else {
			var array = new MemorySegment @NotNull[size];
			for (int i = 0; i < array.length; i++) {
				array[i] = deserializeMemorySegment(in);
			}
			return array;
		}
	}

	public static @NotNull MemorySegment @NotNull [] deserializeMemorySegmentArray(BufDataInput in) {
		var array = new MemorySegment @NotNull [in.readInt()];
		for (int i = 0; i < array.length; i++) {
			array[i] = deserializeMemorySegment(in);
		}
		return array;
	}
}
