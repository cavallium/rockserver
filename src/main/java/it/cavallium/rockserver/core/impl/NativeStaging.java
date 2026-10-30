package it.cavallium.rockserver.core.impl;

import it.cavallium.buffer.Buf;
import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.List;

/**
 * Native staging buffers for the {@code multiGetByteBuffers(MemorySegment[], MemorySegment[])} calls.
 *
 * <p>Each staging call makes one arena allocation and hands out slices of it, so a chunk of 256 keys costs one
 * malloc, one zero-fill and one free instead of 256 of each. Every slice has exactly the requested length, because
 * the JNI layer turns each segment into a ByteBuffer and uses its capacity as the key length or the value slot size.
 * The slices live as long as the arena that owns the block.
 */
public final class NativeStaging {

	private NativeStaging() {
	}

	/**
	 * Copies {@code keys} into one native block and returns one exact-length slice per key.
	 */
	public static MemorySegment[] stageKeys(Arena arena, List<Buf> keys) {
		int count = keys.size();
		long total = 0L;
		for (int i = 0; i < count; i++) {
			total += keys.get(i).size();
		}
		var block = arena.allocate(total, 1);
		var out = new MemorySegment[count];
		long position = 0L;
		for (int i = 0; i < count; i++) {
			var key = keys.get(i);
			int length = key.size();
			copyInto(key, block, position, length);
			out[i] = block.asSlice(position, length);
			position += length;
		}
		return out;
	}

	/**
	 * Copies {@code keys} into one native block and returns one exact-length slice per key.
	 */
	public static MemorySegment[] stageKeys(Arena arena, byte[][] keys) {
		long total = 0L;
		for (byte[] key : keys) {
			total += key.length;
		}
		var block = arena.allocate(total, 1);
		var out = new MemorySegment[keys.length];
		long position = 0L;
		for (int i = 0; i < keys.length; i++) {
			byte[] key = keys[i];
			MemorySegment.copy(key, 0, block, ValueLayout.JAVA_BYTE, position, key.length);
			out[i] = block.asSlice(position, key.length);
			position += key.length;
		}
		return out;
	}

	/**
	 * Allocates one zeroed native block and returns one slice of exactly {@code sizes[i]} bytes per entry.
	 */
	public static MemorySegment[] allocateSlices(Arena arena, long[] sizes) {
		long total = 0L;
		for (long size : sizes) {
			total += size;
		}
		var block = arena.allocate(total, 1);
		var out = new MemorySegment[sizes.length];
		long position = 0L;
		for (int i = 0; i < sizes.length; i++) {
			out[i] = block.asSlice(position, sizes[i]);
			position += sizes[i];
		}
		return out;
	}

	/**
	 * Returns whether the first {@code length} bytes of {@code actual} equal {@code expected}, which must be exactly
	 * {@code length} bytes long. {@link MemorySegment#mismatch} is backed by the JDK's vectorized mismatch intrinsic.
	 */
	public static boolean contentEquals(MemorySegment actual, Buf expected, int length) {
		if (length != expected.size() || actual.byteSize() < length) {
			return false;
		}
		if (length == 0) {
			return true;
		}
		return MemorySegment.mismatch(actual, 0, length, asSegment(expected), 0, length) == -1L;
	}

	private static void copyInto(Buf source, MemorySegment block, long position, int length) {
		if (length == 0) {
			return;
		}
		byte[] backing = source.getBackingByteArrayStrict();
		if (backing != null) {
			MemorySegment.copy(backing, source.getBackingByteArrayOffset(), block, ValueLayout.JAVA_BYTE, position,
					length);
		} else {
			// MemorySegmentBuf and other non-array buffers
			MemorySegment.copy(source.asMemorySegment(), 0, block, position, length);
		}
	}

	private static MemorySegment asSegment(Buf source) {
		byte[] backing = source.getBackingByteArrayStrict();
		if (backing != null) {
			return MemorySegment.ofArray(backing).asSlice(source.getBackingByteArrayOffset(), source.size());
		}
		return source.asMemorySegment();
	}
}
