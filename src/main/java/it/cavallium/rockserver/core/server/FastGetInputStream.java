package it.cavallium.rockserver.core.server;


import io.grpc.Drainable;
import io.grpc.KnownLength;
import it.cavallium.rockserver.core.common.*;
import it.cavallium.rockserver.core.common.api.proto.*;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import it.cavallium.buffer.Buf;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.util.Objects;

import org.jetbrains.annotations.Nullable;

final class FastGetInputStream extends InputStream implements Drainable, KnownLength {

	private static final int COPY_CHUNK_BYTES = 16 * 1024;
	private static final byte[] EMPTY_PREFIX = new byte[0];
	private static final ThreadLocal<byte[]> COPY_CHUNK = ThreadLocal.withInitial(
			() -> new byte[COPY_CHUNK_BYTES]);

	private final byte[] prefix;
	private final @Nullable Buf value;
	private final boolean pinned;
	private int prefixOffset;
	private int valueOffset;

	FastGetInputStream(boolean present, @Nullable Buf value, boolean pinned) {
		this.value = value;
		this.pinned = pinned;
		this.prefix = present ? protobufBytesPrefix(Objects.requireNonNull(value).size()) : EMPTY_PREFIX;
		long totalLength = (long) prefix.length + (value != null ? value.size() : 0L);
		if (totalLength > Integer.MAX_VALUE) {
			throw new IllegalArgumentException("Get response exceeds the gRPC message length limit: " + totalLength);
		}
	}

	@Override
	public int read() {
		if (prefixOffset < prefix.length) {
			return prefix[prefixOffset++] & 0xff;
		}
		if (value == null || valueOffset >= value.size()) {
			return -1;
		}
		return value.getByte(valueOffset++) & 0xff;
	}

	@Override
	public int read(byte[] target, int offset, int length) {
		Objects.checkFromIndexSize(offset, length, target.length);
		if (length == 0) {
			return 0;
		}
		int copied = 0;
		if (prefixOffset < prefix.length) {
			int count = Math.min(length, prefix.length - prefixOffset);
			System.arraycopy(prefix, prefixOffset, target, offset, count);
			prefixOffset += count;
			offset += count;
			length -= count;
			copied += count;
		}
		if (length > 0 && value != null && valueOffset < value.size()) {
			int count = Math.min(length, value.size() - valueOffset);
			if (pinned) {
				MemorySegment.copy(value.asMemorySegmentStrict(),
						ValueLayout.JAVA_BYTE,
						valueOffset,
						target,
						offset,
						count);
			} else {
				System.arraycopy(value.getBackingByteArray(),
						value.getBackingByteArrayOffset() + valueOffset,
						target,
						offset,
						count);
			}
			valueOffset += count;
			copied += count;
		}
		return copied == 0 ? -1 : copied;
	}

	@Override
	public int drainTo(OutputStream target) throws IOException {
		int written = 0;
		if (prefixOffset < prefix.length) {
			int count = prefix.length - prefixOffset;
			target.write(prefix, prefixOffset, count);
			prefixOffset = prefix.length;
			written += count;
		}
		if (value == null || valueOffset >= value.size()) {
			return written;
		}
		int remaining = value.size() - valueOffset;
		if (!pinned) {
			target.write(value.getBackingByteArray(),
					value.getBackingByteArrayOffset() + valueOffset,
					remaining);
			valueOffset += remaining;
			return written + remaining;
		}

		MemorySegment segment = value.asMemorySegmentStrict();
		byte[] chunk = COPY_CHUNK.get();
		while (remaining > 0) {
			int count = Math.min(remaining, chunk.length);
			MemorySegment.copy(segment, ValueLayout.JAVA_BYTE, valueOffset, chunk, 0, count);
			target.write(chunk, 0, count);
			valueOffset += count;
			remaining -= count;
			written += count;
		}
		return written;
	}

	@Override
	public int available() {
		return prefix.length - prefixOffset + (value != null ? value.size() - valueOffset : 0);
	}

	static byte[] protobufBytesPrefix(int valueLength) {
		byte[] result = new byte[1 + varintSize(valueLength)];
		result[0] = 0x0a;
		int value = valueLength;
		int index = 1;
		while ((value & ~0x7f) != 0) {
			result[index++] = (byte) ((value & 0x7f) | 0x80);
			value >>>= 7;
		}
		result[index] = (byte) value;
		return result;
	}

	static int varintSize(int value) {
		int size = 1;
		while ((value & ~0x7f) != 0) {
			size++;
			value >>>= 7;
		}
		return size;
	}
}
