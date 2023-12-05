package it.cavallium.rockserver.core.impl;

/*
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

import static it.cavallium.rockserver.core.impl.XXHashUtils.PRIME1;
import static it.cavallium.rockserver.core.impl.XXHashUtils.PRIME2;
import static it.cavallium.rockserver.core.impl.XXHashUtils.PRIME3;
import static it.cavallium.rockserver.core.impl.XXHashUtils.PRIME4;
import static it.cavallium.rockserver.core.impl.XXHashUtils.PRIME5;
import static java.lang.Integer.rotateLeft;

import java.lang.foreign.Arena;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.foreign.ValueLayout.OfByte;
import java.lang.foreign.ValueLayout.OfInt;
import java.nio.ByteOrder;

/**
 * Safe Java implementation of {@link XXHash32}.
 */
public final class XXHash32JavaSafe extends XXHash32 {


	public static final ByteOrder NATIVE_BYTE_ORDER = ByteOrder.nativeOrder();
	public static final XXHash32 INSTANCE = new XXHash32JavaSafe();
	private static final OfInt INT_LE = ValueLayout.JAVA_INT.withOrder(ByteOrder.LITTLE_ENDIAN);
	private static final OfInt INT_BE = ValueLayout.JAVA_INT.withOrder(ByteOrder.BIG_ENDIAN);
	private static final OfByte BYTE_BE = ValueLayout.JAVA_BYTE.withOrder(ByteOrder.BIG_ENDIAN);

	@Override
	public int hash(byte[] buf, int off, int len, int seed) {
		checkRange(buf, off, len);

		final int end = off + len;
		int h32;

		if (len >= 16) {
			final int limit = end - 16;
			int v1 = seed + PRIME1 + PRIME2;
			int v2 = seed + PRIME2;
			int v3 = seed + 0;
			int v4 = seed - PRIME1;
			do {
				v1 += readIntLE(buf, off) * PRIME2;
				v1 = rotateLeft(v1, 13);
				v1 *= PRIME1;
				off += 4;

				v2 += readIntLE(buf, off) * PRIME2;
				v2 = rotateLeft(v2, 13);
				v2 *= PRIME1;
				off += 4;

				v3 += readIntLE(buf, off) * PRIME2;
				v3 = rotateLeft(v3, 13);
				v3 *= PRIME1;
				off += 4;

				v4 += readIntLE(buf, off) * PRIME2;
				v4 = rotateLeft(v4, 13);
				v4 *= PRIME1;
				off += 4;
			} while (off <= limit);

			h32 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
		} else {
			h32 = seed + PRIME5;
		}

		h32 += len;

		while (off <= end - 4) {
			h32 += readIntLE(buf, off) * PRIME3;
			h32 = rotateLeft(h32, 17) * PRIME4;
			off += 4;
		}

		while (off < end) {
			h32 += (buf[off] & 0xFF) * PRIME5;
			h32 = rotateLeft(h32, 11) * PRIME1;
			++off;
		}

		h32 ^= h32 >>> 15;
		h32 *= PRIME2;
		h32 ^= h32 >>> 13;
		h32 *= PRIME3;
		h32 ^= h32 >>> 16;

		return h32;
	}

	@Override
	public MemorySegment hash(Arena arena, MemorySegment buf, int off, int len, int seed) {
		checkRange(buf, off, len);

		final int end = off + len;
		int h32;

		if (len >= 16) {
			final int limit = end - 16;
			int v1 = seed + PRIME1 + PRIME2;
			int v2 = seed + PRIME2;
			int v3 = seed + 0;
			int v4 = seed - PRIME1;
			do {
				v1 += readIntLE(buf, off) * PRIME2;
				v1 = rotateLeft(v1, 13);
				v1 *= PRIME1;
				off += 4;

				v2 += readIntLE(buf, off) * PRIME2;
				v2 = rotateLeft(v2, 13);
				v2 *= PRIME1;
				off += 4;

				v3 += readIntLE(buf, off) * PRIME2;
				v3 = rotateLeft(v3, 13);
				v3 *= PRIME1;
				off += 4;

				v4 += readIntLE(buf, off) * PRIME2;
				v4 = rotateLeft(v4, 13);
				v4 *= PRIME1;
				off += 4;
			} while (off <= limit);

			h32 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
		} else {
			h32 = seed + PRIME5;
		}

		h32 += len;

		while (off <= end - 4) {
			h32 += readIntLE(buf, off) * PRIME3;
			h32 = rotateLeft(h32, 17) * PRIME4;
			off += 4;
		}

		while (off < end) {
			h32 += (buf.get(BYTE_BE, off) & 0xFF) * PRIME5;
			h32 = rotateLeft(h32, 11) * PRIME1;
			++off;
		}

		h32 ^= h32 >>> 15;
		h32 *= PRIME2;
		h32 ^= h32 >>> 13;
		h32 *= PRIME3;
		h32 ^= h32 >>> 16;

		return arena.allocate(INT_BE, h32);
	}

	private static void checkRange(byte[] buf, int off) {
		if (off < 0 || off >= buf.length) {
			throw new ArrayIndexOutOfBoundsException(off);
		}
	}

	private static void checkRange(MemorySegment buf, int off) {
		if (off < 0 || off >= buf.byteSize()) {
			throw new ArrayIndexOutOfBoundsException(off);
		}
	}

	private static void checkRange(byte[] buf, int off, int len) {
		checkLength(len);
		if (len > 0) {
			checkRange(buf, off);
			checkRange(buf, off + len - 1);
		}
	}

	private static void checkRange(MemorySegment buf, int off, int len) {
		checkLength(len);
		if (len > 0) {
			checkRange(buf, off);
			checkRange(buf, off + len - 1);
		}
	}

	private static void checkLength(int len) {
		if (len < 0) {
			throw new IllegalArgumentException("lengths must be >= 0");
		}
	}

	private static int readIntBE(byte[] buf, int i) {
		return ((buf[i] & 0xFF) << 24) | ((buf[i+1] & 0xFF) << 16) | ((buf[i+2] & 0xFF) << 8) | (buf[i+3] & 0xFF);
	}

	private static int readIntLE(byte[] buf, int i) {
		return (buf[i] & 0xFF) | ((buf[i+1] & 0xFF) << 8) | ((buf[i+2] & 0xFF) << 16) | ((buf[i+3] & 0xFF) << 24);
	}

	private static int readIntLE(MemorySegment buf, int i) {
		return buf.get(INT_LE, i);
	}

	private static int readInt(byte[] buf, int i) {
		if (NATIVE_BYTE_ORDER == ByteOrder.BIG_ENDIAN) {
			return readIntBE(buf, i);
		} else {
			return readIntLE(buf, i);
		}
	}
}