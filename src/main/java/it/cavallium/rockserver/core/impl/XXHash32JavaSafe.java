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

import it.cavallium.buffer.Buf;
import java.lang.foreign.MemorySegment;
import java.lang.foreign.ValueLayout;
import java.lang.invoke.MethodHandles;
import java.lang.invoke.VarHandle;
import java.nio.ByteOrder;
import java.util.Objects;

/**
 * Safe Java implementation of {@link XXHash32}.
 * <p>
 * Heap buffers are read with one little-endian int load per 4 bytes straight from the backing array, other buffers
 * through their {@link MemorySegment} view: never byte by byte through the {@link Buf} interface.
 * <p>
 * A Vector API variant (the four accumulators as one 128-bit int vector) was measured and not kept: the four scalar
 * chains already overlap, so it was slower than this up to 64 bytes and equal from 120 bytes on.
 */
public final class XXHash32JavaSafe extends XXHash32 {

	public static final XXHash32 INSTANCE = new XXHash32JavaSafe();

	private static final VarHandle ARRAY_INT_LE = MethodHandles.byteArrayViewVarHandle(int[].class,
			ByteOrder.LITTLE_ENDIAN);
	private static final ValueLayout.OfInt SEGMENT_INT_LE = ValueLayout.JAVA_INT_UNALIGNED
			.withOrder(ByteOrder.LITTLE_ENDIAN);

	@Override
	public int hash(byte[] buf, int off, int len, int seed) {
		Objects.checkFromIndexSize(off, len, buf.length);
		return hashArray(buf, off, len, seed);
	}

	@Override
	public int hash(Buf buf, int off, int len, int seed) {
		Objects.checkFromIndexSize(off, len, buf.size());
		byte[] array = buf.getBackingByteArrayStrict();
		if (array != null) {
			return hashArray(array, buf.getBackingByteArrayOffset() + off, len, seed);
		} else {
			return hashSegment(buf.asMemorySegment(), off, len, seed);
		}
	}

	private static int hashArray(byte[] buf, int off, int len, int seed) {
		final int end = off + len;
		int h32;

		if (len >= 16) {
			final int limit = end - 16;
			int v1 = seed + PRIME1 + PRIME2;
			int v2 = seed + PRIME2;
			int v3 = seed;
			int v4 = seed - PRIME1;
			do {
				v1 = round(v1, (int) ARRAY_INT_LE.get(buf, off));
				v2 = round(v2, (int) ARRAY_INT_LE.get(buf, off + 4));
				v3 = round(v3, (int) ARRAY_INT_LE.get(buf, off + 8));
				v4 = round(v4, (int) ARRAY_INT_LE.get(buf, off + 12));
				off += 16;
			} while (off <= limit);

			h32 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
		} else {
			h32 = seed + PRIME5;
		}

		h32 += len;

		while (off <= end - 4) {
			h32 = tailInt(h32, (int) ARRAY_INT_LE.get(buf, off));
			off += 4;
		}

		while (off < end) {
			h32 = tailByte(h32, buf[off]);
			++off;
		}

		return avalanche(h32);
	}

	private static int hashSegment(MemorySegment buf, long off, int len, int seed) {
		final long end = off + len;
		int h32;

		if (len >= 16) {
			final long limit = end - 16;
			int v1 = seed + PRIME1 + PRIME2;
			int v2 = seed + PRIME2;
			int v3 = seed;
			int v4 = seed - PRIME1;
			do {
				v1 = round(v1, buf.get(SEGMENT_INT_LE, off));
				v2 = round(v2, buf.get(SEGMENT_INT_LE, off + 4));
				v3 = round(v3, buf.get(SEGMENT_INT_LE, off + 8));
				v4 = round(v4, buf.get(SEGMENT_INT_LE, off + 12));
				off += 16;
			} while (off <= limit);

			h32 = rotateLeft(v1, 1) + rotateLeft(v2, 7) + rotateLeft(v3, 12) + rotateLeft(v4, 18);
		} else {
			h32 = seed + PRIME5;
		}

		h32 += len;

		while (off <= end - 4) {
			h32 = tailInt(h32, buf.get(SEGMENT_INT_LE, off));
			off += 4;
		}

		while (off < end) {
			h32 = tailByte(h32, buf.get(ValueLayout.JAVA_BYTE, off));
			++off;
		}

		return avalanche(h32);
	}

	private static int round(int v, int input) {
		return rotateLeft(v + input * PRIME2, 13) * PRIME1;
	}

	private static int tailInt(int h32, int input) {
		return rotateLeft(h32 + input * PRIME3, 17) * PRIME4;
	}

	private static int tailByte(int h32, byte input) {
		return rotateLeft(h32 + (input & 0xFF) * PRIME5, 11) * PRIME1;
	}

	private static int avalanche(int h32) {
		h32 ^= h32 >>> 15;
		h32 *= PRIME2;
		h32 ^= h32 >>> 13;
		h32 *= PRIME3;
		h32 ^= h32 >>> 16;
		return h32;
	}
}
