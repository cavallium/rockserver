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
import java.util.Objects;

/**
 * Safe Java implementation of {@link XXHash32}.
 */
public final class XXHash32JavaSafe extends XXHash32 {


	public static final XXHash32 INSTANCE = new XXHash32JavaSafe();

	@Override
	public int hash(byte[] buf, int off, int len, int seed) {
		Objects.checkFromIndexSize(off, len, buf.length);

		final int end = off + len;
		int h32;

		if (len >= 16) {
			final int limit = end - 16;
			int v1 = seed + PRIME1 + PRIME2;
			int v2 = seed + PRIME2;
			int v3 = seed;
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
	public void hash(Buf buf, int off, int len, int seed, Buf result) {
		Objects.checkFromIndexSize(off, len, buf.size());

		final int end = off + len;
		int h32;

		if (len >= 16) {
			final int limit = end - 16;
			int v1 = seed + PRIME1 + PRIME2;
			int v2 = seed + PRIME2;
			int v3 = seed;
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
			h32 += (buf.getByte(off) & 0xFF) * PRIME5;
			h32 = rotateLeft(h32, 11) * PRIME1;
			++off;
		}

		h32 ^= h32 >>> 15;
		h32 *= PRIME2;
		h32 ^= h32 >>> 13;
		h32 *= PRIME3;
		h32 ^= h32 >>> 16;

		assert result.size() >= Integer.BYTES;
		result.setInt(0, h32);
	}

	private static int readIntLE(byte[] buf, int i) {
		return (buf[i] & 0xFF) | ((buf[i+1] & 0xFF) << 8) | ((buf[i+2] & 0xFF) << 16) | ((buf[i+3] & 0xFF) << 24);
	}

	private static int readIntLE(Buf buf, int i) {
		return buf.getIntLE(i);
	}
}