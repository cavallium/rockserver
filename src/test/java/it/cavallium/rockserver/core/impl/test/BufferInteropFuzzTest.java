package it.cavallium.rockserver.core.impl.test;

import com.code_intelligence.jazzer.api.FuzzedDataProvider;
import com.code_intelligence.jazzer.junit.FuzzTest;
import com.google.protobuf.ByteString;
import it.cavallium.buffer.Buf;
import it.cavallium.rockserver.core.common.Utils;

import java.nio.ByteBuffer;
import java.util.Arrays;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class BufferInteropFuzzTest {

	private static final int MAX_INPUT_SIZE = 64 * 1_024;
	private static final int MAX_PADDING = 32;

	@FuzzTest(maxDuration = "30s")
	void heapDirectAndReadOnlyBuffersExposeTheSameWindow(FuzzedDataProvider data) {
		int prefix = data.consumeInt(0, MAX_PADDING);
		int suffix = data.consumeInt(0, MAX_PADDING);
		int fromSelector = data.consumeInt();
		int toSelector = data.consumeInt();
		byte marker = data.consumeByte();
		byte[] payload = data.consumeBytes(MAX_INPUT_SIZE);
		int from = Math.floorMod(fromSelector, payload.length + 1);
		int to = from + Math.floorMod(toSelector, payload.length - from + 1);
		byte[] expected = Arrays.copyOfRange(payload, from, to);
		byte[] storage = new byte[prefix + payload.length + suffix];
		Arrays.fill(storage, marker);
		System.arraycopy(payload, 0, storage, prefix, payload.length);

		ByteBuffer heap = window(ByteBuffer.wrap(storage), prefix + from, prefix + to);
		int heapPosition = heap.position();
		assertArrayEquals(expected, Utils.fromHeapByteBuffer(heap).asArray());
		assertArrayEquals(expected, Utils.fromByteBuffer(heap).asArray());
		assertEquals(heapPosition, heap.position());

		ByteBuffer slicedHeap = ByteBuffer.wrap(storage, prefix, payload.length).slice();
		slicedHeap.position(from);
		slicedHeap.limit(to);
		assertArrayEquals(expected, Utils.fromHeapByteBuffer(slicedHeap).asArray());
		assertEquals(from, slicedHeap.position());

		ByteBuffer direct = ByteBuffer.allocateDirect(storage.length);
		direct.put(storage).flip();
		window(direct, prefix + from, prefix + to);
		assertArrayEquals(expected, Utils.fromByteBuffer(direct).asArray());
		assertEquals(prefix + to, direct.position());

		ByteBuffer readOnly = window(ByteBuffer.wrap(storage).asReadOnlyBuffer(), prefix + from, prefix + to);
		assertArrayEquals(expected, Utils.fromByteBuffer(readOnly).asArray());
		assertEquals(prefix + to, readOnly.position());
	}

	@FuzzTest(maxDuration = "30s")
	void bufProtobufAndByteBufferConversionsRespectBackingRanges(FuzzedDataProvider data) {
		int prefix = data.consumeInt(0, MAX_PADDING);
		int suffix = data.consumeInt(0, MAX_PADDING);
		byte marker = data.consumeByte();
		byte[] payload = data.consumeBytes(MAX_INPUT_SIZE);
		byte[] storage = new byte[prefix + payload.length + suffix];
		Arrays.fill(storage, marker);
		System.arraycopy(payload, 0, storage, prefix, payload.length);
		Buf ranged = Buf.wrap(storage, prefix, prefix + payload.length);

		ByteString byteString = Utils.toByteString(ranged);
		assertArrayEquals(payload, byteString.toByteArray());
		assertArrayEquals(payload, Utils.toBuf(byteString).asArray());

		ByteBuffer byteBuffer = Utils.asByteBuffer(ranged);
		assertEquals(payload.length, byteBuffer.remaining());
		byte[] fromByteBuffer = new byte[byteBuffer.remaining()];
		byteBuffer.get(fromByteBuffer);
		assertArrayEquals(payload, fromByteBuffer);
	}

	private static ByteBuffer window(ByteBuffer buffer, int from, int to) {
		buffer.position(from);
		buffer.limit(to);
		return buffer;
	}
}
