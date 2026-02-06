package it.cavallium.rockserver.core.common;

import it.cavallium.buffer.Buf;
import java.util.Iterator;
import java.util.Spliterator;
import java.util.Spliterators;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;
import org.jetbrains.annotations.NotNull;

public interface SerializedKVBatch {

	@NotNull Buf serialized();

	default Stream<KV> decode() {
		Buf buf = serialized();
		int offset = 0;
		int batchSize = buf.getIntLE(offset);
		offset += Integer.BYTES;

		int finalOffset = offset;
		return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<KV>() {
			int currentOffset = finalOffset;
			int currentIndex = 0;

			@Override
			public boolean hasNext() {
				return currentIndex < batchSize;
			}

			@Override
			public KV next() {
				currentIndex++;
				// Read Keys
				int keysCount = buf.getByte(currentOffset);
				currentOffset += Byte.BYTES;

				Buf[] keyBufs = new Buf[keysCount];
				for (int i = 0; i < keysCount; i++) {
					int keyLen = buf.getIntLE(currentOffset);
					currentOffset += Integer.BYTES;
					keyBufs[i] = buf.subList(currentOffset, currentOffset + keyLen);
					currentOffset += keyLen;
				}
				Keys keys = new Keys(keyBufs);

				// Read Value
				int valLen = buf.getIntLE(currentOffset);
				currentOffset += Integer.BYTES;
				Buf userValue = buf.subList(currentOffset, currentOffset + valLen);
				currentOffset += valLen;

				return new KV(keys, userValue);
			}
		}, Spliterator.ORDERED), false);
	}

	record SerializedKVBatchRef(@NotNull Buf serialized) implements SerializedKVBatch {
	}
}
