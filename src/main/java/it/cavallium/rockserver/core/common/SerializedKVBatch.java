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
        if (buf == null) {
            throw new IllegalArgumentException("serialized batch must not be null");
        }
        int offset = 0;
        offset = requireReadable(buf, offset, Integer.BYTES, "batch size");
        int batchSize = buf.getIntLE(offset);
        if (batchSize < 0) {
            throw new IllegalArgumentException("Serialized KV batch size must not be negative: " + batchSize);
        }
        offset += Integer.BYTES;
        int maxBatchSize = remaining(buf, offset) / (Byte.BYTES + Integer.BYTES);
        if (batchSize > maxBatchSize) {
            throw new IllegalArgumentException("Serialized KV batch size " + batchSize
                    + " exceeds remaining payload capacity: " + maxBatchSize);
        }

        int finalOffset = offset;
        return StreamSupport.stream(Spliterators.spliteratorUnknownSize(new Iterator<KV>() {
            int currentOffset = finalOffset;
            int currentIndex = 0;

            @Override
            public boolean hasNext() {
                if (currentIndex < batchSize) {
                    return true;
                }
                if (currentOffset != buf.size()) {
                    throw new IllegalArgumentException("Serialized KV batch has trailing bytes: "
                            + remaining(buf, currentOffset));
                }
                return false;
            }

            @Override
            public KV next() {
                currentIndex++;
                // Read Keys
                currentOffset = requireReadable(buf, currentOffset, Byte.BYTES, "key count");
                int keysCount = Byte.toUnsignedInt(buf.getByte(currentOffset));
                currentOffset += Byte.BYTES;
                int maxKeyCount = remaining(buf, currentOffset) / Integer.BYTES;
                if (keysCount > maxKeyCount) {
                    throw new IllegalArgumentException("Serialized KV key count " + keysCount
                            + " exceeds remaining payload capacity: " + maxKeyCount);
                }

                Buf[] keyBufs = new Buf[keysCount];
                for (int i = 0; i < keysCount; i++) {
                    currentOffset = requireReadable(buf, currentOffset, Integer.BYTES, "key length");
                    int keyLen = buf.getIntLE(currentOffset);
                    if (keyLen < 0) {
                        throw new IllegalArgumentException("Serialized KV key length must not be negative: " + keyLen);
                    }
                    currentOffset += Integer.BYTES;
                    currentOffset = requireReadable(buf, currentOffset, keyLen, "key payload");
                    keyBufs[i] = buf.subList(currentOffset, currentOffset + keyLen);
                    currentOffset += keyLen;
                }
                Keys keys = new Keys(keyBufs);

                // Read Value
                currentOffset = requireReadable(buf, currentOffset, Integer.BYTES, "value length");
                int valLen = buf.getIntLE(currentOffset);
                if (valLen < 0) {
                    throw new IllegalArgumentException("Serialized KV value length must not be negative: " + valLen);
                }
                currentOffset += Integer.BYTES;
                currentOffset = requireReadable(buf, currentOffset, valLen, "value payload");
                Buf userValue = buf.subList(currentOffset, currentOffset + valLen);
                currentOffset += valLen;

                return new KV(keys, userValue);
            }
        }, Spliterator.ORDERED), false);
    }

    private static int requireReadable(Buf buf, int offset, int length, String fieldName) {
        if (length < 0) {
            throw new IllegalArgumentException(fieldName + " length must not be negative: " + length);
        }
        if (offset < 0 || offset > buf.size()) {
            throw new IllegalArgumentException(fieldName + " offset is outside serialized batch bounds: " + offset);
        }
        int remaining = remaining(buf, offset);
        if (length > remaining) {
            throw new IllegalArgumentException(fieldName + " length " + length
                    + " exceeds remaining serialized batch bytes: " + remaining);
        }
        return offset;
    }

    private static int remaining(Buf buf, int offset) {
        return buf.size() - offset;
    }

    record SerializedKVBatchRef(@NotNull Buf serialized) implements SerializedKVBatch {
    }
}
