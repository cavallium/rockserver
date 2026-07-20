package it.cavallium.rockserver.core.cluster.internal;

import java.io.FilterInputStream;
import java.io.IOException;
import java.io.InputStream;

public final class BoundedInputStream extends FilterInputStream {

    private final int maxSize;
    private final String description;
    private int consumed;

    public BoundedInputStream(InputStream input, int maxSize, String description) {
        super(requireArgument(input, "input"));
        if (maxSize < 0) {
            throw new IllegalArgumentException("maxSize must not be negative: " + maxSize);
        }
        this.maxSize = maxSize;
        this.description = requireArgument(description, "description");
    }

    @Override
    public int read() throws IOException {
        var value = in.read();
        if (value == -1) {
            return -1;
        }
        requireCapacity(1);
        consumed++;
        return value;
    }

    @Override
    public int read(byte[] value, int offset, int length) throws IOException {
        checkRange(requireArgument(value, "value"), offset, length);
        if (length == 0) {
            return 0;
        }
        var read = in.read(value, offset, boundedLength(length));
        if (read <= 0) {
            return read;
        }
        requireCapacity(read);
        consumed += read;
        return read;
    }

    @Override
    public long skip(long length) throws IOException {
        if (length <= 0) {
            return 0;
        }
        var skipped = in.skip(boundedLength(length));
        if (skipped <= 0) {
            return skipped;
        }
        requireCapacity(skipped);
        consumed += (int) skipped;
        return skipped;
    }

    private int boundedLength(int requestedLength) {
        return (int) Math.min(requestedLength, remainingProbeBytes());
    }

    private long boundedLength(long requestedLength) {
        return Math.min(requestedLength, remainingProbeBytes());
    }

    private long remainingProbeBytes() {
        return (long) maxSize - consumed + 1;
    }

    private void requireCapacity(long additionalBytes) {
        if (additionalBytes < 0 || consumed > maxSize - additionalBytes) {
            throw new IllegalArgumentException("Invalid " + description + " length: "
                    + ((long) consumed + additionalBytes));
        }
    }

    private static <T> T requireArgument(T value, String name) {
        if (value == null) {
            throw new IllegalArgumentException(name + " must not be null");
        }
        return value;
    }

    private static void checkRange(byte[] value, int offset, int length) {
        if (offset < 0 || length < 0 || offset > value.length - length) {
            throw new IndexOutOfBoundsException("Range [" + offset + ", " + offset + " + " + length
                    + ") out of bounds for length " + value.length);
        }
    }
}
