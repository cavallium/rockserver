package it.cavallium.rockserver.core.cluster.internal;

import java.io.ByteArrayOutputStream;

public final class BoundedByteArrayOutputStream extends ByteArrayOutputStream {

    private final int maxSize;
    private final String description;

    public BoundedByteArrayOutputStream(int maxSize, String description) {
        super(initialCapacity(maxSize));
        this.maxSize = maxSize;
        this.description = requireArgument(description, "description");
    }

    @Override
    public synchronized void write(int value) {
        requireCapacity(1);
        super.write(value);
    }

    @Override
    public synchronized void write(byte[] value, int offset, int length) {
        checkRange(requireArgument(value, "value"), offset, length);
        requireCapacity(length);
        super.write(value, offset, length);
    }

    private void requireCapacity(int additionalBytes) {
        if (additionalBytes < 0 || count > maxSize - additionalBytes) {
            throw new IllegalArgumentException("Invalid " + description + " length: "
                    + ((long) count + additionalBytes));
        }
    }

    private static int initialCapacity(int maxSize) {
        if (maxSize < 0) {
            throw new IllegalArgumentException("maxSize must not be negative: " + maxSize);
        }
        return Math.min(maxSize, 1024);
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
