package it.cavallium.rockserver.core.cluster.internal;

import java.io.FilterOutputStream;
import java.io.IOException;
import java.io.OutputStream;

public final class BoundedOutputStream extends FilterOutputStream {

    private final int maxSize;
    private final String description;
    private int written;

    public BoundedOutputStream(OutputStream output, int maxSize, String description) {
        super(requireArgument(output, "output"));
        if (maxSize < 0) {
            throw new IllegalArgumentException("maxSize must not be negative: " + maxSize);
        }
        this.maxSize = maxSize;
        this.description = requireArgument(description, "description");
    }

    @Override
    public void write(int value) throws IOException {
        requireCapacity(1);
        out.write(value);
        written++;
    }

    @Override
    public void write(byte[] value, int offset, int length) throws IOException {
        checkRange(requireArgument(value, "value"), offset, length);
        requireCapacity(length);
        out.write(value, offset, length);
        written += length;
    }

    private void requireCapacity(int additionalBytes) {
        if (additionalBytes < 0 || written > maxSize - additionalBytes) {
            throw new IllegalArgumentException("Invalid " + description + " length: "
                    + ((long) written + additionalBytes));
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
