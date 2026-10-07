package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/** Canonical saved outcome; values follow the order of non-NONE mutation return modes. */
public final class CommandResult {
    public static final int MAX_BYTES = 2 * 1024 * 1024;
    public enum Status { SUCCESS, RESULT_TOO_LARGE, CONFLICT, SESSION_EXPIRED }
    private final Status status;
    private final List<byte[]> values;

    public CommandResult(Status status, List<byte[]> values) {
        if (status == null || values == null) throw new NullPointerException();
        if (values.size() > 256 || (status != Status.SUCCESS && !values.isEmpty())) {
            throw new IllegalArgumentException("Invalid command result");
        }
        long bytes = 8;
        var owned = new ArrayList<byte[]>(values.size());
        for (byte[] value : values) {
            bytes += 4L + (value == null ? 0 : value.length);
            if (bytes > MAX_BYTES) throw new IllegalArgumentException("Result exceeds byte budget");
            owned.add(value == null ? null : value.clone());
        }
        this.status = status;
        this.values = Collections.unmodifiableList(owned);
    }
    public Status status() { return status; }
    public int valueCount() { return values.size(); }
    public byte[] value(int index) {
        byte[] value = values.get(index);
        return value == null ? null : value.clone();
    }
    public byte[] encode() {
        int bytes = 8;
        for (byte[] value : values) bytes += 4 + (value == null ? 0 : value.length);
        int code = switch (status) {
            case SUCCESS -> 0; case RESULT_TOO_LARGE -> 1; case CONFLICT -> 2; case SESSION_EXPIRED -> 3;
        };
        var output = ByteBuffer.allocate(bytes).putInt(code).putInt(values.size());
        for (byte[] value : values) {
            output.putInt(value == null ? -1 : value.length);
            if (value != null) output.put(value);
        }
        return output.array();
    }
    public static CommandResult decode(byte[] encoded) {
        if (encoded.length < 8 || encoded.length > MAX_BYTES) throw new IllegalArgumentException("Invalid result size");
        var input = ByteBuffer.wrap(encoded);
        var status = switch (input.getInt()) {
            case 0 -> Status.SUCCESS; case 1 -> Status.RESULT_TOO_LARGE; case 2 -> Status.CONFLICT; case 3 -> Status.SESSION_EXPIRED;
            default -> throw new IllegalArgumentException("Unknown result status");
        };
        int count = input.getInt();
        if (count < 0 || count > 256 || count > input.remaining() / 4
                || (status != Status.SUCCESS && count != 0)) throw new IllegalArgumentException("Invalid result count");
        var values = new ArrayList<byte[]>(count);
        for (int i = 0; i < count; i++) {
            if (input.remaining() < 4) throw new IllegalArgumentException("Truncated result");
            int length = input.getInt();
            if (length < -1 || length > input.remaining()) throw new IllegalArgumentException("Invalid result value length");
            byte[] value = length == -1 ? null : new byte[length];
            if (value != null) input.get(value);
            values.add(value);
        }
        if (input.hasRemaining()) throw new IllegalArgumentException("Trailing result bytes");
        return new CommandResult(status, values);
    }
}
