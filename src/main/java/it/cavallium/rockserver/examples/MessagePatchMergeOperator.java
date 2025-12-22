package it.cavallium.rockserver.examples;

import it.cavallium.rockserver.core.impl.FFMByteArrayMergeOperator;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * Domain-aware merge operator that applies a MessagePatch operand to a stored Message value.
 *
 * Value format (Message):
 *  - ver: 1 byte (value 1)
 *  - id: int32
 *  - pinned: 1 byte (0/1)
 *  - updatedAt: int64
 *  - textLen: int32
 *  - text: bytes[textLen]
 *
 * Operand format (MessagePatch):
 *  - ver: 1 byte (value 1)
 *  - id: int32 (optional semantics; used when existing value is null)
 *  - flags: 1 byte (bit0 appendText, bit1 pinnedSet, bit2 updatedAt)
 *  - if appendText: int32 len + bytes
 *  - if pinnedSet: 1 byte (0/1)
 *  - if updatedAt: int64
 */
public class MessagePatchMergeOperator extends FFMByteArrayMergeOperator {

    public MessagePatchMergeOperator() {
        super("MessagePatchMergeOperator");
    }

    @Override
    public byte[] merge(byte[] key, byte[] existingValue, List<byte[]> operands) {
        Message current = decodeMessageSafe(existingValue);
        // If current is null, initialize with id from key or from first operand
        if (current == null) {
            int id = 0;
            if (key != null && key.length >= 4) {
                id = ByteBuffer.wrap(key, 0, 4).getInt();
            } else if (!operands.isEmpty()) {
                id = readIdFromPatch(operands.getFirst());
            }
            current = new Message(id, "", false, System.currentTimeMillis());
        }

        for (byte[] op : operands) {
            MessagePatch patch = decodePatchSafe(op);
            if (patch == null) continue; // ignore malformed patch
            // If base id is 0 and patch has an id, adopt it
            int id = current.id != 0 ? current.id : (patch.id != null ? patch.id : current.id);
            String text = current.text;
            if (patch.appendText != null) {
                text = (text != null ? text : "") + patch.appendText;
            }
            boolean pinned = patch.pinnedSet != null ? patch.pinnedSet : current.pinned;
            long updatedAt = patch.updatedAt != null ? patch.updatedAt : System.currentTimeMillis();
            current = new Message(id, text, pinned, updatedAt);
        }

        return encodeMessage(current);
    }

    // --- Internal model ---
    private record Message(int id, String text, boolean pinned, long updatedAt) {}
    private record MessagePatch(Integer id, String appendText, Boolean pinnedSet, Long updatedAt) {}

    // --- Encoding/decoding helpers (must match example formats) ---
    private static Message decodeMessageSafe(byte[] data) {
        if (data == null || data.length == 0) return null;
        try {
            ByteBuffer buf = ByteBuffer.wrap(data);
            int ver = Byte.toUnsignedInt(buf.get());
            if (ver != 1) return null;
            int id = buf.getInt();
            boolean pinned = buf.get() != 0;
            long updatedAt = buf.getLong();
            int len = buf.getInt();
            if (len < 0 || len > buf.remaining()) return null;
            byte[] t = new byte[len];
            buf.get(t);
            String text = new String(t, StandardCharsets.UTF_8);
            return new Message(id, text, pinned, updatedAt);
        } catch (Throwable ignored) {
            return null;
        }
    }

    private static int readIdFromPatch(byte[] data) {
        try {
            ByteBuffer buf = ByteBuffer.wrap(data);
            int ver = Byte.toUnsignedInt(buf.get());
            if (ver != 1) return 0;
            return buf.getInt();
        } catch (Throwable ignored) {
            return 0;
        }
    }

    private static MessagePatch decodePatchSafe(byte[] data) {
        if (data == null || data.length == 0) return null;
        try {
            ByteBuffer buf = ByteBuffer.wrap(data);
            int ver = Byte.toUnsignedInt(buf.get());
            if (ver != 1) return null;
            int id = buf.getInt();
            byte flags = buf.get();
            String append = null;
            Boolean pinned = null;
            Long ts = null;
            if ((flags & 0x1) != 0) { int len = buf.getInt(); byte[] b = new byte[len]; buf.get(b); append = new String(b, StandardCharsets.UTF_8); }
            if ((flags & 0x2) != 0) { pinned = buf.get() != 0; }
            if ((flags & 0x4) != 0) { ts = buf.getLong(); }
            return new MessagePatch(id, append, pinned, ts);
        } catch (Throwable ignored) {
            return null;
        }
    }

    private static byte[] encodeMessage(Message m) {
        byte[] textBytes = m.text != null ? m.text.getBytes(StandardCharsets.UTF_8) : new byte[0];
        ByteBuffer buf = ByteBuffer.allocate(1 + 4 + 1 + 8 + 4 + textBytes.length);
        buf.put((byte) 1);
        buf.putInt(m.id);
        buf.put((byte) (m.pinned ? 1 : 0));
        buf.putLong(m.updatedAt);
        buf.putInt(textBytes.length);
        buf.put(textBytes);
        return buf.array();
    }
}
