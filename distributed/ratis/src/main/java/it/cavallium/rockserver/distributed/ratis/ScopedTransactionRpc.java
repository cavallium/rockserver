package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.ApplyPosition;
import it.cavallium.rockserver.core.common.distributed.PhysicalRangePage;
import it.cavallium.rockserver.core.common.distributed.TransactionScope;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.Objects;
import java.util.UUID;

/** The current bounded query protocol for data and catalogue groups. */
public final class ScopedTransactionRpc {
    public static final int MAGIC = 0x52535458, HEADER_BYTES = 12, HANDLE_BYTES = 56;
    public static final int POINT = 1, STATE = 2, OPEN = 10, GET = 11, PAGE = 12, PLAN = 13, CLOSE = 14, STAGE = 15, PARTICIPANT_STATE = 20, PARTICIPANT_OUTCOME = 21;
    // A legal physical page continuation can exceed an atomic command's 2 MiB bound.
    public static final int MAX_REQUEST_BYTES = PhysicalRangePage.MAX_BYTES + 128;
    public static final int MAX_REPLY_BYTES = PhysicalRangePage.MAX_BYTES + 128;
    private static final Comparator<TransactionScope.Span> SPAN_ORDER = Comparator
            .comparing(TransactionScope.Span::columnId)
            .thenComparing(TransactionScope.Span::lowerInclusive, ScopedTransactionRpc::compareBounds)
            .thenComparing(TransactionScope.Span::isPoint)
            .thenComparing(TransactionScope.Span::upperExclusive, ScopedTransactionRpc::compareBounds);

    private ScopedTransactionRpc() {}

    public static byte[] pointRequest(UUID column,byte[] key) {
        Objects.requireNonNull(key); if (key.length>2*1024*1024-16) throw invalid("Point key exceeds budget");
        var body=ByteBuffer.allocate(20+key.length); uuid(body,column); putBlob(body,key); return frame(POINT,body.array());
    }
    public static byte[] pointValue(byte[] encoded,int maximum) {
        var packet=parse(encoded,MAX_REPLY_BYTES); if (packet.kind()!=POINT) throw invalid("Expected point reply");
        var input=ByteBuffer.wrap(packet.body()); var value=blob(input,maximum,true);
        if (input.hasRemaining()) throw invalid("Trailing point reply"); return value;
    }

    public record Packet(int kind, byte[] body) {
        public Packet {
            requireKind(kind);
            Objects.requireNonNull(body);
            if (body.length > MAX_REPLY_BYTES - HEADER_BYTES) throw invalid("Frame body exceeds budget");
            body = body.clone();
        }
        @Override public byte[] body() { return body.clone(); }
    }

    public record Handle(UUID owner, UUID id, long generation, ApplyPosition position) {
        public Handle {
            Objects.requireNonNull(owner); Objects.requireNonNull(id); Objects.requireNonNull(position);
            if (generation < 0 || position.term() <= 0) throw invalid("Invalid transaction handle position/generation");
        }
    }

    public static byte[] frame(int kind, byte[] body) {
        requireKind(kind); Objects.requireNonNull(body);
        if (body.length > MAX_REPLY_BYTES - HEADER_BYTES) throw invalid("Frame body exceeds budget");
        return ByteBuffer.allocate(HEADER_BYTES + body.length).putInt(MAGIC)
                .putInt(kind).putInt(body.length).put(body).array();
    }

    public static Packet parse(byte[] encoded, int maxBytes) {
        Objects.requireNonNull(encoded);
        if (maxBytes < HEADER_BYTES || maxBytes > MAX_REPLY_BYTES || encoded.length > maxBytes) {
            throw invalid("Invalid frame budget");
        }
        var input = ByteBuffer.wrap(encoded);
        require(input, HEADER_BYTES);
        if (input.getInt() != MAGIC) throw invalid("Unsupported read frame");
        int kind = input.getInt(), length = input.getInt();
        requireKind(kind);
        if (length < 0 || length != input.remaining()) throw invalid("Invalid frame body length");
        byte[] body = new byte[length]; input.get(body);
        return new Packet(kind, body);
    }

    public static void writeHandle(ByteBuffer output, Handle handle) {
        Objects.requireNonNull(handle); require(output, HANDLE_BYTES);
        uuid(output, handle.owner()); uuid(output, handle.id());
        output.putLong(handle.generation()).putLong(handle.position().term()).putLong(handle.position().index());
    }

    public static Handle readHandle(ByteBuffer input) {
        require(input, HANDLE_BYTES);
        UUID owner = uuid(input), id = uuid(input);
        long generation = input.getLong(), term = input.getLong(), index = input.getLong();
        return new Handle(owner, id, generation, new ApplyPosition(term, index));
    }

    public static UUID uuid(ByteBuffer input) {
        require(input, 16); return new UUID(input.getLong(), input.getLong());
    }

    public static void uuid(ByteBuffer output, UUID id) {
        Objects.requireNonNull(id); require(output, 16);
        output.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits());
    }

    public static byte[] blob(ByteBuffer input, int maximum, boolean nullable) {
        int length = blobLength(input, maximum, nullable);
        if (length < 0) return null;
        byte[] bytes = new byte[length]; input.get(bytes); return bytes;
    }

    public static void putBlob(ByteBuffer output, byte[] bytes) {
        require(output, 4);
        if (bytes == null) { output.putInt(-1); return; }
        require(output, 4L + bytes.length); output.putInt(bytes.length).put(bytes);
    }

    /** Canonical scope order includes all bounds and point/range identity, including equal lower bounds. */
    public static byte[] scopeBytes(TransactionScope scope) {
        Objects.requireNonNull(scope);
        var spans = scope.spans().stream().sorted(SPAN_ORDER).toList();
        long size = 4;
        for (var span : spans) size += 25L + length(span.lowerInclusive()) + length(span.upperExclusive());
        if (size > MAX_REQUEST_BYTES - HEADER_BYTES) throw invalid("Scope exceeds request budget");
        var output = ByteBuffer.allocate((int) size).putInt(spans.size());
        for (var span : spans) {
            uuid(output, span.columnId()); output.put((byte) (span.isPoint() ? 1 : 0));
            putBlob(output, span.lowerInclusive()); putBlob(output, span.upperExclusive());
        }
        return output.array();
    }

    /** Scope is the final field of its body. Preflight validates all lengths before allocating bound arrays. */
    public static TransactionScope decodeScope(ByteBuffer input) {
        require(input, 4);
        var preflight = input.duplicate().order(input.order());
        int count = preflight.getInt();
        if (count < 1 || count > TransactionScope.MAX_SPANS || 25L * count > preflight.remaining()) {
            throw invalid("Invalid scope span count");
        }
        long boundBytes = 0;
        for (int i = 0; i < count; i++) {
            require(preflight, 17); preflight.position(preflight.position() + 16);
            boolean point = flag(preflight);
            int lower = skipBlob(preflight, TransactionScope.MAX_BOUND_BYTES, !point);
            int upper = skipBlob(preflight, TransactionScope.MAX_BOUND_BYTES, true);
            if (point && upper >= 0) throw invalid("Point scope has an upper bound");
            boundBytes += Math.max(lower, 0) + (long) Math.max(upper, 0);
            if (boundBytes > TransactionScope.MAX_BOUND_BYTES) throw invalid("Scope bound budget exceeded");
        }
        if (preflight.hasRemaining()) throw invalid("Trailing scope data");
        input.getInt();
        var spans = new ArrayList<TransactionScope.Span>(count);
        TransactionScope.Span previous = null;
        for (int i = 0; i < count; i++) {
            UUID column = uuid(input); boolean point = flag(input);
            byte[] lower = blob(input, TransactionScope.MAX_BOUND_BYTES, !point);
            byte[] upper = blob(input, TransactionScope.MAX_BOUND_BYTES, true);
            var span = point ? TransactionScope.Span.point(column, lower) : TransactionScope.Span.range(column, lower, upper);
            if (previous != null && SPAN_ORDER.compare(previous, span) > 0) throw invalid("Noncanonical scope order");
            spans.add(span); previous = span;
        }
        return new TransactionScope(spans);
    }

    /** Continuation is derived from the final row; it cannot disagree with the rows on the wire. */
    public static byte[] pageBytes(PhysicalRangePage page) {
        Objects.requireNonNull(page);
        int size = 5;
        for (var row : page.rows()) size += 8 + row.key().length + row.value().length;
        var output = ByteBuffer.allocate(size).putInt(page.rows().size());
        for (var row : page.rows()) { putBlob(output, row.key()); putBlob(output, row.value()); }
        return output.put((byte) (page.hasMore() ? 1 : 0)).array();
    }

    public static PhysicalRangePage decodePage(byte[] encoded) {
        Objects.requireNonNull(encoded);
        if (encoded.length > PhysicalRangePage.MAX_BYTES) throw invalid("Page exceeds byte budget");
        var input = ByteBuffer.wrap(encoded); require(input, 5);
        int count = input.getInt();
        if (count < 0 || count > PhysicalRangePage.MAX_ITEMS || 8L * count + 1 > input.remaining()) {
            throw invalid("Invalid page row count");
        }
        // Validate every bound and the terminal flag before allocating row buffers.
        var preflight = input.duplicate().order(input.order());
        int resumeBytes = 0;
        for (int i = 0; i < count; i++) {
            resumeBytes = skipBlob(preflight, PhysicalRangePage.MAX_BYTES, false);
            skipBlob(preflight, PhysicalRangePage.MAX_BYTES, false);
        }
        if ((long) encoded.length + 4 + resumeBytes > PhysicalRangePage.MAX_BYTES) {
            throw invalid("Page continuation exceeds byte budget");
        }
        boolean hasMore = flag(preflight);
        if (preflight.hasRemaining() || count == 0 && hasMore) throw invalid("Invalid page continuation");
        var rows = new ArrayList<PhysicalRangePage.Row>(count);
        for (int i = 0; i < count; i++) rows.add(new PhysicalRangePage.Row(
                blob(input, PhysicalRangePage.MAX_BYTES, false), blob(input, PhysicalRangePage.MAX_BYTES, false)));
        flag(input);
        return new PhysicalRangePage(rows, hasMore);
    }

    private static int skipBlob(ByteBuffer input, int maximum, boolean nullable) {
        int length = blobLength(input, maximum, nullable);
        if (length >= 0) input.position(input.position() + length);
        return length;
    }
    private static int blobLength(ByteBuffer input, int maximum, boolean nullable) {
        if (maximum < 0) throw invalid("Invalid blob budget");
        require(input, 4); int length = input.getInt();
        if (length == -1 && nullable) return length;
        if (length < 0 || length > maximum || length > input.remaining()) throw invalid("Invalid blob length");
        return length;
    }
    private static boolean flag(ByteBuffer input) {
        require(input, 1); byte value = input.get();
        if (value != 0 && value != 1) throw invalid("Invalid boolean flag");
        return value == 1;
    }
    private static int length(byte[] bytes) { return bytes == null ? 0 : bytes.length; }
    private static int compareBounds(byte[] left, byte[] right) {
        if (left == right) return 0;
        if (left == null) return -1;
        if (right == null) return 1;
        return Arrays.compareUnsigned(left, right);
    }
    private static void requireKind(int kind) {
        if (kind != POINT && kind != STATE && kind != OPEN && kind != GET && kind != PAGE && kind != PLAN && kind != CLOSE && kind != STAGE && kind != PARTICIPANT_STATE && kind != PARTICIPANT_OUTCOME) {
            throw invalid("Unknown read frame kind");
        }
    }
    private static void require(ByteBuffer input, long bytes) {
        if (bytes < 0 || bytes > input.remaining()) throw invalid("Truncated RPC data");
    }
    private static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
}
