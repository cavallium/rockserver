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
    public static final int POINT = 1, STATE = 2, OPEN = 10, GET = 11, PAGE = 12, PLAN = 13, CLOSE = 14, STAGE = 15, PARTICIPANT_STATE = 20, PARTICIPANT_OUTCOME = 21, SNAPSHOT_GATE_STATE = 22, FINALIZATION_PREPARE = 23, FINALIZATION_CANCEL = 24, PARTICIPANT_FENCE = 26, PARTICIPANT_RETIRE_PREPARE = 27, PARTICIPANT_RETIRE_CANCEL = 28, SNAPSHOT_OWNER = 30, SNAPSHOT_GET = 31, SNAPSHOT_PAGE = 32, SNAPSHOT_RELEASE = 33, SNAPSHOT_CAPTURE = 34, SNAPSHOT_READY = 35, SNAPSHOT_BLOCKERS = 36, SNAPSHOT_HELP = 37, SNAPSHOT_GATE_PREPARE = 38, SNAPSHOT_GATE_CANCEL = 39, HOME_RECOVERY_PREPARE = 40, HOME_RECOVERY_RESULT = 41, HOME_RECOVERY_CANCEL = 42, HOME_FENCE = 43, HOME_RETIRE_PREPARE = 44, HOME_RETIRE_CANCEL = 45, SNAPSHOT_FENCE = 46, SNAPSHOT_RETIRE_PREPARE = 47, SNAPSHOT_RETIRE_CANCEL = 48;
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

    public static int kind(byte[] encoded,int maxBytes) {
        Objects.requireNonNull(encoded);
        if (maxBytes<HEADER_BYTES || maxBytes>MAX_REPLY_BYTES || encoded.length>maxBytes) throw invalid("Invalid frame budget");
        var input=ByteBuffer.wrap(encoded); require(input,HEADER_BYTES);
        if (input.getInt()!=MAGIC) throw invalid("Unsupported read frame");
        int kind=input.getInt(),length=input.getInt(); requireKind(kind);
        if (length<0 || length!=input.remaining()) throw invalid("Invalid frame body length"); return kind;
    }
    public static Packet parse(byte[] encoded,int maxBytes) {
        int kind=kind(encoded,maxBytes); byte[] body=Arrays.copyOfRange(encoded,HEADER_BYTES,encoded.length); return new Packet(kind,body);
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

    public record SnapshotBinding(java.util.UUID fenceId,byte[] digest) {
        public SnapshotBinding { Objects.requireNonNull(fenceId); Objects.requireNonNull(digest); if (digest.length!=32) throw invalid("Invalid snapshot binding digest"); digest=digest.clone(); }
        public static SnapshotBinding of(it.cavallium.rockserver.core.common.distributed.SnapshotFence fence) {
            try { return new SnapshotBinding(fence.fenceId(),java.security.MessageDigest.getInstance("SHA-256").digest(fence.encode())); }
            catch (java.security.NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); }
        }
        @Override public byte[] digest() { return digest.clone(); }
        @Override public boolean equals(Object other) { return other instanceof SnapshotBinding binding && fenceId.equals(binding.fenceId) && Arrays.equals(digest,binding.digest); }
        @Override public int hashCode() { return Objects.hash(fenceId,Arrays.hashCode(digest)); }
    }
    public static void snapshotBinding(ByteBuffer output,SnapshotBinding binding) { uuid(output,binding.fenceId()); output.put(binding.digest()); }
    public static SnapshotBinding snapshotBinding(ByteBuffer input) { require(input,48); var id=uuid(input); byte[] digest=new byte[32]; input.get(digest); return new SnapshotBinding(id,digest); }
    public static final int SNAPSHOT_TOKEN_BYTES=48;
    public static boolean ownerSnapshot(int kind) { return kind==SNAPSHOT_CAPTURE || pinnedSnapshot(kind); }
    public static boolean pinnedSnapshot(int kind) { return kind==SNAPSHOT_GET || kind==SNAPSHOT_PAGE || kind==SNAPSHOT_RELEASE; }
    public static void snapshotToken(ByteBuffer output,it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus.Capture token) {
        uuid(output,token.ownerIncarnation()); uuid(output,token.snapshotId()); output.putLong(token.position().term()).putLong(token.position().index());
    }
    public static it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus.Capture snapshotToken(ByteBuffer input) {
        require(input,SNAPSHOT_TOKEN_BYTES);
        return new it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus.Capture(uuid(input),uuid(input),new ApplyPosition(input.getLong(),input.getLong()));
    }
    /** A null payload is explicit SNAPSHOT_LOST; a successful RELEASE has an empty payload. */
    public static byte[] snapshotReply(SnapshotBinding binding,it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus.Capture token,byte[] payload) {
        int size=48+SNAPSHOT_TOKEN_BYTES+1+(payload==null ? 0 : payload.length);
        if (size>MAX_REPLY_BYTES-HEADER_BYTES) throw invalid("Snapshot reply exceeds bound");
        var output=ByteBuffer.allocate(size); snapshotBinding(output,binding); snapshotToken(output,token); output.put((byte)(payload==null ? 1 : 0)); if (payload!=null) output.put(payload); return output.array();
    }
    public static byte[] snapshotPayload(byte[] bytes,SnapshotBinding expectedBinding,it.cavallium.rockserver.core.common.distributed.SnapshotFenceStatus.Capture expected) throws RatisSnapshotLostException {
        var input=ByteBuffer.wrap(bytes); var binding=snapshotBinding(input); var token=snapshotToken(input); require(input,1); int lost=input.get() & 255;
        if (!binding.equals(expectedBinding) || !token.equals(expected) || lost>1 || lost==1 && input.hasRemaining()) throw invalid("Snapshot reply token/status changed");
        if (lost==1) throw new RatisSnapshotLostException("Historical pin unavailable on its owner");
        byte[] payload=new byte[input.remaining()]; input.get(payload); return payload;
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
    public static boolean homeRecovery(int kind) { return kind==HOME_RECOVERY_PREPARE || kind==HOME_RECOVERY_RESULT || kind==HOME_RECOVERY_CANCEL; }
    private static void requireKind(int kind) {
        if (kind != POINT && kind != STATE && kind != OPEN && kind != GET && kind != PAGE && kind != PLAN && kind != CLOSE && kind != STAGE && kind != PARTICIPANT_STATE && kind != PARTICIPANT_OUTCOME && kind != PARTICIPANT_FENCE && kind != PARTICIPANT_RETIRE_PREPARE && kind != PARTICIPANT_RETIRE_CANCEL && kind != SNAPSHOT_GATE_STATE && kind != SNAPSHOT_FENCE && kind != SNAPSHOT_RETIRE_PREPARE && kind != SNAPSHOT_RETIRE_CANCEL && kind != FINALIZATION_PREPARE && kind != FINALIZATION_CANCEL && kind != HOME_FENCE && kind != HOME_RETIRE_PREPARE && kind != HOME_RETIRE_CANCEL && kind != SNAPSHOT_OWNER && kind != SNAPSHOT_READY && kind != SNAPSHOT_BLOCKERS && kind != SNAPSHOT_HELP && kind != SNAPSHOT_GATE_PREPARE && kind != SNAPSHOT_GATE_CANCEL && !homeRecovery(kind) && !ownerSnapshot(kind)) {
            throw invalid("Unknown read frame kind");
        }
    }
    private static void require(ByteBuffer input, long bytes) {
        if (bytes < 0 || bytes > input.remaining()) throw invalid("Truncated RPC data");
    }
    private static IllegalArgumentException invalid(String message) { return new IllegalArgumentException(message); }
}
