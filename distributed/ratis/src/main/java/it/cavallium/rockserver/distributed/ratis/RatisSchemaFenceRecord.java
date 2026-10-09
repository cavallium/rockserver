package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.SchemaChange;
import java.nio.ByteBuffer;
import java.util.Arrays;
import java.util.Objects;
import java.util.UUID;

/** The current bounded DATA FENCE record, qualified by its exact administrative header and group. */
public record RatisSchemaFenceRecord(SchemaChange.Header header, UUID groupId) {
    public static final int MAGIC=0x5253464e, MAX_BYTES=16*1024;
    public RatisSchemaFenceRecord {
        Objects.requireNonNull(header); Objects.requireNonNull(groupId);
        if (header.participants().stream().noneMatch(p -> p.groupId().equals(groupId)))
            throw new IllegalArgumentException("Schema fence group is outside original membership");
        if (24L+header.encode().length>MAX_BYTES) throw new IllegalArgumentException("Schema fence exceeds bound");
    }
    public byte[] encode() {
        byte[] bytes=header.encode(); var out=ByteBuffer.allocate(24+bytes.length).putInt(MAGIC);
        ScopedTransactionRpc.uuid(out,groupId); ScopedTransactionRpc.putBlob(out,bytes); return out.array();
    }
    public static boolean isFence(byte[] bytes) { return bytes!=null && bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static RatisSchemaFenceRecord decode(byte[] bytes) {
        if (bytes==null || bytes.length<24 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid schema fence size");
        var in=ByteBuffer.wrap(bytes); if (in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid schema fence magic");
        UUID group=ScopedTransactionRpc.uuid(in);
        var header=SchemaChange.Header.decode(ScopedTransactionRpc.blob(in,MAX_BYTES-24,false));
        if (in.hasRemaining()) throw new IllegalArgumentException("Trailing schema fence record");
        return new RatisSchemaFenceRecord(header,group);
    }
    @Override public boolean equals(Object other) { return other instanceof RatisSchemaFenceRecord record && groupId.equals(record.groupId) && Arrays.equals(header.encode(),record.header.encode()); }
    @Override public int hashCode() { return Objects.hash(groupId,Arrays.hashCode(header.encode())); }
}
