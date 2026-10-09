package it.cavallium.rockserver.distributed.ratis;

import java.nio.ByteBuffer;
import java.util.Objects;
import java.util.UUID;

/** Permanent global UUID/API ownership, including the first immutable schema that issued a column. */
public final class RatisColumnIdentities {
    public static final int VALUE_BYTES=72;
    private RatisColumnIdentities() {}
    public static byte[] columnKey(UUID column) {
        Objects.requireNonNull(column); var out=ByteBuffer.allocate(17).put((byte)0x55); uuid(out,column); return out.array();
    }
    public static byte[] apiKey(long apiId) {
        if (apiId<=0) throw new IllegalArgumentException("Column API ID must be positive");
        return ByteBuffer.allocate(9).put((byte)0x49).putLong(apiId).array();
    }
    public static byte[] value(UUID domain,UUID column,long apiId,byte[] firstIssuedSchemaDigest) {
        Objects.requireNonNull(domain); Objects.requireNonNull(column);
        if (apiId<=0 || firstIssuedSchemaDigest==null || firstIssuedSchemaDigest.length!=32) throw new IllegalArgumentException("Invalid column issuance identity");
        var out=ByteBuffer.allocate(VALUE_BYTES); uuid(out,domain); uuid(out,column); out.putLong(apiId).put(firstIssuedSchemaDigest); return out.array();
    }
    public static byte[] requireOwned(byte[] value,UUID domain,UUID column,long apiId) {
        Objects.requireNonNull(domain); Objects.requireNonNull(column);
        if (apiId<=0 || value==null || value.length!=VALUE_BYTES) throw new IllegalArgumentException("Invalid column ownership marker");
        var in=ByteBuffer.wrap(value);
        if (!uuid(in).equals(domain) || !uuid(in).equals(column) || in.getLong()!=apiId) throw new IllegalArgumentException("Column identity belongs to another owner");
        byte[] digest=new byte[32]; in.get(digest); return digest;
    }
    private static void uuid(ByteBuffer out,UUID id) { out.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
    private static UUID uuid(ByteBuffer in) { return new UUID(in.getLong(),in.getLong()); }
}
