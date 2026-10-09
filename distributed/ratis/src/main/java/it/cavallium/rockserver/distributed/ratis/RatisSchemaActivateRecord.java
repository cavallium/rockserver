package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Activation binds original installation and a qualified catalogue publication cut. */
public record RatisSchemaActivateRecord(SchemaChange.Header header, UUID groupId, ApplyPosition expectedInstallPosition,
        UUID catalogueGroup, ApplyPosition cataloguePosition) {
    public static final int MAGIC=0x52534143, MAX_BYTES=16*1024;
    public RatisSchemaActivateRecord {
        Objects.requireNonNull(header); Objects.requireNonNull(groupId); Objects.requireNonNull(expectedInstallPosition);
        Objects.requireNonNull(catalogueGroup); Objects.requireNonNull(cataloguePosition);
        if (expectedInstallPosition.term()==0 || cataloguePosition.term()==0 || groupId.equals(catalogueGroup)
                || header.participants().stream().noneMatch(p -> p.groupId().equals(groupId))) throw new IllegalArgumentException("Invalid activation participant/cut");
        if (72L+header.encode().length>MAX_BYTES) throw new IllegalArgumentException("Activation exceeds bound");
    }
    public byte[] encode() {
        byte[] bytes=header.encode(); var out=ByteBuffer.allocate(72+bytes.length).putInt(MAGIC);
        ScopedTransactionRpc.uuid(out,groupId); out.putLong(expectedInstallPosition.term()).putLong(expectedInstallPosition.index());
        ScopedTransactionRpc.uuid(out,catalogueGroup); out.putLong(cataloguePosition.term()).putLong(cataloguePosition.index());
        ScopedTransactionRpc.putBlob(out,bytes); return out.array();
    }
    public static boolean isActivate(byte[] bytes) { return bytes!=null && bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static RatisSchemaActivateRecord decode(byte[] bytes) {
        if (bytes==null || bytes.length<72 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid activation record size");
        var in=ByteBuffer.wrap(bytes); if (in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid activation record magic");
        UUID group=ScopedTransactionRpc.uuid(in); var install=new ApplyPosition(in.getLong(),in.getLong());
        UUID catalogue=ScopedTransactionRpc.uuid(in); var cut=new ApplyPosition(in.getLong(),in.getLong());
        var header=SchemaChange.Header.decode(ScopedTransactionRpc.blob(in,MAX_BYTES-72,false));
        if (in.hasRemaining()) throw new IllegalArgumentException("Trailing activation record");
        return new RatisSchemaActivateRecord(header,group,install,catalogue,cut);
    }
    @Override public boolean equals(Object other) { return other instanceof RatisSchemaActivateRecord r && Arrays.equals(encode(),r.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
}
