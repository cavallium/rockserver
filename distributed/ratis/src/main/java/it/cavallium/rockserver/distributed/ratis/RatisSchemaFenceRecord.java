package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Current DATA FENCE input; configured replicas require the complete immutable admission. */
public final class RatisSchemaFenceRecord {
    public static final int MAGIC=0x5253464e, MAX_BYTES=28+SchemaChange.MAX_STATUS_BYTES+SchemaAdmissionManifest.MAX_BYTES;
    private final SchemaChange.Header header;
    private final UUID groupId;
    private final byte[] admissions,admissionDigest;
    /** Explicit unqualified input for trusted native fixtures. */
    public RatisSchemaFenceRecord(SchemaChange.Header header,UUID groupId) { this(header,groupId,null); }
    public RatisSchemaFenceRecord(SchemaChange.Header header,UUID groupId,byte[] admissions) {
        this.header=Objects.requireNonNull(header); this.groupId=Objects.requireNonNull(groupId);
        if(header.participants().stream().noneMatch(p -> p.groupId().equals(groupId))) throw new IllegalArgumentException("Schema fence group is outside original membership");
        if(admissions!=null && (admissions.length==0 || admissions.length>SchemaAdmissionManifest.MAX_BYTES)) throw new IllegalArgumentException("Invalid schema admission size");
        this.admissions=admissions==null ? null : admissions.clone();
        admissionDigest=admissions==null ? null : OperatorUpload.sha256(this.admissions);
        if(28L+header.encode().length+(admissions==null ? 0 : admissions.length)>MAX_BYTES) throw new IllegalArgumentException("Schema fence exceeds bound");
    }
    public SchemaChange.Header header() { return header; }
    public UUID groupId() { return groupId; }
    public byte[] admissions() { return admissions==null ? null : admissions.clone(); }
    public byte[] admissionDigest() { return admissionDigest==null ? null : admissionDigest.clone(); }
    public byte[] encode() {
        byte[] bytes=header.encode(); var out=ByteBuffer.allocate(28+bytes.length+(admissions==null ? 0 : admissions.length)).putInt(MAGIC);
        ScopedTransactionRpc.uuid(out,groupId); ScopedTransactionRpc.putBlob(out,bytes); ScopedTransactionRpc.putBlob(out,admissions); return out.array();
    }
    public static boolean isFence(byte[] bytes) { return bytes!=null && bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static RatisSchemaFenceRecord decode(byte[] bytes) {
        if(bytes==null || bytes.length<28 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid schema fence size");
        var in=ByteBuffer.wrap(bytes); if(in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid schema fence magic");
        UUID group=ScopedTransactionRpc.uuid(in); var header=SchemaChange.Header.decode(ScopedTransactionRpc.blob(in,SchemaChange.MAX_STATUS_BYTES,false));
        byte[] admissions=ScopedTransactionRpc.blob(in,SchemaAdmissionManifest.MAX_BYTES,true);
        if(in.hasRemaining()) throw new IllegalArgumentException("Trailing schema fence record");
        return new RatisSchemaFenceRecord(header,group,admissions);
    }
    @Override public boolean equals(Object other) { return other instanceof RatisSchemaFenceRecord r && groupId.equals(r.groupId) && Arrays.equals(header.encode(),r.header.encode()) && Arrays.equals(admissions,r.admissions); }
    @Override public int hashCode() { return Objects.hash(groupId,Arrays.hashCode(header.encode()),Arrays.hashCode(admissions)); }
}
