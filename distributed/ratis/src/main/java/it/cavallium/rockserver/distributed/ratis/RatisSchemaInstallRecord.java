package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.*;

/** Full current installation input; unchanged physical bindings are derived from native pool births. */
public record RatisSchemaInstallRecord(SchemaChange.Definition definition, UUID groupId,
        ApplyPosition expectedFencePosition, String changedPhysicalName, boolean mergeAdmission) {
    public static final int MAGIC=0x5253494e, MAX_BYTES=2*1024*1024-ScopedTransactionRpc.HEADER_BYTES;
    public RatisSchemaInstallRecord {
        Objects.requireNonNull(definition); Objects.requireNonNull(groupId); Objects.requireNonNull(expectedFencePosition);
        if (expectedFencePosition.term()==0 || definition.header().participants().stream().noneMatch(p -> p.groupId().equals(groupId)))
            throw new IllegalArgumentException("Invalid installation fence/group");
        if (definition.header().kind()==SchemaChange.Kind.CREATE_COLUMN) {
            if (changedPhysicalName==null || changedPhysicalName.isEmpty() || changedPhysicalName.length()>4096
                    || changedPhysicalName.indexOf('\0')>=0 || !StandardCharsets.US_ASCII.newEncoder().canEncode(changedPhysicalName))
                throw new IllegalArgumentException("Invalid changed column physical name");
        } else if (changedPhysicalName!=null || mergeAdmission) throw new IllegalArgumentException("Deletion carries no physical binding");
        if (45L+definition.encode().length+(changedPhysicalName==null ? 0 : changedPhysicalName.length())>MAX_BYTES)
            throw new IllegalArgumentException("Installation record exceeds command bound");
    }
    public SchemaChange.Header header() { return definition.header(); }
    public byte[] encode() {
        byte[] definitionBytes=definition.encode(),name=changedPhysicalName==null ? null : changedPhysicalName.getBytes(StandardCharsets.US_ASCII);
        var out=ByteBuffer.allocate(45+definitionBytes.length+(name==null ? 0 : name.length)).putInt(MAGIC);
        ScopedTransactionRpc.uuid(out,groupId); out.putLong(expectedFencePosition.term()).putLong(expectedFencePosition.index());
        ScopedTransactionRpc.putBlob(out,definitionBytes); ScopedTransactionRpc.putBlob(out,name); out.put((byte)(mergeAdmission ? 1 : 0)); return out.array();
    }
    public static boolean isInstall(byte[] bytes) { return bytes!=null && bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static RatisSchemaInstallRecord decode(byte[] bytes) {
        if (bytes==null || bytes.length<45 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid installation record size");
        var in=ByteBuffer.wrap(bytes); if (in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid installation record magic");
        UUID group=ScopedTransactionRpc.uuid(in); var fence=new ApplyPosition(in.getLong(),in.getLong());
        var definition=SchemaChange.Definition.decode(ScopedTransactionRpc.blob(in,SchemaChange.MAX_DEFINITION_BYTES,false));
        byte[] name=ScopedTransactionRpc.blob(in,4096,true);
        if (in.remaining()!=1) throw new IllegalArgumentException("Invalid installation record tail"); int merge=in.get() & 255;
        if (merge>1) throw new IllegalArgumentException("Invalid merge admission flag");
        var result=new RatisSchemaInstallRecord(definition,group,fence,name==null ? null : new String(name,StandardCharsets.US_ASCII),merge==1);
        if (!Arrays.equals(bytes,result.encode())) throw new IllegalArgumentException("Noncanonical installation record"); return result;
    }
    @Override public boolean equals(Object other) { return other instanceof RatisSchemaInstallRecord r && Arrays.equals(encode(),r.encode()); }
    @Override public int hashCode() { return Arrays.hashCode(encode()); }
}
