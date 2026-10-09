package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Portable original ACTIVATE evidence bound to a qualified catalogue publication cut. */
public final class NativeSchemaActivationProof {
    public static final int BYTES=4+NativeSchemaInstallProof.BYTES+16+16+16;
    private static final int MAGIC=0x4e534150;
    private final NativeSchemaInstallProof installation;
    private final UUID catalogueGroup;
    private final ApplyPosition cataloguePosition,position;
    public NativeSchemaActivationProof(NativeSchemaInstallProof installation,UUID catalogueGroup,ApplyPosition cataloguePosition,ApplyPosition position) {
        this.installation=Objects.requireNonNull(installation); this.catalogueGroup=Objects.requireNonNull(catalogueGroup);
        this.cataloguePosition=Objects.requireNonNull(cataloguePosition); this.position=Objects.requireNonNull(position);
        if(cataloguePosition.term()<1 || position.index()<=installation.position().index()) throw new IllegalArgumentException("Invalid schema ACTIVATE proof");
        installation.requireCut(position);
    }
    public NativeSchemaInstallProof installation() { return installation; }
    public UUID catalogueGroup() { return catalogueGroup; }
    public ApplyPosition cataloguePosition() { return cataloguePosition; }
    public ApplyPosition position() { return position; }
    public void requireHeader(SchemaChange.Header header) { installation.requireHeader(header); }
    public void requireCut(ApplyPosition cut) {
        if(cut==null || position.index()>cut.index() || position.term()>cut.term() || position.index()==cut.index() && position.term()!=cut.term()) throw new IllegalArgumentException("ACTIVATE proof exceeds completed cut");
    }
    public byte[] encode() { return ByteBuffer.allocate(BYTES).putInt(MAGIC).put(installation.encode()).putLong(catalogueGroup.getMostSignificantBits()).putLong(catalogueGroup.getLeastSignificantBits()).putLong(cataloguePosition.term()).putLong(cataloguePosition.index()).putLong(position.term()).putLong(position.index()).array(); }
    public static NativeSchemaActivationProof decode(byte[] encoded) {
        if(encoded==null || encoded.length!=BYTES) throw new IllegalArgumentException("Invalid ACTIVATE proof size");
        var in=ByteBuffer.wrap(encoded); if(in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid ACTIVATE proof tag");
        byte[] installation=new byte[NativeSchemaInstallProof.BYTES]; in.get(installation);
        return new NativeSchemaActivationProof(NativeSchemaInstallProof.decode(installation),new UUID(in.getLong(),in.getLong()),new ApplyPosition(in.getLong(),in.getLong()),new ApplyPosition(in.getLong(),in.getLong()));
    }
    @Override public boolean equals(Object other) { return other instanceof NativeSchemaActivationProof proof && installation.equals(proof.installation) && catalogueGroup.equals(proof.catalogueGroup) && cataloguePosition.equals(proof.cataloguePosition) && position.equals(proof.position); }
    @Override public int hashCode() { return Objects.hash(installation,catalogueGroup,cataloguePosition,position); }
}
