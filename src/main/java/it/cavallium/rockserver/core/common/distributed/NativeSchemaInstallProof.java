package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Portable original INSTALL evidence; physical CF names remain local. */
public final class NativeSchemaInstallProof {
    public static final int BYTES=4+NativeSchemaFence.BYTES+32+16;
    private static final int MAGIC=0x4e534950;
    private final NativeSchemaFence fence;
    private final byte[] targetSchemaDigest;
    private final ApplyPosition position;
    public NativeSchemaInstallProof(NativeSchemaFence fence,byte[] targetSchemaDigest,ApplyPosition position) {
        this.fence=Objects.requireNonNull(fence); this.position=Objects.requireNonNull(position);
        if(targetSchemaDigest.length!=32 || position.index()<=fence.position().index()) throw new IllegalArgumentException("Invalid schema INSTALL proof");
        fence.requireCut(position); this.targetSchemaDigest=targetSchemaDigest.clone();
    }
    public NativeSchemaInstallProof(SchemaChange.Header header,UUID group,ApplyPosition fencePosition,ApplyPosition installPosition) {
        this(new NativeSchemaFence(header,group,fencePosition),header.targetSchemaDigest(),installPosition);
    }
    public NativeSchemaFence fence() { return fence; }
    public ApplyPosition position() { return position; }
    public byte[] targetSchemaDigest() { return targetSchemaDigest.clone(); }
    public void requireHeader(SchemaChange.Header header) {
        fence.requireHeader(header);
        if(!Arrays.equals(targetSchemaDigest,header.targetSchemaDigest())) throw new IllegalArgumentException("INSTALL target schema changed");
    }
    public void requireCut(ApplyPosition cut) {
        if(cut==null || position.index()>cut.index() || position.term()>cut.term() || position.index()==cut.index() && position.term()!=cut.term()) throw new IllegalArgumentException("INSTALL proof exceeds completed cut");
    }
    public byte[] encode() { return ByteBuffer.allocate(BYTES).putInt(MAGIC).put(fence.encode()).put(targetSchemaDigest).putLong(position.term()).putLong(position.index()).array(); }
    public static NativeSchemaInstallProof decode(byte[] encoded) {
        if(encoded==null || encoded.length!=BYTES) throw new IllegalArgumentException("Invalid INSTALL proof size");
        var in=ByteBuffer.wrap(encoded); if(in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid INSTALL proof tag");
        byte[] fence=new byte[NativeSchemaFence.BYTES],target=new byte[32]; in.get(fence).get(target);
        return new NativeSchemaInstallProof(NativeSchemaFence.decode(fence),target,new ApplyPosition(in.getLong(),in.getLong()));
    }
    @Override public boolean equals(Object other) { return other instanceof NativeSchemaInstallProof proof && fence.equals(proof.fence) && position.equals(proof.position) && Arrays.equals(targetSchemaDigest,proof.targetSchemaDigest); }
    @Override public int hashCode() { return Objects.hash(fence,position,Arrays.hashCode(targetSchemaDigest)); }
}
