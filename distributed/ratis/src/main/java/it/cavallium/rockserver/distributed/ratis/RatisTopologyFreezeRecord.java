package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Complete immutable FREEZE input for deterministic follower replay; no catalogue or owner RPC on decode. */
public final class RatisTopologyFreezeRecord {
    public static final int MAGIC=0x52544652,MAX_BYTES=56+TopologyChange.MAX_DEFINITION_BYTES;
    private final TopologyChange.Definition definition;
    private final TopologyChange.Header header;
    private final UUID group;
    private final byte[] admission,encoded;
    public RatisTopologyFreezeRecord(TopologyChange.Definition definition,UUID groupId,byte[] admissionDigest) {
        this.definition=Objects.requireNonNull(definition); this.group=Objects.requireNonNull(groupId); header=definition.header();
        if(definition.kind()==TopologyChange.Kind.RELOCATE || admissionDigest==null || admissionDigest.length!=32) throw new IllegalArgumentException("FREEZE requires split/merge and an exact admission commitment");
        header.participant(TopologyChange.Stage.FREEZE,groupId); admission=admissionDigest.clone(); byte[] body=definition.encode();
        var out=ByteBuffer.allocate(56+body.length).putInt(MAGIC); ScopedTransactionRpc.uuid(out,groupId); out.put(admission); ScopedTransactionRpc.putBlob(out,body); encoded=out.array();
    }
    public TopologyChange.Definition definition() { return definition; }
    public TopologyChange.Header header() { return header; }
    public UUID groupId() { return group; }
    public byte[] admissionDigest() { return admission.clone(); }
    public byte[] encode() { return encoded.clone(); }
    public static boolean isFreeze(byte[] bytes) { return bytes!=null && bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC; }
    public static RatisTopologyFreezeRecord decode(byte[] bytes) {
        if(bytes==null || bytes.length<56 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid topology FREEZE record bound");
        var in=ByteBuffer.wrap(bytes); if(in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid topology FREEZE tag");
        UUID group=ScopedTransactionRpc.uuid(in); byte[] admission=new byte[32]; in.get(admission); var definition=TopologyChange.Definition.decode(ScopedTransactionRpc.blob(in,TopologyChange.MAX_DEFINITION_BYTES,false));
        if(in.hasRemaining()) throw new IllegalArgumentException("Trailing topology FREEZE bytes"); var record=new RatisTopologyFreezeRecord(definition,group,admission);
        if(!Arrays.equals(bytes,record.encoded)) throw new IllegalArgumentException("Noncanonical topology FREEZE record"); return record;
    }
    /** This only validates a local artifact's binding, never replica membership/readiness authority. */
    public void requireFreeze(NativeTopologyFreeze freeze) {
        Objects.requireNonNull(freeze); freeze.requireRequest(header,admission);
        var parent=definition.parents().stream().filter(p->p.groupId().equals(group)).findFirst().orElseThrow(); var directory=definition.sourceDirectory();
        freeze.requireParent(group,parent.rangeEpoch(),directory.domainId(),directory.schemaDigest(),parent.lowerInclusive(),parent.upperExclusive());
    }
    @Override public boolean equals(Object other) { return other instanceof RatisTopologyFreezeRecord record && Arrays.equals(encoded,record.encoded); }
    @Override public int hashCode() { return Arrays.hashCode(encoded); }
}
