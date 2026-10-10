package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Common committed input. Each replica resolves its own durable birth during native apply. */
public final class RatisTopologyActivationRecord {
    public static final int MAGIC=0x52544143,MAX_BYTES=120+TopologyChange.MAX_DEFINITION_BYTES;
    private final TopologyChange.Definition definition;
    private final UUID group,catalogue;
    private final ApplyPosition catalogueCut;
    private final byte[] freeze,publication,encoded;
    public RatisTopologyActivationRecord(TopologyChange.Definition definition,UUID group,byte[] freeze,
            UUID catalogue,ApplyPosition catalogueCut,byte[] publication) {
        this.definition=Objects.requireNonNull(definition);this.group=Objects.requireNonNull(group);
        this.catalogue=Objects.requireNonNull(catalogue);this.catalogueCut=Objects.requireNonNull(catalogueCut);
        if(definition.kind()==TopologyChange.Kind.RELOCATE || catalogueCut.term()<1 || catalogueCut.index()<1
                || freeze==null || freeze.length!=32 || publication==null || publication.length!=32)
            throw new IllegalArgumentException("Invalid topology activation input");
        definition.header().participant(TopologyChange.Stage.ACTIVATE,group);
        if(!Arrays.equals(publication,NativeTopologyActivation.publicationCommitment(definition,freeze,catalogue)))
            throw new IllegalArgumentException("Topology publication commitment differs");
        this.freeze=freeze.clone();this.publication=publication.clone();byte[] body=definition.encode();
        var out=ByteBuffer.allocate(120+body.length).putInt(MAGIC);ScopedTransactionRpc.uuid(out,group);
        ScopedTransactionRpc.uuid(out,catalogue);out.putLong(catalogueCut.term()).putLong(catalogueCut.index());
        out.put(this.freeze).put(this.publication);ScopedTransactionRpc.putBlob(out,body);encoded=out.array();
    }
    /** Encoding a view is structural validation; fresh leader admission remains separately required. */
    public static RatisTopologyActivationRecord of(RatisTopologyAuthority.View view,UUID group) {
        view.require(RatisTopologyAuthority.Request.of(view.definition().header(),TopologyChange.Stage.ACTIVATE));
        byte[] freeze=view.state().freezeDigest();
        return new RatisTopologyActivationRecord(view.definition(),group,freeze,view.catalogue(),view.cut(),
                NativeTopologyActivation.publicationCommitment(view.definition(),freeze,view.catalogue()));
    }
    public TopologyChange.Definition definition(){return definition;}
    public TopologyChange.Header header(){return definition.header();}
    public UUID groupId(){return group;}
    public UUID catalogueGroup(){return catalogue;}
    public ApplyPosition catalogueCut(){return catalogueCut;}
    public byte[] freezeDigest(){return freeze.clone();}
    public byte[] publicationCommitment(){return publication.clone();}
    /** A new leader may qualify the saved input at a later cut of the same publication. */
    public void requireAuthority(RatisTopologyAuthority.View view){
        view.require(RatisTopologyAuthority.Request.of(header(),TopologyChange.Stage.ACTIVATE));
        if(!catalogue.equals(view.catalogue()) || !Arrays.equals(definition.encode(),view.definition().encode())
                || !Arrays.equals(freeze,view.state().freezeDigest()) || view.cut().index()<catalogueCut.index()
                || view.cut().term()<catalogueCut.term() || view.cut().index()==catalogueCut.index() && view.cut().term()!=catalogueCut.term())
            throw new IllegalArgumentException("Topology activation authority changed or precedes its publication cut");
    }
    public byte[] encode(){return encoded.clone();}
    public static boolean isActivation(byte[] bytes){return bytes!=null && bytes.length>=4 && ByteBuffer.wrap(bytes).getInt()==MAGIC;}
    public static RatisTopologyActivationRecord decode(byte[] bytes){
        if(bytes==null || bytes.length<120 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid topology activation record bound");
        var in=ByteBuffer.wrap(bytes);if(in.getInt()!=MAGIC)throw new IllegalArgumentException("Invalid topology activation tag");
        UUID group=ScopedTransactionRpc.uuid(in),catalogue=ScopedTransactionRpc.uuid(in);
        var cut=new ApplyPosition(in.getLong(),in.getLong());byte[] freeze=new byte[32],publication=new byte[32];in.get(freeze).get(publication);
        var definition=TopologyChange.Definition.decode(ScopedTransactionRpc.blob(in,TopologyChange.MAX_DEFINITION_BYTES,false));
        if(in.hasRemaining())throw new IllegalArgumentException("Trailing topology activation bytes");
        var record=new RatisTopologyActivationRecord(definition,group,freeze,catalogue,cut,publication);
        if(!Arrays.equals(bytes,record.encoded))throw new IllegalArgumentException("Noncanonical topology activation record");return record;
    }
    @Override public boolean equals(Object other){return other instanceof RatisTopologyActivationRecord record && Arrays.equals(encoded,record.encoded);}
    @Override public int hashCode(){return Arrays.hashCode(encoded);}
}
