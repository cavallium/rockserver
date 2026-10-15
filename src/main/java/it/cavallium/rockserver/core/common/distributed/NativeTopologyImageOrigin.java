package it.cavallium.rockserver.core.common.distributed;

import java.nio.ByteBuffer;
import java.util.*;

/** Image-only full source provenance; compact local birth/activation receipts remain authoritative native rows. */
public final class NativeTopologyImageOrigin {
    private static final int MAGIC=0x4e54494f;
    public static final int MAX_BYTES=78+NativeTopologySeed.MAX_BYTES+NativeTopologyActivation.MAX_BYTES;
    private final NativeTopologySeed seed;
    private final NativeTopologyActivation activation;
    private final byte[] encoded;
    private final byte[] learnerAdoptionDigest;
    private final byte[] adoptedSchemaAnchorDigest;
    public NativeTopologyImageOrigin(NativeTopologySeed seed,NativeTopologyActivation activation) {this(seed,activation,null);}
    public NativeTopologyImageOrigin(NativeTopologySeed seed,NativeTopologyActivation activation,byte[] learnerAdoptionDigest) {
        this(seed,activation,learnerAdoptionDigest,null);
    }
    public NativeTopologyImageOrigin(NativeTopologySeed seed,NativeTopologyActivation activation,byte[] learnerAdoptionDigest,byte[] adoptedSchemaAnchorDigest) {
        if(adoptedSchemaAnchorDigest!=null && (learnerAdoptionDigest==null || adoptedSchemaAnchorDigest.length!=32))throw new IllegalArgumentException("Schema alias anchor lacks bounded adoption identity");
        this.adoptedSchemaAnchorDigest=adoptedSchemaAnchorDigest==null?null:adoptedSchemaAnchorDigest.clone();
        if(learnerAdoptionDigest!=null && learnerAdoptionDigest.length!=32)throw new IllegalArgumentException("Invalid learner adoption digest");this.learnerAdoptionDigest=learnerAdoptionDigest==null ? null : learnerAdoptionDigest.clone();
        this.seed=Objects.requireNonNull(seed); this.activation=Objects.requireNonNull(activation);
        activation.requireSeed(seed); activation.birth().resolveDescriptor(seed);
        requireCatalogueAdmission(seed,activation.catalogueGroup());
        byte[] recipe=seed.encode(),receipt=activation.encode();
        if(78L+recipe.length+receipt.length>MAX_BYTES) throw new IllegalArgumentException("Topology image origin exceeds bound");
        encoded=ByteBuffer.allocate(78+recipe.length+receipt.length).putInt(MAGIC).putInt(recipe.length).put(recipe).putInt(receipt.length).put(receipt).put((byte)(learnerAdoptionDigest==null ? 0 : 1)).put(learnerAdoptionDigest==null ? new byte[32] : learnerAdoptionDigest).put((byte)(adoptedSchemaAnchorDigest==null?0:1)).put(adoptedSchemaAnchorDigest==null?new byte[32]:adoptedSchemaAnchorDigest).array();
    }
    public byte[] adoptedSchemaAnchorDigest(){return adoptedSchemaAnchorDigest==null?null:adoptedSchemaAnchorDigest.clone();}
    public byte[] learnerAdoptionDigest() {return learnerAdoptionDigest==null ? null : learnerAdoptionDigest.clone();}
    public NativeTopologySeed seed() { return seed; }
    public NativeTopologyActivation activation() { return activation; }
    public void requireImage(UUID group,long epoch,UUID domain,byte[] lower,byte[] upper,ApplyPosition cut) {
        var target=activation.birth().resolveDescriptor(seed);
        if(!target.groupId().equals(group) || target.rangeEpoch()!=epoch || !seed.header().domainId().equals(domain)
                || !Arrays.equals(target.lowerInclusive(),lower) || !Arrays.equals(target.upperExclusive(),upper)) throw new IllegalArgumentException("Topology image differs from original target group/geometry");
        activation.requireCut(cut);
    }
    /** Derive this receiver's pointer and owner receipt while preserving the source's original committed activation position. */
    public NativeTopologyActivation localActivation(NativeTopologySeed localSeed) {
        Objects.requireNonNull(localSeed);
        if(!Arrays.equals(seed.definition().encode(),localSeed.definition().encode()) || !Arrays.equals(activation.commonFreezeDigest(),localSeed.authority().freezeDigest())) throw new IllegalArgumentException("Local topology image seed changes original definition/parent cuts");
        requireCatalogueAdmission(localSeed,activation.catalogueGroup());
        var birth=localSeed.birth(activation.birth().groupId());
        return new NativeTopologyActivation(localSeed,birth,activation.position(),activation.catalogueGroup(),activation.catalogueCut(),activation.publicationCommitment());
    }
    /** Qualified learner import preserves the original source owner/recipe; it never rebinds activation. */
    public void requireLearner(NativeTopologyLearnerRegistration registration) {
        Objects.requireNonNull(registration);if(!equals(registration.origin()))throw new IllegalArgumentException("Learner image changed original source provenance");
        var range=registration.definition().parents().getFirst();requireImage(range.groupId(),range.rangeEpoch(),registration.schema().domainId(),range.lowerInclusive(),range.upperExclusive(),registration.imageCut());
    }
    private static void requireCatalogueAdmission(NativeTopologySeed seed,UUID catalogue) {
        byte[] initial=TopologyChange.State.begin(seed.header()).beginFencing().encode();
        byte[] expected=OperatorUpload.sha256(ByteBuffer.allocate(16+initial.length).putLong(catalogue.getMostSignificantBits()).putLong(catalogue.getLeastSignificantBits()).put(initial).array());
        for(var parent:seed.parentFreezes().values()) parent.requireRequest(seed.header(),expected);
    }
    public byte[] encode() { return encoded.clone(); }
    public static NativeTopologyImageOrigin decode(byte[] bytes) {
        if(bytes==null || bytes.length<12 || bytes.length>MAX_BYTES) throw new IllegalArgumentException("Invalid topology image origin size");
        try {
            var in=ByteBuffer.wrap(bytes); if(in.getInt()!=MAGIC) throw new IllegalArgumentException("Invalid topology image origin tag");
            int recipe=in.getInt(); if(recipe<1 || recipe>NativeTopologySeed.MAX_BYTES || recipe>in.remaining()-4) throw new IllegalArgumentException("Invalid topology image seed length");
            byte[] seed=new byte[recipe]; in.get(seed);
            int receipt=in.getInt(); if(receipt<1 || receipt>NativeTopologyActivation.MAX_BYTES || receipt!=in.remaining()-66) throw new IllegalArgumentException("Invalid topology image activation length");
            byte[] activation=new byte[receipt]; in.get(activation);
            int present=in.get()&255;byte[] adoption=new byte[32];in.get(adoption);if(present>1 || present==0 && !Arrays.equals(adoption,new byte[32]))throw new IllegalArgumentException("Noncanonical adoption pointer");int anchored=in.get()&255;byte[] anchor=new byte[32];in.get(anchor);if(anchored>1 || anchored==0 && !Arrays.equals(anchor,new byte[32]))throw new IllegalArgumentException("Noncanonical schema alias anchor");var result=new NativeTopologyImageOrigin(NativeTopologySeed.decode(seed),NativeTopologyActivation.decode(activation),present==0 ? null : adoption,anchored==0?null:anchor);
            if(!Arrays.equals(bytes,result.encoded)) throw new IllegalArgumentException("Noncanonical topology image origin"); return result;
        } catch(java.nio.BufferUnderflowException invalid) { throw new IllegalArgumentException("Truncated topology image origin",invalid); }
    }
    @Override public boolean equals(Object other) { return other instanceof NativeTopologyImageOrigin origin && Arrays.equals(encoded,origin.encoded); }
    @Override public int hashCode() { return Arrays.hashCode(encoded); }
}
