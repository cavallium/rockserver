package it.cavallium.rockserver.core.common.distributed;

import java.io.*;
import java.util.*;

/** Receiver-local authority for one sealed source GROUP metadata image, never descendant USER ownership. */
public final class NativeMetadataRecoveryRegistration {
    public static final int MAX_BYTES=RangeDirectoryCodec.MAX_ENCODED_BYTES+NativeTopologyFreeze.MAX_BYTES+65536+16*RangeDirectoryCodec.MAX_COLUMNS+245;
    /** Retained authority for inventorying a replaced CURRENT image; never permission to install it. */
    public record CurrentPredecessor(byte[] registrationDigest,ApplyPosition imageCut,byte[] imageHash) {
        public CurrentPredecessor {if(registrationDigest==null || registrationDigest.length!=32 || imageHash==null || imageHash.length!=32 || imageCut==null || imageCut.term()<1 || imageCut.index()<0)throw new IllegalArgumentException("Invalid CURRENT predecessor");registrationDigest=registrationDigest.clone();imageHash=imageHash.clone();}
        public static CurrentPredecessor of(NativeCurrentRangeRecovery previous){return new CurrentPredecessor(previous.digest(),previous.imageCut(),previous.imageHash());}
        @Override public byte[] registrationDigest(){return registrationDigest.clone();}@Override public byte[] imageHash(){return imageHash.clone();}
        @Override public boolean equals(Object other){return other instanceof CurrentPredecessor p && imageCut.equals(p.imageCut) && Arrays.equals(registrationDigest,p.registrationDigest) && Arrays.equals(imageHash,p.imageHash);}
        @Override public int hashCode(){return Objects.hash(imageCut,Arrays.hashCode(registrationDigest),Arrays.hashCode(imageHash));}
    }
    private final CurrentPredecessor currentPredecessor;
    private final UUID transfer,catalogue;
    private final RangeDirectory directory;
    private final NativeTopologyFreeze freeze;
    private final ReplicaDescriptor receiver,source;
    private final ApplyPosition catalogueCut,imageCut;
    private final long configurationTerm,configurationIndex;
    private final byte[] configuration,imageHash,encoded,digest;
    private final Set<UUID> merges;

    public NativeMetadataRecoveryRegistration(UUID transfer,UUID catalogue,ApplyPosition catalogueCut,
            RangeDirectory sourceDirectory,NativeTopologyFreeze freeze,ReplicaDescriptor receiver,ReplicaDescriptor source,
            long configurationTerm,long configurationIndex,byte[] configuration,ApplyPosition imageCut,byte[] imageHash,Set<UUID> merges,CurrentPredecessor currentPredecessor) {
        this.currentPredecessor=currentPredecessor;
        this.transfer=Objects.requireNonNull(transfer);this.catalogue=Objects.requireNonNull(catalogue);this.catalogueCut=Objects.requireNonNull(catalogueCut);
        directory=Objects.requireNonNull(sourceDirectory);this.freeze=Objects.requireNonNull(freeze);this.receiver=Objects.requireNonNull(receiver);this.source=Objects.requireNonNull(source);this.imageCut=Objects.requireNonNull(imageCut);
        var header=freeze.header();var range=range();
        if(!directory.clusterId().equals(header.clusterId()) || !directory.databaseId().equals(header.databaseId()) || !directory.domainId().equals(header.domainId()) || directory.generation()!=header.sourceGeneration()
                || !Arrays.equals(directory.payloadDigest(),header.sourceDirectoryDigest()) || !Arrays.equals(directory.schemaDigest(),header.schemaDigest())
                || !TopologyChange.Participant.of(range).equals(header.participant(TopologyChange.Stage.FREEZE,freeze.groupId()))
                || !Arrays.equals(freeze.boundsDigest(),NativeTopologyFreeze.boundsDigest(range.lowerInclusive(),range.upperExclusive())))throw new IllegalArgumentException("Metadata source differs from original frozen directory");
        if(!range.replicas().contains(receiver) || !range.replicas().contains(source) || source.role()!=ReplicaDescriptor.Role.VOTER)throw new IllegalArgumentException("Metadata recovery owner is outside original source membership");
        freeze.requireCut(imageCut);if(currentPredecessor!=null && !covers(imageCut,currentPredecessor.imageCut()))throw new IllegalArgumentException("META image precedes CURRENT predecessor");
        if(catalogueCut.term()<1 || catalogueCut.index()<1 || imageCut.term()<1 || configuration==null || configuration.length<1 || configuration.length>65536 || configurationTerm<0 || configurationIndex< -1
                || configurationIndex== -1 && configurationTerm!=0 || configurationTerm>imageCut.term() || configurationIndex>imageCut.index() || configurationIndex==imageCut.index() && configurationTerm!=imageCut.term()
                || imageHash==null || imageHash.length!=32)throw new IllegalArgumentException("Invalid metadata image/configuration authority");
        this.configurationTerm=configurationTerm;this.configurationIndex=configurationIndex;this.configuration=configuration.clone();this.imageHash=imageHash.clone();this.merges=Set.copyOf(merges);
        if(!directory.columns().containsAll(this.merges))throw new IllegalArgumentException("Metadata merge projection differs from source");
        encoded=encodeFields();if(encoded.length>MAX_BYTES)throw new IllegalArgumentException("Metadata recovery registration exceeds bound");digest=RangeImportAdmission.sha256(encoded);
    }
    public CurrentPredecessor currentPredecessor(){return currentPredecessor;}
    public UUID transferId(){return transfer;}public UUID catalogue(){return catalogue;}public ApplyPosition catalogueCut(){return catalogueCut;}
    public RangeDirectory sourceDirectory(){return directory;}public NativeTopologyFreeze freeze(){return freeze;}public UUID groupId(){return freeze.groupId();}
    public RangeDescriptor range(){return directory.ranges().stream().filter(range->range.groupId().equals(freeze.groupId())).findFirst().orElseThrow(()->new IllegalArgumentException("Metadata source group is absent"));}
    public ReplicaDescriptor receiver(){return receiver;}public ReplicaDescriptor sourceOwner(){return source;}public ApplyPosition imageCut(){return imageCut;}
    public long configurationTerm(){return configurationTerm;}public long configurationIndex(){return configurationIndex;}public byte[] sourceConfiguration(){return configuration.clone();}
    public byte[] imageHash(){return imageHash.clone();}public Set<UUID> mergeColumns(){return merges;}public int encodedSize(){return encoded.length;}public byte[] encode(){return encoded.clone();}public byte[] digest(){return digest.clone();}
    /** A fresh sealed role may supersede CURRENT authority without claiming its image was installed. */
    public void requireTransitionOf(NativeCurrentRangeRecovery previous) {
        Objects.requireNonNull(previous);var before=previous.range();var after=range();var old=previous.receiver();
        if(!Objects.equals(currentPredecessor,CurrentPredecessor.of(previous)) || transfer.equals(previous.transferId()) || !catalogue.equals(previous.catalogue()) || !directory.clusterId().equals(previous.directory().clusterId()) || !directory.databaseId().equals(previous.directory().databaseId()) || !directory.domainId().equals(previous.directory().domainId()) || !groupId().equals(previous.groupId()) || !after.rangeId().equals(before.rangeId()) || after.rangeEpoch()!=before.rangeEpoch() || !Arrays.equals(after.lowerInclusive(),before.lowerInclusive()) || !Arrays.equals(after.upperExclusive(),before.upperExclusive())
                || !receiver.nodeId().equals(old.nodeId()) || !receiver.nodeIncarnation().equals(old.nodeIncarnation()) || !receiver.replicaId().equals(old.replicaId()) || !receiver.peerId().equals(old.peerId()) || !receiver.address().equals(old.address()) || !covers(catalogueCut,previous.catalogueCut()) || !covers(imageCut,previous.imageCut()))throw new IllegalArgumentException("META transition changed or regressed CURRENT identity/authority");
    }
    public void requireSuccessorOf(NativeMetadataRecoveryRegistration previous) {
        if(!Objects.equals(currentPredecessor,previous.currentPredecessor) || transfer.equals(previous.transfer) && !equals(previous) || !catalogue.equals(previous.catalogue) || !receiver.equals(previous.receiver) || !Arrays.equals(directory.payloadDigest(),previous.directory.payloadDigest()) || !freeze.equals(previous.freeze) || !merges.equals(previous.merges)
                || !covers(catalogueCut,previous.catalogueCut) || !covers(imageCut,previous.imageCut) || configurationIndex<previous.configurationIndex || configurationTerm<previous.configurationTerm
                || configurationIndex==previous.configurationIndex && (configurationTerm!=previous.configurationTerm || !Arrays.equals(configuration,previous.configuration))
                || imageCut.equals(previous.imageCut) && source.equals(previous.source) && !Arrays.equals(imageHash,previous.imageHash))throw new IllegalArgumentException("Metadata recovery changed or regressed original source authority");
    }
    public static boolean covers(ApplyPosition current,ApplyPosition previous){return current.term()>=previous.term() && current.index()>=previous.index() && (current.index()!=previous.index() || current.term()==previous.term());}
    private byte[] encodeFields(){try{var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);out.writeInt(0x4d445247);uuid(out,transfer);uuid(out,catalogue);cut(out,catalogueCut);blob(out,RangeDirectoryCodec.encode(directory));blob(out,freeze.encode());uuid(out,receiver.replicaId());uuid(out,source.replicaId());out.writeLong(configurationTerm);out.writeLong(configurationIndex);blob(out,configuration);cut(out,imageCut);out.write(imageHash);out.writeInt(merges.size());for(var id:merges.stream().sorted(RangeDirectoryCodec.UUID_ORDER).toList())uuid(out,id);out.writeBoolean(currentPredecessor!=null);if(currentPredecessor!=null){out.write(currentPredecessor.registrationDigest);cut(out,currentPredecessor.imageCut);out.write(currentPredecessor.imageHash);}out.flush();return bytes.toByteArray();}catch(IOException impossible){throw new AssertionError(impossible);}}
    public static NativeMetadataRecoveryRegistration decode(byte[] bytes) {
        if(bytes==null || bytes.length<161 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid metadata recovery registration length");
        try{var in=new DataInputStream(new ByteArrayInputStream(bytes));if(in.readInt()!=0x4d445247)throw new IOException("Invalid metadata recovery tag");var transfer=uuid(in);var catalogue=uuid(in);var catalogueCut=cut(in);var directory=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES));var freeze=NativeTopologyFreeze.decode(blob(in,NativeTopologyFreeze.MAX_BYTES));var range=directory.ranges().stream().filter(r->r.groupId().equals(freeze.groupId())).findFirst().orElseThrow();var receiver=member(range,uuid(in));var source=member(range,uuid(in));long term=in.readLong(),index=in.readLong();byte[] cfg=blob(in,65536);var imageCut=cut(in);byte[] hash=in.readNBytes(32);int count=in.readInt();if(count<0 || count>RangeDirectoryCodec.MAX_COLUMNS || count*16L>in.available())throw new IOException("Invalid metadata merge count");var merges=new HashSet<UUID>();for(int n=0;n<count;n++)if(!merges.add(uuid(in)))throw new IOException("Duplicate metadata merge column");int present=in.readUnsignedByte();if(present>1)throw new IOException("Invalid CURRENT predecessor presence");CurrentPredecessor previous=present==0?null:new CurrentPredecessor(in.readNBytes(32),cut(in),in.readNBytes(32));var result=new NativeMetadataRecoveryRegistration(transfer,catalogue,catalogueCut,directory,freeze,receiver,source,term,index,cfg,imageCut,hash,merges,previous);if(in.available()!=0 || !Arrays.equals(bytes,result.encoded))throw new IOException("Noncanonical metadata recovery registration");return result;}
        catch(IOException|NoSuchElementException invalid){throw new IllegalArgumentException("Invalid metadata recovery registration",invalid);}
    }
    private static ReplicaDescriptor member(RangeDescriptor range,UUID replica){return range.replicas().stream().filter(value->value.replicaId().equals(replica)).findFirst().orElseThrow();}
    private static void blob(DataOutputStream out,byte[] value)throws IOException{out.writeInt(value.length);out.write(value);}private static byte[] blob(DataInputStream in,int maximum)throws IOException{int size=in.readInt();if(size<0 || size>maximum || size>in.available())throw new IOException("Metadata recovery field exceeds bound");return in.readNBytes(size);}
    private static void uuid(DataOutputStream out,UUID id)throws IOException{out.writeLong(id.getMostSignificantBits());out.writeLong(id.getLeastSignificantBits());}private static UUID uuid(DataInputStream in)throws IOException{return new UUID(in.readLong(),in.readLong());}
    private static void cut(DataOutputStream out,ApplyPosition cut)throws IOException{out.writeLong(cut.term());out.writeLong(cut.index());}private static ApplyPosition cut(DataInputStream in)throws IOException{return new ApplyPosition(in.readLong(),in.readLong());}
    @Override public boolean equals(Object other){return other instanceof NativeMetadataRecoveryRegistration r && Arrays.equals(encoded,r.encoded);}@Override public int hashCode(){return Arrays.hashCode(encoded);}
}
