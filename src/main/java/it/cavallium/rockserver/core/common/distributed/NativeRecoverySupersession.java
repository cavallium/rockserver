package it.cavallium.rockserver.core.common.distributed;

import java.io.*;
import java.nio.ByteBuffer;
import java.util.*;

/** Durable receiver fence for replacing obsolete CURRENT authority; never evidence of image readiness. */
public final class NativeRecoverySupersession {
    public static final int MAX_SCOPE_BYTES=RangeDirectoryCodec.MAX_ENCODED_BYTES+TopologyChange.MAX_HEADER_BYTES+192;
    public static final int MAX_BYTES=MAX_SCOPE_BYTES+304;
    private static final int MAGIC=0x4e525350,SCOPE_MAGIC=0x4e525353;

    public record Scope(UUID catalogue,ApplyPosition catalogueCut,TopologyChange.Header header,RangeDirectory sourceDirectory,
                        UUID groupId,ReplicaDescriptor receiver,byte[] selectedMetadataRequestDigest,
                        byte[] originalCurrentRequestDigest,byte[] effectiveCurrentRequestDigest) {
        public Scope {
            Objects.requireNonNull(catalogue);positive(catalogueCut);Objects.requireNonNull(header);Objects.requireNonNull(sourceDirectory);Objects.requireNonNull(groupId);Objects.requireNonNull(receiver);
            selectedMetadataRequestDigest=exact(selectedMetadataRequestDigest,32);originalCurrentRequestDigest=exact(originalCurrentRequestDigest,32);effectiveCurrentRequestDigest=exact(effectiveCurrentRequestDigest,32);
            var range=find(sourceDirectory,groupId);
            if(header.kind()==TopologyChange.Kind.RELOCATE || !header.clusterId().equals(sourceDirectory.clusterId()) || !header.databaseId().equals(sourceDirectory.databaseId())
                    || !header.domainId().equals(sourceDirectory.domainId()) || header.schemaEpoch()!=sourceDirectory.schemaEpoch() || header.sourceGeneration()!=sourceDirectory.generation()
                    || !Arrays.equals(header.sourceDirectoryDigest(),sourceDirectory.payloadDigest()) || !Arrays.equals(header.schemaDigest(),sourceDirectory.schemaDigest())
                    || !header.participant(TopologyChange.Stage.FREEZE,groupId).equals(TopologyChange.Participant.of(range)) || !range.replicas().contains(receiver))
                throw new IllegalArgumentException("Supersession differs from original frozen source scope");
        }
        @Override public byte[] selectedMetadataRequestDigest(){return selectedMetadataRequestDigest.clone();}
        @Override public byte[] originalCurrentRequestDigest(){return originalCurrentRequestDigest.clone();}
        @Override public byte[] effectiveCurrentRequestDigest(){return effectiveCurrentRequestDigest.clone();}
        public RangeDescriptor range(){return find(sourceDirectory,groupId);}
        /** Validates the original CURRENT owner before any irreversible local fence. */
        public void requireCurrent(NativeCurrentRangeRecovery current){
            Objects.requireNonNull(current);var before=current.range();var after=range();var old=current.receiver();var directory=current.directory();
            if(!catalogue.equals(current.catalogue()) || !groupId.equals(current.groupId()) || !after.rangeId().equals(before.rangeId()) || after.rangeEpoch()!=before.rangeEpoch()
                    || !sourceDirectory.clusterId().equals(directory.clusterId()) || !sourceDirectory.databaseId().equals(directory.databaseId()) || !sourceDirectory.domainId().equals(directory.domainId())
                    || !Arrays.equals(after.lowerInclusive(),before.lowerInclusive()) || !Arrays.equals(after.upperExclusive(),before.upperExclusive())
                    || !receiver.replicaId().equals(old.replicaId()) || !receiver.nodeId().equals(old.nodeId()) || !receiver.nodeIncarnation().equals(old.nodeIncarnation())
                    || !receiver.peerId().equals(old.peerId()) || !receiver.address().equals(old.address())
                    || !NativeMetadataRecoveryRegistration.covers(catalogueCut,current.catalogueCut()) || sourceDirectory.generation()<directory.generation())
                throw new IllegalArgumentException("Supersession changed or regressed original CURRENT ownership/authority");
        }
        public int encodedSize(){return Math.toIntExact(Math.addExact(172L,Math.addExact(header.encodedSize(),RangeDirectoryCodec.encodedSize(sourceDirectory))));}
        public byte[] encode(){return write(out->{out.writeInt(SCOPE_MAGIC);uuid(out,catalogue);cut(out,catalogueCut);blob(out,header.encode());blob(out,RangeDirectoryCodec.encode(sourceDirectory));uuid(out,groupId);uuid(out,receiver.replicaId());out.write(selectedMetadataRequestDigest);out.write(originalCurrentRequestDigest);out.write(effectiveCurrentRequestDigest);},MAX_SCOPE_BYTES);}
        public byte[] digest(){return RangeImportAdmission.sha256(encode());}
        public static Scope decode(byte[] bytes){bounded(bytes,MAX_SCOPE_BYTES);try{var in=input(bytes,SCOPE_MAGIC);var catalogue=uuid(in);var cat=cut(in);var header=TopologyChange.Header.decode(blob(in,TopologyChange.MAX_HEADER_BYTES));var directory=RangeDirectoryCodec.decode(blob(in,RangeDirectoryCodec.MAX_ENCODED_BYTES));var group=uuid(in);var replica=uuid(in);var receiver=find(directory,group).replicas().stream().filter(r->r.replicaId().equals(replica)).findFirst().orElseThrow();var result=new Scope(catalogue,cat,header,directory,group,receiver,in.readNBytes(32),in.readNBytes(32),in.readNBytes(32));canonical(in,bytes,result.encode());return result;}catch(IOException|NoSuchElementException bad){throw new IllegalArgumentException("Invalid supersession scope",bad);}}
        @Override public boolean equals(Object other){return other instanceof Scope scope && Arrays.equals(encode(),scope.encode());}
        @Override public int hashCode(){return Arrays.hashCode(encode());}
    }

    /** A factual configuration tuple/hash, including bootstrap (0,-1); not an applied-prefix proof. */
    public record ConfigurationReference(long term,long index,byte[] digest) {
        public ConfigurationReference {if(term<0 || index< -1 || index== -1 && term!=0)throw new IllegalArgumentException("Invalid captured configuration tuple");digest=exact(digest,32);}
        @Override public byte[] digest(){return digest.clone();}
        @Override public boolean equals(Object other){return other instanceof ConfigurationReference ref && term==ref.term && index==ref.index && Arrays.equals(digest,ref.digest);}
        @Override public int hashCode(){return Objects.hash(term,index,Arrays.hashCode(digest));}
    }

    /** Independent durable observations. None of these fields imply installed, completed or ready. */
    public record CapturedState(byte[] currentRegistrationDigest,ApplyPosition registeredImageCut,byte[] registeredImageHash,
                                byte[] localImageReceipt,byte[] pendingIntent,ApplyPosition marker,ConfigurationReference configuration) {
        public CapturedState {
            boolean registered=currentRegistrationDigest!=null;
            if(registered!=(registeredImageCut!=null) || registered!=(registeredImageHash!=null))throw new IllegalArgumentException("Captured CURRENT registration/image must be coupled");
            if(registered){currentRegistrationDigest=exact(currentRegistrationDigest,32);positive(registeredImageCut);registeredImageHash=exact(registeredImageHash,32);}
            if(localImageReceipt!=null){localImageReceipt=exact(localImageReceipt,48);receiptCut(localImageReceipt);}
            if(pendingIntent!=null){pendingIntent=exact(pendingIntent,48);receiptCut(pendingIntent);}
            if(marker!=null)positive(marker);
        }
        @Override public byte[] currentRegistrationDigest(){return copy(currentRegistrationDigest);}
        @Override public byte[] registeredImageHash(){return copy(registeredImageHash);}
        @Override public byte[] localImageReceipt(){return copy(localImageReceipt);}
        @Override public byte[] pendingIntent(){return copy(pendingIntent);}
        @Override public boolean equals(Object other){return other instanceof CapturedState c && Arrays.equals(currentRegistrationDigest,c.currentRegistrationDigest) && Objects.equals(registeredImageCut,c.registeredImageCut) && Arrays.equals(registeredImageHash,c.registeredImageHash) && Arrays.equals(localImageReceipt,c.localImageReceipt) && Arrays.equals(pendingIntent,c.pendingIntent) && Objects.equals(marker,c.marker) && Objects.equals(configuration,c.configuration);}
        @Override public int hashCode(){return Objects.hash(Arrays.hashCode(currentRegistrationDigest),registeredImageCut,Arrays.hashCode(registeredImageHash),Arrays.hashCode(localImageReceipt),Arrays.hashCode(pendingIntent),marker,configuration);}
    }

    private final Scope scope;
    private final CapturedState captured;
    private final byte[] encoded,digest,scopeDigest;
    public NativeRecoverySupersession(Scope scope,CapturedState captured){this.scope=Objects.requireNonNull(scope);this.captured=Objects.requireNonNull(captured);encoded=encodeFields();digest=RangeImportAdmission.sha256(encoded);scopeDigest=scope.digest();}
    public Scope scope(){return scope;}public CapturedState captured(){return captured;}public UUID groupId(){return scope.groupId();}public RangeDescriptor range(){return scope.range();}public ReplicaDescriptor receiver(){return scope.receiver();}public RangeDirectory sourceDirectory(){return scope.sourceDirectory();}
    public byte[] scopeDigest(){return scopeDigest.clone();}
    public int encodedSize(){return encoded.length;}public byte[] encode(){return encoded.clone();}public byte[] digest(){return digest.clone();}

    public void requireCurrent(NativeCurrentRangeRecovery current){scope.requireCurrent(current);}

    /** Checks retained immutable role authority without reclassifying its already-applied prefix. */
    public void requireMetadataScope(NativeMetadataRecoveryRegistration metadata) {
        Objects.requireNonNull(metadata);
        if(!scope.catalogue.equals(metadata.catalogue()) || !scope.groupId.equals(metadata.groupId()) || !scope.receiver.equals(metadata.receiver())
                || !Arrays.equals(scope.header.encode(),metadata.freeze().header().encode()) || !Arrays.equals(scope.sourceDirectory.payloadDigest(),metadata.sourceDirectory().payloadDigest()))throw new IllegalArgumentException("META changed supersession scope");
        if(captured.currentRegistrationDigest!=null){var previous=metadata.currentPredecessor();if(previous==null || !Arrays.equals(previous.registrationDigest(),captured.currentRegistrationDigest) || !previous.imageCut().equals(captured.registeredImageCut) || !Arrays.equals(previous.imageHash(),captured.registeredImageHash))throw new IllegalArgumentException("META changed captured CURRENT predecessor");}
    }
    /** Qualifies NEW META authority and coverage, independently of successor request UUIDs. */
    public void requireMetadata(NativeMetadataRecoveryRegistration metadata) {
        requireMetadataScope(metadata);
        if(!NativeMetadataRecoveryRegistration.covers(metadata.catalogueCut(),scope.catalogueCut))throw new IllegalArgumentException("META precedes supersession catalogue cut");
        requireCovered(metadata.imageCut(),captured.marker);requireCovered(metadata.imageCut(),captured.registeredImageCut);
        if(captured.localImageReceipt!=null)requireCovered(metadata.imageCut(),receiptCut(captured.localImageReceipt));
        if(captured.pendingIntent!=null)requireCovered(metadata.imageCut(),receiptCut(captured.pendingIntent));
        var cfg=captured.configuration;
        if(cfg!=null){if(cfg.index>=0)requireCovered(metadata.imageCut(),new ApplyPosition(cfg.term,cfg.index));if(metadata.configurationIndex()==cfg.index && (metadata.configurationTerm()!=cfg.term || !Arrays.equals(RangeImportAdmission.sha256(metadata.sourceConfiguration()),cfg.digest)))throw new IllegalArgumentException("META changed captured configuration at the same tuple");}
    }
    private byte[] encodeFields(){return write(out->{out.writeInt(MAGIC);blob(out,scope.encode());int fields=(captured.currentRegistrationDigest!=null?1:0)|(captured.localImageReceipt!=null?2:0)|(captured.pendingIntent!=null?4:0)|(captured.marker!=null?8:0)|(captured.configuration!=null?16:0);out.writeByte(fields);if((fields&1)!=0){out.write(captured.currentRegistrationDigest);cut(out,captured.registeredImageCut);out.write(captured.registeredImageHash);}if((fields&2)!=0)out.write(captured.localImageReceipt);if((fields&4)!=0)out.write(captured.pendingIntent);if((fields&8)!=0)cut(out,captured.marker);if((fields&16)!=0){out.writeLong(captured.configuration.term);out.writeLong(captured.configuration.index);out.write(captured.configuration.digest);}},MAX_BYTES);}
    public static NativeRecoverySupersession decode(byte[] bytes){bounded(bytes,MAX_BYTES);try{var in=input(bytes,MAGIC);var scope=Scope.decode(blob(in,MAX_SCOPE_BYTES));int fields=in.readUnsignedByte();if((fields&~31)!=0)throw new IOException("Unknown captured fields");byte[] registration=null,hash=null,receipt=null,pending=null;ApplyPosition registered=null,marker=null;ConfigurationReference cfg=null;if((fields&1)!=0){registration=in.readNBytes(32);registered=cut(in);hash=in.readNBytes(32);}if((fields&2)!=0)receipt=in.readNBytes(48);if((fields&4)!=0)pending=in.readNBytes(48);if((fields&8)!=0)marker=cut(in);if((fields&16)!=0)cfg=new ConfigurationReference(in.readLong(),in.readLong(),in.readNBytes(32));var result=new NativeRecoverySupersession(scope,new CapturedState(registration,registered,hash,receipt,pending,marker,cfg));canonical(in,bytes,result.encoded);return result;}catch(IOException bad){throw new IllegalArgumentException("Invalid recovery supersession",bad);}}
    @Override public boolean equals(Object other){return other instanceof NativeRecoverySupersession value && Arrays.equals(encoded,value.encoded);}@Override public int hashCode(){return Arrays.hashCode(encoded);}
    private static void requireCovered(ApplyPosition target,ApplyPosition floor){if(floor!=null && !NativeMetadataRecoveryRegistration.covers(target,floor))throw new IllegalArgumentException("META precedes captured native state");}
    private static ApplyPosition receiptCut(byte[] receipt){var in=ByteBuffer.wrap(receipt);var cut=new ApplyPosition(in.getLong(),in.getLong());positive(cut);return cut;}
    private static void positive(ApplyPosition cut){if(cut==null || cut.term()<1)throw new IllegalArgumentException("Invalid captured cut");}
    private static RangeDescriptor find(RangeDirectory directory,UUID group){return directory.ranges().stream().filter(r->r.groupId().equals(group)).findFirst().orElseThrow(()->new IllegalArgumentException("Supersession source group absent"));}
    private static byte[] exact(byte[] bytes,int size){if(bytes==null || bytes.length!=size)throw new IllegalArgumentException("Invalid supersession field length");return bytes.clone();}
    private static byte[] copy(byte[] bytes){return bytes==null?null:bytes.clone();}
    private static void bounded(byte[] bytes,int max){if(bytes==null || bytes.length<4 || bytes.length>max)throw new IllegalArgumentException("Supersession size exceeds bound");}
    private static DataInputStream input(byte[] bytes,int magic)throws IOException{var in=new DataInputStream(new ByteArrayInputStream(bytes));if(in.readInt()!=magic)throw new IOException("Invalid supersession tag");return in;}
    private static void canonical(DataInputStream in,byte[] original,byte[] encoded)throws IOException{if(in.available()!=0 || !Arrays.equals(original,encoded))throw new IOException("Noncanonical supersession");}
    private interface Writer{void write(DataOutputStream out)throws IOException;}
    private static byte[] write(Writer writer,int max){try{var bytes=new ByteArrayOutputStream();var out=new DataOutputStream(bytes);writer.write(out);out.flush();if(bytes.size()>max)throw new IllegalArgumentException("Supersession exceeds bound");return bytes.toByteArray();}catch(IOException impossible){throw new AssertionError(impossible);}}
    private static void uuid(DataOutputStream out,UUID id)throws IOException{out.writeLong(id.getMostSignificantBits());out.writeLong(id.getLeastSignificantBits());}private static UUID uuid(DataInputStream in)throws IOException{return new UUID(in.readLong(),in.readLong());}
    private static void cut(DataOutputStream out,ApplyPosition value)throws IOException{out.writeLong(value.term());out.writeLong(value.index());}private static ApplyPosition cut(DataInputStream in)throws IOException{return new ApplyPosition(in.readLong(),in.readLong());}
    private static void blob(DataOutputStream out,byte[] value)throws IOException{out.writeInt(value.length);out.write(value);}private static byte[] blob(DataInputStream in,int max)throws IOException{int length=in.readInt();if(length<0 || length>max || length>in.available())throw new IOException("Supersession field exceeds bound");return in.readNBytes(length);}
}
