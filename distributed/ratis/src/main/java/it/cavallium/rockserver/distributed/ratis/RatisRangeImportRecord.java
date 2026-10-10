package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import java.io.*;
import java.nio.ByteBuffer;
import java.security.*;
import java.util.*;

/** Current bounded import commands. References identify forced bytes, never local paths. */
public sealed interface RatisRangeImportRecord permits RatisRangeImportRecord.Install,RatisRangeImportRecord.Activate {
    int MAX_BYTES=16*1024, MAGIC=0x52494950;
    byte[] encode();
    void requireDefinition(RangeImportDefinition definition);

    record Install(UUID importId,UUID groupId,long rangeEpoch,byte[] definitionDigest,
                   RatisReferencedFile manifestReference,Map<UUID,RatisReferencedFile> fileReferences) implements RatisRangeImportRecord {
        public Install {
            Objects.requireNonNull(importId); Objects.requireNonNull(groupId); Objects.requireNonNull(manifestReference);
            if(rangeEpoch<0 || definitionDigest.length!=32 || fileReferences.size()>NativeSstImportManifest.MAX_FILES || manifestReference.length()>NativeSstImportManifest.MAX_BYTES || manifestReference.length()==0 || !groupId.equals(manifestReference.groupId()))throw new IllegalArgumentException("Invalid INSTALL reference binding/bounds");
            definitionDigest=definitionDigest.clone(); fileReferences=Map.copyOf(fileReferences);
            for(var reference:fileReferences.values())if(!groupId.equals(reference.groupId()) || reference.length()==0 || reference.length()>NativeSstImportManifest.MAX_FILE_BYTES)throw new IllegalArgumentException("Foreign or oversized INSTALL file reference");
        }
        @Override public byte[] definitionDigest() { return definitionDigest.clone(); }
        @Override public void requireDefinition(RangeImportDefinition definition) {
            var manifest=definition.manifest(groupId);
            if(!importId.equals(definition.importId()) || rangeEpoch!=manifest.rangeEpoch() || !Arrays.equals(definitionDigest,definition.digest()) || manifestReference.length()!=manifest.encode().length || !Arrays.equals(manifestReference.sha256(),manifest.digest()))throw new IllegalArgumentException("INSTALL differs from original import definition/manifest");
            var expected=new HashMap<UUID,NativeSstImportManifest.File>(); manifest.columns().forEach(c->c.files().forEach(file->expected.put(file.fileId(),file)));
            if(!expected.keySet().equals(fileReferences.keySet()))throw new IllegalArgumentException("INSTALL file coverage differs");
            for(var entry:expected.entrySet()) { var reference=fileReferences.get(entry.getKey()); var file=entry.getValue(); if(reference.length()!=file.bytes() || !Arrays.equals(reference.sha256(),file.sha256()))throw new IllegalArgumentException("INSTALL file bytes differ"); }
        }
        public void requireCut(ApplyPosition position) { requireEarlier(manifestReference,position); fileReferences.values().forEach(reference->requireEarlier(reference,position)); }
        @Override public byte[] encode() {
            var out=ByteBuffer.allocate(5+16+16+8+32+RatisReferencedFile.BYTES+4+fileReferences.size()*(16+RatisReferencedFile.BYTES)).putInt(MAGIC).put((byte)1);
            uuid(out,importId); uuid(out,groupId); out.putLong(rangeEpoch).put(definitionDigest).put(manifestReference.encode()).putInt(fileReferences.size());
            fileReferences.keySet().stream().sorted(Comparator.comparing(UUID::toString)).forEach(id->{ uuid(out,id); out.put(fileReferences.get(id).encode()); }); return out.array();
        }
    }
    /** Exact retained per-import PUBLISHED head at one completed catalogue cut. */
    record Publication(UUID catalogueGroup,ApplyPosition completedCatalogueCut,RatisRangeImportCatalogue.Head head) {
        public Publication { Objects.requireNonNull(catalogueGroup); Objects.requireNonNull(completedCatalogueCut); Objects.requireNonNull(head); if(head.phase()!=RatisRangeImportCatalogue.Phase.PUBLISHED)throw new IllegalArgumentException("Activation requires published catalogue import"); }
        public void requireDefinition(RangeImportDefinition definition) { head.requireDefinition(definition); }
        public byte[] encode() { byte[] h=head.encode(); var b=ByteBuffer.allocate(36+h.length); uuid(b,catalogueGroup); b.putLong(completedCatalogueCut.term()).putLong(completedCatalogueCut.index()).putInt(h.length).put(h); return b.array(); }
        public byte[] digest() { try { return MessageDigest.getInstance("SHA-256").digest(head.encode()); } catch(NoSuchAlgorithmException impossible) { throw new AssertionError(impossible); } }
    }
    record Activate(NativeSstImportManifest.Receipt receipt,Publication publication) implements RatisRangeImportRecord {
        public Activate { Objects.requireNonNull(receipt); Objects.requireNonNull(publication); if(!receipt.importId().equals(publication.head().importId()))throw new IllegalArgumentException("Activation receipt belongs to another published import"); }
        @Override public void requireDefinition(RangeImportDefinition definition) { definition.requireReceipt(receipt); publication.requireDefinition(definition); }
        @Override public byte[] encode() { byte[] authority=publication.encode(); return ByteBuffer.allocate(5+NativeSstImportManifest.Receipt.BYTES+authority.length).putInt(MAGIC).put((byte)2).put(receipt.encode()).put(authority).array(); }
    }
    static RatisRangeImportRecord decode(byte[] bytes) {
        if(bytes==null || bytes.length<5 || bytes.length>MAX_BYTES)throw new IllegalArgumentException("Invalid range import record size");
        try {
            var b=ByteBuffer.wrap(bytes); if(b.getInt()!=MAGIC)throw new IllegalArgumentException("Invalid range import tag"); int type=b.get()&255; RatisRangeImportRecord result;
            if(type==1) {
                UUID id=uuid(b),group=uuid(b); long epoch=b.getLong(); byte[] digest=new byte[32]; b.get(digest); var manifest=reference(b); int count=b.getInt(); if(count<0 || count>NativeSstImportManifest.MAX_FILES || b.remaining()!=count*(16+RatisReferencedFile.BYTES))throw new IllegalArgumentException("Invalid INSTALL file count");
                var files=new HashMap<UUID,RatisReferencedFile>(); for(int i=0;i<count;i++) { UUID file=uuid(b); if(files.put(file,reference(b))!=null)throw new IllegalArgumentException("Duplicate INSTALL file"); } result=new Install(id,group,epoch,digest,manifest,files);
            } else if(type==2) {
                byte[] receipt=new byte[NativeSstImportManifest.Receipt.BYTES]; b.get(receipt); UUID catalogue=uuid(b); var cut=new ApplyPosition(b.getLong(),b.getLong()); int n=b.getInt(); if(n!=b.remaining() || n>RatisRangeImportCatalogue.MAX_HEAD_BYTES)throw new IllegalArgumentException("Invalid publication head length"); byte[] head=new byte[n]; b.get(head); result=new Activate(NativeSstImportManifest.Receipt.decode(receipt),new Publication(catalogue,cut,RatisRangeImportCatalogue.Head.decode(head)));
            } else throw new IllegalArgumentException("Unknown range import record kind");
            if(b.hasRemaining() || !Arrays.equals(bytes,result.encode()))throw new IllegalArgumentException("Noncanonical range import record"); return result;
        } catch(java.nio.BufferUnderflowException invalid) { throw new IllegalArgumentException("Truncated range import record",invalid); }
    }
    private static void requireEarlier(RatisReferencedFile reference,ApplyPosition position) { Objects.requireNonNull(position); if(reference.index()>=position.index() || reference.term()>position.term())throw new IllegalArgumentException("INSTALL reference must precede committed entry"); }
    private static RatisReferencedFile reference(ByteBuffer b) { byte[] bytes=new byte[RatisReferencedFile.BYTES]; b.get(bytes); return RatisReferencedFile.decode(bytes); }
    private static UUID uuid(ByteBuffer b) { return new UUID(b.getLong(),b.getLong()); }
    private static void uuid(ByteBuffer b,UUID id) { b.putLong(id.getMostSignificantBits()).putLong(id.getLeastSignificantBits()); }
}
