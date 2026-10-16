package it.cavallium.rockserver.distributed.ratis;

import it.cavallium.rockserver.core.common.distributed.*;
import it.cavallium.rockserver.core.impl.rocksdb.*;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.file.*;
import java.util.*;

/** Independently retained frozen META provenance images, charged to the source artifact reservation ledger. */
final class RatisTopologyRecoveryAnchors {
    static final int MAX_METADATA_BYTES=128*1024;
    private static final long DIRECTORY_METADATA_BYTES=3L*MAX_METADATA_BYTES-2*40; // Reservation marker and its pending replacement share this ceiling.
    // Bound independently of USER volume. Larger metadata histories fail this admitted policy.
    static NativeRangeImage.Limits metadataLimits(NativeRangeImage.Limits limits){return new NativeRangeImage.Limits(limits.maxRowBytes(),Math.min(limits.maxImageBytes(),RatisTopologySourceSnapshotService.MAX_METADATA_BYTES),limits.maxColumns());}
    record Saved(NativeTopologyFreeze freeze,TopologyChange.Owner owner,ApplyPosition catalogueCut,ReplicatedRangeStore.Configuration configuration,
                 NativeRangeImage.Manifest image,Path file) {}
    private final RatisTopologySourceSnapshotArtifacts artifacts;
    private final NativeRangeImage.Limits limits;
    RatisTopologyRecoveryAnchors(RatisTopologySourceSnapshotArtifacts artifacts,NativeRangeImage.Limits limits){this.artifacts=Objects.requireNonNull(artifacts);this.limits=metadataLimits(Objects.requireNonNull(limits));}
    private static UUID id(NativeTopologyFreeze freeze,TopologyChange.Owner owner){return UUID.nameUUIDFromBytes(ByteBuffer.allocate(48).putLong(freeze.header().intentId().getMostSignificantBits()).putLong(freeze.header().intentId().getLeastSignificantBits()).putLong(freeze.groupId().getMostSignificantBits()).putLong(freeze.groupId().getLeastSignificantBits()).putLong(owner.replicaId().getMostSignificantBits()).putLong(owner.replicaId().getLeastSignificantBits()).array());}
    synchronized Saved capture(TopologyChange.Definition definition,ReplicatedRangeStore store,NativeTopologyFreeze freeze,ReplicaDescriptor receiver,ApplyPosition catalogueCut)throws Exception {
        var owner=TopologyChange.Owner.of(receiver);var directory=artifacts.reserveTopologyRecovery(id(freeze,owner));
        Path original=directory.resolve("capture"),image=directory.resolve("image"),receipt=directory.resolve("anchor");
        var fields=recoverFields(original,value->requireCapture(value,freeze,owner,catalogueCut));
        final Map<String,byte[]> captured=fields;
        var retained=recoverFields(receipt,value->requireReceipt(image,value,captured,freeze,owner,catalogueCut));
        if(retained!=null)return read(directory,freeze,owner,catalogueCut);
        if(fields==null && Files.exists(image,LinkOption.NOFOLLOW_LINKS))throw new IOException("Topology recovery image lost its original capture authority");
        if(fields!=null && !Files.exists(image,LinkOption.NOFOLLOW_LINKS) && !cut(fields).equals(store.applied()))throw new IOException("Original topology anchor capture lost its exact native cut");
        if(!Files.exists(image,LinkOption.NOFOLLOW_LINKS)) {
            requireWriteSlot(directory);
            try(var pin=store.captureMetadataForRecovery()) {
                freeze.requireCut(pin.position());if(!pin.groupId().equals(freeze.groupId()))throw new IOException("Anchor pin crossed original source");
                var cfg=pin.configuration();if(cfg==null)throw new IOException("Anchor lacks original native configuration");
                freeze.header().requireDefinition(definition);var range=definition.parents().stream().filter(p->p.groupId().equals(freeze.groupId())).findFirst().orElseThrow();
                if(!range.replicas().contains(receiver))throw new IOException("Anchor receiver is not original source owner");RatisRangeStateMachine.requireSchemaMembership(range,cfg.data());
                if(fields==null) {
                    fields=new HashMap<>();fields.put("freeze",freeze.encode());fields.put("owner",owner(owner));fields.put("catalogueCut",position(catalogueCut));fields.put("cut",position(pin.position()));fields.put("configuration",configuration(cfg));
                    writeFields(original,fields);
                } else if(!cut(fields).equals(pin.position()) || !Arrays.equals(fields.get("configuration"),configuration(cfg)))throw new IOException("Topology anchor capture changed original cut/configuration");
                NativeRangeImage.writeMetadataAdmitted(image,pin,pin.interval(),limits,artifacts.topologyRecoveryAdmission());
            }
        }
        var manifest=validateImage(image,fields);fields=new HashMap<>(fields);fields.put("hash",manifest.sha256());fields.put("bytes",ByteBuffer.allocate(8).putLong(manifest.bytes()).array());writeFields(receipt,fields);
        return read(directory,freeze,owner,catalogueCut);
    }
    Saved read(NativeTopologyFreeze freeze,ReplicaDescriptor receiver)throws IOException {
        var directory=artifacts.topologyRecoveryDirectory(id(freeze,TopologyChange.Owner.of(receiver)));var capture=directory.resolve("capture");
        var original=readFields(Files.exists(capture,LinkOption.NOFOLLOW_LINKS)?capture:directory.resolve("capture.pending"));
        return read(directory,freeze,TopologyChange.Owner.of(receiver),position(original.get("catalogueCut")));
    }
    private Saved read(Path directory,NativeTopologyFreeze freeze,TopologyChange.Owner owner,ApplyPosition catalogueCut)throws IOException {
        var captured=recoverFields(directory.resolve("capture"),value->requireCapture(value,freeze,owner,catalogueCut));
        var fields=recoverFields(directory.resolve("anchor"),value->requireReceipt(directory.resolve("image"),value,captured,freeze,owner,catalogueCut));
        if(fields==null)throw new IOException("Topology recovery anchor has no completed receipt");
        var image=validateImage(directory.resolve("image"),fields);
        var originalCut=position(fields.get("catalogueCut"));
        SessionJournalFiles.force(directory.resolve("image"));SessionJournalFiles.force(directory.resolve("anchor"));SessionJournalFiles.force(directory);
        return new Saved(freeze,owner,originalCut,ReplicatedRangeStore.Configuration.decode(fields.get("configuration")),image,directory.resolve("image"));
    }
    @FunctionalInterface private interface FieldsValidator {void validate(Map<String,byte[]> fields)throws IOException;}
    /** Reconcile complete exact sidecars before choosing a cut; uncertain bytes are retained, never overwritten. */
    private static Map<String,byte[]> recoverFields(Path path,FieldsValidator validator)throws IOException {
        var pending=path.resolveSibling(path.getFileName()+".pending");
        Map<String,byte[]> current=Files.exists(path,LinkOption.NOFOLLOW_LINKS)?readFields(path):null;
        if(current!=null)validator.validate(current);
        if(Files.exists(pending,LinkOption.NOFOLLOW_LINKS)) {
            var next=readFields(pending);validator.validate(next);
            if(current!=null && !sameFields(current,next))throw new IOException("Topology recovery sidecar conflicts with original "+path.getFileName());
            SessionJournalFiles.force(pending);
            if(current==null)Files.move(pending,path,StandardCopyOption.ATOMIC_MOVE);
            else {SessionJournalFiles.force(path);Files.delete(pending);}
            SessionJournalFiles.force(path);SessionJournalFiles.force(path.getParent());current=next;
        }
        return current;
    }
    private static boolean sameFields(Map<String,byte[]> first,Map<String,byte[]> second){return first.keySet().equals(second.keySet()) && first.entrySet().stream().allMatch(row->Arrays.equals(row.getValue(),second.get(row.getKey())));}
    private static void requireCapture(Map<String,byte[]> fields,NativeTopologyFreeze freeze,TopologyChange.Owner owner,ApplyPosition catalogueCut)throws IOException {
        if(!fields.keySet().equals(Set.of("freeze","owner","catalogueCut","cut","configuration")))throw new IOException("Invalid original topology capture fields");
        requireOriginal(fields,freeze,owner,catalogueCut);
        try {var cut=cut(fields);freeze.requireCut(cut);var cfg=ReplicatedRangeStore.Configuration.decode(fields.get("configuration"));if(cfg.index()>=0 && !RatisRelocationOwnerRpc.covers(cut,new ApplyPosition(cfg.term(),cfg.index())))throw new IOException("Capture configuration exceeds original cut");}
        catch(IllegalArgumentException|IllegalStateException invalid){throw new IOException("Invalid original topology cut/configuration",invalid);}
    }
    private void requireReceipt(Path image,Map<String,byte[]> fields,Map<String,byte[]> captured,NativeTopologyFreeze freeze,TopologyChange.Owner owner,ApplyPosition catalogueCut)throws IOException {
        if(captured==null || !fields.keySet().equals(Set.of("freeze","owner","catalogueCut","cut","configuration","hash","bytes")))throw new IOException("Completed anchor lacks exact original capture");
        requireOriginal(fields,freeze,owner,catalogueCut);for(var row:captured.entrySet())if(!Arrays.equals(row.getValue(),fields.get(row.getKey())))throw new IOException("Completed anchor changes original capture");
        var manifest=validateImage(image,fields);var bytes=fields.get("bytes");
        if(!Arrays.equals(manifest.sha256(),fields.get("hash")) || bytes==null || bytes.length!=8 || manifest.bytes()!=ByteBuffer.wrap(bytes).getLong())throw new IOException("Retained topology image identity changed");
    }
    private static void requireWriteSlot(Path directory)throws IOException {
        try(var entries=Files.newDirectoryStream(directory,".native-range-building-*.tmp")){if(entries.iterator().hasNext())throw new IOException("Unsettled topology image scratch prevents another writer");}
    }
    private NativeRangeImage.Manifest validateImage(Path image,Map<String,byte[]> fields)throws IOException {
        var freeze=NativeTopologyFreeze.decode(fields.get("freeze"));var configuration=fields.get("configuration");byte[][] actual={null,null};
        if(NativeRangeImage.kind(image,artifacts.topologyRecoveryAdmission())!=NativeRangeImage.Kind.METADATA)throw new IOException("Topology anchor requires a META image");
        var manifest=NativeRangeImage.visitMetadataAdmitted(image,limits,(section,column,key,value)->{if(section==NativeRangeImage.Section.STATE && key.length==17){if(key[16]==1)actual[0]=value;if(key[16]==0x60)actual[1]=value;}},artifacts.topologyRecoveryAdmission());
        freeze.requireCut(manifest.position());freeze.requireParent(manifest.owner().groupId(),manifest.owner().epoch(),manifest.interval().domainId(),manifest.schemaDigest(),manifest.interval().lowerInclusive(),manifest.interval().upperExclusive());
        if(!manifest.owner().groupId().equals(freeze.groupId()) || !manifest.position().equals(cut(fields)) || !Arrays.equals(actual[0],configuration) || !Arrays.equals(actual[1],freeze.encode()))throw new IOException("Topology recovery image lost original META cut/configuration/FREEZE");
        return manifest;
    }
    private static void requireOriginal(Map<String,byte[]> fields,NativeTopologyFreeze freeze,TopologyChange.Owner owner,ApplyPosition catalogueCut)throws IOException {
        try {var originalOwner=owner(fields.get("owner"));var originalCut=position(fields.get("catalogueCut"));
            if(originalCut.term()==0 || !Arrays.equals(fields.get("freeze"),freeze.encode()) || !originalOwner.equals(owner)
                    || !freeze.header().participant(TopologyChange.Stage.FREEZE,freeze.groupId()).owners().contains(owner)
                    || !RatisRelocationOwnerRpc.covers(catalogueCut,originalCut))throw new IOException("Topology anchor changed original capture authority");}
        catch(IllegalArgumentException invalid){throw new IOException("Invalid original topology capture",invalid);}
    }
    private static byte[] owner(TopologyChange.Owner owner){return ByteBuffer.allocate(49).putLong(owner.replicaId().getMostSignificantBits()).putLong(owner.replicaId().getLeastSignificantBits()).put((byte)owner.role().ordinal()).put(owner.identityDigest()).array();}
    private static TopologyChange.Owner owner(byte[] bytes)throws IOException {if(bytes==null || bytes.length!=49)throw new IOException("Invalid metadata anchor owner");var in=ByteBuffer.wrap(bytes);var id=new UUID(in.getLong(),in.getLong());int role=in.get()&255;if(role>=ReplicaDescriptor.Role.values().length)throw new IOException("Invalid metadata anchor role");byte[] identity=new byte[32];in.get(identity);return new TopologyChange.Owner(id,ReplicaDescriptor.Role.values()[role],identity);}
    private static ApplyPosition position(byte[] bytes)throws IOException {if(bytes==null || bytes.length!=16)throw new IOException("Invalid topology capture cut");var in=ByteBuffer.wrap(bytes);return new ApplyPosition(in.getLong(),in.getLong());}
    private static byte[] position(ApplyPosition cut){return ByteBuffer.allocate(16).putLong(cut.term()).putLong(cut.index()).array();}
    private static ApplyPosition cut(Map<String,byte[]> fields)throws IOException {return position(fields.get("cut"));}
    private static byte[] configuration(ReplicatedRangeStore.Configuration cfg){return ByteBuffer.allocate(16+cfg.data().length).putLong(cfg.term()).putLong(cfg.index()).put(cfg.data()).array();}
    private static Map<String,byte[]> readFields(Path path)throws IOException {if(!Files.isRegularFile(path,LinkOption.NOFOLLOW_LINKS) || Files.size(path)>MAX_METADATA_BYTES)throw new IOException("Invalid topology anchor metadata");return RatisSchemaClientJournal.read(path);}
    private static void writeFields(Path path,Map<String,byte[]> fields)throws IOException {long size=1024;for(var bytes:fields.values())size+=bytes.length;if(size>MAX_METADATA_BYTES)throw new IOException("Topology anchor metadata budget exhausted");RatisSchemaClientJournal.write(path,fields);SessionJournalFiles.force(path);SessionJournalFiles.force(path.getParent());}
    static void validateDirectory(Path directory,NativeRangeImage.Limits limits)throws IOException {
        limits=metadataLimits(limits);int files=0,images=0;long metadata=0;
        try(var entries=Files.newDirectoryStream(directory)){for(var file:entries){
            if(++files>6 || !Files.isRegularFile(file,LinkOption.NOFOLLOW_LINKS))throw new IOException("Invalid recovery artifact directory");
            String name=file.getFileName().toString();boolean image=name.equals("image") || name.matches("[.]native-range-building-.*[.]tmp");long bytes=Files.size(file);
            if(image && ++images>2 || !image && !Set.of("capture","capture.pending","anchor","anchor.pending").contains(name) || bytes>(image?limits.maxImageBytes():MAX_METADATA_BYTES))throw new IOException("Recovery artifact quota/identity exceeded");
            if(!image && (metadata=Math.addExact(metadata,bytes))>DIRECTORY_METADATA_BYTES)throw new IOException("Recovery aggregate metadata reservation exceeded");
        }}
    }
}
